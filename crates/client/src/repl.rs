use std::path::PathBuf;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use arrow::util::pretty::pretty_format_batches;
use ffq_common::{EngineConfig, FfqError};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde_json::{Map, Value};

use crate::Engine;
use crate::engine::TableSchemaOrigin;

/// REPL startup options.
#[derive(Debug, Clone)]
pub struct ReplOptions {
    /// Engine configuration used for the session.
    pub config: EngineConfig,
}

/// Runs the interactive FFQ SQL REPL.
///
/// The REPL supports SQL statements and shell-style commands (for example `\help`, `\tables`,
/// `\schema`, `\mode`), with persistent history via `~/.ffq_history`.
///
/// # Errors
/// Returns an error if engine bootstrap or line-editor initialization fails.
pub fn run_repl(opts: ReplOptions) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(opts.config)?;
    let mut rl = DefaultEditor::new()?;
    let history_path = repl_history_path();
    if let Err(err) = rl.load_history(&history_path) {
        if !matches!(err, ReadlineError::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound) {
            eprintln!(
                "warning: failed to load history '{}': {err}",
                history_path.display()
            );
        }
    }

    let mut plan_enabled = false;
    let mut timing_enabled = false;
    let mut output_mode = OutputMode::Table;
    let mut sql_buffer = String::new();

    eprintln!("FFQ REPL (type \\q to quit)");
    loop {
        let prompt = if sql_buffer.is_empty() {
            "ffq> "
        } else {
            " ...> "
        };
        let line = match rl.readline(prompt) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = rl.add_history_entry(line.as_str());
                }
                line
            }
            Err(ReadlineError::Interrupted) => {
                if !sql_buffer.trim().is_empty() {
                    eprintln!("error: statement cancelled");
                    sql_buffer.clear();
                }
                continue;
            }
            Err(ReadlineError::Eof) => {
                if !sql_buffer.trim().is_empty() {
                    eprintln!("error: unterminated SQL statement (missing ';')");
                }
                break;
            }
            Err(err) => {
                eprintln!("error: readline failed: {err}");
                break;
            }
        };

        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }

        // Shell commands are only recognized when not in the middle of SQL input.
        if raw.starts_with('\\') && sql_buffer.trim().is_empty() {
            match handle_command(
                raw,
                &engine,
                &mut plan_enabled,
                &mut timing_enabled,
                &mut output_mode,
            ) {
                CommandResult::Continue => continue,
                CommandResult::Exit => break,
            }
        }

        // Ignore SQL comments.
        if raw.starts_with("--") {
            continue;
        }

        append_sql_line(&mut sql_buffer, raw);

        if !statement_terminated(&sql_buffer) {
            continue;
        }

        let sql = sql_buffer
            .trim_end()
            .trim_end_matches(';')
            .trim()
            .to_string();
        sql_buffer.clear();
        if sql.is_empty() {
            continue;
        }
        let is_write_stmt = is_write_statement(&sql);

        let started = Instant::now();
        match engine.sql(&sql) {
            Ok(df) => {
                if plan_enabled {
                    println!("{:#?}", df.logical_plan());
                }
                match futures::executor::block_on(df.collect()) {
                    Ok(batches) => {
                        if is_write_stmt && is_empty_sink_result(&batches) {
                            println!("OK");
                        } else if batches.is_empty() {
                            println!("OK: 0 rows");
                        } else {
                            if let Err(e) = print_batches(&batches, output_mode) {
                                print_repl_error("render", &e);
                            }
                        }
                    }
                    Err(e) => print_repl_error("execution", &e),
                }
            }
            Err(e) => print_repl_error("planning", &e),
        }
        if timing_enabled {
            eprintln!("time: {:.3} ms", started.elapsed().as_secs_f64() * 1000.0);
        }
    }
    if let Err(err) = rl.save_history(&history_path) {
        eprintln!(
            "warning: failed to save history '{}': {err}",
            history_path.display()
        );
    }
    let _ = futures::executor::block_on(engine.shutdown());
    Ok(())
}

fn repl_history_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".ffq_history");
    }
    PathBuf::from(".ffq_history")
}

fn statement_terminated(sql: &str) -> bool {
    sql.trim_end().ends_with(';')
}

fn append_sql_line(sql_buffer: &mut String, raw: &str) {
    if !sql_buffer.is_empty() {
        sql_buffer.push('\n');
    }
    sql_buffer.push_str(raw);
}

fn is_write_statement(sql: &str) -> bool {
    sql.split_whitespace()
        .next()
        .map(|token| token.eq_ignore_ascii_case("insert"))
        .unwrap_or(false)
}

fn is_empty_sink_result(batches: &[RecordBatch]) -> bool {
    batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0)
}

enum CommandResult {
    Continue,
    Exit,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OutputMode {
    Table,
    Csv,
    Json,
}

fn handle_command(
    raw: &str,
    engine: &Engine,
    plan_enabled: &mut bool,
    timing_enabled: &mut bool,
    output_mode: &mut OutputMode,
) -> CommandResult {
    let parts = raw.split_whitespace().collect::<Vec<_>>();
    match parts.first().copied().unwrap_or_default() {
        "\\q" => CommandResult::Exit,
        "\\help" => {
            print_help();
            CommandResult::Continue
        }
        "\\tables" => {
            let names = engine.list_tables();
            if names.is_empty() {
                println!("(no tables registered)");
            } else {
                for name in names {
                    println!("{name}");
                }
            }
            CommandResult::Continue
        }
        "\\schema" => {
            if parts.len() != 2 {
                eprintln!("error: usage: \\schema <table>");
                return CommandResult::Continue;
            }
            match engine.table_schema_with_origin(parts[1]) {
                Ok(Some((schema, origin))) => {
                    println!(
                        "origin: {}",
                        match origin {
                            TableSchemaOrigin::CatalogDefined => "catalog-defined",
                            TableSchemaOrigin::Inferred => "inferred",
                        }
                    );
                    for field in schema.fields() {
                        println!(
                            "{}: {:?}{}",
                            field.name(),
                            field.data_type(),
                            if field.is_nullable() { " nullable" } else { "" }
                        );
                    }
                }
                Ok(None) => {
                    eprintln!("error: table '{}' has no schema", parts[1]);
                }
                Err(e) => print_repl_error("schema", &e),
            }
            CommandResult::Continue
        }
        "\\plan" => {
            if parts.len() != 2 || !matches!(parts[1], "on" | "off") {
                eprintln!("error: usage: \\plan on|off");
                return CommandResult::Continue;
            }
            *plan_enabled = parts[1] == "on";
            println!("plan {}", if *plan_enabled { "on" } else { "off" });
            CommandResult::Continue
        }
        "\\timing" => {
            if parts.len() != 2 || !matches!(parts[1], "on" | "off") {
                eprintln!("error: usage: \\timing on|off");
                return CommandResult::Continue;
            }
            *timing_enabled = parts[1] == "on";
            println!("timing {}", if *timing_enabled { "on" } else { "off" });
            CommandResult::Continue
        }
        "\\mode" => {
            if parts.len() != 2 {
                eprintln!("error: usage: \\mode table|csv|json");
                return CommandResult::Continue;
            }
            *output_mode = match parts[1] {
                "table" => OutputMode::Table,
                "csv" => OutputMode::Csv,
                "json" => OutputMode::Json,
                _ => {
                    eprintln!("error: usage: \\mode table|csv|json");
                    return CommandResult::Continue;
                }
            };
            println!("mode {}", parts[1]);
            CommandResult::Continue
        }
        other => {
            eprintln!("error: unknown command '{other}'. try \\help");
            CommandResult::Continue
        }
    }
}

fn print_help() {
    println!("REPL commands:");
    println!("  \\help              show this help");
    println!("  \\q                 quit");
    println!("  \\tables            list registered tables");
    println!("  \\schema <table>    show table schema");
    println!("  \\plan on|off       toggle logical plan output");
    println!("  \\timing on|off     toggle query elapsed-time output");
    println!("  \\mode table|csv|json  set output rendering mode");
}

fn print_batches(batches: &[RecordBatch], mode: OutputMode) -> Result<(), FfqError> {
    match mode {
        OutputMode::Table => {
            let rendered = pretty_format_batches(batches)
                .map_err(|e| FfqError::Execution(format!("table render failed: {e}")))?;
            println!("{rendered}");
        }
        OutputMode::Csv => {
            print_batches_csv(batches)?;
        }
        OutputMode::Json => {
            print_batches_json(batches)?;
        }
    }
    Ok(())
}

fn print_batches_csv(batches: &[RecordBatch]) -> Result<(), FfqError> {
    if batches.is_empty() {
        return Ok(());
    }
    let schema = batches[0].schema();
    let header = schema
        .fields()
        .iter()
        .map(|f| csv_escape(f.name()))
        .collect::<Vec<_>>()
        .join(",");
    println!("{header}");
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cols = Vec::with_capacity(batch.num_columns());
            for col in 0..batch.num_columns() {
                let arr = batch.column(col);
                let v = array_value_to_string(arr, row)
                    .map_err(|e| FfqError::Execution(format!("csv render failed: {e}")))?;
                cols.push(csv_escape(&v));
            }
            println!("{}", cols.join(","));
        }
    }
    Ok(())
}

fn print_batches_json(batches: &[RecordBatch]) -> Result<(), FfqError> {
    let mut rows = Vec::<Value>::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut obj = Map::new();
            for col in 0..batch.num_columns() {
                let arr = batch.column(col);
                let key = schema.field(col).name().clone();
                if arr.is_null(row) {
                    obj.insert(key, Value::Null);
                } else {
                    let v = array_value_to_string(arr, row)
                        .map_err(|e| FfqError::Execution(format!("json render failed: {e}")))?;
                    obj.insert(key, Value::String(v));
                }
            }
            rows.push(Value::Object(obj));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&rows)
            .map_err(|e| FfqError::Execution(format!("json encode failed: {e}")))?
    );
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn print_repl_error(stage: &str, err: &FfqError) {
    let (category, hint) = classify_error(err);
    eprintln!("[{category}] {stage}: {err}");
    if let Some(hint) = hint {
        eprintln!("hint: {hint}");
    }
}

fn classify_error(err: &FfqError) -> (&'static str, Option<&'static str>) {
    match err {
        FfqError::Planning(msg) => ("planning", planning_hint(msg)),
        FfqError::Execution(msg) => ("execution", execution_hint(msg)),
        FfqError::InvalidConfig(msg) => ("config", config_hint(msg)),
        FfqError::Io(_) => (
            "io",
            Some("check file paths/permissions and that fixture/catalog files exist"),
        ),
        FfqError::Unsupported(msg) => ("unsupported", unsupported_hint(msg)),
    }
}

fn planning_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("unknown table") {
        return Some("register the table first; try \\tables to inspect current session tables");
    }
    if m.contains("join key") {
        return Some("verify table schemas include join columns and names match query references");
    }
    None
}

fn execution_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("schema inference failed") {
        return Some(
            "check parquet path(s) exist/readable and set schema policy (--schema-inference on|strict|permissive)",
        );
    }
    if m.contains("schema drift detected") {
        return Some(
            "data files changed since cached schema; use --schema-drift-policy refresh (or FFQ_SCHEMA_DRIFT_POLICY=refresh)",
        );
    }
    if m.contains("incompatible parquet files") {
        return Some(
            "table points to parquet files with incompatible schemas; align file schemas or use a separate table per schema",
        );
    }
    if m.contains("connect coordinator failed")
        || m.contains("transport error")
        || m.contains("coordinator")
    {
        return Some(
            "check --coordinator-endpoint and ensure coordinator/worker services are reachable",
        );
    }
    if m.contains("query vector dim") {
        return Some(
            "ensure query vector length matches embedding column fixed-size list dimension",
        );
    }
    None
}

fn config_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("schema inference failed") {
        return Some(
            "schema inference failed: verify parquet files exist and are readable; or set schema explicitly in catalog",
        );
    }
    if m.contains("schema drift detected") {
        return Some(
            "set --schema-drift-policy refresh to auto-refresh on file changes, or keep fail for strict reproducibility",
        );
    }
    if m.contains("incompatible parquet files") {
        return Some(
            "all parquet files in one table must have compatible schema; split mismatched files into separate tables",
        );
    }
    if m.contains("has no schema") {
        return Some(
            "define table schema in catalog or enable inference (--schema-inference on|strict|permissive)",
        );
    }
    if m.contains("schema") {
        return Some("define table schema in catalog/TableDef before querying");
    }
    if m.contains("uri or paths") {
        return Some("set table uri or paths in catalog entry");
    }
    if m.contains("catalog") {
        return Some("verify --catalog path exists and has .json/.toml extension");
    }
    None
}

fn unsupported_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("order by") {
        return Some("v1 supports ORDER BY only for cosine_similarity(...) DESC LIMIT k pattern");
    }
    if m.contains("qdrant") {
        return Some(
            "enable required feature flags (vector/qdrant) or use brute-force fallback shape",
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow_schema::Schema;
    use ffq_common::Result;
    use ffq_storage::{TableDef, TableStats};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn write_statement_detection_is_case_insensitive() {
        assert!(is_write_statement("INSERT INTO t SELECT * FROM s"));
        assert!(is_write_statement("insert into t select * from s"));
        assert!(!is_write_statement("SELECT * FROM t"));
    }

    #[test]
    fn empty_sink_result_detects_empty_batches() {
        assert!(is_empty_sink_result(&[]));

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(is_empty_sink_result(&[empty_batch]));
    }

    #[test]
    fn command_dispatch_toggles_modes_and_handles_invalid_command() {
        let engine = Engine::new(EngineConfig::default()).expect("engine");
        let mut plan_enabled = false;
        let mut timing_enabled = false;
        let mut output_mode = OutputMode::Table;

        let result = handle_command(
            "\\plan on",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Continue));
        assert!(plan_enabled);

        let result = handle_command(
            "\\timing on",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Continue));
        assert!(timing_enabled);

        let result = handle_command(
            "\\mode json",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Continue));
        assert_eq!(output_mode, OutputMode::Json);

        let result = handle_command(
            "\\mode invalid",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Continue));
        assert_eq!(output_mode, OutputMode::Json);

        let result = handle_command(
            "\\doesnotexist",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Continue));

        let result = handle_command(
            "\\q",
            &engine,
            &mut plan_enabled,
            &mut timing_enabled,
            &mut output_mode,
        );
        assert!(matches!(result, CommandResult::Exit));
    }

    #[test]
    fn multiline_sql_accumulation_preserves_newlines_and_termination() {
        let mut sql_buffer = String::new();
        append_sql_line(&mut sql_buffer, "SELECT a");
        assert!(!statement_terminated(&sql_buffer));

        append_sql_line(&mut sql_buffer, "FROM t");
        assert!(!statement_terminated(&sql_buffer));
        assert_eq!(sql_buffer, "SELECT a\nFROM t");

        append_sql_line(&mut sql_buffer, "WHERE a > 1;");
        assert!(statement_terminated(&sql_buffer));
        assert_eq!(sql_buffer, "SELECT a\nFROM t\nWHERE a > 1;");
    }

    #[test]
    fn sql_dispatch_executes_against_fixture_table() {
        let parquet_path = unique_temp_path("ffq_repl_core", "parquet");
        write_test_parquet(
            &parquet_path,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        )
        .expect("write parquet");

        let engine = Engine::new(EngineConfig::default()).expect("engine");
        engine.register_table(
            "t",
            TableDef {
                name: "t".to_string(),
                uri: parquet_path.to_string_lossy().to_string(),
                paths: Vec::new(),
                format: "parquet".to_string(),
                schema: Some(Schema::new(vec![arrow_schema::Field::new(
                    "a",
                    arrow_schema::DataType::Int64,
                    false,
                )])),
                stats: TableStats::default(),
                options: HashMap::new(),
            },
        );

        let batches =
            futures::executor::block_on(engine.sql("SELECT a FROM t").expect("sql").collect())
                .expect("collect");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);

        let _ = std::fs::remove_file(parquet_path);
    }

    fn write_test_parquet(path: &std::path::Path, cols: Vec<ArrayRef>) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), cols)
            .map_err(|e| FfqError::Execution(format!("build test batch failed: {e}")))?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| FfqError::Execution(format!("open parquet writer failed: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| FfqError::Execution(format!("write parquet failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FfqError::Execution(format!("close parquet writer failed: {e}")))?;
        Ok(())
    }

    fn unique_temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{nanos}.{ext}"))
    }
}
