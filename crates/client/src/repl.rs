use std::io::Write;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use arrow::util::display::array_value_to_string;
use arrow::record_batch::RecordBatch;
use ffq_common::{EngineConfig, FfqError};
use serde_json::{Map, Value};

use crate::Engine;

#[derive(Debug, Clone)]
pub struct ReplOptions {
    pub config: EngineConfig,
}

pub fn run_repl(opts: ReplOptions) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(opts.config)?;
    let mut plan_enabled = false;
    let mut timing_enabled = false;
    let mut output_mode = OutputMode::Table;
    let mut sql_buffer = String::new();

    eprintln!("FFQ REPL (type \\q to quit)");
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        let prompt = if sql_buffer.is_empty() { "ffq> " } else { " ...> " };
        print!("{prompt}");
        std::io::stdout().flush()?;
        line.clear();
        // Ctrl+D => EOF => exit
        if stdin.read_line(&mut line)? == 0 {
            if !sql_buffer.trim().is_empty() {
                eprintln!("error: unterminated SQL statement (missing ';')");
            }
            break;
        }
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

        if !sql_buffer.is_empty() {
            sql_buffer.push('\n');
        }
        sql_buffer.push_str(raw);

        if !statement_terminated(&sql_buffer) {
            continue;
        }

        let sql = sql_buffer.trim_end().trim_end_matches(';').trim().to_string();
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
    let _ = futures::executor::block_on(engine.shutdown());
    Ok(())
}

fn statement_terminated(sql: &str) -> bool {
    sql.trim_end().ends_with(';')
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
            match engine.table_schema(parts[1]) {
                Ok(Some(schema)) => {
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

fn print_batches(
    batches: &[RecordBatch],
    mode: OutputMode,
) -> Result<(), FfqError> {
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
    if m.contains("connect coordinator failed")
        || m.contains("transport error")
        || m.contains("coordinator")
    {
        return Some(
            "check --coordinator-endpoint and ensure coordinator/worker services are reachable",
        );
    }
    if m.contains("query vector dim") {
        return Some("ensure query vector length matches embedding column fixed-size list dimension");
    }
    None
}

fn config_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
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
        return Some("enable required feature flags (vector/qdrant) or use brute-force fallback shape");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_schema::Schema;

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
}
