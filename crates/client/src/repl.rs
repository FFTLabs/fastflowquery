use std::io::Write;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use arrow::util::display::array_value_to_string;
use arrow::record_batch::RecordBatch;
use ffq_common::EngineConfig;
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

        let started = Instant::now();
        match engine.sql(&sql) {
            Ok(df) => {
                if plan_enabled {
                    println!("{:#?}", df.logical_plan());
                }
                match futures::executor::block_on(df.collect()) {
                    Ok(batches) => {
                        if batches.is_empty() {
                            println!("OK: 0 rows");
                        } else {
                            print_batches(&batches, output_mode)?;
                        }
                    }
                    Err(e) => eprintln!("error: {e}"),
                }
            }
            Err(e) => eprintln!("error: {e}"),
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
                Err(e) => eprintln!("error: {e}"),
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
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        OutputMode::Table => {
            let rendered = pretty_format_batches(batches)?;
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

fn print_batches_csv(batches: &[RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
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
                let v = array_value_to_string(arr, row)?;
                cols.push(csv_escape(&v));
            }
            println!("{}", cols.join(","));
        }
    }
    Ok(())
}

fn print_batches_json(batches: &[RecordBatch]) -> Result<(), Box<dyn std::error::Error>> {
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
                    let v = array_value_to_string(arr, row)?;
                    obj.insert(key, Value::String(v));
                }
            }
            rows.push(Value::Object(obj));
        }
    }
    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
