use std::io::Write;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use ffq_common::EngineConfig;

use crate::Engine;

#[derive(Debug, Clone)]
pub struct ReplOptions {
    pub config: EngineConfig,
}

pub fn run_repl(opts: ReplOptions) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(opts.config)?;
    let mut plan_enabled = false;
    let mut timing_enabled = false;

    eprintln!("FFQ REPL (type \\q to quit)");
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        print!("ffq> ");
        std::io::stdout().flush()?;
        line.clear();
        // Ctrl+D => EOF => exit
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }

        if raw.starts_with('\\') {
            match handle_command(raw, &engine, &mut plan_enabled, &mut timing_enabled) {
                CommandResult::Continue => continue,
                CommandResult::Exit => break,
            }
        }

        let sql = raw.trim_end_matches(';');
        let started = Instant::now();
        match engine.sql(sql) {
            Ok(df) => {
                if plan_enabled {
                    println!("{:#?}", df.logical_plan());
                }
                match futures::executor::block_on(df.collect()) {
                    Ok(batches) => {
                        if batches.is_empty() {
                            println!("OK: 0 rows");
                        } else {
                            let rendered = pretty_format_batches(&batches)?;
                            println!("{rendered}");
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

enum CommandResult {
    Continue,
    Exit,
}

fn handle_command(
    raw: &str,
    engine: &Engine,
    plan_enabled: &mut bool,
    timing_enabled: &mut bool,
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
}
