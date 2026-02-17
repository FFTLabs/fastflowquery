use std::io::Write;

use arrow::util::pretty::pretty_format_batches;
use ffq_common::EngineConfig;

use crate::Engine;

#[derive(Debug, Clone)]
pub struct ReplOptions {
    pub config: EngineConfig,
}

pub fn run_repl(opts: ReplOptions) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(opts.config)?;

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
        if raw == "\\q" || raw.eq_ignore_ascii_case("quit") || raw.eq_ignore_ascii_case("exit") {
            break;
        }
        let sql = raw.trim_end_matches(';');
        match engine.sql(sql) {
            Ok(df) => match futures::executor::block_on(df.collect()) {
                Ok(batches) => {
                    if batches.is_empty() {
                        println!("OK: 0 rows");
                    } else {
                        let rendered = pretty_format_batches(&batches)?;
                        println!("{rendered}");
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            },
            Err(e) => eprintln!("error: {e}"),
        }
    }
    let _ = futures::executor::block_on(engine.shutdown());
    Ok(())
}
