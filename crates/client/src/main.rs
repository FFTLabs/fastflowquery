use arrow::util::pretty::pretty_format_batches;
use ffq_client::Engine;
use ffq_client::repl::{run_repl, ReplOptions};
use ffq_common::EngineConfig;
use ffq_storage::Catalog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .map(|a| a == "--help" || a == "-h")
        .unwrap_or(false)
    {
        print_usage();
        return Ok(());
    }

    if args.first().map(|a| a.as_str()) == Some("repl") {
        let opts = parse_repl_opts(&args)?;
        return run_repl(ReplOptions { config: opts.config });
    }

    let opts = parse_query_opts(&args)?;
    let engine = Engine::new(EngineConfig::default())?;
    if let Some(catalog_path) = &opts.catalog {
        let catalog = Catalog::load(catalog_path)?;
        for table in catalog.tables() {
            let name = table.name.clone();
            engine.register_table(name, table);
        }
    }
    let df = engine.sql(&opts.sql)?;

    if opts.plan_only {
        println!("{:#?}", df.logical_plan());
    } else {
        let batches = futures::executor::block_on(df.collect())?;
        if batches.is_empty() {
            println!("OK: 0 rows");
        } else {
            let rendered = pretty_format_batches(&batches)?;
            println!("{rendered}");
        }
    }

    let _ = futures::executor::block_on(engine.shutdown());
    Ok(())
}

#[derive(Debug, Clone)]
struct QueryOpts {
    sql: String,
    plan_only: bool,
    catalog: Option<String>,
}

#[derive(Debug, Clone)]
struct ReplOpts {
    config: EngineConfig,
}

fn parse_query_opts(args: &[String]) -> Result<QueryOpts, Box<dyn std::error::Error>> {
    // Backward-compatible forms:
    //   ffq-client "SELECT 1"
    //   ffq-client --plan "SELECT 1"
    // New subcommand form:
    //   ffq-client query --sql "SELECT ..." [--catalog path] [--plan]
    if args.first().map(|a| a.as_str()) != Some("query") {
        let mut tail = args.to_vec();
        let plan_only = tail.first().map(|s| s == "--plan").unwrap_or(false);
        if plan_only {
            tail.remove(0);
        }
        let sql = tail
            .first()
            .cloned()
            .unwrap_or_else(|| "SELECT 1".to_string());
        return Ok(QueryOpts {
            sql,
            plan_only,
            catalog: None,
        });
    }

    let mut sql = "SELECT 1".to_string();
    let mut plan_only = false;
    let mut catalog = None;

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--sql" => {
                i += 1;
                sql = args
                    .get(i)
                    .cloned()
                    .ok_or("missing value for --sql")?;
            }
            "--catalog" => {
                i += 1;
                catalog = Some(
                    args.get(i)
                        .cloned()
                        .ok_or("missing value for --catalog")?,
                );
            }
            "--plan" => {
                plan_only = true;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
        i += 1;
    }

    Ok(QueryOpts {
        sql,
        plan_only,
        catalog,
    })
}

fn parse_repl_opts(args: &[String]) -> Result<ReplOpts, Box<dyn std::error::Error>> {
    let mut config = EngineConfig::default();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--catalog" => {
                i += 1;
                config.catalog_path = Some(
                    args.get(i)
                        .cloned()
                        .ok_or("missing value for --catalog")?,
                );
            }
            "--coordinator-endpoint" => {
                i += 1;
                config.coordinator_endpoint = Some(
                    args.get(i)
                        .cloned()
                        .ok_or("missing value for --coordinator-endpoint")?,
                );
            }
            "--batch-size-rows" => {
                i += 1;
                config.batch_size_rows = args
                    .get(i)
                    .ok_or("missing value for --batch-size-rows")?
                    .parse()
                    .map_err(|_| "invalid value for --batch-size-rows")?;
            }
            "--mem-budget-bytes" => {
                i += 1;
                config.mem_budget_bytes = args
                    .get(i)
                    .ok_or("missing value for --mem-budget-bytes")?
                    .parse()
                    .map_err(|_| "invalid value for --mem-budget-bytes")?;
            }
            "--spill-dir" => {
                i += 1;
                config.spill_dir = args
                    .get(i)
                    .cloned()
                    .ok_or("missing value for --spill-dir")?;
            }
            "--shuffle-partitions" => {
                i += 1;
                config.shuffle_partitions = args
                    .get(i)
                    .ok_or("missing value for --shuffle-partitions")?
                    .parse()
                    .map_err(|_| "invalid value for --shuffle-partitions")?;
            }
            "--broadcast-threshold-bytes" => {
                i += 1;
                config.broadcast_threshold_bytes = args
                    .get(i)
                    .ok_or("missing value for --broadcast-threshold-bytes")?
                    .parse()
                    .map_err(|_| "invalid value for --broadcast-threshold-bytes")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument for repl: {other}").into()),
        }
        i += 1;
    }
    Ok(ReplOpts { config })
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  ffq-client \"<SQL>\"");
    eprintln!("  ffq-client --plan \"<SQL>\"");
    eprintln!("  ffq-client query --sql \"<SQL>\" [--catalog PATH] [--plan]");
    eprintln!(
        "  ffq-client repl [--catalog PATH] [--coordinator-endpoint URL] [--batch-size-rows N] [--mem-budget-bytes N] [--spill-dir PATH] [--shuffle-partitions N] [--broadcast-threshold-bytes N]"
    );
}
