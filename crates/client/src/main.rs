//! `ffq-client` command-line entrypoint.
//!
//! Architecture role:
//! - provides query mode and interactive REPL mode
//! - maps CLI/env configuration into [`ffq_common::EngineConfig`]
//! - executes SQL via [`ffq_client::Engine`] and renders result batches
//!
//! Key flows:
//! - `query`: one-shot SQL execution
//! - `repl`: interactive SQL and shell commands
//!
//! Feature flags:
//! - runtime behavior follows features enabled in `ffq-client` (for example `distributed`).

use arrow::util::pretty::pretty_format_batches;
use ffq_client::Engine;
use ffq_client::repl::{ReplOptions, run_repl};
use ffq_common::{EngineConfig, FfqError, SchemaDriftPolicy, SchemaInferencePolicy};
use ffq_storage::Catalog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = run() {
        print_cli_error(&*err);
        std::process::exit(1);
    }
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
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
        return run_repl(ReplOptions {
            config: opts.config,
        });
    }

    let opts = parse_query_opts(&args)?;
    let engine = Engine::new(EngineConfig::default())?;
    if let Some(catalog_path) = &opts.catalog {
        let catalog = Catalog::load(catalog_path)?;
        for table in catalog.tables() {
            let name = table.name.clone();
            engine.register_table_checked(name, table)?;
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
/// Parsed one-shot query mode CLI options.
struct QueryOpts {
    sql: String,
    plan_only: bool,
    catalog: Option<String>,
}

#[derive(Debug, Clone)]
/// Parsed REPL mode CLI options.
struct ReplOpts {
    config: EngineConfig,
}

/// Parse query-mode CLI arguments.
///
/// Supports both legacy form (`ffq-client "SELECT 1"`) and subcommand form
/// (`ffq-client query --sql ...`).
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
                sql = args.get(i).cloned().ok_or("missing value for --sql")?;
            }
            "--catalog" => {
                i += 1;
                catalog = Some(args.get(i).cloned().ok_or("missing value for --catalog")?);
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

/// Parse REPL CLI arguments and map into [`EngineConfig`].
fn parse_repl_opts(args: &[String]) -> Result<ReplOpts, Box<dyn std::error::Error>> {
    let mut config = EngineConfig::default();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--catalog" => {
                i += 1;
                config.catalog_path =
                    Some(args.get(i).cloned().ok_or("missing value for --catalog")?);
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
            "--join-radix-bits" => {
                i += 1;
                config.join_radix_bits = args
                    .get(i)
                    .ok_or("missing value for --join-radix-bits")?
                    .parse()
                    .map_err(|_| "invalid value for --join-radix-bits")?;
            }
            "--schema-inference" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --schema-inference")?;
                config.schema_inference = parse_schema_inference_policy(raw)?;
            }
            "--schema-writeback" => {
                i += 1;
                let raw = args.get(i).ok_or("missing value for --schema-writeback")?;
                config.schema_writeback = parse_bool(raw, "--schema-writeback")?;
            }
            "--schema-drift-policy" => {
                i += 1;
                let raw = args
                    .get(i)
                    .ok_or("missing value for --schema-drift-policy")?;
                config.schema_drift_policy = parse_schema_drift_policy(raw)?;
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

/// Print CLI usage for query and REPL modes.
fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  ffq-client \"<SQL>\"");
    eprintln!("  ffq-client --plan \"<SQL>\"");
    eprintln!("  ffq-client query --sql \"<SQL>\" [--catalog PATH] [--plan]");
    eprintln!(
        "  ffq-client repl [--catalog PATH] [--coordinator-endpoint URL] [--batch-size-rows N] [--mem-budget-bytes N] [--spill-dir PATH] [--shuffle-partitions N] [--broadcast-threshold-bytes N] [--join-radix-bits N] [--schema-inference off|on|strict|permissive] [--schema-writeback true|false] [--schema-drift-policy fail|refresh]"
    );
}

/// Parse schema inference policy option value.
fn parse_schema_inference_policy(
    raw: &str,
) -> Result<SchemaInferencePolicy, Box<dyn std::error::Error>> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(SchemaInferencePolicy::Off),
        "on" => Ok(SchemaInferencePolicy::On),
        "strict" => Ok(SchemaInferencePolicy::Strict),
        "permissive" => Ok(SchemaInferencePolicy::Permissive),
        other => Err(format!(
            "invalid value for --schema-inference: {other} (expected off|on|strict|permissive)"
        )
        .into()),
    }
}

/// Parse schema drift policy option value.
fn parse_schema_drift_policy(raw: &str) -> Result<SchemaDriftPolicy, Box<dyn std::error::Error>> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "fail" => Ok(SchemaDriftPolicy::Fail),
        "refresh" => Ok(SchemaDriftPolicy::Refresh),
        other => Err(format!(
            "invalid value for --schema-drift-policy: {other} (expected fail|refresh)"
        )
        .into()),
    }
}

/// Parse boolean-like CLI option values.
fn parse_bool(raw: &str, flag: &str) -> Result<bool, Box<dyn std::error::Error>> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid value for {flag}: {other} (expected true|false)").into()),
    }
}

/// Print categorized CLI errors with recovery hints.
fn print_cli_error(err: &(dyn std::error::Error + 'static)) {
    if let Some(ffq) = err.downcast_ref::<FfqError>() {
        let (category, hint) = classify_ffq_error(ffq);
        eprintln!("[{category}] {ffq}");
        if let Some(hint) = hint {
            eprintln!("hint: {hint}");
        }
        return;
    }
    let msg = err.to_string();
    eprintln!("[error] {msg}");
    if msg
        .to_ascii_lowercase()
        .contains("incompatible parquet files")
    {
        eprintln!(
            "hint: table points to parquet files with incompatible schemas; align schemas or split into separate tables"
        );
    }
}

/// Map internal error type to user-facing category + hint.
///
/// Taxonomy mapping:
/// - `Planning` -> `[planning]`
/// - `Execution` -> `[execution]`
/// - `InvalidConfig` -> `[config]`
/// - `Unsupported` -> `[unsupported]`
/// - `Io` -> `[io]`
fn classify_ffq_error(err: &FfqError) -> (&'static str, Option<&'static str>) {
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
    if m.contains("e_recursive_cte_overflow") {
        return Some(
            "increase recursive CTE depth limit (FFQ_RECURSIVE_CTE_MAX_DEPTH / config.recursive_cte_max_depth)",
        );
    }
    if m.contains("unknown table") {
        return Some("table is not registered; pass --catalog or register it before querying");
    }
    if m.contains("join key") {
        return Some("verify schemas include join columns and names match query references");
    }
    None
}

fn execution_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("e_subquery_scalar_row_violation") {
        return Some("scalar subquery must return one column and at most one row");
    }
    if m.contains("schema inference failed") {
        return Some(
            "check parquet path(s) exist/readable and set schema policy (--schema-inference on|strict|permissive)",
        );
    }
    if m.contains("schema drift detected") {
        return Some(
            "data files changed since cached schema; set FFQ_SCHEMA_DRIFT_POLICY=refresh (or --schema-drift-policy refresh in REPL)",
        );
    }
    if m.contains("incompatible parquet files") {
        return Some(
            "table points to parquet files with incompatible schemas; align file schemas or split into separate tables",
        );
    }
    if m.contains("connect coordinator failed")
        || m.contains("transport error")
        || m.contains("coordinator")
    {
        return Some(
            "check FFQ_COORDINATOR_ENDPOINT and ensure coordinator/worker services are reachable",
        );
    }
    None
}

fn config_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("schema inference failed") {
        return Some(
            "verify parquet files exist and are readable; or define schema explicitly in catalog",
        );
    }
    if m.contains("schema drift detected") {
        return Some("set FFQ_SCHEMA_DRIFT_POLICY=refresh to auto-refresh schema on file changes");
    }
    if m.contains("incompatible parquet files") {
        return Some(
            "all parquet files in one table must have compatible schema; split mismatched files into separate tables",
        );
    }
    if m.contains("has no schema") {
        return Some(
            "define table schema in catalog or enable inference with FFQ_SCHEMA_INFERENCE=on|strict|permissive",
        );
    }
    if m.contains("catalog") {
        return Some("verify --catalog path exists and has .json/.toml extension");
    }
    None
}

fn unsupported_hint(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("e_subquery_unsupported_correlation") {
        return Some(
            "rewrite the correlated predicate to supported equality correlation shape, or use uncorrelated subquery form",
        );
    }
    if m.contains("qdrant") {
        return Some(
            "enable required feature flags (vector/qdrant) or use brute-force fallback shape",
        );
    }
    None
}
