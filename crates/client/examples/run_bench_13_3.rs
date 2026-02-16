use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_schema::{DataType, Field, Schema};
use ffq_client::bench_fixtures::{
    default_benchmark_fixture_root, generate_default_benchmark_fixtures,
};
use ffq_client::bench_queries::{load_benchmark_query_from_root, BenchmarkQueryId};
use ffq_client::Engine;
use ffq_common::{EngineConfig, FfqError, Result};
use ffq_planner::LiteralValue;
use ffq_storage::{TableDef, TableStats};
use serde::Serialize;

#[derive(Debug, Clone)]
struct CliOptions {
    fixture_root: PathBuf,
    query_root: PathBuf,
    out_dir: PathBuf,
    warmup: usize,
    iterations: usize,
}

#[derive(Debug, Clone)]
struct QuerySpec {
    id: BenchmarkQueryId,
    variant: &'static str,
    dataset: &'static str,
    params: HashMap<String, LiteralValue>,
}

#[derive(Debug, Serialize)]
struct RuntimeMeta {
    batch_size_rows: usize,
    mem_budget_bytes: usize,
    shuffle_partitions: usize,
}

#[derive(Debug, Serialize)]
struct HostMeta {
    os: String,
    arch: String,
    logical_cpus: usize,
}

#[derive(Debug, Serialize)]
struct QueryResultRow {
    query_id: String,
    variant: String,
    dataset: String,
    iterations: usize,
    warmup_iterations: usize,
    elapsed_ms: f64,
    rows_out: u64,
    bytes_out: Option<u64>,
    success: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkArtifact {
    run_id: String,
    timestamp_unix_ms: u128,
    mode: String,
    feature_flags: Vec<String>,
    fixture_root: String,
    query_root: String,
    runtime: RuntimeMeta,
    host: HostMeta,
    results: Vec<QueryResultRow>,
}

fn main() -> Result<()> {
    let opts = parse_args(env::args().skip(1).collect())?;
    ensure_fixtures(&opts.fixture_root)?;
    fs::create_dir_all(&opts.out_dir)?;

    let config = EngineConfig::default();
    let mut results = Vec::new();
    let engine = Engine::new(config.clone())?;
    register_benchmark_tables(&engine, &opts.fixture_root)?;

    for spec in canonical_specs() {
        let query = load_benchmark_query_from_root(&opts.query_root, spec.id)?;
        match execute_query(
            &engine,
            &query,
            spec.params.clone(),
            opts.warmup,
            opts.iterations,
        ) {
            Ok((elapsed_ms, rows_out)) => {
                results.push(QueryResultRow {
                    query_id: spec.id.stable_id().to_string(),
                    variant: spec.variant.to_string(),
                    dataset: spec.dataset.to_string(),
                    iterations: opts.iterations,
                    warmup_iterations: opts.warmup,
                    elapsed_ms,
                    rows_out,
                    bytes_out: None,
                    success: true,
                    error: None,
                });
            }
            Err(err) => {
                results.push(QueryResultRow {
                    query_id: spec.id.stable_id().to_string(),
                    variant: spec.variant.to_string(),
                    dataset: spec.dataset.to_string(),
                    iterations: opts.iterations,
                    warmup_iterations: opts.warmup,
                    elapsed_ms: 0.0,
                    rows_out: 0,
                    bytes_out: None,
                    success: false,
                    error: Some(err.to_string()),
                });
            }
        }
    }

    futures::executor::block_on(engine.shutdown())?;

    let run_id = format!("bench13_3_embedded_{}", now_millis());
    let artifact = BenchmarkArtifact {
        run_id: run_id.clone(),
        timestamp_unix_ms: now_millis(),
        mode: "embedded".to_string(),
        feature_flags: feature_flags(),
        fixture_root: opts.fixture_root.display().to_string(),
        query_root: opts.query_root.display().to_string(),
        runtime: RuntimeMeta {
            batch_size_rows: config.batch_size_rows,
            mem_budget_bytes: config.mem_budget_bytes,
            shuffle_partitions: config.shuffle_partitions,
        },
        host: HostMeta {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        },
        results,
    };

    let json_path = opts.out_dir.join(format!("{run_id}.json"));
    let csv_path = opts.out_dir.join(format!("{run_id}.csv"));
    write_json(&json_path, &artifact)?;
    write_csv(&csv_path, &artifact)?;
    print_summary(&artifact, &json_path, &csv_path);

    if artifact.results.iter().any(|r| !r.success) {
        return Err(FfqError::Execution(
            "one or more benchmark queries failed; see JSON output".to_string(),
        ));
    }
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<CliOptions> {
    let mut fixture_root = default_benchmark_fixture_root();
    let mut query_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/queries")
        .to_path_buf();
    let mut out_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/results")
        .to_path_buf();
    let mut warmup = 1usize;
    let mut iterations = 3usize;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--fixture-root" => {
                i += 1;
                fixture_root = PathBuf::from(require_arg(&args, i, "--fixture-root")?);
            }
            "--query-root" => {
                i += 1;
                query_root = PathBuf::from(require_arg(&args, i, "--query-root")?);
            }
            "--out-dir" => {
                i += 1;
                out_dir = PathBuf::from(require_arg(&args, i, "--out-dir")?);
            }
            "--warmup" => {
                i += 1;
                let raw = require_arg(&args, i, "--warmup")?;
                warmup = raw.parse::<usize>().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid --warmup '{raw}': {e}"))
                })?;
            }
            "--iterations" => {
                i += 1;
                let raw = require_arg(&args, i, "--iterations")?;
                iterations = raw.parse::<usize>().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid --iterations '{raw}': {e}"))
                })?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(FfqError::InvalidConfig(format!(
                    "unknown argument: {other}. Use --help."
                )));
            }
        }
        i += 1;
    }

    if iterations == 0 {
        return Err(FfqError::InvalidConfig(
            "--iterations must be >= 1".to_string(),
        ));
    }

    Ok(CliOptions {
        fixture_root,
        query_root,
        out_dir,
        warmup,
        iterations,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: run_bench_13_3 [--fixture-root PATH] [--query-root PATH] [--out-dir PATH] [--warmup N] [--iterations N]"
    );
}

fn require_arg(args: &[String], idx: usize, flag: &str) -> Result<String> {
    args.get(idx).cloned().ok_or_else(|| {
        FfqError::InvalidConfig(format!("missing value for {flag}; run with --help"))
    })
}

fn ensure_fixtures(root: &Path) -> Result<()> {
    let required = [
        root.join("tpch_sf1/lineitem.parquet"),
        root.join("tpch_sf1/orders.parquet"),
        root.join("tpch_sf1/customer.parquet"),
        root.join("rag_synth/docs.parquet"),
    ];
    if required.iter().all(|p| p.exists()) {
        return Ok(());
    }
    generate_default_benchmark_fixtures(root)
}

fn register_benchmark_tables(engine: &Engine, root: &Path) -> Result<()> {
    register_parquet(
        engine,
        "lineitem",
        &root.join("tpch_sf1/lineitem.parquet"),
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Utf8, false),
        ]),
    )?;
    register_parquet(
        engine,
        "orders",
        &root.join("tpch_sf1/orders.parquet"),
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderdate", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int64, false),
        ]),
    )?;
    register_parquet(
        engine,
        "customer",
        &root.join("tpch_sf1/customer.parquet"),
        Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
        ]),
    )?;
    register_parquet(
        engine,
        "docs",
        &root.join("rag_synth/docs.parquet"),
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("lang", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::FixedSizeList(
                    std::sync::Arc::new(Field::new("item", DataType::Float32, true)),
                    64,
                ),
                true,
            ),
        ]),
    )?;
    Ok(())
}

fn register_parquet(engine: &Engine, name: &str, path: &Path, schema: Schema) -> Result<()> {
    if !path.exists() {
        return Err(FfqError::InvalidConfig(format!(
            "fixture file does not exist: {}",
            path.display()
        )));
    }
    let canonical = path.canonicalize().map_err(|e| {
        FfqError::Io(std::io::Error::new(
            e.kind(),
            format!("failed to canonicalize {}: {e}", path.display()),
        ))
    })?;
    engine.register_table(
        name,
        TableDef {
            name: name.to_string(),
            uri: canonical.display().to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(schema),
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    );
    Ok(())
}

fn canonical_specs() -> Vec<QuerySpec> {
    let mut specs = vec![
        QuerySpec {
            id: BenchmarkQueryId::TpchQ1,
            variant: "baseline",
            dataset: "tpch_sf1",
            params: HashMap::new(),
        },
        QuerySpec {
            id: BenchmarkQueryId::TpchQ3,
            variant: "baseline",
            dataset: "tpch_sf1",
            params: HashMap::new(),
        },
    ];
    #[cfg(feature = "vector")]
    {
        specs.push(QuerySpec {
            id: BenchmarkQueryId::RagTopkBruteforce,
            variant: "vector_bruteforce",
            dataset: "rag_synth",
            params: rag_query_params(),
        });
    }
    specs
}

#[cfg(feature = "vector")]
fn rag_query_params() -> HashMap<String, LiteralValue> {
    let mut params = HashMap::new();
    let mut q = vec![0.0_f32; 64];
    q[0] = 1.0;
    q[1] = 0.5;
    params.insert("q".to_string(), LiteralValue::VectorF32(q));
    params
}

fn execute_query(
    engine: &Engine,
    sql: &str,
    params: HashMap<String, LiteralValue>,
    warmup: usize,
    iterations: usize,
) -> Result<(f64, u64)> {
    for _ in 0..warmup {
        execute_once(engine, sql, params.clone())?;
    }

    let mut total_ms = 0.0_f64;
    let mut rows_out = 0_u64;
    for _ in 0..iterations {
        let (elapsed_ms, rows) = execute_once(engine, sql, params.clone())?;
        total_ms += elapsed_ms;
        rows_out = rows;
    }
    Ok((total_ms / (iterations as f64), rows_out))
}

fn execute_once(
    engine: &Engine,
    sql: &str,
    params: HashMap<String, LiteralValue>,
) -> Result<(f64, u64)> {
    let start = Instant::now();
    let batches = if params.is_empty() {
        futures::executor::block_on(engine.sql(sql)?.collect())?
    } else {
        futures::executor::block_on(engine.sql_with_params(sql, params)?.collect())?
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
    let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok((elapsed_ms, rows))
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

fn feature_flags() -> Vec<String> {
    let mut out = Vec::new();
    #[cfg(feature = "distributed")]
    {
        out.push("distributed".to_string());
    }
    #[cfg(feature = "vector")]
    {
        out.push("vector".to_string());
    }
    #[cfg(feature = "qdrant")]
    {
        out.push("qdrant".to_string());
    }
    #[cfg(feature = "profiling")]
    {
        out.push("profiling".to_string());
    }
    out
}

fn write_json(path: &Path, artifact: &BenchmarkArtifact) -> Result<()> {
    let payload = serde_json::to_string_pretty(artifact)
        .map_err(|e| FfqError::InvalidConfig(format!("benchmark json encode failed: {e}")))?;
    fs::write(path, payload)?;
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn write_csv(path: &Path, artifact: &BenchmarkArtifact) -> Result<()> {
    let mut out = String::new();
    out.push_str("run_id,timestamp_unix_ms,mode,query_id,variant,dataset,iterations,warmup_iterations,elapsed_ms,rows_out,bytes_out,success,error\n");
    for r in &artifact.results {
        let error = r.error.as_deref().unwrap_or("");
        let bytes_out = r.bytes_out.map_or_else(String::new, |v| v.to_string());
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{:.3},{},{},{},{}\n",
            csv_escape(&artifact.run_id),
            artifact.timestamp_unix_ms,
            csv_escape(&artifact.mode),
            csv_escape(&r.query_id),
            csv_escape(&r.variant),
            csv_escape(&r.dataset),
            r.iterations,
            r.warmup_iterations,
            r.elapsed_ms,
            r.rows_out,
            bytes_out,
            r.success,
            csv_escape(error),
        ));
    }
    fs::write(path, out)?;
    Ok(())
}

fn print_summary(artifact: &BenchmarkArtifact, json_path: &Path, csv_path: &Path) {
    println!("Embedded benchmark run: {}", artifact.run_id);
    println!(
        "Queries: {}, warmup: {}, iterations: {}",
        artifact.results.len(),
        artifact.results.first().map_or(0, |r| r.warmup_iterations),
        artifact.results.first().map_or(0, |r| r.iterations)
    );
    for row in &artifact.results {
        let status = if row.success { "ok" } else { "failed" };
        println!(
            "- {:<22} {:<18} {:>10.3} ms avg  rows_out={}  {}",
            row.query_id, row.variant, row.elapsed_ms, row.rows_out, status
        );
        if let Some(err) = &row.error {
            println!("  error: {err}");
        }
    }
    println!("JSON: {}", json_path.display());
    println!("CSV:  {}", csv_path.display());
}
