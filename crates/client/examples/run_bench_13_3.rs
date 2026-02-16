use std::collections::HashMap;
use std::env;
use std::fs;
#[cfg(feature = "distributed")]
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
#[cfg(feature = "distributed")]
use std::thread;
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
    mode: BenchMode,
    fixture_root: PathBuf,
    query_root: PathBuf,
    out_dir: PathBuf,
    warmup: usize,
    iterations: usize,
    #[cfg(feature = "vector")]
    rag_matrix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    Embedded,
    Distributed,
}

impl BenchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::Distributed => "distributed",
        }
    }
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
    runtime_tag: String,
    dataset: String,
    backend: String,
    n_docs: Option<usize>,
    effective_dim: Option<usize>,
    top_k: Option<usize>,
    filter_selectivity: Option<f32>,
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
    rag_comparisons: Vec<RagComparisonRow>,
}

#[derive(Debug, Serialize)]
struct RagComparisonRow {
    effective_dim: usize,
    top_k: usize,
    filter_selectivity: f32,
    brute_force_elapsed_ms: f64,
    qdrant_elapsed_ms: f64,
    qdrant_speedup_x: f64,
}

#[cfg(feature = "vector")]
#[derive(Debug, Clone, Copy)]
struct RagVariant {
    n_docs: usize,
    effective_dim: usize,
    k: usize,
    filter_selectivity: f32,
}

fn main() -> Result<()> {
    let opts = parse_args(env::args().skip(1).collect())?;
    if opts.mode == BenchMode::Distributed {
        distributed_preflight()?;
    }
    ensure_fixtures(&opts.fixture_root)?;
    fs::create_dir_all(&opts.out_dir)?;

    let config = EngineConfig::default();
    let mut results = Vec::new();
    let engine = Engine::new(config.clone())?;
    register_benchmark_tables(&engine, &opts.fixture_root)?;

    for spec in canonical_specs(opts.mode) {
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
                    runtime_tag: opts.mode.as_str().to_string(),
                    dataset: spec.dataset.to_string(),
                    backend: "sql_baseline".to_string(),
                    n_docs: None,
                    effective_dim: None,
                    top_k: None,
                    filter_selectivity: None,
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
                    runtime_tag: opts.mode.as_str().to_string(),
                    dataset: spec.dataset.to_string(),
                    backend: "sql_baseline".to_string(),
                    n_docs: None,
                    effective_dim: None,
                    top_k: None,
                    filter_selectivity: None,
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
    #[cfg(feature = "vector")]
    if opts.mode == BenchMode::Embedded {
        run_rag_matrix(&engine, &opts, &mut results)?;
    }

    futures::executor::block_on(engine.shutdown())?;

    let rag_comparisons = build_rag_comparisons(&results);
    let run_id = format!("bench13_3_embedded_{}", now_millis());
    let artifact = BenchmarkArtifact {
        run_id: run_id.clone(),
        timestamp_unix_ms: now_millis(),
        mode: opts.mode.as_str().to_string(),
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
        rag_comparisons,
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
    let mut mode = env::var("FFQ_BENCH_MODE")
        .ok()
        .as_deref()
        .map(parse_mode)
        .transpose()?
        .unwrap_or(BenchMode::Embedded);
    let mut fixture_root = default_benchmark_fixture_root();
    let mut query_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/queries")
        .to_path_buf();
    let mut out_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/results")
        .to_path_buf();
    let mut warmup = 1usize;
    let mut iterations = 3usize;
    #[cfg(feature = "vector")]
    let mut rag_matrix = env::var("FFQ_BENCH_RAG_MATRIX")
        .unwrap_or_else(|_| "1000,16,10,1.0;5000,32,10,0.8;10000,64,10,0.2".to_string());

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                i += 1;
                mode = parse_mode(&require_arg(&args, i, "--mode")?)?;
            }
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
            #[cfg(feature = "vector")]
            "--rag-matrix" => {
                i += 1;
                rag_matrix = require_arg(&args, i, "--rag-matrix")?;
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
        mode,
        fixture_root,
        query_root,
        out_dir,
        warmup,
        iterations,
        #[cfg(feature = "vector")]
        rag_matrix,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: run_bench_13_3 [--mode embedded|distributed] [--fixture-root PATH] [--query-root PATH] [--out-dir PATH] [--warmup N] [--iterations N] [--rag-matrix \"N,dim,k,sel;...\"]"
    );
}

fn parse_mode(raw: &str) -> Result<BenchMode> {
    match raw.to_ascii_lowercase().as_str() {
        "embedded" => Ok(BenchMode::Embedded),
        "distributed" => Ok(BenchMode::Distributed),
        other => Err(FfqError::InvalidConfig(format!(
            "invalid benchmark mode '{other}'; expected embedded|distributed"
        ))),
    }
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

fn canonical_specs(mode: BenchMode) -> Vec<QuerySpec> {
    #[allow(unused_mut)]
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
    let _ = mode;
    specs
}

fn distributed_preflight() -> Result<()> {
    #[cfg(not(feature = "distributed"))]
    {
        return Err(FfqError::InvalidConfig(
            "distributed mode requested but ffq-client was not built with 'distributed' feature"
                .to_string(),
        ));
    }
    #[cfg(feature = "distributed")]
    {
        let endpoint = env::var("FFQ_COORDINATOR_ENDPOINT").map_err(|_| {
            FfqError::InvalidConfig(
                "FFQ_COORDINATOR_ENDPOINT must be set for distributed benchmark mode".to_string(),
            )
        })?;
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(FfqError::InvalidConfig(
                "FFQ_COORDINATOR_ENDPOINT must include scheme (http://...)".to_string(),
            ));
        }
        wait_for_endpoint(&endpoint, std::time::Duration::from_secs(30))
    }
}

#[cfg(feature = "distributed")]
fn wait_for_endpoint(endpoint: &str, timeout: std::time::Duration) -> Result<()> {
    let (host, port) = parse_endpoint_host_port(endpoint)?;
    let start = Instant::now();
    let mut last_err = String::new();
    while start.elapsed() < timeout {
        let addrs = (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| FfqError::Execution(format!("resolve endpoint failed: {e}")))?;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(2)) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    last_err = e.to_string();
                }
            }
        }
        thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(FfqError::Execution(format!(
        "coordinator endpoint not reachable within {}s ({endpoint}): {last_err}",
        timeout.as_secs()
    )))
}

#[cfg(feature = "distributed")]
fn parse_endpoint_host_port(endpoint: &str) -> Result<(String, u16)> {
    let (scheme, rest) = if let Some(x) = endpoint.strip_prefix("http://") {
        ("http", x)
    } else if let Some(x) = endpoint.strip_prefix("https://") {
        ("https", x)
    } else {
        return Err(FfqError::InvalidConfig(format!(
            "endpoint must start with http:// or https://: {endpoint}"
        )));
    };
    let authority = rest
        .split('/')
        .next()
        .ok_or_else(|| FfqError::InvalidConfig(format!("invalid endpoint: {endpoint}")))?;
    let default_port = if scheme == "https" { 443 } else { 80 };
    if let Some((host, port_raw)) = authority.rsplit_once(':') {
        let port = port_raw.parse::<u16>().map_err(|e| {
            FfqError::InvalidConfig(format!("invalid endpoint port '{port_raw}': {e}"))
        })?;
        return Ok((host.to_string(), port));
    }
    Ok((authority.to_string(), default_port))
}

#[cfg(feature = "vector")]
fn rag_query_params(effective_dim: usize) -> Result<HashMap<String, LiteralValue>> {
    if effective_dim == 0 || effective_dim > 64 {
        return Err(FfqError::InvalidConfig(format!(
            "effective_dim must be in 1..=64, got {effective_dim}"
        )));
    }
    let mut params = HashMap::new();
    let mut q = vec![0.0_f32; 64];
    for (i, item) in q.iter_mut().enumerate().take(effective_dim) {
        *item = if i % 2 == 0 { 1.0 } else { 0.5 };
    }
    params.insert("q".to_string(), LiteralValue::VectorF32(q));
    Ok(params)
}

#[cfg(feature = "vector")]
fn run_rag_matrix(
    engine: &Engine,
    opts: &CliOptions,
    results: &mut Vec<QueryResultRow>,
) -> Result<()> {
    let variants = parse_rag_matrix(&opts.rag_matrix)?;
    let brute_template = fs::read_to_string(
        opts.query_root.join("rag_topk_bruteforce.template.sql"),
    )
    .map_err(|e| {
        FfqError::Io(std::io::Error::new(
            e.kind(),
            format!("read rag brute-force template failed: {e}"),
        ))
    })?;

    #[cfg(feature = "qdrant")]
    let mut qdrant_engine_registered = false;
    #[cfg(feature = "qdrant")]
    let qdrant_template = fs::read_to_string(opts.query_root.join("rag_topk_qdrant.template.sql"))
        .map_err(|e| {
            FfqError::Io(std::io::Error::new(
                e.kind(),
                format!("read rag qdrant template failed: {e}"),
            ))
        })?;

    for variant in variants {
        let where_clause = format!(
            "WHERE id <= {}",
            ((variant.n_docs as f32) * variant.filter_selectivity).floor() as usize
        );
        let brute_sql = render_rag_template(&brute_template, "docs", &where_clause, variant.k);
        let params = rag_query_params(variant.effective_dim)?;
        match execute_query(engine, &brute_sql, params, opts.warmup, opts.iterations) {
            Ok((elapsed_ms, rows_out)) => results.push(QueryResultRow {
                query_id: BenchmarkQueryId::RagTopkBruteforce.stable_id().to_string(),
                variant: format!(
                    "n{}_d{}_k{}_s{:.2}",
                    variant.n_docs, variant.effective_dim, variant.k, variant.filter_selectivity
                ),
                runtime_tag: opts.mode.as_str().to_string(),
                dataset: "rag_synth".to_string(),
                backend: "vector_bruteforce".to_string(),
                n_docs: Some(variant.n_docs),
                effective_dim: Some(variant.effective_dim),
                top_k: Some(variant.k),
                filter_selectivity: Some(variant.filter_selectivity),
                iterations: opts.iterations,
                warmup_iterations: opts.warmup,
                elapsed_ms,
                rows_out,
                bytes_out: None,
                success: true,
                error: None,
            }),
            Err(err) => results.push(QueryResultRow {
                query_id: BenchmarkQueryId::RagTopkBruteforce.stable_id().to_string(),
                variant: format!(
                    "n{}_d{}_k{}_s{:.2}",
                    variant.n_docs, variant.effective_dim, variant.k, variant.filter_selectivity
                ),
                runtime_tag: opts.mode.as_str().to_string(),
                dataset: "rag_synth".to_string(),
                backend: "vector_bruteforce".to_string(),
                n_docs: Some(variant.n_docs),
                effective_dim: Some(variant.effective_dim),
                top_k: Some(variant.k),
                filter_selectivity: Some(variant.filter_selectivity),
                iterations: opts.iterations,
                warmup_iterations: opts.warmup,
                elapsed_ms: 0.0,
                rows_out: 0,
                bytes_out: None,
                success: false,
                error: Some(err.to_string()),
            }),
        }

        #[cfg(feature = "qdrant")]
        {
            if qdrant_config_present() {
                if !qdrant_engine_registered {
                    register_qdrant_table(engine)?;
                    qdrant_engine_registered = true;
                }
                let qdrant_where = qdrant_lang_where_for_selectivity(variant.filter_selectivity);
                let qdrant_sql =
                    render_rag_template(&qdrant_template, "docs_idx", &qdrant_where, variant.k);
                let qparams = rag_query_params(variant.effective_dim)?;
                match execute_query(engine, &qdrant_sql, qparams, opts.warmup, opts.iterations) {
                    Ok((elapsed_ms, rows_out)) => results.push(QueryResultRow {
                        query_id: BenchmarkQueryId::RagTopkQdrant.stable_id().to_string(),
                        variant: format!(
                            "n{}_d{}_k{}_s{:.2}",
                            variant.n_docs,
                            variant.effective_dim,
                            variant.k,
                            variant.filter_selectivity
                        ),
                        runtime_tag: opts.mode.as_str().to_string(),
                        dataset: "rag_synth".to_string(),
                        backend: "vector_qdrant".to_string(),
                        n_docs: Some(variant.n_docs),
                        effective_dim: Some(variant.effective_dim),
                        top_k: Some(variant.k),
                        filter_selectivity: Some(variant.filter_selectivity),
                        iterations: opts.iterations,
                        warmup_iterations: opts.warmup,
                        elapsed_ms,
                        rows_out,
                        bytes_out: None,
                        success: true,
                        error: None,
                    }),
                    Err(err) => results.push(QueryResultRow {
                        query_id: BenchmarkQueryId::RagTopkQdrant.stable_id().to_string(),
                        variant: format!(
                            "n{}_d{}_k{}_s{:.2}",
                            variant.n_docs,
                            variant.effective_dim,
                            variant.k,
                            variant.filter_selectivity
                        ),
                        runtime_tag: opts.mode.as_str().to_string(),
                        dataset: "rag_synth".to_string(),
                        backend: "vector_qdrant".to_string(),
                        n_docs: Some(variant.n_docs),
                        effective_dim: Some(variant.effective_dim),
                        top_k: Some(variant.k),
                        filter_selectivity: Some(variant.filter_selectivity),
                        iterations: opts.iterations,
                        warmup_iterations: opts.warmup,
                        elapsed_ms: 0.0,
                        rows_out: 0,
                        bytes_out: None,
                        success: false,
                        error: Some(err.to_string()),
                    }),
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vector")]
fn parse_rag_matrix(raw: &str) -> Result<Vec<RagVariant>> {
    let mut out = Vec::new();
    for item in raw.split(';').filter(|s| !s.trim().is_empty()) {
        let parts = item.split(',').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 4 {
            return Err(FfqError::InvalidConfig(format!(
                "invalid rag matrix item '{item}'; expected N,dim,k,selectivity"
            )));
        }
        let n_docs = parts[0]
            .parse::<usize>()
            .map_err(|e| FfqError::InvalidConfig(format!("invalid N '{}': {e}", parts[0])))?;
        let effective_dim = parts[1]
            .parse::<usize>()
            .map_err(|e| FfqError::InvalidConfig(format!("invalid dim '{}': {e}", parts[1])))?;
        let k = parts[2]
            .parse::<usize>()
            .map_err(|e| FfqError::InvalidConfig(format!("invalid k '{}': {e}", parts[2])))?;
        let filter_selectivity = parts[3].parse::<f32>().map_err(|e| {
            FfqError::InvalidConfig(format!("invalid selectivity '{}': {e}", parts[3]))
        })?;
        if !(0.0..=1.0).contains(&filter_selectivity) {
            return Err(FfqError::InvalidConfig(format!(
                "selectivity must be in [0,1], got {}",
                filter_selectivity
            )));
        }
        out.push(RagVariant {
            n_docs,
            effective_dim,
            k,
            filter_selectivity,
        });
    }
    if out.is_empty() {
        return Err(FfqError::InvalidConfig(
            "rag matrix is empty; provide at least one variant".to_string(),
        ));
    }
    Ok(out)
}

#[cfg(feature = "vector")]
fn render_rag_template(template: &str, table: &str, where_clause: &str, k: usize) -> String {
    template
        .replace("{{table}}", table)
        .replace("{{where_clause}}", where_clause)
        .replace("{{k}}", &k.to_string())
}

fn build_rag_comparisons(results: &[QueryResultRow]) -> Vec<RagComparisonRow> {
    let mut out = Vec::new();
    let mut brute_by_key: HashMap<(usize, usize, u32), f64> = HashMap::new();
    let mut qdrant_by_key: HashMap<(usize, usize, u32), f64> = HashMap::new();
    for row in results {
        let (Some(dim), Some(k), Some(sel)) =
            (row.effective_dim, row.top_k, row.filter_selectivity)
        else {
            continue;
        };
        if !row.success {
            continue;
        }
        let key = (dim, k, (sel * 1000.0).round() as u32);
        match row.backend.as_str() {
            "vector_bruteforce" => {
                brute_by_key.insert(key, row.elapsed_ms);
            }
            "vector_qdrant" => {
                qdrant_by_key.insert(key, row.elapsed_ms);
            }
            _ => {}
        }
    }
    for (key, brute_ms) in brute_by_key {
        if let Some(qdrant_ms) = qdrant_by_key.get(&key) {
            let speedup = if *qdrant_ms > 0.0 {
                brute_ms / *qdrant_ms
            } else {
                0.0
            };
            out.push(RagComparisonRow {
                effective_dim: key.0,
                top_k: key.1,
                filter_selectivity: (key.2 as f32) / 1000.0,
                brute_force_elapsed_ms: brute_ms,
                qdrant_elapsed_ms: *qdrant_ms,
                qdrant_speedup_x: speedup,
            });
        }
    }
    out.sort_by(|a, b| {
        a.effective_dim
            .cmp(&b.effective_dim)
            .then_with(|| a.top_k.cmp(&b.top_k))
            .then_with(|| a.filter_selectivity.total_cmp(&b.filter_selectivity))
    });
    out
}

#[cfg(feature = "qdrant")]
fn qdrant_config_present() -> bool {
    env::var("FFQ_BENCH_QDRANT_COLLECTION").is_ok()
}

#[cfg(feature = "qdrant")]
fn register_qdrant_table(engine: &Engine) -> Result<()> {
    let collection = env::var("FFQ_BENCH_QDRANT_COLLECTION").map_err(|_| {
        FfqError::InvalidConfig("FFQ_BENCH_QDRANT_COLLECTION must be set".to_string())
    })?;
    let endpoint = env::var("FFQ_BENCH_QDRANT_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:6334".to_string());
    let mut options = HashMap::new();
    options.insert("qdrant.endpoint".to_string(), endpoint);
    options.insert("qdrant.collection".to_string(), collection.clone());
    options.insert("qdrant.with_payload".to_string(), "true".to_string());
    let emb_item = Field::new("item", DataType::Float32, true);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lang", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
        Field::new("score", DataType::Float32, true),
        Field::new(
            "emb",
            DataType::FixedSizeList(std::sync::Arc::new(emb_item), 64),
            true,
        ),
    ]);
    engine.register_table(
        "docs_idx",
        TableDef {
            name: "docs_idx".to_string(),
            uri: collection,
            paths: Vec::new(),
            format: "qdrant".to_string(),
            schema: Some(schema),
            stats: TableStats::default(),
            options,
        },
    );
    Ok(())
}

#[cfg(feature = "qdrant")]
fn qdrant_lang_where_for_selectivity(sel: f32) -> String {
    if sel <= 0.3 {
        "WHERE lang = 'de'".to_string()
    } else if sel <= 0.85 {
        "WHERE lang = 'en'".to_string()
    } else {
        String::new()
    }
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
    out.push_str("run_id,timestamp_unix_ms,mode,query_id,variant,runtime_tag,dataset,backend,n_docs,effective_dim,top_k,filter_selectivity,iterations,warmup_iterations,elapsed_ms,rows_out,bytes_out,success,error\n");
    for r in &artifact.results {
        let error = r.error.as_deref().unwrap_or("");
        let bytes_out = r.bytes_out.map_or_else(String::new, |v| v.to_string());
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{},{},{},{}\n",
            csv_escape(&artifact.run_id),
            artifact.timestamp_unix_ms,
            csv_escape(&artifact.mode),
            csv_escape(&r.query_id),
            csv_escape(&r.variant),
            csv_escape(&r.runtime_tag),
            csv_escape(&r.dataset),
            csv_escape(&r.backend),
            r.n_docs.map_or_else(String::new, |v| v.to_string()),
            r.effective_dim.map_or_else(String::new, |v| v.to_string()),
            r.top_k.map_or_else(String::new, |v| v.to_string()),
            r.filter_selectivity
                .map_or_else(String::new, |v| format!("{v:.3}")),
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
    println!("Benchmark run: {} ({})", artifact.run_id, artifact.mode);
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
    if !artifact.rag_comparisons.is_empty() {
        println!("RAG baseline vs qdrant:");
        for c in &artifact.rag_comparisons {
            println!(
                "- dim={} k={} sel={:.2}: brute={:.3}ms qdrant={:.3}ms speedup={:.2}x",
                c.effective_dim,
                c.top_k,
                c.filter_selectivity,
                c.brute_force_elapsed_ms,
                c.qdrant_elapsed_ms,
                c.qdrant_speedup_x
            );
        }
    }
    println!("JSON: {}", json_path.display());
    println!("CSV:  {}", csv_path.display());
}
