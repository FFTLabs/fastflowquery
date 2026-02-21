#[cfg(not(feature = "distributed"))]
fn main() {
    eprintln!(
        "bench_pipelined_shuffle_ttfr requires the `distributed` feature.\nrun with: cargo run -p ffq-client --example bench_pipelined_shuffle_ttfr --features distributed"
    );
    std::process::exit(1);
}

#[cfg(feature = "distributed")]
mod imp {
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_common::{FfqError, Result};
use ffq_distributed::{
    Coordinator, CoordinatorConfig, DefaultTaskExecutor, InProcessControlPlane, QueryState, Worker,
    WorkerConfig,
};
use ffq_planner::{AggExpr, Expr, LogicalPlan, PhysicalPlannerConfig, create_physical_plan};
use ffq_storage::{Catalog, TableDef, TableStats};
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct CliOptions {
    out_dir: PathBuf,
    rows: usize,
    shuffle_partitions: usize,
    warmup: usize,
    iterations: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct ModeMetrics {
    ttfr_avg_ms: f64,
    total_avg_ms: f64,
    throughput_rows_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct Artifact {
    run_id: String,
    timestamp_unix_ms: u128,
    rows: usize,
    shuffle_partitions: usize,
    warmup: usize,
    iterations: usize,
    baseline_non_streaming: ModeMetrics,
    streaming: ModeMetrics,
    ttfr_improvement_pct: f64,
    total_runtime_regression_pct: f64,
    throughput_regression_pct: f64,
}

#[tokio::main(flavor = "current_thread")]
pub async fn run() -> Result<()> {
    let opts = parse_args(std::env::args().skip(1).collect())?;
    fs::create_dir_all(&opts.out_dir)?;

    let fixture_dir = unique_dir("ffq_bench_v2_pipe_shuffle");
    fs::create_dir_all(&fixture_dir)?;
    let parquet_path = fixture_dir.join("lineitem.parquet");
    write_synthetic_lineitem(&parquet_path, opts.rows)?;

    let baseline = run_mode(&opts, &parquet_path, false).await?;
    let streaming = run_mode(&opts, &parquet_path, true).await?;

    let ttfr_improvement_pct = if baseline.ttfr_avg_ms > 0.0 {
        ((baseline.ttfr_avg_ms - streaming.ttfr_avg_ms) / baseline.ttfr_avg_ms) * 100.0
    } else {
        0.0
    };
    let total_runtime_regression_pct = if baseline.total_avg_ms > 0.0 {
        ((streaming.total_avg_ms - baseline.total_avg_ms) / baseline.total_avg_ms) * 100.0
    } else {
        0.0
    };
    let throughput_regression_pct = if baseline.throughput_rows_per_sec > 0.0 {
        ((baseline.throughput_rows_per_sec - streaming.throughput_rows_per_sec)
            / baseline.throughput_rows_per_sec)
            * 100.0
    } else {
        0.0
    };

    let run_id = format!("bench_v2_pipelined_shuffle_ttfr_{}", now_millis());
    let artifact = Artifact {
        run_id: run_id.clone(),
        timestamp_unix_ms: now_millis(),
        rows: opts.rows,
        shuffle_partitions: opts.shuffle_partitions,
        warmup: opts.warmup,
        iterations: opts.iterations,
        baseline_non_streaming: baseline,
        streaming,
        ttfr_improvement_pct,
        total_runtime_regression_pct,
        throughput_regression_pct,
    };

    let json_path = opts.out_dir.join(format!("{run_id}.json"));
    let csv_path = opts.out_dir.join(format!("{run_id}.csv"));
    let json = serde_json::to_vec_pretty(&artifact)
        .map_err(|e| FfqError::Execution(format!("encode benchmark artifact failed: {e}")))?;
    fs::write(&json_path, json)?;
    fs::write(&csv_path, render_csv(&artifact))?;

    println!("FFQ v2 pipelined-shuffle TTFR benchmark");
    println!(
        "baseline  ttfr_ms={:.3} total_ms={:.3} throughput_rows_per_sec={:.3}",
        artifact.baseline_non_streaming.ttfr_avg_ms,
        artifact.baseline_non_streaming.total_avg_ms,
        artifact.baseline_non_streaming.throughput_rows_per_sec
    );
    println!(
        "streaming ttfr_ms={:.3} total_ms={:.3} throughput_rows_per_sec={:.3}",
        artifact.streaming.ttfr_avg_ms,
        artifact.streaming.total_avg_ms,
        artifact.streaming.throughput_rows_per_sec
    );
    println!(
        "delta ttfr_improvement_pct={:.2} total_runtime_regression_pct={:.2} throughput_regression_pct={:.2}",
        artifact.ttfr_improvement_pct,
        artifact.total_runtime_regression_pct,
        artifact.throughput_regression_pct
    );
    println!("json: {}", json_path.display());
    println!("csv: {}", csv_path.display());

    let _ = fs::remove_file(&parquet_path);
    let _ = fs::remove_dir_all(&fixture_dir);
    Ok(())
}

async fn run_mode(
    opts: &CliOptions,
    parquet_path: &Path,
    pipelined_shuffle: bool,
) -> Result<ModeMetrics> {
    let mut ttfr_samples = Vec::with_capacity(opts.iterations);
    let mut total_samples = Vec::with_capacity(opts.iterations);

    for i in 0..(opts.warmup + opts.iterations) {
        let query_id = (700000 + i as u64).to_string();
        let run = run_once(
            parquet_path,
            opts.rows,
            opts.shuffle_partitions,
            pipelined_shuffle,
            &query_id,
        )
        .await?;
        if i >= opts.warmup {
            ttfr_samples.push(run.0);
            total_samples.push(run.1);
        }
    }

    let ttfr_avg_ms = ttfr_samples.iter().sum::<f64>() / (ttfr_samples.len() as f64);
    let total_avg_ms = total_samples.iter().sum::<f64>() / (total_samples.len() as f64);
    let throughput_rows_per_sec = if total_avg_ms > 0.0 {
        (opts.rows as f64) / (total_avg_ms / 1_000.0)
    } else {
        0.0
    };
    Ok(ModeMetrics {
        ttfr_avg_ms,
        total_avg_ms,
        throughput_rows_per_sec,
    })
}

async fn run_once(
    parquet_path: &Path,
    rows: usize,
    shuffle_partitions: usize,
    pipelined_shuffle: bool,
    query_id: &str,
) -> Result<(f64, f64)> {
    let mut coordinator_catalog = Catalog::new();
    let schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
    ]);
    coordinator_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: parquet_path.display().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some(schema.clone()),
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let mut worker_catalog = Catalog::new();
    worker_catalog.register_table(TableDef {
        name: "lineitem".to_string(),
        uri: parquet_path.display().to_string(),
        paths: Vec::new(),
        format: "parquet".to_string(),
        schema: Some(schema),
        stats: TableStats::default(),
        options: HashMap::new(),
    });
    let worker_catalog = Arc::new(worker_catalog);

    let logical = LogicalPlan::Aggregate {
        group_exprs: vec![Expr::Column("l_orderkey".to_string())],
        aggr_exprs: vec![(
            AggExpr::Sum(Expr::Column("l_quantity".to_string())),
            "sum_qty".to_string(),
        )],
        input: Box::new(LogicalPlan::TableScan {
            table: "lineitem".to_string(),
            projection: None,
            filters: vec![],
        }),
    };
    let physical = create_physical_plan(
        &logical,
        &PhysicalPlannerConfig {
            shuffle_partitions,
            ..PhysicalPlannerConfig::default()
        },
    )?;
    let physical_json = serde_json::to_vec(&physical)
        .map_err(|e| FfqError::Execution(format!("encode physical plan failed: {e}")))?;

    let run_root = unique_dir("ffq_bench_v2_pipe_shuffle_run");
    let spill_dir = run_root.join("spill");
    let shuffle_root = run_root.join("shuffle");
    fs::create_dir_all(&spill_dir)?;
    fs::create_dir_all(&shuffle_root)?;

    let coordinator = Arc::new(Mutex::new(Coordinator::with_catalog(
        CoordinatorConfig {
            shuffle_root: shuffle_root.clone(),
            pipelined_shuffle_enabled: pipelined_shuffle,
            pipelined_shuffle_min_map_completion_ratio: if pipelined_shuffle { 0.0 } else { 1.0 },
            pipelined_shuffle_min_committed_offset_bytes: 1,
            ..CoordinatorConfig::default()
        },
        coordinator_catalog,
    )));

    {
        let mut c = coordinator.lock().await;
        c.submit_query(query_id.to_string(), &physical_json)?;
    }

    let control = Arc::new(InProcessControlPlane::new(Arc::clone(&coordinator)));
    let exec = Arc::new(DefaultTaskExecutor::new(Arc::clone(&worker_catalog)));
    let worker1 = Worker::new(
        WorkerConfig {
            worker_id: "bench-w1".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        Arc::clone(&control),
        Arc::clone(&exec),
    );
    let worker2 = Worker::new(
        WorkerConfig {
            worker_id: "bench-w2".to_string(),
            cpu_slots: 1,
            spill_dir: spill_dir.clone(),
            shuffle_root: shuffle_root.clone(),
            ..WorkerConfig::default()
        },
        control,
        Arc::clone(&exec),
    );

    let started = Instant::now();
    let mut final_status = None;
    for _ in 0..20_000 {
        let _ = worker1.poll_once().await?;
        let _ = worker2.poll_once().await?;
        let st = {
            let c = coordinator.lock().await;
            c.get_query_status(query_id)?
        };
        match st.state {
            QueryState::Succeeded => {
                final_status = Some(st);
                break;
            }
            QueryState::Failed | QueryState::Canceled => {
                return Err(FfqError::Execution(format!(
                    "benchmark query {} failed: {}",
                    query_id, st.message
                )));
            }
            QueryState::Queued | QueryState::Running => {}
        }
    }
    let total_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let status = final_status.ok_or_else(|| {
        FfqError::Execution("benchmark query did not finish in poll budget".to_string())
    })?;
    let ttfr_ms = status
        .stage_metrics
        .values()
        .filter_map(|m| {
            if m.first_reduce_row_ms > 0 {
                Some(m.first_reduce_row_ms as f64)
            } else {
                None
            }
        })
        .min_by(|a, b| a.total_cmp(b))
        .unwrap_or(total_ms);

    let _ = rows; // keep arg visible for future extensions.
    let _ = fs::remove_dir_all(&run_root);
    Ok((ttfr_ms, total_ms))
}

fn write_synthetic_lineitem(path: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
    ]));
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .map_err(|e| FfqError::Execution(format!("create parquet writer failed: {e}")))?;
    let batch_size = 8192usize;
    let mut produced = 0usize;
    while produced < rows {
        let n = (rows - produced).min(batch_size);
        let keys = (0..n)
            .map(|i| ((produced + i) as i64) % 50_000)
            .collect::<Vec<_>>();
        let qty = (0..n)
            .map(|i| ((produced + i) % 97) as f64 + 1.0)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Float64Array::from(qty)),
            ],
        )
        .map_err(|e| FfqError::Execution(format!("build synthetic batch failed: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| FfqError::Execution(format!("write synthetic batch failed: {e}")))?;
        produced += n;
    }
    writer
        .close()
        .map_err(|e| FfqError::Execution(format!("close synthetic parquet failed: {e}")))?;
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<CliOptions> {
    let mut out_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/bench/results")
        .to_path_buf();
    let mut rows = std::env::var("FFQ_PIPE_TTFR_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(300_000);
    let mut shuffle_partitions = std::env::var("FFQ_PIPE_TTFR_SHUFFLE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64);
    let mut warmup = std::env::var("FFQ_PIPE_TTFR_WARMUP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);
    let mut iterations = std::env::var("FFQ_PIPE_TTFR_ITERATIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3);

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--out-dir" => {
                i += 1;
                out_dir = PathBuf::from(require_arg(&args, i, "--out-dir")?);
            }
            "--rows" => {
                i += 1;
                let raw = require_arg(&args, i, "--rows")?;
                rows = raw
                    .parse::<usize>()
                    .map_err(|e| FfqError::InvalidConfig(format!("invalid --rows '{raw}': {e}")))?;
            }
            "--shuffle-partitions" => {
                i += 1;
                let raw = require_arg(&args, i, "--shuffle-partitions")?;
                shuffle_partitions = raw.parse::<usize>().map_err(|e| {
                    FfqError::InvalidConfig(format!("invalid --shuffle-partitions '{raw}': {e}"))
                })?;
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
                eprintln!(
                    "Usage: bench_pipelined_shuffle_ttfr [--out-dir PATH] [--rows N] [--shuffle-partitions N] [--warmup N] [--iterations N]"
                );
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

    if rows == 0 || shuffle_partitions == 0 || iterations == 0 {
        return Err(FfqError::InvalidConfig(
            "rows, shuffle-partitions, and iterations must be >= 1".to_string(),
        ));
    }
    Ok(CliOptions {
        out_dir,
        rows,
        shuffle_partitions,
        warmup,
        iterations,
    })
}

fn require_arg(args: &[String], idx: usize, flag: &str) -> Result<String> {
    args.get(idx).cloned().ok_or_else(|| {
        FfqError::InvalidConfig(format!("missing value for {flag}; run with --help"))
    })
}

fn unique_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}"))
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis()
}

fn render_csv(a: &Artifact) -> String {
    let mut out = String::new();
    out.push_str("run_id,rows,shuffle_partitions,warmup,iterations,mode,ttfr_avg_ms,total_avg_ms,throughput_rows_per_sec,ttfr_improvement_pct,total_runtime_regression_pct,throughput_regression_pct\n");
    out.push_str(&format!(
        "{},{},{},{},{},baseline_non_streaming,{:.6},{:.6},{:.6},,,\n",
        a.run_id,
        a.rows,
        a.shuffle_partitions,
        a.warmup,
        a.iterations,
        a.baseline_non_streaming.ttfr_avg_ms,
        a.baseline_non_streaming.total_avg_ms,
        a.baseline_non_streaming.throughput_rows_per_sec
    ));
    out.push_str(&format!(
        "{},{},{},{},{},streaming,{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
        a.run_id,
        a.rows,
        a.shuffle_partitions,
        a.warmup,
        a.iterations,
        a.streaming.ttfr_avg_ms,
        a.streaming.total_avg_ms,
        a.streaming.throughput_rows_per_sec,
        a.ttfr_improvement_pct,
        a.total_runtime_regression_pct,
        a.throughput_regression_pct
    ));
    out
}
} // mod imp

#[cfg(feature = "distributed")]
fn main() -> ffq_common::Result<()> {
    imp::run()
}
