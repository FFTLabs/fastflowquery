use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_client::Engine;
use ffq_common::{EngineConfig, Result};
use ffq_storage::{TableDef, TableStats};
use parquet::arrow::ArrowWriter;

fn main() -> Result<()> {
    let rows = std::env::var("FFQ_JOIN_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200_000);
    let iterations = std::env::var("FFQ_JOIN_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4);
    let key_cardinality = std::env::var("FFQ_JOIN_BENCH_KEYS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(rows / 4);

    let (left_path, right_path, left_schema, right_schema) =
        write_fixture_tables(rows, key_cardinality)?;
    let baseline = run_bench(
        &left_path,
        &right_path,
        left_schema.clone(),
        right_schema.clone(),
        0,
        iterations,
        rows,
        key_cardinality,
    )?;
    let radix = run_bench(
        &left_path,
        &right_path,
        left_schema,
        right_schema,
        8,
        iterations,
        rows,
        key_cardinality,
    )?;

    let baseline_ms = baseline.as_secs_f64() * 1000.0;
    let radix_ms = radix.as_secs_f64() * 1000.0;
    let speedup = if radix_ms > 0.0 {
        baseline_ms / radix_ms
    } else {
        f64::INFINITY
    };

    println!("FFQ join radix microbench");
    println!("rows={rows} key_cardinality={key_cardinality} iterations={iterations}");
    println!("baseline(join_radix_bits=0): {:.2} ms", baseline_ms);
    println!("radix(join_radix_bits=8): {:.2} ms", radix_ms);
    println!("speedup: {:.3}x", speedup);

    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);
    Ok(())
}

fn run_bench(
    left_path: &str,
    right_path: &str,
    left_schema: Arc<Schema>,
    right_schema: Arc<Schema>,
    join_radix_bits: u8,
    iterations: usize,
    rows: usize,
    key_cardinality: usize,
) -> Result<std::time::Duration> {
    let mut cfg = EngineConfig::default();
    cfg.batch_size_rows = 8192;
    cfg.join_radix_bits = join_radix_bits;

    let engine = Engine::new(cfg)?;
    register_table(&engine, "bench_left", left_path, left_schema.as_ref())?;
    register_table(&engine, "bench_right", right_path, right_schema.as_ref())?;

    let sql = "SELECT SUM(lv) AS total \
               FROM bench_left \
               JOIN bench_right ON bench_left.k = bench_right.k";

    // One warmup run.
    let warmup = futures::executor::block_on(engine.sql(sql)?.collect())?;
    if warmup.is_empty() {
        return Err(ffq_common::FfqError::Execution(
            "join benchmark warmup returned no rows".to_string(),
        ));
    }

    let started = Instant::now();
    for _ in 0..iterations {
        let batches = futures::executor::block_on(engine.sql(sql)?.collect())?;
        if batches.is_empty() {
            return Err(ffq_common::FfqError::Execution(
                "join benchmark iteration returned no rows".to_string(),
            ));
        }
    }
    let elapsed = started.elapsed() / iterations as u32;
    let _ = futures::executor::block_on(engine.shutdown());
    println!(
        "mode bits={} avg={:.2}ms (rows={}, keys={})",
        join_radix_bits,
        elapsed.as_secs_f64() * 1000.0,
        rows,
        key_cardinality
    );
    Ok(elapsed)
}

fn register_table(engine: &Engine, name: &str, path: &str, schema: &Schema) -> Result<()> {
    engine.register_table_checked(
        name.to_string(),
        TableDef {
            name: name.to_string(),
            uri: path.to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(schema.clone()),
            stats: TableStats::default(),
            options: HashMap::new(),
        },
    )
}

fn write_fixture_tables(
    rows: usize,
    key_cardinality: usize,
) -> Result<(String, String, Arc<Schema>, Arc<Schema>)> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ffq_common::FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let left_path = std::env::temp_dir()
        .join(format!("ffq_join_bench_left_{nanos}.parquet"))
        .to_string_lossy()
        .to_string();
    let right_path = std::env::temp_dir()
        .join(format!("ffq_join_bench_right_{nanos}.parquet"))
        .to_string_lossy()
        .to_string();
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
    ]));

    let left_keys = Int64Array::from(
        (0..rows)
            .map(|i| (i % key_cardinality) as i64)
            .collect::<Vec<_>>(),
    );
    let left_vals = Int64Array::from((0..rows).map(|i| i as i64).collect::<Vec<_>>());
    let right_keys = Int64Array::from(
        (0..rows)
            .map(|i| (i % key_cardinality) as i64)
            .collect::<Vec<_>>(),
    );
    let right_vals = Int64Array::from((0..rows).map(|i| (rows - i) as i64).collect::<Vec<_>>());

    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![Arc::new(left_keys), Arc::new(left_vals)],
    )
    .map_err(|e| ffq_common::FfqError::Execution(format!("left batch build failed: {e}")))?;
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![Arc::new(right_keys), Arc::new(right_vals)],
    )
    .map_err(|e| ffq_common::FfqError::Execution(format!("right batch build failed: {e}")))?;

    write_batch(&left_path, left_schema.clone(), &left_batch)?;
    write_batch(&right_path, right_schema.clone(), &right_batch)?;
    Ok((left_path, right_path, left_schema, right_schema))
}

fn write_batch(path: &str, schema: Arc<Schema>, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| ffq_common::FfqError::Execution(format!("parquet writer init failed: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| ffq_common::FfqError::Execution(format!("parquet write failed: {e}")))?;
    writer
        .close()
        .map_err(|e| ffq_common::FfqError::Execution(format!("parquet close failed: {e}")))?;
    Ok(())
}
