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
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let build_rows = std::env::var("FFQ_BLOOM_BUILD_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(50_000);
    let probe_rows = std::env::var("FFQ_BLOOM_PROBE_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(400_000);
    let build_key_cardinality = std::env::var("FFQ_BLOOM_BUILD_KEYS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10_000);
    let probe_key_space = std::env::var("FFQ_BLOOM_PROBE_KEY_SPACE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(100_000);
    let iterations = std::env::var("FFQ_BLOOM_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3);

    let (left_path, right_path, left_schema, right_schema) = write_fixture_tables(
        build_rows,
        probe_rows,
        build_key_cardinality,
        probe_key_space,
    )?;

    let without = run_bench(
        &left_path,
        &right_path,
        left_schema.clone(),
        right_schema.clone(),
        build_rows as u64 * 16,
        probe_rows as u64 * 16,
        false,
        iterations,
    )?;
    let with = run_bench(
        &left_path,
        &right_path,
        left_schema,
        right_schema,
        build_rows as u64 * 16,
        probe_rows as u64 * 16,
        true,
        iterations,
    )?;

    let without_ms = without.as_secs_f64() * 1000.0;
    let with_ms = with.as_secs_f64() * 1000.0;
    let speedup = if with_ms > 0.0 {
        without_ms / with_ms
    } else {
        f64::INFINITY
    };
    let simulated_probe_after = simulate_bloom_prefilter_i64(
        build_rows,
        probe_rows,
        build_key_cardinality,
        probe_key_space,
        20,
    );
    let probe_before_bytes = (probe_rows as u64) * 16;
    let probe_after_bytes = (simulated_probe_after as u64) * 16;
    let reduced = if probe_before_bytes > 0 {
        100.0 - ((probe_after_bytes as f64 / probe_before_bytes as f64) * 100.0)
    } else {
        0.0
    };

    println!("FFQ join bloom microbench");
    println!(
        "build_rows={} probe_rows={} build_keys={} probe_key_space={} iterations={}",
        build_rows, probe_rows, build_key_cardinality, probe_key_space, iterations
    );
    println!("without bloom: {:.2} ms", without_ms);
    println!("with bloom: {:.2} ms", with_ms);
    println!("speedup: {:.3}x", speedup);
    println!(
        "simulated_probe_bytes_before={} simulated_probe_bytes_after={} reduction={:.1}%",
        probe_before_bytes, probe_after_bytes, reduced
    );
    println!(
        "expected_probe_reduction≈{:.1}%",
        (1.0 - (build_key_cardinality as f64 / probe_key_space as f64)).max(0.0) * 100.0
    );

    let _ = std::fs::remove_file(&left_path);
    let _ = std::fs::remove_file(&right_path);
    Ok(())
}

fn run_bench(
    left_path: &str,
    right_path: &str,
    left_schema: Arc<Schema>,
    right_schema: Arc<Schema>,
    left_bytes: u64,
    right_bytes: u64,
    bloom_enabled: bool,
    iterations: usize,
) -> Result<std::time::Duration> {
    let mut cfg = EngineConfig::default();
    cfg.batch_size_rows = 8192;
    cfg.join_bloom_enabled = bloom_enabled;
    cfg.join_bloom_bits = 20;
    cfg.join_radix_bits = 8;

    let engine = Engine::new(cfg)?;
    register_table(
        &engine,
        "build_side",
        left_path,
        left_schema.as_ref(),
        left_bytes,
    )?;
    register_table(
        &engine,
        "probe_side",
        right_path,
        right_schema.as_ref(),
        right_bytes,
    )?;
    // Keep `build_side` as the right input so the current physical join default
    // (`build_side = right`) can build bloom from the smaller table.
    let sql = "SELECT SUM(probe_side.rv) AS total \
               FROM probe_side \
               JOIN build_side ON probe_side.k = build_side.k";

    let _ = futures::executor::block_on(engine.sql(sql)?.collect())?;
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = futures::executor::block_on(engine.sql(sql)?.collect())?;
    }
    let elapsed = started.elapsed() / iterations as u32;
    let _ = futures::executor::block_on(engine.shutdown());
    Ok(elapsed)
}

fn register_table(
    engine: &Engine,
    name: &str,
    path: &str,
    schema: &Schema,
    bytes: u64,
) -> Result<()> {
    engine.register_table_checked(
        name.to_string(),
        TableDef {
            name: name.to_string(),
            uri: path.to_string(),
            paths: Vec::new(),
            format: "parquet".to_string(),
            schema: Some(schema.clone()),
            stats: TableStats {
                rows: None,
                bytes: Some(bytes),
            },
            options: HashMap::new(),
        },
    )
}

fn write_fixture_tables(
    build_rows: usize,
    probe_rows: usize,
    build_key_cardinality: usize,
    probe_key_space: usize,
) -> Result<(String, String, Arc<Schema>, Arc<Schema>)> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ffq_common::FfqError::Execution(format!("clock error: {e}")))?
        .as_nanos();
    let left_path = std::env::temp_dir()
        .join(format!("ffq_join_bloom_build_{nanos}.parquet"))
        .to_string_lossy()
        .to_string();
    let right_path = std::env::temp_dir()
        .join(format!("ffq_join_bloom_probe_{nanos}.parquet"))
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

    let build_keys = Int64Array::from(
        (0..build_rows)
            .map(|i| (i % build_key_cardinality) as i64)
            .collect::<Vec<_>>(),
    );
    let build_vals = Int64Array::from((0..build_rows).map(|i| i as i64).collect::<Vec<_>>());
    let probe_keys = Int64Array::from(
        (0..probe_rows)
            .map(|i| (i % probe_key_space) as i64)
            .collect::<Vec<_>>(),
    );
    let probe_vals = Int64Array::from((0..probe_rows).map(|i| i as i64).collect::<Vec<_>>());

    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![Arc::new(build_keys), Arc::new(build_vals)],
    )
    .map_err(|e| ffq_common::FfqError::Execution(format!("build batch failed: {e}")))?;
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![Arc::new(probe_keys), Arc::new(probe_vals)],
    )
    .map_err(|e| ffq_common::FfqError::Execution(format!("probe batch failed: {e}")))?;

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

fn simulate_bloom_prefilter_i64(
    build_rows: usize,
    probe_rows: usize,
    build_key_cardinality: usize,
    probe_key_space: usize,
    bloom_log2_bits: u8,
) -> usize {
    let mut bloom = TinyBloom::new(bloom_log2_bits, 3);
    for i in 0..build_rows {
        let key = (i % build_key_cardinality) as i64;
        bloom.insert(key);
    }
    let mut kept = 0usize;
    for i in 0..probe_rows {
        let key = (i % probe_key_space) as i64;
        if bloom.may_contain(key) {
            kept += 1;
        }
    }
    kept
}

struct TinyBloom {
    bits: Vec<u64>,
    bit_mask: u64,
    hash_count: u8,
}

impl TinyBloom {
    fn new(log2_bits: u8, hash_count: u8) -> Self {
        let eff_bits = log2_bits.clamp(8, 26);
        let bit_count = 1usize << eff_bits;
        let words = bit_count.div_ceil(64);
        Self {
            bits: vec![0_u64; words],
            bit_mask: (bit_count as u64) - 1,
            hash_count: hash_count.max(1),
        }
    }

    fn insert(&mut self, key: i64) {
        let h1 = hash_i64_seed(key, 0);
        let h2 = hash_i64_seed(key, 0x9e37_79b9_7f4a_7c15);
        for i in 0..self.hash_count {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2 | 1)) & self.bit_mask;
            let word = (bit / 64) as usize;
            let offset = (bit % 64) as u32;
            self.bits[word] |= 1_u64 << offset;
        }
    }

    fn may_contain(&self, key: i64) -> bool {
        let h1 = hash_i64_seed(key, 0);
        let h2 = hash_i64_seed(key, 0x9e37_79b9_7f4a_7c15);
        for i in 0..self.hash_count {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2 | 1)) & self.bit_mask;
            let word = (bit / 64) as usize;
            let offset = (bit % 64) as u32;
            if (self.bits[word] & (1_u64 << offset)) == 0 {
                return false;
            }
        }
        true
    }
}

fn hash_i64_seed(v: i64, seed: u64) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut h);
    v.hash(&mut h);
    h.finish()
}
