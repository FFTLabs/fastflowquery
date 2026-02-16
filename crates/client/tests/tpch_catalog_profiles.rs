use std::env;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ffq_client::bench_queries::{load_benchmark_query, BenchmarkQueryId};
use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::Catalog;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct CwdGuard {
    prev_cwd: PathBuf,
}

impl CwdGuard {
    fn capture() -> Self {
        Self {
            prev_cwd: env::current_dir().expect("current dir"),
        }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = env::set_current_dir(&self.prev_cwd);
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn q1_sql() -> String {
    load_benchmark_query(BenchmarkQueryId::TpchQ1).expect("load canonical tpch q1")
}

fn q3_sql() -> String {
    load_benchmark_query(BenchmarkQueryId::TpchQ3).expect("load canonical tpch q3")
}

fn tpch_parquet_ready() -> bool {
    let root = repo_root().join("tests/bench/fixtures/tpch_dbgen_sf1_parquet");
    root.join("lineitem.parquet").exists()
        && root.join("orders.parquet").exists()
        && root.join("customer.parquet").exists()
}

fn run_catalog_profile_query_checks(profile_path: &Path) {
    let _guard = ENV_LOCK.lock().expect("env lock");
    Catalog::load(&profile_path.to_string_lossy()).expect("catalog profile should parse");

    if !tpch_parquet_ready() {
        eprintln!(
            "skipping tpch catalog profile test; missing parquet fixtures in tests/bench/fixtures/tpch_dbgen_sf1_parquet"
        );
        return;
    }
    let _cwd_guard = CwdGuard::capture();

    let root = repo_root();
    env::set_current_dir(&root).expect("set current dir");
    let mut cfg = EngineConfig::default();
    cfg.catalog_path = Some(profile_path.to_string_lossy().to_string());
    let engine = Engine::new(cfg).expect("engine");
    let q1_batches = futures::executor::block_on(engine.sql(&q1_sql()).expect("q1 sql").collect())
        .expect("q1 collect");
    let q3_batches = futures::executor::block_on(engine.sql(&q3_sql()).expect("q3 sql").collect())
        .expect("q3 collect");

    let q1_rows: usize = q1_batches.iter().map(|b| b.num_rows()).sum();
    let q3_rows: usize = q3_batches.iter().map(|b| b.num_rows()).sum();
    assert!(q1_rows > 0, "q1 returned no rows");
    assert!(q3_rows > 0, "q3 returned no rows");
}

#[test]
fn tpch_official_catalog_profile_json_runs_q1_q3_without_manual_registration() {
    let profile =
        repo_root().join("tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.json");
    run_catalog_profile_query_checks(&profile);
}

#[test]
fn tpch_official_catalog_profile_toml_runs_q1_q3_without_manual_registration() {
    let profile =
        repo_root().join("tests/fixtures/catalog/tpch_dbgen_sf1_parquet.tables.toml");
    run_catalog_profile_query_checks(&profile);
}
