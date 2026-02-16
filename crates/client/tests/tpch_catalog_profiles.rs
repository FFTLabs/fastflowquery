use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ffq_client::Engine;
use ffq_common::EngineConfig;
use ffq_storage::Catalog;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    prev_catalog: Option<String>,
    prev_endpoint: Option<String>,
    prev_cwd: PathBuf,
}

impl EnvGuard {
    fn capture() -> Self {
        Self {
            prev_catalog: env::var("FFQ_CATALOG_PATH").ok(),
            prev_endpoint: env::var("FFQ_COORDINATOR_ENDPOINT").ok(),
            prev_cwd: env::current_dir().expect("current dir"),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        let _ = env::set_current_dir(&self.prev_cwd);
        if let Some(value) = &self.prev_catalog {
            env::set_var("FFQ_CATALOG_PATH", value);
        } else {
            env::remove_var("FFQ_CATALOG_PATH");
        }
        if let Some(value) = &self.prev_endpoint {
            env::set_var("FFQ_COORDINATOR_ENDPOINT", value);
        } else {
            env::remove_var("FFQ_COORDINATOR_ENDPOINT");
        }
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn q1_sql() -> String {
    fs::read_to_string(repo_root().join("tests/bench/queries/tpch_q1.sql"))
        .expect("read tpch q1 sql")
}

fn q3_sql() -> String {
    fs::read_to_string(repo_root().join("tests/bench/queries/tpch_q3.sql"))
        .expect("read tpch q3 sql")
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
    let _env_guard = EnvGuard::capture();

    let root = repo_root();
    env::set_current_dir(&root).expect("set current dir");
    env::set_var("FFQ_CATALOG_PATH", profile_path.to_string_lossy().to_string());
    env::remove_var("FFQ_COORDINATOR_ENDPOINT");

    let engine = Engine::new(EngineConfig::default()).expect("engine");
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
