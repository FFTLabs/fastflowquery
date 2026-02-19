use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ffq_client::bench_fixtures::{
    FixtureIndex, FixtureManifest, generate_default_benchmark_fixtures,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn unique_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}"))
}

fn list_files(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).expect("read dir");
        for entry in entries {
            let entry = entry.expect("entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .expect("relative path")
                    .to_string_lossy()
                    .to_string();
                out.insert(rel);
            }
        }
    }
    out
}

fn load_manifest(path: &Path) -> FixtureManifest {
    let payload = std::fs::read_to_string(path).expect("read manifest");
    serde_json::from_str(&payload).expect("parse manifest")
}

fn load_index(path: &Path) -> FixtureIndex {
    let payload = std::fs::read_to_string(path).expect("read index");
    serde_json::from_str(&payload).expect("parse index")
}

fn parquet_rows_and_schema(path: &Path) -> (usize, Vec<String>) {
    let file = std::fs::File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader build")
        .build()
        .expect("parquet reader open");
    let mut rows = 0_usize;
    let mut schema = Vec::<String>::new();
    for batch in reader {
        let batch = batch.expect("parquet batch");
        rows += batch.num_rows();
        if schema.is_empty() {
            schema = batch
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}:{}", f.name(), f.data_type(), f.is_nullable()))
                .collect();
        }
    }
    (rows, schema)
}

#[test]
fn benchmark_fixture_generation_is_deterministic() {
    let dir1 = unique_dir("ffq_bench_fx_a");
    let dir2 = unique_dir("ffq_bench_fx_b");
    std::fs::create_dir_all(&dir1).expect("create dir1");
    std::fs::create_dir_all(&dir2).expect("create dir2");

    generate_default_benchmark_fixtures(&dir1).expect("generate dir1");
    generate_default_benchmark_fixtures(&dir2).expect("generate dir2");

    let files1 = list_files(&dir1);
    let files2 = list_files(&dir2);
    assert_eq!(files1, files2, "fixture file sets differ");

    let idx1 = load_index(&dir1.join("index.json"));
    let idx2 = load_index(&dir2.join("index.json"));
    assert_eq!(idx1, idx2, "fixture indexes differ");

    for fixture in &idx1.fixtures {
        let m1 = load_manifest(&dir1.join(fixture).join("manifest.json"));
        let m2 = load_manifest(&dir2.join(fixture).join("manifest.json"));
        assert_eq!(m1, m2, "fixture manifest differs for {fixture}");
        for file in &m1.files {
            let p1 = dir1.join(fixture).join(&file.file);
            let p2 = dir2.join(fixture).join(&file.file);
            let (rows1, schema1) = parquet_rows_and_schema(&p1);
            let (rows2, schema2) = parquet_rows_and_schema(&p2);
            assert_eq!(rows1, rows2, "row count mismatch for {}", file.file);
            assert_eq!(schema1, schema2, "schema mismatch for {}", file.file);
            assert_eq!(
                rows1 as i64, file.rows,
                "manifest row count mismatch for {}",
                file.file
            );
            assert_eq!(
                schema1, file.schema,
                "manifest schema mismatch for {}",
                file.file
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir1);
    let _ = std::fs::remove_dir_all(&dir2);
}
