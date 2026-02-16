use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListBuilder, Float32Builder, Float64Array, Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_common::{FfqError, Result};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

const TPCH_CUSTOMER_ROWS: i64 = 10_000;
const TPCH_ORDERS_ROWS: i64 = 50_000;
const TPCH_LINEITEM_ROWS: i64 = 200_000;
const RAG_DOC_ROWS: i64 = 10_000;
const RAG_EMBED_DIM: i32 = 64;
const RAG_SYNTH_SEED: u64 = 42;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureFileManifest {
    pub file: String,
    pub rows: i64,
    pub schema: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureManifest {
    pub fixture: String,
    pub description: String,
    pub deterministic_seed: u64,
    pub files: Vec<FixtureFileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixtureIndex {
    pub fixture_set: String,
    pub fixtures: Vec<String>,
}

pub fn generate_default_benchmark_fixtures(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root)?;
    let tpch_root = root.join("tpch_sf1");
    let rag_root = root.join("rag_synth");
    std::fs::create_dir_all(&tpch_root)?;
    std::fs::create_dir_all(&rag_root)?;

    let tpch_manifest = generate_tpch_fixture(&tpch_root)?;
    let rag_manifest = generate_rag_fixture(&rag_root)?;
    write_json(
        &tpch_root.join("manifest.json"),
        &tpch_manifest,
        "tpch manifest encode failed",
    )?;
    write_json(
        &rag_root.join("manifest.json"),
        &rag_manifest,
        "rag manifest encode failed",
    )?;

    let index = FixtureIndex {
        fixture_set: "ffq_bench_v1".to_string(),
        fixtures: vec!["tpch_sf1".to_string(), "rag_synth".to_string()],
    };
    write_json(
        &root.join("index.json"),
        &index,
        "fixture index encode failed",
    )?;
    Ok(())
}

fn generate_tpch_fixture(root: &Path) -> Result<FixtureManifest> {
    let customer_path = root.join("customer.parquet");
    let orders_path = root.join("orders.parquet");
    let lineitem_path = root.join("lineitem.parquet");

    let customer_schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
    ]));
    let customer = build_customer_cols();
    write_parquet(&customer_path, customer_schema.clone(), customer)?;

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderdate", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int64, false),
    ]));
    let orders = build_orders_cols();
    write_parquet(&orders_path, orders_schema.clone(), orders)?;

    let lineitem_schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Utf8, false),
    ]));
    let lineitem = build_lineitem_cols();
    write_parquet(&lineitem_path, lineitem_schema.clone(), lineitem)?;

    Ok(FixtureManifest {
        fixture: "tpch_sf1".to_string(),
        description: "Deterministic synthetic TPCH-style fixture for Q1/Q3 benchmark paths."
            .to_string(),
        deterministic_seed: RAG_SYNTH_SEED,
        files: vec![
            file_manifest("customer.parquet", TPCH_CUSTOMER_ROWS, customer_schema.as_ref()),
            file_manifest("orders.parquet", TPCH_ORDERS_ROWS, orders_schema.as_ref()),
            file_manifest("lineitem.parquet", TPCH_LINEITEM_ROWS, lineitem_schema.as_ref()),
        ],
    })
}

fn generate_rag_fixture(root: &Path) -> Result<FixtureManifest> {
    let docs_path = root.join("docs.parquet");
    let item_field = Field::new("item", DataType::Float32, true);
    let docs_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("lang", DataType::Utf8, false),
        Field::new(
            "emb",
            DataType::FixedSizeList(Arc::new(item_field), RAG_EMBED_DIM),
            true,
        ),
    ]));
    let docs = build_rag_cols()?;
    write_parquet(&docs_path, docs_schema.clone(), docs)?;

    Ok(FixtureManifest {
        fixture: "rag_synth".to_string(),
        description: "Deterministic synthetic RAG embeddings fixture for top-k benchmark paths."
            .to_string(),
        deterministic_seed: RAG_SYNTH_SEED,
        files: vec![file_manifest(
            "docs.parquet",
            RAG_DOC_ROWS,
            docs_schema.as_ref(),
        )],
    })
}

fn build_customer_cols() -> Vec<ArrayRef> {
    let segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
    let keys = (1..=TPCH_CUSTOMER_ROWS).collect::<Vec<_>>();
    let segs = (1..=TPCH_CUSTOMER_ROWS)
        .map(|i| segments[(i as usize) % segments.len()])
        .collect::<Vec<_>>();
    vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(StringArray::from(segs)),
    ]
}

fn build_orders_cols() -> Vec<ArrayRef> {
    let orderkey = (1..=TPCH_ORDERS_ROWS).collect::<Vec<_>>();
    let custkey = (1..=TPCH_ORDERS_ROWS)
        .map(|i| ((i * 37) % TPCH_CUSTOMER_ROWS) + 1)
        .collect::<Vec<_>>();
    let orderdate = (1..=TPCH_ORDERS_ROWS)
        .map(|i| date_string(1992, 1, 1, (i % 2400) as i32))
        .collect::<Vec<_>>();
    let shippriority = (1..=TPCH_ORDERS_ROWS)
        .map(|i| (i % 5) as i64)
        .collect::<Vec<_>>();
    vec![
        Arc::new(Int64Array::from(orderkey)),
        Arc::new(Int64Array::from(custkey)),
        Arc::new(StringArray::from(orderdate)),
        Arc::new(Int64Array::from(shippriority)),
    ]
}

fn build_lineitem_cols() -> Vec<ArrayRef> {
    let flags = ["A", "N", "R"];
    let statuses = ["F", "O"];

    let orderkey = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| ((i * 13) % TPCH_ORDERS_ROWS) + 1)
        .collect::<Vec<_>>();
    let quantity = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| ((i % 50) + 1) as f64)
        .collect::<Vec<_>>();
    let extendedprice = quantity
        .iter()
        .enumerate()
        .map(|(idx, q)| q * (100.0 + ((idx as i64 + 1) % 100) as f64))
        .collect::<Vec<_>>();
    let discount = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| ((i % 10) as f64) / 100.0)
        .collect::<Vec<_>>();
    let tax = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| ((i % 8) as f64) / 100.0)
        .collect::<Vec<_>>();
    let returnflag = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| flags[(i as usize) % flags.len()])
        .collect::<Vec<_>>();
    let linestatus = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| statuses[(i as usize) % statuses.len()])
        .collect::<Vec<_>>();
    let shipdate = (1..=TPCH_LINEITEM_ROWS)
        .map(|i| date_string(1992, 1, 1, (i % 2800) as i32))
        .collect::<Vec<_>>();

    vec![
        Arc::new(Int64Array::from(orderkey)),
        Arc::new(Float64Array::from(quantity)),
        Arc::new(Float64Array::from(extendedprice)),
        Arc::new(Float64Array::from(discount)),
        Arc::new(Float64Array::from(tax)),
        Arc::new(StringArray::from(returnflag)),
        Arc::new(StringArray::from(linestatus)),
        Arc::new(StringArray::from(shipdate)),
    ]
}

fn build_rag_cols() -> Result<Vec<ArrayRef>> {
    let ids = (1..=RAG_DOC_ROWS).collect::<Vec<_>>();
    let titles = (1..=RAG_DOC_ROWS)
        .map(|i| format!("doc-{i:06}"))
        .collect::<Vec<_>>();
    let langs = (1..=RAG_DOC_ROWS)
        .map(|i| if i % 5 == 0 { "de" } else { "en" })
        .collect::<Vec<_>>();

    let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), RAG_EMBED_DIM);
    for doc_id in 1..=RAG_DOC_ROWS {
        for dim in 0..RAG_EMBED_DIM {
            let v = deterministic_embedding_value(doc_id, dim as i64);
            emb.values().append_value(v);
        }
        emb.append(true);
    }

    let emb_array = emb.finish();
    let expected = RAG_DOC_ROWS as usize;
    if emb_array.len() != expected {
        return Err(FfqError::Execution(format!(
            "unexpected embedding row count: got {}, expected {}",
            emb_array.len(),
            expected
        )));
    }

    Ok(vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(titles)),
        Arc::new(StringArray::from(langs)),
        Arc::new(emb_array),
    ])
}

fn deterministic_embedding_value(doc_id: i64, dim: i64) -> f32 {
    let x = (doc_id * 31 + dim * 17 + RAG_SYNTH_SEED as i64) as f64;
    ((x.sin() + x.cos()) * 0.5) as f32
}

fn date_string(mut year: i32, mut month: i32, mut day: i32, mut add_days: i32) -> String {
    while add_days > 0 {
        day += 1;
        let max_day = days_in_month(year, month);
        if day > max_day {
            day = 1;
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
            }
        }
        add_days -= 1;
    }
    format!("{year:04}-{month:02}-{day:02}")
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn write_parquet(path: &Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) -> Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| FfqError::Execution(format!("build batch failed: {e}")))?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| FfqError::Execution(format!("parquet writer create failed: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| FfqError::Execution(format!("parquet write failed: {e}")))?;
    writer
        .close()
        .map_err(|e| FfqError::Execution(format!("parquet close failed: {e}")))?;
    Ok(())
}

fn schema_fields(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}:{:?}:{}", f.name(), f.data_type(), f.is_nullable()))
        .collect()
}

fn file_manifest(file: &str, rows: i64, schema: &Schema) -> FixtureFileManifest {
    FixtureFileManifest {
        file: file.to_string(),
        rows,
        schema: schema_fields(schema),
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T, err_prefix: &str) -> Result<()> {
    let payload = serde_json::to_string_pretty(value)
        .map_err(|e| FfqError::Execution(format!("{err_prefix}: {e}")))?;
    std::fs::write(path, payload)?;
    Ok(())
}

pub fn default_benchmark_fixture_root() -> PathBuf {
    PathBuf::from("./tests/bench/fixtures")
}
