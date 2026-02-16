use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ffq_common::{FfqError, Result};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TpchParquetFileManifest {
    pub file: String,
    pub rows: i64,
    pub schema: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TpchParquetManifest {
    pub fixture: String,
    pub source_format: String,
    pub files: Vec<TpchParquetFileManifest>,
}

pub fn convert_tpch_sf1_tbl_to_parquet(
    input_dir: &Path,
    output_dir: &Path,
) -> Result<TpchParquetManifest> {
    if !input_dir.exists() {
        return Err(FfqError::InvalidConfig(format!(
            "input dir does not exist: {}",
            input_dir.display()
        )));
    }
    std::fs::create_dir_all(output_dir)?;

    let customer = convert_customer(input_dir, output_dir)?;
    let orders = convert_orders(input_dir, output_dir)?;
    let lineitem = convert_lineitem(input_dir, output_dir)?;

    let mut files = vec![customer, lineitem, orders];
    files.sort_by(|a, b| a.file.cmp(&b.file));

    let manifest = TpchParquetManifest {
        fixture: "tpch_dbgen_sf1_parquet".to_string(),
        source_format: "tpch_dbgen_tbl".to_string(),
        files,
    };
    let manifest_path = output_dir.join("manifest.json");
    let payload = serde_json::to_string_pretty(&manifest)
        .map_err(|e| FfqError::Execution(format!("manifest json encode failed: {e}")))?;
    std::fs::write(manifest_path, payload)?;
    Ok(manifest)
}

pub fn default_tpch_dbgen_tbl_input_dir() -> PathBuf {
    PathBuf::from("./tests/bench/fixtures/tpch_dbgen_sf1")
}

pub fn default_tpch_dbgen_parquet_output_dir() -> PathBuf {
    PathBuf::from("./tests/bench/fixtures/tpch_dbgen_sf1_parquet")
}

fn convert_customer(input_dir: &Path, output_dir: &Path) -> Result<TpchParquetFileManifest> {
    let src = input_dir.join("customer.tbl");
    let dst = output_dir.join("customer.parquet");
    let mut c_custkey = Vec::<i64>::new();
    let mut c_mktsegment = Vec::<String>::new();
    for fields in read_tbl_fields(&src)? {
        c_custkey.push(parse_i64(&fields, 0, "customer.c_custkey")?);
        c_mktsegment.push(parse_string(&fields, 6, "customer.c_mktsegment")?);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
    ]));
    write_parquet(
        &dst,
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(c_custkey)),
            Arc::new(StringArray::from(c_mktsegment)),
        ],
    )?;
    Ok(file_manifest(
        "customer.parquet",
        &schema,
        count_rows(&dst)?,
    ))
}

fn convert_orders(input_dir: &Path, output_dir: &Path) -> Result<TpchParquetFileManifest> {
    let src = input_dir.join("orders.tbl");
    let dst = output_dir.join("orders.parquet");
    let mut o_orderkey = Vec::<i64>::new();
    let mut o_custkey = Vec::<i64>::new();
    let mut o_orderdate = Vec::<String>::new();
    let mut o_shippriority = Vec::<i64>::new();
    for fields in read_tbl_fields(&src)? {
        o_orderkey.push(parse_i64(&fields, 0, "orders.o_orderkey")?);
        o_custkey.push(parse_i64(&fields, 1, "orders.o_custkey")?);
        o_orderdate.push(parse_string(&fields, 4, "orders.o_orderdate")?);
        o_shippriority.push(parse_i64(&fields, 7, "orders.o_shippriority")?);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderdate", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int64, false),
    ]));
    write_parquet(
        &dst,
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(o_orderkey)),
            Arc::new(Int64Array::from(o_custkey)),
            Arc::new(StringArray::from(o_orderdate)),
            Arc::new(Int64Array::from(o_shippriority)),
        ],
    )?;
    Ok(file_manifest("orders.parquet", &schema, count_rows(&dst)?))
}

fn convert_lineitem(input_dir: &Path, output_dir: &Path) -> Result<TpchParquetFileManifest> {
    let src = input_dir.join("lineitem.tbl");
    let dst = output_dir.join("lineitem.parquet");

    let mut l_orderkey = Vec::<i64>::new();
    let mut l_quantity = Vec::<f64>::new();
    let mut l_extendedprice = Vec::<f64>::new();
    let mut l_discount = Vec::<f64>::new();
    let mut l_tax = Vec::<f64>::new();
    let mut l_returnflag = Vec::<String>::new();
    let mut l_linestatus = Vec::<String>::new();
    let mut l_shipdate = Vec::<String>::new();

    for fields in read_tbl_fields(&src)? {
        l_orderkey.push(parse_i64(&fields, 0, "lineitem.l_orderkey")?);
        l_quantity.push(parse_f64(&fields, 4, "lineitem.l_quantity")?);
        l_extendedprice.push(parse_f64(&fields, 5, "lineitem.l_extendedprice")?);
        l_discount.push(parse_f64(&fields, 6, "lineitem.l_discount")?);
        l_tax.push(parse_f64(&fields, 7, "lineitem.l_tax")?);
        l_returnflag.push(parse_string(&fields, 8, "lineitem.l_returnflag")?);
        l_linestatus.push(parse_string(&fields, 9, "lineitem.l_linestatus")?);
        l_shipdate.push(parse_string(&fields, 10, "lineitem.l_shipdate")?);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Utf8, false),
    ]));
    write_parquet(
        &dst,
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(l_orderkey)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(StringArray::from(l_returnflag)),
            Arc::new(StringArray::from(l_linestatus)),
            Arc::new(StringArray::from(l_shipdate)),
        ],
    )?;
    Ok(file_manifest(
        "lineitem.parquet",
        &schema,
        count_rows(&dst)?,
    ))
}

fn read_tbl_fields(path: &Path) -> Result<Vec<Vec<String>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for (line_no, line_res) in reader.lines().enumerate() {
        let line = line_res?;
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        let body = trimmed.strip_suffix('|').unwrap_or(trimmed);
        if body.is_empty() {
            continue;
        }
        let fields = body
            .split('|')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        if fields.is_empty() {
            return Err(FfqError::Execution(format!(
                "empty row in {} at line {}",
                path.display(),
                line_no + 1
            )));
        }
        rows.push(fields);
    }
    Ok(rows)
}

fn parse_i64(fields: &[String], idx: usize, name: &str) -> Result<i64> {
    let v = fields
        .get(idx)
        .ok_or_else(|| FfqError::Execution(format!("missing field {name} at index {idx}")))?;
    v.parse::<i64>()
        .map_err(|e| FfqError::Execution(format!("parse i64 failed for {name}: {e}")))
}

fn parse_f64(fields: &[String], idx: usize, name: &str) -> Result<f64> {
    let v = fields
        .get(idx)
        .ok_or_else(|| FfqError::Execution(format!("missing field {name} at index {idx}")))?;
    v.parse::<f64>()
        .map_err(|e| FfqError::Execution(format!("parse f64 failed for {name}: {e}")))
}

fn parse_string(fields: &[String], idx: usize, name: &str) -> Result<String> {
    fields
        .get(idx)
        .cloned()
        .ok_or_else(|| FfqError::Execution(format!("missing field {name} at index {idx}")))
}

fn write_parquet(path: &Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) -> Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| FfqError::Execution(format!("record batch build failed: {e}")))?;
    // Deterministic writer settings for stable output layout across runs.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| FfqError::Execution(format!("parquet writer init failed: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| FfqError::Execution(format!("parquet write failed: {e}")))?;
    writer
        .close()
        .map_err(|e| FfqError::Execution(format!("parquet close failed: {e}")))?;
    Ok(())
}

fn file_manifest(file: &str, schema: &Schema, rows: i64) -> TpchParquetFileManifest {
    TpchParquetFileManifest {
        file: file.to_string(),
        rows,
        schema: schema
            .fields()
            .iter()
            .map(|f| format!("{}:{:?}:{}", f.name(), f.data_type(), f.is_nullable()))
            .collect(),
    }
}

fn count_rows(path: &Path) -> Result<i64> {
    let file = File::open(path)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| FfqError::Execution(format!("parquet reader init failed: {e}")))?;
    let mut iter = reader
        .build()
        .map_err(|e| FfqError::Execution(format!("parquet reader build failed: {e}")))?;
    let mut rows = 0_i64;
    for batch in &mut iter {
        let b = batch.map_err(|e| FfqError::Execution(format!("parquet read failed: {e}")))?;
        rows += b.num_rows() as i64;
    }
    Ok(rows)
}
