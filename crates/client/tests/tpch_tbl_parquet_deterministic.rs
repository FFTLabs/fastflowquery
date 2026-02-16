use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ffq_client::tpch_tbl::convert_tpch_sf1_tbl_to_parquet;

fn unique_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}_{nanos}"))
}

fn write_lines(path: &Path, lines: &[&str]) {
    let body = lines.join("\n");
    fs::write(path, format!("{body}\n")).expect("write tbl");
}

fn list_files(path: &Path) -> BTreeSet<String> {
    fs::read_dir(path)
        .expect("read_dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().to_string())
        .collect()
}

#[test]
fn tpch_tbl_conversion_is_deterministic_for_schema_rows_and_file_set() {
    let src = unique_dir("ffq_tpch_tbl_src");
    let out1 = unique_dir("ffq_tpch_tbl_out1");
    let out2 = unique_dir("ffq_tpch_tbl_out2");
    fs::create_dir_all(&src).expect("mkdir src");
    fs::create_dir_all(&out1).expect("mkdir out1");
    fs::create_dir_all(&out2).expect("mkdir out2");

    // customer: c_custkey idx0, c_mktsegment idx6
    write_lines(
        &src.join("customer.tbl"),
        &[
            "1|Customer#000000001|Address1|1|10-123-123-1234|100.00|BUILDING|comment|",
            "2|Customer#000000002|Address2|2|10-123-123-1235|200.00|AUTOMOBILE|comment|",
        ],
    );
    // orders: o_orderkey idx0, o_custkey idx1, o_orderdate idx4, o_shippriority idx7
    write_lines(
        &src.join("orders.tbl"),
        &[
            "1|1|O|173665.47|1996-01-02|5-LOW|Clerk#000000001|0|comment|",
            "2|2|F|46929.18|1996-12-01|1-URGENT|Clerk#000000002|0|comment|",
        ],
    );
    // lineitem: mapped fields used by Q1/Q3 pipeline
    write_lines(
        &src.join("lineitem.tbl"),
        &[
            "1|1552|7706|1|17.0|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|comment|",
            "2|674|7311|2|36.0|45983.16|0.09|0.06|R|F|1997-04-12|1997-03-14|1997-04-20|TAKE BACK RETURN|MAIL|comment|",
        ],
    );

    let m1 = convert_tpch_sf1_tbl_to_parquet(&src, &out1).expect("convert out1");
    let m2 = convert_tpch_sf1_tbl_to_parquet(&src, &out2).expect("convert out2");

    assert_eq!(m1.files, m2.files, "file schema/row manifests differ");
    assert_eq!(list_files(&out1), list_files(&out2), "file sets differ");
}
