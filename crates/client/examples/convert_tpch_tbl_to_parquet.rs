use std::path::{Path, PathBuf};

use ffq_client::tpch_tbl::{
    convert_tpch_sf1_tbl_to_parquet, default_tpch_dbgen_parquet_output_dir,
    default_tpch_dbgen_tbl_input_dir,
};
use ffq_common::{FfqError, Result};

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut input_dir = default_tpch_dbgen_tbl_input_dir();
    let mut output_dir = default_tpch_dbgen_parquet_output_dir();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--input-dir" => {
                i += 1;
                input_dir = PathBuf::from(require_arg(&args, i, "--input-dir")?);
            }
            "--output-dir" => {
                i += 1;
                output_dir = PathBuf::from(require_arg(&args, i, "--output-dir")?);
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other => {
                return Err(FfqError::InvalidConfig(format!(
                    "unknown argument: {other}. Use --help."
                )));
            }
        }
        i += 1;
    }

    let manifest = convert_tpch_sf1_tbl_to_parquet(Path::new(&input_dir), Path::new(&output_dir))?;
    println!(
        "Converted TPC-H dbgen .tbl -> parquet at {} ({} files)",
        output_dir.display(),
        manifest.files.len()
    );
    Ok(())
}

fn require_arg(args: &[String], idx: usize, flag: &str) -> Result<String> {
    args.get(idx).cloned().ok_or_else(|| {
        FfqError::InvalidConfig(format!("missing value for {flag}; run with --help"))
    })
}

fn print_usage() {
    eprintln!("Usage: convert_tpch_tbl_to_parquet [--input-dir PATH] [--output-dir PATH]");
}
