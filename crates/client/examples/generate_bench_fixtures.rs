use std::path::PathBuf;

use ffq_client::bench_fixtures::{default_benchmark_fixture_root, generate_default_benchmark_fixtures};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(default_benchmark_fixture_root);
    generate_default_benchmark_fixtures(&root)?;
    println!("benchmark fixtures generated at {}", root.display());
    Ok(())
}
