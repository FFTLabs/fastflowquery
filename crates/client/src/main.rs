use ffq_common::EngineConfig;
use ffq_client::Engine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = std::env::args().nth(1).unwrap_or_else(|| "SELECT 1".to_string());

    let engine = Engine::new(EngineConfig::default())?;
    let df = engine.sql(&sql)?;

    let batches = futures::executor::block_on(df.collect())?;
    println!("OK: {} batches (skeleton)", batches.len());
    
    futures::executor::block_on(engine.shutdown())?;
    Ok(())
}
