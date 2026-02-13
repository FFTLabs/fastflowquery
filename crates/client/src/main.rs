use ffq_common::EngineConfig;
use ffq_client::Engine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();

    let plan_only = args.first().map(|s| s == "--plan").unwrap_or(false);
    if plan_only {
        args.remove(0);
    }

    let sql = args
        .get(0)
        .cloned()
        .unwrap_or_else(|| "SELECT 1".to_string());

    let engine = Engine::new(EngineConfig::default())?;
    let df = engine.sql(&sql)?;

    // This proves SQL parsing + SQL->LogicalPlan works without needing any tables.
    println!("{:#?}", df.logical_plan());

    // Only run execution if NOT plan-only.
    if !plan_only {
        // This will fail until we can query real data / have schema inference.
        let batches = futures::executor::block_on(df.collect())?;
        println!("OK: {} batches (skeleton)", batches.len());
    }

    futures::executor::block_on(engine.shutdown())?;
    Ok(())
}
