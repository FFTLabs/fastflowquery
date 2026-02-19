use ffq_planner::{LogicalPlan, PhysicalPlan, PhysicalPlannerConfig, create_physical_plan};

#[test]
fn physical_plan_is_serializable() {
    // Simple plan: scan -> limit
    let logical = LogicalPlan::Limit {
        n: 10,
        input: Box::new(LogicalPlan::TableScan {
            table: "t".to_string(),
            projection: Some(vec!["id".to_string()]),
            filters: vec![],
        }),
    };

    let phys = create_physical_plan(&logical, &PhysicalPlannerConfig::default()).unwrap();

    let s = serde_json::to_string(&phys).unwrap();
    let _back: ffq_planner::PhysicalPlan = serde_json::from_str(&s).unwrap();

    let v = PhysicalPlan::VectorTopK(ffq_planner::VectorTopKExec {
        table: "docs_idx".to_string(),
        query_vector: vec![1.0, 2.0, 3.0],
        k: 5,
        filter: None,
    });
    let sv = serde_json::to_string(&v).unwrap();
    let _back_v: ffq_planner::PhysicalPlan = serde_json::from_str(&sv).unwrap();
}
