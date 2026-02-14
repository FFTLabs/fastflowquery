use ffq_planner::{
    create_physical_plan, JoinStrategyHint, JoinType, LogicalPlan, PhysicalPlannerConfig,
};

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
}
