use ffq_planner::{ExchangeExec, PhysicalPlan};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StageId(pub usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageNode {
    pub id: StageId,
    pub operators: Vec<String>,
    pub parents: Vec<StageId>,
    pub children: Vec<StageId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageDag {
    pub stages: Vec<StageNode>,
}

impl StageDag {
    pub fn new() -> Self {
        Self { stages: vec![] }
    }

    pub fn root_id(&self) -> Option<StageId> {
        self.stages.first().map(|s| s.id)
    }
}

pub fn build_stage_dag(plan: &PhysicalPlan) -> StageDag {
    let mut dag = StageDag::new();
    let root = new_stage(&mut dag);
    visit_plan(plan, root, &mut dag);
    dag
}

fn new_stage(dag: &mut StageDag) -> StageId {
    let id = StageId(dag.stages.len());
    dag.stages.push(StageNode {
        id,
        operators: Vec::new(),
        parents: Vec::new(),
        children: Vec::new(),
    });
    id
}

fn add_operator(dag: &mut StageDag, stage: StageId, op: &str) {
    dag.stages[stage.0].operators.push(op.to_string());
}

fn add_edge(dag: &mut StageDag, parent: StageId, child: StageId) {
    if !dag.stages[parent.0].children.contains(&child) {
        dag.stages[parent.0].children.push(child);
    }
    if !dag.stages[child.0].parents.contains(&parent) {
        dag.stages[child.0].parents.push(parent);
    }
}

fn visit_plan(plan: &PhysicalPlan, stage: StageId, dag: &mut StageDag) {
    add_operator(dag, stage, op_name(plan));

    match plan {
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(read)) => {
            let upstream = new_stage(dag);
            add_edge(dag, upstream, stage);
            visit_upstream(&read.input, upstream, dag);
        }
        _ => {
            for child in plan.children() {
                visit_plan(child, stage, dag);
            }
        }
    }
}

// Traverse upstream subtree for a stage produced before a ShuffleRead boundary.
fn visit_upstream(plan: &PhysicalPlan, stage: StageId, dag: &mut StageDag) {
    add_operator(dag, stage, op_name(plan));

    match plan {
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(read)) => {
            let upstream = new_stage(dag);
            add_edge(dag, upstream, stage);
            visit_upstream(&read.input, upstream, dag);
        }
        _ => {
            for child in plan.children() {
                visit_upstream(child, stage, dag);
            }
        }
    }
}

fn op_name(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::ParquetScan(_) => "ParquetScan",
        PhysicalPlan::ParquetWrite(_) => "ParquetWrite",
        PhysicalPlan::Filter(_) => "Filter",
        PhysicalPlan::Project(_) => "Project",
        PhysicalPlan::CoalesceBatches(_) => "CoalesceBatches",
        PhysicalPlan::PartialHashAggregate(_) => "PartialHashAggregate",
        PhysicalPlan::FinalHashAggregate(_) => "FinalHashAggregate",
        PhysicalPlan::HashJoin(_) => "HashJoin",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(_)) => "ShuffleWrite",
        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(_)) => "ShuffleRead",
        PhysicalPlan::Exchange(ExchangeExec::Broadcast(_)) => "Broadcast",
        PhysicalPlan::Limit(_) => "Limit",
        PhysicalPlan::TopKByScore(_) => "TopKByScore",
        PhysicalPlan::VectorTopK(_) => "VectorTopK",
    }
}

#[cfg(test)]
mod tests {
    use super::build_stage_dag;
    use std::collections::HashSet;

    use ffq_planner::{
        create_physical_plan, AggExpr, Expr, LogicalPlan, PhysicalPlan, PhysicalPlannerConfig,
    };

    #[test]
    fn cuts_stage_at_shuffle_read() {
        let logical = LogicalPlan::Aggregate {
            group_exprs: vec![Expr::Column("k".to_string())],
            aggr_exprs: vec![(AggExpr::Sum(Expr::Column("v".to_string())), "s".to_string())],
            input: Box::new(LogicalPlan::TableScan {
                table: "t".to_string(),
                projection: None,
                filters: vec![],
            }),
        };
        let physical = create_physical_plan(&logical, &PhysicalPlannerConfig::default())
            .expect("physical plan");
        let dag = build_stage_dag(&physical);

        assert!(dag.stages.len() >= 2);
        // Root stage should include final aggregate and shuffle read.
        let root_ops: HashSet<_> = dag.stages[0].operators.iter().cloned().collect();
        assert!(root_ops.contains("FinalHashAggregate"));
        assert!(root_ops.contains("ShuffleRead"));
        // At least one upstream stage should include partial aggregate/shuffle write.
        assert!(dag.stages.iter().skip(1).any(|s| {
            let ops: HashSet<_> = s.operators.iter().cloned().collect();
            ops.contains("PartialHashAggregate") && ops.contains("ShuffleWrite")
        }));
        // There should be stage dependencies.
        assert!(dag.stages.iter().any(|s| !s.children.is_empty()));
    }

    #[test]
    fn single_stage_without_shuffle() {
        let physical = PhysicalPlan::ParquetScan(ffq_planner::ParquetScanExec {
            table: "t".to_string(),
            projection: None,
            filters: vec![],
        });
        let dag = build_stage_dag(&physical);
        assert_eq!(dag.stages.len(), 1);
        assert_eq!(dag.stages[0].operators, vec!["ParquetScan".to_string()]);
    }
}
