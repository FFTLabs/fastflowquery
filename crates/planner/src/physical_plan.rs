use ffq_common::Result;

use crate::logical_plan::{AggExpr, Expr, JoinStrategyHint, JoinType};

/// The physical operator graph.
///
/// In v1 this is still a logical-ish physical plan (i.e., it can still carry Expr).
/// Later we'll split "physical expr" vs "logical expr" more strictly.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    ParquetScan(ParquetScanExec),
    Filter(FilterExec),
    Project(ProjectExec),
    CoalesceBatches(CoalesceBatchesExec),

    PartialHashAggregate(PartialHashAggregateExec),
    FinalHashAggregate(FinalHashAggregateExec),

    HashJoin(HashJoinExec),

    Exchange(ExchangeExec),

    Limit(LimitExec),
}

impl PhysicalPlan {
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::ParquetScan(_) => vec![],
            PhysicalPlan::Filter(x) => vec![x.input.as_ref()],
            PhysicalPlan::Project(x) => vec![x.input.as_ref()],
            PhysicalPlan::CoalesceBatches(x) => vec![x.input.as_ref()],
            PhysicalPlan::PartialHashAggregate(x) => vec![x.input.as_ref()],
            PhysicalPlan::FinalHashAggregate(x) => vec![x.input.as_ref()],
            PhysicalPlan::HashJoin(x) => vec![x.left.as_ref(), x.right.as_ref()],
            PhysicalPlan::Exchange(x) => match x {
                ExchangeExec::ShuffleWrite(e) => vec![e.input.as_ref()],
                ExchangeExec::ShuffleRead(e) => vec![e.input.as_ref()],
                ExchangeExec::Broadcast(e) => vec![e.input.as_ref()],
            },
            PhysicalPlan::Limit(x) => vec![x.input.as_ref()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    /// Table name from the catalog (v1).
    pub table: String,
    /// Column names (pushdown) if known.
    pub projection: Option<Vec<String>>,
    /// Pushdown-able predicates (best-effort; execution decides how much it can push).
    pub filters: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct FilterExec {
    pub predicate: Expr,
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ProjectExec {
    /// (expr, output_name)
    pub exprs: Vec<(Expr, String)>,
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct CoalesceBatchesExec {
    pub target_batch_rows: usize,
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct PartialHashAggregateExec {
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<(AggExpr, String)>,
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct FinalHashAggregateExec {
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<(AggExpr, String)>,
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone, Copy)]
pub enum BuildSide {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct HashJoinExec {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub on: Vec<(String, String)>,
    pub join_type: JoinType,
    /// From optimizer (broadcast/shuffle hint). Physical planner inserts exchanges accordingly.
    pub strategy_hint: JoinStrategyHint,
    /// The side we build the hash table from (usually the broadcast side).
    pub build_side: BuildSide,
}

#[derive(Debug, Clone)]
pub enum ExchangeExec {
    ShuffleWrite(ShuffleWriteExchange),
    ShuffleRead(ShuffleReadExchange),
    Broadcast(BroadcastExchange),
}

#[derive(Debug, Clone)]
pub struct ShuffleWriteExchange {
    pub input: Box<PhysicalPlan>,
    pub partitioning: PartitioningSpec,
}

#[derive(Debug, Clone)]
pub struct ShuffleReadExchange {
    pub input: Box<PhysicalPlan>,
    pub partitioning: PartitioningSpec,
}

#[derive(Debug, Clone)]
pub struct BroadcastExchange {
    pub input: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub enum PartitioningSpec {
    /// Hash partition by expressions into N partitions.
    Hash { exprs: Vec<Expr>, partitions: usize },
    /// Single partition.
    Single,
}

#[derive(Debug, Clone)]
pub struct LimitExec {
    pub n: usize,
    pub input: Box<PhysicalPlan>,
}
