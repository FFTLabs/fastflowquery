use crate::logical_plan::{AggExpr, Expr, JoinStrategyHint, JoinType};
use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The physical operator graph.
///
/// In v1 this is still a logical-ish physical plan (i.e., it can still carry Expr).
/// Later we'll split "physical expr" vs "logical expr" more strictly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    /// Parquet table scan.
    ParquetScan(ParquetScanExec),
    /// Parquet sink write.
    ParquetWrite(ParquetWriteExec),
    /// Row filter.
    Filter(FilterExec),
    /// Projection.
    Project(ProjectExec),
    /// Batch coalescing.
    CoalesceBatches(CoalesceBatchesExec),

    /// Partial aggregate.
    PartialHashAggregate(PartialHashAggregateExec),
    /// Final aggregate.
    FinalHashAggregate(FinalHashAggregateExec),

    /// Hash join.
    HashJoin(HashJoinExec),

    /// Data exchange boundary.
    Exchange(ExchangeExec),

    /// Limit.
    Limit(LimitExec),
    /// Brute-force top-k.
    TopKByScore(TopKByScoreExec),
    /// Index-backed vector top-k.
    VectorTopK(VectorTopKExec),
    /// Custom operator instantiated via runtime physical operator registry.
    Custom(CustomExec),
}

impl PhysicalPlan {
    /// Returns direct child operators.
    ///
    /// This is used by explain/inspection code and assumes `VectorTopK` is a
    /// leaf and exchange operators have exactly one child.
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::ParquetScan(_) => vec![],
            PhysicalPlan::ParquetWrite(x) => vec![x.input.as_ref()],
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
            PhysicalPlan::TopKByScore(x) => vec![x.input.as_ref()],
            PhysicalPlan::VectorTopK(_) => vec![],
            PhysicalPlan::Custom(x) => vec![x.input.as_ref()],
        }
    }
}

/// Physical parquet scan operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetScanExec {
    /// Table name from the catalog (v1).
    pub table: String,
    /// Resolved schema attached by planner/coordinator for deterministic worker execution.
    #[serde(default)]
    pub schema: Option<Schema>,
    /// Column names (pushdown) if known.
    pub projection: Option<Vec<String>>,
    /// Pushdown-able predicates (best-effort; execution decides how much it can push).
    pub filters: Vec<Expr>,
}

/// Physical parquet sink operator.
///
/// The execution runtime uses `table` to resolve target path and commit
/// semantics from catalog/table options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetWriteExec {
    /// Target table.
    pub table: String,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Row filter operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExec {
    /// Predicate.
    pub predicate: Expr,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Projection operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectExec {
    /// (expr, output_name)
    pub exprs: Vec<(Expr, String)>,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Batch coalescing operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoalesceBatchesExec {
    /// Desired row count per output batch.
    pub target_batch_rows: usize,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Phase-1 hash aggregate over local/shuffle partitions.
///
/// Must be followed by compatible repartition + final aggregate for global SQL
/// aggregate semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialHashAggregateExec {
    /// Grouping expressions.
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions and aliases.
    pub aggr_exprs: Vec<(AggExpr, String)>,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Phase-2 hash aggregate merging partial states after shuffle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalHashAggregateExec {
    /// Grouping expressions.
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions and aliases.
    pub aggr_exprs: Vec<(AggExpr, String)>,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Side chosen to build the hash table for [`HashJoinExec`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BuildSide {
    /// Build hash table from left input.
    Left,
    /// Build hash table from right input.
    Right,
}

/// Hash join physical operator.
///
/// Contract:
/// - `on` is positional key mapping `(left_key, right_key)`.
/// - `strategy_hint` records optimizer intent; exchange nodes define actual
///   data movement.
/// - `build_side` must match the side expected to be in-memory hash build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashJoinExec {
    /// Left input.
    pub left: Box<PhysicalPlan>,
    /// Right input.
    pub right: Box<PhysicalPlan>,
    /// Join key pairs `(left_key, right_key)`.
    pub on: Vec<(String, String)>,
    /// Join type.
    pub join_type: JoinType,
    /// From optimizer (broadcast/shuffle hint). Physical planner inserts exchanges accordingly.
    pub strategy_hint: JoinStrategyHint,
    /// The side we build the hash table from (usually the broadcast side).
    pub build_side: BuildSide,
}

/// Stage-boundary exchange operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExchangeExec {
    /// Shuffle write boundary.
    ShuffleWrite(ShuffleWriteExchange),
    /// Shuffle read boundary.
    ShuffleRead(ShuffleReadExchange),
    /// Broadcast boundary.
    Broadcast(BroadcastExchange),
}

/// Shuffle write boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleWriteExchange {
    /// Input plan.
    pub input: Box<PhysicalPlan>,
    /// Partitioning specification.
    pub partitioning: PartitioningSpec,
}

/// Shuffle read boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleReadExchange {
    /// Input plan.
    pub input: Box<PhysicalPlan>,
    /// Partitioning specification.
    pub partitioning: PartitioningSpec,
}

/// Broadcast boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastExchange {
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Partitioning contract used by exchanges and distributed stage planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningSpec {
    /// Hash partition by expressions into N partitions.
    HashKeys {
        /// Partition key names.
        keys: Vec<String>,
        /// Partition count.
        partitions: usize,
    },
    /// Single partition.
    Single,
}

/// Limit operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitExec {
    /// Maximum number of rows.
    pub n: usize,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Brute-force top-k by score expression.
///
/// Used both as explicit SQL top-k execution path and as fallback when vector
/// index rewrite does not apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopKByScoreExec {
    /// Score expression.
    pub score_expr: Expr,
    /// Number of rows to keep.
    pub k: usize,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}

/// Index-backed vector top-k physical operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorTopKExec {
    /// Table name.
    pub table: String,
    /// Query vector literal.
    pub query_vector: Vec<f32>,
    /// Number of rows to keep.
    pub k: usize,
    /// Optional provider-specific filter payload.
    pub filter: Option<String>,
}

/// Custom physical operator descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomExec {
    /// Registered factory name.
    pub op_name: String,
    /// Opaque operator configuration map.
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Input plan.
    pub input: Box<PhysicalPlan>,
}
