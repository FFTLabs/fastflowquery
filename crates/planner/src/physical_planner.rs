use ffq_common::{FfqError, Result};

use crate::logical_plan::{Expr, JoinStrategyHint, JoinType, LogicalPlan};
use crate::physical_plan::{
    BroadcastExchange, BuildSide, ExchangeExec, FilterExec, FinalHashAggregateExec, HashJoinExec,
    LimitExec, ParquetScanExec, ParquetWriteExec, PartialHashAggregateExec, PartitioningSpec,
    PhysicalPlan, ProjectExec, ShuffleReadExchange, ShuffleWriteExchange, TopKByScoreExec,
};

#[derive(Debug, Clone)]
pub struct PhysicalPlannerConfig {
    /// Number of hash partitions used by shuffle exchanges.
    pub shuffle_partitions: usize,
    /// Target row count for coalescing output batches.
    pub target_batch_rows: usize,
}

impl Default for PhysicalPlannerConfig {
    fn default() -> Self {
        Self {
            shuffle_partitions: 64,
            target_batch_rows: 8192,
        }
    }
}

/// Lower analyzed/optimized logical plan to executable physical operators.
///
/// Contracts:
/// - logical semantics are preserved;
/// - aggregate lowers to `PartialHashAggregate -> Exchange -> FinalHashAggregate`;
/// - join hints are honored where possible, with `Auto` safely falling back to
///   shuffle shape;
/// - unsupported logical shapes return a planning error.
pub fn create_physical_plan(
    logical: &LogicalPlan,
    cfg: &PhysicalPlannerConfig,
) -> Result<PhysicalPlan> {
    match logical {
        LogicalPlan::TableScan {
            table,
            projection,
            filters,
        } => Ok(PhysicalPlan::ParquetScan(ParquetScanExec {
            table: table.clone(),
            schema: None,
            projection: projection.clone(),
            filters: filters.clone(),
        })),

        LogicalPlan::Filter { predicate, input } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::Filter(FilterExec {
                predicate: predicate.clone(),
                input: Box::new(child),
            }))
        }

        LogicalPlan::Projection { exprs, input } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::Project(ProjectExec {
                exprs: exprs.clone(),
                input: Box::new(child),
            }))
        }

        LogicalPlan::Limit { n, input } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::Limit(LimitExec {
                n: *n,
                input: Box::new(child),
            }))
        }
        LogicalPlan::TopKByScore {
            score_expr,
            k,
            input,
        } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::TopKByScore(TopKByScoreExec {
                score_expr: score_expr.clone(),
                k: *k,
                input: Box::new(child),
            }))
        }
        LogicalPlan::VectorTopK {
            table,
            query_vector,
            k,
            filter,
        } => Ok(PhysicalPlan::VectorTopK(
            crate::physical_plan::VectorTopKExec {
                table: table.clone(),
                query_vector: query_vector.clone(),
                k: *k,
                filter: filter.clone(),
            },
        )),

        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            // Aggregate -> Partial -> ShuffleExchange(hash(group_keys)) -> Final
            let child = create_physical_plan(input, cfg)?;

            let partial = PhysicalPlan::PartialHashAggregate(PartialHashAggregateExec {
                group_exprs: group_exprs.clone(),
                aggr_exprs: aggr_exprs.clone(),
                input: Box::new(child),
            });

            let keys = group_exprs
                .iter()
                .map(expr_to_key_name)
                .collect::<Result<Vec<_>>>()?;

            let partitioning = PartitioningSpec::HashKeys {
                keys,
                partitions: cfg.shuffle_partitions,
            };

            let write = PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
                input: Box::new(partial),
                partitioning: partitioning.clone(),
            }));

            let read = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
                input: Box::new(write),
                partitioning,
            }));

            Ok(PhysicalPlan::FinalHashAggregate(FinalHashAggregateExec {
                group_exprs: group_exprs.clone(),
                aggr_exprs: aggr_exprs.clone(),
                input: Box::new(read),
            }))
        }

        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            strategy_hint,
        } => {
            if *join_type != JoinType::Inner {
                return Err(FfqError::Unsupported(
                    "only INNER join supported in v1".to_string(),
                ));
            }

            let l = create_physical_plan(left, cfg)?;
            let r = create_physical_plan(right, cfg)?;

            // Build exchanges based on optimizer hint.
            // Note: This is a *plan shape* decision; execution will implement actual exchange behavior.
            match *strategy_hint {
                JoinStrategyHint::BroadcastLeft => {
                    let bcast_left =
                        PhysicalPlan::Exchange(ExchangeExec::Broadcast(BroadcastExchange {
                            input: Box::new(l),
                        }));
                    Ok(PhysicalPlan::HashJoin(HashJoinExec {
                        left: Box::new(bcast_left),
                        right: Box::new(r),
                        on: on.clone(),
                        join_type: *join_type,
                        strategy_hint: *strategy_hint,
                        build_side: BuildSide::Left,
                    }))
                }
                JoinStrategyHint::BroadcastRight => {
                    let bcast_right =
                        PhysicalPlan::Exchange(ExchangeExec::Broadcast(BroadcastExchange {
                            input: Box::new(r),
                        }));
                    Ok(PhysicalPlan::HashJoin(HashJoinExec {
                        left: Box::new(l),
                        right: Box::new(bcast_right),
                        on: on.clone(),
                        join_type: *join_type,
                        strategy_hint: *strategy_hint,
                        build_side: BuildSide::Right,
                    }))
                }
                JoinStrategyHint::Shuffle | JoinStrategyHint::Auto => {
                    // v1: Auto treated as Shuffle at physical level unless optimizer already decided broadcast.
                    // Shuffle both sides by join keys.
                    let left_keys: Vec<String> = on.iter().map(|(lk, _)| lk.clone()).collect();
                    let right_keys: Vec<String> = on.iter().map(|(_, rk)| rk.clone()).collect();

                    let part_l = PartitioningSpec::HashKeys {
                        keys: left_keys,
                        partitions: cfg.shuffle_partitions,
                    };
                    let part_r = PartitioningSpec::HashKeys {
                        keys: right_keys,
                        partitions: cfg.shuffle_partitions,
                    };

                    let lw =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
                            input: Box::new(l),
                            partitioning: part_l.clone(),
                        }));
                    let lr =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
                            input: Box::new(lw),
                            partitioning: part_l,
                        }));

                    let rw =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
                            input: Box::new(r),
                            partitioning: part_r.clone(),
                        }));
                    let rr =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
                            input: Box::new(rw),
                            partitioning: part_r,
                        }));

                    Ok(PhysicalPlan::HashJoin(HashJoinExec {
                        left: Box::new(lr),
                        right: Box::new(rr),
                        on: on.clone(),
                        join_type: *join_type,
                        strategy_hint: *strategy_hint,
                        build_side: BuildSide::Right, // arbitrary for shuffle-join, executor can decide
                    }))
                }
            }
        }
        LogicalPlan::InsertInto { table, input, .. } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::ParquetWrite(ParquetWriteExec {
                table: table.clone(),
                input: Box::new(child),
            }))
        }
    }
}

fn expr_to_key_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Column(name) => Ok(name.clone()),
        Expr::ColumnRef { name, .. } => Ok(name.clone()),
        _ => Err(FfqError::Unsupported(
            "v1 physical planner only supports grouping by plain columns".to_string(),
        )),
    }
}
