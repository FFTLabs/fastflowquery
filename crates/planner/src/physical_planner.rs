use ffq_common::{FfqError, Result};

use crate::logical_plan::{AggExpr, Expr, JoinStrategyHint, LogicalPlan};
use crate::physical_plan::{
    BroadcastExchange, BuildSide, CteRefExec, ExchangeExec, ExistsSubqueryFilterExec, FilterExec,
    FinalHashAggregateExec, HashJoinAlternativeExec, HashJoinExec, InSubqueryFilterExec, LimitExec,
    ParquetScanExec, ParquetWriteExec, PartialHashAggregateExec, PartitioningSpec, PhysicalPlan,
    ProjectExec, ScalarSubqueryFilterExec, ShuffleReadExchange, ShuffleWriteExchange,
    TopKByScoreExec, UnionAllExec, WindowExec,
};

#[derive(Debug, Clone)]
/// Physical planning knobs that control exchange fanout and output batch sizing.
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
        LogicalPlan::InSubqueryFilter {
            input,
            expr,
            subquery,
            negated,
            correlation: _,
        } => {
            let child = create_physical_plan(input, cfg)?;
            let sub = create_physical_plan(subquery, cfg)?;
            Ok(PhysicalPlan::InSubqueryFilter(InSubqueryFilterExec {
                input: Box::new(child),
                expr: expr.clone(),
                subquery: Box::new(sub),
                negated: *negated,
            }))
        }
        LogicalPlan::ExistsSubqueryFilter {
            input,
            subquery,
            negated,
            correlation: _,
        } => {
            let child = create_physical_plan(input, cfg)?;
            let sub = create_physical_plan(subquery, cfg)?;
            Ok(PhysicalPlan::ExistsSubqueryFilter(
                ExistsSubqueryFilterExec {
                    input: Box::new(child),
                    subquery: Box::new(sub),
                    negated: *negated,
                },
            ))
        }
        LogicalPlan::ScalarSubqueryFilter {
            input,
            expr,
            op,
            subquery,
            correlation: _,
        } => {
            let child = create_physical_plan(input, cfg)?;
            let sub = create_physical_plan(subquery, cfg)?;
            Ok(PhysicalPlan::ScalarSubqueryFilter(
                ScalarSubqueryFilterExec {
                    input: Box::new(child),
                    expr: expr.clone(),
                    op: *op,
                    subquery: Box::new(sub),
                },
            ))
        }

        LogicalPlan::Projection { exprs, input } => {
            let child = create_physical_plan(input, cfg)?;
            Ok(PhysicalPlan::Project(ProjectExec {
                exprs: exprs.clone(),
                input: Box::new(child),
            }))
        }
        LogicalPlan::Window { exprs, input } => {
            let child = create_physical_plan(input, cfg)?;
            let partitioning = window_phase1_partitioning(exprs, cfg);
            let write = PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
                input: Box::new(child),
                partitioning: partitioning.clone(),
            }));
            let read = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
                input: Box::new(write),
                partitioning,
            }));
            Ok(PhysicalPlan::Window(WindowExec {
                exprs: exprs.clone(),
                input: Box::new(read),
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
        LogicalPlan::UnionAll { left, right } => {
            let l = create_physical_plan(left, cfg)?;
            let r = create_physical_plan(right, cfg)?;
            Ok(PhysicalPlan::UnionAll(UnionAllExec {
                left: Box::new(l),
                right: Box::new(r),
            }))
        }
        LogicalPlan::CteRef { name, plan } => {
            let child = create_physical_plan(plan, cfg)?;
            Ok(PhysicalPlan::CteRef(CteRefExec {
                name: name.clone(),
                plan: Box::new(child),
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
        LogicalPlan::HybridVectorScan {
            source,
            query_vectors,
            k,
            ef_search,
            prefilter,
            metric,
            provider,
        } => Ok(PhysicalPlan::VectorKnn(
            crate::physical_plan::VectorKnnExec {
                source: source.clone(),
                query_vector: query_vectors.first().cloned().ok_or_else(|| {
                    ffq_common::FfqError::Planning(
                        "HybridVectorScan requires at least one query vector".to_string(),
                    )
                })?,
                k: *k,
                ef_search: *ef_search,
                prefilter: prefilter.clone(),
                metric: metric.clone(),
                provider: provider.clone(),
            },
        )),

        LogicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            if has_count_distinct(aggr_exprs) {
                return lower_count_distinct_aggregate(group_exprs, aggr_exprs, input, cfg);
            }
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
                        alternatives: Vec::new(),
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
                        alternatives: Vec::new(),
                    }))
                }
                JoinStrategyHint::Shuffle
                | JoinStrategyHint::Auto
                | JoinStrategyHint::SortMerge => {
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
                            input: Box::new(l.clone()),
                            partitioning: part_l.clone(),
                        }));
                    let lr =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
                            input: Box::new(lw),
                            partitioning: part_l,
                        }));

                    let rw =
                        PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
                            input: Box::new(r.clone()),
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
                        build_side: BuildSide::Right, // arbitrary for shuffle/sort-merge shape, executor can decide
                        alternatives: if matches!(
                            *strategy_hint,
                            JoinStrategyHint::Auto | JoinStrategyHint::Shuffle
                        ) {
                            vec![
                                HashJoinAlternativeExec {
                                    left: Box::new(PhysicalPlan::Exchange(
                                        ExchangeExec::Broadcast(BroadcastExchange {
                                            input: Box::new(l.clone()),
                                        }),
                                    )),
                                    right: Box::new(r.clone()),
                                    strategy_hint: JoinStrategyHint::BroadcastLeft,
                                    build_side: BuildSide::Left,
                                },
                                HashJoinAlternativeExec {
                                    left: Box::new(l),
                                    right: Box::new(PhysicalPlan::Exchange(
                                        ExchangeExec::Broadcast(BroadcastExchange {
                                            input: Box::new(r),
                                        }),
                                    )),
                                    strategy_hint: JoinStrategyHint::BroadcastRight,
                                    build_side: BuildSide::Right,
                                },
                            ]
                        } else {
                            Vec::new()
                        },
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

fn has_count_distinct(aggr_exprs: &[(AggExpr, String)]) -> bool {
    aggr_exprs
        .iter()
        .any(|(agg, _)| matches!(agg, AggExpr::CountDistinct(_)))
}

fn lower_count_distinct_aggregate(
    group_exprs: &[Expr],
    aggr_exprs: &[(AggExpr, String)],
    input: &LogicalPlan,
    cfg: &PhysicalPlannerConfig,
) -> Result<PhysicalPlan> {
    if aggr_exprs
        .iter()
        .any(|(agg, _)| !matches!(agg, AggExpr::CountDistinct(_)))
    {
        return Err(FfqError::Unsupported(
            "mixed DISTINCT/non-DISTINCT aggregates are not supported yet".to_string(),
        ));
    }

    let mut distinct_args: Vec<Expr> = Vec::new();
    let mut distinct_pos: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (agg, _) in aggr_exprs {
        let AggExpr::CountDistinct(expr) = agg else {
            continue;
        };
        let key = format!("{expr:?}");
        if let std::collections::hash_map::Entry::Vacant(v) = distinct_pos.entry(key) {
            v.insert(distinct_args.len());
            distinct_args.push(expr.clone());
        }
    }

    let mut dedup_group_exprs = group_exprs.to_vec();
    dedup_group_exprs.extend(distinct_args.clone());

    let dedup_keys = dedup_group_exprs
        .iter()
        .map(expr_to_key_name)
        .collect::<Result<Vec<_>>>()?;
    let dedup_partitioning = PartitioningSpec::HashKeys {
        keys: dedup_keys,
        partitions: cfg.shuffle_partitions,
    };

    let child = create_physical_plan(input, cfg)?;
    let dedup_partial = PhysicalPlan::PartialHashAggregate(PartialHashAggregateExec {
        group_exprs: dedup_group_exprs.clone(),
        aggr_exprs: vec![],
        input: Box::new(child),
    });
    let dedup_write = PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
        input: Box::new(dedup_partial),
        partitioning: dedup_partitioning.clone(),
    }));
    let dedup_read = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
        input: Box::new(dedup_write),
        partitioning: dedup_partitioning,
    }));
    let dedup_final = PhysicalPlan::FinalHashAggregate(FinalHashAggregateExec {
        group_exprs: dedup_group_exprs.clone(),
        aggr_exprs: vec![],
        input: Box::new(dedup_read),
    });

    let mut outer_aggs = Vec::with_capacity(aggr_exprs.len());
    for (agg, alias) in aggr_exprs {
        let AggExpr::CountDistinct(expr) = agg else {
            return Err(FfqError::Unsupported(
                "mixed DISTINCT/non-DISTINCT aggregates are not supported yet".to_string(),
            ));
        };
        let key = format!("{expr:?}");
        let dpos = *distinct_pos
            .get(&key)
            .ok_or_else(|| FfqError::Planning("internal DISTINCT rewrite error".to_string()))?;
        let expr_idx = group_exprs.len() + dpos;
        let expr_name = expr_to_key_name(&dedup_group_exprs[expr_idx])?;
        outer_aggs.push((
            AggExpr::Count(Expr::ColumnRef {
                name: expr_name,
                index: expr_idx,
            }),
            alias.clone(),
        ));
    }

    let outer_group = group_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            Ok(Expr::ColumnRef {
                name: expr_to_key_name(expr)?,
                index: idx,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let outer_keys = outer_group
        .iter()
        .map(expr_to_key_name)
        .collect::<Result<Vec<_>>>()?;
    let outer_partitioning = PartitioningSpec::HashKeys {
        keys: outer_keys,
        partitions: cfg.shuffle_partitions,
    };

    let outer_partial = PhysicalPlan::PartialHashAggregate(PartialHashAggregateExec {
        group_exprs: outer_group.clone(),
        aggr_exprs: outer_aggs.clone(),
        input: Box::new(dedup_final),
    });
    let outer_write = PhysicalPlan::Exchange(ExchangeExec::ShuffleWrite(ShuffleWriteExchange {
        input: Box::new(outer_partial),
        partitioning: outer_partitioning.clone(),
    }));
    let outer_read = PhysicalPlan::Exchange(ExchangeExec::ShuffleRead(ShuffleReadExchange {
        input: Box::new(outer_write),
        partitioning: outer_partitioning,
    }));
    Ok(PhysicalPlan::FinalHashAggregate(FinalHashAggregateExec {
        group_exprs: outer_group,
        aggr_exprs: outer_aggs,
        input: Box::new(outer_read),
    }))
}

fn window_phase1_partitioning(
    exprs: &[crate::logical_plan::WindowExpr],
    cfg: &PhysicalPlannerConfig,
) -> PartitioningSpec {
    if exprs.is_empty() {
        return PartitioningSpec::Single;
    }
    let first = &exprs[0].partition_by;
    // Phase-1 distributed window contract: when all window expressions share
    // the same PARTITION BY keys and they are plain columns, hash-distribute
    // by that key set. Otherwise, fall back to a single partition for
    // correctness.
    if first.is_empty() {
        return PartitioningSpec::Single;
    }
    let first_sig = first
        .iter()
        .map(|e| format!("{e:?}"))
        .collect::<Vec<_>>()
        .join("|");
    if exprs.iter().any(|w| {
        w.partition_by
            .iter()
            .map(|e| format!("{e:?}"))
            .collect::<Vec<_>>()
            .join("|")
            != first_sig
    }) {
        return PartitioningSpec::Single;
    }
    let mut keys = Vec::with_capacity(first.len());
    for e in first {
        match expr_to_key_name(e) {
            Ok(k) => keys.push(k),
            Err(_) => return PartitioningSpec::Single,
        }
    }
    PartitioningSpec::HashKeys {
        keys,
        partitions: cfg.shuffle_partitions,
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
