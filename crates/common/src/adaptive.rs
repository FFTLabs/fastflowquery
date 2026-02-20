//! Shared adaptive reduce-partition planning primitives.
//!
//! This module is runtime-agnostic and is used by both embedded and
//! distributed execution paths to keep adaptive partition decisions identical
//! for the same observed partition-byte statistics.

use std::collections::HashMap;

/// One reduce-task assignment produced by adaptive planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceTaskAssignment {
    /// Reduce partition ids this task should consume.
    pub assigned_reduce_partitions: Vec<u32>,
    /// Hash-shard split index for hot-partition splitting.
    pub assigned_reduce_split_index: u32,
    /// Total hash-shard split count for this assignment.
    pub assigned_reduce_split_count: u32,
}

/// One partition-bytes histogram bucket for AQE diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionBytesHistogramBucket {
    /// Inclusive upper bound in bytes for the bucket.
    pub upper_bound_bytes: u64,
    /// Number of partitions in this bucket.
    pub partition_count: u32,
}

/// Adaptive reduce-layout planning result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveReducePlan {
    /// Planned reduce task count before AQE adjustments.
    pub planned_reduce_tasks: u32,
    /// Final adaptive reduce task count.
    pub adaptive_reduce_tasks: u32,
    /// Target bytes per reduce task used by the planner.
    pub target_bytes: u64,
    /// Final reduce-task assignments.
    pub assignments: Vec<ReduceTaskAssignment>,
    /// Number of skew-split reduce tasks in final assignments.
    pub skew_split_tasks: u32,
    /// AQE event messages describing major planner decisions.
    pub aqe_events: Vec<String>,
    /// Histogram of observed bytes by reduce partition.
    pub partition_bytes_histogram: Vec<PartitionBytesHistogramBucket>,
}

/// Compute deterministic adaptive reduce assignments from observed partition bytes.
#[allow(clippy::too_many_arguments)]
pub fn plan_adaptive_reduce_layout(
    planned_partitions: u32,
    target_bytes: u64,
    bytes_by_partition: &HashMap<u32, u64>,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
    max_partitions_per_task: u32,
) -> AdaptiveReducePlan {
    let planned_reduce_tasks = planned_partitions.max(1);
    let mut assignments = if bytes_by_partition.is_empty() {
        (0..planned_reduce_tasks)
            .map(|p| ReduceTaskAssignment {
                assigned_reduce_partitions: vec![p],
                assigned_reduce_split_index: 0,
                assigned_reduce_split_count: 1,
            })
            .collect::<Vec<_>>()
    } else {
        deterministic_coalesce_split_groups(
            planned_reduce_tasks,
            target_bytes,
            bytes_by_partition,
            min_reduce_tasks,
            max_reduce_tasks,
            max_partitions_per_task,
        )
    };

    if assignments.is_empty() {
        assignments.push(ReduceTaskAssignment {
            assigned_reduce_partitions: vec![0],
            assigned_reduce_split_index: 0,
            assigned_reduce_split_count: 1,
        });
    }

    let adaptive_reduce_tasks = assignments.len() as u32;
    let skew_split_tasks = assignments
        .iter()
        .filter(|a| a.assigned_reduce_split_count > 1)
        .count() as u32;
    let reason = if adaptive_reduce_tasks > planned_reduce_tasks {
        "split"
    } else if adaptive_reduce_tasks < planned_reduce_tasks {
        "coalesce"
    } else {
        "unchanged"
    };
    let aqe_events = vec![format!(
        "adaptive_layout planned={} adaptive={} reason={} skew_splits={}",
        planned_reduce_tasks, adaptive_reduce_tasks, reason, skew_split_tasks
    )];
    AdaptiveReducePlan {
        planned_reduce_tasks,
        adaptive_reduce_tasks,
        target_bytes,
        assignments,
        skew_split_tasks,
        aqe_events,
        partition_bytes_histogram: build_partition_bytes_histogram(bytes_by_partition),
    }
}

/// Build a stable bytes histogram for reduce partitions.
pub fn build_partition_bytes_histogram(
    bytes_by_partition: &HashMap<u32, u64>,
) -> Vec<PartitionBytesHistogramBucket> {
    const BOUNDS: &[u64] = &[
        64 * 1024,
        256 * 1024,
        1 * 1024 * 1024,
        4 * 1024 * 1024,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        u64::MAX,
    ];
    let mut counts = vec![0_u32; BOUNDS.len()];
    for bytes in bytes_by_partition.values() {
        let idx = BOUNDS
            .iter()
            .position(|b| bytes <= b)
            .unwrap_or(BOUNDS.len() - 1);
        counts[idx] = counts[idx].saturating_add(1);
    }
    BOUNDS
        .iter()
        .zip(counts)
        .filter(|(_, c)| *c > 0)
        .map(|(upper, partition_count)| PartitionBytesHistogramBucket {
            upper_bound_bytes: *upper,
            partition_count,
        })
        .collect()
}

fn deterministic_coalesce_split_groups(
    planned_partitions: u32,
    target_bytes: u64,
    bytes_by_partition: &HashMap<u32, u64>,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
    max_partitions_per_task: u32,
) -> Vec<ReduceTaskAssignment> {
    if planned_partitions <= 1 {
        return vec![ReduceTaskAssignment {
            assigned_reduce_partitions: vec![0],
            assigned_reduce_split_index: 0,
            assigned_reduce_split_count: 1,
        }];
    }
    if target_bytes == 0 {
        return (0..planned_partitions)
            .map(|p| ReduceTaskAssignment {
                assigned_reduce_partitions: vec![p],
                assigned_reduce_split_index: 0,
                assigned_reduce_split_count: 1,
            })
            .collect();
    }

    let mut parts = bytes_by_partition
        .iter()
        .map(|(p, b)| (*p, *b))
        .collect::<Vec<_>>();
    parts.sort_by_key(|(p, _)| *p);

    let mut groups: Vec<Vec<u32>> = Vec::new();
    let mut current: Vec<u32> = Vec::new();
    let mut current_bytes = 0_u64;
    for (p, bytes) in parts {
        if !current.is_empty() && current_bytes.saturating_add(bytes) > target_bytes {
            groups.push(current);
            current = Vec::new();
            current_bytes = 0;
        }
        current.push(p);
        current_bytes = current_bytes.saturating_add(bytes);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    if groups.is_empty() {
        groups.push((0..planned_partitions).collect::<Vec<_>>());
    }

    let groups = split_groups_by_max_partitions(groups, max_partitions_per_task);
    let groups = enforce_group_count_bounds(groups, min_reduce_tasks, max_reduce_tasks);
    apply_hot_partition_splitting(groups, bytes_by_partition, target_bytes, max_reduce_tasks)
}

fn split_groups_by_max_partitions(
    groups: Vec<Vec<u32>>,
    max_partitions_per_task: u32,
) -> Vec<Vec<u32>> {
    if max_partitions_per_task == 0 {
        return groups;
    }
    let chunk = max_partitions_per_task as usize;
    let mut out = Vec::new();
    for g in groups {
        if g.len() <= chunk {
            out.push(g);
        } else {
            for c in g.chunks(chunk) {
                out.push(c.to_vec());
            }
        }
    }
    out
}

fn enforce_group_count_bounds(
    mut groups: Vec<Vec<u32>>,
    min_reduce_tasks: u32,
    max_reduce_tasks: u32,
) -> Vec<Vec<u32>> {
    let min_eff = min_reduce_tasks.max(1) as usize;
    let max_eff = if max_reduce_tasks == 0 {
        usize::MAX
    } else {
        max_reduce_tasks.max(min_reduce_tasks.max(1)) as usize
    };

    while groups.len() < min_eff {
        let Some((idx, _)) = groups.iter().enumerate().find(|(_, g)| g.len() > 1) else {
            break;
        };
        let g = groups.remove(idx);
        let split_at = g.len() / 2;
        groups.insert(idx, g[split_at..].to_vec());
        groups.insert(idx, g[..split_at].to_vec());
    }

    while groups.len() > max_eff && groups.len() > 1 {
        let mut tail = groups.pop().expect("non-empty");
        groups.last_mut().expect("at least one").append(&mut tail);
    }
    groups
}

fn apply_hot_partition_splitting(
    groups: Vec<Vec<u32>>,
    bytes_by_partition: &HashMap<u32, u64>,
    target_bytes: u64,
    max_reduce_tasks: u32,
) -> Vec<ReduceTaskAssignment> {
    let mut layouts = groups
        .into_iter()
        .map(|g| ReduceTaskAssignment {
            assigned_reduce_partitions: g,
            assigned_reduce_split_index: 0,
            assigned_reduce_split_count: 1,
        })
        .collect::<Vec<_>>();
    if target_bytes == 0 {
        return layouts;
    }
    let max_eff = if max_reduce_tasks == 0 {
        u32::MAX
    } else {
        max_reduce_tasks.max(1)
    };
    let mut hot = bytes_by_partition
        .iter()
        .map(|(p, b)| (*p, *b))
        .collect::<Vec<_>>();
    hot.sort_by_key(|(p, _)| *p);
    for (partition, bytes) in hot {
        if bytes <= target_bytes {
            continue;
        }
        let Some(idx) = layouts.iter().position(|l| {
            l.assigned_reduce_split_count == 1
                && l.assigned_reduce_partitions.len() == 1
                && l.assigned_reduce_partitions[0] == partition
        }) else {
            continue;
        };
        let desired = bytes.div_ceil(target_bytes).max(2) as u32;
        let current_tasks = layouts.len() as u32;
        let max_for_this = 1 + max_eff.saturating_sub(current_tasks);
        let split_count = desired.min(max_for_this);
        if split_count <= 1 {
            continue;
        }
        layouts.remove(idx);
        for split_index in (0..split_count).rev() {
            layouts.insert(
                idx,
                ReduceTaskAssignment {
                    assigned_reduce_partitions: vec![partition],
                    assigned_reduce_split_index: split_index,
                    assigned_reduce_split_count: split_count,
                },
            );
        }
    }
    layouts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_plan_is_deterministic() {
        let mut a = HashMap::new();
        a.insert(0_u32, 10_u64);
        a.insert(1_u32, 15_u64);
        a.insert(2_u32, 5_u64);
        a.insert(3_u32, 20_u64);
        let mut b = HashMap::new();
        b.insert(3_u32, 20_u64);
        b.insert(1_u32, 15_u64);
        b.insert(0_u32, 10_u64);
        b.insert(2_u32, 5_u64);
        let pa = plan_adaptive_reduce_layout(4, 25, &a, 1, 0, 0);
        let pb = plan_adaptive_reduce_layout(4, 25, &b, 1, 0, 0);
        assert_eq!(pa.assignments, pb.assignments);
    }
}
