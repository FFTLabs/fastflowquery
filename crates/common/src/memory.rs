//! Shared memory-budget and spill-pressure helpers.
//!
//! This module provides a lightweight engine-level budget manager that can be
//! shared by embedded runtime and distributed workers. Callers reserve bytes
//! for one query/task execution and receive pressure guidance used to:
//! - reduce batch sizes under pressure
//! - trigger spill decisions earlier under pressure

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pressure level derived from requested vs granted memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Plenty of budget available.
    Normal,
    /// Budget is tight; prefer smaller batches and earlier spill.
    Elevated,
    /// Budget is heavily constrained.
    Critical,
}

/// Runtime hints derived from memory pressure.
#[derive(Debug, Clone, Copy)]
pub struct MemoryPressureSignal {
    /// Pressure classification.
    pub pressure: MemoryPressure,
    /// Effective budget granted to this execution branch.
    pub effective_mem_budget_bytes: usize,
    /// Recommended target batch size.
    pub suggested_batch_size_rows: usize,
    /// Spill trigger ratio numerator.
    pub spill_trigger_ratio_num: u32,
    /// Spill trigger ratio denominator.
    pub spill_trigger_ratio_den: u32,
}

impl MemoryPressureSignal {
    /// Return `estimated_bytes > spill_threshold` in a ratio-safe way.
    #[must_use]
    pub fn should_spill(&self, estimated_bytes: usize) -> bool {
        if self.effective_mem_budget_bytes == 0 {
            return true;
        }
        let estimated = estimated_bytes as u128;
        let den = self.spill_trigger_ratio_den.max(1) as u128;
        let num = self.spill_trigger_ratio_num as u128;
        let budget = self.effective_mem_budget_bytes as u128;
        estimated.saturating_mul(den) > budget.saturating_mul(num)
    }

    /// Compute an integer spill target after applying pressure ratio.
    #[must_use]
    pub fn spill_target_bytes(&self, base_num: u32, base_den: u32) -> usize {
        let den = self.spill_trigger_ratio_den.max(1) as u128;
        let num = self.spill_trigger_ratio_num as u128;
        let base_num = base_num as u128;
        let base_den = base_den.max(1) as u128;
        let budget = self.effective_mem_budget_bytes as u128;
        let adjusted = budget
            .saturating_mul(num)
            .saturating_mul(base_num)
            .saturating_div(den.saturating_mul(base_den));
        adjusted.min(usize::MAX as u128) as usize
    }
}

/// Shared engine-level budget manager.
#[derive(Debug)]
pub struct MemorySpillManager {
    engine_budget_bytes: usize,
    in_use_bytes: AtomicUsize,
    base_batch_size_rows: usize,
    min_batch_size_rows: usize,
}

impl MemorySpillManager {
    /// Create manager with an engine-level budget and batch-size bounds.
    #[must_use]
    pub fn new(
        engine_budget_bytes: usize,
        base_batch_size_rows: usize,
        min_batch_size_rows: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine_budget_bytes,
            in_use_bytes: AtomicUsize::new(0),
            base_batch_size_rows: base_batch_size_rows.max(1),
            min_batch_size_rows: min_batch_size_rows.max(1),
        })
    }

    /// Reserve memory for one query/task and compute pressure guidance.
    #[must_use]
    pub fn reserve(self: &Arc<Self>, requested_bytes: usize) -> MemoryReservation {
        if self.engine_budget_bytes == usize::MAX || requested_bytes == 0 {
            let signal = MemoryPressureSignal {
                pressure: MemoryPressure::Normal,
                effective_mem_budget_bytes: requested_bytes,
                suggested_batch_size_rows: self.base_batch_size_rows,
                spill_trigger_ratio_num: 1,
                spill_trigger_ratio_den: 1,
            };
            return MemoryReservation {
                manager: Arc::clone(self),
                reserved_bytes: 0,
                signal,
            };
        }

        loop {
            let current = self.in_use_bytes.load(Ordering::Acquire);
            let available = self.engine_budget_bytes.saturating_sub(current);
            let granted = requested_bytes.min(available);
            let next = current.saturating_add(granted);
            if self
                .in_use_bytes
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let signal = self.signal_for(requested_bytes, granted);
                return MemoryReservation {
                    manager: Arc::clone(self),
                    reserved_bytes: granted,
                    signal,
                };
            }
        }
    }

    fn signal_for(&self, requested: usize, granted: usize) -> MemoryPressureSignal {
        if requested == 0 {
            return MemoryPressureSignal {
                pressure: MemoryPressure::Normal,
                effective_mem_budget_bytes: granted,
                suggested_batch_size_rows: self.base_batch_size_rows,
                spill_trigger_ratio_num: 1,
                spill_trigger_ratio_den: 1,
            };
        }
        let ratio = granted as f64 / requested as f64;
        if ratio >= 0.75 {
            MemoryPressureSignal {
                pressure: MemoryPressure::Normal,
                effective_mem_budget_bytes: granted,
                suggested_batch_size_rows: self.base_batch_size_rows,
                spill_trigger_ratio_num: 1,
                spill_trigger_ratio_den: 1,
            }
        } else if ratio >= 0.40 {
            MemoryPressureSignal {
                pressure: MemoryPressure::Elevated,
                effective_mem_budget_bytes: granted,
                suggested_batch_size_rows: (self.base_batch_size_rows / 2)
                    .max(self.min_batch_size_rows),
                spill_trigger_ratio_num: 4,
                spill_trigger_ratio_den: 5,
            }
        } else {
            MemoryPressureSignal {
                pressure: MemoryPressure::Critical,
                effective_mem_budget_bytes: granted,
                suggested_batch_size_rows: (self.base_batch_size_rows / 4)
                    .max(self.min_batch_size_rows),
                spill_trigger_ratio_num: 3,
                spill_trigger_ratio_den: 5,
            }
        }
    }
}

/// RAII reservation that releases engine budget on drop.
#[derive(Debug)]
pub struct MemoryReservation {
    manager: Arc<MemorySpillManager>,
    reserved_bytes: usize,
    signal: MemoryPressureSignal,
}

impl MemoryReservation {
    /// Pressure signal for this reservation.
    #[must_use]
    pub fn signal(&self) -> MemoryPressureSignal {
        self.signal
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.reserved_bytes > 0 {
            self.manager
                .in_use_bytes
                .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_releases_budget_on_drop() {
        let manager = MemorySpillManager::new(100, 1024, 128);
        {
            let r1 = manager.reserve(80);
            assert_eq!(r1.signal().effective_mem_budget_bytes, 80);
            let r2 = manager.reserve(80);
            assert_eq!(r2.signal().effective_mem_budget_bytes, 20);
            assert_eq!(r2.signal().pressure, MemoryPressure::Critical);
        }
        let r3 = manager.reserve(100);
        assert_eq!(r3.signal().effective_mem_budget_bytes, 100);
        assert_eq!(r3.signal().pressure, MemoryPressure::Normal);
    }

    #[test]
    fn should_spill_uses_ratio() {
        let manager = MemorySpillManager::new(50, 1024, 128);
        let reservation = manager.reserve(100);
        let signal = reservation.signal();
        assert_eq!(signal.spill_trigger_ratio_num, 4);
        assert_eq!(signal.spill_trigger_ratio_den, 5);
        assert!(!signal.should_spill(39));
        assert!(signal.should_spill(41));
    }
}
