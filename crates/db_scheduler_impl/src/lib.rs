use db_comm_api_base::{BinaryInfo, Identifier, ResourceKind, ResourceMap};
use db_scheduler_api::{
    AssignmentDecision, ResourceEstimator, ResourcePressureDecision, Scheduler, WorkerBudgetInfo,
};

/// Resource-constrained, resource-stealing scheduler.
///
/// Operates on a single resource kind at a time. For multi-resource scheduling,
/// compose multiple instances with different `resource_kind` values.
#[derive(Clone)]
pub struct ResourceStealingScheduler {
    pub resource_kind: ResourceKind,
    pub base_overhead: u64,
    pub pressure_threshold: u64,
    /// Temporary-budget divisors used when an opportunistic worker requests
    /// a task in `assign_normal`. The slowest idle worker (rank 0) gets
    /// `available / temp_factors[0]`; rank 1 gets `available / temp_factors[1]`;
    /// later ranks fall back to the final element of the slice. Empty
    /// vector means "no temporary budget" — opportunistic workers stick
    /// with their reserved budget only.
    pub temp_factors: Vec<f64>,
}

impl ResourceStealingScheduler {
    pub fn memory() -> Self {
        Self::for_kind(
            ResourceKind::memory(),
            150 * 1024 * 1024,
            500 * 1024 * 1024,
        )
    }

    /// Build a scheduler for an arbitrary resource kind. Pair with
    /// task-tuned overheads/thresholds appropriate for the kind (e.g.
    /// for `"gpu_vram"` you might pass overheads in MB instead of MiB,
    /// and a pressure threshold proportional to the device's free
    /// memory rather than a fixed 500 MiB).
    ///
    /// For multi-resource scheduling, instantiate one
    /// `ResourceStealingScheduler` per kind and dispatch tasks to the
    /// scheduler whose kind is the bottleneck for that task. The
    /// `Scheduler` trait is per-kind by design — tasks that are
    /// limited by both memory AND GPU VRAM register both schedulers
    /// and the runner picks whichever yields a `NoFit` last (the
    /// composed-AND across kinds).
    pub fn for_kind(
        resource_kind: ResourceKind,
        base_overhead: u64,
        pressure_threshold: u64,
    ) -> Self {
        Self {
            resource_kind,
            base_overhead,
            pressure_threshold,
            temp_factors: vec![1.5, 2.0, 3.0, 4.0],
        }
    }

    fn get(&self, map: &ResourceMap) -> u64 {
        map.get(&self.resource_kind)
    }

    fn singleton(&self, value: u64) -> ResourceMap {
        ResourceMap::from([(self.resource_kind.clone(), value)])
    }

    /// Pick the temp-budget divisor for an opportunistic worker at the given
    /// idle-rank index. Empty `temp_factors` means "infinite" — return f64::INFINITY
    /// so the caller's temp budget collapses to zero and the worker uses its
    /// reserved budget unchanged.
    fn temp_factor_for(&self, idle_rank: usize) -> f64 {
        if self.temp_factors.is_empty() {
            return f64::INFINITY;
        }
        let last = self.temp_factors.len() - 1;
        self.temp_factors[idle_rank.min(last)]
    }
}

impl<I: Identifier> Scheduler<I> for ResourceStealingScheduler {
    fn initial_budget(&self, worker_index: u32, max_resources: &ResourceMap) -> ResourceMap {
        let max = self.get(max_resources);
        let value = match worker_index {
            0 => max,
            1 => max / 2 + self.base_overhead,
            2 => max / 4 + self.base_overhead,
            n => max / (n as u64 + 2) + self.base_overhead,
        };
        self.singleton(value)
    }

    fn assign_initial(
        &self,
        worker: &WorkerBudgetInfo<I>,
        pending: &[BinaryInfo<I>],
        total_assigned: &ResourceMap,
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator,
    ) -> AssignmentDecision {
        if worker.has_initial_assignment {
            return AssignmentDecision::NoFit;
        }
        if pending.is_empty() {
            return AssignmentDecision::NoPendingTasks;
        }

        let budget = self.get(&worker.reserved_budgets);
        let total = self.get(total_assigned);
        let max = self.get(max_resources);

        for (i, binary) in pending.iter().enumerate() {
            let est_map = estimator.estimate(binary.size);
            let estimated = self.get(&est_map);
            if estimated > budget {
                continue;
            }

            let would_exceed = (total + estimated) > max;

            return AssignmentDecision::Assign {
                worker_id: worker.worker_id,
                binary_index: i,
                estimated_usage: est_map,
                opportunistic: would_exceed,
            };
        }

        AssignmentDecision::NoFit
    }

    fn assign_normal(
        &self,
        worker: &WorkerBudgetInfo<I>,
        all_workers: &[WorkerBudgetInfo<I>],
        pending: &[BinaryInfo<I>],
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator,
        _retry_attempt: bool,
    ) -> AssignmentDecision {
        if pending.is_empty() {
            return AssignmentDecision::NoPendingTasks;
        }

        let max = self.get(max_resources);
        let actual_total: u64 = all_workers.iter().map(|w| self.get(&w.actual_usage)).sum();
        let available = max.saturating_sub(actual_total);

        let mut idle_workers: Vec<&WorkerBudgetInfo<I>> = all_workers
            .iter()
            .filter(|w| w.is_idle && w.current_task.is_none())
            .collect();
        idle_workers.sort_by_key(|w| self.get(&w.reserved_budgets));

        let worker_idle_index = match idle_workers
            .iter()
            .position(|w| w.worker_id == worker.worker_id)
        {
            Some(idx) => idx,
            None => return AssignmentDecision::NoFit,
        };

        let temp_factor: f64 = self.temp_factor_for(worker_idle_index);

        let worker_budget = self.get(&worker.reserved_budgets);
        let effective_budget = if worker.is_opportunistic {
            let temp_budget = (available as f64 / temp_factor) as u64;
            worker_budget.min(temp_budget)
        } else {
            worker_budget
        };

        for (i, binary) in pending.iter().enumerate() {
            let est_map = estimator.estimate(binary.size);
            let estimated = self.get(&est_map);
            if estimated <= effective_budget {
                return AssignmentDecision::Assign {
                    worker_id: worker.worker_id,
                    binary_index: i,
                    estimated_usage: est_map,
                    opportunistic: false,
                };
            }
        }

        AssignmentDecision::NoFit
    }

    fn check_resource_pressure(
        &self,
        workers: &[WorkerBudgetInfo<I>],
        max_resources: &ResourceMap,
        _in_pressure_phase: bool,
    ) -> ResourcePressureDecision {
        let max = self.get(max_resources);
        let actual_usage: u64 = workers.iter().map(|w| self.get(&w.actual_usage)).sum();
        let num_workers = workers.len() as u64;
        if num_workers == 0 {
            return ResourcePressureDecision::NoAction;
        }
        let threshold = self.pressure_threshold.min(max / num_workers);

        if actual_usage > max.saturating_sub(threshold) {
            let mut opp: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.is_opportunistic && w.current_task.is_some())
                .collect();
            if !opp.is_empty() {
                opp.sort_by_key(|w| self.get(&w.estimated_usage));
                let victim = opp[opp.len() / 2];
                return ResourcePressureDecision::Kill {
                    worker_id: victim.worker_id,
                    reason: format!(
                        "Median opportunistic worker killed under {} pressure (usage: {}MB)",
                        self.resource_kind,
                        actual_usage / (1024 * 1024)
                    ),
                };
            }
        }

        if actual_usage > max {
            let active: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.current_task.is_some())
                .collect();
            if let Some(smallest) = active.iter().min_by_key(|w| self.get(&w.estimated_usage)) {
                return ResourcePressureDecision::Kill {
                    worker_id: smallest.worker_id,
                    reason: format!(
                        "Smallest active worker killed under {} pressure (usage: {}MB)",
                        self.resource_kind,
                        actual_usage / (1024 * 1024)
                    ),
                };
            }
        }

        ResourcePressureDecision::NoAction
    }
}



#[cfg(test)]
mod tests;
