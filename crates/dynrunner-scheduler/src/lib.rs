use dynrunner_core::{Identifier, ResourceKind, ResourceMap, TaskInfo};
use dynrunner_scheduler_api::{
    AssignmentDecision, KillReason, ResourceEstimator, ResourcePressureDecision, Scheduler,
    WorkerBudgetInfo,
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
    /// Headroom below the cgroup cap (`max_resources`) at which userland
    /// preempt activates. Both pressure-check branches operate against an
    /// `effective_max = max.saturating_sub(cgroup_safety_margin)` so the
    /// framework's smallest-active-worker kill fires BEFORE the kernel's
    /// cgroup-OOM. Without this margin, the active-kill threshold races
    /// the kernel's `memory.max` enforcement and loses — kernel SIGKILL
    /// strikes first, the worker never gets a userland teardown chance,
    /// and the OOM event surfaces as a process-tree death rather than a
    /// scheduler-mediated kill. Default in `::memory()` / `::for_kind()`
    /// is 1 GiB. Set to `0` to restore the pre-fix behaviour (preempt
    /// only AT the cgroup cap, racing kernel-OOM).
    pub cgroup_safety_margin: u64,
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
        Self::for_kind(ResourceKind::memory(), 150 * 1024 * 1024, 500 * 1024 * 1024)
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
            cgroup_safety_margin: 1024 * 1024 * 1024,
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
        pending: &[TaskInfo<I>],
        total_assigned: &ResourceMap,
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator<I>,
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
            let est_map = estimator.estimate(binary);
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
        pending: &[TaskInfo<I>],
        max_resources: &ResourceMap,
        estimator: &dyn ResourceEstimator<I>,
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
            let est_map = estimator.estimate(binary);
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
        in_pressure_phase: bool,
    ) -> ResourcePressureDecision {
        // The pressure-phase flag is the manager-side authority on
        // "the cluster has crossed an OOM-pressure boundary and active
        // preempt is now warranted". Outside that phase no kill should
        // fire — opportunistic-victim selection and smallest-active
        // selection are both pressure-phase concerns. Pre-fix the flag
        // was unused (underscore-prefixed dead parameter), so the
        // descending-budget smallest-active branch fired
        // unconditionally as soon as one worker's actual_usage
        // overshot `effective_max`, producing the observed 100–400 ms
        // `NoFaultUnderBudget` kill cadence on secondaries that never
        // enter a pressure phase. The gate here restores the
        // architectural intent: the SCHEDULER decides whether the
        // system is in pressure; outside of pressure, return NoAction
        // unconditionally.
        if !in_pressure_phase {
            return ResourcePressureDecision::NoAction;
        }
        let max = self.get(max_resources);
        let actual_usage: u64 = workers.iter().map(|w| self.get(&w.actual_usage)).sum();
        let num_workers = workers.len() as u64;
        if num_workers == 0 {
            return ResourcePressureDecision::NoAction;
        }
        // Reserve a headroom band below the cgroup cap so the
        // framework's preempt fires before the kernel's cgroup-OOM.
        // Both kill branches operate against `effective_max`; the
        // user-supplied `pressure_threshold` is then layered on top
        // for the opportunistic branch as before. With the defaults
        // (`cgroup_safety_margin = 1 GiB`, `pressure_threshold = 500 MiB`):
        //   - opportunistic-kill fires when usage > max − 1.5 GiB
        //   - active-kill fires when usage > max − 1 GiB
        // giving userland a ~1 GiB window before the kernel SIGKILLs.
        let effective_max = max.saturating_sub(self.cgroup_safety_margin);
        let threshold = self.pressure_threshold.min(effective_max / num_workers);

        if actual_usage > effective_max.saturating_sub(threshold) {
            let mut opp: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.is_opportunistic && w.current_task.is_some())
                .collect();
            if !opp.is_empty() {
                opp.sort_by_key(|w| self.get(&w.estimated_usage));
                let victim = opp[opp.len() / 2];
                // Opportunistic workers explicitly opted in to being
                // killable when they were assigned a temp-budget task;
                // the displaced task is no-fault from the retry-budget
                // perspective.
                return ResourcePressureDecision::Kill {
                    worker_id: victim.worker_id,
                    reason: KillReason::NoFaultMemoryStealing,
                };
            }
        }

        if actual_usage > effective_max {
            let active: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.current_task.is_some())
                .collect();
            if let Some(smallest) = active.iter().min_by_key(|w| self.get(&w.estimated_usage)) {
                // Classify the smallest-active victim:
                //   * under reserved budget → another worker (or
                //     external pressure) caused the overshoot; this
                //     task is no-fault and should requeue silently.
                //   * at or above reserved budget → the task itself
                //     overshot its estimate; counts against retry
                //     budget. `OomLastResort` records the
                //     no-alternative-candidate edge so operators can
                //     correlate "framework had no smaller victim" with
                //     the at-fault outcome; otherwise (multiple active
                //     candidates existed and the framework picked the
                //     smallest) → `OomOverBudget`.
                let reserved = self.get(&smallest.reserved_budgets);
                let actual = self.get(&smallest.actual_usage);
                let only_candidate = active.len() == 1;
                let reason = if actual < reserved {
                    KillReason::NoFaultUnderBudget
                } else if only_candidate {
                    KillReason::OomLastResort
                } else {
                    KillReason::OomOverBudget
                };
                return ResourcePressureDecision::Kill {
                    worker_id: smallest.worker_id,
                    reason,
                };
            }
        }

        ResourcePressureDecision::NoAction
    }
}

#[cfg(test)]
mod tests;
