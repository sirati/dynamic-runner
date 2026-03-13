use db_comm_api_base::{BinaryInfo, Identifier, MemoryBytes};
use db_scheduler_api::{
    AssignmentDecision, MemoryEstimator, OomDecision, Scheduler, WorkerBudgetInfo,
};

/// Memory-constrained, memory-stealing scheduler.
///
/// This is a faithful port of the Python `DecisionWorkerManMixin` assignment
/// logic and `ExecutionWorkerManBaseImpl` OOM logic.
#[derive(Clone)]
pub struct MemoryStealingScheduler;

impl<I: Identifier> Scheduler<I> for MemoryStealingScheduler {
    /// Calculate initial budget for a worker given its index.
    ///
    /// Ports `WorkerManagerBase._calculate_initial_budget`:
    ///   index 0: max_memory
    ///   index 1: max_memory/2 + 150MB
    ///   index 2: max_memory/4 + 150MB
    ///   index n: max_memory/(n+2) + 150MB
    fn initial_budget(&self, worker_index: u32, max_memory: MemoryBytes) -> MemoryBytes {
        const BASE_150MB: u64 = 150 * 1024 * 1024;
        match worker_index {
            0 => max_memory,
            1 => max_memory / 2 + BASE_150MB,
            2 => max_memory / 4 + BASE_150MB,
            n => max_memory / (n as u64 + 2) + BASE_150MB,
        }
    }

    /// Assign a binary during the initial phase with opportunistic marking.
    ///
    /// Ports `DecisionWorkerManMixin._assign_binary_to_worker_initial_phase`.
    fn assign_initial(
        &self,
        worker: &WorkerBudgetInfo<I>,
        pending: &[BinaryInfo<I>],
        total_assigned_memory: MemoryBytes,
        max_memory: MemoryBytes,
        estimator: &dyn MemoryEstimator,
    ) -> AssignmentDecision {
        if worker.has_initial_assignment {
            return AssignmentDecision::NoFit;
        }
        if pending.is_empty() {
            return AssignmentDecision::NoPendingTasks;
        }

        let budget = worker.reserved_budget;

        for (i, binary) in pending.iter().enumerate() {
            let estimated = estimator.estimate_memory(binary.size);
            if estimated > budget {
                continue;
            }

            let would_exceed = (total_assigned_memory + estimated) > max_memory;

            return AssignmentDecision::Assign {
                worker_id: worker.worker_id,
                binary_index: i,
                estimated_memory: estimated,
                opportunistic: would_exceed,
            };
        }

        AssignmentDecision::NoFit
    }

    /// Assign a binary during the normal phase.
    ///
    /// Ports `DecisionWorkerManMixin._assign_binary_to_worker_normal`.
    fn assign_normal(
        &self,
        worker: &WorkerBudgetInfo<I>,
        all_workers: &[WorkerBudgetInfo<I>],
        pending: &[BinaryInfo<I>],
        max_memory: MemoryBytes,
        estimator: &dyn MemoryEstimator,
        _retry_attempt: bool,
    ) -> AssignmentDecision {
        if pending.is_empty() {
            return AssignmentDecision::NoPendingTasks;
        }

        // Calculate actual memory usage across all workers
        let actual_total: u64 = all_workers.iter().map(|w| w.actual_memory_usage).sum();
        let available = max_memory.saturating_sub(actual_total);

        // Sort idle workers by budget to determine this worker's position
        let mut idle_workers: Vec<&WorkerBudgetInfo<I>> = all_workers
            .iter()
            .filter(|w| w.is_idle && w.current_task.is_none())
            .collect();
        idle_workers.sort_by_key(|w| w.reserved_budget);

        let worker_idle_index = match idle_workers
            .iter()
            .position(|w| w.worker_id == worker.worker_id)
        {
            Some(idx) => idx,
            None => return AssignmentDecision::NoFit,
        };

        // Temporary budget factor: 1st=1.5, 2nd=2.0, 3rd+=index+1
        let temp_factor: f64 = match worker_idle_index {
            0 => 1.5,
            1 => 2.0,
            n => (n + 1) as f64,
        };

        // Determine effective budget
        let effective_budget = if worker.is_opportunistic {
            let temp_budget = (available as f64 / temp_factor) as u64;
            worker.reserved_budget.min(temp_budget)
        } else {
            worker.reserved_budget
        };

        for (i, binary) in pending.iter().enumerate() {
            let estimated = estimator.estimate_memory(binary.size);
            if estimated <= effective_budget {
                return AssignmentDecision::Assign {
                    worker_id: worker.worker_id,
                    binary_index: i,
                    estimated_memory: estimated,
                    opportunistic: false,
                };
            }
        }

        AssignmentDecision::NoFit
    }

    /// Check memory pressure and decide whether to kill a worker.
    ///
    /// Ports `ExecutionWorkerManBaseImpl._check_memory_pressure_and_kill`.
    fn check_oom(
        &self,
        workers: &[WorkerBudgetInfo<I>],
        max_memory: MemoryBytes,
        _in_oom_phase: bool,
    ) -> OomDecision {
        let actual_usage: u64 = workers.iter().map(|w| w.actual_memory_usage).sum();
        let num_workers = workers.len() as u64;
        if num_workers == 0 {
            return OomDecision::NoAction;
        }
        let threshold = (500u64 * 1024 * 1024).min(max_memory / num_workers);

        // First: kill median opportunistic worker if usage > (max - threshold)
        if actual_usage > max_memory.saturating_sub(threshold) {
            let mut opp: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.is_opportunistic && w.current_task.is_some())
                .collect();
            if !opp.is_empty() {
                opp.sort_by_key(|w| w.estimated_memory);
                let victim = opp[opp.len() / 2];
                return OomDecision::Kill {
                    worker_id: victim.worker_id,
                    reason: format!(
                        "Median opportunistic worker OOM killed (usage: {}MB)",
                        actual_usage / (1024 * 1024)
                    ),
                };
            }
        }

        // Second: kill smallest active worker if usage > max
        if actual_usage > max_memory {
            let active: Vec<&WorkerBudgetInfo<I>> = workers
                .iter()
                .filter(|w| w.current_task.is_some())
                .collect();
            if let Some(smallest) = active.iter().min_by_key(|w| w.estimated_memory) {
                return OomDecision::Kill {
                    worker_id: smallest.worker_id,
                    reason: format!(
                        "Smallest active worker OOM killed (usage: {}MB)",
                        actual_usage / (1024 * 1024)
                    ),
                };
            }
        }

        OomDecision::NoAction
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use db_comm_api_base::WorkerId;
    use serde::{Deserialize, Serialize};

    use super::*;

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    struct FixedEstimator(u64);
    impl MemoryEstimator for FixedEstimator {
        fn estimate_memory(&self, _binary_size: u64) -> MemoryBytes {
            self.0
        }
    }

    struct LinearEstimator;
    impl MemoryEstimator for LinearEstimator {
        fn estimate_memory(&self, binary_size: u64) -> MemoryBytes {
            binary_size * 2
        }
    }

    fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
        BinaryInfo {
            path: PathBuf::from(format!("/tmp/{name}")),
            size,
            identifier: TestId(name.into()),
        }
    }

    fn make_worker(id: WorkerId, budget: MemoryBytes, idle: bool, opportunistic: bool) -> WorkerBudgetInfo<TestId> {
        WorkerBudgetInfo {
            worker_id: id,
            reserved_budget: budget,
            actual_memory_usage: 0,
            is_idle: idle,
            is_opportunistic: opportunistic,
            has_initial_assignment: false,
            current_task: None,
            estimated_memory: 0,
        }
    }

    // ── initial_budget tests ──

    #[test]
    fn initial_budget_worker_0() {
        let s = MemoryStealingScheduler;
        let max = 8 * 1024 * 1024 * 1024u64; // 8GB
        assert_eq!(Scheduler::<TestId>::initial_budget(&s, 0, max), max);
    }

    #[test]
    fn initial_budget_worker_1() {
        let s = MemoryStealingScheduler;
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 2 + 150 * 1024 * 1024;
        assert_eq!(Scheduler::<TestId>::initial_budget(&s, 1, max), expected);
    }

    #[test]
    fn initial_budget_worker_2() {
        let s = MemoryStealingScheduler;
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 4 + 150 * 1024 * 1024;
        assert_eq!(Scheduler::<TestId>::initial_budget(&s, 2, max), expected);
    }

    #[test]
    fn initial_budget_worker_3() {
        let s = MemoryStealingScheduler;
        let max = 8 * 1024 * 1024 * 1024u64;
        // index 3: max/(3+2) + 150MB = max/5 + 150MB
        let expected = max / 5 + 150 * 1024 * 1024;
        assert_eq!(Scheduler::<TestId>::initial_budget(&s, 3, max), expected);
    }

    #[test]
    fn initial_budget_worker_4() {
        let s = MemoryStealingScheduler;
        let max = 8 * 1024 * 1024 * 1024u64;
        // index 4: max/6 + 150MB
        let expected = max / 6 + 150 * 1024 * 1024;
        assert_eq!(Scheduler::<TestId>::initial_budget(&s, 4, max), expected);
    }

    // ── assign_initial tests ──

    #[test]
    fn assign_initial_picks_fitting_task() {
        let s = MemoryStealingScheduler;
        let worker = make_worker(0, 500, true, false);
        let binaries = vec![make_binary("big", 1000), make_binary("small", 100)];
        let estimator = LinearEstimator;

        let decision = s.assign_initial(&worker, &binaries, 0, 1000, &estimator);
        match decision {
            AssignmentDecision::Assign {
                worker_id,
                binary_index,
                estimated_memory,
                opportunistic,
            } => {
                assert_eq!(worker_id, 0);
                assert_eq!(binary_index, 1); // small (200 estimate) fits 500 budget
                assert_eq!(estimated_memory, 200);
                assert!(!opportunistic);
            }
            _ => panic!("expected Assign, got {decision:?}"),
        }
    }

    #[test]
    fn assign_initial_marks_opportunistic_when_exceeding_max() {
        let s = MemoryStealingScheduler;
        let worker = make_worker(0, 500, true, false);
        let binaries = vec![make_binary("medium", 100)];
        let estimator = LinearEstimator; // 100 * 2 = 200

        // total_assigned=900, max=1000 → 900+200=1100 > 1000 → opportunistic
        let decision = s.assign_initial(&worker, &binaries, 900, 1000, &estimator);
        match decision {
            AssignmentDecision::Assign { opportunistic, .. } => {
                assert!(opportunistic);
            }
            _ => panic!("expected Assign"),
        }
    }

    #[test]
    fn assign_initial_no_fit() {
        let s = MemoryStealingScheduler;
        let worker = make_worker(0, 100, true, false);
        let binaries = vec![make_binary("huge", 1000)];
        let estimator = LinearEstimator; // 2000 > 100

        let decision = s.assign_initial(&worker, &binaries, 0, 10000, &estimator);
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    #[test]
    fn assign_initial_no_pending() {
        let s = MemoryStealingScheduler;
        let worker = make_worker(0, 500, true, false);
        let decision = s.assign_initial(&worker, &[], 0, 1000, &FixedEstimator(100));
        assert!(matches!(decision, AssignmentDecision::NoPendingTasks));
    }

    #[test]
    fn assign_initial_skips_already_assigned() {
        let s = MemoryStealingScheduler;
        let mut worker = make_worker(0, 500, true, false);
        worker.has_initial_assignment = true;
        let binaries = vec![make_binary("a", 10)];

        let decision = s.assign_initial(&worker, &binaries, 0, 1000, &FixedEstimator(10));
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    // ── assign_normal tests ──

    #[test]
    fn assign_normal_picks_fitting_task() {
        let s = MemoryStealingScheduler;
        let workers = vec![make_worker(0, 500, true, false)];
        let binaries = vec![make_binary("a", 100)];

        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, 10000, &LinearEstimator, false);
        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_memory,
                ..
            } => {
                assert_eq!(binary_index, 0);
                assert_eq!(estimated_memory, 200);
            }
            _ => panic!("expected Assign, got {decision:?}"),
        }
    }

    #[test]
    fn assign_normal_opportunistic_caps_to_available() {
        let s = MemoryStealingScheduler;
        // One opportunistic idle worker, available memory is tight
        let mut w = make_worker(0, 1000, true, true);
        w.actual_memory_usage = 0;
        let workers = vec![w.clone()];
        let binaries = vec![make_binary("a", 100)]; // est=200

        // max=300, actual_total=0, available=300
        // temp_factor=1.5 (first idle), temp_budget=200
        // effective = min(1000, 200) = 200
        // est=200 <= 200 → fits
        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, 300, &LinearEstimator, false);
        assert!(matches!(decision, AssignmentDecision::Assign { .. }));
    }

    #[test]
    fn assign_normal_opportunistic_rejects_too_large() {
        let s = MemoryStealingScheduler;
        let w = make_worker(0, 1000, true, true);
        let workers = vec![w];
        let binaries = vec![make_binary("a", 200)]; // est=400

        // max=300, actual_total=0, available=300
        // temp_factor=1.5, temp_budget=200
        // effective = min(1000, 200) = 200
        // est=400 > 200 → no fit
        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, 300, &LinearEstimator, false);
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    #[test]
    fn assign_normal_non_idle_worker_returns_no_fit() {
        let s = MemoryStealingScheduler;
        let w = make_worker(0, 500, false, false); // not idle
        let workers = vec![w];
        let binaries = vec![make_binary("a", 10)];

        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, 10000, &FixedEstimator(10), false);
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    // ── check_oom tests ──

    #[test]
    fn check_oom_no_action_below_threshold() {
        let s = MemoryStealingScheduler;
        let workers = vec![WorkerBudgetInfo {
            worker_id: 0,
            reserved_budget: 1000,
            actual_memory_usage: 100,
            is_idle: false,
            is_opportunistic: false,
            has_initial_assignment: true,
            current_task: Some(make_binary("a", 10)),
            estimated_memory: 100,
        }];
        let decision = Scheduler::<TestId>::check_oom(&s, &workers, 10000, false);
        assert!(matches!(decision, OomDecision::NoAction));
    }

    #[test]
    fn check_oom_kills_opportunistic_first() {
        let s = MemoryStealingScheduler;
        let max_memory = 1000u64;
        // Two workers: one normal, one opportunistic, both active
        let workers = vec![
            WorkerBudgetInfo {
                worker_id: 0,
                reserved_budget: 500,
                actual_memory_usage: 400,
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("a", 10)),
                estimated_memory: 400,
            },
            WorkerBudgetInfo {
                worker_id: 1,
                reserved_budget: 500,
                actual_memory_usage: 400,
                is_idle: false,
                is_opportunistic: true,
                has_initial_assignment: true,
                current_task: Some(make_binary("b", 10)),
                estimated_memory: 400,
            },
        ];
        // actual=800, threshold=min(500MB,1000/2)=500, limit=max-threshold=500
        // 800 > 500 → kill median opportunistic → worker 1
        let decision = Scheduler::<TestId>::check_oom(&s, &workers, max_memory, false);
        match decision {
            OomDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
            _ => panic!("expected Kill"),
        }
    }

    #[test]
    fn check_oom_kills_smallest_active_when_no_opportunistic() {
        let s = MemoryStealingScheduler;
        let max_memory = 1000u64;
        let workers = vec![
            WorkerBudgetInfo {
                worker_id: 0,
                reserved_budget: 500,
                actual_memory_usage: 600,
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("a", 10)),
                estimated_memory: 600,
            },
            WorkerBudgetInfo {
                worker_id: 1,
                reserved_budget: 500,
                actual_memory_usage: 500,
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("b", 10)),
                estimated_memory: 300,
            },
        ];
        // actual=1100 > max=1000 → kill smallest by estimated_memory → worker 1 (300)
        let decision = Scheduler::<TestId>::check_oom(&s, &workers, max_memory, false);
        match decision {
            OomDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
            _ => panic!("expected Kill"),
        }
    }

    #[test]
    fn check_oom_still_runs_during_oom_phase() {
        // The scheduler no longer short-circuits during OOM phase.
        // The manager is responsible for deciding what to do with killed tasks
        // during OOM phase (e.g. not requeuing to pending_binaries).
        let s = MemoryStealingScheduler;
        let workers = vec![WorkerBudgetInfo {
            worker_id: 0,
            reserved_budget: 500,
            actual_memory_usage: 99999,
            is_idle: false,
            is_opportunistic: true,
            has_initial_assignment: true,
            current_task: Some(make_binary("a", 10)),
            estimated_memory: 99999,
        }];
        let decision = Scheduler::<TestId>::check_oom(&s, &workers, 100, true);
        // Should still detect OOM and return Kill, even during OOM phase
        assert!(matches!(decision, OomDecision::Kill { .. }));
    }

    #[test]
    fn check_oom_no_action_empty_workers() {
        let s = MemoryStealingScheduler;
        let decision = Scheduler::<TestId>::check_oom(&s, &[], 10000, false);
        assert!(matches!(decision, OomDecision::NoAction));
    }

    // ── Multi-idle worker temp_factor tests ──

    #[test]
    fn assign_normal_temp_factor_ordering() {
        let s = MemoryStealingScheduler;
        // Three idle workers with different budgets
        let workers = vec![
            make_worker(0, 100, true, true),  // lowest budget → idle index 0 → factor 1.5
            make_worker(1, 200, true, true),  // middle → idle index 1 → factor 2.0
            make_worker(2, 300, true, true),  // highest → idle index 2 → factor 3.0
        ];
        let binaries = vec![make_binary("a", 50)]; // est=100

        // max=600, actual_total=0, available=600
        // Worker 0: factor 1.5, temp_budget=400, effective=min(100,400)=100, est=100 ≤ 100 → fits
        let d0 =
            s.assign_normal(&workers[0], &workers, &binaries, 600, &LinearEstimator, false);
        assert!(matches!(d0, AssignmentDecision::Assign { .. }));

        // Worker 1: factor 2.0, temp_budget=300, effective=min(200,300)=200, est=100 ≤ 200 → fits
        let d1 =
            s.assign_normal(&workers[1], &workers, &binaries, 600, &LinearEstimator, false);
        assert!(matches!(d1, AssignmentDecision::Assign { .. }));

        // Worker 2: factor 3.0, temp_budget=200, effective=min(300,200)=200, est=100 ≤ 200 → fits
        let d2 =
            s.assign_normal(&workers[2], &workers, &binaries, 600, &LinearEstimator, false);
        assert!(matches!(d2, AssignmentDecision::Assign { .. }));
    }
}
