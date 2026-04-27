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
}

impl ResourceStealingScheduler {
    pub fn memory() -> Self {
        Self {
            resource_kind: ResourceKind::memory(),
            base_overhead: 150 * 1024 * 1024,
            pressure_threshold: 500 * 1024 * 1024,
        }
    }

    fn get(&self, map: &ResourceMap) -> u64 {
        map.get(&self.resource_kind)
    }

    fn singleton(&self, value: u64) -> ResourceMap {
        ResourceMap::from([(self.resource_kind.clone(), value)])
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

        let temp_factor: f64 = match worker_idle_index {
            0 => 1.5,
            1 => 2.0,
            n => (n + 1) as f64,
        };

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
mod tests {
    use std::path::PathBuf;

    use db_comm_api_base::WorkerId;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    struct FixedEstimator(u64);
    impl ResourceEstimator for FixedEstimator {
        fn estimate(&self, _binary_size: u64) -> ResourceMap {
            ResourceMap::from([(ResourceKind::memory(), self.0)])
        }
    }

    struct LinearEstimator;
    impl ResourceEstimator for LinearEstimator {
        fn estimate(&self, binary_size: u64) -> ResourceMap {
            ResourceMap::from([(ResourceKind::memory(), binary_size * 2)])
        }
    }

    fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
        BinaryInfo {
            path: PathBuf::from(format!("/tmp/{name}")),
            size,
            identifier: TestId(name.into()),
        }
    }

    fn mem(value: u64) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), value)])
    }

    fn make_worker(
        id: WorkerId,
        budget: u64,
        idle: bool,
        opportunistic: bool,
    ) -> WorkerBudgetInfo<TestId> {
        WorkerBudgetInfo {
            worker_id: id,
            reserved_budgets: mem(budget),
            actual_usage: mem(0),
            is_idle: idle,
            is_opportunistic: opportunistic,
            has_initial_assignment: false,
            current_task: None,
            estimated_usage: mem(0),
        }
    }

    fn sched() -> ResourceStealingScheduler {
        ResourceStealingScheduler::memory()
    }

    // ── initial_budget tests ──

    #[test]
    fn initial_budget_worker_0() {
        let s = sched();
        let max = 8 * 1024 * 1024 * 1024u64;
        assert_eq!(
            Scheduler::<TestId>::initial_budget(&s, 0, &mem(max)),
            mem(max)
        );
    }

    #[test]
    fn initial_budget_worker_1() {
        let s = sched();
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 2 + 150 * 1024 * 1024;
        assert_eq!(
            Scheduler::<TestId>::initial_budget(&s, 1, &mem(max)),
            mem(expected)
        );
    }

    #[test]
    fn initial_budget_worker_2() {
        let s = sched();
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 4 + 150 * 1024 * 1024;
        assert_eq!(
            Scheduler::<TestId>::initial_budget(&s, 2, &mem(max)),
            mem(expected)
        );
    }

    #[test]
    fn initial_budget_worker_3() {
        let s = sched();
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 5 + 150 * 1024 * 1024;
        assert_eq!(
            Scheduler::<TestId>::initial_budget(&s, 3, &mem(max)),
            mem(expected)
        );
    }

    #[test]
    fn initial_budget_worker_4() {
        let s = sched();
        let max = 8 * 1024 * 1024 * 1024u64;
        let expected = max / 6 + 150 * 1024 * 1024;
        assert_eq!(
            Scheduler::<TestId>::initial_budget(&s, 4, &mem(max)),
            mem(expected)
        );
    }

    // ── assign_initial tests ──

    #[test]
    fn assign_initial_picks_fitting_task() {
        let s = sched();
        let worker = make_worker(0, 500, true, false);
        let binaries = vec![make_binary("big", 1000), make_binary("small", 100)];

        let decision = s.assign_initial(&worker, &binaries, &mem(0), &mem(1000), &LinearEstimator);
        match decision {
            AssignmentDecision::Assign {
                worker_id,
                binary_index,
                estimated_usage,
                opportunistic,
            } => {
                assert_eq!(worker_id, 0);
                assert_eq!(binary_index, 1);
                assert_eq!(estimated_usage.get(&ResourceKind::memory()), 200);
                assert!(!opportunistic);
            }
            _ => panic!("expected Assign, got {decision:?}"),
        }
    }

    #[test]
    fn assign_initial_marks_opportunistic_when_exceeding_max() {
        let s = sched();
        let worker = make_worker(0, 500, true, false);
        let binaries = vec![make_binary("medium", 100)];

        let decision =
            s.assign_initial(&worker, &binaries, &mem(900), &mem(1000), &LinearEstimator);
        match decision {
            AssignmentDecision::Assign { opportunistic, .. } => {
                assert!(opportunistic);
            }
            _ => panic!("expected Assign"),
        }
    }

    #[test]
    fn assign_initial_no_fit() {
        let s = sched();
        let worker = make_worker(0, 100, true, false);
        let binaries = vec![make_binary("huge", 1000)];

        let decision =
            s.assign_initial(&worker, &binaries, &mem(0), &mem(10000), &LinearEstimator);
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    #[test]
    fn assign_initial_no_pending() {
        let s = sched();
        let worker = make_worker(0, 500, true, false);
        let decision = s.assign_initial(&worker, &[], &mem(0), &mem(1000), &FixedEstimator(100));
        assert!(matches!(decision, AssignmentDecision::NoPendingTasks));
    }

    #[test]
    fn assign_initial_skips_already_assigned() {
        let s = sched();
        let mut worker = make_worker(0, 500, true, false);
        worker.has_initial_assignment = true;
        let binaries = vec![make_binary("a", 10)];

        let decision =
            s.assign_initial(&worker, &binaries, &mem(0), &mem(1000), &FixedEstimator(10));
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    // ── assign_normal tests ──

    #[test]
    fn assign_normal_picks_fitting_task() {
        let s = sched();
        let workers = vec![make_worker(0, 500, true, false)];
        let binaries = vec![make_binary("a", 100)];

        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, &mem(10000), &LinearEstimator, false);
        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } => {
                assert_eq!(binary_index, 0);
                assert_eq!(estimated_usage.get(&ResourceKind::memory()), 200);
            }
            _ => panic!("expected Assign, got {decision:?}"),
        }
    }

    #[test]
    fn assign_normal_opportunistic_caps_to_available() {
        let s = sched();
        let w = make_worker(0, 1000, true, true);
        let workers = vec![w];
        let binaries = vec![make_binary("a", 100)];

        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, &mem(300), &LinearEstimator, false);
        assert!(matches!(decision, AssignmentDecision::Assign { .. }));
    }

    #[test]
    fn assign_normal_opportunistic_rejects_too_large() {
        let s = sched();
        let w = make_worker(0, 1000, true, true);
        let workers = vec![w];
        let binaries = vec![make_binary("a", 200)];

        let decision =
            s.assign_normal(&workers[0], &workers, &binaries, &mem(300), &LinearEstimator, false);
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    #[test]
    fn assign_normal_non_idle_worker_returns_no_fit() {
        let s = sched();
        let w = make_worker(0, 500, false, false);
        let workers = vec![w];
        let binaries = vec![make_binary("a", 10)];

        let decision = s.assign_normal(
            &workers[0],
            &workers,
            &binaries,
            &mem(10000),
            &FixedEstimator(10),
            false,
        );
        assert!(matches!(decision, AssignmentDecision::NoFit));
    }

    // ── check_resource_pressure tests ──

    #[test]
    fn check_pressure_no_action_below_threshold() {
        let s = sched();
        let workers = vec![WorkerBudgetInfo {
            worker_id: 0,
            reserved_budgets: mem(1000),
            actual_usage: mem(100),
            is_idle: false,
            is_opportunistic: false,
            has_initial_assignment: true,
            current_task: Some(make_binary("a", 10)),
            estimated_usage: mem(100),
        }];
        let decision =
            Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(10000), false);
        assert!(matches!(decision, ResourcePressureDecision::NoAction));
    }

    #[test]
    fn check_pressure_kills_opportunistic_first() {
        let s = sched();
        let max = 1000u64;
        let workers = vec![
            WorkerBudgetInfo {
                worker_id: 0,
                reserved_budgets: mem(500),
                actual_usage: mem(400),
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("a", 10)),
                estimated_usage: mem(400),
            },
            WorkerBudgetInfo {
                worker_id: 1,
                reserved_budgets: mem(500),
                actual_usage: mem(400),
                is_idle: false,
                is_opportunistic: true,
                has_initial_assignment: true,
                current_task: Some(make_binary("b", 10)),
                estimated_usage: mem(400),
            },
        ];
        let decision =
            Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(max), false);
        match decision {
            ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
            _ => panic!("expected Kill"),
        }
    }

    #[test]
    fn check_pressure_kills_smallest_active_when_no_opportunistic() {
        let s = sched();
        let max = 1000u64;
        let workers = vec![
            WorkerBudgetInfo {
                worker_id: 0,
                reserved_budgets: mem(500),
                actual_usage: mem(600),
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("a", 10)),
                estimated_usage: mem(600),
            },
            WorkerBudgetInfo {
                worker_id: 1,
                reserved_budgets: mem(500),
                actual_usage: mem(500),
                is_idle: false,
                is_opportunistic: false,
                has_initial_assignment: true,
                current_task: Some(make_binary("b", 10)),
                estimated_usage: mem(300),
            },
        ];
        let decision =
            Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(max), false);
        match decision {
            ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
            _ => panic!("expected Kill"),
        }
    }

    #[test]
    fn check_pressure_still_runs_during_pressure_phase() {
        let s = sched();
        let workers = vec![WorkerBudgetInfo {
            worker_id: 0,
            reserved_budgets: mem(500),
            actual_usage: mem(99999),
            is_idle: false,
            is_opportunistic: true,
            has_initial_assignment: true,
            current_task: Some(make_binary("a", 10)),
            estimated_usage: mem(99999),
        }];
        let decision =
            Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
        assert!(matches!(decision, ResourcePressureDecision::Kill { .. }));
    }

    #[test]
    fn check_pressure_no_action_empty_workers() {
        let s = sched();
        let decision =
            Scheduler::<TestId>::check_resource_pressure(&s, &[], &mem(10000), false);
        assert!(matches!(decision, ResourcePressureDecision::NoAction));
    }

    // ── Multi-idle worker temp_factor tests ──

    #[test]
    fn assign_normal_temp_factor_ordering() {
        let s = sched();
        let workers = vec![
            make_worker(0, 100, true, true),
            make_worker(1, 200, true, true),
            make_worker(2, 300, true, true),
        ];
        let binaries = vec![make_binary("a", 50)];

        let d0 =
            s.assign_normal(&workers[0], &workers, &binaries, &mem(600), &LinearEstimator, false);
        assert!(matches!(d0, AssignmentDecision::Assign { .. }));

        let d1 =
            s.assign_normal(&workers[1], &workers, &binaries, &mem(600), &LinearEstimator, false);
        assert!(matches!(d1, AssignmentDecision::Assign { .. }));

        let d2 =
            s.assign_normal(&workers[2], &workers, &binaries, &mem(600), &LinearEstimator, false);
        assert!(matches!(d2, AssignmentDecision::Assign { .. }));
    }
}
