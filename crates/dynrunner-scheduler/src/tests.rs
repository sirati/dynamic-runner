use std::path::PathBuf;

use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TypeId, WorkerId};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

struct FixedEstimator(u64);
impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), self.0)])
    }
}

struct LinearEstimator;
impl ResourceEstimator<TestId> for LinearEstimator {
    fn estimate(&self, task: &TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), task.size * 2)])
    }
}

fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
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
