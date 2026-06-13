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
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
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

/// Borrow a candidate list the way a clone-free `WorkerView` exposes
/// it to the scheduler (`&[&TaskInfo<I>]`).
fn refs<I>(v: &[TaskInfo<I>]) -> Vec<&TaskInfo<I>> {
    v.iter().collect()
}

// ── assign_initial tests ──

#[test]
fn assign_initial_picks_fitting_task() {
    let s = sched();
    let worker = make_worker(0, 500, true, false);
    let binaries = vec![make_binary("big", 1000), make_binary("small", 100)];

    let decision = s.assign_initial(&worker, &refs(&binaries), &mem(0), &mem(1000), &LinearEstimator);
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

    let decision = s.assign_initial(&worker, &refs(&binaries), &mem(900), &mem(1000), &LinearEstimator);
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

    let decision = s.assign_initial(&worker, &refs(&binaries), &mem(0), &mem(10000), &LinearEstimator);
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

    let decision = s.assign_initial(&worker, &refs(&binaries), &mem(0), &mem(1000), &FixedEstimator(10));
    assert!(matches!(decision, AssignmentDecision::NoFit));
}

// ── assign_normal tests ──

#[test]
fn assign_normal_picks_fitting_task() {
    let s = sched();
    let workers = vec![make_worker(0, 500, true, false)];
    let binaries = vec![make_binary("a", 100)];

    let decision = s.assign_normal(
        &workers[0],
        &workers,
        &refs(&binaries),
        &mem(10000),
        &LinearEstimator,
        false,
    );
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

    let decision = s.assign_normal(
        &workers[0],
        &workers,
        &refs(&binaries),
        &mem(300),
        &LinearEstimator,
        false,
    );
    assert!(matches!(decision, AssignmentDecision::Assign { .. }));
}

#[test]
fn assign_normal_opportunistic_rejects_too_large() {
    let s = sched();
    let w = make_worker(0, 1000, true, true);
    let workers = vec![w];
    let binaries = vec![make_binary("a", 200)];

    let decision = s.assign_normal(
        &workers[0],
        &workers,
        &refs(&binaries),
        &mem(300),
        &LinearEstimator,
        false,
    );
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
        &refs(&binaries),
        &mem(10000),
        &FixedEstimator(10),
        false,
    );
    assert!(matches!(decision, AssignmentDecision::NoFit));
}

// ── check_resource_pressure tests ──

#[test]
fn check_pressure_no_action_below_threshold() {
    // Test uses proxy-byte numbers (max=10000) so the production
    // 1 GiB safety margin would saturate `effective_max` to zero and
    // kill the worker on every tick. Override the margin to zero
    // here so the test exercises ONLY the threshold-driven NoAction
    // branch — the safety-margin behaviour has dedicated tests
    // (`active_kill_fires_at_margin_not_at_cgroup_cap`,
    // `safety_margin_zero_restores_pre_fix_behavior`). Runs under
    // `in_pressure_phase=true` so the in_pressure_phase gate
    // (pinned separately) doesn't mask the threshold check.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(10000), true);
    assert!(matches!(decision, ResourcePressureDecision::NoAction));
}

#[test]
fn check_pressure_kills_opportunistic_first() {
    // Pre-fix threshold semantics under proxy-byte numbers; the
    // safety-margin shift has dedicated tests below.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(max), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
        _ => panic!("expected Kill"),
    }
}

#[test]
fn check_pressure_kills_smallest_active_when_no_opportunistic() {
    // Pre-fix threshold semantics under proxy-byte numbers; the
    // safety-margin shift has dedicated tests below.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(max), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
        _ => panic!("expected Kill"),
    }
}

#[test]
fn check_pressure_kills_during_pressure_phase() {
    // Pins that pressure_phase=true is the precondition for any kill
    // branch to fire. With usage 99999 vs cap 100 the kill is obvious;
    // the assertion is shape-only (Kill vs NoAction).
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
    assert!(matches!(decision, ResourcePressureDecision::Kill { .. }));
}

#[test]
fn check_pressure_no_action_outside_pressure_phase_even_when_over_budget() {
    // Gate test: even when actual_usage massively exceeds effective_max
    // (the scenario that would otherwise hit the smallest-active kill
    // branch), `in_pressure_phase=false` short-circuits to NoAction.
    // This pins the architectural intent that the SCHEDULER decides
    // whether the system is in pressure; the kill path is reserved
    // for explicit pressure-phase entry by the manager.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), false);
    assert!(
        matches!(decision, ResourcePressureDecision::NoAction),
        "expected NoAction with in_pressure_phase=false regardless of usage, got {decision:?}"
    );
}

#[test]
fn check_pressure_no_action_outside_pressure_phase_with_opportunistic_overshoot() {
    // Gate test, opportunistic-branch variant: even when the
    // opportunistic-victim selection would otherwise trigger
    // (`actual_usage > effective_max − threshold` with a live
    // opportunistic worker), `in_pressure_phase=false` short-circuits
    // to NoAction. Pin both branches independently so a future
    // re-arrangement that only gates one of them is caught.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
    let max = 1000u64;
    let workers = vec![
        worker_active(0, 500, 400, false),
        worker_active(1, 500, 400, true),
    ];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(max), false);
    assert!(
        matches!(decision, ResourcePressureDecision::NoAction),
        "expected NoAction with in_pressure_phase=false regardless of opportunistic-victim selection, got {decision:?}"
    );
}

#[test]
fn check_pressure_no_action_empty_workers() {
    let s = sched();
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &[], &mem(10000), false);
    assert!(matches!(decision, ResourcePressureDecision::NoAction));
}

// ── cgroup_safety_margin tests ──
// These pin the contract that `cgroup_safety_margin` shifts BOTH kill
// branches down by the margin so userland preempt fires before the
// kernel's cgroup-OOM. Pre-fix the active-kill threshold was exactly
// the cgroup cap and the framework consistently lost the race against
// the kernel SIGKILL — see the bug-report context for the production
// repro that drove this change.

#[test]
fn pressure_kill_fires_before_cgroup_oom() {
    // Layout: cgroup cap 100, margin 10, pressure_threshold 5.
    //   effective_max = 100 − 10 = 90
    //   opp-kill threshold = effective_max − threshold = 85
    // With usage 92 (between 85 and the legacy `max − threshold = 95`)
    // the pre-fix scheduler would have stayed quiet; the post-fix
    // scheduler must fire the opportunistic-kill branch.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 10,
        pressure_threshold: 5,
        ..sched()
    };
    let workers = vec![
        WorkerBudgetInfo {
            worker_id: 0,
            reserved_budgets: mem(50),
            actual_usage: mem(46),
            is_idle: false,
            is_opportunistic: false,
            has_initial_assignment: true,
            current_task: Some(make_binary("a", 10)),
            estimated_usage: mem(46),
        },
        WorkerBudgetInfo {
            worker_id: 1,
            reserved_budgets: mem(50),
            actual_usage: mem(46),
            is_idle: false,
            is_opportunistic: true,
            has_initial_assignment: true,
            current_task: Some(make_binary("b", 10)),
            estimated_usage: mem(46),
        },
    ];
    // Sum of actual_usage = 92.
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 1),
        _ => panic!("expected Kill of opportunistic worker, got {decision:?}"),
    }
}

#[test]
fn active_kill_fires_at_margin_not_at_cgroup_cap() {
    // Layout: cgroup cap 100, margin 10, no opportunistic workers.
    //   effective_max = 90
    // With usage 95 (above effective_max but below the cgroup cap):
    //   pre-fix: `usage > max` is `95 > 100` → false → NoAction
    //            (and the kernel cgroup-OOM eventually wins)
    //   post-fix: `usage > effective_max` is `95 > 90` → fires
    //             smallest-active kill so userland gets there first.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 10,
        pressure_threshold: 5,
        ..sched()
    };
    let workers = vec![WorkerBudgetInfo {
        worker_id: 7,
        reserved_budgets: mem(100),
        actual_usage: mem(95),
        is_idle: false,
        is_opportunistic: false,
        has_initial_assignment: true,
        current_task: Some(make_binary("a", 10)),
        estimated_usage: mem(95),
    }];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, .. } => assert_eq!(worker_id, 7),
        _ => panic!("expected Kill of smallest active worker, got {decision:?}"),
    }
}

#[test]
fn safety_margin_zero_restores_pre_fix_behavior() {
    // Regression pin: setting `cgroup_safety_margin = 0` is the
    // documented escape hatch ("preempt only AT cgroup cap, races
    // kernel-OOM"). Same workers/usage as
    // `active_kill_fires_at_margin_not_at_cgroup_cap` but with the
    // margin disabled must return NoAction. If a future default-zero
    // mistake re-emerges this test catches it. Runs under
    // `in_pressure_phase=true` to exercise the safety-margin branch
    // — the in_pressure_phase gate itself is pinned separately by
    // `check_pressure_no_action_outside_pressure_phase_*`.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        pressure_threshold: 5,
        ..sched()
    };
    let workers = vec![WorkerBudgetInfo {
        worker_id: 7,
        reserved_budgets: mem(100),
        actual_usage: mem(95),
        is_idle: false,
        is_opportunistic: false,
        has_initial_assignment: true,
        current_task: Some(make_binary("a", 10)),
        estimated_usage: mem(95),
    }];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
    assert!(
        matches!(decision, ResourcePressureDecision::NoAction),
        "expected NoAction with margin=0 and usage<cap, got {decision:?}"
    );
}

#[test]
fn memory_constructor_default_margin_is_1gib() {
    // Pins the production default so a refactor cannot silently
    // change the headroom band that ships to operators.
    let s = ResourceStealingScheduler::memory();
    assert_eq!(s.cgroup_safety_margin, 1024 * 1024 * 1024);
}

// ── Multi-idle worker temp_factor tests ──

// ── KillReason classification tests ──
//
// Pin the four-way discriminator at the decision site. Each scenario
// builds a synthetic worker set, drives `check_resource_pressure`
// once, and asserts the `KillReason` carried by the returned `Kill`
// variant. Margin is set to zero so the proxy-byte numbers don't
// saturate `effective_max` (same convention as the legacy pressure
// tests above).

use dynrunner_scheduler_api::KillReason;

fn worker_active(
    id: WorkerId,
    reserved: u64,
    actual: u64,
    opportunistic: bool,
) -> WorkerBudgetInfo<TestId> {
    WorkerBudgetInfo {
        worker_id: id,
        reserved_budgets: mem(reserved),
        actual_usage: mem(actual),
        is_idle: false,
        is_opportunistic: opportunistic,
        has_initial_assignment: true,
        current_task: Some(make_binary("t", 10)),
        estimated_usage: mem(actual),
    }
}

#[test]
fn kill_reason_no_fault_memory_stealing_when_opportunistic_picked() {
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
    let workers = vec![
        // Non-opportunistic worker under its budget — present so the
        // opportunistic-branch threshold is crossed.
        worker_active(0, 500, 400, false),
        // Opportunistic victim — median of one is itself.
        worker_active(1, 500, 400, true),
    ];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(1000), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, reason } => {
            assert_eq!(worker_id, 1);
            assert_eq!(reason, KillReason::NoFaultMemoryStealing);
        }
        _ => panic!("expected Kill(NoFaultMemoryStealing), got {decision:?}"),
    }
}

#[test]
fn kill_reason_no_fault_under_budget_when_smallest_active_is_below_reserved() {
    // Two non-opportunistic workers. Worker 1 is the smallest active
    // by estimated_usage but its actual_usage is below its reserved
    // budget — another worker drove the cgroup into pressure, the
    // victim is innocent. Note: actual_usage sum (1200 > 1000)
    // crosses effective_max so the smallest-active branch fires.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        pressure_threshold: 5,
        ..sched()
    };
    let workers = vec![
        worker_active(0, 500, 800, false),
        // Reserved 500, actual 400, estimated 100 (smallest).
        WorkerBudgetInfo {
            worker_id: 1,
            reserved_budgets: mem(500),
            actual_usage: mem(400),
            is_idle: false,
            is_opportunistic: false,
            has_initial_assignment: true,
            current_task: Some(make_binary("b", 10)),
            estimated_usage: mem(100),
        },
    ];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(1000), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, reason } => {
            assert_eq!(worker_id, 1);
            assert_eq!(reason, KillReason::NoFaultUnderBudget);
        }
        _ => panic!("expected Kill(NoFaultUnderBudget), got {decision:?}"),
    }
}

#[test]
fn kill_reason_oom_over_budget_when_smallest_active_is_over_reserved() {
    // Two non-opportunistic workers, both over their reserved budget.
    // The smallest-active (by estimated_usage) is the victim and its
    // actual_usage >= reserved → OomOverBudget.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
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
            actual_usage: mem(550),
            is_idle: false,
            is_opportunistic: false,
            has_initial_assignment: true,
            current_task: Some(make_binary("b", 10)),
            estimated_usage: mem(300),
        },
    ];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(1000), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, reason } => {
            assert_eq!(worker_id, 1);
            assert_eq!(reason, KillReason::OomOverBudget);
        }
        _ => panic!("expected Kill(OomOverBudget), got {decision:?}"),
    }
}

#[test]
fn kill_reason_oom_last_resort_when_single_active_over_budget() {
    // Single non-opportunistic active worker, over its reserved
    // budget, no alternative candidate exists → OomLastResort.
    let s = ResourceStealingScheduler {
        cgroup_safety_margin: 0,
        ..sched()
    };
    let workers = vec![WorkerBudgetInfo {
        worker_id: 7,
        reserved_budgets: mem(100),
        actual_usage: mem(200),
        is_idle: false,
        is_opportunistic: false,
        has_initial_assignment: true,
        current_task: Some(make_binary("solo", 10)),
        estimated_usage: mem(200),
    }];
    let decision = Scheduler::<TestId>::check_resource_pressure(&s, &workers, &mem(100), true);
    match decision {
        ResourcePressureDecision::Kill { worker_id, reason } => {
            assert_eq!(worker_id, 7);
            assert_eq!(reason, KillReason::OomLastResort);
        }
        _ => panic!("expected Kill(OomLastResort), got {decision:?}"),
    }
}

#[test]
fn assign_normal_temp_factor_ordering() {
    let s = sched();
    let workers = vec![
        make_worker(0, 100, true, true),
        make_worker(1, 200, true, true),
        make_worker(2, 300, true, true),
    ];
    let binaries = vec![make_binary("a", 50)];

    let d0 = s.assign_normal(
        &workers[0],
        &workers,
        &refs(&binaries),
        &mem(600),
        &LinearEstimator,
        false,
    );
    assert!(matches!(d0, AssignmentDecision::Assign { .. }));

    let d1 = s.assign_normal(
        &workers[1],
        &workers,
        &refs(&binaries),
        &mem(600),
        &LinearEstimator,
        false,
    );
    assert!(matches!(d1, AssignmentDecision::Assign { .. }));

    let d2 = s.assign_normal(
        &workers[2],
        &workers,
        &refs(&binaries),
        &mem(600),
        &LinearEstimator,
        false,
    );
    assert!(matches!(d2, AssignmentDecision::Assign { .. }));
}
