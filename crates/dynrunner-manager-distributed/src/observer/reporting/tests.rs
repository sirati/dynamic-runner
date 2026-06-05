//! Deterministic tests for the observer reporter.
//!
//! Two layers:
//!   * Pure layer (`stats` / `format` / `idle`): seed a `ClusterState`
//!     (for `stats`) or hand-built `StatsSnapshot`s (for `format` /
//!     `idle`) and assert the computation + formatting rules — no clock.
//!   * Cadence layer (`run`): `tokio::time::pause` + `advance` so the
//!     10-min stats tick, the 1-min idle tick, and the idle threshold
//!     all fire at known virtual instants with zero wall-clock race.

use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, PhaseId, TaskDep, TaskInfo, TypeId, WorkerId};
use dynrunner_protocol_primary_secondary::ClusterMutation;

use crate::ClusterState;

use super::format::render_report;
use super::idle::IdleDetector;
use super::reporter::{IDLE_THRESHOLD, SharedSnapshotSource};
use super::stats::StatsSnapshot;
use super::stats::StatsSnapshot as Snap;

// ── fixture helpers ──

fn task(task_id: &str, phase: &str, deps: &[&str]) -> TaskInfo<()> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{task_id}")),
        size: 1,
        identifier: (),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("T"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: task_id.to_string(),
        task_depends_on: deps
            .iter()
            .map(|d| TaskDep {
                task_id: d.to_string(),
                phase_id: PhaseId::from(phase),
                inherit_outputs: false,
            })
            .collect(),
        preferred_secondaries: Default::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// Seed a `ClusterState` purely through its public wire-mutation
/// `apply` API — the same path live broadcasts take — so the tests
/// exercise the real apply rules (e.g. `TaskBlocked` → `TaskState::Blocked`)
/// rather than reaching into private fields. Each entry lands as
/// `Pending` via `TaskAdded`, then the per-`Seed` mutation transitions
/// it. The hash assigned to entry `i` is `hash-i`; `Seed::Blocked`'s
/// `on` names the prereq entry's hash.
fn seed_state(
    phase_deps: &[(&str, &[&str])],
    entries: &[(TaskInfo<()>, Seed)],
) -> ClusterState<()> {
    let mut s = ClusterState::<()>::new();
    // Phase dep graph.
    let mut deps = std::collections::HashMap::new();
    for (child, parents) in phase_deps {
        deps.insert(
            PhaseId::from(*child),
            parents.iter().map(|p| PhaseId::from(*p)).collect(),
        );
    }
    s.apply(ClusterMutation::PhaseDepsSet { deps });
    for (i, (t, seed)) in entries.iter().enumerate() {
        let hash = format!("hash-{i}");
        s.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: t.clone(),
        });
        match seed {
            Seed::Pending => {}
            Seed::InFlight { secondary, worker } => {
                s.apply(ClusterMutation::TaskAssigned {
                    hash: hash.clone(),
                    secondary: secondary.to_string(),
                    worker: *worker,

                    version: Default::default(),
                });
            }
            Seed::Completed => {
                s.apply(ClusterMutation::TaskCompleted {
                    hash: hash.clone(),
                    result_data: None,
                });
            }
            Seed::Failed(kind) => {
                s.apply(ClusterMutation::TaskFailed {
                    hash: hash.clone(),
                    kind: kind.clone(),
                    error: "x".to_string(),

                    version: Default::default(),
                });
            }
            Seed::Blocked { on } => {
                s.apply(ClusterMutation::TaskBlocked {
                    hash: hash.clone(),
                    on: on.to_string(),
                });
            }
        }
    }
    s
}

enum Seed {
    Pending,
    InFlight {
        secondary: &'static str,
        worker: WorkerId,
    },
    Completed,
    Failed(ErrorType),
    /// Cascade-paused on the prereq whose hash is `on`.
    Blocked {
        on: &'static str,
    },
}

// ── stats: CRDT-derived computation ──

#[test]
fn stats_counts_each_bucket() {
    let s = seed_state(
        &[],
        &[
            (task("c1", "P", &[]), Seed::Completed),
            (task("c2", "P", &[]), Seed::Completed),
            (task("r", "P", &[]), Seed::Failed(ErrorType::Recoverable)),
            (
                task("o", "P", &[]),
                Seed::Failed(ErrorType::ResourceExhausted(
                    dynrunner_core::ResourceKind::memory(),
                )),
            ),
            (
                task("nf", "P", &[]),
                Seed::Failed(ErrorType::NonRecoverable),
            ),
            (
                task("if1", "P", &[]),
                Seed::InFlight {
                    secondary: "sec-a",
                    worker: 0,
                },
            ),
        ],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.succeeded, 2);
    assert_eq!(snap.fail_retry, 1);
    assert_eq!(snap.fail_oom, 1);
    assert_eq!(snap.fail_final, 1);
    assert_eq!(snap.in_flight, 1);
    assert_eq!(snap.per_secondary_in_flight.get("sec-a"), Some(&1));
}

#[test]
fn stats_unfulfillable_reads_counts_not_outcome() {
    // TRAP: outcome_counts() folds Unfulfillable into fail_final. The
    // snapshot must read counts().unfulfillable for its own line and
    // NOT double it into fail_final.
    let s = seed_state(
        &[],
        &[(
            task("u", "P", &[]),
            Seed::Failed(ErrorType::Unfulfillable {
                reason: "no resource".to_string().into(),
            }),
        )],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.unfulfillable, 1, "discrete Unfulfillable counted");
    assert_eq!(
        snap.fail_final, 0,
        "Unfulfillable must NOT also land in fail_final"
    );
}

#[test]
fn stats_invalid_task_reads_counts_not_outcome() {
    // TRAP (sibling of `stats_unfulfillable_reads_counts_not_outcome`):
    // outcome_counts() folds the discrete TaskState::InvalidTask into
    // fail_final. The snapshot must read counts().invalid_task for its
    // own line and net it OUT of fail_final so the two lines are
    // disjoint — a single invalid task is one metric, not two.
    let s = seed_state(
        &[],
        &[(
            task("inv", "P", &[]),
            Seed::Failed(ErrorType::InvalidTask {
                reason: "missing dep nope".to_string().into(),
            }),
        )],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.invalid_task, 1, "discrete InvalidTask counted");
    assert_eq!(
        snap.fail_final, 0,
        "InvalidTask must NOT also land in fail_final"
    );
}

#[test]
fn stats_invalid_task_unfulfillable_and_final_are_pairwise_disjoint() {
    // All three terminal-failure lines coexist on one ledger; assert
    // each tallies exactly its own and none cross-contaminates. This
    // pins the double-netting in `from_cluster_state` (subtract BOTH
    // unfulfillable AND invalid_task out of the folded fail_final).
    let s = seed_state(
        &[],
        &[
            (
                task("nf", "P", &[]),
                Seed::Failed(ErrorType::NonRecoverable),
            ),
            (
                task("u", "P", &[]),
                Seed::Failed(ErrorType::Unfulfillable {
                    reason: "no resource".to_string().into(),
                }),
            ),
            (
                task("inv", "P", &[]),
                Seed::Failed(ErrorType::InvalidTask {
                    reason: "dup id".to_string().into(),
                }),
            ),
        ],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.fail_final, 1, "only the NonRecoverable task");
    assert_eq!(snap.unfulfillable, 1, "only the Unfulfillable task");
    assert_eq!(snap.invalid_task, 1, "only the InvalidTask task");
}

#[test]
fn stats_blocked_is_separate_from_waiting_on_deps() {
    // `up` is Unfulfillable; `dep_blocked` is its dependent and cascades
    // to TaskState::Blocked (a separate category). `waiter` is a Pending
    // task whose prereq `pending_prereq` is not yet terminal → waiting
    // on deps. `ready` has a satisfied dep (its prereq completed).
    let s = seed_state(
        &[],
        &[
            // entry 0 (hash-0): `up`, Unfulfillable prereq.
            (
                task("up", "P", &[]),
                Seed::Failed(ErrorType::Unfulfillable {
                    reason: "r".to_string().into(),
                }),
            ),
            // entry 1: `dep_blocked` cascade-paused on hash-0 → Blocked.
            (
                task("dep_blocked", "P", &["up"]),
                Seed::Blocked { on: "hash-0" },
            ),
            // entry 2: `pending_prereq`, no deps → ready in active phase.
            (task("pending_prereq", "P", &[]), Seed::Pending),
            // entry 3: `waiter` waits on the still-Pending prereq.
            (task("waiter", "P", &["pending_prereq"]), Seed::Pending),
            // entry 4: `done_prereq`, Completed (satisfies `ready`).
            (task("done_prereq", "P", &[]), Seed::Completed),
            // entry 5: `ready`, dep satisfied + active phase → ready.
            (task("ready", "P", &["done_prereq"]), Seed::Pending),
        ],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.blocked, 1, "cascade-blocked dependent");
    // `pending_prereq` (no deps, active phase) is ready; `waiter` waits.
    assert_eq!(snap.waiting_on_deps, 1, "waiter only");
    // ready set: pending_prereq + ready (deps satisfied, active phase).
    assert_eq!(snap.ready_in_queue, 2);
}

#[test]
fn stats_ready_excludes_blocked_phase() {
    // Phase B depends on A; A still has a live (Pending) task, so B is
    // NOT dispatchable and its Pending task is neither ready nor
    // waiting-on-deps (it's phase-gated).
    let s = seed_state(
        &[("B", &["A"])],
        &[
            (task("a", "A", &[]), Seed::Pending),
            (task("b", "B", &[]), Seed::Pending),
        ],
    );
    let snap = Snap::from_cluster_state(&s);
    // `a` is in active phase A (no deps) → ready. `b` is phase-gated.
    assert_eq!(snap.ready_in_queue, 1);
    assert_eq!(snap.waiting_on_deps, 0);
}

#[test]
fn stats_ready_includes_phase_after_upstream_terminates() {
    // Same graph, but A's task is Completed → B becomes dispatchable.
    let s = seed_state(
        &[("B", &["A"])],
        &[
            (task("a", "A", &[]), Seed::Completed),
            (task("b", "B", &[]), Seed::Pending),
        ],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.ready_in_queue, 1, "b is now ready");
}

/// Apply a `SecondaryCapacity` record for each `(secondary, worker_count)`
/// pair — the same wire mutation `primary/connect.rs` originates at the
/// `SecondaryWelcome` accept. The occupancy DENOMINATORS
/// (`total_secondaries` = `known_secondaries().count()`, `total_workers`
/// = `total_worker_count()`) read exactly this replicated map, so the
/// tests seed it through the public apply path rather than reaching into
/// private fields. `resources` are irrelevant to occupancy → empty.
fn with_capacities(mut s: ClusterState<()>, caps: &[(&str, u32)]) -> ClusterState<()> {
    for (secondary, worker_count) in caps {
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: secondary.to_string(),
            worker_count: *worker_count,
            resources: vec![],
        });
    }
    s
}

// ── stats: occupancy (D1 capacity × D2 InFlight, CRDT-derived) ──

#[test]
fn stats_occupancy_busy_and_total_secondaries() {
    // 3 secondaries declared (total). InFlight on sec-a and sec-b only;
    // sec-c idle → busy = 2. (sec-a runs two tasks but counts ONCE as a
    // busy secondary.)
    let s = with_capacities(
        seed_state(
            &[],
            &[
                (
                    task("t0", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-a",
                        worker: 0,
                    },
                ),
                (
                    task("t1", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-a",
                        worker: 1,
                    },
                ),
                (
                    task("t2", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-b",
                        worker: 0,
                    },
                ),
            ],
        ),
        &[("sec-a", 4), ("sec-b", 4), ("sec-c", 2)],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.busy_secondaries, 2, "sec-a + sec-b, sec-c idle");
    assert_eq!(snap.total_secondaries, 3, "all three known");
}

#[test]
fn stats_occupancy_busy_workers_is_distinct_secondary_worker_pairs() {
    // sec-a runs two tasks on distinct workers 0 and 1; sec-b runs one
    // on worker 0. Distinct (secondary, worker) slots = (a,0),(a,1),(b,0)
    // = 3 busy workers. Total = 4 + 4 + 2 = 10.
    let s = with_capacities(
        seed_state(
            &[],
            &[
                (
                    task("t0", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-a",
                        worker: 0,
                    },
                ),
                (
                    task("t1", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-a",
                        worker: 1,
                    },
                ),
                (
                    task("t2", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-b",
                        worker: 0,
                    },
                ),
            ],
        ),
        &[("sec-a", 4), ("sec-b", 4), ("sec-c", 2)],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.busy_workers, 3, "distinct (secondary,worker) slots");
    assert_eq!(snap.total_workers, 10, "4 + 4 + 2 advertised slots");
}

#[test]
fn stats_occupancy_worker_id_distinct_across_secondaries() {
    // The SAME local worker id (0) on TWO secondaries is TWO distinct
    // slots — busy_workers keys on the (secondary, worker) PAIR, not the
    // bare worker id, so they must not collapse to one.
    let s = with_capacities(
        seed_state(
            &[],
            &[
                (
                    task("t0", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-a",
                        worker: 0,
                    },
                ),
                (
                    task("t1", "P", &[]),
                    Seed::InFlight {
                        secondary: "sec-b",
                        worker: 0,
                    },
                ),
            ],
        ),
        &[("sec-a", 1), ("sec-b", 1)],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.busy_workers, 2, "(a,0) and (b,0) are distinct slots");
    assert_eq!(snap.busy_secondaries, 2);
    assert_eq!(snap.total_workers, 2);
    assert_eq!(snap.total_secondaries, 2);
}

#[test]
fn stats_occupancy_zero_numerators_when_no_in_flight() {
    // Capacity declared but nothing executing → numerators 0,
    // denominators still reflect the roster.
    let s = with_capacities(
        seed_state(&[], &[(task("p", "P", &[]), Seed::Pending)]),
        &[("sec-a", 4), ("sec-b", 2)],
    );
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.busy_secondaries, 0);
    assert_eq!(snap.busy_workers, 0);
    assert_eq!(snap.total_secondaries, 2);
    assert_eq!(snap.total_workers, 6);
}

// ── format: delta + inclusion rule ──

fn snap_with(succeeded: usize, in_flight: usize) -> StatsSnapshot {
    StatsSnapshot {
        succeeded,
        in_flight,
        ..Default::default()
    }
}

#[test]
fn format_renders_total_and_delta_for_changed_nonzero() {
    let prev = snap_with(2, 0);
    let cur = snap_with(5, 0);
    let body = render_report(&cur, &prev).expect("nonzero change reports");
    assert!(body.contains("succeeded: 5(+3)"), "got: {body}");
}

#[test]
fn format_omits_zero_metrics_silently() {
    // succeeded 0→0 (zero), in_flight 0→3 (changed nonzero).
    let prev = StatsSnapshot::default();
    let cur = snap_with(0, 3);
    let body = render_report(&cur, &prev).expect("in-flight change reports");
    assert!(body.contains("in-flight: 3"), "got: {body}");
    assert!(!body.contains("succeeded"), "zero metric omitted");
    // No metric was omitted *because unchanged* → no footer.
    assert!(!body.contains("Omitted unchanged stats."), "got: {body}");
}

#[test]
fn format_appends_footer_when_a_nonzero_metric_is_unchanged() {
    // succeeded 4→4 (nonzero, unchanged → omitted-because-unchanged),
    // in_flight 0→2 (changed → included). Footer expected.
    let prev = snap_with(4, 0);
    let cur = snap_with(4, 2);
    let body = render_report(&cur, &prev).expect("in-flight change reports");
    assert!(body.contains("in-flight: 2"), "got: {body}");
    assert!(!body.contains("succeeded"), "unchanged succeeded omitted");
    assert!(
        body.trim_end().ends_with("Omitted unchanged stats."),
        "footer expected, got: {body}"
    );
}

#[test]
fn format_silent_when_nothing_changed_even_if_nonzero() {
    // Everything nonzero but unchanged → nothing included → no report
    // (a footer-only report is never wake-worthy).
    let prev = snap_with(7, 3);
    let cur = snap_with(7, 3);
    assert!(render_report(&cur, &prev).is_none());
}

#[test]
fn format_silent_on_all_zero() {
    let prev = StatsSnapshot::default();
    let cur = StatsSnapshot::default();
    assert!(render_report(&cur, &prev).is_none());
}

// ── format: occupancy ratio inclusion rule ──

fn occupancy_snap(
    busy_secondaries: usize,
    total_secondaries: usize,
    busy_workers: usize,
    total_workers: usize,
) -> StatsSnapshot {
    StatsSnapshot {
        busy_secondaries,
        total_secondaries,
        busy_workers,
        total_workers,
        ..Default::default()
    }
}

#[test]
fn format_occupancy_renders_busy_over_total() {
    // Both numerators nonzero and changed (0→2 secondaries, 0→3 workers).
    let prev = StatsSnapshot::default();
    let cur = occupancy_snap(2, 3, 3, 10);
    let body = render_report(&cur, &prev).expect("occupancy change reports");
    assert!(body.contains("busy secondaries: 2/3"), "got: {body}");
    assert!(body.contains("busy workers: 3/10"), "got: {body}");
}

#[test]
fn format_occupancy_omitted_when_numerator_zero() {
    // busy=0 → numerator-not-present → omitted as zero (NOT unchanged),
    // even though the denominator (total) is nonzero. No footer.
    let prev = StatsSnapshot::default();
    let cur = occupancy_snap(0, 3, 0, 10);
    // Nothing else changed → whole report silent.
    assert!(
        render_report(&cur, &prev).is_none(),
        "zero-numerator occupancy is not wake-worthy"
    );
}

#[test]
fn format_occupancy_omitted_unchanged_appends_footer() {
    // Occupancy nonzero but identical to last announcement → omitted-
    // because-unchanged → footer; an unrelated changed metric carries
    // the report so it is not silent.
    let prev = occupancy_snap(2, 3, 3, 10);
    let mut cur = occupancy_snap(2, 3, 3, 10);
    cur.succeeded = 5; // changed nonzero so SOMETHING is included.
    let body = render_report(&cur, &prev).expect("succeeded change reports");
    assert!(body.contains("succeeded: 5(+5)"), "got: {body}");
    assert!(
        !body.contains("busy secondaries"),
        "unchanged occupancy omitted"
    );
    assert!(
        !body.contains("busy workers"),
        "unchanged occupancy omitted"
    );
    assert!(
        body.trim_end().ends_with("Omitted unchanged stats."),
        "footer expected, got: {body}"
    );
}

#[test]
fn format_occupancy_changed_when_only_total_changes() {
    // busy unchanged (2→2) but total grew (3→4, a new secondary joined)
    // → "changed" per the ratio rule (busy OR total). Included.
    let prev = occupancy_snap(2, 3, 5, 12);
    let cur = occupancy_snap(2, 4, 5, 12);
    let body = render_report(&cur, &prev).expect("total change reports");
    assert!(body.contains("busy secondaries: 2/4"), "got: {body}");
    // The worker ratio is genuinely unchanged → omitted, footer appears.
    assert!(
        !body.contains("busy workers"),
        "unchanged worker ratio omitted"
    );
    assert!(
        body.trim_end().ends_with("Omitted unchanged stats."),
        "footer expected, got: {body}"
    );
}

#[test]
fn format_occupancy_changed_when_only_busy_changes() {
    // total unchanged but busy moved (2→3 secondaries) → included.
    let prev = occupancy_snap(2, 4, 5, 12);
    let cur = occupancy_snap(3, 4, 5, 12);
    let body = render_report(&cur, &prev).expect("busy change reports");
    assert!(body.contains("busy secondaries: 3/4"), "got: {body}");
}

// ── idle detector ──

fn in_flight_snap(pairs: &[(&str, usize)], ready: usize) -> StatsSnapshot {
    StatsSnapshot {
        ready_in_queue: ready,
        per_secondary_in_flight: pairs.iter().map(|(s, n)| (s.to_string(), *n)).collect(),
        ..Default::default()
    }
}

#[test]
fn idle_fires_once_after_threshold_with_ready_work() {
    let mut det = IdleDetector::new(IDLE_THRESHOLD);
    let t0 = Instant::now();
    // sec-a busy, sec-b idle; ready work present. First tick observes
    // both, stamps sec-b idle. No fire yet (threshold not elapsed).
    let snap = in_flight_snap(&[("sec-a", 2)], 5);
    // sec-b is known only once it has been seen in-flight; seed it by
    // first observing it busy, then idle.
    let busy = in_flight_snap(&[("sec-a", 2), ("sec-b", 1)], 5);
    assert!(det.tick(&busy, t0).is_empty());
    // sec-b goes idle now.
    assert!(det.tick(&snap, t0 + Duration::from_secs(1)).is_empty());
    // Still under threshold.
    assert!(
        det.tick(&snap, t0 + Duration::from_secs(30)).is_empty(),
        "not yet 1 minute idle"
    );
    // Threshold elapsed → fires once for sec-b.
    let fired = det.tick(&snap, t0 + Duration::from_secs(62));
    assert_eq!(fired, vec!["sec-b".to_string()]);
    // Does NOT repeat.
    assert!(
        det.tick(&snap, t0 + Duration::from_secs(120)).is_empty(),
        "one-shot per idle spell"
    );
}

#[test]
fn idle_does_not_fire_without_ready_work() {
    let mut det = IdleDetector::new(IDLE_THRESHOLD);
    let t0 = Instant::now();
    // sec-b observed busy then idle, but NO ready work → never fires.
    let busy = in_flight_snap(&[("sec-b", 1)], 0);
    let idle = in_flight_snap(&[], 0);
    assert!(det.tick(&busy, t0).is_empty());
    assert!(det.tick(&idle, t0 + Duration::from_secs(1)).is_empty());
    assert!(det.tick(&idle, t0 + Duration::from_secs(120)).is_empty());
}

#[test]
fn idle_rearms_after_secondary_receives_task() {
    let mut det = IdleDetector::new(IDLE_THRESHOLD);
    let t0 = Instant::now();
    let busy = in_flight_snap(&[("sec-b", 1)], 5);
    let idle = in_flight_snap(&[], 5);
    assert!(det.tick(&busy, t0).is_empty());
    assert!(det.tick(&idle, t0 + Duration::from_secs(1)).is_empty());
    // Fires once at +62s.
    assert_eq!(
        det.tick(&idle, t0 + Duration::from_secs(62)),
        vec!["sec-b".to_string()]
    );
    // Receives a task → gate clears + re-arms.
    assert!(det.tick(&busy, t0 + Duration::from_secs(70)).is_empty());
    // Goes idle again; a fresh spell can alert after a fresh threshold.
    assert!(det.tick(&idle, t0 + Duration::from_secs(71)).is_empty());
    let fired = det.tick(&idle, t0 + Duration::from_secs(140));
    assert_eq!(fired, vec!["sec-b".to_string()], "re-armed spell alerts");
}

// ── shared snapshot source ──

#[test]
fn shared_source_publishes_latest() {
    use super::reporter::CrdtSnapshotSource;
    let src = SharedSnapshotSource::new(StatsSnapshot::default());
    assert_eq!(src.snapshot(), StatsSnapshot::default());
    src.publish(snap_with(9, 0));
    assert_eq!(src.snapshot().succeeded, 9);
    // A clone shares the same cell.
    let clone = src.clone();
    src.publish(snap_with(11, 0));
    assert_eq!(clone.snapshot().succeeded, 11);
}

// ── live-feed wiring: CRDT projection → publish → reporter observes ──

#[test]
fn live_feed_publishes_real_crdt_projection_to_reporter() {
    // This is the production hand-off the observer run loop performs:
    // project the live `ClusterState` into a `StatsSnapshot` and
    // `publish` it into the shared cell the reporter task reads on its
    // next tick. The reporter is seeded all-zero (a fresh observer); the
    // first publish must make the reporter observe the REAL counts so
    // its cadence has wake-worthy data — not the seeded zero snapshot.
    use super::reporter::CrdtSnapshotSource;

    // Seed a cluster with a spread of states — the same view a late-
    // joiner observer restores from a running cluster.
    let state = seed_state(
        &[],
        &[
            (task("c1", "P", &[]), Seed::Completed),
            (task("c2", "P", &[]), Seed::Completed),
            (task("r", "P", &[]), Seed::Failed(ErrorType::Recoverable)),
            (
                task("if1", "P", &[]),
                Seed::InFlight {
                    secondary: "sec-a",
                    worker: 0,
                },
            ),
            (task("ready1", "P", &[]), Seed::Pending),
        ],
    );

    // The reporter starts on the seeded default — every metric zero, so
    // a tick at this instant would correctly stay silent.
    let source = SharedSnapshotSource::new(StatsSnapshot::default());
    assert_eq!(source.snapshot(), StatsSnapshot::default());

    // Drive ONE publish through the seam, exactly as the run loop does
    // after `restore_from_snapshot_and_skip_setup`: project the live
    // CRDT and publish it. A second handle (the reporter's clone) must
    // now observe the real, NON-zero projection.
    let reporter_view = source.clone();
    source.publish(StatsSnapshot::from_cluster_state(&state));

    let observed = reporter_view.snapshot();
    assert_ne!(
        observed,
        StatsSnapshot::default(),
        "reporter must observe a NON-zero snapshot after the live publish"
    );
    // The observed counts must reflect the real CRDT, not a placeholder.
    assert_eq!(observed.succeeded, 2);
    assert_eq!(observed.fail_retry, 1);
    assert_eq!(observed.in_flight, 1);
    assert_eq!(observed.per_secondary_in_flight.get("sec-a"), Some(&1));
    assert_eq!(observed.ready_in_queue, 1);
    // And it equals the direct projection — the cell is a faithful
    // pass-through, no lossy copy.
    assert_eq!(observed, StatsSnapshot::from_cluster_state(&state));
}

// ── cadence driver (paused virtual clock) ──

/// A clock that reads the paused `tokio::time` virtual instant. Under
/// `start_paused` + `advance`, `tokio::time::Instant::now()` jumps
/// deterministically, so the idle threshold elapses at a known virtual
/// time with no wall-clock race.
struct VirtualClock;
impl super::reporter::Clock for VirtualClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

#[tokio::test(start_paused = true)]
async fn driver_emits_stats_on_10min_cadence_and_idle_on_threshold() {
    // The driver pulls a fresh snapshot from the source each tick. We
    // script the source to report a changed-nonzero succeeded count and
    // a persistently-idle known secondary with ready work, then advance
    // the virtual clock past both cadences and confirm the driver runs
    // to completion (no hang) when cancelled. The emission itself goes
    // to the tracing sink; this test pins the cadence/cancel wiring
    // deterministically (the per-rule emission content is covered by
    // the `format` + `idle` unit tests above).
    let src = SharedSnapshotSource::new(in_flight_snap(&[("sec-b", 1)], 5));
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let driver = tokio::spawn({
        let src = src.clone();
        async move {
            super::reporter::run_reporter(src, VirtualClock, async move {
                let _ = cancel_rx.await;
            })
            .await;
        }
    });
    // First observe sec-b busy (handled in the seeded snapshot), then
    // flip it idle so a spell starts.
    src.publish(in_flight_snap(&[], 5));
    // Advance past the idle threshold AND a stats period; yield so the
    // driver's interval arms fire.
    tokio::time::advance(Duration::from_secs(700)).await;
    tokio::task::yield_now().await;
    // Cancel and confirm the driver terminates (no deadlock).
    let _ = cancel_tx.send(());
    tokio::time::advance(Duration::from_millis(1)).await;
    let joined = tokio::time::timeout(Duration::from_secs(1), driver).await;
    assert!(joined.is_ok(), "driver must terminate on cancel");
}
