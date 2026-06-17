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

use super::format::{ResourceBaseline, render_report as render_report_full};
use super::idle::IdleDetector;
use super::reporter::{IDLE_THRESHOLD, SharedSnapshotSource};
use super::stats::StatsSnapshot;
use super::stats::StatsSnapshot as Snap;

/// Pre-#575 `Option<String>` test seam — wraps the new
/// [`render_report_full`] entry with an all-`None` resource baseline.
/// The legacy tests below NEVER populate the snapshot's `avg_*` fields,
/// so every resource line trips `present() == false` and the body is
/// IDENTICAL to the pre-#575 output. #575-specific tests call the full
/// surface directly so they can read the per-field baseline back.
fn render_report(cur: &StatsSnapshot, prev: &StatsSnapshot) -> Option<String> {
    render_report_full(cur, prev, &ResourceBaseline::default()).body
}

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
                def_id: None,
            })
            .collect(),
        preferred_secondaries: Default::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
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
            def_id: None,
        });
        match seed {
            Seed::Pending => {}
            Seed::InFlight { secondary, worker } => {
                s.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: hash.clone(),
                    secondary: secondary.to_string(),
                    worker: *worker,
                    version: Default::default(),
                });
            }
            Seed::Completed => {
                s.apply(ClusterMutation::TaskCompleted {
                    attempt: 0,
                    hash: hash.clone(),
                    result_data: None,
                });
            }
            Seed::Failed(kind) => {
                s.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
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

/// Apply a `PeerJoined` (so the secondary is `is_peer_alive`) + a
/// `SecondaryCapacity` record for each `(secondary, worker_count)` pair —
/// the same wire mutations `primary/connect.rs` originates at the
/// `SecondaryWelcome` accept. The occupancy DENOMINATORS
/// (`total_secondaries` = `alive_secondary_members().count()`,
/// `total_workers` = `alive_worker_count()`) read this replicated map
/// FILTERED to live members, so the tests must seed BOTH the membership and
/// the capacity through the public apply path (a capacity-only secondary is
/// a departed/never-alive ghost the live denominator excludes — see
/// `stats_occupancy_excludes_departed_secondary`). `resources` are
/// irrelevant to occupancy → empty.
fn with_capacities(mut s: ClusterState<()>, caps: &[(&str, u32)]) -> ClusterState<()> {
    for (secondary, worker_count) in caps {
        s.apply(ClusterMutation::PeerJoined {
            peer_id: secondary.to_string(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
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

/// RCA disco-prune edge B: a DEPARTED secondary's set-once
/// `SecondaryCapacity` record lingers in `secondary_capacities` after its
/// membership flips `Dead` (capacity is never deleted, so a re-admission can
/// restore it). The occupancy DENOMINATORS must EXCLUDE it — counting a gone
/// secondary's workers/slots inflated "X/Y busy" with a ghost. Pre-fix
/// (`known_secondaries().count()` / `total_worker_count()`) this read 2/6;
/// the live-filtered denominators read 1/4 (only the surviving sec-a).
#[test]
fn stats_occupancy_excludes_departed_secondary() {
    use dynrunner_protocol_primary_secondary::RemovalCause;

    // Two live secondaries, then sec-b DEPARTS (PeerRemoved ⇒ peer_state
    // Dead) while its capacity record stays. sec-a runs one task.
    let mut s = with_capacities(
        seed_state(
            &[],
            &[(
                task("t0", "P", &[]),
                Seed::InFlight {
                    secondary: "sec-a",
                    worker: 0,
                },
            )],
        ),
        &[("sec-a", 4), ("sec-b", 2)],
    );
    s.apply(ClusterMutation::PeerRemoved {
        id: "sec-b".to_string(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });

    // The capacity record for the departed secondary is still present —
    // this is the lingering ghost the unfiltered denominators counted.
    assert!(
        s.known_secondaries().any(|id| id == "sec-b"),
        "departed secondary's capacity record must still linger (set-once)"
    );

    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.busy_secondaries, 1, "only sec-a is running a task");
    assert_eq!(
        snap.total_secondaries, 1,
        "departed sec-b excluded from the live secondary denominator"
    );
    assert_eq!(snap.busy_workers, 1, "one slot on sec-a busy");
    assert_eq!(
        snap.total_workers, 4,
        "only sec-a's 4 live slots count; sec-b's lingering 2 excluded"
    );
    assert!(
        !snap.alive_secondaries.contains("sec-b"),
        "departed secondary must be absent from the live roster too"
    );
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

/// Idle-detector snapshot fixture. `pairs` is the per-secondary in-flight
/// load this tick; `alive` is the live-member roster the detector prunes
/// against (`alive_secondary_members()` in production). A secondary that
/// is idle this tick (absent from `pairs`) but still ALIVE must appear in
/// `alive` — otherwise the detector correctly drops it as departed. The
/// union of `pairs` ids and `alive` is taken so a caller can pass just the
/// roster once; an in-flight id is alive by construction.
fn in_flight_snap(pairs: &[(&str, usize)], alive: &[&str], ready: usize) -> StatsSnapshot {
    let mut alive_secondaries: std::collections::HashSet<String> =
        alive.iter().map(|s| s.to_string()).collect();
    alive_secondaries.extend(pairs.iter().map(|(s, _)| s.to_string()));
    StatsSnapshot {
        ready_in_queue: ready,
        per_secondary_in_flight: pairs.iter().map(|(s, n)| (s.to_string(), *n)).collect(),
        alive_secondaries,
        ..Default::default()
    }
}

#[test]
fn idle_fires_once_after_threshold_with_ready_work() {
    let mut det = IdleDetector::new(IDLE_THRESHOLD);
    let t0 = Instant::now();
    // sec-a busy, sec-b idle; ready work present. First tick observes
    // both, stamps sec-b idle. No fire yet (threshold not elapsed).
    // sec-b stays ALIVE while idle, so it is not pruned as departed.
    let snap = in_flight_snap(&[("sec-a", 2)], &["sec-b"], 5);
    // sec-b is known only once it has been seen in-flight; seed it by
    // first observing it busy, then idle.
    let busy = in_flight_snap(&[("sec-a", 2), ("sec-b", 1)], &[], 5);
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
    let busy = in_flight_snap(&[("sec-b", 1)], &[], 0);
    let idle = in_flight_snap(&[], &["sec-b"], 0);
    assert!(det.tick(&busy, t0).is_empty());
    assert!(det.tick(&idle, t0 + Duration::from_secs(1)).is_empty());
    assert!(det.tick(&idle, t0 + Duration::from_secs(120)).is_empty());
}

#[test]
fn idle_rearms_after_secondary_receives_task() {
    let mut det = IdleDetector::new(IDLE_THRESHOLD);
    let t0 = Instant::now();
    let busy = in_flight_snap(&[("sec-b", 1)], &[], 5);
    let idle = in_flight_snap(&[], &["sec-b"], 5);
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
    let src = SharedSnapshotSource::new(in_flight_snap(&[("sec-b", 1)], &[], 5));
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let (_outage_tx, outage_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::observer::lost_visibility::EndedOutage>();
    let (_force_print_tx, force_print_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let driver = tokio::spawn({
        let src = src.clone();
        async move {
            super::reporter::run_reporter(
                src,
                VirtualClock,
                outage_rx,
                force_print_rx,
                crate::observer::lost_visibility::WakeNoteSlot::default(),
                async move {
                    let _ = cancel_rx.await;
                },
            )
            .await;
        }
    });
    // First observe sec-b busy (handled in the seeded snapshot), then
    // flip it idle so a spell starts (still alive, so not pruned).
    src.publish(in_flight_snap(&[], &["sec-b"], 5));
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

// ── wake-stream outage seam: late run + skip-one grid bookkeeping ──
//
// Simulated time = injected `Instant`s (the same pattern as the idle
// tests above); emissions are captured SYNCHRONOUSLY via
// `capture_important` (the documented non-flaky idiom). The async
// `run_reporter` wiring for the same seam is pinned separately below via
// the note-slot side effect (no capture across `.await`).

use super::reporter::{LATE_STATS_MIN_SPACING, Reporter, StatsGridGate};
use crate::observer::lost_visibility::{EndedOutage, WakeNoteSlot};

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

#[test]
fn grid_gate_runs_every_tick_without_late_emit() {
    // No outage / no late emit: every grid occurrence runs — including
    // ticks during a connection outage (the gate has NO connectivity
    // input; the pre-existing emit-while-down behaviour is preserved by
    // construction and pinned in the scenario tests below).
    let t0 = Instant::now();
    let mut gate = StatsGridGate::new();
    assert!(gate.grid_tick(t0 + secs(600)));
    assert!(gate.grid_tick(t0 + secs(1200)));
    assert!(gate.grid_tick(t0 + secs(1800)));
}

#[test]
fn grid_gate_late_run_due_only_if_occurrence_in_down_window() {
    let t0 = Instant::now();
    let mut gate = StatsGridGate::new();
    // Before any occurrence ever fired: never a late run.
    assert!(!gate.late_run_due(t0));
    gate.grid_tick(t0 + secs(600));
    // Outage 650→1370 spans the 1200 occurrence → due.
    gate.grid_tick(t0 + secs(1200));
    assert!(gate.late_run_due(t0 + secs(650)));
    // Outage 1250→1670 (7 min) spans NO occurrence → not due.
    assert!(!gate.late_run_due(t0 + secs(1250)));
}

#[test]
fn grid_gate_skips_single_next_occurrence_within_spacing() {
    // The spec exception: the next on-grid occurrence is skipped iff it
    // lands < 5 min after the late emit; the one after fires on the
    // ORIGINAL grid (the skip clears either way — one candidate only).
    let t0 = Instant::now();
    let mut gate = StatsGridGate::new();
    gate.grid_tick(t0 + secs(1200));
    gate.record_late_emit(t0 + secs(1620));
    assert!(
        !gate.grid_tick(t0 + secs(1800)),
        "1800 is 180s (< {LATE_STATS_MIN_SPACING:?}) after the late emit — skipped"
    );
    assert!(
        gate.grid_tick(t0 + secs(2400)),
        "the following occurrence fires on the original grid"
    );
}

#[test]
fn grid_gate_no_skip_when_next_occurrence_outside_spacing() {
    let t0 = Instant::now();
    let mut gate = StatsGridGate::new();
    gate.grid_tick(t0 + secs(1200));
    gate.record_late_emit(t0 + secs(1430));
    assert!(
        gate.grid_tick(t0 + secs(1800)),
        "1800 is 370s (≥ 5 min) after the late emit — fires on grid"
    );
}

/// The full down-12-minutes replay (spec edge case), next grid slot ≥5min
/// away: the grid occurrence during the outage runs (preserved
/// behaviour) but is silent against the frozen CRDT; the regain runs ONE
/// late stats log immediately, carrying the reconnection note; the next
/// on-grid occurrence (430s later, ≥ the 5-min spacing) fires on the
/// original grid.
#[test]
fn outage_12min_late_periodic_carries_note_next_grid_fires() {
    let t0 = Instant::now();
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        let mut gate = StatsGridGate::new();
        let note = WakeNoteSlot::default();

        // t+600 (connected): first grid occurrence, changed stats emit.
        assert!(gate.grid_tick(t0 + secs(600)));
        assert!(reporter.on_stats_tick(&snap_with(3, 0)));
        note.flush_after_host(); // no note pending — no-op

        // Outage 650→1370 (12 min). t+1200: the grid occurrence still
        // RUNS while down (preserved) but the CRDT is frozen → silent.
        assert!(gate.grid_tick(t0 + secs(1200)));
        assert!(!reporter.on_stats_tick(&snap_with(3, 0)), "frozen CRDT → silent");

        // t+1370: regain. The loss was logged (>5 min), the policy parked
        // the note and sent EndedOutage{down_since: 650}.
        note.set("reconnection-note".to_string());
        let ended = EndedOutage {
            down_since: t0 + secs(650),
        };
        assert!(gate.late_run_due(ended.down_since));
        // The late run: fresh post-reconnect data → emits, hosts the note.
        assert!(reporter.on_stats_tick(&snap_with(9, 0)));
        note.flush_after_host();
        gate.record_late_emit(t0 + secs(1370));
        assert!(!note.is_pending(), "the late log carried the note");

        // t+1800: 430s after the late emit (≥ 5 min) — fires on grid.
        assert!(gate.grid_tick(t0 + secs(1800)));
        assert!(reporter.on_stats_tick(&snap_with(12, 0)));
        note.flush_after_host(); // empty — the note never duplicates
    });

    let stats: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.message.contains("periodic cluster stats"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        stats.len(),
        3,
        "t+600, the late run, t+1800 — and nothing else: {events:?}"
    );
    let notes: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.message.contains("reconnection-note"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(notes.len(), 1, "the note attaches exactly once: {events:?}");
    assert_eq!(
        notes[0],
        stats[1] + 1,
        "the note rides immediately with the LATE periodic: {events:?}"
    );
}

/// The down-12-minutes variant where the next on-grid occurrence lands
/// <5min after the late run: that single occurrence is skipped; the one
/// after fires on the original grid.
#[test]
fn outage_12min_late_periodic_skips_next_grid_slot_within_5min() {
    let t0 = Instant::now();
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        let mut gate = StatsGridGate::new();
        let note = WakeNoteSlot::default();

        assert!(gate.grid_tick(t0 + secs(600)));
        assert!(reporter.on_stats_tick(&snap_with(3, 0)));

        // Outage 650→1620; the 1200 occurrence elapses while down.
        assert!(gate.grid_tick(t0 + secs(1200)));
        assert!(!reporter.on_stats_tick(&snap_with(3, 0)));

        // Regain at 1620: late run emits + hosts the note.
        note.set("reconnection-note".to_string());
        assert!(gate.late_run_due(t0 + secs(650)));
        assert!(reporter.on_stats_tick(&snap_with(9, 0)));
        note.flush_after_host();
        gate.record_late_emit(t0 + secs(1620));

        // 1800 is 180s after the late run (< 5 min): SKIPPED, even though
        // the snapshot changed — the occurrence is consumed, not run.
        assert!(!gate.grid_tick(t0 + secs(1800)));

        // 2400 fires on the ORIGINAL grid (no shift) and diffs against
        // the LATE announcement baseline.
        assert!(gate.grid_tick(t0 + secs(2400)));
        assert!(reporter.on_stats_tick(&snap_with(15, 0)));
    });

    let stats: Vec<&crate::test_capture::CapturedEvent> = events
        .iter()
        .filter(|e| e.message.contains("periodic cluster stats"))
        .collect();
    assert_eq!(
        stats.len(),
        3,
        "t+600, the late run, t+2400 — the t+1800 occurrence is skipped: {events:?}"
    );
    assert!(
        stats[2].message.contains("15(+6)"),
        "the post-skip occurrence diffs against the late announcement: {:?}",
        stats[2].message
    );
}

/// Down 7 minutes with NO grid occurrence inside the down window: no late
/// run at regain — the parked note simply rides the next REGULAR
/// emission (here the next on-grid periodic).
#[test]
fn outage_without_elapsed_periodic_has_no_late_run() {
    let t0 = Instant::now();
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        let mut gate = StatsGridGate::new();
        let note = WakeNoteSlot::default();

        // Outage 30→450 (7 min): the first grid occurrence is at 600 —
        // nothing elapsed while down.
        note.set("reconnection-note".to_string());
        assert!(
            !gate.late_run_due(t0 + secs(30)),
            "no occurrence elapsed while down — no late run"
        );
        assert!(note.is_pending(), "the note waits for the next host");

        // The next regular grid occurrence hosts the note.
        assert!(gate.grid_tick(t0 + secs(600)));
        assert!(reporter.on_stats_tick(&snap_with(4, 0)));
        note.flush_after_host();
        assert!(!note.is_pending());
    });

    let stats: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.message.contains("periodic cluster stats"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(stats.len(), 1, "only the on-grid emission: {events:?}");
    let notes: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.message.contains("reconnection-note"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        notes,
        vec![stats[0] + 1],
        "the note rides the next regular periodic, exactly once: {events:?}"
    );
}

/// While the connection stays down (loss logged, never regained) the
/// periodic keeps its pre-existing behaviour: grid ticks run and emit iff
/// the delta rule says so — e.g. changes that landed BEFORE the freeze
/// are still announced by the first in-outage tick. (Decision recorded in
/// the module docs: the reporter has no connectivity gate.)
#[test]
fn periodic_keeps_emitting_per_delta_rule_while_down() {
    let t0 = Instant::now();
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        let mut gate = StatsGridGate::new();

        // Changes landed at t+580; outage begins at t+590.
        // t+600 (down): the tick still runs and announces the pre-outage
        // delta.
        assert!(gate.grid_tick(t0 + secs(600)));
        assert!(
            reporter.on_stats_tick(&snap_with(5, 0)),
            "pre-outage changes are announced by the in-outage tick (current \
             behaviour preserved)"
        );
        // t+1200 (still down): frozen CRDT → silent.
        assert!(gate.grid_tick(t0 + secs(1200)));
        assert!(!reporter.on_stats_tick(&snap_with(5, 0)));
    });
    let stats: Vec<_> = events
        .iter()
        .filter(|e| e.message.contains("periodic cluster stats"))
        .collect();
    assert_eq!(stats.len(), 1, "one emission, per the delta rule: {events:?}");
}

/// The async `run_reporter` wiring for the outage seam, under the paused
/// clock: an `EndedOutage` whose down window swallowed a grid occurrence
/// triggers the late stats run on the driver task. Observable WITHOUT a
/// capture-across-await (the documented flaky pattern): the late run
/// flushes the shared note slot, so the parked note disappearing IS the
/// late emission. Cancel-safety of the new arm is exercised by the
/// constant sibling ticks (idle interval fires 10× per stats interval)
/// and the clean cancel at the end.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn driver_runs_late_stats_on_ended_outage_signal() {
    let src = SharedSnapshotSource::new(StatsSnapshot::default());
    let note = WakeNoteSlot::default();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let (outage_tx, outage_rx) = tokio::sync::mpsc::unbounded_channel::<EndedOutage>();
    let (_force_print_tx, force_print_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    // Virtual t0 (the paused clock's view) — the whole run is "down".
    let down_since = tokio::time::Instant::now().into_std();
    let driver = tokio::task::spawn({
        let src = src.clone();
        let note = note.clone();
        async move {
            super::reporter::run_reporter(
                src,
                VirtualClock,
                outage_rx,
                force_print_rx,
                note,
                async move {
                    let _ = cancel_rx.await;
                },
            )
            .await;
        }
    });
    // Let the driver initialise its intervals at virtual t0 BEFORE the
    // clock moves (otherwise they would be created at t0+601 and no grid
    // occurrence would land inside the down window).
    tokio::task::yield_now().await;
    // Let a grid occurrence elapse "while down" (ticks keep firing — the
    // preserved behaviour; all-zero snapshot → silent).
    tokio::time::advance(Duration::from_secs(601)).await;
    tokio::task::yield_now().await;

    // Regain: publish the post-reconnect data and signal the outage end.
    src.publish(snap_with(7, 0));
    note.set("late-run-probe".to_string());
    outage_tx
        .send(EndedOutage { down_since })
        .expect("driver alive");
    // The late run is immediate (no further time advance needed) — give
    // the driver task a few polls.
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        !note.is_pending(),
        "the late stats run must fire immediately on the EndedOutage signal \
         and flush (host) the parked note"
    );

    let _ = cancel_tx.send(());
    tokio::time::advance(Duration::from_millis(1)).await;
    let joined = tokio::time::timeout(Duration::from_secs(1), driver).await;
    assert!(joined.is_ok(), "driver must terminate on cancel");
}

/// Counter-wiring: an `EndedOutage` whose down window contains NO grid
/// occurrence must NOT run a late stats log — the note stays parked,
/// waiting for the next genuine host.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn driver_skips_late_stats_when_no_periodic_elapsed_while_down() {
    let src = SharedSnapshotSource::new(StatsSnapshot::default());
    let note = WakeNoteSlot::default();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let (outage_tx, outage_rx) = tokio::sync::mpsc::unbounded_channel::<EndedOutage>();
    let (_force_print_tx, force_print_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let driver = tokio::task::spawn({
        let src = src.clone();
        let note = note.clone();
        async move {
            super::reporter::run_reporter(
                src,
                VirtualClock,
                outage_rx,
                force_print_rx,
                note,
                async move {
                    let _ = cancel_rx.await;
                },
            )
            .await;
        }
    });
    // Let the driver initialise its intervals at virtual t0.
    tokio::task::yield_now().await;
    // 7 virtual minutes pass, but NO grid occurrence elapses inside the
    // down window (the first stats occurrence is at t0+600; the outage
    // spans t0+30 → t0+450).
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    let down_since = tokio::time::Instant::now().into_std();
    tokio::time::advance(Duration::from_secs(420)).await;
    tokio::task::yield_now().await;

    src.publish(snap_with(7, 0));
    note.set("late-run-probe".to_string());
    outage_tx
        .send(EndedOutage { down_since })
        .expect("driver alive");
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        note.is_pending(),
        "no grid occurrence elapsed while down — no late run, the note waits"
    );

    let _ = cancel_tx.send(());
    tokio::time::advance(Duration::from_millis(1)).await;
    let joined = tokio::time::timeout(Duration::from_secs(1), driver).await;
    assert!(joined.is_ok(), "driver must terminate on cancel");
}

// ── #574: 10-min skip predicate + 1-hour safety net + SIGUSR1 force-print ──

/// `snap_with` only sets the two earlier helpers' fields; for the skip
/// predicate tests we need to move each candidate field independently.
/// 8 inputs is intentional — every field the skip predicate distinguishes
/// gets its own positional slot so test bodies stay readable as flat
/// `snap_full(succeeded, fail_retry, fail_oom, fail_final, …)`.
#[allow(clippy::too_many_arguments)]
fn snap_full(
    succeeded: usize,
    fail_retry: usize,
    fail_oom: usize,
    fail_final: usize,
    unfulfillable: usize,
    invalid_task: usize,
    setup_succeeded: usize,
    in_flight: usize,
) -> StatsSnapshot {
    StatsSnapshot {
        succeeded,
        fail_retry,
        fail_oom,
        fail_final,
        unfulfillable,
        invalid_task,
        setup_succeeded,
        in_flight,
        ..Default::default()
    }
}

// ── #575 resource-stat inclusion tests (per-field 25% threshold + zero-baseline) ──

/// A snapshot pre-populated with averaged resource numbers — the test
/// fixture for the #575 format paths.
fn resource_snap(p50: u64, p90: u64, free_mem: u64) -> StatsSnapshot {
    StatsSnapshot {
        avg_mem_p50_bytes: Some(p50),
        avg_mem_p90_bytes: Some(p90),
        avg_total_free_memory_bytes: Some(free_mem),
        ..Default::default()
    }
}

#[test]
fn resource_avg_first_value_always_included_against_none_baseline() {
    let cur = resource_snap(500 * 1024 * 1024, 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
    let outcome = render_report_full(&cur, &StatsSnapshot::default(), &ResourceBaseline::default());
    let body = outcome.body.expect("first-ever resource emit reports");
    assert!(body.contains("mem P50"), "got: {body}");
    assert!(body.contains("mem P90"), "got: {body}");
    assert!(body.contains("free host memory"), "got: {body}");
    // The next baseline advances per-field for every line we included.
    assert_eq!(
        outcome.next_resource_baseline.mem_p50_bytes,
        Some(500 * 1024 * 1024)
    );
    assert_eq!(
        outcome.next_resource_baseline.mem_p90_bytes,
        Some(1024 * 1024 * 1024)
    );
    // P10/P30/P70/avg + swap + cpu are still None on the prev_printed
    // side; their `value` was `None` (snapshot default), so they
    // weren't rendered and the baseline stays None for them.
    assert_eq!(outcome.next_resource_baseline.mem_p10_bytes, None);
}

#[test]
fn resource_avg_30pct_move_included_10pct_move_omitted_per_field() {
    let prev_baseline = ResourceBaseline {
        mem_p50_bytes: Some(1000),
        mem_p90_bytes: Some(1000),
        ..Default::default()
    };
    let cur = StatsSnapshot {
        avg_mem_p50_bytes: Some(1300),
        avg_mem_p90_bytes: Some(1100),
        ..Default::default()
    };
    let outcome = render_report_full(&cur, &StatsSnapshot::default(), &prev_baseline);
    let body = outcome.body.expect("P50 30% move must trip the gate");
    assert!(body.contains("mem P50"), "got: {body}");
    assert!(
        !body.contains("mem P90"),
        "P90 10% move is below 25%, expected omitted; got: {body}"
    );
    // Per-field baseline advance: P50 moved → new baseline; P90 omitted
    // → baseline unchanged.
    assert_eq!(outcome.next_resource_baseline.mem_p50_bytes, Some(1300));
    assert_eq!(outcome.next_resource_baseline.mem_p90_bytes, Some(1000));
}

#[test]
fn resource_avg_zero_baseline_includes_first_nonzero() {
    let prev_baseline = ResourceBaseline {
        mem_p50_bytes: Some(0),
        ..Default::default()
    };
    let cur = StatsSnapshot {
        avg_mem_p50_bytes: Some(500),
        ..Default::default()
    };
    let outcome = render_report_full(&cur, &StatsSnapshot::default(), &prev_baseline);
    let body = outcome.body.expect("zero-baseline first nonzero must include");
    assert!(body.contains("mem P50"), "got: {body}");
    assert_eq!(outcome.next_resource_baseline.mem_p50_bytes, Some(500));
}

#[test]
fn resource_avg_none_value_omitted_does_not_consume_baseline() {
    let prev_baseline = ResourceBaseline {
        mem_p50_bytes: Some(1000),
        ..Default::default()
    };
    let cur = StatsSnapshot::default();
    let outcome = render_report_full(&cur, &StatsSnapshot::default(), &prev_baseline);
    assert!(outcome.body.is_none(), "None value -> nothing wake-worthy");
    // Baseline must NOT regress (no-regress contract).
    assert_eq!(outcome.next_resource_baseline.mem_p50_bytes, Some(1000));
}

#[test]
fn resource_avg_omitted_unchanged_does_not_trigger_footer() {
    let prev_baseline = ResourceBaseline {
        mem_p50_bytes: Some(1000),
        ..Default::default()
    };
    let cur = StatsSnapshot {
        succeeded: 5,
        avg_mem_p50_bytes: Some(1050),
        ..Default::default()
    };
    let prev = StatsSnapshot {
        succeeded: 0,
        ..Default::default()
    };
    let outcome = render_report_full(&cur, &prev, &prev_baseline);
    let body = outcome.body.expect("succeeded change reports");
    assert!(body.contains("succeeded: 5"), "got: {body}");
    // The resource line was OMITTED but does NOT contribute to the
    // "Omitted unchanged stats." footer (resource lines are not in the
    // wake-worthiness contract for the existing 13 operational metrics).
    assert!(
        !body.contains("Omitted unchanged stats."),
        "resource-only unchanged must not trip the footer; got: {body}"
    );
}

#[test]
fn resource_avg_per_field_baseline_independence() {
    let prev_baseline = ResourceBaseline {
        mem_p50_bytes: Some(1000),
        mem_p90_bytes: Some(1000),
        ..Default::default()
    };
    let cur = StatsSnapshot {
        avg_mem_p50_bytes: Some(1300),
        avg_mem_p90_bytes: Some(1050),
        ..Default::default()
    };
    let outcome = render_report_full(&cur, &StatsSnapshot::default(), &prev_baseline);
    assert!(outcome.body.is_some());
    assert_eq!(outcome.next_resource_baseline.mem_p50_bytes, Some(1300));
    assert_eq!(
        outcome.next_resource_baseline.mem_p90_bytes,
        Some(1000),
        "P90's baseline must NOT advance just because P50 emitted"
    );
}
/// T1: a tick whose ONLY diff is `succeeded` (an in-set throughput
/// counter) elides under `on_stats_tick_skippable`, and the
/// last-announced baseline is NOT advanced — invariant 1. The
/// no-advance is observed via a follow-on `on_stats_tick_skippable`
/// call on a snapshot where a non-eligible field has ALSO moved: the
/// emitted body's `succeeded` line carries the ACCUMULATED delta
/// against the seed (proving the skipped tick did not reset the
/// baseline to its own value).
#[test]
fn skippable_succeeded_only_elides_and_keeps_baseline() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // Seed the baseline by emitting one regular tick at succeeded=1.
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        // Only `succeeded` moves; the skippable path elides.
        assert!(!reporter.on_stats_tick_skippable(&snap_full(2, 0, 0, 0, 0, 0, 0, 0)));
        // Now a tick where a non-eligible field (in_flight) has moved
        // too — predicate fails, the emit fires. Critically, the
        // succeeded delta in the EMITTED body must be against the
        // SEED's succeeded=1 (the last-PRINTED baseline), NOT against
        // the prior skipped tick's succeeded=2 — proving the skip did
        // not advance `last_announced`.
        assert!(reporter.on_stats_tick_skippable(&snap_full(3, 0, 0, 0, 0, 0, 0, 7)));
    });
    assert_eq!(events.len(), 2, "seed + the post-skip mixed-diff emission");
    let body = &events[1].message;
    assert!(
        body.contains("succeeded: 3(+2)"),
        "the post-skip emit's succeeded delta is against the last-PRINTED snapshot (1, not 2): got\n{body}"
    );
}

/// T2: a tick whose ONLY diff is `fail_retry` elides — invariant 1.
#[test]
fn skippable_fail_retry_only_elides() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        assert!(!reporter.on_stats_tick_skippable(&snap_full(1, 5, 0, 0, 0, 0, 0, 0)));
    });
    assert_eq!(events.len(), 1);
}

/// T3: a tick whose diff covers MULTIPLE in-set counters
/// (succeeded + fail_oom + fail_final) elides — the skip predicate is
/// subset (not strict equality on one field) — invariant 6.
#[test]
fn skippable_multiple_eligible_counters_elide() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        assert!(!reporter.on_stats_tick_skippable(&snap_full(3, 0, 1, 1, 0, 0, 0, 0)));
    });
    assert_eq!(events.len(), 1);
}

/// T4: a tick whose diff includes a NON-eligible field (here
/// `in_flight`) is NOT skipped — the eligible counter moving alongside
/// is irrelevant: the predicate is a subset check on the WHOLE diff.
#[test]
fn skippable_mixed_with_in_flight_prints() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        // succeeded AND in_flight both change → not a subset.
        assert!(reporter.on_stats_tick_skippable(&snap_full(2, 0, 0, 0, 0, 0, 0, 3)));
    });
    // Two emissions: seed + the mixed-diff tick.
    assert_eq!(events.len(), 2);
}

/// T4b: `unfulfillable` is OUT of the skip-eligible set (owner
/// decision: it is an exceptional-flow counter, not routine throughput).
/// A diff that moves only `unfulfillable` prints.
#[test]
fn skippable_unfulfillable_only_prints() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        assert!(reporter.on_stats_tick_skippable(&snap_full(1, 0, 0, 0, 1, 0, 0, 0)));
    });
    assert_eq!(events.len(), 2);
}

/// T4c: `invalid_task` is OUT of the skip-eligible set. A diff that
/// moves only `invalid_task` prints.
#[test]
fn skippable_invalid_task_only_prints() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        assert!(reporter.on_stats_tick_skippable(&snap_full(1, 0, 0, 0, 0, 1, 0, 0)));
    });
    assert_eq!(events.len(), 2);
}

/// T4d: `setup_succeeded` is OUT of the skip-eligible set (owner
/// decision: it is a setup-task category, not the routine
/// worker-throughput counters).
#[test]
fn skippable_setup_succeeded_only_prints() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        assert!(reporter.on_stats_tick_skippable(&snap_full(1, 0, 0, 0, 0, 0, 1, 0)));
    });
    assert_eq!(events.len(), 2);
}

/// T5: the 6th consecutive skippable tick is the 1-hour safety net and
/// IS emitted, even with only `succeeded` movement — invariant 2.
#[test]
fn safety_net_fires_on_6th_skippable_tick() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // Seed: emit one tick so the baseline is non-default.
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        // 5 skipped throughput-only ticks.
        for n in 2..=6 {
            assert!(
                !reporter.on_stats_tick_skippable(&snap_full(n, 0, 0, 0, 0, 0, 0, 0)),
                "tick {n} after seed must skip (counter < safety net)"
            );
        }
        // 6th skippable tick after the seed = the safety-net boundary.
        assert!(
            reporter.on_stats_tick_skippable(&snap_full(7, 0, 0, 0, 0, 0, 0, 0)),
            "the 6th routine tick since the last emit must fire the safety net"
        );
    });
    // Seed + safety-net = 2 emissions; the 5 intermediate ticks elided.
    assert_eq!(events.len(), 2);
}

/// T6: the 1-hour safety net's delta is against the LAST-PRINTED
/// snapshot, not the last-tick snapshot — invariant 3. Across 5 skipped
/// 10-min ticks each bumping `succeeded` by 1_000, the 1-hour emission
/// shows the accumulated +5_000 delta plus the 6th tick's own +1_000 =
/// +6_000.
#[test]
fn safety_net_delta_is_against_last_printed_snapshot() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // First emit at succeeded = 1_000 so the baseline is durable.
        assert!(reporter.on_stats_tick(&snap_full(1_000, 0, 0, 0, 0, 0, 0, 0)));
        // 5 skipped ticks: succeeded keeps climbing by 1_000 each tick.
        for k in 1..=5 {
            let snap = snap_full(1_000 + 1_000 * k, 0, 0, 0, 0, 0, 0, 0);
            assert!(!reporter.on_stats_tick_skippable(&snap));
        }
        // 6th tick = safety net boundary. succeeded = 7_000.
        assert!(reporter.on_stats_tick_skippable(&snap_full(7_000, 0, 0, 0, 0, 0, 0, 0)));
    });
    assert_eq!(events.len(), 2, "seed + safety net = 2 emissions");
    let safety_body = &events[1].message;
    assert!(
        safety_body.contains("succeeded: 7000(+6000)"),
        "safety-net emission must render the accumulated delta against the last-PRINTED snapshot; got:\n{safety_body}"
    );
}

/// T7: a SIGUSR1 force-print renders the FULL snapshot — every field is
/// in the emitted body, including unchanged and zero values —
/// invariant 4.
#[test]
fn force_print_emits_full_snapshot_including_unchanged() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // Seed an emission so `last_announced` is non-default; the
        // force-print must still show every field of the CURRENT snap.
        assert!(reporter.on_stats_tick(&snap_full(5, 0, 0, 0, 0, 0, 0, 2)));
        // Force-print on a snapshot identical to the seed: every line
        // (including the unchanged ones) MUST appear in the body.
        assert!(reporter.on_force_print(&snap_full(5, 0, 0, 0, 0, 0, 0, 2)));
    });
    assert_eq!(events.len(), 2);
    let force_body = &events[1].message;
    for needle in [
        "succeeded: 5",
        "setup: 0",
        "failed (retry): 0",
        "failed (oom): 0",
        "failed (final): 0",
        "unfulfillable: 0",
        "invalid_task: 0",
        "in-flight: 2",
        "waiting on deps: 0",
        "blocked (upstream unfulfillable): 0",
        "ready in queue: 0",
        "busy secondaries: 0/0",
        "busy workers: 0/0",
    ] {
        assert!(
            force_body.contains(needle),
            "force-print body must include `{needle}` (full-snapshot contract); got:\n{force_body}"
        );
    }
    assert!(!force_body.contains("Omitted"));
}

/// T8: after a force-print, the next 10-min tick diffs against the
/// POST-signal snapshot (the force-print advanced `last_announced`) —
/// invariant 5. The diff against the same snapshot is ∅, so the next
/// tick elides cleanly without re-emitting the (now-already-printed)
/// fields.
#[test]
fn force_print_advances_last_announced() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // Force-print at succeeded=5.
        assert!(reporter.on_force_print(&snap_full(5, 0, 0, 0, 0, 0, 0, 0)));
        // The next ordinary 10-min tick with the SAME snapshot must
        // elide — the baseline was advanced by the force-print, so the
        // diff is empty.
        assert!(!reporter.on_stats_tick_skippable(&snap_full(5, 0, 0, 0, 0, 0, 0, 0)));
    });
    assert_eq!(events.len(), 1, "exactly one emission (the force-print)");
}

/// T9: a 10-min tick whose diff is exactly ∅ (no field moved at all)
/// elides — the skip predicate is `diff ⊆ {eligible}` and ∅ is a
/// subset of any set — invariant 6 (owner-approved default).
#[test]
fn skippable_empty_diff_elides() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        assert!(reporter.on_stats_tick(&snap_full(2, 1, 0, 0, 0, 0, 0, 4)));
        // Exact same snapshot — diff is ∅.
        assert!(!reporter.on_stats_tick_skippable(&snap_full(2, 1, 0, 0, 0, 0, 0, 4)));
    });
    assert_eq!(events.len(), 1);
}

/// T10: a SIGUSR1 force-print mid-skip-streak emits regardless of how
/// many ticks have been skipped, and resets the safety-net counter so
/// the next 1-hour boundary is 6 ticks AWAY from the force-print —
/// invariant 5 (the force-print is on the same baseline-write path as
/// the periodic emit).
#[test]
fn force_print_during_skip_streak_resets_safety_net() {
    let events = crate::test_capture::capture_important(|| {
        let mut reporter = Reporter::new();
        // Seed.
        assert!(reporter.on_stats_tick(&snap_full(1, 0, 0, 0, 0, 0, 0, 0)));
        // 3 skipped ticks.
        for n in 2..=4 {
            assert!(!reporter.on_stats_tick_skippable(&snap_full(n, 0, 0, 0, 0, 0, 0, 0)));
        }
        // Force-print mid-streak.
        assert!(reporter.on_force_print(&snap_full(4, 0, 0, 0, 0, 0, 0, 0)));
        // 5 more skipped ticks — none should fire the safety net
        // (we just printed). The counter restarted at 0.
        for n in 5..=9 {
            assert!(
                !reporter.on_stats_tick_skippable(&snap_full(n, 0, 0, 0, 0, 0, 0, 0)),
                "tick {n} (5 ticks after force-print) must still skip"
            );
        }
        // The 6th routine tick after the force-print fires the safety
        // net — measured from the force-print's emission, not from the
        // seed before it.
        assert!(reporter.on_stats_tick_skippable(&snap_full(10, 0, 0, 0, 0, 0, 0, 0)));
    });
    // Seed + force-print + safety-net-after-force-print = 3 emissions.
    assert_eq!(events.len(), 3);
    let safety_body = &events[2].message;
    assert!(
        safety_body.contains("succeeded: 10(+6)"),
        "safety net delta must be against the force-print baseline (succeeded=4); got:\n{safety_body}"
    );
}

/// Predicate-direct: `diff_subset_of_skip_eligible` is the single seam
/// the skip path consults. Pin the eligible / non-eligible classification
/// directly so a future field addition lands on the destructure-or-die
/// check in `StatsSnapshot`.
#[test]
fn diff_subset_of_skip_eligible_classifies_fields_correctly() {
    let base = snap_full(10, 5, 2, 1, 0, 0, 0, 0);

    // Eligible counters: any subset of {succeeded, fail_retry,
    // fail_oom, fail_final} moving alone returns true.
    let mut cur = base.clone();
    cur.succeeded = 11;
    assert!(cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.fail_retry = 99;
    assert!(cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.fail_oom = 99;
    assert!(cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.fail_final = 99;
    assert!(cur.diff_subset_of_skip_eligible(&base));
    // Multiple eligible counters moving simultaneously — still in.
    let mut cur = base.clone();
    cur.succeeded = 99;
    cur.fail_retry = 99;
    cur.fail_oom = 99;
    cur.fail_final = 99;
    assert!(cur.diff_subset_of_skip_eligible(&base));
    // Zero diff — ∅ is a subset of any set.
    assert!(base.diff_subset_of_skip_eligible(&base));

    // NON-eligible: any of these fields moving (even alone) FAILS the
    // predicate (the tick must print).
    let mut cur = base.clone();
    cur.unfulfillable = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.invalid_task = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.setup_succeeded = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.in_flight = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.waiting_on_deps = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.blocked = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.ready_in_queue = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.busy_secondaries = 1;
    cur.total_secondaries = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.busy_workers = 1;
    cur.total_workers = 1;
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.alive_secondaries.insert("sec-x".to_string());
    assert!(!cur.diff_subset_of_skip_eligible(&base));
    let mut cur = base.clone();
    cur.per_secondary_in_flight
        .insert("sec-x".to_string(), 1);
    assert!(!cur.diff_subset_of_skip_eligible(&base));
}

/// `render_report_full` is unconditional: every line is emitted,
/// including zero values. No footer, no parenthetical deltas.
#[test]
fn render_report_full_emits_every_line_including_zeros() {
    let snap = StatsSnapshot::default();
    let body = super::format::render_report_full(&snap);
    for needle in [
        "succeeded: 0",
        "setup: 0",
        "failed (retry): 0",
        "failed (oom): 0",
        "failed (final): 0",
        "unfulfillable: 0",
        "invalid_task: 0",
        "in-flight: 0",
        "waiting on deps: 0",
        "blocked (upstream unfulfillable): 0",
        "ready in queue: 0",
        "busy secondaries: 0/0",
        "busy workers: 0/0",
    ] {
        assert!(body.contains(needle), "full-render must include `{needle}` (zero-valued); got:\n{body}");
    }
    assert!(!body.contains("Omitted"));
    assert!(!body.contains("(+"), "full-render must not show parenthetical deltas");
}

// ── #589 loop-health: observer fleet aggregation + 25%-threshold + skip-eligible ──

use dynrunner_protocol_primary_secondary::SecondaryResourceSampleRecord;
use super::stats::DominantArm;

/// Helper: drop a (PeerJoined + SecondaryCapacity + SecondaryResourceSample)
/// triple onto a fresh ClusterState so a secondary is alive AND has a
/// resource sample record the observer's `live_compute_resource_samples`
/// accessor returns. Lets the loop-health tests build a 3-secondary
/// fleet with crafted per-secondary samples.
fn seed_resource_fleet(
    samples: &[(&str, SecondaryResourceSampleRecord)],
) -> ClusterState<()> {
    let mut s = ClusterState::<()>::new();
    for (secondary, record) in samples {
        s.apply(ClusterMutation::PeerJoined {
            peer_id: secondary.to_string(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: secondary.to_string(),
            worker_count: 1,
            resources: Vec::new(),
        });
        s.apply(ClusterMutation::SecondaryResourceSample {
            secondary: secondary.to_string(),
            record: record.clone(),
        });
    }
    s
}

fn lh_record(
    member_gen: u64,
    iters_per_sec_milli: u64,
    // (arm_name, time-share milli-percent, time ms/s)
    dominant: (&str, u32, u64),
    unacked: u32,
) -> SecondaryResourceSampleRecord {
    SecondaryResourceSampleRecord {
        member_gen,
        emitted_at_ms: 1_700_000_000_000,
        mem_p10_bytes: 0,
        mem_p30_bytes: 0,
        mem_p50_bytes: 0,
        mem_p70_bytes: 0,
        mem_p90_bytes: 0,
        mem_avg_bytes: 0,
        total_free_memory_bytes: 0,
        total_swap_used_bytes: 0,
        total_free_swap_bytes: 0,
        cpu_utilization_milli: 0,
        oploop_iters_per_sec_milli: iters_per_sec_milli,
        dominant_arm_name: dominant.0.to_string(),
        dominant_arm_pct_milli: dominant.1,
        dominant_arm_time_ms_per_sec: dominant.2,
        max_unacked_for_secs: unacked,
    }
}

/// T3 — `max_unacked_for_secs` fleet aggregation: across 3 secondaries
/// with values {30s, 60s, 120s}, the observer emits max=120.
#[test]
fn loop_health_max_unacked_fleet_max() {
    let s = seed_resource_fleet(&[
        ("sec-a", lh_record(0, 5_000, ("inbox", 40_000, 400), 30)),
        ("sec-b", lh_record(0, 6_000, ("inbox", 45_000, 450), 60)),
        ("sec-c", lh_record(0, 4_000, ("mem_check", 90_000, 900), 120)),
    ]);
    let snap = Snap::from_cluster_state(&s);
    assert_eq!(snap.max_unacked_for_secs, Some(120));
}

/// T2 (observer side) — `dominant_arm`: max-by-pct (TIME share) across
/// secondaries emits the single hottest arm name+pct+time, carrying the
/// ms/s from the SAME winning secondary (NOT a fleet average).
#[test]
fn loop_health_dominant_arm_max_by_pct() {
    let s = seed_resource_fleet(&[
        ("sec-a", lh_record(0, 5_000, ("inbox", 40_000, 400), 0)),
        ("sec-b", lh_record(0, 6_000, ("mem_check", 80_000, 800), 0)),
        ("sec-c", lh_record(0, 4_000, ("anti_entropy", 30_000, 300), 0)),
    ]);
    let snap = Snap::from_cluster_state(&s);
    let dominant = snap.dominant_arm.expect("at least one secondary with a non-empty dominant arm");
    assert_eq!(dominant.arm_name, "mem_check");
    assert_eq!(dominant.pct_milli, 80_000);
    // The ms/s comes from sec-b (the max-pct winner), not sec-a/sec-c.
    assert_eq!(dominant.time_ms_per_sec, 800);
}

/// T1 (observer side) — `avg_oploop_iters_per_sec_milli` is the
/// arithmetic mean across secondaries with NON-ZERO readings; a
/// zero (cold-start / pre-#589) secondary is excluded from the
/// denominator.
#[test]
fn loop_health_avg_iters_excludes_zero_sentinels() {
    let s = seed_resource_fleet(&[
        ("sec-a", lh_record(0, 10_000, ("inbox", 50_000, 500), 0)),
        ("sec-b", lh_record(0, 20_000, ("inbox", 50_000, 500), 0)),
        // sec-c is cold-start / pre-#589 (iter rate at the wire-default
        // sentinel) — MUST NOT drag the average toward zero.
        ("sec-c", lh_record(0, 0, ("", 0, 0), 0)),
    ]);
    let snap = Snap::from_cluster_state(&s);
    // (10_000 + 20_000) / 2 = 15_000; cold-start sec-c excluded.
    assert_eq!(snap.avg_oploop_iters_per_sec_milli, Some(15_000));
}

/// Empty fleet (no secondary has emitted) ⇒ every loop-health
/// aggregate is `None` — the freshly-promoted-primary cold-start
/// window contract carried over from #575.
#[test]
fn loop_health_empty_fleet_yields_none() {
    let s = ClusterState::<()>::new();
    let snap = Snap::from_cluster_state(&s);
    assert!(snap.avg_oploop_iters_per_sec_milli.is_none());
    assert!(snap.dominant_arm.is_none());
    assert!(snap.max_unacked_for_secs.is_none());
}

/// T4 — 25%-threshold inclusion: a dominant_arm_pct change from 30% to
/// 50% is INCLUDED (>25% relative); from 50% to 55% is EXCLUDED
/// (<25% relative). Mirrors the #575 resource-line gate.
#[test]
fn loop_health_dominant_pct_25pct_threshold() {
    let cur_50 = StatsSnapshot {
        dominant_arm: Some(DominantArm {
            arm_name: "mem_check".to_string(),
            pct_milli: 50_000,
            time_ms_per_sec: 500,
        }),
        ..Default::default()
    };
    let prev = StatsSnapshot::default();

    // 30% baseline → 50% current: rel = (50_000 - 30_000) / 30_000 =
    // ~0.667 > 0.25, INCLUDE.
    let baseline_30 = ResourceBaseline {
        dominant_arm_pct_milli: Some(30_000),
        ..Default::default()
    };
    let r1 = render_report_full(&cur_50, &prev, &baseline_30);
    let body1 = r1.body.expect("at least one line moved enough to include");
    assert!(
        body1.contains("dominant arm (fleet max): mem_check:50.00% time=500ms/s"),
        "30%→50% must include the dominant-arm line; got:\n{body1}"
    );

    // 50% baseline → 50% current: rel = 0 ≤ 0.25, EXCLUDE.
    let baseline_50 = ResourceBaseline {
        dominant_arm_pct_milli: Some(50_000),
        ..Default::default()
    };
    let r2 = render_report_full(&cur_50, &prev, &baseline_50);
    assert!(
        r2.body.is_none(),
        "50%→50% must exclude (no other line moved); got:\n{:?}",
        r2.body
    );

    // 50% baseline → 55% current (rel = 0.1 < 0.25), EXCLUDE.
    let cur_55 = StatsSnapshot {
        dominant_arm: Some(DominantArm {
            arm_name: "mem_check".to_string(),
            pct_milli: 55_000,
            time_ms_per_sec: 550,
        }),
        ..Default::default()
    };
    let r3 = render_report_full(&cur_55, &prev, &baseline_50);
    assert!(
        r3.body.is_none(),
        "50%→55% (rel ≈ 0.1) must exclude; got:\n{:?}",
        r3.body
    );
}

/// T5 — skip-predicate interaction: a tick where ONLY the new
/// loop-health fields change is SKIP-eligible (per #574 extension).
/// The destructure-or-die trip stays compile-checked (the new fields
/// are bound with `_` in `diff_subset_of_skip_eligible`); this test
/// pins the BEHAVIOUR — a loop-health-only change must not force a
/// 10-minute emission.
#[test]
fn loop_health_only_change_is_skip_eligible() {
    let prev = StatsSnapshot::default();
    let cur = StatsSnapshot {
        avg_oploop_iters_per_sec_milli: Some(15_000),
        dominant_arm: Some(DominantArm {
            arm_name: "mem_check".to_string(),
            pct_milli: 80_000,
            time_ms_per_sec: 800,
        }),
        max_unacked_for_secs: Some(120),
        ..Default::default()
    };
    assert!(
        cur.diff_subset_of_skip_eligible(&prev),
        "a loop-health-only change must be SKIP-eligible (the 10-minute \
         emission elides; the 1-hour safety net or an outside-the-skip- \
         set field forces the print)"
    );
}

/// Companion to T5: a non-skip-eligible field moving DEFEATS the skip
/// predicate even when the loop-health axis moved too. Pins that the
/// new loop-health fields do not accidentally widen the skip set.
#[test]
fn loop_health_plus_in_flight_change_is_not_skip_eligible() {
    let prev = StatsSnapshot::default();
    let cur = StatsSnapshot {
        avg_oploop_iters_per_sec_milli: Some(15_000),
        in_flight: 7, // outside the skip set
        ..Default::default()
    };
    assert!(
        !cur.diff_subset_of_skip_eligible(&prev),
        "an in_flight change must defeat skip-eligibility regardless of \
         loop-health movement"
    );
}
