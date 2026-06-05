//! Tests for the snapshot type, the lattice-merge restore, and the
//! cross-variant snapshot round-trip pins.
//!
//! Covers `snapshot()` deep-clone capture, the merge rules documented
//! on `ClusterStateSnapshot` (terminal-wins per task, higher epoch
//! wins for primary, replace-if-empty for phase_deps / observers /
//! peer_holdings), and the variant round-trip pin for Unfulfillable
//! and Blocked entries through snapshot/restore.

use super::*;

#[test]
fn snapshot_round_trip_preserves_state() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "p".into(),
        task: mk_task("p"),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "i".into(),
        task: mk_task("i"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        hash: "i".into(),
        secondary: "s1".into(),
        worker: 7,
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "c".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::PrimaryChanged {
        new: "s1".into(),
        epoch: 3,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    let deps: HashMap<PhaseId, Vec<PhaseId>> = [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
        .into_iter()
        .collect();
    s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    assert_eq!(joiner.counts(), s.counts());
    assert_eq!(joiner.current_primary(), Some("s1"));
    assert_eq!(joiner.primary_epoch(), 3);
    assert_eq!(joiner.phase_deps(), &deps);
    assert!(matches!(
        joiner.task_state("p"),
        Some(TaskState::Pending { .. })
    ));
    assert!(matches!(
        joiner.task_state("i"),
        Some(TaskState::InFlight { .. })
    ));
    assert!(matches!(
        joiner.task_state("c"),
        Some(TaskState::Completed { .. })
    ));
}

/// A terminal `InvalidTask` entry survives a snapshot → restore cycle
/// onto a fresh joiner: it ranks as a strongest terminal (so a stale
/// peer's Pending observation cannot overwrite it on merge) and its
/// `reason` body is preserved verbatim.
#[test]
fn snapshot_round_trip_preserves_invalid_task() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "bad".into(),
        task: mk_task("bad"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "bad".into(),
        kind: ErrorType::InvalidTask {
            reason: "duplicate (phase,task_id)".to_string().into(),
        },
        error: "invalid_task:duplicate (phase,task_id)".into(),
        version: Default::default(),
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    match joiner.task_state("bad") {
        Some(TaskState::InvalidTask { reason, .. }) => {
            assert_eq!(reason, "duplicate (phase,task_id)");
        }
        other => panic!("expected InvalidTask after restore, got {other:?}"),
    }
    assert_eq!(joiner.counts(), s.counts());

    // Lattice rank: a stale peer's later Pending snapshot must NOT
    // overwrite the terminal InvalidTask on the joiner.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.apply(ClusterMutation::TaskAdded {
        hash: "bad".into(),
        task: mk_task("bad"),
    });
    joiner.restore(stale.snapshot());
    assert!(
        matches!(
            joiner.task_state("bad"),
            Some(TaskState::InvalidTask { .. })
        ),
        "terminal InvalidTask must win over a stale Pending snapshot"
    );
}

/// Pins the Step 8 contract that `ClusterStateSnapshot` carries
/// the replicated observer set so a late-joiner's first restore
/// populates `RoleTable.observers` before any `PeerJoined`
/// mutation arrives. Without this the joiner's election filter
/// (`secondary::election::lowest_alive` skips observers) could
/// fire against an empty set and promote an observer candidate
/// in the gap between snapshot-restore and the next live broadcast.
#[test]
fn snapshot_round_trip_preserves_observers() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: true,
        can_be_primary: false,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-2".into(),
        is_observer: true,
        can_be_primary: false,
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    // Joiner is empty: snapshot's observers REPLACE the empty
    // local set per the first-bootstrap branch in `restore`.
    joiner.restore(snap);
    assert_eq!(
        joiner.role_table().observers,
        HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
    );
}

/// Pins the Step 8 "first-bootstrap only" branch on `restore`:
/// a joiner that has already observed a live `PeerJoined`
/// broadcast (so `observers` is non-empty) keeps its local set
/// rather than overwriting from a (possibly stale) snapshot.
/// Mirrors the `phase_deps` "replaced if local empty, else kept"
/// shape.
#[test]
fn restore_keeps_local_observers_when_already_populated() {
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::PeerJoined {
        peer_id: "live-obs".into(),
        is_observer: true,
        can_be_primary: false,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::PeerJoined {
        peer_id: "stale-obs".into(),
        is_observer: true,
        can_be_primary: false,
    });

    joiner.restore(peer.snapshot());
    // Local set wins (live `PeerJoined` path is authoritative
    // once it has fired); snapshot's observers field is inert.
    assert_eq!(
        joiner.role_table().observers,
        HashSet::from(["live-obs".to_string()])
    );
}

#[test]
fn restore_lattice_merge_preserves_local_terminal() {
    // Joiner has already observed TaskCompleted via a live broadcast
    // before the snapshot RPC response arrives. The snapshot's
    // weaker InFlight state must NOT override the local terminal.
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    joiner.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    peer.apply(ClusterMutation::TaskAssigned {
        hash: "h".into(),
        secondary: "s".into(),
        worker: 0,
        version: Default::default(),
    });

    joiner.restore(peer.snapshot());
    assert!(matches!(
        joiner.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
}

#[test]
fn restore_lattice_merge_promotes_pending_to_in_flight() {
    // Joiner has only seen TaskAdded; snapshot has the InFlight
    // entry. The stronger lattice element (InFlight) wins.
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    peer.apply(ClusterMutation::TaskAssigned {
        hash: "h".into(),
        secondary: "s".into(),
        worker: 4,
        version: Default::default(),
    });

    joiner.restore(peer.snapshot());
    match joiner.task_state("h") {
        Some(TaskState::InFlight { worker, .. }) => assert_eq!(*worker, 4),
        other => panic!("expected InFlight, got {other:?}"),
    }
}

#[test]
fn restore_higher_epoch_wins_for_primary() {
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::PrimaryChanged {
        new: "old".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::PrimaryChanged {
        new: "new".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    joiner.restore(peer.snapshot());
    assert_eq!(joiner.current_primary(), Some("new"));
    assert_eq!(joiner.primary_epoch(), 5);

    // Reverse direction: a stale snapshot must not regress epoch.
    let mut stale_peer = ClusterState::<RunnerIdentifier>::new();
    stale_peer.apply(ClusterMutation::PrimaryChanged {
        new: "ancient".into(),
        epoch: 2,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    joiner.restore(stale_peer.snapshot());
    assert_eq!(joiner.current_primary(), Some("new"));
    assert_eq!(joiner.primary_epoch(), 5);
}

#[test]
fn restore_idempotent_under_double_apply() {
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    peer.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });

    let snap = peer.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap.clone());
    let counts_once = joiner.counts();
    joiner.restore(snap);
    assert_eq!(joiner.counts(), counts_once);
}

/// `ClusterStateSnapshot` round-trips the new Unfulfillable and
/// Blocked variants without loss; the late-joiner / reconnect
/// path observes the same state the originating replica recorded.
#[test]
fn pending_pool_unfulfillable_state_round_trips_via_snapshot() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "u".into(),
        task: mk_task("u"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "u".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing dep".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "u".into(),
    });
    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    match joiner.task_state("u") {
        Some(TaskState::Unfulfillable { reason, .. }) => {
            assert_eq!(reason, "missing dep");
        }
        other => panic!("expected Unfulfillable, got {other:?}"),
    }
    match joiner.task_state("b") {
        Some(TaskState::Blocked { on, .. }) => assert_eq!(on, "u"),
        other => panic!("expected Blocked, got {other:?}"),
    }
}

/// Migration shim (snapshot-only): a legacy snapshot carries deps that
/// predate the `(phase_id, task_id)` identity, so they decode with the
/// sentinel (empty) phase. On `restore`, the shim must inject the
/// enclosing task's phase into every sentinel dep — and leave any dep
/// that already names its phase (a new, explicit cross-phase dep)
/// untouched.
#[test]
fn restore_migrates_unphased_deps_to_enclosing_phase() {
    use dynrunner_core::TaskDep;

    // Build a task in phase "p0" (mk_task's phase) whose dep list mixes
    // a legacy un-phased dep (sentinel phase) and an explicit
    // cross-phase dep.
    let mut task = mk_task("dependent");
    task.task_depends_on = vec![
        // Legacy un-phased dep: sentinel phase, to be migrated.
        TaskDep {
            task_id: "legacy_prereq".into(),
            phase_id: PhaseId::default(),
            inherit_outputs: false,
        },
        // New explicit cross-phase dep: must NOT be rewritten.
        TaskDep {
            task_id: "explicit_prereq".into(),
            phase_id: PhaseId::from("other-phase"),
            inherit_outputs: true,
        },
    ];

    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task,
    });
    let snap = source.snapshot();

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    let restored = match joiner.task_state("h") {
        Some(TaskState::Pending { task, .. }) => task,
        other => panic!("expected Pending, got {other:?}"),
    };
    let deps = &restored.task_depends_on;
    assert_eq!(deps.len(), 2);
    // Legacy dep took the enclosing task's phase ("p0").
    assert_eq!(deps[0].task_id, "legacy_prereq");
    assert_eq!(
        deps[0].phase_id,
        PhaseId::from("p0"),
        "sentinel migrated to enclosing phase"
    );
    assert!(!deps[0].is_unphased());
    // Explicit cross-phase dep is unaffected by the shim.
    assert_eq!(deps[1].task_id, "explicit_prereq");
    assert_eq!(
        deps[1].phase_id,
        PhaseId::from("other-phase"),
        "explicit dep untouched"
    );
    assert!(deps[1].inherit_outputs);
}

/// Consumer-invariant round-trip — the regression test for the
/// bootstrap-relocated-primary false-strand bug.
///
/// A primary applies the full alive-secondary fact set (a `PeerJoined`,
/// a `SecondaryCapacity`, and a `RunComplete`), then a fresh node is
/// seeded PURELY from `snapshot()` then `restore()` (the
/// bootstrap-relocation / promotion path that bypasses `handle_welcome`).
/// After restore the downstream consumers the operational loop arms
/// fleet-dead on MUST read honest values: `is_peer_alive(secondary)`
/// true, `alive_secondary_members()` non-empty, a positive
/// `alive_remote_secondary_count()` (a `current_primary` is set so the
/// `id` vs `current_primary` filter is genuinely exercised), and
/// `run_complete()` true.
///
/// Without the `alive_members` projection (part 1 of the fix),
/// `peer_state` would stay empty after restore, so `is_peer_alive` is
/// false for all ids, `alive_secondary_members()` is empty, and
/// `alive_remote_secondary_count()` is a FALSE ZERO from tick 0 — the
/// exact condition that fires fleet-dead at 30s while remote
/// secondaries are alive. This test fails on that omission.
#[test]
fn consumer_invariants_survive_snapshot_restore() {
    use dynrunner_core::{ResourceAmount, ResourceKind};

    let mem = |amount: u64| ResourceAmount {
        kind: ResourceKind::memory(),
        amount,
    };

    let mut s = ClusterState::<RunnerIdentifier>::new();
    // A co-located primary + a remote worker-secondary, exactly the
    // bootstrap-relocation roster.
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "primary".into(),
        is_observer: false,
        can_be_primary: true,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-remote".into(),
        is_observer: false,
        can_be_primary: false,
    });
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-remote".into(),
        worker_count: 4,
        resources: vec![mem(2 * 1024 * 1024 * 1024)],
    });
    // The co-located host is the recognized primary so the
    // `id != current_primary` cut in `alive_remote_secondary_count`
    // is genuinely exercised.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "primary".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    s.apply(ClusterMutation::RunComplete);
    s.apply(ClusterMutation::RunAborted {
        reason: "abort-reason".into(),
    });

    // Seed a FRESH empty node purely from the snapshot — the
    // bootstrap-relocation / promotion path.
    let mut relocated = ClusterState::<RunnerIdentifier>::new();
    relocated.restore(s.snapshot());

    assert!(
        relocated.is_peer_alive("sec-remote"),
        "alive membership must survive snapshot/restore"
    );
    let members: HashSet<&str> = relocated.alive_secondary_members().collect();
    assert_eq!(members, HashSet::from(["sec-remote"]));
    assert_eq!(
        relocated.alive_remote_secondary_count(),
        1,
        "honest remote-secondary count post-restore (false-zero is the bug)"
    );
    assert!(
        relocated.run_complete(),
        "run_complete must survive restore"
    );
    assert_eq!(
        relocated.run_aborted(),
        Some("abort-reason"),
        "run_aborted reason must survive restore"
    );
}

/// Dead-wins / sticky-removal merge on the `alive_members` field,
/// mirroring `apply_peer.rs`: restoring an alive id over a local
/// `Dead` peer keeps it Dead; restoring over an absent id inserts
/// Alive; the merge is idempotent on repeat. The inserted entry's
/// `is_observer` is reconstructed from the co-restored observer set.
#[test]
fn restore_alive_members_dead_wins_and_idempotent() {
    // Source (the snapshotting primary) sees both ids alive, one of
    // them an observer.
    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: false,
    });
    source.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-b".into(),
        is_observer: true,
        can_be_primary: false,
    });
    let snap = source.snapshot();

    // Local node has already locally removed "sec-a" (sticky Dead) and
    // never seen "obs-b".
    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: false,
    });
    local.apply(ClusterMutation::PeerRemoved {
        id: "sec-a".into(),
        cause: RemovalCause::KeepaliveMiss,
    });

    local.restore(snap.clone());
    // Dead wins: the snapshot's Alive "sec-a" does NOT resurrect the
    // locally-removed id.
    assert!(
        !local.is_peer_alive("sec-a"),
        "sticky-removal: local Dead is never overwritten by a snapshot Alive"
    );
    // Absent id is inserted Alive, with its observer flag reconstructed
    // from the co-restored observer set.
    assert!(local.is_peer_alive("obs-b"), "absent id inserted Alive");
    assert!(
        local.role_table().observers.contains("obs-b"),
        "observer flag transfers cohesively with alive membership"
    );

    // Idempotent: a second restore of the same snapshot changes nothing.
    local.restore(snap);
    assert!(!local.is_peer_alive("sec-a"));
    assert!(local.is_peer_alive("obs-b"));
}
