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
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "i".into(),
        task: mk_task("i"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
        hash: "i".into(),
        secondary: "s1".into(),
        worker: 7,
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
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
        def_id: None,
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
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
        def_id: None,
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

/// A terminal `SkippedAlreadyDone` entry survives a snapshot → restore
/// cycle onto a fresh joiner: it rides the snapshot's `tasks` map
/// automatically (no new snapshot field), restore routes it through the
/// shared `merge_task_state` join, and it lands as `SkippedAlreadyDone`
/// (its `to_completed_event → None` so restore fires no spurious
/// completion). A stale peer's later Pending snapshot must NOT overwrite it
/// (a skip is terminal — it out-ranks any non-terminal in the join band).
#[test]
fn snapshot_round_trip_preserves_skipped_already_done() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "skip".into(),
        task: mk_task("skip"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskSkippedAlreadyDone {
        hash: "skip".into(),
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    assert!(
        matches!(
            joiner.task_state("skip"),
            Some(TaskState::SkippedAlreadyDone { .. })
        ),
        "the skip survives snapshot/restore"
    );
    assert_eq!(joiner.counts(), s.counts());
    assert_eq!(joiner.counts().skipped_already_done, 1);

    // A stale Pending snapshot must NOT overwrite the terminal skip.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.apply(ClusterMutation::TaskAdded {
        hash: "skip".into(),
        task: mk_task("skip"),
        def_id: None,
    });
    joiner.restore(stale.snapshot());
    assert!(
        matches!(
            joiner.task_state("skip"),
            Some(TaskState::SkippedAlreadyDone { .. })
        ),
        "terminal SkippedAlreadyDone must win over a stale Pending snapshot"
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
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-2".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
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

/// C6: `restore` MERGES the capability 2P-set rather than the old
/// "first-bootstrap-replace, else keep local" workaround. A joiner that
/// already observed one observer via a live `PeerJoined` and then restores
/// a snapshot carrying a DIFFERENT observer ends up with BOTH — the proper
/// grow-set union, not a clobber-or-ignore. (The old keep-local-else-
/// replace branch was a band-aid for the broken merge; the 2P-set unions
/// the capability entries and the projection re-derives both observers.)
#[test]
fn restore_merges_capability_2p_set_unioning_observers() {
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::PeerJoined {
        peer_id: "live-obs".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::PeerJoined {
        peer_id: "snap-obs".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });

    joiner.restore(peer.snapshot());
    // The 2P-set union: BOTH observers are present after restore (each was
    // alive on its origin, so each rode `alive_members` into the joiner and
    // projects in). No clobber, no ignore — a proper CRDT merge.
    assert_eq!(
        joiner.role_table().observers,
        HashSet::from(["live-obs".to_string(), "snap-obs".to_string()])
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
        def_id: None,
    });
    joiner.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: None,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
        def_id: None,
    });
    peer.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
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
        def_id: None,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
        def_id: None,
    });
    peer.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
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
        def_id: None,
    });
    peer.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
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
        def_id: None,
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
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
        def_id: None,
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
        def_id: None,
    });
    let snap = source.snapshot();

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    let restored = match joiner.task_state("h") {
        Some(state @ TaskState::Pending { .. }) => state.to_task_info(),
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
/// `alive_worker_secondary_count()`, and `run_complete()` true.
///
/// Without the `alive_members` projection (part 1 of the fix),
/// `peer_state` would stay empty after restore, so `is_peer_alive` is
/// false for all ids, `alive_secondary_members()` is empty, and
/// `alive_worker_secondary_count()` is a FALSE ZERO from tick 0 — the
/// exact condition that fires fleet-dead at 30s while
/// secondaries are alive. This test fails on that omission.
#[test]
fn consumer_invariants_survive_snapshot_restore() {
    use dynrunner_core::{ResourceAmount, ResourceKind};

    let mem = |amount: u64| ResourceAmount {
        kind: ResourceKind::memory(),
        amount,
    };

    let mut s = ClusterState::<RunnerIdentifier>::new();
    // A same-peer primary+secondary host + a remote worker-secondary,
    // exactly the bootstrap-promotion roster.
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "setup".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-remote".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-remote".into(),
        worker_count: 4,
        resources: vec![mem(2 * 1024 * 1024 * 1024)],
    });
    // The same-peer host is the recognized primary (the production
    // promoted shape; the count is identity-blind, so the recognized
    // primary's id is immaterial to it).
    s.apply(ClusterMutation::PrimaryChanged {
        new: "setup".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    s.apply(ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 5,
            fail_final: 2,
            ..Default::default()
        },
    });
    s.apply(ClusterMutation::RunAborted {
        reason: "abort-reason".into(),
        counts: Default::default(),
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
        relocated.alive_worker_secondary_count(),
        1,
        "honest worker-secondary count post-restore (false-zero is the bug)"
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
    // #513 — the verdict's carried counts ride the snapshot ALONGSIDE the run
    // latches, so a node seeded purely from a snapshot (the late-joiner /
    // promotion path) narrates the SAME authoritative terminal partition.
    let counts = relocated
        .terminal_outcome()
        .expect("the verdict counts must survive snapshot/restore with the latch");
    assert_eq!(counts.succeeded, 5);
    assert_eq!(counts.fail_final, 2);
}

/// Dead-wins / sticky-removal merge on the `alive_members` field,
/// mirroring `apply_peer.rs`: restoring an alive id over a local
/// `Dead` peer keeps it Dead; restoring over an absent id inserts
/// Alive; the merge is idempotent on repeat. The observer ROLE rides
/// the separate `capabilities` 2P-set (C6) and the `role_table().observers`
/// projection re-derives from `capability × alive` after both merge.
#[test]
fn restore_alive_members_dead_wins_and_idempotent() {
    // Source (the snapshotting primary) sees both ids alive, one of
    // them an observer.
    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    source.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-b".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    let snap = source.snapshot();

    // Local node has already locally removed "sec-a" (sticky Dead) and
    // never seen "obs-b".
    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    local.apply(ClusterMutation::PeerRemoved {
        id: "sec-a".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });

    local.restore(snap.clone());
    // Dead wins: the snapshot's Alive "sec-a" does NOT resurrect the
    // locally-removed id.
    assert!(
        !local.is_peer_alive("sec-a"),
        "sticky-removal: local Dead is never overwritten by a snapshot Alive"
    );
    // Absent id is inserted Alive; its observer ROLE comes from the
    // co-restored `capabilities` 2P-set, and the projection composes it
    // with the (now-Alive) liveness bit.
    assert!(local.is_peer_alive("obs-b"), "absent id inserted Alive");
    assert!(
        local.role_table().observers.contains("obs-b"),
        "observer role projects in once the capability + alive bit converge"
    );

    // Idempotent: a second restore of the same snapshot changes nothing.
    local.restore(snap);
    assert!(!local.is_peer_alive("sec-a"));
    assert!(local.is_peer_alive("obs-b"));
}

/// The `capabilities` 2P-set round-trips through the snapshot WIRE (serde
/// JSON, the same path `RequestClusterSnapshot`/`ClusterSnapshot` use): an
/// `Advertised` entry and a `Departed` tombstone both survive
/// encode→decode verbatim (C6).
#[test]
fn snapshot_wire_round_trips_capabilities_2p_set() {
    use dynrunner_core::TaskVersion;

    let mut origin = ClusterState::<RunnerIdentifier>::new();
    origin.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 1,
            seq: 2,
        },
        member_gen: 0,
    });
    origin.apply(ClusterMutation::PeerJoined {
        peer_id: "gone".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
        member_gen: 0,
    });
    origin.apply(ClusterMutation::PeerRemoved {
        id: "gone".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });

    // Encode → decode the snapshot exactly as the wire path does.
    let snap = origin.snapshot();
    let json = serde_json::to_string(&snap).unwrap();
    let decoded: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&json).unwrap();

    assert_eq!(
        decoded.capabilities.get("obs-1"),
        Some(&crate::cluster_state::CapabilityEntry::Advertised {
            is_observer: true,
            can_be_primary: false,
            cap_version: TaskVersion {
                primary_epoch: 1,
                seq: 2
            },
            member_gen: 0,
        })
    );
    assert_eq!(
        decoded.capabilities.get("gone"),
        Some(&CapabilityEntry::Departed {
            member_gen: 0,
            // The tombstone PRESERVES the advertisement current at
            // departure (the re-admission lattice restores it).
            is_observer: false,
            can_be_primary: true,
            cap_version: TaskVersion {
                primary_epoch: 1,
                seq: 1
            },
        }),
        "the Departed tombstone must survive the snapshot wire round-trip"
    );
}

/// #358 wire regression: a snapshot whose TUPLE-keyed grow-MAX maps (F4
/// `phase_event_tallies`, P3 `retry_passes_used`) are NON-EMPTY must
/// serialize to JSON and round-trip. Pre-fix the plain `HashMap<(K1, K2),
/// u32>` fields made `serde_json::to_string(&snapshot)` ERROR ("key must
/// be a string") the moment a tally / used-count existed — every snapshot
/// responder then warn-and-DROPPED its reply, silently breaking the
/// late-joiner / anti-entropy heal path on any run past its first
/// terminal event. The `tuple_keyed_map` pair-list encoding is the fix;
/// this pins encode → decode → restore end-to-end.
#[test]
fn snapshot_wire_round_trips_non_empty_tuple_keyed_grow_max_maps() {
    use crate::cluster_state::PhaseTally;
    use crate::primary::retry_bucket::BucketKind;

    let mut origin = ClusterState::<RunnerIdentifier>::new();
    let p = PhaseId::from("p0");
    origin.record_phase_event_tally((p.clone(), PhaseTally::Completed), 6);
    origin.record_phase_event_tally((p.clone(), PhaseTally::Failed), 2);
    origin.record_retry_pass_used((p.clone(), BucketKind::Recoverable), 1);

    // Encode → decode the snapshot exactly as the wire path does.
    let snap = origin.snapshot();
    let json = serde_json::to_string(&snap)
        .expect("a snapshot with non-empty tuple-keyed maps must serialize");
    let decoded: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&json).expect("wire snapshot decodes");
    assert_eq!(
        decoded
            .phase_event_tallies
            .get(&(p.clone(), PhaseTally::Completed)),
        Some(&6)
    );
    assert_eq!(
        decoded
            .phase_event_tallies
            .get(&(p.clone(), PhaseTally::Failed)),
        Some(&2)
    );
    assert_eq!(
        decoded
            .retry_passes_used
            .get(&(p.clone(), BucketKind::Recoverable)),
        Some(&1)
    );

    // And the decoded snapshot restores (max-merge) into a cold replica.
    let mut cold = ClusterState::<RunnerIdentifier>::new();
    cold.restore(decoded);
    assert_eq!(
        cold.phase_event_tally_for(&(p.clone(), PhaseTally::Completed)),
        6
    );
    assert_eq!(cold.phase_event_tally_for(&(p.clone(), PhaseTally::Failed)), 2);
    assert_eq!(cold.retry_pass_used_for(&(p, BucketKind::Recoverable)), 1);
}

/// Backward-compat: a snapshot from a sender that PREDATES the
/// `capabilities` field (its JSON omits the key entirely) must decode with
/// an EMPTY capability map (`#[serde(default)]`), not a missing-field
/// error. Mirrors the legacy wire BYTES (the omitted field), not a
/// symmetric round-trip of the new shape.
#[test]
fn legacy_snapshot_without_capabilities_decodes_empty() {
    // Hand-build the legacy wire shape: every field a pre-`capabilities`
    // sender emitted, with NO `capabilities` key.
    let legacy = serde_json::json!({
        "tasks": {},
        "current_primary": "primary-x",
        "primary_epoch": 4,
        "phase_deps": {},
        "peer_holdings": {},
        "task_outputs": {},
        "secondary_capacities": {},
        "alive_members": [],
        "run_complete": false,
        "run_aborted": null
    });
    let decoded: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&legacy.to_string()).unwrap();
    assert!(
        decoded.capabilities.is_empty(),
        "a pre-field snapshot must decode capabilities as an empty map"
    );
    // The rest of the legacy fields still decode (sanity that the shape is
    // otherwise the current one).
    assert_eq!(decoded.current_primary.as_deref(), Some("primary-x"));
    assert_eq!(decoded.primary_epoch, 4);
}

/// Snapshot-pull RE-ADMISSION heal: a replica still holding a peer
/// `Dead` at generation 0 (it missed the re-admitting `PeerJoined`)
/// restores a snapshot from a healed node holding the peer `Alive` at
/// generation 1 — the strictly-higher generation re-admits the peer on
/// the puller too (pre-fix the Dead-wins vacant-only merge kept the
/// stale tombstone forever). The reverse direction stays sticky: a
/// stale snapshot (gen-0 Alive) cannot re-bury OR resurrect anything on
/// the healed node.
#[test]
fn snapshot_restore_readmits_lower_generation_dead_entry() {
    let join = |generation: u64| ClusterMutation::<RunnerIdentifier>::PeerJoined {
        peer_id: "sec-2".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: generation,
    };

    // The HEALED node: removed at gen 0, re-admitted at gen 1.
    let mut healed = ClusterState::<RunnerIdentifier>::new();
    healed.apply(join(0));
    healed.apply(ClusterMutation::PeerRemoved {
        id: "sec-2".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    healed.apply(join(1));
    assert!(healed.is_peer_alive("sec-2"));
    assert_eq!(healed.peer_member_gen("sec-2"), 1);

    // The STALE replica: observed the removal, missed the re-admission.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    stale.apply(join(0));
    stale.apply(ClusterMutation::PeerRemoved {
        id: "sec-2".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    assert!(!stale.is_peer_alive("sec-2"));

    // Heal pull: the stale replica restores the healed node's snapshot
    // (through the wire encode→decode, the same path the snapshot RPC
    // uses).
    let json = serde_json::to_string(&healed.snapshot()).unwrap();
    let snap: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&json).unwrap();
    stale.restore(snap);
    assert!(
        stale.is_peer_alive("sec-2"),
        "the strictly-higher-generation Alive must re-admit the stale Dead"
    );
    assert_eq!(stale.peer_member_gen("sec-2"), 1);
    assert!(
        stale.role_table().can_be_primary.contains("sec-2"),
        "the capability must heal through the generation-first 2P-set merge"
    );

    // Reverse direction: a stale replica's snapshot (now taken from a
    // replica that ONLY saw the gen-0 join) cannot regress the healed
    // node — same-or-lower generation keeps the local entry.
    let mut never_saw_removal = ClusterState::<RunnerIdentifier>::new();
    never_saw_removal.apply(join(0));
    let stale_snap = never_saw_removal.snapshot();
    healed.restore(stale_snap);
    assert!(healed.is_peer_alive("sec-2"));
    assert_eq!(
        healed.peer_member_gen("sec-2"),
        1,
        "a lower-generation snapshot never regresses the incarnation"
    );
}

// ── L4.5: self-describing def store survives snapshot/restore ──

/// A def interned at a primary-allocated id, snapshotted, and restored onto
/// a FRESH replica REBUILDS the def store from the self-describing inline
/// def: `resolve(def_id)` yields the SAME id + content, and the hash↔id
/// bijection is re-established (`id_for_hash` round-trips). The snapshot
/// drops the def store as a wire field, so without the self-describing
/// rebuild the restored replica would hold the def CONTENT (inline in the
/// `TaskState`) but no id↔def / hash↔id binding — a dangling def_id ref.
#[test]
fn snapshot_restore_rebuilds_self_describing_def_store() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("the-task"),
        def_id: Some(7),
    });
    // Sanity: the originator bound id 7 and the def carries it (stamped at
    // intern time).
    assert_eq!(s.def_id_for_hash_for_test("h"), Some(TaskDefId(7)));
    assert_eq!(
        s.resolve_def_for_test(TaskDefId(7)).map(|d| d.def_id),
        Some(TaskDefId(7)),
        "the originator's def is self-describing"
    );

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    // The fresh joiner starts with an EMPTY def store — nothing resolves.
    assert!(joiner.resolve_def_for_test(TaskDefId(7)).is_none());
    joiner.restore(snap);

    // id→def REBUILT: resolve(7) yields the same def + content.
    let restored = joiner
        .resolve_def_for_test(TaskDefId(7))
        .expect("restore rebuilds the id→def binding from the inline def");
    assert_eq!(restored.def_id, TaskDefId(7));
    assert_eq!(restored.task_id, "the-task");
    // hash↔id BIJECTION rebuilt: the content hash resolves back to the SAME
    // id (the `intern_at` on restore re-recorded the binding).
    assert_eq!(joiner.def_id_for_hash_for_test("h"), Some(TaskDefId(7)));
}

/// A bijection collision on restore is the loud-but-SAFE graceful degrade:
/// a snapshot whose def carries an id ALREADY bound to a DIFFERENT hash on
/// the joiner cannot rebind it (a converged registry never produces one;
/// a failover cross-epoch transient can). The restore logs LOUD, KEEPS the
/// existing id binding, and re-anchors the conflicting def by CONTENT so it
/// still resolves by hash — the def is never lost (its content round-trips
/// via the inline `TaskState`).
#[test]
fn snapshot_restore_def_id_collision_keeps_local_and_reanchors_by_content() {
    // Joiner already bound id 5 to hash "local".
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::TaskAdded {
        hash: "local".into(),
        task: mk_task("local-task"),
        def_id: Some(5),
    });

    // A peer snapshot binds the SAME id 5 to a DIFFERENT hash — the
    // bijection-violating restore.
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::TaskAdded {
        hash: "other".into(),
        task: mk_task("other-task"),
        def_id: Some(5),
    });
    joiner.restore(peer.snapshot());

    // id 5 stays bound to the LOCAL def; the collision never rebound it.
    assert_eq!(joiner.def_id_for_hash_for_test("local"), Some(TaskDefId(5)));
    assert_eq!(
        joiner
            .resolve_def_for_test(TaskDefId(5))
            .map(|d| d.task_id.clone()),
        Some("local-task".into()),
        "the colliding restore never rebound id 5 to the peer's def"
    );
    // The conflicting def is NOT lost — re-anchored by content under a
    // fresh local id, so it still resolves by hash.
    let other_id = joiner
        .def_id_for_hash_for_test("other")
        .expect("the colliding def is re-anchored by content, not dropped");
    assert_ne!(other_id, TaskDefId(5), "it took a fresh local id, not 5");
    assert_eq!(
        joiner.resolve_def_for_test(other_id).map(|d| d.task_id.clone()),
        Some("other-task".into()),
    );
}

/// Adding the self-describing `def_id` field must NOT make the def store
/// contribute to the convergence digest (CL-A2): two replicas that bound
/// the SAME content to DIFFERENT def ids still produce the SAME digest,
/// because the digest folds the `tasks` join-key projection (which never
/// reads the def content), not the def store. A def's presence / its id is
/// implied by the tasks fold via the content-based join key — folding the
/// id would double-count and diverge anti-entropy.
#[test]
fn def_id_does_not_contribute_to_digest() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("same-content"),
        def_id: Some(3),
    });
    let mut b = ClusterState::<RunnerIdentifier>::new();
    // SAME content (same hash + task), DIFFERENT def id.
    b.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("same-content"),
        def_id: Some(99),
    });

    assert_ne!(
        a.def_id_for_hash_for_test("h"),
        b.def_id_for_hash_for_test("h"),
        "the two replicas bound the same content to different ids"
    );
    assert_eq!(
        a.digest(),
        b.digest(),
        "the def id must not perturb the convergence digest"
    );
}
