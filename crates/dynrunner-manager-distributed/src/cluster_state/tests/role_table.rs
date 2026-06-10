//! Tests for the replicated `RoleTable` and the role-change hook
//! fan-out.
//!
//! Single concern: pin the Step 2 contract that every `PrimaryChanged`
//! and every `PeerJoined`/`PeerRemoved` mutation that actually changes
//! the table fires every registered hook against the post-mutation
//! `RoleTable` — and that NoOp paths (lower epoch, duplicate at same
//! epoch, observer-already-in-set) MUST NOT fire hooks. The
//! transport-side write-through cache relies on this contract.

use super::*;

// ── RoleTable + role-change hook tests ──
//
// These pin the Step 2 contract: every `PrimaryChanged` that
// returns `Applied` mutates the replicated `RoleTable` AND fires
// every registered hook against the post-mutation table — never
// the pre-mutation snapshot. NoOp paths (lower epoch, same value
// at same epoch) must NOT fire hooks; otherwise a transport-side
// cache could observe spurious updates on idempotent re-delivery.

/// `PrimaryChanged` mutation updates the replicated `RoleTable`
/// in lockstep with `current_primary`. Pins the cross-field
/// invariant the transport-side cache will rely on.
#[test]
fn role_table_updates_on_primary_changed() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.role_table().primary, None);

    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-2".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(s.role_table().primary, Some("sec-2".to_string()));

    // Higher epoch wins → table tracks the new holder.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-7".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(s.role_table().primary, Some("sec-7".to_string()));

    // Lower epoch is a NoOp and must NOT regress the table.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-stale".into(),
        epoch: 2,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(s.role_table().primary, Some("sec-7".to_string()));
}

/// Hook callbacks fire AFTER each `Applied` `PrimaryChanged`,
/// observing the post-mutation `RoleTable`. NoOp applies (lower
/// epoch / duplicate at same epoch) must NOT fire the hook —
/// the transport cache would otherwise see spurious updates on
/// idempotent re-delivery and could trigger needless cache
/// invalidation downstream.
#[test]
fn role_change_hook_fires_after_apply() {
    use std::sync::Mutex;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let observed: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed);
        s.register_role_change_hook(Box::new(move |table: &RoleTable| {
            observed.lock().unwrap().push(table.primary.clone());
        }));
    }

    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-b".into(),
        epoch: 2,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-c".into(),
        epoch: 3,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });

    // Three Applied mutations → three callback fires, in order.
    let obs = observed.lock().unwrap().clone();
    assert_eq!(
        obs,
        vec![
            Some("sec-a".to_string()),
            Some("sec-b".to_string()),
            Some("sec-c".to_string())
        ],
    );

    // A NoOp re-delivery (same holder at same epoch) does NOT
    // fire the hook.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-c".into(),
        epoch: 3,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    let obs_after_noop = observed.lock().unwrap().clone();
    assert_eq!(obs_after_noop.len(), 3, "NoOp must not fire hook");
}

/// `ClusterMutation::PeerJoined { is_observer: true }` inserts
/// the peer into the replicated observer set with set semantics
/// (idempotent) and fires role-change hooks when (and only when)
/// the set actually changes. Pins the "observer-set replicated
/// through RoleTable" contract that election filtering and the
/// PrimaryChanged observer-rejection defense both rely on, now
/// flowing through the single-writer CRDT apply path.
#[test]
fn peer_joined_observer_inserts_into_role_table_and_fires_hooks_on_change() {
    use std::sync::Mutex;

    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.role_table().observers.is_empty());

    let observed: Arc<Mutex<Vec<HashSet<String>>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed);
        s.register_role_change_hook(Box::new(move |table: &RoleTable| {
            observed.lock().unwrap().push(table.observers.clone());
        }));
    }

    // First insert fires the hook with the new set.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.role_table().observers,
        HashSet::from(["obs-1".to_string()])
    );

    // Re-apply the same `PeerJoined { is_observer: true }`:
    // set-semantics NoOp, no hook fire.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::NoOp
    );

    // Add a second observer: hook fires with the union.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-2".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.role_table().observers,
        HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
    );

    // Hook history: 2 actual changes (two distinct inserts);
    // the duplicate `PeerJoined` was a silent NoOp.
    let obs = observed.lock().unwrap().clone();
    assert_eq!(obs.len(), 2, "expected 2 fires, got {}", obs.len());
    assert_eq!(obs[0], HashSet::from(["obs-1".to_string()]));
    assert_eq!(
        obs[1],
        HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
    );
}

/// `ClusterMutation::PeerJoined { is_observer: false }` for a peer
/// already in `RoleTable.observers` MUST NOT regress the projection
/// (only `PeerRemoved` may remove peers from the set). A first-seen
/// non-observer peer is recorded in `peer_state` — that is the
/// widened apply rule's tracking contract — but the observer set
/// stays untouched. This pins the "stale flip-back does not regress
/// the observer set" guarantee the receiver-side relies on.
#[test]
fn peer_joined_non_observer_does_not_remove_existing_observer() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert!(s.role_table().observers.contains("obs-1"));

    // `is_observer: false` for an already-Alive observer is a
    // NoOp under the non-regression rule — neither peer_state nor
    // the observer projection mutate.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::NoOp
    );
    assert!(
        s.role_table().observers.contains("obs-1"),
        "obs-1 must remain in role_table.observers (only PeerRemoved \
         removes peers from the projection)"
    );

    // A first-seen non-observer peer is now tracked in peer_state
    // (Applied), but does not enter the observer projection.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "never-joined".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert!(!s.role_table().observers.contains("never-joined"));
}

/// `restore` going through the snapshot-merge path also mutates
/// the `RoleTable` AND fires hooks when `current_primary` flips.
/// Pins the late-joiner / reconnect path; without this, a node
/// that learns its first primary identity via snapshot RPC
/// would leave the transport cache stuck at `None`.
#[test]
fn role_change_hook_fires_on_restore_when_primary_advances() {
    use std::sync::Mutex;
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    let observed: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed);
        joiner.register_role_change_hook(Box::new(move |table: &RoleTable| {
            observed.lock().unwrap().push(table.primary.clone());
        }));
    }

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::PrimaryChanged {
        new: "lead".into(),
        epoch: 7,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    joiner.restore(peer.snapshot());

    assert_eq!(joiner.role_table().primary, Some("lead".to_string()));
    let obs = observed.lock().unwrap().clone();
    assert_eq!(obs, vec![Some("lead".to_string())]);
}

// ── can_be_primary: explicit per-peer primary-capability ──
//
// These pin that capability is a SEPARATE first-class CRDT property:
// set at join (twin of `is_observer`), updatable at runtime via the
// dedicated mutation, and replicated through the snapshot. NOT deduced
// from membership / liveness / observer status.

/// A peer that joins with `can_be_primary = true` is recorded in the
/// `RoleTable.can_be_primary` set and visible through the
/// `can_be_primary(id)` accessor; a peer that joins with it `false`
/// is NOT capability-eligible (capability is explicit, not deduced
/// from membership).
#[test]
fn peer_joined_records_can_be_primary_capability() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    s.apply(ClusterMutation::PeerJoined {
        peer_id: "capable".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "incapable".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });

    assert!(
        s.can_be_primary("capable"),
        "a peer joining with can_be_primary=true must be capability-eligible"
    );
    assert!(
        !s.can_be_primary("incapable"),
        "a peer joining with can_be_primary=false must NOT be capability-eligible"
    );
    assert!(
        !s.can_be_primary("never-joined"),
        "an unknown peer is not capability-eligible (capability is explicit)"
    );
    assert!(s.role_table().can_be_primary.contains("capable"));
    assert!(!s.role_table().can_be_primary.contains("incapable"));
}

/// `SetCanBePrimary` updates a peer's capability at runtime (C6) — both
/// granting it and revoking it — arbitrated by the monotone `cap_version`
/// (a newer `false` beats an older `true`). The `can_be_primary(id)`
/// PROJECTION ANDs in the LOCAL alive bit, so the peer is JOINED first to
/// make it alive. (Capability is decoupled from membership at the APPLY
/// level — the merge does not gate on `peer_state` — but the read-side
/// projection requires liveness, so a runtime grant only becomes visible
/// for an alive peer.) Explicit increasing `cap_version`s model the
/// monotone stamp the origination choke point mints in production.
#[test]
fn set_can_be_primary_updates_capability_at_runtime() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Join `p` (alive) so the projection can include it once granted.
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 0,
            seq: 1,
        },
        member_gen: 0,
    });
    assert!(!s.can_be_primary("p"));

    // Grant at runtime (higher cap_version).
    let granted = s.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "p".into(),
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 0,
            seq: 2,
        },
    });
    assert_eq!(granted, ApplyOutcome::Applied);
    assert!(s.can_be_primary("p"));

    // Re-applying the SAME (value, version) is a NoOp (idempotent merge).
    let again = s.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "p".into(),
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 0,
            seq: 2,
        },
    });
    assert_eq!(again, ApplyOutcome::NoOp);

    // Revoke at runtime (a STILL-HIGHER cap_version beats the grant).
    let revoked = s.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "p".into(),
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 0,
            seq: 3,
        },
    });
    assert_eq!(revoked, ApplyOutcome::Applied);
    assert!(!s.can_be_primary("p"));

    // A STALE revoke (lower version than the current entry) loses → NoOp.
    let stale = s.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "p".into(),
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 0,
            seq: 1,
        },
    });
    assert_eq!(stale, ApplyOutcome::NoOp);
}

/// The primary-capability 2P-set round-trips through a snapshot: a
/// freshly-restored late-joiner picks up the originator's converged
/// `capabilities` on `restore` (C6). The `can_be_primary(id)` PROJECTION
/// ANDs in the LOCAL alive bit — so a joined-and-alive capable peer
/// projects in, while a pre-armed capability for a never-joined (not
/// alive) peer is carried in the 2P-set but does NOT project until the
/// peer is also alive.
#[test]
fn snapshot_round_trips_capability_2p_set() {
    let mut origin = ClusterState::<RunnerIdentifier>::new();
    // compute-a JOINS (alive) with capability → projects into can_be_primary.
    origin.apply(ClusterMutation::PeerJoined {
        peer_id: "compute-a".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    // compute-b is pre-armed via SetCanBePrimary WITHOUT a join → the
    // capability is held in the 2P-set but it is not alive, so it does NOT
    // project on the originator either.
    origin.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "compute-b".into(),
        can_be_primary: true,
        cap_version: Default::default(),
    });
    assert!(origin.can_be_primary("compute-a"));
    assert!(
        !origin.can_be_primary("compute-b"),
        "a pre-armed capability for a never-joined (not-alive) peer must not project"
    );

    let snap = origin.snapshot();
    // The 2P-set carries BOTH capability entries (the replicated truth);
    // the projection liveness AND is applied at read time, not in storage.
    assert!(snap.capabilities.contains_key("compute-a"));
    assert!(snap.capabilities.contains_key("compute-b"));

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    assert!(!joiner.can_be_primary("compute-a"));
    joiner.restore(snap);

    // After restore the joiner has converged the 2P-set AND the alive
    // membership (compute-a was alive on origin, so it rode `alive_members`),
    // so compute-a projects in; compute-b (never alive) does not.
    assert!(
        joiner.can_be_primary("compute-a"),
        "snapshot restore must converge the capability 2P-set + alive bit"
    );
    assert!(
        !joiner.can_be_primary("compute-b"),
        "a never-alive pre-armed capability stays held but unprojected after restore"
    );
}

/// `PeerRemoved` clears a peer's primary-capability — the exact twin of
/// the observer projection. A dead id never resurrects, so it must not
/// linger in the capability set.
#[test]
fn peer_removed_clears_can_be_primary() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "doomed".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert!(s.can_be_primary("doomed"));

    s.apply(ClusterMutation::PeerRemoved {
        id: "doomed".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    assert!(
        !s.can_be_primary("doomed"),
        "a removed peer must be dropped from the capability set"
    );
}
