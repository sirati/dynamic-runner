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
    });
    assert_eq!(s.role_table().primary, Some("sec-2".to_string()));

    // Higher epoch wins → table tracks the new holder.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-7".into(),
        epoch: 5,
    });
    assert_eq!(s.role_table().primary, Some("sec-7".to_string()));

    // Lower epoch is a NoOp and must NOT regress the table.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-stale".into(),
        epoch: 2,
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
    });
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-b".into(),
        epoch: 2,
    });
    s.apply(ClusterMutation::PrimaryChanged {
        new: "sec-c".into(),
        epoch: 3,
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
    });
    let obs_after_noop = observed.lock().unwrap().clone();
    assert_eq!(obs_after_noop.len(), 3, "NoOp must not fire hook");
}

/// `ClusterMutation::PeerJoined { is_observer: true }` inserts
/// the peer into the replicated observer set with set semantics
/// (idempotent) and fires role-change hooks when (and only when)
/// the set actually changes. Pins the "observer-set replicated
/// through RoleTable" contract that election filtering and
/// PromotePrimary defense both rely on, now flowing through the
/// single-writer CRDT apply path.
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
        }),
        ApplyOutcome::NoOp
    );

    // Add a second observer: hook fires with the union.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-2".into(),
            is_observer: true,
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
    });
    joiner.restore(peer.snapshot());

    assert_eq!(joiner.role_table().primary, Some("lead".to_string()));
    let obs = observed.lock().unwrap().clone();
    assert_eq!(obs, vec![Some("lead".to_string())]);
}
