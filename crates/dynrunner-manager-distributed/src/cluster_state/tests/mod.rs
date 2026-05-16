//! Tests for the `cluster_state` CRDT.
//!
//! Single concern: pin the per-mutation apply semantics, the snapshot
//! /restore lattice merge, the peer-lifecycle role-table projection,
//! the dispatcher-channel emit boundaries, and the per-peer resource-
//! holdings round-trip.

use super::*;
use dynrunner_core::{ErrorType, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, RemovalCause, RoleChangeHookRegistrar, RoleTable,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

mod apply_basics;
mod snapshot;

pub(super) fn mk_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    TaskInfo {
        path: PathBuf::from(format!("/tasks/{name}")),
        size: 0,
        identifier: RunnerIdentifier::from(name),
        phase_id: PhaseId::from("p0"),
        type_id: TypeId::from("t0"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some(name.into()),
        task_depends_on: Vec::new(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}



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

#[test]
fn task_preferred_secondaries_updated_apply_writes_to_task() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "h".into(),
            secondaries: vec!["secondary-2".into(), "secondary-5".into()],
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Pending { task }) = s.task_state("h") else {
        panic!("expected Pending");
    };
    assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-2", "secondary-5"]);
}

#[test]
fn task_preferred_secondaries_updated_apply_unknown_hash_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "nope".into(),
            secondaries: vec!["secondary-1".into()],
        }),
        ApplyOutcome::NoOp
    );
}

#[test]
fn task_preferred_secondaries_updated_apply_preserves_state() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "h".into(),
            secondaries: vec!["secondary-7".into()],
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Unfulfillable { task, reason }) = s.task_state("h") else {
        panic!("state must stay Unfulfillable across preferred-secondaries update");
    };
    assert_eq!(reason, "missing");
    assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-7"]);
}

// ── Discrete Unfulfillable / Blocked state pins ──

/// `TaskFailed { kind: ErrorType::Unfulfillable, .. }` lands in the
/// discrete `TaskState::Unfulfillable { reason, task }` variant,
/// NOT in `TaskState::Failed { kind: Unfulfillable, .. }`. The
/// `reason` field carries the inner `BoundedString` body verbatim
/// (stored as `String` in the in-memory ledger).
#[test]
fn task_failed_with_unfulfillable_lands_in_unfulfillable_variant() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing toolchain xyz".to_string().into(),
            },
            error: "unfulfillable".into(),
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("h") {
        Some(TaskState::Unfulfillable { reason, .. }) => {
            assert_eq!(reason, "missing toolchain xyz");
        }
        other => panic!("expected Unfulfillable, got {other:?}"),
    }
}

/// Regression pin for the dispatcher in the `TaskFailed` apply
/// arm: generic non-recoverable errors must still land in
/// `TaskState::Failed`, NOT in `Unfulfillable`. Pins that the
/// kind-based routing only fires for `Unfulfillable` and every
/// other `ErrorType` keeps the legacy shape.
#[test]
fn task_failed_with_generic_nonrecoverable_lands_in_failed_variant() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
    });
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Failed { kind: ErrorType::NonRecoverable, .. })
    ));
    // And Recoverable also stays in Failed (sanity check the
    // dispatcher routes ONLY Unfulfillable to the new variant).
    let mut s2 = ClusterState::<RunnerIdentifier>::new();
    s2.apply(ClusterMutation::TaskAdded {
        hash: "h2".into(),
        task: mk_task("h2"),
    });
    s2.apply(ClusterMutation::TaskFailed {
        hash: "h2".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
    });
    assert!(matches!(
        s2.task_state("h2"),
        Some(TaskState::Failed { kind: ErrorType::Recoverable, .. })
    ));
}

/// `ClusterMutation::TaskBlocked { hash, on }` lands a `Pending`
/// entry in `TaskState::Blocked { on, task }`. Pins the cascade
/// broadcast shape: dependents of an Unfulfillable prereq mirror
/// across every replica as Blocked (not Failed), carrying the
/// prereq's hash so auto-resume can identify them.
#[test]
fn cascade_on_unfulfillable_marks_dependents_blocked() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Prereq enters Unfulfillable.
    s.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    // Dependent enters Blocked-on-prereq via cascade broadcast.
    s.apply(ClusterMutation::TaskAdded {
        hash: "dep".into(),
        task: mk_task("dep"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskBlocked {
            hash: "dep".into(),
            on: "prereq".into(),
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("dep") {
        Some(TaskState::Blocked { on, .. }) => assert_eq!(on, "prereq"),
        other => panic!("expected Blocked, got {other:?}"),
    }
    // Re-apply against an already-Blocked entry with the same
    // `on` is a silent NoOp (idempotent under at-least-once
    // delivery).
    assert_eq!(
        s.apply(ClusterMutation::TaskBlocked {
            hash: "dep".into(),
            on: "prereq".into(),
        }),
        ApplyOutcome::NoOp
    );
}

// ── PeerRemoved + widened PeerJoined apply-rule tests ──
//
// These pin the peer-lifecycle contract on `ClusterState`:
//
//  1. `PeerRemoved` is sticky-per-id: once Dead, always Dead. A
//     duplicate broadcast is a NoOp; a late `PeerJoined` for the
//     same id is dropped with a warn log (no resurrection).
//  2. The observer-set projection is maintained in lockstep with
//     the `peer_state` map — removal of an observer drops them
//     from `RoleTable.observers`.

/// Local capture layer for warn-level tracing events. Scoped to
/// the cluster_state test module — we only need it for the
/// `peer_joined_dead_is_noop` warn-log assertion, so keep it
/// module-private rather than lifting into a shared test util.
struct WarnCapture {
    records: Arc<std::sync::Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        if event.metadata().target() != "dynrunner_cluster_state" {
            return;
        }
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.0 = value.to_string();
                }
            }
            fn record_debug(
                &mut self,
                field: &tracing::field::Field,
                value: &dyn std::fmt::Debug,
            ) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        if let Ok(mut buf) = self.records.lock() {
            buf.push(visitor.0);
        }
    }
}

/// Run `body` against a scoped subscriber that captures every
/// warn-level `dynrunner_cluster_state` event.
fn with_warn_capture<F, R>(body: F) -> (R, Vec<String>)
where
    F: FnOnce() -> R,
{
    use tracing_subscriber::layer::SubscriberExt;
    let records: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let layer = WarnCapture {
        records: Arc::clone(&records),
    };
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    let out = tracing::subscriber::with_default(subscriber, body);
    let captured = records.lock().unwrap().clone();
    (out, captured)
}

/// Idempotent removal: a second `PeerRemoved` for the same id is
/// a silent NoOp under sticky-per-id semantics.
#[test]
fn peer_removed_is_sticky() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
        }),
        ApplyOutcome::Applied
    );
    // Re-applying PeerRemoved for the same id is a silent NoOp —
    // the entry is already Dead.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::MassDeathEscalation,
        }),
        ApplyOutcome::NoOp
    );
}

/// `TaskCompleted` apply arm auto-resumes every Blocked dependent
/// whose `on` matches the completing hash back to `Pending`.
/// Event-driven: the same broadcast that converges the prereq to
/// Completed converges every blocked dependent to Pending in one
/// apply call across every replica.
#[test]
fn task_completed_auto_resumes_blocked_dependents() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Prereq landed Unfulfillable then was reinjected (Unfulfillable→Pending).
    s.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    s.apply(ClusterMutation::TaskReinjected { hash: "prereq".into() });
    // Two dependents Blocked-on-prereq.
    for h in ["d1", "d2"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
        });
        s.apply(ClusterMutation::TaskBlocked {
            hash: h.into(),
            on: "prereq".into(),
        });
    }
    // An unrelated Blocked-on-other-prereq dependent must NOT auto-resume.
    s.apply(ClusterMutation::TaskAdded {
        hash: "unrelated".into(),
        task: mk_task("unrelated"),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "unrelated".into(),
        on: "some-other-prereq".into(),
    });
    // Prereq completes — every Blocked-on-prereq entry resumes.
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted { hash: "prereq".into() }),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        s.task_state("d1"),
        Some(TaskState::Pending { .. })
    ));
    assert!(matches!(
        s.task_state("d2"),
        Some(TaskState::Pending { .. })
    ));
    // Unrelated stays Blocked — the auto-resume keys on the `on`
    // field, not blanket-resumes every Blocked entry.
    assert!(matches!(
        s.task_state("unrelated"),
        Some(TaskState::Blocked { .. })
    ));
}

/// `TaskReinjected` apply rule tightening: post-variant, only
/// `TaskState::Unfulfillable { .. }` transitions to `Pending`.
/// Other states (including the legacy `Failed { NonRecoverable, .. }`
/// the pre-variant matcher accepted) are NoOp.
#[test]
fn reinject_task_command_filters_to_unfulfillable_only() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Unfulfillable → Pending: accepted.
    s.apply(ClusterMutation::TaskAdded {
        hash: "u".into(),
        task: mk_task("u"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "u".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected { hash: "u".into() }),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        s.task_state("u"),
        Some(TaskState::Pending { .. })
    ));

    // Failed{NonRecoverable} → reinject: NoOp (pre-variant
    // matcher accepted this; the tightened rule rejects).
    s.apply(ClusterMutation::TaskAdded {
        hash: "f".into(),
        task: mk_task("f"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "f".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected { hash: "f".into() }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(
        s.task_state("f"),
        Some(TaskState::Failed { .. })
    ));
}


/// Sticky-per-id under the cross-direction race: once a peer is
/// Dead, a late `PeerJoined` for the same id is a NoOp and emits
/// a warn log. Respawn requires a fresh id.
#[test]
fn peer_joined_dead_is_noop() {
    let ((), records) = with_warn_capture(|| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
        });
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
        });
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "p1".into(),
                is_observer: true,
            }),
            ApplyOutcome::NoOp,
            "PeerJoined for a Dead id must be NoOp"
        );
        assert!(
            !s.role_table().observers.contains("p1"),
            "Dead peer must not appear in the observer projection",
        );
    });
    assert!(
        records.iter().any(|m| m.contains("PeerJoined for dead id ignored")),
        "expected warn log on PeerJoined for dead id, captured: {records:?}",
    );
}

/// The widened `PeerJoined` apply rule preserves the observer-set
/// extension semantics: a new observer peer enters the projection,
/// re-application is silent, and a subsequent distinct observer
/// extends the set.
#[test]
fn peer_joined_alive_extends_observer_set() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
        }),
        ApplyOutcome::NoOp,
        "re-applying the same PeerJoined is idempotent NoOp"
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-b".into(),
            is_observer: true,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.role_table().observers,
        HashSet::from(["obs-a".to_string(), "obs-b".to_string()]),
    );
}

/// Removing an observer drops it from `RoleTable.observers` and
/// fires role-change hooks against the post-mutation projection.
#[test]
fn peer_removed_observer_drops_from_role_table() {
    use std::sync::Mutex;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: true,
    });
    assert!(s.role_table().observers.contains("obs-1"));

    let observed: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed);
        s.register_role_change_hook(Box::new(move |table: &RoleTable| {
            observed.lock().unwrap().push(table.observers.len());
        }));
    }

    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "obs-1".into(),
            cause: RemovalCause::KeepaliveMiss,
        }),
        ApplyOutcome::Applied
    );
    assert!(
        !s.role_table().observers.contains("obs-1"),
        "PeerRemoved on an observer must drop it from RoleTable.observers"
    );
    let hook_fires = observed.lock().unwrap().clone();
    assert_eq!(
        hook_fires,
        vec![0],
        "role-change hook must fire once with the shrunk set"
    );
}

/// End-to-end: a state-changing `PeerJoined` apply, with a
/// dispatcher sender installed, MUST deliver a corresponding
/// `PeerLifecycleEvent::Added` on the channel. This pins the
/// "apply emits, dispatcher rx receives" contract — the
/// boundary that replaces the prior stub `emit_lifecycle_event`.
#[tokio::test]
async fn apply_peer_joined_emits_event_through_dispatcher() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_lifecycle_sender(tx);

    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "peer-x".into(),
            is_observer: false,
        }),
        ApplyOutcome::Applied
    );
    // The receiver MUST observe exactly one event with the
    // matching id / observer flag. `try_recv` confirms the
    // emit was non-blocking from the apply path's side.
    match rx.try_recv() {
        Ok(crate::peer_lifecycle::PeerLifecycleEvent::Added { id, is_observer }) => {
            assert_eq!(id, "peer-x");
            assert!(!is_observer);
        }
        other => panic!("expected Added event, got {other:?}"),
    }

    // Apply a removal as well to confirm the channel keeps
    // accepting subsequent events.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "peer-x".into(),
            cause: RemovalCause::KeepaliveMiss,
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(crate::peer_lifecycle::PeerLifecycleEvent::Removed { id, cause }) => {
            assert_eq!(id, "peer-x");
            assert_eq!(cause, RemovalCause::KeepaliveMiss);
        }
        other => panic!("expected Removed event, got {other:?}"),
    }
}

// ── TaskCompleted / TaskFailed dispatcher fan-out tests ──
//
// Pin the "apply emits, dispatcher rx receives" contract for the
// task-completion module — the boundary the PyO3
// `task_completed_listener` kwarg ultimately observes.

/// A successful `TaskCompleted` apply MUST emit
/// `TaskCompletedEvent { success: true, error_kind: None,
/// task_hash, task_id }` on the installed dispatcher channel.
#[tokio::test]
async fn task_completed_listener_fires_on_task_completed_apply() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    let task = mk_task("alpha");
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-alpha".into(),
        task: task.clone(),
    });
    // Move it through to InFlight so the success transition isn't
    // a Pending → Completed shortcut (the apply rule covers both
    // but the in-flight path is the production shape).
    s.apply(ClusterMutation::TaskAssigned {
        hash: "h-alpha".into(),
        secondary: "sec-1".into(),
        worker: 0,
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h-alpha".into()
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-alpha");
            assert_eq!(event.task_id.as_deref(), Some("alpha"));
            assert!(event.success);
            assert!(event.error_kind.is_none());
        }
        other => panic!("expected TaskCompleted event, got {other:?}"),
    }
}

/// A `TaskFailed` apply MUST emit
/// `TaskCompletedEvent { success: false, error_kind:
/// Some(<wire_value>), ... }` so consumers can dispatch on the
/// wire-stable error tag without re-deriving it from `Debug`.
#[tokio::test]
async fn task_completed_listener_fires_on_task_failed_with_error_kind() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-beta".into(),
        task: mk_task("beta"),
    });
    // Pending → Failed (NonRecoverable). The wire tag for
    // NonRecoverable is `"non_recoverable"`.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h-beta".into(),
            kind: ErrorType::NonRecoverable,
            error: "disk full".into(),
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-beta");
            assert_eq!(event.task_id.as_deref(), Some("beta"));
            assert!(!event.success);
            assert_eq!(event.error_kind.as_deref(), Some("non_recoverable"));
        }
        other => panic!("expected TaskFailed event, got {other:?}"),
    }
}

/// `TaskFailed { kind: Unfulfillable, .. }` against a Pending
/// task drives the `TaskState::Unfulfillable` transition; the
/// dispatcher event still fires with `success=false` and the
/// wire-stable `unfulfillable:<reason>` tag. Validates that the
/// Unfulfillable arm hooks into the same emit point as the
/// Failed arm — consumers don't have to know which terminal
/// the CRDT chose.
#[tokio::test]
async fn task_completed_listener_fires_on_unfulfillable_terminal() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-gamma".into(),
        task: mk_task("gamma"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h-gamma".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing toolchain".to_owned().into(),
            },
            error: "missing toolchain".into(),
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-gamma");
            assert!(!event.success);
            assert_eq!(
                event.error_kind.as_deref(),
                Some("unfulfillable:missing toolchain"),
            );
        }
        other => panic!("expected Unfulfillable event, got {other:?}"),
    }
}

/// A `TaskCompleted` apply that re-deduplicates (the task was
/// already `Completed`) MUST NOT emit a dispatcher event. The
/// apply rule is a NoOp; the dispatcher channel should stay
/// silent so consumers don't see ghost "task X completed again"
/// notifications.
#[tokio::test]
async fn task_completed_dedup_does_not_re_emit() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-delta".into(),
        task: mk_task("delta"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h-delta".into(),
    });
    // Drain the first (valid) event so we can prove the
    // dedup-apply doesn't enqueue a second.
    rx.try_recv().expect("first TaskCompleted must emit");
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h-delta".into()
        }),
        ApplyOutcome::NoOp
    );
    // No event should follow the NoOp dedup apply.
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "dedup TaskCompleted must not re-emit a dispatcher event",
    );
}

// ── PeerResourceHoldingsUpdated apply-rule + snapshot tests ──

/// First-time announce for an unseen peer inserts the holdings
/// set into `peer_holdings`. The wire `Vec<String>` collects to
/// a `HashSet<String>` so equality checks and dedup are
/// set-based.
#[test]
fn peer_resource_holdings_updated_apply_inserts_holdings() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.peer_holdings().is_empty());
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["res-1".into(), "res-2".into()],
            epoch: 0,
        }),
        ApplyOutcome::Applied
    );
    let stored = s.peer_holdings().get("peer-a").expect("entry present");
    assert_eq!(
        *stored,
        HashSet::from(["res-1".to_string(), "res-2".to_string()])
    );
}

/// An announce whose `epoch` is strictly older than the local
/// `primary_epoch` is a NoOp — supersede-old-pending defends
/// against a stale pre-failover announce overwriting holdings
/// observed under the current primary. Equal-or-newer epoch
/// applies; only `epoch < primary_epoch` is rejected.
#[test]
fn peer_resource_holdings_updated_stale_epoch_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Advance primary_epoch to 5.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "lead".into(),
        epoch: 5,
    });
    assert_eq!(s.primary_epoch(), 5);

    // epoch < primary_epoch → NoOp, ledger untouched.
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["stale".into()],
            epoch: 4,
        }),
        ApplyOutcome::NoOp
    );
    assert!(s.peer_holdings().get("peer-a").is_none());

    // epoch == primary_epoch → Applied (same-epoch announces are
    // legitimate within the current primary's reign).
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["fresh".into()],
            epoch: 5,
        }),
        ApplyOutcome::Applied
    );
    assert!(
        s.peer_holdings()
            .get("peer-a")
            .unwrap()
            .contains("fresh")
    );

    // epoch > primary_epoch → Applied (an announce from a peer
    // that already learned of a newer primary is still
    // authoritative about its own holdings).
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-b".into(),
            holdings: vec!["future".into()],
            epoch: 6,
        }),
        ApplyOutcome::Applied
    );
    assert!(
        s.peer_holdings()
            .get("peer-b")
            .unwrap()
            .contains("future")
    );
}

/// Re-application of a `PeerResourceHoldingsUpdated` whose
/// `holdings` set (as collected to a HashSet) equals the
/// already-stored set is a NoOp. Different ordering of the same
/// strings on the wire is still equal under HashSet semantics —
/// the apply rule does not depend on wire order.
#[test]
fn peer_resource_holdings_updated_same_set_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r1".into(), "r2".into()],
            epoch: 0,
        }),
        ApplyOutcome::Applied
    );
    // Same set, ordering swapped on the wire.
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r2".into(), "r1".into()],
            epoch: 0,
        }),
        ApplyOutcome::NoOp
    );
    // Duplicate string in incoming Vec collapses on collect; still
    // equal to the stored set → NoOp.
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r1".into(), "r2".into(), "r1".into()],
            epoch: 0,
        }),
        ApplyOutcome::NoOp
    );
    // A different set (superset) Applies.
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r1".into(), "r2".into(), "r3".into()],
            epoch: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        *s.peer_holdings().get("peer-a").unwrap(),
        HashSet::from(["r1".to_string(), "r2".to_string(), "r3".to_string()])
    );
    // A strictly smaller set also Applies (the announce is
    // authoritative for the announcing peer's current holdings;
    // shrinking is a real event when the peer evicts).
    assert_eq!(
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r1".into()],
            epoch: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        *s.peer_holdings().get("peer-a").unwrap(),
        HashSet::from(["r1".to_string()])
    );
}

/// `ClusterStateSnapshot` round-trips the per-peer holdings map
/// so a late-joiner sees current holdings before the next live
/// `PeerResourceHoldingsUpdated` broadcast arrives. Pins the
/// "snapshot carries replicated CRDT data" contract for the new
/// field.
#[test]
fn peer_resource_holdings_snapshot_round_trip() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
        peer_id: "peer-a".into(),
        holdings: vec!["r1".into(), "r2".into()],
        epoch: 0,
    });
    s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
        peer_id: "peer-b".into(),
        holdings: vec!["r3".into()],
        epoch: 0,
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    assert_eq!(
        *joiner.peer_holdings().get("peer-a").unwrap(),
        HashSet::from(["r1".to_string(), "r2".to_string()])
    );
    assert_eq!(
        *joiner.peer_holdings().get("peer-b").unwrap(),
        HashSet::from(["r3".to_string()])
    );
}

/// Pins the first-bootstrap-only contract on `restore`: a joiner
/// that has already observed a live `PeerResourceHoldingsUpdated`
/// broadcast (so `peer_holdings` is non-empty) keeps its local
/// map rather than overwriting from a (possibly stale) snapshot.
/// Mirrors the `observers` and `phase_deps` "replaced if local
/// empty, else kept" shape.
#[test]
fn peer_resource_holdings_restore_keeps_local_when_non_empty() {
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::PeerResourceHoldingsUpdated {
        peer_id: "live-peer".into(),
        holdings: vec!["live-res".into()],
        epoch: 0,
    });

    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::PeerResourceHoldingsUpdated {
        peer_id: "stale-peer".into(),
        holdings: vec!["stale-res".into()],
        epoch: 0,
    });

    joiner.restore(peer.snapshot());
    // Local map wins (live apply path is authoritative once it
    // has fired); snapshot's peer_holdings field is inert.
    assert!(joiner.peer_holdings().contains_key("live-peer"));
    assert!(!joiner.peer_holdings().contains_key("stale-peer"));
}
