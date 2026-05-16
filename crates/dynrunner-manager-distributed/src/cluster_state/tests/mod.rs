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
mod cascade_and_reinject;
mod role_table;
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
