//! Tests for the peer-lifecycle apply rules and the
//! `peer_state` + `RoleTable.observers` lockstep.
//!
//! Pins the contract:
//!
//!   1. `PeerRemoved` is sticky-per-id: once Dead, always Dead. A
//!      duplicate broadcast is a NoOp; a late `PeerJoined` for the
//!      same id is dropped with a warn log (no resurrection).
//!   2. The observer-set projection is maintained in lockstep with
//!      the `peer_state` map — removal of an observer drops them
//!      from `RoleTable.observers` and fires role-change hooks.
//!
//! Also defines the `WarnCapture` tracing layer used by
//! `peer_joined_dead_is_noop` to assert on the warn-log emit.

use super::*;

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
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
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
    let records: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
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
            can_be_primary: false,
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
            cause: RemovalCause::KeepaliveMiss,
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
            can_be_primary: false,
        });
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
        });
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "p1".into(),
                is_observer: true,
                can_be_primary: false,
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
        records
            .iter()
            .any(|m| m.contains("PeerJoined for dead id ignored")),
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
            can_be_primary: false,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
            can_be_primary: false,
        }),
        ApplyOutcome::NoOp,
        "re-applying the same PeerJoined is idempotent NoOp"
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-b".into(),
            is_observer: true,
            can_be_primary: false,
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
        can_be_primary: false,
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
