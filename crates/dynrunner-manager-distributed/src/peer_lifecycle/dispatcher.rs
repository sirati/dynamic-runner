//! Peer-lifecycle dispatcher task.
//!
//! Single concern: drain a [`tokio::sync::mpsc::UnboundedReceiver<PeerLifecycleEvent>`]
//! and fan each event out to a fixed list of [`LifecycleListener`]
//! consumers. Owns no state beyond the rx + listener vector.
//!
//! # Why an unbounded channel
//!
//! The producer side is the cluster-state apply path, which already
//! gates membership churn behind the CRDT — peer joins / removes
//! happen at a rate bounded by real-world cluster-membership events
//! (peer connect / death notifications), not by an in-loop hot
//! path. Bounded would force `apply` to choose between `try_send`'s
//! drop-on-full (silent loss; breaks CCD-9 because the apply path
//! cannot recover the dropped event) and `blocking_send` (deadlocks
//! the apply task against a slow listener). Unbounded keeps the
//! apply path strictly non-blocking and lets the dispatcher
//! catch-up after a stall on its own time.
//!
//! # Panic isolation
//!
//! Every `listener.on_event(...)` invocation runs inside
//! [`std::panic::catch_unwind`] wrapped with `AssertUnwindSafe`.
//! A panicking listener is logged at `warn` and the dispatcher
//! continues with the next listener (and the next event). This is
//! the load-bearing guard that keeps the PyO3 bridge from taking
//! the dispatcher down when user Python code raises an exception
//! the bridge fails to convert cleanly — the bridge swallows
//! `PyErr` paths to `tracing::warn`, but a `pyo3::panic::PanicException`
//! escaping the bridge is caught here.
//!
//! # Exit condition
//!
//! The dispatcher exits when the receiver returns `None` —
//! i.e. every sender has been dropped. The single sender lives on
//! `ClusterState` (installed via `install_lifecycle_sender`), so
//! the dispatcher exits exactly when the cluster state is dropped.

use std::panic::AssertUnwindSafe;

use tokio::sync::mpsc::UnboundedReceiver;

use super::event::PeerLifecycleEvent;
use super::listener::LifecycleListener;

/// Drain `rx` and fan each event to every entry in `listeners`, in
/// registration order. Exits when `rx` closes (last sender dropped).
///
/// Spawned once per coordinator at `run()` start (see
/// `PrimaryCoordinator::run` / `SecondaryCoordinator::run_until_setup_or_done`).
/// The returned future is `Send` because every captured value is
/// `Send`; callers wrap it in `tokio::spawn` or `spawn_local` per the
/// surrounding runtime's shape.
pub async fn run_peer_lifecycle_dispatcher(
    mut rx: UnboundedReceiver<PeerLifecycleEvent>,
    listeners: Vec<Box<dyn LifecycleListener>>,
) {
    while let Some(event) = rx.recv().await {
        for (idx, listener) in listeners.iter().enumerate() {
            // `AssertUnwindSafe` is safe here: the listener is a
            // shared reference (`&dyn Trait`), `event` is borrowed
            // read-only, and we don't observe any state across the
            // unwind boundary. A panicking listener leaves no
            // half-mutated dispatcher state because the dispatcher
            // owns no per-listener mutable state — only the rx and
            // the listener vector, both untouched by the call.
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                listener.on_event(&event);
            }));
            if let Err(panic) = result {
                // Extract a printable message from the unwind
                // payload. Standard panics deliver `&str` / `String`;
                // anything else falls back to a generic label.
                let msg = if let Some(s) = panic.downcast_ref::<&'static str>() {
                    (*s).to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<non-string panic payload>".to_string()
                };
                tracing::warn!(
                    target: "dynrunner_peer_lifecycle",
                    listener_index = idx,
                    event = ?event,
                    panic_message = %msg,
                    "peer-lifecycle listener panicked; isolating and continuing dispatch",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc::unbounded_channel;

    use super::super::event::RemovalCause;
    use super::*;

    /// Capturing listener used by the dispatcher tests below. Records
    /// every event it observes into a shared `Mutex<Vec<_>>` so the
    /// test can assert against the captured order.
    struct CapturingListener {
        captured: Arc<Mutex<Vec<PeerLifecycleEvent>>>,
    }
    impl LifecycleListener for CapturingListener {
        fn on_event(&self, event: &PeerLifecycleEvent) {
            self.captured.lock().unwrap().push(event.clone());
        }
    }

    /// Listener that panics on every fire. Pairs with `CapturingListener`
    /// in the isolation test to prove the panic is contained and the
    /// next listener still receives the same event.
    struct PanickingListener;
    impl LifecycleListener for PanickingListener {
        fn on_event(&self, _event: &PeerLifecycleEvent) {
            panic!("listener panicked on purpose");
        }
    }

    /// Pins the dispatcher's core fan-out contract: events appear at
    /// every registered listener in the same order they were enqueued.
    #[tokio::test]
    async fn lifecycle_dispatcher_drains_events_to_rust_listener() {
        let (tx, rx) = unbounded_channel();
        let captured: Arc<Mutex<Vec<PeerLifecycleEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let listeners: Vec<Box<dyn LifecycleListener>> = vec![Box::new(CapturingListener {
            captured: Arc::clone(&captured),
        })];

        let dispatcher = tokio::spawn(run_peer_lifecycle_dispatcher(rx, listeners));

        tx.send(PeerLifecycleEvent::Added {
            id: "peer-a".into(),
            is_observer: false,
        })
        .unwrap();
        tx.send(PeerLifecycleEvent::Added {
            id: "peer-b".into(),
            is_observer: true,
        })
        .unwrap();
        tx.send(PeerLifecycleEvent::Removed {
            id: "peer-a".into(),
            cause: RemovalCause::KeepaliveMiss,
        })
        .unwrap();

        drop(tx);
        dispatcher.await.unwrap();

        let observed = captured.lock().unwrap().clone();
        assert_eq!(observed.len(), 3);
        assert_eq!(
            observed[0],
            PeerLifecycleEvent::Added {
                id: "peer-a".into(),
                is_observer: false,
            }
        );
        assert_eq!(
            observed[1],
            PeerLifecycleEvent::Added {
                id: "peer-b".into(),
                is_observer: true,
            }
        );
        assert_eq!(
            observed[2],
            PeerLifecycleEvent::Removed {
                id: "peer-a".into(),
                cause: RemovalCause::KeepaliveMiss,
            }
        );
    }

    /// Pins the panic-isolation contract: a panicking listener must
    /// NOT halt the dispatcher; subsequent listeners on the same
    /// event still fire, and the dispatcher keeps draining
    /// subsequent events.
    #[tokio::test]
    async fn lifecycle_dispatcher_isolates_python_panic() {
        let (tx, rx) = unbounded_channel();
        let captured: Arc<Mutex<Vec<PeerLifecycleEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let listeners: Vec<Box<dyn LifecycleListener>> = vec![
            Box::new(PanickingListener),
            Box::new(CapturingListener {
                captured: Arc::clone(&captured),
            }),
        ];

        let dispatcher = tokio::spawn(run_peer_lifecycle_dispatcher(rx, listeners));

        tx.send(PeerLifecycleEvent::Added {
            id: "peer-a".into(),
            is_observer: false,
        })
        .unwrap();
        tx.send(PeerLifecycleEvent::Removed {
            id: "peer-a".into(),
            cause: RemovalCause::MassDeathEscalation,
        })
        .unwrap();

        drop(tx);
        dispatcher.await.unwrap();

        // Both events reached the capturing listener even though the
        // earlier listener in the vector panicked on each fire.
        let observed = captured.lock().unwrap().clone();
        assert_eq!(observed.len(), 2);
    }
}
