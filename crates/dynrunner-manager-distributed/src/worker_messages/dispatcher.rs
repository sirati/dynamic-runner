//! Worker custom-message dispatcher task.
//!
//! Single concern: drain a
//! [`tokio::sync::mpsc::UnboundedReceiver<WorkerCustomMessage>`] and
//! fan each event out to a fixed list of [`WorkerMessageListener`]
//! consumers. Owns no state beyond the rx + listener vector.
//!
//! # Why an unbounded channel
//!
//! The producer side is the secondary's worker-event bridge, which is
//! already rate-bounded by real worker wire throughput (one frame per
//! `Response::Custom`, ≤ 100 KiB each). Bounded would force the
//! operational loop's pool-event arm to choose between `try_send`'s
//! drop-on-full (silent consumer-message loss) and `blocking_send`
//! (wedging the operational loop against a slow Python listener).
//! Unbounded keeps the bridge strictly non-blocking and lets the
//! dispatcher catch up after a stall on its own time.
//!
//! # Panic isolation
//!
//! Every `listener.on_message(...)` invocation runs inside
//! [`std::panic::catch_unwind`] wrapped with `AssertUnwindSafe`.
//! A panicking listener is logged at `warn` and the dispatcher
//! continues with the next listener (and the next event). This is
//! the load-bearing guard that keeps the PyO3 bridge from taking
//! the dispatcher down when user Python code raises an exception
//! the bridge fails to convert cleanly — the bridge swallows
//! `PyErr` paths to `tracing::warn`, but a `pyo3::panic::PanicException`
//! escaping the bridge is caught here. Same isolation contract as
//! [`crate::task_completed::run_task_completed_dispatcher`].
//!
//! # Exit condition
//!
//! The dispatcher exits when the receiver returns `None` —
//! i.e. every sender has been dropped. The single sender lives on
//! the `SecondaryCoordinator` (its `worker_message_tx`), so the
//! dispatcher exits exactly when the coordinator is dropped.

use std::panic::AssertUnwindSafe;

use tokio::sync::mpsc::UnboundedReceiver;

use super::event::WorkerCustomMessage;
use super::listener::WorkerMessageListener;

/// Drain `rx` and fan each event to every entry in `listeners`, in
/// registration order. Exits when `rx` closes (last sender dropped).
///
/// Spawned once per coordinator at run start (see
/// `SecondaryCoordinator::run_until_setup_or_done`). The returned
/// future is `Send` because every captured value is `Send`; callers
/// wrap it in `tokio::spawn` or `spawn_local` per the surrounding
/// runtime's shape.
pub async fn run_worker_message_dispatcher(
    mut rx: UnboundedReceiver<WorkerCustomMessage>,
    listeners: Vec<Box<dyn WorkerMessageListener>>,
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
                listener.on_message(&event);
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
                    target: "dynrunner_worker_messages",
                    listener_index = idx,
                    worker_id = event.worker_id,
                    topic = %event.topic,
                    panic_message = %msg,
                    "worker-message listener panicked; isolating and continuing dispatch",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    /// Capturing listener used by the dispatcher tests below. Records
    /// every event it observes into a shared `Mutex<Vec<_>>` so the
    /// test can assert against the captured order.
    struct CapturingListener {
        captured: Arc<Mutex<Vec<WorkerCustomMessage>>>,
    }
    impl WorkerMessageListener for CapturingListener {
        fn on_message(&self, event: &WorkerCustomMessage) {
            self.captured.lock().unwrap().push(event.clone());
        }
    }

    /// Listener that panics on every fire. Pairs with `CapturingListener`
    /// in the isolation test to prove the panic is contained and the
    /// next listener still receives the same event.
    struct PanickingListener;
    impl WorkerMessageListener for PanickingListener {
        fn on_message(&self, _event: &WorkerCustomMessage) {
            panic!("listener panicked on purpose");
        }
    }

    fn msg(worker_id: u32, topic: &str, data: &[u8]) -> WorkerCustomMessage {
        WorkerCustomMessage {
            worker_id,
            type_id: "build".into(),
            topic: topic.into(),
            data: data.to_vec(),
        }
    }

    /// Pins the dispatcher's core fan-out contract: events appear at
    /// every registered listener in the same order they were enqueued
    /// (the "listener sees N in order" half of the e2e contract).
    #[tokio::test]
    async fn worker_message_dispatcher_drains_events_in_order() {
        let (tx, rx) = unbounded_channel();
        let captured: Arc<Mutex<Vec<WorkerCustomMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let listeners: Vec<Box<dyn WorkerMessageListener>> = vec![Box::new(CapturingListener {
            captured: Arc::clone(&captured),
        })];

        let dispatcher = tokio::spawn(run_worker_message_dispatcher(rx, listeners));

        tx.send(msg(0, "batch", b"one")).unwrap();
        tx.send(msg(0, "batch", b"two")).unwrap();
        tx.send(msg(1, "progress", b"three")).unwrap();

        drop(tx);
        dispatcher.await.unwrap();

        let observed = captured.lock().unwrap().clone();
        assert_eq!(observed.len(), 3);
        assert_eq!(observed[0].data, b"one");
        assert_eq!(observed[1].data, b"two");
        assert_eq!(observed[2].data, b"three");
        assert_eq!(observed[2].worker_id, 1);
        assert_eq!(observed[2].topic, "progress");
    }

    /// Pins the panic-isolation contract: a panicking listener must
    /// NOT halt the dispatcher; subsequent listeners on the same
    /// event still fire, and the dispatcher keeps draining
    /// subsequent events.
    #[tokio::test]
    async fn worker_message_dispatcher_isolates_panicking_listener() {
        let (tx, rx) = unbounded_channel();
        let captured: Arc<Mutex<Vec<WorkerCustomMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let listeners: Vec<Box<dyn WorkerMessageListener>> = vec![
            Box::new(PanickingListener),
            Box::new(CapturingListener {
                captured: Arc::clone(&captured),
            }),
        ];

        let dispatcher = tokio::spawn(run_worker_message_dispatcher(rx, listeners));

        tx.send(msg(0, "batch", b"one")).unwrap();
        tx.send(msg(0, "batch", b"two")).unwrap();

        drop(tx);
        dispatcher.await.unwrap();

        // Both events reached the capturing listener even though the
        // earlier listener in the vector panicked on each fire.
        let observed = captured.lock().unwrap().clone();
        assert_eq!(observed.len(), 2);
    }
}
