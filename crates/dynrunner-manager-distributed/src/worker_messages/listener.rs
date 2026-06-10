//! Listener trait for worker custom-message events.
//!
//! Single concern: define the consumer-side surface the dispatcher
//! calls into. Implementations live wherever the consumer lives (the
//! PyO3 bridge in `dynrunner-pyo3`, telemetry adapters, etc.); this
//! crate only owns the trait shape so the dispatcher can be generic
//! over them.
//!
//! # Sync, fast, infallible
//!
//! The trait is synchronous and returns `()`. Listeners that need to
//! cross thread / runtime boundaries are expected to enqueue onto
//! their own channel (the Python adapter acquires the GIL and invokes
//! the consumer's `worker_message_listener`; a reply goes through the
//! `SecondaryControlCommand` channel back into the secondary's
//! operational loop — never a direct cross-call into worker
//! management). Errors are swallowed inside the listener — the
//! dispatcher cannot distinguish "skip this listener" from "halt the
//! dispatcher", and halting on a single consumer's failure would drop
//! every subsequent event for every other consumer.
//!
//! # Panic isolation is the dispatcher's job
//!
//! Listeners may not panic-clean. The dispatcher catches panics
//! around each `on_message` call so a buggy listener cannot tear the
//! dispatcher task down; see
//! [`super::dispatcher::run_worker_message_dispatcher`].

use super::event::WorkerCustomMessage;

/// Consumer of [`WorkerCustomMessage`]s drained off the dispatcher
/// channel. Trait object–compatible (`Send + Sync`) so the coordinator
/// can hold a heterogeneous `Vec<Box<dyn WorkerMessageListener>>`.
pub trait WorkerMessageListener: Send + Sync {
    /// Called once per event drained off the dispatcher channel, in
    /// the order the worker-event bridge emitted them (per-worker
    /// wire order is preserved end-to-end).
    fn on_message(&self, event: &WorkerCustomMessage);
}
