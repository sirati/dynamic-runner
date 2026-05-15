//! Listener trait for peer-lifecycle events.
//!
//! Single concern: define the consumer-side surface the dispatcher
//! calls into. Implementations live wherever the consumer lives (the
//! PyO3 bridge in `dynrunner-pyo3`, future Rust-side respawn handlers
//! in `secondary/`, telemetry adapters, etc.); this crate only owns
//! the trait shape so the dispatcher can be generic over them.
//!
//! # Sync, fast, infallible
//!
//! The trait is synchronous and returns `()`. Listeners that need to
//! cross thread / runtime boundaries are expected to enqueue onto
//! their own channel (e.g. the Python adapter acquires the GIL and
//! invokes a Python callback; the future respawn handler will fire
//! a `tokio::sync::mpsc::send` against the secondary's command
//! channel). Errors are swallowed inside the listener — the
//! dispatcher cannot distinguish "skip this listener" from "halt the
//! dispatcher", and halting on a single consumer's failure would
//! drop every subsequent event for every other consumer.
//!
//! # Panic isolation is the dispatcher's job
//!
//! Listeners may not panic-clean. The dispatcher catches panics
//! around each `on_event` call so a buggy listener cannot tear the
//! dispatcher task down; see [`super::dispatcher::run_peer_lifecycle_dispatcher`].

use super::event::PeerLifecycleEvent;

/// Consumer of [`PeerLifecycleEvent`]s drained off the dispatcher
/// channel. Trait object–compatible (`Send + Sync`) so the
/// coordinator can hold a heterogeneous `Vec<Box<dyn LifecycleListener>>`.
pub trait LifecycleListener: Send + Sync {
    /// Called once per event drained off the dispatcher channel, in
    /// the order the apply path emitted them.
    fn on_event(&self, event: &PeerLifecycleEvent);
}
