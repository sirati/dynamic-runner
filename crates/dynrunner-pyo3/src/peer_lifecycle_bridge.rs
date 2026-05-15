//! PyO3 bridge: adapt a Python listener object into a
//! [`LifecycleListener`] the manager-distributed dispatcher can call.
//!
//! Single concern of this file: convert one Rust `&PeerLifecycleEvent`
//! into one Python method call (`on_peer_added(id, is_observer)` or
//! `on_peer_removed(id, cause)`), under the GIL, swallowing every
//! exception path so the dispatcher task never tears down. Nothing
//! about WHICH manager owns the listener, HOW the manager threads
//! the kwarg through, or WHEN `register_lifecycle_listener` runs
//! lives here — those concerns belong to the manager pyclass files
//! and are uniformly thin (single line each).
//!
//! Duck-typing: a listener is anything Python that exposes
//! `on_peer_added(id: str, is_observer: bool)` and/or
//! `on_peer_removed(id: str, cause: str)`. Missing methods are
//! treated as a silent opt-out (matches the `on_phase_start` /
//! `on_phase_end` shape in `crate::managers::lifecycle`: consumers
//! opt in per event family). The listener is still installed so the
//! other half can still fire.
//!
//! Error / exception handling:
//!   - `PyErr` from the call surfaces a `tracing::warn` and is
//!     swallowed (the dispatcher's `catch_unwind` only catches Rust
//!     panics; a `PyErr` is a value-level error pyo3 wraps).
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     dispatcher's `catch_unwind` isolates — so even a Python
//!     `assert` or a pyo3-side panic can't take the dispatcher down.

use pyo3::prelude::*;

use dynrunner_manager_distributed::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};

/// Adapter that holds an unbound Python listener and dispatches each
/// event to the matching Python method. `Send + Sync` is satisfied by
/// `Py<PyAny>`'s contract — the underlying object is reference-counted
/// through Python's GIL.
pub(crate) struct PyPeerLifecycleListener {
    /// The Python listener object. Held as an unbound `Py<PyAny>` so
    /// the adapter outlives any single `Python<'py>` lifetime; each
    /// `on_event` re-binds under a fresh GIL acquisition.
    listener: Py<PyAny>,
}

impl PyPeerLifecycleListener {
    /// Build a bridge from a Python listener object.
    ///
    /// Boxed at the call site (returned as `Box<dyn LifecycleListener>`)
    /// so the manager-distributed registration API consumes a uniform
    /// trait-object shape and the caller doesn't need to spell out
    /// the concrete type.
    pub(crate) fn new(listener: Py<PyAny>) -> Box<dyn LifecycleListener> {
        Box::new(Self { listener })
    }
}

impl LifecycleListener for PyPeerLifecycleListener {
    fn on_event(&self, event: &PeerLifecycleEvent) {
        // GIL acquisition crosses the runtime boundary. Per-event cost
        // is one attach + one method lookup + one call; the apply
        // path's emit is non-blocking so this latency is invisible to
        // the CRDT.
        let outcome: PyResult<()> = Python::attach(|py| {
            let listener = self.listener.bind(py);
            match event {
                PeerLifecycleEvent::Added { id, is_observer } => {
                    invoke_added(listener, id.as_str(), *is_observer)
                }
                PeerLifecycleEvent::Removed { id, cause } => {
                    // The cause is rendered through its `Debug` impl so
                    // the Python side sees a stable string shape (e.g.
                    // `"KeepaliveMiss"`, `"MassDeathEscalation"`,
                    // `"FatalError(..)"`). A typed surface would require
                    // a parallel Python enum class on every framework
                    // upgrade; the string is the lowest-overhead
                    // duck-typed signal and matches the way operators
                    // already read removal causes in tracing logs.
                    let cause_str = format!("{cause:?}");
                    invoke_removed(listener, id.as_str(), &cause_str)
                }
            }
        });
        if let Err(e) = outcome {
            tracing::warn!(
                target: "dynrunner_pyo3_peer_lifecycle",
                event = ?event,
                error = %e,
                "Python peer-lifecycle listener raised; swallowed to keep dispatcher alive",
            );
        }
    }
}

/// Dispatch `on_peer_added(id, is_observer)` if the listener
/// implements it; silently skip otherwise. Duck-typed opt-in keeps
/// consumers free to subscribe only to one event family.
fn invoke_added(listener: &Bound<'_, PyAny>, id: &str, is_observer: bool) -> PyResult<()> {
    if !listener.hasattr("on_peer_added")? {
        return Ok(());
    }
    listener.call_method1("on_peer_added", (id, is_observer))?;
    Ok(())
}

/// Dispatch `on_peer_removed(id, cause)` if the listener implements
/// it; silently skip otherwise. See `invoke_added` for the duck-typed
/// rationale.
fn invoke_removed(listener: &Bound<'_, PyAny>, id: &str, cause: &str) -> PyResult<()> {
    if !listener.hasattr("on_peer_removed")? {
        return Ok(());
    }
    listener.call_method1("on_peer_removed", (id, cause))?;
    Ok(())
}
