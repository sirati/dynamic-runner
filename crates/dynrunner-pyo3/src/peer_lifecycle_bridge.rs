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
//! `on_peer_removed(id: str, cause: dict)`. Missing methods are
//! treated as a silent opt-out (matches the `on_phase_start` /
//! `on_phase_end` shape in `crate::managers::lifecycle`: consumers
//! opt in per event family). The listener is still installed so the
//! other half can still fire.
//!
//! `cause` shape: a `dict` with two keys, mirroring the Rust
//! `RemovalCause` enum so consumers get a stable, typed surface
//! independent of Rust's `Debug` impl:
//!   - `kind`: one of `"keepalive_miss"`, `"fatal_error"`,
//!     `"self_departure"`.
//!   - `reason`: `None` for the authoritative-detection `"keepalive_miss"`
//!     variant; for `"fatal_error"` / `"self_departure"` it is the
//!     (byte-capped) diagnostic string the reporting peer attached.
//!     Encoded once here so adding a new `RemovalCause` variant is a
//!     localised single-file change rather than a string-format
//!     contract spread across consumers.
//!
//! Error / exception handling:
//!   - `PyErr` from the call surfaces a `tracing::warn` and is
//!     swallowed (the dispatcher's `catch_unwind` only catches Rust
//!     panics; a `PyErr` is a value-level error pyo3 wraps).
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     dispatcher's `catch_unwind` isolates — so even a Python
//!     `assert` or a pyo3-side panic can't take the dispatcher down.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_manager_distributed::peer_lifecycle::{
    LifecycleListener, PeerLifecycleEvent, RemovalCause,
};

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
    /// the concrete type. Returning `Box<dyn ...>` instead of `Self`
    /// is the load-bearing API contract.
    #[allow(clippy::new_ret_no_self)]
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
                    // Build the typed `{kind, reason}` dict here so the
                    // Python contract is independent of Rust's `Debug`
                    // impl (which is not a stable API and would change
                    // shape if a future variant carried structured
                    // payload). A new `RemovalCause` variant therefore
                    // requires exactly one edit — `encode_cause` — and
                    // no consumer-side string parsing.
                    let py_cause = encode_cause(py, cause)?;
                    invoke_removed(listener, id.as_str(), py_cause)
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
/// rationale. The `cause` is the typed `{kind, reason}` dict built by
/// `encode_cause`.
fn invoke_removed<'py>(
    listener: &Bound<'py, PyAny>,
    id: &str,
    cause: Bound<'py, PyDict>,
) -> PyResult<()> {
    if !listener.hasattr("on_peer_removed")? {
        return Ok(());
    }
    listener.call_method1("on_peer_removed", (id, cause))?;
    Ok(())
}

/// Encode a [`RemovalCause`] as the wire-stable
/// `{"kind": str, "reason": str | None}` dict the Python listener
/// observes. The single concern of this helper is the
/// Rust-enum → Python-dict mapping; centralising it here means a new
/// variant is exactly one match arm, with no second site that needs
/// to learn about it (no callers know the shape).
fn encode_cause<'py>(py: Python<'py>, cause: &RemovalCause) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    match cause {
        RemovalCause::KeepaliveMiss => {
            dict.set_item("kind", "keepalive_miss")?;
            dict.set_item("reason", py.None())?;
        }
        RemovalCause::FatalError(bs) => {
            dict.set_item("kind", "fatal_error")?;
            dict.set_item("reason", bs.as_str())?;
        }
        RemovalCause::SelfDeparture(bs) => {
            dict.set_item("kind", "self_departure")?;
            dict.set_item("reason", bs.as_str())?;
        }
        RemovalCause::RosterReemit => {
            dict.set_item("kind", "roster_reemit")?;
            dict.set_item("reason", py.None())?;
        }
    }
    Ok(dict)
}

#[cfg(test)]
mod tests {
    //! Contract tests for the listener bridge. Each test drives the
    //! `LifecycleListener::on_event` surface (i.e. the dispatcher's
    //! entry point) with a hand-built `PeerLifecycleEvent`, captures
    //! the resulting Python call on a mock listener, and asserts the
    //! typed `cause` dict shape per `RemovalCause` variant.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python peer_lifecycle_bridge`
    use super::*;
    use dynrunner_core::BoundedString;
    use pyo3::types::PyList;

    /// Per-call atomic counter so each `make_recording_listener` gets a
    /// unique module name. `PyModule::from_code` resolves duplicate
    /// names through `sys.modules`, so without this the parallel
    /// `cargo test` harness would have two threads mutating the same
    /// module-level `removed_calls` list.
    static MODULE_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Compile a tiny recording listener module + return both the
    /// instance (as a `Py<PyAny>` ready for `PyPeerLifecycleListener::new`)
    /// and a handle on the module globals so the test can inspect the
    /// recorded calls afterwards. Mirrors the helper pattern in
    /// `managers::multi_process_respawner`'s tests.
    fn make_recording_listener() -> (Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let module_name = format!("mock_listener_{nonce}");
        let file_name = format!("{module_name}.py");
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "added_calls = []\n\
                     removed_calls = []\n\
                     class Listener:\n    \
                         def on_peer_added(self, peer_id, is_observer):\n        \
                             added_calls.append((peer_id, is_observer))\n    \
                         def on_peer_removed(self, peer_id, cause):\n        \
                             # Copy cause to a plain dict so the test can\n        \
                             # inspect it after the bridge call returns.\n        \
                             removed_calls.append((peer_id, dict(cause)))\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile mock listener module");
            let cls = module.getattr("Listener").unwrap();
            let instance = cls.call0().unwrap().unbind();
            let globals = module.dict().unbind().into_any();
            (instance, globals)
        })
    }

    /// Pull a captured `removed_calls` entry out of the module
    /// globals. Returns `(peer_id, cause_dict_as_kind_reason)` where
    /// `reason` is `None` when the dict's `reason` is Python `None`.
    fn captured_removed(globals: &Py<PyAny>, idx: usize) -> (String, String, Option<String>) {
        Python::attach(|py| {
            let g = globals.bind(py);
            let removed = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("removed_calls")
                .unwrap()
                .unwrap();
            let list = removed.cast::<PyList>().unwrap();
            let entry = list.get_item(idx).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let peer_id: String = tuple.get_item(0).unwrap().extract().unwrap();
            let cause = tuple.get_item(1).unwrap();
            let cause_dict = cause.cast::<PyDict>().unwrap();
            let kind: String = cause_dict
                .get_item("kind")
                .unwrap()
                .expect("cause dict must carry a 'kind' key")
                .extract()
                .unwrap();
            let reason_obj = cause_dict
                .get_item("reason")
                .unwrap()
                .expect("cause dict must carry a 'reason' key");
            let reason: Option<String> = if reason_obj.is_none() {
                None
            } else {
                Some(reason_obj.extract().unwrap())
            };
            (peer_id, kind, reason)
        })
    }

    #[test]
    fn removed_keepalive_miss_emits_typed_dict() {
        let (listener_obj, globals) = make_recording_listener();
        let bridge = PyPeerLifecycleListener::new(listener_obj);
        bridge.on_event(&PeerLifecycleEvent::Removed {
            id: "sec-1".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
        });
        let (peer_id, kind, reason) = captured_removed(&globals, 0);
        assert_eq!(peer_id, "sec-1");
        assert_eq!(kind, "keepalive_miss");
        assert_eq!(reason, None);
    }

    #[test]
    fn removed_fatal_error_emits_typed_dict_with_reason() {
        let (listener_obj, globals) = make_recording_listener();
        let bridge = PyPeerLifecycleListener::new(listener_obj);
        bridge.on_event(&PeerLifecycleEvent::Removed {
            id: "sec-3".to_owned(),
            cause: RemovalCause::FatalError(BoundedString::from("disk full")),
        });
        let (peer_id, kind, reason) = captured_removed(&globals, 0);
        assert_eq!(peer_id, "sec-3");
        assert_eq!(kind, "fatal_error");
        assert_eq!(reason.as_deref(), Some("disk full"));
    }

    #[test]
    fn removed_listener_without_method_is_silent_optout() {
        // Listener exposes only `on_peer_added`; the bridge MUST NOT
        // raise when `on_peer_removed` is missing. This is the
        // duck-typed opt-in contract documented at the top of the file.
        let nonce = MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let module_name = format!("mock_listener_partial_{nonce}");
        let file_name = format!("{module_name}.py");
        let listener_obj = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "class Listener:\n    \
                         def on_peer_added(self, peer_id, is_observer):\n        \
                             pass\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .unwrap();
            module
                .getattr("Listener")
                .unwrap()
                .call0()
                .unwrap()
                .unbind()
        });
        let bridge = PyPeerLifecycleListener::new(listener_obj);
        // Must not panic / propagate; the bridge swallows the missing
        // method as a silent opt-out.
        bridge.on_event(&PeerLifecycleEvent::Removed {
            id: "sec-x".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
        });
    }

    #[test]
    fn added_event_still_routes_to_on_peer_added() {
        // Regression pin: the typed-cause refactor touches the Removed
        // arm; the Added arm's positional contract is unchanged and
        // must keep flowing through. Kept alongside the cause tests
        // because both paths live in `on_event`.
        let (listener_obj, globals) = make_recording_listener();
        let bridge = PyPeerLifecycleListener::new(listener_obj);
        bridge.on_event(&PeerLifecycleEvent::Added {
            id: "sec-7".to_owned(),
            is_observer: true,
        });
        Python::attach(|py| {
            let g = globals.bind(py);
            let added = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("added_calls")
                .unwrap()
                .unwrap();
            let list = added.cast::<PyList>().unwrap();
            assert_eq!(list.len(), 1);
            let entry = list.get_item(0).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let peer_id: String = tuple.get_item(0).unwrap().extract().unwrap();
            let is_observer: bool = tuple.get_item(1).unwrap().extract().unwrap();
            assert_eq!(peer_id, "sec-7");
            assert!(is_observer);
        });
    }
}
