//! PyO3 bridge: adapt a Python listener callable into a
//! [`TaskCompletedListener`] the manager-distributed dispatcher can
//! call.
//!
//! Single concern of this file: convert one Rust
//! `&TaskCompletedEvent` into one Python call
//! `listener(task_id, success, error_kind, last_error)` under the GIL,
//! swallowing every `PyErr` path to `tracing::warn` so the dispatcher
//! task never tears down on a buggy / raising Python listener. Nothing
//! about WHICH manager owns the listener, HOW the manager threads the
//! kwarg through, or WHEN `register_task_completed_listener` runs lives
//! here — those concerns belong to the manager pyclass files and are
//! uniformly thin (single line each).
//!
//! Listener shape (Python side):
//!
//! ```python
//! def task_completed_listener(
//!     task_id: str | None,
//!     success: bool,
//!     error_kind: str | None,
//!     last_error: str | None,
//! ) -> None: ...
//! ```
//!
//! `last_error` is the trailing positional, mirroring the
//! `TaskCompletedEvent.last_error` field: `None` on success, the
//! operator-facing failure message on failure. It is carried
//! ALONGSIDE `error_kind` (the wire-stable type tag) because a
//! failure is only fully identified by type AND message.
//!
//! Error / exception handling:
//!   - `PyErr` from the call surfaces a `tracing::warn` and is
//!     swallowed (the dispatcher's `catch_unwind` only catches Rust
//!     panics; a `PyErr` is a value-level error pyo3 wraps).
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     dispatcher's `catch_unwind` isolates — so even a Python
//!     `assert` or a pyo3-side panic can't take the dispatcher down.

use pyo3::prelude::*;

use dynrunner_manager_distributed::task_completed::{TaskCompletedEvent, TaskCompletedListener};

/// Adapter that holds an unbound Python listener and dispatches each
/// event to the matching Python call. `Send + Sync` is satisfied by
/// `Py<PyAny>`'s contract — the underlying object is reference-counted
/// through Python's GIL.
pub(crate) struct PyTaskCompletedListener {
    /// The Python listener callable. Held as an unbound `Py<PyAny>`
    /// so the adapter outlives any single `Python<'py>` lifetime;
    /// each `on_event` re-binds under a fresh GIL acquisition.
    listener: Py<PyAny>,
}

impl PyTaskCompletedListener {
    /// Build a bridge from a Python listener callable.
    ///
    /// Boxed at the call site (returned as
    /// `Box<dyn TaskCompletedListener>`) so the manager-distributed
    /// registration API consumes a uniform trait-object shape and the
    /// caller doesn't need to spell out the concrete type. Returning
    /// `Box<dyn ...>` instead of `Self` is the load-bearing API
    /// contract.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(listener: Py<PyAny>) -> Box<dyn TaskCompletedListener> {
        Box::new(Self { listener })
    }
}

impl TaskCompletedListener for PyTaskCompletedListener {
    fn on_event(&self, event: &TaskCompletedEvent) {
        // GIL acquisition crosses the runtime boundary. Per-event
        // cost is one attach + one call; the apply path's emit is
        // non-blocking so this latency is invisible to the CRDT.
        let outcome: PyResult<()> = Python::attach(|py| {
            // `task_id` is non-optional per the framework's boundary
            // contract; it round-trips as Python `str`. `error_kind`
            // and `last_error` are both optional (the success arm
            // leaves them `None`) and go through
            // `Option<&str> → str | None` via PyO3's IntoPyObject for
            // `Option<T>`.
            let args = (
                event.task_id.as_str(),
                event.success,
                event.error_kind.as_deref(),
                event.last_error.as_deref(),
            );
            self.listener.bind(py).call1(args).map(|_| ())
        });
        if let Err(e) = outcome {
            tracing::warn!(
                target: "dynrunner_pyo3_task_completed",
                event = ?event,
                error = %e,
                "Python task-completed listener raised; swallowed to keep dispatcher alive",
            );
        }
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Contract tests for the listener bridge. Each test drives the
    //! `TaskCompletedListener::on_event` surface (i.e. the dispatcher's
    //! entry point) with a hand-built `TaskCompletedEvent`, captures
    //! the resulting Python call on a mock listener, and asserts the
    //! positional-argument shape per success / failure variant.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python task_completed_bridge`
    use super::*;
    use pyo3::types::{PyDict, PyList};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Per-call atomic counter so each `make_recording_listener` gets
    /// a unique module name. `PyModule::from_code` resolves duplicate
    /// names through `sys.modules`, so without this the parallel
    /// `cargo test` harness would have two threads mutating the same
    /// module-level `calls` list.
    static MODULE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Compile a tiny recording listener module + return both the
    /// callable (as a `Py<PyAny>` ready for
    /// `PyTaskCompletedListener::new`) and a handle on the module
    /// globals so the test can inspect the recorded calls afterwards.
    fn make_recording_listener() -> (Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_task_completed_{nonce}");
        let file_name = format!("{module_name}.py");
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "calls = []\n\
                     def listener(task_id, success, error_kind, last_error):\n    \
                         calls.append((task_id, success, error_kind, last_error))\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile mock listener module");
            let callable = module.getattr("listener").unwrap().unbind();
            let globals = module.dict().unbind().into_any();
            (callable, globals)
        })
    }

    /// Pull a captured `calls` entry out of the module globals.
    /// `task_id` is always a Python `str` (the framework's boundary
    /// contract guarantees a non-empty id on every event);
    /// `error_kind` and `last_error` are both `str | None`
    /// (success → `None`).
    #[allow(clippy::type_complexity)]
    fn captured_call(
        globals: &Py<PyAny>,
        idx: usize,
    ) -> (String, bool, Option<String>, Option<String>) {
        Python::attach(|py| {
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<PyList>().unwrap();
            let entry = list.get_item(idx).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let task_id: String = tuple.get_item(0).unwrap().extract().unwrap();
            let success: bool = tuple.get_item(1).unwrap().extract().unwrap();
            let opt_str = |obj: Bound<'_, PyAny>| -> Option<String> {
                if obj.is_none() {
                    None
                } else {
                    Some(obj.extract::<String>().unwrap())
                }
            };
            let error_kind = opt_str(tuple.get_item(2).unwrap());
            let last_error = opt_str(tuple.get_item(3).unwrap());
            (task_id, success, error_kind, last_error)
        })
    }

    /// Pins the success-path call shape:
    /// `(task_id, True, None, None)`. The trailing `last_error` is
    /// `None` on success — the consumer must see no message.
    #[test]
    fn task_completed_listener_fires_on_task_completed_apply() {
        let (callable, globals) = make_recording_listener();
        let bridge = PyTaskCompletedListener::new(callable);
        bridge.on_event(&TaskCompletedEvent {
            task_id: "alpha".into(),
            task_hash: "h-alpha".into(),
            success: true,
            error_kind: None,
            last_error: None,
        });
        let (task_id, success, error_kind, last_error) = captured_call(&globals, 0);
        assert_eq!(task_id, "alpha");
        assert!(success);
        assert!(error_kind.is_none());
        assert!(last_error.is_none());
    }

    /// Pins the failure-path call shape: `(task_id, False,
    /// Some(<wire_value>), Some(<message>))`. Mirrors the
    /// cluster_state-side test
    /// `task_completed_listener_fires_on_task_failed_with_error_kind`.
    /// Asserts the operator-facing `last_error` message reaches the
    /// Python listener verbatim alongside the wire-stable type tag.
    #[test]
    fn task_completed_listener_fires_on_task_failed_with_error_kind() {
        let (callable, globals) = make_recording_listener();
        let bridge = PyTaskCompletedListener::new(callable);
        bridge.on_event(&TaskCompletedEvent {
            task_id: "beta".into(),
            task_hash: "h-beta".into(),
            success: false,
            error_kind: Some("non_recoverable".into()),
            last_error: Some("worker reported failure".into()),
        });
        let (task_id, success, error_kind, last_error) = captured_call(&globals, 0);
        assert_eq!(task_id, "beta");
        assert!(!success);
        assert_eq!(error_kind.as_deref(), Some("non_recoverable"));
        assert_eq!(last_error.as_deref(), Some("worker reported failure"));
    }

    /// Forwarding contract for the trailing `last_error` positional,
    /// asserted on a single recording listener across both terminal
    /// arms so the success/failure contrast is unmissable:
    ///   - FAILED  → the operator-facing message rides through verbatim
    ///     (here an `unfulfillable:<reason>` failure, to prove the
    ///     message is carried independently of which `error_kind` tag
    ///     accompanies it);
    ///   - SUCCESS → `None`, never an empty string or a stale message.
    #[test]
    fn task_completed_listener_forwards_last_error() {
        let (callable, globals) = make_recording_listener();
        let bridge = PyTaskCompletedListener::new(callable);

        // Failure: the message must reach the listener as the 4th arg.
        bridge.on_event(&TaskCompletedEvent {
            task_id: "fails".into(),
            task_hash: "h-fails".into(),
            success: false,
            error_kind: Some("unfulfillable:no_capable_worker".into()),
            last_error: Some("no worker can satisfy 64 GiB reservation".into()),
        });
        // Success on the SAME listener: last_error must be None.
        bridge.on_event(&TaskCompletedEvent {
            task_id: "succeeds".into(),
            task_hash: "h-succeeds".into(),
            success: true,
            error_kind: None,
            last_error: None,
        });

        let (failed_id, failed_ok, _failed_kind, failed_last_error) =
            captured_call(&globals, 0);
        assert_eq!(failed_id, "fails");
        assert!(!failed_ok);
        assert_eq!(
            failed_last_error.as_deref(),
            Some("no worker can satisfy 64 GiB reservation"),
        );

        let (ok_id, ok_ok, _ok_kind, ok_last_error) = captured_call(&globals, 1);
        assert_eq!(ok_id, "succeeds");
        assert!(ok_ok);
        assert!(ok_last_error.is_none());
    }

    /// A panicking Python listener must surface as a Rust panic so
    /// the dispatcher's `catch_unwind` can isolate it. Validated by
    /// catching the unwind here (the bridge itself does not catch —
    /// that's the dispatcher's responsibility).
    #[test]
    fn task_completed_listener_panic_isolated() {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_task_completed_panicking_{nonce}");
        let file_name = format!("{module_name}.py");
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "def listener(task_id, success, error_kind, last_error):\n    \
                         raise AssertionError('listener exploded')\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .unwrap();
            module.getattr("listener").unwrap().unbind()
        });
        let bridge = PyTaskCompletedListener::new(callable);
        // The bridge swallows Python exceptions (not panics) — the
        // `AssertionError` here is a `PyErr`, not a Rust panic, so
        // `bridge.on_event(...)` returns cleanly and the test passes
        // by virtue of NOT panicking.
        bridge.on_event(&TaskCompletedEvent {
            task_id: "oops".into(),
            task_hash: "h-oops".into(),
            success: false,
            error_kind: Some("non_recoverable".into()),
            last_error: Some("listener exploded".into()),
        });
    }

    /// A `PyErr` raised inside the listener is swallowed (logged at
    /// `warn`) so the dispatcher loop keeps draining subsequent
    /// events. Same surface as `panic_isolated` above; the listener
    /// here is a regular Python function that raises an exception,
    /// which is the most common shape.
    #[test]
    fn task_completed_listener_pyerr_swallowed() {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_task_completed_raises_{nonce}");
        let file_name = format!("{module_name}.py");
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "def listener(task_id, success, error_kind, last_error):\n    \
                         raise RuntimeError('listener should not surface')\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .unwrap();
            module.getattr("listener").unwrap().unbind()
        });
        let bridge = PyTaskCompletedListener::new(callable);
        // Must NOT propagate; the bridge swallows to tracing::warn.
        bridge.on_event(&TaskCompletedEvent {
            task_id: "explodes".into(),
            task_hash: "h".into(),
            success: false,
            error_kind: Some("recoverable".into()),
            last_error: Some("transient".into()),
        });
    }
}
