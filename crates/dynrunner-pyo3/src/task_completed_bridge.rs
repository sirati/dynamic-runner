//! PyO3 bridge: adapt a Python listener callable into a
//! [`TaskCompletedListener`] the manager-distributed dispatcher can
//! call.
//!
//! Single concern of this file: convert one Rust
//! `&TaskCompletedEvent` into one Python call
//! `listener(task_id, success, error_kind)` under the GIL, swallowing
//! every `PyErr` path to `tracing::warn` so the dispatcher task never
//! tears down on a buggy / raising Python listener. Nothing about
//! WHICH manager owns the listener, HOW the manager threads the kwarg
//! through, or WHEN `register_task_completed_listener` runs lives
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
//! ) -> None: ...
//! ```
//!
//! Error / exception handling:
//!   - `PyErr` from the call surfaces a `tracing::warn` and is
//!     swallowed (the dispatcher's `catch_unwind` only catches Rust
//!     panics; a `PyErr` is a value-level error pyo3 wraps).
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     dispatcher's `catch_unwind` isolates — so even a Python
//!     `assert` or a pyo3-side panic can't take the dispatcher down.

use pyo3::prelude::*;

use dynrunner_manager_distributed::task_completed::{
    TaskCompletedEvent, TaskCompletedListener,
};

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
            // remains optional (the success arm leaves it `None`)
            // and goes through `Option<&str> → str | None` via
            // PyO3's IntoPyObject for `Option<T>`.
            let args = (
                event.task_id.as_str(),
                event.success,
                event.error_kind.as_deref(),
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
                     def listener(task_id, success, error_kind):\n    \
                         calls.append((task_id, success, error_kind))\n",
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
    /// `error_kind` remains `str | None` (success → `None`).
    fn captured_call(
        globals: &Py<PyAny>,
        idx: usize,
    ) -> (String, bool, Option<String>) {
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
            let error_kind_obj = tuple.get_item(2).unwrap();
            let error_kind = if error_kind_obj.is_none() {
                None
            } else {
                Some(error_kind_obj.extract::<String>().unwrap())
            };
            (task_id, success, error_kind)
        })
    }

    /// Pins the success-path call shape: `(task_id, True, None)`.
    #[test]
    fn task_completed_listener_fires_on_task_completed_apply() {
        let (callable, globals) = make_recording_listener();
        let bridge = PyTaskCompletedListener::new(callable);
        bridge.on_event(&TaskCompletedEvent {
            task_id: "alpha".into(),
            task_hash: "h-alpha".into(),
            success: true,
            error_kind: None,
        });
        let (task_id, success, error_kind) = captured_call(&globals, 0);
        assert_eq!(task_id, "alpha");
        assert!(success);
        assert!(error_kind.is_none());
    }

    /// Pins the failure-path call shape: `(task_id, False,
    /// Some(<wire_value>))`. Mirrors the cluster_state-side test
    /// `task_completed_listener_fires_on_task_failed_with_error_kind`.
    #[test]
    fn task_completed_listener_fires_on_task_failed_with_error_kind() {
        let (callable, globals) = make_recording_listener();
        let bridge = PyTaskCompletedListener::new(callable);
        bridge.on_event(&TaskCompletedEvent {
            task_id: "beta".into(),
            task_hash: "h-beta".into(),
            success: false,
            error_kind: Some("non_recoverable".into()),
        });
        let (task_id, success, error_kind) = captured_call(&globals, 0);
        assert_eq!(task_id, "beta");
        assert!(!success);
        assert_eq!(error_kind.as_deref(), Some("non_recoverable"));
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
                    "def listener(task_id, success, error_kind):\n    \
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
                    "def listener(task_id, success, error_kind):\n    \
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
        });
    }
}
