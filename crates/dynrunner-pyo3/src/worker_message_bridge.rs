//! PyO3 bridge: adapt the consumer's duck-typed
//! `worker_message_listener` TaskDefinition attribute into a
//! [`WorkerMessageListener`] the manager-distributed dispatcher can
//! call.
//!
//! Single concern of this file: convert one Rust
//! `&WorkerCustomMessage` into one Python call
//! `listener(worker_id, type_id, topic, data, secondary_handle)`
//! under the GIL, swallowing every `PyErr` path to `tracing::warn` so
//! the dispatcher task never tears down on a buggy / raising Python
//! listener. Nothing about WHICH coordinator owns the listener, HOW
//! the secondary detects the duck-typed attribute, or WHEN
//! registration runs lives here — those concerns belong to
//! `managers/secondary/run.rs` and stay thin there.
//!
//! Listener shape (Python side — a TaskDefinition attribute, the
//! `task_completed_listener` idiom):
//!
//! ```python
//! def worker_message_listener(
//!     self, worker_id: int, type_id: str, topic: str, data: bytes,
//!     secondary_handle: SecondaryHandle,
//! ) -> None: ...
//! ```
//!
//! `secondary_handle` is the SAME
//! [`crate::managers::secondary_handle::PySecondaryHandle`] instance
//! on every fire (minted once at registration): the listener replies
//! via `secondary_handle.send_to_worker(...)` and — once feature 5
//! lands — relays via `send_to_primary(...)`.
//!
//! Error / exception handling (the `task_completed_listener` idiom):
//!   - `PyErr` from the call surfaces a `tracing::warn` and is
//!     swallowed (the dispatcher's `catch_unwind` only catches Rust
//!     panics; a `PyErr` is a value-level error pyo3 wraps).
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     dispatcher's `catch_unwind` isolates — so even a Python
//!     `assert` or a pyo3-side panic can't take the dispatcher down.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use dynrunner_manager_distributed::worker_messages::{WorkerCustomMessage, WorkerMessageListener};

use crate::managers::secondary_handle::PySecondaryHandle;

/// Adapter that holds the unbound Python listener plus the minted
/// `SecondaryHandle` pyobject and dispatches each event to the
/// matching Python call. `Send + Sync` is satisfied by `Py<PyAny>`'s
/// contract — the underlying objects are reference-counted through
/// Python's GIL.
pub(crate) struct PyWorkerMessageListener {
    /// The Python listener callable (the bound
    /// `task_definition.worker_message_listener` method). Held as an
    /// unbound `Py<PyAny>` so the adapter outlives any single
    /// `Python<'py>` lifetime; each `on_message` re-binds under a
    /// fresh GIL acquisition.
    listener: Py<PyAny>,
    /// The one `SecondaryHandle` instance passed as the trailing
    /// positional on every fire.
    handle: Py<PySecondaryHandle>,
}

impl PyWorkerMessageListener {
    /// Build a bridge from a Python listener callable + the minted
    /// handle. Boxed at the call site (returned as
    /// `Box<dyn WorkerMessageListener>`) so the manager-distributed
    /// registration API consumes a uniform trait-object shape.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        listener: Py<PyAny>,
        handle: Py<PySecondaryHandle>,
    ) -> Box<dyn WorkerMessageListener> {
        Box::new(Self { listener, handle })
    }
}

impl WorkerMessageListener for PyWorkerMessageListener {
    fn on_message(&self, event: &WorkerCustomMessage) {
        // GIL acquisition crosses the runtime boundary. Per-event
        // cost is one attach + one call; the worker-event bridge's
        // emit is non-blocking so this latency is invisible to the
        // secondary's operational loop.
        let outcome: PyResult<()> = Python::attach(|py| {
            let data = PyBytes::new(py, &event.data);
            let args = (
                event.worker_id,
                event.type_id.as_str(),
                event.topic.as_str(),
                data,
                self.handle.bind(py),
            );
            self.listener.bind(py).call1(args).map(|_| ())
        });
        if let Err(e) = outcome {
            tracing::warn!(
                target: "dynrunner_pyo3_worker_messages",
                worker_id = event.worker_id,
                topic = %event.topic,
                error = %e,
                "Python worker-message listener raised; swallowed to keep dispatcher alive",
            );
        }
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Contract tests for the listener bridge. Each test drives the
    //! `WorkerMessageListener::on_message` surface (the dispatcher's
    //! entry point) with a hand-built `WorkerCustomMessage`, captures
    //! the resulting Python call on a mock listener, and asserts the
    //! positional-argument shape.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python worker_message_bridge`
    use super::*;
    use dynrunner_manager_distributed::secondary::SecondaryControlCommand;
    use pyo3::types::{PyDict, PyList};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Per-call atomic counter so each recording listener gets a
    /// unique module name (parallel `cargo test` harness safety —
    /// same rationale as the task_completed_bridge tests).
    static MODULE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn make_handle() -> (
        Py<PySecondaryHandle>,
        tokio::sync::mpsc::UnboundedReceiver<SecondaryControlCommand>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle =
            Python::attach(|py| Py::new(py, PySecondaryHandle::new(tx)).expect("mint handle"));
        (handle, rx)
    }

    fn make_recording_listener() -> (Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_worker_message_{nonce}");
        let file_name = format!("{module_name}.py");
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "calls = []\n\
                     def listener(worker_id, type_id, topic, data, secondary_handle):\n    \
                         calls.append((worker_id, type_id, topic, bytes(data), secondary_handle))\n",
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

    /// Pins the positional call shape `(worker_id, type_id, topic,
    /// data: bytes, secondary_handle)` — the FROZEN consumer
    /// signature.
    #[test]
    fn worker_message_listener_receives_frozen_signature() {
        let (callable, globals) = make_recording_listener();
        let (handle, _rx) = make_handle();
        let bridge = PyWorkerMessageListener::new(callable, handle);
        bridge.on_message(&WorkerCustomMessage {
            worker_id: 3,
            type_id: "dep_graph".into(),
            topic: "phase4-batch".into(),
            data: b"\x00binary\npayload:".to_vec(),
        });
        Python::attach(|py| {
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<PyList>().unwrap();
            assert_eq!(list.len(), 1);
            let entry = list.get_item(0).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let worker_id: u32 = tuple.get_item(0).unwrap().extract().unwrap();
            let type_id: String = tuple.get_item(1).unwrap().extract().unwrap();
            let topic: String = tuple.get_item(2).unwrap().extract().unwrap();
            let data: Vec<u8> = tuple.get_item(3).unwrap().extract().unwrap();
            assert_eq!(worker_id, 3);
            assert_eq!(type_id, "dep_graph");
            assert_eq!(topic, "phase4-batch");
            assert_eq!(data, b"\x00binary\npayload:");
            // The trailing positional is the SecondaryHandle pyclass.
            let handle_obj = tuple.get_item(4).unwrap();
            assert!(handle_obj.cast::<PySecondaryHandle>().is_ok());
        });
    }

    /// The listener can reply through the handed `secondary_handle`:
    /// `send_to_worker` queues a `SecondaryControlCommand` on the
    /// control channel (the reply half of the e2e contract).
    #[test]
    fn listener_reply_via_handle_reaches_control_channel() {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_worker_message_reply_{nonce}");
        let file_name = format!("{module_name}.py");
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "def listener(worker_id, type_id, topic, data, secondary_handle):\n    \
                         secondary_handle.send_to_worker(worker_id, 'reply:' + topic, data)\n",
                )
                .unwrap()
                .as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .unwrap();
            module.getattr("listener").unwrap().unbind()
        });
        let (handle, mut rx) = make_handle();
        let bridge = PyWorkerMessageListener::new(callable, handle);
        bridge.on_message(&WorkerCustomMessage {
            worker_id: 7,
            type_id: "t".into(),
            topic: "ping".into(),
            data: b"abc".to_vec(),
        });
        let cmd = rx.try_recv().expect("reply queued on control channel");
        match cmd {
            SecondaryControlCommand::SendToWorker {
                worker_id,
                topic,
                data,
            } => {
                assert_eq!(worker_id, 7);
                assert_eq!(topic, "reply:ping");
                assert_eq!(data, b"abc");
            }
            other => panic!("expected the listener's SendToWorker reply, got {other:?}"),
        }
    }

    /// Oversize reply via the handle raises `ValueError` naming size
    /// and limit (the call-site enforcement contract), and the raise
    /// is swallowed by the bridge (dispatcher stays alive).
    #[test]
    fn handle_send_to_worker_rejects_oversize_with_valueerror() {
        let (handle, mut rx) = make_handle();
        Python::attach(|py| {
            let bound = handle.bind(py);
            let oversize = vec![0u8; dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES + 1];
            let err = bound
                .call_method1("send_to_worker", (0u32, "big", PyBytes::new(py, &oversize)))
                .expect_err("oversize must raise");
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let msg = err.to_string();
            assert!(msg.contains(&(oversize.len()).to_string()), "names size: {msg}");
            assert!(
                msg.contains(
                    &dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES.to_string()
                ),
                "names limit: {msg}"
            );
        });
        assert!(rx.try_recv().is_err(), "nothing queued on oversize");
    }

    /// `send_to_primary` queues a `SendToPrimary` control command for
    /// the operational loop (default `important=False`; the kwarg is
    /// forwarded), and oversize raises `ValueError` naming size +
    /// limit with nothing queued — the same call-site contract as
    /// `send_to_worker`.
    #[test]
    fn handle_send_to_primary_queues_control_command() {
        let (handle, mut rx) = make_handle();
        Python::attach(|py| {
            let bound = handle.bind(py);
            bound
                .call_method1("send_to_primary", ("t", PyBytes::new(py, b"d")))
                .expect("droppable send queues");
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("important", true).unwrap();
            bound
                .call_method("send_to_primary", ("u", PyBytes::new(py, b"e")), Some(&kwargs))
                .expect("important send queues");
        });
        match rx.try_recv().expect("droppable command queued") {
            SecondaryControlCommand::SendToPrimary {
                topic,
                data,
                important,
            } => {
                assert_eq!(topic, "t");
                assert_eq!(data, b"d");
                assert!(!important, "default delivery class is droppable");
            }
            other => panic!("expected SendToPrimary, got {other:?}"),
        }
        match rx.try_recv().expect("important command queued") {
            SecondaryControlCommand::SendToPrimary {
                topic, important, ..
            } => {
                assert_eq!(topic, "u");
                assert!(important, "important kwarg forwarded");
            }
            other => panic!("expected SendToPrimary, got {other:?}"),
        }

        let (handle, mut rx) = make_handle();
        Python::attach(|py| {
            let bound = handle.bind(py);
            let oversize = vec![0u8; dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES + 1];
            let err = bound
                .call_method1("send_to_primary", ("big", PyBytes::new(py, &oversize)))
                .expect_err("oversize must raise");
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
            let msg = err.to_string();
            assert!(msg.contains(&(oversize.len()).to_string()), "names size: {msg}");
            assert!(
                msg.contains(
                    &dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES.to_string()
                ),
                "names limit: {msg}"
            );
        });
        assert!(rx.try_recv().is_err(), "nothing queued on oversize");
    }

    /// A `PyErr` raised inside the listener is swallowed (logged at
    /// `warn`) so the dispatcher loop keeps draining subsequent
    /// events — the `task_completed_listener` isolation idiom.
    #[test]
    fn worker_message_listener_pyerr_swallowed() {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_worker_message_raises_{nonce}");
        let file_name = format!("{module_name}.py");
        let callable = Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(
                    "def listener(worker_id, type_id, topic, data, secondary_handle):\n    \
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
        let (handle, _rx) = make_handle();
        let bridge = PyWorkerMessageListener::new(callable, handle);
        // Must NOT propagate; the bridge swallows to tracing::warn.
        bridge.on_message(&WorkerCustomMessage {
            worker_id: 0,
            type_id: "t".into(),
            topic: "boom".into(),
            data: Vec::new(),
        });
    }
}
