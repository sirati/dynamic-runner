//! Python `TaskDefinition.custom_message_handler` bridge (F5).
//!
//! Single concern: turn the consumer's duck-typed
//! `custom_message_handler` attribute into the
//! [`dynrunner_manager_distributed::primary::OnCustomMessage`] closure
//! the `PrimaryCoordinator`'s handler-dispatch decision invokes. Same
//! GIL-reacquiring shape as the `make_on_phase_*` bridges in
//! `managers/lifecycle.rs`; same duck-typed-attribute registration idiom
//! as `task_completed_listener` / `peer_lifecycle_listener`.
//!
//! Frozen consumer signature (fired ON the primary only):
//!
//! ```python
//! def custom_message_handler(
//!     self, origin: str, topic: str, data: bytes, important: bool,
//!     primary_handle: PrimaryHandle,
//! ) -> None: ...
//! ```
//!
//! `primary_handle` is load-bearing: the handler IS the streamed-spawn
//! site (`primary_handle.spawn_tasks(batch)`), and the handle's
//! in-runtime `try_send` path already works from coordinator-fired
//! callbacks — the coordinator drains the queued commands through the
//! same `drain_callback_queued_commands` chokepoint `on_phase_end`
//! uses, after every handler invocation.
//!
//! Error policy: a handler raise is REPORTED to the dispatch decision
//! (the closure returns `Err(reason)`), which leaves an important
//! message `Unhandled` for a backoff retry (poison-capped) and loses a
//! droppable one — the F5 contract. The raise is logged here (the only
//! place the `PyErr` exists); the decision owns the retry policy.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::mpsc as tokio_mpsc;

use dynrunner_manager_distributed::primary::{OnCustomMessage, PrimaryCommand};

use crate::identifier::RunnerIdentifier;
use crate::managers::primary_handle::{PyPrimaryHandle, ReinjectCapCell};

/// Does the consumer's `TaskDefinition` expose a (non-`None`)
/// `custom_message_handler`? Checked GIL-side at hook-capture time so
/// the coordinator's `Option<OnCustomMessage>` honestly encodes
/// "consumer has no handler" (the dispatch decision then consumes
/// important messages unhandled with a WARN instead of invoking a
/// missing attribute on every message).
pub(crate) fn has_custom_message_handler(task_definition: &Bound<'_, PyAny>) -> bool {
    task_definition
        .getattr("custom_message_handler")
        .map(|attr| !attr.is_none())
        .unwrap_or(false)
}

/// Build the `OnCustomMessage` closure: re-acquire the GIL and call
/// `task_definition.custom_message_handler(origin, topic, data,
/// important, primary_handle)`.
///
/// `sender` MUST be the command channel the coordinator that fires this
/// closure actually drains (read it off the coordinator AFTER any
/// `replace_command_channel`, e.g. via `command_sender()`), so the
/// handler's `spawn_tasks` lands on THAT primary's loop — on the
/// promoted-primary recipe this is the relocated/promoted coordinator's
/// own channel, never the dead submitter's.
///
/// The handle minted here is purpose-built for the handler: its
/// reinject-cap cell is marked run-started (the closure only ever fires
/// inside a running coordinator), so the cap setter raises the honest
/// "run already started" error instead of silently writing a cell no
/// config will ever read.
pub(crate) fn make_custom_message_handler(
    task_definition: Py<PyAny>,
    sender: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,
) -> PyResult<OnCustomMessage> {
    let reinject_cap = ReinjectCapCell::default();
    reinject_cap.mark_run_started();
    let handle = PyPrimaryHandle::from_sender(sender, reinject_cap)?;
    Ok(Box::new(
        move |origin: &str, topic: &str, data: &[u8], important: bool| {
            // DEADLOCK INVARIANT: this `Python::attach` runs from the
            // coordinator's operational loop (the dispatch decision
            // fires it). Any Python-facing blocking wait inside the
            // handler (`PrimaryHandle::*`) detects the in-runtime
            // context and takes the `try_send` fire-and-forget shape,
            // so the loop is never parked on a reply it must itself
            // produce — same invariant as the phase-edge hooks.
            Python::attach(|py| {
                let handle_obj = Py::new(py, handle.clone()).map_err(|e| {
                    format!("custom_message_handler bridge: PrimaryHandle wrap failed: {e}")
                })?;
                let args = (
                    origin,
                    topic,
                    PyBytes::new(py, data),
                    important,
                    handle_obj,
                );
                match task_definition
                    .bind(py)
                    .call_method1("custom_message_handler", args)
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // Log here (the only place the PyErr exists);
                        // the dispatch decision owns the retry/poison
                        // policy off the returned reason.
                        tracing::warn!(
                            error = %e,
                            origin,
                            topic,
                            important,
                            "TaskDefinition.custom_message_handler raised"
                        );
                        Err(e.value(py).to_string())
                    }
                }
            })
        },
    ))
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Python-linked bridge tests. Excluded from default `cargo test`
    //! (they need an embedded interpreter); enabled by the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --no-default-features \
    //!        --features test-with-python custom_message`
    use super::*;
    use pyo3::types::PyDict;

    fn task_obj(py: Python<'_>, body: &str) -> Py<PyAny> {
        let module = pyo3::types::PyModule::from_code(
            py,
            &std::ffi::CString::new(body).unwrap(),
            c"test_custom_message_handler.py",
            c"test_custom_message_handler",
        )
        .expect("test module compiles");
        module.getattr("Task").unwrap().call0().unwrap().unbind()
    }

    fn stub_sender() -> tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>> {
        // The handler tests never drain the channel; capacity 4 is
        // plenty and a full channel only matters for spawn-issuing
        // handlers (not exercised here).
        let (tx, rx) = tokio_mpsc::channel(4);
        // Keep the receiver alive for the test's duration by leaking it
        // — a dropped receiver would make every handle send raise.
        std::mem::forget(rx);
        tx
    }

    /// The closure calls the consumer hook with the frozen positional
    /// shape `(origin, topic, data, important, primary_handle)` and a
    /// clean return maps to `Ok(())`.
    #[test]
    fn handler_receives_frozen_signature_and_clean_return_is_ok() {
        Python::attach(|py| {
            let task = task_obj(
                py,
                r#"
class Task:
    def __init__(self):
        self.calls = []
    def custom_message_handler(self, origin, topic, data, important, primary_handle):
        assert isinstance(data, bytes)
        assert primary_handle is not None
        self.calls.append((origin, topic, bytes(data), important))
"#,
            );
            assert!(has_custom_message_handler(&task.bind(py).clone()));
            let mut cb = make_custom_message_handler(task.clone_ref(py), stub_sender())
                .expect("bridge builds");
            let out = cb("sec-1", "phase4-batch", b"payload", true);
            assert!(out.is_ok(), "clean handler return maps to Ok: {out:?}");
            let calls: Vec<(String, String, Vec<u8>, bool)> = task
                .bind(py)
                .getattr("calls")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(
                calls,
                vec![(
                    "sec-1".to_string(),
                    "phase4-batch".to_string(),
                    b"payload".to_vec(),
                    true
                )]
            );
        });
    }

    /// A handler raise maps to `Err(reason)` — the dispatch decision's
    /// retry/poison input.
    #[test]
    fn handler_raise_maps_to_err_with_reason() {
        Python::attach(|py| {
            let task = task_obj(
                py,
                r#"
class Task:
    def custom_message_handler(self, origin, topic, data, important, primary_handle):
        raise RuntimeError("decode failed")
"#,
            );
            let mut cb = make_custom_message_handler(task, stub_sender()).expect("bridge builds");
            let out = cb("sec-1", "t", b"x", true);
            let reason = out.expect_err("a raise maps to Err");
            assert!(
                reason.contains("decode failed"),
                "the raise reason is carried: {reason}"
            );
        });
    }

    /// The duck-typed attribute check: absent or `None` attribute means
    /// "no handler" (the coordinator's `Option` stays `None`).
    #[test]
    fn missing_or_none_attribute_is_no_handler() {
        Python::attach(|py| {
            let absent = task_obj(py, "class Task:\n    pass\n");
            assert!(!has_custom_message_handler(&absent.bind(py).clone()));
            let none_attr = task_obj(
                py,
                "class Task:\n    custom_message_handler = None\n",
            );
            assert!(!has_custom_message_handler(&none_attr.bind(py).clone()));
            // Sanity: a dict-shaped object with the attr present counts.
            let d = PyDict::new(py);
            d.set_item("x", 1).unwrap();
            assert!(!has_custom_message_handler(&d.into_any()));
        });
    }
}
