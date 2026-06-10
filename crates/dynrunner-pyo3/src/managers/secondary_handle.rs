//! `SecondaryHandle` — the minimal secondary-side twin of
//! [`crate::managers::primary_handle::PyPrimaryHandle`].
//!
//! Single concern: give consumer Python code running ON a secondary
//! (today: the duck-typed `worker_message_listener` TaskDefinition
//! hook) a typed capability to act through the secondary's control
//! plane. The handle holds ONLY channel senders — it never touches
//! the worker pool or the mesh; every action is a queued
//! [`SecondaryControlCommand`] the secondary's operational loop
//! drains on its own thread (the dispatch-decoupling law).
//!
//! Minted inside `PySecondaryCoordinator::run` (the only place the
//! inner coordinator's control sender exists) and handed to the
//! consumer exclusively as the `secondary_handle` argument of
//! `worker_message_listener(worker_id, type_id, topic, data,
//! secondary_handle)` — consumers never construct one.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use dynrunner_manager_distributed::secondary::SecondaryControlCommand;
use dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES;

// The worker-IPC and mesh custom-message limits are deliberately ONE
// consumer-facing number (both gates in this file quote it as "the
// 100 KiB custom-message limit"); a divergence would make this file's
// pre-checks lie to one of the two downstream defensive re-checks.
const _: () = assert!(
    CUSTOM_MESSAGE_MAX_BYTES == dynrunner_protocol_primary_secondary::CUSTOM_MESSAGE_MAX_BYTES
);

/// Python-facing `SecondaryHandle`.
#[pyclass(name = "SecondaryHandle")]
pub(crate) struct PySecondaryHandle {
    control_tx: tokio::sync::mpsc::UnboundedSender<SecondaryControlCommand>,
}

impl PySecondaryHandle {
    /// Build from the inner coordinator's control sender (a clone of
    /// `SecondaryCoordinator::secondary_control_sender()`).
    pub(crate) fn new(
        control_tx: tokio::sync::mpsc::UnboundedSender<SecondaryControlCommand>,
    ) -> Self {
        Self { control_tx }
    }
}

#[pymethods]
impl PySecondaryHandle {
    /// Queue a `Command::Custom { topic, data }` to one of THIS
    /// secondary's workers (the reply direction of the
    /// worker↔secondary custom-message channel). Non-blocking: the
    /// message rides the control channel to the operational loop,
    /// which delivers it through the worker slot's typed
    /// custom-message allowance (inline when Idle; mid-task over the
    /// full-duplex socket when Processing — the worker reads it via
    /// `Task.poll_messages()`).
    ///
    /// Raises `ValueError` (naming size + limit) when `data` exceeds
    /// `CUSTOM_MESSAGE_MAX_BYTES` (100 KiB); `RuntimeError` when the
    /// secondary's operational loop is gone (run torn down).
    fn send_to_worker(&self, worker_id: u32, topic: &str, data: &[u8]) -> PyResult<()> {
        if data.len() > CUSTOM_MESSAGE_MAX_BYTES {
            return Err(PyValueError::new_err(format!(
                "send_to_worker data for topic {topic:?} is {} bytes, exceeding the \
                 CUSTOM_MESSAGE_MAX_BYTES limit of {CUSTOM_MESSAGE_MAX_BYTES} bytes \
                 (100 KiB). Custom messages are for signal-sized consumer payloads; \
                 split bulk data into multiple messages or publish it through the \
                 task-output channels.",
                data.len(),
            )));
        }
        self.control_tx
            .send(SecondaryControlCommand::SendToWorker {
                worker_id,
                topic: topic.to_owned(),
                data: data.to_vec(),
            })
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "send_to_worker failed: the secondary's operational loop has \
                     shut down (run torn down)",
                )
            })
    }

    /// Relay a custom message to the CURRENT PRIMARY (droppable, or
    /// retained + CRDT-resident when `important=True`). Non-blocking:
    /// the message rides the control channel to the operational loop,
    /// whose send seam stamps the per-origin idempotency key and hands
    /// the frame to the primary-send chokepoint — `important=False` is
    /// droppable (at-most-once; lost on failover/no-route by design),
    /// `important=True` is retained and replayed until the primary
    /// confirms the landing, then ledger-recorded until the consumer's
    /// `custom_message_handler` resolves it (see the
    /// `CustomMessageHandler` contract in `task_protocol.py`).
    ///
    /// Raises `ValueError` (naming size + limit) when `data` exceeds
    /// `CUSTOM_MESSAGE_MAX_BYTES` (100 KiB); `RuntimeError` when the
    /// secondary's operational loop is gone (run torn down).
    #[pyo3(signature = (topic, data, important = false))]
    fn send_to_primary(&self, topic: &str, data: &[u8], important: bool) -> PyResult<()> {
        if data.len() > CUSTOM_MESSAGE_MAX_BYTES {
            return Err(PyValueError::new_err(format!(
                "send_to_primary data for topic {topic:?} is {} bytes, exceeding the \
                 CUSTOM_MESSAGE_MAX_BYTES limit of {CUSTOM_MESSAGE_MAX_BYTES} bytes \
                 (100 KiB). Custom messages are for signal-sized consumer payloads; \
                 split bulk data into multiple messages or publish it through the \
                 task-output channels.",
                data.len(),
            )));
        }
        self.control_tx
            .send(SecondaryControlCommand::SendToPrimary {
                topic: topic.to_owned(),
                data: data.to_vec(),
                important,
            })
            .map_err(|_| {
                PyRuntimeError::new_err(
                    "send_to_primary failed: the secondary's operational loop has \
                     shut down (run torn down)",
                )
            })
    }
}
