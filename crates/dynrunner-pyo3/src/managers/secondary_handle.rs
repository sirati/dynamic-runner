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

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use dynrunner_manager_distributed::secondary::SecondaryControlCommand;
use dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES;

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
    /// retained + CRDT-resident when `important=True`).
    ///
    /// TODO(F5): stub — the secondary→primary custom-message channel
    /// (DistributedMessage::CustomMessage + the #352 retention reuse +
    /// the CustomMessagePosted/Handled CRDT lattice) is feature 5's
    /// surface and is implemented by its owning change; this method
    /// signature is the frozen seam it fills in. Until then it raises
    /// `NotImplementedError` so a consumer wiring against it fails
    /// loudly rather than silently dropping.
    #[pyo3(signature = (topic, data, important = false))]
    #[allow(unused_variables)]
    fn send_to_primary(&self, topic: &str, data: &[u8], important: bool) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "SecondaryHandle.send_to_primary is not wired yet: the \
             secondary→primary custom-message channel (feature 5) ships \
             separately. send_to_worker is live.",
        ))
    }
}
