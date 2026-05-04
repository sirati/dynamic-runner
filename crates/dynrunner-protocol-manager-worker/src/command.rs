use dynrunner_core::ErrorType;

#[derive(Debug, Clone)]
pub enum Command {
    Stop,
    /// Dispatch a task to the worker.
    ///
    /// `relative_path` is the worker-facing identifier the framework
    /// passes through verbatim — for file-based tasks it's a real
    /// path the worker opens; for `uses_file_based_items=False` tasks
    /// (FR-2) it's an opaque key the worker resolves however it
    /// wants.
    ///
    /// `payload` is the opaque per-item data the consumer attached to
    /// the original `TaskInfo.payload` (JSON value, serialised as a
    /// string for transport). `None` means "the task carries no
    /// payload"; the wire then collapses to the historical
    /// `<path>\n` line. `Some(json)` ships the wrapped form so a
    /// worker that opted into payload data (FR-3 use case) can
    /// process the task without an additional filesystem read.
    ProcessTask {
        relative_path: String,
        payload: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum Response {
    Ready,
    Done {
        result_data: Option<Vec<u8>>,
    },
    Error {
        error_type: ErrorType,
        message: String,
    },
    /// Unhandled exception in the worker (formerly `PickledError`).
    /// Carries plain strings: no Python objects are deserialised.
    WorkerException {
        exception_type: String,
        message: String,
        traceback: String,
    },
    PhaseUpdate {
        phase_name: String,
    },
    Keepalive,
}
