use db_comm_api_base::ErrorType;

#[derive(Debug, Clone)]
pub enum Command {
    Stop,
    ProcessTask { relative_path: String },
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
