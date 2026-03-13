use crate::types::ErrorType;

#[derive(Debug, Clone)]
pub enum Command {
    Stop,
    ProcessBinary { relative_path: String },
}

#[derive(Debug, Clone)]
pub enum Response {
    Ready,
    Done {
        warnings: u32,
        filtered: u32,
    },
    Error {
        error_type: ErrorType,
        message: String,
    },
    PickledError {
        exception_type: String,
        message: String,
        traceback: String,
    },
    PhaseUpdate {
        phase_name: String,
    },
    Keepalive,
}
