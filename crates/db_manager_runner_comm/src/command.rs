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
