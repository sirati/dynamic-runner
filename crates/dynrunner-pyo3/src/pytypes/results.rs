//! Python-visible result types: `ProcessingStats` + `FailedTask`.

use pyo3::prelude::*;

use super::task_info::PyTaskInfo;

/// Python-visible processing stats.
#[pyclass(name = "ProcessingStats")]
pub(crate) struct PyProcessingStats {
    #[pyo3(get)]
    pub(crate) completed: u32,
    #[pyo3(get)]
    pub(crate) total: u32,
    #[pyo3(get)]
    pub(crate) errored: u32,
    #[pyo3(get)]
    pub(crate) skipped: u32,
}

/// Python-visible failed task.
#[pyclass(name = "FailedTask")]
pub(crate) struct PyFailedTask {
    #[pyo3(get)]
    pub(crate) binary: PyTaskInfo,
    #[pyo3(get)]
    pub(crate) error_type: String,
    #[pyo3(get)]
    pub(crate) error_message: String,
}
