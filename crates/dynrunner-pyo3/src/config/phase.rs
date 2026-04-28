use pyo3::prelude::*;

use dynrunner_scheduler_api::ProcessingPhase;

/// Python mirror of `dynrunner_scheduler_api::ProcessingPhase`. Not equal to the
/// worker-side phase strings (which stay open) — this enum names only the
/// orchestrator's pipeline phases.
#[pyclass(name = "Phase", eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyPhase {
    InitialAssignment,
    MainPhase,
    RetryPhase,
    ResourcePressurePhase,
    UnassignedPhase,
    Complete,
}

impl From<ProcessingPhase> for PyPhase {
    fn from(p: ProcessingPhase) -> Self {
        match p {
            ProcessingPhase::InitialAssignment => Self::InitialAssignment,
            ProcessingPhase::MainPhase => Self::MainPhase,
            ProcessingPhase::RetryPhase => Self::RetryPhase,
            ProcessingPhase::ResourcePressurePhase => Self::ResourcePressurePhase,
            ProcessingPhase::UnassignedPhase => Self::UnassignedPhase,
            ProcessingPhase::Complete => Self::Complete,
        }
    }
}

impl From<PyPhase> for ProcessingPhase {
    fn from(p: PyPhase) -> Self {
        match p {
            PyPhase::InitialAssignment => Self::InitialAssignment,
            PyPhase::MainPhase => Self::MainPhase,
            PyPhase::RetryPhase => Self::RetryPhase,
            PyPhase::ResourcePressurePhase => Self::ResourcePressurePhase,
            PyPhase::UnassignedPhase => Self::UnassignedPhase,
            PyPhase::Complete => Self::Complete,
        }
    }
}
