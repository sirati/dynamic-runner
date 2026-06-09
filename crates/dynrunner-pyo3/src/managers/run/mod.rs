//! Free-function entry points for the four runner modes.
//!
//! These are the recommended Python-facing API: build a typed config (e.g.
//! `LocalManagerConfig`), call `run_local(config, ...)`, get a result
//! object back. The legacy `Rust*Manager` / `Rust*Coordinator` classes
//! remain callable for one release as deprecated shims.
//!
//! Implementation strategy: the free functions construct the legacy
//! pyclass via the Python module, set the right kwargs, and return its
//! results as a dict. This keeps the actual run logic single-sourced in
//! the manager pyclasses themselves — no duplication of the
//! tokio-runtime / SubprocessWorkerFactory plumbing.
//!
//! The four `run_*` entry points live in sibling files keyed by mode
//! (`local`, `primary`, `secondary`, `distributed`); they all share the
//! `module()` helper to resolve the `dynamic_runner` Python module for
//! the pyclass lookup. `compute_task_hash` is a small Python-facing
//! utility that pre-computes the file_hash a primary will assign to a
//! `TaskInfo` so callers can stage files against it; it stays here
//! because it is too small (~10 LoC) to deserve its own sibling and is
//! the only non-mode helper.

use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::pytypes::extract_binaries;

mod distributed;
mod local;
mod primary;
mod secondary;

pub(crate) use distributed::run_distributed;
pub(crate) use local::run_local;
pub(crate) use primary::run_primary;
pub(crate) use secondary::run_secondary;

/// Compute the file_hash that the Rust primary will assign to a Python
/// `TaskInfo` when it sends a `TaskAssignment`. The hash is stable
/// for a given (phase_id, path, identifier) triple — `phase_id` is
/// folded into the recipe so the same content declared in two phases
/// hashes distinctly. Pipelines pre-stage files against this hash so the
/// secondary's `ExtractionCache` accepts the stage notification.
#[pyfunction]
pub(crate) fn compute_task_hash(py: Python<'_>, binary: &Bound<'_, PyAny>) -> PyResult<String> {
    let single = pyo3::types::PyList::new(py, [binary])?;
    let mut rust_binaries = extract_binaries(&single)?;
    // The content hash is over the scheduling unit only; the discovery
    // already-done marker does not participate (it is not on `TaskInfo`),
    // so discard the bit and hash the task.
    let (bin, _skipped) = rust_binaries.pop().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("compute_task_hash: failed to extract binary")
    })?;
    Ok(dynrunner_manager_distributed::compute_task_hash(&bin))
}

/// Resolve the top-level `dynamic_runner` Python module so the
/// per-mode entry points can fetch the legacy pyclass they delegate to.
pub(super) fn module<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
    py.import("dynamic_runner")
}
