use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use dynrunner_core::{Identifier, ResourceKind, ResourceMap, TaskInfo, TypeId};
use dynrunner_scheduler_api::ResourceEstimator;

use crate::pytypes::{probe_pytask, task_to_pytask};

/// Memory estimator bridge that dispatches per `TypeId`.
///
/// Each entry in `by_type` is the cached classification result for one
/// type's `estimator_attr` Python method:
///   * `Linear` / `Constant` — probed at construction; pure-Rust eval
///     thereafter (no GIL on the hot path).
///   * `PyCallable` — estimator depends on more than `size`, so we
///     re-enter Python under the GIL on every `estimate(...)` call.
///
/// Cloning shares the per-type cache via `Arc` (cheap; the inner
/// `Py<PyAny>` ref-counts on the Python side via Drop).
pub(crate) struct PyMemoryEstimatorBridge {
    by_type: Arc<HashMap<TypeId, EstimatorKind>>,
}

enum EstimatorKind {
    /// `memory = slope * size + intercept` — probe found a linear fit.
    Linear { slope: f64, intercept: f64 },
    /// Both probes returned the same value; treat as constant.
    Constant(u64),
    /// Probe inconclusive (estimator depends on payload, type, etc.).
    /// Calls Python under the GIL on every `estimate`.
    PyCallable(Py<PyAny>),
}

impl Clone for PyMemoryEstimatorBridge {
    fn clone(&self) -> Self {
        Self {
            by_type: Arc::clone(&self.by_type),
        }
    }
}

/// Probe sizes (bytes) used to fit per-type estimators at construction.
const PROBE_SIZE_A: u64 = 1 << 20; // 1 MiB
const PROBE_SIZE_B: u64 = 1 << 30; // 1 GiB

/// Fallback memory amount returned when no estimator is registered for
/// `task.type_id` or the `PyCallable` variant raised.
const FALLBACK_MEMORY: u64 = 1 << 20; // 1 MiB

impl PyMemoryEstimatorBridge {
    /// Build a bridge by probing each `(type_id, estimator_attr)` pair on
    /// `task_definition`.
    ///
    /// For each pair we look up the named method (e.g. `"estimate_memory"`)
    /// and call it twice with TaskInfo-like dummies that differ only in
    /// `size` and `type_id`. The two return values classify the estimator
    /// as `Constant`, `Linear`, or — when the fit can't be determined from
    /// `size` alone — `PyCallable` (re-enters Python per call).
    pub(crate) fn from_python(
        py: Python<'_>,
        task_definition: &Bound<'_, PyAny>,
        types: &[(TypeId, String)],
    ) -> PyResult<Self> {
        let mut by_type: HashMap<TypeId, EstimatorKind> = HashMap::with_capacity(types.len());
        for (type_id, attr) in types {
            let method = task_definition.getattr(attr.as_str())?;
            let kind = probe_estimator(py, &method, type_id)?;
            by_type.insert(type_id.clone(), kind);
        }
        Ok(Self {
            by_type: Arc::new(by_type),
        })
    }
}

fn probe_estimator(
    py: Python<'_>,
    method: &Bound<'_, PyAny>,
    type_id: &TypeId,
) -> PyResult<EstimatorKind> {
    let probe_a = Py::new(py, probe_pytask(PROBE_SIZE_A, type_id.as_str()))?;
    let probe_b = Py::new(py, probe_pytask(PROBE_SIZE_B, type_id.as_str()))?;

    let est_a: u64 = method.call1((probe_a,))?.extract()?;
    let est_b: u64 = method.call1((probe_b,))?.extract()?;

    if est_a == est_b {
        return Ok(EstimatorKind::Constant(est_a));
    }

    let dx = PROBE_SIZE_B as f64 - PROBE_SIZE_A as f64;
    let dy = est_b as f64 - est_a as f64;
    let slope = dy / dx;
    let intercept = est_a as f64 - slope * PROBE_SIZE_A as f64;

    // Verify the linear fit reproduces both probe points (within 1 byte
    // of float-rounding error). If not, the estimator depends on more
    // than `size` — fall back to per-item Python calls.
    let predicted_a = (slope * PROBE_SIZE_A as f64 + intercept).max(0.0) as u64;
    let predicted_b = (slope * PROBE_SIZE_B as f64 + intercept).max(0.0) as u64;
    if (predicted_a as i64 - est_a as i64).unsigned_abs() <= 1
        && (predicted_b as i64 - est_b as i64).unsigned_abs() <= 1
    {
        Ok(EstimatorKind::Linear { slope, intercept })
    } else {
        tracing::warn!(
            type_id = %type_id,
            "memory estimator probe inconclusive; falling back to per-item Python call"
        );
        Ok(EstimatorKind::PyCallable(method.clone().unbind()))
    }
}

impl<I: Identifier> ResourceEstimator<I> for PyMemoryEstimatorBridge {
    fn estimate(&self, task: &TaskInfo<I>) -> ResourceMap {
        match self.by_type.get(&task.type_id) {
            Some(EstimatorKind::Linear { slope, intercept }) => {
                let bytes = (slope * task.size as f64 + intercept).max(0.0) as u64;
                ResourceMap::from([(ResourceKind::memory(), bytes)])
            }
            Some(EstimatorKind::Constant(v)) => {
                ResourceMap::from([(ResourceKind::memory(), *v)])
            }
            Some(EstimatorKind::PyCallable(py_fn)) => {
                Python::attach(|py| -> PyResult<u64> {
                    let bound = py_fn.bind(py);
                    let py_task = Py::new(py, task_to_pytask(task))?;
                    let bytes: u64 = bound.call1((py_task,))?.extract()?;
                    Ok(bytes)
                })
                .map(|b| ResourceMap::from([(ResourceKind::memory(), b)]))
                .unwrap_or_else(|e| {
                    tracing::error!(
                        error = %e,
                        type_id = %task.type_id,
                        "PyCallable estimator raised; falling back to {} bytes",
                        FALLBACK_MEMORY,
                    );
                    ResourceMap::from([(ResourceKind::memory(), FALLBACK_MEMORY)])
                })
            }
            None => {
                tracing::error!(
                    type_id = %task.type_id,
                    "no estimator registered; falling back to {} bytes",
                    FALLBACK_MEMORY,
                );
                ResourceMap::from([(ResourceKind::memory(), FALLBACK_MEMORY)])
            }
        }
    }
}
