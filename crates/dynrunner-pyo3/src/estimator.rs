use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use dynrunner_core::{Identifier, ResourceKind, ResourceMap, TaskInfo, TypeId};
use dynrunner_scheduler_api::ResourceEstimator;

use crate::pytypes::task_to_pytask;

/// Memory estimator bridge that dispatches per `TypeId`.
///
/// Each entry in `by_type` is the unbound `Py<PyAny>` for one type's
/// `estimator_attr` Python method. `estimate(...)` re-enters Python
/// under the GIL on every call — Python is the source of truth for
/// the estimator's shape, so we never cache predictions.
///
/// Cloning shares the per-type table via `Arc` (cheap; the inner
/// `Py<PyAny>` ref-counts on the Python side via Drop).
pub(crate) struct PyMemoryEstimatorBridge {
    by_type: Arc<HashMap<TypeId, Py<PyAny>>>,
}

impl Clone for PyMemoryEstimatorBridge {
    fn clone(&self) -> Self {
        Self {
            by_type: Arc::clone(&self.by_type),
        }
    }
}

/// Fallback memory amount returned when no estimator is registered for
/// `task.type_id` or the Python call raised.
const FALLBACK_MEMORY: u64 = 1 << 20; // 1 MiB

impl PyMemoryEstimatorBridge {
    /// Build a bridge by resolving each `(type_id, estimator_attr)` pair
    /// to an unbound Python method on `task_definition`.
    pub(crate) fn from_python(
        task_definition: &Bound<'_, PyAny>,
        types: &[(TypeId, String)],
    ) -> PyResult<Self> {
        let mut by_type: HashMap<TypeId, Py<PyAny>> = HashMap::with_capacity(types.len());
        for (type_id, attr) in types {
            let method = task_definition.getattr(attr.as_str())?;
            by_type.insert(type_id.clone(), method.unbind());
        }
        Ok(Self {
            by_type: Arc::new(by_type),
        })
    }
}

impl<I: Identifier> ResourceEstimator<I> for PyMemoryEstimatorBridge {
    fn estimate(&self, task: &TaskInfo<I>) -> ResourceMap {
        match self.by_type.get(&task.type_id) {
            Some(py_fn) => Python::attach(|py| -> PyResult<u64> {
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
                    "Python estimator raised; falling back to {} bytes",
                    FALLBACK_MEMORY,
                );
                ResourceMap::from([(ResourceKind::memory(), FALLBACK_MEMORY)])
            }),
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
