use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

use dynrunner_core::{
    Identifier, ResourceKind, ResourceMap, TaskInfo, TypeId, compute_task_hash,
};
use dynrunner_scheduler_api::ResourceEstimator;

use crate::pytypes::task_to_pytask;

/// Memory estimator bridge that dispatches per `TypeId`.
///
/// Each entry in `by_type` is the unbound `Py<PyAny>` for one type's
/// `estimator_attr` Python method.
///
/// # Why the estimate is memoised (the deadlock-heat fix)
///
/// `estimate(...)` re-enters Python under the GIL (`Python::attach`).
/// The relocated-primary operational loop calls it on EVERY dispatch
/// decision — once per pending task per idle-worker recheck, fired by
/// the per-completion `TasksAdded` recheck (see
/// `primary/lifecycle/dispatch.rs::dispatch_to_idle_workers` →
/// `Scheduler::assign_normal`, which loops `estimator.estimate(binary)`
/// over the pending view). A synchronous `Python::attach` on that hot
/// path is the prime GIL-ping-pong suspect behind the relocated-primary
/// wedge: it contends for the GIL against any Python-side handle call
/// (`PrimaryHandle::*`), and the loop runs under `py.detach` precisely
/// so the GIL is free for those callers — re-grabbing it per dispatch
/// decision defeats that.
///
/// The estimate is a PURE FUNCTION of the task: the trait contract says
/// it dispatches on `task.type_id` and reads `task.payload`, and the
/// scheduler relies on the SAME estimate being returned for the same
/// task across every recheck (the value it commits as the worker's
/// reserved budget). So memoising it by the task's wire-canonical
/// content hash (`compute_task_hash` — the cluster-wide task identity;
/// two tasks that hash equal are the SAME task to the whole cluster,
/// duplicate hashes are rejected at `SpawnTasks`/`PendingPool::extend`)
/// changes nothing about the values — same estimate per task — and
/// moves the `Python::attach` from per-dispatch-decision to
/// per-distinct-task (first sighting). A `FALLBACK_MEMORY` outcome (no
/// estimator registered, or the Python call raised) is NOT cached, so a
/// transient Python failure is re-attempted on the next dispatch rather
/// than pinned for the run.
///
/// Cloning shares both the per-type table and the cache via `Arc`
/// (cheap; the inner `Py<PyAny>` ref-counts on the Python side via
/// Drop). The shared cache means clones minted for the
/// promoted-primary recipe see the same memo table — consistent
/// estimates across a promotion.
pub(crate) struct PyMemoryEstimatorBridge {
    by_type: Arc<HashMap<TypeId, Py<PyAny>>>,
    /// Memo of successful estimates keyed by `compute_task_hash`.
    /// Interior-mutable because `ResourceEstimator::estimate` takes
    /// `&self`; the lock is held only for the map probe/insert, never
    /// across the `Python::attach`, so it can't serialise the GIL.
    cache: Arc<Mutex<HashMap<String, ResourceMap>>>,
}

impl Clone for PyMemoryEstimatorBridge {
    fn clone(&self) -> Self {
        Self {
            by_type: Arc::clone(&self.by_type),
            cache: Arc::clone(&self.cache),
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
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Re-enter Python under the GIL to compute the estimate for `task`.
    /// Single source of the `Python::attach` shape; the memoisation in
    /// `estimate` wraps this so the attach fires at most once per
    /// distinct task.
    ///
    /// Returns `Ok(estimate)` only when a registered estimator produced
    /// a value, and `Err(fallback)` for the two transient outcomes (no
    /// estimator registered, or the Python call raised) — the `Result`
    /// discriminant is what lets the caller cache successes while
    /// re-attempting fallbacks. Both arms still log + carry
    /// `FALLBACK_MEMORY` so the existing behaviour (and value) of a
    /// fallback is byte-for-byte unchanged for the scheduler.
    fn estimate_uncached<I: Identifier>(
        &self,
        task: &TaskInfo<I>,
    ) -> Result<ResourceMap, ResourceMap> {
        let fallback = || ResourceMap::from([(ResourceKind::memory(), FALLBACK_MEMORY)]);
        match self.by_type.get(&task.type_id) {
            Some(py_fn) => Python::attach(|py| -> PyResult<u64> {
                let bound = py_fn.bind(py);
                let py_task = Py::new(py, task_to_pytask(task))?;
                let bytes: u64 = bound.call1((py_task,))?.extract()?;
                Ok(bytes)
            })
            .map(|b| ResourceMap::from([(ResourceKind::memory(), b)]))
            .map_err(|e| {
                tracing::error!(
                    error = %e,
                    type_id = %task.type_id,
                    "Python estimator raised; falling back to {} bytes",
                    FALLBACK_MEMORY,
                );
                fallback()
            }),
            None => {
                tracing::error!(
                    type_id = %task.type_id,
                    "no estimator registered; falling back to {} bytes",
                    FALLBACK_MEMORY,
                );
                Err(fallback())
            }
        }
    }
}

impl<I: Identifier> ResourceEstimator<I> for PyMemoryEstimatorBridge {
    fn estimate(&self, task: &TaskInfo<I>) -> ResourceMap {
        let key = compute_task_hash(task);
        // Hit: return the memoised estimate without touching Python.
        if let Some(cached) = self
            .cache
            .lock()
            .expect("PyMemoryEstimatorBridge cache poisoned")
            .get(&key)
        {
            return cached.clone();
        }
        // Miss: compute via Python (the lock is NOT held across the
        // attach, so a concurrent caller never serialises on the GIL).
        match self.estimate_uncached(task) {
            // Success → memoise so the next dispatch decision for this
            // task is a pure cache hit (no `Python::attach`).
            Ok(estimate) => {
                self.cache
                    .lock()
                    .expect("PyMemoryEstimatorBridge cache poisoned")
                    .insert(key, estimate.clone());
                estimate
            }
            // Fallback (no estimator / Python raised) → transient; do
            // NOT memoise, so a recovered estimator is re-attempted on
            // the next dispatch rather than pinned for the run.
            Err(fallback) => fallback,
        }
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Estimator memoisation contract. The cache must (a) return the
    //! SAME estimate the direct Python call produces, and (b) re-enter
    //! Python at most once per distinct task — the property that takes
    //! the `Python::attach` off the per-dispatch hot path.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python estimator`
    use super::*;
    use dynrunner_core::{AffinityId, PhaseId, SoftPreferredSecondaries, TaskDep};
    use pyo3::types::PyModule;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Per-call atomic counter so each module gets a unique name (the
    /// parallel `cargo test` harness resolves duplicate names through
    /// `sys.modules`).
    static MODULE_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Compile a task-definition stub whose `estimate_memory` method
    /// records a module-level CALL COUNT and returns a fixed byte size.
    /// Returns the bound task-definition object plus a handle on the
    /// module globals so the test can read the count back.
    fn counting_task_definition(py: Python<'_>, bytes: u64) -> (Bound<'_, PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let module_name = format!("mock_estimator_{nonce}");
        let file_name = format!("{module_name}.py");
        let source = format!(
            "estimate_calls = 0\n\
             class Task:\n    \
                 def estimate_memory(self, task):\n        \
                     global estimate_calls\n        \
                     estimate_calls += 1\n        \
                     return {bytes}\n"
        );
        let module = PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new(file_name).unwrap().as_c_str(),
            std::ffi::CString::new(module_name).unwrap().as_c_str(),
        )
        .expect("compile mock estimator module");
        let task = module.getattr("Task").unwrap().call0().unwrap();
        let globals = module.dict().unbind().into_any();
        (task, globals)
    }

    fn read_call_count(py: Python<'_>, globals: &Py<PyAny>) -> u64 {
        globals
            .bind(py)
            .cast::<pyo3::types::PyDict>()
            .unwrap()
            .get_item("estimate_calls")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap()
    }

    fn mk_task(content: &str) -> TaskInfo<Arc<str>> {
        TaskInfo {
            path: PathBuf::from(content),
            size: 1,
            identifier: Arc::<str>::from(content),
            phase_id: PhaseId::from("phase-A"),
            type_id: TypeId::from("t"),
            affinity_id: None::<AffinityId>,
            payload: serde_json::Value::Null,
            task_id: "id".into(),
            task_depends_on: Vec::<TaskDep>::new(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            kind: Default::default(),
            resolved_path: None,
        }
    }

    /// The cached estimate equals the direct (uncached) estimate for
    /// the same task, AND the Python estimator is invoked exactly ONCE
    /// across repeated `estimate` calls for that task — the
    /// per-dispatch `Python::attach` is gone after the first sighting.
    #[test]
    fn cached_estimate_equals_direct_and_attaches_once_per_task() {
        Python::attach(|py| {
            const BYTES: u64 = 7 << 20; // 7 MiB, distinct from FALLBACK
            let (task_def, globals) = counting_task_definition(py, BYTES);
            let bridge = PyMemoryEstimatorBridge::from_python(
                &task_def,
                &[(TypeId::from("t"), "estimate_memory".to_string())],
            )
            .expect("bridge init");

            let task = mk_task("/bin/x");
            let expected = ResourceMap::from([(ResourceKind::memory(), BYTES)]);

            // First call: a miss → one Python attach → the real value.
            let first = ResourceEstimator::estimate(&bridge, &task);
            assert_eq!(first, expected, "first estimate must equal the Python value");
            assert_eq!(
                read_call_count(py, &globals),
                1,
                "first estimate re-enters Python exactly once"
            );

            // Subsequent calls for the SAME task: cache hits → no further
            // Python invocation, SAME value.
            for _ in 0..5 {
                let again = ResourceEstimator::estimate(&bridge, &task);
                assert_eq!(again, first, "cached estimate must equal the direct estimate");
            }
            assert_eq!(
                read_call_count(py, &globals),
                1,
                "repeated estimates for the same task must NOT re-enter Python \
                 (the per-dispatch attach is off the hot path)"
            );
        });
    }

    /// Two DISTINCT tasks each pay one attach (the cache keys per task,
    /// not globally), and a clone of the bridge shares the same memo
    /// table (so a promoted-primary clone sees consistent estimates).
    #[test]
    fn distinct_tasks_each_attach_once_and_clone_shares_cache() {
        Python::attach(|py| {
            const BYTES: u64 = 3 << 20;
            let (task_def, globals) = counting_task_definition(py, BYTES);
            let bridge = PyMemoryEstimatorBridge::from_python(
                &task_def,
                &[(TypeId::from("t"), "estimate_memory".to_string())],
            )
            .expect("bridge init");

            let task_a = mk_task("/bin/a");
            let task_b = mk_task("/bin/b");

            let _ = ResourceEstimator::estimate(&bridge, &task_a);
            let _ = ResourceEstimator::estimate(&bridge, &task_b);
            assert_eq!(
                read_call_count(py, &globals),
                2,
                "two distinct tasks → two attaches (one each)"
            );

            // A clone shares the Arc'd cache: re-estimating either task
            // through the clone is a hit, no new attach.
            let clone = bridge.clone();
            let _ = ResourceEstimator::estimate(&clone, &task_a);
            let _ = ResourceEstimator::estimate(&clone, &task_b);
            assert_eq!(
                read_call_count(py, &globals),
                2,
                "a bridge clone must share the memo table — no re-attach"
            );
        });
    }
}
