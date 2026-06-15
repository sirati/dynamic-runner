//! PyO3 bridge: adapt a Python `affine_instance_satisfied(task_id,
//! payload_json) -> bool` callable into an [`AffineSatisfiedProbe`] the
//! secondary's run-once affine executor consults BEFORE invoking
//! [`crate::affine_action_bridge::PyImportAction`] (#537), mirroring that
//! bridge 1:1.
//!
//! Single concern of this file: convert one Rust [`TaskInfo`] into one
//! Python call, under the GIL, and translate the Python outcome into a Rust
//! `bool`. Nothing about WHICH manager owns the probe, HOW the manager
//! threads the kwarg through, or WHEN `set_affine_satisfied_probe` runs
//! lives here — those concerns belong to the secondary manager pyclass files
//! and are uniformly thin (single line each), exactly like
//! [`crate::affine_action_bridge`].
//!
//! Callable signature (Python side — the consumer's
//! `affine_instance_satisfied`):
//!
//! ```python
//! def affine_instance_satisfied(task_id: str, payload_json: str) -> bool: ...
//! ```
//!
//! The same `(task_id, payload_json)` shape `import_task` already accepts —
//! a consumer that already dispatches on `task_id` for the import callable
//! can dispatch on it again for the probe without re-deriving identity.
//! `payload_json` is the JSON-serialized gate payload (the same projection
//! the import bridge applies) so a consumer that needs the gate's payload
//! to answer the probe has it.
//!
//! Classification (mirrors the import bridge's option-A contract): the
//! PYTHON callable is consulted SYNCHRONOUSLY (the probe IS the
//! short-circuit's value proposition — async would defeat it). This bridge
//! does NOT retry; it CLASSIFIES the callable's outcome:
//!   - returns truthy ⇒ `true` (the gate is locally present; the executor
//!     seeds `affine_done` and the dependent dispatches on the unchanged
//!     `AlreadyDone` path — zero scaffolding).
//!   - returns falsy ⇒ `false` (the unchanged import path runs).
//!   - raises anything ⇒ logged + `false`. NEVER PROPAGATES — a probe is an
//!     OPTIMIZATION, and a buggy probe must degrade to today's behaviour,
//!     not break the run. The Rust-side executor caches the `false`
//!     verdict briefly so a flaky probe is not hammered.

use pyo3::prelude::*;

use dynrunner_core::{RunnerIdentifier, TaskInfo};
use dynrunner_manager_distributed::AffineSatisfiedProbe;

/// Adapter that holds an unbound Python probe callable and dispatches each
/// [`AffineSatisfiedProbe::is_satisfied`] to it. `Send + Sync` is satisfied
/// by `Py<PyAny>`'s contract — the trait requires both so an
/// `Arc<dyn AffineSatisfiedProbe<RunnerIdentifier>>` survives the relocation
/// handoff onto the observer tail (same rationale as
/// [`crate::affine_action_bridge::PyImportAction`]).
pub(crate) struct PyAffineSatisfiedProbe {
    /// The Python callable. Held unbound so the adapter outlives any single
    /// `Python<'py>` lifetime; each `is_satisfied` re-binds under a fresh
    /// GIL acquisition.
    callable: Py<PyAny>,
}

impl PyAffineSatisfiedProbe {
    /// Build a bridge from a Python callable.
    ///
    /// Returned as `Arc<dyn AffineSatisfiedProbe<RunnerIdentifier>>` (not
    /// `Self`) so the manager-distributed registration API
    /// (`set_affine_satisfied_probe`) consumes a uniform trait-object shape
    /// and the caller doesn't spell out the concrete type — the same
    /// contract as
    /// [`crate::affine_action_bridge::PyImportAction::new`].
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        callable: Py<PyAny>,
    ) -> std::sync::Arc<dyn AffineSatisfiedProbe<RunnerIdentifier>> {
        std::sync::Arc::new(Self { callable })
    }
}

impl AffineSatisfiedProbe<RunnerIdentifier> for PyAffineSatisfiedProbe {
    fn is_satisfied(&self, task: &TaskInfo<RunnerIdentifier>) -> bool {
        // GIL acquisition crosses the runtime boundary. The Python callable
        // (the consumer's `affine_instance_satisfied`) runs SYNCHRONOUSLY —
        // which is exactly the #537 probe contract (a local FS stat at
        // most), so blocking here is by design, not a hazard. The whole
        // purpose of the short-circuit is to AVOID the heavier import
        // scaffolding (off-loop spawn_local, queued CRDT frames).
        let task_id = task.task_id.clone();
        let payload_json =
            serde_json::to_string(&task.payload).unwrap_or_else(|_| "null".to_string());
        let outcome: PyResult<bool> = Python::attach(|py| {
            let res = self
                .callable
                .bind(py)
                .call1((task_id.as_str(), payload_json.as_str()))?;
            res.is_truthy()
        });
        match outcome {
            Ok(satisfied) => satisfied,
            Err(e) => {
                // A probe is an OPTIMIZATION: a buggy probe must degrade
                // to today's behaviour (run the import), not break the
                // run. Log loudly so the operator can diagnose, but
                // return `false`. The Rust-side executor caches this
                // `false` briefly (the `Errored` cache TTL) so a flaky
                // probe is not hammered. NEVER POISONS `affine_done` —
                // only an actual `true` verdict marks the gate
                // locally-done.
                tracing::warn!(
                    target: "dynrunner_pyo3_affine_satisfied",
                    task_id = %task.task_id,
                    error = %e,
                    "affine satisfied probe raised; classified as not-satisfied \
                     (today's import path runs); the verdict is cached briefly \
                     to avoid hammering a flaky probe"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure-Rust-over-GIL contract tests: a stub Python callable records its
    //! (task_id, payload_json) args and is scripted to return True / return
    //! False / raise, pinning the round-trip + the
    //! exception-treated-as-false classification. No real import.
    use super::*;
    use std::ffi::CString;

    /// Compile a stub module exporting `yes` / `no` / `raise_err` callables
    /// plus a `calls` list the test reads back.
    fn stub_module<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyModule> {
        let src = "calls = []\n\
                   def yes(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n    \
                       return True\n\
                   def no(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n    \
                       return False\n\
                   def raise_err(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n    \
                       raise ValueError('probe is broken')\n";
        PyModule::from_code(
            py,
            CString::new(src).unwrap().as_c_str(),
            CString::new(format!("{name}.py")).unwrap().as_c_str(),
            CString::new(name).unwrap().as_c_str(),
        )
        .expect("compile stub module")
    }

    /// A minimal SecondaryAffine `TaskInfo` whose `task_id` + `payload` the
    /// bridge projects onto the Python call.
    fn affine_task(task_id: &str, payload: serde_json::Value) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: std::path::PathBuf::from("/tmp/affine"),
            size: 0,
            identifier: RunnerIdentifier::from("import-bin"),
            phase_id: dynrunner_core::PhaseId::from("default"),
            type_id: dynrunner_core::TypeId::from("default"),
            affinity_id: None,
            payload,
            task_id: task_id.to_string(),
            task_depends_on: Vec::new(),
            preferred_secondaries: Default::default(),
            preferred_version: Default::default(),
            kind: dynrunner_core::TaskKind::SecondaryAffine,
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    #[test]
    fn py_affine_satisfied_probe_round_trips_and_classifies() {
        // True: a truthy callable returns true and receives (task_id,
        // payload_json) verbatim.
        let (probe, calls) = Python::attach(|py| {
            let m = stub_module(py, "stub_satisfied_yes");
            let probe = PyAffineSatisfiedProbe::new(m.getattr("yes").unwrap().unbind());
            (probe, m.getattr("calls").unwrap().unbind())
        });
        let task = affine_task("import-A", serde_json::json!({"archive": "/nix/store/x.nar"}));
        assert!(
            probe.is_satisfied(&task),
            "a truthy callable returns true",
        );
        Python::attach(|py| {
            let calls = calls.bind(py).cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(calls.len(), 1);
            let (tid, payload): (String, String) =
                calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(tid, "import-A");
            assert_eq!(
                payload, "{\"archive\":\"/nix/store/x.nar\"}",
                "the payload round-trips as JSON text",
            );
        });

        // False: a falsy callable returns false (the executor runs the
        // unchanged import path).
        let no_probe = Python::attach(|py| {
            let m = stub_module(py, "stub_satisfied_no");
            PyAffineSatisfiedProbe::new(m.getattr("no").unwrap().unbind())
        });
        assert!(
            !no_probe.is_satisfied(&affine_task("import-B", serde_json::Value::Null)),
            "a falsy callable returns false",
        );

        // Exception ⇒ treat as false (degrades to today's behaviour). The
        // bridge must NEVER propagate — a buggy probe is an optimization
        // failure, not a run failure.
        let raise_probe = Python::attach(|py| {
            let m = stub_module(py, "stub_satisfied_raise");
            PyAffineSatisfiedProbe::new(m.getattr("raise_err").unwrap().unbind())
        });
        assert!(
            !raise_probe.is_satisfied(&affine_task("import-C", serde_json::Value::Null)),
            "a raising callable is classified false; the import path runs",
        );
    }
}
