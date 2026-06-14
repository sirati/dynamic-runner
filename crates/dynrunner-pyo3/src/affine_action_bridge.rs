//! PyO3 bridge: adapt a Python `import_task(task_id, payload_json)` callable
//! into an [`ImportAction`] the secondary's run-once affine executor invokes
//! (#497 P6), mirroring [`crate::upload_action_bridge`] (#336 P1) exactly.
//!
//! Single concern of this file: convert one Rust [`TaskInfo`] into one Python
//! call, under the GIL, and translate the Python outcome into a Rust
//! `Result<(), ImportError>`. Nothing about WHICH manager owns the action, HOW
//! the manager threads the kwarg through, or WHEN `set_import_action` runs
//! lives here — those concerns belong to the secondary manager pyclass files
//! and are uniformly thin (single line each), exactly like
//! [`crate::upload_action_bridge`].
//!
//! Callable signature (Python side — the consumer's `job_manager`
//! `import_task`):
//!
//! ```python
//! def import_task(task_id: str, payload_json: str) -> None: ...
//! ```
//!
//! Retry / classification (mirrors the upload bridge's option-A contract): the
//! PYTHON callable owns any bounded per-step TRANSIENT retry it can absorb.
//! This bridge does NOT retry; it CLASSIFIES the callable's FINAL outcome onto
//! the cluster's three #495 failure classes (see
//! [`dynrunner_manager_distributed::affine_action`] for WHY the import uses the
//! #495 classes rather than the upload's transient/permanent pair):
//!   - returns cleanly ⇒ `Ok(())` (the import is locally done).
//!   - raises `OSError` ⇒ [`ImportError::Transient`]. `OSError` is exactly the
//!     PyO3 gateway's transport-fault class (every `GatewayError` maps to
//!     `OSError` except `NotConnected` → `RuntimeError`; see
//!     `gateway/ssh.rs::map_gateway_err` and `gateway/retry.py`), so an
//!     `OSError` surviving the Python-side retry is a whole-action transient
//!     the executor's bounded OUTER retry may re-attempt before folding to a
//!     re-routable `Recoverable` work-task failure (#495).
//!   - raises anything else ⇒ [`ImportError::NonRecoverable`] (a programming
//!     error / `NotConnected` / a structurally un-importable source — its
//!     dependents cascade non-recoverably, no retry).

use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

use dynrunner_core::{RunnerIdentifier, TaskInfo};
use dynrunner_manager_distributed::{ImportAction, ImportError};

/// Adapter that holds an unbound Python import callable and dispatches each
/// [`ImportAction::import`] to it. `Send + Sync` is satisfied by
/// `Py<PyAny>`'s contract — the trait requires both so an
/// `Arc<dyn ImportAction<RunnerIdentifier>>` survives the relocation handoff
/// onto the observer tail.
pub(crate) struct PyImportAction {
    /// The Python callable. Held unbound so the adapter outlives any single
    /// `Python<'py>` lifetime; each `import` re-binds under a fresh GIL
    /// acquisition.
    callable: Py<PyAny>,
}

impl PyImportAction {
    /// Build a bridge from a Python callable.
    ///
    /// Returned as `Arc<dyn ImportAction<RunnerIdentifier>>` (not `Self`) so
    /// the manager-distributed registration API (`set_import_action`) consumes
    /// a uniform trait-object shape and the caller doesn't spell out the
    /// concrete type — the same contract as
    /// [`crate::upload_action_bridge::PyUploadAction::new`]. The secondary is
    /// monomorphized at `RunnerIdentifier`, so the trait object's `I` is fixed
    /// here (the import trait carries the generic, not the method, to stay
    /// object-safe — see the port docs).
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(callable: Py<PyAny>) -> std::sync::Arc<dyn ImportAction<RunnerIdentifier>> {
        std::sync::Arc::new(Self { callable })
    }
}

#[async_trait::async_trait(?Send)]
impl ImportAction<RunnerIdentifier> for PyImportAction {
    async fn import(&self, task: &TaskInfo<RunnerIdentifier>) -> Result<(), ImportError> {
        // GIL acquisition crosses the runtime boundary. The Python callable
        // (the consumer's `import_task`) runs the import (e.g.
        // `nix-store --import`, a toolchain build, a cache prime)
        // SYNCHRONOUSLY — which is exactly the #497 affine-executor contract
        // (the import runs to completion inside the secondary's run-once
        // executor before the gated work tasks are released), so blocking here
        // is by design, not a hazard.
        let task_id = task.task_id.clone();
        let payload_json =
            serde_json::to_string(&task.payload).unwrap_or_else(|_| "null".to_string());
        let outcome: PyResult<()> = Python::attach(|py| {
            self.callable
                .bind(py)
                .call1((task_id.as_str(), payload_json.as_str()))?;
            Ok(())
        });
        match outcome {
            Ok(()) => Ok(()),
            Err(e) => {
                // Classify the FINAL exception (the Python callable already
                // exhausted any per-step retry): OSError = transient transport
                // fault (the executor's bounded outer retry may re-attempt,
                // then folds to a re-routable `Recoverable`), anything else =
                // non-recoverable (its dependents cascade).
                let transient = Python::attach(|py| e.is_instance_of::<PyOSError>(py));
                let reason = format!(
                    "python import callable raised for task '{}': {}",
                    task.task_id, e
                );
                if transient {
                    tracing::warn!(
                        target: "dynrunner_pyo3_import_action",
                        task_id = %task.task_id,
                        error = %e,
                        "import callable raised a transient (OSError) fault"
                    );
                    Err(ImportError::Transient(reason))
                } else {
                    tracing::warn!(
                        target: "dynrunner_pyo3_import_action",
                        task_id = %task.task_id,
                        error = %e,
                        "import callable raised a non-recoverable (non-OSError) fault"
                    );
                    Err(ImportError::NonRecoverable(reason))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure-Rust-over-GIL contract tests: a stub Python callable records its
    //! (task_id, payload_json) args and is scripted to succeed / raise OSError
    //! / raise ValueError, pinning the round-trip + the
    //! transient-vs-non-recoverable classification. No real import.
    use super::*;
    use std::ffi::CString;

    /// Compile a stub module exporting `record` (an arg-recording success
    /// callable) and `raise_os` / `raise_value` raising callables, plus a
    /// `calls` list the test reads back.
    fn stub_module<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyModule> {
        let src = "calls = []\n\
                   def record(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n\
                   def raise_os(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n    \
                       raise OSError('nfs read reset')\n\
                   def raise_value(task_id, payload_json):\n    \
                       calls.append((task_id, payload_json))\n    \
                       raise ValueError('archive corrupt')\n";
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

    /// Drive the `?Send` async `import` to completion on a single-thread
    /// runtime within a `LocalSet` (the trait future is not `Send`).
    fn block_on_import(
        action: &dyn ImportAction<RunnerIdentifier>,
        task: &TaskInfo<RunnerIdentifier>,
    ) -> Result<(), ImportError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, action.import(task))
    }

    #[test]
    fn py_import_action_round_trips_and_classifies() {
        // Success: a clean callable returns Ok and receives (task_id,
        // payload_json) verbatim.
        let (action, calls) = Python::attach(|py| {
            let m = stub_module(py, "stub_import_ok");
            let action = PyImportAction::new(m.getattr("record").unwrap().unbind());
            (action, m.getattr("calls").unwrap().unbind())
        });
        let task = affine_task("import-A", serde_json::json!({"archive": "/nix/store/x.nar"}));
        let res = block_on_import(action.as_ref(), &task);
        assert!(res.is_ok(), "a clean callable returns Ok; got {res:?}");
        Python::attach(|py| {
            let calls = calls.bind(py).cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(calls.len(), 1);
            let (tid, payload): (String, String) =
                calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(tid, "import-A");
            assert_eq!(
                payload, "{\"archive\":\"/nix/store/x.nar\"}",
                "the payload round-trips as JSON text"
            );
        });

        // OSError ⇒ Transient (transport fault the executor's outer retry may
        // re-attempt before folding to a re-routable Recoverable).
        let os_action = Python::attach(|py| {
            let m = stub_module(py, "stub_import_os");
            PyImportAction::new(m.getattr("raise_os").unwrap().unbind())
        });
        let os_res =
            block_on_import(os_action.as_ref(), &affine_task("import-B", serde_json::Value::Null));
        match os_res {
            Err(ImportError::Transient(_)) => {}
            other => panic!("OSError must classify Transient; got {other:?}"),
        }

        // A non-OSError ⇒ NonRecoverable (its dependents cascade).
        let value_action = Python::attach(|py| {
            let m = stub_module(py, "stub_import_value");
            PyImportAction::new(m.getattr("raise_value").unwrap().unbind())
        });
        let value_res = block_on_import(
            value_action.as_ref(),
            &affine_task("import-C", serde_json::Value::Null),
        );
        match value_res {
            Err(ImportError::NonRecoverable(_)) => {}
            other => panic!("a non-OSError must classify NonRecoverable; got {other:?}"),
        }
    }
}
