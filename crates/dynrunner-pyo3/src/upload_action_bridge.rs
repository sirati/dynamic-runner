//! PyO3 bridge: adapt a Python `upload(source, dest)` callable into an
//! [`UploadAction`] the setup-task executor invokes (#336 P1).
//!
//! Single concern of this file: convert one Rust
//! [`UploadFileRef`] into one Python call, under the GIL, and translate the
//! Python outcome into a Rust `Result<(), UploadError>`. Nothing about WHICH
//! manager owns the action, HOW the manager threads the kwarg through, or
//! WHEN `set_upload_action` runs lives here — those concerns belong to the
//! manager pyclass files and are uniformly thin (single line each), exactly
//! like [`crate::fulfillability_matcher_bridge`].
//!
//! Callable signature (Python side — typically
//! `SlurmJobManager.upload_task_file`):
//!
//! ```python
//! def upload(source: str, dest: str | None, root: UploadRoot) -> None: ...
//! ```
//!
//! `root` (#644) is the framework mount-root selector — always passed as an
//! explicit 3rd positional arg, never encoded into `dest`. A pre-#644 consumer
//! override that only accepts `(source, dest)` stays compatible by tolerating
//! the extra positional (`*_` / a defaulted param), as the upload-action
//! typealias documents.
//!
//! Retry / classification (owner decision 2026-06-14, option A): the PYTHON
//! callable owns the bounded per-blob TRANSIENT retry (the shipped
//! `retry_transient` helper the bulk walk uses). This bridge does NOT retry;
//! it CLASSIFIES the callable's FINAL outcome:
//!   - returns cleanly ⇒ `Ok(())`.
//!   - raises `OSError` ⇒ [`UploadError::Transient`]. `OSError` is exactly the
//!     PyO3 gateway's transport-fault class (every `GatewayError` maps to
//!     `OSError` except `NotConnected` → `RuntimeError`; see
//!     `gateway/ssh.rs::map_gateway_err` and `gateway/retry.py`), so an
//!     `OSError` surviving the Python-side `retry_transient` is a
//!     whole-action transient the executor's bounded OUTER retry may
//!     re-attempt.
//!   - raises anything else ⇒ [`UploadError::Permanent`] (a programming
//!     error / `NotConnected` / a missing source — no retry).

use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

use dynrunner_core::UploadFileRef;
use dynrunner_manager_distributed::{UploadAction, UploadError};

/// Adapter that holds an unbound Python upload callable and dispatches each
/// [`UploadAction::upload`] to it. `Send + Sync` is satisfied by
/// `Py<PyAny>`'s contract — the trait requires both so an
/// `Arc<dyn UploadAction>` survives the relocation handoff onto the observer
/// tail.
pub(crate) struct PyUploadAction {
    /// The Python callable. Held unbound so the adapter outlives any single
    /// `Python<'py>` lifetime; each `upload` re-binds under a fresh GIL
    /// acquisition.
    callable: Py<PyAny>,
}

impl PyUploadAction {
    /// Build a bridge from a Python callable.
    ///
    /// Returned as `Arc<dyn UploadAction>` (not `Self`) so the
    /// manager-distributed registration API (`set_upload_action`) consumes a
    /// uniform trait-object shape and the caller doesn't spell out the
    /// concrete type — the same contract as
    /// [`crate::fulfillability_matcher_bridge::PyFulfillabilityMatcher::new`].
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(callable: Py<PyAny>) -> std::sync::Arc<dyn UploadAction> {
        std::sync::Arc::new(Self { callable })
    }
}

#[async_trait::async_trait(?Send)]
impl UploadAction for PyUploadAction {
    async fn upload(&self, file: &UploadFileRef) -> Result<(), UploadError> {
        // GIL acquisition crosses the runtime boundary. The Python callable
        // (`SlurmJobManager.upload_task_file`) runs the ssh/scp transfer
        // SYNCHRONOUSLY — which is exactly the #489 setup-executor contract
        // (a setup task runs to completion inside the coordinator loop
        // iteration that received its assignment), so blocking here is by
        // design, not a hazard.
        let source = file.source.to_string_lossy().into_owned();
        let dest = file
            .dest
            .as_ref()
            .map(|d| d.to_string_lossy().into_owned());
        // #644: the framework mount-root selector crosses as an EXPLICIT 3rd
        // positional arg (a Python `UploadRoot` member) — never encoded into
        // `dest`. The Python callable maps it to the cluster mount base. A
        // 2-arg consumer override (pre-#644) stays compatible by tolerating
        // the extra positional (e.g. `*_`), per the upload-action typealias.
        let root = crate::pytypes::PyUploadRoot::from(file.root);
        let outcome: PyResult<()> = Python::attach(|py| {
            self.callable
                .bind(py)
                .call1((source.as_str(), dest.as_deref(), root))?;
            Ok(())
        });
        match outcome {
            Ok(()) => Ok(()),
            Err(e) => {
                // Classify the FINAL exception (the Python callable already
                // exhausted its own `retry_transient`): OSError = transient
                // transport fault, anything else = permanent.
                let transient = Python::attach(|py| e.is_instance_of::<PyOSError>(py));
                let reason = format!(
                    "python upload callable raised for source '{}': {}",
                    file.source.display(),
                    e
                );
                if transient {
                    tracing::warn!(
                        target: "dynrunner_pyo3_upload_action",
                        source = %file.source.display(),
                        error = %e,
                        "upload callable raised a transient (OSError) fault"
                    );
                    Err(UploadError::Transient(reason))
                } else {
                    tracing::warn!(
                        target: "dynrunner_pyo3_upload_action",
                        source = %file.source.display(),
                        error = %e,
                        "upload callable raised a permanent (non-OSError) fault"
                    );
                    Err(UploadError::Permanent(reason))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure-Rust-over-GIL contract tests: a stub Python callable records its
    //! (source, dest) args and is scripted to succeed / raise OSError /
    //! raise ValueError, pinning the round-trip + the transient-vs-permanent
    //! classification. No real transfer.
    use super::*;
    use std::ffi::CString;

    /// Compile a stub module exporting `record` (an arg-recording success
    /// callable) and `raise_os` / `raise_value` raising callables, plus a
    /// `calls` list the test reads back. Each callable accepts the #644 3-arg
    /// shape `(source, dest, root)` and records all three.
    fn stub_module<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyModule> {
        let src = "calls = []\n\
                   def record(source, dest, root):\n    \
                       calls.append((source, dest, root))\n\
                   def raise_os(source, dest, root):\n    \
                       calls.append((source, dest, root))\n    \
                       raise OSError('scp stream reset')\n\
                   def raise_value(source, dest, root):\n    \
                       calls.append((source, dest, root))\n    \
                       raise ValueError('source missing')\n";
        PyModule::from_code(
            py,
            CString::new(src).unwrap().as_c_str(),
            CString::new(format!("{name}.py")).unwrap().as_c_str(),
            CString::new(name).unwrap().as_c_str(),
        )
        .expect("compile stub module")
    }

    fn file_ref(source: &str, dest: Option<&str>) -> UploadFileRef {
        UploadFileRef {
            source: std::path::PathBuf::from(source),
            dest: dest.map(std::path::PathBuf::from),
            root: dynrunner_core::UploadRoot::Source,
        }
    }

    fn file_ref_rooted(
        source: &str,
        dest: Option<&str>,
        root: dynrunner_core::UploadRoot,
    ) -> UploadFileRef {
        UploadFileRef {
            source: std::path::PathBuf::from(source),
            dest: dest.map(std::path::PathBuf::from),
            root,
        }
    }

    /// Drive the `?Send` async `upload` to completion on a single-thread
    /// runtime within a `LocalSet` (the trait future is not `Send`).
    fn block_on_upload(
        action: &dyn UploadAction,
        file: &UploadFileRef,
    ) -> Result<(), UploadError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, action.upload(file))
    }

    #[test]
    fn clean_callable_succeeds_and_round_trips_the_ref() {
        let (action, calls) = Python::attach(|py| {
            let m = stub_module(py, "stub_upload_ok");
            let action = PyUploadAction::new(m.getattr("record").unwrap().unbind());
            (action, m.getattr("calls").unwrap().unbind())
        });
        let res = block_on_upload(action.as_ref(), &file_ref("/src/x.a", Some("rel/x.a")));
        assert!(res.is_ok(), "a clean callable returns Ok; got {res:?}");
        Python::attach(|py| {
            let calls = calls.bind(py).cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(calls.len(), 1);
            let (s, d, r): (String, Option<String>, crate::pytypes::PyUploadRoot) =
                calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(s, "/src/x.a");
            assert_eq!(d, Some("rel/x.a".to_string()), "the dest round-trips");
            assert_eq!(
                r,
                crate::pytypes::PyUploadRoot::Source,
                "the default Source root crosses as the 3rd arg"
            );
        });
    }

    #[test]
    fn output_root_passes_as_third_arg() {
        // #644: an `UploadFileRef { root: Output }` passes the OUTPUT selector
        // to the stub callable as an EXPLICIT 3rd positional arg — never folded
        // into `dest`. The default `Source` case is covered by the round-trip
        // test above.
        let (action, calls) = Python::attach(|py| {
            let m = stub_module(py, "stub_upload_output_root");
            let action = PyUploadAction::new(m.getattr("record").unwrap().unbind());
            (action, m.getattr("calls").unwrap().unbind())
        });
        let res = block_on_upload(
            action.as_ref(),
            &file_ref_rooted("/src/o.a", Some("rel/o.a"), dynrunner_core::UploadRoot::Output),
        );
        assert!(res.is_ok(), "a clean callable returns Ok; got {res:?}");
        Python::attach(|py| {
            let calls = calls.bind(py).cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(calls.len(), 1);
            let (s, d, r): (String, Option<String>, crate::pytypes::PyUploadRoot) =
                calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(s, "/src/o.a");
            assert_eq!(
                d,
                Some("rel/o.a".to_string()),
                "dest is unchanged — root is NOT folded into it"
            );
            assert_eq!(
                r,
                crate::pytypes::PyUploadRoot::Output,
                "the OUTPUT selector crosses verbatim as the 3rd arg"
            );
        });
    }

    #[test]
    fn none_dest_round_trips_as_python_none() {
        let action = Python::attach(|py| {
            let m = stub_module(py, "stub_upload_none_dest");
            PyUploadAction::new(m.getattr("record").unwrap().unbind())
        });
        let res = block_on_upload(action.as_ref(), &file_ref("/src/y.a", None));
        assert!(res.is_ok());
    }

    #[test]
    fn os_error_classifies_transient() {
        let action = Python::attach(|py| {
            let m = stub_module(py, "stub_upload_os");
            PyUploadAction::new(m.getattr("raise_os").unwrap().unbind())
        });
        let res = block_on_upload(action.as_ref(), &file_ref("/src/z.a", None));
        match res {
            Err(UploadError::Transient(_)) => {}
            other => panic!("OSError must classify Transient; got {other:?}"),
        }
    }

    #[test]
    fn other_exception_classifies_permanent() {
        let action = Python::attach(|py| {
            let m = stub_module(py, "stub_upload_value");
            PyUploadAction::new(m.getattr("raise_value").unwrap().unbind())
        });
        let res = block_on_upload(action.as_ref(), &file_ref("/src/w.a", None));
        match res {
            Err(UploadError::Permanent(_)) => {}
            other => panic!("a non-OSError must classify Permanent; got {other:?}"),
        }
    }
}
