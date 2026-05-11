//! Python adapter for the [`dynrunner_discovery`] walker.
//!
//! Exposes `FolderProxy` and `FileProxy` pyclasses (the per-entry handles
//! visited Python code mutates via `enter()` / `mark()`) and the
//! [`find_items`] pyfunction that drives a [`Filesystem`] walk through a
//! Python `task_definition.visit(...)` method, returning a populated list
//! of [`PyTaskInfo`].

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{PhaseId, RunnerIdentifier, TaskInfo, TypeId};
use dynrunner_discovery::{FileInfo, FolderInfo, VisitOutcome, Visitor, WalkError, walk};
use dynrunner_gateway::{
    Filesystem, Gateway, GatewayConfig, LocalGateway, SshGateway, parse_gateway_url,
};

use crate::pytypes::{PyTaskInfo, identifier_from_pyobj};

/// Per-subfolder slot handed to a Python visit() call. Holds the entry's
/// `name` and the Python-side mutation state for `enter()`.
#[pyclass(name = "FolderProxy", unsendable)]
pub(crate) struct FolderProxy {
    #[pyo3(get)]
    name: String,
    enter_yes: Mutex<bool>,
    enter_payload: Mutex<Option<Py<PyAny>>>,
}

#[pymethods]
impl FolderProxy {
    /// Mark this subfolder for descent. The driver will call the visitor
    /// recursively on each entered folder; `payload` becomes the
    /// `parent_payload` of that recursive call.
    #[pyo3(signature = (yes, payload=None))]
    fn enter(&self, yes: bool, payload: Option<Py<PyAny>>) {
        *self.enter_yes.lock().unwrap() = yes;
        *self.enter_payload.lock().unwrap() = payload;
    }
}

/// Per-file slot handed to a Python visit() call. Holds `name` + `size`
/// and the Python-side mutation state for `mark()`.
#[pyclass(name = "FileProxy", unsendable)]
pub(crate) struct FileProxy {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    size: u64,
    mark_yes: Mutex<bool>,
    mark_payload: Mutex<Option<Py<PyAny>>>,
}

#[pymethods]
impl FileProxy {
    /// Slate this file for processing. `payload` is what the discovery
    /// pipeline records as the file's task-specific identifier (e.g.
    /// asm-tokenizer's `BinaryIdentifier`).
    #[pyo3(signature = (yes, payload=None))]
    fn mark(&self, yes: bool, payload: Option<Py<PyAny>>) {
        *self.mark_yes.lock().unwrap() = yes;
        *self.mark_payload.lock().unwrap() = payload;
    }
}

/// Implements [`Visitor`] by calling a stored Python `visit` method on
/// each directory listing.
struct PyVisitorBridge {
    visit_method: Py<PyAny>,
}

impl Visitor for PyVisitorBridge {
    type Payload = Py<PyAny>;
    type Error = PyErr;

    async fn visit(
        &mut self,
        parent_payload: Option<&Py<PyAny>>,
        subfolders: &[FolderInfo],
        files: &[FileInfo],
    ) -> Result<VisitOutcome<Py<PyAny>>, PyErr> {
        Python::attach(|py| -> PyResult<_> {
            let folder_proxies: Vec<Py<FolderProxy>> = subfolders
                .iter()
                .map(|f| {
                    Py::new(
                        py,
                        FolderProxy {
                            name: f.name.clone(),
                            enter_yes: Mutex::new(false),
                            enter_payload: Mutex::new(None),
                        },
                    )
                })
                .collect::<PyResult<_>>()?;
            let file_proxies: Vec<Py<FileProxy>> = files
                .iter()
                .map(|f| {
                    Py::new(
                        py,
                        FileProxy {
                            name: f.name.clone(),
                            size: f.size,
                            mark_yes: Mutex::new(false),
                            mark_payload: Mutex::new(None),
                        },
                    )
                })
                .collect::<PyResult<_>>()?;

            let py_folders = PyList::empty(py);
            for f in &folder_proxies {
                py_folders.append(f.clone_ref(py))?;
            }
            let py_files = PyList::empty(py);
            for f in &file_proxies {
                py_files.append(f.clone_ref(py))?;
            }

            let parent_obj: Py<PyAny> = parent_payload
                .map(|p| p.clone_ref(py))
                .unwrap_or_else(|| py.None());

            self.visit_method
                .call1(py, (parent_obj, py_folders, py_files))?;

            let mut outcome: VisitOutcome<Py<PyAny>> = VisitOutcome::default();
            for (i, fp) in folder_proxies.iter().enumerate() {
                let borrowed = fp.borrow(py);
                let yes = *borrowed.enter_yes.lock().unwrap();
                if yes {
                    let payload = borrowed
                        .enter_payload
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap_or_else(|| py.None());
                    outcome.enter.push((i, payload));
                }
            }
            for (i, fp) in file_proxies.iter().enumerate() {
                let borrowed = fp.borrow(py);
                let yes = *borrowed.mark_yes.lock().unwrap();
                if yes {
                    let payload = borrowed
                        .mark_payload
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap_or_else(|| py.None());
                    outcome.mark.push((i, payload));
                }
            }
            Ok(outcome)
        })
    }
}

/// Build a `PyTaskInfo` from a marked file. The `payload` from `mark()`
/// is the task-specific identifier object; it's resolved via the same
/// `identifier_from_pyobj` helper that `extract_binaries` uses, and the
/// resulting `TaskInfo<RunnerIdentifier>` is converted to its Python
/// wrapper. Phase / type / affinity / payload are left at defaults —
/// the caller's `organize_items` pass owns those assignments.
fn pytaskinfo_from_mark(
    py: Python<'_>,
    relative_path: &std::path::Path,
    size: u64,
    payload: &Py<PyAny>,
) -> PyResult<PyTaskInfo> {
    let identifier: RunnerIdentifier = identifier_from_pyobj(payload.bind(py))?;
    let task: TaskInfo<RunnerIdentifier> = TaskInfo {
        path: relative_path.to_path_buf(),
        size,
        identifier,
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        resolved_path: None,
    };
    Ok(PyTaskInfo::from(&task))
}

/// Run the walker against a backend chosen by `gateway_config`. The
/// gateway is connected for the duration of the walk and disconnected
/// afterwards on either success or failure.
async fn walk_with<F: Filesystem + Gateway>(
    mut gw: F,
    root: &str,
    bridge: &mut PyVisitorBridge,
) -> Result<Vec<dynrunner_discovery::Marked<Py<PyAny>>>, WalkError<PyErr>> {
    gw.connect()
        .await
        .map_err(|e| WalkError::Fs(dynrunner_gateway::FsError::Other(format!("{e}"))))?;
    let result = walk(&gw, root, bridge).await;
    let _ = gw.disconnect().await;
    result
}

/// Drive a Rust filesystem walk through the visit() method on
/// `task_definition`, returning a list of populated `PyTaskInfo`s.
///
/// `root` is interpreted relative to the chosen backend. The backend is
/// selected by `gateway_url`:
///   - `None` or `"local"` — `LocalGateway`
///   - `"ssh://[user@]host[:port]"` — `SshGateway`
///
/// `ssh_config_path` and `ssh_identity_file` are SSH-side overrides
/// passed through to `SshConfig` when the URL routes to the SSH
/// backend. Without them, `parse_gateway_url` leaves both at `None`
/// and the gateway falls back to the user's `~/.ssh/config` (or
/// system defaults) — which on slurm-test-env doesn't exist and
/// the master handshake fails. This is the same plumbing pattern as
/// `_dispatch_slurm` for the main pipeline; `find_items` was missed
/// in that refactor (asm-tokenizer Tier-2 report at 394be31 — task
/// #39). Both kwargs are ignored on `local` mode (no SshConfig to
/// write to).
///
/// `relative_path` in each returned TaskInfo is relative to `root`.
#[pyfunction]
#[pyo3(signature = (task_definition, root, gateway_url=None, ssh_config_path=None, ssh_identity_file=None))]
pub(crate) fn find_items<'py>(
    py: Python<'py>,
    task_definition: &Bound<'py, PyAny>,
    root: &str,
    gateway_url: Option<&str>,
    ssh_config_path: Option<&str>,
    ssh_identity_file: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let visit_method = task_definition.getattr("visit")?.unbind();

    let gateway_config = match gateway_url {
        None => GatewayConfig::Local,
        Some(url) => {
            let mut cfg = parse_gateway_url(url)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            if let GatewayConfig::Ssh(ref mut ssh_cfg) = cfg {
                if let Some(p) = ssh_config_path {
                    ssh_cfg.config_file = Some(p.to_owned());
                }
                if let Some(p) = ssh_identity_file {
                    ssh_cfg.identity_file = Some(p.to_owned());
                }
            }
            cfg
        }
    };

    let root_owned = root.to_owned();
    let marked = py.detach(|| -> Result<_, WalkError<PyErr>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                WalkError::Fs(dynrunner_gateway::FsError::Other(format!(
                    "tokio runtime: {e}"
                )))
            })?;
        rt.block_on(async {
            let mut bridge = PyVisitorBridge { visit_method };
            match gateway_config {
                GatewayConfig::Local => {
                    walk_with(LocalGateway::new(), &root_owned, &mut bridge).await
                }
                GatewayConfig::Ssh(cfg) => {
                    walk_with(SshGateway::new(cfg), &root_owned, &mut bridge).await
                }
            }
        })
    })
    .map_err(|e| match e {
        WalkError::Visitor(py_err) => py_err,
        WalkError::Fs(fs_err) => {
            pyo3::exceptions::PyOSError::new_err(format!("filesystem: {fs_err}"))
        }
        WalkError::IndexOutOfBounds {
            kind,
            index,
            len,
            path,
        } => pyo3::exceptions::PyIndexError::new_err(format!(
            "{kind} index {index} (len={len}) at {path}",
        )),
    })?;

    let out = PyList::empty(py);
    for m in marked {
        let info = pytaskinfo_from_mark(py, &m.relative_path, m.size, &m.payload)?;
        out.append(Py::new(py, info)?)?;
    }
    Ok(out)
}
