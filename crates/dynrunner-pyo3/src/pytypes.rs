use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{AffinityId, Identifier, TaskInfo, PhaseId, RunnerIdentifier, TypeId};

/// Canonical identifier-key separator. Matches the Python
/// `TokenizerIdentifier.identifier_key()` join order
/// `"binary_name/platform/compiler/version/opt_level"`. Sibling task
/// packages can compose their own key with the same separator.
const ID_SEP: char = '/';

fn join_identifier(
    binary_name: &str,
    platform: &str,
    compiler: &str,
    version: &str,
    opt_level: &str,
) -> RunnerIdentifier {
    Arc::from(
        format!(
            "{binary_name}{ID_SEP}{platform}{ID_SEP}{compiler}{ID_SEP}{version}{ID_SEP}{opt_level}"
        )
        .as_str(),
    )
}

fn split_identifier(id: &str) -> (String, String, String, String, String) {
    let mut parts = id.splitn(5, ID_SEP);
    (
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
    )
}

/// Python-visible wrapper for BinaryIdentifier.
#[pyclass(name = "BinaryIdentifier", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBinaryIdentifier {
    #[pyo3(get)]
    binary_name: String,
    #[pyo3(get)]
    platform: String,
    #[pyo3(get)]
    compiler: String,
    #[pyo3(get)]
    version: String,
    #[pyo3(get)]
    opt_level: String,
}

#[pymethods]
impl PyBinaryIdentifier {
    #[new]
    fn new(
        binary_name: String,
        platform: String,
        compiler: String,
        version: String,
        opt_level: String,
    ) -> Self {
        Self {
            binary_name,
            platform,
            compiler,
            version,
            opt_level,
        }
    }
}

impl From<&PyBinaryIdentifier> for RunnerIdentifier {
    fn from(py: &PyBinaryIdentifier) -> Self {
        join_identifier(
            &py.binary_name,
            &py.platform,
            &py.compiler,
            &py.version,
            &py.opt_level,
        )
    }
}

/// Python-visible wrapper for TaskInfo.
#[pyclass(name = "TaskInfo", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyTaskInfo {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    identifier: PyBinaryIdentifier,
    #[pyo3(get)]
    phase_id: String,
    #[pyo3(get)]
    type_id: String,
    #[pyo3(get)]
    affinity_id: Option<String>,
    /// Stored as a JSON-serialized string so we can pass it across the FFI
    /// boundary without depending on pythonize. Phase 2A's Python-side
    /// `payload` is a JSON-serializable dict; we json.dumps on extraction.
    #[pyo3(get)]
    payload_json: String,
}

#[pymethods]
impl PyTaskInfo {
    #[new]
    #[pyo3(signature = (
        path,
        size,
        identifier,
        phase_id = String::new(),
        type_id = String::new(),
        affinity_id = None,
        payload_json = "null".to_string(),
    ))]
    fn new(
        path: String,
        size: u64,
        identifier: PyBinaryIdentifier,
        phase_id: String,
        type_id: String,
        affinity_id: Option<String>,
        payload_json: String,
    ) -> Self {
        Self {
            path,
            size,
            identifier,
            phase_id,
            type_id,
            affinity_id,
            payload_json,
        }
    }
}

impl From<&PyTaskInfo> for TaskInfo<RunnerIdentifier> {
    fn from(py: &PyTaskInfo) -> Self {
        let phase_id = if py.phase_id.is_empty() {
            PhaseId::from("default")
        } else {
            PhaseId::from(py.phase_id.as_str())
        };
        let type_id = if py.type_id.is_empty() {
            TypeId::from("default")
        } else {
            TypeId::from(py.type_id.as_str())
        };
        let affinity_id = py.affinity_id.as_deref().map(AffinityId::from);
        let payload: serde_json::Value =
            serde_json::from_str(&py.payload_json).unwrap_or(serde_json::Value::Null);
        TaskInfo {
            path: PathBuf::from(&py.path),
            size: py.size,
            identifier: RunnerIdentifier::from(&py.identifier),
            phase_id,
            type_id,
            affinity_id,
            payload,
        }
    }
}

impl From<&TaskInfo<RunnerIdentifier>> for PyTaskInfo {
    fn from(bi: &TaskInfo<RunnerIdentifier>) -> Self {
        let (binary_name, platform, compiler, version, opt_level) =
            split_identifier(&bi.identifier);
        PyTaskInfo {
            path: bi.path.to_string_lossy().into_owned(),
            size: bi.size,
            identifier: PyBinaryIdentifier {
                binary_name,
                platform,
                compiler,
                version,
                opt_level,
            },
            phase_id: bi.phase_id.as_str().to_owned(),
            type_id: bi.type_id.as_str().to_owned(),
            affinity_id: bi.affinity_id.as_ref().map(|a| a.as_str().to_owned()),
            payload_json: serde_json::to_string(&bi.payload).unwrap_or_else(|_| "null".into()),
        }
    }
}

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

/// Build a `PyTaskInfo` Python object from any `TaskInfo<I>`.
///
/// The identifier is rendered as a stand-in `PyBinaryIdentifier` whose
/// `binary_name` field carries the JSON-serialized `I`; the other
/// identifier fields are empty. The estimator path only ever reads
/// `size`, `type_id`, `phase_id`, `affinity_id`, and `payload`, so this
/// stand-in is sufficient when we don't know the concrete `I` (and we
/// never do at the bridge layer — the bridge is generic over `I`).
pub(crate) fn task_to_pytask<I: Identifier>(task: &TaskInfo<I>) -> PyTaskInfo {
    let identifier_json = serde_json::to_string(&task.identifier).unwrap_or_else(|_| "null".into());
    PyTaskInfo {
        path: task.path.to_string_lossy().into_owned(),
        size: task.size,
        identifier: PyBinaryIdentifier {
            binary_name: identifier_json,
            platform: String::new(),
            compiler: String::new(),
            version: String::new(),
            opt_level: String::new(),
        },
        phase_id: task.phase_id.as_str().to_owned(),
        type_id: task.type_id.as_str().to_owned(),
        affinity_id: task.affinity_id.as_ref().map(|a| a.as_str().to_owned()),
        payload_json: serde_json::to_string(&task.payload).unwrap_or_else(|_| "null".into()),
    }
}

pub(crate) fn extract_binaries(
    binaries: &Bound<'_, PyList>,
) -> PyResult<Vec<TaskInfo<RunnerIdentifier>>> {
    let py = binaries.py();
    // We use Python's `json.dumps` on the (potentially-arbitrary) `payload`
    // dict to bridge it through to a `serde_json::Value`. Round-tripping via
    // a string avoids adding `pythonize` as a dep; called once per item at
    // run start, so the cost is negligible.
    let json_module = py.import("json")?;
    let dumps = json_module.getattr("dumps")?;

    binaries
        .iter()
        .map(|item| {
            let path_obj = item.getattr("path")?;
            let path: String = path_obj.str()?.to_string();
            let size: u64 = item.getattr("size")?.extract()?;
            let ident = item.getattr("identifier")?;

            // Prefer the structured-identifier interface (`identifier_key()`)
            // when the Python identifier is a TokenizerIdentifier dataclass
            // or compatible. Fall back to PyBinaryIdentifier's 5 explicit
            // fields for backward compat.
            let identifier: RunnerIdentifier = if let Ok(key_attr) = ident.getattr("identifier_key")
            {
                let key: String = key_attr.call0()?.extract()?;
                Arc::from(key.as_str())
            } else {
                let binary_name: String = ident.getattr("binary_name")?.extract()?;
                let platform: String = ident.getattr("platform")?.extract()?;
                let compiler: String = ident.getattr("compiler")?.extract()?;
                let version: String = ident.getattr("version")?.extract()?;
                let opt_level: String = ident.getattr("opt_level")?.extract()?;
                join_identifier(&binary_name, &platform, &compiler, &version, &opt_level)
            };

            // Phase 2A added phase_id / type_id / affinity_id / payload to the
            // Python TaskInfo with safe defaults (empty strings / None / {}).
            // Fall back to "default" / "default" / None / Null when the
            // attribute is missing so legacy callers still parse.
            let phase_id_str: String = item
                .getattr("phase_id")
                .and_then(|v| v.extract())
                .unwrap_or_default();
            let phase_id = if phase_id_str.is_empty() {
                PhaseId::from("default")
            } else {
                PhaseId::from(phase_id_str)
            };

            let type_id_str: String = item
                .getattr("type_id")
                .and_then(|v| v.extract())
                .unwrap_or_default();
            let type_id = if type_id_str.is_empty() {
                TypeId::from("default")
            } else {
                TypeId::from(type_id_str)
            };

            let affinity_id: Option<AffinityId> = item
                .getattr("affinity_id")
                .ok()
                .and_then(|v| v.extract::<Option<String>>().ok().flatten())
                .map(AffinityId::from);

            let payload = match item.getattr("payload") {
                Ok(p) if !p.is_none() => {
                    let json_str: String = dumps.call1((&p,))?.extract()?;
                    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            };

            Ok(TaskInfo {
                path: PathBuf::from(path),
                size,
                identifier,
                phase_id,
                type_id,
                affinity_id,
                payload,
            })
        })
        .collect()
}

