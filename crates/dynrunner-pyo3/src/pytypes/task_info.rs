//! `PyTaskInfo` pyclass + conversions to/from `TaskInfo<RunnerIdentifier>`.

use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{AffinityId, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TypeId};

use super::identifier::{split_identifier, PyBinaryIdentifier};

/// Python-visible wrapper for TaskInfo.
#[pyclass(name = "TaskInfo", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyTaskInfo {
    #[pyo3(get)]
    pub(super) path: String,
    #[pyo3(get)]
    pub(super) size: u64,
    #[pyo3(get)]
    pub(super) identifier: PyBinaryIdentifier,
    #[pyo3(get)]
    pub(super) phase_id: String,
    #[pyo3(get)]
    pub(super) type_id: String,
    #[pyo3(get)]
    pub(super) affinity_id: Option<String>,
    /// Stored as a JSON-serialized string so we can pass it across the FFI
    /// boundary without depending on pythonize. Phase 2A's Python-side
    /// `payload` is a JSON-serializable dict; we json.dumps on extraction.
    #[pyo3(get)]
    pub(super) payload_json: String,
    #[pyo3(get)]
    pub(super) task_id: Option<String>,
    #[pyo3(get)]
    pub(super) task_depends_on: Vec<String>,
    /// Python-facing view of [`TaskInfo::preferred_secondaries`].
    /// Exposed as a `list[str]` because PyO3 doesn't surface
    /// `#[serde(transparent)]` newtype wrappers cleanly to Python;
    /// the Rust-side `SoftPreferredSecondaries` newtype is reconstructed
    /// at the `From<&PyTaskInfo> for TaskInfo<RunnerIdentifier>` boundary.
    /// Empty list == no preference (free pool).
    #[pyo3(get)]
    pub(super) preferred_secondaries: Vec<String>,
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
        task_id = None,
        task_depends_on = Vec::new(),
        preferred_secondaries = Vec::new(),
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: String,
        size: u64,
        identifier: PyBinaryIdentifier,
        phase_id: String,
        type_id: String,
        affinity_id: Option<String>,
        payload_json: String,
        task_id: Option<String>,
        task_depends_on: Vec<String>,
        preferred_secondaries: Vec<String>,
    ) -> Self {
        Self {
            path,
            size,
            identifier,
            phase_id,
            type_id,
            affinity_id,
            payload_json,
            task_id,
            task_depends_on,
            preferred_secondaries,
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
            task_id: py.task_id.clone(),
            task_depends_on: py.task_depends_on.clone(),
            preferred_secondaries: SoftPreferredSecondaries::new(py.preferred_secondaries.clone()),
            resolved_path: None,
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
            task_id: bi.task_id.clone(),
            task_depends_on: bi.task_depends_on.clone(),
            preferred_secondaries: bi.preferred_secondaries.as_slice().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure-Rust tests over the PyO3 conversion paths. The Python
    //! interpreter is not required because the relevant conversions
    //! cross the `&PyTaskInfo` → `TaskInfo<RunnerIdentifier>` boundary
    //! without touching `pyo3::Python`. Tests that need the
    //! interpreter belong in the integration-test layer.
    use super::*;

    fn sample_pytask(preferred: Vec<String>) -> PyTaskInfo {
        PyTaskInfo {
            path: "/tmp/x".into(),
            size: 16,
            identifier: PyBinaryIdentifier {
                binary_name: "bin".into(),
                platform: "x86_64".into(),
                compiler: "gcc".into(),
                version: "12".into(),
                opt_level: "O2".into(),
            },
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: None,
            task_depends_on: Vec::new(),
            preferred_secondaries: preferred,
        }
    }

    #[test]
    fn pytaskinfo_to_taskinfo_carries_preferred_secondaries() {
        // Non-empty hint must survive the FFI-boundary conversion
        // verbatim — the Python `list[str]` shape on `PyTaskInfo`
        // becomes a Rust-side `SoftPreferredSecondaries` newtype
        // wrapping the same list. Verifies the newtype boundary is
        // crossed exactly once at the conversion point, not at every
        // consumer.
        let py = sample_pytask(vec!["sec-a".into(), "sec-b".into()]);
        let rust: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py);
        assert_eq!(
            rust.preferred_secondaries.as_slice(),
            &["sec-a".to_string(), "sec-b".to_string()],
        );

        // Reverse direction: the Rust-side newtype is rendered back
        // as a `Vec<String>` for Python. Round-trip preserves the
        // exact ordering.
        let py_back: PyTaskInfo = PyTaskInfo::from(&rust);
        assert_eq!(
            py_back.preferred_secondaries,
            vec!["sec-a".to_string(), "sec-b".to_string()],
        );

        // Empty hint: round-trip remains empty (no spurious values
        // injected by `SoftPreferredSecondaries::default()`).
        let py_empty = sample_pytask(Vec::new());
        let rust_empty: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py_empty);
        assert!(rust_empty.preferred_secondaries.is_empty());
        let py_empty_back: PyTaskInfo = PyTaskInfo::from(&rust_empty);
        assert!(py_empty_back.preferred_secondaries.is_empty());
    }
}
