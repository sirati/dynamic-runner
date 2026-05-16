//! Bridge helpers for converting Python `TaskInfo`-shaped objects to
//! the Rust-side `TaskInfo<RunnerIdentifier>` and back.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{
    AffinityId, Identifier, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TypeId,
};

use super::identifier::{identifier_from_pyobj, PyBinaryIdentifier};
use super::task_info::PyTaskInfo;

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
        task_id: task.task_id.clone(),
        task_depends_on: task.task_depends_on.clone(),
        preferred_secondaries: task.preferred_secondaries.as_slice().to_vec(),
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
            let identifier = identifier_from_pyobj(&ident)?;

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

            // Optional task-level dependency fields. Both default
            // to "absent / empty" so existing consumers without
            // these attributes (or with None) parse cleanly.
            let task_id: Option<String> = item
                .getattr("task_id")
                .ok()
                .and_then(|v| v.extract::<Option<String>>().ok().flatten());
            let task_depends_on: Vec<String> = item
                .getattr("task_depends_on")
                .ok()
                .and_then(|v| v.extract::<Vec<String>>().ok())
                .unwrap_or_default();
            // Optional soft-preferred-secondaries hint. Missing /
            // None / wrong-type all collapse to the empty default;
            // the newtype keeps the soft-vs-strict semantic
            // boundary explicit on the Rust side.
            let preferred_secondaries: Vec<String> = item
                .getattr("preferred_secondaries")
                .ok()
                .and_then(|v| v.extract::<Vec<String>>().ok())
                .unwrap_or_default();

            Ok(TaskInfo {
                path: PathBuf::from(path),
                size,
                identifier,
                phase_id,
                type_id,
                affinity_id,
                payload,
                task_id,
                task_depends_on,
                preferred_secondaries: SoftPreferredSecondaries::new(preferred_secondaries),
                resolved_path: None,
            })
        })
        .collect()
}
