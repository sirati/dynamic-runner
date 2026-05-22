//! Bridge helpers for converting Python `TaskInfo`-shaped objects to
//! the Rust-side `TaskInfo<RunnerIdentifier>` and back.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{
    AffinityId, Identifier, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskDep, TaskInfo,
    TypeId,
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
        // Project `Vec<TaskDep>` down to bare task_ids for the Python
        // bridge (kept consistent with `PyTaskInfo::from(&TaskInfo)`). The
        // `inherit_outputs` flag does not cross this layer; it stays a
        // Rust-side dispatch concern.
        task_depends_on: task
            .task_depends_on
            .iter()
            .map(|dep| dep.task_id.clone())
            .collect(),
        preferred_secondaries: task.preferred_secondaries.as_slice().to_vec(),
    }
}

/// Extract one ``task_depends_on`` entry into a Rust-side ``TaskDep``.
///
/// Single concern: bridge the two legal Python shapes — bare ``str``
/// (legacy ``Vec<String>`` contract) and ``TaskDep`` dataclass (new,
/// carrying ``inherit_outputs``) — into one Rust value. Order of
/// attempts:
///
/// 1. Try ``extract::<String>``. Succeeds for plain ``str`` values; the
///    result becomes ``TaskDep { task_id, inherit_outputs: false }``,
///    matching the untagged ``Bare`` arm in
///    ``dynrunner_core::types::task::TaskDepWire``.
/// 2. Fall back to attribute reads (``task_id`` / ``inherit_outputs``).
///    Works for the Python ``TaskDep`` dataclass (and any duck-typed
///    object exposing those two attributes). Missing
///    ``inherit_outputs`` is NOT inferred — it must be a ``bool``;
///    a ``TaskDep`` instance always carries it (default ``False``).
///
/// Failure surfaces as a ``PyErr`` propagated up to ``extract_binaries``,
/// which becomes a ``ValueError`` / ``AttributeError`` at the Python
/// boundary — the same shape the surrounding extractors raise for
/// malformed inputs.
fn extract_task_dep(obj: &Bound<'_, PyAny>) -> PyResult<TaskDep> {
    if let Ok(s) = obj.extract::<String>() {
        return Ok(TaskDep {
            task_id: s,
            inherit_outputs: false,
        });
    }
    let task_id: String = obj.getattr("task_id")?.extract()?;
    let inherit_outputs: bool = obj.getattr("inherit_outputs")?.extract()?;
    Ok(TaskDep {
        task_id,
        inherit_outputs,
    })
}

/// Walk a Python iterable of ``task_depends_on`` entries and produce
/// the Rust-side ``Vec<TaskDep>``. Each entry is bridged by
/// :func:`extract_task_dep`; the first per-entry error propagates and
/// aborts the walk.
fn extract_task_depends_on(value: &Bound<'_, PyAny>) -> PyResult<Vec<TaskDep>> {
    // ``None`` collapses to the empty default — matches the historical
    // ``v.extract::<Vec<String>>().ok().unwrap_or_default()`` behaviour
    // for consumers passing ``task_depends_on=None`` (or omitting the
    // attribute entirely upstream in the caller, which the
    // ``.getattr().ok()`` chain handles before reaching us).
    if value.is_none() {
        return Ok(Vec::new());
    }
    let iter = value.try_iter()?;
    let mut out = Vec::new();
    for item in iter {
        out.push(extract_task_dep(&item?)?);
    }
    Ok(out)
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
            // ``task_depends_on`` entries cross the FFI boundary as
            // either bare strings (legacy ``Vec<String>`` shape) or
            // ``TaskDep`` dataclass instances (new — opts into the
            // transitive-ancestry output read via ``inherit_outputs``).
            // ``extract_task_dep`` is the single duck-typed walker that
            // knows both shapes; ``extract_task_depends_on`` applies it
            // to every entry of the iterable. Missing / wrong-typed
            // ``task_depends_on`` collapses to the empty-default,
            // matching the legacy back-compat path.
            let task_depends_on: Vec<TaskDep> = item
                .getattr("task_depends_on")
                .ok()
                .map(|v| extract_task_depends_on(&v))
                .transpose()?
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

#[cfg(all(test, feature = "test-with-python"))]
mod tests {
    //! Python-interpreter-backed tests for the ``task_depends_on``
    //! mixed-shape bridge. Single concern: ensure ``extract_task_dep``
    //! accepts bare ``str`` AND attribute-bearing ``TaskDep`` instances
    //! without regressing either path. Pure-Rust round-trip tests for
    //! the surrounding ``PyTaskInfo`` boundary live in
    //! ``pytypes::task_info::tests``.
    use super::*;
    use pyo3::types::PyAnyMethods;

    /// Construct a Python dataclass with the ``TaskDep`` shape via
    /// ``types.SimpleNamespace`` — equivalent for ``getattr`` purposes
    /// to the real ``dynamic_runner._shared.TaskDep`` dataclass, but
    /// avoids importing the Python package from a pure-Rust test.
    fn make_task_dep<'py>(
        py: Python<'py>,
        task_id: &str,
        inherit_outputs: bool,
    ) -> Bound<'py, PyAny> {
        let types = py.import("types").expect("types module");
        let simplens = types.getattr("SimpleNamespace").expect("SimpleNamespace");
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("task_id", task_id).unwrap();
        kwargs.set_item("inherit_outputs", inherit_outputs).unwrap();
        simplens.call((), Some(&kwargs)).expect("SimpleNamespace(...)")
    }

    #[test]
    fn extract_task_dep_bare_string_defaults_inherit_outputs_false() {
        Python::attach(|py| {
            let obj = pyo3::types::PyString::new(py, "alpha");
            let dep = extract_task_dep(&obj.into_any()).expect("bare string");
            assert_eq!(dep.task_id, "alpha");
            assert!(!dep.inherit_outputs);
        });
    }

    #[test]
    fn extract_task_dep_dataclass_carries_inherit_outputs() {
        Python::attach(|py| {
            let obj = make_task_dep(py, "beta", true);
            let dep = extract_task_dep(&obj).expect("attribute-bearing object");
            assert_eq!(dep.task_id, "beta");
            assert!(dep.inherit_outputs);

            let obj2 = make_task_dep(py, "gamma", false);
            let dep2 = extract_task_dep(&obj2).expect("attribute-bearing object");
            assert_eq!(dep2.task_id, "gamma");
            assert!(!dep2.inherit_outputs);
        });
    }

    #[test]
    fn extract_task_depends_on_mixed_iterable() {
        // The wire-equivalent of `["A", TaskDep("B", inherit_outputs=True)]`:
        // a Python tuple mixing the two legal entry shapes. The bridge
        // must preserve order and the inherit-outputs flag.
        Python::attach(|py| {
            let bare = pyo3::types::PyString::new(py, "A").into_any();
            let struct_dep = make_task_dep(py, "B", true);
            let tuple = pyo3::types::PyTuple::new(py, [bare, struct_dep])
                .expect("mixed tuple");
            let deps =
                extract_task_depends_on(tuple.as_any()).expect("mixed iterable");
            assert_eq!(deps.len(), 2);
            assert_eq!(deps[0].task_id, "A");
            assert!(!deps[0].inherit_outputs);
            assert_eq!(deps[1].task_id, "B");
            assert!(deps[1].inherit_outputs);
        });
    }

    #[test]
    fn extract_task_depends_on_none_defaults_empty() {
        // Consumers may pass `task_depends_on=None`; the bridge collapses
        // to the empty default rather than raising. Matches the historical
        // behaviour where the wrong-type path collapsed via
        // `extract::<Vec<String>>().ok().unwrap_or_default()`.
        Python::attach(|py| {
            let none = py.None().into_bound(py);
            let deps = extract_task_depends_on(&none).expect("None");
            assert!(deps.is_empty());
        });
    }
}
