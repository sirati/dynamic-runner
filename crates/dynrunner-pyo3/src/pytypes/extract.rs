//! Bridge helpers for converting Python `TaskInfo`-shaped objects to
//! the Rust-side `TaskInfo<RunnerIdentifier>` and back.

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{
    AffinityId, Identifier, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskDep, TaskInfo,
    TypeId,
};

use super::identifier::{PyBinaryIdentifier, identifier_from_pyobj};
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
        // `TaskInfo.task_id` is non-optional + non-empty by the
        // boundary contract validated at `extract_binaries` / the
        // `PyTaskInfo::__new__` constructor; this is a verbatim move.
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
        // The already-done marker is a discovery-INPUT signal only; it
        // does not live on `TaskInfo<I>`, so the Rust→Python projection
        // reconstitutes the default.
        skipped_already_done: false,
    }
}

/// Extract one ``task_depends_on`` entry into a Rust-side ``TaskDep``.
///
/// Single concern: bridge the two legal Python shapes — bare ``str``
/// (legacy ``Vec<String>`` contract) and ``TaskDep`` dataclass
/// (carrying ``inherit_outputs`` and an optional cross-phase
/// ``phase_id``) — into one Rust value carrying the dep's full
/// ``(phase_id, task_id)`` identity. A phase-less entry resolves to the
/// ENCLOSING task's phase. Order of attempts:
///
/// 1. Try ``extract::<String>``. Succeeds for plain ``str`` values; the
///    result becomes
///    ``TaskDep { task_id, phase_id: <enclosing>, inherit_outputs: false }``.
/// 2. Fall back to attribute reads (``task_id`` / ``inherit_outputs`` /
///    optional ``phase_id``). Works for the Python ``TaskDep``
///    dataclass (and any duck-typed object exposing those attributes).
///    Missing ``inherit_outputs`` is NOT inferred — it must be a
///    ``bool``;
///    a ``TaskDep`` instance always carries it (default ``False``).
///
/// Failure surfaces as a ``PyErr`` propagated up to ``extract_binaries``,
/// which becomes a ``ValueError`` / ``AttributeError`` at the Python
/// boundary — the same shape the surrounding extractors raise for
/// malformed inputs.
fn extract_task_dep(obj: &Bound<'_, PyAny>, enclosing_phase: &PhaseId) -> PyResult<TaskDep> {
    // A dep's full identity is ``(phase_id, task_id)``. A bare string
    // names no phase, so it resolves to the ENCLOSING task's phase —
    // the consumer-boundary normalization for the common intra-phase
    // case. This is NOT the framework runtime default: by the time the
    // dep leaves this extractor it carries an explicit phase. A
    // cross-phase dependency must set ``TaskDep(phase_id=...)``.
    if let Ok(s) = obj.extract::<String>() {
        return Ok(TaskDep {
            task_id: s,
            phase_id: enclosing_phase.clone(),
            inherit_outputs: false,
        });
    }
    let task_id: String = obj.getattr("task_id")?.extract()?;
    let inherit_outputs: bool = obj.getattr("inherit_outputs")?.extract()?;
    // ``phase_id`` is optional on the Python ``TaskDep`` dataclass: an
    // empty / missing value resolves to the enclosing task's phase
    // (intra-phase, the common case); a non-empty value names a
    // cross-phase prerequisite explicitly.
    let phase_id_str: String = obj
        .getattr("phase_id")
        .and_then(|v| v.extract())
        .unwrap_or_default();
    let phase_id = if phase_id_str.is_empty() {
        enclosing_phase.clone()
    } else {
        PhaseId::from(phase_id_str)
    };
    Ok(TaskDep {
        task_id,
        phase_id,
        inherit_outputs,
    })
}

/// Walk a Python iterable of ``task_depends_on`` entries and produce
/// the Rust-side ``Vec<TaskDep>``. Each entry is bridged by
/// :func:`extract_task_dep`; the first per-entry error propagates and
/// aborts the walk. `enclosing_phase` is the phase of the task that
/// owns these deps — used to resolve phase-less entries to their
/// intra-phase identity.
fn extract_task_depends_on(
    value: &Bound<'_, PyAny>,
    enclosing_phase: &PhaseId,
) -> PyResult<Vec<TaskDep>> {
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
        out.push(extract_task_dep(&item?, enclosing_phase)?);
    }
    Ok(out)
}

/// Convert a Python list of `TaskInfo`-shaped objects into the
/// Rust-side scheduling units, each PAIRED with its discovery-time
/// `skipped_already_done` marker.
///
/// The marker rides the discovery boundary as a parallel bit, NOT on the
/// core `TaskInfo<I>` (a "this item's outputs already exist" signal is a
/// discovery-time routing decision, not a property of the scheduling
/// unit, and is meaningless on every non-skip task — see the
/// `cluster_mutation::TaskSkippedAlreadyDone` design). Missing attribute
/// / non-bool value ⇒ `false` (back-compat: a producer that never marks a
/// skip yields today's all-`Pending` batch). This is the ONE extract
/// function (R4): each consumer (distributed seed seam / single-process /
/// `--list-files`) takes the pair and decides whether the bit matters —
/// there is no bit-discarding near-duplicate.
pub(crate) fn extract_binaries(
    binaries: &Bound<'_, PyList>,
) -> PyResult<Vec<(TaskInfo<RunnerIdentifier>, bool)>> {
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

            // Mandatory task identifier. Missing attribute, `None`,
            // or empty string are all rejected with an
            // operator-actionable ValueError so producer-side bugs
            // (forgotten task_id, accidental "") surface here rather
            // than later as opaque "feature doesn't work" symptoms.
            // The framework treats `task_id` opaquely; the producer
            // composes whatever identity scheme suits its domain.
            let task_id_opt: Option<String> = item
                .getattr("task_id")
                .ok()
                .and_then(|v| v.extract::<Option<String>>().ok().flatten());
            let task_id = match task_id_opt {
                Some(s) if !s.is_empty() => s,
                Some(_) => {
                    return Err(PyValueError::new_err(
                        "TaskInfo.task_id must be a non-empty str; \
                         consumer set it to the empty string. \
                         See `dynamic_runner._shared.task_info.TaskInfo`.",
                    ));
                }
                None => {
                    return Err(PyValueError::new_err(
                        "TaskInfo.task_id is required (non-empty str). \
                         Consumer must populate it at every TaskInfo \
                         construction. \
                         See `dynamic_runner._shared.task_info.TaskInfo`.",
                    ));
                }
            };
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
                .map(|v| extract_task_depends_on(&v, &phase_id))
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

            // Discovery-time already-done marker. Missing attribute or a
            // non-bool value collapses to `false` (back-compat: the bit
            // is OPTIONAL — a producer that predates the marker, or never
            // marks a skip, yields today's all-`Pending` batch). The bit
            // is NOT written into the constructed `TaskInfo` — it rides
            // back alongside it as the second tuple element so the ingest
            // seam can partition on it.
            let skipped_already_done: bool = item
                .getattr("skipped_already_done")
                .ok()
                .and_then(|v| v.extract::<bool>().ok())
                .unwrap_or(false);

            Ok((
                TaskInfo {
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
                    preferred_version: Default::default(),
                    resolved_path: None,
                },
                skipped_already_done,
            ))
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
        simplens
            .call((), Some(&kwargs))
            .expect("SimpleNamespace(...)")
    }

    /// As `make_task_dep` but carrying an explicit cross-phase
    /// `phase_id`, mirroring `TaskDep(task_id, phase_id=...)`.
    fn make_phased_task_dep<'py>(
        py: Python<'py>,
        task_id: &str,
        phase_id: &str,
        inherit_outputs: bool,
    ) -> Bound<'py, PyAny> {
        let types = py.import("types").expect("types module");
        let simplens = types.getattr("SimpleNamespace").expect("SimpleNamespace");
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("task_id", task_id).unwrap();
        kwargs.set_item("phase_id", phase_id).unwrap();
        kwargs.set_item("inherit_outputs", inherit_outputs).unwrap();
        simplens
            .call((), Some(&kwargs))
            .expect("SimpleNamespace(...)")
    }

    #[test]
    fn extract_task_dep_bare_string_resolves_enclosing_phase() {
        Python::attach(|py| {
            let obj = pyo3::types::PyString::new(py, "alpha");
            let enclosing = PhaseId::from("phaseX");
            let dep = extract_task_dep(&obj.into_any(), &enclosing).expect("bare string");
            assert_eq!(dep.task_id, "alpha");
            assert_eq!(dep.phase_id, enclosing, "bare string takes enclosing phase");
            assert!(!dep.inherit_outputs);
        });
    }

    #[test]
    fn extract_task_dep_dataclass_carries_inherit_outputs() {
        Python::attach(|py| {
            let enclosing = PhaseId::from("phaseX");
            let obj = make_task_dep(py, "beta", true);
            let dep = extract_task_dep(&obj, &enclosing).expect("attribute-bearing object");
            assert_eq!(dep.task_id, "beta");
            assert_eq!(
                dep.phase_id, enclosing,
                "phase-less dataclass takes enclosing phase"
            );
            assert!(dep.inherit_outputs);

            let obj2 = make_task_dep(py, "gamma", false);
            let dep2 = extract_task_dep(&obj2, &enclosing).expect("attribute-bearing object");
            assert_eq!(dep2.task_id, "gamma");
            assert!(!dep2.inherit_outputs);
        });
    }

    #[test]
    fn extract_task_dep_explicit_phase_is_cross_phase() {
        // An explicit `phase_id` names a cross-phase prerequisite and is
        // NOT overridden by the enclosing phase.
        Python::attach(|py| {
            let enclosing = PhaseId::from("phaseX");
            let obj = make_phased_task_dep(py, "delta", "phaseY", true);
            let dep = extract_task_dep(&obj, &enclosing).expect("phased dep");
            assert_eq!(dep.task_id, "delta");
            assert_eq!(dep.phase_id, PhaseId::from("phaseY"));
            assert!(dep.inherit_outputs);
        });
    }

    #[test]
    fn extract_task_depends_on_mixed_iterable() {
        // The wire-equivalent of `["A", TaskDep("B", inherit_outputs=True)]`:
        // a Python tuple mixing the two legal entry shapes. The bridge
        // must preserve order and the inherit-outputs flag, and resolve
        // phase-less entries to the enclosing phase.
        Python::attach(|py| {
            let enclosing = PhaseId::from("phaseX");
            let bare = pyo3::types::PyString::new(py, "A").into_any();
            let struct_dep = make_task_dep(py, "B", true);
            let tuple = pyo3::types::PyTuple::new(py, [bare, struct_dep]).expect("mixed tuple");
            let deps = extract_task_depends_on(tuple.as_any(), &enclosing).expect("mixed iterable");
            assert_eq!(deps.len(), 2);
            assert_eq!(deps[0].task_id, "A");
            assert_eq!(deps[0].phase_id, enclosing);
            assert!(!deps[0].inherit_outputs);
            assert_eq!(deps[1].task_id, "B");
            assert_eq!(deps[1].phase_id, enclosing);
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
            let enclosing = PhaseId::from("phaseX");
            let none = py.None().into_bound(py);
            let deps = extract_task_depends_on(&none, &enclosing).expect("None");
            assert!(deps.is_empty());
        });
    }

    /// Build a minimal TaskInfo-shaped `SimpleNamespace` the duck-typed
    /// `extract_binaries` accepts. `skipped` is `Some(bool)` to SET the
    /// `skipped_already_done` attribute, or `None` to OMIT it entirely
    /// (the back-compat / pre-marker producer shape).
    fn make_task_item<'py>(
        py: Python<'py>,
        task_id: &str,
        skipped: Option<bool>,
    ) -> Bound<'py, PyAny> {
        let types = py.import("types").expect("types module");
        let simplens = types.getattr("SimpleNamespace").expect("SimpleNamespace");
        let ident_kwargs = pyo3::types::PyDict::new(py);
        ident_kwargs.set_item("binary_name", task_id).unwrap();
        ident_kwargs.set_item("platform", "x64").unwrap();
        ident_kwargs.set_item("compiler", "gcc").unwrap();
        ident_kwargs.set_item("version", "1").unwrap();
        ident_kwargs.set_item("opt_level", "O0").unwrap();
        let identifier = simplens
            .call((), Some(&ident_kwargs))
            .expect("identifier SimpleNamespace");

        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("task_id", task_id).unwrap();
        kwargs
            .set_item("path", format!("/corpus/{task_id}"))
            .unwrap();
        kwargs.set_item("size", 1u64).unwrap();
        kwargs.set_item("identifier", identifier).unwrap();
        kwargs.set_item("type_id", "t").unwrap();
        kwargs.set_item("task_depends_on", Vec::<String>::new()).unwrap();
        // Only set the marker when the test asks for it — `None` leaves
        // the attribute absent so the missing-attr ⇒ false path is real.
        if let Some(flag) = skipped {
            kwargs.set_item("skipped_already_done", flag).unwrap();
        }
        simplens.call((), Some(&kwargs)).expect("item SimpleNamespace")
    }

    /// R5 plumbing-e2e (extract layer): the `skipped_already_done` bit on a
    /// Python discovery item must survive the extract boundary as the
    /// second element of the marked pair — a MARKED item yields `true`, an
    /// explicitly-UNMARKED item yields `false`, and an item that OMITS the
    /// attribute entirely yields `false` (the optional / back-compat
    /// contract). The bit must NOT bleed into the constructed core
    /// `TaskInfo` (it has no such field — that is the boundary invariant).
    #[test]
    fn extract_binaries_threads_skipped_marker() {
        Python::attach(|py| {
            let marked = make_task_item(py, "already-done", Some(true));
            let unmarked = make_task_item(py, "needs-run", Some(false));
            let absent = make_task_item(py, "legacy-producer", None);
            let list = PyList::new(py, [marked, unmarked, absent]).expect("list");

            let out = extract_binaries(&list).expect("extract");
            assert_eq!(out.len(), 3, "every item is extracted (none dropped)");

            // Order is preserved; the marker rides as the second element.
            assert_eq!(out[0].0.task_id, "already-done");
            assert!(
                out[0].1,
                "an item with skipped_already_done=True must extract as marked"
            );
            assert_eq!(out[1].0.task_id, "needs-run");
            assert!(
                !out[1].1,
                "an item with skipped_already_done=False must extract as unmarked"
            );
            assert_eq!(out[2].0.task_id, "legacy-producer");
            assert!(
                !out[2].1,
                "an item that OMITS the attribute must extract as unmarked \
                 (the optional / back-compat default)"
            );
        });
    }
}
