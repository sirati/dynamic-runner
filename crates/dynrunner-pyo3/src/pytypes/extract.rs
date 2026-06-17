//! Bridge helpers for converting Python `TaskInfo`-shaped objects to
//! the Rust-side `TaskInfo<RunnerIdentifier>` and back.

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{
    AffinityId, Identifier, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskDep, TaskInfo,
    TypeId, UploadFileRef, UploadRoot,
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
        // `kind` IS on `TaskInfo<I>`, so the projection reflects it on both
        // mutually-exclusive kind bools.
        is_setup: task.kind.is_setup(),
        is_secondary_affine: task.kind.is_secondary_affine(),
        // `setup_affinity` IS on `TaskInfo<I>`, so the projection reflects it.
        setup_affinity: task.setup_affinity.clone(),
        // `required_files` IS on `TaskInfo<I>` (#336 P2), so the projection
        // reflects it — each core `UploadFileRef` renders back to its
        // `(source, optional dest, root)` triple (#644).
        required_files: task
            .required_files()
            .iter()
            .map(|f| {
                (
                    f.source.to_string_lossy().into_owned(),
                    f.dest.as_ref().map(|d| d.to_string_lossy().into_owned()),
                    f.root.into(),
                )
            })
            .collect(),
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
            // Un-resolved at the consumer boundary: the originating primary
            // stamps the prereq's resolved def-id at TaskAdded origination.
            def_id: None,
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
        // Un-resolved at the consumer boundary; the originator stamps it.
        def_id: None,
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

/// Extract one ``files`` entry into a Rust-side [`UploadFileRef`] (#336 P2).
///
/// Single concern: bridge the legal Python shapes a consumer writes in
/// ``TaskInfo.files`` — a bare ``str``/``Path`` source (the common case;
/// destination is derived, root defaults to ``UploadRoot.SOURCE``), a
/// ``(source, dest)`` 2-tuple (explicit placement under the srcbins root), or a
/// ``(source, dest, root)`` 3-tuple (explicit placement + a framework mount
/// selector, #644). Order of attempts:
///
/// 1. Try ``extract::<String>``. Succeeds for a bare ``str`` source and (via
///    ``PathBuf``'s string coercion) a ``Path``; becomes
///    ``UploadFileRef { source, dest: None, root: Source }``.
/// 2. Fall back to a 3-element ``(source, dest, root)`` sequence.
/// 3. Fall back to a 2-element ``(source, dest)`` sequence (root ⇒ Source).
///    ``dest`` may be ``None`` (same as the bare case) or an explicit
///    ``str``/``Path``.
///
/// A shape that is none of these raises a ``ValueError`` naming the offending
/// entry — the same loud-at-the-boundary contract the surrounding extractors
/// use.
fn extract_one_required_file(obj: &Bound<'_, PyAny>) -> PyResult<UploadFileRef> {
    // A bare source (str / Path coerced to str) — destination derived,
    // default (srcbins) root.
    if let Ok(source) = obj.extract::<String>() {
        return Ok(UploadFileRef {
            source: PathBuf::from(source),
            dest: None,
            root: UploadRoot::default(),
        });
    }
    // A `(source, dest, root)` triple — explicit placement + framework mount
    // selector (#644). Tried BEFORE the 2-tuple so a 3-element entry binds the
    // root rather than failing the 2-tuple extract.
    if let Ok((source, dest, root)) =
        obj.extract::<(String, Option<String>, crate::pytypes::PyUploadRoot)>()
    {
        return Ok(UploadFileRef {
            source: PathBuf::from(source),
            dest: dest.map(PathBuf::from),
            root: root.into(),
        });
    }
    // A `(source, dest)` pair — default (srcbins) root.
    if let Ok((source, dest)) = obj.extract::<(String, Option<String>)>() {
        return Ok(UploadFileRef {
            source: PathBuf::from(source),
            dest: dest.map(PathBuf::from),
            root: UploadRoot::default(),
        });
    }
    Err(PyValueError::new_err(
        "TaskInfo.files entry must be a source path (str/Path), a \
         (source, dest) pair, or a (source, dest, root) triple; got an \
         unsupported shape. See `dynamic_runner._shared.task_info.TaskInfo.files`.",
    ))
}

/// Walk a Python iterable of ``files`` entries and produce the Rust-side
/// ``Vec<UploadFileRef>`` (#336 P2). Each entry is bridged by
/// :func:`extract_one_required_file`; the first per-entry error propagates and
/// aborts the walk. ``None`` is handled by the caller (collapses to empty); a
/// non-iterable value raises at ``try_iter``.
fn extract_required_files(value: &Bound<'_, PyAny>) -> PyResult<Vec<UploadFileRef>> {
    let iter = value.try_iter()?;
    let mut out = Vec::new();
    for item in iter {
        out.push(extract_one_required_file(&item?)?);
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

            // Optional kind markers — the consumer-boundary surface of the
            // first-class `TaskKind`. Each missing attribute or non-bool value
            // collapses to `false` (back-compat: a producer that predates the
            // markers yields ordinary worker tasks). `is_secondary_affine =
            // True` ⇒ `TaskKind::SecondaryAffine` (#497, the per-secondary
            // import GATE); else `is_setup = True` ⇒ `TaskKind::Setup` (a
            // framework setup primitive); else `TaskKind::Work`. Both
            // primitives are declared REGARDLESS of any CLI flag (unconditional).
            // This is the SINGLE point (on this boundary) the Python kind bools
            // map to the Rust `TaskKind`; `is_secondary_affine` wins over
            // `is_setup` (a task is at most one kind).
            let getattr_bool = |name: &str| -> bool {
                item.getattr(name)
                    .ok()
                    .and_then(|v| v.extract::<bool>().ok())
                    .unwrap_or(false)
            };
            let kind = if getattr_bool("is_secondary_affine") {
                dynrunner_core::TaskKind::SecondaryAffine
            } else if getattr_bool("is_setup") {
                dynrunner_core::TaskKind::Setup
            } else {
                dynrunner_core::TaskKind::Work
            };

            // Optional `setup_affinity` — the consumer names the member that
            // runs a setup task IN-PROCESS. Missing / non-str / empty
            // collapses to `None` (the executor defaults to the primary).
            // A routing concern consulted only for a `Setup` task; carried
            // verbatim onto the core `TaskInfo`.
            let setup_affinity: Option<String> = item
                .getattr("setup_affinity")
                .ok()
                .and_then(|v| v.extract::<Option<String>>().ok())
                .flatten()
                .filter(|s| !s.is_empty());

            // Optional `files` — the consumer-declared required files this WORK
            // task needs uploaded before it runs (#336 P2). Each entry is a
            // bare `str`/`Path` source, or a `(source, dest)` pair. Missing /
            // None collapses to empty (the common case — every pre-#336 task).
            // The framework's files-attach transform DEDUPS these across the
            // batch into upload setup tasks + deps; this boundary only carries
            // the declaration through onto `required_files`.
            let required_files: Vec<UploadFileRef> = match item.getattr("files") {
                Ok(v) if !v.is_none() => extract_required_files(&v)?,
                _ => Vec::new(),
            };

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
                    kind,
                    setup_affinity,
                    upload_file: None,
                    // Normalize to the storage shape (empty ⇒ `None`, so the
                    // common no-files task never bloats the wire / the enum).
                    required_files: dynrunner_core::required_files_storage(required_files),
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

    // ── #336 P2: the `files=` consumer surface ──────────────────────────────

    /// Build a TaskInfo-shaped item carrying a `files` attribute (#336 P2).
    /// `files` is set verbatim to the supplied Python value (a list of bare
    /// sources and/or `(source, dest)` tuples).
    fn make_task_item_with_files<'py>(
        py: Python<'py>,
        task_id: &str,
        files: Bound<'py, PyAny>,
    ) -> Bound<'py, PyAny> {
        let item = make_task_item(py, task_id, None);
        item.setattr("files", files).expect("set files");
        item
    }

    #[test]
    fn extract_one_required_file_accepts_bare_source_and_pair() {
        Python::attach(|py| {
            // Bare str source -> derived destination.
            let bare = pyo3::types::PyString::new(py, "/src/a").into_any();
            let f = extract_one_required_file(&bare).expect("bare source");
            assert_eq!(f.source, PathBuf::from("/src/a"));
            assert_eq!(f.dest, None);

            // (source, dest) pair -> explicit placement.
            let pair = pyo3::types::PyTuple::new(
                py,
                [
                    pyo3::types::PyString::new(py, "/src/b").into_any(),
                    pyo3::types::PyString::new(py, "/dst/b").into_any(),
                ],
            )
            .expect("pair");
            let f = extract_one_required_file(pair.as_any()).expect("pair");
            assert_eq!(f.source, PathBuf::from("/src/b"));
            assert_eq!(f.dest, Some(PathBuf::from("/dst/b")));

            // (source, None) pair -> derived destination.
            let pair_none = pyo3::types::PyTuple::new(
                py,
                [
                    pyo3::types::PyString::new(py, "/src/c").into_any(),
                    py.None().into_bound(py),
                ],
            )
            .expect("pair none");
            let f = extract_one_required_file(pair_none.as_any()).expect("pair none");
            assert_eq!(f.source, PathBuf::from("/src/c"));
            assert_eq!(f.dest, None);
        });
    }

    #[test]
    fn extract_binaries_threads_files_onto_required_files() {
        // The `files=` consumer surface crosses the extract boundary onto the
        // core `required_files`. A missing `files` attribute (the back-compat
        // default) yields empty.
        Python::attach(|py| {
            // A `(source, dest, root)` 3-tuple selecting the OUTPUT mount (#644).
            let output_root = py
                .get_type::<crate::pytypes::PyUploadRoot>()
                .getattr("OUTPUT")
                .expect("UploadRoot.OUTPUT");
            let files = PyList::new(
                py,
                [
                    pyo3::types::PyString::new(py, "/src/a").into_any(),
                    pyo3::types::PyTuple::new(
                        py,
                        [
                            pyo3::types::PyString::new(py, "/src/b").into_any(),
                            pyo3::types::PyString::new(py, "/dst/b").into_any(),
                        ],
                    )
                    .expect("pair")
                    .into_any(),
                    pyo3::types::PyTuple::new(
                        py,
                        [
                            pyo3::types::PyString::new(py, "/src/c").into_any(),
                            pyo3::types::PyString::new(py, "/dst/c").into_any(),
                            output_root,
                        ],
                    )
                    .expect("triple")
                    .into_any(),
                ],
            )
            .expect("files list");
            let with_files = make_task_item_with_files(py, "build", files.into_any());
            let without = make_task_item(py, "plain", None);
            let list = PyList::new(py, [with_files, without]).expect("list");

            let out = extract_binaries(&list).expect("extract");
            assert_eq!(out.len(), 2);
            // The `files=`-carrying task gets three required files.
            let build = &out[0].0;
            assert_eq!(build.task_id, "build");
            assert_eq!(build.required_files().len(), 3);
            assert_eq!(build.required_files()[0].source, PathBuf::from("/src/a"));
            assert_eq!(build.required_files()[0].dest, None);
            // Bare + 2-tuple entries default to the SOURCE root (#644 back-compat).
            assert_eq!(build.required_files()[0].root, UploadRoot::Source);
            assert_eq!(build.required_files()[1].source, PathBuf::from("/src/b"));
            assert_eq!(build.required_files()[1].dest, Some(PathBuf::from("/dst/b")));
            assert_eq!(build.required_files()[1].root, UploadRoot::Source);
            // The 3-tuple entry carries the OUTPUT selector verbatim.
            assert_eq!(build.required_files()[2].source, PathBuf::from("/src/c"));
            assert_eq!(build.required_files()[2].dest, Some(PathBuf::from("/dst/c")));
            assert_eq!(
                build.required_files()[2].root,
                UploadRoot::Output,
                "#644: a (source, dest, root) entry threads the OUTPUT mount selector"
            );
            // The plain task (no `files` attribute) has none.
            assert!(out[1].0.required_files().is_empty(), "no files -> empty");
        });
    }

    #[test]
    fn files_extract_then_augment_dedups_to_one_upload_per_unique_file() {
        // END-TO-END plumbing (#336 P2): the Python `files=` surface ->
        // `extract_binaries` -> `required_files` -> the framework's
        // files-attach augment DEDUPS shared files into ONE upload setup task
        // per unique file. Three builds share /tc/common; b1 also lists
        // /tc/delta. Asserts the dedup at the augment layer the primary runs.
        Python::attach(|py| {
            let mk = |task_id: &str, srcs: &[&str]| {
                let entries: Vec<_> = srcs
                    .iter()
                    .map(|s| pyo3::types::PyString::new(py, s).into_any())
                    .collect();
                let files = PyList::new(py, entries).expect("files");
                make_task_item_with_files(py, task_id, files.into_any())
            };
            let list = PyList::new(
                py,
                [
                    mk("b1", &["/tc/common", "/tc/delta"]),
                    mk("b2", &["/tc/common"]),
                    mk("b3", &["/tc/common"]),
                ],
            )
            .expect("list");

            let extracted = extract_binaries(&list).expect("extract");
            // Run the SAME augment transform the primary's originator runs.
            let aug = dynrunner_manager_distributed::augment_batch_for_staging(
                extracted,
                dynrunner_manager_distributed::StagingStrategy::Disabled,
            );

            // EXACTLY two upload setup tasks (common, delta) — one per UNIQUE
            // file, NOT four (one per (task, file) pair).
            let uploads: Vec<_> = aug
                .batch
                .iter()
                .filter(|(t, _)| t.kind.is_setup() && t.upload_file.is_some())
                .collect();
            assert_eq!(
                uploads.len(),
                2,
                "deduped: one upload per unique file (common, delta)"
            );

            // The single /tc/common upload id, shared by all three builds.
            let common_id = uploads
                .iter()
                .find(|(t, _)| {
                    t.upload_file.as_ref().unwrap().source.as_path()
                        == std::path::Path::new("/tc/common")
                })
                .map(|(t, _)| t.task_id.clone())
                .expect("a common upload");
            for name in ["b1", "b2", "b3"] {
                let work = aug
                    .batch
                    .iter()
                    .find(|(t, _)| t.task_id == name)
                    .map(|(t, _)| t)
                    .expect("work task");
                assert!(
                    work.task_depends_on.iter().any(|d| d.task_id == common_id),
                    "{name} gates on the single shared /tc/common upload"
                );
            }
        });
    }
}
