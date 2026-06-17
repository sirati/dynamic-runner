//! `PyTaskInfo` pyclass + conversions to/from `TaskInfo<RunnerIdentifier>`.
//!
//! `task_id` is required (non-optional, non-empty) at this boundary —
//! the silent-skip path that used to mask producer-side bugs is gone.
//! The validation lives in two places: `PyTaskInfo::__new__` (so
//! Python-constructed instances cannot bypass the contract) and the
//! companion `crate::pytypes::extract_binaries` (which is the boundary
//! the Python `TaskInfo` dataclass crosses without going through
//! `__new__`). Internal Rust-side constructors of `PyTaskInfo` (e.g.
//! `From<&TaskInfo>` for round-trip uses) are not gated because the
//! Rust-side `TaskInfo.task_id` is itself non-optional and validated
//! upstream.

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use dynrunner_core::{
    AffinityId, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskDep, TaskInfo, TypeId,
    UploadFileRef,
};

use super::identifier::{PyBinaryIdentifier, split_identifier};

/// Python-visible wrapper for TaskInfo.
#[pyclass(name = "TaskInfo", from_py_object)]
#[derive(Clone, Debug)]
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
    /// Stable per-task identifier. Required (non-empty) — validated
    /// at construction (`__new__`) and at the
    /// `crate::pytypes::extract_binaries` boundary. Mirrors
    /// `dynrunner_core::TaskInfo::task_id`.
    #[pyo3(get)]
    pub(super) task_id: String,
    /// Python-facing view of [`TaskInfo::task_depends_on`]. Exposed as a
    /// `list[str]` of predecessor `task_id`s — matches the legacy
    /// `tuple[str, ...]` shape the Python `TaskInfo` dataclass already
    /// publishes. The Rust-side `TaskDep` carries an additional
    /// `inherit_outputs` flag that is configured server-side (not via
    /// this bridge), so the Python boundary stays bare-string and the
    /// reverse direction reconstitutes with `inherit_outputs = false`
    /// (the legacy-default, matching the untagged `Bare` deserializer arm).
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
    /// Discovery-time "already-done" marker. `True` ⇒ the producer
    /// determined this item's outputs already exist, so the framework
    /// materialises it DIRECTLY as a terminal `SkippedAlreadyDone`
    /// ledger entry (never dispatched) instead of as `Pending`. Default
    /// `false` ⇒ today's behaviour. This is a discovery-BOUNDARY routing
    /// signal — it rides alongside the task at the
    /// `crate::pytypes::extract_binaries` boundary and is NOT carried on
    /// the core Rust `TaskInfo<I>` (nor folded into the content hash):
    /// the `From`/`task_to_pytask` conversions deliberately drop it.
    #[pyo3(get)]
    pub(super) skipped_already_done: bool,
    /// First-class task-KIND marker — the consumer-boundary surface of
    /// the Rust [`dynrunner_core::TaskKind`]. `False` (default) ⇒
    /// `TaskKind::Work` (an ordinary worker task — every existing
    /// consumer). `True` ⇒ `TaskKind::Setup`: a framework setup primitive
    /// that is never worker-assigned, executed in-process by its affinity
    /// member, non-reassignable on death, depend-able, and counted in the
    /// separate setup bucket. Available UNCONDITIONALLY (the primitive is
    /// not behind any CLI flag): a consumer can declare a setup task
    /// regardless of how the run is configured. Carried on the core
    /// `TaskInfo<I>` (unlike `skipped_already_done`) as `kind` — the
    /// `From<&PyTaskInfo>` / `From<&TaskInfo>` conversions thread it.
    #[pyo3(get)]
    pub(super) is_setup: bool,
    /// First-class task-KIND marker for the SecondaryAffine gate — the
    /// consumer-boundary surface of [`dynrunner_core::TaskKind::SecondaryAffine`]
    /// (#497). `False` (default) ⇒ no SecondaryAffine gate. `True` ⇒
    /// `TaskKind::SecondaryAffine`: a primary-side GATE never worker-assigned,
    /// never executed by the primary, never counted in success/fail — its
    /// per-secondary IMPORT runs ONCE locally on each compute secondary whose
    /// dependent WORK tasks gate on it. A SecondaryAffine task uses
    /// `setup_affinity = None` and carries its `task_depends_on` (which may be
    /// EMPTY — the no-dep case is AffineReady at spawn — or a #336 upload
    /// setup-task id; both compose at the CRDT layer with no new dep
    /// machinery). Mutually exclusive with `is_setup` at the SINGLE
    /// kind-selector mapping site below: `is_secondary_affine` wins. Available
    /// UNCONDITIONALLY (no CLI flag). Carried on the core `TaskInfo<I>` as
    /// `kind`.
    #[pyo3(get)]
    pub(super) is_secondary_affine: bool,
    /// EXECUTOR-affinity member for a setup task (`is_setup = True`): the
    /// peer id of the member that runs this setup task IN-PROCESS. A
    /// consumer setup task names its source-owning member here (e.g. a
    /// compute node id); `None` defaults the executor to the primary
    /// itself. Ignored for an ordinary work task (`is_setup = False`).
    /// Maps to the core [`dynrunner_core::TaskInfo::setup_affinity`] — the
    /// `From<&PyTaskInfo>` / `From<&TaskInfo>` conversions thread it.
    #[pyo3(get)]
    pub(super) setup_affinity: Option<String>,
    /// Consumer-boundary surface of the core
    /// [`dynrunner_core::TaskInfo::required_files`] (#336 P2): the files this
    /// WORK task needs UPLOADED to the cluster before it runs, each a
    /// `(source, optional dest, root)` triple. Empty (the default) ⇒ no
    /// attached files — the task behaves exactly as today. The framework's
    /// files-attach transform DEDUPS these across the batch into one upload
    /// setup task per unique file + the work task's deps; a consumer declares
    /// them via the Python `TaskInfo.files=[path, ...]` (or
    /// `[(src, dest), ...]` / `[(src, dest, root), ...]`) list. `root` (#644)
    /// selects the framework mount the upload lands under (default
    /// [`PyUploadRoot::Source`]). The `From<&PyTaskInfo>` / `From<&TaskInfo>`
    /// conversions thread the triple onto `required_files`.
    #[pyo3(get)]
    pub(super) required_files: Vec<(String, Option<String>, super::PyUploadRoot)>,
}

#[pymethods]
impl PyTaskInfo {
    /// Build a `TaskInfo`. `task_id` is REQUIRED and must be a
    /// non-empty `str`; passing `None` or `""` raises `ValueError`
    /// at construction. The producer-side bug surface this guards
    /// against is "I forgot to set task_id, my dependent task
    /// silently never runs" — symptoms used to surface as opaque
    /// scheduling stalls; they now surface as loud construction
    /// errors with the operator-actionable hint below.
    #[new]
    #[pyo3(signature = (
        path,
        size,
        identifier,
        task_id,
        phase_id = String::new(),
        type_id = String::new(),
        affinity_id = None,
        payload_json = "null".to_string(),
        task_depends_on = Vec::new(),
        preferred_secondaries = Vec::new(),
        skipped_already_done = false,
        is_setup = false,
        is_secondary_affine = false,
        setup_affinity = None,
        required_files = Vec::new(),
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: String,
        size: u64,
        identifier: PyBinaryIdentifier,
        task_id: String,
        phase_id: String,
        type_id: String,
        affinity_id: Option<String>,
        payload_json: String,
        task_depends_on: Vec<String>,
        preferred_secondaries: Vec<String>,
        skipped_already_done: bool,
        is_setup: bool,
        is_secondary_affine: bool,
        setup_affinity: Option<String>,
        required_files: Vec<(String, Option<String>, super::PyUploadRoot)>,
    ) -> PyResult<Self> {
        if task_id.is_empty() {
            return Err(PyValueError::new_err(
                "TaskInfo.task_id must be a non-empty str; \
                 consumer must populate it at every TaskInfo \
                 construction. See `dynamic_runner._shared.task_info.TaskInfo`.",
            ));
        }
        Ok(Self {
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
            skipped_already_done,
            is_setup,
            is_secondary_affine,
            setup_affinity,
            required_files,
        })
    }
}

/// Convert a `PyTaskInfo`'s `(source, optional dest, root)` triples into the
/// core `TaskInfo::required_files` STORAGE shape
/// (`Option<Box<Vec<UploadFileRef>>>`). The SINGLE point the pyclass
/// file-attach surface crosses into the core type. A `None` dest is the
/// derive-destination case (strip-prefix under the chosen `root`), a `Some`
/// dest is explicit placement; `root` selects the framework mount (#644). An
/// empty list normalizes to `None` via [`TaskInfo::required_files_storage`]
/// (the common case never allocates / never rides the wire).
pub(super) fn required_files_to_core(
    triples: &[(String, Option<String>, super::PyUploadRoot)],
) -> Option<Box<[UploadFileRef]>> {
    dynrunner_core::required_files_storage(
        triples
            .iter()
            .map(|(source, dest, root)| UploadFileRef {
                source: PathBuf::from(source),
                dest: dest.as_ref().map(PathBuf::from),
                root: (*root).into(),
            })
            .collect(),
    )
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
            phase_id: phase_id.clone(),
            type_id,
            affinity_id,
            payload,
            // PyTaskInfo's invariant (validated at `__new__`) is that
            // `task_id` is non-empty; the conversion is a verbatim move.
            task_id: py.task_id.clone(),
            // `PyTaskInfo`'s Python contract is bare task_ids — they name
            // no phase, so each resolves to the ENCLOSING task's phase
            // (the intra-phase case this bare-list shape expresses). A
            // dep's full identity is `(phase_id, task_id)`; the
            // attribute-rich boundary (`extract.rs`) handles explicit
            // cross-phase deps. `inherit_outputs = false` is the
            // legacy-default for this bare-list shape.
            task_depends_on: py
                .task_depends_on
                .iter()
                .map(|task_id| TaskDep {
                    task_id: task_id.clone(),
                    phase_id: phase_id.clone(),
                    inherit_outputs: false,
                    def_id: None,
                })
                .collect(),
            preferred_secondaries: SoftPreferredSecondaries::new(py.preferred_secondaries.clone()),
            preferred_version: Default::default(),
            // The consumer-boundary kind bools map to the first-class
            // `TaskKind` here — the SINGLE point this pyclass surface crosses
            // into the Rust kind. `is_secondary_affine` wins over `is_setup`
            // (a task is at most one kind); both false ⇒ ordinary `Work`.
            kind: if py.is_secondary_affine {
                dynrunner_core::TaskKind::SecondaryAffine
            } else if py.is_setup {
                dynrunner_core::TaskKind::Setup
            } else {
                dynrunner_core::TaskKind::Work
            },
            // The consumer-boundary executor-affinity id is carried verbatim
            // onto the core `TaskInfo` — the primary's setup selector reads
            // it to target the in-process executor member. Threaded only for
            // the routing concern; the kind decides whether it is consulted.
            setup_affinity: py.setup_affinity.clone(),
            // #336 P1's `upload_file` is the SETUP task's own action payload;
            // a `PyTaskInfo` from a consumer is always a WORK task, so it
            // never carries one (the framework derives the upload setup tasks
            // from `required_files`). Always `None` here.
            upload_file: None,
            // #336 P2: the consumer-declared required files (a WORK task's
            // `files=[...]`) cross verbatim onto the core `required_files`.
            // The framework's files-attach transform then DEDUPS them into
            // upload setup tasks + deps — this boundary only carries the
            // declaration through.
            required_files: required_files_to_core(&py.required_files),
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
            // Project `Vec<TaskDep>` down to bare task_ids for Python — the
            // `inherit_outputs` flag is not part of the Python contract at
            // this bridge layer (a deeper config-time concern).
            task_depends_on: bi
                .task_depends_on
                .iter()
                .map(|dep| dep.task_id.clone())
                .collect(),
            preferred_secondaries: bi.preferred_secondaries.as_slice().to_vec(),
            // The already-done marker is a discovery-INPUT signal only —
            // it is not carried on the core `TaskInfo<I>`, so the
            // round-trip-back direction reconstitutes the default. By the
            // time a task is in the ledger, its skip status is encoded in
            // its `TaskState` variant, not in this discovery-time field.
            skipped_already_done: false,
            // `kind` IS carried on the core `TaskInfo<I>`, so the
            // round-trip-back faithfully reflects it. The two kind bools are
            // the mutually-exclusive projection of the single `TaskKind`.
            is_setup: bi.kind.is_setup(),
            is_secondary_affine: bi.kind.is_secondary_affine(),
            // `setup_affinity` IS carried on the core `TaskInfo<I>`, so the
            // round-trip-back faithfully reflects it.
            setup_affinity: bi.setup_affinity.clone(),
            // `required_files` IS carried on the core `TaskInfo<I>` (#336 P2),
            // so the round-trip-back faithfully reflects it — each core
            // `UploadFileRef` projects back to its `(source, optional dest,
            // root)` triple (#644).
            required_files: bi
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
            task_id: "test-task".into(),
            task_depends_on: Vec::new(),
            preferred_secondaries: preferred,
            skipped_already_done: false,
            is_setup: false,
            is_secondary_affine: false,
            setup_affinity: None,
            required_files: Vec::new(),
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

    #[test]
    fn pytaskinfo_required_files_round_trip_to_core() {
        // #336 P2: the consumer-boundary `required_files` (a list of
        // `(source, optional dest)` pairs on `PyTaskInfo`) crosses verbatim
        // onto the core `TaskInfo::required_files` as `UploadFileRef`s, and
        // round-trips back to the same pairs. A `None` dest is the
        // derive-destination case; a `Some` dest is explicit placement.
        let mut py = sample_pytask(Vec::new());
        py.required_files = vec![
            ("/src/a".to_string(), None, crate::pytypes::PyUploadRoot::Source),
            (
                "/src/b".to_string(),
                Some("/dst/b".to_string()),
                crate::pytypes::PyUploadRoot::Output,
            ),
        ];
        let rust: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py);
        assert_eq!(rust.required_files().len(), 2);
        assert_eq!(rust.required_files()[0].source, PathBuf::from("/src/a"));
        assert_eq!(rust.required_files()[0].dest, None);
        assert_eq!(
            rust.required_files()[0].root,
            dynrunner_core::UploadRoot::Source
        );
        assert_eq!(rust.required_files()[1].source, PathBuf::from("/src/b"));
        assert_eq!(rust.required_files()[1].dest, Some(PathBuf::from("/dst/b")));
        assert_eq!(
            rust.required_files()[1].root,
            dynrunner_core::UploadRoot::Output,
            "#644: the OUTPUT root selector crosses the FFI boundary verbatim"
        );

        // Reverse direction preserves the triples (incl. dest option + root).
        let py_back: PyTaskInfo = PyTaskInfo::from(&rust);
        assert_eq!(
            py_back.required_files,
            vec![
                ("/src/a".to_string(), None, crate::pytypes::PyUploadRoot::Source),
                (
                    "/src/b".to_string(),
                    Some("/dst/b".to_string()),
                    crate::pytypes::PyUploadRoot::Output,
                ),
            ],
        );

        // Empty stays empty (the common case — no spurious entries).
        let py_empty = sample_pytask(Vec::new());
        let rust_empty: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py_empty);
        assert!(rust_empty.required_files().is_empty());
        assert!(
            rust_empty.required_files.is_none(),
            "empty normalizes to None storage (no wire bloat)"
        );
        let py_empty_back: PyTaskInfo = PyTaskInfo::from(&rust_empty);
        assert!(py_empty_back.required_files.is_empty());
    }

    fn sample_identifier() -> PyBinaryIdentifier {
        PyBinaryIdentifier {
            binary_name: "bin".into(),
            platform: "x86_64".into(),
            compiler: "gcc".into(),
            version: "12".into(),
            opt_level: "O2".into(),
        }
    }

    #[test]
    fn pytaskinfo_new_rejects_empty_task_id() {
        // The boundary-validation contract: an empty `task_id` is
        // operator error (typo, accidental ""). The error string
        // names the offending field + points at the consumer-side
        // dataclass so a producer-side mistake surfaces as a loud
        // ValueError, not an opaque "feature doesn't work" later.
        let err = PyTaskInfo::new(
            "/tmp/x".into(),
            16,
            sample_identifier(),
            String::new(),
            "default".into(),
            "default".into(),
            None,
            "null".into(),
            Vec::new(),
            Vec::new(),
            false,
            false,
            false,
            None,
            Vec::new(),
        )
        .expect_err("empty task_id must fail");
        // We assert against the rendered message (no Python
        // interpreter required) to pin the operator-actionable
        // contract.
        assert!(
            err.to_string().contains("non-empty"),
            "error must mention the non-empty contract; got: {err}"
        );
    }

    #[test]
    fn pytaskinfo_new_accepts_non_empty_task_id() {
        // Happy path: a non-empty task_id constructs cleanly. Mirror
        // of the validation test so the success path stays pinned.
        let ok = PyTaskInfo::new(
            "/tmp/x".into(),
            16,
            sample_identifier(),
            "stable-id".into(),
            "default".into(),
            "default".into(),
            None,
            "null".into(),
            Vec::new(),
            Vec::new(),
            false,
            false,
            false,
            None,
            Vec::new(),
        )
        .expect("non-empty task_id must succeed");
        assert_eq!(ok.task_id, "stable-id");
    }

    #[test]
    fn py_taskinfo_is_secondary_affine_maps_to_kind() {
        // #497: the consumer-boundary `is_secondary_affine` bool maps to the
        // first-class `TaskKind::SecondaryAffine` at the SINGLE kind-selector
        // site, and the two kind bools are mutually exclusive (a task is at
        // most one kind).

        // `is_secondary_affine = True` ⇒ SecondaryAffine.
        let mut py = sample_pytask(Vec::new());
        py.is_secondary_affine = true;
        let rust: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py);
        assert_eq!(rust.kind, dynrunner_core::TaskKind::SecondaryAffine);
        // Round-trip-back faithfully reflects the kind on both bools.
        let py_back: PyTaskInfo = PyTaskInfo::from(&rust);
        assert!(py_back.is_secondary_affine);
        assert!(!py_back.is_setup);

        // `is_secondary_affine` wins over `is_setup` (mutually exclusive — the
        // affine gate is selected first).
        let mut py_both = sample_pytask(Vec::new());
        py_both.is_secondary_affine = true;
        py_both.is_setup = true;
        let rust_both: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py_both);
        assert_eq!(rust_both.kind, dynrunner_core::TaskKind::SecondaryAffine);

        // Only `is_setup` ⇒ Setup (unchanged).
        let mut py_setup = sample_pytask(Vec::new());
        py_setup.is_setup = true;
        let rust_setup: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py_setup);
        assert_eq!(rust_setup.kind, dynrunner_core::TaskKind::Setup);

        // Both false ⇒ ordinary Work.
        let py_work = sample_pytask(Vec::new());
        let rust_work: TaskInfo<RunnerIdentifier> = TaskInfo::from(&py_work);
        assert_eq!(rust_work.kind, dynrunner_core::TaskKind::Work);
    }
}
