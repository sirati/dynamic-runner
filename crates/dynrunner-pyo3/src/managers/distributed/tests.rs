#![cfg(test)]
#![cfg(feature = "test-with-python")]
//! Pre-run `handle()` factory contract tests for the in-process
//! distributed manager. Mirrors `PyPrimaryCoordinator::handle`'s
//! shape — same single concern: can the Python caller fetch a
//! `PrimaryHandle` BEFORE the blocking `run()` enters the
//! detached tokio runtime?
//!
//! Tests require an embedded CPython interpreter (gated behind
//! the `test-with-python` feature). Invoke as:
//!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
//!        --features test-with-python pydistributed_manager`
//!
//! Scope: limited to (1) the factory call surface and (2) the
//! cap-cell seeding. End-to-end command dispatch is already
//! exercised by the `primary_handle.rs` tests against a stub
//! receiver; the channel-and-cell wiring on this manager carries
//! the same `PyPrimaryHandle::from_sender` constructor, so the
//! same dispatch contract holds transitively.
use super::*;
use pyo3::types::{PyAnyMethods, PyModule};

/// Compile a tiny Python module that exports a `TaskDefinition`-
/// shaped stub + a default `task_args` Namespace. The shape is
/// the minimum `LoadedTaskDefinition::from_python` needs:
///   * `get_phases()` → one PhaseSpec with one TaskTypeSpec.
///   * `build_worker_command_args(...)` → `[]`.
///   * `estimate_memory_returns` attribute referenced by the
///     `estimator_attr` lookup → trivial callable.
///
/// Centralised so each test phrases the stub once.
fn build_task_definition_module(py: Python<'_>) -> Bound<'_, PyModule> {
    // Stubs are pure-Python `SimpleNamespace` instances to avoid
    // importing `dynamic_runner.task_protocol` (the test
    // interpreter doesn't have the wheel installed; the cdylib
    // under test isn't even on sys.path here). The `from_python`
    // extractors duck-type via `getattr`, so any object with the
    // right attribute names + types works.
    let source = r#"
from types import SimpleNamespace

def estimate_memory(item):
    return 1024 * 1024

_TYPE = SimpleNamespace(
    type_id="t",
    worker_module="stub_worker_module",
    estimator_attr="estimate_memory",
    timeout_seconds=None,
    reserved_memory_per_worker=0,
    max_concurrent=None,
)

_PHASE = SimpleNamespace(
    phase_id="p",
    depends_on=[],
    types=(_TYPE,),
)

class _StubTask:
    uses_file_based_items = False
    # `LoadedTopology::from_python` reads `task_definition.<estimator_attr>`
    # on the matching type; expose the callable as an attribute on
    # the stub directly.
    estimate_memory = staticmethod(estimate_memory)
    def get_phases(self):
        return (_PHASE,)
    def build_worker_command_args(self, type_id, args, source_dir, output_dir, skip_existing):
        return []

task = _StubTask()
task_args = SimpleNamespace()
"#;
    PyModule::from_code(
        py,
        std::ffi::CString::new(source).unwrap().as_c_str(),
        std::ffi::CString::new("stub_task_def.py")
            .unwrap()
            .as_c_str(),
        std::ffi::CString::new("stub_task_def").unwrap().as_c_str(),
    )
    .expect("compile stub TaskDefinition module")
}

/// Construct a `PyDistributedManager` with the supplied
/// `unfulfillable_reinject_max_per_task`. Returns the manager
/// already wrapped in a `PyClass` cell so subsequent `.handle()`
/// calls can flow through the PyO3 method-dispatch surface (the
/// production call path).
fn build_manager(py: Python<'_>, cap: Option<u32>) -> PyResult<Py<PyDistributedManager>> {
    build_manager_with(py, cap, None, None)
}

/// As [`build_manager`] but also threads an optional `import_action`
/// Python callable — the #501 affine-import forwarding surface — and
/// an optional `upload_action` Python callable — the #336 P1 / #493
/// option-A primary-side upload forwarding surface.
fn build_manager_with(
    py: Python<'_>,
    cap: Option<u32>,
    import_action: Option<Py<PyAny>>,
    upload_action: Option<Py<PyAny>>,
) -> PyResult<Py<PyDistributedManager>> {
    let module = build_task_definition_module(py);
    let task = module.getattr("task")?;
    let task_args = module.getattr("task_args")?;
    // `&Bound<'_, PyAny>` for the `new` signature.
    let mgr = PyDistributedManager::new(
        py,
        /* num_secondaries */ 1,
        /* num_workers_per_secondary */ 1,
        /* ram_per_secondary */ 64 * 1024 * 1024,
        /* source_dir */ "/tmp/src".into(),
        /* output_dir */ "/tmp/out".into(),
        &task,
        &task_args,
        /* skip_existing */ false,
        /* log_paths */ None,
        /* worker_spec */ None,
        /* distributed_config */ None,
        /* max_resources_per_secondary */ None,
        /* source_pre_staged_root */ None,
        /* stage_via_setup_tasks */ false,
        /* peer_lifecycle_listener */ None,
        /* task_completed_listener */ None,
        /* import_action */ import_action,
        /* affine_satisfied_probe */ None,
        /* upload_action */ upload_action,
        /* unfulfillable_reinject_max_per_task */ cap,
        /* log_dir */ None,
        /* scheduler_config */ None,
        /* panik_watcher_paths */ None,
        /* panik_watcher_poll_interval_secs */ 10.0,
        /* memprofile_enabled */ false,
        /* forwarded_argv */ Vec::new(),
    )?;
    Py::new(py, mgr)
}

/// Test (1) from the brief: the factory produces a `PrimaryHandle`
/// BEFORE `run()` is called.
#[test]
fn handle_returns_pyprimaryhandle_before_run() {
    Python::attach(|py| {
        let mgr = build_manager(py, None).expect("manager constructs");
        let handle_obj = mgr
            .bind(py)
            .call_method0("handle")
            .expect("handle() must succeed before run()");
        // Downcast to the concrete pyclass — proves the type
        // contract independent of any Python-side getattr name
        // collision.
        let _handle: pyo3::PyRef<'_, crate::managers::primary_handle::PyPrimaryHandle> = handle_obj
            .cast::<crate::managers::primary_handle::PyPrimaryHandle>()
            .expect("handle() must return a PrimaryHandle pyclass")
            .borrow();
    });
}

/// Test (3) variant from the brief: the reinject cap kwarg is
/// seeded into the shared cell at `__init__`, so the handle
/// produced by `handle()` carries the same value.
#[test]
fn handle_reinject_cap_seed_from_init_kwarg() {
    Python::attach(|py| {
        let mgr = build_manager(py, Some(7)).expect("manager constructs");
        // Read the cap through the manager's control-plane
        // helper — this is the same cell the produced handle
        // clones, so a match here proves the round-trip. The
        // crate-internal `cap_snapshot()` accessor exists so
        // tests don't reach through private fields.
        let snapshot = mgr.borrow(py).control_plane.cap_snapshot();
        assert_eq!(snapshot, Some(7), "cap kwarg must seed the cell");
        // Sanity: the factory still succeeds with the cap set.
        let _ = mgr
            .bind(py)
            .call_method0("handle")
            .expect("handle() must succeed with seeded cap");
    });
}

/// Test (2) from the brief: two `handle()` calls return distinct
/// `PrimaryHandle` instances backed by the same underlying
/// channel. We can't directly compare `mpsc::Sender`s, but
/// `tokio::sync::mpsc::Sender::same_channel` exposes the
/// equivalence we want; calling it on the cloned senders proves
/// the factory does not mint a fresh channel per call.
#[test]
fn handle_clones_share_same_command_channel() {
    Python::attach(|py| {
        let mgr = build_manager(py, None).expect("manager constructs");
        let h1 = mgr.bind(py).call_method0("handle").expect("first handle");
        let h2 = mgr.bind(py).call_method0("handle").expect("second handle");
        // Both downcasts must succeed (factory returns the same
        // pyclass); after that, the manager's control-plane
        // helper exposes a `same_command_channel` accessor that
        // confirms each handle's sender shares the manager's
        // receiver. Same `Sender::same_channel` semantics as
        // pre-refactor, routed through the helper so tests don't
        // reach into the handle's `sender` field.
        let r1 = h1
            .cast::<crate::managers::primary_handle::PyPrimaryHandle>()
            .unwrap();
        let r2 = h2
            .cast::<crate::managers::primary_handle::PyPrimaryHandle>()
            .unwrap();
        let mgr_ref = mgr.borrow(py);
        assert!(
            mgr_ref
                .control_plane
                .same_command_channel(&r1.borrow().sender),
            "first handle must share the manager's command channel"
        );
        assert!(
            mgr_ref
                .control_plane
                .same_command_channel(&r2.borrow().sender),
            "second handle must share the manager's command channel"
        );
    });
}

/// #501 regression: the `import_action` kwarg must be STORED on the
/// in-process distributed manager at `__init__`, so the `run()` spawn
/// loop can install it on every in-process secondary's affine
/// executor. Pre-fix `PyDistributedManager::new` had no such param at
/// all, so `run_distributed(import_action=...)` could not thread the
/// consumer's callable through and every in-process affine gate
/// deadlocked "upstream unfulfillable". A `None` import_action leaves
/// the field empty (a task with no affine deps runs unchanged).
#[test]
fn import_action_kwarg_is_stored_on_manager() {
    Python::attach(|py| {
        // A bare callable stands in for the consumer's `import_task`;
        // the manager stores the unbound handle verbatim.
        let import_callable = py
            .eval(c"lambda task_id, payload_json: None", None, None)
            .expect("compile import stub")
            .unbind();

        let with_action = build_manager_with(py, None, Some(import_callable), None)
            .expect("manager constructs with import_action");
        assert!(
            with_action.borrow(py).import_action.is_some(),
            "import_action kwarg must be stored so run()'s spawn loop can \
             install it on every in-process secondary"
        );

        // Absence stays absent — no accidental default importer.
        let without_action = build_manager_with(py, None, None, None)
            .expect("manager constructs without import_action");
        assert!(
            without_action.borrow(py).import_action.is_none(),
            "no import_action kwarg must leave the field empty"
        );
    });
}

/// #493 regression: the `upload_action` kwarg must be STORED on the
/// in-process distributed manager at `__init__`, so the `run()` body
/// can install it on the in-process primary's setup executor BEFORE
/// `run()` enters. Pre-fix `PyDistributedManager::new` had no such
/// param, so `run_distributed(upload_action=...)` could not thread the
/// consumer's callable through; any setup task asking for an upload
/// (derived from a TaskInfo `files=`) then failed with a wiring-error
/// terminal. A `None` upload_action leaves the field empty (a task with
/// no `files=` runs unchanged).
#[test]
fn upload_action_kwarg_is_stored_on_manager() {
    Python::attach(|py| {
        // A bare callable stands in for the consumer's uploader
        // (e.g. `gw.upload_file` or `SlurmJobManager.upload_task_file`);
        // the manager stores the unbound handle verbatim, the bridge
        // wraps it as `Arc<dyn UploadAction>` at `run()` entry.
        let upload_callable = py
            .eval(c"lambda source, dest: None", None, None)
            .expect("compile upload stub")
            .unbind();

        let with_action = build_manager_with(py, None, None, Some(upload_callable))
            .expect("manager constructs with upload_action");
        assert!(
            with_action.borrow(py).upload_action.is_some(),
            "upload_action kwarg must be stored so run()'s primary-install \
             step can wrap it as Arc<dyn UploadAction> and install it BEFORE \
             primary.run() enters"
        );

        // Absence stays absent — no accidental default uploader, no
        // wiring-error masking.
        let without_action = build_manager_with(py, None, None, None)
            .expect("manager constructs without upload_action");
        assert!(
            without_action.borrow(py).upload_action.is_none(),
            "no upload_action kwarg must leave the field empty"
        );
    });
}
