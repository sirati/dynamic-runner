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
        std::ffi::CString::new("stub_task_def.py").unwrap().as_c_str(),
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
        /* peer_lifecycle_listener */ None,
        /* task_completed_listener */ None,
        /* unfulfillable_reinject_max_per_task */ cap,
        /* log_dir */ None,
        /* scheduler_config */ None,
        /* panik_watcher_paths */ None,
        /* panik_watcher_poll_interval_secs */ 10.0,
        /* memprofile_enabled */ false,
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
        let _handle: pyo3::PyRef<'_, crate::managers::primary_handle::PyPrimaryHandle> =
            handle_obj
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
        let h2 = mgr
            .bind(py)
            .call_method0("handle")
            .expect("second handle");
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
            mgr_ref.control_plane.same_command_channel(&r1.borrow().sender),
            "first handle must share the manager's command channel"
        );
        assert!(
            mgr_ref.control_plane.same_command_channel(&r2.borrow().sender),
            "second handle must share the manager's command channel"
        );
    });
}
