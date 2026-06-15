#![cfg(test)]
#![cfg(feature = "test-with-python")]
//! `RustSecondaryCoordinator` constructor-wiring tests.
//!
//! Single concern: pin that the consumer-supplied Python kwargs reach the
//! pyclass fields the `run()` install path reads BEFORE `coord.run()` enters.
//! The #501 regression here is the `import_action` kwarg: `run()` installs it
//! on the inner `SecondaryCoordinator` via `set_import_action`
//! (`managers/secondary/run.rs`), so it MUST first be stored on the pyclass
//! field by the constructor. The DISTRIBUTED/SLURM entry point
//! (`run/secondary.rs::run_secondary`) builds THIS pyclass via its kwargs
//! dict, so verifying the constructor stores the kwarg also covers that path's
//! forwarding (a missing kwarg there is a `TypeError` at construction).
//!
//! Tests require an embedded CPython interpreter (gated behind the
//! `test-with-python` feature). Invoke as:
//!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
//!        --features test-with-python secondary`
use super::*;
use pyo3::types::{PyAnyMethods, PyModule};

/// Compile a minimal `TaskDefinition`-shaped stub + default `task_args`
/// Namespace — the minimum `LoadedTaskDefinition::from_python` needs. Mirrors
/// `distributed::tests::build_task_definition_module` (the duck-typed
/// `getattr` extractors accept any object with the right attributes).
fn build_task_definition_module(py: Python<'_>) -> Bound<'_, PyModule> {
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

/// Construct a `PySecondaryCoordinator` with the supplied `import_action`
/// (the #501 forwarding surface). `output_dir` doubles as the log-mount root
/// so the constructor's `create_dir_all` lands under a writable temp tree.
fn build_secondary(
    py: Python<'_>,
    import_action: Option<Py<PyAny>>,
) -> PyResult<PySecondaryCoordinator> {
    let module = build_task_definition_module(py);
    let task = module.getattr("task")?;
    let task_args = module.getattr("task_args")?;
    let out = std::env::temp_dir().join("dynrunner-sec-import-test-out");
    PySecondaryCoordinator::new(
        py,
        /* primary_url */ "tcp://127.0.0.1:1".into(),
        /* secondary_id */ "sec-import-test".into(),
        /* num_workers */ 1,
        /* ram_bytes */ 64 * 1024 * 1024,
        /* source_dir */ std::env::temp_dir().to_string_lossy().into_owned(),
        /* output_dir */ out.to_string_lossy().into_owned(),
        &task,
        &task_args,
        /* skip_existing */ false,
        /* log_paths */ None,
        /* worker_spec */ None,
        /* distributed_config */ None,
        /* src_network */ None,
        /* src_tmp */ None,
        /* max_resources */ None,
        /* peer_lifecycle_listener */ None,
        /* import_action */ import_action,
        /* affine_satisfied_probe */ None,
        /* log_dir */ None,
        /* scheduler_config */ None,
        /* panik_watcher_paths */ None,
        /* panik_watcher_poll_interval_secs */ 10.0,
        /* unfulfillable_reinject_max_per_task */ None,
        /* mem_manager_reserved_bytes */ None,
        /* memprofile_enabled */ false,
        /* forwarded_argv */ Vec::new(),
        /* finalize_run_config */ None,
        /* quic_bind_port */ None,
    )
}

/// #501 regression: the `import_action` kwarg must be STORED on the secondary
/// pyclass at `__init__`, because `run()` reads `self.import_action` and
/// installs it via `set_import_action` on the inner coordinator. The
/// distributed/SLURM `run_secondary` free function builds this pyclass through
/// its kwargs dict, so a stored field here is the prerequisite for that path's
/// affine gate to ever dispatch (pre-fix the free fn never set the kwarg and
/// every distributed affine gate deadlocked "upstream unfulfillable").
#[test]
fn import_action_kwarg_is_stored_on_secondary() {
    Python::attach(|py| {
        let import_callable = py
            .eval(c"lambda task_id, payload_json: None", None, None)
            .expect("compile import stub")
            .unbind();

        let with_action =
            build_secondary(py, Some(import_callable)).expect("secondary constructs with action");
        assert!(
            with_action.import_action.is_some(),
            "import_action kwarg must be stored so run() can install it"
        );

        let without_action = build_secondary(py, None).expect("secondary constructs without action");
        assert!(
            without_action.import_action.is_none(),
            "no import_action kwarg must leave the field empty"
        );
    });
}
