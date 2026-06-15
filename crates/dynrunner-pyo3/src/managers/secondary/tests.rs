#![cfg(test)]
#![cfg(feature = "test-with-python")]
//! `RustSecondaryCoordinator` constructor-wiring tests.
//!
//! Single concern: pin that the consumer-supplied Python kwargs reach the
//! pyclass fields the `run()` install path reads BEFORE `coord.run()` enters.
//! (#577) The pre-#577 `import_action` kwarg regression is gone — gate
//! bodies run in worker subprocesses dispatched via the normal task-
//! dispatch path; the consumer registers a `TaskTypeSpec` whose
//! `worker_module` holds the `@task_function` handler.
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

// (#577) The pre-#577 `import_action_kwarg_is_stored_on_secondary` test
// and its `build_secondary(import_action)` harness are GONE — the
// `import_action` kwarg + field were removed framework-side (gate bodies
// now run in worker subprocesses dispatched via the normal task-dispatch
// path; the consumer registers a `TaskTypeSpec` whose `worker_module`
// holds the `@task_function` handler).
