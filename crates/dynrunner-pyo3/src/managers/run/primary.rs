//! `run_primary` — entry point for the network-based primary coordinator.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::config::primary_secondary::PyPrimaryConfig;
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};

use super::module;

/// Run the network-based primary coordinator. Spawns secondaries via the
/// `spawn_secondary` callback (called once per `config.num_secondaries`).
///
/// `source_dir` (optional) is the local source-tree root the primary
/// reads file contents from for the initial staging walk. Threaded
/// to `RustPrimaryCoordinator.__init__(source_dir=...)` so the
/// inner coordinator's config has the root needed for the content-
/// hash + per-secondary fan-out without each caller re-implementing
/// the staging orchestration. `None` is the right default for pre-
/// staged-source mode, `uses_file_based_items=False`, and remote-
/// only primaries.
///
/// `output_dir` and `task_args` (optional, paired) are forwarded into
/// `on_run_start` when both are `Some`. They are split out from
/// `source_dir` so dispatch helpers that hold the per-run output
/// directory + the parsed argparse Namespace can drive the modern
/// `on_run_start(self, source_dir, output_dir, args, primary_handle)`
/// signature on the primary side. The handle is the in-flight
/// `PrimaryHandle` minted off the coordinator before `run()` enters,
/// so the task's `on_run_start` can drive `primary_handle.spawn_tasks(...)`
/// from inside its lifecycle. Legacy task signatures without the
/// `primary_handle` kwarg are accepted via the bridge's positional-
/// only fallback (see `crate::managers::lifecycle::fire_on_run_start`).
/// When any of the three is missing, `on_run_start` is skipped
/// entirely (back-compat for callers that don't own the dispatch
/// surface).
///
/// `source_pre_staged_root` (optional) carries the
/// `--source-already-staged` signal for the `--multi-computer local`
/// path: when `Some`, the Python dispatch helper has already returned
/// an empty `binaries` list and `RustPrimaryCoordinator::run` will
/// flip `required_setup_on_promote=true` so the chosen secondary runs
/// discovery + ledger-seed on its bind-mounted `src_network`.
/// Mirrors the SLURM pipeline's direct construction of
/// `RustPrimaryCoordinator(source_pre_staged_root=...)` so all three
/// multi-computer modes use the same setup-promote handshake.
#[pyfunction]
#[pyo3(signature = (
    config,
    task_definition,
    spawn_secondary,
    binaries,
    source_dir = None,
    output_dir = None,
    task_args = None,
    source_pre_staged_root = None,
    unfulfillable_reinject_max_per_task = None,
    respawn_policy = None,
    respawn_spawner = None,
    fulfillability_matcher = None,
    peer_lifecycle_listener = None,
    task_completed_listener = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_primary<'py>(
    py: Python<'py>,
    config: PyRef<'py, PyPrimaryConfig>,
    task_definition: &Bound<'py, PyAny>,
    spawn_secondary: Py<PyAny>,
    binaries: &Bound<'py, PyList>,
    source_dir: Option<String>,
    output_dir: Option<String>,
    task_args: Option<Py<PyAny>>,
    source_pre_staged_root: Option<std::path::PathBuf>,
    unfulfillable_reinject_max_per_task: Option<u32>,
    respawn_policy: Option<Py<PyAny>>,
    respawn_spawner: Option<Py<PyAny>>,
    fulfillability_matcher: Option<Py<PyAny>>,
    peer_lifecycle_listener: Option<Py<PyAny>>,
    task_completed_listener: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;
    if let Some(sd) = source_dir.as_ref() {
        kwargs.set_item("source_dir", sd)?;
    }
    if let Some(root) = source_pre_staged_root.as_ref() {
        kwargs.set_item("source_pre_staged_root", root)?;
    }
    if let Some(n) = unfulfillable_reinject_max_per_task {
        kwargs.set_item("unfulfillable_reinject_max_per_task", n)?;
    }
    if let Some(p) = respawn_policy.as_ref() {
        kwargs.set_item("respawn_policy", p)?;
    }
    if let Some(s) = respawn_spawner.as_ref() {
        kwargs.set_item("respawn_spawner", s)?;
    }
    if let Some(m) = fulfillability_matcher.as_ref() {
        kwargs.set_item("fulfillability_matcher", m)?;
    }
    if let Some(l) = peer_lifecycle_listener.as_ref() {
        kwargs.set_item("peer_lifecycle_listener", l)?;
    }
    if let Some(l) = task_completed_listener.as_ref() {
        kwargs.set_item("task_completed_listener", l)?;
    }

    let cls = module(py)?.getattr("RustPrimaryCoordinator")?;
    let args = (
        config.num_secondaries,
        task_definition.clone(),
        spawn_secondary,
    );
    let coord = cls.call(args, Some(&kwargs))?;

    // Fire `on_run_start` with a freshly-minted `PrimaryHandle` so the
    // consumer can drive `primary_handle.spawn_tasks(...)` from inside
    // their lifecycle. We mint the handle BEFORE `coord.run(...)`
    // enters its detached tokio runtime — `RustPrimaryCoordinator.handle`
    // explicitly supports pre-run construction so the Python caller can
    // hand the handle to an executor / thread before the blocking
    // `run()` starts.
    //
    // When any of (source_dir, output_dir, task_args) is absent the
    // hook is skipped — the call signature would be ill-defined and
    // back-compat with non-CLI callers that don't own a Namespace
    // is preserved.
    if let (Some(sd), Some(od), Some(ta)) = (
        source_dir.as_ref(),
        output_dir.as_ref(),
        task_args.as_ref(),
    ) {
        let primary_handle = coord.call_method0("handle")?.unbind();
        fire_on_run_start(task_definition, sd, od, ta.bind(py), Some(primary_handle))?;
    }

    let run_outcome = coord.call_method1("run", (binaries.clone(),));
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    dict.set_item("failed", coord.getattr("failed")?)?;
    // `stranded` carries the cluster-collapse counter that the
    // underlying `RustPrimaryCoordinator.run` raises a `RuntimeError`
    // on; on every successful return it's zero, but exposing the field
    // unconditionally keeps the Python-facing dict shape consistent
    // (consumers' "Completed: / Failed: / Stranded:" log line).
    dict.set_item("stranded", coord.getattr("stranded")?)?;
    Ok(dict.into_any().unbind())
}
