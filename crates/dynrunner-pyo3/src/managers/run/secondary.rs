//! `run_secondary` — entry point for a secondary connecting to a remote primary.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::config::log_paths::LogPathConfig;
use crate::config::primary_secondary::PySecondaryConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};

use super::module;

/// Run a secondary that connects to a remote primary at `primary_url`.
#[pyfunction]
#[pyo3(signature = (
    config,
    primary_url,
    task_definition,
    task_args,
    source_dir,
    output_dir,
    skip_existing = false,
    log_paths = None,
    worker_spec = None,
    log_dir = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_secondary<'py>(
    py: Python<'py>,
    config: PyRef<'py, PySecondaryConfig>,
    primary_url: String,
    task_definition: &Bound<'py, PyAny>,
    task_args: &Bound<'py, PyAny>,
    source_dir: String,
    output_dir: String,
    skip_existing: bool,
    log_paths: Option<LogPathConfig>,
    worker_spec: Option<WorkerSpec>,
    log_dir: Option<String>,
) -> PyResult<Py<PyAny>> {
    // Legacy positional `ram_bytes` retained for back-compat; the typed
    // path passes the full multi-resource map via the `max_resources`
    // kwarg, which the legacy class prefers when present.
    let ram_bytes = config.max_resources.inner.get("memory").copied().unwrap_or(0);
    let kwargs = PyDict::new(py);
    kwargs.set_item("skip_existing", skip_existing)?;
    if let Some(lp) = log_paths {
        kwargs.set_item("log_paths", lp)?;
    }
    if let Some(ws) = worker_spec {
        kwargs.set_item("worker_spec", ws)?;
    }
    if let Some(ld) = log_dir {
        kwargs.set_item("log_dir", ld)?;
    }
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;
    kwargs.set_item("max_resources", config.max_resources.clone())?;
    if let Some(sn) = config.src_network.as_ref() {
        kwargs.set_item("src_network", sn.clone())?;
    }
    // src_tmp is non-Optional on PySecondaryConfig (always
    // resolved by `__new__`); pass it through unconditionally.
    kwargs.set_item("src_tmp", config.src_tmp.clone())?;

    let cls = module(py)?.getattr("RustSecondaryCoordinator")?;
    let args = (
        primary_url,
        config.secondary_id.clone(),
        config.num_workers,
        ram_bytes,
        source_dir.clone(),
        output_dir.clone(),
        task_definition.clone(),
        task_args.clone(),
    );
    let coord = cls.call(args, Some(&kwargs))?;

    // Phase 5B: fire `on_run_start` synchronously under the GIL before
    // entering the secondary's coordination loop. The secondary owns
    // the source/output dirs and `task_args`; failure aborts the run
    // (consumer setup hasn't completed; dispatching would race
    // half-built resources). The secondary holds no `PrimaryHandle`
    // (the handle is the primary's coordinator surface), so the
    // bridge call goes through the positional-only path; legacy and
    // modern task signatures both accept it.
    fire_on_run_start(task_definition, &source_dir, &output_dir, task_args, None)?;

    let run_outcome = coord.call_method0("run");

    // Phase 5B: fire `on_run_end` regardless. Exceptions log and are
    // swallowed; the coord error (if any) is propagated below.
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}
