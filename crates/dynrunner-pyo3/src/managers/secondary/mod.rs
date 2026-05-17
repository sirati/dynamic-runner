//! `RustSecondaryCoordinator` PyO3 wrapper — owns the inner
//! `SecondaryCoordinator` and the persisted Python handles
//! (`task_definition_py` / `task_args_py`) needed for the
//! setup-promote yield path.
//!
//! Split:
//!   - This file owns the pyclass struct definition + field visibility.
//!   - [`new`] is the `#[pymethods] impl` block holding the constructor.
//!   - [`run`] is the `#[pymethods] impl` block holding `run()` + the
//!     `completed` getter. The `run()` body is ~500 lines because it
//!     drives a single `py.detach` closure containing the entire tokio
//!     bootstrap + the setup-promote outer loop; splitting that closure
//!     across helpers would require threading 20+ captured locals as
//!     parameters and changing the cancel-safety boundary (the
//!     `select!` arms are documented as cancel-safe ONLY at the
//!     existing closure scope). The closure stays a single unit;
//!     run.rs is at 528 LoC by design.

use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{PhaseId, ResourceMap};

use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::task_def::TypeRegistry;

mod new;
mod run;

#[pyclass(name = "RustSecondaryCoordinator")]
pub(crate) struct PySecondaryCoordinator {
    pub(super) python_executable: PathBuf,
    pub(super) primary_url: String,
    pub(super) secondary_id: String,
    pub(super) num_workers: u32,
    pub(super) max_resources: ResourceMap,
    pub(super) source_dir: PathBuf,
    pub(super) output_dir: PathBuf,
    pub(super) log_dir: PathBuf,
    pub(super) log_paths: LogPathConfig,
    pub(super) worker_spec: Option<WorkerSpec>,
    pub(super) distributed_config: DistributedConfig,
    /// Shared-drive directory where the primary stages source binaries.
    /// `None` for single-node modes (file-ready resolution falls back
    /// to absolute paths from the primary's view).
    pub(super) src_network: Option<PathBuf>,
    /// Per-secondary scratch directory where StageFile copies land.
    /// `None` falls back to a system tempdir under
    /// `db_secondary_<id>` (the historical default).
    pub(super) src_tmp: Option<PathBuf>,
    pub(super) types: TypeRegistry,
    /// Phase dependency graph extracted from
    /// `LoadedTaskDefinition::from_python`. Retained on the wrapper
    /// (rather than left to drop after construction like the legacy
    /// path did) because the setup-promote yield needs it: the Python
    /// `task.discover_items` call resolves the per-task list but not
    /// the graph metadata, and the Rust core seeds both as a single
    /// mutation batch via `ingest_setup_discovery`.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) skip_existing: bool,
    pub(super) estimator: PyMemoryEstimatorBridge,
    /// Held for the setup-promote outer loop. When the Rust core
    /// signals `RunOutcome::SetupPending`, the wrapper re-acquires the
    /// GIL and invokes `task_definition_py.discover_items(<root>,
    /// task_args_py)` to enumerate the staged corpus. Kept as a
    /// `Py<PyAny>` (not `Bound<'py, _>`) because the wrapper outlives
    /// any single `Python<'py>` lifetime; `bind(py)` re-materialises a
    /// `Bound` at each call site.
    pub(super) task_definition_py: Py<PyAny>,
    /// Held for the same reason as `task_definition_py`: the second
    /// positional argument to `discover_items`. Originates from the
    /// `task_args` Python object passed into the constructor.
    pub(super) task_args_py: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner `SecondaryCoordinator` at `run()`
    /// start. Constructor-only — see the matching field on
    /// `PyPrimaryCoordinator` for the rationale.
    pub(super) peer_lifecycle_listener: Option<Py<PyAny>>,
    /// Scheduler tuning forwarded into the `ResourceStealingScheduler`
    /// the coordinator constructs at `run()` start. Carries the
    /// `cgroup_safety_margin` / `pressure_threshold` knobs so the
    /// secondary's userland OOM-preempt fires before the kernel's
    /// cgroup-OOM (mirrors the `PyLocalManager` / `PyPrimaryCoordinator`
    /// surface so every Rust manager-hosting pyclass exposes the same
    /// tuning shape).
    pub(super) scheduler_config: SchedulerConfig,
    /// Filesystem paths the operator-initiated panik watcher polls.
    /// Empty means "no watcher" — `spawn_panik_watcher` returns a
    /// never-firing receiver and the coordinator's panik arm never
    /// hits. Forwarded into
    /// [`dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig::paths`]
    /// verbatim; resolving consumer-specific filenames (e.g.
    /// `/tmp/<consumer>.panik`) is the Python caller's concern.
    pub(super) panik_watcher_paths: Vec<PathBuf>,
    /// Poll cadence (seconds) for the panik watcher. Default 10.0
    /// per the 2026-05-17 design thread.
    pub(super) panik_watcher_poll_interval_secs: f64,
    pub(super) completed: u32,
}
