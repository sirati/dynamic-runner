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
pub(crate) mod run;

mod tests;

#[pyclass(name = "RustSecondaryCoordinator")]
pub(crate) struct PySecondaryCoordinator {
    pub(super) python_executable: PathBuf,
    pub(super) primary_url: String,
    pub(super) secondary_id: String,
    /// Port this secondary's OWN peer-mesh listeners bind (QUIC UDP +
    /// WSS TCP, same number). `None` = OS-picked ephemeral (the
    /// historical behaviour, and the non-SLURM default). Sourced from
    /// the `--secondary-quic-port` CLI flag: under the SLURM wrapper
    /// the port was pre-allocated host-side and recorded in the
    /// late-joiner's `connection_info/<id>.info` file, so the mesh MUST
    /// bind it or the recorded port is dead for every dialing peer.
    pub(super) quic_bind_port: Option<u16>,
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
    /// path did) because pre-staged discovery needs it: the Python
    /// `task.discover_items` call resolves the per-task list but not
    /// the graph metadata, and the Rust core seeds both as a single
    /// mutation batch.
    ///
    /// Captured into the promoted-primary discovery recipe
    /// (`build_promoted_primary_recipe`): a mode-2 SLURM-relocated primary's
    /// `discover_on_promotion` seeds this phase graph alongside the discovered
    /// tasks.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) skip_existing: bool,
    pub(super) estimator: PyMemoryEstimatorBridge,
    /// Held for pre-staged discovery: the wrapper re-acquires the GIL
    /// and invokes `task_definition_py.discover_items(<root>,
    /// task_args_py)` to enumerate the staged corpus. Kept as a
    /// `Py<PyAny>` (not `Bound<'py, _>`) because the wrapper outlives
    /// any single `Python<'py>` lifetime; `bind(py)` re-materialises a
    /// `Bound` at each call site.
    pub(super) task_definition_py: Py<PyAny>,
    /// Held for the same reason as `task_definition_py`: the second
    /// positional argument to `discover_items`. Originates from the
    /// `task_args` Python object passed into the constructor.
    ///
    /// Captured into the promoted-primary discovery recipe
    /// (`build_promoted_primary_recipe`) as the args the relocated primary's
    /// `discover_items` call receives.
    pub(super) task_args_py: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner `SecondaryCoordinator` at `run()`
    /// start. Constructor-only — see the matching field on
    /// `PyPrimaryCoordinator` for the rationale.
    pub(super) peer_lifecycle_listener: Option<Py<PyAny>>,
    /// (#577) The `import_action` field is GONE — gate bodies run in
    /// worker subprocesses dispatched via the normal task-dispatch path.

    /// Optional Python per-(gate,node) satisfied probe callable supplied at
    /// `__init__` (#537). `Some` iff the caller passed
    /// `affine_instance_satisfied=<callable>`; bridged through
    /// [`crate::affine_satisfied_bridge::PyAffineSatisfiedProbe`] and
    /// installed on the inner `SecondaryCoordinator` via
    /// `set_affine_satisfied_probe` at `run()` start. A registered probe lets
    /// the PRODUCING node short-circuit the run-once affine executor (no
    /// worker dispatch, no `QueuedAfterLocalDependency` frames) when it
    /// already holds the gate's product locally. `None` (the default) leaves
    /// the executor with today's behaviour bit-for-bit. Constructor-only.
    pub(super) affine_satisfied_probe: Option<Py<PyAny>>,
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
    /// Rust-side bundle of the secondary's command channel +
    /// reinject-cap cell, shared with every `PyPrimaryHandle` minted
    /// from this coordinator. Mirrors
    /// `PyPrimaryCoordinator::control_plane` exactly — same helper
    /// type, same lifecycle (`new()` at `__init__`, `to_handle()`
    /// from `handle()` pymethod, `take_for_run()` consumed at
    /// `run()` entry).
    ///
    /// The minted handle reaches the SECONDARY's `command_rx` (via
    /// `replace_command_channel` at `run()` start), so a Python
    /// `on_run_start` captures a handle whose `spawn_tasks` /
    /// `fail_permanent` / `reinject_task` /
    /// `update_preferred_secondaries` calls dispatch through THIS
    /// secondary's coordinator. Post-promotion the calls land on
    /// the live `primary_pending` pool / `primary_failed` ledger;
    /// pre-promotion the per-variant handlers either short-circuit
    /// with a typed error or silently skip pool-side mirror steps
    /// (the originator's CRDT broadcast still fires so every
    /// receiver converges).
    pub(super) control_plane: crate::managers::control_plane::PrimaryControlPlane,
    pub(super) completed: u32,
    /// Bytes withheld from the workers cgroup so the secondary process
    /// itself has a protected slice of the container's `memory.max`.
    /// `None` skips nesting entirely (legacy flat layout). See
    /// `dynrunner_manager_distributed::SecondaryConfig::mem_manager_reserved_bytes`
    /// for the full contract. Plumbed in through the
    /// `mem_manager_reserved_bytes` kwarg on `__init__`; forwarded
    /// into the inner `SecondaryConfig` at `run()` entry.
    pub(super) mem_manager_reserved_bytes: Option<u64>,
    /// Operator-supplied `--memprofile` opt-in. Forwarded to the
    /// inner `SecondaryConfig` at `run()` entry via the dedicated
    /// `resolve_secondary_memprofile_dir` helper, which prefers
    /// the operator's `output_dir` (always set) and falls back to
    /// the SLURM wrapper bind-mount when the caller intentionally
    /// supplies no dir. See
    /// `dynrunner_manager_distributed::SecondaryConfig::output_dir`.
    pub(super) memprofile_enabled: bool,
    /// The consumer's run-config SEED — the token sequence the framework
    /// forwards onto a joining / promoted node's command line. Sourced
    /// from the consumer's parsed `args.forwarded_argv`: on a cold-start
    /// secondary the boot argv carries only framework-regenerated flags,
    /// so this seed is usually EMPTY — the REAL value arrives via the
    /// primary's post-welcome `RunConfig` push into the coordinator's
    /// shared handle (#277; the old pre-coordinator mesh-fetch shim is
    /// gone). Threaded at `run()` entry into this secondary's
    /// `SecondaryConfig.forwarded_argv` as the handle's starting value;
    /// the PROMOTED `PrimaryConfig.forwarded_argv` reads the SAME shared
    /// handle (post-push), so a node promoted to primary answers
    /// `RequestRunConfig` identically — no split-brain.
    pub(super) forwarded_argv: Vec<String>,
    /// The consumer's run-config finalize closure (the `args=`/`argv=` path's
    /// deferred reparse). `Some` when the Python dispatcher supplied
    /// `finalize_run_config=`: a callable taking the delivered
    /// `forwarded_argv` and returning the re-parsed argparse namespace, from
    /// which Rust re-runs `build_worker_command_args` per type and swaps the
    /// result into the shared worker-command source the factory reads. `None`
    /// (out-of-tree callers / no deferral needed) makes the finalize a no-op.
    /// Held as a `Py<PyAny>` (re-bound under a fresh GIL scope at fire time)
    /// for the same lifetime reason as `task_definition_py`.
    pub(super) finalize_run_config: Option<Py<PyAny>>,
}
