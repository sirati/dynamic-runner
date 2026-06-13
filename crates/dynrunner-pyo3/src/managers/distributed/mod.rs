//! `RustDistributedManager` PyO3 wrapper — drives the in-process
//! distributed pipeline (one primary + N secondaries connected over
//! channel transports) from a single pyclass.
//!
//! Split:
//!   - This file owns the pyclass struct definition + field visibility.
//!   - [`new`] is the constructor + `handle()` factory pymethods block.
//!   - [`run`] is the run() + completed/failed/stranded getter
//!     pymethods block; the run() body is ~480 lines because it
//!     drives a single py.detach closure mirroring
//!     `PyPrimaryCoordinator::run` (same captured-locals + cancel-
//!     safety constraints).
//!   - [`tests`] is the pre-run handle-factory contract suite (gated
//!     on the `test-with-python` feature like the primary suite).

use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{PhaseId, ResourceMap, TypeId};

use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::task_def::TypeRegistry;

mod new;
mod run;

mod tests;

#[pyclass(name = "RustDistributedManager")]
pub(crate) struct PyDistributedManager {
    pub(super) python_executable: PathBuf,
    pub(super) num_secondaries: u32,
    pub(super) num_workers_per_secondary: u32,
    pub(super) max_resources_per_secondary: ResourceMap,
    pub(super) source_dir: PathBuf,
    pub(super) output_dir: PathBuf,
    /// Per-run log-mount root passed to
    /// `LogPathConfig::resolve_log_dir`. Resolved by
    /// `LoadedTaskDefinition::from_python` from the caller-supplied
    /// `log_dir` kwarg, falling back to `output_dir` for single-host
    /// deployments where the two roots coincide. Threaded into the
    /// `run()` loop's per-secondary log-dir resolution so logs land
    /// under the dedicated log-mount tree on SLURM deployments
    /// (`/app/log-network`) rather than the output-mount tree
    /// (`/app/out-network`).
    pub(super) log_path: PathBuf,
    pub(super) log_paths: LogPathConfig,
    pub(super) worker_spec: Option<WorkerSpec>,
    pub(super) distributed_config: DistributedConfig,
    pub(super) types: TypeRegistry,
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Phases declared `PhaseSpec.may_be_empty` — registered on the
    /// in-process primary before `run()` (the empty-drain opt-out).
    pub(super) phase_may_be_empty: Vec<PhaseId>,
    pub(super) skip_existing: bool,
    pub(super) uses_file_based_items: bool,
    pub(super) max_concurrent_per_type: HashMap<TypeId, u32>,
    pub(super) estimator: PyMemoryEstimatorBridge,
    pub(super) completed: u32,
    pub(super) failed: u32,
    /// Tasks that exited the inner run loop without a recorded
    /// outcome (`total - completed - failed`). Mirrors the same
    /// counter on `PyPrimaryCoordinator`; surfaced via the `stranded`
    /// PyO3 getter so the Python in-process distributed entrypoint
    /// can include it in the result dict.
    pub(super) stranded: u32,
    /// Pre-staged-source mode (`--source-already-staged`) signal.
    /// Mirrors `PyPrimaryCoordinator.source_pre_staged_root`: when
    /// `Some`, the corpus is already staged on the host fs and the
    /// submitter passed no locally-discovered binaries. The in-process
    /// run then constructs `SeedSource::RelocatedSeed` (phase graph +
    /// `DiscoveryDebt=Owed`, no tasks). Under mesh-always the setup peer
    /// RELOCATES (uniform with SLURM); the relocate TARGET (a promoted
    /// in-process secondary) carries the consumer's discovery policy on its
    /// promote recipe and inherits the `Owed` marker via its snapshot, so its
    /// `discover_on_promotion` driver runs `task.discover_items` on the shared
    /// host fs and seeds the tasks (the driver gates on the `Owed` marker, not
    /// on relocation). Also threaded into `PrimaryConfig` as the staging root.
    /// The cold path (`None`) discovers the corpus upfront in Python and
    /// cold-seeds it (`DiscoveryDebt` stays `Undeclared`).
    pub(super) source_pre_staged_root: Option<PathBuf>,
    /// Framework file-staging selector (`--stage-via-setup-tasks`, #489 P3/P4):
    /// `false` (default) → the OLD StageFile path; `true` → the setup-task
    /// model (per-file pre-succeeded setup tasks + `TaskDep` gating, the
    /// #488-free path). Threaded into `PrimaryConfig.staging_strategy` for BOTH
    /// the bootstrap in-process primary AND every per-secondary promote recipe
    /// (the relocate target, where the mode-2 discovery seeds the setup tasks),
    /// so the flag is honored on whichever peer holds the primary.
    pub(super) stage_via_setup_tasks: bool,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B). The
    /// distributed in-process pipeline drives a primary; secondaries
    /// don't fire user-visible phase hooks.
    pub(super) task_definition: Py<PyAny>,
    /// The Python `task_args` namespace, held for the in-process
    /// `--source-already-staged` discovery policy: the local primary owes
    /// discovery (`DiscoveryDebt=Owed`) and runs `task.discover_items(<root>,
    /// task_args)` itself via `discover_on_promotion`. Second positional arg
    /// to `discover_items`. Unused on the cold in-process path (the corpus is
    /// discovered upfront in Python and cold-seeded).
    pub(super) task_args: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the in-process primary at `run()` start. The
    /// in-process secondaries do NOT get the listener — the manager
    /// pyclass represents one cluster's worth of events, and the
    /// primary's `cluster_state` apply path is the canonical
    /// emitter (the per-secondary mirrors fire the same events from
    /// their own apply paths; routing them all to the same listener
    /// would deliver N+1 copies of each peer membership change).
    /// Constructor-only — see the matching field on
    /// `PyPrimaryCoordinator` for the rationale.
    pub(super) peer_lifecycle_listener: Option<Py<PyAny>>,

    /// Optional Python task-completion listener supplied at
    /// `__init__`. Same shape + single-source-of-truth rationale as
    /// `peer_lifecycle_listener`: registered on the in-process
    /// primary only; per-secondary mirrors fire the same events from
    /// their own apply paths but routing all to the same listener
    /// would deliver N+1 copies of each terminal task transition.
    pub(super) task_completed_listener: Option<Py<PyAny>>,

    /// Rust-side bundle of the command channel + reinject-cap cell
    /// shared with every `PyPrimaryHandle` minted from this manager.
    /// Single concern split out into
    /// `crate::managers::control_plane` so the init/handle/run-take
    /// sequence is owned in one place rather than re-implemented on
    /// each primary-hosting manager. See that module's doc for the
    /// lifecycle contract.
    pub(super) control_plane: crate::managers::control_plane::PrimaryControlPlane,

    /// Scheduler tuning shared by both the in-process primary AND every
    /// in-process secondary the manager spawns. The same
    /// `cgroup_safety_margin` / `pressure_threshold` knobs (carried
    /// here via `SchedulerConfig`) drive both kill branches on every
    /// node; sharing a single value across the cluster mirrors how
    /// SLURM operators tune the framework: one CLI flag pair, applied
    /// uniformly to whichever Rust coordinator is the local one.
    pub(super) scheduler_config: SchedulerConfig,

    /// Panik-watcher paths shared by both the in-process primary
    /// AND every in-process secondary. Same shape and rationale as
    /// `scheduler_config`: one operator surface, applied uniformly to
    /// whichever Rust coordinator runs on the local node. Empty
    /// vector disables the watcher (a never-firing oneshot receiver
    /// is what `spawn_panik_watcher` returns).
    pub(super) panik_watcher_paths: Vec<PathBuf>,
    /// Poll cadence (seconds) for the panik watcher.
    pub(super) panik_watcher_poll_interval_secs: f64,
    /// Operator-supplied `--memprofile` opt-in. Forwarded from the
    /// `secondary_template`'s `memprofile_enabled` field by the
    /// Python `run_distributed` bridge. Combined with
    /// `self.output_dir` (always set) at `run()` entry via the
    /// shared resolver helper so the in-process secondaries
    /// receive the same memprofile output dir as their
    /// out-of-process counterparts.
    pub(super) memprofile_enabled: bool,
    /// The consumer's run-config (the operator's `args.forwarded_argv`),
    /// shared by both the in-process primary AND every in-process secondary
    /// the manager spawns. Every node shares the submitter's argv directly
    /// (one process, no cold-start mesh fetch), so the same byte-identical
    /// copy seeds each node's node-local `forwarded_argv` — the
    /// `RequestRunConfig` responder then serves it uniformly regardless of
    /// which node a peer happens to query.
    pub(super) forwarded_argv: Vec<String>,
}
