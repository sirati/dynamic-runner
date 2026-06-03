//! `RustPrimaryCoordinator` PyO3 wrapper — owns the inner
//! `PrimaryCoordinator` plus the constructor-stashed Python handles
//! (peer-lifecycle / task-completed / fulfillability-matcher
//! listeners) that get registered on the inner coordinator at
//! `run()` start.
//!
//! Split:
//!   - This file owns the pyclass struct definition + the Rust-only
//!     `set_slurm_job_manager_from_rust` impl (used by the SLURM
//!     pipeline; not exposed to Python).
//!   - [`new`] holds the constructor + small pymethods (`handle()`,
//!     `uses_file_based_items`, `queue_initial_staging`).
//!   - [`run`] holds `run()` + the `completed` / `failed` / `stranded`
//!     getters. `run()` itself is ~450 lines of one detached-tokio
//!     bootstrap with extensive in-line GIL-discipline rationale; the
//!     cohesive-concern boundary on it matches the secondary
//!     coordinator's `run()` (see managers/secondary/mod.rs for the
//!     same exception).

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use dynrunner_core::PhaseId;
use dynrunner_manager_distributed::StagingEntry;

use crate::config::distributed::DistributedConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::estimator::PyMemoryEstimatorBridge;

mod new;
mod run;

#[pyclass(name = "RustPrimaryCoordinator")]
pub(crate) struct PyPrimaryCoordinator {
    pub(super) num_secondaries: u32,
    pub(super) estimator: PyMemoryEstimatorBridge,
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) spawn_secondary: Py<PyAny>,
    pub(super) distributed_config: DistributedConfig,
    /// Optional caller-supplied bind port for the network server.
    /// When `Some`, the primary binds exactly this port; this is what
    /// the SLURM packaging path needs because it sets up an SSH `-R`
    /// forward to a port it picked itself, and the Rust side has to
    /// honour the same number end-to-end. When `None`, we fall back
    /// to a temp-listener pre-pick + drop + re-bind dance (legacy
    /// behaviour, retained for in-process / local-multi-computer
    /// callers that don't tunnel and don't care which port lands).
    pub(super) listen_port: Option<u16>,
    pub(super) completed: u32,
    pub(super) failed: u32,
    /// Tasks that exited the inner run loop without a recorded
    /// outcome (`total - completed - failed`). Mirrors
    /// `PrimaryCoordinator::stranded_count` after `run()` returns; the
    /// `stranded` PyO3 getter exposes it so consumers (Python `run.py`,
    /// SLURM pipeline) can include the per-category count in their
    /// "Completed: / Failed: / Stranded:" output and ops scripts can
    /// distinguish "everything ran but some failed" from "the cluster
    /// collapsed before all tasks even dispatched".
    pub(super) stranded: u32,
    // Pre-`run()` queue of StageFile notifications. The pipeline calls
    // `notify_stage_file(...)` on this pyclass as part of packaging
    // (before `run()` ever starts the coordinator). On `run()`, this
    // list is moved into `PrimaryCoordinator::queue_stage_file` so the
    // coordinator flushes them once secondary connections are up.
    /// Tuple shape: `(secondary_id, file_hash, content_hash, src_path, dest_path)`.
    /// `file_hash` is the task identifier for cache lookup;
    /// `content_hash` is the SHA256 of the file contents that the
    /// staging integrity check will verify against.
    pub(super) pending_stage_files: Vec<StagingEntry>,
    /// Pre-staged-source mode (`--source-already-staged` on the
    /// pipeline). When `Some`, this is the gateway-side host path
    /// the wrapper bind-mounts into each secondary container at
    /// `src_network`. The primary uses it to compute the wire-side
    /// `local_path` (TaskInfo.path with this prefix stripped) so
    /// secondary's `src_network.join(<local_path>)` resolves to the
    /// in-container bind-mount path. Propagated as a bool to
    /// secondaries via `InitialAssignment.pre_staged_mode` so
    /// dispatch skips the hash machinery.
    pub(super) source_pre_staged_root: Option<std::path::PathBuf>,
    /// Local source-tree root for the staging walk. Threaded into
    /// `PrimaryConfig.source_dir` so the inner coordinator owns a
    /// root for the content-hash + per-secondary fan-out without
    /// each caller re-implementing the staging orchestration.
    /// SLURM and network-primary callers both pass it; `None` is
    /// the right default for pre-staged-source mode,
    /// `uses_file_based_items=False`, and remote-only primaries
    /// that never read the source from this filesystem.
    pub(super) source_dir: Option<std::path::PathBuf>,
    /// Rust-side bundle of the command channel + reinject-cap cell
    /// shared with every `PyPrimaryHandle` minted from this
    /// coordinator. Single concern split out into
    /// `crate::managers::control_plane` so the init/handle/run-take
    /// sequence is owned in one place rather than re-implemented on
    /// each primary-hosting manager. See that module's doc for the
    /// lifecycle contract.
    pub(super) control_plane: crate::managers::control_plane::PrimaryControlPlane,
    /// Whether dispatched task items back to real files. Read at
    /// construction from `TaskDefinition.uses_file_based_items`
    /// (defaults to True). Propagated to secondaries via
    /// `InitialAssignment.uses_file_based_items` so dispatch skips
    /// extraction-cache resolution and treats `local_path` as an
    /// opaque worker identifier when False.
    pub(super) uses_file_based_items: bool,
    /// Per-type concurrency caps, harvested from each
    /// `TaskTypeSpec.max_concurrent` at construction. Empty when no
    /// type declares a cap. Forwarded to `PrimaryConfig` so the
    /// scheduler refuses to dispatch beyond the cap.
    pub(super) max_concurrent_per_type: std::collections::HashMap<dynrunner_core::TypeId, u32>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B).
    pub(super) task_definition: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// the object is bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner `PrimaryCoordinator` at `run()` start.
    /// Constructor-only — no setter — because the manager-distributed
    /// `register_lifecycle_listener` API also requires
    /// pre-`run()` registration (the listener vector is
    /// `mem::take`-d into the spawned dispatcher).
    pub(super) peer_lifecycle_listener: Option<Py<PyAny>>,

    /// Optional Python task-completion listener supplied at
    /// `__init__`. `Some` iff the caller passed
    /// `task_completed_listener=<callable>`; the callable is bridged
    /// through `crate::task_completed_bridge::PyTaskCompletedListener`
    /// and registered on the inner `PrimaryCoordinator` at `run()`
    /// start via `register_task_completed_listener`. Constructor-only
    /// — same pre-`run()` registration contract as the peer-lifecycle
    /// listener.
    pub(super) task_completed_listener: Option<Py<PyAny>>,

    /// Optional Python fulfillability matcher supplied at `__init__`.
    /// `Some` iff the caller passed `fulfillability_matcher=<callable>`;
    /// the object is bridged through
    /// `crate::fulfillability_matcher_bridge::PyFulfillabilityMatcher`
    /// and installed on the inner `PrimaryCoordinator` at `run()`
    /// start. Constructor-only — no setter — because the
    /// manager-distributed `set_fulfillability_matcher` API also
    /// requires pre-`run()` registration (the matcher trait object
    /// is owned by `self` and read in the operational `select!`).
    pub(super) fulfillability_matcher: Option<Py<PyAny>>,

    /// Optional opaque handle to the deployment-mode job manager,
    /// installed by the SLURM pipeline after `run_preparation` returns
    /// and BEFORE `run()` enters. Stored as `Arc<dyn Any + Send + Sync>`
    /// so this crate stays decoupled from any specific batch-system
    /// crate; the respawn caller (inside the manager-distributed
    /// operational loop) downcasts to the concrete handle.
    ///
    /// Threaded into the inner `PrimaryCoordinator` via
    /// `set_slurm_job_manager` at `run()` start. `None` for the
    /// in-process / network-primary callers that don't drive a SLURM
    /// submit-loop.
    pub(super) slurm_job_manager: Option<Arc<dyn Any + Send + Sync>>,

    /// Scheduler tuning forwarded into every `ResourceStealingScheduler`
    /// the coordinator constructs at `run()` start. Sourced from the
    /// caller's `scheduler_config` kwarg (defaulting via
    /// `SchedulerConfig::default()`). The OOM-preempt safety margin
    /// (`cgroup_safety_margin`) and pressure threshold ride through
    /// here, mirroring the `PyLocalManager` path so every Rust
    /// manager-hosting pyclass exposes the same tuning surface.
    pub(super) scheduler_config: SchedulerConfig,

    /// Respawn policy supplied by the Python caller. `Disabled`
    /// (the default) leaves the inner coordinator's respawn pipeline
    /// unwired — no listener registered, no JoinSet arm reachable.
    /// `OnSecondaryDeath { budget }` translates to an
    /// `enable_respawn` call at `run()` entry, paired with the
    /// spawner stored below.
    pub(super) respawn_policy: crate::config::respawn::PyRespawnPolicy,

    /// Secondary spawner adapter (currently only
    /// [`PyMultiProcessSpawner`]; the SLURM equivalent will land
    /// behind the same `as_arc` boundary). `None` when no spawner
    /// was supplied at construction; combined with `respawn_policy
    /// = Disabled` this means the respawn pipeline is fully off.
    /// Held as `Py<PyAny>` so the underlying pyclass refcount stays
    /// tied to the coordinator's lifetime; the actual
    /// `Arc<dyn SecondarySpawner>` is extracted via `as_arc` at
    /// `run()` entry.
    pub(super) respawn_spawner: Option<Py<PyAny>>,

    /// Panik-watcher paths — same shape as on `PySecondaryCoordinator`.
    /// Empty means "no watcher" — the operator passed no
    /// `--panik-file` flags. Forwarded into
    /// [`dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig::paths`]
    /// at `run()` entry.
    pub(super) panik_watcher_paths: Vec<std::path::PathBuf>,
    /// Poll cadence (seconds) for the panik watcher. Default 10.0
    /// per the 2026-05-17 design thread.
    pub(super) panik_watcher_poll_interval_secs: f64,
}

// Rust-only surface for the SLURM-pipeline orchestrator. Not exposed
// to Python because the parked handle is the in-process Rust
// `SlurmJobManager` instance — it travels Rust-to-Rust across the
// pipeline → coordinator boundary, never through Python identity.
impl PyPrimaryCoordinator {
    /// Park the deployment-mode job-manager handle so the inner
    /// `PrimaryCoordinator` sees it at `run()` start. Called by
    /// `slurm::pipeline::drive_rust_primary` after `run_preparation`
    /// returns and BEFORE `run()` enters.
    ///
    /// Single concern: relay the opaque handle into the
    /// manager-distributed coordinator. The PyO3 wrapper holds it
    /// between construction and `run()` because the inner
    /// `PrimaryCoordinator` does not exist yet at the call site —
    /// it's built inside the detached tokio runtime.
    pub(crate) fn set_slurm_job_manager_from_rust(&mut self, jm: Arc<dyn Any + Send + Sync>) {
        self.slurm_job_manager = Some(jm);
    }
}
