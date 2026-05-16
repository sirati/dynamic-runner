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
    /// `Some`, the submitter has no local view of the corpus and
    /// the `_dispatch_single_process` helper has handed us an empty
    /// `binaries` list on purpose. The primary's bootstrap
    /// `PromotePrimary` then carries `required_setup=true` so the
    /// chosen secondary runs discovery + ledger-seed on its bind-
    /// mounted source root. Threaded through to `PrimaryConfig`
    /// uniformly with the SLURM / network-primary paths so
    /// `--source-already-staged` works in every multi-computer mode
    /// without per-caller special casing.
    pub(super) source_pre_staged_root: Option<PathBuf>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B). The
    /// distributed in-process pipeline drives a primary; secondaries
    /// don't fire user-visible phase hooks.
    pub(super) task_definition: Py<PyAny>,
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
}
