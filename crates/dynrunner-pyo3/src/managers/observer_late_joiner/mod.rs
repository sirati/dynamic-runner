//! Late-joiner observer dispatcher: join a running cluster via the
//! peer mesh, restore its snapshot, then observe live broadcasts.
//!
//! # Concern
//!
//! Single PyO3 entry point for the
//! `--observer-join-from-peer-info-dir <path>` CLI flag. The flow is
//! one straight line:
//!
//! 1. Read peer-info `*.info` files via
//!    [`dynrunner_slurm::read_peer_info_dir_v2`] and convert the v2
//!    records into [`PeerConnectionInfo`] seed entries
//!    (`secondary_id` + `cert` + addresses + `quic_port` +
//!    `is_observer`).
//! 2. Start a real [`PeerNetwork`] under our observer-id (the CN baked
//!    into the QUIC cert; peer dialers validate against it).
//! 3. Drive [`PeerTransport::join_running_cluster`] with the seed and
//!    the shared default budget; the trait's default impl dials,
//!    sends `RequestClusterSnapshot` to the first reachable seed peer,
//!    and returns the serialized snapshot JSON.
//! 4. Deserialize the snapshot, construct a [`SecondaryCoordinator`]
//!    paired with [`NoPrimaryTransport`] (peer-only mesh participant;
//!    see `no_primary.rs` for the design rationale), call
//!    [`SecondaryCoordinator::restore_from_snapshot_and_skip_setup`]
//!    to install the snapshot AND latch `setup_phase_completed=true`,
//!    then run [`SecondaryCoordinator::run_until_setup_or_done`]
//!    until it returns `RunOutcome::Done`.
//!
//! # Module boundary
//!
//! This module owns ONLY the dispatcher glue. The bootstrap RPC
//! (`join_running_cluster`), the snapshot install
//! (`cluster_state.restore`), and the setup-skip latch
//! (`setup_phase_completed=true`) all live in the protocol /
//! manager-distributed crates that are the canonical owners of those
//! concerns; this module just sequences the existing primitives.
//!
//! # Why a dedicated pyclass (not a flag on `RustSecondaryCoordinator`)
//!
//! A late-joiner has fundamentally different inputs than a normal
//! secondary:
//!   - no `primary_url` (the observer never speaks primary protocol),
//!   - no `secondary_id` from argv (the observer picks its own id;
//!     it's a peer-mesh participant, not a SLURM-spawned worker),
//!   - no worker count (`num_workers=0` is structural, not a knob),
//!   - peer-info-dir replaces the primary's `PeerInfo` broadcast
//!     as the initial seed source.
//!
//! Cramming these into `PySecondaryCoordinator` would require
//! `Option<>`-everywhere on construction args + a "late-joiner mode"
//! `if` cascade across `run()`. A sibling pyclass with its own
//! `new` / `run` shape keeps the two concerns visibly separate while
//! still sharing the load-bearing run loop (one
//! `secondary.run_until_setup_or_done(&mut factory)` call serves both
//! flavours; the setup-skip latch handles all the conditional state).
//!
//! # File split
//!
//! `mod.rs` owns the pyclass struct + the public `run_observer_late_joiner`
//! pyfunction dispatcher. `new` is the constructor pymethods block.
//! `run` is the run() + completed getter pymethods block. `helpers`
//! carries `map_read_dir_error` + `records_to_seed` + their tests.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::config::distributed::DistributedConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::task_def::LoadedTopology;

mod helpers;
mod new;
mod run;

/// Late-joining observer dispatcher.
///
/// Construction parses the task_definition (to recover the resource
/// estimator and phase-deps the run loop needs) and stashes the
/// caller's configuration knobs. The actual peer-join + snapshot
/// restore + observation loop runs inside [`PyObserverLateJoiner::run`]
/// (under `py.detach`).
#[pyclass(name = "RustObserverLateJoiner")]
pub(crate) struct PyObserverLateJoiner {
    /// Logical peer-id this observer registers under in the mesh.
    /// Also the CN baked into the observer's QUIC cert. Defaulted at
    /// construction to `"observer-<random>"` if the caller doesn't
    /// supply one; the late-joiner CLI flow auto-generates this
    /// (the operator running `--observer-join-from-peer-info-dir`
    /// rarely cares about the id).
    pub(super) observer_id: String,
    /// Directory holding the SLURM wrapper's `<secondary_id>.info`
    /// files. Read once at the start of `run()` to build the seed
    /// list for [`PeerTransport::join_running_cluster`].
    pub(super) peer_info_dir: PathBuf,
    /// Held for the estimator / phase-deps the SecondaryCoordinator
    /// needs to drive its run loop. We deliberately use the lighter
    /// [`LoadedTopology`] (no `build_worker_command_args` invocation,
    /// no path-resolution side effects) rather than the heavier
    /// [`crate::task_def::LoadedTaskDefinition`] because the observer
    /// has `num_workers = 0` — no worker subprocess will ever spawn,
    /// so the per-type cmd_args are useless and asking Python to
    /// build them would be a wasted excursion.
    pub(super) topology: LoadedTopology,
    pub(super) distributed_config: DistributedConfig,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner observer-mode `SecondaryCoordinator`
    /// at `run()` start. Constructor-only; see the matching field
    /// on `PyPrimaryCoordinator` for the rationale.
    pub(super) peer_lifecycle_listener: Option<Py<PyAny>>,
    /// Static set of `holdings` this observer advertises to the
    /// cluster (e.g. asm-dataset-nix passes the local Nix-store
    /// outpaths it can serve). Drained into the observer-side
    /// announcer at `run()` time so a `PrimaryChanged` mutation
    /// triggers a `PeerResourceHoldingsUpdated` broadcast carrying
    /// the cluster's current `primary_epoch`. Defaults to empty
    /// when the kwarg is omitted — a consumer that doesn't host
    /// any resources simply never announces anything, which is the
    /// correct shape for a pure observer.
    ///
    /// Stored as `HashSet` to deduplicate at the boundary; the
    /// announcer's `build_payload` sorts before send so the wire
    /// order is stable regardless of insertion sequence on the
    /// Python side.
    pub(super) holdings: std::collections::HashSet<String>,
    /// Scheduler tuning forwarded into the observer-mode
    /// `SecondaryCoordinator`'s inner `ResourceStealingScheduler`. The
    /// observer never spawns workers (`num_workers = 0`), but the
    /// coordinator still constructs the scheduler at run start — keep
    /// the tuning surface consistent with the other Rust manager-hosting
    /// pyclasses so a single CLI flag pair drives every node uniformly.
    pub(super) scheduler_config: SchedulerConfig,
    pub(super) completed: u32,
}

/// Free-function entry point: construct the late-joiner pyclass and
/// drive its `run()` under the GIL. Mirrors `run_secondary`'s shape
/// so the Python dispatcher in `run.py` follows the same
/// build-then-call rhythm across runner modes.
#[pyfunction]
#[pyo3(signature = (
    peer_info_dir,
    task_definition,
    observer_id = None,
    distributed_config = None,
    holdings = None,
    scheduler_config = None,
))]
pub(crate) fn run_observer_late_joiner<'py>(
    py: Python<'py>,
    peer_info_dir: PathBuf,
    task_definition: &Bound<'py, PyAny>,
    observer_id: Option<String>,
    distributed_config: Option<DistributedConfig>,
    holdings: Option<Vec<String>>,
    scheduler_config: Option<SchedulerConfig>,
) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    if let Some(id) = observer_id.as_ref() {
        kwargs.set_item("observer_id", id)?;
    }
    if let Some(dc) = distributed_config.as_ref() {
        kwargs.set_item("distributed_config", dc.clone())?;
    }
    if let Some(h) = holdings.as_ref() {
        kwargs.set_item("holdings", h.clone())?;
    }
    if let Some(sc) = scheduler_config.as_ref() {
        kwargs.set_item("scheduler_config", sc.clone())?;
    }
    // Resolve the legacy class via the package, mirroring `run_secondary`
    // / `run_distributed`'s "build-via-Python-module-attribute" pattern
    // so the wiring stays uniform across runner modes.
    let module = py.import("dynamic_runner")?;
    let cls = module.getattr("RustObserverLateJoiner")?;
    let args = (peer_info_dir, task_definition.clone());
    let observer = cls.call(args, Some(&kwargs))?;
    observer.call_method0("run")?;

    let dict = PyDict::new(py);
    dict.set_item("completed", observer.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}
