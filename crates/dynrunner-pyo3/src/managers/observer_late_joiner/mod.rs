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
//! 4. Deserialize the snapshot(s) — a decode failure is FATAL — and
//!    cold-join the standalone
//!    [`dynrunner_manager_distributed::observer::ObserverCoordinator`]
//!    via [`dynrunner_manager_distributed::observer::build_cold_join_observer`],
//!    then drive its single
//!    [`dynrunner_manager_distributed::observer::ObserverCoordinator::run`]
//!    loop until it returns a terminal (`Done` / `Aborted` / `Panik`) or a
//!    strand backstop surfaces an `Err`.
//!
//! # Module boundary
//!
//! This module owns ONLY the dispatcher glue. The bootstrap RPC
//! (`join_running_cluster`), the snapshot install (`cluster_state.restore`),
//! and the whole observation runtime (reporter, failure policies, announcer,
//! panik arm, strand backstops, teardown) all live in the protocol /
//! manager-distributed crates that own those concerns; this wrapper just
//! reads the seed, runs the bootstrap RPC, and hands the decoded snapshots to
//! the cold-join factory.
//!
//! # Why a dedicated pyclass
//!
//! A late-joiner observer has fundamentally different inputs than a normal
//! secondary:
//!   - no `primary_url` (the observer never speaks primary protocol),
//!   - no `secondary_id` from argv (the observer picks its own id;
//!     it's a peer-mesh participant, not a SLURM-spawned worker),
//!   - no worker count / scheduler / estimator (a zero-authority observer
//!     runs no workers and holds no scheduler),
//!   - peer-info-dir replaces the primary's `PeerInfo` broadcast
//!     as the initial seed source.
//!
//! A sibling pyclass with its own `new` / `run` shape keeps the observer
//! dispatch visibly separate from the secondary's, constructing the
//! standalone [`dynrunner_manager_distributed::observer::ObserverCoordinator`]
//! (the ONE observer impl) rather than a secondary in an observer mode.
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

mod helpers;
mod new;
mod run;

/// Late-joining observer dispatcher.
///
/// Construction stashes the caller's configuration knobs; the actual
/// peer-join + snapshot restore + observation loop runs inside
/// [`PyObserverLateJoiner::run`] (under `py.detach`), which cold-joins the
/// standalone
/// [`dynrunner_manager_distributed::observer::ObserverCoordinator`]. A
/// zero-authority observer needs no estimator / scheduler / worker config —
/// it holds the replicated CRDT and narrates the run from it.
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
    pub(super) distributed_config: DistributedConfig,
    /// Static set of `holdings` this observer advertises to the
    /// cluster (e.g. asm-dataset-nix passes the local Nix-store
    /// outpaths it can serve). Handed to the cold-join factory's
    /// announcer attach at `run()` time so the bootstrap restore's
    /// `PrimaryChanged` apply triggers a `PeerResourceHoldingsUpdated`
    /// broadcast carrying the cluster's current `primary_epoch`.
    /// Defaults to empty when the kwarg is omitted — a consumer that
    /// doesn't host any resources simply never announces anything, which
    /// is the correct shape for a pure observer.
    ///
    /// Stored as `HashSet` to deduplicate at the boundary; the
    /// announcer's `build_payload` sorts before send so the wire
    /// order is stable regardless of insertion sequence on the
    /// Python side.
    pub(super) holdings: std::collections::HashSet<String>,
    /// Panik-watcher paths — same shape as on
    /// `PySecondaryCoordinator`. An observer that runs on its own
    /// host can be told to bail by a per-host panik file; observers
    /// share the same shared-network sentinel as the rest of the
    /// cluster, so an operator can trip every node at once.
    pub(super) panik_watcher_paths: Vec<PathBuf>,
    pub(super) panik_watcher_poll_interval_secs: f64,
    pub(super) completed: u32,
}

/// Free-function entry point: construct the late-joiner pyclass and
/// drive its `run()` under the GIL. Mirrors `run_secondary`'s shape
/// so the Python dispatcher in `run.py` follows the same
/// build-then-call rhythm across runner modes.
#[pyfunction]
#[pyo3(signature = (
    peer_info_dir,
    observer_id = None,
    distributed_config = None,
    holdings = None,
    panik_watcher_paths = None,
    panik_watcher_poll_interval_secs = 10.0,
))]
pub(crate) fn run_observer_late_joiner<'py>(
    py: Python<'py>,
    peer_info_dir: PathBuf,
    observer_id: Option<String>,
    distributed_config: Option<DistributedConfig>,
    holdings: Option<Vec<String>>,
    panik_watcher_paths: Option<Vec<PathBuf>>,
    panik_watcher_poll_interval_secs: f64,
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
    if let Some(paths) = panik_watcher_paths.as_ref() {
        kwargs.set_item("panik_watcher_paths", paths.clone())?;
    }
    kwargs.set_item(
        "panik_watcher_poll_interval_secs",
        panik_watcher_poll_interval_secs,
    )?;
    // Resolve the legacy class via the package, mirroring `run_secondary`
    // / `run_distributed`'s "build-via-Python-module-attribute" pattern
    // so the wiring stays uniform across runner modes.
    let module = py.import("dynamic_runner")?;
    let cls = module.getattr("RustObserverLateJoiner")?;
    let args = (peer_info_dir,);
    let observer = cls.call(args, Some(&kwargs))?;
    observer.call_method0("run")?;

    let dict = PyDict::new(py);
    dict.set_item("completed", observer.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}
