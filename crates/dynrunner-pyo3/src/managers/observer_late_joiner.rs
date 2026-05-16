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

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_manager_distributed::{
    cluster_state::ClusterStateSnapshot, PeerCertInfo, RunOutcome, SecondaryConfig,
    SecondaryCoordinator,
};
use dynrunner_protocol_primary_secondary::{
    PeerConnectionInfo, PeerTransport, DEFAULT_JOIN_TIMEOUT,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_slurm::{read_peer_info_dir_v2, PeerInfoReadDirError, PeerInfoRecord};
use dynrunner_transport_quic::{NoPrimaryTransport, PeerNetwork};

use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::identifier::RunnerIdentifier;
use crate::network::{detect_ipv4, detect_ipv6, gethostname};
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::LoadedTopology;

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
    observer_id: String,
    /// Directory holding the SLURM wrapper's `<secondary_id>.info`
    /// files. Read once at the start of `run()` to build the seed
    /// list for [`PeerTransport::join_running_cluster`].
    peer_info_dir: PathBuf,
    /// Held for the estimator / phase-deps the SecondaryCoordinator
    /// needs to drive its run loop. We deliberately use the lighter
    /// [`LoadedTopology`] (no `build_worker_command_args` invocation,
    /// no path-resolution side effects) rather than the heavier
    /// [`crate::task_def::LoadedTaskDefinition`] because the observer
    /// has `num_workers = 0` — no worker subprocess will ever spawn,
    /// so the per-type cmd_args are useless and asking Python to
    /// build them would be a wasted excursion.
    topology: LoadedTopology,
    distributed_config: DistributedConfig,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner observer-mode `SecondaryCoordinator`
    /// at `run()` start. Constructor-only; see the matching field
    /// on `PyPrimaryCoordinator` for the rationale.
    peer_lifecycle_listener: Option<Py<PyAny>>,
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
    holdings: std::collections::HashSet<String>,
    completed: u32,
}

#[pymethods]
impl PyObserverLateJoiner {
    #[new]
    #[pyo3(signature = (
        peer_info_dir,
        task_definition,
        observer_id = None,
        distributed_config = None,
        peer_lifecycle_listener = None,
        holdings = None,
    ))]
    fn new(
        peer_info_dir: PathBuf,
        task_definition: &Bound<'_, PyAny>,
        observer_id: Option<String>,
        distributed_config: Option<DistributedConfig>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        holdings: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;
        // Default observer-id includes a small random suffix so two
        // concurrent observer-dispatchers on the same gateway don't
        // collide on the peer-id (the mesh keys on it). The format
        // mirrors the secondary-id shape (`<role>-<unique>`) so peer
        // logs read uniformly.
        let observer_id = observer_id.unwrap_or_else(|| {
            // Nanosecond timestamp plus 16 bits of process-entropy so
            // two observers launched in the same nanosecond bucket on
            // the same gateway can't collide on the peer id.
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let pid = std::process::id() & 0xffff;
            format!("observer-{ts:08x}-{pid:04x}")
        });
        // Dedup at the boundary — Python typically passes a list, but
        // the announcer's storage is set-semantics (`HashSet`). The
        // alternative (push the dedup onto the consumer) would mean
        // every Python caller has to know about the wire-side
        // contract; doing it here once keeps the kwarg's shape
        // operator-friendly (`list[str]`).
        let holdings: std::collections::HashSet<String> =
            holdings.unwrap_or_default().into_iter().collect();
        Ok(Self {
            observer_id,
            peer_info_dir,
            topology,
            distributed_config: distributed_config.unwrap_or_default(),
            peer_lifecycle_listener,
            holdings,
            completed: 0,
        })
    }

    /// Read the peer-info dir, dial into the mesh, restore the
    /// cluster snapshot, and drive the observation loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        // -- pre-detach: read peer-info dir (synchronous file I/O,
        // small enough that we don't bother offloading; surfacing
        // ReadDirError as a Python exception before we even spin up
        // tokio keeps the error path simple).
        let records = read_peer_info_dir_v2(&self.peer_info_dir).map_err(map_read_dir_error)?;
        let seed = records_to_seed(&records);
        if seed.is_empty() {
            // `read_peer_info_dir_v2` already errors on the empty /
            // all-v1 case; this guards against the (currently
            // unreachable) future shape where the filter drops every
            // record post-conversion. Fail loud rather than spin in
            // `join_running_cluster`'s connect window.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "observer late-joiner: peer-info dir produced zero usable seed entries \
                 after v2 filtering — refusing to enter join_running_cluster with an \
                 empty seed (would hang on the connect-budget)",
            ));
        }

        let observer_id = self.observer_id.clone();
        let estimator = self.topology.estimator.clone();
        // `connect_timeout` is intentionally NOT plumbed: it gates
        // the submitter-bound `NetworkClient` dial loop in the
        // regular-secondary path, which an observer doesn't have
        // (we hand SecondaryCoordinator a `NoPrimaryTransport` stub
        // — see `no_primary.rs`). The observer's analogous budget
        // lives in `DEFAULT_JOIN_TIMEOUT` on the peer-side
        // `join_running_cluster` call below; tying the two
        // together would conflate primary-handshake retry semantics
        // with peer-mesh bootstrap rendezvous semantics.
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        // Take the Python peer-lifecycle listener (if any) out of
        // `self` so it can move into the detached tokio runtime.
        // Wrapped through `PyPeerLifecycleListener::new` into a
        // `Box<dyn LifecycleListener>` at the boundary so the
        // manager-distributed registration API stays
        // PyO3-agnostic.
        let peer_lifecycle_listener =
            self.peer_lifecycle_listener
                .take()
                .map(crate::peer_lifecycle_bridge::PyPeerLifecycleListener::new);
        // Move the holdings set out of `self` so it can be drained into
        // `attach_observer_announcer` on the tokio side. After this
        // point `self.holdings` is empty; the observer is single-shot
        // per `__init__` so a second `run()` would never make sense
        // anyway (the snapshot RPC + restore latch are also one-shot).
        let holdings = std::mem::take(&mut self.holdings);

        let result: Result<u32, PyErr> = py.detach(|| -> Result<u32, PyErr> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to create tokio runtime: {e}"
                    ))
                })?;
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                // 1. Stand up the real peer transport with our chosen
                //    observer-id. The CN baked into the cert MUST
                //    match `observer_id` because every dialing peer
                //    validates the SAN against the logical id.
                let mut peer_network =
                    PeerNetwork::<RunnerIdentifier>::start(&observer_id).await.map_err(
                        |e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "observer late-joiner: failed to start peer network: {e}"
                            ))
                        },
                    )?;
                let peer_cert_pem = peer_network.cert_pem().to_string();
                let peer_port = peer_network.port();

                // 2. Bootstrap rendezvous: hand the seed list to the
                //    trait default impl, which sequences the dial +
                //    snapshot request + reply wait. Errors get
                //    typed strings; we PyErr them with the snapshot
                //    JSON context so the operator can correlate.
                let snapshot_json = peer_network
                    .join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT)
                    .await
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "observer late-joiner: join_running_cluster failed: {e}"
                        ))
                    })?;

                // 3. Decode the snapshot. The wire frame is a String
                //    (the protocol crate keeps `I` erased there); we
                //    materialise it back into the typed snapshot here
                //    so the manager-distributed crate gets the
                //    `ClusterStateSnapshot<RunnerIdentifier>` it
                //    expects.
                let snap: ClusterStateSnapshot<RunnerIdentifier> =
                    serde_json::from_str(&snapshot_json).map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "observer late-joiner: failed to decode \
                             ClusterStateSnapshot from join_running_cluster reply: {e}"
                        ))
                    })?;

                // 4. Construct the observer's coordinator. is_observer=true,
                //    num_workers=0; the Step 7 election filter + the
                //    self-exclusion guard inside `secondary/election.rs`
                //    together keep the observer out of every
                //    promote-to-primary path.
                let config = SecondaryConfig {
                    secondary_id: observer_id.clone(),
                    num_workers: 0,
                    max_resources: dynrunner_core::ResourceMap::from([(
                        dynrunner_core::ResourceKind::memory(),
                        // 1 GiB: a marker value — the observer
                        // doesn't run workers, so its resource map
                        // is irrelevant to actual work, but the
                        // worker-pool budget math (which never
                        // triggers on an empty pool — see
                        // `scheduler::check_resource_pressure`'s
                        // `num_workers == 0` early return) reads it
                        // for completeness.
                        1024 * 1024 * 1024,
                    )]),
                    hostname: gethostname(),
                    keepalive_interval: dist_keepalive,
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    retry_max_passes: dist_retry_max_passes,
                    primary_link_failure_threshold: dist_primary_link_failure_threshold,
                    primary_link_failure_window: dist_primary_link_failure_window,
                    setup_deadline: dist_setup_deadline,
                    is_observer: true,
                };

                // No-op factory: the run loop's only `factory`
                // consumer is `initialize_workers`, which is gated
                // by `!setup_phase_completed`. We're about to set
                // that latch to `true` via
                // `restore_from_snapshot_and_skip_setup`, so the
                // factory's `spawn_worker` is unreachable —
                // any factory satisfying the trait bound works.
                // We reuse the existing `SubprocessWorkerFactory`
                // with placeholder fields rather than adding a
                // dedicated `NoopWorkerFactory` because Step 11
                // (trait deletion in the unification refactor)
                // is about to remove the type-parameter that
                // forces a concrete factory here.
                let mut factory = SubprocessWorkerFactory {
                    python_executable: PathBuf::new(),
                    source_dir: PathBuf::new(),
                    output_dir: PathBuf::new(),
                    log_dir: PathBuf::new(),
                    log_paths: Default::default(),
                    worker_module: String::new(),
                    worker_cmd_args: Vec::new(),
                    skip_existing: false,
                    connection_mode: ConnectionMode::Socketpair,
                    manual_start_worker: false,
                    worker_spec: None,
                    child_processes: Vec::new(),
                };

                let mut secondary: SecondaryCoordinator<
                    NoPrimaryTransport,
                    _,
                    _,
                    _,
                    _,
                    RunnerIdentifier,
                > = SecondaryCoordinator::new(
                    config,
                    NoPrimaryTransport,
                    peer_network,
                    ResourceStealingScheduler::memory(),
                    estimator,
                );

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run_until_setup_or_done` enters — the
                // coordinator's `register_lifecycle_listener` contract
                // requires pre-run registration because the listener
                // vector is `mem::take`-d into the spawned dispatcher
                // on first entry.
                if let Some(listener) = peer_lifecycle_listener {
                    secondary.register_lifecycle_listener(listener);
                }

                // CertExchange path is skipped (setup_phase_completed
                // latched true), but PeerInfo broadcasts that arrive
                // post-restore still consult the local
                // `peer_cert_info` when this observer's id shows up
                // in their distribution. Populating it keeps the
                // broadcast handler symmetric — observers participate
                // in cert exchange so peers can dial back into the
                // observer (e.g. for snapshot RPCs from a later
                // joiner).
                secondary.set_peer_cert_info(PeerCertInfo {
                    public_cert_pem: peer_cert_pem,
                    ipv4_address: Some(detect_ipv4(None)),
                    ipv6_address: detect_ipv6(None),
                    quic_port: peer_port,
                });

                // Attach the resource-holdings announcer's hook +
                // channel BEFORE the snapshot restore: the restore's
                // `cluster_state.restore` path fires
                // `fire_role_change_hooks` from inside its
                // `primary_epoch > local` branch, which we want to
                // count as the post-restore initial trigger. With
                // attach-then-restore the snapshot's apply naturally
                // emits the first `AnnounceTrigger` into the queue;
                // a separate explicit "fire one trigger" step would
                // duplicate that stimulus.
                //
                // The handle is held until the spawn site below.
                let announcer_handle = secondary.attach_observer_announcer(holdings);

                // 5. Install the snapshot AND latch
                //    setup_phase_completed=true. The single-method
                //    `restore_from_snapshot_and_skip_setup` is the
                //    only place outside the secondary crate allowed
                //    to touch the latch (see its doc-comment).
                secondary.restore_from_snapshot_and_skip_setup(snap);

                // TODO(E1-merge): spawn `run_observer_announcer` here
                // once the production `AnnouncerSender` impl is wired
                // up. Today the missing pieces are:
                //   1. `ClusterMutation::PeerResourceHoldingsUpdated`
                //      (lands with the sibling E1 subtask).
                //   2. A cross-task outbox from the announcer to the
                //      secondary's run loop so the announcer doesn't
                //      need shared access to `peer_transport` (which
                //      stays `&mut self`-owned by the run loop).
                // The announcer module + the attach call above are
                // already in place; the spawn site is a single
                // `tokio::task::spawn_local(run_observer_announcer(
                // announcer_handle.rx, announcer_handle.holdings,
                // announcer_handle.peer_id, sender,
                // announcer_handle.primary_epoch_mirror))` once
                // those two land. For now we drop the handle —
                // the trigger queue absorbs role-change fires
                // (`ANNOUNCE_CHANNEL_CAPACITY = 8` per
                // `crate::observer::lifecycle`) and the hook's
                // `try_send` then silently drops, which is
                // structurally identical to "announcer task not yet
                // started" so the safety property holds even with
                // E1 unmerged.
                let _ = announcer_handle;

                // 6. Drive the run loop. The first iteration's
                //    setup-skip guard fires immediately; subsequent
                //    iterations are `RunOutcome::Done` once the
                //    cluster broadcasts `RunComplete`. SetupPending
                //    is unreachable for an observer (only
                //    pre-staged-mode primaries emit the
                //    PromotePrimary that triggers it, and an observer
                //    is never the elected secondary).
                loop {
                    let outcome = secondary
                        .run_until_setup_or_done(&mut factory)
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "observer late-joiner: secondary run loop failed: {e}"
                            ))
                        })?;
                    match outcome {
                        RunOutcome::Done => break,
                        RunOutcome::SetupPending => {
                            // Defensive: a late-joiner observer
                            // should never see SetupPending — that
                            // outcome comes from a
                            // PromotePrimary{required_setup=true}
                            // arrival, which an observer cannot
                            // accept (the election filter + the
                            // dispatch.rs defensive reject keep
                            // observers off the promote path).
                            // Surface it as a typed error rather
                            // than retrying — silent re-entry on
                            // an unreachable branch would mask a
                            // protocol bug.
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                                "observer late-joiner: secondary returned \
                                 RunOutcome::SetupPending — unreachable for an \
                                 observer (PromotePrimary should be rejected); \
                                 this indicates a protocol or election-filter \
                                 regression",
                            ));
                        }
                    }
                }

                Ok(secondary.completed_count() as u32)
            }))
        });

        self.completed = result?;
        Ok(())
    }

    /// Observed completion count read off the snapshot + any live
    /// broadcasts the observer ingested during its run window.
    /// Equivalent to a regular secondary's `completed_count` —
    /// surfaces the union of completed tasks visible in
    /// `cluster_state.tasks`.
    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

/// Translate a [`PeerInfoReadDirError`] into the right PyError shape
/// for the operator. Single concern: error-mapping at the FFI
/// boundary; keeps the `run()` body focused on orchestration.
fn map_read_dir_error(e: PeerInfoReadDirError) -> PyErr {
    match e {
        PeerInfoReadDirError::Io { ref dir, .. } => {
            // io::ErrorKind::NotFound is the most operator-actionable
            // shape (typo'd path, dir not yet created); other I/O
            // errors get the generic OSError shape. The full chain
            // is preserved via the Display impl.
            pyo3::exceptions::PyOSError::new_err(format!(
                "observer late-joiner: peer-info directory unreadable ({dir}): {e}"
            ))
        }
        PeerInfoReadDirError::Parse { ref path, .. } => {
            pyo3::exceptions::PyValueError::new_err(format!(
                "observer late-joiner: malformed peer-info file ({path}): {e}"
            ))
        }
        PeerInfoReadDirError::NoV2Records { ref dir } => {
            // The dir is structurally OK but produced no v2 records
            // — fail loud per the late-joiner design constraint.
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "observer late-joiner: peer-info dir {dir} contains no v2 records — \
                 either the dir is empty, has no `*.info` files, or every file is \
                 legacy v1 (pre-Step-7 wrapper). The late-joiner snapshot RPC \
                 requires the v2 envelope (cert_pem_b64 + quic_port + at least \
                 one of ipv4/ipv6). Re-run the cluster with a Step-7-or-newer \
                 wrapper, or point this flag at a different directory."
            ))
        }
    }
}

/// Convert SLURM-wrapper [`PeerInfoRecord`] entries into the wire-shape
/// [`PeerConnectionInfo`] entries `join_running_cluster` consumes.
///
/// # Filter logic
///
/// `read_peer_info_dir_v2` has already dropped v1 records; we just
/// need to construct the wire struct. Records missing `secondary_id`
/// or `quic_port` (a malformed v2 envelope that nonetheless parsed
/// — e.g. the wrapper crashed between writing the URI line and the
/// envelope tail) are dropped silently here rather than erroring:
/// the joiner's `peer_count() > 0` gate already handles the
/// "no usable seeds survived" case via [`JoinError::NoReachablePeer`].
///
/// [`JoinError::NoReachablePeer`]:
///   dynrunner_protocol_primary_secondary::JoinError::NoReachablePeer
fn records_to_seed(records: &[PeerInfoRecord]) -> Vec<PeerConnectionInfo> {
    records
        .iter()
        .filter_map(|r| {
            let secondary_id = r.secondary_id.clone()?;
            let quic_port = r.quic_port?;
            // `cert` is `String` (not `Option<String>`) on the wire
            // frame; v2 records that lack a cert_pem are pre-handshake
            // partial-writes from the wrapper and won't QUIC-dial
            // anyway. Empty string when absent matches the channel
            // transport's test convention and surfaces a CN-mismatch
            // (loud failure) on the dialer, not a silent drop.
            let cert = r.cert_pem.clone().unwrap_or_default();
            Some(PeerConnectionInfo {
                secondary_id,
                cert,
                ipv4: r.ipv4.clone(),
                ipv6: r.ipv6.clone(),
                port: quic_port,
                is_observer: r.is_observer.unwrap_or(false),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm::{parse_peer_info, PeerInfoBuilder};

    /// Construct a `PeerInfoRecord` end-to-end via the SLURM-wrapper
    /// public surface (`Builder::format` then `parse`). Goes through
    /// the same bytes the on-disk file carries so the test exercises
    /// the same conversion path the late-joiner runs in production.
    fn record_from_builder(b: PeerInfoBuilder) -> PeerInfoRecord {
        parse_peer_info(&b.format()).expect("builder output round-trips through parse")
    }

    /// `records_to_seed` drops records missing the snapshot-RPC-required
    /// fields (`secondary_id` / `quic_port`) rather than producing a
    /// half-filled `PeerConnectionInfo` that would CN-fail on dial.
    /// The "happy" record passes through with every field carried over.
    #[test]
    fn records_to_seed_drops_records_missing_required_fields() {
        let happy = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT-PEM-HERE")
                .ipv4("10.0.0.1")
                .quic_port(51200)
                .is_observer(false),
        );
        // Missing quic_port: the dial would have nowhere to go.
        let no_port = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("sec-2")
                .cert_pem("CERT-PEM-HERE"),
        );
        // Missing secondary_id: the snapshot RPC envelope keys on
        // the responder's secondary_id; without it, the joiner
        // can't construct the unicast Address::Peer target.
        let no_id = record_from_builder(
            PeerInfoBuilder::new("compute3", 40003).quic_port(51202),
        );

        let seed = records_to_seed(&[happy, no_port, no_id]);
        assert_eq!(seed.len(), 1);
        assert_eq!(seed[0].secondary_id, "sec-1");
        assert_eq!(seed[0].port, 51200);
        assert_eq!(seed[0].cert, "CERT-PEM-HERE");
        assert_eq!(seed[0].ipv4.as_deref(), Some("10.0.0.1"));
        assert!(!seed[0].is_observer);
    }

    /// `records_to_seed` carries `is_observer` through verbatim — the
    /// joiner's election filter (Step 7) needs the flag on every
    /// seed entry so it doesn't pick an observer as the responder
    /// preference.
    #[test]
    fn records_to_seed_preserves_is_observer_flag() {
        let observer = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("obs-1")
                .cert_pem("CERT-OBS")
                .quic_port(51200)
                .is_observer(true),
        );
        let regular = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("reg-1")
                .cert_pem("CERT-REG")
                .quic_port(51201)
                .is_observer(false),
        );

        let mut seed = records_to_seed(&[observer, regular]);
        seed.sort_by(|a, b| a.secondary_id.cmp(&b.secondary_id));
        assert_eq!(seed.len(), 2);
        assert_eq!(seed[0].secondary_id, "obs-1");
        assert!(seed[0].is_observer);
        assert_eq!(seed[1].secondary_id, "reg-1");
        assert!(!seed[1].is_observer);
    }

    /// Records missing the optional `is_observer` envelope key
    /// default to `false` — matches the pre-Step-7 v2 senders
    /// (cf. PeerConnectionInfo's `#[serde(default)]`).
    #[test]
    fn records_to_seed_defaults_is_observer_to_false() {
        let r = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT")
                .quic_port(51200),
            // no is_observer set
        );
        let seed = records_to_seed(&[r]);
        assert_eq!(seed.len(), 1);
        assert!(!seed[0].is_observer);
    }
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
))]
pub(crate) fn run_observer_late_joiner<'py>(
    py: Python<'py>,
    peer_info_dir: PathBuf,
    task_definition: &Bound<'py, PyAny>,
    observer_id: Option<String>,
    distributed_config: Option<DistributedConfig>,
    holdings: Option<Vec<String>>,
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
