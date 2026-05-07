use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

use self::primary_link::PrimaryLink;

/// Configuration for the secondary coordinator.
pub struct SecondaryConfig {
    pub secondary_id: String,
    pub num_workers: u32,
    pub max_resources: dynrunner_core::ResourceMap,
    pub hostname: String,
    pub keepalive_interval: Duration,
    /// Directory containing ZIP files (for SLURM mode). `None` for local/channel mode.
    pub src_network: Option<PathBuf>,
    /// Temporary directory for extracted binaries. Defaults to a temp dir if `None`.
    pub src_tmp: Option<PathBuf>,
    /// Peer timeout threshold (default: 120s). A peer is considered dead if no
    /// keepalive is received within this duration.
    pub peer_timeout: Duration,
    /// Number of missed keepalives from the primary before the secondary
    /// suspects primary death and starts the failover election (default 3,
    /// matching the primary's `keepalive_miss_threshold`).
    pub keepalive_miss_threshold: u32,
    /// Maximum number of retry passes the primary runs after the
    /// main pass drains. Mirrors `PrimaryConfig::retry_max_passes` —
    /// pre-demotion the local primary owned this; post-demotion the
    /// promoted secondary owns retry for tasks IT dispatched, so the
    /// same knob has to live on this side too. Default 1 (so total
    /// attempts per task = main pass + 1 retry pass = 2). 0 disables
    /// retry entirely on the primary side.
    ///
    /// Only consulted when this secondary is acting as primary
    /// (`is_primary == true`). On non-promoted secondaries the
    /// field is inert — the live primary's `retry_max_passes` is what
    /// drives retry while the live primary is still authoritative.
    pub retry_max_passes: u32,
}

impl Default for SecondaryConfig {
    fn default() -> Self {
        Self {
            secondary_id: String::new(),
            num_workers: 1,
            max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024)]),
            hostname: String::new(),
            keepalive_interval: Duration::from_secs(1),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
        }
    }
}

/// Cached `FullTaskList` payload kept on every secondary so that, on
/// promotion, the new primary can rebuild its `PendingPool` without
/// asking the (now-dead) original primary for another snapshot.
///
/// One alias per concern keeps the secondary struct legible and
/// lets `populate_primary_tasks` accept the same shape it caches.
type CachedTaskListSnapshot<I> = (
    Vec<dynrunner_protocol_primary_secondary::TaskListEntry<I>>,
    HashSet<String>,
    HashMap<PhaseId, Vec<PhaseId>>,
);

/// Per-item bookkeeping for an in-flight primary dispatch.
///
/// One concern: hold everything the primary needs to recover
/// from a rejection (peer reports "No idle worker available") without
/// losing the binary. `phase_id` drives the pool's in-flight counter
/// (see `on_item_finished`); `target_secondary_id` lets the recovery
/// path mark the right peer as backpressured; `binary` is what we
/// `pool.requeue` so the next dispatch cycle can retry.
///
/// Replaces the original `HashMap<String, PhaseId>` ledger — that
/// shape only supported the success path (decrement in_flight) and
/// silently leaked the binary on dispatch-side rejection.
pub(super) struct PrimaryInFlightItem<I: Identifier> {
    pub(super) phase_id: PhaseId,
    pub(super) target_secondary_id: String,
    pub(super) binary: TaskInfo<I>,
}

/// Certificate info for peer connections, set before `run()`.
pub struct PeerCertInfo {
    pub public_cert_pem: String,
    pub ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub quic_port: u16,
}

/// The secondary coordinator: connects to primary, manages local workers.
///
/// Unlike `LocalManager` which runs a 5-phase pipeline, the secondary receives
/// individual task assignments from the primary and dispatches them to local
/// workers. It reports completions back and requests more work.
///
/// Generic over:
/// - `PT`: primary transport (e.g. WSS connection or channel)
/// - `P`: peer transport (e.g. `PeerNetwork` or `NoPeerTransport`)
/// - `M`: manager endpoint for worker communication
/// - `S`: scheduler
/// - `E`: memory estimator
/// - `I`: identifier type
pub struct SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    config: SecondaryConfig,
    primary_transport: PT,
    peer_transport: P,
    scheduler: S,
    estimator: E,

    // Certificate info for peer connections (set before run)
    peer_cert_info: Option<PeerCertInfo>,

    // Workers
    pool: WorkerPool<M, I>,

    // Task tracking: file_hash -> worker_id
    active_tasks: HashMap<String, WorkerId>,
    completed_tasks: HashSet<String>,

    // State
    transfer_complete: bool,
    is_primary: bool,

    // ZIP extraction cache
    extraction_cache: ExtractionCache,

    /// Pre-staged source mode flag — set from the
    /// `InitialAssignment.pre_staged_mode` the primary sends. When
    /// true, file resolution skips the extraction-cache hash check
    /// and trusts `src_network/<local_path>` directly. Off until
    /// the InitialAssignment lands, which is fine: no TaskAssignment
    /// can arrive before InitialAssignment.
    pre_staged_mode: bool,

    /// Whether dispatched task items are backed by real files (the
    /// historical contract). Set from
    /// `InitialAssignment.uses_file_based_items`. When false, the
    /// extraction-cache resolution is skipped entirely and the
    /// wire's `local_path` is passed through to the worker as an
    /// opaque identifier — no `stat()`, no hash, no `.exists()`
    /// check. Defaults to TRUE before InitialAssignment lands so
    /// the historical behaviour remains in place.
    uses_file_based_items: bool,

    // Peer keepalive tracking: peer_id -> last_seen timestamp
    peer_keepalives: HashMap<String, f64>,

    // Primary keepalive tracking for failover detection (F2). `None` until
    // the first primary message arrives. Updated on every primary message,
    // not just `Keepalive`, so an actively-routing primary doesn't get
    // falsely declared dead.
    primary_last_seen: Option<Instant>,

    /// Sticky flag for "the primary's transport returned None on
    /// `recv()`". Used as the `if` guard on the `primary_transport.recv`
    /// arm of `process_tasks`'s `select!` so the persistently-None
    /// future doesn't hot-loop. Once true, we drive failover via the
    /// election state machine and route work via
    /// `primary_link.current_primary()` once a peer wins the election.
    ///
    /// Pre-fix the recv arm bare-broke the loop on `None` and the
    /// secondary exited cleanly with `completed=0` — losing every
    /// task the primary peer was about to dispatch. Dataset
    /// reported this on the dev-box-primary scenario where the local
    /// transport closed at the phase boundary.
    primary_disconnected: bool,

    // Failover election state (F2). Defaults to Normal until the primary
    // misses keepalives.
    election: election::ElectionState,

    // Deferred peer messages to send (queued from sync handlers)
    pending_peer_messages: Vec<(String, DistributedMessage<I>)>,

    /// Routing target + per-worker request rate limiting for the
    /// secondary→primary link. Single owner of "where do operational
    /// sends go?" and "is this worker's request allowed to fire yet?"
    /// — see `primary_link.rs` for the boundary contract.
    pub(in crate::secondary) primary_link: PrimaryLink,

    /// One-shot watchdog deadline for "did the peer mesh form?".
    /// Set to `now + 30s` when `wait_for_setup` kicks off the per-peer
    /// dials with at least one peer in the list; cleared on first
    /// keepalive tick after the deadline passes (after the watchdog
    /// has logged its result). `None` means either we haven't reached
    /// the dial step yet, the peer list was empty (single-secondary
    /// runs), or the watchdog has already fired.
    ///
    /// Without this, the per-peer "QUIC to peer X timed out, trying
    /// WSS" / "WSS to peer X also failed" lines are scattered across
    /// the log with no single signal that the secondary is now
    /// running primary-only — operators have to grep + count to
    /// realise. Cohort 4 (tokenizer) hit exactly this: 5 secondaries,
    /// each printed 4 dial-failure lines, and silence after that;
    /// the actual "0 peers connected ⇒ degraded" state was implied.
    peer_mesh_check_at: Option<Instant>,
    /// Number of peers we asked the transport to dial. Used by the
    /// watchdog to phrase the WARN ("0 of N peers reachable") and to
    /// suppress the watchdog when peers is empty (single-secondary).
    peer_dial_count: u32,
    /// One-shot guard: have we already emitted `MeshReady` to the
    /// primary? The primary defers `PromotePrimary` until every
    /// secondary has reported, so duplicate sends would over-count
    /// (harmless on the receiving end today, but the contract is
    /// "exactly once per secondary"). Toggled true the first time
    /// `report_mesh_ready_if_needed` decides the mesh has settled
    /// (mesh formed, watchdog elapsed, or no peers to dial).
    mesh_ready_sent: bool,

    // primary state (populated on promotion + full task list).
    // `primary_pending` is `None` until the secondary first receives a
    // `FullTaskList` snapshot from the live primary (or, if it gets
    // promoted before any snapshot, until it observes one as the new
    // primary). The pool is rebuilt — not patched — on every snapshot,
    // because the wire format describes the authoritative pending set.
    primary_pending: Option<PendingPool<I>>,
    primary_completed: HashSet<String>,
    /// Per-item ledger for every primary dispatch that hasn't
    /// terminated yet. Keyed by the same task hash used in
    /// `completed_tasks` / `active_tasks`. Stores `phase_id` (drives
    /// the pool's `on_item_finished` counter), `target_secondary_id`
    /// (used by the backpressure path to mark the right peer), and
    /// the full `binary` (used to `pool.requeue` on a rejection so
    /// the task isn't silently dropped from the pool).
    primary_in_flight: HashMap<String, PrimaryInFlightItem<I>>,

    /// Per-task ledger of Recoverable failures observed on the
    /// primary path. Mirrors the live primary's `failed_tasks` set, but
    /// keyed to the secondary that's currently acting as primary.
    /// Populated by `note_primary_item_failed` whenever a Recoverable
    /// failure terminates a dispatch slot for a task this node
    /// dispatched; drained by `primary_drain_check_and_retry`
    /// when the main pass quiesces (pool empty + no in-flight + no
    /// active local tasks) and re-injected into `primary_pending` via
    /// `pool.reinject(item)` for one more attempt.
    ///
    /// Stores the binary alongside the hash so the re-injection step
    /// has the full `TaskInfo` to put back into the pool — `primary_in_flight`
    /// already kept this shape for rejection-recovery; the failed
    /// ledger keeps the same shape for retry-injection.
    ///
    /// Closes the gap demotion introduced: post-demotion the local
    /// primary's `run_retry_passes` is a no-op, so without this
    /// primary-side ledger every Recoverable failure became
    /// terminal at the cluster level.
    primary_failed: HashMap<String, TaskInfo<I>>,

    /// Number of retry passes consumed against `config.retry_max_passes`.
    /// Bumped by `primary_drain_check_and_retry` once per
    /// re-injection event. The retry budget is exhausted when this
    /// counter reaches `retry_max_passes`; subsequent main-pass drains
    /// with `primary_failed` non-empty terminate the run with
    /// the residual entries marked permanently failed.
    primary_retry_passes_used: u32,

    /// One-shot guard for the budget-exhausted WARN emitted by
    /// `primary_drain_check_and_retry`. The drain-check fires
    /// every keepalive tick (and synchronously after every
    /// `note_primary_item_failed`); without this flag the warning would
    /// duplicate every tick for the rest of the run. Pure logging
    /// hygiene — the actual failure count lives in
    /// `primary_failed`.
    exhaustion_warning_emitted: bool,

    /// Per-peer backpressure backoff for the primary path.
    /// Mirrors `PrimaryCoordinator::backpressured_secondaries` — when
    /// a peer rejects a `TaskAssignment` with the wire signal
    /// "No idle worker available", record the peer with an expiry
    /// timestamp; until expiry, `handle_primary_task_request` skips
    /// re-dispatching to that peer (binary stays in the pool for
    /// another candidate). Cleared when the peer reports an actual
    /// `TaskComplete` (proves it's healthy) or when the backoff
    /// window expires naturally.
    backpressured_secondaries: HashMap<String, Instant>,

    // Cached snapshot of the live primary's last `FullTaskList` broadcast.
    // Every secondary keeps the cache up to date so that, on promotion,
    // we can populate `primary_pending` immediately without round-tripping
    // through a fresh `FullTaskList` (which would require a now-dead
    // primary). Stores the wire-format payload verbatim so the
    // PendingPool reconstruction logic lives in one place
    // (`populate_primary_tasks`).
    cached_full_task_list: Option<CachedTaskListSnapshot<I>>,

    /// Set by handlers that detect an unrecoverable local fault (peer
    /// mesh fully failed to form, etc.). The main `process_tasks`
    /// loop checks this once per iteration AFTER the deferred-message
    /// flush; if `Some`, the loop returns `Err(reason)` and the
    /// secondary's `run()` propagates that out so the process exits
    /// non-zero.
    ///
    /// One-concern wiring: handlers (e.g. the peer-mesh watchdog)
    /// only WRITE this; the main loop only READS. Avoids `break`
    /// from inside a sub-handler — every flag-setter stays cancel-
    /// safe and the loop owns its own exit condition.
    pub(super) fatal_exit: Option<String>,

    /// Replicated mirror of the cluster ledger. Maintained by applying
    /// every `DistributedMessage::ClusterMutation` the primary
    /// broadcasts. Read-only on this node — only the originator may
    /// produce mutations (Phase L will move the originator-side logic
    /// onto whichever node currently holds the primary role).
    pub(super) cluster_state: ClusterState<I>,
}

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub fn new(
        config: SecondaryConfig,
        primary_transport: PT,
        peer_transport: P,
        scheduler: S,
        estimator: E,
    ) -> Self {
        let tmp_dir = config.src_tmp.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("db_secondary_{}", &config.secondary_id))
        });
        let extraction_cache = ExtractionCache::new(tmp_dir, config.src_network.clone());
        let primary_link = PrimaryLink::new(config.secondary_id.clone());
        Self {
            config,
            primary_transport,
            peer_transport,
            scheduler,
            estimator,
            peer_cert_info: None,
            pool: WorkerPool::new(),
            active_tasks: HashMap::new(),
            completed_tasks: HashSet::new(),
            transfer_complete: false,
            is_primary: false,
            extraction_cache,
            peer_keepalives: HashMap::new(),
            primary_last_seen: None,
            primary_disconnected: false,
            election: election::ElectionState::Normal,
            pending_peer_messages: Vec::new(),
            primary_link,
            peer_mesh_check_at: None,
            peer_dial_count: 0,
            mesh_ready_sent: false,
            primary_pending: None,
            primary_completed: HashSet::new(),
            primary_in_flight: HashMap::new(),
            primary_failed: HashMap::new(),
            primary_retry_passes_used: 0,
            exhaustion_warning_emitted: false,
            backpressured_secondaries: HashMap::new(),
            cached_full_task_list: None,
            pre_staged_mode: false,
            uses_file_based_items: true,
            fatal_exit: None,
            cluster_state: ClusterState::new(),
        }
    }

    /// Whether the run is in pre-staged-source mode (set from the
    /// primary's `InitialAssignment`). Exposed within the secondary
    /// module so dispatch / setup can pick the right resolution path.
    pub(super) fn pre_staged_mode(&self) -> bool {
        self.pre_staged_mode
    }

    pub(super) fn set_pre_staged_mode(&mut self, on: bool) {
        self.pre_staged_mode = on;
    }

    /// Whether dispatched task items back to real files (default true).
    /// When false, the worker receives `local_path` as an opaque
    /// identifier and the framework performs no filesystem
    /// resolution.
    pub(super) fn uses_file_based_items(&self) -> bool {
        self.uses_file_based_items
    }

    pub(super) fn set_uses_file_based_items(&mut self, on: bool) {
        self.uses_file_based_items = on;
    }

    /// Single source of truth for "given the wire's `local_path`,
    /// what's the on-disk path the worker should open?"
    ///
    /// Two structural cases, with one option-axis inside the
    /// file-based case:
    ///   - `!uses_file_based_items` (FR-2): items aren't files. The
    ///     wire's `local_path` is an opaque worker identifier;
    ///     framework does no filesystem IO on it. (Different
    ///     concern from resolution — the worker reads its payload
    ///     via JSON / stdin / comm-fd.)
    ///   - file-based: framework looks for the file. Hash
    ///     verification is OPTIONAL — only meaningful when the
    ///     primary actually computed a content hash (i.e. it
    ///     transferred / verified the file). In pre-staged mode
    ///     the bind-mount IS the contract; no transfer happened so
    ///     there's nothing to dedup against, and the resolver
    ///     accepts the bind-mounted file by existence alone.
    ///
    /// Used by every dispatch + assignment site on the secondary
    /// (operational TaskAssignment in `dispatch.rs`, initial-batch
    /// in `setup.rs`, primary self-assign + repopulate in
    /// `primary.rs`). Centralising here keeps the option-axis
    /// (verify-or-not) consistent across sites.
    pub(super) fn resolve_for_dispatch(
        &mut self,
        zip_ref: Option<&str>,
        local_path: &str,
        file_hash: &str,
    ) -> Option<std::path::PathBuf> {
        if !self.uses_file_based_items {
            return Some(std::path::PathBuf::from(local_path));
        }
        // In pre-staged mode the primary doesn't compute a content
        // hash (no transfer), so pass None and let the resolver
        // accept by existence. Otherwise hash-verify like the
        // historical path.
        let expected_content_hash = if self.pre_staged_mode {
            None
        } else {
            Some(file_hash)
        };
        self.extraction_cache
            .resolve_binary(zip_ref, local_path, file_hash, expected_content_hash)
    }

    /// True iff `secondary_id` is currently in the primary's
    /// backpressure backoff window (recently returned "No idle worker
    /// available"). Used by `handle_primary_task_request` to skip
    /// re-dispatching to an unresponsive peer. Mirrors
    /// `PrimaryCoordinator::is_backpressured`.
    pub(super) fn is_primary_peer_backpressured(&self, secondary_id: &str) -> bool {
        self.backpressured_secondaries
            .get(secondary_id)
            .is_some_and(|t| Instant::now() < *t)
    }

    /// Set certificate info for peer connections. Must be called before `run()`
    /// if peer-to-peer QUIC is enabled.
    pub fn set_peer_cert_info(&mut self, info: PeerCertInfo) {
        self.peer_cert_info = Some(info);
    }

    pub fn completed_count(&self) -> usize {
        self.completed_tasks.len()
    }

    /// Test-only inspector for the primary retry budget
    /// counter. Lets tests assert that the retry pass actually
    /// consumed budget (vs. e.g. the success arriving without
    /// re-injection because the test fixture fixed the worker
    /// behaviour after one pass anyway). Public-but-test-gated so
    /// production callers don't depend on this internal counter
    /// shape.
    #[cfg(test)]
    pub fn primary_retry_passes_used_for_test(&self) -> u32 {
        self.primary_retry_passes_used
    }

    /// Test-only inspector for the primary's residual
    /// failed-task ledger after the retry budget is exhausted. Used
    /// by the multi-pass-exhaustion regression test to assert that a
    /// task which fails Recoverably across all permitted passes ends
    /// up permanently in `primary_failed`. Counts only
    /// primary-dispatched failures (tasks that went through
    /// `handle_primary_task_request`); initial-assignment failures
    /// observed by the local worker bypass this ledger by design.
    #[cfg(test)]
    pub fn primary_failed_count_for_test(&self) -> usize {
        self.primary_failed.len()
    }

    /// Test-only inspector for the replicated cluster ledger this
    /// secondary maintains by applying primary-broadcast
    /// `ClusterMutation`s. Returns the per-state counts so tests can
    /// assert convergence with the primary's view.
    #[cfg(test)]
    pub fn cluster_state_counts_for_test(&self) -> crate::cluster_state::StateCounts {
        self.cluster_state.counts()
    }

    /// Run the secondary coordination loop:
    /// 1. Initialize local workers
    /// 2. Send welcome and cert exchange to primary
    /// 3. Wait for peer list, initial assignment, transfer complete
    /// 4. Process tasks: receive assignments, run on local workers, report back
    pub async fn run(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        tracing::info!(
            secondary = %self.config.secondary_id,
            workers = self.config.num_workers,
            resources = %self.config.max_resources,
            "secondary starting"
        );

        // Initialize workers
        self.initialize_workers(factory).await?;

        // Phase 1: Send welcome
        self.send_welcome().await?;

        // Phase 2: Send cert exchange
        self.send_cert_exchange().await?;

        // Phase 3+4: Wait for peer list, initial assignment, and transfer complete
        self.wait_for_setup().await?;

        // Phase 5: Process tasks
        self.process_tasks(factory).await?;

        // Stop workers
        self.stop_all_workers().await;

        tracing::info!(
            secondary = %self.config.secondary_id,
            completed = self.completed_tasks.len(),
            "secondary finished"
        );

        Ok(())
    }

    fn max_resources(&self) -> dynrunner_core::ResourceMap {
        self.config.max_resources.clone()
    }
}

mod dispatch;
mod election;
mod peer;
mod primary_link;
mod processing;
mod resource;
mod primary;
mod setup;
mod staging;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
