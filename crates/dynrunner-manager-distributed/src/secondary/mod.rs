use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use dynrunner_core::{
    ErrorType, Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

use self::primary_link::PrimaryLink;

/// Outcome reported by `SecondaryCoordinator::run_until_setup_or_done`.
///
/// The PyO3 wrapper drives the secondary in a loop and inspects this
/// value to decide whether to run Python-side setup discovery before
/// re-entering, or to break out and shut down. The Rust-only callers
/// (tests, the existing `run` entry point) only ever observe `Done` —
/// `SetupPending` requires a `required_setup: true` wire promotion,
/// which never happens in those contexts.
///
/// - `SetupPending`: the secondary was promoted with `required_setup =
///   true` and the process-tasks loop yielded so the caller can run
///   Python's `task.discover_items` against the locally-mounted staged
///   source and feed the result back via `ingest_setup_discovery`. The
///   worker pool is left running; re-entering `run_until_setup_or_done`
///   resumes the loop.
/// - `Done`: the loop reached one of its normal terminations
///   (RunComplete observed, drain-down after primary disconnect, or
///   single-secondary clean exit). The worker pool has been stopped
///   and the secondary is finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    SetupPending,
    Done,
}

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

    /// Number of consecutive primary-link recv-None probes after
    /// which the secondary arms failover (i.e. sets
    /// `primary_disconnected = true` and lets the election state
    /// machine take over). Default is `primary_link::DEFAULT_FAILURE_THRESHOLD`
    /// (5). Lower values arm faster — but bounding below 3 risks
    /// promoting on a single dropped TCP packet retransmit, which is
    /// wrong (per the architectural invariant: a transient packet
    /// drop is not a leadership event).
    pub primary_link_failure_threshold: u32,

    /// Wall-clock window after the first observed primary-link recv
    /// failure within which the threshold-attempts counter must
    /// breach to avoid time-based arming. Default is
    /// `primary_link::DEFAULT_FAILURE_WINDOW` (30s). Used to bound
    /// failover latency on slow-keepalive configurations where 5
    /// probes would exceed the SLURM time budget.
    pub primary_link_failure_window: Duration,

    /// Observer mode: this secondary participates in cluster updates
    /// (ClusterMutation broadcasts, PeerInfo, Keepalive, peer-routed
    /// task-state messages) but cannot become primary and has no
    /// workers. Use case: the dispatcher in SLURM mode hosts an
    /// in-process observer so it stays connected to the cluster as
    /// a non-candidate secondary even after a primary handoff/death
    /// — the surviving SLURM secondaries elect among themselves and
    /// the dispatcher's observer just receives the broadcasts.
    ///
    /// When `is_observer = true`:
    ///   - `num_workers` should be 0 (no work to take on); the
    ///     framework does not validate this, but processing-loop
    ///     paths that iterate workers behave correctly with an
    ///     empty pool.
    ///   - The election state machine refuses to enter `Candidate`
    ///     state — the observer never self-promotes even when it
    ///     would otherwise be the lowest-id alive peer. See
    ///     `election.rs::run_election_tick`'s `we_lead` branch.
    ///   - A `PromotePrimary` naming this secondary is rejected
    ///     with a loud error (defensive: should not happen if peers
    ///     honour the same flag, but protects against a misconfigured
    ///     peer or a wire-level forgery).
    ///
    /// Default `false` (regular secondary). The peer-mesh-side
    /// fortification (peers filtering observers from `lowest_alive`
    /// candidate selection) requires extending `PeerConnectionInfo`
    /// with this flag; tracked as a follow-up to this commit.
    pub is_observer: bool,

    /// Maximum wall-clock the secondary will spend in setup phases
    /// (send_welcome + send_cert_exchange + wait_for_setup) before
    /// concluding the cluster is dead and exiting cold. Default 60s.
    ///
    /// Concern: a late-arriving secondary scheduled AFTER the run
    /// has logically completed (primary already exited) cannot reach
    /// the now-dead primary URL. Without this deadline the transport
    /// layer's internal connection retries hold the boot path
    /// indefinitely (asm-dataset-nix T7 attempt 2 observed
    /// ~345 retries × 1s = ~6min before SLURM container teardown
    /// reaped the secondary). 60s gives a slow primary handshake
    /// enough headroom on healthy clusters; well under SLURM's
    /// per-job minimum so a dead-cluster boot reaps fast.
    ///
    /// `R1` (mid-run primary disconnect detection) deliberately
    /// lives in the processing loop, not the setup loop — the
    /// setup-phase `wait_for_setup` is documented as cancellation-
    /// unsafe under tokio::select! racing of `recv()` (see
    /// `setup.rs:79-96`), so we apply the deadline at the
    /// orchestration boundary instead of nested inside the recv
    /// loop. On timeout the recv future is cancelled at the outer
    /// boundary, no subsequent iteration touches the (possibly
    /// partial) transport state, so the cancellation hazard the
    /// setup-loop comment warns about does not arise.
    pub setup_deadline: Duration,
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
            primary_link_failure_threshold: primary_link::DEFAULT_FAILURE_THRESHOLD,
            primary_link_failure_window: primary_link::DEFAULT_FAILURE_WINDOW,
            is_observer: false,
            setup_deadline: Duration::from_secs(60),
        }
    }
}

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

/// One entry in the secondary's `primary_failed` ledger. Carries
/// both the original `TaskInfo` (so the secondary can re-inject the
/// binary on a retry pass without re-fetching it from the cluster
/// state) and the last-observed `ErrorType` (so the per-class
/// outcome breakdown — fail_retry/fail_oom/fail_final — can be
/// computed locally at log time).
///
/// On retry, the entry is overwritten with the new pass's
/// `ErrorType`; the binary is unchanged.
#[derive(Debug, Clone)]
pub(super) struct FailedTaskEntry<I: Identifier> {
    pub(super) binary: TaskInfo<I>,
    pub(super) error_type: ErrorType,
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
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
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

    /// Test-only counter: number of `WorkerEvent::TaskCompleted` events
    /// this secondary's OWN workers fired (i.e. tasks actually
    /// dispatched to and executed by this node's worker pool). Distinct
    /// from `completed_tasks` (which is the cluster-wide set of all
    /// task hashes any secondary observed terminal). Pinned by the
    /// peer-repoll-on-primary-changed regression test to assert
    /// post-fix distribution across secondaries: pre-fix the promoted
    /// secondary's pool burns through small workloads before any peer
    /// re-polls (production keepalive default = 5s), so peer
    /// `local_tasks_run` stays 0; post-fix every secondary's idle
    /// workers retry against the freshly-identified primary inside
    /// the PromotePrimary dispatch tick and pick up work.
    #[cfg(test)]
    local_tasks_run: usize,

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

    // primary state (populated on promotion).
    // `primary_pending` is `None` until this secondary is promoted; on
    // the became-primary transition `populate_primary_from_cluster_state`
    // rebuilds it from the continuously-replicated `cluster_state` mirror.
    // No wire round-trip: the cluster ledger has been kept in sync via
    // `ClusterMutation` broadcasts since the run started.
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
    /// Each entry now also carries the last-observed `ErrorType` so
    /// the post-restructure `succeeded/fail_retry/fail_oom/fail_final`
    /// log surface can partition failures by class. Pre-restructure
    /// the secondary lost OOM-vs-final classification at this
    /// boundary (the binary was carried, the failure class was not).
    ///
    /// Closes the gap demotion introduced: post-demotion the local
    /// primary's `run_retry_passes` is a no-op, so without this
    /// primary-side ledger every Recoverable failure became
    /// terminal at the cluster level.
    primary_failed: HashMap<String, FailedTaskEntry<I>>,

    /// Retry budget for the primary-side re-injection loop. Owns
    /// both the attempt counter (originally `retry_max_passes`) and
    /// the optional SLURM-wallclock deadline (read once at
    /// construction from `$SLURM_JOB_END_TIME`). Consulted via
    /// `RetryBudget::should_retry()` from `primary_drain_check_and_retry`
    /// and the two drain-down exit predicates in `processing.rs`;
    /// bumped via `record_attempt()` once per completed re-injection
    /// cycle. See `retry_budget.rs` for the dual-axis design.
    primary_retry_budget: retry_budget::RetryBudget,

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

    /// Set by handlers that detect an unrecoverable local fault.
    /// The main `process_tasks` loop checks this once per iteration
    /// AFTER the deferred-message flush; if `Some`, the loop returns
    /// `Err(reason)` and the secondary's `run()` propagates that out
    /// so the process exits non-zero.
    ///
    /// One-concern wiring: handlers only WRITE this; the main loop
    /// only READS. Avoids `break` from inside a sub-handler — every
    /// flag-setter stays cancel-safe and the loop owns its own exit
    /// condition.
    pub(super) fatal_exit: Option<String>,

    /// "Peer mesh did not form" sentinel. Set true by
    /// `check_peer_mesh_watchdog` when the 30s deadline elapses with
    /// zero connected peers. The watchdog used to make this fatal,
    /// stranding every remaining task in the run; the failure is now
    /// a degraded state instead — task dispatch over WSS still works,
    /// only the peer-mesh-dependent paths (failover election, peer
    /// keepalive broadcasts) fail-loud-or-skip on this flag.
    ///
    /// Read by:
    ///   - the failover election entry in `run_election_tick`: a
    ///     primary-silent transition without a quorum-capable peer
    ///     mesh sets `fatal_exit` (degraded run can't elect a new
    ///     primary, so the only safe move is to bail clearly instead
    ///     of self-promoting solo).
    ///   - the inter-secondary keepalive paths
    ///     (`send_keepalive`'s broadcast, `check_peer_timeouts`):
    ///     skip the cycle. With zero peers these are no-ops anyway,
    ///     but the explicit guard documents the contract and avoids
    ///     a surprise the day a future peer-transport variant
    ///     buffers messages even when nothing is connected.
    ///
    /// Distinct from `peer_mesh_check_at`: the watchdog field tracks
    /// the in-flight deadline (cleared when mesh forms OR watchdog
    /// fires). `peer_mesh_degraded` is the post-fire latch that
    /// callers consult to decide whether peer-mesh-dependent
    /// behaviour is available.
    pub(super) peer_mesh_degraded: bool,

    /// Replicated mirror of the cluster ledger. Maintained by applying
    /// every `DistributedMessage::ClusterMutation` the primary
    /// broadcasts. Read-only on this node — only the originator may
    /// produce mutations (Phase L will move the originator-side logic
    /// onto whichever node currently holds the primary role).
    pub(super) cluster_state: ClusterState<I>,

    /// Worker IDs queued for respawn at the next `process_tasks`
    /// tick. Populated by code paths that observe a worker
    /// subprocess as dead WITHOUT a corresponding
    /// `WorkerEvent::Disconnected` arriving on the pool's event
    /// channel:
    ///
    ///   - `handle_primary_task_request`'s self-assign Err arm
    ///     (the local worker's pipe is broken when we try to write
    ///     the next task to it),
    ///   - `dispatch_message`'s peer-assign Err arm (same shape on
    ///     the peer-assigned path).
    ///
    /// In both cases the worker subprocess has voluntarily exited
    /// — typically because `NonRecoverableError` in the task
    /// handler causes the runtime to send the error response,
    /// then the framework's restart-on-next-assignment contract
    /// (see `dynamic_runner.worker.runtime.NonRecoverableError`
    /// docstring) kicks in. The `assign_task` write subsequently
    /// fails on Broken pipe and the worker_id ends up here.
    ///
    /// `process_tasks` drains the set at the end of each tick and
    /// calls `pool.restart_worker(wid, factory, _)` for each
    /// entry, then re-issues a `TaskRequest` so the fresh worker
    /// pulls fresh work from the primary's pool. Idempotent on
    /// duplicate entries — the worker either restarted at the
    /// last drain (set was emptied) or is still queued (no-op
    /// already in flight).
    pub(super) pending_worker_restarts: HashSet<WorkerId>,

    /// Set true by the `PromotePrimary { required_setup: true }` arm
    /// in `dispatch.rs` when this secondary is promoted into the
    /// setup-secondary role (the submitter deferred all run-setup work
    /// to us — no `TaskAdded` batch was pre-seeded on the cluster
    /// ledger). The outer process-tasks loop yields back to the PyO3
    /// wrapper when this is true so Python's `task.discover_items` can
    /// run on this node (which has the staged source filesystem
    /// bind-mounted locally). The wrapper then calls
    /// `ingest_setup_discovery`, which seeds the ledger with
    /// `PhaseDepsSet` + `TaskAdded` mutations, broadcasts them to
    /// every peer, clears this flag, and hydrates the primary pool
    /// from the now-populated `cluster_state`.
    ///
    /// Never set on the pre-seeded promotion path (`required_setup:
    /// false` from the local submitter — local did discovery and the
    /// ledger is pre-seeded) nor on the failover-election path (the
    /// ledger has content from the CRDT broadcasts that ran during
    /// the live-primary phase). The wire-level `required_setup` flag
    /// is the only discriminator; "ledger empty" is NOT a proxy
    /// because failover-at-startup can legitimately observe an
    /// empty ledger.
    pub(super) setup_pending: bool,

    /// Re-entry guard for `run_until_setup_or_done`. The first call
    /// runs `initialize_workers`, the setup-handshake (`send_welcome`,
    /// `send_cert_exchange`, `wait_for_setup`) and then enters
    /// `process_tasks`. If `process_tasks` returns early with
    /// `RunOutcome::SetupPending`, the caller (the PyO3 wrapper) runs
    /// Python discovery and re-enters this same method to resume.
    /// On that second entry the per-secondary setup phase must NOT
    /// run again — `initialize_workers` would race against the
    /// already-spawned worker pool and `wait_for_setup` would block
    /// on wire messages that have already been consumed. This flag
    /// is set the moment setup completes successfully and gates the
    /// setup block on every subsequent entry.
    ///
    /// Always false on the pre-seeded (`required_setup_on_promote =
    /// false`) and failover paths; the existing `run` wrapper only
    /// calls `run_until_setup_or_done` once, so the flag transition
    /// is `false → true (mid-call) → (method returns Done)` for
    /// those callers.
    pub(super) setup_phase_completed: bool,
}

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
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
        let primary_link = PrimaryLink::with_failover_threshold(
            config.secondary_id.clone(),
            config.primary_link_failure_threshold,
            config.primary_link_failure_window,
        );
        // RetryBudget consumes `config.retry_max_passes` as the
        // attempt-count cap and reads `$SLURM_JOB_END_TIME` ONCE
        // here (startup) for the wallclock deadline. The
        // env-var is documented as Unix-epoch seconds; absence is
        // the legacy non-SLURM path (silent), parse failure logs WARN.
        // See `retry_budget.rs` for the dual-axis design.
        let primary_retry_budget = retry_budget::RetryBudget::from_env_and_legacy(
            config.retry_max_passes,
            retry_budget::DEFAULT_SAFETY_MARGIN,
        );
        let mut this = Self {
            config,
            primary_transport,
            peer_transport,
            scheduler,
            estimator,
            peer_cert_info: None,
            pool: WorkerPool::new(),
            active_tasks: HashMap::new(),
            completed_tasks: HashSet::new(),
            #[cfg(test)]
            local_tasks_run: 0,
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
            primary_retry_budget,
            exhaustion_warning_emitted: false,
            backpressured_secondaries: HashMap::new(),
            pre_staged_mode: false,
            uses_file_based_items: true,
            fatal_exit: None,
            peer_mesh_degraded: false,
            cluster_state: ClusterState::new(),
            pending_worker_restarts: HashSet::new(),
            setup_pending: false,
            setup_phase_completed: false,
        };
        // Attach the transport's write-through role cache to our
        // authoritative `cluster_state.role_table`. The hook fires
        // on every applied `PrimaryChanged` mutation; the cache
        // serves Step 3's `Address::Role(_)` dispatch on the send
        // hot path. Transports that don't override
        // `register_with_cluster_state` (e.g. `NoPeerTransport`,
        // test stubs) get the default no-op — safe by construction.
        this.peer_transport
            .register_with_cluster_state(&mut this.cluster_state);
        this
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

    /// Late-joiner bootstrap entry: install a snapshot received from a
    /// peer's `RequestClusterSnapshot` response, then mark setup as
    /// completed so the next `run_until_setup_or_done` invocation
    /// skips the welcome / cert-exchange / wait-for-setup phases and
    /// enters `process_tasks` directly.
    ///
    /// # Concern
    ///
    /// Single helper for "the late-joining observer dispatcher already
    /// owns its cluster view (it called `join_running_cluster` before
    /// constructing this coordinator), so the existing run loop should
    /// pick up from the live-processing phase rather than re-run the
    /// primary-handshake setup". Mirrors what
    /// `run_until_setup_or_done`'s second-iteration branch does on the
    /// `SetupPending` re-entry path — that branch's `if !self
    /// .setup_phase_completed { … }` guard is the single source of
    /// truth for "skip setup". We just set the latch.
    ///
    /// # Why a dedicated entry-point (not an inline `cluster_state` +
    /// `setup_phase_completed` writer on the caller)
    ///
    /// `cluster_state` and `setup_phase_completed` are intentionally
    /// `pub(super)` — the secondary module owns the latch's lifecycle
    /// and external callers were forbidden from poking it directly so
    /// the legacy run-loop invariants stay enforced. The Step 9
    /// late-joiner path is the first legitimate external caller that
    /// needs to set both atomically; expressing it as one named method
    /// keeps the latch's exposure scoped to that single use case.
    ///
    /// # When to call
    ///
    /// After
    /// [`crate::PeerTransport::join_running_cluster`] returns the
    /// snapshot JSON, the caller deserializes it into a
    /// `ClusterStateSnapshot<I>` and passes it here. Subsequent
    /// `run_until_setup_or_done` calls observe `setup_phase_completed
    /// = true` and route straight to `process_tasks`. The role-change
    /// hook the transport registered in `new()` fires from inside
    /// `cluster_state.restore` so the peer-mesh role-cache is warmed
    /// (e.g. `current_primary` is now resolvable for
    /// `Address::Role(Role::Primary)` sends).
    pub fn restore_from_snapshot_and_skip_setup(
        &mut self,
        snap: crate::cluster_state::ClusterStateSnapshot<I>,
    ) {
        self.cluster_state.restore(snap);
        self.setup_phase_completed = true;
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
    /// shape. Forwards through the encapsulated `RetryBudget`.
    #[cfg(test)]
    pub fn primary_retry_passes_used_for_test(&self) -> u32 {
        self.primary_retry_budget.attempts_used()
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

    /// Test-only inspector for the count of tasks this secondary's
    /// OWN worker pool ran (i.e. local `WorkerEvent::TaskCompleted`
    /// fires). Distinct from `completed_count()` which reports the
    /// cluster-wide observed-terminal set. Used by the
    /// `setup_promote_multi_secondary_distributes_to_idle_peers_on_promote`
    /// regression test to assert post-fix distribution across all 4
    /// secondaries.
    #[cfg(test)]
    pub fn local_tasks_run_for_test(&self) -> usize {
        self.local_tasks_run
    }

    /// Run the secondary coordination loop:
    /// 1. Initialize local workers
    /// 2. Send welcome and cert exchange to primary
    /// 3. Wait for peer list, initial assignment, transfer complete
    /// 4. Process tasks: receive assignments, run on local workers, report back
    ///
    /// Convenience wrapper around `run_until_setup_or_done` for callers
    /// that don't participate in the setup-promote handshake (every
    /// caller other than the PyO3 secondary wrapper, which has to
    /// re-enter the loop after running Python `task.discover_items`).
    /// The outcome can only be `Done` here, because `SetupPending`
    /// requires a `PromotePrimary { required_setup: true }` wire arrival
    /// and no test/non-pyo3 setup ever sends one.
    pub async fn run(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        match self.run_until_setup_or_done(factory).await? {
            RunOutcome::Done => Ok(()),
            RunOutcome::SetupPending => Err(
                "secondary yielded SetupPending but caller is the legacy run() \
                 wrapper which cannot drive setup discovery — programming error \
                 (only the PyO3 secondary wrapper should invoke a secondary that \
                 may be promoted with required_setup=true)"
                    .to_string(),
            ),
        }
    }

    /// Drive the secondary coordination loop until it either yields
    /// for setup discovery (`RunOutcome::SetupPending`) or reaches a
    /// terminal state (`RunOutcome::Done`).
    ///
    /// First invocation: runs `initialize_workers`, the setup handshake
    /// (welcome / cert exchange / wait_for_setup) under
    /// `config.setup_deadline`, then enters `process_tasks`.
    ///
    /// Subsequent invocations (only reached on the `SetupPending`
    /// caller-loop re-entry): skip the setup phase — workers are still
    /// alive and the handshake messages have already been consumed —
    /// and re-enter `process_tasks` directly. The re-entry guard is
    /// `self.setup_phase_completed`, set the moment the first
    /// invocation finishes the handshake successfully.
    ///
    /// Cleanup (`stop_all_workers` + the "secondary finished" log)
    /// fires only on the `Done` branch. On `SetupPending` the worker
    /// pool is intentionally left running so the caller's re-entry
    /// finds it in the same state `process_tasks` yielded from.
    ///
    /// Cancel-safety: `process_tasks` already documents that every
    /// arm of its `select!` is cancel-safe (mpsc recv + tokio
    /// interval ticks); the early break on `setup_pending` simply
    /// abandons the in-flight future of whichever arm was awaiting,
    /// and the next entry rebuilds a fresh `select!`. No state is
    /// dropped except those in-flight recv futures, which are
    /// cancel-safe by construction.
    pub async fn run_until_setup_or_done(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        if !self.setup_phase_completed {
            tracing::info!(
                secondary = %self.config.secondary_id,
                workers = self.config.num_workers,
                resources = %self.config.max_resources,
                "secondary starting"
            );

            // Initialize workers (local pool — no network, no deadline).
            self.initialize_workers(factory).await?;

            // Network-touching setup (Phases 1-4) is bounded by
            // `setup_deadline`. See SecondaryConfig::setup_deadline for
            // the rationale. The deadline is applied at the orchestration
            // boundary, NOT inside `wait_for_setup`, because the recv
            // loop is documented as cancellation-unsafe under inner
            // select! racing (see setup.rs:79-96). Cancelling the whole
            // setup future on timeout is safe because we never re-enter
            // any of these phases — we go straight to cleanup-and-exit.
            let deadline = self.config.setup_deadline;
            let setup = async {
                self.send_welcome().await?;
                self.send_cert_exchange().await?;
                self.wait_for_setup().await?;
                Ok::<(), String>(())
            };
            match tokio::time::timeout(deadline, setup).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.stop_all_workers().await;
                    return Err(e);
                }
                Err(_elapsed) => {
                    let peers = self.peer_transport.peer_count();
                    self.stop_all_workers().await;
                    if peers == 0 {
                        // The asm-dataset-nix T7 attempt 2 scenario:
                        // primary URL unreachable AND no peers have
                        // dialled in. The run is almost certainly
                        // already complete and SLURM is just booting
                        // a queued secondary against the graveyard.
                        // Exit fast with a clear log.
                        tracing::warn!(
                            secondary = %self.config.secondary_id,
                            deadline_secs = deadline.as_secs(),
                            "setup deadline elapsed with no primary and no peers — \
                             run appears already complete, exiting cold"
                        );
                        return Err(format!(
                            "setup deadline ({}s) elapsed: no primary, no peers \
                             (cluster appears dead, run likely complete)",
                            deadline.as_secs()
                        ));
                    } else {
                        // Peers reachable but setup didn't complete. This
                        // is a distinct scenario from cold-start (primary
                        // unresponsive but mesh is alive — could be a
                        // partial cluster bring-up race). Surface
                        // separately so operators can distinguish.
                        tracing::error!(
                            secondary = %self.config.secondary_id,
                            deadline_secs = deadline.as_secs(),
                            peer_count = peers,
                            "setup deadline elapsed despite peers reachable — \
                             primary unresponsive, exiting"
                        );
                        return Err(format!(
                            "setup deadline ({}s) elapsed: primary unresponsive \
                             despite {} peer(s) reachable",
                            deadline.as_secs(),
                            peers
                        ));
                    }
                }
            }

            // Latch BEFORE entering process_tasks so a SetupPending
            // yield doesn't trigger a redo on re-entry.
            self.setup_phase_completed = true;
        }

        // Phase 5: Process tasks. May yield with SetupPending or
        // run to completion.
        let outcome = self.process_tasks(factory).await?;

        match outcome {
            RunOutcome::Done => {
                // Normal termination — stop workers and log finish.
                self.stop_all_workers().await;
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    completed = self.completed_tasks.len(),
                    "secondary finished"
                );
            }
            RunOutcome::SetupPending => {
                // Workers stay alive; the caller's re-entry resumes
                // the loop in `process_tasks`. No final log line yet —
                // the run isn't actually finished.
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "secondary yielding for setup discovery"
                );
            }
        }

        Ok(outcome)
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
mod retry_budget;
mod setup;
mod staging;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
