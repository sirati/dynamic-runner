use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, PhaseId, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::zip_extract::ExtractionCache;

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
        }
    }
}

/// Cached `FullTaskList` payload kept on every secondary so that, on
/// promotion, the new primary can rebuild its `PendingPool` without
/// asking the (now-dead) original primary for another snapshot.
///
/// One alias per concern keeps the secondary struct legible and
/// lets `populate_slurm_tasks` accept the same shape it caches.
type CachedTaskListSnapshot<I> = (
    Vec<dynrunner_protocol_primary_secondary::TaskListEntry<I>>,
    HashSet<String>,
    HashMap<PhaseId, Vec<PhaseId>>,
);

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
    is_slurm_primary: bool,

    // ZIP extraction cache
    extraction_cache: ExtractionCache,

    // Peer keepalive tracking: peer_id -> last_seen timestamp
    peer_keepalives: HashMap<String, f64>,

    // Primary keepalive tracking for failover detection (F2). `None` until
    // the first primary message arrives. Updated on every primary message,
    // not just `Keepalive`, so an actively-routing primary doesn't get
    // falsely declared dead.
    primary_last_seen: Option<Instant>,

    // Failover election state (F2). Defaults to Normal until the primary
    // misses keepalives.
    election: election::ElectionState,

    // Deferred peer messages to send (queued from sync handlers)
    pending_peer_messages: Vec<(String, DistributedMessage<I>)>,

    // Per-worker task request rate limiting
    last_request_time: HashMap<WorkerId, Instant>,
    request_backoff: HashMap<WorkerId, Duration>,

    // SLURM-primary state (populated on promotion + full task list).
    // `slurm_pending` is `None` until the secondary first receives a
    // `FullTaskList` snapshot from the live primary (or, if it gets
    // promoted before any snapshot, until it observes one as the new
    // primary). The pool is rebuilt — not patched — on every snapshot,
    // because the wire format describes the authoritative pending set.
    slurm_pending: Option<PendingPool<I>>,
    slurm_completed: HashSet<String>,
    /// Phase id of every item that the SLURM-primary has dispatched
    /// from `slurm_pending` but not yet seen complete. Mirrors the
    /// pool's in-flight bookkeeping at the per-item granularity so
    /// `on_item_finished` can be called with the right phase id when
    /// a TaskComplete / TaskFailed arrives. Keyed by the same task
    /// hash used in `completed_tasks` / `active_tasks`.
    slurm_in_flight: HashMap<String, PhaseId>,

    // Cached snapshot of the live primary's last `FullTaskList` broadcast.
    // Every secondary keeps the cache up to date so that, on promotion,
    // we can populate `slurm_pending` immediately without round-tripping
    // through a fresh `FullTaskList` (which would require a now-dead
    // primary). Stores the wire-format payload verbatim so the
    // PendingPool reconstruction logic lives in one place
    // (`populate_slurm_tasks`).
    cached_full_task_list: Option<CachedTaskListSnapshot<I>>,

    // Identity of the current SLURM-primary peer, if the original primary
    // is dead and an election has resolved. `None` while the original
    // primary is alive (TaskRequest goes to `primary_transport`); `Some`
    // while we're voting for or have voted for a candidate (TaskRequest
    // is routed to that peer via `peer_transport`). Cleared whenever a
    // live primary message arrives.
    slurm_primary_peer_id: Option<String>,
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
            is_slurm_primary: false,
            extraction_cache,
            peer_keepalives: HashMap::new(),
            primary_last_seen: None,
            election: election::ElectionState::Normal,
            pending_peer_messages: Vec::new(),
            last_request_time: HashMap::new(),
            request_backoff: HashMap::new(),
            slurm_pending: None,
            slurm_completed: HashSet::new(),
            slurm_in_flight: HashMap::new(),
            cached_full_task_list: None,
            slurm_primary_peer_id: None,
        }
    }

    /// Set certificate info for peer connections. Must be called before `run()`
    /// if peer-to-peer QUIC is enabled.
    pub fn set_peer_cert_info(&mut self, info: PeerCertInfo) {
        self.peer_cert_info = Some(info);
    }

    pub fn completed_count(&self) -> usize {
        self.completed_tasks.len()
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
mod processing;
mod resource;
mod setup;
mod slurm;
mod staging;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
