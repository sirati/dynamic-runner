use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use db_comm_api_base::{BinaryInfo, Identifier, WorkerId};
use db_manager_runner_comm::ManagerEndpoint;
use db_local_manager::pool::{OomKillResult, WorkerPool};
use db_local_manager::worker::WorkerEvent;
use db_local_manager::WorkerFactory;
use db_primary_secondary_comm::{
    DistributedBinaryInfo, DistributedMessage, MessageType, PeerTransport, PrimaryTransport,
    TaskInfo,
};
use db_scheduler_api::{MemoryEstimator, Scheduler};

use crate::zip_extract::ExtractionCache;

/// Configuration for the secondary coordinator.
pub struct SecondaryConfig {
    pub secondary_id: String,
    pub num_workers: u32,
    pub ram_bytes: u64,
    pub hostname: String,
    pub keepalive_interval: Duration,
    /// Directory containing ZIP files (for SLURM mode). `None` for local/channel mode.
    pub src_network: Option<PathBuf>,
    /// Temporary directory for extracted binaries. Defaults to a temp dir if `None`.
    pub src_tmp: Option<PathBuf>,
    /// Peer timeout threshold (default: 120s). A peer is considered dead if no
    /// keepalive is received within this duration.
    pub peer_timeout: Duration,
}

impl Default for SecondaryConfig {
    fn default() -> Self {
        Self {
            secondary_id: String::new(),
            num_workers: 1,
            ram_bytes: 1024 * 1024 * 1024,
            hostname: String::new(),
            keepalive_interval: Duration::from_secs(1),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
        }
    }
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
    E: MemoryEstimator,
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

    // Deferred peer messages to send (queued from sync handlers)
    pending_peer_messages: Vec<(String, DistributedMessage<I>)>,

    // Per-worker task request rate limiting
    last_request_time: HashMap<WorkerId, Instant>,
    request_backoff: HashMap<WorkerId, Duration>,

    // SLURM-primary state (populated on promotion + full task list)
    slurm_pending_binaries: Vec<BinaryInfo<I>>,
    slurm_completed: HashSet<String>,
}

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: MemoryEstimator + Clone,
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
            pending_peer_messages: Vec::new(),
            last_request_time: HashMap::new(),
            request_backoff: HashMap::new(),
            slurm_pending_binaries: Vec::new(),
            slurm_completed: HashSet::new(),
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
            ram_gb = self.config.ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            "secondary starting"
        );

        // Initialize workers
        self.initialize_workers(factory).await;

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

    async fn initialize_workers(&mut self, factory: &mut impl WorkerFactory<M>) {
        self.pool
            .initialize(
                self.config.num_workers,
                self.config.ram_bytes,
                &self.scheduler,
                factory,
                false,
            )
            .await;
    }

    async fn send_welcome(&mut self) -> Result<(), String> {
        let msg = DistributedMessage::SecondaryWelcome {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            ram_bytes: self.config.ram_bytes,
            worker_count: self.config.num_workers,
            hostname: self.config.hostname.clone(),
        };
        self.primary_transport.send(msg).await
    }

    async fn send_cert_exchange(&mut self) -> Result<(), String> {
        let (cert_pem, ipv4, ipv6, port) = match &self.peer_cert_info {
            Some(info) => (
                info.public_cert_pem.clone(),
                info.ipv4_address.clone(),
                info.ipv6_address.clone(),
                info.quic_port,
            ),
            None => (String::new(), Some("127.0.0.1".into()), None, 0),
        };

        let msg = DistributedMessage::CertExchange {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            public_cert_pem: cert_pem,
            ipv4_address: ipv4,
            ipv6_address: ipv6,
            quic_port: port,
        };
        self.primary_transport.send(msg).await
    }

    /// Wait for PeerInfo + InitialAssignment + TransferComplete from primary.
    /// Dispatches any initial task assignments to local workers.
    async fn wait_for_setup(&mut self) -> Result<(), String> {
        tracing::debug!("waiting for setup messages from primary");

        let mut got_peer_info = false;
        let mut got_assignment = false;
        let mut got_transfer = false;

        while !got_peer_info || !got_assignment || !got_transfer {
            match self.primary_transport.recv().await {
                Some(msg) => match msg.msg_type() {
                    MessageType::PeerInfo => {
                        got_peer_info = true;
                        if let DistributedMessage::PeerInfo { peers, .. } = &msg {
                            let peer_count = peers
                                .iter()
                                .filter(|p| p.secondary_id != self.config.secondary_id)
                                .count();
                            tracing::info!(peers = peer_count, "received peer list, connecting to peers");
                            self.peer_transport.connect_to_peers(peers).await;
                            tracing::info!(
                                connected = self.peer_transport.peer_count(),
                                "peer connections established"
                            );
                        }
                    }
                    MessageType::InitialAssignment => {
                        got_assignment = true;
                        if let DistributedMessage::InitialAssignment {
                            zip_files,
                            workers_ready,
                            ..
                        } = msg
                        {
                            self.handle_initial_assignment(zip_files, workers_ready).await;
                        }
                        tracing::debug!("received initial assignment");
                    }
                    MessageType::TransferComplete => {
                        got_transfer = true;
                        self.transfer_complete = true;
                        tracing::debug!("received transfer complete");
                    }
                    other => {
                        tracing::debug!(?other, "unexpected message during setup");
                    }
                },
                None => return Err("primary disconnected during setup".into()),
            }
        }

        Ok(())
    }

    /// Handle initial assignment from primary.
    async fn handle_initial_assignment(
        &mut self,
        zip_files: Vec<db_primary_secondary_comm::ZipFileAssignment<I>>,
        workers_ready: Vec<db_primary_secondary_comm::WorkerReadyInfo>,
    ) {
        let mut tasks: Vec<(String, String, DistributedBinaryInfo<I>, String)> = Vec::new();
        for zip_file in &zip_files {
            for entry in &zip_file.binaries {
                tasks.push((
                    zip_file.zip_name.clone(),
                    entry.local_path.clone(),
                    entry.binary_info.clone(),
                    entry.hash.clone(),
                ));
            }
        }

        for (i, (zip_name, local_path, binary_info, hash)) in tasks.into_iter().enumerate() {
            let worker_id = workers_ready
                .get(i)
                .map(|w| w.worker_id)
                .unwrap_or(i as u32);
            let wid = worker_id.min(self.pool.workers.len() as u32 - 1);

            let zip_ref = if zip_name.is_empty() {
                None
            } else {
                Some(zip_name.as_str())
            };
            let resolved_path = self
                .extraction_cache
                .resolve_binary(zip_ref, &local_path, &hash);

            let binary = match resolved_path {
                Some(path) => BinaryInfo {
                    path,
                    size: binary_info.size,
                    identifier: binary_info.identifier.clone(),
                },
                None => distributed_to_binary(&binary_info),
            };

            let estimated = self.estimator.estimate_memory(binary.size);

            if (wid as usize) < self.pool.workers.len() && self.pool.workers[wid as usize].is_idle_state() {
                match self.pool.workers[wid as usize]
                    .assign_task(binary, estimated, false)
                    .await
                {
                    Ok(()) => {
                        self.active_tasks.insert(hash, wid);
                        tracing::info!(
                            worker_id = wid,
                            binary = ?binary_info.identifier,
                            "initial task assigned"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            worker_id = wid,
                            error = %e,
                            "failed to assign initial task"
                        );
                    }
                }
            }
        }
    }

    /// Main task processing loop.
    ///
    /// Uses `tokio::select!` to multiplex between:
    /// - Worker events from the shared channel (spawned poll tasks)
    /// - Messages from primary (task assignments, promotions)
    /// - Messages from peers (keepalives, timeout detection)
    /// - Keepalive timer
    /// - OOM check timer (100ms)
    async fn process_tasks(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        tracing::info!("entering task processing loop");

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        let mut oom_interval = tokio::time::interval(Duration::from_millis(100));

        // Request tasks only for workers that didn't get initial assignments
        for i in 0..self.pool.workers.len() {
            if self.pool.workers[i].is_idle_state() {
                self.request_task_for_worker(i as WorkerId).await?;
            }
        }

        loop {
            // Workers that need restart after disconnect
            let mut workers_to_restart: Vec<WorkerId> = Vec::new();

            tokio::select! {
                event = self.pool.recv_event() => {
                    if let Some(event) = event {
                        let restart = self.handle_worker_event(event).await?;
                        if let Some(wid) = restart {
                            workers_to_restart.push(wid);
                        }
                    }
                }
                msg = self.primary_transport.recv() => {
                    match msg {
                        Some(m) => {
                            self.dispatch_message(m).await?;
                        }
                        None => {
                            tracing::info!("primary disconnected");
                            break;
                        }
                    }
                }
                peer_msg = self.peer_transport.recv_peer() => {
                    if let Some(m) = peer_msg {
                        self.handle_peer_message(m);
                    }
                }
                _ = keepalive_interval.tick() => {
                    self.send_keepalive().await;
                    self.check_peer_timeouts();
                }
                _ = oom_interval.tick() => {
                    self.check_oom(factory).await;
                }
            }

            // Flush any deferred peer messages
            for (peer_id, msg) in std::mem::take(&mut self.pending_peer_messages) {
                let _ = self.peer_transport.send_to_peer(&peer_id, msg).await;
            }

            // Restart any workers that disconnected
            for wid in workers_to_restart {
                self.pool.restart_worker(wid, factory, false).await;
                let _ = self.request_task_for_worker(wid).await;
            }
        }

        Ok(())
    }

    /// Send keepalive to both primary and all peers.
    async fn send_keepalive(&mut self) {
        let active_count = self
            .pool.workers
            .iter()
            .filter(|w| w.current_binary.is_some())
            .count() as u32;
        let msg = DistributedMessage::Keepalive {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            active_workers: active_count,
        };
        // Send to primary
        let _ = self.primary_transport.send(msg.clone()).await;
        // Broadcast to peers
        let _ = self.peer_transport.broadcast(msg).await;
    }

    /// Handle a message from a peer secondary.
    fn handle_peer_message(&mut self, msg: DistributedMessage<I>) {
        match msg {
            DistributedMessage::Keepalive {
                secondary_id,
                timestamp,
                active_workers,
                ..
            } => {
                self.peer_keepalives.insert(secondary_id.clone(), timestamp);
                tracing::trace!(
                    peer = %secondary_id,
                    active_workers,
                    "peer keepalive received"
                );
            }
            DistributedMessage::TaskComplete {
                secondary_id,
                task_hash,
                ..
            } => {
                // Track peer's completed task to avoid duplicate processing
                self.completed_tasks.insert(task_hash.clone());
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    "peer task complete"
                );
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                task_hash,
                error_type,
                ..
            } => {
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    error_type,
                    "peer task failed"
                );
            }
            DistributedMessage::TimeoutDetected {
                timed_out_secondary_id,
                last_seen,
                ..
            } => {
                tracing::warn!(
                    timed_out = %timed_out_secondary_id,
                    last_seen,
                    "peer timeout detected by another secondary"
                );
            }
            DistributedMessage::TimeoutQuery {
                query_secondary_id,
                sender_id,
                ..
            } => {
                // Respond with our last known keepalive for the queried secondary
                let last_keepalive = self.peer_keepalives.get(&query_secondary_id).copied();
                let response: DistributedMessage<I> = DistributedMessage::TimeoutResponse {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    query_secondary_id,
                    last_keepalive,
                };
                tracing::debug!(peer = %sender_id, "timeout query received, queueing response");
                // Queue for async send — will be flushed in the main loop
                self.pending_peer_messages.push((sender_id, response));
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled peer message");
            }
        }
    }

    /// Check for peer timeouts based on keepalive tracking.
    fn check_peer_timeouts(&mut self) {
        let now = timestamp_now();
        let timeout_secs = self.config.peer_timeout.as_secs_f64();
        let mut timed_out = Vec::new();

        for (peer_id, last_seen) in &self.peer_keepalives {
            if now - last_seen > timeout_secs {
                timed_out.push(peer_id.clone());
            }
        }

        for peer_id in timed_out {
            let last_seen = self.peer_keepalives.remove(&peer_id).unwrap_or(0.0);
            tracing::warn!(
                peer = %peer_id,
                last_seen,
                elapsed = now - last_seen,
                "peer timeout detected"
            );
        }
    }

    /// Check memory pressure and kill workers if needed.
    ///
    /// Delegates to `WorkerPool::check_oom`, then reports the killed task
    /// to primary, restarts the worker, and requests a new task.
    async fn check_oom(&mut self, factory: &mut impl WorkerFactory<M>) {
        match self.pool.check_oom(&self.scheduler, self.config.ram_bytes, false) {
            OomKillResult::Killed {
                worker_id,
                reason,
                ..
            } => {
                // Find and report the task as failed
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: "OutOfMemory".into(),
                        error_message: reason,
                    };
                    let _ = self.primary_transport.send(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                // Restart the worker and request a new task
                self.pool.restart_worker(worker_id, factory, false).await;
                let _ = self.request_task_for_worker(worker_id).await;
            }
            OomKillResult::NoAction => {}
        }
    }

    /// Handle a worker event (completion, disconnection, etc.)
    ///
    /// Returns `Some(worker_id)` if the worker needs to be restarted (e.g.
    /// after disconnect). The caller is responsible for calling
    /// `restart_worker` since it requires `&mut factory`.
    async fn handle_worker_event(
        &mut self,
        event: WorkerEvent<I>,
    ) -> Result<Option<WorkerId>, String> {
        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                binary,
                ..
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                // Find the file hash for this worker's task
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);
                    self.completed_tasks.insert(hash.clone());

                    if result.success {
                        // Report completion to primary
                        let msg = DistributedMessage::TaskComplete {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            warnings: result.warnings,
                            filtered: result.filtered,
                        };
                        self.primary_transport.send(msg.clone()).await?;
                        // Broadcast to peers
                        let _ = self.peer_transport.broadcast(msg).await;
                    } else {
                        // Report error to primary
                        let msg = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            error_type: result
                                .error_type
                                .map(|e| format!("{:?}", e))
                                .unwrap_or_else(|| "Unknown".into()),
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                        };
                        self.primary_transport.send(msg.clone()).await?;
                        // Broadcast to peers
                        let _ = self.peer_transport.broadcast(msg).await;
                    }

                    // Request next task for this worker
                    self.request_task_for_worker(worker_id).await?;
                }

                tracing::info!(
                    worker_id,
                    binary = ?binary.as_ref().map(|b| &b.identifier),
                    success = result.success,
                    "task completed"
                );

                Ok(None)
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    "worker disconnected"
                );

                // Find and report the task as failed
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: "NonRecoverable".into(),
                        error_message: result
                            .error_message
                            .unwrap_or_else(|| "Worker disconnected".into()),
                    };
                    let _ = self.primary_transport.send(msg.clone()).await;
                    // Broadcast failure to peers
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                let _ = binary; // binary info already reported

                // Signal that this worker needs restart
                Ok(Some(worker_id))
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
                Ok(None)
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "worker keepalive");
                Ok(None)
            }
            WorkerEvent::Ready { worker_id } => {
                tracing::debug!(worker_id, "worker ready");
                Ok(None)
            }
        }
    }

    const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    /// Request a task from the primary for the given worker.
    /// When acting as SLURM-primary, handles the request locally.
    /// Skips the request if within the backoff window for this worker.
    async fn request_task_for_worker(&mut self, worker_id: WorkerId) -> Result<(), String> {
        // When SLURM-primary, handle task requests locally
        if self.is_slurm_primary && !self.slurm_pending_binaries.is_empty() {
            let available_memory = if (worker_id as usize) < self.pool.workers.len() {
                self.pool.workers[worker_id as usize].reserved_budget
            } else {
                self.config.ram_bytes / self.config.num_workers as u64
            };
            return self
                .handle_slurm_task_request(
                    self.config.secondary_id.clone(),
                    worker_id,
                    available_memory,
                )
                .await;
        }

        let now = Instant::now();
        let backoff = self.request_backoff.get(&worker_id).copied()
            .unwrap_or(Self::INITIAL_BACKOFF);

        if let Some(last) = self.last_request_time.get(&worker_id) {
            if now.duration_since(*last) < backoff {
                return Ok(());
            }
        }

        let available_memory = if (worker_id as usize) < self.pool.workers.len() {
            self.pool.workers[worker_id as usize].reserved_budget
        } else {
            self.config.ram_bytes / self.config.num_workers as u64
        };

        let msg = DistributedMessage::TaskRequest {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            available_memory,
        };
        self.last_request_time.insert(worker_id, now);

        // Double the backoff for next time (capped)
        let next_backoff = (backoff * 2).min(Self::MAX_BACKOFF);
        self.request_backoff.insert(worker_id, next_backoff);

        self.primary_transport.send(msg).await
    }

    /// Reset rate limiting for a worker after a successful task assignment.
    fn reset_request_backoff(&mut self, worker_id: WorkerId) {
        self.request_backoff.remove(&worker_id);
        self.last_request_time.remove(&worker_id);
    }

    /// Dispatch a message from the primary.
    async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match msg {
            DistributedMessage::TaskAssignment {
                worker_id,
                file_hash,
                binary_info,
                zip_file,
                local_path,
                ..
            } => {
                // Resolve binary path: file-ready or ZIP extraction
                let zip_ref = zip_file.as_deref().filter(|z| !z.is_empty());
                let resolved_path = self
                    .extraction_cache
                    .resolve_binary(zip_ref, &local_path, &file_hash);

                let binary = match resolved_path {
                    Some(path) => BinaryInfo {
                        path,
                        size: binary_info.size,
                        identifier: binary_info.identifier.clone(),
                    },
                    None => distributed_to_binary(&binary_info),
                };
                let estimated = self.estimator.estimate_memory(binary.size);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);

                // Find the target worker — prefer the requested one, fall back to any idle
                let target_wid = if self.pool.workers[wid as usize].is_idle_state() {
                    wid
                } else {
                    self.pool.workers
                        .iter()
                        .position(|w| w.is_idle_state())
                        .map(|i| i as u32)
                        .unwrap_or(wid)
                };

                let worker = &mut self.pool.workers[target_wid as usize];
                if worker.is_idle_state() {
                    match worker.assign_task(binary, estimated, false).await {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, target_wid);
                            self.reset_request_backoff(target_wid);
                            tracing::info!(
                                worker_id = target_wid,
                                binary = ?binary_info.identifier,
                                estimated_mb = estimated / (1024 * 1024),
                                "assigned task from primary"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id = target_wid,
                                error = %e,
                                "failed to assign task"
                            );
                            let msg = DistributedMessage::TaskFailed {
                                sender_id: self.config.secondary_id.clone(),
                                timestamp: timestamp_now(),
                                secondary_id: self.config.secondary_id.clone(),
                                worker_id: target_wid,
                                task_hash: file_hash,
                                error_type: "NonRecoverable".into(),
                                error_message: e,
                            };
                            self.primary_transport.send(msg).await?;
                        }
                    }
                } else {
                    tracing::warn!(
                        worker_id = target_wid,
                        "no idle worker available for task assignment"
                    );
                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: target_wid,
                        task_hash: file_hash,
                        error_type: "Recoverable".into(),
                        error_message: "No idle worker available".into(),
                    };
                    self.primary_transport.send(msg).await?;
                }
                Ok(())
            }
            DistributedMessage::PromotePrimary { new_primary_id, .. } => {
                self.is_slurm_primary = new_primary_id == self.config.secondary_id;
                if self.is_slurm_primary {
                    tracing::info!("this secondary has been promoted to SLURM-primary");
                } else {
                    tracing::info!(
                        new_primary = %new_primary_id,
                        "another secondary promoted to SLURM-primary"
                    );
                }
                Ok(())
            }
            DistributedMessage::FullTaskList {
                all_tasks,
                completed_tasks,
                pending_tasks,
                ..
            } => {
                let completed_set: HashSet<String> = completed_tasks.into_iter().collect();
                tracing::info!(
                    total = all_tasks.len(),
                    completed = completed_set.len(),
                    pending = pending_tasks.len(),
                    "received full task list"
                );

                if self.is_slurm_primary {
                    self.populate_slurm_tasks(all_tasks, completed_set);
                }
                Ok(())
            }
            DistributedMessage::TaskRequest {
                secondary_id,
                worker_id,
                available_memory,
                ..
            } if self.is_slurm_primary => {
                self.handle_slurm_task_request(secondary_id, worker_id, available_memory)
                    .await
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }

    /// Populate SLURM-primary pending task list from full task list.
    fn populate_slurm_tasks(
        &mut self,
        all_tasks: Vec<TaskInfo<I>>,
        completed: HashSet<String>,
    ) {
        self.slurm_completed = completed.clone();
        self.slurm_pending_binaries.clear();

        for task in all_tasks {
            if completed.contains(&task.hash)
                || self.completed_tasks.contains(&task.hash)
                || self.active_tasks.contains_key(&task.hash)
            {
                continue;
            }

            let path = task.file_path.as_deref().unwrap_or(&task.local_path);

            // Try to resolve via extraction cache first
            let resolved = self
                .extraction_cache
                .resolve_binary(None, path, &task.hash);

            let binary_path = resolved.unwrap_or_else(|| std::path::PathBuf::from(path));

            self.slurm_pending_binaries.push(BinaryInfo {
                path: binary_path,
                size: task.binary_info.size,
                identifier: task.binary_info.identifier.clone(),
            });
        }

        // Sort by size descending for better packing
        self.slurm_pending_binaries.sort_by(|a, b| b.size.cmp(&a.size));

        tracing::info!(
            pending = self.slurm_pending_binaries.len(),
            completed = self.slurm_completed.len(),
            "populated SLURM-primary task list"
        );
    }

    /// Handle a task request from a peer when acting as SLURM-primary.
    /// Finds a suitable task and sends a TaskAssignment back.
    async fn handle_slurm_task_request(
        &mut self,
        requesting_secondary_id: String,
        worker_id: WorkerId,
        available_memory: u64,
    ) -> Result<(), String> {
        if self.slurm_pending_binaries.is_empty() {
            tracing::debug!(
                secondary = %requesting_secondary_id,
                worker_id,
                "no pending tasks for SLURM-primary assignment"
            );
            return Ok(());
        }

        // Remove any tasks that have been completed since population
        self.slurm_pending_binaries.retain(|b| {
            let hash = format!("{:016x}", {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                b.path.hash(&mut h);
                b.identifier.hash(&mut h);
                h.finish()
            });
            !self.completed_tasks.contains(&hash)
        });

        if self.slurm_pending_binaries.is_empty() {
            return Ok(());
        }

        // Find a task that fits the available memory
        let mut assigned_idx = None;
        for (i, binary) in self.slurm_pending_binaries.iter().enumerate() {
            let estimated = self.estimator.estimate_memory(binary.size);
            if estimated <= available_memory {
                assigned_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = assigned_idx {
            let binary = self.slurm_pending_binaries.remove(idx);
            let file_hash = format!("{:016x}", {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                binary.path.hash(&mut hasher);
                binary.identifier.hash(&mut hasher);
                hasher.finish()
            });

            if requesting_secondary_id == self.config.secondary_id {
                // Assign directly to local worker (avoid recursive dispatch_message cycle)
                let resolved = self
                    .extraction_cache
                    .resolve_binary(None, &binary.path.to_string_lossy(), &file_hash);
                let actual_binary = match resolved {
                    Some(path) => BinaryInfo {
                        path,
                        size: binary.size,
                        identifier: binary.identifier.clone(),
                    },
                    None => binary.clone(),
                };
                let estimated = self.estimator.estimate_memory(actual_binary.size);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
                if self.pool.workers[wid as usize].is_idle_state() {
                    match self.pool.workers[wid as usize]
                        .assign_task(actual_binary, estimated, false)
                        .await
                    {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, wid);
                            self.reset_request_backoff(wid);
                        }
                        Err(e) => {
                            tracing::error!(worker_id = wid, error = %e, "failed to assign SLURM task locally");
                        }
                    }
                }
            } else {
                // Send TaskAssignment to peer
                let msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: requesting_secondary_id.clone(),
                    worker_id,
                    zip_file: None,
                    binary_info: DistributedBinaryInfo {
                        path: binary.path.to_string_lossy().into_owned(),
                        size: binary.size,
                        identifier: binary.identifier.clone(),
                    },
                    local_path: binary.path.to_string_lossy().into_owned(),
                    file_hash,
                };
                let _ = self
                    .peer_transport
                    .send_to_peer(&requesting_secondary_id, msg)
                    .await;
            }

            tracing::info!(
                secondary = %requesting_secondary_id,
                worker_id,
                binary = ?binary.identifier,
                remaining = self.slurm_pending_binaries.len(),
                "SLURM-primary assigned task"
            );
        }

        Ok(())
    }

    async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}

fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn distributed_to_binary<I: Identifier>(info: &DistributedBinaryInfo<I>) -> BinaryInfo<I> {
    BinaryInfo {
        path: std::path::PathBuf::from(&info.path),
        size: info.size,
        identifier: info.identifier.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_comm_api_base::{MessageReceiver, MessageSender};
    use db_manager_runner_comm::{Command, Response};
    use db_scheduler_impl::MemoryStealingScheduler;
    use db_transport_channel::{channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd};
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc as tokio_mpsc;

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[derive(Clone)]
    struct FixedEstimator(u64);
    impl MemoryEstimator for FixedEstimator {
        fn estimate_memory(&self, _size: u64) -> db_comm_api_base::MemoryBytes {
            self.0
        }
    }

    /// No-op peer transport for tests that don't need peers.
    struct NoPeers;
    impl<I: Identifier> PeerTransport<I> for NoPeers {
        async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> {
            Ok(())
        }
        async fn send_to_peer(
            &mut self,
            _peer_id: &str,
            _msg: DistributedMessage<I>,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
            std::future::pending().await
        }
        fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
            None
        }
        fn peer_count(&self) -> usize {
            0
        }
        async fn connect_to_peers(
            &mut self,
            _peers: &[db_primary_secondary_comm::PeerConnectionInfo],
        ) {
        }
    }

    /// Factory that spawns fake workers via channel transport.
    struct FakeWorkerFactory;
    impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
        fn spawn_worker(&mut self, _worker_id: WorkerId) -> (ChannelManagerEnd, Option<u32>) {
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner = runner_end;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessBinary { .. }) => {
                            let _ = runner
                                .send(Response::Done {
                                    warnings: 0,
                                    filtered: 0,
                                })
                                .await;
                        }
                        None => break,
                    }
                }
            });
            (manager_end, None)
        }
    }

    /// Simulate a primary that coordinates with the secondary.
    async fn fake_primary(
        binaries: Vec<BinaryInfo<TestId>>,
        secondary_id: String,
        mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ) {
        let mut pending = binaries;
        let total = pending.len();
        let mut completed = 0usize;

        // Wait for welcome + cert exchange
        let mut got_welcome = false;
        let mut got_cert = false;
        while !got_welcome || !got_cert {
            if let Some(msg) = from_secondary.recv().await {
                match msg.msg_type() {
                    MessageType::SecondaryWelcome => got_welcome = true,
                    MessageType::CertExchange => got_cert = true,
                    _ => {}
                }
            }
        }

        // Send peer list (empty — no peers in test)
        to_secondary
            .send(DistributedMessage::PeerInfo {
                sender_id: "primary".into(),
                timestamp: 0.0,
                peers: vec![],
            })
            .unwrap();

        // Send initial assignment (empty — tasks come via TaskAssignment)
        to_secondary
            .send(DistributedMessage::InitialAssignment {
                sender_id: "primary".into(),
                timestamp: 0.0,
                secondary_id: secondary_id.clone(),
                zip_files: vec![],
                workers_ready: vec![],
            })
            .unwrap();

        // Send transfer complete
        to_secondary
            .send(DistributedMessage::TransferComplete {
                sender_id: "primary".into(),
                timestamp: 0.0,
                total_files: 0,
                total_bytes: 0,
            })
            .unwrap();

        // Process messages from secondary (task requests, completions)
        while completed < total {
            if let Some(msg) = from_secondary.recv().await {
                match msg.msg_type() {
                    MessageType::TaskComplete => {
                        completed += 1;
                    }
                    MessageType::TaskRequest => {
                        if let Some(binary) = pending.pop() {
                            send_task_assignment(
                                &to_secondary,
                                &secondary_id,
                                &binary,
                                extract_worker_id(&msg),
                            );
                        }
                    }
                    MessageType::Keepalive => {}
                    _ => {}
                }
            }
        }

        // Drop channel to signal secondary to stop
        drop(to_secondary);
    }

    fn extract_worker_id(msg: &DistributedMessage<TestId>) -> WorkerId {
        match msg {
            DistributedMessage::TaskRequest { worker_id, .. } => *worker_id,
            _ => 0,
        }
    }

    fn send_task_assignment(
        tx: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
        secondary_id: &str,
        binary: &BinaryInfo<TestId>,
        worker_id: WorkerId,
    ) {
        let hash = format!("hash_{}", binary.identifier.0);
        tx.send(DistributedMessage::TaskAssignment {
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id: secondary_id.into(),
            worker_id,
            zip_file: None,
            binary_info: DistributedBinaryInfo {
                path: binary.path.to_string_lossy().into_owned(),
                size: binary.size,
                identifier: binary.identifier.clone(),
            },
            local_path: binary.path.to_string_lossy().into_owned(),
            file_hash: hash,
        })
        .unwrap();
    }

    fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
        BinaryInfo {
            path: std::path::PathBuf::from(name),
            size,
            identifier: TestId(name.into()),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn secondary_with_real_workers_processes_tasks() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
                let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };

                let config = SecondaryConfig {
                    secondary_id: "sec-0".into(),
                    num_workers: 1,
                    ram_bytes: 1024 * 1024 * 1024,
                    hostname: "test-host".into(),
                    keepalive_interval: Duration::from_secs(60),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                };

                let binaries = vec![
                    make_binary("a", 50),
                    make_binary("b", 60),
                    make_binary("c", 70),
                ];

                let secondary_id = config.secondary_id.clone();
                let primary_handle = tokio::task::spawn_local(fake_primary(
                    binaries,
                    secondary_id,
                    sec_to_pri_rx,
                    pri_to_sec_tx,
                ));

                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    NoPeers,
                    MemoryStealingScheduler,
                    FixedEstimator(100),
                );

                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();

                assert_eq!(secondary.completed_count(), 3);

                primary_handle.await.unwrap();
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn secondary_multi_worker_processes_tasks() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
                let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };

                let config = SecondaryConfig {
                    secondary_id: "sec-0".into(),
                    num_workers: 2,
                    ram_bytes: 2 * 1024 * 1024 * 1024,
                    hostname: "test-host".into(),
                    keepalive_interval: Duration::from_secs(60),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                };

                let binaries: Vec<BinaryInfo<TestId>> = (0..6)
                    .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                    .collect();

                let secondary_id = config.secondary_id.clone();
                let primary_handle = tokio::task::spawn_local(fake_primary(
                    binaries,
                    secondary_id,
                    sec_to_pri_rx,
                    pri_to_sec_tx,
                ));

                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    NoPeers,
                    MemoryStealingScheduler,
                    FixedEstimator(100),
                );

                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();

                assert_eq!(secondary.completed_count(), 6);

                primary_handle.await.unwrap();
            })
            .await;
    }
}
