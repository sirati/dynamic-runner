use std::collections::{HashMap, HashSet};
use std::time::Duration;

use db_comm_api_base::{BinaryInfo, Identifier, ManagerEndpoint, WorkerId};
use db_local_manager::worker::{WorkerEvent, WorkerHandle};
use db_local_manager::WorkerFactory;
use db_primary_secondary_comm::{DistributedBinaryInfo, DistributedMessage, MessageType};
use db_scheduler_api::{MemoryEstimator, Scheduler};

/// Configuration for the secondary coordinator.
pub struct SecondaryConfig {
    pub secondary_id: String,
    pub num_workers: u32,
    pub ram_bytes: u64,
    pub hostname: String,
    pub keepalive_interval: Duration,
}

/// Trait for the secondary's transport to the primary.
pub trait PrimaryTransport<I: Identifier> {
    /// Send a message to the primary.
    fn send(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next message from the primary.
    fn recv(&mut self) -> impl std::future::Future<Output = Option<DistributedMessage<I>>>;
}

/// The secondary coordinator: connects to primary, manages local workers.
///
/// Unlike `LocalManager` which runs a 5-phase pipeline, the secondary receives
/// individual task assignments from the primary and dispatches them to local
/// workers. It reports completions back and requests more work.
pub struct SecondaryCoordinator<PT: PrimaryTransport<I>, M: ManagerEndpoint, S: Scheduler<I>, E: MemoryEstimator, I: Identifier>
{
    config: SecondaryConfig,
    primary_transport: PT,
    scheduler: S,
    estimator: E,

    // Workers
    workers: Vec<WorkerHandle<M, I>>,

    // Task tracking: file_hash -> worker_id
    active_tasks: HashMap<String, WorkerId>,
    completed_tasks: HashSet<String>,

    // State
    transfer_complete: bool,
    is_slurm_primary: bool,
}

impl<PT, M, S, E, I> SecondaryCoordinator<PT, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    M: ManagerEndpoint,
    S: Scheduler<I> + Clone,
    E: MemoryEstimator + Clone,
    I: Identifier,
{
    pub fn new(config: SecondaryConfig, primary_transport: PT, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            primary_transport,
            scheduler,
            estimator,
            workers: Vec::new(),
            active_tasks: HashMap::new(),
            completed_tasks: HashSet::new(),
            transfer_complete: false,
            is_slurm_primary: false,
        }
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
        self.process_tasks().await?;

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
        for i in 0..self.config.num_workers {
            let transport = factory.spawn_worker(i);
            let mut handle = WorkerHandle::new(i, transport);
            handle.reserved_budget = self.scheduler.initial_budget(i, self.config.ram_bytes);
            tracing::info!(
                worker_id = i,
                budget_mb = handle.reserved_budget / (1024 * 1024),
                "worker created"
            );
            self.workers.push(handle);
        }

        // Wait for all workers to become ready
        loop {
            let all_ready = self.workers.iter().all(|w| w.is_ready());
            if all_ready {
                tracing::info!("all workers ready");
                break;
            }
            for worker in &mut self.workers {
                if !worker.is_ready() {
                    worker.poll_ready().await;
                }
            }
            tokio::task::yield_now().await;
        }
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
        let msg = DistributedMessage::CertExchange {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            public_cert_pem: String::new(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 0,
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
                        tracing::debug!("received peer list");
                    }
                    MessageType::InitialAssignment => {
                        got_assignment = true;
                        // Extract tasks from zip_files and assign to workers
                        if let DistributedMessage::InitialAssignment {
                            zip_files,
                            workers_ready,
                            ..
                        } = msg
                        {
                            // Collect (worker_id, binary, hash) tuples from zip entries
                            let mut tasks: Vec<(DistributedBinaryInfo<I>, String)> = Vec::new();
                            for zip_file in &zip_files {
                                for entry in &zip_file.binaries {
                                    tasks.push((entry.binary_info.clone(), entry.hash.clone()));
                                }
                            }

                            // Match tasks to workers using workers_ready info
                            for (i, (binary_info, hash)) in tasks.into_iter().enumerate() {
                                let worker_id = workers_ready
                                    .get(i)
                                    .map(|w| w.worker_id)
                                    .unwrap_or(i as u32);
                                let wid = worker_id.min(self.workers.len() as u32 - 1);

                                let binary = distributed_to_binary(&binary_info);
                                let estimated = self.estimator.estimate_memory(binary.size);

                                if (wid as usize) < self.workers.len()
                                    && self.workers[wid as usize].is_idle_state()
                                {
                                    match self.workers[wid as usize]
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

    /// Main task processing loop.
    ///
    /// Uses `tokio::select!` to multiplex between:
    /// - Messages from primary (task assignments, promotions)
    /// - Worker completion events (poll workers)
    /// - Keepalive timer
    async fn process_tasks(&mut self) -> Result<(), String> {
        tracing::info!("entering task processing loop");

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);

        // Request tasks only for workers that didn't get initial assignments
        for i in 0..self.workers.len() {
            if self.workers[i].is_idle_state() {
                self.request_task_for_worker(i as WorkerId).await?;
            }
        }

        loop {
            // Check if any workers are processing — poll them
            let worker_event = self.poll_any_worker().await;

            if let Some(event) = worker_event {
                self.handle_worker_event(event).await?;
                continue; // Check for more events immediately
            }

            // No worker events — wait for primary message or keepalive
            tokio::select! {
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
                _ = keepalive_interval.tick() => {
                    let active_count = self.workers.iter()
                        .filter(|w| w.current_binary.is_some())
                        .count() as u32;
                    let msg = DistributedMessage::Keepalive {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        active_workers: active_count,
                    };
                    let _ = self.primary_transport.send(msg).await;
                }
            }
        }

        Ok(())
    }

    /// Poll all workers that are currently processing for completion.
    /// Returns the first event found, or None if all are still running.
    async fn poll_any_worker(&mut self) -> Option<WorkerEvent<I>> {
        for worker in &mut self.workers {
            if worker.current_binary.is_some() {
                if let Some(event) = worker.poll_status().await {
                    return Some(event);
                }
            }
        }
        None
    }

    /// Handle a worker event (completion, disconnection, etc.)
    async fn handle_worker_event(&mut self, event: WorkerEvent<I>) -> Result<(), String> {
        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                binary,
                ..
            } => {
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
                            task_hash: hash,
                            warnings: result.warnings,
                            filtered: result.filtered,
                        };
                        self.primary_transport.send(msg).await?;
                    } else {
                        // Report error to primary
                        let msg = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash,
                            error_type: result
                                .error_type
                                .map(|e| format!("{:?}", e))
                                .unwrap_or_else(|| "Unknown".into()),
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                        };
                        self.primary_transport.send(msg).await?;
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
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
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
                    let _ = self.primary_transport.send(msg).await;
                }

                let _ = binary; // binary info already reported
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "worker keepalive");
            }
            WorkerEvent::Ready { worker_id } => {
                tracing::debug!(worker_id, "worker ready");
            }
        }
        Ok(())
    }

    /// Request a task from the primary for the given worker.
    async fn request_task_for_worker(&mut self, worker_id: WorkerId) -> Result<(), String> {
        let available_memory = if (worker_id as usize) < self.workers.len() {
            self.workers[worker_id as usize].reserved_budget
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
        self.primary_transport.send(msg).await
    }

    /// Dispatch a message from the primary.
    async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match msg {
            DistributedMessage::TaskAssignment {
                worker_id,
                file_hash,
                binary_info,
                ..
            } => {
                let binary = distributed_to_binary(&binary_info);
                let estimated = self.estimator.estimate_memory(binary.size);
                let wid = worker_id.min(self.workers.len() as u32 - 1);

                // Find the target worker — prefer the requested one, fall back to any idle
                let target_wid = if self.workers[wid as usize].is_idle_state() {
                    wid
                } else {
                    // Find any idle worker
                    self.workers
                        .iter()
                        .position(|w| w.is_idle_state())
                        .map(|i| i as u32)
                        .unwrap_or(wid) // Fall back to requested worker
                };

                let worker = &mut self.workers[target_wid as usize];
                if worker.is_idle_state() {
                    match worker.assign_task(binary, estimated, false).await {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, target_wid);
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
                            // Report error back
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
                    // Report error: no idle worker
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
                tracing::info!(
                    promoted = self.is_slurm_primary,
                    new_primary = %new_primary_id,
                    "primary promotion"
                );
                Ok(())
            }
            DistributedMessage::FullTaskList { .. } => {
                tracing::info!("received full task list");
                Ok(())
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }

    async fn stop_all_workers(&mut self) {
        for worker in &mut self.workers {
            if !worker.is_stopped() {
                worker.stop().await;
                tracing::info!(worker_id = worker.worker_id, "worker stopped");
            }
        }
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
    use serde::{Deserialize, Serialize};
    use db_comm_api_base::{Command, CommandReceiver, Response, ResponseSender};
    use db_scheduler_impl::MemoryStealingScheduler;
    use db_transport_channel::{channel_pair, ChannelManagerEnd};
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

    /// Channel-based transport to fake primary.
    struct ChannelPrimaryTransport {
        tx: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
        rx: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    }

    impl PrimaryTransport<TestId> for ChannelPrimaryTransport {
        async fn send(&mut self, msg: DistributedMessage<TestId>) -> Result<(), String> {
            self.tx.send(msg).map_err(|e| e.to_string())
        }

        async fn recv(&mut self) -> Option<DistributedMessage<TestId>> {
            self.rx.recv().await
        }
    }

    /// Factory that spawns fake workers via channel transport.
    struct FakeWorkerFactory;
    impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
        fn spawn_worker(&mut self, _worker_id: WorkerId) -> ChannelManagerEnd {
            let (manager_end, runner_end) = channel_pair();
            tokio::spawn(async move {
                let mut runner = runner_end;
                let _ = runner.send_response(Response::Ready).await;
                loop {
                    match runner.recv_command().await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessBinary { .. }) => {
                            let _ = runner
                                .send_response(Response::Done {
                                    warnings: 0,
                                    filtered: 0,
                                })
                                .await;
                        }
                        None => break,
                    }
                }
            });
            manager_end
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

        // Send peer list
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
                        // Assign next pending binary
                        if let Some(binary) = pending.pop() {
                            send_task_assignment(
                                &to_secondary,
                                &secondary_id,
                                &binary,
                                // Use the worker_id from the request
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

    #[tokio::test]
    async fn secondary_with_real_workers_processes_tasks() {
        let _ = tracing_subscriber::fmt::try_init();

        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

        let transport = ChannelPrimaryTransport {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };

        let config = SecondaryConfig {
            secondary_id: "sec-0".into(),
            num_workers: 1,
            ram_bytes: 1024 * 1024 * 1024,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        let secondary_id = config.secondary_id.clone();
        let primary_handle = tokio::spawn(fake_primary(
            binaries,
            secondary_id,
            sec_to_pri_rx,
            pri_to_sec_tx,
        ));

        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            MemoryStealingScheduler,
            FixedEstimator(100),
        );

        let mut factory = FakeWorkerFactory;
        secondary.run(&mut factory).await.unwrap();

        assert_eq!(secondary.completed_count(), 3);

        primary_handle.await.unwrap();
    }

    #[tokio::test]
    async fn secondary_multi_worker_processes_tasks() {
        let _ = tracing_subscriber::fmt::try_init();

        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

        let transport = ChannelPrimaryTransport {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };

        let config = SecondaryConfig {
            secondary_id: "sec-0".into(),
            num_workers: 2,
            ram_bytes: 2 * 1024 * 1024 * 1024,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
        };

        let binaries: Vec<BinaryInfo<TestId>> = (0..6)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        let secondary_id = config.secondary_id.clone();
        let primary_handle = tokio::spawn(fake_primary(
            binaries,
            secondary_id,
            sec_to_pri_rx,
            pri_to_sec_tx,
        ));

        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            MemoryStealingScheduler,
            FixedEstimator(100),
        );

        let mut factory = FakeWorkerFactory;
        secondary.run(&mut factory).await.unwrap();

        assert_eq!(secondary.completed_count(), 6);

        primary_handle.await.unwrap();
    }
}
