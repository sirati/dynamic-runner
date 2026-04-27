use std::collections::{HashMap, HashSet};
use std::time::Duration;

use db_comm_api_base::{BinaryInfo, Identifier, ResourceMap};
use db_primary_secondary_comm::{
    DistributedBinaryInfo, DistributedMessage, MessageType, PeerConnectionInfo,
    SecondaryTransport, TaskInfo, WorkerReadyInfo, ZipBinaryEntry, ZipFileAssignment,
};
use db_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};

use crate::state::{SecondaryConnection, SecondaryConnectionState};

/// Configuration for the primary coordinator.
pub struct PrimaryConfig {
    pub node_id: String,
    pub num_secondaries: u32,
    pub connect_timeout: Duration,
    pub peer_timeout: Duration,
}

impl Default for PrimaryConfig {
    fn default() -> Self {
        Self {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(600),
            peer_timeout: Duration::from_secs(300),
        }
    }
}

/// Virtual worker tracked by the authoritative primary for each remote worker.
#[derive(Debug, Clone)]
struct RemoteWorkerState<I: Identifier> {
    worker_id: u32,
    secondary_id: String,
    resource_budgets: ResourceMap,
    current_task: Option<BinaryInfo<I>>,
    estimated_resources: ResourceMap,
    is_idle: bool,
}

impl<I: Identifier> RemoteWorkerState<I> {
    fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.resource_budgets.clone(),
            actual_usage: ResourceMap::new(),
            is_idle: self.is_idle,
            is_opportunistic: false,
            has_initial_assignment: self.current_task.is_some(),
            current_task: self.current_task.clone(),
            estimated_usage: self.estimated_resources.clone(),
        }
    }
}

/// The primary coordinator: orchestrates work across secondaries.
///
/// Generic over `T: SecondaryTransport<I>` so it works with both QUIC connections
/// and in-process channels for testing.
pub struct PrimaryCoordinator<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> {
    config: PrimaryConfig,
    transport: T,
    scheduler: S,
    estimator: E,

    // Secondary state
    secondaries: HashMap<String, SecondaryConnectionState>,

    // Worker tracking (virtual workers across all secondaries)
    workers: Vec<RemoteWorkerState<I>>,

    // Task state
    total_tasks: usize,
    all_binaries: Vec<BinaryInfo<I>>,
    pending_binaries: Vec<BinaryInfo<I>>,
    completed_tasks: HashSet<String>,
    failed_tasks: HashSet<String>,

    // SLURM-primary promotion
    slurm_primary_id: Option<String>,
}

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub fn new(config: PrimaryConfig, transport: T, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            transport,
            scheduler,
            estimator,
            secondaries: HashMap::new(),
            workers: Vec::new(),
            total_tasks: 0,
            all_binaries: Vec::new(),
            pending_binaries: Vec::new(),
            completed_tasks: HashSet::new(),
            failed_tasks: HashSet::new(),
            slurm_primary_id: None,
        }
    }

    pub fn completed_count(&self) -> usize {
        self.completed_tasks.len()
    }

    pub fn failed_count(&self) -> usize {
        self.failed_tasks.len()
    }

    pub fn secondary_count(&self) -> usize {
        self.secondaries.len()
    }

    /// Run the full coordination pipeline.
    pub async fn run(&mut self, binaries: Vec<BinaryInfo<I>>) -> Result<(), String> {
        self.all_binaries = binaries.clone();
        self.pending_binaries = binaries;
        self.total_tasks = self.pending_binaries.len();
        let total = self.total_tasks;
        tracing::info!(total, num_secondaries = self.config.num_secondaries, "primary starting");

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections().await?;

        // Phase 3: Send peer lists
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary)
        self.wait_for_peer_connections().await?;

        // Phase 5: Initial assignment
        self.perform_initial_assignment().await?;

        // Phase 6: Send transfer complete
        self.send_transfer_complete().await?;

        // Phase 7: Promote SLURM-primary
        self.promote_slurm_primary().await?;

        // Phase 8: Send full task list to SLURM-primary
        self.send_full_task_list().await?;

        // Phase 9: Operational loop
        self.operational_loop().await?;

        tracing::info!(
            completed = self.completed_tasks.len(),
            failed = self.failed_tasks.len(),
            total,
            "primary finished"
        );

        Ok(())
    }

    // ── Phase 1+2: Wait for Welcomes and Cert Exchanges ──

    async fn wait_for_connections(&mut self) -> Result<(), String> {
        tracing::info!("waiting for {} secondaries", self.config.num_secondaries);

        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let expected = self.config.num_secondaries as usize;

        loop {
            // Check if all secondaries have completed cert exchange
            let cert_done = self.secondaries.values()
                .filter(|s| s.is_at_least_cert_exchanged())
                .count();
            if cert_done >= expected {
                break;
            }

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => return Err("transport closed".into()),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(format!(
                        "timeout waiting for secondaries: {}/{}",
                        self.secondaries.len(),
                        expected
                    ));
                }
            }
        }

        tracing::info!("all {} secondaries connected", self.secondaries.len());
        Ok(())
    }

    /// Central message dispatcher — routes incoming messages by type.
    async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match msg.msg_type() {
            MessageType::SecondaryWelcome => self.handle_welcome(msg),
            MessageType::CertExchange => self.handle_cert_exchange(msg),
            MessageType::TaskRequest => self.handle_task_request(msg).await?,
            MessageType::TaskComplete => self.handle_task_complete(msg),
            MessageType::TaskFailed => self.handle_task_failed(msg),
            MessageType::Keepalive => { /* consume silently */ }
            other => {
                tracing::debug!(?other, "unhandled message type");
            }
        }
        Ok(())
    }

    fn handle_welcome(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::SecondaryWelcome {
            secondary_id,
            resources,
            worker_count,
            hostname,
            ..
        } = msg
        {
            let ram_bytes = resources.iter()
                .find(|r| r.kind == db_comm_api_base::ResourceKind::Memory)
                .map(|r| r.amount)
                .unwrap_or(0);
            tracing::info!(
                secondary = %secondary_id,
                workers = worker_count,
                ram_gb = ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                "secondary connected"
            );

            let conn = SecondaryConnection::new(secondary_id.clone());
            let conn = conn.receive_welcome(worker_count, resources, hostname, 0, None);
            self.secondaries.insert(
                secondary_id,
                SecondaryConnectionState::Handshaking(conn),
            );
        }
    }

    fn handle_cert_exchange(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::CertExchange {
            secondary_id,
            public_cert_pem,
            ipv4_address,
            ipv6_address,
            quic_port,
            ..
        } = msg
        {
            if let Some(state) = self.secondaries.remove(&secondary_id) {
                if let SecondaryConnectionState::Handshaking(conn) = state {
                    let conn = conn.receive_cert_exchange(
                        public_cert_pem,
                        ipv4_address,
                        ipv6_address,
                        quic_port,
                    );
                    self.secondaries.insert(
                        secondary_id.clone(),
                        SecondaryConnectionState::CertExchanging(conn),
                    );
                    tracing::debug!(secondary = %secondary_id, "cert exchange received");
                } else {
                    self.secondaries.insert(secondary_id, state);
                }
            }
        }
    }

    // ── Phase 3: Send Peer Lists ──

    async fn send_peer_lists(&mut self) -> Result<(), String> {
        tracing::info!("sending peer lists");

        let peers: Vec<PeerConnectionInfo> = self
            .secondaries
            .values()
            .map(|s| PeerConnectionInfo {
                secondary_id: s.id().to_string(),
                cert: s.cert_pem().unwrap_or("").to_string(),
                ipv4: s.ipv4().map(|s| s.to_string()),
                ipv6: None,
                port: s.quic_port(),
            })
            .collect();

        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in &secondary_ids {
            let msg = DistributedMessage::PeerInfo {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                peers: peers.clone(),
            };
            self.transport.send_to(secondary_id, msg).await?;
        }

        // Transition all from CertExchanging -> PeerDiscovery
        for secondary_id in &secondary_ids {
            if let Some(state) = self.secondaries.remove(secondary_id) {
                if let SecondaryConnectionState::CertExchanging(conn) = state {
                    self.secondaries.insert(
                        secondary_id.clone(),
                        SecondaryConnectionState::PeerDiscovery(conn.begin_peer_discovery()),
                    );
                } else {
                    self.secondaries.insert(secondary_id.clone(), state);
                }
            }
        }

        Ok(())
    }

    // ── Phase 4: Wait for Peer Connections ──

    async fn wait_for_peer_connections(&mut self) -> Result<(), String> {
        // For single-secondary, skip peer connection wait
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();

        for secondary_id in secondary_ids {
            if let Some(state) = self.secondaries.remove(&secondary_id) {
                if let SecondaryConnectionState::PeerDiscovery(conn) = state {
                    self.secondaries.insert(
                        secondary_id,
                        SecondaryConnectionState::InitialAssigning(conn.peers_ready()),
                    );
                } else {
                    self.secondaries.insert(secondary_id, state);
                }
            }
        }

        Ok(())
    }

    // ── Phase 5: Initial Assignment ──

    async fn perform_initial_assignment(&mut self) -> Result<(), String> {
        tracing::info!("performing initial assignment");

        let mut global_worker_id: u32 = 0;
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();

        for secondary_id in &secondary_ids {
            let state = self.secondaries.get(secondary_id).unwrap();
            let num_workers = state.num_workers();
            let ram_bytes = state.resources().iter()
                .find(|r| r.kind == db_comm_api_base::ResourceKind::Memory)
                .map(|r| r.amount)
                .unwrap_or(0);
            let max_res = db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, ram_bytes)]);

            for local_idx in 0..num_workers {
                let budget = self.scheduler.initial_budget(local_idx, &max_res);
                self.workers.push(RemoteWorkerState {
                    worker_id: global_worker_id,
                    secondary_id: secondary_id.clone(),
                    resource_budgets: budget,
                    current_task: None,
                    estimated_resources: ResourceMap::new(),
                    is_idle: true,
                });
                global_worker_id += 1;
            }
        }

        // Sort pending by size descending for better packing
        self.pending_binaries.sort_by(|a, b| b.size.cmp(&a.size));

        // Perform initial assignment for each worker
        let mut assignments_per_secondary: HashMap<String, Vec<(u32, BinaryInfo<I>, ResourceMap)>> =
            HashMap::new();
        let mut total_assigned_resources = ResourceMap::new();

        for worker_idx in 0..self.workers.len() {
            let worker_info = self.workers[worker_idx].budget_info();
            let max_res = self.workers[worker_idx].resource_budgets.clone();
            let decision = self.scheduler.assign_initial(
                &worker_info,
                &self.pending_binaries,
                &total_assigned_resources,
                &max_res,
                &self.estimator,
            );

            if let AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                let binary = self.pending_binaries.remove(binary_index);
                total_assigned_resources.add(&estimated_usage);

                let secondary_id = self.workers[worker_idx].secondary_id.clone();
                // Compute local worker index within that secondary
                let local_worker_id = self.workers[..worker_idx + 1]
                    .iter()
                    .filter(|w| w.secondary_id == secondary_id)
                    .count() as u32
                    - 1;

                self.workers[worker_idx].current_task = Some(binary.clone());
                self.workers[worker_idx].estimated_resources = estimated_usage.clone();
                self.workers[worker_idx].is_idle = false;

                assignments_per_secondary
                    .entry(secondary_id)
                    .or_default()
                    .push((local_worker_id, binary, estimated_usage));
            }
        }

        // Send initial assignments to each secondary
        for (secondary_id, assignments) in &assignments_per_secondary {
            let zip_files = vec![ZipFileAssignment {
                zip_name: String::new(),
                binaries: assignments
                    .iter()
                    .map(|(_, binary, _)| ZipBinaryEntry {
                        local_path: binary.path.to_string_lossy().into_owned(),
                        binary_info: binary_to_distributed(binary),
                        hash: compute_task_hash(binary),
                    })
                    .collect(),
            }];

            let workers_ready: Vec<WorkerReadyInfo> = assignments
                .iter()
                .map(|(worker_id, _, est_res)| WorkerReadyInfo {
                    worker_id: *worker_id,
                    resource_budgets: est_res.iter()
                        .map(|(kind, amount)| db_comm_api_base::ResourceAmount { kind, amount })
                        .collect(),
                })
                .collect();

            let msg = DistributedMessage::InitialAssignment {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: secondary_id.clone(),
                zip_files,
                workers_ready,
            };
            self.transport.send_to(secondary_id, msg).await?;
        }

        // Transition all to Operational
        for secondary_id in &secondary_ids {
            if let Some(state) = self.secondaries.remove(secondary_id) {
                let new_state = match state {
                    SecondaryConnectionState::InitialAssigning(conn) => {
                        SecondaryConnectionState::Operational(conn.assignments_sent())
                    }
                    other => other,
                };
                self.secondaries.insert(secondary_id.clone(), new_state);
            }
        }

        let assigned: usize = assignments_per_secondary.values().map(|v| v.len()).sum();
        tracing::info!(
            assigned,
            remaining = self.pending_binaries.len(),
            "initial assignment complete"
        );

        Ok(())
    }

    // ── Phase 6: Transfer Complete ──

    async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in secondary_ids {
            let msg = DistributedMessage::TransferComplete {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                total_files: 0,
                total_bytes: 0,
            };
            self.transport.send_to(&secondary_id, msg).await?;
        }
        tracing::info!("transfer complete sent to all secondaries");
        Ok(())
    }

    // ── Phase 7: Operational Loop ──

    async fn operational_loop(&mut self) -> Result<(), String> {
        tracing::info!("entering operational loop");

        loop {
            // Check termination: all tasks accounted for
            if self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks {
                tracing::info!("all tasks completed or failed");
                break;
            }

            let active_workers = self.workers.iter().filter(|w| w.current_task.is_some()).count();
            if self.pending_binaries.is_empty() && active_workers == 0 {
                tracing::info!("no pending binaries and no active workers");
                break;
            }

            // Use a timeout on recv to avoid stalling indefinitely if a
            // secondary disconnects while processing a task. The timeout
            // is generous — if no message arrives in 5 minutes and there
            // are in-flight tasks, something is wrong.
            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => {
                            tracing::info!("transport closed");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    let active = self.workers.iter().filter(|w| w.current_task.is_some()).count();
                    if active > 0 {
                        tracing::warn!(
                            active_workers = active,
                            completed = self.completed_tasks.len(),
                            failed = self.failed_tasks.len(),
                            total = self.total_tasks,
                            "operational loop timeout with active workers, marking in-flight tasks as failed"
                        );
                        // Mark all in-flight tasks as failed
                        for worker in &mut self.workers {
                            if let Some(binary) = worker.current_task.take() {
                                let hash = compute_task_hash(&binary);
                                self.failed_tasks.insert(hash);
                                worker.estimated_resources = ResourceMap::new();
                                worker.is_idle = true;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // ── Phase 7: Promote SLURM-primary ──

    async fn promote_slurm_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.slurm_primary_id = Some(first_id.clone());
            tracing::info!(slurm_primary = %first_id, "promoting secondary to SLURM-primary");

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
            };
            self.transport.send_to(&first_id, msg).await?;
        }
        Ok(())
    }

    // ── Phase 8: Send full task list ──

    async fn send_full_task_list(&mut self) -> Result<(), String> {
        let slurm_id = match &self.slurm_primary_id {
            Some(id) => id.clone(),
            None => return Ok(()),
        };

        let all_tasks: Vec<TaskInfo<I>> = self
            .all_binaries
            .iter()
            .map(|binary| {
                let hash = compute_task_hash(binary);
                TaskInfo {
                    local_path: binary.path.to_string_lossy().into_owned(),
                    binary_info: binary_to_distributed(binary),
                    hash: hash.clone(),
                    file_path: Some(binary.path.to_string_lossy().into_owned()),
                }
            })
            .collect();

        // Include both completed tasks and currently in-flight tasks as "completed"
        // so the SLURM-primary doesn't re-assign tasks that are already being processed
        let active_hashes: HashSet<String> = self
            .workers
            .iter()
            .filter_map(|w| w.current_task.as_ref().map(compute_task_hash))
            .collect();
        let excluded: HashSet<String> = self
            .completed_tasks
            .union(&active_hashes)
            .cloned()
            .collect();

        let completed_list: Vec<String> = excluded.iter().cloned().collect();
        let pending_list: Vec<String> = all_tasks
            .iter()
            .filter(|t| !excluded.contains(&t.hash))
            .map(|t| t.hash.clone())
            .collect();

        let msg = DistributedMessage::FullTaskList {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            all_tasks,
            completed_tasks: completed_list,
            pending_tasks: pending_list,
        };
        self.transport.send_to(&slurm_id, msg).await?;

        tracing::info!(
            slurm_primary = %slurm_id,
            total = self.all_binaries.len(),
            "sent full task list"
        );
        Ok(())
    }

    async fn handle_task_request(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            ref secondary_id,
            worker_id,
            ref available_resources,
            ..
        } = msg
        {
            let available_res: ResourceMap = available_resources.iter()
                .map(|r| (r.kind, r.amount))
                .collect();
            // Find matching worker
            let mut target_idx = None;
            let mut local_idx: u32 = 0;
            for (idx, w) in self.workers.iter().enumerate() {
                if w.secondary_id == *secondary_id {
                    if local_idx == worker_id {
                        target_idx = Some(idx);
                        break;
                    }
                    local_idx += 1;
                }
            }

            let mut assigned = false;

            if let Some(idx) = target_idx {
                // Mark worker idle
                self.workers[idx].current_task = None;
                self.workers[idx].estimated_resources = ResourceMap::new();
                self.workers[idx].is_idle = true;
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Try to assign from local pending
                if !self.pending_binaries.is_empty() {
                    let worker_info = self.workers[idx].budget_info();
                    let all_infos: Vec<WorkerBudgetInfo<I>> =
                        self.workers.iter().map(|w| w.budget_info()).collect();
                    let max_res = self.workers[idx].resource_budgets.clone();

                    let decision = self.scheduler.assign_normal(
                        &worker_info,
                        &all_infos,
                        &self.pending_binaries,
                        &max_res,
                        &self.estimator,
                        false,
                    );

                    if let AssignmentDecision::Assign {
                        binary_index,
                        estimated_usage,
                        ..
                    } = decision
                    {
                        let binary = self.pending_binaries.remove(binary_index);
                        let sec_id = self.workers[idx].secondary_id.clone();
                        self.workers[idx].current_task = Some(binary.clone());
                        self.workers[idx].estimated_resources = estimated_usage;
                        self.workers[idx].is_idle = false;

                        let assignment_msg = DistributedMessage::TaskAssignment {
                            sender_id: self.config.node_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: sec_id.clone(),
                            worker_id,
                            zip_file: None,
                            binary_info: binary_to_distributed(&binary),
                            local_path: binary.path.to_string_lossy().into_owned(),
                            file_hash: compute_task_hash(&binary),
                        };
                        self.transport.send_to(&sec_id, assignment_msg).await?;

                        tracing::debug!(
                            secondary = %sec_id,
                            worker_id,
                            binary = ?binary.identifier,
                            "task assigned"
                        );
                        assigned = true;
                    }
                }
            }

            // If no local assignment was made, relay to SLURM-primary
            if !assigned {
                if let Some(slurm_id) = self.slurm_primary_id.clone() {
                    self.transport.send_to(&slurm_id, msg).await?;
                }
            }
        }
        Ok(())
    }

    fn handle_task_complete(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskComplete {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = msg
        {
            self.completed_tasks.insert(task_hash);

            // Mark the specific worker idle using secondary_id + local worker_id
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        w.current_task = None;
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                completed = self.completed_tasks.len(),
                "task complete"
            );
        }
    }

    fn handle_task_failed(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = msg
        {
            // Find the specific worker and recover the binary if it's a
            // recoverable error so it can be re-assigned to another worker.
            let mut recovered_binary: Option<BinaryInfo<I>> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        recovered_binary = w.current_task.take();
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            if error_type == "Recoverable" {
                // Re-enqueue recoverable failures for assignment to another worker
                if let Some(binary) = recovered_binary {
                    tracing::info!(
                        secondary = %secondary_id,
                        worker_id,
                        error = %error_message,
                        "recoverable failure, re-enqueuing task"
                    );
                    self.pending_binaries.push(binary);
                } else {
                    // Can't recover — no binary info available
                    self.failed_tasks.insert(task_hash);
                }
            } else {
                // Non-recoverable: permanently mark as failed
                self.failed_tasks.insert(task_hash);
            }

            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                error_type = %error_type,
                error = %error_message,
                "task failed"
            );
        }
    }
}

// ── Helper functions ──

fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn binary_to_distributed<I: Identifier>(binary: &BinaryInfo<I>) -> DistributedBinaryInfo<I> {
    DistributedBinaryInfo {
        path: binary.path.to_string_lossy().into_owned(),
        size: binary.size,
        identifier: binary.identifier.clone(),
    }
}

fn compute_task_hash<I: Identifier>(binary: &BinaryInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use db_scheduler_impl::ResourceStealingScheduler;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[derive(Clone)]
    struct FixedEstimator(u64);
    impl ResourceEstimator for FixedEstimator {
        fn estimate(&self, _size: u64) -> db_comm_api_base::ResourceMap {
            db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, self.0)])
        }
    }

    fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
        BinaryInfo {
            path: std::path::PathBuf::from(name),
            size,
            identifier: TestId(name.into()),
        }
    }

    use db_transport_channel::ChannelSecondaryTransportEnd;

    /// Simulate a secondary that sends welcome + cert, then echoes assignments as completions.
    async fn fake_secondary(
        secondary_id: String,
        num_workers: u32,
        ram_bytes: u64,
        mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ) {
        // Send welcome
        outgoing_to_primary
            .send(DistributedMessage::SecondaryWelcome {
                sender_id: secondary_id.clone(),
                timestamp: 0.0,
                secondary_id: secondary_id.clone(),
                resources: vec![db_comm_api_base::ResourceAmount {
                    kind: db_comm_api_base::ResourceKind::Memory,
                    amount: ram_bytes,
                }],
                worker_count: num_workers,
                hostname: "test-host".into(),
            })
            .unwrap();

        // Send cert exchange
        outgoing_to_primary
            .send(DistributedMessage::CertExchange {
                sender_id: secondary_id.clone(),
                timestamp: 0.0,
                secondary_id: secondary_id.clone(),
                public_cert_pem: "FAKE_CERT".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 5000,
            })
            .unwrap();

        // Process messages from primary
        while let Some(msg) = incoming_from_primary.recv().await {
            match msg {
                DistributedMessage::PeerInfo { .. } => {
                    // No peer connections needed in test
                }
                DistributedMessage::InitialAssignment { zip_files, .. } => {
                    // Complete all initially assigned tasks
                    for zip_file in &zip_files {
                        for entry in &zip_file.binaries {
                            outgoing_to_primary
                                .send(DistributedMessage::TaskComplete {
                                    sender_id: secondary_id.clone(),
                                    timestamp: 0.0,
                                    secondary_id: secondary_id.clone(),
                                    worker_id: 0,
                                    task_hash: entry.hash.clone(),
                                    result_data: None,
                                })
                                .unwrap();

                            // Request next task
                            outgoing_to_primary
                                .send(DistributedMessage::TaskRequest {
                                    sender_id: secondary_id.clone(),
                                    timestamp: 0.0,
                                    secondary_id: secondary_id.clone(),
                                    worker_id: 0,
                                    available_resources: vec![db_comm_api_base::ResourceAmount {
                                        kind: db_comm_api_base::ResourceKind::Memory,
                                        amount: ram_bytes,
                                    }],
                                })
                                .unwrap();
                        }
                    }
                }
                DistributedMessage::TransferComplete { .. } => {}
                DistributedMessage::TaskAssignment { file_hash, .. } => {
                    // Complete the assigned task
                    outgoing_to_primary
                        .send(DistributedMessage::TaskComplete {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            worker_id: 0,
                            task_hash: file_hash,
                            result_data: None,
                        })
                        .unwrap();

                    // Request next task
                    outgoing_to_primary
                        .send(DistributedMessage::TaskRequest {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            worker_id: 0,
                            available_resources: vec![db_comm_api_base::ResourceAmount {
                                kind: db_comm_api_base::ResourceKind::Memory,
                                amount: ram_bytes,
                            }],
                        })
                        .unwrap();
                }
                _ => {}
            }
        }
    }

    fn setup_test(
        num_secondaries: u32,
    ) -> (
        ChannelSecondaryTransportEnd<TestId>,
        Vec<(
            String,
            tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
            tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
        )>,
    ) {
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        let mut secondary_ends = Vec::new();

        for i in 0..num_secondaries {
            let id = format!("sec-{i}");
            let (to_sec_tx, to_sec_rx) = tokio_mpsc::unbounded_channel();
            outgoing.insert(id.clone(), to_sec_tx);
            secondary_ends.push((id, to_sec_rx, incoming_tx.clone()));
        }

        (ChannelSecondaryTransportEnd { outgoing, incoming_rx }, secondary_ends)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn single_secondary_processes_all_tasks() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let (transport, secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            primary.run(binaries).await.unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_secondaries_distribute_work() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<BinaryInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            primary.run(binaries).await.unwrap();

            assert_eq!(primary.completed_count(), 6);
            assert_eq!(primary.failed_count(), 0);
        }).await;
    }

    // ── End-to-end tests: real Primary + real Secondary with workers ──

    use db_comm_api_base::{MessageReceiver, MessageSender};
    use db_manager_runner_comm::{Command, Response};
    use db_transport_channel::{channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd};
    use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
    use db_local_manager::WorkerFactory;
    use db_primary_secondary_comm::PeerTransport;

    /// No-op peer transport for tests.
    struct NoPeers;
    impl<I: Identifier> PeerTransport<I> for NoPeers {
        async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> { Ok(()) }
        async fn send_to_peer(&mut self, _peer_id: &str, _msg: DistributedMessage<I>) -> Result<(), String> { Ok(()) }
        async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> { std::future::pending().await }
        fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> { None }
        fn peer_count(&self) -> usize { 0 }
        async fn connect_to_peers(&mut self, _peers: &[db_primary_secondary_comm::PeerConnectionInfo]) {}
    }

    /// Factory that spawns fake workers via channel transport.
    struct FakeWorkerFactory;
    impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
        fn spawn_worker(
            &mut self,
            _worker_id: u32,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner = runner_end;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessTask { .. }) => {
                            let _ = runner
                                .send(Response::Done {
                                    result_data: None,
                                })
                                .await;
                        }
                        None => break,
                    }
                }
            });
            Ok((manager_end, None))
        }
    }

    /// Wire up a real SecondaryCoordinator as a tokio task, connected to the
    /// primary via channels. Returns the secondary's channel ends that should
    /// be plugged into the primary's ChannelTransport.
    fn spawn_real_secondary(
        secondary_id: String,
        num_workers: u32,
        max_resources: db_comm_api_base::ResourceMap,
    ) -> (
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,  // primary→secondary
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
        tokio::task::JoinHandle<usize>,                    // returns completed count
    ) {
        // primary→secondary channel
        let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
        // secondary→primary channel
        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

        let handle = tokio::task::spawn_local(async move {
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };
            let config = SecondaryConfig {
                secondary_id,
                num_workers,
                max_resources,
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
            };
            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();
            secondary.completed_count()
        });

        (pri_to_sec_tx, sec_to_pri_rx, handle)
    }

    /// End-to-end: 1 real primary + 1 real secondary (2 workers), 5 tasks.
    #[tokio::test(flavor = "current_thread")]
    async fn e2e_primary_and_secondary_single_node() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, 1024 * 1024 * 1024u64)]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            // Build primary transport wired to the real secondary
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            // Forward secondary→primary messages into the primary's incoming channel
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<BinaryInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            primary.run(binaries).await.unwrap();

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            // Drop primary to close transport channels, allowing secondaries to exit
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5);
            assert_eq!(failed, 0);
            assert_eq!(sec_completed, 5);
        }).await;
    }

    /// End-to-end: 1 real primary + 2 real secondaries (2 workers each), 10 tasks.
    #[tokio::test(flavor = "current_thread")]
    async fn e2e_primary_and_two_secondaries() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let max_res = db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, 2 * 1024 * 1024 * 1024u64)]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();

            for i in 0..2u32 {
                let secondary_id = format!("sec-{i}");
                let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                    spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());

                outgoing.insert(secondary_id, pri_to_sec_tx);
                sec_handles.push(handle);

                // Forward secondary→primary
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(incoming_tx); // Only forwarding tasks hold senders now

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<BinaryInfo<TestId>> = (0..10)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            primary.run(binaries).await.unwrap();

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            // Drop primary to close transport channels, allowing secondaries to exit
            drop(primary);

            let mut total_sec_completed = 0;
            for handle in sec_handles {
                total_sec_completed += handle.await.unwrap();
            }

            assert_eq!(completed, 10);
            assert_eq!(failed, 0);
            assert_eq!(total_sec_completed, 10);
        }).await;
    }
}
