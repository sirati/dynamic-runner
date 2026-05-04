
use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, MessageType, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::{distributed_to_binary, timestamp_now};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn initialize_workers(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        let max = self.max_resources();
        self.pool
            .initialize(
                self.config.num_workers,
                &max,
                &self.scheduler,
                factory,
                false,
            )
            .await
    }

    pub(super) async fn send_welcome(&mut self) -> Result<(), String> {
        let msg = DistributedMessage::SecondaryWelcome {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            resources: self.config.max_resources.to_resource_amounts(),
            worker_count: self.config.num_workers,
            hostname: self.config.hostname.clone(),
        };
        self.primary_transport.send(msg).await
    }

    pub(super) async fn send_cert_exchange(&mut self) -> Result<(), String> {
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
    ///
    /// Sends keepalives to the primary on the same interval used by
    /// `process_tasks` so the primary's heartbeat-monitor sees fresh
    /// `last_seen` while this secondary is busy doing peer dials. Peer
    /// connect-to-peers can take 10s × num_peers in the worst case
    /// (QUIC timeout cascade); without an in-loop keepalive the
    /// primary's `keepalive_miss_threshold * keepalive_interval`
    /// budget elapses before any keepalive ever fires from
    /// `process_tasks`, and the primary kills the secondary as
    /// "missed keepalives" before it's even fully connected. The
    /// keepalive payload's `active_workers=0` is honest here — no
    /// task is dispatched until peer-setup completes.
    pub(super) async fn wait_for_setup(&mut self) -> Result<(), String> {
        tracing::debug!("waiting for setup messages from primary");

        let mut got_peer_info = false;
        let mut got_assignment = false;
        let mut got_transfer = false;
        let mut keepalive_interval =
            tokio::time::interval(self.config.keepalive_interval);

        while !got_peer_info || !got_assignment || !got_transfer {
            tokio::select! {
                received = self.primary_transport.recv() => {
                    let msg = match received {
                        Some(m) => m,
                        None => return Err("primary disconnected during setup".into()),
                    };
                    match msg.msg_type() {
                        MessageType::PeerInfo => {
                            got_peer_info = true;
                            if let DistributedMessage::PeerInfo { peers, .. } = &msg {
                                let peer_count = peers
                                    .iter()
                                    .filter(|p| p.secondary_id != self.config.secondary_id)
                                    .count();
                                tracing::info!(peers = peer_count, "received peer list, kicking off peer dials");
                                // Non-blocking: spawns per-peer dial tasks
                                // and returns immediately so the surrounding
                                // select can keep ticking keepalives.
                                // Successful dials register through the same
                                // channel the accept loop uses; unreachable
                                // peers fail silently in the background and
                                // never block the secondary's primary-link.
                                self.peer_transport.connect_to_peers(peers).await;
                            }
                        }
                        MessageType::InitialAssignment => {
                            got_assignment = true;
                            if let DistributedMessage::InitialAssignment {
                                zip_files,
                                workers_ready,
                                staged_files,
                                pre_staged_mode,
                                uses_file_based_items,
                                ..
                            } = msg
                            {
                                self.set_pre_staged_mode(pre_staged_mode);
                                self.set_uses_file_based_items(uses_file_based_items);
                                self.handle_initial_assignment(
                                    zip_files,
                                    workers_ready,
                                    staged_files,
                                )
                                .await;
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
                    }
                }
                _ = keepalive_interval.tick() => {
                    self.send_keepalive().await;
                }
            }
        }

        Ok(())
    }

    /// Handle initial assignment from primary. `staged_files` carries
    /// the per-secondary StageFile records that used to ride as
    /// separate `DistributedMessage::StageFile` messages flushed just
    /// before this one — those messages would land in
    /// `wait_for_setup`'s "unexpected message during setup" arm and
    /// be silently dropped, so the inline form is now the path.
    /// Register them in the extraction cache BEFORE iterating
    /// per-task assignments so resolution succeeds for every task in
    /// this batch instead of every task being routed through the
    /// fail-loud guard and re-enqueued.
    pub(super) async fn handle_initial_assignment(
        &mut self,
        zip_files: Vec<dynrunner_protocol_primary_secondary::ZipFileAssignment<I>>,
        workers_ready: Vec<dynrunner_protocol_primary_secondary::WorkerReadyInfo>,
        staged_files: Vec<dynrunner_protocol_primary_secondary::StagedFileRecord>,
    ) {
        for record in &staged_files {
            self.stage_and_register(
                &record.file_hash,
                &record.content_hash,
                &record.src_path,
                &record.dest_path,
            );
        }

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
            let resolved_path =
                self.resolve_for_dispatch(zip_ref, &local_path, &hash);

            // Same fail-loud guard as the operational dispatch path
            // (see `dispatch::report_unresolvable_task`). Without
            // this, a misconfigured secondary or a StageFile-vs-
            // InitialAssignment race silently passes the primary's
            // filesystem-view path through to the worker, which fails
            // at exec time and triggers a Recoverable re-enqueue —
            // pushing the same task into the operational loop
            // (where the dispatch.rs guard now correctly fails it
            // NonRecoverable). Failing fast here makes the two paths
            // behave consistently and avoids the wasted re-enqueue.
            match self
                .report_unresolvable_task(wid, &hash, &local_path, &resolved_path)
                .await
            {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        worker_id = wid,
                        error = %e,
                        "failed to send TaskFailed for unresolvable initial-assignment task"
                    );
                    continue;
                }
            }

            // Hydrate from the wire info first (preserves
            // phase/type/affinity/payload), then override the path
            // if extraction-cache resolution found a local copy.
            let mut binary = distributed_to_binary(&binary_info);
            if let Some(path) = resolved_path {
                binary.path = path;
            }

            let estimated = self.estimator.estimate(&binary);

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
}
