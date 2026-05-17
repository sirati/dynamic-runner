
use std::time::Instant;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, MessageType, PeerTransport,
    SecondarySetupBootstrap, SetupBootstrap, SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::{distributed_to_binary, timestamp_now};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
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
                // Operator-supplied via `--mem-manager-reserved` →
                // SecondaryConfig → here. `None` = skip nesting
                // (legacy flat layout); `Some(n)` = create the
                // workers cgroup and reserve n bytes for the
                // secondary process. See SecondaryConfig docs.
                self.config.mem_manager_reserved_bytes,
            )
            .await
    }

    pub(super) async fn send_welcome(&mut self) -> Result<(), String> {
        // Step 10: route the setup-phase frame through the narrow
        // `SetupBootstrap` surface. The underlying wire stays the
        // same `primary_transport`; the call-site type is what
        // changed — `SetupBootstrapMessage` accepts only the three
        // setup variants, so a refactor that accidentally adds a
        // runtime frame here fails at compile time. The adapter is
        // built fresh for this single send + dropped, releasing the
        // borrow before the next phase's call site claims
        // `&mut self.primary_transport` again.
        let msg = SetupBootstrapMessage::SecondaryWelcome {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            resources: self.config.max_resources.to_resource_amounts(),
            worker_count: self.config.num_workers,
            hostname: self.config.hostname.clone(),
            // Task #36: surface observer status to the primary so
            // peer broadcasts can carry it. The primary stores this
            // on its per-secondary connection state and fans it out
            // via PeerInfo's `PeerConnectionInfo.is_observer`,
            // letting OTHER secondaries filter observers from their
            // `lowest_alive` candidate selection in election.
            is_observer: self.config.is_observer,
        };
        let mut bootstrap = SecondarySetupBootstrap::new(&mut self.primary_transport);
        SetupBootstrap::<I>::send(&mut bootstrap, msg).await
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

        let msg = SetupBootstrapMessage::CertExchange {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            public_cert_pem: cert_pem,
            ipv4_address: ipv4,
            ipv6_address: ipv6,
            quic_port: port,
        };
        let mut bootstrap = SecondarySetupBootstrap::new(&mut self.primary_transport);
        SetupBootstrap::<I>::send(&mut bootstrap, msg).await
    }

    /// Wait for PeerInfo + InitialAssignment + TransferComplete from primary.
    /// Dispatches any initial task assignments to local workers.
    ///
    /// Plain serial recv loop. The earlier 1cb8cb8 version wrapped this
    /// in a `tokio::select!` that ALSO drove a keepalive_interval ticker
    /// — to keep the primary's heartbeat-monitor seeing fresh
    /// `last_seen` during the multi-second peer-dial cascade in
    /// `connect_to_peers`. After c9a7808 made `connect_to_peers`
    /// non-blocking (per-peer dials run as `spawn_local` tasks),
    /// `wait_for_setup` itself completes in <50ms in normal cases —
    /// well inside any reasonable
    /// `keepalive_miss_threshold * keepalive_interval` budget — and
    /// the in-loop keepalive is no longer load-bearing. Worse, the
    /// select! shape introduced cancellation hazards: when the tick
    /// arm fired between iterations it cancelled an in-flight
    /// `primary_transport.recv()` future, and partially-decoded
    /// inbound messages could be lost depending on the transport
    /// impl's cancellation safety. Reverting to the simple await
    /// removes that hazard. If a future change reintroduces blocking
    /// inside this function, prefer spawning a separate keepalive-
    /// emitter task over racing the recv with select.
    pub(super) async fn wait_for_setup(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
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
                            tracing::info!(peers = peer_count, "received peer list, kicking off peer dials");
                            // Observer-set replication no longer rides
                            // PeerInfo: the primary originates one
                            // `ClusterMutation::PeerJoined { is_observer:
                            // true }` per observer secondary
                            // immediately after this PeerInfo broadcast
                            // (`primary/peer_setup.rs::send_peer_lists`),
                            // and the receiver applies them via the
                            // standard `apply_cluster_mutations` path —
                            // the same single-writer CRDT channel that
                            // serves every other replicated field. The
                            // `PeerConnectionInfo.is_observer` flag on
                            // the wire frame is retained for backwards
                            // compatibility (Batch D's wider peer-
                            // lifecycle plumbing will consume it) but
                            // is no longer the source of truth for
                            // `RoleTable.observers`.
                            // Non-blocking: per-peer dials run as
                            // spawn_local tasks; returns immediately.
                            self.peer_transport.connect_to_peers(peers).await;
                            // Arm the peer-mesh watchdog. 30s = 10s
                            // QUIC timeout + 10s WSS fallback timeout
                            // + 10s slack for the accept side to
                            // finish handshakes that completed near
                            // the deadline. After this point a 0-peer
                            // count means "the cluster blocks
                            // peer-direct connectivity" rather than
                            // "the dials are still in flight".
                            if peer_count > 0 {
                                self.peer_dial_count = peer_count as u32;
                                self.peer_mesh_check_at =
                                    Some(Instant::now() + std::time::Duration::from_secs(30));
                            }
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
                                factory,
                            )
                            .await;
                        }
                        tracing::info!("received initial assignment");
                    }
                    MessageType::TransferComplete => {
                        got_transfer = true;
                        self.transfer_complete = true;
                        tracing::info!("received transfer complete");
                    }
                    MessageType::ClusterMutation => {
                        // Setup-phase ClusterMutation broadcasts (e.g.
                        // the primary's `seed_cluster_state` TaskAdded
                        // batch fired between Phase 4 and Phase 5)
                        // must update the local mirror or the
                        // post-setup view starts out incomplete. CRDT
                        // semantics make this idempotent against any
                        // re-applied mutation post-setup.
                        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
                            self.apply_cluster_mutations(mutations);
                        }
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
        factory: &mut impl WorkerFactory<M>,
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
            // phase/type/affinity/payload), then surface the locally-
            // resolved on-disk path via the dedicated `resolved_path`
            // field. `binary.path` stays as the wire-supplied
            // identifier so consumers' `task.relative_path` keeps
            // its mirror-against-source-tree meaning regardless of
            // where the secondary's extraction cache landed the file.
            let mut binary = distributed_to_binary(&binary_info);
            if let Some(path) = resolved_path {
                binary.resolved_path = Some(path);
            }

            let estimated = self.estimator.estimate(&binary);

            if (wid as usize) < self.pool.workers.len() && self.pool.workers[wid as usize].is_idle_state() {
                // Per-type subprocess dispatch: bind the worker's
                // loaded TypeId to this task's `type_id` (no-op fast
                // path when they already match — the dominant case).
                if let Err(e) = self
                    .pool
                    .ensure_worker_for_type(wid, &binary.type_id, factory, false)
                    .await
                {
                    tracing::error!(
                        worker_id = wid,
                        error = %e,
                        type_id = %binary.type_id,
                        "failed to ensure worker type for initial task; skipping"
                    );
                    continue;
                }
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
