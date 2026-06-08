use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedBinaryInfo, DistributedMessage, MessageType, SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::{distributed_to_binary, timestamp_now};

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Build and initialise the local worker pool, returning it for the
    /// caller to move INTO the `Configuring` lifecycle state.
    ///
    /// Worker-spawn relocation (typed lifecycle): the pool is no longer a
    /// flat coordinator field initialised before the handshake. It is
    /// built here as the entry action of the `AwaitingPrimary →
    /// Configuring` transition — fired by `wait_for_setup` on the FIRST
    /// primary-originated setup frame (the de-facto announce; PeerInfo
    /// arrives first on the ordered primary link, before
    /// `InitialAssignment` dispatch). If the primary never announces, this
    /// is never called and no worker pool is ever built — exactly the
    /// invariant the typed lifecycle enforces (a not-yet-configured peer
    /// must not spin up worker processes).
    pub(super) async fn initialize_workers(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<dynrunner_manager_local::pool::WorkerPool<M, I>, String> {
        let max = self.max_resources();
        // Two distinct callers materialise nested cgroups here:
        //   * `Some(n)` — operator-supplied `--mem-manager-reserved`
        //     reserves `n` bytes for the secondary process so a
        //     worker kernel-OOM doesn't reap the secondary too;
        //   * `Some(0)` — "create the cgroup leaves but don't
        //     tighten `memory.max`". This is what the memprofile
        //     sampler needs: per-worker subgroups exist
        //     (`WorkerHandle.subcgroup` becomes `Some(...)`) so the
        //     sampler can read `memory.current`, but no enforcement
        //     changes.
        // We pick the latter when memprofile is enabled
        // (`output_dir` set) and no explicit reservation is set,
        // and leave the operator's explicit value untouched when
        // both are configured. `None` (legacy flat layout) when
        // neither feature is opted into. Mirrors
        // `LocalManager::initialize_workers`.
        let mem_manager_reserved_bytes = if self.config.output_dir.is_some() {
            self.config.mem_manager_reserved_bytes.or(Some(0))
        } else {
            self.config.mem_manager_reserved_bytes
        };
        let mut pool = dynrunner_manager_local::pool::WorkerPool::new();
        pool.initialize(
            self.config.num_workers,
            &max,
            &self.scheduler,
            factory,
            false,
            mem_manager_reserved_bytes,
        )
        .await?;
        Ok(pool)
    }

    /// Construct the per-task memory-profile sampler if the operator
    /// enabled it via `config.output_dir`. Relocated faithfully from the
    /// pre-typed-lifecycle `run_until_setup_or_done_inner`: it is now
    /// called from `enter_configuring_on_first_primary_frame`, AFTER the
    /// pool spawns and BEFORE the `InitialAssignment` dispatch, so the
    /// initial-assignment sampler hook still captures the first batch.
    ///
    /// Deferred from `SecondaryCoordinator::new` because the sampler
    /// spawns a background tokio task on the current runtime — at
    /// construction time the caller may not be inside one. The sampler is
    /// constructed regardless of cgroup-v2 availability so its event queue
    /// exists for non-cgroup messages (Disconnected fan-out) and the
    /// secondary-side lifecycle test can pin construction independently of
    /// cgroup-v2. Mirrors `LocalManager::process_binaries`.
    pub(super) fn build_sampler_if_enabled(&mut self) {
        if self.sampler.is_none()
            && let Some(dir) = self.config.output_dir.as_ref()
        {
            self.sampler = Some(
                dynrunner_manager_local::memprofile::MemProfileSampler::spawn(
                    dynrunner_manager_local::memprofile::MemProfileConfig::new(dir.clone()),
                ),
            );
        }
    }

    pub(super) async fn send_welcome(&mut self) -> Result<(), String> {
        // Setup-phase frame. Construction stays narrow-typed
        // (`SetupBootstrapMessage` accepts only the three setup
        // variants, so a runtime frame here fails at compile time),
        // but routing is OPAQUE: the frame ships over the one mesh
        // addressed to `Destination::Primary`. The role cache is cold
        // this early (no `PrimaryChanged` observed), so it resolves to
        // the bootstrap primary host — exactly the primary this welcome
        // is for. Wire bytes are identical (the
        // `From<SetupBootstrapMessage>` conversion is lossless).
        let msg = SetupBootstrapMessage::SecondaryWelcome {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            resources: self.config.max_resources.to_resource_amounts(),
            worker_count: self.config.num_workers,
            hostname: self.config.hostname.clone(),
            // Wire role advertisement: surface observer status to the
            // primary so peer broadcasts can carry it via PeerInfo's
            // `PeerConnectionInfo.is_observer`, letting OTHER secondaries
            // filter observers from `lowest_alive` candidate selection.
            // A compute SecondaryCoordinator is never an observer (the
            // observer role IS the standalone ObserverCoordinator, which
            // advertises `true` on its own join path), so this is a
            // constant `false`.
            is_observer: false,
            // Advertise primary-capability (twin of `is_observer`): under
            // mesh-always (pillar 1) a network compute secondary always holds
            // a peer mesh, so it declares `true` and the bootstrap-relocation /
            // promotion selection may move authority to it. Only an observer
            // host (or the in-process same-host secondary, which has no
            // peer-to-peer mesh) declares `false` so the submitter stays
            // primary. The primary records this in the replicated
            // `RoleTable.can_be_primary` via the `PeerJoined` it
            // originates on welcome-accept.
            can_be_primary: self.config.can_be_primary,
        };
        self.send_setup_frame(msg).await
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
        self.send_setup_frame(msg).await
    }

    /// Ship one setup-phase frame to the primary, opaquely.
    ///
    /// Single chokepoint for the two setup-phase sends. Keeps the
    /// narrow-typed `SetupBootstrapMessage` construction at the call
    /// sites (the compile-time "no runtime frames during setup" guard)
    /// while routing through the [`Destination::Primary`] egress edge:
    /// convert to the wire shape via the lossless
    /// `From<SetupBootstrapMessage>` and `send_to(Destination::Primary,
    /// ..)`. The role table is cold during setup (no `PrimaryChanged`
    /// yet), so the edge resolver falls back to the bootstrap primary id
    /// — the dialled primary these frames address. No locality branching
    /// leaks to the manager; setup is just another destination-addressed
    /// send.
    async fn send_setup_frame(&mut self, msg: SetupBootstrapMessage) -> Result<(), String> {
        let wire: DistributedMessage<I> = msg.into();
        self.send_to(
            dynrunner_protocol_primary_secondary::Destination::Primary,
            wire,
        )
        .await
    }

    /// Receive the next setup frame, draining the run-config backstop's
    /// backlog first. The backstop (see
    /// [`Self::finalize_run_config_before_workers`]) may have pulled
    /// PeerInfo / InitialAssignment / TransferComplete frames off the inbox
    /// while bounded-waiting for the `RunConfig` answer; those belong to
    /// `wait_for_setup`'s progress loop, so it must consume them before any
    /// fresh `inbox.recv()`. Empty backlog (the common path — the push lands
    /// before the first setup frame) falls straight through to `recv`.
    pub(in crate::secondary) async fn recv_setup_frame(&mut self) -> Option<DistributedMessage<I>> {
        if let Some(buffered) = self.setup_frame_backlog.pop_front() {
            return Some(buffered);
        }
        self.inbox.recv().await
    }

    /// Ensure the post-welcome `RunConfig` push has landed, then fire the
    /// consumer's run-config finalize policy — BEFORE the worker pool spawns.
    ///
    /// Fired ONCE at the `AwaitingPrimary → Configuring` transition (the top
    /// of [`Self::enter_configuring_on_first_primary_frame`], before
    /// [`Self::initialize_workers`]) so the per-type `cmd_args` the finalize
    /// re-derives from the DELIVERED `forwarded_argv` are live for the initial
    /// workers (and, via the shared worker-command source the closure swaps,
    /// every respawn).
    ///
    /// Backstop: if the push has not landed yet
    /// (`!self.forwarded_argv_was_pushed` — an empty argv is a valid landing,
    /// so the dedicated latch, not emptiness, is the test), actively unicast
    /// ONE `RequestRunConfig` to the primary IN-BAND over the existing mesh
    /// connection (NOT a new dial) and bounded-wait for the answer. Frames
    /// that are not the answer are buffered into `setup_frame_backlog` so the
    /// outer setup loop still sees them. On timeout, proceed with whatever the
    /// shared handle holds (last-writer-wins) — a missing run-config is not
    /// fatal (e.g. a non-pushing legacy primary). Normally the push lands
    /// first and this is a no-op short-circuit.
    pub(in crate::secondary) async fn finalize_run_config_before_workers(
        &mut self,
    ) -> Result<(), String> {
        // Nothing to do unless a finalize policy was registered. The `args=`
        // path (compiler_suit) registers an IDENTITY finalizer (Some), so the
        // seam DOES run for it — harmlessly: the identity ignores the delivered
        // argv and the rebuild is byte-identical (compiler_suit's
        // `build_worker_command_args` does `del args`). Only Rust-only test
        // fixtures register `None`, which skips the seam here.
        let Some(mut finalize) = self.finalize_run_config.take() else {
            return Ok(());
        };

        if !self.forwarded_argv_was_pushed {
            self.request_and_await_run_config().await;
        }

        // Read the delivered argv off the shared handle (single source of
        // truth) and hand it to the consumer's reparse closure. The closure
        // does the cmd_args rebuild + swap internally (under the GIL, in the
        // pyo3 wrapper); the coordinator stays Python-free.
        let delivered = self
            .forwarded_argv
            .lock()
            .expect("forwarded_argv mutex poisoned")
            .clone();
        let result = finalize(delivered).await;
        // Put the closure back defensively (mirrors the setup-discovery
        // re-arm discipline) even though it fires at most once per run.
        self.finalize_run_config = Some(finalize);
        result
    }

    /// Send ONE in-band `RequestRunConfig` to the primary and bounded-wait
    /// for the `RunConfig` answer (stored via `store_pushed_run_config`).
    /// Non-answer frames are buffered for `wait_for_setup` to drain. Best
    /// effort: a send failure or a timeout simply returns — the caller
    /// proceeds with whatever the shared handle holds.
    async fn request_and_await_run_config(&mut self) {
        let request = DistributedMessage::RequestRunConfig {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
        };
        // In-band over the EXISTING mesh connection (the bootstrap primary
        // link is routable this early via `bootstrap_primary_id`); NOT a new
        // dial. A no-route error is non-fatal — fall through to the wait,
        // which will time out and proceed.
        if let Err(e) = self.send_to(Destination::Primary, request).await {
            tracing::debug!(
                error = %e,
                "run-config backstop: RequestRunConfig send failed; \
                 proceeding to bounded-wait (will fall back on timeout)"
            );
        }
        // Bounded-wait, reusing the setup recv discipline. The deadline is the
        // keepalive-derived setup budget; on expiry the caller proceeds with
        // the current handle value (last-writer-wins).
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold.max(1))
            .max(std::time::Duration::from_secs(2));
        let wait = async {
            while !self.forwarded_argv_was_pushed {
                match self.inbox.recv().await {
                    Some(msg) => {
                        if let MessageType::RunConfig = msg.msg_type() {
                            if let DistributedMessage::RunConfig { forwarded_argv, .. } = msg {
                                self.store_pushed_run_config(forwarded_argv);
                            }
                        } else {
                            // A setup/operational frame the outer loop still
                            // needs — buffer it (no frame loss, no duplicated
                            // handling).
                            self.setup_frame_backlog.push_back(msg);
                        }
                    }
                    None => break, // primary link closed; let the caller proceed
                }
            }
        };
        if tokio::time::timeout(deadline, wait).await.is_err() {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                "run-config backstop: timed out waiting for RunConfig answer; \
                 proceeding with the current (possibly empty) forwarded_argv"
            );
        }
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
            // Opaque inbound: the role inbound stream. During setup the
            // mesh is still forming, so the mesh-pump demuxes the primary's
            // setup frames onto this secondary's slot inbox as the primary
            // host dials/accepts; the manager addresses peers by id and
            // never sees a transport role split.
            match self.recv_setup_frame().await {
                Some(msg) => {
                    // Run-config PUSH is PRE-ANNOUNCE config delivery: it
                    // fires from the primary's welcome handler, BEFORE the
                    // PeerInfo/InitialAssignment/TransferComplete announce
                    // batch, so on the ordered primary link it normally
                    // arrives first. Store it and `continue` WITHOUT triggering
                    // the `AwaitingPrimary → Configuring` transition — the
                    // announce (PeerInfo) is the real first frame that spawns
                    // workers, and by then the latch is set so the finalize
                    // backstop short-circuits (the push already landed). Doing
                    // the store before `enter_configuring` also means a
                    // RunConfig-first frame never spuriously arms the backstop.
                    if let MessageType::RunConfig = msg.msg_type() {
                        if let DistributedMessage::RunConfig { forwarded_argv, .. } = msg {
                            self.store_pushed_run_config(forwarded_argv);
                        }
                        continue;
                    }
                    // FIRST primary-originated frame = the announce. This
                    // is the `AwaitingPrimary → Configuring` boundary: the
                    // primary has made contact, so spawn the worker pool
                    // (the entry action of `Configuring`) BEFORE handling
                    // this frame's content. The frames are ordered
                    // (PeerInfo → InitialAssignment → TransferComplete) on
                    // the primary link, so the pool exists before any
                    // `InitialAssignment` dispatch — no race. Awaited
                    // before the match below exactly as the pre-relocation
                    // flow awaited `initialize_workers` before
                    // `wait_for_setup`. Idempotent: only fires while the
                    // lifecycle is still `AwaitingPrimary` (the
                    // `enter_configuring` transition is a no-op from any
                    // other state, but the spawn is gated on the state
                    // check so workers are built at most once).
                    self.enter_configuring_on_first_primary_frame(factory)
                        .await?;
                    match msg.msg_type() {
                        MessageType::PeerInfo => {
                            got_peer_info = true;
                            if let DistributedMessage::PeerInfo {
                                target: None,
                                peers,
                                ..
                            } = &msg
                            {
                                let peer_count = peers
                                    .iter()
                                    .filter(|p| p.secondary_id != self.config.secondary_id)
                                    .count();
                                tracing::info!(
                                    peers = peer_count,
                                    "received peer list, kicking off peer dials"
                                );
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
                                //
                                // PHASE-C-SEAM[C-NODE]: peer-mesh DIAL. In the
                                // one-mesh model the transport (and
                                // `connect_to_peers`) lives in the `Node`'s
                                // `Mesh`, not on the coordinator — the
                                // secondary holds only a `MeshClient` /
                                // `RoleInbox`, neither of which dials. Dialing
                                // the rest of the mesh on a PeerInfo is a
                                // transport/membership concern the `Node`
                                // (mesh-pump) owns: it observes every inbound
                                // wire frame, so it can dial off the same
                                // PeerInfo without a manager-layer
                                // `connect_to_peers` call (clarification RV-2 —
                                // don't re-derive membership at the manager
                                // layer). The watchdog arming below stays here
                                // (it operates on this coordinator's own
                                // `MeshFormation` sub-concern, which the
                                // coordinator still owns).
                                let _ = peers;
                                // Arm the peer-mesh watchdog. 30s = 10s
                                // QUIC timeout + 10s WSS fallback timeout
                                // + 10s slack for the accept side to
                                // finish handshakes that completed near
                                // the deadline. After this point a 0-peer
                                // count means "the cluster blocks
                                // peer-direct connectivity" rather than
                                // "the dials are still in flight".
                                if peer_count > 0 {
                                    self.mesh.peer_dial_count = peer_count as u32;
                                    self.mesh.peer_mesh_check_at =
                                        Some(Instant::now() + std::time::Duration::from_secs(30));
                                }
                            }
                        }
                        MessageType::InitialAssignment => {
                            got_assignment = true;
                            if let DistributedMessage::InitialAssignment {
                                target: None,
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
                            // The `got_peer_info / got_assignment / got_transfer`
                            // trio (local to this loop) is the SINGLE
                            // `Configuring → Operational` gate — there is no
                            // parallel tracking on `ConfiguringState`.
                            got_transfer = true;
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
                            if let DistributedMessage::ClusterMutation {
                                target: None,
                                mutations,
                                ..
                            } = msg
                            {
                                self.apply_cluster_mutations(mutations);
                            }
                        }
                        // `MessageType::RunConfig` is handled BEFORE the
                        // `enter_configuring` trigger above (pre-announce
                        // config delivery, store-and-`continue`), so it never
                        // reaches this match.
                        other => {
                            tracing::debug!(?other, "unexpected message during setup");
                        }
                    }
                }
                None => return Err("primary disconnected during setup".into()),
            }
        }

        Ok(())
    }

    /// `AwaitingPrimary → Configuring` entry action, fired on the FIRST
    /// primary-originated setup frame (the announce). Spawns the worker
    /// pool, moves it into the new `Configuring` state, and stands up the
    /// per-task memprofile sampler — the exact pre-handshake side effects
    /// the pre-relocation flow ran in `run_until_setup_or_done_inner`,
    /// now relocated here so they only happen once the primary has made
    /// contact.
    ///
    /// Idempotent: a no-op unless the lifecycle is still
    /// `AwaitingPrimary`, so it fires at most once per run (subsequent
    /// setup frames find the lifecycle already `Configuring`). The
    /// carried-forward config flags (`pre_staged_mode` /
    /// `uses_file_based_items`) start at their historical defaults
    /// (`false` / `true`) and are updated by the `InitialAssignment`
    /// handler via `set_pre_staged_mode` / `set_uses_file_based_items`.
    pub(in crate::secondary) async fn enter_configuring_on_first_primary_frame(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        if !matches!(
            self.lifecycle,
            super::lifecycle::SecondaryLifecycle::AwaitingPrimary { .. }
        ) {
            return Ok(());
        }
        // Finalize the consumer's run-config BEFORE spawning workers: the
        // worker pool reads the per-type `cmd_args` at spawn, and those are
        // re-derived from the post-welcome-pushed `forwarded_argv` (with an
        // in-band `RequestRunConfig` backstop if the push has not landed yet).
        // Must precede `initialize_workers` so the initial pool's argv is the
        // finalized command, not the empty boot-CLI placeholder. No-op when no
        // finalize policy was registered.
        self.finalize_run_config_before_workers().await?;
        tracing::info!(
            secondary = %self.config.secondary_id,
            workers = self.config.num_workers,
            "primary announced (first setup frame); spawning workers and entering Configuring"
        );
        // Spawn the pool (awaited) BEFORE any InitialAssignment dispatch.
        let pool = self.initialize_workers(factory).await?;
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            super::lifecycle::SecondaryLifecycle::connecting(),
        );
        // `pre_staged_mode` / `uses_file_based_items` default to the
        // historical pre-InitialAssignment values; the InitialAssignment
        // handler overwrites them via `set_pre_staged_mode` /
        // `set_uses_file_based_items` once that frame lands.
        self.lifecycle = lifecycle.enter_configuring(pool, false, true);
        // Stand up the memprofile sampler now that the pool exists —
        // BEFORE the InitialAssignment dispatch so the initial-assignment
        // sampler hook captures the first batch (the same ordering the
        // pre-relocation flow had).
        self.build_sampler_if_enabled();
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

            let zip_ref = if zip_name.is_empty() {
                None
            } else {
                Some(zip_name.as_str())
            };
            let resolved_path = self.resolve_for_dispatch(zip_ref, &local_path, &hash);

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
            // Keyed by the ORIGINAL wire `worker_id` (no clamp), the same
            // un-clamped id the operational `dispatch_message` path reports.
            match self
                .report_unresolvable_task(worker_id, &hash, &local_path, &resolved_path)
                .await
            {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        worker_id,
                        error = %e,
                        "failed to send TaskFailed for unresolvable initial-assignment task"
                    );
                    continue;
                }
            }

            // Select the dispatch target worker SAFELY, mirroring the
            // operational `dispatch_message` selection. Prefer the primary's
            // requested slot IF it is a valid, idle worker; otherwise fall
            // back to any idle worker. Both `.get()` and `position()` return
            // `None` for an empty pool, so a 0-worker pool (an observer /
            // late-joiner that spawned no slots) is just the degenerate "no
            // idle worker" case — no `pool.workers.len() - 1` underflow and
            // no unconditional index into the slice. An out-of-range
            // `worker_id` likewise resolves to `None` on the preference and
            // falls back to an idle worker, never silently clamped onto the
            // last slot. `None` ⇒ skip this task's assignment (no worker can
            // take it); the authority still holds it Pending.
            let pool = self.pool_mut();
            let target_wid: Option<u32> = pool
                .workers
                .get(worker_id as usize)
                .filter(|w| w.is_idle_state())
                .map(|_| worker_id)
                .or_else(|| {
                    pool.workers
                        .iter()
                        .position(|w| w.is_idle_state())
                        .map(|i| i as u32)
                });

            let Some(wid) = target_wid else {
                tracing::debug!(
                    requested_worker_id = worker_id,
                    "no idle worker for initial task assignment; leaving it for the \
                     authority to dispatch"
                );
                continue;
            };

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

            // Per-type subprocess dispatch: bind the worker's
            // loaded TypeId to this task's `type_id` (no-op fast
            // path when they already match — the dominant case).
            if let Err(e) = self
                .pool_mut()
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
            // Snapshot for the sampler hook before the binary
            // is moved into `assign_task`. The hook reads
            // `task_id` off the borrowed `TaskInfo`; cloning
            // once here keeps the success arm simple.
            let binary_for_hook = binary.clone();
            match self.pool_mut().workers[wid as usize]
                .assign_task(
                    binary,
                    estimated,
                    false,
                    // Initial assignments fire at run start before
                    // any task has produced outputs. The wire
                    // `InitialAssignment` message carries no
                    // `predecessor_outputs` field (there are none
                    // to gather), so an empty map is the correct
                    // wire shape here.
                    std::collections::BTreeMap::new(),
                )
                .await
            {
                Ok(()) => {
                    self.notify_sampler_assigned(wid, &binary_for_hook);
                    self.active_tasks_mut().insert(hash, wid);
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
