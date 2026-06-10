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

/// Setup-progress heartbeat cadence for [`wait_for_setup`]'s recv:
/// when NO setup frame arrives for this long while the trio
/// (PeerInfo / InitialAssignment / TransferComplete) is incomplete,
/// one WARN names which frames are still missing and how long the
/// wait has lasted. Narration only — the wait itself is unbounded
/// (the unconfigured-deadline owns the give-up policy). Without this
/// heartbeat a secondary wedged in setup (e.g. the primary's directed
/// sends never reach it) is TOTALLY silent until that deadline — the
/// #362 production shape: "received peer list" and then nothing, ever.
const SETUP_STALL_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

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
            // Advertise this node's liveness-beacon listener port so peers
            // can beacon it once it becomes primary (the "primary on ANY
            // peer" invariant). `None` when no listener was bound.
            liveness_port: self.liveness_port,
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
    /// Recv loop with two periodic arms: the 60s stall-warn narration and
    /// the setup-phase anti-entropy digest broadcast (see the cadence
    /// comment at the interval's construction below).
    ///
    /// History of the select-vs-serial shape: the earlier 1cb8cb8 version
    /// raced the recv against a keepalive ticker; after c9a7808 made
    /// `connect_to_peers` non-blocking that keepalive stopped being
    /// load-bearing and the select was removed because cancelling the OLD
    /// transport-level `primary_transport.recv()` could lose a partially
    /// decoded wire frame. That hazard no longer exists: today's recv is
    /// [`Self::recv_setup_frame`] — a sync backlog pop + `RoleInbox::recv`
    /// (a plain mpsc recv, documented cancel-safe; the mesh-pump only ever
    /// delivers WHOLE frames into the slot) — so racing it in a `select!`
    /// drops no data. The stall-warn deadline is computed OUTSIDE the
    /// select iteration so the ~20s anti-entropy tick cannot keep
    /// resetting it (a per-iteration `timeout` would never fire — the
    /// watchdog-needs-a-fires-under-load law).
    pub(super) async fn wait_for_setup(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<Option<super::RunOutcome>, String> {
        tracing::debug!("waiting for setup messages from primary");

        let mut got_peer_info = false;
        let mut got_assignment = false;
        let mut got_transfer = false;

        // Setup-phase anti-entropy cadence — the EMIT half of the
        // relocation-handoff heal. The APPLY half (the `StateDigest` /
        // `ClusterSnapshot` arms below) already lets a mid-setup secondary
        // pull a missed `PrimaryChanged` once a digest REACHES it; this tick
        // is what makes that reachable in the demoted-submitter topology.
        // The submitter's accept loop registers a connection only on its
        // FIRST frame (`peer_id = first_msg.sender_id()`), and the
        // bootstrap-redial re-fold (transport-quic `bootstrap_redial`) sends
        // nothing on the fresh wire — so a trio-waiting secondary that never
        // speaks leaves its re-dialed wire permanently unregistered, the
        // observer's digest broadcast fans over an EMPTY writer table, and
        // the heal deadlocks (the 2026-06 4/4 leaderless wedge). Emitting
        // this secondary's own digest on the same jittered cadence every
        // OTHER waiting state already uses (`process_tasks`, the primary's
        // operational loop, the observer tail) both re-registers the wire
        // (the digest is the identifying first frame) and advertises this
        // replica's state, closing the loop. Same `Skip` + dropped-first-tick
        // shape as the operational arm.
        let mut anti_entropy_interval =
            tokio::time::interval(crate::anti_entropy::tick_period(&self.config.secondary_id));
        anti_entropy_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        anti_entropy_interval.reset();

        while !got_peer_info || !got_assignment || !got_transfer {
            // Terminal-exit backstop. A terminal CRDT flag set DURING setup
            // (RunComplete / RunAborted) means the RUN IS OVER — even though
            // the setup trio never completed and this secondary did no (or
            // only partial) work. Tearing down is correct: the run-over cue
            // SUPERSEDES the unfinished setup. The flag self-heals via
            // anti-entropy (`cluster_state/digest.rs`,
            // `state_digest.rs::is_behind`), so it lands here even if the
            // live broadcast was missed. Without this check, a secondary
            // still wedged in the trio-wait (never-connecting peer, transient
            // stall) lingers to the `unconfigured_deadline` (600s) holding
            // its SLURM job slot post-run-complete — the straggler symptom.
            //
            // The terminal recording + routing exactly MIRRORS the
            // operational loop's flag-to-terminal mapping
            // (`process_tasks.rs`): `run_aborted()` is checked BEFORE
            // `run_complete()` because an abort is a hard cluster shutdown,
            // both record the SAME lifecycle terminal the operational loop
            // records (`enter_terminal_aborted` / `enter_terminal_done`), and
            // `Some(RunOutcome::Terminal)` routes to the SAME line-1192
            // teardown match the operational `process_tasks` return uses. The
            // operational side additionally gates `run_complete` on
            // `active_tasks.is_empty()`; here there are no active tasks (no
            // dispatch happens pre-`Operational`), so the gate is vacuously
            // satisfied and omitted. The trio-completion success exit
            // (`Ok(None)` below) is untouched.
            if let Some(reason) = self.cluster_state.run_aborted() {
                let reason = reason.to_string();
                tracing::error!(
                    reason = %reason,
                    "RunAborted observed during setup; tearing down without \
                     waiting for setup to complete"
                );
                self.enter_terminal_aborted(reason);
                return Ok(Some(super::RunOutcome::Terminal));
            }
            if self.cluster_state.run_complete() {
                tracing::info!(
                    "RunComplete observed during setup; tearing down without \
                     waiting for setup to complete"
                );
                self.enter_terminal_done();
                return Ok(Some(super::RunOutcome::Terminal));
            }
            // Opaque inbound: the role inbound stream. During setup the
            // mesh is still forming, so the mesh-pump demuxes the primary's
            // setup frames onto this secondary's slot inbox as the primary
            // host dials/accepts; the manager addresses peers by id and
            // never sees a transport role split.
            //
            // Stall heartbeat (#362): a persistent 60s deadline arm logs
            // WHICH trio frames are still missing and loops back to
            // waiting — narration only, no behavior change (the
            // unconfigured-deadline still owns the give-up policy).
            let received = 'frame: loop {
                // One stall window per warn cycle, computed OUT here so the
                // anti-entropy arm firing cannot reset it: a per-`select!`
                // `timeout(...)` would be re-armed by every ~20s digest tick
                // and the 60s stall warn would NEVER fire (the
                // watchdog-needs-a-fires-under-load law). The deadline
                // persists across select iterations; only a delivered frame
                // (which exits this loop entirely) or the warn itself starts
                // a fresh window — exactly the pre-select semantics.
                let stall_deadline =
                    tokio::time::Instant::now() + SETUP_STALL_WARN_INTERVAL;
                loop {
                    tokio::select! {
                        // Cancel-safe: `recv_setup_frame` is a sync backlog
                        // pop + `RoleInbox::recv` (a plain mpsc recv), so a
                        // tick winning the race loses no frame.
                        frame = self.recv_setup_frame() => break 'frame frame,
                        // Anti-entropy tick: broadcast this secondary's
                        // digest so every peer can detect divergence and
                        // pull — and so the submitter's accept loop has a
                        // first frame to register a re-dialed bootstrap
                        // wire under. Pure EMIT of the role-agnostic frame
                        // built by `crate::anti_entropy`; the receive-side
                        // compare+pull lives in the `StateDigest` arm below.
                        // `interval.tick` is cancel-safe (tokio docs).
                        _ = anti_entropy_interval.tick() => {
                            let digest = self.cluster_state.digest();
                            let frame = crate::anti_entropy::digest_broadcast(
                                &self.config.secondary_id,
                                timestamp_now(),
                                digest,
                            );
                            if let Err(e) = self.send_to(Destination::All, frame).await {
                                tracing::warn!(
                                    error = %e,
                                    "setup-phase anti-entropy digest broadcast \
                                     failed; the next tick retries"
                                );
                            }
                        }
                        _ = tokio::time::sleep_until(stall_deadline) => {
                            tracing::warn!(
                                got_peer_info,
                                got_assignment,
                                got_transfer,
                                "still waiting for the setup trio from the primary \
                                 (no setup frame for 60s); the missing directed \
                                 frames may indicate the primary has no route to \
                                 this node or proceeded without it"
                            );
                            // Restart the stall window; keep waiting.
                            break;
                        }
                    }
                }
            };
            match received {
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
                    // Anti-entropy frames are replicated-STATE traffic, not a
                    // primary announce: handled BEFORE the `enter_configuring`
                    // trigger (like the RunConfig push above) so a digest from
                    // a non-primary peer (e.g. the relocated submitter's
                    // observer) never spawns the worker pool. Participating
                    // here — instead of dropping these frames in the
                    // `other =>` arm — is what makes a LOST relocation
                    // `PrimaryChanged` broadcast recoverable while the fleet
                    // is still mid-setup (the run_20260610_185621 leaderless
                    // wedge): the observer's ~20s digest cadence proves this
                    // replica behind, the pull's snapshot restores the
                    // primary fact, and the restore helper's identity seam
                    // fires the `PromotionSignal` on a self-named heal (or
                    // re-points `current_primary` on a peer-named one). Both
                    // helpers are the SAME single writers the operational
                    // router uses.
                    if let MessageType::StateDigest = msg.msg_type() {
                        if let DistributedMessage::StateDigest {
                            digest, sender_id, ..
                        } = msg
                        {
                            self.reconcile_state_digest(&sender_id, &digest).await;
                        }
                        continue;
                    }
                    if let MessageType::ClusterSnapshot = msg.msg_type() {
                        if let DistributedMessage::ClusterSnapshot { snapshot_json, .. } = msg {
                            self.restore_cluster_snapshot_frame(&snapshot_json);
                        }
                        // The restored snapshot may carry a terminal latch
                        // (RunComplete / RunAborted) — re-loop to the single
                        // terminal-exit check at the loop head, exactly as
                        // the ClusterMutation arm below does.
                        continue;
                    }
                    // FIRST primary-originated frame = the announce. This
                    // is the `AwaitingPrimary → Configuring` boundary: the
                    // primary has made contact, so spawn the worker pool
                    // (the entry action of `Configuring`) BEFORE handling
                    // this frame's content. Fired on EVERY primary frame and
                    // idempotent (a no-op once `Configuring`), and AWAITED
                    // before the per-type match below — so the worker pool
                    // exists before any `InitialAssignment` dispatch arm runs,
                    // WHICHEVER frame arrives first. This pool-before-dispatch
                    // guarantee does NOT rely on the three setup frames arriving
                    // in a fixed order: `got_peer_info / got_assignment /
                    // got_transfer` are tracked independently and the gate
                    // releases once all three are set, in any interleaving. That
                    // independence matters because the frames ride DIFFERENT
                    // egress edges — PeerInfo broadcasts over `Destination::All`,
                    // while `InitialAssignment` and `TransferComplete` are
                    // directed `Destination::Secondary(id)` sends (the
                    // relay-capable router path; see
                    // `primary::lifecycle::mutations::send_transfer_complete`) —
                    // so no single ordered link carries all three. The two
                    // directed frames to THIS secondary DO arrive in send order
                    // (same directed link, FIFO), but the gate does not depend on
                    // it. Awaited before the match below exactly as the
                    // pre-relocation flow awaited `initialize_workers` before
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
                                // Truthful narration (#362): the
                                // coordinator does NOT dial — the Node
                                // mesh-pump dials off this same frame,
                                // and its "peer-dial sweep" transport
                                // line is the authoritative record of
                                // how many dials were actually spawned
                                // (possibly ZERO on the highest-id
                                // node, whose legs all arrive inbound
                                // under the lower-id-dials rule). The
                                // old text "kicking off peer dials"
                                // promised dials this node may never
                                // make.
                                tracing::info!(
                                    peers = peer_count,
                                    "received peer list; the mesh-pump runs the \
                                     peer-dial sweep (see 'peer-dial sweep' for \
                                     what was actually dialed)"
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
                                //
                                // The liveness-beacon path DOES consume the
                                // roster here: capture each peer's
                                // id→liveness-address so the beacon can target
                                // whichever peer is/becomes primary, and
                                // re-point it. Address capture only — no
                                // role/CRDT/membership decision (those stay
                                // the Node/CRDT concerns above).
                                self.ingest_peer_liveness_addrs(peers);
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
                                self.set_staging_dispatch_context(
                                    super::StagingDispatchContext {
                                        pre_staged_mode,
                                        uses_file_based_items,
                                    },
                                );
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
                            // A terminal flag (RunComplete / RunAborted) may
                            // have JUST landed in the mirror above. Re-loop to
                            // the single terminal-exit check at the loop head
                            // INSTEAD of blocking on the next `recv_setup_frame`
                            // — the run-over batch is frequently the primary's
                            // last act before it drops the link, so waiting for
                            // a further frame would fall through to the
                            // `None => Err("primary disconnected")` arm instead
                            // of the clean Done/Aborted teardown. The flag-check
                            // logic lives in exactly one place (the loop head);
                            // this `continue` just re-evaluates it eagerly.
                            continue;
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

        // Trio completed: the NORMAL success exit. `Ok(None)` signals the
        // orchestration to proceed to the operational handoff
        // (`process_tasks`) — this path is unchanged by the terminal-exit
        // backstop above.
        Ok(None)
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
    /// pre-staged / file-based dispatch flags are NOT lifecycle state — they
    /// live on the coordinator's shared `StagingDispatchContext` handle
    /// (seeded at its historical defaults in `new`) and are updated by the
    /// `InitialAssignment` handler via `set_staging_dispatch_context`.
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
        // The pre-staged / file-based dispatch flags live on the
        // coordinator's shared `StagingDispatchContext` handle (seeded at its
        // historical pre-InitialAssignment default in `new`); the
        // InitialAssignment handler overwrites them via
        // `set_staging_dispatch_context` once that frame lands.
        self.lifecycle = lifecycle.enter_configuring(pool);
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
