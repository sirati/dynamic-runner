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

/// This module's tracing target (the default `module_path!` — events here
/// don't override it), named as a const so the log-shape tests
/// (`tests::setup_wait_observability`) capture exactly this module's
/// emissions. Mirrors `primary::reconciliation_probe::LOG_TARGET`.
#[cfg(test)]
pub(crate) const LOG_TARGET: &str = module_path!();

/// Cap on the welcome/cert handshake-retry backoff in [`wait_for_setup`].
/// The backoff floor is config-derived ([`handshake_retry_initial`]) so it
/// scales with the deployment's keepalive tempo; this cap bounds the
/// doubling so a long pre-primary wait still re-offers the handshake at
/// least every 30s (cheap frames; the primary's welcome handling is
/// idempotent — re-applies NoOp on the CRDT and re-push the same
/// run-config).
const HANDSHAKE_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// Initial welcome/cert handshake-retry backoff: the keepalive interval,
/// floored at 100ms (a zero/near-zero keepalive must not busy-spin the
/// handshake arm). Config-derived so tests with millisecond keepalives
/// observe retries fast and production (multi-second keepalives) retries
/// on its own tempo without a new knob.
fn handshake_retry_initial(keepalive_interval: std::time::Duration) -> std::time::Duration {
    keepalive_interval.max(std::time::Duration::from_millis(100))
}

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

    /// One welcome + cert-exchange handshake ATTEMPT toward the resolved
    /// primary, with the send error ABSORBED into a `false`.
    ///
    /// The canonical failure is a no-route: the bootstrap wire has not
    /// been folded into the mesh yet (the background bring-up dial is
    /// still churning) or it dropped and the redial supervisor has not
    /// restored it. Neither is fatal — [`Self::wait_for_setup`]'s retry
    /// cadence owns recovery and the orchestration-level
    /// `unconfigured_deadline` owns give-up — so this absorbs rather than
    /// propagates. `attempt` is narration-only (first failure surfaces at
    /// INFO, the steady retry churn at DEBUG).
    pub(super) async fn try_send_handshake(&mut self, attempt: u32) -> bool {
        let result = async {
            self.send_welcome().await?;
            self.send_cert_exchange().await
        }
        .await;
        match result {
            Ok(()) => true,
            Err(error) => {
                if attempt <= 1 {
                    tracing::info!(
                        secondary = %self.config.secondary_id,
                        error = %error,
                        "setup handshake not deliverable yet (bootstrap wire \
                         not up); retrying on a capped backoff — the \
                         unconfigured-deadline owns give-up"
                    );
                } else {
                    tracing::debug!(
                        secondary = %self.config.secondary_id,
                        attempt,
                        error = %error,
                        "setup handshake retry not deliverable yet"
                    );
                }
                false
            }
        }
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

    /// Re-arm the pre-`Operational` setup deadline iff `sender` is the
    /// primary (the warm `current_primary` or, while the role table is
    /// cold, the bootstrap-dialled primary id — the SAME resolution the
    /// egress edge uses). A frame from any OTHER peer (a sibling's digest
    /// beacon, a relayed snapshot) is NOT primary liveness and must not
    /// keep a primary-less secondary alive past its deadline — the
    /// "setup deadline elapsed despite peers reachable" exit stays honest.
    fn note_setup_primary_liveness(&self, sender: &str) {
        let is_primary = match self.cluster_state.current_primary() {
            Some(p) => p == sender,
            None => self.bootstrap_primary_id.as_deref() == Some(sender),
        };
        if is_primary {
            self.setup_deadline.extend();
        }
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
    /// Recv loop with two periodic arms: the escalating wait-mark
    /// narration (the owner's 30s/1m/5m schedule over the setup-deadline's
    /// anchor — see [`super::wait_marks`]; the 10m point is the deadline
    /// abort itself) and the setup-phase anti-entropy digest broadcast
    /// (see the cadence comment at the interval's construction below).
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
    /// drops no data. The wait-mark arm sleeps to a STORED next-mark
    /// instant so the ~20s anti-entropy tick cannot keep resetting it (a
    /// per-iteration `timeout` would never fire — the
    /// watchdog-needs-a-fires-under-load law).
    pub(super) async fn wait_for_setup(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<Option<super::RunOutcome>, String> {
        // READY (owner-spec line 2): this is the precise point the
        // secondary can act on instructions from setup — its mesh
        // acceptors have been up since transport bring-up (#389 made the
        // bootstrap dial a background concern), the inbox is registered,
        // and THIS loop now consumes it; the welcome handshake offered
        // just below is what solicits those instructions. Logged BEFORE
        // the first handshake attempt so the line marks readiness, not
        // first contact. The wait-mark schedule + the abort horizon are
        // named here so an operator reading a wedged node's log knows
        // what narration to expect and when the node will give up.
        tracing::info!(
            secondary = %self.config.secondary_id,
            hostname = %self.config.hostname,
            wait_mark_secs = ?super::wait_marks::WAIT_MARKS.map(|m| m.as_secs()),
            give_up_after_secs = self.setup_deadline.horizon().as_secs(),
            "secondary ready to receive instructions from setup; waiting \
             for the primary (the wait is narrated at the escalating marks \
             and the run is abandoned after the configured horizon of \
             primary silence)"
        );

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
        let anti_entropy_period = crate::anti_entropy::tick_period(&self.config.secondary_id);
        let mut anti_entropy_interval = tokio::time::interval(anti_entropy_period);
        anti_entropy_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        anti_entropy_interval.reset();
        // Name the beacon ONCE at arming (run_20260611_005927 forensics:
        // the emission itself is a silent broadcast and the receive-side
        // reconcile logs only on divergence, so a parked fleet of
        // equally-empty replicas produced ZERO digest lines and the
        // cadence read as absent — operators could not distinguish
        // "beacon dead" from "beacon healthy but converged"). Per-tick
        // emission stays DEBUG below.
        tracing::info!(
            period_secs = anti_entropy_period.as_secs_f64(),
            "setup-phase anti-entropy digest beacon armed (broadcasts on \
             the jittered cadence from AwaitingPrimary onward; per-tick \
             emission logs at DEBUG)"
        );

        // Setup-phase failover-election cadence (#420 face (c)). Once a primary
        // has been silent past the election threshold (half the unconfigured
        // deadline) with a FORMED mesh, this tick ARMS + DRIVES the SAME
        // failover election the operational loop runs, so a survivor promotes
        // instead of the whole fleet dying one-by-one on its unconfigured
        // deadline. Cadence = `keepalive_interval` (the same gather/advance
        // tempo the operational election uses, so a setup-phase election
        // converges on the protocol's own clock). The arming gate
        // (`maybe_arm_setup_election`) is idempotent + threshold-guarded, so
        // ticking before the threshold is a cheap no-op; the per-tick drive is
        // a no-op until an election is armed.
        let mut setup_election_interval = tokio::time::interval(self.config.keepalive_interval);
        setup_election_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        setup_election_interval.reset();

        // ── Welcome/cert handshake: first attempt + retry cadence ──────
        //
        // The handshake is a SETUP concern, owned here (not a once-or-die
        // preamble in the orchestration): the first attempt fires at entry
        // — the same position in the setup sequence the pre-move
        // coordinator block had, strictly before any frame is consumed —
        // and a capped-backoff arm below RE-SENDS it until the WHOLE setup
        // trio has landed (the gate this loop waits on). Three production
        // failure shapes make the retry load-bearing:
        //   * NO ROUTE at boot (run_20260611_005927) — the bootstrap wire
        //     has not been folded yet (the bring-up dial runs in the
        //     background); the attempt is absorbed and re-offered until
        //     the wire lands.
        //   * WELCOME LOST on a dying wire (run_20260611_005927) — the
        //     frame was queued onto a wire that died before delivery; the
        //     redial restores the pipe but nothing else would re-enroll
        //     this node. A BROADCAST frame is NOT enrolment proof: the
        //     asm-dataset LMU bring-up (15 secondaries) lost 5 welcomes
        //     exactly because the primary's `PeerJoined` broadcasts
        //     (originated by the OTHER 10 welcomes) flipped the receivers
        //     out of `AwaitingPrimary` and the old lifecycle-gated arm
        //     disarmed before the first post-wire retry — the primary
        //     never learned of them and proceeded with quorum into a
        //     fleet their setup deadlines had already killed.
        //   * TRIO FRAME LOST after enrolment (run_20260612_105712) — the
        //     directed halves landed (proof the welcome was received) but
        //     the roster broadcast was lost to the joiner's leg-
        //     registration race, and the old directed-frame disarm
        //     (`!got_assignment && !got_transfer`) stopped the retries
        //     with `got_peer_info` still false: the member wedged with NO
        //     recovery channel and was silence-judged dead. The retry now
        //     persists for the whole trio-wait: each re-welcome doubles
        //     as a trio-retransmit request — the primary re-serves the
        //     trio on a duplicate welcome from a member whose operational
        //     loop has not provably started (see `primary::peer_setup::
        //     re_serve_setup_on_duplicate_welcome`) — and the loop
        //     exiting (trio complete) is what ends the retries.
        // Re-sends are safe: the primary's welcome handling is idempotent
        // (CRDT re-applies NoOp; the run-config re-push delivers the same
        // value; the trio re-serve is receiver-idempotent), and the capped
        // backoff (≤ one handshake per `HANDSHAKE_RETRY_MAX`) bounds the
        // steady-state churn.
        let mut handshake_attempts: u32 = 0;
        let mut handshake_backoff = handshake_retry_initial(self.config.keepalive_interval);
        // An operator-tempo floor above the cap is honoured (the cap only
        // bounds the doubling, never lowers the configured floor) —
        // mirrors the bring-up dial's backoff contract.
        let handshake_backoff_cap = HANDSHAKE_RETRY_MAX.max(handshake_backoff);
        if self.lifecycle.mark_handshake_sent() {
            handshake_attempts = 1;
            self.try_send_handshake(handshake_attempts).await;
        }
        let mut handshake_at = tokio::time::Instant::now() + handshake_backoff;

        // The escalating wait-mark narration (owner-spec line 3): marks at
        // 30s/1m/5m of waiting for instructions, measured on the SAME
        // clock the give-up policy measures — the schedule reads the
        // shared `setup_deadline` cell's anchor, which `wait_for_setup`
        // re-arms on every primary-originated frame (see
        // `note_setup_primary_liveness`). The 10m point of the spec's
        // schedule is the deadline expiry itself: the orchestration's
        // abort line + structured `BringUpFailed` fatal IS the 10m mark.
        // Supersedes the fixed 60s stall heartbeat (#362) — one wait, one
        // narration schedule.
        let mut wait_marks =
            super::wait_marks::SetupWaitMarks::new(self.setup_deadline.clone());

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
            // Handshake retries persist until the WHOLE trio has landed
            // (i.e. for this loop's entire wait): a re-welcome is the
            // trio-retransmit request the primary's duplicate-welcome
            // re-serve answers. A partial trio is NOT a disarm — the
            // run_20260612_105712 member had both directed halves
            // (`got_assignment`/`got_transfer` true, the old disarm) yet
            // sat at `got_peer_info=false` forever once the retries
            // stopped, with nothing left to retransmit the lost roster.
            // The lifecycle state is deliberately NOT the gate either:
            // the `AwaitingPrimary → Configuring` announce fires on ANY
            // primary-originated frame, including BROADCASTS
            // (`PeerJoined` / `PeerInfo` / cold-seed `ClusterMutation`
            // batches), and receiving a broadcast proves nothing about
            // the welcome having landed (the asm-dataset LMU 5-of-15
            // welcome loss). The `got_*` flags can only change between
            // trio-loop iterations (they flip on a received frame, which
            // exits the select loop below), so sampling here is exact —
            // and the loop guard makes `trio_incomplete` true on entry;
            // the explicit gate documents the arm's contract and keeps it
            // correct under any future loop restructuring. Re-sends stay
            // safe past `Configuring`: the primary's welcome handling is
            // idempotent and the immediately-following cert-exchange
            // restores the connection record the re-welcome resets.
            let trio_incomplete = !got_peer_info || !got_assignment || !got_transfer;
            let received = loop {
                tokio::select! {
                    // Cancel-safe: `recv_setup_frame` is a sync backlog
                    // pop + `RoleInbox::recv` (a plain mpsc recv), so a
                    // tick winning the race loses no frame.
                    frame = self.recv_setup_frame() => break frame,
                    // Welcome/cert handshake retry (see the arming
                    // comment above the trio loop): persistent
                    // deadline, capped exponential backoff, armed for
                    // the whole trio-wait — the loop exiting (trio
                    // complete) is what ends the retries. `sleep_until`
                    // is cancel-safe (tokio docs).
                    _ = tokio::time::sleep_until(handshake_at), if trio_incomplete => {
                        handshake_attempts += 1;
                        let delivered =
                            self.try_send_handshake(handshake_attempts).await;
                        if delivered && handshake_attempts > 1 {
                            tracing::info!(
                                attempt = handshake_attempts,
                                "setup handshake re-sent (welcome + cert \
                                 exchange re-offered to the primary)"
                            );
                        }
                        handshake_at =
                            tokio::time::Instant::now() + handshake_backoff;
                        handshake_backoff =
                            (handshake_backoff * 2).min(handshake_backoff_cap);
                    }
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
                        tracing::debug!(
                            "setup-phase anti-entropy digest broadcast \
                             (the beacon tick)"
                        );
                        let frame = crate::anti_entropy::digest_broadcast(
                            &self.config.secondary_id,
                            timestamp_now(),
                            digest,
                            // A compute SecondaryCoordinator is never an
                            // observer (the observer role IS the standalone
                            // ObserverCoordinator).
                            false,
                        );
                        if let Err(e) = self.send_to(Destination::All, frame).await {
                            tracing::warn!(
                                error = %e,
                                "setup-phase anti-entropy digest broadcast \
                                 failed; the next tick retries"
                            );
                        }
                    }
                    // Setup-phase failover-election tick (#420 face (c)). The
                    // silence clock is the SAME re-armable `setup_deadline`
                    // anchor the wait-mark schedule + the give-up policy read
                    // (re-armed by `note_setup_primary_liveness` on every
                    // primary frame), so it measures PRIMARY SILENCE, never
                    // slow-fleet assembly: a SLOW-but-LIVE primary keeps
                    // pushing the anchor forward and `silent_for` stays below
                    // the threshold, so NO election arms (the negative case).
                    // Once a primary has been genuinely silent past the
                    // threshold with a formed mesh, `maybe_arm_setup_election`
                    // arms (idempotent + membership-seeded), and
                    // `drive_setup_election_tick` advances it on the keepalive
                    // cadence — a winner promotes via the PromotionSignal while
                    // staying in this wait (its own new primary re-sends the
                    // trio), so the whole fleet no longer dies on its deadline.
                    // `interval.tick` is cancel-safe (tokio docs).
                    _ = setup_election_interval.tick() => {
                        // Own-tick-health gate FIRST (the SAME shared
                        // authority the operational keepalive arm and the
                        // primary's heartbeat sweep feed): a setup-election
                        // tick that fires long past its cadence means THIS
                        // node's runtime was frozen/starved, so the
                        // `silent_for` it would measure reflects OUR stall,
                        // not the primary's silence. `starved` defers the
                        // ARM this tick (the primary may simply be unheard
                        // because we couldn't process its setup frames), and
                        // the re-based trustworthy floor clamps the seeded
                        // `primary_last_seen` in the election legs of
                        // `drive_setup_election_tick` (#423).
                        let starved =
                            self.own_tick_health.observe_tick(std::time::Instant::now());
                        // `setup_deadline` is built on `tokio::time::Instant`
                        // (the same clock its `sleep_until` reader uses), so
                        // measure the silence against `tokio::time::Instant::now()`.
                        let silent_for = tokio::time::Instant::now()
                            .saturating_duration_since(self.setup_deadline.anchor());
                        if !starved {
                            self.maybe_arm_setup_election(silent_for);
                        }
                        self.drive_setup_election_tick().await;
                    }
                    // Wait-mark narration (the owner's 30s/1m/5m schedule;
                    // see the arming comment above the trio loop). The
                    // sleep targets the STORED next-mark instant — sibling
                    // arms firing never reset it (the fires-under-load
                    // law), and a wake at a superseded mark (the anchor
                    // moved while sleeping) fires nothing and re-sleeps to
                    // the fresh window's first mark. `sleep_until` is
                    // cancel-safe (tokio docs).
                    _ = tokio::time::sleep_until(wait_marks.next_mark_at()) => {
                        if let Some(waited) = wait_marks.fire() {
                            tracing::warn!(
                                waited_secs = waited.as_secs(),
                                got_peer_info,
                                got_assignment,
                                got_transfer,
                                handshake_attempts,
                                "still waiting for instructions from setup \
                                 (no primary-originated frame this whole \
                                 window); the missing frames may indicate \
                                 the primary has no route to this node, is \
                                 still assembling, or proceeded without it \
                                 — the run is abandoned at the configured \
                                 setup deadline"
                            );
                        }
                    }
                }
            };
            match received {
                Some(msg) => {
                    // Primary-liveness evidence FIRST, for EVERY frame shape
                    // (directed or broadcast — any frame from the primary
                    // proves it alive): re-arm the pre-`Operational`
                    // deadline so it measures PRIMARY SILENCE, never
                    // slow-fleet assembly. Deliberately a DIFFERENT
                    // predicate from the trio gate the handshake retry is
                    // armed on: a frame from the primary proves it alive
                    // and assembling — exactly what the deadline exists to
                    // detect the absence of — but proves nothing about the
                    // trio having fully landed. See `setup_deadline.rs`
                    // for the LMU fleet-death replay.
                    self.note_setup_primary_liveness(msg.sender_id());
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
                            digest,
                            sender_id,
                            sender_is_observer,
                            ..
                        } = msg
                        {
                            self.reconcile_state_digest(&sender_id, sender_is_observer, &digest)
                                .await;
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
                    // Setup-phase failover-election frames (#420 face (c)):
                    // `TimeoutQuery` / `TimeoutResponse` / `PromotionVote` /
                    // `PromotionConfirm`. Handled BEFORE the `enter_configuring`
                    // announce trigger (like the StateDigest / RunConfig frames
                    // above) and `continue`d, so an election frame from a peer
                    // is NOT mistaken for a primary announce and never spawns
                    // the worker pool. This is the receive half of the setup
                    // election — its responder + tally edges run through the
                    // SAME election machinery the operational loop uses (state
                    // resolved via the op-OR-setup accessors), so a setup-phase
                    // election interoperates with operational voters over the
                    // same protocol frames. Pre-fix these fell to the `other =>`
                    // debug-drop arm, so a setup-wedged secondary could neither
                    // start nor answer an election — the run_~1429 leaderless
                    // fleet death.
                    if matches!(
                        msg.msg_type(),
                        MessageType::TimeoutQuery
                            | MessageType::TimeoutResponse
                            | MessageType::PromotionVote
                            | MessageType::PromotionConfirm
                    ) {
                        self.handle_setup_election_frame(msg).await;
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
                // The typed worker died before Ready (or its spawn
                // failed) with THIS task bound to the attempt. The task
                // must never be silently skipped (assigned-but-
                // terminal-less strands the run); report it on the same
                // fault attribution every deferred-loss edge uses: a
                // self-inflicted death (nonzero exit / bug signal)
                // CHARGES the task's retry budget, an environment loss
                // requeues it uncharged. The dead slot itself is healed
                // by the operational-entry restart sweep in
                // `process_tasks` (the restart machinery lives in
                // operational state and is unreachable here).
                let exit_status = self.pool_mut().workers[wid as usize].try_reap_exit();
                tracing::error!(
                    worker_id = wid,
                    error = %e,
                    exit_status = exit_status.as_ref().map(|s| s.to_string()),
                    type_id = %binary.type_id,
                    "failed to ensure worker type for initial task; reporting \
                     the task to the authority"
                );
                // No OOM watcher exists during setup; `false` keeps an
                // un-correlated SIGKILL in the uncharged class.
                let report = match dynrunner_manager_local::oom::classify_disconnect_fault(
                    dynrunner_core::ErrorType::Recoverable,
                    exit_status.as_ref(),
                    false,
                ) {
                    dynrunner_manager_local::oom::DisconnectFault::TaskFault(et) => {
                        let death = exit_status
                            .as_ref()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "exit status unavailable".into());
                        self.report_deferred_task_failed(
                            wid,
                            &hash,
                            et,
                            format!(
                                "worker subprocess died before running its \
                                 initial-assignment task ({death}): {e}"
                            ),
                        )
                        .await
                    }
                    dynrunner_manager_local::oom::DisconnectFault::InfraLoss => {
                        self.report_deferred_task_lost(wid, &hash).await
                    }
                };
                if let Err(send_err) = report {
                    tracing::error!(
                        worker_id = wid,
                        error = %send_err,
                        "failed to send TaskFailed for the initial-assignment \
                         task whose typed worker did not come up"
                    );
                }
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
