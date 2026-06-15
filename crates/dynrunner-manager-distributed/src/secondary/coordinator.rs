//! Inherent-impl methods on `SecondaryCoordinator`: constructor +
//! listener-registration + observer-announcer attachment + per-mode
//! flags + `run` entry points.
//!
//! Single concern: the body of every `impl SecondaryCoordinator`
//! method that's not naturally owned by one of the operational
//! submodules (dispatch / election / processing / ...). The
//! `select!`-loop body, peer-message dispatch, election state-machine,
//! and the per-arm wire handlers all live in their own modules; this
//! file is the entry-point catch-all.

use dynrunner_core::Identifier;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tracing::Instrument;

use super::lifecycle::{OperationalLatches, SecondaryLifecycle};
use super::primary_link::PrimaryLink;
use super::resource;
use super::{PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryCoordinator};
use crate::cluster_state::ClusterState;
use crate::process::{MeshClient, PromotionSignal, RoleInbox};
use crate::zip_extract::ExtractionCache;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Build a secondary coordinator over the one mesh.
    ///
    /// `client` (egress) + `inbox` (ingress) are the coordinator's entire
    /// view of the mesh — minted together with this role's `Arc<RoleSlot>`
    /// by `Mesh::register_local_role(LocalRole::Secondary, peer_id)` and
    /// handed in here. The coordinator never names a transport; the
    /// `Node`'s `Mesh` owns it.
    pub fn new(
        config: SecondaryConfig,
        client: MeshClient<I>,
        inbox: RoleInbox<I>,
        scheduler: S,
        estimator: E,
    ) -> Self {
        let tmp_dir = config.src_tmp.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("db_secondary_{}", &config.secondary_id))
        });
        let extraction_cache = ExtractionCache::new(tmp_dir, config.src_network.clone());
        // Peer-lifecycle dispatcher channel. Built at construction so
        // the `cluster_state` apply path has an installed sender
        // from the first `PeerJoined`/`PeerRemoved` mutation; the
        // receiver waits on `self` until `run_until_setup_or_done`
        // hands it to the dispatcher task. Events emitted before the
        // dispatcher is spawned queue on the unbounded channel and
        // drain on first poll.
        let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        // Task-completion dispatcher channel; same construction-time
        // installation rationale as `lifecycle_tx`.
        let (task_completed_tx, task_completed_rx) = tokio::sync::mpsc::unbounded_channel();
        // Worker custom-message dispatcher channel; built at
        // construction (like the two above) so the worker-event
        // bridge has a sender from the first frame — events emitted
        // before the dispatcher spawns queue on the unbounded channel
        // and drain on first poll.
        let (worker_message_tx, worker_message_rx) = tokio::sync::mpsc::unbounded_channel();
        // Secondary control-plane ingress (the PyO3 `SecondaryHandle`
        // → operational loop direction). Unbounded for the same
        // reason as the dispatcher channels: the producing listener
        // must never block, and the volume is bounded by real
        // consumer replies (≤ 100 KiB each, API-capped).
        let (secondary_control_tx, secondary_control_rx) = tokio::sync::mpsc::unbounded_channel();
        // Off-loop SecondaryAffine-import completion channel (#497 P5). Built
        // at construction (like the dispatcher channels) so a `StartedRun`'s
        // detached import task always has a sender; unbounded for the same
        // never-block-the-producer reason. The receiver is taken into a
        // loop-local at `process_tasks` entry.
        let (affine_import_tx, affine_import_rx) = tokio::sync::mpsc::unbounded_channel();
        // Command channel for the PyO3 `PrimaryHandle` surface.
        // Mirrors `PrimaryCoordinator::new` exactly: bounded capacity
        // sized so a noisy caller can't OOM the secondary, but with
        // enough slack to absorb a batch of commands before
        // backpressure surfaces. The receiver is taken out for the
        // duration of `process_tasks` and put back at loop exit.
        let (command_tx, command_rx) =
            tokio::sync::mpsc::channel(crate::primary::COMMAND_CHANNEL_CAPACITY);
        // Seed the SHARED node-local run-config handle off the config before
        // it moves into `this.config`. The responder, the finalize fire, and
        // the promotion recipe all read this one handle; `store_pushed_run_config`
        // is the single writer. Seeded from the boot CLI's `forwarded_argv`
        // (usually empty — the post-welcome push delivers the real value).
        let forwarded_argv =
            std::sync::Arc::new(std::sync::Mutex::new(config.forwarded_argv.clone()));
        // Seed the SHARED staging-dispatch-context handle at its historical
        // pre-`InitialAssignment` default (file-based, not pre-staged). The
        // `InitialAssignment` handler is the SOLE writer; the dispatch
        // resolver and the promotion recipe read this one handle.
        let staging_dispatch_context = std::sync::Arc::new(std::sync::Mutex::new(
            super::StagingDispatchContext::default(),
        ));
        // The pre-`Operational` deadline horizon, read off the config
        // before it moves into `this.config`. Un-armed here (`new` may run
        // outside a tokio runtime); the orchestration arms it at setup
        // entry and `wait_for_setup` re-arms it on primary liveness.
        let setup_deadline = super::setup_deadline::SetupDeadline::new(config.unconfigured_deadline);
        // Own-tick-health authority, built off the keepalive cadence before
        // `config` moves into `this.config` (mirroring `setup_deadline`). The
        // SAME shared primitive the primary's heartbeat sweep consumes; fed
        // once per keepalive-arm tick and read by every silence-based
        // judgment (the primary-silence backstop, the peer-keepalive reaper,
        // the setup-phase election arm).
        let own_tick_health = crate::own_tick_health::OwnTickHealth::new(config.keepalive_interval);
        let snapshot_streams =
            crate::snapshot_stream::SnapshotStreamResponder::new(&config.secondary_id);
        let inbound_snapshots =
            crate::snapshot_stream::InboundSnapshotStreams::new(&config.secondary_id);
        let pull_coordinator =
            crate::pull_coordinator::PullCoordinator::new(&config.secondary_id);
        // Settled-CRDT spill: attach this coordinator's spill segment to
        // the state it owns (degrades to disabled — fat-but-correct — on
        // any setup failure; see `settled_spill`).
        let mut cluster_state = ClusterState::new();
        let settled_spill =
            crate::settled_spill::SettledSpillDriver::start("secondary", &mut cluster_state);
        let mut this = Self {
            config,
            client,
            inbox,
            promotion_tx: None,
            bootstrap_primary_id: None,
            scheduler,
            estimator,
            peer_cert_info: None,
            liveness_port: None,
            beacon_target: crate::liveness::BeaconTarget::new(),
            beacon_liveness: crate::liveness::BeaconLiveness::new(),
            peer_liveness_addrs: crate::liveness::PeerLivenessAddrs::new(),
            #[cfg(test)]
            local_tasks_run: 0,
            extraction_cache,
            // The lifecycle begins as `Connecting` the moment the
            // coordinator is built. `Connecting` carries no entry-instant;
            // the long `unconfigured_deadline` that governs the whole
            // pre-`Operational` span is applied relatively as the
            // `tokio::time::timeout` wrapping the setup trio in
            // `run_until_setup_or_done`. The worker pool, election,
            // keepalive tracking, and primary link do NOT exist yet —
            // they are constructed at the `AwaitingPrimary → Configuring`
            // and `Configuring → Operational` transitions. Because the
            // pool field exists only from `Configuring` onward, a
            // pre-`Configuring` worker-spawn is unrepresentable by
            // construction; a stray pre-`Operational` task-accept is held
            // off instead by the `op_mut()` / `pool_mut()` expect-contract
            // (dispatch is routed to run only after `enter_operational`).
            lifecycle: SecondaryLifecycle::connecting(),
            // The orthogonal peer-mesh sub-concern starts in its resting
            // pre-dial state, carried across the lifecycle's config states
            // (it begins forming on the unconfigured peer).
            mesh: super::lifecycle::MeshFormation::default(),
            fatal_exit: None,
            deposed_primary: false,
            cluster_state,
            settled_spill,
            snapshot_streams,
            inbound_snapshots,
            pull_coordinator,
            lifecycle_rx: Some(lifecycle_rx),
            peer_lifecycle_listeners: Vec::new(),
            lifecycle_dispatcher_handle: None,
            task_completed_rx: Some(task_completed_rx),
            task_completed_listeners: Vec::new(),
            upload_action: None,
            import_action: None,
            affine_satisfied_probe: None,
            task_completed_dispatcher_handle: None,
            worker_message_tx,
            worker_message_rx: Some(worker_message_rx),
            worker_message_listeners: Vec::new(),
            worker_message_dispatcher_handle: None,
            secondary_control_tx,
            secondary_control_rx: Some(secondary_control_rx),
            affine_import_tx,
            affine_import_rx: Some(affine_import_rx),
            announcer_outbox_tx: None,
            announcer_outbox_rx: None,
            panik_signal_rx: None,
            fatal_exit_signal_rx: None,
            on_phase_end: None,
            on_phase_start: None,
            finalize_run_config: None,
            forwarded_argv_was_pushed: false,
            setup_frame_backlog: std::collections::VecDeque::new(),
            setup_deadline,
            // No setup-phase election armed at construction; the setup election
            // driver in `wait_for_setup` populates this on primary-silence.
            setup_election: None,
            setup_election_seedless_warn: crate::warn_throttle::WarnThrottle::new(
                std::time::Duration::from_secs(60),
            ),
            own_tick_health,
            command_rx: Some(command_rx),
            command_tx,
            // Lazily constructed in `run_until_setup_or_done_inner`
            // post-`initialize_workers` — see the doc on the
            // `sampler` field for the runtime-context rationale.
            sampler: None,
            forwarded_argv,
            staging_dispatch_context,
            // The reporting concern's buffered-terminal-replay queue starts
            // empty; it fills when a terminal-bearing report's send is
            // absorbed on a transient no-route OR sent and awaiting its
            // app-level TerminalAck (#352 — see the field doc on
            // `SecondaryCoordinator`).
            pending_report_replays: Vec::new(),
            // The drain's aggregated-log rate limiter beside it (no
            // emit yet, nothing suppressed). The #366 per-report
            // replay-attempt tally lives on each retained entry.
            replay_log_last_emit: None,
            replay_log_suppressed: 0,
            // Per-secondary monotonic delivery-confirmation counter; 1 so
            // a zero seq never appears on the wire.
            next_delivery_seq: 1,
            next_custom_msg_seq: 1,
            delivery_ack_timeout: resource::DEFAULT_DELIVERY_ACK_TIMEOUT,
            op_loop_arm_stats: None,
            op_loop_arm_stats_cell: None,
            collection_stats: crate::collection_stats::CollectionStatsEmitter::new(
                std::time::Instant::now(),
            ),
        };
        // Install the peer-lifecycle sender on `cluster_state` so the
        // `PeerJoined` / `PeerRemoved` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        // Done before any other registration so a mutation that
        // somehow lands during construction still has a sender to
        // enqueue against (defensive: today no mutation is applied
        // pre-`run_until_setup_or_done()`, but the contract should
        // not depend on that).
        this.cluster_state.install_lifecycle_sender(lifecycle_tx);
        // Install the task-completion sender alongside the
        // peer-lifecycle one — the two are independent dispatcher
        // modules with independent channels; same construction-time
        // installation contract.
        this.cluster_state
            .install_task_completed_sender(task_completed_tx);
        // Recognition→routing publish: register the role-change hook that
        // publishes `role_table.primary` into the mesh's
        // `RoleHolderView`, so the INGRESS relay can forward a directed
        // `Primary` frame that lands on a host with no live Primary slot
        // toward the recognized holder. EGRESS resolution is unchanged and
        // stays at this edge (`Self::send_to` reads
        // `cluster_state.current_primary()` / the bootstrap fallback) —
        // the view carries only the routing-holder fact, never the
        // bootstrap fallback.
        crate::process::attach_primary_recognition(
            &mut this.cluster_state,
            this.client.role_holder_view(),
        );
        this
    }

    /// Register the C4 promotion-signal sender. Must be called BEFORE
    /// `run_until_setup_or_done` enters; same pre-run, single-shot family
    /// as [`Self::register_panik_signal_rx`] /
    /// [`Self::register_fatal_exit_signal_rx`].
    ///
    /// On a self-named `PrimaryChanged` (an election win via
    /// `fire_local_promotion`, or a transferred primary), the secondary
    /// FIRES a [`PromotionSignal`] on this sender — it NEVER builds a
    /// primary itself (SUPREME-LAW #3). The matching receiver lives on the
    /// `Node`, which constructs the snapshot-seeded `PrimaryCoordinator` on
    /// the signal (threading this secondary's `WorkerFactory` to the spawn
    /// site). Absent registration (Rust-only unit fixtures) the fire site
    /// is a best-effort no-op and the test asserts on the CRDT identity
    /// advance directly.
    ///
    /// Single concern: own the registration surface for the promotion
    /// egress; the build of the primary on the signal is the `Node`'s
    /// concern.
    pub fn register_promotion_signal(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<PromotionSignal<I>>,
    ) {
        self.promotion_tx = Some(tx);
    }

    /// Set the bootstrap primary's peer-id — the id this secondary
    /// dialled at startup, folded into the mesh as a routable peer. The
    /// edge resolver ([`Self::send_to`]) uses it as the cold-cache
    /// fallback for [`dynrunner_protocol_primary_secondary::Destination::Primary`]
    /// before any `PrimaryChanged` warms `cluster_state.current_primary()`.
    ///
    /// Pre-run setter (same family as
    /// [`Self::set_peer_cert_info`]/[`Self::register_panik_signal_rx`]),
    /// called by the run-mode wiring alongside the transport's
    /// mesh-link registration so the edge knows which peer-id the
    /// bootstrap wire reaches.
    pub fn set_bootstrap_primary_id(&mut self, primary_id: String) {
        self.bootstrap_primary_id = Some(primary_id);
    }

    /// Wire the shared arm-stats bridge to the off-runtime
    /// [`crate::runtime_watchdog`] (called at node bootstrap, before `run`).
    /// The `process_tasks` loop then publishes its live arm stats into the
    /// cell on entry and clears them on exit, so a freeze dump names this
    /// secondary loop's hot arm. Observation-only. See
    /// [`crate::oploop_instrumentation::OpLoopArmStatsCell`].
    pub fn set_op_loop_arm_stats_cell(
        &mut self,
        cell: crate::oploop_instrumentation::OpLoopArmStatsCell,
    ) {
        self.op_loop_arm_stats_cell = Some(cell);
    }

    /// `&mut` access to the operational state. The operational handlers
    /// (worker dispatch, election, keepalive, the task-completion / OOM
    /// paths) are reachable ONLY through this accessor — they are
    /// unrepresentable while the lifecycle is `Connecting` /
    /// `AwaitingPrimary` / `Configuring` (those variants carry no
    /// [`super::lifecycle::OperationalState`]).
    ///
    /// `expect`-unwrapped rather than `Option`-returning because every
    /// caller is an operational-loop handler that physically runs only
    /// after `enter_operational` (the `process_tasks` select! arms, the
    /// per-frame dispatch reached from `handle_inbound`, the keepalive /
    /// election / OOM ticks). Reaching it in a pre-`Operational` state is
    /// a coordinator-internal logic bug, not a runtime condition to
    /// recover from — the panic is the loud signal the type invariant was
    /// violated. (The handlers that legitimately run pre-`Operational` —
    /// `apply_cluster_mutations` from `wait_for_setup`, the setup-frame
    /// sends — touch only `cluster_state` / `transport` / config, never
    /// this state.)
    #[track_caller]
    pub(in crate::secondary) fn op_mut(&mut self) -> &mut super::lifecycle::OperationalState<M, I> {
        self.lifecycle.operational_mut().expect(
            "operational handler reached before the lifecycle entered Operational — \
             type-invariant violation (worker dispatch / election / keepalive are \
             reachable only from OperationalState)",
        )
    }

    /// `&` access to the operational state, iff the lifecycle has reached
    /// `Operational`. `None` in every pre-`Operational` / terminal state.
    /// Used by the read-only paths that may run before the loop is fully
    /// operational (e.g. the mesh watchdog's keepalive-active count).
    pub(in crate::secondary) fn op_ref(&self) -> Option<&super::lifecycle::OperationalState<M, I>> {
        self.lifecycle.operational_ref()
    }

    /// `&mut` access to the worker pool from whichever state carries it
    /// (`Configuring` or `Operational`). `expect`-unwrapped: the
    /// pool-touching handlers run only after the pool was spawned at the
    /// `AwaitingPrimary → Configuring` entry. Used by handlers shared
    /// between the configuration and operational phases (the
    /// `report_unresolvable_task` fail-loud guard, the initial-assignment
    /// dispatch).
    #[track_caller]
    pub(in crate::secondary) fn pool_mut(
        &mut self,
    ) -> &mut dynrunner_manager_local::pool::WorkerPool<M, I> {
        self.lifecycle.pool_mut().expect(
            "worker pool reached before the lifecycle entered Configuring — \
             type-invariant violation (the pool is spawned at the \
             AwaitingPrimary → Configuring entry)",
        )
    }

    /// `&` (shared) sibling of [`Self::pool_mut`]. `None` pre-`Configuring`
    /// / terminal. Used by the read-only sampler hooks (which fire from
    /// both the initial-assignment and operational dispatch sites).
    pub(in crate::secondary) fn pool_ref(
        &self,
    ) -> Option<&dynrunner_manager_local::pool::WorkerPool<M, I>> {
        self.lifecycle.pool_ref()
    }

    /// `&mut` access to the OWN-worker `active_tasks` map from whichever
    /// state carries it (`Configuring` or `Operational`). `expect`-
    /// unwrapped: own-worker tracking runs only after the pool spawned (it
    /// is first populated by the `InitialAssignment` dispatch in
    /// `Configuring`). Used by the initial-assignment dispatch site, which
    /// runs in `Configuring` (`op_mut()` would panic there).
    #[track_caller]
    pub(in crate::secondary) fn active_tasks_mut(
        &mut self,
    ) -> &mut std::collections::HashMap<String, dynrunner_core::WorkerId> {
        self.lifecycle.active_tasks_mut().expect(
            "active_tasks reached before the lifecycle entered Configuring — \
             type-invariant violation (own-worker tracking starts at the \
             InitialAssignment dispatch in Configuring)",
        )
    }

    /// Whether the peer mesh has latched into its degraded state (the
    /// watchdog deadline elapsed with zero connected peers). Read
    /// accessor over the orthogonal [`super::lifecycle::MeshFormation`]
    /// sub-concern — a degraded mesh is NOT fatal; only the
    /// peer-mesh-dependent paths (failover election, peer-keepalive
    /// broadcasts) consult this to fail-loud-or-skip.
    pub(in crate::secondary) fn is_mesh_degraded(&self) -> bool {
        self.mesh.degraded
    }

    /// Register a [`crate::peer_lifecycle::LifecycleListener`] to be
    /// invoked off the apply path for every `PeerJoined`/`PeerRemoved`
    /// state transition. Must be called BEFORE
    /// `run_until_setup_or_done` enters; calls afterwards are dropped
    /// silently (the field is `mem::take`-d into the dispatcher on
    /// the first invocation).
    ///
    /// Single concern: own the registration surface; the dispatcher
    /// task in `crate::peer_lifecycle::dispatcher` owns the
    /// invocation semantics.
    pub fn register_lifecycle_listener(
        &mut self,
        listener: Box<dyn crate::peer_lifecycle::LifecycleListener>,
    ) {
        self.peer_lifecycle_listeners.push(listener);
    }

    /// Tear down the peer-lifecycle dispatcher task spawned at
    /// `run_until_setup_or_done`'s first entry. No-op when the
    /// dispatcher was never spawned (e.g. the coordinator's
    /// `run_until_setup_or_done` was never called). Mirrors the same
    /// helper on `PrimaryCoordinator` — see that doc for the
    /// Drop-vs-explicit design rationale.
    pub(in crate::secondary) async fn cleanup_lifecycle_dispatcher(&mut self) {
        if let Some(handle) = self.lifecycle_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Register the panik-watcher signal receiver. Must be called
    /// BEFORE `run_until_setup_or_done` enters; calls afterwards have
    /// no effect on the active loop (the field is `Option::take`-n
    /// into the loop's local state on first entry, then moved into its
    /// home on
    /// [`super::lifecycle::OperationalState::panik_signal_rx`]).
    ///
    /// Single concern: the coordinator owns the panik-react logic
    /// (announce a self-authored `ClusterMutation::PeerRemoved
    /// { SelfDeparture }` on the file-source path — observability only,
    /// no cluster-wide cancellation — kill all worker process trees,
    /// record the `Panik` lifecycle terminal and return
    /// `RunOutcome::Terminal`). The PyO3
    /// wrapper owns spawning [`crate::panik_watcher::spawn_panik_watcher`]
    /// and threading its `take_signal_rx()` here; that separation is
    /// what lets each Rust-only caller (tests, the existing `run`
    /// wrapper) skip the watcher entirely without conditional
    /// branches in the operational loop.
    pub fn register_panik_signal_rx(
        &mut self,
        rx: tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>,
    ) {
        self.panik_signal_rx = Some(rx);
    }

    /// Register an externally-armed fatal-exit signal receiver. The
    /// matching sender is handed to a run-loop-external policy (the
    /// observer's invalid_task monitor) that cannot reach `self.fatal_exit`
    /// directly because it runs on the task-completed dispatcher task. On
    /// the first message, the `process_tasks` select! arm latches
    /// `self.fatal_exit` with the carried reason and the run exits
    /// non-zero. Pre-`run_until_setup_or_done` contract, same single-shot
    /// shape as [`Self::register_panik_signal_rx`]; absent registration
    /// leaves the arm parked on `pending()`.
    pub fn register_fatal_exit_signal_rx(
        &mut self,
        rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        self.fatal_exit_signal_rx = Some(rx);
    }

    /// Register a [`crate::task_completed::TaskCompletedListener`].
    /// Same single-shot, pre-`run_until_setup_or_done`-only contract
    /// as [`Self::register_lifecycle_listener`].
    pub fn register_task_completed_listener(
        &mut self,
        listener: Box<dyn crate::task_completed::TaskCompletedListener>,
    ) {
        self.task_completed_listeners.push(listener);
    }

    /// Wire the upload-action port (#336 P1) this secondary's in-process
    /// setup executor uses to perform an assigned upload setup task's file
    /// upload. Set before `run`. Absence leaves the executor with no
    /// uploader — an assigned setup task carrying an upload-file ref then
    /// fails as a wiring error (a no-ref setup task no-op-succeeds). See
    /// [`crate::upload_action`].
    pub fn set_upload_action(
        &mut self,
        action: std::sync::Arc<dyn crate::upload_action::UploadAction>,
    ) {
        self.upload_action = Some(action);
    }

    /// Wire the import-action port (#497 P4) this secondary's run-once affine
    /// executor uses to perform an assigned work task's gating SecondaryAffine
    /// import. Set before `run`. Absence leaves the executor with no importer
    /// — a work task that gates on a SecondaryAffine import then fails as a
    /// wiring error (a work task with no affine dependency runs unchanged).
    /// See [`crate::affine_action`].
    pub fn set_import_action(
        &mut self,
        action: std::sync::Arc<dyn crate::affine_action::ImportAction<I>>,
    ) {
        self.import_action = Some(action);
    }

    /// Wire the OPTIONAL per-(gate,node) satisfied probe (#537) this
    /// secondary's run-once affine executor consults BEFORE invoking the
    /// `ImportAction`. A registered probe lets the PRODUCING node (the
    /// member that built and published the gate's product) skip the
    /// entire run-once scaffolding — no `tokio::task::spawn_local`, no
    /// `QueuedAfterLocalDependency` / `LocalDependencyReleased` frames —
    /// when it already holds the closure locally. Set before `run`;
    /// absence (the default) leaves the executor with today's behaviour
    /// bit-for-bit. See [`crate::affine_satisfied`].
    pub fn set_affine_satisfied_probe(
        &mut self,
        probe: std::sync::Arc<dyn crate::affine_satisfied::AffineSatisfiedProbe<I>>,
    ) {
        self.affine_satisfied_probe = Some(probe);
    }

    /// Register a [`crate::worker_messages::WorkerMessageListener`]
    /// (the consumer's duck-typed `worker_message_listener`
    /// TaskDefinition hook, bridged through PyO3). Same single-shot,
    /// pre-`run_until_setup_or_done`-only contract as
    /// [`Self::register_task_completed_listener`].
    pub fn register_worker_message_listener(
        &mut self,
        listener: Box<dyn crate::worker_messages::WorkerMessageListener>,
    ) {
        self.worker_message_listeners.push(listener);
    }

    /// Clone of the secondary control-plane ingress sender. External
    /// surfaces (the PyO3 `SecondaryHandle`) queue
    /// [`super::control::SecondaryControlCommand`]s here; the
    /// `process_tasks` select drains them on the operational loop's
    /// own thread (the dispatch-decoupling law — no foreign task ever
    /// touches the pool).
    pub fn secondary_control_sender(
        &self,
    ) -> tokio::sync::mpsc::UnboundedSender<super::control::SecondaryControlCommand> {
        self.secondary_control_tx.clone()
    }

    /// Accept the per-phase lifecycle hooks for the post-promotion path.
    /// Mirrors the shape `PrimaryCoordinator::run` accepts:
    /// `on_phase_start(&PhaseId)` fires when a phase flips Blocked →
    /// Active; `on_phase_end(&PhaseId, completed, failed)` fires when
    /// the phase reaches `Drained`.
    ///
    /// Must be called before `run_until_setup_or_done` enters.
    ///
    /// The secondary holds NO phase machine and never fires these
    /// itself. It is a registration ANCHOR (R4 SEAM): pyo3 keeps the
    /// `register_phase_lifecycle_callbacks` pre-run contract stable for
    /// callers minting a handle from a secondary; the authoritative
    /// `PrimaryCoordinator` — which owns the phase machine — is the
    /// real fire site that R4 re-homes the registration onto.
    ///
    /// Single concern: accept ownership of the boxed GIL-reacquiring
    /// closures from the PyO3 wrapper and hold them as the wiring anchor.
    pub fn register_phase_lifecycle_callbacks(
        &mut self,
        on_phase_start: crate::primary::OnPhaseStart,
        on_phase_end: crate::primary::OnPhaseEnd,
    ) {
        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
    }

    /// Register the consumer's run-config finalize policy. Must be called
    /// BEFORE [`Self::run`]; same pre-run, single-shot family as the other
    /// `register_*` policy hooks.
    ///
    /// With a finalize registered, the `AwaitingPrimary → Configuring`
    /// transition fires it ONCE — after ensuring the post-welcome `RunConfig`
    /// push has landed (sending an in-band `RequestRunConfig` backstop if it
    /// has not) and BEFORE [`Self::initialize_workers`] spawns the pool — so
    /// the per-type `cmd_args` the closure re-derives from the delivered argv
    /// are live for the initial workers (and every respawn). The framework
    /// (this coordinator) owns the DRIVE / timing; the reparse + cmd_args
    /// rebuild is the consumer's POLICY (the pyo3 wrapper supplies the
    /// closure). The `args=` path (compiler_suit) registers an IDENTITY
    /// finalizer (Some) — the seam fires but is a faithful no-op (identity
    /// ignores the argv; the rebuild is byte-identical). Only Rust-only test
    /// fixtures register `None`, which skips the seam entirely.
    ///
    /// Single concern: own the registration surface for the finalize policy;
    /// the cmd_args swap itself is the closure's (consumer's) concern.
    pub fn register_finalize_run_config(&mut self, finalize: super::FinalizeRunConfigFn) {
        self.finalize_run_config = Some(finalize);
    }

    /// Clone of the SHARED node-local run-config handle (single source of
    /// truth). The pyo3 promotion recipe captures this clone so it reads the
    /// DELIVERED `forwarded_argv` (post-push) at the promotion instant, rather
    /// than a stale boot-time copy — without the recipe ever borrowing the
    /// coordinator. The responder and the finalize fire read the same handle;
    /// [`Self::store_pushed_run_config`] is the single writer.
    pub fn run_config_handle(&self) -> std::sync::Arc<std::sync::Mutex<Vec<String>>> {
        self.forwarded_argv.clone()
    }

    /// Tear down the task-completion dispatcher task. Mirrors
    /// [`Self::cleanup_lifecycle_dispatcher`] — same Drop-vs-explicit
    /// design rationale.
    pub(in crate::secondary) async fn cleanup_task_completed_dispatcher(&mut self) {
        if let Some(handle) = self.task_completed_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Spawn the worker custom-message dispatcher (first call only —
    /// the receiver is `take()`n, so re-entry is a no-op). The
    /// worker-event bridge only `tx.send()`s; the consumer's
    /// `worker_message_listener` (Python, GIL-bound) fires on this
    /// dispatcher task, strictly off the operational loop. Extracted
    /// from `run_until_setup_or_done_inner` so tests can stand up the
    /// REAL pipeline.
    pub(in crate::secondary) fn spawn_worker_message_dispatcher(&mut self) {
        use tracing::Instrument as _;
        if let Some(rx) = self.worker_message_rx.take() {
            let listeners = std::mem::take(&mut self.worker_message_listeners);
            let handle = tokio::task::spawn_local(
                crate::worker_messages::run_worker_message_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.worker_message_dispatcher_handle = Some(handle);
        }
    }

    /// Abort + join the worker-message dispatcher task. Mirrors
    /// [`Self::cleanup_task_completed_dispatcher`] — same
    /// Drop-vs-explicit cleanup rationale.
    pub(in crate::secondary) async fn cleanup_worker_message_dispatcher(&mut self) {
        if let Some(handle) = self.worker_message_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Forward an observer-mode announcer attach onto the underlying
    /// [`ClusterState`] AND allocate the coordinator-side outbox the
    /// production [`crate::observer::announcer::PeerMeshAnnouncerSender`]
    /// posts into. Returns the bundle the caller subsequently spawns
    /// the announcer task with.
    ///
    /// # Outputs
    ///
    /// - `handle`: the [`crate::observer::AnnouncerHandle`] threaded
    ///   into `run_observer_announcer` for `rx` / `holdings` /
    ///   `peer_id` / `primary_epoch_mirror`. Same shape as the
    ///   pre-outbox API.
    /// - `sender`: the production [`PeerMeshAnnouncerSender`] passed
    ///   into `run_observer_announcer` as the
    ///   [`crate::observer::announcer::AnnouncerSender`] impl. Holds
    ///   a clone of the freshly-built outbox sender.
    ///
    /// # Side effects on `self`
    ///
    /// - Builds the outbox (`mpsc::channel::<AnnouncerOutboxItem<I>>`),
    ///   stashes the receiver on `self.announcer_outbox_rx`, and the
    ///   sender clone on `self.announcer_outbox_tx`. The receiver is
    ///   later taken by `process_tasks` for its drain arm; the sender
    ///   clone on `self` is held purely to keep the channel alive
    ///   when the announcer task's clone gets dropped (so the drain
    ///   arm doesn't observe a spurious close).
    ///
    /// # Single concern
    ///
    /// Bundle the three independent setup steps (CRDT hook registration,
    /// outbox channel allocation, production sender construction) into
    /// one call so the late-joiner dispatcher does not have to know
    /// the wiring details. Each constituent piece is testable in
    /// isolation through the underlying APIs
    /// ([`crate::observer::attach_observer_announcer`] for the hook,
    /// the announcer's tests for the sender contract).
    pub fn attach_observer_announcer(
        &mut self,
        holdings: std::collections::HashSet<String>,
    ) -> (
        crate::observer::AnnouncerHandle,
        crate::observer::announcer::PeerMeshAnnouncerSender<I>,
    ) {
        let peer_id = self.config.secondary_id.clone();
        let handle = crate::observer::attach_observer_announcer(
            &mut self.cluster_state,
            holdings,
            peer_id.clone(),
        );
        // Capacity 32: an announcer trigger fires once per role-change
        // event; the announcer's retry-on-failure loop is rate-limited
        // by `MAX_BACKOFF=5s` so the steady-state in-flight count is
        // ≤1. 32 absorbs a flap-burst without applying back-pressure
        // to the announcer task (which would deadlock against the
        // drain arm if both ran on the same LocalSet).
        const ANNOUNCER_OUTBOX_CAPACITY: usize = 32;
        let (outbox_tx, outbox_rx) = tokio::sync::mpsc::channel::<
            crate::observer::announcer::AnnouncerOutboxItem<I>,
        >(ANNOUNCER_OUTBOX_CAPACITY);
        self.announcer_outbox_rx = Some(outbox_rx);
        self.announcer_outbox_tx = Some(outbox_tx.clone());
        let sender = crate::observer::announcer::PeerMeshAnnouncerSender::new(peer_id, outbox_tx);
        (handle, sender)
    }

    /// Record the run-config dispatch flags (`pre_staged_mode` /
    /// `uses_file_based_items`) the primary stamped into this secondary's
    /// `InitialAssignment`. The SINGLE writer to the shared
    /// [`super::StagingDispatchContext`] handle, whose SOLE reader is the
    /// dispatch resolver ([`Self::resolve_for_dispatch`]). Called from
    /// `wait_for_setup`'s `InitialAssignment` handler. (The promotion recipe
    /// does NOT read this cell — a relocate-target's cell is at `Default` at
    /// promotion; the recipe sources the flags from the node's own local
    /// producer instead.)
    pub(in crate::secondary) fn set_staging_dispatch_context(
        &mut self,
        ctx: super::StagingDispatchContext,
    ) {
        *self
            .staging_dispatch_context
            .lock()
            .expect("staging_dispatch_context mutex poisoned") = ctx;
    }

    /// Read the current staging-dispatch context off the shared handle.
    ///
    /// The DISPATCH-side reader: `resolve_for_dispatch` consults this so a
    /// PLAIN secondary executing assigned tasks keys off the flags its primary
    /// stamped into the `InitialAssignment` (the cell's sole writer). NOT the
    /// promotion-recipe source — a relocate-target's cell is at `Default` at
    /// promotion (no `InitialAssignment` yet), so the recipe sources the two
    /// flags from this node's own local producer instead (see
    /// `managers/secondary/run.rs::extract_staging_dispatch_flags`).
    fn staging_dispatch_context(&self) -> super::StagingDispatchContext {
        *self
            .staging_dispatch_context
            .lock()
            .expect("staging_dispatch_context mutex poisoned")
    }

    /// Clone of the shared cell handle, for the relocate-staging Tier-2 test:
    /// it captures the relocate-target's live cell into the promote recipe to
    /// PROVE the cell stays at `Default` at promotion (no `InitialAssignment`)
    /// while the recipe still stamps the correct flags from the local producer.
    /// Not on the production path (the recipe no longer reads the cell).
    #[cfg(test)]
    pub(crate) fn staging_dispatch_context_handle_for_test(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<super::StagingDispatchContext>> {
        self.staging_dispatch_context.clone()
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
    pub(in crate::secondary) fn resolve_for_dispatch(
        &mut self,
        zip_ref: Option<&str>,
        local_path: &str,
        file_hash: &str,
    ) -> Option<std::path::PathBuf> {
        let staging_ctx = self.staging_dispatch_context();
        if !staging_ctx.uses_file_based_items {
            return Some(std::path::PathBuf::from(local_path));
        }
        // In pre-staged mode the primary doesn't compute a content
        // hash (no transfer), so pass None and let the resolver
        // accept by existence. Otherwise hash-verify like the
        // historical path.
        let expected_content_hash = if staging_ctx.pre_staged_mode {
            None
        } else {
            Some(file_hash)
        };
        self.extraction_cache
            .resolve_binary(zip_ref, local_path, file_hash, expected_content_hash)
    }

    /// Set certificate info for peer connections. Must be called before `run()`
    /// if peer-to-peer QUIC is enabled.
    pub fn set_peer_cert_info(&mut self, info: PeerCertInfo) {
        self.peer_cert_info = Some(info);
    }

    /// Record this node's OWN liveness-beacon listener UDP port. Called by
    /// the run boundary after it binds the [`crate::liveness::LivenessListener`],
    /// BEFORE `run()` (so the value is on hand when `send_cert_exchange`
    /// advertises it). Advertised in this node's
    /// `CertExchange.liveness_port`.
    pub fn set_liveness_port(&mut self, port: u16) {
        self.liveness_port = Some(port);
    }

    /// A clone of the beacon-target cell. The run boundary hands this to
    /// [`crate::liveness::LivenessBeacon::spawn`] so the dedicated beacon
    /// thread reads the current primary's liveness address the coordinator
    /// publishes into it.
    pub fn beacon_target(&self) -> crate::liveness::BeaconTarget {
        self.beacon_target.clone()
    }

    /// Install the node's [`crate::liveness::BeaconLiveness`] POLL view
    /// (a clone of the one the [`crate::liveness::LivenessListener`]
    /// publishes into). Called by the run boundary after it binds the
    /// listener, BEFORE `run()`. The failover-detector consults this view's
    /// entry for the current primary as the UNION counterpart of the
    /// mesh-frame liveness legs, so a CPU-starved-but-beaconing primary is
    /// not false-elected against. Absent this call the view stays empty and
    /// the union degrades to mesh-frame-only (the prior behaviour).
    pub fn set_beacon_liveness(&mut self, view: crate::liveness::BeaconLiveness) {
        self.beacon_liveness = view;
    }

    /// Clone of the cross-thread `PrimaryCommand` sender. Callers
    /// (PyO3 `PrimaryHandle` minted from a `PySecondaryCoordinator`,
    /// future Rust-side control planes) clone this BEFORE invoking
    /// `run_until_setup_or_done()` so they have an ingress for "from
    /// outside the operational loop, please apply this mutation".
    /// The sender itself is `Clone` and `Send` so the returned handle
    /// is freely passable across threads / async runtimes. Mirrors
    /// `PrimaryCoordinator::command_sender` exactly.
    pub fn command_sender(&self) -> tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>> {
        self.command_tx.clone()
    }

    /// Swap the internal command-channel pair for an externally-
    /// supplied one. The PyO3 layer uses this so the
    /// `PrimaryHandle` it exposes to Python at `__init__` time is the
    /// same channel the (later-constructed) `SecondaryCoordinator`
    /// reads from — without this, the channel created in `new()`
    /// can't be reached from Python before
    /// `run_until_setup_or_done()` starts because the coordinator
    /// itself is built inside the detached tokio runtime.
    ///
    /// Must be called BEFORE `run_until_setup_or_done()` enters the
    /// `process_tasks` loop; calling it after the loop has taken the
    /// receiver out (via `command_rx.take()`) replaces the stored-
    /// back receiver but the loop has already moved on to the local
    /// copy. The PyO3 surface enforces this with the same
    /// "before run() only" contract `PrimaryCoordinator` uses.
    pub fn replace_command_channel(
        &mut self,
        tx: tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>>,
        rx: tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>,
    ) {
        self.command_tx = tx;
        self.command_rx = Some(rx);
    }

    /// Late-joiner bootstrap entry: install a snapshot received from a
    /// peer's snapshot-stream response, then mark setup as
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
    /// primary-handshake setup". The `run_until_setup_or_done` entry's
    /// `if !self.setup_phase_completed { … }` guard is the single source of
    /// truth for "skip setup"; we just set the latch.
    ///
    /// # Why a dedicated entry-point (not an inline `cluster_state` +
    /// `setup_phase_completed` writer on the caller)
    ///
    /// `cluster_state` and `setup_phase_completed` are intentionally
    /// `pub(in crate::secondary)` — the secondary module owns the latch's lifecycle
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
    /// = true` and route straight to `process_tasks`. The
    /// `cluster_state.restore` populates `current_primary` from the
    /// snapshot, so the egress edge ([`Self::send_to`]) resolves
    /// `Destination::Primary` to the live primary immediately (no
    /// bootstrap link needed — a late-joiner never dialled one).
    pub fn restore_from_snapshot_and_skip_setup(
        &mut self,
        snap: crate::cluster_state::ClusterStateSnapshot<I>,
    ) {
        self.cluster_state.restore(snap);
        // Land the lifecycle directly in `Operational` with an EMPTY pool
        // (a re-bootstrapping late-joiner runs no workers until it pulls
        // its own). This replaces the old `setup_phase_completed = true`
        // bool poke: the lifecycle projection `setup_phase_completed()` is
        // true for `Operational`, so the next `run_until_setup_or_done`
        // skips the welcome/cert/wait-for-setup handshake and routes
        // straight to `process_tasks`. Observer non-candidacy is enforced
        // entirely peer-side (the election machine filters peers in the
        // replicated `RoleTable.observers` from candidate selection); a
        // standalone observer is the `ObserverCoordinator`, not a lifecycle
        // variant of this coordinator — hence a direct `Operational`
        // construction.
        //
        // The take-once runtime latches are NOT consumed here: they stay
        // on the coordinator's `Option` fields and are surrendered at the
        // SINGLE `process_tasks`-entry boundary (uniform with the normal
        // path), so this constructs the state shell with an empty
        // `OperationalLatches` and discards it. The `PrimaryLink` is built
        // fresh from config, exactly as the normal `Operational` entry
        // does.
        let primary_link = PrimaryLink::with_failover_threshold(
            self.config.primary_link_failure_threshold,
            self.config.primary_link_failure_window,
        );
        let (lifecycle, _empty) =
            SecondaryLifecycle::operational_observer(OperationalLatches::empty(), primary_link);
        self.lifecycle = lifecycle;
    }

    /// Cluster-wide count of successfully-completed tasks, read off the
    /// replicated CRDT (`cluster_state.outcome_counts().succeeded`).
    ///
    /// A pure observer (and every non-authority secondary) keeps NO
    /// per-node completed/failed/total counter: terminal state is read
    /// ONLY from the CRDT, which every replica converges to. This is
    /// what the late-joiner observer's `completed` getter surfaces — it
    /// reflects every completion visible in the restored snapshot plus
    /// any the observer ingested live, regardless of which node ran the
    /// task.
    pub fn completed_count(&self) -> usize {
        self.cluster_state.outcome_counts().succeeded
    }

    /// The per-secondary terminal outcome, or `None` if the lifecycle has
    /// not reached a terminal.
    ///
    /// The single source of truth for "how did this secondary end" is the
    /// [`SecondaryLifecycle`] terminal; this accessor projects it to the
    /// public [`RunOutcome`](super::RunOutcome)'s sibling boundary type
    /// [`super::SecondaryTerminal`] for the PyO3 wrapper, which reads it
    /// after `run_until_setup_or_done` reports `RunOutcome::Terminal` to
    /// decide the process exit code (`Done`→Ok, `Aborted`→`exit(1)`,
    /// `Panik`→`exit(137)`).
    pub fn terminal(&self) -> Option<super::SecondaryTerminal> {
        self.lifecycle.terminal()
    }

    /// Drive the lifecycle to the `Done` terminal (normal completion).
    pub(in crate::secondary) fn enter_terminal_done(&mut self) {
        self.replace_lifecycle(SecondaryLifecycle::enter_done);
    }

    /// Drive the lifecycle to the `Aborted` terminal, carrying the
    /// cluster-wide abort reason.
    pub(in crate::secondary) fn enter_terminal_aborted(&mut self, reason: String) {
        self.replace_lifecycle(|lc| lc.enter_aborted(reason));
    }

    /// Drive the lifecycle to the `Panik` terminal, carrying the matched
    /// panik file path + reason.
    pub(in crate::secondary) fn enter_terminal_panik(
        &mut self,
        matched_path: std::path::PathBuf,
        reason: String,
    ) {
        self.replace_lifecycle(|lc| lc.enter_panik(matched_path, reason));
    }

    /// Drive the lifecycle to the `Failed` terminal, carrying the
    /// `fatal_exit` reason the run loop also propagates as its `Err`.
    pub(in crate::secondary) fn enter_terminal_failed(&mut self, reason: String) {
        self.replace_lifecycle(|lc| lc.enter_failed(reason));
    }

    /// Drive the lifecycle to the `BringUpFailed` terminal (the
    /// setup-instructions wait expired), carrying the deadline-expiry
    /// diagnosis the run loop also propagates as its `Err`.
    pub(in crate::secondary) fn enter_terminal_bring_up_failed(&mut self, reason: String) {
        self.replace_lifecycle(|lc| lc.enter_bring_up_failed(reason));
    }

    /// Move the lifecycle out, apply a by-value terminal transition, and
    /// store it back. The terminal `enter_*` transitions consume `self` by
    /// value (they are absorbing), so the move-out/move-in is the only way
    /// to drive them through a `&mut self` coordinator method; the
    /// placeholder is the cheap `Connecting` variant.
    fn replace_lifecycle(
        &mut self,
        transition: impl FnOnce(SecondaryLifecycle<M, I>) -> SecondaryLifecycle<M, I>,
    ) {
        let lifecycle = std::mem::replace(&mut self.lifecycle, SecondaryLifecycle::connecting());
        self.lifecycle = transition(lifecycle);
    }

    /// Read-only borrow of the replicated cluster ledger.
    ///
    /// # Single concern
    ///
    /// A shared, non-mutating view of the CRDT for callers that
    /// PROJECT it (e.g. the observer's CRDT-derived periodic stats
    /// reporter, which reads the ledger through
    /// `StatsSnapshot::from_cluster_state`). It exposes exactly the
    /// `&ClusterState<I>` the public projection accessors
    /// (`tasks_iter`/`counts`/`outcome_counts`/`phase_deps`) already
    /// hang off — no authority, no mutation, no pool.
    ///
    /// # Why a borrow and not an owned/shared handle
    ///
    /// The coordinator owns its `cluster_state` field by value; the
    /// CRDT is deliberately NOT `Arc`-shared (that would ripple into
    /// every apply site). A `&self` borrow keeps that ownership intact
    /// and is the correct shape for a synchronous, in-loop projection
    /// at a point where the caller already holds `&SecondaryCoordinator`
    /// (the late-joiner observer publishes its projection right after
    /// `restore_from_snapshot_and_skip_setup` and on each run-loop
    /// return — both `&self`-reachable moments).
    pub fn cluster_state(&self) -> &ClusterState<I> {
        &self.cluster_state
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

    /// Test accessor: snapshot of `self.sampler.is_some()`. Mirrors
    /// `LocalManager::sampler_is_some` — used by the secondary's
    /// memprofile lifecycle test to pin "constructed iff
    /// `output_dir` was set, torn down by terminal cleanup".
    #[cfg(test)]
    pub(in crate::secondary) fn sampler_is_some(&self) -> bool {
        self.sampler.is_some()
    }

    /// Test seam mirroring `LocalManager::install_sampler_for_test`:
    /// stand up the memprofile sampler on a coordinator built
    /// outside the `run_until_setup_or_done` runtime-context dance.
    /// Lets sampler-hook integration tests fire
    /// `notify_sampler_assigned` / `notify_sampler_completed` /
    /// `notify_sampler_disconnected` directly without going through
    /// the full `process_tasks` loop.
    #[cfg(test)]
    pub(in crate::secondary) fn install_sampler_for_test(
        &mut self,
        sampler: dynrunner_manager_local::memprofile::MemProfileSampler,
    ) {
        self.sampler = Some(sampler);
    }

    /// Test seam: land the lifecycle DIRECTLY in `Operational` with a
    /// live operational state (empty pool, `ElectionState::Normal`, empty
    /// peer-keepalives, a fresh `PrimaryLink` from config, empty
    /// pending/active collections), bypassing the full
    /// handshake/`wait_for_setup`/`enter_operational` boundary.
    ///
    /// This is the test analog of `restore_from_snapshot_and_skip_setup`'s
    /// `operational_observer` construction, for the (non-observer) tests
    /// that drive the operational handlers — election state machine,
    /// peer-keepalive/primary-liveness tracking, the worker pool — via
    /// direct method calls. Those tests construct `make_secondary()` (which
    /// starts `Connecting`, where no operational state exists) and then
    /// reach the operational fields; they must call this first so
    /// [`Self::op_mut`] / [`Self::op_ref`] / [`Self::pool_mut`] resolve.
    /// Election semantics depend on the peer-side `RoleTable.observers`
    /// filter (not the state shape), so reusing the empty-pool
    /// `operational_observer` lifecycle constructor here is faithful.
    ///
    /// `take`-s the take-once latch `Option` fields (matching the single
    /// `enter_operational` consumption boundary) and discards them; tests
    /// driving the loop end-to-end go through `run_until_setup_or_done`
    /// instead, which owns the real latch round-trip.
    #[cfg(test)]
    pub(in crate::secondary) fn enter_operational_for_test(&mut self) {
        let primary_link = PrimaryLink::with_failover_threshold(
            self.config.primary_link_failure_threshold,
            self.config.primary_link_failure_window,
        );
        let (lifecycle, _empty) =
            SecondaryLifecycle::operational_observer(OperationalLatches::empty(), primary_link);
        self.lifecycle = lifecycle;
    }

    /// Test seam: synchronously spill EVERY settle-eligible ledger entry of
    /// this coordinator's `cluster_state` to a per-test `spill_path`,
    /// returning the entries evicted. The affine-gate spill-hole tests need
    /// the spilled state to live INSIDE the coordinator's `cluster_state`
    /// (so `unmet_local_affine_dep` reads it); the spill goes to the
    /// caller-owned unique path — NOT the role-shared file the production
    /// driver attached at construction — so it is isolated from every other
    /// secondary the parallel test run constructs. `spill_path` (and its
    /// parent dir) must outlive every later settled read of these entries.
    #[cfg(test)]
    pub(in crate::secondary) fn force_settled_spill_for_test(
        &mut self,
        spill_path: &std::path::Path,
    ) -> usize {
        // Drop the production driver's writer (bound to the process-shared
        // `settled_CRDT.secondary.cbor`) and re-spill over the isolated path.
        self.cluster_state.detach_spill_writer_for_test();
        self.cluster_state.test_spill_all(spill_path)
    }

    /// Test seam mirroring
    /// `LocalManager::install_worker_subcgroup_for_test`: inject a
    /// `SubcgroupHandle` onto an existing worker slot so the
    /// sampler-hook integration test can hand the sampler a
    /// tempdir-rooted leaf path. In production the pool's spawn site
    /// materialises the handle before `factory.spawn_worker`; tests
    /// that use the in-process channel factories never enter that
    /// code path, hence this seam.
    #[cfg(test)]
    pub(in crate::secondary) fn install_worker_subcgroup_for_test(
        &mut self,
        worker_id: dynrunner_core::WorkerId,
        handle: dynrunner_manager_local::cgroup::SubcgroupHandle,
    ) {
        self.pool_mut().workers[worker_id as usize].subcgroup = Some(handle);
    }

    /// Run the secondary coordination loop:
    /// 1. Initialize local workers
    /// 2. Send welcome and cert exchange to primary
    /// 3. Wait for peer list, initial assignment, transfer complete
    /// 4. Process tasks: receive assignments, run on local workers, report back
    ///
    /// The single production secondary entry (the `Node::run` secondary arm
    /// drives this). It wraps `run_until_setup_or_done` and, on a terminal,
    /// reads the per-secondary terminal off the lifecycle (the single source
    /// of truth) and projects it to a `Result`: `Done`⇒`Ok`,
    /// `Aborted`/`Panik`/`Failed`⇒`Err`. (The `Node::run` secondary arm
    /// surfaces this `Err`; the pyo3 boundary maps the lifecycle terminal to
    /// the `std::process::exit` code via [`Self::terminal`].)
    pub async fn run(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        // Reached a terminal — read the per-secondary terminal off the
        // lifecycle (the single source of truth). The PyO3 wrapper takes the
        // structured terminal (via `terminal()`) and calls
        // `std::process::exit` (137 panik / 1 abort); this `run` path
        // surfaces `Aborted`/`Panik` as a normal String error and `Done` as
        // `Ok`. (`Failed` never reaches here — `fatal_exit` propagates as the
        // run loop's `Err` before this match.)
        let RunOutcome::Terminal = self.run_until_setup_or_done(factory).await?;
        match self.lifecycle.terminal() {
            Some(super::SecondaryTerminal::Done) | None => Ok(()),
            Some(super::SecondaryTerminal::Panik {
                matched_path,
                reason,
            }) => Err(format!(
                "secondary panik shutdown: {reason} (matched_path={})",
                matched_path.display()
            )),
            Some(super::SecondaryTerminal::Aborted { reason }) => {
                Err(format!("run aborted by primary: {reason}"))
            }
            // Like `Failed`, the bring-up expiry normally propagates as the
            // run loop's `Err` before this match (the deadline site returns
            // `Err(reason)` after recording the terminal); both arms exist
            // for the same defensive completeness.
            Some(
                super::SecondaryTerminal::Failed { reason }
                | super::SecondaryTerminal::BringUpFailed { reason },
            ) => Err(reason),
        }
    }

    /// Drive the secondary coordination loop until it either yields
    /// reaches a terminal (`RunOutcome::Terminal`, with the specific
    /// per-secondary terminal recorded on the lifecycle and readable via
    /// [`Self::terminal`]).
    ///
    /// Enters `AwaitingPrimary`, runs the setup handshake (welcome / cert
    /// exchange / wait_for_setup) under `config.unconfigured_deadline` —
    /// `wait_for_setup` spawns the worker pool and enters `Configuring` on
    /// the first primary frame — then `process_tasks` drives the
    /// `Configuring → Operational` transition and runs the loop.
    ///
    /// Cancel-safety: `process_tasks` already documents that every
    /// arm of its `select!` is cancel-safe (mpsc recv + tokio
    /// interval ticks). No state is dropped except in-flight recv
    /// futures, which are cancel-safe by construction.
    ///
    /// # Cleanup discipline
    ///
    /// Thin wrapper around [`Self::run_until_setup_or_done_inner`]
    /// whose secondary concern is to drive the peer-lifecycle
    /// dispatcher's abort-on-exit contract. Every exit path flows
    /// through `cleanup_lifecycle_dispatcher` before returning, so the
    /// spawned dispatcher task is always aborted and joined before the
    /// caller observes the result.
    pub async fn run_until_setup_or_done(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        // Role-tag the whole secondary run future so every event this task
        // emits is attributed to the secondary role and routed to the
        // per-role full log. This is the single entry all production
        // secondary paths flow through (the legacy `run` wrapper delegates
        // here), so one span here covers them all. A secondary that never
        // promotes only ever carries this span → `secondary.log`; a peer
        // that activates a same-peer primary spawns a SEPARATE task whose
        // own primary span keeps that authority's events in `primary.log`.
        // See `dynrunner_core::role_span`.
        let span = tracing::info_span!(
            dynrunner_core::SECONDARY_ROLE_SPAN,
            kind = "secondary",
            id = %self.config.secondary_id
        );
        async {
            let result = self.run_until_setup_or_done_inner(factory).await;
            self.cleanup_lifecycle_dispatcher().await;
            // Independent of `cleanup_lifecycle_dispatcher`.
            self.cleanup_task_completed_dispatcher().await;
            // Independent of both — same abort-on-exit contract.
            self.cleanup_worker_message_dispatcher().await;
            result
        }
        .instrument(span)
        .await
    }

    /// Original `run_until_setup_or_done` body, factored out so the
    /// public wrapper can drive cleanup-on-exit regardless of how
    /// this function returns. See [`Self::run_until_setup_or_done`]
    /// for the rationale.
    async fn run_until_setup_or_done_inner(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        // Spawn the peer-lifecycle dispatcher on first entry (idempotent:
        // once the receiver has been taken this branch is a no-op). The
        // sender end was installed on `cluster_state` in `new()` so
        // any apply that lands before the dispatcher polls queues on
        // the unbounded channel and drains here. `spawn_local`
        // matches the rest of the secondary's LocalSet-bound spawn
        // pattern.
        //
        // The returned `JoinHandle` is stored on `self` so
        // `cleanup_lifecycle_dispatcher` (called from the outer
        // wrapper on exit) can abort the task and await its
        // termination. Without this, an error-return from inside the
        // run loop would leave the dispatcher blocked on its input
        // channel forever (the sender on `cluster_state` is still
        // alive as long as the coordinator object is).
        // Propagate the secondary role span (current here, inside the
        // instrumented `run_until_setup_or_done` future) into the spawned
        // dispatcher tasks so the events THEY emit are attributed to the
        // secondary role too. `spawn_local` otherwise detaches the span
        // context. See `dynrunner_core::role_span`.
        if let Some(rx) = self.lifecycle_rx.take() {
            let listeners = std::mem::take(&mut self.peer_lifecycle_listeners);
            let handle = tokio::task::spawn_local(
                crate::peer_lifecycle::run_peer_lifecycle_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.lifecycle_dispatcher_handle = Some(handle);
        }
        // Same shape for the task-completion dispatcher: spawn on
        // first entry only (once the receiver has been moved the
        // take() returns None and the branch is a no-op).
        if let Some(rx) = self.task_completed_rx.take() {
            let listeners = std::mem::take(&mut self.task_completed_listeners);
            let handle = tokio::task::spawn_local(
                crate::task_completed::run_task_completed_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }
        // Same shape for the worker custom-message dispatcher — see
        // [`Self::spawn_worker_message_dispatcher`].
        self.spawn_worker_message_dispatcher();
        // Enter `AwaitingPrimary` (`Connecting → AwaitingPrimary`): the
        // secondary is now actively trying to reach a primary, but none
        // has announced yet. The peer mesh keeps forming (the orthogonal
        // `MeshFormation` sub-concern is untouched by this transition).
        // No worker pool, no task acceptance, no election, no keepalive in
        // this state — only the setup handshake below. Idempotent: a no-op
        // from any state other than `Connecting` (a late-joiner observer is
        // already `Operational`, so this leaves it unchanged).
        self.lifecycle = std::mem::replace(&mut self.lifecycle, SecondaryLifecycle::connecting())
            .enter_awaiting_primary();

        // Terminal-during-setup signal. `Some(RunOutcome::Terminal)` means
        // a RunComplete / RunAborted CRDT flag landed while `wait_for_setup`
        // was still waiting on the trio — the run is over, so the operational
        // handoff (`process_tasks`) is skipped and control routes straight to
        // the terminal-match teardown below (which the lifecycle terminal,
        // already recorded by `wait_for_setup`, selects). Stays `None` on the
        // normal trio-completion path.
        let mut setup_terminal: Option<RunOutcome> = None;

        // Skip the per-secondary setup phase once the lifecycle has
        // reached `Operational` (or terminal) — the `setup_phase_completed`
        // projection replaces the old flat bool latch. This gates the
        // late-joiner observer (which `restore_from_snapshot_and_skip_setup`
        // landed directly in `Operational`, so workers are alive and the
        // handshake frames are already consumed).
        if !self.lifecycle.setup_phase_completed() {
            tracing::info!(
                secondary = %self.config.secondary_id,
                workers = self.config.num_workers,
                resources = %self.config.max_resources,
                "secondary starting"
            );

            // NOTE: the worker pool and the memprofile sampler are NO
            // LONGER built here. The typed lifecycle relocates the spawn
            // to the `AwaitingPrimary → Configuring` entry, fired by
            // `wait_for_setup` on the FIRST primary-originated setup frame
            // (the announce). If the primary never announces, the
            // lifecycle never leaves `AwaitingPrimary` and no worker pool
            // is ever built. See `enter_configuring_on_first_primary_frame`.

            // The pre-`Operational` span (`AwaitingPrimary` + the
            // `Configuring` excursion `wait_for_setup` drives) is bounded
            // by `unconfigured_deadline` (default 10 min) — the long
            // pre-config horizon that SUPERSEDES the old 60s
            // `setup_deadline`. It is generous because a slow authority
            // `discover_items` walk can legitimately delay the first
            // announcement; the SHORT election deadline is a property of
            // `Operational` and physically cannot fire here. The deadline
            // is applied at the orchestration boundary, NOT inside
            // `wait_for_setup` (cancelling the whole setup future on
            // expiry is safe because we never re-enter any of these
            // phases — we go straight to cleanup-and-exit), but it is the
            // RE-ARMABLE primary-liveness deadline (`setup_deadline.rs`),
            // not a fixed `timeout`: `wait_for_setup` EXTENDS it on every
            // frame whose sender is the primary, so it elapses only after
            // a full horizon of PRIMARY SILENCE. A live primary still
            // assembling its fleet (its setup-liveness digest beacon, the
            // welcome-driven `PeerJoined` broadcasts, the directed setup
            // frames) keeps this secondary waiting indefinitely — the
            // asm-dataset LMU fleet death (15 welcomed/announce-received
            // secondaries killed by a FIXED deadline while the primary was
            // alive in its equal-length quorum-proceed window) cannot
            // recur. The deadline detects a DEAD primary; a slow fleet is
            // not a dead primary.
            let deadline = self.config.unconfigured_deadline;
            self.setup_deadline.arm();
            // Reader clone taken BEFORE the setup future mutably borrows
            // `self`; both clones share the one deadline cell.
            let setup_deadline = self.setup_deadline.clone();
            // The pin + select live in their own block so the setup future
            // (which mutably borrows `self`) is DROPPED at the block's end
            // — before the outcome match below re-borrows `self` for the
            // teardown paths. A `None` break is exactly the cancellation
            // the old fixed `timeout` performed.
            let setup_outcome = {
                let setup = async {
                    // The welcome / cert-exchange handshake is OWNED by
                    // `wait_for_setup` (its entry attempt + the capped-backoff
                    // retry arm): a no-route at boot — the background bring-up
                    // dial has not folded the bootstrap wire yet — or a welcome
                    // lost on a dying wire is absorbed and re-offered there,
                    // rather than aborting the run here. The
                    // `unconfigured_deadline` wrapping this future stays the
                    // single give-up policy.
                    //
                    // `wait_for_setup` returns `Some(RunOutcome::Terminal)` when a
                    // terminal CRDT flag (RunComplete / RunAborted) landed DURING
                    // setup before the trio completed — it has already recorded
                    // the matching lifecycle terminal. `None` is the normal
                    // trio-completion success (proceed to the operational
                    // handoff). Propagate the signal so the orchestration can
                    // skip `process_tasks` and route straight to teardown.
                    let setup_terminal = self.wait_for_setup(factory).await?;
                    Ok::<Option<RunOutcome>, String>(setup_terminal)
                };
                tokio::pin!(setup);
                // Persistent-deadline select (the fires-under-load law): the
                // `sleep_until` is rebuilt each iteration from the STORED
                // absolute instant, so sibling progress on the setup future
                // never resets it. An extension only moves the stored instant
                // FORWARD: the arm wakes at the superseded instant, observes
                // `!expired()`, and re-sleeps to the new one. `&mut setup` is
                // polled across iterations without being dropped (cancel-safe
                // by pinning); only a true expiry abandons it.
                loop {
                    tokio::select! {
                        res = &mut setup => break Some(res),
                        _ = tokio::time::sleep_until(setup_deadline.deadline()) => {
                            if setup_deadline.expired() {
                                break None;
                            }
                            // Extended while sleeping — loop re-arms the
                            // sleep at the new stored instant.
                        }
                    }
                }
            };
            match setup_outcome {
                // Trio completed normally: fall through to `process_tasks`.
                Some(Ok(None)) => {}
                // A terminal CRDT flag was observed DURING setup. The
                // lifecycle terminal is already recorded; skip the operational
                // handoff and route straight to the SAME terminal-match
                // teardown the operational `process_tasks` return uses.
                Some(Ok(Some(RunOutcome::Terminal))) => {
                    setup_terminal = Some(RunOutcome::Terminal);
                }
                Some(Err(e)) => {
                    // Drain the sampler BEFORE `stop_all_workers`
                    // so the last tick reads still see the
                    // per-worker cgroup leaves the pool's teardown
                    // is about to Drop-rmdir. Same ordering
                    // invariant as the terminal-cleanup path below.
                    self.shutdown_sampler_if_present().await;
                    self.stop_all_workers().await;
                    return Err(e);
                }
                None => {
                    // A full `unconfigured_deadline` of PRIMARY SILENCE
                    // (the re-armable deadline elapsed un-extended).
                    // Role-aware alive-secondary count over GLOBAL STATE,
                    // NOT the transport's role-blind `peer_count()`:
                    // post-de-role the transport counts the folded primary
                    // as an ordinary mesh peer, so it would read 1 (the
                    // primary link) when there are 0 alive secondaries and
                    // wrongly take the "peers reachable" branch. We are
                    // pre-`Operational` here (the setup trio never
                    // completed), so `alive_secondary_count` reads the
                    // replicated MEMBERSHIP roster (`PeerJoined` secondaries
                    // applied during setup), which is the faithful "has any
                    // secondary joined" signal — keepalives do not flow
                    // until `Operational`.
                    let peers = self.alive_secondary_count();
                    self.shutdown_sampler_if_present().await;
                    self.stop_all_workers().await;
                    // Either branch below is the 10-minute point of the
                    // owner's wait-narration schedule (the 30s/1m/5m marks
                    // fired inside `wait_for_setup`; the abort line IS the
                    // 10m mark) — and the give-up is recorded as the TYPED
                    // `BringUpFailed` lifecycle terminal so the node
                    // outcome surfaces the structured
                    // `RunError::BringUpFailed` (non-zero exit with the
                    // bring-up story + the one-knob hint), never the
                    // generic policy-exit misattribution or a silent
                    // cold-exit.
                    if peers == 0 {
                        // The asm-dataset-nix T7 attempt 2 scenario:
                        // primary URL unreachable AND no peers have
                        // dialled in. The run is almost certainly
                        // already complete and SLURM is just booting
                        // a queued secondary against the graveyard.
                        // Exit fast with a clear log.
                        tracing::error!(
                            secondary = %self.config.secondary_id,
                            deadline_secs = deadline.as_secs(),
                            "setup deadline elapsed with no primary and no peers — \
                             no instructions ever arrived from setup; run appears \
                             already complete, aborting"
                        );
                        let reason = format!(
                            "setup deadline ({}s) elapsed: no primary, no peers \
                             (cluster appears dead, run likely complete)",
                            deadline.as_secs()
                        );
                        self.enter_terminal_bring_up_failed(reason.clone());
                        return Err(reason);
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
                             primary unresponsive, aborting"
                        );
                        let reason = format!(
                            "setup deadline ({}s) elapsed: primary unresponsive \
                             despite {} peer(s) reachable",
                            deadline.as_secs(),
                            peers
                        );
                        self.enter_terminal_bring_up_failed(reason.clone());
                        return Err(reason);
                    }
                }
            }

            // No explicit `setup_phase_completed` latch to set: the
            // `Configuring → Operational` transition at the top of
            // `process_tasks` (next) flips the lifecycle to `Operational`,
            // and the `setup_phase_completed()` projection reads true from
            // there on. A late-joiner observer therefore observes
            // `Operational` and skips this whole block — the same
            // fire-once guard the flat bool gave, now derived from the
            // typed state.
        }

        // Phase 5: Process tasks. The first thing it does is drive the
        // `Configuring → Operational` transition (consuming the take-once
        // latches), then runs to a terminal. SKIPPED when a terminal CRDT
        // flag was already observed during setup (`setup_terminal`): the run
        // is over and the lifecycle terminal is already recorded, so entering
        // the operational loop would be wrong (no `Operational` handoff for a
        // run that has already completed). Either way the lifecycle terminal
        // is the single source of truth for the teardown match below.
        if setup_terminal.is_none() {
            let RunOutcome::Terminal = self.process_tasks(factory).await?;
        }

        // The terminal was recorded on the lifecycle (the single source of
        // truth) — by `process_tasks` on the operational path, or by
        // `wait_for_setup` on the terminal-during-setup path. Read it back to
        // choose the matching teardown; both paths converge here.
        match self.lifecycle.terminal() {
            Some(super::SecondaryTerminal::Done) | None => {
                // Normal termination — drain the sampler BEFORE
                // `stop_all_workers` so its last tick still sees
                // the per-worker cgroup leaves the pool's teardown
                // is about to Drop-rmdir. Mirrors
                // `LocalManager::process_binaries`'s teardown order.
                self.shutdown_sampler_if_present().await;
                self.stop_all_workers().await;
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    completed = self.completed_count(),
                    "secondary finished"
                );
            }
            Some(super::SecondaryTerminal::Panik {
                matched_path,
                reason,
            }) => {
                // Workers have already been taken down via the
                // panik-react path's `kill_all_workers_with_grace`;
                // skip the clean `stop_all_workers` ladder (it would
                // try to send a protocol Stop on a dead transport
                // and waste teardown time). The PyO3 wrapper reads
                // the `Panik` terminal and calls
                // `std::process::exit(137)`.
                tracing::error!(
                    secondary = %self.config.secondary_id,
                    matched_path = %matched_path.display(),
                    reason = %reason,
                    "secondary panik shutdown"
                );
            }
            Some(super::SecondaryTerminal::Aborted { reason }) => {
                // Run aborted cluster-wide. The run is over, so tear
                // down workers the same way as `Done` (drain the
                // sampler before `stop_all_workers`); the PyO3
                // wrapper reads the `Aborted` terminal and calls
                // `std::process::exit(1)`. Logged at error level —
                // an abort is a failure outcome, not a clean finish.
                self.shutdown_sampler_if_present().await;
                self.stop_all_workers().await;
                tracing::error!(
                    secondary = %self.config.secondary_id,
                    reason = %reason,
                    "secondary exiting: run aborted by primary"
                );
            }
            Some(super::SecondaryTerminal::Failed { reason }) => {
                // A `Failed` terminal is reached only via the
                // `fatal_exit` read, which propagates an `Err` from
                // `process_tasks` (short-circuiting the `?` above) —
                // so this arm is unreachable on a `RunOutcome::
                // Terminal`. Guard defensively rather than weaken
                // the match.
                tracing::error!(
                    secondary = %self.config.secondary_id,
                    reason = %reason,
                    "secondary reported Terminal with a Failed lifecycle \
                     (unexpected — fatal_exit should propagate Err)"
                );
                self.shutdown_sampler_if_present().await;
                self.stop_all_workers().await;
            }
            Some(super::SecondaryTerminal::BringUpFailed { reason }) => {
                // Same disposition as `Failed`: the deadline-expiry site
                // records this terminal AND returns `Err` (short-circuiting
                // the `?` above, after its own worker teardown), so this
                // arm is unreachable on a `RunOutcome::Terminal`. Guard
                // defensively rather than weaken the match.
                tracing::error!(
                    secondary = %self.config.secondary_id,
                    reason = %reason,
                    "secondary reported Terminal with a BringUpFailed \
                     lifecycle (unexpected — the deadline expiry should \
                     propagate Err)"
                );
                self.shutdown_sampler_if_present().await;
                self.stop_all_workers().await;
            }
        }

        Ok(RunOutcome::Terminal)
    }

    pub(in crate::secondary) fn max_resources(&self) -> dynrunner_core::ResourceMap {
        self.config.max_resources.clone()
    }
}
