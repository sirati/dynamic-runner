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
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tracing::Instrument;

use super::lifecycle::{OperationalLatches, SecondaryLifecycle};
use super::primary_link::PrimaryLink;
use super::{PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryCoordinator};
use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub fn new(config: SecondaryConfig, transport: Tr, scheduler: S, estimator: E) -> Self {
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
        // Command channel for the PyO3 `PrimaryHandle` surface.
        // Mirrors `PrimaryCoordinator::new` exactly: bounded capacity
        // sized so a noisy caller can't OOM the secondary, but with
        // enough slack to absorb a batch of commands before
        // backpressure surfaces. The receiver is taken out for the
        // duration of `process_tasks` and put back at loop exit so
        // `SetupPending` re-entries keep the channel.
        let (command_tx, command_rx) =
            tokio::sync::mpsc::channel(crate::primary::COMMAND_CHANNEL_CAPACITY);
        let mut this = Self {
            config,
            transport,
            bootstrap_primary_id: None,
            scheduler,
            estimator,
            peer_cert_info: None,
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
            cluster_state: ClusterState::new(),
            lifecycle_rx: Some(lifecycle_rx),
            peer_lifecycle_listeners: Vec::new(),
            lifecycle_dispatcher_handle: None,
            task_completed_rx: Some(task_completed_rx),
            task_completed_listeners: Vec::new(),
            task_completed_dispatcher_handle: None,
            announcer_outbox_tx: None,
            announcer_outbox_rx: None,
            panik_signal_rx: None,
            fatal_exit_signal_rx: None,
            on_cluster_state_refresh: None,
            primary_activator: None,
            activated_primary_handle: None,
            pending_transfer_activation: false,
            on_phase_end: None,
            on_phase_start: None,
            command_rx: Some(command_rx),
            command_tx,
            // Lazily constructed in `run_until_setup_or_done_inner`
            // post-`initialize_workers` — see the doc on the
            // `sampler` field for the runtime-context rationale.
            sampler: None,
            // Co-located loopback channels — registered by the pyo3
            // composition (`register_colocated_*`) only when this host
            // also runs an on-demand co-located primary. `None` everywhere
            // else (the forward / drain become no-ops).
            colocated_primary_inbound_tx: None,
            colocated_loopback_inbound_rx: None,
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
        // NOTE: no transport role-cache attachment. "Who is primary now"
        // is resolved at THIS edge (`Self::send_to` reads
        // `cluster_state.current_primary()` / the bootstrap fallback);
        // the transport is `PeerId`-only and never mirrors the role
        // table. The former `transport.register_with_cluster_state(..)`
        // wiring (which subscribed a transport-resident write-through
        // cache to drive `Address::Role(Primary)` routing) is removed —
        // resolution moved to the edge.
        this
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

    /// Register the co-located primary's INBOUND sender (channel CH2).
    ///
    /// The composed runtime (pyo3 secondary wrapper) builds both
    /// coordinators on one host and connects them with two unbounded
    /// channels. CH2 carries the secondary→primary direction: when this
    /// host holds the primary role, [`Self::handle_inbound`] forwards
    /// every `is_primary_facing` frame into this sender (so the
    /// co-located `PrimaryCoordinator`'s `recv_peer` drains it), and the
    /// secondary's own-host terminal reports route here via the
    /// [`Self::send_to`] `Loopback` arm. `None` (no co-located primary
    /// composed — every non-pyo3 path) leaves the forward a no-op.
    ///
    /// Pre-`run_until_setup_or_done` contract, same one-shot shape as
    /// [`Self::register_primary_activator`]: the slot is `take`-n into
    /// the operational loop's latches at the `enter_operational`
    /// boundary.
    pub fn register_colocated_primary_inbound(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<
            dynrunner_protocol_primary_secondary::DistributedMessage<I>,
        >,
    ) {
        self.colocated_primary_inbound_tx = Some(tx);
    }

    /// Register the co-located primary's loopback RECEIVER (channel CH1).
    ///
    /// CH1 carries the primary→secondary direction: the co-located
    /// `PrimaryCoordinator`'s egress (own-host `TaskAssignment` loopback +
    /// the `Destination::All` broadcast leg) sends into the matching
    /// sender, and the secondary drains this receiver in its operational
    /// `select!` loop alongside `transport.recv_peer`, feeding each frame
    /// through [`Self::handle_inbound`] exactly as a wire frame so a
    /// loopback `TaskAssignment` / `ClusterMutation` / `RunComplete` is
    /// processed identically to a mesh-delivered one. `None` outside a
    /// co-located composition — the drain arm parks on `pending()`.
    ///
    /// Pre-`run_until_setup_or_done` contract, one-shot: the receiver is
    /// `take`-n at the first `process_tasks` entry and moved into its
    /// resumable home on
    /// [`super::lifecycle::OperationalState::colocated_loopback_inbound_rx`],
    /// where it survives a `SetupPending` re-entry (on a promoted node it is
    /// the sole path to the co-located primary's `RunComplete`).
    pub fn register_colocated_loopback_inbound(
        &mut self,
        rx: tokio::sync::mpsc::UnboundedReceiver<
            dynrunner_protocol_primary_secondary::DistributedMessage<I>,
        >,
    ) {
        self.colocated_loopback_inbound_rx = Some(rx);
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
    /// `run_until_setup_or_done` was never called, or only the
    /// SetupPending-yielding first entry ran and no second entry
    /// reached the terminal cleanup). Mirrors the same helper on
    /// `PrimaryCoordinator` — see that doc for the Drop-vs-explicit
    /// design rationale.
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
    /// resumable home on
    /// [`super::lifecycle::OperationalState::panik_signal_rx`] where it
    /// survives a `SetupPending` re-entry — on a regular pre-staged
    /// secondary this is the sole in-loop path for a post-discovery SIGTERM
    /// to reach the graceful-shutdown cascade).
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

    /// Register a callback invoked on a modest periodic tick from the
    /// `process_tasks` loop with a read-only borrow of the live,
    /// post-apply `cluster_state`. The matching consumer (the PyO3
    /// observer's live-snapshot feed) cannot borrow the CRDT directly —
    /// the run loop owns the `&mut cluster_state` for its whole lifetime
    /// — so the loop calls IN to the consumer's closure at the one
    /// in-loop moment it legitimately holds `&self.cluster_state`.
    ///
    /// Pre-`run_until_setup_or_done` contract, same single-shot
    /// registration shape as [`Self::register_fatal_exit_signal_rx`]:
    /// the slot is `Option::take`-n into the loop's local state on first
    /// entry, so a registration after the loop started has no effect on
    /// the active loop. Absent registration leaves the slot `None` — the
    /// periodic tick still fires but invokes nothing.
    ///
    /// Single concern: own the registration surface. This crate never
    /// learns what the consumer does with the borrow (it projects + the
    /// `StatsSnapshot` / `SharedSnapshotSource` shapes live entirely in
    /// the PyO3 layer); the callback is the clean boundary. The tick is
    /// periodic — NOT per-`ClusterMutation` — so the projection cost is
    /// `O(ledger)` per tick rather than `O(ledger × mutations)`.
    pub fn register_cluster_state_refresh(&mut self, callback: super::ClusterStateRefreshFn<I>) {
        self.on_cluster_state_refresh = Some(callback);
    }

    /// Register the ON-DEMAND primary-activator closure: the
    /// construction-on-demand of a co-located
    /// [`crate::primary::PrimaryCoordinator`].
    ///
    /// The runtime layer captures the primary's construction inputs (the
    /// host's mesh transport handle, the loopback/demux channels, config,
    /// scheduler, estimator, command/phase wiring) into this closure and
    /// hands it here. There is NO pre-built coordinator — when this node is
    /// named primary (failover-self election win OR a bootstrap transfer
    /// naming it), [`Self::activate_co_located_primary_on_demand`]
    /// `take()`s this closure, snapshots `cluster_state`, and invokes it to
    /// build and spawn the primary into its seeded resume. Pre-
    /// `run_until_setup_or_done` contract, same single-shot shape as the
    /// other `register_*` setters. Absent registration (Rust-only tests /
    /// legacy single-`run()` callers / a `disable_peer_overlay` host)
    /// leaves the activator `None`; a node whose replicated
    /// `can_be_primary` marker is unset never reaches the build, so the
    /// `None` is benign there and the `PrimaryChanged` broadcast still
    /// fires. See [`super::PrimaryActivator`].
    pub fn register_primary_activator(&mut self, activator: super::PrimaryActivator<I>) {
        self.primary_activator = Some(activator);
    }

    /// Take the `JoinHandle` of the co-located primary this node activated
    /// on demand (if any), for the runtime to join at wind-down so the
    /// activated-primary future is never leaked. `None` on every node that
    /// was never named primary (no build ever ran). Single-shot: the
    /// handle is moved out, leaving `None`.
    pub fn take_activated_primary_handle(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.activated_primary_handle.take()
    }

    /// Extract the composed-primary wiring — the lifecycle/command
    /// channels the PyO3 wrapper minted on this `SecondaryCoordinator`
    /// so its `PrimaryHandle` clone stays a stable type — for transfer
    /// onto the co-located [`crate::primary::PrimaryCoordinator`], the
    /// real consumer.
    ///
    /// Returns `(command_tx, command_rx, on_phase_start, on_phase_end)`,
    /// taking each out of `self`. The composed runtime hands the command
    /// pair to the primary via `replace_command_channel` and the phase
    /// callbacks via `register_lifecycle_*`/`register_phase_*` — the
    /// authority owns the phase machine and the externally-issued
    /// `PrimaryCommand` ingress, NOT the follower secondary. This is the
    /// site that makes these fields live (consumed in-crate), so the
    /// fields carry no `#[allow(dead_code)]`.
    ///
    /// One-shot: a second call yields `(_, None, None, None)` because
    /// the receiver/closures were already taken. The `command_tx` clone
    /// is `Clone`, so the secondary retains its own copy for any handle
    /// minted off it; only the receiver is uniquely transferred.
    #[allow(clippy::type_complexity)]
    pub fn take_composed_primary_wiring(
        &mut self,
    ) -> (
        tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>>,
        Option<tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>>,
        Option<crate::primary::OnPhaseStart>,
        Option<crate::primary::OnPhaseEnd>,
    ) {
        (
            self.command_tx.clone(),
            self.command_rx.take(),
            self.on_phase_start.take(),
            self.on_phase_end.take(),
        )
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

    /// Accept the per-phase lifecycle hooks for the post-promotion path.
    /// Mirrors the shape `PrimaryCoordinator::run` accepts:
    /// `on_phase_start(&PhaseId)` fires when a phase flips Blocked →
    /// Active; `on_phase_end(&PhaseId, completed, failed)` fires when
    /// the phase reaches `Drained`.
    ///
    /// Must be called before `run_until_setup_or_done` enters.
    ///
    /// The secondary holds NO phase machine and never fires these
    /// itself. It is a registration ANCHOR: the runtime moves the closures
    /// into the on-demand primary-activator closure (the authority that
    /// owns the phase machine is built from them via
    /// [`Self::take_composed_primary_wiring`]); the co-located primary
    /// fires them once it is activated on demand. On a node that registers
    /// no activator (in-process distributed secondaries on a
    /// `NoPeerTransport` mesh) the closures stay dormant and are never
    /// invoked.
    ///
    /// Single concern: accept ownership of the boxed GIL-reacquiring
    /// closures from the PyO3 wrapper and hold them until the
    /// composition transfers them to the authority.
    pub fn register_phase_lifecycle_callbacks(
        &mut self,
        on_phase_start: crate::primary::OnPhaseStart,
        on_phase_end: crate::primary::OnPhaseEnd,
    ) {
        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
    }

    /// Tear down the task-completion dispatcher task. Mirrors
    /// [`Self::cleanup_lifecycle_dispatcher`] — same Drop-vs-explicit
    /// design rationale, same re-entrant SetupPending non-cleanup
    /// discipline.
    pub(in crate::secondary) async fn cleanup_task_completed_dispatcher(&mut self) {
        if let Some(handle) = self.task_completed_dispatcher_handle.take() {
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

    /// Set pre-staged-source mode from the primary's
    /// `InitialAssignment`. The flag is read directly at the resolution
    /// site (`expected_content_hash` selection); no getter is needed.
    pub(in crate::secondary) fn set_pre_staged_mode(&mut self, on: bool) {
        // Written from `wait_for_setup`'s `InitialAssignment` handler,
        // which runs in `Configuring`; the flag is carried forward into
        // `Operational` at `enter_operational`.
        self.lifecycle.set_pre_staged_mode(on);
    }

    /// `true` iff this node is the single deterministically-designated
    /// setup-discovery node — the lowest-id alive, non-observer,
    /// `can_be_primary` worker-secondary in the replicated membership
    /// mirror.
    ///
    /// This is the SAME candidate set + `.min()` rule the primary's
    /// `select_bootstrap_primary` (primary/coordinator.rs) applies to
    /// pick the bootstrap-promotion target, computed here against the
    /// secondary's own replicated `cluster_state`:
    ///   - [`ClusterState::alive_secondary_members`] — advertised
    ///     worker-secondary capacity (`worker_count > 0`) AND alive; the
    ///     faithful liveness signal in the SETUP window where no
    ///     operational keepalive map exists yet (exactly the substitution
    ///     the secondary election makes in its cold-start branch,
    ///     `alive_secondary_ids`).
    ///   - `role_table().observers` excluded — an observer hosts no
    ///     workers and can never become primary, mirroring the election's
    ///     observer self-exclusion and `select_bootstrap_primary`'s
    ///     defensive cut.
    ///   - [`ClusterState::can_be_primary`] — the explicit replicated
    ///     capability marker; only a peer that declared it can host the
    ///     primary on demand is eligible.
    ///
    /// The primary's selection ALSO filters `mesh_ready_secondaries` /
    /// `transport.has_peer`, which a secondary cannot read for its peers;
    /// its membership/liveness mirror is the faithful analogue (the same
    /// substitution `alive_secondary_ids` makes). The designated
    /// discoverer is therefore the SAME node `select_bootstrap_primary`
    /// promotes — discovery and promotion re-coupled through one
    /// deterministic rule, with no cross-call between the two concerns.
    ///
    /// Self-healing: if the designated node dies before discovering, the
    /// `.min()` re-resolves to the next eligible node on the next tick
    /// (the predicate is re-evaluated every loop iteration), and that
    /// node's empty-ledger axis is still true, so it picks up discovery —
    /// the same liveness-driven re-resolution the election has.
    fn is_designated_discoverer(&self) -> bool {
        let observers = &self.cluster_state.role_table().observers;
        let designated = self
            .cluster_state
            .alive_secondary_members()
            .filter(|id| !observers.contains(*id))
            .filter(|id| self.cluster_state.can_be_primary(id))
            .min();
        designated == Some(self.config.secondary_id.as_str())
    }

    /// Single source of truth for the setup-discovery `SetupPending`
    /// yield discriminator.
    ///
    /// `true` iff (a) the authority deferred discovery
    /// (`pre_staged_mode`, set from the empty `InitialAssignment` the
    /// submitter sends when it has no local corpus view), (b) this node
    /// hasn't already run its own discovery pass
    /// ([`Self::setup_discovery_done`]), (c) the replicated ledger is
    /// still empty (`cluster_state.task_count() == 0` — no node has
    /// seeded it yet), (d) this node is the single deterministically-
    /// designated discoverer ([`Self::is_designated_discoverer`]), and
    /// (e) this node is the recognized post-promotion authority
    /// (`cluster_state.current_primary() == self`).
    ///
    /// Axes (d) and (e) together make discovery run on EXACTLY ONE node
    /// and only AFTER that node has become the authority: the designated
    /// discoverer is the same lowest-id-eligible node
    /// `select_bootstrap_primary` promotes (axis d), and axis (e) holds
    /// the yield until that node's own `PrimaryChanged` has propagated —
    /// so its `ingest_setup_discovery` broadcast is consumed by its own
    /// already-operational co-located primary, never into the void.
    ///
    /// `process_tasks` consults this once per tick and yields
    /// `RunOutcome::SetupPending` when true so the PyO3 wrapper can run
    /// Python's `task.discover_items` against the locally bind-mounted
    /// corpus and feed the result back via
    /// [`Self::ingest_setup_discovery`]. The predicate is self-clearing
    /// on every axis: a non-empty ledger (this node's own ingest or a
    /// peer's broadcast) flips (c) false, and `ingest_setup_discovery`
    /// flips (b) true — so the yield FIRES AT MOST ONCE per node and an
    /// empty discovery never re-yields. Legacy / failover runs leave
    /// `pre_staged_mode` false, so the predicate is always false there.
    pub(in crate::secondary) fn setup_discovery_pending(&self) -> bool {
        self.lifecycle.pre_staged_mode()
            && !self.lifecycle.setup_discovery_done()
            && self.cluster_state.task_count() == 0
            && self.is_designated_discoverer()
            && self.cluster_state.current_primary() == Some(self.config.secondary_id.as_str())
    }

    pub(in crate::secondary) fn set_uses_file_based_items(&mut self, on: bool) {
        self.lifecycle.set_uses_file_based_items(on);
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
        if !self.lifecycle.uses_file_based_items() {
            return Some(std::path::PathBuf::from(local_path));
        }
        // In pre-staged mode the primary doesn't compute a content
        // hash (no transfer), so pass None and let the resolver
        // accept by existence. Otherwise hash-verify like the
        // historical path.
        let expected_content_hash = if self.lifecycle.pre_staged_mode() {
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
    /// peer's `RequestClusterSnapshot` response, then mark setup as
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
    /// primary-handshake setup". Mirrors what
    /// `run_until_setup_or_done`'s second-iteration branch does on the
    /// `SetupPending` re-entry path — that branch's `if !self
    /// .setup_phase_completed { … }` guard is the single source of
    /// truth for "skip setup". We just set the latch.
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
    /// Convenience wrapper around `run_until_setup_or_done` for callers
    /// that don't participate in the setup-promote handshake (every
    /// caller other than the PyO3 secondary wrapper, which has to
    /// re-enter the loop after running Python `task.discover_items`).
    /// The outcome can only be `Done` here, because `SetupPending`
    /// requires an `InitialAssignment { pre_staged_mode: true }` wire
    /// arrival (the discovery-yield carrier) and no test/non-pyo3 setup
    /// ever sends one.
    pub async fn run(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        match self.run_until_setup_or_done(factory).await? {
            RunOutcome::SetupPending => Err(
                "secondary yielded SetupPending but caller is the legacy run() \
                 wrapper which cannot drive setup discovery — programming error \
                 (only the PyO3 secondary wrapper should invoke a secondary that \
                 may be promoted with required_setup=true)"
                    .to_string(),
            ),
            // Reached a terminal — read the per-secondary terminal off the
            // lifecycle (the single source of truth). The PyO3 wrapper
            // takes the structured terminal and calls `std::process::exit`
            // (137 panik / 1 abort); the legacy `run()` path has no such
            // side-effect channel, so it surfaces `Aborted`/`Panik` as a
            // normal String error and `Done` as `Ok`. (`Failed` never
            // reaches here — `fatal_exit` propagates as the run loop's
            // `Err` before this match. The watcher is not triggered by
            // legacy `run()` callers, so the panik arm is structurally cold
            // in production Rust-only usage.)
            RunOutcome::Terminal => match self.lifecycle.terminal() {
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
                Some(super::SecondaryTerminal::Failed { reason }) => Err(reason),
            },
        }
    }

    /// Drive the secondary coordination loop until it either yields
    /// for setup discovery (`RunOutcome::SetupPending`) or reaches a
    /// terminal (`RunOutcome::Terminal`, with the specific per-secondary
    /// terminal recorded on the lifecycle and readable via
    /// [`Self::terminal`]).
    ///
    /// First invocation: enters `AwaitingPrimary`, runs the setup
    /// handshake (welcome / cert exchange / wait_for_setup) under
    /// `config.unconfigured_deadline` — `wait_for_setup` spawns the worker
    /// pool and enters `Configuring` on the first primary frame — then
    /// `process_tasks` drives the `Configuring → Operational` transition
    /// and runs the loop.
    ///
    /// Subsequent invocations (only reached on the `SetupPending`
    /// caller-loop re-entry): skip the setup phase — workers are still
    /// alive and the handshake messages have already been consumed —
    /// and re-enter `process_tasks` directly. The re-entry guard is the
    /// `self.lifecycle.setup_phase_completed()` projection (true once the
    /// lifecycle reaches `Operational`), which `process_tasks` flips on
    /// the first invocation.
    ///
    /// Cleanup (`stop_all_workers` + the "secondary finished" log)
    /// fires only on the `Done` branch. On `SetupPending` the worker
    /// pool is intentionally left running so the caller's re-entry
    /// finds it in the same state `process_tasks` yielded from.
    ///
    /// Cancel-safety: `process_tasks` already documents that every
    /// arm of its `select!` is cancel-safe (mpsc recv + tokio
    /// interval ticks); the early break on `setup_pending` simply
    /// abandons the in-flight future of whichever arm was awaiting,
    /// and the next entry rebuilds a fresh `select!`. No state is
    /// dropped except those in-flight recv futures, which are
    /// cancel-safe by construction.
    ///
    /// # Cleanup discipline
    ///
    /// Thin wrapper around [`Self::run_until_setup_or_done_inner`]
    /// whose secondary concern is to drive the peer-lifecycle
    /// dispatcher's abort-on-exit contract. `Done` and any `Err`
    /// path flow through `cleanup_lifecycle_dispatcher` before
    /// returning, so the spawned dispatcher task is always aborted
    /// and joined before the caller observes the result. The
    /// `SetupPending` yield path deliberately bypasses cleanup —
    /// the caller will re-enter, the dispatcher is still useful
    /// across that boundary, and the receiver has been moved into
    /// the task so a fresh spawn would be impossible anyway.
    pub async fn run_until_setup_or_done(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        // Role-tag the whole secondary run future so every event this task
        // emits is attributed to the secondary role and routed to the
        // per-role full log. This is the single entry all production
        // secondary paths flow through (the legacy `run` wrapper delegates
        // here), so one span here covers them all — including the events
        // emitted on a `SetupPending` re-entry. A secondary that never
        // promotes only ever carries this span → `secondary.log`; a peer
        // that activates a co-located primary spawns a SEPARATE task whose
        // own primary span keeps that authority's events in `primary.log`.
        // See `dynrunner_core::role_span`.
        let span = tracing::info_span!(
            dynrunner_core::SECONDARY_ROLE_SPAN,
            kind = "secondary",
            id = %self.config.secondary_id
        );
        async {
            let result = self.run_until_setup_or_done_inner(factory).await;
            // SetupPending is a re-entrant yield, not a terminal exit;
            // the dispatcher must stay alive across the boundary so the
            // next `run_until_setup_or_done` re-entry inherits it.
            // Match on the borrow to keep the result move-back intact.
            let cleanup = !matches!(&result, Ok(RunOutcome::SetupPending));
            if cleanup {
                self.cleanup_lifecycle_dispatcher().await;
                // Independent of `cleanup_lifecycle_dispatcher`; same
                // Done/Err vs. SetupPending discipline as documented above.
                self.cleanup_task_completed_dispatcher().await;
            }
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
        // Spawn the peer-lifecycle dispatcher on first entry (idempotent
        // on `SetupPending` re-entry: the receiver was already taken,
        // so this branch is a no-op the second time around). The
        // sender end was installed on `cluster_state` in `new()` so
        // any apply that lands before the dispatcher polls queues on
        // the unbounded channel and drains here. `spawn_local`
        // matches the rest of the secondary's LocalSet-bound spawn
        // pattern.
        //
        // The returned `JoinHandle` is stored on `self` so
        // `cleanup_lifecycle_dispatcher` (called from the outer
        // wrapper on Done / Err exits — NOT on the re-entrant
        // SetupPending yield) can abort the task and await its
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
        // first entry only (the receiver was moved on first entry,
        // so the take() returns None on SetupPending re-entry and the
        // branch is a no-op).
        if let Some(rx) = self.task_completed_rx.take() {
            let listeners = std::mem::take(&mut self.task_completed_listeners);
            let handle = tokio::task::spawn_local(
                crate::task_completed::run_task_completed_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }
        // Enter `AwaitingPrimary` (`Connecting → AwaitingPrimary`): the
        // secondary is now actively trying to reach a primary, but none
        // has announced yet. The peer mesh keeps forming (the orthogonal
        // `MeshFormation` sub-concern is untouched by this transition).
        // No worker pool, no task acceptance, no election, no keepalive in
        // this state — only the setup handshake below. Idempotent: a no-op
        // from any state other than `Connecting` (a `SetupPending`
        // re-entry is already `Operational`, so this leaves it unchanged).
        self.lifecycle = std::mem::replace(&mut self.lifecycle, SecondaryLifecycle::connecting())
            .enter_awaiting_primary();

        // Skip the per-secondary setup phase once the lifecycle has
        // reached `Operational` (or terminal) — the `setup_phase_completed`
        // projection replaces the old flat bool latch. This gates a
        // `SetupPending` re-entry (already `Operational`, so workers are
        // alive and the handshake frames are already consumed) and the
        // late-joiner observer (which `restore_from_snapshot_and_skip_setup`
        // landed directly in `Operational`).
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
            // `wait_for_setup`, because the recv loop is documented as
            // cancellation-unsafe under inner select! racing. Cancelling
            // the whole setup future on timeout is safe because we never
            // re-enter any of these phases — we go straight to
            // cleanup-and-exit.
            let deadline = self.config.unconfigured_deadline;
            let setup = async {
                // The welcome / cert-exchange handshake is the only
                // primary-facing action available pre-announce. Gate it on
                // the `AwaitingPrimary` one-shot so a re-entry never
                // re-sends (defensive; this branch only runs pre-config).
                if self.lifecycle.mark_handshake_sent() {
                    self.send_welcome().await?;
                    self.send_cert_exchange().await?;
                }
                self.wait_for_setup(factory).await?;
                Ok::<(), String>(())
            };
            match tokio::time::timeout(deadline, setup).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    // Drain the sampler BEFORE `stop_all_workers`
                    // so the last tick reads still see the
                    // per-worker cgroup leaves the pool's teardown
                    // is about to Drop-rmdir. Same ordering
                    // invariant as the terminal-cleanup path below.
                    self.shutdown_sampler_if_present().await;
                    self.stop_all_workers().await;
                    return Err(e);
                }
                Err(_elapsed) => {
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
                    if peers == 0 {
                        // The asm-dataset-nix T7 attempt 2 scenario:
                        // primary URL unreachable AND no peers have
                        // dialled in. The run is almost certainly
                        // already complete and SLURM is just booting
                        // a queued secondary against the graveyard.
                        // Exit fast with a clear log.
                        tracing::warn!(
                            secondary = %self.config.secondary_id,
                            deadline_secs = deadline.as_secs(),
                            "setup deadline elapsed with no primary and no peers — \
                             run appears already complete, exiting cold"
                        );
                        return Err(format!(
                            "setup deadline ({}s) elapsed: no primary, no peers \
                             (cluster appears dead, run likely complete)",
                            deadline.as_secs()
                        ));
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
                             primary unresponsive, exiting"
                        );
                        return Err(format!(
                            "setup deadline ({}s) elapsed: primary unresponsive \
                             despite {} peer(s) reachable",
                            deadline.as_secs(),
                            peers
                        ));
                    }
                }
            }

            // No explicit `setup_phase_completed` latch to set: the
            // `Configuring → Operational` transition at the top of
            // `process_tasks` (next) flips the lifecycle to `Operational`,
            // and the `setup_phase_completed()` projection reads true from
            // there on. A `SetupPending` re-entry therefore observes
            // `Operational` and skips this whole block — the same
            // fire-once re-entry guard the flat bool gave, now derived
            // from the typed state.
        }

        // Phase 5: Process tasks. The first thing it does is drive the
        // `Configuring → Operational` transition (consuming the take-once
        // latches). May yield with SetupPending or run to completion.
        let outcome = self.process_tasks(factory).await?;

        match outcome {
            RunOutcome::SetupPending => {
                // Workers stay alive; the caller's re-entry resumes
                // the loop in `process_tasks`. No final log line yet —
                // the run isn't actually finished.
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "secondary yielding for setup discovery"
                );
            }
            RunOutcome::Terminal => {
                // `process_tasks` already recorded the per-secondary
                // terminal on the lifecycle (the single source of truth);
                // read it back to choose the matching teardown.
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
                }
            }
        }

        Ok(outcome)
    }

    pub(in crate::secondary) fn max_resources(&self) -> dynrunner_core::ResourceMap {
        self.config.max_resources.clone()
    }
}
