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

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::primary_link::PrimaryLink;
use super::{PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryCoordinator};
use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
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
        let primary_link = PrimaryLink::with_failover_threshold(
            config.secondary_id.clone(),
            config.primary_link_failure_threshold,
            config.primary_link_failure_window,
        );
        // RetryBudget consumes `config.retry_max_passes` as the
        // attempt-count cap and reads `$SLURM_JOB_END_TIME` ONCE
        // here (startup) for the wallclock deadline. The
        // env-var is documented as Unix-epoch seconds; absence is
        // the legacy non-SLURM path (silent), parse failure logs WARN.
        // See `retry_budget.rs` for the dual-axis design.
        let primary_retry_budget = super::retry_budget::RetryBudget::from_env_and_legacy(
            config.retry_max_passes,
            super::retry_budget::DEFAULT_SAFETY_MARGIN,
        );
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
        let (task_completed_tx, task_completed_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let mut this = Self {
            config,
            primary_transport,
            peer_transport,
            scheduler,
            estimator,
            peer_cert_info: None,
            pool: WorkerPool::new(),
            active_tasks: HashMap::new(),
            completed_tasks: HashSet::new(),
            #[cfg(test)]
            local_tasks_run: 0,
            transfer_complete: false,
            is_primary: false,
            promoted_at: None,
            extraction_cache,
            peer_keepalives: HashMap::new(),
            primary_last_seen: None,
            primary_disconnected: false,
            election: super::election::ElectionState::Normal,
            pending_peer_messages: Vec::new(),
            primary_link,
            peer_mesh_check_at: None,
            peer_dial_count: 0,
            mesh_ready_sent: false,
            primary_pending: None,
            primary_completed: HashSet::new(),
            primary_in_flight: HashMap::new(),
            primary_failed: HashMap::new(),
            primary_retry_budget,
            exhaustion_warning_emitted: false,
            backpressured_secondaries: HashMap::new(),
            pre_staged_mode: false,
            uses_file_based_items: true,
            fatal_exit: None,
            peer_mesh_degraded: false,
            cluster_state: ClusterState::new(),
            pending_worker_restarts: HashSet::new(),
            setup_pending: false,
            setup_phase_completed: false,
            lifecycle_rx: Some(lifecycle_rx),
            peer_lifecycle_listeners: Vec::new(),
            lifecycle_dispatcher_handle: None,
            task_completed_rx: Some(task_completed_rx),
            task_completed_listeners: Vec::new(),
            task_completed_dispatcher_handle: None,
            announcer_outbox_tx: None,
            announcer_outbox_rx: None,
            panik_signal_rx: None,
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
        // Attach the transport's write-through role cache to our
        // authoritative `cluster_state.role_table`. The hook fires
        // on every applied `PrimaryChanged` mutation; the cache
        // serves Step 3's `Address::Role(_)` dispatch on the send
        // hot path. Transports that don't override
        // `register_with_cluster_state` (e.g. `NoPeerTransport`,
        // test stubs) get the default no-op — safe by construction.
        this.peer_transport
            .register_with_cluster_state(&mut this.cluster_state);
        this
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
    /// into the loop's local state on first entry).
    ///
    /// Single concern: the coordinator owns the panik-react logic
    /// (broadcast `ClusterMutation::PanikRequested`, kill all worker
    /// process trees, return `RunOutcome::PanikShutdown`). The PyO3
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

    /// Register a [`crate::task_completed::TaskCompletedListener`].
    /// Same single-shot, pre-`run_until_setup_or_done`-only contract
    /// as [`Self::register_lifecycle_listener`].
    pub fn register_task_completed_listener(
        &mut self,
        listener: Box<dyn crate::task_completed::TaskCompletedListener>,
    ) {
        self.task_completed_listeners.push(listener);
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
        let (outbox_tx, outbox_rx) =
            tokio::sync::mpsc::channel::<crate::observer::announcer::AnnouncerOutboxItem<I>>(
                ANNOUNCER_OUTBOX_CAPACITY,
            );
        self.announcer_outbox_rx = Some(outbox_rx);
        self.announcer_outbox_tx = Some(outbox_tx.clone());
        let sender = crate::observer::announcer::PeerMeshAnnouncerSender::new(
            peer_id, outbox_tx,
        );
        (handle, sender)
    }

    /// Whether the run is in pre-staged-source mode (set from the
    /// primary's `InitialAssignment`). Exposed within the secondary
    /// module so dispatch / setup can pick the right resolution path.
    pub(in crate::secondary) fn pre_staged_mode(&self) -> bool {
        self.pre_staged_mode
    }

    pub(in crate::secondary) fn set_pre_staged_mode(&mut self, on: bool) {
        self.pre_staged_mode = on;
    }

    /// Whether dispatched task items back to real files (default true).
    /// When false, the worker receives `local_path` as an opaque
    /// identifier and the framework performs no filesystem
    /// resolution.
    #[allow(dead_code)]
    pub(in crate::secondary) fn uses_file_based_items(&self) -> bool {
        self.uses_file_based_items
    }

    pub(in crate::secondary) fn set_uses_file_based_items(&mut self, on: bool) {
        self.uses_file_based_items = on;
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
        if !self.uses_file_based_items {
            return Some(std::path::PathBuf::from(local_path));
        }
        // In pre-staged mode the primary doesn't compute a content
        // hash (no transfer), so pass None and let the resolver
        // accept by existence. Otherwise hash-verify like the
        // historical path.
        let expected_content_hash = if self.pre_staged_mode {
            None
        } else {
            Some(file_hash)
        };
        self.extraction_cache
            .resolve_binary(zip_ref, local_path, file_hash, expected_content_hash)
    }

    /// True iff `secondary_id` is currently in the primary's
    /// backpressure backoff window (recently returned "No idle worker
    /// available"). Used by `handle_primary_task_request` to skip
    /// re-dispatching to an unresponsive peer. Mirrors
    /// `PrimaryCoordinator::is_backpressured`.
    pub(in crate::secondary) fn is_primary_peer_backpressured(&self, secondary_id: &str) -> bool {
        self.backpressured_secondaries
            .get(secondary_id)
            .is_some_and(|t| Instant::now() < *t)
    }

    /// Set certificate info for peer connections. Must be called before `run()`
    /// if peer-to-peer QUIC is enabled.
    pub fn set_peer_cert_info(&mut self, info: PeerCertInfo) {
        self.peer_cert_info = Some(info);
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
    /// = true` and route straight to `process_tasks`. The role-change
    /// hook the transport registered in `new()` fires from inside
    /// `cluster_state.restore` so the peer-mesh role-cache is warmed
    /// (e.g. `current_primary` is now resolvable for
    /// `Address::Role(Role::Primary)` sends).
    pub fn restore_from_snapshot_and_skip_setup(
        &mut self,
        snap: crate::cluster_state::ClusterStateSnapshot<I>,
    ) {
        self.cluster_state.restore(snap);
        self.setup_phase_completed = true;
    }

    pub fn completed_count(&self) -> usize {
        self.completed_tasks.len()
    }

    /// Test-only inspector for the primary retry budget
    /// counter. Lets tests assert that the retry pass actually
    /// consumed budget (vs. e.g. the success arriving without
    /// re-injection because the test fixture fixed the worker
    /// behaviour after one pass anyway). Public-but-test-gated so
    /// production callers don't depend on this internal counter
    /// shape. Forwards through the encapsulated `RetryBudget`.
    #[cfg(test)]
    pub fn primary_retry_passes_used_for_test(&self) -> u32 {
        self.primary_retry_budget.attempts_used()
    }

    /// Test-only inspector for the primary's residual
    /// failed-task ledger after the retry budget is exhausted. Used
    /// by the multi-pass-exhaustion regression test to assert that a
    /// task which fails Recoverably across all permitted passes ends
    /// up permanently in `primary_failed`. Counts only
    /// primary-dispatched failures (tasks that went through
    /// `handle_primary_task_request`); initial-assignment failures
    /// observed by the local worker bypass this ledger by design.
    #[cfg(test)]
    pub fn primary_failed_count_for_test(&self) -> usize {
        self.primary_failed.len()
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
    /// requires a `PromotePrimary { required_setup: true }` wire arrival
    /// and no test/non-pyo3 setup ever sends one.
    pub async fn run(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        match self.run_until_setup_or_done(factory).await? {
            RunOutcome::Done => Ok(()),
            RunOutcome::SetupPending => Err(
                "secondary yielded SetupPending but caller is the legacy run() \
                 wrapper which cannot drive setup discovery — programming error \
                 (only the PyO3 secondary wrapper should invoke a secondary that \
                 may be promoted with required_setup=true)"
                    .to_string(),
            ),
            // Surface PanikShutdown as a String error on the legacy
            // `run()` path. The PyO3 wrapper takes the structured
            // `RunOutcome` directly and calls `std::process::exit(137)`;
            // the legacy wrapper has no such side-effect channel, so
            // operators using the Rust-only API observe panik as a
            // normal error return with the matched path in the
            // message. (Tests using the legacy `run()` don't trigger
            // the watcher — no panik file is ever created — so this
            // arm is structurally cold in production Rust-only usage.)
            RunOutcome::PanikShutdown {
                matched_path,
                reason,
            } => Err(format!(
                "secondary panik shutdown: {reason} (matched_path={})",
                matched_path.display()
            )),
        }
    }

    /// Drive the secondary coordination loop until it either yields
    /// for setup discovery (`RunOutcome::SetupPending`) or reaches a
    /// terminal state (`RunOutcome::Done`).
    ///
    /// First invocation: runs `initialize_workers`, the setup handshake
    /// (welcome / cert exchange / wait_for_setup) under
    /// `config.setup_deadline`, then enters `process_tasks`.
    ///
    /// Subsequent invocations (only reached on the `SetupPending`
    /// caller-loop re-entry): skip the setup phase — workers are still
    /// alive and the handshake messages have already been consumed —
    /// and re-enter `process_tasks` directly. The re-entry guard is
    /// `self.setup_phase_completed`, set the moment the first
    /// invocation finishes the handshake successfully.
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
        if let Some(rx) = self.lifecycle_rx.take() {
            let listeners = std::mem::take(&mut self.peer_lifecycle_listeners);
            let handle = tokio::task::spawn_local(
                crate::peer_lifecycle::run_peer_lifecycle_dispatcher(rx, listeners),
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
                crate::task_completed::run_task_completed_dispatcher(rx, listeners),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }
        if !self.setup_phase_completed {
            tracing::info!(
                secondary = %self.config.secondary_id,
                workers = self.config.num_workers,
                resources = %self.config.max_resources,
                "secondary starting"
            );

            // Initialize workers (local pool — no network, no deadline).
            self.initialize_workers(factory).await?;

            // Network-touching setup (Phases 1-4) is bounded by
            // `setup_deadline`. See SecondaryConfig::setup_deadline for
            // the rationale. The deadline is applied at the orchestration
            // boundary, NOT inside `wait_for_setup`, because the recv
            // loop is documented as cancellation-unsafe under inner
            // select! racing (see setup.rs:79-96). Cancelling the whole
            // setup future on timeout is safe because we never re-enter
            // any of these phases — we go straight to cleanup-and-exit.
            let deadline = self.config.setup_deadline;
            let setup = async {
                self.send_welcome().await?;
                self.send_cert_exchange().await?;
                self.wait_for_setup().await?;
                Ok::<(), String>(())
            };
            match tokio::time::timeout(deadline, setup).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.stop_all_workers().await;
                    return Err(e);
                }
                Err(_elapsed) => {
                    let peers = self.peer_transport.peer_count();
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

            // Latch BEFORE entering process_tasks so a SetupPending
            // yield doesn't trigger a redo on re-entry.
            self.setup_phase_completed = true;
        }

        // Phase 5: Process tasks. May yield with SetupPending or
        // run to completion.
        let outcome = self.process_tasks(factory).await?;

        match &outcome {
            RunOutcome::Done => {
                // Normal termination — stop workers and log finish.
                self.stop_all_workers().await;
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    completed = self.completed_tasks.len(),
                    "secondary finished"
                );
            }
            RunOutcome::SetupPending => {
                // Workers stay alive; the caller's re-entry resumes
                // the loop in `process_tasks`. No final log line yet —
                // the run isn't actually finished.
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "secondary yielding for setup discovery"
                );
            }
            RunOutcome::PanikShutdown {
                matched_path,
                reason,
            } => {
                // Workers have already been taken down via the
                // panik-react path's `kill_all_workers_with_grace`;
                // skip the clean `stop_all_workers` ladder (it would
                // try to send a protocol Stop on a dead transport
                // and waste teardown time). The PyO3 wrapper will
                // call `std::process::exit(137)` as soon as it sees
                // this variant.
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    matched_path = %matched_path.display(),
                    reason = %reason,
                    "secondary panik shutdown"
                );
            }
        }

        Ok(outcome)
    }

    pub(in crate::secondary) fn max_resources(&self) -> dynrunner_core::ResourceMap {
        self.config.max_resources.clone()
    }
}
