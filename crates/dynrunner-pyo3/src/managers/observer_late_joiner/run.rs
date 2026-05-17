//! `PyObserverLateJoiner::run` — reads the peer-info dir, dials into
//! the mesh, restores the cluster snapshot, and drives the observation
//! loop. Also exposes the `completed` getter.

use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_manager_distributed::{
    cluster_state::ClusterStateSnapshot,
    observer::run_observer_announcer,
    PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryCoordinator,
};
use dynrunner_protocol_primary_secondary::{PeerTransport, DEFAULT_JOIN_TIMEOUT};
use dynrunner_slurm::read_peer_info_dir_v2;
use dynrunner_transport_quic::{NoPrimaryTransport, PeerNetwork};

use crate::config::connection::ConnectionMode;
use crate::identifier::RunnerIdentifier;
use crate::network::{detect_ipv4, detect_ipv6, gethostname};
use crate::subprocess_factory::SubprocessWorkerFactory;

use super::PyObserverLateJoiner;
use super::helpers::{map_read_dir_error, records_to_seed};

#[pymethods]
impl PyObserverLateJoiner {
    /// Read the peer-info dir, dial into the mesh, restore the
    /// cluster snapshot, and drive the observation loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        // -- pre-detach: read peer-info dir (synchronous file I/O,
        // small enough that we don't bother offloading; surfacing
        // ReadDirError as a Python exception before we even spin up
        // tokio keeps the error path simple).
        let records = read_peer_info_dir_v2(&self.peer_info_dir).map_err(map_read_dir_error)?;
        let seed = records_to_seed(&records);
        if seed.is_empty() {
            // `read_peer_info_dir_v2` already errors on the empty /
            // all-v1 case; this guards against the (currently
            // unreachable) future shape where the filter drops every
            // record post-conversion. Fail loud rather than spin in
            // `join_running_cluster`'s connect window.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "observer late-joiner: peer-info dir produced zero usable seed entries \
                 after v2 filtering — refusing to enter join_running_cluster with an \
                 empty seed (would hang on the connect-budget)",
            ));
        }

        let observer_id = self.observer_id.clone();
        let estimator = self.topology.estimator.clone();
        // `connect_timeout` is intentionally NOT plumbed: it gates
        // the submitter-bound `NetworkClient` dial loop in the
        // regular-secondary path, which an observer doesn't have
        // (we hand SecondaryCoordinator a `NoPrimaryTransport` stub
        // — see `no_primary.rs`). The observer's analogous budget
        // lives in `DEFAULT_JOIN_TIMEOUT` on the peer-side
        // `join_running_cluster` call below; tying the two
        // together would conflate primary-handshake retry semantics
        // with peer-mesh bootstrap rendezvous semantics.
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        // Take the Python peer-lifecycle listener (if any) out of
        // `self` so it can move into the detached tokio runtime.
        // Wrapped through `PyPeerLifecycleListener::new` into a
        // `Box<dyn LifecycleListener>` at the boundary so the
        // manager-distributed registration API stays
        // PyO3-agnostic.
        let peer_lifecycle_listener =
            self.peer_lifecycle_listener
                .take()
                .map(crate::peer_lifecycle_bridge::PyPeerLifecycleListener::new);
        // Move the holdings set out of `self` so it can be drained into
        // `attach_observer_announcer` on the tokio side. After this
        // point `self.holdings` is empty; the observer is single-shot
        // per `__init__` so a second `run()` would never make sense
        // anyway (the snapshot RPC + restore latch are also one-shot).
        let holdings = std::mem::take(&mut self.holdings);
        let scheduler_config = self.scheduler_config.clone();
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval = std::time::Duration::from_secs_f64(
            self.panik_watcher_poll_interval_secs,
        );

        // Terminal-outcome shapes for the observer late-joiner's run.
        // `Done` returns the observed-completion count; `Panik`
        // signals the outer scope to call `std::process::exit(137)`
        // after the GIL is re-acquired. Same shape the regular
        // secondary uses — keeps the two pyclasses' panik response
        // structurally aligned.
        enum ObserverRunOutcome {
            Done(u32),
            Panik(std::path::PathBuf),
        }
        let result: Result<ObserverRunOutcome, PyErr> =
            py.detach(|| -> Result<ObserverRunOutcome, PyErr> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to create tokio runtime: {e}"
                    ))
                })?;
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                // 1. Stand up the real peer transport with our chosen
                //    observer-id. The CN baked into the cert MUST
                //    match `observer_id` because every dialing peer
                //    validates the SAN against the logical id.
                let mut peer_network =
                    PeerNetwork::<RunnerIdentifier>::start(&observer_id).await.map_err(
                        |e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "observer late-joiner: failed to start peer network: {e}"
                            ))
                        },
                    )?;
                let peer_cert_pem = peer_network.cert_pem().to_string();
                let peer_port = peer_network.port();

                // 2. Bootstrap rendezvous: hand the seed list to the
                //    trait default impl, which sequences the dial +
                //    snapshot request + reply wait. Errors get
                //    typed strings; we PyErr them with the snapshot
                //    JSON context so the operator can correlate.
                let snapshot_json = peer_network
                    .join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT)
                    .await
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "observer late-joiner: join_running_cluster failed: {e}"
                        ))
                    })?;

                // 3. Decode the snapshot. The wire frame is a String
                //    (the protocol crate keeps `I` erased there); we
                //    materialise it back into the typed snapshot here
                //    so the manager-distributed crate gets the
                //    `ClusterStateSnapshot<RunnerIdentifier>` it
                //    expects.
                let snap: ClusterStateSnapshot<RunnerIdentifier> =
                    serde_json::from_str(&snapshot_json).map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "observer late-joiner: failed to decode \
                             ClusterStateSnapshot from join_running_cluster reply: {e}"
                        ))
                    })?;

                // 4. Construct the observer's coordinator. is_observer=true,
                //    num_workers=0; the Step 7 election filter + the
                //    self-exclusion guard inside `secondary/election.rs`
                //    together keep the observer out of every
                //    promote-to-primary path.
                let config = SecondaryConfig {
                    secondary_id: observer_id.clone(),
                    num_workers: 0,
                    max_resources: dynrunner_core::ResourceMap::from([(
                        dynrunner_core::ResourceKind::memory(),
                        // 1 GiB: a marker value — the observer
                        // doesn't run workers, so its resource map
                        // is irrelevant to actual work, but the
                        // worker-pool budget math (which never
                        // triggers on an empty pool — see
                        // `scheduler::check_resource_pressure`'s
                        // `num_workers == 0` early return) reads it
                        // for completeness.
                        1024 * 1024 * 1024,
                    )]),
                    hostname: gethostname(),
                    keepalive_interval: dist_keepalive,
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    retry_max_passes: dist_retry_max_passes,
                    primary_link_failure_threshold: dist_primary_link_failure_threshold,
                    primary_link_failure_window: dist_primary_link_failure_window,
                    setup_deadline: dist_setup_deadline,
                    is_observer: true,
                    // Observer has zero workers — the watcher's
                    // decision arm short-circuits on an empty pool
                    // and the sample arm reports the host reading
                    // with `tracked_workers_count = 0`. Default
                    // cadences mirror the live secondary path.
                    resource_check_interval: std::time::Duration::from_millis(100),
                    log_oom_watcher: false,
                    promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                    // Observers never promote (the election filter +
                    // setup-promote dispatch reject keep them off the
                    // primary path), so the cap is structurally inert
                    // — leave at the default `None` (unbounded).
                    unfulfillable_reinject_max_per_task: None,
                };

                // No-op factory: the run loop's only `factory`
                // consumer is `initialize_workers`, which is gated
                // by `!setup_phase_completed`. We're about to set
                // that latch to `true` via
                // `restore_from_snapshot_and_skip_setup`, so the
                // factory's `spawn_worker` is unreachable —
                // any factory satisfying the trait bound works.
                // We reuse the existing `SubprocessWorkerFactory`
                // with placeholder fields rather than adding a
                // dedicated `NoopWorkerFactory` because Step 11
                // (trait deletion in the unification refactor)
                // is about to remove the type-parameter that
                // forces a concrete factory here.
                let mut factory = SubprocessWorkerFactory {
                    python_executable: PathBuf::new(),
                    source_dir: PathBuf::new(),
                    output_dir: PathBuf::new(),
                    log_dir: PathBuf::new(),
                    log_paths: Default::default(),
                    // Empty registry — the observer's factory is
                    // unreachable (snapshot-restore latches
                    // setup_phase_completed=true before any
                    // `initialize_workers` would consult it). Empty is
                    // the correct placeholder; first_type_runtime()
                    // would surface a clear error if a future code
                    // path accidentally reached spawn.
                    types: Default::default(),
                    skip_existing: false,
                    connection_mode: ConnectionMode::Socketpair,
                    manual_start_worker: false,
                    worker_spec: None,
                    child_processes: Vec::new(),
                };

                let mut secondary: SecondaryCoordinator<
                    NoPrimaryTransport,
                    _,
                    _,
                    _,
                    _,
                    RunnerIdentifier,
                > = SecondaryCoordinator::new(
                    config,
                    NoPrimaryTransport,
                    peer_network,
                    scheduler_config.build_memory_scheduler(),
                    estimator,
                );

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run_until_setup_or_done` enters — the
                // coordinator's `register_lifecycle_listener` contract
                // requires pre-run registration because the listener
                // vector is `mem::take`-d into the spawned dispatcher
                // on first entry.
                if let Some(listener) = peer_lifecycle_listener {
                    secondary.register_lifecycle_listener(listener);
                }

                // CertExchange path is skipped (setup_phase_completed
                // latched true), but PeerInfo broadcasts that arrive
                // post-restore still consult the local
                // `peer_cert_info` when this observer's id shows up
                // in their distribution. Populating it keeps the
                // broadcast handler symmetric — observers participate
                // in cert exchange so peers can dial back into the
                // observer (e.g. for snapshot RPCs from a later
                // joiner).
                secondary.set_peer_cert_info(PeerCertInfo {
                    public_cert_pem: peer_cert_pem,
                    ipv4_address: Some(detect_ipv4(None)),
                    ipv6_address: detect_ipv6(None),
                    quic_port: peer_port,
                });

                // Wire the panik watcher in the same shape as the
                // regular secondary. The observer doesn't own
                // workers but still participates in the
                // cluster-wide stop: its `process_tasks` panik arm
                // broadcasts `PanikRequested` (peers that haven't
                // tripped their own file learn about the stop
                // here) and the post-loop scope below exits 137.
                let mut panik_watcher = dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                    dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                        paths: panik_watcher_paths,
                        poll_interval: panik_watcher_poll_interval,
                    },
                );
                if let Some(rx) = panik_watcher.take_signal_rx() {
                    secondary.register_panik_signal_rx(rx);
                }

                // Attach the resource-holdings announcer's hook +
                // outbox BEFORE the snapshot restore: the restore's
                // `cluster_state.restore` path fires
                // `fire_role_change_hooks` from inside its
                // `primary_epoch > local` branch, which we want to
                // count as the post-restore initial trigger. With
                // attach-then-restore the snapshot's apply naturally
                // emits the first `AnnounceTrigger` into the queue;
                // a separate explicit "fire one trigger" step would
                // duplicate that stimulus.
                //
                // The bundle carries the `AnnouncerHandle` (rx /
                // holdings / peer_id / primary_epoch_mirror — the
                // four `run_observer_announcer` inputs) plus the
                // production `PeerMeshAnnouncerSender` that forwards
                // each announce onto the coordinator-side outbox the
                // operational loop drains.
                let (announcer_handle, announcer_sender) =
                    secondary.attach_observer_announcer(holdings);

                // 5. Install the snapshot AND latch
                //    setup_phase_completed=true. The single-method
                //    `restore_from_snapshot_and_skip_setup` is the
                //    only place outside the secondary crate allowed
                //    to touch the latch (see its doc-comment).
                secondary.restore_from_snapshot_and_skip_setup(snap);

                // 5.5. Spawn the announcer task. The coordinator's
                //      `select!` arm drains the production sender's
                //      outbox onto `peer_transport.send`; here we just
                //      hand the task its four inputs (rx / holdings /
                //      peer_id / primary_epoch_mirror) plus the
                //      production sender, and store the JoinHandle for
                //      shutdown abort+join — same discipline as the
                //      peer-lifecycle dispatcher's cleanup.
                let announcer_task = tokio::task::spawn_local(run_observer_announcer(
                    announcer_handle.rx,
                    announcer_handle.holdings,
                    announcer_handle.peer_id,
                    announcer_sender,
                    announcer_handle.primary_epoch_mirror,
                ));

                // 6. Drive the run loop. The first iteration's
                //    setup-skip guard fires immediately; subsequent
                //    iterations are `RunOutcome::Done` once the
                //    cluster broadcasts `RunComplete`. SetupPending
                //    is unreachable for an observer (only
                //    pre-staged-mode primaries emit the
                //    PromotePrimary that triggers it, and an observer
                //    is never the elected secondary).
                //
                // # Why a sub-block
                //
                // Wrapped in an inner async block whose result is
                // captured BEFORE the announcer-task cleanup so any
                // `?`-propagated error or early `Err`-return still
                // routes through the abort+await on
                // `announcer_task`. Without the wrapper an
                // error-return would skip the cleanup and leak the
                // announcer task into the next observer dispatcher
                // run.
                let loop_result: Result<ObserverRunOutcome, PyErr> = async {
                    // `RunOutcome` adds the `PanikShutdown` arm but
                    // both terminal arms (Done | Panik) still
                    // terminate the loop — clippy still sees it as
                    // never iterating. The loop is retained as
                    // defensive scaffolding for a future "retry on
                    // SetupPending" branch — same shape the in-process
                    // distributed manager will need if observers ever
                    // become promotable.
                    #[allow(clippy::never_loop)]
                    loop {
                        let outcome = secondary
                            .run_until_setup_or_done(&mut factory)
                            .await
                            .map_err(|e| {
                                pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "observer late-joiner: secondary run loop failed: {e}"
                                ))
                            })?;
                        match outcome {
                            RunOutcome::Done => break,
                            RunOutcome::PanikShutdown {
                                matched_path,
                                reason,
                            } => {
                                tracing::warn!(
                                    matched_path = %matched_path.display(),
                                    reason = %reason,
                                    "observer panik shutdown; propagating \
                                     to PyO3 boundary for exit(137)"
                                );
                                return Ok(ObserverRunOutcome::Panik(matched_path));
                            }
                            RunOutcome::SetupPending => {
                                // Defensive: a late-joiner observer
                                // should never see SetupPending — that
                                // outcome comes from a
                                // PromotePrimary{required_setup=true}
                                // arrival, which an observer cannot
                                // accept (the election filter + the
                                // dispatch.rs defensive reject keep
                                // observers off the promote path).
                                // Surface it as a typed error rather
                                // than retrying — silent re-entry on
                                // an unreachable branch would mask a
                                // protocol bug.
                                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                                    "observer late-joiner: secondary returned \
                                     RunOutcome::SetupPending — unreachable for an \
                                     observer (PromotePrimary should be rejected); \
                                     this indicates a protocol or election-filter \
                                     regression",
                                ));
                            }
                        }
                    }

                    Ok(ObserverRunOutcome::Done(secondary.completed_count() as u32))
                }
                .await;

                // Announcer-task cleanup. The task is `spawn_local`-ed
                // on this LocalSet and holds a clone of the
                // coordinator-side outbox `mpsc::Sender`; the
                // coordinator itself holds another clone on
                // `announcer_outbox_tx`, so neither side observes a
                // closed-channel `None` on natural shutdown. The
                // explicit abort+await mirrors the peer-lifecycle
                // dispatcher's cleanup discipline (see
                // `SecondaryCoordinator::cleanup_lifecycle_dispatcher`):
                // `abort()` is the kill signal, `await` synchronises
                // with the task's actual termination so a follow-on
                // observer dispatcher run (test-driven; the
                // production single-shot Python wrapper exits after
                // this) starts with a quiesced runtime.
                announcer_task.abort();
                let _ = announcer_task.await;

                loop_result
            }))
        });

        match result? {
            ObserverRunOutcome::Done(completed) => {
                self.completed = completed;
                Ok(())
            }
            ObserverRunOutcome::Panik(matched_path) => {
                // GIL re-acquired (the `py.detach` block returned).
                // Surface the cause to the dispatcher log one last
                // time then exit(137) — same exit-on-panik shape as
                // `PySecondaryCoordinator::run`.
                tracing::error!(
                    matched_path = %matched_path.display(),
                    "panik shutdown: observer exiting with code 137"
                );
                std::process::exit(137);
            }
        }
    }

    /// Observed completion count read off the snapshot + any live
    /// broadcasts the observer ingested during its run window.
    /// Equivalent to a regular secondary's `completed_count` —
    /// surfaces the union of completed tasks visible in
    /// `cluster_state.tasks`.
    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}
