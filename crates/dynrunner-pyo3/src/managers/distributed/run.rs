//! `PyDistributedManager::run` — drives the in-process primary +
//! N secondaries pipeline on a detached tokio runtime over channel
//! transports. Also exposes the `completed` / `failed` / `stranded`
//! getters Python `run_distributed` reads after `run()` returns.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, RunError, SecondaryConfig, SecondaryCoordinator,
};
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;

use crate::config::connection::ConnectionMode;
use crate::identifier::RunnerIdentifier;
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;

use super::PyDistributedManager;

#[pymethods]
impl PyDistributedManager {
    /// Run the distributed processing pipeline.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let num_workers = self.num_workers_per_secondary;
        let max_resources_per_secondary = self.max_resources_per_secondary.clone();
        let estimator = self.estimator.clone();
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_path = self.log_path.clone();
        let log_paths = self.log_paths.clone();
        // Single scheduler-tuning snapshot is shared between the
        // in-process primary AND every spawned secondary; cloning into
        // the per-secondary task closure below preserves the same
        // budget shape across the cluster.
        let scheduler_config = self.scheduler_config.clone();
        // Panik-watcher config — same kwarg surface as the standalone
        // primary/secondary pyclasses. Shared verbatim by the
        // in-process primary AND every spawned secondary so a panik
        // file appearing on the host triggers the SAME response on
        // every coordinator in the process; without that the in-
        // process secondaries would silently outlive a primary panik
        // (their workers are spawned in their own pgids and survive
        // their parent's exit).
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval = std::time::Duration::from_secs_f64(
            self.panik_watcher_poll_interval_secs,
        );
        // Compose the per-secondary memprofile output dir once on
        // the GIL thread so the per-secondary spawn closures below
        // receive identical `Option<PathBuf>` values without each
        // re-deriving from `self`. The operator's `output_dir`
        // (always set) wins over the SLURM wrapper bind-mount
        // probe — in-process distributed runs never expose the
        // wrapper but always have a Python-supplied output dir.
        let memprofile_output_dir =
            crate::managers::secondary::run::resolve_secondary_memprofile_dir(
                self.memprofile_enabled,
                Some(self.output_dir.as_path()),
            );
        // Same shape as `PySecondaryCoordinator::run`: derive the
        // memuse log path on the GIL thread so every per-secondary
        // spawn closure clones it as a ready-made
        // `Option<PathBuf>`. Defaults to
        // `{self.output_dir}/memuse.log`; `None` only if
        // `self.output_dir` is itself unset (it isn't — the field
        // is always populated by the constructor).
        let memuse_log_path =
            dynrunner_manager_local::memuse::derive_memuse_log_path(
                Some(self.output_dir.as_path()),
                None,
            );

        // Pre-compute per-secondary log directories under the GIL —
        // `resolve_log_dir` calls into Python's `datetime` module —
        // before detaching for the tokio runtime. Each secondary gets
        // its own `{timestamp}/{secondary_id}` subdirectory so the
        // default `worker_<id>.log` filename never collides across
        // secondaries on a shared mount, and `create_dir_all` errors
        // surface here at run start rather than as silent log loss.
        // `log_path` (not `output_dir`) is the log-mount root — on
        // SLURM deployments it points at `/app/log-network` while
        // `output_dir` is `/app/out-network`. Single-host callers
        // that did not supply a separate log dir get `log_path ==
        // output_dir` from the fallback in `LoadedTaskDefinition`.
        let mut sec_log_dirs: Vec<(String, PathBuf)> =
            Vec::with_capacity(num_secondaries as usize);
        for i in 0..num_secondaries {
            let sid = format!("sec-{i}");
            let dir = log_paths.resolve_log_dir(py, &log_path, &sid)?;
            std::fs::create_dir_all(&dir).map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "failed to create log directory {dir:?} for {sid}: {e}"
                ))
            })?;
            sec_log_dirs.push((sid, dir));
        }
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_oom_retry_max_passes = self.distributed_config.oom_retry_max_passes();
        let dist_mass_death_grace = self.distributed_config.mass_death_grace();
        let dist_mass_death_min_count = self.distributed_config.mass_death_min_count();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        let dist_setup_promote_deadline = self.distributed_config.setup_promote_deadline();
        let dist_resource_check_interval = self.distributed_config.resource_check_interval();
        let dist_log_oom_watcher = self.distributed_config.log_oom_watcher();
        let worker_spec = self.worker_spec.clone();
        // Per-type subprocess dispatch: the factory carries the full
        // `TypeRegistry`. `spawn_worker` defaults to `types.first()`
        // for initial pool init (preserves pre-fix single-type
        // behaviour); `spawn_worker_for_type` consults the registry
        // for per-task respawn on TypeId mismatch. Cloned per
        // secondary below in the spawn loop.
        if self.types.first().is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            ));
        }
        let types = self.types.clone();
        let skip_existing = self.skip_existing;
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        let phase_deps = self.phase_deps.clone();
        let source_pre_staged_root = self.source_pre_staged_root.clone();
        // Pre-staged mode: the submitter has no local view of the
        // staged corpus, so `_dispatch_single_process` handed us an
        // empty binaries list and the bootstrap `PromotePrimary` must
        // tell the chosen secondary to run discovery + ledger-seed on
        // its bind-mounted `src_network`. The Python dispatch layer is
        // the single source of truth for "binaries empty" here — when
        // `source_pre_staged_root.is_some()` the helper has already
        // ensured the empty-list invariant, so we mirror the
        // submitter-side pipeline gate without re-checking on the
        // binaries (the `PyPrimaryCoordinator::run` gate that pairs
        // `is_some()` with `binaries.is_empty()` defends against the
        // SLURM pipeline path where binaries may legitimately be non-
        // empty; that case does not exist for the in-process manager).
        let required_setup_on_promote = source_pre_staged_root.is_some();

        // Phase 5B: re-acquire the GIL from the coordinator's LocalSet
        // and dispatch to the Python TaskDefinition's `on_phase_*`
        // methods. Built before `py.detach` so the closures can capture
        // ref-bumped `Py<PyAny>` clones.
        let on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(
                self.task_definition.clone_ref(py),
            ),
        );
        let on_phase_end: crate::managers::lifecycle::OnPhaseEnd = Box::new(
            crate::managers::lifecycle::make_on_phase_end(
                self.task_definition.clone_ref(py),
            ),
        );

        // Clone the task_definition once per secondary so the in-process
        // composition can fire `on_phase_end` through a promoted
        // secondary's co-located primary on the SAME Python
        // `TaskDefinition` instance the live primary's callback already
        // targets. Each spawned in-process secondary registers these
        // callbacks and, under composition, transfers them to its
        // co-located parked primary (which owns the phase machine and
        // fires the cascade once activated). Each per-secondary closure
        // pair is pushed in the order the secondaries are spawned below;
        // the spawn loop pops one pair off this vec per iteration so each
        // closure captures its own `Py<PyAny>` ref-bump.
        let mut sec_phase_lifecycle_callbacks: Vec<(
            crate::managers::lifecycle::OnPhaseStart,
            crate::managers::lifecycle::OnPhaseEnd,
        )> = Vec::with_capacity(num_secondaries as usize);
        for _ in 0..num_secondaries {
            let on_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
                crate::managers::lifecycle::make_on_phase_start(
                    self.task_definition.clone_ref(py),
                ),
            );
            let on_end: crate::managers::lifecycle::OnPhaseEnd = Box::new(
                crate::managers::lifecycle::make_on_phase_end(
                    self.task_definition.clone_ref(py),
                ),
            );
            sec_phase_lifecycle_callbacks.push((on_start, on_end));
        }

        // Take the Python peer-lifecycle listener (if any) out of
        // `self` so it can move into the detached tokio runtime.
        // Wrapped through `PyPeerLifecycleListener::new` into a
        // `Box<dyn LifecycleListener>` at the boundary so the
        // manager-distributed registration API stays
        // PyO3-agnostic. The in-process secondaries do NOT receive
        // the listener (see the field doc on
        // `peer_lifecycle_listener`).
        let peer_lifecycle_listener =
            self.peer_lifecycle_listener
                .take()
                .map(crate::peer_lifecycle_bridge::PyPeerLifecycleListener::new);

        // Same shape for the task-completion listener: independent
        // dispatcher pair on the in-process primary; same
        // pre-`run()` registration contract.
        let task_completed_listener =
            self.task_completed_listener
                .take()
                .map(crate::task_completed_bridge::PyTaskCompletedListener::new);

        // Snapshot the cap, flip `run_started`, and consume the
        // receiver for the detached runtime in one step. The helper
        // owns the single-shot guard and the snapshot ordering; the
        // sender clone returned in `wiring` keeps backing future
        // `handle()` calls. Mirrors `PyPrimaryCoordinator::run`.
        let wiring = self.control_plane.take_for_run()?;
        let unfulfillable_reinject_max_per_task = wiring.cap_snapshot;
        let command_tx = wiring.command_tx;
        let command_rx = wiring.command_rx;

        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut stranded = 0u32;
        // Cluster-collapsed signal carried out of the detached tokio
        // runtime — see `PyPrimaryCoordinator::run` for the full
        // rationale; the in-process distributed manager mirrors the
        // same translation so a collapse here surfaces as a
        // `RuntimeError` to the Python caller of `run_distributed`.
        let mut cluster_collapsed: Option<RunError> = None;
        // Panik outcome carried out of the detached tokio runtime —
        // same shape as `PyPrimaryCoordinator::run`. `Some` iff the
        // in-process primary's `run` returned `RunError::PanikShutdown`.
        let mut panik_shutdown_path: Option<std::path::PathBuf> = None;
        // Setup-promote deadline carried out of the detached tokio
        // runtime — same shape as `PyPrimaryCoordinator::run`. `Some`
        // iff the in-process primary's `run` returned
        // `RunError::SetupDeadlineExpired`.
        let mut setup_deadline_expired: Option<RunError> = None;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                use tokio::sync::mpsc as tokio_mpsc;

                let mut sec_handles = Vec::new();
                let mut all_child_processes: Vec<Option<std::process::Child>> = Vec::new();

                // Build the primary's single `Tr: TunneledPeerTransport`.
                // Post-collapse this is the ONE transport the coordinator
                // holds. `shared_outgoing` is the writer table the
                // in-process path registers each per-secondary writer
                // into directly (no accept loops here, so the
                // registration sink goes unused); `inbound` is the sink
                // the per-secondary forwarder feeds — it is the
                // transport's real, single inbound stream (no fan-out
                // tap, no separate legacy `ChannelSecondaryTransportEnd`
                // consumer). Role-addressed / `Address::Peer` sends and
                // the unified `recv_peer()` both run over this one
                // transport.
                let (peer_transport, shared_outgoing, inbound, _registration) =
                    dynrunner_transport_tunnel::TunneledPeerTransport::<
                        RunnerIdentifier,
                    >::new("primary".into());

                for ((secondary_id, sec_log), (sec_on_phase_start, sec_on_phase_end)) in
                    sec_log_dirs.into_iter().zip(sec_phase_lifecycle_callbacks)
                {
                    // primary→secondary channel
                    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
                    // secondary→primary channel
                    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

                    // Register the per-secondary writer directly into the
                    // transport's shared writer table so
                    // `transport.send_to_peer(sec_id, ..)` /
                    // `Address::Peer(sec_id)` / role-resolved dispatch
                    // reach this secondary. (The QUIC path registers via
                    // the accept-loop registration sink instead; in-
                    // process there are no accept loops, so the direct
                    // insert is the registration.)
                    shared_outgoing
                        .borrow_mut()
                        .insert(secondary_id.clone(), pri_to_sec_tx);

                    // Forward secondary→primary messages straight into
                    // the transport's single inbound stream — the
                    // in-process analogue of a QUIC/WSS accept loop's
                    // reader task feeding the inbound sink. No fan-out
                    // tap: `recv_peer()` drains this same stream.
                    let fwd_tx = inbound.clone();
                    tokio::task::spawn_local(async move {
                        let mut rx = sec_to_pri_rx;
                        while let Some(msg) = rx.recv().await {
                            if fwd_tx.send(msg).is_err() {
                                break;
                            }
                        }
                    });

                    let sec_python = python_executable.clone();
                    let sec_worker_spec = worker_spec.clone();
                    let sec_source = source_dir.clone();
                    let sec_output = output_dir.clone();
                    let sec_log_paths = log_paths.clone();
                    let sec_types = types.clone();
                    let sec_estimator = estimator.clone();
                    let sec_max_resources = max_resources_per_secondary.clone();
                    let sec_scheduler_config = scheduler_config.clone();
                    let sec_panik_paths = panik_watcher_paths.clone();
                    let sec_panik_poll = panik_watcher_poll_interval;
                    let sec_memprofile_output_dir = memprofile_output_dir.clone();
                    let sec_memuse_log_path = memuse_log_path.clone();

                    let handle = tokio::task::spawn_local(async move {
                        let transport = ChannelPrimaryTransportEnd {
                            tx: sec_to_pri_tx,
                            rx: pri_to_sec_rx,
                        };
                        let config = SecondaryConfig {
                            secondary_id,
                            num_workers,
                            max_resources: sec_max_resources,
                            hostname: "localhost".into(),
                            keepalive_interval: dist_keepalive,
                            // In-process mode: primary and
                            // secondaries share filesystem
                            // visibility, so the staging walk's
                            // relative `src_path` (e.g.
                            // `input-0.txt`, derived from
                            // `binary.path` post-strip-prefix)
                            // resolves under the primary's
                            // `source_dir`. Without this set,
                            // `stage_and_register`'s `stage_file`
                            // call rejects every relative
                            // src_path with "no src_network is
                            // configured" and the next
                            // TaskAssignment surfaces as the
                            // legacy "expected StageFile
                            // notification first" failure even
                            // though staging WAS queued — pairs
                            // with the staging-walk fix above:
                            // both are needed for the in-process
                            // pipeline to actually process file-
                            // backed items.
                            src_network: Some(sec_source.clone()),
                            src_tmp: None,
                            peer_timeout: dist_peer_timeout,
                            keepalive_miss_threshold: dist_keepalive_miss_threshold,
                            retry_max_passes: dist_retry_max_passes,
                            oom_retry_max_passes: dist_oom_retry_max_passes,
                            primary_link_failure_threshold:
                                dist_primary_link_failure_threshold,
                            primary_link_failure_window:
                                dist_primary_link_failure_window,
                            setup_deadline: dist_setup_deadline,
                            is_observer: false,
                            resource_check_interval: dist_resource_check_interval,
                            log_oom_watcher: dist_log_oom_watcher,
                            promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                            // In-process distributed manager: the
                            // `ReinjectTask` per-task budget cap, mirrored
                            // from the in-process primary's
                            // `PrimaryConfig` so an externally-issued
                            // `reinject_task` honours the operator's knob
                            // symmetrically regardless of which authority
                            // (live or co-located) services it. Inert on
                            // a secondary until it holds the primary role
                            // via its co-located primary.
                            unfulfillable_reinject_max_per_task,
                            // In-process distributed manager runs primary
                            // and secondaries in the same process, so
                            // nesting the workers cgroup would tighten
                            // the cap on the shared address space.
                            // Leave unset; only the network-secondary
                            // path (where the secondary runs in its own
                            // SLURM container) opts in via
                            // `--mem-manager-reserved`.
                            mem_manager_reserved_bytes: None,
                            // Per-secondary memprofile output dir
                            // resolved on the GIL thread above from
                            // the operator's `--memprofile` opt-in
                            // plus `self.output_dir` (always set).
                            // `Some(path)` activates per-task
                            // sampling on the in-process secondary
                            // path symmetrically with the SLURM and
                            // multi-computer-local secondaries.
                            output_dir: sec_memprofile_output_dir.clone(),
                            // Default-on aggregate memuse log under
                            // `{self.output_dir}/memuse.log`. Same
                            // shape every other dispatch path
                            // produces; preserves the
                            // `Option<PathBuf>` test-fixture
                            // flexibility (None = silent).
                            memuse_log_path: sec_memuse_log_path.clone(),
                        };

                        let estimator = sec_estimator;

                        let mut factory = SubprocessWorkerFactory {
                            python_executable: sec_python,
                            source_dir: sec_source,
                            output_dir: sec_output,
                            log_dir: sec_log,
                            log_paths: sec_log_paths,
                            types: sec_types,
                            skip_existing,
                            connection_mode: ConnectionMode::Socketpair,
                            manual_start_worker: false,
                            worker_spec: sec_worker_spec.clone(),
                            child_processes: Vec::new(),
                        };

                        // Compose the opaque secondary transport: the
                        // co-located channel end is the uplink to the
                        // in-process primary, `NoPeerTransport` is the
                        // (absent) mesh. `Address::Role(Role::Primary)`
                        // resolves to the loopback channel while the role
                        // cache is cold — exactly the in-process
                        // primary. See `UnifiedSecondaryTransport`.
                        let unified =
                            dynrunner_transport_tunnel::UnifiedSecondaryTransport::new(
                                config.secondary_id.clone(),
                                transport,
                                dynrunner_transport_quic::NoPeerTransport,
                            );
                        let mut secondary = SecondaryCoordinator::new(
                            config,
                            unified,
                            sec_scheduler_config.build_memory_scheduler(),
                            estimator,
                        );

                        // Per-secondary panik watcher. One watcher per
                        // coordinator is the simplest correct shape: a
                        // single shared `oneshot::Sender` couldn't
                        // fan out to N receivers, and broadcasting
                        // through a different channel type would
                        // complicate the framework API. Polling
                        // overhead at the user-spec'd 10s cadence is
                        // negligible (one stat per path per 10s, per
                        // secondary).
                        let mut panik_watcher =
                            dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                                dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                                    paths: sec_panik_paths,
                                    poll_interval: sec_panik_poll,
                                    // SECONDARY-role spawner (in-
                                    // process, alongside an in-
                                    // process primary). Same
                                    // rationale as the standalone
                                    // secondary in
                                    // `managers/secondary/run.rs`:
                                    // host-side shutdown-manager
                                    // forwards SLURM signals as
                                    // `kill -TERM` into this
                                    // process, and the secondary's
                                    // watcher must route that into
                                    // the panik cascade. NOTE the
                                    // primary running in the SAME
                                    // process has a SEPARATE
                                    // watcher (below) with
                                    // SIGTERM listening OFF —
                                    // primary's shutdown
                                    // semantics are out of scope.
                                    // Because only ONE handler is
                                    // installed process-wide and
                                    // multiple `Signal` instances
                                    // share it, the per-secondary
                                    // watchers in a
                                    // multiple-secondary
                                    // in-process deployment ALL
                                    // see the same SIGTERM and
                                    // ALL fire panik together —
                                    // which is exactly the
                                    // semantics we want: SIGTERM
                                    // is a process-level signal,
                                    // panik is cluster-level,
                                    // every coordinator in this
                                    // process should cascade.
                                    listen_for_sigterm: true,
                                },
                            );
                        if let Some(rx) = panik_watcher.take_signal_rx() {
                            secondary.register_panik_signal_rx(rx);
                        }

                        // Install the per-secondary phase-lifecycle
                        // callbacks BEFORE `run()` enters — same pre-run
                        // registration contract as
                        // `register_lifecycle_listener` /
                        // `register_panik_signal_rx`. In the IN-PROCESS
                        // distributed manager the authority is the
                        // in-process `PrimaryCoordinator` (built below),
                        // which fires `on_phase_*` directly; these
                        // in-process secondaries use a `NoPeerTransport`
                        // mesh and therefore compose NO co-located parked
                        // primary, so their registered callbacks stay
                        // dormant (no transfer, no promotion in-process)
                        // and never call into Python. They are registered
                        // for shape-parity with the SLURM secondary path
                        // (which DOES transfer them to a co-located parked
                        // primary); the closures target the SAME single
                        // process-wide Python `TaskDefinition` instance
                        // the in-process primary's callbacks already use.
                        secondary.register_phase_lifecycle_callbacks(
                            sec_on_phase_start,
                            sec_on_phase_end,
                        );

                        let result = secondary.run(&mut factory).await;
                        if let Err(e) = &result {
                            tracing::error!(error = %e, "secondary failed");
                        }

                        // Collect child processes for cleanup
                        let children: Vec<Option<std::process::Child>> =
                            factory.child_processes.drain(..).collect();

                        (secondary.completed_count(), children)
                    });

                    sec_handles.push(handle);
                }
                // Drop the original inbound sink so only the per-secondary
                // forwarding tasks hold senders — once every secondary
                // exits and its forwarder ends, the transport's
                // `recv_peer()` observes `None` (the inbound-closed
                // signal the operational loop's `transport_closed` gate
                // keys off).
                drop(inbound);

                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    // `--source-already-staged` is a dispatch-layer
                    // discriminator, not a SLURM-only signal: the
                    // Python `_dispatch_single_process` helper threads
                    // `args.source_already_staged` into the
                    // constructor's `source_pre_staged_root` kwarg, we
                    // hoist it onto the PrimaryConfig, and derive
                    // `required_setup_on_promote` from
                    // `source_pre_staged_root.is_some()`. The dispatch
                    // helper has already returned an empty
                    // `binaries` list in pre-staged mode, so the
                    // chosen secondary owns the discovery + ledger-
                    // seed via the bootstrap `PromotePrimary`.
                    source_pre_staged_root: source_pre_staged_root.clone(),
                    uses_file_based_items,
                    required_setup_on_promote,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    fleet_dead_timeout: std::time::Duration::from_secs(30),
                    mesh_ready_timeout: std::time::Duration::from_secs(60),
                    mass_death_grace: dist_mass_death_grace,
                    mass_death_min_count: dist_mass_death_min_count,
                    // Threaded into PrimaryConfig so the manager's
                    // run() has the local source root needed for the
                    // initial staging walk's content-hash + per-
                    // secondary fan-out. The explicit
                    // `queue_initial_staging_from_binaries` call
                    // below pre-populates the queue today; threading
                    // the field uniformly keeps the manager's
                    // future-direction (auto-stage when no caller
                    // pre-queues) wired without each caller re-
                    // implementing the orchestration.
                    source_dir: Some(source_dir.clone()),
                    // Snapshot taken on the GIL thread (see above) so
                    // the in-process distributed primary honours the
                    // same `unfulfillable_reinject_max_per_task` knob
                    // every other primary path does. The
                    // `PrimaryHandle::set_unfulfillable_reinject_max_per_task`
                    // setter writes through the shared cell pre-run;
                    // post-`mark_run_started` writes raise on the
                    // handle side, so the value frozen here is the
                    // single source of truth for the inner loop.
                    unfulfillable_reinject_max_per_task,
                    setup_promote_deadline: dist_setup_promote_deadline,
                };

                let mut primary = PrimaryCoordinator::new(
                    config,
                    peer_transport,
                    scheduler_config.build_memory_scheduler(),
                    estimator,
                );

                // Swap in the Python-facing command channel so the
                // `PrimaryHandle` Python is holding talks to the same
                // receiver the operational loop reads from. Same
                // pre-`run()` contract as `PyPrimaryCoordinator`.
                primary.replace_command_channel(command_tx, command_rx);

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE the primary's `run()` enters — the
                // coordinator's `register_lifecycle_listener` contract
                // requires pre-run registration because the listener
                // vector is `mem::take`-d into the spawned dispatcher.
                if let Some(listener) = peer_lifecycle_listener {
                    primary.register_lifecycle_listener(listener);
                }

                // Same shape for the task-completion listener:
                // independent dispatcher pair with the same pre-run
                // registration contract.
                if let Some(listener) = task_completed_listener {
                    primary.register_task_completed_listener(listener);
                }

                // Panik watcher for the in-process primary. Each
                // in-process secondary spawn_local closure above also
                // wires its own watcher — every coordinator on this
                // process polls independently and fires its own
                // teardown when its file appears. Handle held in
                // scope for `Drop::abort()` at loop exit.
                let mut panik_watcher = dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                    dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                        paths: panik_watcher_paths,
                        poll_interval: panik_watcher_poll_interval,
                        // PRIMARY-role spawner: SIGTERM listening
                        // OFF. The host-driven SIGTERM cascade is
                        // a secondary-side concern (SLURM
                        // time-limit applies to allocations
                        // running secondary jobs; the primary
                        // typically runs on the operator host,
                        // not in a SLURM-allocated container).
                        // Primary shutdown is driven by the
                        // sentinel-file path, by orchestrator
                        // teardown, or by panik broadcast from a
                        // secondary that hit SIGTERM.
                        listen_for_sigterm: false,
                    },
                );
                if let Some(rx) = panik_watcher.take_signal_rx() {
                    primary.register_panik_signal_rx(rx);
                }

                // Initial staging is now driven by
                // `PrimaryCoordinator::run` itself: with
                // `PrimaryConfig.source_dir = Some(source_dir)`
                // threaded above, the manager's auto-stage gate
                // (`pending_stage_files.is_empty()` &&
                // `uses_file_based_items` && pre-staged-mode off
                // && source_dir is Some) walks `binaries ×
                // secondary_ids` once secondaries have welcomed
                // and queues the entries before initial
                // assignment. Removes the previous explicit pre-
                // call here in favour of a single source of truth
                // at the manager boundary; consistent with the
                // network-primary path, which also relies on the
                // auto-stage. The SLURM pipeline retains its
                // explicit `queue_initial_staging` because that
                // caller's source-root resolution depends on
                // `--source-already-staged` and other flags
                // unique to it; the gate detects the non-empty
                // queue and skips.

                // phase_deps + lifecycle closures captured from the
                // outer scope (5A built phase_deps; 5B built the
                // GIL-reacquiring on_phase_* closures).
                let result = primary
                    .run(rust_binaries, phase_deps, on_phase_start, on_phase_end)
                    .await;
                if let Err(e) = &result {
                    tracing::error!(error = %e, "primary failed");
                }
                match result {
                    Err(RunError::ClusterCollapsed { .. }) => {
                        cluster_collapsed = result.err();
                    }
                    Err(RunError::PanikShutdown {
                        matched_path,
                        reason: _,
                    }) => {
                        panik_shutdown_path = Some(matched_path);
                    }
                    Err(e @ RunError::SetupDeadlineExpired { .. }) => {
                        setup_deadline_expired = Some(e);
                    }
                    Err(RunError::Other(_)) | Ok(()) => {
                        // Legacy log-and-swallow for non-structured
                        // errors — see `PyPrimaryCoordinator::run`
                        // for the rationale.
                    }
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;
                stranded = primary.stranded_count() as u32;

                // Drop primary to close channels, allowing secondaries to exit
                drop(primary);

                // Wait for secondaries and clean up child processes
                for handle in sec_handles {
                    if let Ok((_, children)) = handle.await {
                        all_child_processes.extend(children);
                    }
                }

                // Tear down all aggregated worker subprocesses via the
                // shared SIGTERM → grace → SIGKILL primitive. See
                // `subprocess_factory::terminate_children` for the
                // rationale (podman SIGTERM handoff vs SIGKILL).
                crate::subprocess_factory::terminate_children(&mut all_child_processes);
            }));
        });

        self.completed = completed;
        self.failed = failed;
        self.stranded = stranded;

        if let Some(matched_path) = panik_shutdown_path {
            // GIL is back. Exit(137) — same shape as
            // `PyPrimaryCoordinator::run`. Skips the
            // cluster-collapsed path because a panik shutdown is a
            // strictly-stronger terminal (the operator declared the
            // whole cluster unwanted; partial accounting is
            // irrelevant). The secondaries spawned above have each
            // already run their own panik-react path (kill_all_workers_with_grace)
            // before joining; their workers' pgids are reaped before
            // we exit.
            tracing::error!(
                matched_path = %matched_path.display(),
                "panik shutdown: distributed manager exiting with code 137"
            );
            std::process::exit(137);
        }

        if let Some(err) = setup_deadline_expired {
            // Surface setup-promote deadline expiry — same shape as
            // `PyPrimaryCoordinator::run`. Sequenced after panik
            // (strictly stronger) and before cluster-collapsed
            // (deadline expiry means zero tasks dispatched, so
            // stranded accounting carries no useful operator pointer).
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = cluster_collapsed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        Ok(())
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }

    #[getter]
    fn failed(&self) -> u32 {
        self.failed
    }

    /// Tasks left without a recorded outcome at the end of the run
    /// (`total - completed - failed`). Mirrors `RustPrimaryCoordinator.stranded`.
    #[getter]
    fn stranded(&self) -> u32 {
        self.stranded
    }
}
