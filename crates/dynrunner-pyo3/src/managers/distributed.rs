use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{PhaseId, ResourceKind, ResourceMap, TypeId};
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, RunError, SecondaryConfig, SecondaryCoordinator,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd};

use crate::identifier::RunnerIdentifier;
use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{LoadedTaskDefinition, TypeRegistry};

#[pyclass(name = "RustDistributedManager")]
pub(crate) struct PyDistributedManager {
    python_executable: PathBuf,
    num_secondaries: u32,
    num_workers_per_secondary: u32,
    max_resources_per_secondary: ResourceMap,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    distributed_config: DistributedConfig,
    types: TypeRegistry,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    skip_existing: bool,
    uses_file_based_items: bool,
    max_concurrent_per_type: HashMap<TypeId, u32>,
    estimator: PyMemoryEstimatorBridge,
    completed: u32,
    failed: u32,
    /// Tasks that exited the inner run loop without a recorded
    /// outcome (`total - completed - failed`). Mirrors the same
    /// counter on `PyPrimaryCoordinator`; surfaced via the `stranded`
    /// PyO3 getter so the Python in-process distributed entrypoint
    /// can include it in the result dict.
    stranded: u32,
    /// Pre-staged-source mode (`--source-already-staged`) signal.
    /// Mirrors `PyPrimaryCoordinator.source_pre_staged_root`: when
    /// `Some`, the submitter has no local view of the corpus and
    /// the `_dispatch_single_process` helper has handed us an empty
    /// `binaries` list on purpose. The primary's bootstrap
    /// `PromotePrimary` then carries `required_setup=true` so the
    /// chosen secondary runs discovery + ledger-seed on its bind-
    /// mounted source root. Threaded through to `PrimaryConfig`
    /// uniformly with the SLURM / network-primary paths so
    /// `--source-already-staged` works in every multi-computer mode
    /// without per-caller special casing.
    source_pre_staged_root: Option<PathBuf>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B). The
    /// distributed in-process pipeline drives a primary; secondaries
    /// don't fire user-visible phase hooks.
    task_definition: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the in-process primary at `run()` start. The
    /// in-process secondaries do NOT get the listener — the manager
    /// pyclass represents one cluster's worth of events, and the
    /// primary's `cluster_state` apply path is the canonical
    /// emitter (the per-secondary mirrors fire the same events from
    /// their own apply paths; routing them all to the same listener
    /// would deliver N+1 copies of each peer membership change).
    /// Constructor-only — see the matching field on
    /// `PyPrimaryCoordinator` for the rationale.
    peer_lifecycle_listener: Option<Py<PyAny>>,

    /// Optional Python task-completion listener supplied at
    /// `__init__`. Same shape + single-source-of-truth rationale as
    /// `peer_lifecycle_listener`: registered on the in-process
    /// primary only; per-secondary mirrors fire the same events from
    /// their own apply paths but routing all to the same listener
    /// would deliver N+1 copies of each terminal task transition.
    task_completed_listener: Option<Py<PyAny>>,

    /// Rust-side bundle of the command channel + reinject-cap cell
    /// shared with every `PyPrimaryHandle` minted from this manager.
    /// Single concern split out into
    /// `crate::managers::control_plane` so the init/handle/run-take
    /// sequence is owned in one place rather than re-implemented on
    /// each primary-hosting manager. See that module's doc for the
    /// lifecycle contract.
    control_plane: crate::managers::control_plane::PrimaryControlPlane,
}

#[pymethods]
impl PyDistributedManager {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        num_workers_per_secondary,
        ram_per_secondary,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        log_paths = None,
        worker_spec = None,
        distributed_config = None,
        max_resources_per_secondary = None,
        source_pre_staged_root = None,
        peer_lifecycle_listener = None,
        task_completed_listener = None,
        unfulfillable_reinject_max_per_task = None,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        num_workers_per_secondary: u32,
        ram_per_secondary: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        distributed_config: Option<DistributedConfig>,
        max_resources_per_secondary: Option<PyResourceMap>,
        source_pre_staged_root: Option<PathBuf>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        task_completed_listener: Option<Py<PyAny>>,
        unfulfillable_reinject_max_per_task: Option<u32>,
    ) -> PyResult<Self> {
        let task = LoadedTaskDefinition::from_python(
            py,
            task_definition,
            task_args,
            &source_dir,
            &output_dir,
            skip_existing,
            log_paths,
        )?;

        // Boundary normalization: typed `max_resources_per_secondary`
        // ResourceMap wins; fall back to a single-key memory map built
        // from the legacy scalar `ram_per_secondary` if no map given.
        let max_resources_per_secondary = max_resources_per_secondary
            .map(|m| m.to_rust())
            .unwrap_or_else(|| {
                ResourceMap::from([(ResourceKind::memory(), ram_per_secondary)])
            });

        // Build the command-channel + reinject-cap bundle. Same
        // helper as `PyPrimaryCoordinator`; see
        // `crate::managers::control_plane` for the lifecycle.
        let control_plane = crate::managers::control_plane::PrimaryControlPlane::new(
            unfulfillable_reinject_max_per_task,
        );

        Ok(Self {
            python_executable: task.python_executable,
            num_secondaries,
            num_workers_per_secondary,
            max_resources_per_secondary,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            types: task.types,
            phase_deps: task.phase_deps,
            skip_existing,
            uses_file_based_items: task.uses_file_based_items,
            max_concurrent_per_type: task.max_concurrent_per_type,
            estimator: task.estimator,
            completed: 0,
            failed: 0,
            stranded: 0,
            source_pre_staged_root,
            task_definition: task_definition.clone().unbind(),
            peer_lifecycle_listener,
            task_completed_listener,
            control_plane,
        })
    }

    /// PrimaryHandle factory for the in-process distributed primary.
    /// Symmetric with `PyPrimaryCoordinator::handle` — each call
    /// returns a freshly-built handle (with its own in-handle tokio
    /// runtime); the underlying `command_tx` and reinject-cap cell
    /// are cloned so multiple Python control planes / threads can
    /// share one manager. Callable BEFORE `run()` so the Python
    /// caller can hand the handle off to its `on_run_start` hook
    /// before the blocking `run()` enters the detached runtime.
    fn handle(&self) -> PyResult<crate::managers::primary_handle::PyPrimaryHandle> {
        self.control_plane.to_handle()
    }

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
        let log_paths = self.log_paths.clone();

        // Pre-compute per-secondary log directories under the GIL —
        // `resolve_log_dir` calls into Python's `datetime` module —
        // before detaching for the tokio runtime. Each secondary gets
        // its own `{timestamp}/{secondary_id}` subdirectory so the
        // default `worker_<id>.log` filename never collides across
        // secondaries on a shared mount, and `create_dir_all` errors
        // surface here at run start rather than as silent log loss.
        let mut sec_log_dirs: Vec<(String, PathBuf)> =
            Vec::with_capacity(num_secondaries as usize);
        for i in 0..num_secondaries {
            let sid = format!("sec-{i}");
            let dir = log_paths.resolve_log_dir(py, &output_dir, &sid)?;
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
        let dist_mass_death_grace = self.distributed_config.mass_death_grace();
        let dist_mass_death_min_count = self.distributed_config.mass_death_min_count();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        let worker_spec = self.worker_spec.clone();
        // TODO(phase-5a-followup): worker subprocesses currently use the
        // first type's worker_module + cmd_args; restart-on-type-shift
        // is not yet implemented. The factory will need a per-type
        // dispatch path that consults the full TypeRegistry.
        let first_type = self.types.first().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            )
        })?;
        let worker_module = first_type.worker_module.clone();
        let worker_cmd_args = first_type.cmd_args.clone();
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

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                use tokio::sync::mpsc as tokio_mpsc;

                let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                let mut sec_handles = Vec::new();
                let mut all_child_processes: Vec<Option<std::process::Child>> = Vec::new();

                // Step 5b: build the primary's peer-mesh view first
                // so the per-secondary forwarder below can tap inbound
                // messages into the peer queue. The
                // `shared_outgoing` handle receives the same sender
                // clones we put into the legacy `outgoing` HashMap,
                // so role-addressed sends through `peer_transport`
                // reach the same wire as legacy `transport.send_to`.
                // See `dynrunner_transport_tunnel` crate docs.
                let (peer_transport, shared_outgoing, inbound_tap) =
                    dynrunner_transport_tunnel::TunneledPeerTransport::<
                        RunnerIdentifier,
                    >::new("primary".into());

                for (secondary_id, sec_log) in sec_log_dirs.into_iter() {
                    // primary→secondary channel
                    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
                    // secondary→primary channel
                    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

                    // Register the per-secondary writer in BOTH the
                    // legacy `outgoing` HashMap (drives
                    // `transport.send_to(sec_id, ..)`) AND the
                    // tunneled peer view's shared writer table
                    // (drives `peer_transport.send_to_peer(sec_id, ..)`
                    // and `Address::Role(_)` dispatch after the
                    // role-cache resolves). Pre-Step-5b the legacy
                    // path was the only consumer; Step 5b makes the
                    // primary a real mesh member by adding the
                    // second registration.
                    shared_outgoing
                        .borrow_mut()
                        .insert(secondary_id.clone(), pri_to_sec_tx.clone());
                    outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

                    // Forward secondary→primary messages
                    let fwd_tx = incoming_tx.clone();
                    let fwd_tap = inbound_tap.clone();
                    tokio::task::spawn_local(async move {
                        // Explicit type annotation: with the tap
                        // fan-out and the legacy forwarder both
                        // calling `send(msg)`-shaped methods the
                        // inferrer can no longer disambiguate the
                        // single-channel path it used pre-tap.
                        // Both sides receive `DistributedMessage<RunnerIdentifier>`
                        // (the wire shape the primary speaks).
                        let mut rx: tokio_mpsc::UnboundedReceiver<
                            dynrunner_protocol_primary_secondary::DistributedMessage<
                                RunnerIdentifier,
                            >,
                        > = sec_to_pri_rx;
                        while let Some(msg) = rx.recv().await {
                            // Fan-out tap: clone each inbound
                            // message into the peer view's queue so
                            // `peer_transport.recv_peer()` can
                            // observe it. The legacy `fwd_tx` send
                            // below is the canonical inbound
                            // consumer; the peer queue is currently
                            // drainless (Step 5b doesn't add the
                            // demoted-primary read arm — Step 6
                            // does). On send failure of the tap
                            // we continue silently: a dropped tap
                            // means the peer view was torn down
                            // first, but the legacy inbound path
                            // must keep flowing.
                            let _ = fwd_tap.send(msg.clone());
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
                    let sec_worker_module = worker_module.clone();
                    let sec_worker_args = worker_cmd_args.clone();
                    let sec_estimator = estimator.clone();
                    let sec_max_resources = max_resources_per_secondary.clone();

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
                            primary_link_failure_threshold:
                                dist_primary_link_failure_threshold,
                            primary_link_failure_window:
                                dist_primary_link_failure_window,
                            setup_deadline: dist_setup_deadline,
                            is_observer: false,
                        };

                        let estimator = sec_estimator;

                        let mut factory = SubprocessWorkerFactory {
                            python_executable: sec_python,
                            source_dir: sec_source,
                            output_dir: sec_output,
                            log_dir: sec_log,
                            log_paths: sec_log_paths,
                            worker_module: sec_worker_module,
                            worker_cmd_args: sec_worker_args,
                            skip_existing,
                            connection_mode: ConnectionMode::Socketpair,
                            manual_start_worker: false,
                            worker_spec: sec_worker_spec.clone(),
                            child_processes: Vec::new(),
                        };

                        let mut secondary = SecondaryCoordinator::new(
                            config,
                            transport,
                            dynrunner_transport_quic::NoPeerTransport,
                            ResourceStealingScheduler::memory(),
                            estimator,
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
                drop(incoming_tx); // Only forwarding tasks hold senders now

                let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
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
                };

                let mut primary = PrimaryCoordinator::new(
                    config,
                    transport,
                    peer_transport,
                    ResourceStealingScheduler::memory(),
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
                if let Err(RunError::ClusterCollapsed { .. }) = &result {
                    cluster_collapsed = result.err();
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

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Pre-run `handle()` factory contract tests for the in-process
    //! distributed manager. Mirrors `PyPrimaryCoordinator::handle`'s
    //! shape — same single concern: can the Python caller fetch a
    //! `PrimaryHandle` BEFORE the blocking `run()` enters the
    //! detached tokio runtime?
    //!
    //! Tests require an embedded CPython interpreter (gated behind
    //! the `test-with-python` feature). Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python pydistributed_manager`
    //!
    //! Scope: limited to (1) the factory call surface and (2) the
    //! cap-cell seeding. End-to-end command dispatch is already
    //! exercised by the `primary_handle.rs` tests against a stub
    //! receiver; the channel-and-cell wiring on this manager carries
    //! the same `PyPrimaryHandle::from_sender` constructor, so the
    //! same dispatch contract holds transitively.
    use super::*;
    use pyo3::types::{PyAnyMethods, PyModule};

    /// Compile a tiny Python module that exports a `TaskDefinition`-
    /// shaped stub + a default `task_args` Namespace. The shape is
    /// the minimum `LoadedTaskDefinition::from_python` needs:
    ///   * `get_phases()` → one PhaseSpec with one TaskTypeSpec.
    ///   * `build_worker_command_args(...)` → `[]`.
    ///   * `estimate_memory_returns` attribute referenced by the
    ///     `estimator_attr` lookup → trivial callable.
    /// Centralised so each test phrases the stub once.
    fn build_task_definition_module(py: Python<'_>) -> Bound<'_, PyModule> {
        // Stubs are pure-Python `SimpleNamespace` instances to avoid
        // importing `dynamic_runner.task_protocol` (the test
        // interpreter doesn't have the wheel installed; the cdylib
        // under test isn't even on sys.path here). The `from_python`
        // extractors duck-type via `getattr`, so any object with the
        // right attribute names + types works.
        let source = r#"
from types import SimpleNamespace

def estimate_memory(item):
    return 1024 * 1024

_TYPE = SimpleNamespace(
    type_id="t",
    worker_module="stub_worker_module",
    estimator_attr="estimate_memory",
    timeout_seconds=None,
    reserved_memory_per_worker=0,
    max_concurrent=None,
)

_PHASE = SimpleNamespace(
    phase_id="p",
    depends_on=[],
    types=(_TYPE,),
)

class _StubTask:
    uses_file_based_items = False
    # `LoadedTopology::from_python` reads `task_definition.<estimator_attr>`
    # on the matching type; expose the callable as an attribute on
    # the stub directly.
    estimate_memory = staticmethod(estimate_memory)
    def get_phases(self):
        return (_PHASE,)
    def build_worker_command_args(self, type_id, args, source_dir, output_dir, skip_existing):
        return []

task = _StubTask()
task_args = SimpleNamespace()
"#;
        PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new("stub_task_def.py").unwrap().as_c_str(),
            std::ffi::CString::new("stub_task_def").unwrap().as_c_str(),
        )
        .expect("compile stub TaskDefinition module")
    }

    /// Construct a `PyDistributedManager` with the supplied
    /// `unfulfillable_reinject_max_per_task`. Returns the manager
    /// already wrapped in a `PyClass` cell so subsequent `.handle()`
    /// calls can flow through the PyO3 method-dispatch surface (the
    /// production call path).
    fn build_manager(py: Python<'_>, cap: Option<u32>) -> PyResult<Py<PyDistributedManager>> {
        let module = build_task_definition_module(py);
        let task = module.getattr("task")?;
        let task_args = module.getattr("task_args")?;
        // `&Bound<'_, PyAny>` for the `new` signature.
        let mgr = PyDistributedManager::new(
            py,
            /* num_secondaries */ 1,
            /* num_workers_per_secondary */ 1,
            /* ram_per_secondary */ 64 * 1024 * 1024,
            /* source_dir */ "/tmp/src".into(),
            /* output_dir */ "/tmp/out".into(),
            &task,
            &task_args,
            /* skip_existing */ false,
            /* log_paths */ None,
            /* worker_spec */ None,
            /* distributed_config */ None,
            /* max_resources_per_secondary */ None,
            /* source_pre_staged_root */ None,
            /* peer_lifecycle_listener */ None,
            /* task_completed_listener */ None,
            /* unfulfillable_reinject_max_per_task */ cap,
        )?;
        Py::new(py, mgr)
    }

    /// Test (1) from the brief: the factory produces a `PrimaryHandle`
    /// BEFORE `run()` is called.
    #[test]
    fn handle_returns_pyprimaryhandle_before_run() {
        Python::attach(|py| {
            let mgr = build_manager(py, None).expect("manager constructs");
            let handle_obj = mgr
                .bind(py)
                .call_method0("handle")
                .expect("handle() must succeed before run()");
            // Downcast to the concrete pyclass — proves the type
            // contract independent of any Python-side getattr name
            // collision.
            let _handle: pyo3::PyRef<'_, crate::managers::primary_handle::PyPrimaryHandle> =
                handle_obj
                    .downcast::<crate::managers::primary_handle::PyPrimaryHandle>()
                    .expect("handle() must return a PrimaryHandle pyclass")
                    .borrow();
        });
    }

    /// Test (3) variant from the brief: the reinject cap kwarg is
    /// seeded into the shared cell at `__init__`, so the handle
    /// produced by `handle()` carries the same value.
    #[test]
    fn handle_reinject_cap_seed_from_init_kwarg() {
        Python::attach(|py| {
            let mgr = build_manager(py, Some(7)).expect("manager constructs");
            // Read the cap through the manager's control-plane
            // helper — this is the same cell the produced handle
            // clones, so a match here proves the round-trip. The
            // crate-internal `cap_snapshot()` accessor exists so
            // tests don't reach through private fields.
            let snapshot = mgr.borrow(py).control_plane.cap_snapshot();
            assert_eq!(snapshot, Some(7), "cap kwarg must seed the cell");
            // Sanity: the factory still succeeds with the cap set.
            let _ = mgr
                .bind(py)
                .call_method0("handle")
                .expect("handle() must succeed with seeded cap");
        });
    }

    /// Test (2) from the brief: two `handle()` calls return distinct
    /// `PrimaryHandle` instances backed by the same underlying
    /// channel. We can't directly compare `mpsc::Sender`s, but
    /// `tokio::sync::mpsc::Sender::same_channel` exposes the
    /// equivalence we want; calling it on the cloned senders proves
    /// the factory does not mint a fresh channel per call.
    #[test]
    fn handle_clones_share_same_command_channel() {
        Python::attach(|py| {
            let mgr = build_manager(py, None).expect("manager constructs");
            let h1 = mgr.bind(py).call_method0("handle").expect("first handle");
            let h2 = mgr
                .bind(py)
                .call_method0("handle")
                .expect("second handle");
            // Both downcasts must succeed (factory returns the same
            // pyclass); after that, the manager's control-plane
            // helper exposes a `same_command_channel` accessor that
            // confirms each handle's sender shares the manager's
            // receiver. Same `Sender::same_channel` semantics as
            // pre-refactor, routed through the helper so tests don't
            // reach into the handle's `sender` field.
            let r1 = h1
                .downcast::<crate::managers::primary_handle::PyPrimaryHandle>()
                .unwrap();
            let r2 = h2
                .downcast::<crate::managers::primary_handle::PyPrimaryHandle>()
                .unwrap();
            let mgr_ref = mgr.borrow(py);
            assert!(
                mgr_ref.control_plane.same_command_channel(&r1.borrow().sender),
                "first handle must share the manager's command channel"
            );
            assert!(
                mgr_ref.control_plane.same_command_channel(&r2.borrow().sender),
                "second handle must share the manager's command channel"
            );
        });
    }
}

// ── Network-based primary coordinator (spawns real secondary processes) ──
