//! `PyPrimaryCoordinator::run` — drives the network-primary
//! coordination pipeline on a detached tokio runtime. Also exposes
//! the `completed` / `failed` / `stranded` getters that Python
//! `run.py` reads after `run()` returns.

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::process::{
    LocalRole, Mesh, Node, NodeRunInputs, PrimaryRunArgs, RunTerminal, SeedSource,
};
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, RelocationPolicy, RunError,
};
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;
use crate::pytypes::extract_binaries;

use super::PyPrimaryCoordinator;

#[pymethods]
impl PyPrimaryCoordinator {
    /// Run the primary coordination pipeline over real network connections.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let estimator = self.estimator.clone();
        let phase_deps = self.phase_deps.clone();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_keepalive_miss_threshold = self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_oom_retry_max_passes = self.distributed_config.oom_retry_max_passes();
        let pending_stage_files = std::mem::take(&mut self.pending_stage_files);
        let source_pre_staged_root = self.source_pre_staged_root.clone();
        // Mode-2 pre-staged signal: when the operator passed
        // `--source-already-staged`, the corpus is already on the cluster and
        // the submitter has NO local view of it, so it cannot cold-seed tasks.
        // It instead originates the relocated seed (phase graph +
        // `DiscoveryDebt=Owed`, no tasks) and relocates; the promoted
        // compute-peer primary (built by the secondary's promotion recipe,
        // which registers the discovery policy) runs `discover_on_promotion`
        // on the `Owed` marker and seeds the tasks itself. Captured as a bool
        // here because `source_pre_staged_root` moves into `PrimaryConfig`
        // inside the detached-runtime closure before the seed is built.
        let source_pre_staged = source_pre_staged_root.is_some();
        let source_dir = self.source_dir.clone();
        // The node-local run-config (the operator's `args.forwarded_argv`),
        // captured on the GIL thread for the detached-runtime `PrimaryConfig`
        // so this primary answers `RequestRunConfig` from its own copy.
        let forwarded_argv = self.forwarded_argv.clone();
        // Take the parked deployment-mode job-manager handle (if any)
        // out for the detached runtime so the inner coordinator can
        // park it on itself before `run()` enters. `None` for the
        // in-process / network-primary paths that never wire it.
        let slurm_job_manager = self.slurm_job_manager.take();

        // Materialise the respawn pipeline wiring. Only the
        // (budget, spawner) pair is meaningful — when either is
        // absent the inner coordinator's respawn pipeline stays
        // unwired (CCD-5). Extract the `Arc<dyn SecondarySpawner>`
        // here while we still hold the GIL: the inner adapters
        // (today only `PyMultiProcessSpawner`; SLURM lands on the
        // same `as_arc` boundary later) expose a Rust-only
        // `as_arc()` method that clones their internal `Arc`.
        let respawn_budget = self.respawn_policy.to_budget();
        let respawn_spawner_arc: Option<
            std::sync::Arc<dyn dynrunner_manager_distributed::primary::respawn::SecondarySpawner>,
        > = match (&self.respawn_spawner, &respawn_budget) {
            (Some(spawner_py), Some(_)) => {
                let bound = spawner_py.bind(py);
                if let Ok(mp) =
                    bound.cast::<crate::managers::multi_process_respawner::PyMultiProcessSpawner>()
                {
                    Some(mp.borrow().as_arc())
                } else if let Ok(slurm) =
                    bound.cast::<crate::slurm::respawn_bridge::PySlurmSpawner>()
                {
                    Some(slurm.borrow().as_arc())
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "respawn_spawner must be a recognised secondary-spawner \
                         pyclass (PyMultiProcessSpawner or PySlurmSpawner); \
                         got an unrecognised type",
                    ));
                }
            }
            // Budget present without spawner (or vice-versa) — the
            // CLI ensures both are supplied together when policy ≠
            // disabled, so this branch is the misconfiguration arm.
            (None, Some(_)) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "respawn_policy is on-secondary-death but no \
                     respawn_spawner was supplied",
                ));
            }
            (_, None) => None,
        };
        // The primary's listen endpoint + pubkey are bound inside the
        // detached tokio runtime (see the `py.detach(|| { … })` block
        // below). Snapshotting them here on the GIL thread is not
        // possible (the mesh bind is async and consumes the port); the
        // detached-runtime block reads them off the bundle the
        // transport factory returns and supplies them to
        // `primary.enable_respawn(...)` directly.
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        // Capture the scheduler-tuning snapshot here on the GIL thread so
        // the detached-runtime block can build the inner
        // `ResourceStealingScheduler` with the operator-supplied
        // OOM-preempt margin / pressure threshold rather than the bare
        // `ResourceStealingScheduler::memory()` default.
        let scheduler_config = self.scheduler_config.clone();
        // Panik-watcher config captured here so the `py.detach` closure
        // owns its own copy. Empty paths yields a no-op watcher and
        // the operational-loop arm parks on `pending().await`.
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval =
            std::time::Duration::from_secs_f64(self.panik_watcher_poll_interval_secs);
        // Snapshot the cap, flip `run_started`, and consume the
        // receiver for the detached runtime in one step. The helper
        // owns the single-shot guard and the snapshot ordering; the
        // sender clone returned in `wiring` keeps backing future
        // `handle()` calls.
        let wiring = self.control_plane.take_for_run()?;
        let unfulfillable_reinject_max_per_task = wiring.cap_snapshot;
        let command_tx = wiring.command_tx;
        let command_rx = wiring.command_rx;

        // Phase 5B: re-acquire the GIL from the coordinator's LocalSet
        // and dispatch to the Python TaskDefinition's `on_phase_*`
        // methods. Each closure owns its own ref-bumped `Py<PyAny>` so
        // the manager owns the lifetime independent of `self`.
        let on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(self.task_definition.clone_ref(py)),
        );
        let on_phase_end: crate::managers::lifecycle::OnPhaseEnd = Box::new(
            crate::managers::lifecycle::make_on_phase_end(self.task_definition.clone_ref(py)),
        );

        // Take the Python peer-lifecycle listener (if any) out of
        // `self` so it can move into the detached tokio runtime.
        // Wrapped through `PyPeerLifecycleListener::new` into a
        // `Box<dyn LifecycleListener>` at the boundary so the
        // manager-distributed registration API stays
        // PyO3-agnostic.
        let peer_lifecycle_listener = self
            .peer_lifecycle_listener
            .take()
            .map(crate::peer_lifecycle_bridge::PyPeerLifecycleListener::new);

        // Same shape as the peer-lifecycle listener: take the Python
        // callable out of `self`, wrap it as a `Box<dyn
        // TaskCompletedListener>` at the bridge boundary, and register
        // it on the inner coordinator BEFORE `run()` enters. The
        // dispatcher takes ownership at run-start (see
        // `PrimaryCoordinator::run_pipeline` -> `mem::take` on
        // `task_completed_listeners`), so registration must happen
        // pre-run.
        let task_completed_listener = self
            .task_completed_listener
            .take()
            .map(crate::task_completed_bridge::PyTaskCompletedListener::new);

        // Same shape for the fulfillability matcher kwarg: take the
        // Python callable out of `self`, wrap it as a
        // `Box<dyn FulfillabilityMatcher<RunnerIdentifier>>` at the
        // bridge boundary, and install it on the inner coordinator
        // BEFORE `run()` enters.
        let fulfillability_matcher = self
            .fulfillability_matcher
            .take()
            .map(crate::fulfillability_matcher_bridge::PyFulfillabilityMatcher::new);

        // Resolve the bind port. When the caller (e.g. the SLURM
        // packaging pipeline) pre-supplied `listen_port`, honour it
        // exactly — that path has already wired an SSH `-R` forward
        // to this number and any divergence makes secondaries dial a
        // port the primary isn't listening on (sshd accepts the relay
        // bind, then RSTs the relay because nothing's behind it on
        // our side). When unset, fall back to the legacy temp-bind +
        // drop + re-bind dance for callers that don't tunnel.
        let port = match self.listen_port {
            Some(p) => p,
            None => {
                let tmp_listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| {
                    pyo3::exceptions::PyOSError::new_err(format!("failed to bind: {e}"))
                })?;
                let p = tmp_listener.local_addr().unwrap().port();
                drop(tmp_listener);
                p
            }
        };

        let primary_url = format!("tcp://127.0.0.1:{}", port);

        // Call the Python spawn_secondary callback for each secondary.
        // Contract (post-refactor): the callback receives
        // `(primary_url, secondary_id, quic_port)` and returns either
        // `None` (SLURM mode: the wrapper script already spawned the
        // secondary; Rust owns no child here) or a `SubprocessSpec`
        // carrying argv (+ optional env). Rust then spawns the
        // `std::process::Child` itself and OWNS its lifetime — kill +
        // wait at end of `run()` run against the Rust-side `Child`
        // handles, never re-entering Python for subprocess control.
        // This is the Rust-side enforcement of
        // `feedback_features_in_rust_python_is_bridge`: Python
        // assembles argv (legitimate CLI/config concern), Rust owns the
        // resulting process tree.
        let mut child_processes: Vec<std::process::Child> = Vec::new();
        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let spec_obj = self
                .spawn_secondary
                .call1(py, (&primary_url, &secondary_id, 0u16))?;
            // `None` → SLURM no-op path (`_slurm_already_spawned`).
            // Anything else MUST be a `SubprocessSpec`-shaped object
            // (the Python `dynamic_runner.SubprocessSpec` dataclass);
            // we refuse the legacy `subprocess.Popen` shape loudly
            // rather than fall back to Python-side ownership.
            if spec_obj.is_none(py) {
                tracing::info!(
                    secondary_id = %secondary_id,
                    "spawn_secondary returned None; assuming external spawn (SLURM-style)"
                );
                continue;
            }
            let spec =
                crate::managers::subprocess_spec::SubprocessSpec::from_pyany(spec_obj.bind(py))?;
            let child = spec.spawn().map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "failed to spawn secondary {secondary_id}: {e}"
                ))
            })?;
            tracing::info!(
                secondary_id = %secondary_id,
                pid = child.id(),
                "spawned secondary process (Rust-owned Child)"
            );
            child_processes.push(child);
        }

        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut stranded = 0u32;
        // Cluster-collapsed signal carried out of the detached tokio
        // runtime. `Some(...)` iff the inner `PrimaryCoordinator::run`
        // returned `RunError::ClusterCollapsed { .. }`; the GIL-side
        // tail of this method translates it into a `PyRuntimeError`
        // so the Python caller's exit code reflects the cluster
        // collapse instead of the historical silent exit-0. Other
        // `RunError::Other(...)` failures keep the legacy log-and-
        // swallow behaviour to minimise the blast radius of this
        // accounting-only patch.
        let mut cluster_collapsed: Option<RunError> = None;
        // Panik outcome carried out of the detached tokio runtime.
        // `Some(matched_path)` iff the inner `PrimaryCoordinator::run`
        // returned `RunError::PanikShutdown { .. }`. The GIL-side tail
        // of this method calls `std::process::exit(137)` so the SLURM
        // wrapper reaps the container.
        let mut panik_shutdown_path: Option<std::path::PathBuf> = None;
        // Pre-phase duplicate-task-id carried out of the detached tokio
        // runtime. `Some(RunError::DuplicateTaskIdPrePhase { .. })` iff
        // the inner `PrimaryCoordinator::run` aborted because the
        // INITIAL batch had a `(phase_id, task_id)` duplicate (#3a) —
        // the primary already broadcast `ClusterMutation::RunAborted`
        // so secondaries/observers exit non-zero; the GIL-side tail
        // translates this into a `PyRuntimeError` so the primary's
        // Python wrapper raises instead of returning exit 0.
        let mut duplicate_task_id_pre_phase: Option<RunError> = None;

        // Policy-abort terminal carried out of the detached tokio runtime.
        // `Some(RunError::FatalPolicyExit)` iff the node's terminal was a
        // deliberate policy abort (a relocated-observer invalid-task Policy-B
        // fatal-exit, or a panicked role task). Unlike a stay-local
        // `RunError::Other` (legacy log-and-swallow ⇒ exit 0), a policy abort
        // MUST surface non-zero — so the GIL-side tail raises a
        // `PyRuntimeError`. (A relocated-observer STRAND — fleet-dead /
        // primary-silence — is a structured `ClusterCollapsed` now and rides
        // the `cluster_collapsed` marker instead.)
        let mut fatal_policy_exit: Option<RunError> = None;
        // Spawn-rejected terminal carried out of the detached tokio runtime.
        // `Some(RunError::SpawnRejected { .. })` iff a runtime `spawn_tasks`
        // batch was wholesale-rejected so the phase dispatched ZERO tasks.
        // The GIL-side tail raises a `PyRuntimeError` so the submitter's
        // wrapper sees a non-zero exit instead of the silent rc=0 that
        // masked the dropped planned work.
        let mut spawn_rejected: Option<RunError> = None;
        // No-relocation-target config error carried out of the detached tokio
        // runtime. `Some(RunError::NoRelocationTarget)` iff this
        // `RelocateToComputePeer` submitter found NO eligible compute peer to
        // promote (pillar 2: the submitter must never stay the run's
        // primary). The GIL-side tail raises a `PyRuntimeError` so the
        // operator sees a clear non-zero exit naming the unsupported topology,
        // never the `Other` swallow.
        let mut no_relocation_target: Option<RunError> = None;
        // Relocated-observer cluster-abort carried out of the detached tokio
        // runtime. `Some(reason)` iff the submitter relocated and the
        // observer tail observed a cluster-wide `RunAborted`
        // (`RunTerminal::Aborted` — a #3a pre-phase duplicate). The GIL-side
        // tail exits 1 (a clean non-zero, like the secondary's aborted arm),
        // distinct from a policy abort's `PyRuntimeError`.
        let mut relocated_aborted: Option<String> = None;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Stand up the submitter primary's mesh-join transport
                // through the backend-opaque factory: it builds the mesh
                // transport, binds the listener wiring its accept loops to
                // the transport's inbound + registration sinks, and reads
                // the respawn trust anchors (endpoint + cert PEM) off the
                // bound listener. No backend type is named here.
                //
                // The trust anchors are surfaced to each respawned
                // secondary via `SecondarySpawnSpec` (per-spawn, so a
                // future cert rotation reaches downstream respawns). In
                // SLURM mode the secondary dials `localhost:<gateway_port>`
                // via the per-secondary reverse tunnel — the gateway-facing
                // port differs but the same PEM authenticates either path.
                // In multi-process local mode the submitter and the spawned
                // subprocess share the same loopback, so this endpoint is
                // already the dial-able address.
                let mesh_bundle =
                    match transport_factory::bind_primary_mesh::<RunnerIdentifier>(port).await {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to start primary mesh transport");
                            return;
                        }
                    };
                tracing::info!(port, "primary network server listening");

                let peer_transport = mesh_bundle.transport;
                let respawn_primary_endpoint = mesh_bundle.respawn_endpoint;
                let respawn_primary_pubkey_pem = mesh_bundle.respawn_pubkey_pem;
                // The factory's listener handle is kept alive for the
                // `LocalSet`'s lifetime so its accept loops stay up;
                // secondaries retry-connect on their own and connections
                // that arrive after we hand control to the coordinator are
                // accepted by those loops.
                let _mesh_server_guard = mesh_bundle.listener_guard;

                // Run the primary coordinator with the network server transport.
                let config = PrimaryConfig {
                    node_id: dynrunner_core::SETUP_NODE_ID.into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    source_pre_staged_root,
                    uses_file_based_items,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    fleet_dead_timeout: std::time::Duration::from_secs(30),
                    mesh_ready_timeout: std::time::Duration::from_secs(60),
                    // Threaded from the constructor's `source_dir`
                    // kwarg so the inner coordinator owns a local
                    // root for the initial staging walk's
                    // content-hash + per-secondary fan-out. SLURM
                    // and network-primary callers both supply it;
                    // `None` is acceptable for callers that don't
                    // read source files from the primary's
                    // filesystem (pre-staged-source mode,
                    // `uses_file_based_items=false`, or future
                    // remote-only primaries).
                    source_dir,
                    unfulfillable_reinject_max_per_task,
                    // The submitter's node-local run-config (the operator's
                    // `args.forwarded_argv`). Answered verbatim on
                    // `RequestRunConfig` so every joining / promoted node
                    // sources a byte-identical copy from the submitter.
                    forwarded_argv,
                    // Staged silence schedule: keepalive-interval-relative
                    // defaults (not surfaced on the Python config today).
                    ..PrimaryConfig::default()
                };

                // Wrap the opaque mesh transport in the role-demux `Mesh`
                // (the one thing in this process that touches the wire) and
                // register the bootstrap Primary slot, minting the
                // coordinator's `(client, inbox)` ends + the `Arc<RoleSlot>`
                // the `Node` holds as the teardown lever. The mesh listener
                // (held in `_mesh_server_guard` above) stays bound to the
                // LocalSet so its accept loops keep feeding the transport.
                let mut mesh = Mesh::new(peer_transport);
                let (pri_slot, pri_client, pri_inbox) = mesh.register_local_role(
                    LocalRole::Primary,
                    PeerId::from(dynrunner_core::SETUP_NODE_ID),
                );

                // BUG-6 demote channel: the bootstrap primary relocates into
                // a standalone observer on any self→other primary-register
                // flip. The RECEIVER goes to `PrimaryCoordinator::new`; the
                // SENDER goes to `NodeRunInputs.primary_demote_tx`, where
                // `Node::run` installs it on the primary's role-change hook
                // (`register_demote_on_displaced`).
                let (demote_tx, demote_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

                // The coordinator never names a transport: the `Mesh` (owned
                // by the `Node`'s pump) holds it; the coordinator reaches the
                // wire only through `client` (egress) + `inbox` (ingress).
                let mut primary: PrimaryCoordinator<_, _, RunnerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        pri_client,
                        pri_inbox,
                        demote_rx,
                        // Pillar 2: the SLURM/network submitter must NEVER
                        // stay the run's primary — it relocates the role to a
                        // compute peer at the bootstrap tail.
                        RelocationPolicy::RelocateToComputePeer,
                        scheduler_config.build_memory_scheduler(),
                        estimator,
                    );

                // Swap in the Python-facing command channel so the
                // `PrimaryHandle` Python is holding talks to the same
                // receiver the operational loop reads from.
                primary.replace_command_channel(command_tx, command_rx);

                // Relay the SLURM-pipeline-parked deployment-mode job
                // manager onto the inner coordinator BEFORE `run()`
                // enters. Same pre-run contract as the other registration
                // setters on `PrimaryCoordinator`: the respawn path
                // reads the field from inside the operational loop, so
                // late installs would be invisible.
                if let Some(jm) = slurm_job_manager {
                    primary.set_slurm_job_manager(jm);
                }

                // Wire the respawn pipeline iff both a budget and a
                // spawner are present. The early-return branch above
                // guarantees they're either both `Some` or both
                // `None`, so a single match-on-Some covers the
                // enabled arm without re-validation. CCD-5: this is
                // the ONLY call site that touches the respawn
                // wiring; no downstream `if policy_enabled` checks
                // live on the hot path.
                if let (Some(spawner), Some(budget)) = (respawn_spawner_arc, respawn_budget) {
                    primary.enable_respawn(
                        spawner,
                        budget,
                        respawn_primary_endpoint,
                        respawn_primary_pubkey_pem,
                    );
                }

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run()` enters — the coordinator's
                // `register_lifecycle_listener` contract requires
                // pre-run registration because `run()` `mem::take`-s
                // the listener vector into the spawned dispatcher.
                if let Some(listener) = peer_lifecycle_listener {
                    primary.register_lifecycle_listener(listener);
                }

                // Same shape for the task-completion listener: an
                // independent dispatcher pair with the same pre-run
                // registration requirement.
                if let Some(listener) = task_completed_listener {
                    primary.register_task_completed_listener(listener);
                }

                // Same boundary as the lifecycle listener: install
                // the Python fulfillability matcher (if any) BEFORE
                // `run()` enters. The setter is one line; the
                // matcher invocation cadence + state-filter +
                // ReinjectTask fire all live behind the
                // manager-distributed API.
                if let Some(matcher) = fulfillability_matcher {
                    primary.set_fulfillability_matcher(matcher);
                }

                // Spawn the panik watcher and hand its signal
                // receiver to the inner coordinator. Empty
                // `panik_watcher_paths` yields a never-firing
                // receiver (no-op task), so callers that don't pass
                // any `--panik-file` flags pay zero runtime cost.
                // Held in scope so its `Drop::abort()` runs at loop
                // exit and cleans up the polling task on every
                // termination path.
                let mut panik_watcher =
                    dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                        dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                            paths: panik_watcher_paths,
                            poll_interval: panik_watcher_poll_interval,
                            // PRIMARY-role spawner: SIGTERM listening
                            // OFF. SLURM-driven SIGTERM is forwarded
                            // by the host shutdown-manager only into
                            // secondary containers — primaries are
                            // outside that path. Sentinel-file panik
                            // and cluster-wide panik broadcast remain
                            // the primary's emergency-stop sources.
                            listen_for_sigterm: false,
                        },
                    );
                if let Some(rx) = panik_watcher.take_signal_rx() {
                    primary.register_panik_signal_rx(rx);
                }

                for (sec_id, file_hash, content_hash, src, dest) in pending_stage_files {
                    primary.queue_stage_file(sec_id, file_hash, content_hash, src, dest);
                }

                // Compose the bootstrap-submitter `Node`: a pure-primary node
                // (no co-located secondary, no promotion recipe — the
                // submitter is the seed authority and can only relocate INTO
                // an observer, never promote). `Node::run` owns the
                // coordinator + the mesh-pump + the lifecycle; the boundary
                // composes the inputs and drives it to a single outcome.
                let (node, _node_promo_tx) = Node::new(mesh);
                let node = node.with_primary(primary, pri_slot);
                // Construct the typed seed at the boundary from the pre-staged
                // signal (the construction-site decision pillar 2 mandates —
                // NOT a runtime flag-if inside the coordinator). Pre-staged
                // (mode-2): the corpus is already on the cluster, so originate
                // ONLY the phase graph + `DiscoveryDebt=Owed` and let the
                // promoted compute-peer primary discover the tasks. Cold
                // (mode-1): the submitter discovered the corpus locally and
                // seeds it directly.
                let seed = if source_pre_staged {
                    SeedSource::RelocatedSeed { phase_deps }
                } else {
                    SeedSource::ColdStart {
                        binaries: rust_binaries,
                        phase_deps,
                    }
                };
                let inputs: NodeRunInputs<
                    crate::subprocess_factory::SubprocessWorkerFactory,
                    _,
                    _,
                    RunnerIdentifier,
                > = NodeRunInputs {
                    primary_run_args: Some(PrimaryRunArgs {
                        seed,
                        on_phase_start,
                        on_phase_end,
                    }),
                    // BUG-6: the SENDER half of the primary's demote channel
                    // (its RECEIVER is already inside the coordinator). The
                    // node installs it on the role-change hook so a self→other
                    // primary flip relocates the submitter into the observer
                    // tail (the `Relocated` swap is consumed INSIDE `Node`).
                    primary_demote_tx: Some(demote_tx),
                    // No co-located secondary, so no worker factory; no
                    // promotion recipe (the submitter is the seed authority).
                    secondary_factory: None,
                    promote: None,
                };

                // Drive the node to its single outcome. The submitter either
                // ran the primary to completion in-place OR relocated into a
                // standalone observer tail; either way `Node::run` resolves to
                // ONE role-agnostic `RunTerminal` (+ counts). The boundary
                // maps that terminal to the GIL-side exit markers uniformly —
                // a relocated observer's Aborted/Panik/strand and a stay-local
                // primary's structured/generic error funnel through the same
                // four-way terminal.
                let outcome = node.run(inputs).await;

                completed = outcome.completed as u32;
                failed = outcome.failed as u32;
                stranded = outcome.stranded as u32;

                match outcome.terminal {
                    RunTerminal::Done => {}
                    RunTerminal::Aborted { reason } => {
                        // Cluster-wide `RunAborted` (#3a pre-phase duplicate),
                        // observed by a relocated submitter-observer tail.
                        relocated_aborted = Some(reason);
                    }
                    RunTerminal::Panik { matched_path } => {
                        // Operator panik (stay-local primary OR relocated
                        // observer) — exit 137.
                        panik_shutdown_path = Some(matched_path);
                    }
                    RunTerminal::Failed { error } => {
                        tracing::error!(error = %error, "primary node run failed");
                        match error {
                            RunError::ClusterCollapsed { .. } => {
                                cluster_collapsed = Some(error);
                            }
                            RunError::PanikShutdown { matched_path, .. } => {
                                panik_shutdown_path = Some(matched_path);
                            }
                            e @ RunError::DuplicateTaskIdPrePhase { .. } => {
                                duplicate_task_id_pre_phase = Some(e);
                            }
                            e @ RunError::FatalPolicyExit { .. } => {
                                // A policy abort (e.g. a relocated-observer
                                // invalid-task fatal-exit). RAISE — never the
                                // `Other` swallow.
                                fatal_policy_exit = Some(e);
                            }
                            e @ RunError::SpawnRejected { .. } => {
                                // A runtime spawn_tasks batch was wholesale-
                                // rejected → the phase dispatched ZERO tasks.
                                // RAISE — never the `Other` swallow: a silent
                                // zero-dispatch is exactly the rc=0 mask this
                                // variant exists to break.
                                spawn_rejected = Some(e);
                            }
                            e @ RunError::NoRelocationTarget => {
                                // The RelocateToComputePeer submitter found no
                                // eligible compute peer to promote. RAISE — the
                                // submitter must NEVER stay primary (pillar 2);
                                // a silent stay-local is exactly what this
                                // errors out instead of.
                                no_relocation_target = Some(e);
                            }
                            RunError::Other(_) => {
                                // The PRESERVED stay-local-primary swallow
                                // (exit 0): a genuinely-unexpected generic
                                // failure surfaces via the stranded-count
                                // accounting + the log line above, not a PyErr.
                                // Every KNOWN must-raise condition is a
                                // structured variant above (strand →
                                // ClusterCollapsed, policy abort →
                                // FatalPolicyExit), so reaching `Other` is
                                // exactly the old blast-radius-minimization
                                // case. (The relocated-observer strand is a
                                // structured ClusterCollapsed now, so it is
                                // NOT swallowed here — it raises above.)
                            }
                        }
                    }
                }
            }));
        });

        // Back with the GIL — terminate secondary processes through
        // the Rust-owned `Child` handles. No re-entry into Python for
        // subprocess control: lifecycle is fully Rust-side after the
        // initial `SubprocessSpec` handoff.
        for mut child in child_processes {
            let pid = child.id();
            if let Err(e) = child.kill() {
                tracing::debug!(pid, error = %e, "child.kill() failed (already exited?)");
            }
            if let Err(e) = child.wait() {
                tracing::debug!(pid, error = %e, "child.wait() failed");
            }
        }

        self.completed = completed;
        self.failed = failed;
        self.stranded = stranded;

        if let Some(matched_path) = panik_shutdown_path {
            // GIL is back. Log + exit(137) so the SLURM wrapper
            // sees the container-stop signal and reaps. No Python
            // stack unwinds — `exit` runs atexit handlers then
            // `_exit`. This path supersedes the cluster-collapsed
            // translation below because a panik shutdown is a
            // strictly-stronger terminal: the operator already
            // declared the entire cluster unwanted, so the partial
            // accounting that drives `ClusterCollapsed` is
            // irrelevant.
            tracing::error!(
                matched_path = %matched_path.display(),
                "panik shutdown: primary exiting with code 137"
            );
            std::process::exit(137);
        }

        if let Some(err) = duplicate_task_id_pre_phase {
            // GIL is back. The primary aborted the run before any phase
            // started because the initial batch had a duplicate
            // `(phase_id, task_id)`. It already broadcast `RunAborted`
            // (secondaries/observers exit 1); surface the structured
            // Display as a `PyRuntimeError` so the primary's Python
            // wrapper raises a clean exception instead of returning
            // exit 0. A structured pre-dispatch terminal with no per-task
            // breakdown, sequenced BEFORE `cluster_collapsed`.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(reason) = relocated_aborted {
            // GIL is back. The submitter relocated full authority and the
            // standalone observer tail observed a cluster-wide `RunAborted`
            // (#3a pre-phase duplicate). Exit 1 — the same clean non-zero a
            // secondary/observer takes on a cluster abort. Sequenced after
            // panik (strictly stronger) and before the strand/collapse
            // accounting (an abort is a definitive cluster terminal).
            tracing::error!(
                reason = %reason,
                "run aborted cluster-wide: relocated submitter-observer exiting with code 1"
            );
            std::process::exit(1);
        }

        if let Some(err) = fatal_policy_exit {
            // GIL is back. A deliberate policy abort (a relocated-observer
            // invalid-task Policy-B fatal-exit, or a panicked role task). It
            // MUST surface non-zero — raise the structured `Err`'s Display as a
            // `PyRuntimeError`. (A relocated-observer STRAND rides
            // `cluster_collapsed` below instead.)
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = spawn_rejected {
            // GIL is back. A runtime spawn_tasks batch was wholesale-rejected
            // → the phase dispatched ZERO tasks. RAISE so the submitter's
            // wrapper sees a non-zero exit instead of the silent rc=0 that
            // masked the dropped planned work. Sequenced alongside the other
            // structured raises and before `cluster_collapsed` (no strand to
            // render — the work never entered the ledger).
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = no_relocation_target {
            // GIL is back. The RelocateToComputePeer submitter had no eligible
            // compute peer to promote (pillar 2). RAISE the structured Display
            // so the operator sees the unsupported-topology message, never the
            // silent rc=0 `Other` swallow.
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
    /// (`total - completed - failed`). Zero on a clean run; `>0` is
    /// the cluster-collapse path the underlying `RunError::ClusterCollapsed`
    /// reports — Python `run.py` reads this getter (alongside
    /// `completed` / `failed`) to log the per-category breakdown
    /// before the `RuntimeError` from `run()` propagates and surfaces
    /// the non-zero exit.
    #[getter]
    fn stranded(&self) -> u32 {
        self.stranded
    }
}
