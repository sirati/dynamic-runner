//! `PyPrimaryCoordinator::run` — drives the network-primary
//! coordination pipeline on a detached tokio runtime. Also exposes
//! the `completed` / `failed` / `stranded` getters that Python
//! `run.py` reads after `run()` returns.

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::{PrimaryConfig, PrimaryCoordinator, RunError};

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
        let dist_mass_death_grace = self.distributed_config.mass_death_grace();
        let dist_mass_death_min_count = self.distributed_config.mass_death_min_count();
        let dist_setup_promote_deadline = self.distributed_config.setup_promote_deadline();
        let pending_stage_files = std::mem::take(&mut self.pending_stage_files);
        let source_pre_staged_root = self.source_pre_staged_root.clone();
        let source_dir = self.source_dir.clone();
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
        // Load-bearing flip for the setup-deferred run path. When
        // `--source-already-staged` is set on the submitter (so
        // `source_pre_staged_root.is_some()`) AND the Python pipeline
        // has not supplied any pre-discovered binaries (the pipeline
        // skips its own `task.discover_items` walk in pre-staged mode
        // and hands an empty list to `run()`), the submitter primary
        // owes no setup work. The bootstrap announcement it emits
        // carries `required_setup=true`, and the chosen secondary
        // runs discovery + ledger-seed on its bind-mounted
        // `src_network` instead. Either signal alone is insufficient:
        // an empty-binaries run with `source_pre_staged_root=None` is
        // a legitimate empty-corpus run, and a non-empty-binaries run
        // with the staged flag means the pipeline already discovered
        // and the local primary should seed normally.
        let required_setup_on_promote =
            source_pre_staged_root.is_some() && rust_binaries.is_empty();

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
        // Setup-promote deadline carried out of the detached tokio
        // runtime. `Some(RunError::SetupDeadlineExpired { .. })` iff
        // the inner `PrimaryCoordinator::run` exited via the demoted-
        // primary setup-deadline arm — the promoted secondary never
        // broadcast TaskAdded / TasksSpawned / RunComplete within
        // `setup_promote_deadline`. The GIL-side tail of this method
        // translates this into a `PyRuntimeError` carrying the
        // diagnostic Display so the consumer's Python wrapper raises
        // a clean exception instead of returning exit 0 with empty
        // counters.
        let mut setup_deadline_expired: Option<RunError> = None;
        // Pre-phase duplicate-task-id carried out of the detached tokio
        // runtime. `Some(RunError::DuplicateTaskIdPrePhase { .. })` iff
        // the inner `PrimaryCoordinator::run` aborted because the
        // INITIAL batch had a `(phase_id, task_id)` duplicate (#3a) —
        // the primary already broadcast `ClusterMutation::RunAborted`
        // so secondaries/observers exit non-zero; the GIL-side tail
        // translates this into a `PyRuntimeError` so the primary's
        // Python wrapper raises instead of returning exit 0.
        let mut duplicate_task_id_pre_phase: Option<RunError> = None;

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
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    source_pre_staged_root,
                    uses_file_based_items,
                    required_setup_on_promote,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    fleet_dead_timeout: std::time::Duration::from_secs(30),
                    mesh_ready_timeout: std::time::Duration::from_secs(60),
                    mass_death_grace: dist_mass_death_grace,
                    mass_death_min_count: dist_mass_death_min_count,
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
                    setup_promote_deadline: dist_setup_promote_deadline,
                };

                // The mesh listener (held in `_mesh_server_guard` above)
                // stays bound to the LocalSet's lifetime so its accept
                // loops keep feeding the transport; the coordinator now
                // holds the single `Tr` transport.
                let mut primary: PrimaryCoordinator<_, _, _, RunnerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        peer_transport,
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

                // phase_deps + lifecycle closures captured from the
                // outer scope (5A built phase_deps; 5B built the
                // GIL-reacquiring on_phase_* closures).
                let result = primary
                    .run(rust_binaries, phase_deps, on_phase_start, on_phase_end)
                    .await;
                if let Err(e) = &result {
                    tracing::error!(error = %e, "primary coordinator failed");
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
                    Err(e @ RunError::DuplicateTaskIdPrePhase { .. }) => {
                        duplicate_task_id_pre_phase = Some(e);
                    }
                    Err(RunError::Other(_)) | Ok(()) => {
                        // Legacy log-and-swallow behaviour for
                        // non-structured errors is preserved here:
                        // these surface through the per-counter
                        // accounting below (stranded count + the
                        // log line above), not as a PyErr.
                    }
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;
                stranded = primary.stranded_count() as u32;
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

        if let Some(err) = setup_deadline_expired {
            // GIL is back. Surface the structured deadline-expiry as
            // a `PyRuntimeError` carrying the Display of the
            // `SetupDeadlineExpired` variant (the diagnostic message
            // is composed in `error.rs::Display`). The consumer's
            // Python wrapper observes a non-zero exit instead of the
            // pre-fix silent hang.
            //
            // Sequenced after `panik_shutdown_path` (panik is a
            // strictly-stronger terminal) but BEFORE
            // `cluster_collapsed` because the setup-deadline path
            // exits with zero tasks dispatched — there's nothing for
            // the stranded-count accounting to surface. Surfacing
            // setup-deadline first keeps the operator's diagnostic
            // pointer at the actual cause ("discovery never started")
            // instead of letting the run trickle through to a
            // `ClusterCollapsed { stranded = 0 }` shape that's
            // technically correct but operationally misleading.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = duplicate_task_id_pre_phase {
            // GIL is back. The primary aborted the run before any phase
            // started because the initial batch had a duplicate
            // `(phase_id, task_id)`. It already broadcast `RunAborted`
            // (secondaries/observers exit 1); surface the structured
            // Display as a `PyRuntimeError` so the primary's Python
            // wrapper raises a clean exception instead of returning
            // exit 0. Sequenced alongside `setup_deadline_expired` (both
            // are structured pre-dispatch terminals with no per-task
            // breakdown) and BEFORE `cluster_collapsed`.
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
