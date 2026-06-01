//! `PySecondaryCoordinator::run` — drives the coordination loop on a
//! dedicated tokio runtime, handling the setup-promote yield by
//! re-acquiring the GIL to call `task.discover_items` whenever the
//! Rust core observes `RunOutcome::SetupPending`. Also exposes the
//! `completed` getter.

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::{RunOutcome, SecondaryConfig, SecondaryCoordinator};
use dynrunner_transport_quic::NetworkClient;

use crate::config::connection::ConnectionMode;
use crate::identifier::RunnerIdentifier;
use crate::network::{detect_ipv4, detect_ipv6, gethostname};
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;

use super::PySecondaryCoordinator;

#[pymethods]
impl PySecondaryCoordinator {
    /// Connect to the primary and run the secondary coordination loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        let primary_url = self.primary_url.clone();
        let secondary_id = self.secondary_id.clone();
        let num_workers = self.num_workers;
        let max_resources = self.max_resources.clone();
        let estimator = self.estimator.clone();
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_dir = self.log_dir.clone();
        let log_paths = self.log_paths.clone();
        let worker_spec = self.worker_spec.clone();
        let scheduler_config = self.scheduler_config.clone();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_connect_retry_delay = self.distributed_config.connect_retry_delay();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_oom_retry_max_passes = self.distributed_config.oom_retry_max_passes();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        let dist_disable_peer_overlay = self.distributed_config.disable_peer_overlay();
        let dist_resource_check_interval = self.distributed_config.resource_check_interval();
        let dist_log_oom_watcher = self.distributed_config.log_oom_watcher();
        let cfg_mem_manager_reserved_bytes = self.mem_manager_reserved_bytes;
        // Resolve the memprofile output directory at run-start.
        // The three-input shape (`memprofile_enabled` + the
        // operator-supplied `output_dir` + the implicit
        // `/app/out-network` constant) lives in the dedicated
        // `resolve_secondary_memprofile_dir` helper so the policy
        // is in one place and unit-testable; the resulting
        // `Option<PathBuf>` is what crosses into
        // `SecondaryConfig.output_dir`. The operator-supplied
        // dir (which Python plumbs from the run-level `--output`)
        // takes precedence over the bind-mount probe so dispatch
        // paths without `/app/out-network` (single-process,
        // multi-computer-local) still get a sampler when the
        // operator opts in.
        let memprofile_output_dir = resolve_secondary_memprofile_dir(
            self.memprofile_enabled,
            Some(self.output_dir.as_path()),
        );
        // Compose the per-secondary memuse log path on the GIL
        // thread so the spawn closure receives a ready-made
        // `Option<PathBuf>`. Defaults to
        // `{self.output_dir}/memuse.log` so every dispatch path
        // writes the same shape; preserves the
        // `Option<PathBuf>` shape (None = disabled) for tests
        // and operators who want to opt out.
        let cfg_memuse_log_path =
            dynrunner_manager_local::memuse::derive_memuse_log_path(
                Some(self.output_dir.as_path()),
                None,
            );
        // Per-type subprocess dispatch: the factory carries the full
        // `TypeRegistry`. `spawn_worker` defaults to `types.first()`
        // for initial pool init (preserves pre-fix single-type
        // behaviour); `spawn_worker_for_type` consults the registry
        // for per-task respawn on TypeId mismatch via
        // `WorkerPool::ensure_worker_for_type`.
        if self.types.first().is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            ));
        }
        let types = self.types.clone();
        let skip_existing = self.skip_existing;
        let cfg_src_network = self.src_network.clone();
        let cfg_src_tmp = self.src_tmp.clone();

        // Snapshot the cap, flip `run_started`, and consume the
        // command-channel receiver for the detached runtime in one
        // step. The helper owns the single-shot guard and the
        // snapshot ordering; the sender clone returned in `wiring`
        // keeps backing future `handle()` calls. Mirrors
        // `PyPrimaryCoordinator::run` and `PyDistributedManager::run`.
        let wiring = self.control_plane.take_for_run()?;
        let unfulfillable_reinject_max_per_task = wiring.cap_snapshot;
        let command_tx = wiring.command_tx;
        let command_rx = wiring.command_rx;

        // Setup-promote yield captures: cloned here so the `py.detach`
        // closure (which runs without the GIL) owns its own handles
        // without borrowing `self`. `task_definition_py` /
        // `task_args_py` are `Send`-safe `Py<PyAny>` reference bumps;
        // `phase_deps_for_ingest` / `setup_discover_root` are plain
        // owned values.
        //
        // `setup_discover_root` mirrors `cfg_src_network`: in pre-staged
        // mode the Python pipeline guarantees it's `Some` (the bind-
        // mount root the staged corpus lives under). In legacy /
        // failover modes the secondary never observes
        // `RunOutcome::SetupPending`, so the `None` arm of the yield
        // handler can surface a programmer-error rather than
        // pretending to walk a non-existent root.
        let task_definition_py = self.task_definition_py.clone_ref(py);
        let task_args_py = self.task_args_py.clone_ref(py);
        let phase_deps_for_ingest = self.phase_deps.clone();
        let setup_discover_root = self.src_network.clone();
        // Panik-watcher config captured before `py.detach` so the
        // tokio-runtime closure owns its own copy. Cloning a `Vec<PathBuf>`
        // is cheap; the watcher only needs read-only access.
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval = std::time::Duration::from_secs_f64(
            self.panik_watcher_poll_interval_secs,
        );
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

        // Phase-lifecycle callbacks for the post-promotion path. Built
        // here under the GIL (the `make_on_phase_*` constructors
        // capture a `Py<PyAny>` clone of `task_definition_py` that the
        // closure body re-binds via `Python::attach` at each fire).
        // Threaded into the inner `SecondaryCoordinator` via
        // `register_phase_lifecycle_callbacks` BEFORE
        // `run_until_setup_or_done` enters so the closures are visible
        // to `note_primary_item_completed` from the first cascade.
        //
        // The secondary's `on_phase_end` invocation fires ONLY when
        // this secondary owns the promoted-primary's `primary_pending`
        // pool — i.e. after `PromotePrimary` (setup-promote or
        // failover) flips `is_primary` true. Non-promoted secondaries
        // hold the closures dormant and never call into Python, so the
        // GIL-reacquiring cost is paid only on the post-promotion
        // path. See
        // `dynrunner-manager-distributed/src/secondary/primary/lifecycle.rs`.
        let sec_on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(
                self.task_definition_py.clone_ref(py),
            ),
        );
        let sec_on_phase_end: crate::managers::lifecycle::OnPhaseEnd = Box::new(
            crate::managers::lifecycle::make_on_phase_end(
                self.task_definition_py.clone_ref(py),
            ),
        );

        // Errors produced inside the async block — including
        // `task.discover_items` raising in setup-promote — must surface
        // as `PyErr` here so the Python-side `run()` returns non-zero.
        // Previously every error path `break`d out of the loop and
        // `self.completed` was set from a zero counter, causing the
        // secondary to exit `0` despite the work never starting; the
        // dispatcher then chained the next task on a missing input.
        // Terminal-outcome shapes for the secondary's `run()`. The
        // `py.detach` closure returns one of these; the outer scope
        // (with the GIL re-acquired) translates to the Python-side
        // surface — completed count for `Done`, `std::process::exit(137)`
        // for `Panik`.
        enum SecondaryRunOutcome {
            Done(u32),
            Panik(std::path::PathBuf),
        }
        let result: Result<SecondaryRunOutcome, PyErr> =
            py.detach(|| -> Result<SecondaryRunOutcome, PyErr> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to create tokio runtime: {e}"
                    ))
                })?;

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Resolve the primary URL to a SocketAddr.
                // Supports formats like "tcp://host:port", "ws://host:port", or "host:port"
                // where `host` may be either a literal IP address or a DNS name —
                // SLURM gateways generally hand out the FQDN from `hostname -f`,
                // so the resolver needs to accept both.
                let addr_str = primary_url
                    .strip_prefix("tcp://")
                    .or_else(|| primary_url.strip_prefix("ws://"))
                    .or_else(|| primary_url.strip_prefix("wss://"))
                    .unwrap_or(&primary_url);

                let addr: std::net::SocketAddr = match tokio::net::lookup_host(addr_str).await {
                    Ok(mut iter) => match iter.next() {
                        Some(a) => a,
                        None => {
                            tracing::error!(url = %primary_url, "DNS lookup returned no addresses for primary URL");
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "DNS lookup returned no addresses for primary URL {primary_url}"
                            )));
                        }
                    },
                    Err(e) => {
                        tracing::error!(url = %primary_url, error = %e, "failed to resolve primary URL");
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "failed to resolve primary URL {primary_url}: {e}"
                        )));
                    }
                };

                // Connect to primary via WSS, retrying until the configured timeout.
                let connect_timeout = dist_connect_timeout;
                let retry_delay = dist_connect_retry_delay;
                let start = std::time::Instant::now();
                let mut attempt = 0u32;
                let client = loop {
                    attempt += 1;
                    let elapsed = start.elapsed();
                    if elapsed > connect_timeout {
                        tracing::error!(
                            addr = %addr,
                            attempts = attempt,
                            "failed to connect to primary after {:.0}s",
                            connect_timeout.as_secs_f64()
                        );
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "failed to connect to primary at {addr} after {:.0}s ({attempt} attempts)",
                            connect_timeout.as_secs_f64()
                        )));
                    }
                    match NetworkClient::connect_wss_only(addr).await {
                        Ok(c) => {
                            tracing::info!(
                                addr = %addr,
                                elapsed_s = elapsed.as_secs_f64(),
                                attempts = attempt,
                                "connected to primary"
                            );
                            break c;
                        }
                        Err(e) => {
                            let remaining = connect_timeout.saturating_sub(elapsed);
                            if remaining > retry_delay {
                                tracing::info!(
                                    attempt,
                                    error = %e,
                                    "connection failed, retrying in {:.0}s...",
                                    retry_delay.as_secs_f64()
                                );
                                tokio::time::sleep(retry_delay).await;
                            } else {
                                tracing::error!(addr = %addr, error = %e, "failed to connect to primary");
                                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "failed to connect to primary at {addr}: {e}"
                                )));
                            }
                        }
                    }
                };

                // Start peer network for peer-to-peer communication. The
                // identity passed to `PeerNetwork::start` is BOTH the
                // CN baked into this secondary's QUIC certificate AND
                // the `peer_id` other secondaries will pass to quinn's
                // `connect(addr, server_name)` to validate that cert.
                // The primary distributes peer info keyed by
                // `secondary_id` (the logical id, e.g. "secondary-0")
                // — so the cert CN must match the logical id, not
                // the SLURM hostname or any worker count. The previous
                // value `format!("sec-{}", num_workers)` produced a
                // CN like "sec-14" (one per cpus_per_task value) that
                // never matched anything quinn expected; QUIC dials
                // failed CN validation on every peer pair and fell
                // back to WSS, eating the 10s-per-peer timeout budget.
                // Pick the peer transport at runtime: real `PeerNetwork`
                // for normal clusters, `NoPeerTransport` for clusters
                // that firewall inter-compute-node networking (LMU
                // SLURM, etc.) where every peer dial would time out
                // anyway. Selection comes from `DistributedConfig
                // .disable_peer_overlay` — see the CLI flag's help
                // text for the failover-incompat caveat. The
                // `EitherPeerTransport` enum lives one level down in
                // dynrunner-transport-quic because the `PeerTransport`
                // trait uses RPIT-in-trait and isn't object-safe;
                // a sum-type is the only way to pick at runtime.
                let (peer_network, peer_cert_pem, peer_port): (
                    dynrunner_transport_quic::EitherPeerTransport<RunnerIdentifier>,
                    String,
                    u16,
                ) = if dist_disable_peer_overlay {
                    tracing::info!("peer overlay disabled by config; using NoPeerTransport");
                    (
                        dynrunner_transport_quic::EitherPeerTransport::Disabled(
                            dynrunner_transport_quic::NoPeerTransport,
                        ),
                        String::new(),
                        0,
                    )
                } else {
                    let pn = dynrunner_transport_quic::PeerNetwork::<RunnerIdentifier>::start(
                        &secondary_id,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        tracing::error!(error = %e, "failed to start peer network");
                        // PeerNetwork::start only fails on cert
                        // generation or bind errors; treat as fatal —
                        // there's no useful fallback here that keeps
                        // peer functionality.
                        panic!("peer network start failed: {e}");
                    });
                    let cert_pem = pn.cert_pem().to_string();
                    let port = pn.port();
                    (
                        dynrunner_transport_quic::EitherPeerTransport::Real(Box::new(pn)),
                        cert_pem,
                        port,
                    )
                };

                let config = SecondaryConfig {
                    secondary_id: secondary_id.clone(),
                    num_workers,
                    max_resources,
                    hostname: gethostname(),
                    keepalive_interval: dist_keepalive,
                    src_network: cfg_src_network,
                    src_tmp: cfg_src_tmp,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    primary_link_failure_threshold: dist_primary_link_failure_threshold,
                    primary_link_failure_window: dist_primary_link_failure_window,
                    setup_deadline: dist_setup_deadline,
                    is_observer: false,
                    resource_check_interval: dist_resource_check_interval,
                    log_oom_watcher: dist_log_oom_watcher,
                    promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                    unfulfillable_reinject_max_per_task,
                    mem_manager_reserved_bytes: cfg_mem_manager_reserved_bytes,
                    output_dir: memprofile_output_dir.clone(),
                    memuse_log_path: cfg_memuse_log_path.clone(),
                };

                let mut factory = SubprocessWorkerFactory {
                    python_executable,
                    source_dir,
                    output_dir,
                    log_dir,
                    log_paths,
                    types,
                    skip_existing,
                    connection_mode: ConnectionMode::Socketpair,
                    manual_start_worker: false,
                    worker_spec,
                    child_processes: Vec::new(),
                };

                let mut secondary: SecondaryCoordinator<_, _, _, _, _, RunnerIdentifier> = SecondaryCoordinator::new(
                    config,
                    client,
                    peer_network,
                    scheduler_config.build_memory_scheduler(),
                    estimator,
                );

                // Swap in the Python-facing command channel so the
                // `PrimaryHandle` Python is holding talks to the same
                // receiver this secondary's `process_tasks` loop
                // reads from. Same pre-run contract as
                // `PyPrimaryCoordinator`.
                secondary.replace_command_channel(command_tx, command_rx);

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run_until_setup_or_done` enters — the
                // coordinator's `register_lifecycle_listener` contract
                // requires pre-run registration because the listener
                // vector is `mem::take`-d into the spawned dispatcher
                // on first entry.
                if let Some(listener) = peer_lifecycle_listener {
                    secondary.register_lifecycle_listener(listener);
                }

                // Set peer cert info so the CertExchange message
                // includes our QUIC details. Both families are
                // detected by `network::detect_ipv4` / `detect_ipv6`
                // (env-var hint first, `hostname -I` fallback); the
                // resulting `PeerCertInfo` is what the
                // `send_cert_exchange` step ships on the wire and the
                // primary then re-broadcasts via `PeerInfo`. The
                // dialer (peer/dial.rs) consumes both families and
                // happy-eyeballs-races them, so a host that has only
                // one family configured is fine — the missing one is
                // simply absent from the candidate set.
                secondary.set_peer_cert_info(
                    dynrunner_manager_distributed::PeerCertInfo {
                        public_cert_pem: peer_cert_pem,
                        ipv4_address: Some(detect_ipv4(None)),
                        ipv6_address: detect_ipv6(None),
                        quic_port: peer_port,
                    },
                );

                // Spawn the panik watcher and register its signal
                // receiver on the coordinator BEFORE entering the
                // setup-promote loop. The watcher polls
                // `panik_watcher_paths` every `panik_watcher_poll_interval`;
                // empty paths config yields a never-firing receiver
                // (the spawn helper returns a no-op task), so callers
                // that don't pass `--panik-file` flags get a
                // structurally-disabled watcher with zero runtime
                // cost. The `PanikWatcher` handle is held in this
                // scope so its `Drop::abort()` runs at loop exit and
                // cleans up the polling task.
                let mut panik_watcher =
                    dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                        dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                            paths: panik_watcher_paths,
                            poll_interval: panik_watcher_poll_interval,
                            // SECONDARY-role spawner: the host-side
                            // shutdown-manager forwards SLURM
                            // time-limit / scancel as
                            // `podman exec <c> kill -TERM <pid>`
                            // into the secondary process. Listening
                            // for SIGTERM here routes that into the
                            // same panik cascade as a sentinel-file
                            // trigger — worker-teardown +
                            // exit(137) — so the secondary releases
                            // SLURM-allocated resources cleanly
                            // before the kernel SIGKILLs at the
                            // SLURM grace deadline.
                            listen_for_sigterm: true,
                        },
                    );
                if let Some(rx) = panik_watcher.take_signal_rx() {
                    secondary.register_panik_signal_rx(rx);
                }

                // Install the phase-lifecycle callbacks for the
                // post-promotion path. Pre-`run_until_setup_or_done`
                // contract — same shape as `register_lifecycle_listener`
                // and `register_panik_signal_rx` above. Non-promoted
                // secondaries never fire either closure; the GIL cost
                // is paid only when the secondary holds the primary
                // pool. The closures themselves were constructed under
                // the GIL above.
                secondary.register_phase_lifecycle_callbacks(
                    sec_on_phase_start,
                    sec_on_phase_end,
                );

                // Setup-promote outer loop: drive
                // `run_until_setup_or_done` to a terminal state,
                // bouncing back through Python's `discover_items` on
                // every `SetupPending` yield. The Rust core enforces
                // that `SetupPending` only ever arises from a
                // `PromotePrimary { required_setup: true }` wire
                // arrival, which only the submitter's pre-staged-mode
                // configuration emits — so legacy / failover runs
                // observe `Done` on the first iteration and the loop
                // exits cleanly without re-entering Python.
                //
                // GIL discipline: this entire async block runs inside
                // `py.detach` (GIL released). Each Python excursion
                // re-acquires via `Python::attach`, makes the single
                // `discover_items` call, converts the iterable into
                // `Vec<TaskInfo<RunnerIdentifier>>` through the
                // workspace-shared `extract_binaries` helper, then
                // returns — yielding the GIL back so the next Rust
                // async tick can proceed. The Python-side time on the
                // GIL is bounded by the cost of one user-defined
                // generator drain plus the per-item attribute reads
                // `extract_binaries` performs; in particular the
                // Rust transport state, worker pool, and `select!`
                // loop are NOT held while Python is running.
                //
                // Cancel-safety: `run_until_setup_or_done` documents
                // its `process_tasks` `select!` arms as cancel-safe
                // (mpsc recv + tokio interval ticks; see
                // `secondary/processing.rs:57-65`). The `SetupPending`
                // early return abandons the in-flight `select!`
                // future and reentry rebuilds it from scratch on the
                // next loop iteration's `run_until_setup_or_done`
                // call. No coordinator state is dropped across the
                // yield (`setup_phase_completed` is latched, workers
                // stay running, transports remain connected).
                // Loop result carries the three distinct terminal
                // shapes the coordinator can produce:
                //   - `Ok(())`: clean shutdown, return to Python normally.
                //   - `Err(PyErr)`: typed run failure, surfaced to
                //     Python as the wrapping exception.
                //   - `Panik(PathBuf)`: operator-initiated emergency
                //     stop; the outer `run()` calls
                //     `std::process::exit(137)` after reacquiring
                //     the GIL.
                //
                // Modelled as an enum (rather than a sentinel string
                // smuggled through `Err(PyErr)`) so the boundary
                // remains typed and the exit-on-panik decision is
                // a structural match, not a string compare.
                enum LoopResult {
                    Ok(()),
                    Err(PyErr),
                    Panik(std::path::PathBuf),
                }
                let loop_result: LoopResult = loop {
                    let outcome = match secondary
                        .run_until_setup_or_done(&mut factory)
                        .await
                    {
                        Ok(o) => o,
                        Err(e) => {
                            tracing::error!(error = %e, "secondary failed");
                            break LoopResult::Err(pyo3::exceptions::PyRuntimeError::new_err(
                                format!("secondary failed: {e}"),
                            ));
                        }
                    };
                    match outcome {
                        RunOutcome::Done => {
                            tracing::info!("secondary finished successfully");
                            break LoopResult::Ok(());
                        }
                        RunOutcome::PanikShutdown {
                            matched_path,
                            reason,
                        } => {
                            // The coordinator has already announced
                            // this node's departure (self-authored
                            // `ClusterMutation::PeerRemoved
                            // { SelfDeparture }`, file source only) and
                            // killed every worker pgid in this
                            // secondary. The PyO3 outer scope owns
                            // the actual `exit(137)` call (and the
                            // log) so this arm just propagates the
                            // matched_path through the loop's typed
                            // result; see [`LoopResult`] above.
                            tracing::warn!(
                                matched_path = %matched_path.display(),
                                reason = %reason,
                                "secondary panik shutdown; propagating \
                                 to PyO3 boundary for exit(137)"
                            );
                            break LoopResult::Panik(matched_path);
                        }
                        RunOutcome::SetupPending => {
                            // Re-acquire the GIL ONLY for the duration
                            // of `task.discover_items` + the typed
                            // conversion. Held resources released back
                            // to the runtime when this block returns.
                            let discovered = Python::attach(|py| -> PyResult<
                                Vec<dynrunner_core::TaskInfo<RunnerIdentifier>>,
                            > {
                                let root = setup_discover_root
                                    .as_ref()
                                    .ok_or_else(|| {
                                        pyo3::exceptions::PyRuntimeError::new_err(
                                            "RunOutcome::SetupPending observed but \
                                             src_network is None — the wrapper has no \
                                             root to pass to task.discover_items; this \
                                             is a programmer error (only pre-staged \
                                             mode emits the SetupPending yield, and \
                                             that mode always supplies src_network)",
                                        )
                                    })?;
                                let task_def = task_definition_py.bind(py);
                                let args = task_args_py.bind(py);
                                let root_py = root.clone().into_pyobject(py)?;
                                // Surface `args.resolved_output_root`
                                // on the secondary so the task's
                                // `discover_items` sees the same
                                // attribute contract the submitter's
                                // `run.py:139` and the SLURM pipeline's
                                // `slurm/pipeline.rs:368` set on the
                                // submitter side. Without this any
                                // `--skip-existing`-style filter
                                // silently no-ops on setup-promote.
                                //
                                // Resolution rule:
                                // - Pre-staged mode
                                //   (`args.source_already_staged`
                                //   non-None): the secondary's
                                //   filesystem-view of the gateway-side
                                //   output dir lives at the
                                //   wrapper-script's static bind-mount
                                //   path `/app/out-network`.
                                //   `args.output` is the submitter's
                                //   local-cache path, forwarded
                                //   verbatim and meaningless here.
                                // - Non-pre-staged: fall back to
                                //   `Path(args.output).resolve()`,
                                //   matching the legacy local-mode
                                //   shape.
                                let pre_staged = args
                                    .getattr("source_already_staged")
                                    .ok()
                                    .filter(|v| !v.is_none())
                                    .is_some();
                                if pre_staged {
                                    args.setattr(
                                        "resolved_output_root",
                                        "/app/out-network",
                                    )?;
                                } else if let Ok(output_attr) =
                                    args.getattr("output")
                                {
                                    let pathlib = py.import("pathlib")?;
                                    let path = pathlib
                                        .getattr("Path")?
                                        .call1((output_attr,))?
                                        .call_method0("resolve")?;
                                    args.setattr(
                                        "resolved_output_root",
                                        path.str()?,
                                    )?;
                                }
                                // Buffer the discover_items iterable
                                // into a `PyList` so the workspace's
                                // existing `extract_binaries` helper
                                // (used by primary.rs::run and the
                                // SLURM pipeline) handles the typed
                                // conversion uniformly — no parallel
                                // extraction logic introduced here.
                                let py_list = PyList::empty(py);
                                let iter = task_def.call_method1(
                                    "discover_items",
                                    (root_py, args),
                                )?;
                                for item in iter.try_iter()? {
                                    py_list.append(item?)?;
                                }
                                extract_binaries(&py_list)
                            });
                            let discovered = match discovered {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        "task.discover_items raised during \
                                         setup-promote; aborting secondary"
                                    );
                                    break LoopResult::Err(e);
                                }
                            };
                            tracing::info!(
                                tasks = discovered.len(),
                                "setup-promote discovery complete; \
                                 ingesting into Rust core"
                            );
                            if let Err(e) = secondary
                                .ingest_setup_discovery(
                                    discovered,
                                    phase_deps_for_ingest.clone(),
                                )
                                .await
                            {
                                tracing::error!(
                                    error = %e,
                                    "ingest_setup_discovery failed; aborting secondary"
                                );
                                break LoopResult::Err(
                                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                                        "ingest_setup_discovery: {e}"
                                    )),
                                );
                            }
                            // Loop continues; the next
                            // `run_until_setup_or_done` call short-
                            // circuits the setup handshake (its
                            // `setup_phase_completed` latch is true)
                            // and re-enters `process_tasks` directly.
                        }
                    }
                };

                let completed = secondary.completed_count() as u32;

                // Tear down tracked worker subprocesses via the shared
                // SIGTERM → grace → SIGKILL primitive. See
                // `subprocess_factory::terminate_children` for why
                // straight SIGKILL is the wrong default for
                // podman-launched workers.
                //
                // Skipped on the panik path: the coordinator's
                // `pool.kill_all_workers_with_grace` already took down
                // every worker pgid (including descendants), and we
                // want the `exit(137)` decision to fire as soon as
                // the outer scope picks up the Panik variant — no
                // additional grace ladder.
                if !matches!(loop_result, LoopResult::Panik(_)) {
                    factory.cleanup_all();
                }

                match loop_result {
                    LoopResult::Ok(()) => Ok(SecondaryRunOutcome::Done(completed)),
                    LoopResult::Err(e) => Err(e),
                    LoopResult::Panik(matched_path) => {
                        Ok(SecondaryRunOutcome::Panik(matched_path))
                    }
                }
            }))
        });

        match result? {
            SecondaryRunOutcome::Done(completed) => {
                self.completed = completed;
                Ok(())
            }
            SecondaryRunOutcome::Panik(matched_path) => {
                // GIL has been re-acquired (the `py.detach` block
                // returned). Log the cause one last time at the
                // PyO3 boundary so operators see the exit signal
                // in the dispatcher log, then exit(137). The
                // SLURM wrapper sees that code and reaps the
                // podman container; no Python stack unwinds
                // because we never return — `exit` calls libc
                // `_exit` after running atexit handlers.
                tracing::error!(
                    matched_path = %matched_path.display(),
                    "panik shutdown: secondary exiting with code 137"
                );
                std::process::exit(137);
            }
        }
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

/// Compose the secondary's memprofile output directory from the
/// operator's `--memprofile` opt-in.
///
/// Production callers use
/// [`resolve_secondary_memprofile_dir`], which probes the on-disk
/// `/app/out-network` bind-mount. The policy itself lives in
/// [`resolve_secondary_memprofile_dir_with_probe`] so tests can
/// inject the probe result without touching the real filesystem.
///
/// Single concern: decide where (if anywhere) the secondary writes
/// `.jsonl.zst` files. Resolution order:
///
///   1. `memprofile_enabled = false` → `None` (no opt-in).
///   2. `operator_output_dir = Some(_)` → use that dir (with the
///      `memprofile/` subdir appended). Honoured uniformly across
///      every dispatch path that owns an `output_dir`:
///      single-process via [`PyDistributedManager`],
///      multi-computer-local via the subprocess secondary
///      ([`PySecondaryCoordinator::output_dir`] auto-resolves to
///      the per-secondary tempdir), SLURM secondary via the
///      wrapper-auto-resolved `/app/out-network`.
///   3. The SLURM wrapper bind-mount exists at
///      [`dynrunner_manager_local::memprofile::config::SLURM_SECONDARY_OUTPUT_DIR`]
///      → use it. Backstop for callers that intentionally pass no
///      operator dir (tests, future flows).
///   4. Else → `None` with a warn: opt-in set but neither anchor
///      is available. The rare operator-misconfig case (e.g.
///      `--memprofile` on a host without our bind-mount AND
///      without a resolved output dir).
pub(crate) fn resolve_secondary_memprofile_dir(
    memprofile_enabled: bool,
    operator_output_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    let bind_mount =
        std::path::Path::new(dynrunner_manager_local::memprofile::config::SLURM_SECONDARY_OUTPUT_DIR);
    resolve_secondary_memprofile_dir_with_probe(
        memprofile_enabled,
        operator_output_dir,
        bind_mount,
        |p| p.exists(),
    )
}

/// Pure-function form of [`resolve_secondary_memprofile_dir`]. The
/// `probe` lets unit tests inject the bind-mount-exists outcome
/// without touching `/app/out-network`. See
/// [`resolve_secondary_memprofile_dir`] for the priority order.
fn resolve_secondary_memprofile_dir_with_probe(
    memprofile_enabled: bool,
    operator_output_dir: Option<&std::path::Path>,
    bind_mount: &std::path::Path,
    probe: impl FnOnce(&std::path::Path) -> bool,
) -> Option<std::path::PathBuf> {
    if !memprofile_enabled {
        return None;
    }
    if let Some(explicit) = operator_output_dir {
        return Some(
            explicit.join(dynrunner_manager_local::memprofile::config::MEMPROFILE_SUBDIR),
        );
    }
    if probe(bind_mount) {
        return Some(
            bind_mount.join(dynrunner_manager_local::memprofile::config::MEMPROFILE_SUBDIR),
        );
    }
    tracing::warn!(
        bind_mount = %bind_mount.display(),
        "--memprofile set but neither an operator-supplied output dir \
         nor the SLURM wrapper bind-mount is available; per-task memory \
         profiling is disabled."
    );
    None
}

#[cfg(test)]
mod tests {
    use super::{resolve_secondary_memprofile_dir, resolve_secondary_memprofile_dir_with_probe};
    use std::path::Path;

    #[test]
    fn disabled_returns_none_regardless_of_probe() {
        // Disabled short-circuits before any anchor is inspected.
        assert!(resolve_secondary_memprofile_dir_with_probe(
            false,
            None,
            Path::new("/whatever"),
            |_| true,
        )
        .is_none());
        assert!(resolve_secondary_memprofile_dir_with_probe(
            false,
            Some(Path::new("/tmp/run-out")),
            Path::new("/whatever"),
            |_| true,
        )
        .is_none());
        assert!(resolve_secondary_memprofile_dir_with_probe(
            false,
            None,
            Path::new("/whatever"),
            |_| false,
        )
        .is_none());
        // The production wrapper also short-circuits when disabled.
        assert!(resolve_secondary_memprofile_dir(false, None).is_none());
        assert!(
            resolve_secondary_memprofile_dir(false, Some(Path::new("/tmp/run-out"))).is_none()
        );
    }

    #[test]
    fn enabled_with_explicit_output_dir_returns_explicit_subdir() {
        // Operator-supplied dir wins; the probe is never consulted.
        let resolved = resolve_secondary_memprofile_dir_with_probe(
            true,
            Some(Path::new("/tmp/run-out")),
            Path::new("/app/out-network"),
            |_| panic!("probe must NOT run when explicit dir is set"),
        )
        .expect("explicit dir + enabled flag must resolve");
        assert_eq!(
            resolved,
            std::path::PathBuf::from("/tmp/run-out/memprofile"),
        );
    }

    #[test]
    fn enabled_with_explicit_takes_precedence_over_present_bind_mount() {
        // Both anchors are available; explicit operator dir is the
        // single source of truth so multi-computer-local + SLURM
        // resolve identically (same shape, different absolute roots).
        let resolved = resolve_secondary_memprofile_dir_with_probe(
            true,
            Some(Path::new("/tmp/run-out")),
            Path::new("/app/out-network"),
            |_| true,
        )
        .expect("explicit dir must win even when probe says yes");
        assert_eq!(
            resolved,
            std::path::PathBuf::from("/tmp/run-out/memprofile"),
        );
    }

    #[test]
    fn enabled_without_explicit_falls_back_to_bind_mount_when_present() {
        // Backstop for callers that intentionally pass no operator dir
        // (legacy tests or future flows that bypass the wrapper).
        let resolved = resolve_secondary_memprofile_dir_with_probe(
            true,
            None,
            Path::new("/app/out-network"),
            |_| true,
        )
        .expect("present bind-mount + no explicit must resolve");
        assert_eq!(
            resolved,
            std::path::PathBuf::from("/app/out-network/memprofile"),
        );
    }

    #[test]
    fn enabled_without_explicit_and_no_bind_mount_returns_none_with_warn() {
        // Operator-misconfig case: opt-in set, neither anchor
        // available. Helper logs the warn and returns None;
        // sampler is not constructed at the call site.
        assert!(resolve_secondary_memprofile_dir_with_probe(
            true,
            None,
            Path::new("/app/out-network"),
            |_| false,
        )
        .is_none());
    }
}
