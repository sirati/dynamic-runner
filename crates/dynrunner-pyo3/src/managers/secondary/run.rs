//! `PySecondaryCoordinator::run` — composes a compute-peer `Node`
//! (a secondary that can be promoted) and drives `Node::run` on a
//! dedicated tokio runtime. The setup-promote yield is now driven by the
//! Rust `SecondaryCoordinator` itself (via the registered setup-discovery
//! policy); this wrapper supplies that policy (a closure that runs Python's
//! `task.discover_items` OFF the runtime thread so the mesh-pump's
//! keepalives keep flowing). Also exposes the `completed` getter.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::TaskInfo;
use dynrunner_manager_distributed::process::{
    LocalRole, Mesh, Node, NodeRunInputs, PrimaryRunArgs, PromotedPrimary, RunTerminal,
};
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator, SetupDiscovery,
};
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::config::connection::ConnectionMode;
use crate::config::scheduler::SchedulerConfig;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;
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
        let dist_keepalive_miss_threshold = self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_oom_retry_max_passes = self.distributed_config.oom_retry_max_passes();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_unconfigured_deadline = self.distributed_config.unconfigured_deadline();
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
        let cfg_memuse_log_path = dynrunner_manager_local::memuse::derive_memuse_log_path(
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
        // Capture the submitter's `--source-already-staged` signal on the
        // GIL thread for the PROMOTED primary's `required_setup_on_promote`.
        // This is the SAME signal the submitter's own `PrimaryConfig` uses
        // (`args.source_already_staged is not None`): the node that owns
        // setup-discovery is exactly the node whose promoted primary must
        // engage the `setup_pending()` suppressor so it does not declare
        // `0+0 >= 0` run-complete before its discovery-broadcast `TaskAdded`
        // batch lands. Sourced from the run's OWN arg (D6/D7 — values
        // originate on the run's config), NOT a derived band-aid chain
        // (`derive_setup_defer_on_promote` was deleted as poison).
        let required_setup_on_promote = self
            .task_args_py
            .bind(py)
            .getattr("source_already_staged")
            .ok()
            .filter(|v| !v.is_none())
            .is_some();
        // Panik-watcher config captured before `py.detach` so the
        // tokio-runtime closure owns its own copy. Cloning a `Vec<PathBuf>`
        // is cheap; the watcher only needs read-only access.
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval =
            std::time::Duration::from_secs_f64(self.panik_watcher_poll_interval_secs);
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

        // Phase-lifecycle callbacks. Built here under the GIL (the
        // `make_on_phase_*` constructors capture a `Py<PyAny>` clone of
        // `task_definition_py` that the closure body re-binds via
        // `Python::attach` at each fire). Registered on the
        // `SecondaryCoordinator` BEFORE `run_until_setup_or_done` enters.
        // The closures fire only when this node is the authority that owns
        // the phase machine; a node that never calls into Python pays no
        // GIL-reacquiring cost.
        let sec_on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(self.task_definition_py.clone_ref(py)),
        );
        let sec_on_phase_end: crate::managers::lifecycle::OnPhaseEnd = Box::new(
            crate::managers::lifecycle::make_on_phase_end(self.task_definition_py.clone_ref(py)),
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
            Aborted(String),
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

                // Stand up the secondary's mesh transport through the
                // backend-opaque factory. It owns every backend-naming
                // step: the WSS dial + retry loop, the peer-overlay
                // selection (real peer mesh for normal clusters vs the
                // firewalled no-overlay path for clusters that firewall
                // inter-compute-node networking — selection comes from
                // `DistributedConfig.disable_peer_overlay`, see the CLI
                // flag's help text for the failover-incompat caveat),
                // reading the backend's cert + QUIC port into the
                // `PeerCertInfo` the `CertExchange` ships, extracting the
                // mesh-send capability, and folding the dialed bootstrap
                // wire into the mesh under the primary's peer-id ("the
                // tunnel is just a way of joining the mesh").
                //
                // The identity passed to the peer mesh is BOTH the CN
                // baked into this secondary's QUIC certificate AND the
                // `peer_id` other secondaries pass to quinn's
                // `connect(addr, server_name)` to validate that cert. The
                // primary distributes peer info keyed by `secondary_id`
                // (the logical id, e.g. "secondary-0") — so the cert CN
                // must match the logical id, not the SLURM hostname or
                // any worker count.
                //
                // The bootstrap primary's peer-id is the conventional
                // `"primary"` — the same id baked into the primary's
                // `PrimaryConfig::node_id`, the cert CN the QUIC dialer
                // validates against, and the host-id `Destination::Primary`
                // resolves to. The matching `set_bootstrap_primary_id`
                // below tells the egress edge to resolve
                // `Destination::Primary` to it while the role table is
                // still cold (pre-`PrimaryChanged`).
                let mesh_bundle = transport_factory::dial_secondary_mesh::<RunnerIdentifier>(
                    transport_factory::SecondaryDialParams {
                        addr,
                        connect_timeout: dist_connect_timeout,
                        retry_delay: dist_connect_retry_delay,
                        disable_peer_overlay: dist_disable_peer_overlay,
                        secondary_id: &secondary_id,
                        bootstrap_primary_id: "primary".to_string(),
                        ipv4_address: Some(detect_ipv4(None)),
                        ipv6_address: detect_ipv6(None),
                    },
                )
                .await
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
                let peer_network = mesh_bundle.transport;
                let secondary_cert_info = mesh_bundle.peer_cert_info;
                // Cloneable mesh-send capability (`Some` only when a real
                // peer mesh exists — `Disabled` overlays have no remote
                // secondaries and thus no failover, so the secondary's
                // `can_be_primary` marker is `false`). See `MeshSendHandle`.
                let mesh_send_handle = mesh_bundle.mesh_send;

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
                    // Internal default (no operator kwarg surfaced for the
                    // app-silence failover backstop); single source of truth
                    // lives in the distributed crate.
                    primary_silence_backstop:
                        dynrunner_manager_distributed::DEFAULT_PRIMARY_SILENCE_BACKSTOP,
                    unconfigured_deadline: dist_unconfigured_deadline,
                    // Primary-capability marker (twin of the wire `is_observer`
                    // role advertisement): a
                    // compute secondary can host the primary ON DEMAND iff
                    // a REAL peer mesh is present (`mesh_send_handle`), so
                    // it can construct a `PrimaryCoordinator` when named.
                    // A `disable_peer_overlay` host has no mesh handle and
                    // joins with `false`, so the submitter never relocates
                    // to it ("primary loss = job loss"). Advertised in the
                    // `SecondaryWelcome`; recorded in the replicated
                    // `RoleTable.can_be_primary` the submitter reads.
                    can_be_primary: mesh_send_handle.is_some(),
                    resource_check_interval: dist_resource_check_interval,
                    log_oom_watcher: dist_log_oom_watcher,
                    promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                    unfulfillable_reinject_max_per_task,
                    mem_manager_reserved_bytes: cfg_mem_manager_reserved_bytes,
                    output_dir: memprofile_output_dir.clone(),
                    memuse_log_path: cfg_memuse_log_path.clone(),
                };

                let factory = SubprocessWorkerFactory {
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

                // Wrap the opaque mesh transport in the role-demux `Mesh`
                // (the one thing in this process that touches the wire) and
                // register the Secondary slot, minting the coordinator's
                // `(client, inbox)` ends + the `Arc<RoleSlot>` the `Node`
                // holds as the teardown lever. The coordinator never names a
                // transport — the `Node`'s pump owns the `Mesh`.
                let mut mesh = Mesh::new(peer_network);
                let (sec_slot, sec_client, sec_inbox) = mesh
                    .register_local_role(LocalRole::Secondary, PeerId::from(secondary_id.as_str()));

                // Clone the scheduler-tuning + estimator for the SECONDARY's
                // own coordinator; the originals are moved into the promote
                // recipe below (which the promoted primary builds its own
                // scheduler/estimator from).
                let mut secondary: SecondaryCoordinator<_, _, _, RunnerIdentifier> =
                    SecondaryCoordinator::new(
                        config,
                        sec_client,
                        sec_inbox,
                        scheduler_config.build_memory_scheduler(),
                        estimator.clone(),
                    );

                // Tell the egress edge which peer-id the bootstrap wire
                // reaches (the conventional `"primary"`, the same id the
                // mesh-link registration keyed the dialed connection
                // under). The edge resolves `Destination::Primary` to it
                // while the role table is cold (pre-`PrimaryChanged`), so
                // setup frames route to the dialled primary before the
                // self-announcement lands.
                secondary.set_bootstrap_primary_id("primary".to_string());

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run` enters — the coordinator's
                // `register_lifecycle_listener` contract requires pre-run
                // registration because the listener vector is `mem::take`-d
                // into the spawned dispatcher on first entry.
                if let Some(listener) = peer_lifecycle_listener {
                    secondary.register_lifecycle_listener(listener);
                }

                // Set peer cert info so the CertExchange message includes
                // our connection details. The `PeerCertInfo` was built by
                // the transport factory from the backend's cert PEM + port
                // plus both detected address families (`network::detect_ipv4`
                // / `detect_ipv6` — env-var hint first, `hostname -I`
                // fallback). It is what the `send_cert_exchange` step ships
                // on the wire and the primary then re-broadcasts via
                // `PeerInfo`. The dialer (peer/dial.rs) consumes both
                // families and happy-eyeballs-races them, so a host that has
                // only one family configured is fine — the missing one is
                // simply absent from the candidate set.
                secondary.set_peer_cert_info(secondary_cert_info);

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

                // Register the consumer's setup-discovery policy. The Rust
                // `SecondaryCoordinator` now OWNS the setup-promote yield loop
                // (the framework drives WHEN); this closure is the consumer's
                // POLICY (it runs Python `task.discover_items` OFF the runtime
                // thread, so the `Node`'s mesh-pump keeps the keepalives
                // flowing during discovery — §14/§15). On a non-pre-staged run
                // the secondary never yields `SetupPending`, so the policy is
                // inert.
                secondary.register_setup_discovery(SetupDiscovery {
                    discover: build_setup_discovery_fn(
                        task_definition_py,
                        task_args_py,
                        setup_discover_root,
                    ),
                    phase_deps: phase_deps_for_ingest,
                });

                // Compose the compute-peer `Node`: a secondary that may be
                // PROMOTED to primary. `Node::new` hands out the
                // `promotion_tx` the secondary signals on a self-named
                // `PrimaryChanged`; `register_promotion_signal` wires it. The
                // `promote` recipe (below) is what `Node::run` calls on that
                // signal to BUILD the snapshot-seeded `PrimaryCoordinator` —
                // the secondary NEVER constructs a primary (SUPREME-LAW #3).
                let (node, promotion_tx, _node_demote_tx) = Node::new(mesh);
                secondary.register_promotion_signal(promotion_tx);

                // The promoted primary's build recipe. Captures the config
                // template + the command channel + the phase callbacks the
                // PROMOTED primary owns (not the secondary — per R4 the
                // secondary holds no phase machine). Invoked at most once, on
                // promotion, with the converged snapshot the secondary
                // captured on the signal.
                let promote = build_promoted_primary_recipe(PromotedPrimaryRecipeInputs {
                    secondary_id: secondary_id.clone(),
                    keepalive_interval: dist_keepalive,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    required_setup_on_promote,
                    scheduler_config,
                    estimator,
                    command_tx,
                    command_rx,
                    on_phase_start: sec_on_phase_start,
                    on_phase_end: sec_on_phase_end,
                });

                let node = node.with_secondary(secondary, sec_slot);
                let inputs: NodeRunInputs<
                    SubprocessWorkerFactory,
                    _,
                    _,
                    RunnerIdentifier,
                > = NodeRunInputs {
                    secondary_factory: Some(factory),
                    promote: Some(promote),
                    primary_run_args: None,
                    primary_demote_tx: None,
                };

                // Drive the node to its single role-agnostic terminal. The
                // node ran the secondary (with its setup-promote loop) and, on
                // a promotion, BUILT + ran the promoted primary in the same
                // process. The factory's worker-teardown ran INSIDE `Node::run`
                // (gated off panik). Map the terminal to the GIL-side outcome.
                let outcome = node.run(inputs).await;
                let completed = outcome.completed as u32;
                match outcome.terminal {
                    RunTerminal::Done => {
                        tracing::info!("secondary node finished successfully");
                        Ok(SecondaryRunOutcome::Done(completed))
                    }
                    RunTerminal::Panik { matched_path } => {
                        tracing::error!(
                            matched_path = %matched_path.display(),
                            "secondary panik shutdown; propagating to PyO3 boundary for exit(137)"
                        );
                        Ok(SecondaryRunOutcome::Panik(matched_path))
                    }
                    RunTerminal::Aborted { reason } => {
                        tracing::error!(
                            reason = %reason,
                            "secondary run aborted by primary; propagating \
                             to PyO3 boundary for exit(1)"
                        );
                        Ok(SecondaryRunOutcome::Aborted(reason))
                    }
                    RunTerminal::Failed { error } => {
                        tracing::error!(error = %error, "secondary node run failed");
                        Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "secondary failed: {error}"
                        )))
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
            SecondaryRunOutcome::Aborted(reason) => {
                // GIL re-acquired. The primary aborted the run
                // cluster-wide (#3a pre-phase duplicate). Log the
                // cause then exit(1) so the SLURM wrapper / Python
                // caller observes a non-zero exit. Same exit-on-
                // terminal shape as the panik arm, code 1 (a clean
                // process-level failure, not the SIGKILL-mapped 137).
                tracing::error!(
                    reason = %reason,
                    "run aborted by primary: secondary exiting with code 1"
                );
                std::process::exit(1);
            }
        }
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

/// Inputs to [`build_promoted_primary_recipe`] — everything the promoted
/// primary's build needs that is captured on the GIL thread / from config.
struct PromotedPrimaryRecipeInputs {
    secondary_id: String,
    keepalive_interval: std::time::Duration,
    peer_timeout: std::time::Duration,
    keepalive_miss_threshold: u32,
    retry_max_passes: u32,
    oom_retry_max_passes: u32,
    /// The submitter's `--source-already-staged` signal (captured on the GIL
    /// thread). When true the promoted primary engages the `setup_pending()`
    /// suppressor so it does not declare `0+0 >= 0` run-complete before its
    /// discovery-broadcast `TaskAdded` batch lands.
    required_setup_on_promote: bool,
    scheduler_config: SchedulerConfig,
    estimator: PyMemoryEstimatorBridge,
    /// The Python `PrimaryHandle`'s command channel ends. The PROMOTED PRIMARY
    /// drains the receiver (post-promotion, externally-issued
    /// `spawn_tasks`/`reinject` land on its `primary_pending` pool); the
    /// secondary does not (R4 seam). Moved into the promoted primary via
    /// `replace_command_channel`.
    command_tx: tokio::sync::mpsc::Sender<
        dynrunner_manager_distributed::primary::PrimaryCommand<RunnerIdentifier>,
    >,
    command_rx: tokio::sync::mpsc::Receiver<
        dynrunner_manager_distributed::primary::PrimaryCommand<RunnerIdentifier>,
    >,
    /// The phase-lifecycle callbacks the PROMOTED primary fires (it owns the
    /// phase machine; the secondary does not — R4 seam).
    on_phase_start: crate::managers::lifecycle::OnPhaseStart,
    on_phase_end: crate::managers::lifecycle::OnPhaseEnd,
}

/// Build the `PromotedPrimaryBuilder` recipe `Node::run` invokes on a
/// promotion signal to construct the snapshot-seeded `PrimaryCoordinator`.
///
/// The node supplies the mesh ends + the demote receiver + the converged
/// `cluster_state` snapshot (carried on the signal, captured atomically at the
/// promotion-fire instant); this recipe builds the coordinator around them and
/// SEEDS it via `seed_from_promotion_snapshot`, returning the ready-to-`run`
/// primary + its (empty — the snapshot carries the tasks) pipeline args. The
/// node stays ignorant of scheduler/estimator/`PrimaryConfig` construction
/// (those are the caller's concern); it only registers the slot + spawns the
/// returned coordinator.
///
/// `FnMut`-but-single-use: a node promotes at most once, so the command
/// channel + phase callbacks (single-use, not `Clone`) are captured in
/// `Option`s and taken on the first (only) invocation.
fn build_promoted_primary_recipe(
    inputs: PromotedPrimaryRecipeInputs,
) -> dynrunner_manager_distributed::process::PromotedPrimaryBuilder<
    dynrunner_scheduler::ResourceStealingScheduler,
    PyMemoryEstimatorBridge,
    RunnerIdentifier,
> {
    let PromotedPrimaryRecipeInputs {
        secondary_id,
        keepalive_interval,
        peer_timeout,
        keepalive_miss_threshold,
        retry_max_passes,
        oom_retry_max_passes,
        required_setup_on_promote,
        scheduler_config,
        estimator,
        command_tx,
        command_rx,
        on_phase_start,
        on_phase_end,
    } = inputs;
    // Single-use pieces captured in Options so the FnMut can take them on its
    // one invocation (a node promotes at most once per lifetime).
    let mut command_channel = Some((command_tx, command_rx));
    let mut phase_callbacks = Some((on_phase_start, on_phase_end));
    Box::new(move |client, inbox, demote_rx, snapshot| {
        let config = PrimaryConfig {
            node_id: secondary_id.clone(),
            keepalive_interval,
            peer_timeout,
            keepalive_miss_threshold,
            retry_max_passes,
            oom_retry_max_passes,
            // The promoted-primary's setup-defer suppressor (D6/D7): the node
            // that owns discovery is exactly the node whose promoted primary
            // must wait for its own discovery batch. Sourced from the run's
            // OWN `--source-already-staged` arg, NOT a derived band-aid.
            required_setup_on_promote,
            ..PrimaryConfig::default()
        };
        let mut primary = PrimaryCoordinator::new(
            config,
            client,
            inbox,
            demote_rx,
            scheduler_config.build_memory_scheduler(),
            estimator.clone(),
        );
        // Transfer the Python `PrimaryHandle`'s command channel so an
        // externally-issued `spawn_tasks` / `reinject` (e.g. from a promoted
        // node's `on_phase_end`) reaches THIS primary's command loop.
        if let Some((tx, rx)) = command_channel.take() {
            primary.replace_command_channel(tx, rx);
        }
        // The promoted primary owns the phase machine, so it fires the
        // `on_phase_*` callbacks (R4: the secondary held them only as the
        // wiring anchor; they belong on the authority).
        if let Some((on_start, on_end)) = phase_callbacks.take() {
            primary.register_phase_lifecycle_callbacks(on_start, on_end);
        }
        // Seed from the promoting host's converged snapshot (NORMAL pre-`run`
        // construction input — not a `run_activated` resume, which is gone):
        // restore the ledger + rebuild the derived pool/roster caches, then
        // the primary enters the ordinary `run` path and originates
        // `PrimaryChanged` itself.
        primary.seed_from_promotion_snapshot(snapshot);
        PromotedPrimary {
            coordinator: primary,
            // The snapshot carries the tasks + phase-deps; a promoted primary
            // enters `run` with empty binaries (the setup-defer / seeded
            // path), exactly like the submitter's setup-deferred local
            // primary.
            run_args: PrimaryRunArgs {
                binaries: Vec::new(),
                phase_deps: HashMap::new(),
                on_phase_start: Box::new(|_| {}),
                on_phase_end: Box::new(|_, _, _, _| {}),
            },
        }
    })
}

/// Build the consumer's setup-discovery policy closure.
///
/// The returned [`dynrunner_manager_distributed::SetupDiscoveryFn`] is
/// invoked by the Rust `SecondaryCoordinator`'s run loop on each
/// `SetupPending` yield (pre-staged mode, empty ledger). It runs Python's
/// `task.discover_items(<root>, args)` and converts the result through the
/// workspace-shared `extract_binaries`.
///
/// # Non-block correctness (§14/§15)
///
/// The secondary's run loop shares ONE single-threaded runtime with the
/// `Node`'s mesh-pump. Running the GIL excursion ON that thread would stall
/// the pump → the secondary's keepalives stop → the primary declares it dead
/// → STRAND. So each invocation runs the GIL excursion on a
/// `tokio::task::spawn_blocking` thread and the returned future merely
/// `.await`s that handle — yielding the runtime thread to the pump, which
/// keeps the mesh alive (keepalives flowing) for the whole discovery
/// duration, however slow the `--source-already-staged` scan is.
///
/// The `Send` Python handles are captured in an `Option` and MOVED into the
/// blocking task on the first (only) invocation — the secondary yields
/// `SetupPending` at most once (the `ingest_setup_discovery` fire-once latch),
/// so an `FnMut` that consumes its handles via `take()` is sufficient and
/// avoids any off-GIL `Py` clone (which would need a `Python` token). A
/// defensive second invocation surfaces a clear error rather than panicking.
fn build_setup_discovery_fn(
    task_definition_py: Py<PyAny>,
    task_args_py: Py<PyAny>,
    setup_discover_root: Option<std::path::PathBuf>,
) -> dynrunner_manager_distributed::SetupDiscoveryFn<RunnerIdentifier> {
    // Captured once; taken on the single invocation (fire-once latch upstream).
    let mut handles = Some((task_definition_py, task_args_py, setup_discover_root));
    Box::new(move || {
        let taken = handles.take();
        let fut = async move {
            let Some((task_definition_py, task_args_py, setup_discover_root)) = taken else {
                return Err(
                    "setup-discovery policy invoked more than once — the secondary \
                     yields SetupPending at most once (ingest fire-once latch); a \
                     second yield is a programmer error"
                        .to_string(),
                );
            };
            // Run the GIL excursion OFF the runtime thread so the mesh-pump
            // keeps the keepalives flowing during discovery (§14/§15).
            tokio::task::spawn_blocking(move || {
                Python::attach(|py| -> Result<Vec<TaskInfo<RunnerIdentifier>>, String> {
                    discover_items_under_gil(
                        py,
                        &task_definition_py,
                        &task_args_py,
                        setup_discover_root.as_ref(),
                    )
                })
            })
            .await
            .map_err(|e| format!("setup-discovery blocking task panicked/aborted: {e}"))?
        };
        Box::pin(fut) as Pin<Box<dyn Future<Output = Result<Vec<TaskInfo<RunnerIdentifier>>, String>>>>
    })
}

/// The GIL-held body of one setup-discovery excursion: resolve the output
/// root attribute, call `task.discover_items(<root>, args)`, and convert the
/// result into typed binaries. Pure under-GIL logic, factored out so the
/// `spawn_blocking` closure stays a thin off-thread wrapper. Returns a
/// `String` error (the secondary aborts the run on it) so no `PyErr` crosses
/// the `Send` boundary.
fn discover_items_under_gil(
    py: Python<'_>,
    task_definition_py: &Py<PyAny>,
    task_args_py: &Py<PyAny>,
    setup_discover_root: Option<&std::path::PathBuf>,
) -> Result<Vec<TaskInfo<RunnerIdentifier>>, String> {
    let root = setup_discover_root.ok_or_else(|| {
        "RunOutcome::SetupPending observed but src_network is None — the wrapper \
         has no root to pass to task.discover_items; this is a programmer error \
         (only pre-staged mode emits the SetupPending yield, and that mode always \
         supplies src_network)"
            .to_string()
    })?;
    let task_def = task_definition_py.bind(py);
    let args = task_args_py.bind(py);
    let root_py = root
        .clone()
        .into_pyobject(py)
        .map_err(|e| format!("failed to convert discovery root to a Python path: {e}"))?;
    // Surface `args.resolved_output_root` on the secondary so the task's
    // `discover_items` sees the same attribute contract the submitter sets.
    // - Pre-staged mode (`args.source_already_staged` non-None): the
    //   secondary's filesystem-view of the gateway-side output dir lives at
    //   the wrapper-script's static bind-mount path `/app/out-network`.
    // - Non-pre-staged: fall back to `Path(args.output).resolve()`.
    let pre_staged = args
        .getattr("source_already_staged")
        .ok()
        .filter(|v| !v.is_none())
        .is_some();
    if pre_staged {
        args.setattr("resolved_output_root", "/app/out-network")
            .map_err(|e| format!("failed to set resolved_output_root: {e}"))?;
    } else if let Ok(output_attr) = args.getattr("output") {
        let resolved = (|| -> PyResult<Bound<'_, PyAny>> {
            let pathlib = py.import("pathlib")?;
            pathlib
                .getattr("Path")?
                .call1((output_attr,))?
                .call_method0("resolve")
        })()
        .map_err(|e| format!("failed to resolve output root: {e}"))?;
        let resolved_str = resolved
            .str()
            .map_err(|e| format!("failed to stringify resolved output root: {e}"))?;
        args.setattr("resolved_output_root", resolved_str)
            .map_err(|e| format!("failed to set resolved_output_root: {e}"))?;
    }
    // Buffer the discover_items iterable into a `PyList` so the shared
    // `extract_binaries` helper handles the typed conversion uniformly.
    let py_list = PyList::empty(py);
    let iter = task_def
        .call_method1("discover_items", (root_py, args))
        .map_err(|e| format!("task.discover_items raised: {e}"))?;
    let iter = iter
        .try_iter()
        .map_err(|e| format!("discover_items result is not iterable: {e}"))?;
    for item in iter {
        let item = item.map_err(|e| format!("discover_items iteration raised: {e}"))?;
        py_list
            .append(item)
            .map_err(|e| format!("failed to buffer a discovered item: {e}"))?;
    }
    extract_binaries(&py_list).map_err(|e| format!("extract_binaries failed: {e}"))
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
    let bind_mount = std::path::Path::new(
        dynrunner_manager_local::memprofile::config::SLURM_SECONDARY_OUTPUT_DIR,
    );
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
        return Some(explicit.join(dynrunner_manager_local::memprofile::config::MEMPROFILE_SUBDIR));
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
    use super::{
        resolve_secondary_memprofile_dir, resolve_secondary_memprofile_dir_with_probe,
    };
    use std::path::Path;

    #[test]
    fn disabled_returns_none_regardless_of_probe() {
        // Disabled short-circuits before any anchor is inspected.
        assert!(
            resolve_secondary_memprofile_dir_with_probe(
                false,
                None,
                Path::new("/whatever"),
                |_| true,
            )
            .is_none()
        );
        assert!(
            resolve_secondary_memprofile_dir_with_probe(
                false,
                Some(Path::new("/tmp/run-out")),
                Path::new("/whatever"),
                |_| true,
            )
            .is_none()
        );
        assert!(
            resolve_secondary_memprofile_dir_with_probe(
                false,
                None,
                Path::new("/whatever"),
                |_| false,
            )
            .is_none()
        );
        // The production wrapper also short-circuits when disabled.
        assert!(resolve_secondary_memprofile_dir(false, None).is_none());
        assert!(resolve_secondary_memprofile_dir(false, Some(Path::new("/tmp/run-out"))).is_none());
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
        assert!(
            resolve_secondary_memprofile_dir_with_probe(
                true,
                None,
                Path::new("/app/out-network"),
                |_| false,
            )
            .is_none()
        );
    }
}
