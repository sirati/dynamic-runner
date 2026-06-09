//! `PySecondaryCoordinator::run` — composes a compute-peer `Node`
//! (a secondary that can be promoted) and drives `Node::run` on a
//! dedicated tokio runtime. The setup-promote yield is now driven by the
//! Rust `SecondaryCoordinator` itself (via the registered setup-discovery
//! policy); this wrapper supplies that policy (a closure that runs Python's
//! `task.discover_items` OFF the runtime thread so the mesh-pump's
//! keepalives keep flowing). Also exposes the `completed` getter.

use std::future::Future;
use std::pin::Pin;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::TaskInfo;
use dynrunner_manager_distributed::process::{
    LocalRole, Mesh, Node, NodeRunInputs, PrimaryRunArgs, PromotedPrimary, RunTerminal, SeedSource,
};
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator,
};
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::config::connection::ConnectionMode;
use crate::config::scheduler::SchedulerConfig;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;
use crate::network::{detect_ipv4_with_source, detect_ipv6_with_source, gethostname};
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
        let dist_resource_check_interval = self.distributed_config.resource_check_interval();
        let dist_log_oom_watcher = self.distributed_config.log_oom_watcher();
        let cfg_mem_manager_reserved_bytes = self.mem_manager_reserved_bytes;
        // The node-local run-config SEED (the consumer's boot-CLI
        // `args.forwarded_argv`, usually empty — the post-welcome push delivers
        // the real value). Feeds `SecondaryConfig.forwarded_argv`, which the
        // coordinator wraps into its SHARED handle. The promoted-primary recipe
        // reads that SAME shared handle (via `run_config_handle()`, below) so a
        // node promoted to primary threads the DELIVERED argv (post-push) — not
        // this stale seed — into its `PrimaryConfig` (step 7 staleness fix).
        let forwarded_argv = self.forwarded_argv.clone();
        // The consumer's run-config finalize closure (deferred reparse), taken
        // off `self` so it can move into the detached runtime's registration.
        let finalize_run_config = self.finalize_run_config.take();
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
        // SHARED worker-command source (single representation): the factory
        // reads it at every spawn; the run-config finalize closure swaps it
        // (post-push). Seeded from the boot-CLI registry (placeholder
        // cmd_args). One Arc, two readers — the factory and the finalize.
        let shared_types: crate::task_def::SharedTypeRegistry =
            crate::task_def::shared_registry(self.types.clone());
        let finalize_shared_types = shared_types.clone();
        let skip_existing = self.skip_existing;
        // The declared `TypeId`s (for the finalize's per-type
        // `build_worker_command_args` loop) + the run paths the rebuild needs,
        // captured on the GIL thread as plain owned values.
        let finalize_type_ids: Vec<String> = self
            .types
            .types
            .iter()
            .map(|t| t.type_id.as_str().to_string())
            .collect();
        let finalize_source_str = self.source_dir.to_string_lossy().into_owned();
        let finalize_output_str = self.output_dir.to_string_lossy().into_owned();
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

        // The relocated primary's OWN live `PrimaryHandle` for its
        // `on_run_start` fire. Minted here (post-`take_for_run`, which left the
        // sender behind so `to_handle` still works) so its sender shares the
        // SAME command channel the recipe threads via `replace_command_channel`
        // — a `spawn_tasks` issued from `on_run_start`/`on_phase_end` against
        // this handle reaches THIS primary's command loop. The `source_dir` +
        // task-definition clone complete the modern `on_run_start` signature.
        let on_run_start_handle = self.control_plane.to_handle()?;
        let on_run_start_source_dir = self.source_dir.to_string_lossy().into_owned();
        let on_run_start_task_definition_py = self.task_definition_py.clone_ref(py);

        // The finalize closure's per-type `build_worker_command_args`
        // rebuild captures a `Py<PyAny>` reference bump of the task
        // definition.
        let finalize_task_definition_py = self.task_definition_py.clone_ref(py);
        // Discovery-policy captures for the PROMOTED primary's
        // `discover_on_promotion` driver: a SLURM-relocated mode-2 primary
        // inherits the `DiscoveryDebt=Owed` marker (seeded by the submitter's
        // relocated seed) and must run the consumer's `discover_items` policy
        // itself to seed the tasks. Mirror the `make_on_phase_*` GIL-thread
        // capture: a `Py<PyAny>` reference bump of the task definition + the
        // task args, plus the staged-corpus root (`src_network` — the
        // bind-mount the pre-staged corpus lives under, matching the old
        // secondary-discovery `setup_discover_root`) and the phase graph. The
        // policy is registered on the promoted primary unconditionally; the
        // driver gates on `discovery_debt() == Owed`, so it is inert on every
        // non-relocated promotion (a failover snapshot is already `Settled`).
        let discovery_task_definition_py = self.task_definition_py.clone_ref(py);
        let discovery_task_args_py = self.task_args_py.clone_ref(py);
        let discovery_root = self.src_network.clone();
        let discovery_phase_deps = self.phase_deps.clone();
        // Staging/discovery config the PROMOTED primary's recipe threads into
        // its `PrimaryConfig` so a relocated primary's dispatch matches the
        // submitter's (the relocate-staging fix). `pre_staged_root` is the
        // secondary's bind-mount root (`src_network`), consulted only in
        // pre-staged mode; `promote_source_dir` is the local source-tree root
        // the relocated primary re-walks for its initial staging (mode-1
        // file-based). Captured here (before `py.detach`) because the recipe is
        // built inside the detached runtime where `self` is gone.
        let promote_pre_staged_root = self.src_network.clone();
        let promote_source_dir = Some(self.source_dir.clone());
        // The two run-config dispatch flags the PROMOTE recipe stamps into the
        // relocated primary's `PrimaryConfig`, sourced from this node's OWN
        // LOCAL PRODUCER (the `task_definition` / `task_args` it booted with) —
        // NOT the `InitialAssignment`-fed `StagingDispatchContext` cell. A
        // relocate-TARGET never receives an `InitialAssignment` before it is
        // promoted (the setup peer relocates → the target promotes → the
        // PROMOTED primary runs `perform_initial_assignment`), so its cell is
        // still at `Default { pre_staged_mode: false, uses_file_based_items:
        // true }` at the promotion instant — reading it stamps the wrong flags
        // (the relocate-staging bug). The local producer is run-uniform, so it
        // carries the value the submitter primary stamped. Mirrors
        // `managers/primary/new.rs` (`uses_file_based_items`) and the
        // `discover_items_under_gil` pre-staged probe (`source_already_staged`).
        let (promote_uses_file_based_items, promote_pre_staged_mode) =
            extract_staging_dispatch_flags(
                self.task_definition_py.bind(py),
                self.task_args_py.bind(py),
            );
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

        // Phase-lifecycle callbacks the PROMOTED primary fires. Built here
        // under the GIL (the `make_on_phase_*` constructors capture a
        // `Py<PyAny>` clone of `task_definition_py` that the closure body
        // re-binds via `Python::attach` at each fire). Routed through the
        // promote recipe's `PrimaryRunArgs` (the channel `run_pipeline` reads)
        // so they fire on the relocated primary. The closures fire only when
        // this node is the authority that owns the phase machine; a node that
        // never calls into Python pays no GIL-reacquiring cost.
        let sec_on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(self.task_definition_py.clone_ref(py)),
        );
        // Honest `on_phase_end`: the closure records a consumer-hook raise into
        // this latch; the SAME latch is installed on the promoted coordinator
        // (`set_phase_hook_raise_latch` in the recipe) so the phase cascade
        // surfaces the raise as a non-zero `FatalPolicyExit` on the relocated
        // primary (mirrors the submitter `primary/run.rs`).
        let sec_phase_hook_raise_latch =
            dynrunner_manager_distributed::PhaseHookRaiseLatch::new();
        let sec_on_phase_end: crate::managers::lifecycle::OnPhaseEnd =
            Box::new(crate::managers::lifecycle::make_on_phase_end_with_raise_latch(
                self.task_definition_py.clone_ref(py),
                sec_phase_hook_raise_latch.clone(),
            ));

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
                // step: the WSS dial + retry loop, starting the peer
                // mesh, reading the backend's cert + QUIC port into the
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
                // The bootstrap/submitter host's peer-id is
                // `SETUP_NODE_ID` — the same id baked into the submitter's
                // `PrimaryConfig::node_id` and the host-id
                // `Destination::Primary` resolves to while the role table is
                // cold. The matching `set_bootstrap_primary_id` below tells
                // the egress edge to resolve `Destination::Primary` to it
                // (pre-`PrimaryChanged`).
                // Resolve the addresses this node will advertise to peers
                // for them to dial it ON, and emit them — with their
                // resolution source — at startup. This is the operator's
                // one-glance check that the node advertises a peer-routable
                // LAN address: a `source=hostname-probe` or
                // `source=localhost-fallback` IPv4 on a clustered node is
                // the classic "QUIC mesh never forms" cause (the node hands
                // peers a container-internal / bridge / loopback address no
                // other host can reach). Observability only — the resolved
                // values are passed through to the dial params unchanged.
                let (advertised_ipv4, ipv4_source) = detect_ipv4_with_source(None);
                let advertised_ipv6 = detect_ipv6_with_source(None);
                tracing::info!(
                    secondary_id = %secondary_id,
                    advertised_ipv4 = %advertised_ipv4,
                    ipv4_source = ipv4_source.as_str(),
                    advertised_ipv6 = advertised_ipv6
                        .as_ref()
                        .map(|(addr, _)| addr.as_str())
                        .unwrap_or("<none>"),
                    ipv6_source = advertised_ipv6
                        .as_ref()
                        .map(|(_, src)| src.as_str())
                        .unwrap_or("<none>"),
                    "resolved advertised peer-mesh address for this node"
                );
                let mesh_bundle = transport_factory::dial_secondary_mesh::<RunnerIdentifier>(
                    transport_factory::SecondaryDialParams {
                        addr,
                        connect_timeout: dist_connect_timeout,
                        retry_delay: dist_connect_retry_delay,
                        secondary_id: &secondary_id,
                        bootstrap_primary_id: dynrunner_core::SETUP_NODE_ID.to_string(),
                        ipv4_address: Some(advertised_ipv4),
                        ipv6_address: advertised_ipv6.map(|(addr, _)| addr),
                    },
                )
                .await
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
                let peer_network = mesh_bundle.transport;
                let secondary_cert_info = mesh_bundle.peer_cert_info;
                // Cloneable mesh-send capability over the secondary's
                // peer mesh; the secondary's `can_be_primary` marker
                // reads `is_some()`. See `MeshSendHandle`.
                let mesh_send_handle = mesh_bundle.mesh_send;
                // Pillar 1 (mesh-always): a network secondary ALWAYS holds a
                // peer mesh, so it is always primary-capable. The previous
                // `mesh_send_handle.is_some()` advertisement keyed capability
                // off mesh presence — now an invariant, not a runtime branch.
                debug_assert!(
                    mesh_send_handle.is_some(),
                    "mesh-always: a network secondary must always hold a peer mesh"
                );

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
                    // role advertisement): a network compute secondary can host
                    // the primary ON DEMAND — under mesh-always (pillar 1) it
                    // ALWAYS holds a peer mesh, so it can always construct a
                    // `PrimaryCoordinator` when named. Constant `true`
                    // (the `debug_assert!(mesh_send_handle.is_some())` above
                    // pins the invariant the old `is_some()` branch read).
                    // Advertised in the `SecondaryWelcome`; recorded in the
                    // replicated `RoleTable.can_be_primary` the submitter reads.
                    // Only OBSERVERS (and the in-process same-host secondary,
                    // built separately in the distributed manager) advertise
                    // `false`.
                    can_be_primary: true,
                    resource_check_interval: dist_resource_check_interval,
                    log_oom_watcher: dist_log_oom_watcher,
                    promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                    unfulfillable_reinject_max_per_task,
                    mem_manager_reserved_bytes: cfg_mem_manager_reserved_bytes,
                    output_dir: memprofile_output_dir.clone(),
                    memuse_log_path: cfg_memuse_log_path.clone(),
                    // The node-local run-config SEED (boot CLI; usually empty —
                    // the post-welcome push delivers the real value). The
                    // coordinator wraps this into its shared handle; the
                    // promoted-primary recipe reads that handle (post-push), so
                    // the seed is only the starting value, not the recipe's
                    // source — no second copy to keep in sync.
                    forwarded_argv,
                };

                let factory = SubprocessWorkerFactory {
                    python_executable,
                    source_dir,
                    output_dir,
                    log_dir,
                    log_paths,
                    // The SHARED worker-command source: the finalize closure
                    // swaps the per-type `cmd_args` here (post-push, before the
                    // pool spawns) and every spawn reads the swapped value.
                    types: shared_types,
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
                // reaches (`SETUP_NODE_ID`, the same id the mesh-link
                // registration keyed the dialed connection under). The edge
                // resolves `Destination::Primary` to it while the role table
                // is cold (pre-`PrimaryChanged`), so setup frames route to
                // the dialled submitter before the self-announcement lands.
                secondary.set_bootstrap_primary_id(dynrunner_core::SETUP_NODE_ID.to_string());

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
                // plus both detected address families
                // (`network::detect_ipv4_with_source` /
                // `detect_ipv6_with_source` — env-var hint first,
                // `hostname -I` fallback; the resolved address + source was
                // logged at startup just before the mesh dial). It is what
                // the `send_cert_exchange` step ships
                // on the wire and the primary then re-broadcasts via
                // `PeerInfo`. The dialer (peer/dial.rs) consumes both
                // families and happy-eyeballs-races them, so a host that has
                // only one family configured is fine — the missing one is
                // simply absent from the candidate set.
                secondary.set_peer_cert_info(secondary_cert_info);

                // ── Liveness beacon (the runtime-CPU-starvation false-death
                // fix) ───────────────────────────────────────────────────
                //
                // This node is primary-capable, so it BOTH (a) runs a
                // liveness LISTENER (it folds beacon datagrams into the
                // death-clock once it holds the primary role — the receiver
                // is threaded to the promoted primary below) AND (b) runs a
                // dedicated-thread liveness BEACON to whichever peer is
                // currently primary. The beacon thread + its own UdpSocket
                // are independent of this tokio runtime, so they keep
                // asserting liveness even while a co-resident CPU-bound build
                // pegs the core and freezes the runtime (the bug). Bind the
                // listener on an ephemeral UDP port on all interfaces;
                // advertise that port in CertExchange so peers beacon us at
                // our advertised ipv4 + this port once we become primary.
                // Best-effort: a bind failure leaves the node with the
                // frame-based death-clock half only (the union still works,
                // it just loses starvation-immunity for this node) — logged,
                // not fatal.
                let mut liveness_ping_rx_for_recipe: Option<
                    tokio::sync::mpsc::UnboundedReceiver<String>,
                > = None;
                let mut _liveness_beacon: Option<
                    dynrunner_manager_distributed::liveness::LivenessBeacon,
                > = None;
                match dynrunner_manager_distributed::liveness::LivenessListener::bind(
                    "0.0.0.0:0".parse().expect("valid bind addr"),
                    // No run-wide token threaded through the boot path; the
                    // ephemeral per-run port already isolates stale runs.
                    None,
                )
                .await
                {
                    Ok((listener, port, ping_rx, beacon_liveness)) => {
                        secondary.set_liveness_port(port);
                        // Install the listener's POLL view on the secondary so
                        // its failover-detector consults the CURRENT PRIMARY's
                        // beacon as the UNION counterpart of the mesh-frame
                        // legs (the primary→secondaries direction): a
                        // CPU-starved-but-beaconing primary is NOT false-elected.
                        secondary.set_beacon_liveness(beacon_liveness);
                        liveness_ping_rx_for_recipe = Some(ping_rx);
                        // Keep the listener alive for the whole run: its recv
                        // task is already spawned inside `bind`, but the
                        // `LivenessListener` handle's Drop aborts that task, so
                        // park the handle in a detached run-length task. The
                        // node RECEIVES beacons (as primary) regardless of
                        // whether its own outbound beacon spawns below.
                        tokio::task::spawn_local(async move {
                            let _listener = listener;
                            std::future::pending::<()>().await;
                        });
                        // Spawn the dedicated-thread beacon. It reads the
                        // current primary's liveness address the coordinator
                        // publishes into `beacon_target` (re-pointed on each
                        // PrimaryChanged) and sends to it every keepalive
                        // interval — on its OWN OS thread + socket, immune to
                        // this runtime's build-CPU starvation.
                        match dynrunner_manager_distributed::liveness::LivenessBeacon::spawn(
                            secondary_id.clone(),
                            // Per-process breadcrumb token (the listener
                            // accepts any token — see `LivenessListener::bind`).
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos() as u64)
                                .unwrap_or(0),
                            dist_keepalive,
                            secondary.beacon_target(),
                        ) {
                            Ok(beacon) => {
                                _liveness_beacon = Some(beacon);
                                tracing::info!(
                                    liveness_port = port,
                                    "liveness beacon + listener active (transport-independent \
                                     keepalive; survives runtime CPU-starvation)"
                                );
                            }
                            Err(e) => {
                                // Listener stays up (we can still RECEIVE
                                // beacons as primary); only our OUTBOUND beacon
                                // is missing, so peers won't get
                                // starvation-immune liveness FROM us — the
                                // union's frame half still carries us.
                                tracing::warn!(
                                    error = %e,
                                    liveness_port = port,
                                    "liveness beacon thread spawn failed; listener still up, \
                                     but this node emits no beacon (frame-only liveness from it)"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "liveness listener bind failed; running with frame-only \
                             death-clock (no beacon path this run)"
                        );
                    }
                }

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

                // The SHARED run-config handle the coordinator's
                // `store_pushed_run_config` writes (the delivered `forwarded_argv`,
                // post-push). Read up-front because it backs THREE consumers
                // below: the promoted-primary recipe's `PrimaryConfig` (step 7),
                // and — via the `SharedRunConfig` complete-namespace cell built
                // from it — the run-config finalize (worker `cmd_args`) AND the
                // promotion-time discovery driver.
                let promote_run_config_handle = secondary.run_config_handle();

                // The node-local COMPLETE run-config namespace (single source of
                // truth) — `Some` iff a Python finalize callable was supplied
                // (the deferred SLURM reparse path). The finalize and the
                // discovery driver share this ONE handle, so the namespace is
                // reparsed at most once and both read it identically (worker
                // `cmd_args` selection flags == discovery selection flags). On a
                // relocate-target the finalize never fires before promotion (no
                // primary PeerInfo arrives), so DISCOVERY resolves it first; on a
                // plain secondary the finalize resolves it first. `None`
                // (out-of-tree, no finalize) makes both inert.
                let run_config = finalize_run_config.map(|reparse| {
                    crate::managers::run_config::SharedRunConfig::deferred(
                        reparse,
                        promote_run_config_handle.clone(),
                    )
                });

                // Register the consumer's run-config finalize policy. The Rust
                // `SecondaryCoordinator` OWNS the WHEN (it fires this at the
                // `AwaitingPrimary → Configuring` transition, after the
                // post-welcome `RunConfig` push delivers `forwarded_argv`, with
                // an in-band `RequestRunConfig` backstop if it has not landed,
                // and BEFORE the worker pool spawns). This closure is the
                // consumer's POLICY: it resolves the COMPLETE namespace through
                // the SHARED `run_config` cell + rebuilds the per-type `cmd_args`
                // OFF the runtime thread (GIL excursion on a `spawn_blocking`
                // thread, §14/§15) and swaps them into the SHARED worker-command
                // source the factory reads. `None` (no Python finalize supplied)
                // makes it inert.
                secondary.register_finalize_run_config(build_finalize_run_config_fn(
                    run_config.clone(),
                    finalize_task_definition_py,
                    finalize_type_ids,
                    finalize_source_str,
                    finalize_output_str,
                    skip_existing,
                    finalize_shared_types,
                ));

                // Compose the compute-peer `Node`: a secondary that may be
                // PROMOTED to primary. `Node::new` hands out the
                // `promotion_tx` the secondary signals on a self-named
                // `PrimaryChanged`; `register_promotion_signal` wires it. The
                // `promote` recipe (below) is what `Node::run` calls on that
                // signal to BUILD the snapshot-seeded `PrimaryCoordinator` —
                // the secondary NEVER constructs a primary (SUPREME-LAW #3).
                let (node, promotion_tx) = Node::new(mesh);
                secondary.register_promotion_signal(promotion_tx);

                // The promoted primary's build recipe. Captures the config
                // template + the command channel + the phase callbacks the
                // PROMOTED primary owns (not the secondary — per R4 the
                // secondary holds no phase machine). Invoked at most once, on
                // promotion, with the converged snapshot the secondary
                // captured on the signal.
                // The consumer's discovery policy for the promoted primary: a
                // mode-2 SLURM-relocated primary inherits `DiscoveryDebt=Owed`
                // and runs `discover_on_promotion`, which consults this policy
                // to seed the staged corpus. `build_setup_discovery_fn` runs
                // `task.discover_items(<root>, args)` OFF the runtime thread
                // (§14/§15); paired with the run's phase graph (the consumer
                // declares it independent of discovery). Inert on a non-mode-2
                // promotion (the driver short-circuits when debt != Owed).
                // Discovery resolves the COMPLETE namespace through the SAME
                // `run_config` cell the finalize uses (single source of truth).
                // When no finalize callable was supplied (out-of-tree / no
                // deferred reparse), fall back to the boot `task_args` as the
                // pre-resolved namespace — the historical discovery contract.
                let discovery_run_config = run_config.clone().unwrap_or_else(|| {
                    crate::managers::run_config::SharedRunConfig::pre_resolved(
                        discovery_task_args_py,
                    )
                });
                // The on_run_start fire reads the SAME complete-namespace cell
                // (single source of truth) — a clone shares the resolved-value
                // cell, so on_run_start, discovery, and the finalize all see ONE
                // namespace.
                let on_run_start_run_config = discovery_run_config.clone();
                let setup_discovery = dynrunner_manager_distributed::SetupDiscovery {
                    discover: build_setup_discovery_fn(
                        discovery_task_definition_py,
                        discovery_run_config,
                        discovery_root,
                    ),
                    phase_deps: discovery_phase_deps,
                };
                // Capture a clone of the node's peer→liveness-address book
                // (populated by THIS secondary from PeerInfo) BEFORE the
                // secondary is moved into the node, so the promoted primary's
                // beacon can resolve its secondaries' raw beacon addresses.
                // Gated on the listener binding (same condition as the ping
                // receiver): no listener ⇒ no beacon infrastructure this run.
                let promote_peer_liveness_addrs = liveness_ping_rx_for_recipe
                    .as_ref()
                    .map(|_| secondary.peer_liveness_addrs());
                let promote = build_promoted_primary_recipe(PromotedPrimaryRecipeInputs {
                    secondary_id: secondary_id.clone(),
                    keepalive_interval: dist_keepalive,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    scheduler_config,
                    estimator,
                    // SLURM secondary: each process owns its own Python
                    // `PrimaryHandle` command channel, so the promoted primary
                    // drains it.
                    command_channel: Some((command_tx, command_rx)),
                    on_phase_start: sec_on_phase_start,
                    on_phase_end: sec_on_phase_end,
                    phase_hook_raise_latch: sec_phase_hook_raise_latch,
                    // SLURM path: the relocated primary must fire `on_run_start`
                    // with its OWN live handle (the submitter's fired in a
                    // DIFFERENT process and is dead post-relocation).
                    on_run_start: Some(OnRunStartContext {
                        task_definition_py: on_run_start_task_definition_py,
                        source_dir: on_run_start_source_dir,
                        run_config: on_run_start_run_config,
                        primary_handle: on_run_start_handle,
                    }),
                    forwarded_argv: promote_run_config_handle,
                    uses_file_based_items: promote_uses_file_based_items,
                    pre_staged_mode: promote_pre_staged_mode,
                    source_pre_staged_root: promote_pre_staged_root,
                    source_dir: promote_source_dir,
                    setup_discovery: Some(setup_discovery),
                    // The node-bound liveness listener's ping receiver: the
                    // promoted primary folds beacon datagrams into the
                    // death-clock (union). `None` if the listener didn't bind.
                    liveness_ping_rx: liveness_ping_rx_for_recipe,
                    // The node's peer→liveness-address book (captured above):
                    // the promoted primary's own beacon resolves its
                    // secondaries' addresses from it (primary→secondaries).
                    peer_liveness_addrs: promote_peer_liveness_addrs,
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

/// The Python `PrimaryHandle`'s command-channel ends (sender + receiver) for a
/// promoted primary's `replace_command_channel`. `pub(crate)` alias so the
/// recipe-input field stays a single named type rather than an inline nested
/// tuple-of-channels.
pub(crate) type PromotedCommandChannel = (
    tokio::sync::mpsc::Sender<dynrunner_manager_distributed::primary::PrimaryCommand<RunnerIdentifier>>,
    tokio::sync::mpsc::Receiver<dynrunner_manager_distributed::primary::PrimaryCommand<RunnerIdentifier>>,
);

/// Node-local context for firing `on_run_start` on the PROMOTED primary.
///
/// Single concern: "what does the relocated primary hand the consumer's
/// `on_run_start(source_dir, output_dir, args, primary_handle)` hook?". Built
/// on the GIL thread (Py-handle ref-bumps) and consumed once, inside the
/// recipe, under `Python::attach`.
///
/// `Some` ONLY on the SLURM secondary path: the consumer's `on_run_start`
/// fired on the SUBMITTER process with the SUBMITTER's `PrimaryHandle`, which
/// is dead once the submitter relocates into an observer — so the compute-node
/// relocated primary must fire `on_run_start` AGAIN with its OWN live handle so
/// the lazy-injection pattern (`on_phase_end → primary_handle.spawn_tasks`)
/// reaches THIS primary. `None` on the in-process `--multi-computer local`
/// path: that submitter's `on_run_start` already fired in the SAME process with
/// the one live handle (re-firing would double-invoke the hook).
pub(crate) struct OnRunStartContext {
    /// The consumer `TaskDefinition` whose `on_run_start` is fired.
    pub task_definition_py: Py<PyAny>,
    /// The local source-tree root — the `source_dir` positional arg.
    pub source_dir: String,
    /// The COMPLETE run-config namespace cell (single source of truth, shared
    /// with discovery): supplies the `args` Namespace AND resolves the
    /// node-local `output_dir` ([`resolve_node_local_output_root`] — the D↔G
    /// converged resolver).
    pub run_config: crate::managers::run_config::SharedRunConfig,
    /// The relocated primary's OWN live `PrimaryHandle` (its command channel is
    /// the SAME the recipe threads via `replace_command_channel`), handed to
    /// the consumer so `spawn_tasks` reaches THIS primary's command loop.
    pub primary_handle: crate::managers::primary_handle::PyPrimaryHandle,
}

/// Inputs to [`build_promoted_primary_recipe`] — everything the promoted
/// primary's build needs that is captured on the GIL thread / from config.
///
/// `pub(crate)` so the in-process `--multi-computer local` manager
/// (`managers/distributed/run.rs`) can build the SAME transport-agnostic
/// recipe for its promotable in-process secondaries (it relocates the setup
/// peer onto one of them), reusing the SLURM submitter's recipe builder rather
/// than duplicating the promoted-primary construction.
pub(crate) struct PromotedPrimaryRecipeInputs {
    pub secondary_id: String,
    pub keepalive_interval: std::time::Duration,
    pub peer_timeout: std::time::Duration,
    pub keepalive_miss_threshold: u32,
    pub retry_max_passes: u32,
    pub oom_retry_max_passes: u32,
    pub scheduler_config: SchedulerConfig,
    pub estimator: PyMemoryEstimatorBridge,
    /// The Python `PrimaryHandle`'s command channel ends, iff this node's
    /// promoted primary should drain externally-issued `spawn_tasks`/`reinject`
    /// from a Python handle (the SLURM secondary path: each secondary process
    /// owns its own handle). Moved into the promoted primary via
    /// `replace_command_channel` on the single recipe fire. `None` on the
    /// in-process `--multi-computer local` path: the ONE Python handle is held
    /// by the setup peer (the bootstrap primary), so the promoted in-process
    /// primary keeps the internal command channel `PrimaryCoordinator::new`
    /// minted — the run loop is fully driven; only runtime `spawn_tasks` via
    /// that one Python handle does not re-route to the relocated primary.
    pub command_channel: Option<PromotedCommandChannel>,
    /// The phase-lifecycle callbacks the PROMOTED primary fires (it owns the
    /// phase machine; the secondary does not — R4 seam). Routed through
    /// `PrimaryRunArgs` (the channel `run_pipeline` actually reads) so they
    /// REPLACE the no-op closures — the bug-D one-line clobber.
    pub on_phase_start: crate::managers::lifecycle::OnPhaseStart,
    pub on_phase_end: crate::managers::lifecycle::OnPhaseEnd,
    /// The raise-latch the honest `on_phase_end` (built via
    /// `make_on_phase_end_with_raise_latch`) records a consumer-hook raise
    /// into. Installed on the promoted coordinator via
    /// `set_phase_hook_raise_latch` BEFORE `run` enters, so a relocated
    /// primary's `on_phase_end` raise surfaces a non-zero `FatalPolicyExit`
    /// (mirrors the submitter `primary/run.rs`). A `detached()` latch (nobody
    /// reads it) keeps the warn-and-continue contract for callers that do not
    /// wire an honest exit.
    pub phase_hook_raise_latch: dynrunner_manager_distributed::PhaseHookRaiseLatch,
    /// Node-local context for firing `on_run_start` on the promoted primary.
    /// `Some` on the SLURM path (the relocated primary must hand the consumer
    /// its OWN live handle + node-local `output_dir` so lazy injection reaches
    /// THIS primary); `None` on the in-process path (the submitter's
    /// `on_run_start` already fired in-process). Fired ONCE inside the recipe,
    /// before the run loop spawns.
    pub on_run_start: Option<OnRunStartContext>,
    /// The SHARED node-local run-config handle (single source of truth —
    /// `store_pushed_run_config` is the one writer). Read `.lock().clone()` at
    /// the promotion instant (always AFTER the post-welcome push landed), so
    /// the promoted `PrimaryConfig.forwarded_argv` carries the DELIVERED argv
    /// — byte-identical to the original submitter (no split-brain) — rather
    /// than the stale boot copy a pre-push capture would have frozen (step 7).
    pub forwarded_argv: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    /// Whether the run's dispatched items are real files
    /// (`TaskDefinition.uses_file_based_items`). Read from this node's OWN
    /// LOCAL PRODUCER (the `task_definition` it booted with) — NOT from the
    /// `InitialAssignment`-fed `StagingDispatchContext` cell, because a
    /// relocate-TARGET never receives an `InitialAssignment` before it is
    /// promoted (the setup peer relocates, then the target promotes, then the
    /// PROMOTED primary runs `perform_initial_assignment`), so its cell is
    /// still at `Default { uses_file_based_items: true }` when the recipe
    /// fires. The local producer is run-uniform (every node booted from the
    /// same consumer `task_definition`), so it carries the value the submitter
    /// primary stamped. Threaded into `PrimaryConfig.uses_file_based_items` so
    /// the promoted primary's own `InitialAssignment` re-stamps it identically;
    /// without it a worker re-requires a StageFile for a no-file item (the
    /// relocate-staging bug). The dispatch resolver (`resolve_for_dispatch`)
    /// keeps reading the wire-fed cell — a PLAIN secondary executing assigned
    /// tasks always has a populated cell, so that path is unchanged.
    pub uses_file_based_items: bool,
    /// Whether the run is in pre-staged mode (`--source-already-staged`).
    /// Sourced from this node's OWN LOCAL PRODUCER (the SLURM secondary reads
    /// `task_args.source_already_staged` non-None — mirroring the submitter's
    /// `source_pre_staged_root.is_some()`; the in-process path passes
    /// `source_pre_staged_root.is_some()` directly) for the SAME reason
    /// `uses_file_based_items` is: the relocate-target's wire-fed cell is at
    /// its `pre_staged_mode: false` default at promotion. Gates whether
    /// `source_pre_staged_root` is threaded into the promoted `PrimaryConfig`.
    pub pre_staged_mode: bool,
    /// The staged-corpus root the secondary's source is bind-mounted under
    /// (`--source-already-staged` → the secondary's `src_network`). Threaded
    /// into the promoted `PrimaryConfig.source_pre_staged_root` IFF the
    /// staging context says `pre_staged_mode` — so the promoted primary's
    /// `wire_local_path` strips the SAME prefix the submitter did. `None`
    /// outside pre-staged mode (the field is consulted only when
    /// `pre_staged_mode` is set).
    pub source_pre_staged_root: Option<std::path::PathBuf>,
    /// The local source-tree root the promoted primary reads file contents
    /// from for its initial staging walk (mode-1 file-based, non-pre-staged
    /// runs). Threaded into the promoted `PrimaryConfig.source_dir` so
    /// `maybe_auto_stage_initial` re-emits the StageFile records the worker
    /// dispatch requires — the relocated primary RE-STAGES from scratch (it
    /// re-runs the full pre-loop chain), so it needs the same source root the
    /// submitter had. `None` for callers without a local source root
    /// (`uses_file_based_items=false` / pre-staged / tests).
    pub source_dir: Option<std::path::PathBuf>,
    /// The consumer's discovery policy + phase graph for the PROMOTED primary's
    /// `discover_on_promotion` driver (mode-2 relocate — SLURM submitter OR the
    /// in-process `--source-already-staged` setup peer that relocates onto this
    /// secondary). Single-use (the `discover` closure is a `FnMut` that consumes
    /// its `Py` handles on the one fire, not `Clone`), so it is `take`-n on the
    /// recipe's single invocation. `None` only for a caller that does not supply
    /// a discovery policy (the in-process COLD path, where the corpus was
    /// cold-seeded and the marker is `Settled`); inert on a non-relocated
    /// promotion (the driver gates on `Owed`).
    pub setup_discovery:
        Option<dynrunner_manager_distributed::SetupDiscovery<RunnerIdentifier>>,
    /// The liveness-beacon ping receiver, from the node-bound
    /// [`dynrunner_manager_distributed::process`]-external
    /// `LivenessListener`. Installed on the promoted primary via
    /// `set_liveness_ping_rx` on the single recipe fire so a promoted node
    /// folds beacon liveness into the death-clock (union). Single-use
    /// (an `mpsc::UnboundedReceiver` is one-owner), captured in an `Option`
    /// and taken on the fire — same single-use discipline as the command
    /// channel + phase callbacks. `None` when no listener was bound.
    pub liveness_ping_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
    /// The node-scoped peer→liveness-address book (a clone of the one the
    /// co-located `SecondaryCoordinator` populated from `PeerInfo`). The
    /// promoted primary reads it to resolve its secondaries' raw beacon
    /// addresses for its OWN dedicated-thread liveness beacon (the
    /// PRIMARY→secondaries direction): a CPU-starved promoted primary keeps
    /// beaconing its secondaries so they do not false-elect a successor.
    /// Shared (not single-use): the recipe READS it to seed the primary's
    /// `set_peer_liveness_addrs`, leaving the secondary's copy intact.
    /// `None` for callers without a listener (no beacon emitted).
    pub peer_liveness_addrs:
        Option<dynrunner_manager_distributed::liveness::PeerLivenessAddrs>,
}

/// Read the run's two staging-dispatch flags from this node's OWN LOCAL
/// PRODUCER — the consumer `task_definition` + `task_args` it booted with.
///
/// Single concern: "what does the local producer say the run's dispatch mode
/// is?". This is the SOURCE the promote recipe must consult (NOT the
/// `InitialAssignment`-fed `StagingDispatchContext` cell): a relocate-TARGET
/// has no `InitialAssignment` before it is promoted, so its cell is still at
/// `Default { pre_staged_mode: false, uses_file_based_items: true }`. The local
/// producer is run-uniform (every node booted from the same consumer
/// `task_definition`), so it carries the value the original submitter primary
/// stamped.
///
/// Mirrors the two existing reads verbatim:
///   * `uses_file_based_items` — `managers/primary/new.rs`: missing/unparseable
///     attribute defaults to `true` (the historical file-based contract).
///   * `pre_staged_mode` — the `discover_items_under_gil` pre-staged probe:
///     `task_args.source_already_staged` non-`None`, mirroring the submitter's
///     `source_pre_staged_root.is_some()`.
fn extract_staging_dispatch_flags(
    task_definition: &Bound<'_, PyAny>,
    task_args: &Bound<'_, PyAny>,
) -> (bool, bool) {
    let uses_file_based_items: bool = task_definition
        .getattr("uses_file_based_items")
        .ok()
        .and_then(|v| v.extract().ok())
        .unwrap_or(true);
    let pre_staged_mode = task_args
        .getattr("source_already_staged")
        .ok()
        .filter(|v| !v.is_none())
        .is_some();
    (uses_file_based_items, pre_staged_mode)
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
///
/// `pub(crate)` so the in-process `--multi-computer local` manager reuses this
/// transport-agnostic builder for its promotable in-process secondaries (it
/// takes `client, inbox, demote_rx, snapshot` — all mesh-backend-opaque).
pub(crate) fn build_promoted_primary_recipe(
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
        scheduler_config,
        estimator,
        command_channel,
        on_phase_start,
        on_phase_end,
        phase_hook_raise_latch,
        on_run_start,
        forwarded_argv,
        uses_file_based_items,
        pre_staged_mode,
        source_pre_staged_root,
        source_dir,
        setup_discovery,
        liveness_ping_rx,
        peer_liveness_addrs,
    } = inputs;
    // Single-use: an `mpsc::UnboundedReceiver` is one-owner, so capture it
    // in an `Option` and `take` on the recipe's single fire (a node
    // promotes at most once per lifetime — same discipline as the command
    // channel / phase callbacks).
    let mut liveness_ping_rx = liveness_ping_rx;
    // Single-use on the one recipe fire (a node promotes at most once): the
    // promoted primary spawns its OWN dedicated-thread liveness beacon from
    // this book. `take`-n in the closure like `liveness_ping_rx`.
    let mut peer_liveness_addrs = peer_liveness_addrs;
    // Single-use pieces captured in Options so the FnMut can take them on its
    // one invocation (a node promotes at most once per lifetime). The command
    // channel is already an `Option` (it is `None` on the in-process path,
    // where the promoted primary keeps the internal channel `new` minted).
    let mut command_channel = command_channel;
    // The phase callbacks are routed through `PrimaryRunArgs` (the channel
    // `run_pipeline` reads — the bug-D clobber fix) rather than the dead
    // `register_phase_lifecycle_callbacks`. Captured in an `Option` so the
    // `FnMut` recipe takes the move-only `OnPhaseStart`/`OnPhaseEnd` on its one
    // fire.
    let mut phase_callbacks = Some((on_phase_start, on_phase_end));
    // The on_run_start context (Some on SLURM, None in-process) is single-use
    // (the move-only Py handles + the PrimaryHandle); take it on the one fire.
    let mut on_run_start = on_run_start;
    // The discovery policy is already `Option`-wrapped (it carries an `FnMut`
    // `discover` closure that is not `Clone`); take it on the single fire.
    let mut setup_discovery = setup_discovery;
    // The run-config handle is shared (not single-use): the recipe READS it at
    // promotion, leaving the secondary's copy intact. No `Option`/`take` — it
    // is cloned-in and read by value at the promotion instant. The two staging
    // flags (`uses_file_based_items` / `pre_staged_mode`) were extracted from
    // this node's OWN local producer on the GIL thread and captured by value;
    // they are NOT read off the `InitialAssignment`-fed cell (a relocate-target
    // has no `InitialAssignment` before promotion — its cell is at `Default`).
    Box::new(move |client, inbox, demote_rx, snapshot| {
        let config = PrimaryConfig {
            node_id: secondary_id.clone(),
            keepalive_interval,
            peer_timeout,
            keepalive_miss_threshold,
            retry_max_passes,
            oom_retry_max_passes,
            // The DELIVERED node-local run-config, read off the shared handle
            // at the promotion instant (post-push, so it reflects the value the
            // primary unicast — not the empty boot seed). Threaded so the
            // promoted primary re-serves `RequestRunConfig` with the SAME argv
            // — byte-identical to the original submitter.
            forwarded_argv: forwarded_argv
                .lock()
                .expect("forwarded_argv mutex poisoned")
                .clone(),
            // Carry the run's staging/discovery context so the relocated
            // primary's dispatch matches the submitter's (the relocate-staging
            // fix). `uses_file_based_items` and `source_pre_staged_root` feed
            // `assignment.rs`'s `InitialAssignment` stamps + `wire_local_path`;
            // `source_dir` feeds `maybe_auto_stage_initial`'s re-walk (the
            // relocated primary re-stages from scratch). Both staging flags are
            // sourced from this node's OWN local producer (extracted on the GIL
            // thread), NOT the `InitialAssignment`-fed cell — the relocate-
            // target's cell is at `Default` at promotion. `source_pre_staged_root`
            // is consulted only in pre-staged mode — mirror the submitter's
            // `is_some()` discriminant by gating on `pre_staged_mode`.
            uses_file_based_items,
            source_pre_staged_root: if pre_staged_mode {
                source_pre_staged_root.clone()
            } else {
                None
            },
            source_dir: source_dir.clone(),
            ..PrimaryConfig::default()
        };
        let mut primary = PrimaryCoordinator::new(
            config,
            client,
            inbox,
            demote_rx,
            // A promotion-built primary: this host won the role on failover /
            // relocation and IS a compute peer. Its seed is
            // `SeedSource::PromotionSnapshot` (below) ⇒
            // `BootstrapRole::PromotedDestination`, so `run_pipeline` runs the
            // operational loop in place and never relocates again — no
            // construction-time policy needed; the seed is the discriminator.
            scheduler_config.build_memory_scheduler(),
            estimator.clone(),
        );
        // Transfer the Python `PrimaryHandle`'s command channel so an
        // externally-issued `spawn_tasks` / `reinject` (e.g. from a promoted
        // node's `on_phase_end`) reaches THIS primary's command loop.
        if let Some((tx, rx)) = command_channel.take() {
            primary.replace_command_channel(tx, rx);
        }
        // Hand the node-bound liveness-beacon ping receiver to THIS promoted
        // primary so its operational loop folds beacon datagrams into the
        // per-secondary death-clock (the union half). The listener stays
        // bound on the node's runtime across the promotion; only the
        // receiver moves to whichever primary is active.
        if let Some(rx) = liveness_ping_rx.take() {
            primary.set_liveness_ping_rx(rx);
        }
        // Spawn the PROMOTED primary's OWN dedicated-thread liveness beacon
        // (the PRIMARY→secondaries direction). The promoted primary's NODE
        // keeps its co-located worker-secondary running builds, so its
        // single-threaded tokio runtime is CPU-starved exactly like any
        // compute node — its OUTBOUND mesh keepalive freezes, and its
        // secondaries would false-elect a successor against a still-alive
        // primary. This off-runtime beacon (its own OS thread + UdpSocket)
        // keeps asserting the primary's liveness through the starvation. The
        // book (populated by the co-located secondary from PeerInfo) is the
        // promoted primary's only source of its secondaries' beacon
        // addresses; the coordinator publishes the live-secondary subset into
        // its `beacon_target` each heartbeat tick (`publish_beacon_targets`).
        // Best-effort: a bind failure leaves the secondaries on the
        // mesh-frame liveness legs alone — logged, not fatal.
        if let Some(book) = peer_liveness_addrs.take() {
            primary.set_peer_liveness_addrs(book);
            match dynrunner_manager_distributed::liveness::LivenessBeacon::spawn(
                secondary_id.clone(),
                // Per-process breadcrumb token (the listener accepts any
                // token — the ephemeral per-run port isolates stale runs).
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0),
                keepalive_interval,
                primary.beacon_target(),
            ) {
                Ok(beacon) => {
                    primary.set_primary_beacon(beacon);
                    tracing::info!(
                        "promoted primary liveness beacon active (transport-independent \
                         primary→secondaries keepalive; survives runtime CPU-starvation)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "promoted primary liveness beacon spawn failed; secondaries fall \
                         back to mesh-frame liveness legs alone for this primary"
                    );
                }
            }
        }
        // Install the phase-hook raise-latch BEFORE `run` enters (pre-run
        // setter contract, mirroring the submitter `primary/run.rs:444`) so a
        // relocated primary's `on_phase_end` raise surfaces a non-zero
        // `FatalPolicyExit` rather than warn-and-continue. The honest
        // `on_phase_end` (built via `make_on_phase_end_with_raise_latch`)
        // records into THIS latch.
        primary.set_phase_hook_raise_latch(phase_hook_raise_latch.clone());
        // Register the consumer's discovery policy so a mode-2 SLURM-relocated
        // primary (which inherits `DiscoveryDebt=Owed` from the submitter's
        // relocated seed) can run `discover_on_promotion` and seed the staged
        // corpus itself. Inert on a non-relocated promotion: the driver gates
        // on `discovery_debt() == Owed`, and a failover-promotion snapshot is
        // already `Settled`, so the policy is never consulted there.
        if let Some(sd) = setup_discovery.take() {
            primary.register_setup_discovery(sd);
        }
        // Seed from the promoting host's converged snapshot (NORMAL pre-`run`
        // construction input — not a `run_activated` resume, which is gone):
        // restore the ledger + rebuild the derived pool/roster caches, then
        // the primary enters the ordinary `run` path and originates
        // `PrimaryChanged` itself.
        primary.seed_from_promotion_snapshot(snapshot);

        // Fire `on_run_start` on the relocated primary (SLURM path only — the
        // in-process submitter already fired it in-process). The consumer
        // receives this primary's OWN live `PrimaryHandle` + the node-local
        // `output_dir` so its lazy-injection pattern (`on_phase_end →
        // primary_handle.spawn_tasks`) reaches THIS primary's command loop. A
        // raise aborts the run (the consumer's setup failed) — surfaced as a
        // `Failed` terminal at the node boundary. Runs synchronously on the
        // node runtime thread; this is a one-time PRE-RUN hook (this primary's
        // operational loop / keepalives have not started yet), so it does not
        // stall an in-flight pump.
        if let Some(ctx) = on_run_start.take() {
            fire_on_run_start_on_promoted_primary(&ctx);
        }

        // Take the real phase callbacks into `PrimaryRunArgs` — the channel
        // `run_pipeline` reads (`coordinator.rs:2640-2641`). This REPLACES the
        // dead `register_phase_lifecycle_callbacks` path whose registration the
        // run-args no-ops used to clobber (bug D). The promoted primary owns
        // the phase machine; the secondary does not (R4 seam).
        let (on_phase_start, on_phase_end) = phase_callbacks.take().expect(
            "promoted-primary recipe fires at most once; phase callbacks must be present",
        );
        PromotedPrimary {
            coordinator: primary,
            // The snapshot already carries the tasks + phase-deps and was
            // restored + hydrated by `seed_from_promotion_snapshot` above, so
            // the promoted primary enters `run` on the inherited CRDT: its
            // run-init originates nothing and just re-hydrates.
            run_args: PrimaryRunArgs {
                seed: SeedSource::PromotionSnapshot,
                on_phase_start,
                on_phase_end,
            },
        }
    })
}

/// Fire the consumer's `on_run_start` on the relocated primary under the GIL.
///
/// Single concern: hand the consumer `(source_dir, node-local output_dir,
/// complete-namespace args, this primary's live handle)`. The node-local
/// `output_dir` is the D↔G converged value [`resolve_node_local_output_root`]
/// computes from the COMPLETE namespace; the `args` Namespace is that same
/// complete namespace (the single source of truth shared with discovery). A
/// resolve/hook failure logs at error and is swallowed at this seam — the run
/// loop's own honesty signals (the phase-hook raise-latch, discovery's
/// `Err`-abort) carry genuine terminal failures; a `Python::attach`-side panic
/// here would unwind across the node runtime thread.
fn fire_on_run_start_on_promoted_primary(ctx: &OnRunStartContext) {
    Python::attach(|py| {
        let result = (|| -> Result<(), String> {
            let args_owned = ctx.run_config.resolve_under_gil(py)?;
            let args = args_owned.bind(py);
            // `on_run_start` REQUIRES an output_dir (its signature is
            // positional). The complete namespace always carries `--output`
            // (a required CLI flag), so the lenient `None` branch is a genuine
            // misconfiguration here — surface it rather than passing an empty
            // dir.
            let output_dir = resolve_node_local_output_root(py, args)?.ok_or_else(|| {
                "on_run_start: the run-config namespace has no `output` attribute \
                 to resolve a node-local output_dir from"
                    .to_string()
            })?;
            let handle = ctx
                .primary_handle
                .clone()
                .into_pyobject(py)
                .map_err(|e| format!("failed to convert PrimaryHandle to a Python object: {e}"))?
                .into_any()
                .unbind();
            crate::managers::lifecycle::fire_on_run_start(
                ctx.task_definition_py.bind(py),
                &ctx.source_dir,
                &output_dir,
                args,
                Some(handle),
            )
            .map_err(|e| format!("TaskDefinition.on_run_start raised: {e}"))
        })();
        if let Err(e) = result {
            tracing::error!(
                error = %e,
                "on_run_start on the relocated primary failed; the consumer's \
                 lazy-injection setup did not complete"
            );
        }
    });
}

/// Build the consumer's setup-discovery policy closure.
///
/// The returned [`dynrunner_manager_distributed::SetupDiscoveryFn`] runs
/// Python's `task.discover_items(<root>, args)` and converts the result
/// through the workspace-shared `extract_binaries`. Pre-staged
/// (`--source-already-staged`) discovery runs on the corpus-mounting node.
///
/// # Non-block correctness (§14/§15)
///
/// The run loop shares ONE single-threaded runtime with the `Node`'s
/// mesh-pump. Running the GIL excursion ON that thread would stall the pump
/// → keepalives stop → a peer declares the node dead → STRAND. So each
/// invocation runs the GIL excursion on a `tokio::task::spawn_blocking`
/// thread and the returned future merely `.await`s that handle — yielding
/// the runtime thread to the pump, which keeps the mesh alive (keepalives
/// flowing) for the whole discovery duration, however slow the
/// `--source-already-staged` scan is.
///
/// The `Send` Python handles are captured in an `Option` and MOVED into the
/// blocking task on the first (only) invocation, so an `FnMut` that consumes
/// its handles via `take()` is sufficient and avoids any off-GIL `Py` clone
/// (which would need a `Python` token). A defensive second invocation
/// surfaces a clear error rather than panicking.
///
/// Wired onto the PROMOTED primary's `discover_on_promotion` driver via
/// [`build_promoted_primary_recipe`]: a mode-2 SLURM-relocated primary runs
/// this policy when its CRDT declares `DiscoveryDebt=Owed`. Also consumed by
/// the in-process `--source-already-staged` local primary
/// (`managers/distributed/run.rs`), which seeds `DiscoveryDebt=Owed` and runs
/// the same driver on the host fs (it does not relocate — the driver gates on
/// the marker, not on relocation).
///
/// `run_config` is the COMPLETE-namespace single source of truth
/// ([`crate::managers::run_config::SharedRunConfig`]) the discovery body
/// resolves under the GIL — NOT the stale boot Namespace. Shared with the
/// run-config finalize (worker `cmd_args`) on the SLURM path so both read ONE
/// namespace.
pub(crate) fn build_setup_discovery_fn(
    task_definition_py: Py<PyAny>,
    run_config: crate::managers::run_config::SharedRunConfig,
    setup_discover_root: Option<std::path::PathBuf>,
) -> dynrunner_manager_distributed::SetupDiscoveryFn<RunnerIdentifier> {
    // Captured once; taken on the single invocation.
    let mut handles = Some((task_definition_py, run_config, setup_discover_root));
    Box::new(move || {
        let taken = handles.take();
        let fut = async move {
            let Some((task_definition_py, run_config, setup_discover_root)) = taken else {
                return Err(
                    "setup-discovery policy invoked more than once — a second \
                     invocation is a programmer error"
                        .to_string(),
                );
            };
            // Capture the role span CURRENT on the runtime thread (this future
            // is awaited inside the primary coordinator's role-instrumented
            // `discover_on_promotion`, so `Span::current()` is the primary role
            // span). `spawn_blocking` otherwise DETACHES the span context, so
            // the consumer's `discover_items` Python logging — forwarded into
            // tracing by the Python→tracing bridge — would carry no role span
            // and route to NO per-role file. Re-entering the captured span on
            // the blocking thread restores the attribution, so those bridged
            // records land in the relocated primary's `primary.log`. This is
            // the "run the emit on the role-tagged thread" half of the bridge;
            // `py_log` itself stays role-agnostic.
            let role_span = tracing::Span::current();
            // Run the GIL excursion OFF the runtime thread so the mesh-pump
            // keeps the keepalives flowing during discovery (§14/§15).
            tokio::task::spawn_blocking(move || {
                let _role_guard = role_span.enter();
                Python::attach(
                    |py| -> Result<Vec<(TaskInfo<RunnerIdentifier>, bool)>, String> {
                        discover_items_under_gil(
                            py,
                            &task_definition_py,
                            &run_config,
                            setup_discover_root.as_ref(),
                        )
                    },
                )
            })
            .await
            .map_err(|e| format!("setup-discovery blocking task panicked/aborted: {e}"))?
        };
        Box::pin(fut)
            as Pin<
                Box<dyn Future<Output = Result<Vec<(TaskInfo<RunnerIdentifier>, bool)>, String>>>,
            >
    })
}

/// Captured inputs for the run-config finalize closure's per-type
/// `build_worker_command_args` rebuild — bundled so the `spawn_blocking`
/// closure moves one value and the GIL body takes refs into it.
struct FinalizeCaptures {
    /// The COMPLETE run-config namespace (single source of truth) the finalize
    /// resolves and the discovery driver shares. Resolving it here caches the
    /// reparse so a later promotion-time discovery reads the SAME namespace.
    run_config: crate::managers::run_config::SharedRunConfig,
    /// The Python `task_definition` (for `build_worker_command_args`).
    task_definition_py: Py<PyAny>,
    /// The declared `TypeId` strings to rebuild cmd_args for.
    type_ids: Vec<String>,
    source_str: String,
    output_str: String,
    skip_existing: bool,
    /// The shared worker-command source the rebuilt cmd_args swap into.
    shared_types: crate::task_def::SharedTypeRegistry,
}

/// Build the consumer's run-config finalize policy closure.
///
/// The returned [`dynrunner_manager_distributed::FinalizeRunConfigFn`] is
/// invoked ONCE by the `SecondaryCoordinator` at the
/// `AwaitingPrimary → Configuring` transition — after the post-welcome
/// `RunConfig` push delivers the consumer's `forwarded_argv`, BEFORE the
/// worker pool spawns. Given the delivered argv, it calls the Python
/// `finalize_run_config(delivered_argv)` to re-parse the full argparse
/// namespace, re-runs `task_definition.build_worker_command_args(type_id,
/// new_args, source, output, skip_existing)` per declared type (the EXACT
/// path `LoadedTaskDefinition::from_python` uses at boot), and swaps the
/// resulting per-type `cmd_args` into the SHARED [`SharedTypeRegistry`] the
/// factory reads at every spawn.
///
/// `None` `run_config` → no-op (the cmd_args stay the boot-CLI build). This is
/// the out-of-tree path (a caller driving the secondary directly with no
/// Python dispatcher closure). The `args=` consumer path (compiler_suit)
/// supplies an IDENTITY-callable-backed [`SharedRunConfig`] instead (Some) —
/// its worker argv does not depend on the forwarded run-config, so the seam
/// fires but rebuilds a byte-identical cmd_args.
///
/// The `run_config` is the SAME [`SharedRunConfig`] handle the discovery driver
/// reads, so the namespace the finalize resolves is cached for a later
/// promotion-time discovery (single source of truth — one reparse).
///
/// # Non-block correctness (§14/§15)
///
/// Mirrors [`build_setup_discovery_fn`] exactly: the GIL excursion runs on a
/// `tokio::task::spawn_blocking` thread and the returned future merely
/// `.await`s the handle, yielding the secondary's single-threaded runtime to
/// the `Node`'s mesh-pump so keepalives keep flowing while Python re-parses /
/// rebuilds. The captured handles are taken on the first (only) invocation
/// (fire-once upstream); a defensive second invocation surfaces a clear
/// error.
fn build_finalize_run_config_fn(
    run_config: Option<crate::managers::run_config::SharedRunConfig>,
    task_definition_py: Py<PyAny>,
    type_ids: Vec<String>,
    source_str: String,
    output_str: String,
    skip_existing: bool,
    shared_types: crate::task_def::SharedTypeRegistry,
) -> dynrunner_manager_distributed::FinalizeRunConfigFn {
    // No run-config handle supplied → an inert closure (no-op). The factory
    // keeps reading the boot-CLI cmd_args the shared cell was seeded with.
    let Some(run_config) = run_config else {
        return Box::new(move |_delivered: Vec<String>| {
            Box::pin(async { Ok(()) }) as Pin<Box<dyn Future<Output = Result<(), String>>>>
        });
    };
    // Captured once; taken on the single fire (the coordinator fires the
    // finalize at most once per run).
    let mut captures = Some(FinalizeCaptures {
        run_config,
        task_definition_py,
        type_ids,
        source_str,
        output_str,
        skip_existing,
        shared_types,
    });
    // The coordinator passes the delivered argv, but resolution reads it off
    // the shared `run_config` handle (the SAME `run_config_handle()`), so the
    // closure arg is redundant here — single source of truth.
    Box::new(move |_delivered: Vec<String>| {
        let taken = captures.take();
        let fut = async move {
            let Some(captures) = taken else {
                return Err("run-config finalize policy invoked more than once — the \
                     coordinator fires it at most once per run; a second fire is a \
                     programmer error"
                    .to_string());
            };
            // Run the GIL excursion OFF the runtime thread so the mesh-pump
            // keeps the keepalives flowing during the reparse + rebuild
            // (§14/§15).
            tokio::task::spawn_blocking(move || {
                Python::attach(|py| -> Result<(), String> {
                    finalize_cmd_args_under_gil(py, &captures)
                })
            })
            .await
            .map_err(|e| format!("run-config finalize blocking task panicked/aborted: {e}"))?
        };
        Box::pin(fut) as Pin<Box<dyn Future<Output = Result<(), String>>>>
    })
}

/// The GIL-held body of the run-config finalize: call the Python
/// `finalize_run_config(delivered)` to get the re-parsed namespace, re-run
/// `build_worker_command_args` per declared type, and swap the rebuilt
/// per-type `cmd_args` into the shared worker-command source. Pure under-GIL
/// logic, factored out so the `spawn_blocking` closure stays a thin off-thread
/// wrapper. Returns a `String` error (the secondary aborts the run on it) so
/// no `PyErr` crosses the `Send` boundary.
///
/// The worker_module / timeout / reserved-memory of each `TypeRuntime` are
/// preserved from the boot registry (only the `cmd_args` depend on the
/// run-config); the rebuild reads them off the current shared registry under
/// the same lock it then swaps, so the swap is atomic from the factory's view.
fn finalize_cmd_args_under_gil(
    py: Python<'_>,
    captures: &FinalizeCaptures,
) -> Result<(), String> {
    // Resolve the consumer's COMPLETE argparse namespace through the SHARED
    // run-config (single source of truth — it reads the delivered argv off its
    // own `run_config_handle()` and caches the reparse). A later
    // promotion-time discovery reads the SAME cached namespace, so the worker
    // `cmd_args` and the discovery selection flags never diverge.
    let new_args_owned = captures.run_config.resolve_under_gil(py)?;
    let new_args = new_args_owned.bind(py);

    // Rebuild the per-type cmd_args via the EXACT boot-time path
    // (`build_worker_command_args`), then assemble a fresh registry that keeps
    // every non-cmd_args field of the existing runtimes.
    let task_def = captures.task_definition_py.bind(py);
    // Snapshot the current runtimes (worker_module / timeout / reserved) under
    // the lock so the rebuilt registry preserves them; the swap re-locks below.
    let existing: Vec<crate::task_def::TypeRuntime> = captures
        .shared_types
        .lock()
        .map_err(|_| "worker TypeRegistry mutex poisoned".to_string())?
        .types
        .clone();

    let mut rebuilt: Vec<crate::task_def::TypeRuntime> =
        Vec::with_capacity(captures.type_ids.len());
    let mut index_by_id: std::collections::HashMap<dynrunner_core::TypeId, usize> =
        std::collections::HashMap::with_capacity(captures.type_ids.len());
    for type_id_str in &captures.type_ids {
        let cmd_args: Vec<String> = task_def
            .call_method1(
                "build_worker_command_args",
                (
                    type_id_str.as_str(),
                    &new_args,
                    captures.source_str.as_str(),
                    captures.output_str.as_str(),
                    captures.skip_existing,
                ),
            )
            .map_err(|e| format!("build_worker_command_args({type_id_str}) raised: {e}"))?
            .extract()
            .map_err(|e| {
                format!("build_worker_command_args({type_id_str}) returned non-list: {e}")
            })?;
        let type_id = dynrunner_core::TypeId::from(type_id_str.as_str());
        // Preserve the boot runtime's non-cmd_args fields for this type.
        let base = existing
            .iter()
            .find(|t| t.type_id == type_id)
            .ok_or_else(|| {
                format!(
                    "finalize: TypeId '{type_id_str}' not present in the boot registry; \
                     the declared type set must not change across the run-config reparse"
                )
            })?;
        index_by_id.insert(type_id.clone(), rebuilt.len());
        rebuilt.push(crate::task_def::TypeRuntime {
            type_id,
            worker_module: base.worker_module.clone(),
            cmd_args,
            timeout: base.timeout,
            reserved_memory_per_worker: base.reserved_memory_per_worker,
        });
    }

    // Atomically swap the rebuilt registry into the shared cell. Every
    // subsequent factory spawn (initial pool + respawn) reads the new cmd_args.
    *captures
        .shared_types
        .lock()
        .map_err(|_| "worker TypeRegistry mutex poisoned".to_string())? =
        crate::task_def::TypeRegistry {
            types: rebuilt,
            index_by_id,
        };
    Ok(())
}

/// Resolve the NODE-LOCAL output root the relocated/pre-staged primary writes
/// under — the bind-mount-aware value, NOT the submitter-side `--output`.
///
/// Single concern: "where does THIS node's filesystem expose the run's output
/// directory?". The D↔G convergence point: the SAME value is BOTH the
/// discovery driver's `args.resolved_output_root` (G) AND the relocated
/// primary's `on_run_start` `output_dir` (D), so it is computed ONCE here and
/// fed to both — no duplicated bind-mount logic.
///
///   * Pre-staged mode (`args.source_already_staged` non-`None`): the
///     secondary's filesystem-view of the gateway-side output dir lives at the
///     wrapper-script's static bind-mount path `/app/out-network` → `Some`.
///   * Non-pre-staged WITH `args.output`: `Path(args.output).resolve()` →
///     `Some`.
///   * Non-pre-staged WITHOUT `args.output`: `Ok(None)` — preserves the
///     original discovery body's lenient "skip setting `resolved_output_root`"
///     behaviour (the boot Namespace need not carry `--output`, which is NOT a
///     framework-regenerated flag). A genuine resolve failure (pathlib raising)
///     is still an `Err`.
///
/// Returns a `String` error (the secondary aborts the run on it) so no `PyErr`
/// crosses the `Send` boundary in the calling closures.
fn resolve_node_local_output_root(
    py: Python<'_>,
    args: &Bound<'_, PyAny>,
) -> Result<Option<String>, String> {
    let pre_staged = args
        .getattr("source_already_staged")
        .ok()
        .filter(|v| !v.is_none())
        .is_some();
    if pre_staged {
        return Ok(Some("/app/out-network".to_string()));
    }
    let Ok(output_attr) = args.getattr("output") else {
        // No `output` attribute: lenient skip (original discovery behaviour).
        return Ok(None);
    };
    let resolved = (|| -> PyResult<Bound<'_, PyAny>> {
        let pathlib = py.import("pathlib")?;
        pathlib
            .getattr("Path")?
            .call1((output_attr,))?
            .call_method0("resolve")
    })()
    .map_err(|e| format!("failed to resolve output root: {e}"))?;
    resolved
        .str()
        .map_err(|e| format!("failed to stringify resolved output root: {e}"))?
        .extract::<String>()
        .map(Some)
        .map_err(|e| format!("failed to extract resolved output root: {e}"))
}

/// The GIL-held body of one setup-discovery excursion: resolve the COMPLETE
/// run-config namespace, surface the node-local output root, call
/// `task.discover_items(<root>, args)`, and convert the result into typed
/// binaries. Pure under-GIL logic, factored out so the `spawn_blocking`
/// closure stays a thin off-thread wrapper. Returns a `String` error (the
/// secondary aborts the run on it) so no `PyErr` crosses the `Send` boundary.
///
/// The discovery namespace is the COMPLETE one [`SharedRunConfig`] resolves —
/// NOT the stale boot Namespace. On a SLURM relocate-target this is the
/// reparse of the delivered `forwarded_argv` (so the consumer selection flags
/// `--platform` / `--compiler` / `--name-regex` / `--exclude-subfolder` +
/// `--skip-existing` are present); on the in-process path it is the submitter's
/// eagerly-parsed namespace. Either way `args.resolved_output_root` is the
/// node-local value [`resolve_node_local_output_root`] computes.
fn discover_items_under_gil(
    py: Python<'_>,
    task_definition_py: &Py<PyAny>,
    run_config: &crate::managers::run_config::SharedRunConfig,
    setup_discover_root: Option<&std::path::PathBuf>,
) -> Result<Vec<(TaskInfo<RunnerIdentifier>, bool)>, String> {
    let root = setup_discover_root.ok_or_else(|| {
        "setup discovery invoked but src_network is None — the wrapper has no \
         root to pass to task.discover_items; this is a programmer error \
         (only pre-staged mode runs discovery, and that mode always supplies \
         src_network)"
            .to_string()
    })?;
    let task_def = task_definition_py.bind(py);
    // The COMPLETE run-config namespace (single source of truth) — carries the
    // consumer selection flags + `--skip-existing` the stale boot Namespace
    // lacked.
    let args_owned = run_config.resolve_under_gil(py)?;
    let args = args_owned.bind(py);
    let root_py = root
        .clone()
        .into_pyobject(py)
        .map_err(|e| format!("failed to convert discovery root to a Python path: {e}"))?;
    // Surface `args.resolved_output_root` (the node-local, bind-mount-aware
    // value) so the task's `discover_items` sees the same attribute contract
    // the submitter sets — the D↔G converged resolver. Lenient skip when the
    // namespace carries no `output` (original discovery behaviour).
    if let Some(resolved_output_root) = resolve_node_local_output_root(py, args)? {
        args.setattr("resolved_output_root", resolved_output_root)
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
    use super::{resolve_secondary_memprofile_dir, resolve_secondary_memprofile_dir_with_probe};
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

/// Tests for [`extract_staging_dispatch_flags`] — the SOURCE the promote
/// recipe reads (the relocate-staging fix). The relocate-target's
/// `InitialAssignment`-fed cell is at `Default` at promotion, so these flags
/// MUST come from the node's own local producer (`task_definition` /
/// `task_args`). These exercise the real getattr path against duck-typed
/// Python stubs.
///
/// Require an embedded CPython interpreter (gated behind `test-with-python`):
///   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
///        --features test-with-python staging_dispatch_flags`
#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod staging_dispatch_flags_tests {
    use super::extract_staging_dispatch_flags;
    use pyo3::prelude::*;
    use pyo3::types::PyModule;

    /// Compile a tiny module exposing a `task_definition` with the given
    /// `uses_file_based_items` and a `task_args` whose `source_already_staged`
    /// is either `None` (not pre-staged) or a path string (pre-staged). Returns
    /// the two stub objects. Stubs are pure-Python `SimpleNamespace`s the
    /// extractor duck-types via `getattr` — no wheel needed.
    fn stubs<'py>(
        py: Python<'py>,
        uses_file_based_items_attr: &str,
        source_already_staged_attr: &str,
    ) -> (Bound<'py, PyAny>, Bound<'py, PyAny>) {
        let source = format!(
            "from types import SimpleNamespace\n\
             task_definition = SimpleNamespace({uses_file_based_items_attr})\n\
             task_args = SimpleNamespace({source_already_staged_attr})\n"
        );
        let module = PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new("stub_staging.py").unwrap().as_c_str(),
            std::ffi::CString::new("stub_staging").unwrap().as_c_str(),
        )
        .expect("compile staging stub module");
        (
            module.getattr("task_definition").unwrap(),
            module.getattr("task_args").unwrap(),
        )
    }

    /// asm-dataset facet: `uses_file_based_items=False`, not pre-staged. The
    /// recipe must stamp `uses_file_based_items=false` so the dispatch target
    /// passes opaque identifiers through (no StageFile).
    #[test]
    fn uses_file_based_items_false_not_pre_staged() {
        Python::attach(|py| {
            let (td, ta) = stubs(py, "uses_file_based_items=False", "source_already_staged=None");
            let (uses_files, pre_staged) = extract_staging_dispatch_flags(&td, &ta);
            assert!(!uses_files, "uses_file_based_items=False must extract false");
            assert!(!pre_staged, "source_already_staged=None must extract not-pre-staged");
        });
    }

    /// asm-tokenizer mode-2 facet: `--source-already-staged` (a non-None
    /// `source_already_staged`). The recipe must stamp `pre_staged_mode=true`.
    #[test]
    fn pre_staged_mode_from_source_already_staged() {
        Python::attach(|py| {
            let (td, ta) = stubs(
                py,
                "uses_file_based_items=True",
                "source_already_staged='/some/staged/root'",
            );
            let (uses_files, pre_staged) = extract_staging_dispatch_flags(&td, &ta);
            assert!(uses_files, "file-based stays true in pre-staged mode");
            assert!(pre_staged, "non-None source_already_staged must extract pre-staged");
        });
    }

    /// mode-1 default facet: file-based, not pre-staged — the historical
    /// contract. A producer with NEITHER attribute set must still yield the
    /// safe defaults (`uses_file_based_items=true`, `pre_staged_mode=false`),
    /// mirroring `managers/primary/new.rs`'s `unwrap_or(true)`.
    #[test]
    fn missing_attributes_default_to_file_based_not_pre_staged() {
        Python::attach(|py| {
            // SimpleNamespace with no fields at all.
            let (td, ta) = stubs(py, "", "");
            let (uses_files, pre_staged) = extract_staging_dispatch_flags(&td, &ta);
            assert!(uses_files, "missing uses_file_based_items defaults to file-based");
            assert!(!pre_staged, "missing source_already_staged defaults to not-pre-staged");
        });
    }
}

/// Relocated-primary run-lifecycle tests (bugs D + G).
///
/// Exercise the REAL pyo3 source the relocated primary runs:
///   * G — [`discover_items_under_gil`] resolves the COMPLETE namespace
///     ([`SharedRunConfig::deferred`] reparse of the delivered argv) so the
///     consumer's `discover_items` sees the selection flags + `--skip-existing`
///     + the node-local `resolved_output_root` the stale boot Namespace lacked.
///   * D — [`build_promoted_primary_recipe`] routes the REAL `on_phase_end`
///     through `PrimaryRunArgs` (NOT the no-op closure `run_pipeline` used to
///     read) AND fires `on_run_start` on the relocated primary with a live
///     handle + node-local `output_dir`.
///
/// No false-greens: each test drives the production builder / GIL body, never a
/// pre-built config. Revert the fix (no-op closures in `PrimaryRunArgs`; the
/// stale `task_args_py` into discovery) and these regress.
///
///   `cargo test -p dynrunner-pyo3 --no-default-features \
///        --features test-with-python relocated_primary`
#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod relocated_primary_tests {
    use super::*;
    use crate::managers::run_config::SharedRunConfig;
    use pyo3::types::PyModule;
    use std::sync::{Arc, Mutex};

    /// A consumer `TaskDefinition` whose `discover_items`, `on_phase_end`, and
    /// `on_run_start` RECORD what the framework handed them onto module-level
    /// lists, so a test reads back the real call arguments. Built from a small
    /// pure-Python module (no wheel). `finalize_run_config` is a real reparse
    /// closure that builds a `Namespace` from the delivered argv tokens.
    fn task_module(py: Python<'_>) -> Bound<'_, PyAny> {
        let source = r#"
import argparse
from types import SimpleNamespace

# Recorded call arguments, read back by the Rust test.
discover_calls = []      # list[(root_str, namespace)]
on_phase_end_calls = []  # list[(phase, completed, failed)]
on_run_start_calls = []  # list[(source_dir, output_dir, has_handle, namespace)]

class _Item:
    """Minimal duck-typed TaskInfo the shared `extract_binaries` accepts."""
    def __init__(self, task_id):
        self.task_id = task_id
        self.path = "/staged/corpus/" + task_id
        self.size = 1
        self.identifier = SimpleNamespace(
            binary_name=task_id,
            platform="x64",
            compiler="gcc",
            version="1",
            opt_level="O0",
        )
        self.type_id = "t"
        self.task_depends_on = []

class Task:
    def discover_items(self, root, args):
        discover_calls.append((str(root), args))
        # Emit ONE item iff the selection flag the boot Namespace lacked is
        # present — proves the COMPLETE namespace reached discovery.
        if getattr(args, "platform", None) == "x64":
            yield _Item("disc-1")

    def on_phase_end(self, phase_id, completed, failed, phase_outputs=None):
        on_phase_end_calls.append((phase_id, completed, failed))
        if getattr(self, "_raise_on_phase_end", False):
            raise RuntimeError("consumer policy abort from on_phase_end")

    def on_run_start(self, source_dir, output_dir, args, primary_handle=None):
        on_run_start_calls.append(
            (source_dir, output_dir, primary_handle is not None, args)
        )

def finalize_run_config(delivered):
    # The deferred reparse: build the COMPLETE Namespace from the delivered
    # forwarded argv (the boot Namespace would lack these).
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform")
    parser.add_argument("--output")
    ns = parser.parse_args(delivered)
    return ns

task = Task()
"#;
        let module = PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new("relocated_stub.py").unwrap().as_c_str(),
            std::ffi::CString::new("relocated_stub").unwrap().as_c_str(),
        )
        .expect("compile relocated-primary stub module");
        module.into_any()
    }

    /// G: discovery resolves the COMPLETE namespace (the deferred reparse of
    /// the delivered argv), so `discover_items` sees `--platform`/`--output`
    /// AND the node-local `resolved_output_root` — none of which are on the
    /// stale boot line. The emitted item proves the selection flag arrived.
    #[test]
    fn discovery_receives_complete_namespace_and_node_local_output_root() {
        Python::attach(|py| {
            let module = task_module(py);
            let task = module.getattr("task").unwrap().unbind();
            // The DELIVERED forwarded argv (post-push) — the complete run-config
            // the boot Namespace lacked.
            let delivered = Arc::new(Mutex::new(vec![
                "--platform".to_string(),
                "x64".to_string(),
                "--output".to_string(),
                "/run/out".to_string(),
            ]));
            let finalize = module.getattr("finalize_run_config").unwrap().unbind();
            let run_config = SharedRunConfig::deferred(finalize, delivered);

            let root = std::path::PathBuf::from("/staged/corpus");
            let items = discover_items_under_gil(py, &task, &run_config, Some(&root))
                .expect("discovery must succeed on the complete namespace");

            // The selection flag reached discovery → one item emitted.
            assert_eq!(items.len(), 1, "selection flag must reach discover_items");

            // Read back the recorded call arguments.
            let discover_calls = module.getattr("discover_calls").unwrap();
            assert_eq!(discover_calls.len().unwrap(), 1);
            let (root_str, ns): (String, Bound<'_, PyAny>) =
                discover_calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(root_str, "/staged/corpus");
            // The COMPLETE namespace: the boot Namespace would not carry
            // `platform`.
            let platform: String = ns.getattr("platform").unwrap().extract().unwrap();
            assert_eq!(platform, "x64");
            // Non-pre-staged node-local output root = resolve(args.output).
            let resolved: String =
                ns.getattr("resolved_output_root").unwrap().extract().unwrap();
            assert_eq!(resolved, "/run/out");
        });
    }

    /// G: a PRE-staged discovery namespace gets the bind-mount output root
    /// (`/app/out-network`), the D↔G converged resolver's pre-staged branch.
    #[test]
    fn discovery_pre_staged_output_root_is_bind_mount() {
        Python::attach(|py| {
            let source = "from types import SimpleNamespace\n\
                 ns = SimpleNamespace(source_already_staged='/staged', output='/ignored')\n";
            let m = PyModule::from_code(
                py,
                std::ffi::CString::new(source).unwrap().as_c_str(),
                std::ffi::CString::new("prestaged_ns.py").unwrap().as_c_str(),
                std::ffi::CString::new("prestaged_ns").unwrap().as_c_str(),
            )
            .unwrap();
            let ns = m.getattr("ns").unwrap();
            let resolved = resolve_node_local_output_root(py, &ns).unwrap();
            assert_eq!(resolved.as_deref(), Some("/app/out-network"));
        });
    }

    /// Build the four mesh/snapshot inputs the promote recipe consumes, plus a
    /// `(tx, rx)` command channel and a live handle minted from the SAME `tx`.
    /// All produced from production constructors (no test-only shims).
    #[allow(clippy::type_complexity)]
    fn recipe_inputs() -> (
        PromotedCommandChannel,
        crate::managers::primary_handle::PyPrimaryHandle,
        dynrunner_manager_distributed::process::Mesh<
            RunnerIdentifier,
            dynrunner_transport_channel::ChannelPeerTransport<RunnerIdentifier>,
        >,
    ) {
        use dynrunner_manager_distributed::primary::COMMAND_CHANNEL_CAPACITY;
        let (tx, rx) = tokio::sync::mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let handle = crate::managers::primary_handle::PyPrimaryHandle::from_sender(
            tx.clone(),
            crate::managers::primary_handle::ReinjectCapCell::default(),
        )
        .expect("handle init");
        let mut transports =
            dynrunner_transport_channel::peer_mesh::<RunnerIdentifier>(&["primary".to_string()]);
        let transport = transports.remove(0);
        let mesh = Mesh::new(transport);
        ((tx, rx), handle, mesh)
    }

    /// D: the promote recipe routes the REAL `on_phase_end` through
    /// `PrimaryRunArgs` (NOT the no-op closure). Invoking the returned
    /// `run_args.on_phase_end` fires the consumer hook (recorded) AND — because
    /// it is the raise-latch variant sharing the latch the recipe installed — a
    /// raising hook records into THAT latch. Reverting the fix (no-op closures
    /// in `PrimaryRunArgs`) leaves the consumer hook uncalled and the latch
    /// empty.
    #[test]
    fn recipe_routes_real_on_phase_end_with_raise_latch_through_run_args() {
        Python::attach(|py| {
            let module = task_module(py);
            let task = module.getattr("task").unwrap();
            // Arm the consumer hook to raise so the latch path is exercised.
            task.setattr("_raise_on_phase_end", true).unwrap();
            let task_def = task.clone().unbind();

            let on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
                crate::managers::lifecycle::make_on_phase_start(task_def.clone_ref(py)),
            );
            let raise_latch = dynrunner_manager_distributed::PhaseHookRaiseLatch::new();
            let on_phase_end: crate::managers::lifecycle::OnPhaseEnd =
                Box::new(crate::managers::lifecycle::make_on_phase_end_with_raise_latch(
                    task_def.clone_ref(py),
                    raise_latch.clone(),
                ));

            let ((tx, rx), _handle, mut mesh) = recipe_inputs();
            let (slot, client, inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("primary"));
            let (_demote_tx, demote_rx) = tokio::sync::mpsc::unbounded_channel();
            let snapshot = dynrunner_manager_distributed::ClusterState::<RunnerIdentifier>::new()
                .snapshot();
            let estimator = PyMemoryEstimatorBridge::from_python(&task, &[]).unwrap();

            let mut recipe = build_promoted_primary_recipe(PromotedPrimaryRecipeInputs {
                secondary_id: "primary".to_string(),
                keepalive_interval: std::time::Duration::from_secs(5),
                peer_timeout: std::time::Duration::from_secs(30),
                keepalive_miss_threshold: 3,
                retry_max_passes: 0,
                oom_retry_max_passes: 0,
                scheduler_config: crate::config::scheduler::SchedulerConfig::default(),
                estimator,
                command_channel: Some((tx, rx)),
                on_phase_start,
                on_phase_end,
                phase_hook_raise_latch: raise_latch.clone(),
                // No on_run_start here — exercised in its own test.
                on_run_start: None,
                forwarded_argv: Arc::new(Mutex::new(Vec::new())),
                uses_file_based_items: true,
                pre_staged_mode: false,
                source_pre_staged_root: None,
                source_dir: None,
                setup_discovery: None,
                liveness_ping_rx: None,
                peer_liveness_addrs: None,
            });

            let mut built = recipe(client, inbox, demote_rx, snapshot);
            // Keep the slot alive for the coordinator's lifetime.
            let _slot = slot;

            // Invoke the REAL `run_args.on_phase_end` — the channel
            // `run_pipeline` reads. A no-op (reverted bug) would record nothing.
            let outputs = std::collections::BTreeMap::new();
            (built.run_args.on_phase_end)(
                &dynrunner_core::PhaseId::from("phase-1"),
                3,
                0,
                &outputs,
            );

            // The consumer hook fired (NOT a no-op).
            let calls = module.getattr("on_phase_end_calls").unwrap();
            assert_eq!(
                calls.len().unwrap(),
                1,
                "run_args.on_phase_end must invoke the consumer hook, not a no-op"
            );
            // The raise was recorded into the SAME latch the recipe installed —
            // the honest-exit path is live on the relocated primary.
            assert!(
                raise_latch.take().is_some(),
                "a raising on_phase_end must record into the recipe's raise-latch"
            );
            drop(built.coordinator);
        });
    }

    /// D: the recipe fires `on_run_start` on the relocated primary, handing the
    /// consumer (source_dir, node-local output_dir, complete-namespace args,
    /// a LIVE handle). Reverting the fix (no on_run_start fire) leaves
    /// `on_run_start_calls` empty.
    #[test]
    fn recipe_fires_on_run_start_with_live_handle_and_node_local_output() {
        Python::attach(|py| {
            let module = task_module(py);
            let task = module.getattr("task").unwrap();
            let task_def = task.clone().unbind();

            let on_phase_start: crate::managers::lifecycle::OnPhaseStart =
                Box::new(crate::managers::lifecycle::make_on_phase_start(task_def.clone_ref(py)));
            let raise_latch = dynrunner_manager_distributed::PhaseHookRaiseLatch::new();
            let on_phase_end: crate::managers::lifecycle::OnPhaseEnd =
                Box::new(crate::managers::lifecycle::make_on_phase_end_with_raise_latch(
                    task_def.clone_ref(py),
                    raise_latch.clone(),
                ));

            // The complete namespace, resolved from the delivered argv.
            let delivered = Arc::new(Mutex::new(vec![
                "--platform".to_string(),
                "x64".to_string(),
                "--output".to_string(),
                "/run/out".to_string(),
            ]));
            let finalize = module.getattr("finalize_run_config").unwrap().unbind();
            let run_config = SharedRunConfig::deferred(finalize, delivered);

            let ((tx, rx), handle, mut mesh) = recipe_inputs();
            let (slot, client, inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("primary"));
            let (_demote_tx, demote_rx) = tokio::sync::mpsc::unbounded_channel();
            let snapshot = dynrunner_manager_distributed::ClusterState::<RunnerIdentifier>::new()
                .snapshot();
            let estimator = PyMemoryEstimatorBridge::from_python(&task, &[]).unwrap();

            let mut recipe = build_promoted_primary_recipe(PromotedPrimaryRecipeInputs {
                secondary_id: "primary".to_string(),
                keepalive_interval: std::time::Duration::from_secs(5),
                peer_timeout: std::time::Duration::from_secs(30),
                keepalive_miss_threshold: 3,
                retry_max_passes: 0,
                oom_retry_max_passes: 0,
                scheduler_config: crate::config::scheduler::SchedulerConfig::default(),
                estimator,
                command_channel: Some((tx, rx)),
                on_phase_start,
                on_phase_end,
                phase_hook_raise_latch: raise_latch,
                on_run_start: Some(OnRunStartContext {
                    task_definition_py: task_def,
                    source_dir: "/local/src".to_string(),
                    run_config,
                    primary_handle: handle,
                }),
                forwarded_argv: Arc::new(Mutex::new(Vec::new())),
                uses_file_based_items: true,
                pre_staged_mode: false,
                source_pre_staged_root: None,
                source_dir: None,
                setup_discovery: None,
                liveness_ping_rx: None,
                peer_liveness_addrs: None,
            });

            let built = recipe(client, inbox, demote_rx, snapshot);
            let _slot = slot;

            let calls = module.getattr("on_run_start_calls").unwrap();
            assert_eq!(
                calls.len().unwrap(),
                1,
                "the relocated primary must fire on_run_start once"
            );
            let (source_dir, output_dir, has_handle, ns): (
                String,
                String,
                bool,
                Bound<'_, PyAny>,
            ) = calls.get_item(0).unwrap().extract().unwrap();
            assert_eq!(source_dir, "/local/src");
            // The node-local output root (non-pre-staged → resolve(args.output)).
            assert_eq!(output_dir, "/run/out");
            assert!(has_handle, "on_run_start must receive a live primary_handle");
            // The args are the COMPLETE namespace (selection flag present).
            let platform: String = ns.getattr("platform").unwrap().extract().unwrap();
            assert_eq!(platform, "x64");
            drop(built.coordinator);
        });
    }
}
