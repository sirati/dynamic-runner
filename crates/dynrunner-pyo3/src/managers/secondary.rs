use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{PhaseId, ResourceKind, ResourceMap};
use dynrunner_manager_distributed::{RunOutcome, SecondaryConfig, SecondaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_quic::NetworkClient;

use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
use crate::network::{detect_ipv4, detect_ipv6, gethostname};
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{LoadedTaskDefinition, TypeRegistry};

#[pyclass(name = "RustSecondaryCoordinator")]
pub(crate) struct PySecondaryCoordinator {
    python_executable: PathBuf,
    primary_url: String,
    secondary_id: String,
    num_workers: u32,
    max_resources: ResourceMap,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    distributed_config: DistributedConfig,
    /// Shared-drive directory where the primary stages source binaries.
    /// `None` for single-node modes (file-ready resolution falls back
    /// to absolute paths from the primary's view).
    src_network: Option<PathBuf>,
    /// Per-secondary scratch directory where StageFile copies land.
    /// `None` falls back to a system tempdir under
    /// `db_secondary_<id>` (the historical default).
    src_tmp: Option<PathBuf>,
    types: TypeRegistry,
    /// Phase dependency graph extracted from
    /// `LoadedTaskDefinition::from_python`. Retained on the wrapper
    /// (rather than left to drop after construction like the legacy
    /// path did) because the setup-promote yield needs it: the Python
    /// `task.discover_items` call resolves the per-task list but not
    /// the graph metadata, and the Rust core seeds both as a single
    /// mutation batch via `ingest_setup_discovery`.
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    skip_existing: bool,
    estimator: PyMemoryEstimatorBridge,
    /// Held for the setup-promote outer loop. When the Rust core
    /// signals `RunOutcome::SetupPending`, the wrapper re-acquires the
    /// GIL and invokes `task_definition_py.discover_items(<root>,
    /// task_args_py)` to enumerate the staged corpus. Kept as a
    /// `Py<PyAny>` (not `Bound<'py, _>`) because the wrapper outlives
    /// any single `Python<'py>` lifetime; `bind(py)` re-materialises a
    /// `Bound` at each call site.
    task_definition_py: Py<PyAny>,
    /// Held for the same reason as `task_definition_py`: the second
    /// positional argument to `discover_items`. Originates from the
    /// `task_args` Python object passed into the constructor.
    task_args_py: Py<PyAny>,
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner `SecondaryCoordinator` at `run()`
    /// start. Constructor-only — see the matching field on
    /// `PyPrimaryCoordinator` for the rationale.
    peer_lifecycle_listener: Option<Py<PyAny>>,
    completed: u32,
}

#[pymethods]
impl PySecondaryCoordinator {
    #[new]
    #[pyo3(signature = (
        primary_url,
        secondary_id,
        num_workers,
        ram_bytes,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        log_paths = None,
        worker_spec = None,
        distributed_config = None,
        src_network = None,
        src_tmp = None,
        max_resources = None,
        peer_lifecycle_listener = None,
    ))]
    fn new(
        py: Python<'_>,
        primary_url: String,
        secondary_id: String,
        num_workers: u32,
        ram_bytes: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        distributed_config: Option<DistributedConfig>,
        src_network: Option<PathBuf>,
        src_tmp: Option<PathBuf>,
        max_resources: Option<PyResourceMap>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
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

        // Resolve this secondary's per-run log directory under the
        // log-mount root, using `secondary_id` so two co-located
        // secondaries on the same shared mount get distinct
        // directories. `create_dir_all` errors surface as
        // construction-time failures — silently swallowing this with
        // `.ok()` produced 6h runs with zero worker log output when
        // the mount happened to be read-only or missing.
        let log_dir =
            task.log_paths
                .resolve_log_dir(py, &task.output_path, &secondary_id)?;
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "failed to create log directory {log_dir:?}: {e}"
            ))
        })?;

        // Boundary normalization: typed `max_resources` ResourceMap wins
        // when supplied; otherwise fall back to a single-key memory map
        // built from the legacy scalar `ram_bytes`.
        let max_resources = max_resources.map(|m| m.to_rust()).unwrap_or_else(|| {
            ResourceMap::from([(ResourceKind::memory(), ram_bytes)])
        });

        Ok(Self {
            python_executable: task.python_executable,
            primary_url,
            secondary_id,
            num_workers,
            max_resources,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            src_network,
            src_tmp,
            types: task.types,
            // `from_python` already extracted phase_deps off the
            // TaskDefinition's `get_phases()`; we keep it on the
            // wrapper for the setup-promote yield path. Legacy
            // (non-pre-staged) runs never inspect this field.
            phase_deps: task.phase_deps,
            skip_existing,
            estimator: task.estimator,
            // Bump the refcount and unbind to a `Py<PyAny>` so the
            // handle outlives the constructor's `Bound` lifetime. The
            // setup-promote yield re-binds each iteration under a
            // fresh `Python::attach` scope.
            task_definition_py: task_definition.clone().unbind(),
            task_args_py: task_args.clone().unbind(),
            peer_lifecycle_listener,
            completed: 0,
        })
    }

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
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_connect_retry_delay = self.distributed_config.connect_retry_delay();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_primary_link_failure_threshold =
            self.distributed_config.primary_link_failure_threshold();
        let dist_primary_link_failure_window =
            self.distributed_config.primary_link_failure_window();
        let dist_setup_deadline = self.distributed_config.setup_deadline();
        let dist_disable_peer_overlay = self.distributed_config.disable_peer_overlay();
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
        let cfg_src_network = self.src_network.clone();
        let cfg_src_tmp = self.src_tmp.clone();

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

        // Errors produced inside the async block — including
        // `task.discover_items` raising in setup-promote — must surface
        // as `PyErr` here so the Python-side `run()` returns non-zero.
        // Previously every error path `break`d out of the loop and
        // `self.completed` was set from a zero counter, causing the
        // secondary to exit `0` despite the work never starting; the
        // dispatcher then chained the next task on a missing input.
        let result: Result<u32, PyErr> = py.detach(|| -> Result<u32, PyErr> {
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
                        dynrunner_transport_quic::EitherPeerTransport::Real(pn),
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
                    primary_link_failure_threshold: dist_primary_link_failure_threshold,
                    primary_link_failure_window: dist_primary_link_failure_window,
                    setup_deadline: dist_setup_deadline,
                    is_observer: false,
                };

                let mut factory = SubprocessWorkerFactory {
                    python_executable,
                    source_dir,
                    output_dir,
                    log_dir,
                    log_paths,
                    worker_module,
                    worker_cmd_args,
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
                    ResourceStealingScheduler::memory(),
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
                let loop_result: Result<(), PyErr> = loop {
                    let outcome = match secondary
                        .run_until_setup_or_done(&mut factory)
                        .await
                    {
                        Ok(o) => o,
                        Err(e) => {
                            tracing::error!(error = %e, "secondary failed");
                            break Err(pyo3::exceptions::PyRuntimeError::new_err(
                                format!("secondary failed: {e}"),
                            ));
                        }
                    };
                    match outcome {
                        RunOutcome::Done => {
                            tracing::info!("secondary finished successfully");
                            break Ok(());
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
                                    break Err(e);
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
                                break Err(pyo3::exceptions::PyRuntimeError::new_err(
                                    format!("ingest_setup_discovery: {e}"),
                                ));
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
                factory.cleanup_all();

                loop_result.map(|()| completed)
            }))
        });

        self.completed = result?;
        Ok(())
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

