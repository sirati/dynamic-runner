use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{ResourceKind, ResourceMap};
use dynrunner_manager_distributed::{SecondaryConfig, SecondaryCoordinator};
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
    skip_existing: bool,
    estimator: PyMemoryEstimatorBridge,
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
            skip_existing,
            estimator: task.estimator,
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

        let mut completed = 0u32;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

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
                            return;
                        }
                    },
                    Err(e) => {
                        tracing::error!(url = %primary_url, error = %e, "failed to resolve primary URL");
                        return;
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
                        return;
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
                                return;
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

                match secondary.run(&mut factory).await {
                    Ok(()) => {
                        tracing::info!("secondary finished successfully");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "secondary failed");
                    }
                }

                completed = secondary.completed_count() as u32;

                // Clean up child processes
                for child in &mut factory.child_processes {
                    if let Some(mut c) = child.take() {
                        let _ = c.kill();
                        let _ = c.wait();
                    }
                }
            }));
        });

        self.completed = completed;
        Ok(())
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

