use std::path::PathBuf;

use pyo3::prelude::*;

use db_distributed_manager::{SecondaryConfig, SecondaryCoordinator};
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_quic::NetworkClient;

use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::TokenizerIdentifier;
use crate::network::{detect_ipv4, gethostname};
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::LoadedTaskDefinition;

#[pyclass(name = "RustSecondaryCoordinator")]
pub(crate) struct PySecondaryCoordinator {
    python_executable: PathBuf,
    primary_url: String,
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
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
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator_slope: f64,
    estimator_intercept: f64,
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

        Ok(Self {
            python_executable: task.python_executable,
            primary_url,
            secondary_id,
            num_workers,
            ram_bytes,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir: task.log_dir,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            src_network,
            src_tmp,
            worker_module: task.worker_module,
            worker_cmd_args: task.worker_cmd_args,
            skip_existing,
            estimator_slope: task.estimator.slope,
            estimator_intercept: task.estimator.intercept,
            completed: 0,
        })
    }

    /// Connect to the primary and run the secondary coordination loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        let primary_url = self.primary_url.clone();
        let secondary_id = self.secondary_id.clone();
        let num_workers = self.num_workers;
        let ram_bytes = self.ram_bytes;
        let slope = self.estimator_slope;
        let intercept = self.estimator_intercept;
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
        let worker_module = self.worker_module.clone();
        let worker_cmd_args = self.worker_cmd_args.clone();
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
                // Parse the primary URL to get the address.
                // Supports formats like "tcp://host:port", "ws://host:port", or "host:port"
                let addr_str = primary_url
                    .strip_prefix("tcp://")
                    .or_else(|| primary_url.strip_prefix("ws://"))
                    .or_else(|| primary_url.strip_prefix("wss://"))
                    .unwrap_or(&primary_url);

                let addr: std::net::SocketAddr = match addr_str.parse() {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::error!(url = %primary_url, error = %e, "failed to parse primary URL");
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

                // Start peer network for peer-to-peer communication
                let peer_network: db_transport_quic::PeerNetwork<TokenizerIdentifier> =
                    db_transport_quic::PeerNetwork::start(&format!("sec-{}", num_workers))
                        .await
                        .unwrap_or_else(|e| {
                            tracing::error!(error = %e, "failed to start peer network, using no-op");
                            // This won't happen in practice since PeerNetwork::start only fails
                            // on cert generation or bind errors, but we handle it gracefully.
                            panic!("peer network start failed: {e}");
                        });

                let peer_cert_pem = peer_network.cert_pem().to_string();
                let peer_port = peer_network.port();

                let config = SecondaryConfig {
                    secondary_id: secondary_id.clone(),
                    num_workers,
                    max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), ram_bytes)]),
                    hostname: gethostname(),
                    keepalive_interval: dist_keepalive,
                    src_network: cfg_src_network,
                    src_tmp: cfg_src_tmp,
                    peer_timeout: dist_peer_timeout,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };

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

                let mut secondary: SecondaryCoordinator<_, _, _, _, _, TokenizerIdentifier> = SecondaryCoordinator::new(
                    config,
                    client,
                    peer_network,
                    ResourceStealingScheduler::memory(),
                    estimator,
                );

                // Set peer cert info so the CertExchange message includes our QUIC details
                secondary.set_peer_cert_info(
                    db_distributed_manager::PeerCertInfo {
                        public_cert_pem: peer_cert_pem,
                        ipv4_address: Some(detect_ipv4(None)),
                        ipv6_address: None,
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

