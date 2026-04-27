use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyList;

use db_distributed_manager::{PrimaryConfig, PrimaryCoordinator};
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_quic::NetworkServer;

use crate::config::distributed::DistributedConfig;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::TokenizerIdentifier;
use crate::pytypes::extract_binaries;

#[pyclass(name = "RustPrimaryCoordinator")]
pub(crate) struct PyPrimaryCoordinator {
    num_secondaries: u32,
    estimator_slope: f64,
    estimator_intercept: f64,
    spawn_secondary: Py<PyAny>,
    distributed_config: DistributedConfig,
    completed: u32,
    failed: u32,
}

#[pymethods]
impl PyPrimaryCoordinator {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        task_definition,
        spawn_secondary,
        distributed_config = None,
    ))]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
        distributed_config: Option<DistributedConfig>,
    ) -> PyResult<Self> {
        let estimate_fn = task_definition.getattr("estimate_memory")?;
        let bridge = PyMemoryEstimatorBridge::from_python(py, &estimate_fn)?;

        Ok(Self {
            num_secondaries,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
            spawn_secondary: spawn_secondary.clone_ref(py),
            distributed_config: distributed_config.unwrap_or_default(),
            completed: 0,
            failed: 0,
        })
    }

    /// Run the primary coordination pipeline over real network connections.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let slope = self.estimator_slope;
        let intercept = self.estimator_intercept;
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_peer_timeout = self.distributed_config.peer_timeout();

        // Pick a free port for the primary server before spawning secondaries.
        let tmp_listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(format!("failed to bind: {e}")))?;
        let port = tmp_listener.local_addr().unwrap().port();
        drop(tmp_listener);

        let primary_url = format!("tcp://127.0.0.1:{}", port);

        // Call the Python spawn_secondary callback for each secondary.
        // The callback receives (primary_url, secondary_id, quic_port) and
        // should return a subprocess.Popen (or compatible object with kill/wait).
        let mut child_processes: Vec<Py<PyAny>> = Vec::new();
        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let process = self.spawn_secondary.call1(
                py,
                (&primary_url, &secondary_id, 0u16),
            )?;
            tracing::info!(
                secondary_id = %secondary_id,
                "spawned secondary process via callback"
            );
            child_processes.push(process);
        }

        let mut completed = 0u32;
        let mut failed = 0u32;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Bind the network server to the port we already picked.
                let bind_addr: std::net::SocketAddr =
                    format!("127.0.0.1:{}", port).parse().unwrap();
                let server: NetworkServer<TokenizerIdentifier> =
                    match NetworkServer::bind(bind_addr).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to start network server");
                            return;
                        }
                    };
                tracing::info!(port, "primary network server listening");

                // Give secondaries a moment to start up.
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Run the primary coordinator with the network server transport.
                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };
                let mut primary: PrimaryCoordinator<_, _, _, TokenizerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        ResourceStealingScheduler::memory(),
                        estimator,
                    );

                let result = primary.run(rust_binaries).await;
                if let Err(e) = &result {
                    tracing::error!(error = %e, "primary coordinator failed");
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;
            }));
        });

        // Back with the GIL — terminate secondary processes via the Python objects.
        for process in &child_processes {
            let _ = process.call_method0(py, "kill");
            let _ = process.call_method0(py, "wait");
        }

        self.completed = completed;
        self.failed = failed;

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
}

