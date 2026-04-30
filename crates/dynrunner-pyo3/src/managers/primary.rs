use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::PhaseId;
use dynrunner_manager_distributed::{PrimaryConfig, PrimaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_quic::NetworkServer;

use crate::config::distributed::DistributedConfig;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::TokenizerIdentifier;
use crate::pytypes::extract_binaries;
use crate::task_def::LoadedTopology;

#[pyclass(name = "RustPrimaryCoordinator")]
pub(crate) struct PyPrimaryCoordinator {
    num_secondaries: u32,
    estimator: PyMemoryEstimatorBridge,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    spawn_secondary: Py<PyAny>,
    distributed_config: DistributedConfig,
    /// Optional caller-supplied bind port for the network server.
    /// When `Some`, the primary binds exactly this port; this is what
    /// the SLURM packaging path needs because it sets up an SSH `-R`
    /// forward to a port it picked itself, and the Rust side has to
    /// honour the same number end-to-end. When `None`, we fall back
    /// to a temp-listener pre-pick + drop + re-bind dance (legacy
    /// behaviour, retained for in-process / local-multi-computer
    /// callers that don't tunnel and don't care which port lands).
    listen_port: Option<u16>,
    completed: u32,
    failed: u32,
    // Pre-`run()` queue of StageFile notifications. The pipeline calls
    // `notify_stage_file(...)` on this pyclass as part of packaging
    // (before `run()` ever starts the coordinator). On `run()`, this
    // list is moved into `PrimaryCoordinator::queue_stage_file` so the
    // coordinator flushes them once secondary connections are up.
    pending_stage_files: Vec<(String, String, String, String)>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B).
    task_definition: Py<PyAny>,
}

#[pymethods]
impl PyPrimaryCoordinator {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        task_definition,
        spawn_secondary,
        distributed_config = None,
        listen_port = None,
    ))]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
        distributed_config: Option<DistributedConfig>,
        listen_port: Option<u16>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(py, task_definition)?;

        Ok(Self {
            num_secondaries,
            estimator: topology.estimator,
            phase_deps: topology.phase_deps,
            spawn_secondary: spawn_secondary.clone_ref(py),
            distributed_config: distributed_config.unwrap_or_default(),
            listen_port,
            completed: 0,
            failed: 0,
            pending_stage_files: Vec::new(),
            task_definition: task_definition.clone().unbind(),
        })
    }

    /// Queue a `StageFile` notification for a secondary. Must be called
    /// BEFORE `run()` (the typical pipeline pattern: stage all files
    /// during packaging, then start the coordinator). The Rust
    /// coordinator flushes these notifications once the secondary
    /// handshake completes and before initial task assignment.
    fn notify_stage_file(
        &mut self,
        secondary_id: String,
        file_hash: String,
        src_path: String,
        dest_path: String,
    ) -> PyResult<()> {
        self.pending_stage_files
            .push((secondary_id, file_hash, src_path, dest_path));
        Ok(())
    }

    /// Run the primary coordination pipeline over real network connections.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let estimator = self.estimator.clone();
        let phase_deps = self.phase_deps.clone();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let pending_stage_files = std::mem::take(&mut self.pending_stage_files);

        // Phase 5B: re-acquire the GIL from the coordinator's LocalSet
        // and dispatch to the Python TaskDefinition's `on_phase_*`
        // methods. Each closure owns its own ref-bumped `Py<PyAny>` so
        // the manager owns the lifetime independent of `self`.
        let on_phase_start: Box<dyn FnMut(&dynrunner_core::PhaseId) + Send> = Box::new(
            crate::managers::lifecycle::make_on_phase_start(
                self.task_definition.clone_ref(py),
            ),
        );
        let on_phase_end: Box<dyn FnMut(&dynrunner_core::PhaseId, u32, u32) + Send> = Box::new(
            crate::managers::lifecycle::make_on_phase_end(
                self.task_definition.clone_ref(py),
            ),
        );

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

                // Secondaries retry-connect on their own; the accept loop in
                // PrimaryCoordinator::run handles connections that arrive
                // after we hand control to it.

                // Run the primary coordinator with the network server transport.
                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                };

                let mut primary: PrimaryCoordinator<_, _, _, TokenizerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        ResourceStealingScheduler::memory(),
                        estimator,
                    );

                for (sec_id, hash, src, dest) in pending_stage_files {
                    primary.queue_stage_file(sec_id, hash, src, dest);
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

