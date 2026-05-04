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
    /// Tuple shape: `(secondary_id, file_hash, content_hash, src_path, dest_path)`.
    /// `file_hash` is the task identifier for cache lookup;
    /// `content_hash` is the SHA256 of the file contents that the
    /// staging integrity check will verify against.
    pending_stage_files: Vec<(String, String, String, String, String)>,
    /// Pre-staged-source mode (`--source-already-staged` on the
    /// pipeline). When `Some`, this is the gateway-side host path
    /// the wrapper bind-mounts into each secondary container at
    /// `src_network`. The primary uses it to compute the wire-side
    /// `local_path` (TaskInfo.path with this prefix stripped) so
    /// secondary's `src_network.join(<local_path>)` resolves to the
    /// in-container bind-mount path. Propagated as a bool to
    /// secondaries via `InitialAssignment.pre_staged_mode` so
    /// dispatch skips the hash machinery.
    source_pre_staged_root: Option<std::path::PathBuf>,
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
        source_pre_staged_root = None,
    ))]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
        distributed_config: Option<DistributedConfig>,
        listen_port: Option<u16>,
        source_pre_staged_root: Option<std::path::PathBuf>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;

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
            source_pre_staged_root,
            task_definition: task_definition.clone().unbind(),
        })
    }

    /// Bulk-queue StageFile notifications for every binary in
    /// `binaries`, broadcast to all `num_secondaries` configured on
    /// this coordinator. Replaces the previous per-binary Python
    /// loop in the SLURM pipeline that called `compute_task_hash`,
    /// `compute_file_hash`, and `notify_stage_file` separately for
    /// each binary — the Python side held no state the loop needed,
    /// the only Python-exclusive piece (relative-path computation)
    /// is trivially Rust, and bundling avoids 4 PyO3 crossings per
    /// binary.
    ///
    /// `source_root` is the absolute path of the consumer's
    /// `--source` directory; the per-binary
    /// `<src_path>` / `<dest_path>` is each binary's path made
    /// relative to it. Binaries whose `path` doesn't sit under
    /// `source_root` (e.g. an absolute-path scan that returned
    /// out-of-tree results) keep their full path — the secondary's
    /// `stage_file` handler treats absolute `src_path` as an
    /// out-of-band-staged source rather than a `src_network`
    /// lookup.
    ///
    /// Reads each binary file once on the primary side to compute
    /// `content_hash` (SHA256). Errors out on the first unreadable
    /// file rather than silently skipping — a broken local
    /// `--source` (or a TaskInfo emitting relative paths that don't
    /// resolve against CWD) is a configuration bug the consumer
    /// wants to surface immediately, not a partial dispatch that
    /// later fails on the secondary as a confusing "not pre-staged
    /// at <path>" error with no breadcrumb pointing back to the
    /// primary's drop.
    ///
    /// If a future consumer needs sentinel-style TaskInfos that
    /// intentionally don't back to a real file, the right shape is
    /// an explicit marker on TaskInfo (e.g. `is_synthetic: bool`)
    /// so this branch can distinguish "no file by design" from
    /// "no file by mistake".
    fn queue_initial_staging(
        &mut self,
        binaries: &Bound<'_, pyo3::types::PyList>,
        source_root: String,
    ) -> PyResult<()> {
        let rust_binaries = crate::pytypes::extract_binaries(binaries)?;
        let source_root = std::path::PathBuf::from(source_root);
        for binary in &rust_binaries {
            let rel = match binary.path.strip_prefix(&source_root) {
                Ok(p) => p.to_string_lossy().into_owned(),
                Err(_) => binary.path.to_string_lossy().into_owned(),
            };
            let file_hash = dynrunner_manager_distributed::compute_task_hash(binary);
            let Some(content_hash) =
                dynrunner_manager_distributed::compute_file_hash(&binary.path)
            else {
                return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
                    "queue_initial_staging: cannot read {} (type_id={}). \
                     Typical causes: TaskInfo emits a relative path that doesn't resolve \
                     against the current working directory; --source points at the wrong tree; \
                     the file is missing or permission-denied. Aborting before dispatch so the \
                     misconfiguration surfaces here rather than as a downstream secondary \
                     'not pre-staged at <path>' error.",
                    binary.path.display(),
                    binary.type_id,
                )));
            };
            for i in 0..self.num_secondaries {
                self.pending_stage_files.push((
                    format!("secondary-{i}"),
                    file_hash.clone(),
                    content_hash.clone(),
                    rel.clone(),
                    rel.clone(),
                ));
            }
        }
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
        let source_pre_staged_root = self.source_pre_staged_root.clone();

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
                    source_pre_staged_root,
                };

                let mut primary: PrimaryCoordinator<_, _, _, TokenizerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        ResourceStealingScheduler::memory(),
                        estimator,
                    );

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

