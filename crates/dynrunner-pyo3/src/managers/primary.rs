use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::PhaseId;
use dynrunner_manager_distributed::{
    compute_initial_staging_entries, PrimaryConfig, PrimaryCoordinator, RunError, StagingError,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_quic::NetworkServer;

use crate::config::distributed::DistributedConfig;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
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
    /// Tasks that exited the inner run loop without a recorded
    /// outcome (`total - completed - failed`). Mirrors
    /// `PrimaryCoordinator::stranded_count` after `run()` returns; the
    /// `stranded` PyO3 getter exposes it so consumers (Python `run.py`,
    /// SLURM pipeline) can include the per-category count in their
    /// "Completed: / Failed: / Stranded:" output and ops scripts can
    /// distinguish "everything ran but some failed" from "the cluster
    /// collapsed before all tasks even dispatched".
    stranded: u32,
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
    /// Local source-tree root for the staging walk. Threaded into
    /// `PrimaryConfig.source_dir` so the inner coordinator owns a
    /// root for the content-hash + per-secondary fan-out without
    /// each caller re-implementing the staging orchestration.
    /// SLURM and network-primary callers both pass it; `None` is
    /// the right default for pre-staged-source mode,
    /// `uses_file_based_items=False`, and remote-only primaries
    /// that never read the source from this filesystem.
    source_dir: Option<std::path::PathBuf>,
    /// Whether dispatched task items back to real files. Read at
    /// construction from `TaskDefinition.uses_file_based_items`
    /// (defaults to True). Propagated to secondaries via
    /// `InitialAssignment.uses_file_based_items` so dispatch skips
    /// extraction-cache resolution and treats `local_path` as an
    /// opaque worker identifier when False.
    uses_file_based_items: bool,
    /// Per-type concurrency caps, harvested from each
    /// `TaskTypeSpec.max_concurrent` at construction. Empty when no
    /// type declares a cap. Forwarded to `PrimaryConfig` so the
    /// scheduler refuses to dispatch beyond the cap.
    max_concurrent_per_type: std::collections::HashMap<dynrunner_core::TypeId, u32>,
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
        source_dir = None,
    ))]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
        distributed_config: Option<DistributedConfig>,
        listen_port: Option<u16>,
        source_pre_staged_root: Option<std::path::PathBuf>,
        source_dir: Option<std::path::PathBuf>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;
        let uses_file_based_items: bool = task_definition
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);

        Ok(Self {
            num_secondaries,
            estimator: topology.estimator,
            phase_deps: topology.phase_deps,
            spawn_secondary: spawn_secondary.clone_ref(py),
            distributed_config: distributed_config.unwrap_or_default(),
            listen_port,
            completed: 0,
            failed: 0,
            stranded: 0,
            pending_stage_files: Vec::new(),
            source_pre_staged_root,
            source_dir,
            uses_file_based_items,
            max_concurrent_per_type: topology.max_concurrent_per_type,
            task_definition: task_definition.clone().unbind(),
        })
    }

    /// Whether items are file-backed (read at construction from
    /// `TaskDefinition.uses_file_based_items`; defaults to True).
    /// Pipeline.py reads this to decide whether to call
    /// `queue_initial_staging` — when False, no primary-side staging
    /// happens at all.
    #[getter]
    fn uses_file_based_items(&self) -> bool {
        self.uses_file_based_items
    }

    /// Bulk-queue StageFile notifications for every binary in
    /// `binaries`, broadcast to all `num_secondaries` configured on
    /// this coordinator.
    ///
    /// PyO3 layer is intentionally a thin extract-and-delegate
    /// shell: the staging walk (path resolution, content hashing,
    /// per-secondary fan-out, error classification) lives in
    /// `dynrunner_manager_distributed::compute_initial_staging_entries`
    /// so the in-process distributed pipeline (which constructs its
    /// `PrimaryCoordinator` directly, never crossing this PyO3
    /// boundary) shares the same code. This wrapper does
    /// PyList → `Vec<TaskInfo>`, delegates, and maps the typed
    /// `StagingError` variants to the consumer-facing Python
    /// exceptions.
    fn queue_initial_staging(
        &mut self,
        binaries: &Bound<'_, pyo3::types::PyList>,
        source_root: String,
    ) -> PyResult<()> {
        let rust_binaries = crate::pytypes::extract_binaries(binaries)?;
        let source_root = std::path::PathBuf::from(source_root);
        // Secondary IDs the SLURM/network primary spawns under;
        // mirrors the format used in `run` below (line ~225) and in
        // `connect.rs`'s missing-secondary diagnostic.
        let secondary_ids: Vec<String> = (0..self.num_secondaries)
            .map(|i| format!("secondary-{i}"))
            .collect();
        let entries = compute_initial_staging_entries(
            &rust_binaries,
            &secondary_ids,
            &source_root,
        )
        .map_err(|e| match e {
            StagingError::SourceUnreadable { .. } => {
                pyo3::exceptions::PyFileNotFoundError::new_err(e.to_string())
            }
        })?;
        self.pending_stage_files.extend(entries);
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
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_mass_death_grace = self.distributed_config.mass_death_grace();
        let dist_mass_death_min_count = self.distributed_config.mass_death_min_count();
        let pending_stage_files = std::mem::take(&mut self.pending_stage_files);
        let source_pre_staged_root = self.source_pre_staged_root.clone();
        let source_dir = self.source_dir.clone();
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        // Load-bearing flip for the setup-deferred run path. When
        // `--source-already-staged` is set on the submitter (so
        // `source_pre_staged_root.is_some()`) AND the Python pipeline
        // has not supplied any pre-discovered binaries (the pipeline
        // skips its own `task.discover_items` walk in pre-staged mode
        // and hands an empty list to `run()`), the submitter primary
        // owes no setup work. The bootstrap `PromotePrimary` it emits
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
                let mut server: NetworkServer<RunnerIdentifier> =
                    match NetworkServer::bind(bind_addr).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to start network server");
                            return;
                        }
                    };
                tracing::info!(port, "primary network server listening");

                // Step 5b: pair the legacy `NetworkServer` (the
                // submitter primary's per-secondary tunnel writers
                // + demuxed inbound) with a `TunneledPeerTransport`
                // so the primary participates in the peer mesh as
                // a real member. Same wire — different trait
                // surface. The PeerCoordinator gets the role-aware
                // mesh view; the legacy `SecondaryTransport::send_to`
                // path keeps working unchanged. `NoPeerTransport`
                // disappears from this call site (it stays valid
                // on the SECONDARY side for the
                // `disable_peer_overlay` firewalled-fabric path).
                let (peer_transport, shared_outgoing, inbound_tap) =
                    dynrunner_transport_tunnel::TunneledPeerTransport::<
                        RunnerIdentifier,
                    >::new("primary".into());
                server.attach_tunnel(shared_outgoing, inbound_tap);

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
                    uses_file_based_items,
                    required_setup_on_promote,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
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
                };

                let mut primary: PrimaryCoordinator<_, _, _, _, RunnerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        peer_transport,
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
                if let Err(RunError::ClusterCollapsed { .. }) = &result {
                    cluster_collapsed = result.err();
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;
                stranded = primary.stranded_count() as u32;
            }));
        });

        // Back with the GIL — terminate secondary processes via the Python objects.
        for process in &child_processes {
            let _ = process.call_method0(py, "kill");
            let _ = process.call_method0(py, "wait");
        }

        self.completed = completed;
        self.failed = failed;
        self.stranded = stranded;

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

