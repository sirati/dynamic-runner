use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::PhaseId;
use dynrunner_manager_distributed::{
    compute_initial_staging_entries, PrimaryConfig, PrimaryCoordinator, RunError, StagingEntry,
    StagingError,
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
    pending_stage_files: Vec<StagingEntry>,
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
    /// Rust-side bundle of the command channel + reinject-cap cell
    /// shared with every `PyPrimaryHandle` minted from this
    /// coordinator. Single concern split out into
    /// `crate::managers::control_plane` so the init/handle/run-take
    /// sequence is owned in one place rather than re-implemented on
    /// each primary-hosting manager. See that module's doc for the
    /// lifecycle contract.
    control_plane: crate::managers::control_plane::PrimaryControlPlane,
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
    /// Optional Python peer-lifecycle listener supplied at `__init__`.
    /// `Some` iff the caller passed `peer_lifecycle_listener=<obj>`;
    /// the object is bridged through
    /// `crate::peer_lifecycle_bridge::PyPeerLifecycleListener` and
    /// registered on the inner `PrimaryCoordinator` at `run()` start.
    /// Constructor-only — no setter — because the manager-distributed
    /// `register_lifecycle_listener` API also requires
    /// pre-`run()` registration (the listener vector is
    /// `mem::take`-d into the spawned dispatcher).
    peer_lifecycle_listener: Option<Py<PyAny>>,

    /// Optional Python task-completion listener supplied at
    /// `__init__`. `Some` iff the caller passed
    /// `task_completed_listener=<callable>`; the callable is bridged
    /// through `crate::task_completed_bridge::PyTaskCompletedListener`
    /// and registered on the inner `PrimaryCoordinator` at `run()`
    /// start via `register_task_completed_listener`. Constructor-only
    /// — same pre-`run()` registration contract as the peer-lifecycle
    /// listener.
    task_completed_listener: Option<Py<PyAny>>,

    /// Optional Python fulfillability matcher supplied at `__init__`.
    /// `Some` iff the caller passed `fulfillability_matcher=<callable>`;
    /// the object is bridged through
    /// `crate::fulfillability_matcher_bridge::PyFulfillabilityMatcher`
    /// and installed on the inner `PrimaryCoordinator` at `run()`
    /// start. Constructor-only — no setter — because the
    /// manager-distributed `set_fulfillability_matcher` API also
    /// requires pre-`run()` registration (the matcher trait object
    /// is owned by `self` and read in the operational `select!`).
    fulfillability_matcher: Option<Py<PyAny>>,

    /// Optional opaque handle to the deployment-mode job manager,
    /// installed by the SLURM pipeline after `run_preparation` returns
    /// and BEFORE `run()` enters. Stored as `Arc<dyn Any + Send + Sync>`
    /// so this crate stays decoupled from any specific batch-system
    /// crate; the respawn caller (inside the manager-distributed
    /// operational loop) downcasts to the concrete handle.
    ///
    /// Threaded into the inner `PrimaryCoordinator` via
    /// `set_slurm_job_manager` at `run()` start. `None` for the
    /// in-process / network-primary callers that don't drive a SLURM
    /// submit-loop.
    slurm_job_manager: Option<Arc<dyn Any + Send + Sync>>,

    /// Respawn policy supplied by the Python caller. `Disabled`
    /// (the default) leaves the inner coordinator's respawn pipeline
    /// unwired — no listener registered, no JoinSet arm reachable.
    /// `OnSecondaryDeath { budget }` translates to an
    /// `enable_respawn` call at `run()` entry, paired with the
    /// spawner stored below.
    respawn_policy: crate::config::respawn::PyRespawnPolicy,

    /// Secondary spawner adapter (currently only
    /// [`PyMultiProcessSpawner`]; the SLURM equivalent will land
    /// behind the same `as_arc` boundary). `None` when no spawner
    /// was supplied at construction; combined with `respawn_policy
    /// = Disabled` this means the respawn pipeline is fully off.
    /// Held as `Py<PyAny>` so the underlying pyclass refcount stays
    /// tied to the coordinator's lifetime; the actual
    /// `Arc<dyn SecondarySpawner>` is extracted via `as_arc` at
    /// `run()` entry.
    respawn_spawner: Option<Py<PyAny>>,
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
        unfulfillable_reinject_max_per_task = None,
        peer_lifecycle_listener = None,
        fulfillability_matcher = None,
        respawn_policy = None,
        respawn_spawner = None,
        task_completed_listener = None,
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
        unfulfillable_reinject_max_per_task: Option<u32>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        fulfillability_matcher: Option<Py<PyAny>>,
        respawn_policy: Option<crate::config::respawn::PyRespawnPolicy>,
        respawn_spawner: Option<Py<PyAny>>,
        task_completed_listener: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;
        let uses_file_based_items: bool = task_definition
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);
        // Build the command-channel + reinject-cap bundle. The helper
        // owns the channel pair, seeds the cap cell from the kwarg,
        // and (later) hands back the handle factory + run-start
        // wiring through a single API. See
        // `crate::managers::control_plane` for the lifecycle.
        let control_plane = crate::managers::control_plane::PrimaryControlPlane::new(
            unfulfillable_reinject_max_per_task,
        );
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
            control_plane,
            peer_lifecycle_listener,
            fulfillability_matcher,
            slurm_job_manager: None,
            respawn_policy: respawn_policy
                .unwrap_or_else(crate::config::respawn::PyRespawnPolicy::rust_disabled),
            respawn_spawner,
            task_completed_listener,
        })
    }

    /// PrimaryHandle factory. Each call returns a freshly-built
    /// handle (with its own in-handle tokio runtime); the underlying
    /// `command_tx` and reinject-cap cell are cloned so multiple
    /// Python control planes / threads can share one coordinator.
    /// Callable BEFORE `run()` so the Python caller can hand the
    /// handle off to its async executor / thread BEFORE the
    /// blocking `run()` starts.
    fn handle(&self) -> PyResult<crate::managers::primary_handle::PyPrimaryHandle> {
        self.control_plane.to_handle()
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
        // Take the parked deployment-mode job-manager handle (if any)
        // out for the detached runtime so the inner coordinator can
        // park it on itself before `run()` enters. `None` for the
        // in-process / network-primary paths that never wire it.
        let slurm_job_manager = self.slurm_job_manager.take();

        // Materialise the respawn pipeline wiring. Only the
        // (budget, spawner) pair is meaningful — when either is
        // absent the inner coordinator's respawn pipeline stays
        // unwired (CCD-5). Extract the `Arc<dyn SecondarySpawner>`
        // here while we still hold the GIL: the inner adapters
        // (today only `PyMultiProcessSpawner`; SLURM lands on the
        // same `as_arc` boundary later) expose a Rust-only
        // `as_arc()` method that clones their internal `Arc`.
        let respawn_budget = self.respawn_policy.to_budget();
        let respawn_spawner_arc: Option<
            std::sync::Arc<
                dyn dynrunner_manager_distributed::primary::respawn::SecondarySpawner,
            >,
        > = match (&self.respawn_spawner, &respawn_budget) {
            (Some(spawner_py), Some(_)) => {
                let bound = spawner_py.bind(py);
                if let Ok(mp) = bound
                    .cast::<crate::managers::multi_process_respawner::PyMultiProcessSpawner>()
                {
                    Some(mp.borrow().as_arc())
                } else if let Ok(slurm) = bound
                    .cast::<crate::slurm::respawn_bridge::PySlurmSpawner>()
                {
                    Some(slurm.borrow().as_arc())
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "respawn_spawner must be a recognised secondary-spawner \
                         pyclass (PyMultiProcessSpawner or PySlurmSpawner); \
                         got an unrecognised type",
                    ));
                }
            }
            // Budget present without spawner (or vice-versa) — the
            // CLI ensures both are supplied together when policy ≠
            // disabled, so this branch is the misconfiguration arm.
            (None, Some(_)) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "respawn_policy is on-secondary-death but no \
                     respawn_spawner was supplied",
                ));
            }
            (_, None) => None,
        };
        // The primary's listen endpoint + pubkey are bound inside the
        // detached tokio runtime (see the `py.detach(|| { … })` block
        // below). Snapshotting them here on the GIL thread is not
        // possible (NetworkServer::bind is async and consumes the
        // port); the detached-runtime block reads them off the bound
        // server and supplies them to `primary.enable_respawn(...)`
        // directly. These placeholders are no longer used past this
        // point — kept as `None` so any future GIL-side caller has a
        // single source-of-truth they cannot accidentally re-derive.
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        // Snapshot the cap, flip `run_started`, and consume the
        // receiver for the detached runtime in one step. The helper
        // owns the single-shot guard and the snapshot ordering; the
        // sender clone returned in `wiring` keeps backing future
        // `handle()` calls.
        let wiring = self.control_plane.take_for_run()?;
        let unfulfillable_reinject_max_per_task = wiring.cap_snapshot;
        let command_tx = wiring.command_tx;
        let command_rx = wiring.command_rx;
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

        // Same shape as the peer-lifecycle listener: take the Python
        // callable out of `self`, wrap it as a `Box<dyn
        // TaskCompletedListener>` at the bridge boundary, and register
        // it on the inner coordinator BEFORE `run()` enters. The
        // dispatcher takes ownership at run-start (see
        // `PrimaryCoordinator::run_pipeline` -> `mem::take` on
        // `task_completed_listeners`), so registration must happen
        // pre-run.
        let task_completed_listener =
            self.task_completed_listener
                .take()
                .map(crate::task_completed_bridge::PyTaskCompletedListener::new);

        // Same shape for the fulfillability matcher kwarg: take the
        // Python callable out of `self`, wrap it as a
        // `Box<dyn FulfillabilityMatcher<RunnerIdentifier>>` at the
        // bridge boundary, and install it on the inner coordinator
        // BEFORE `run()` enters.
        let fulfillability_matcher =
            self.fulfillability_matcher
                .take()
                .map(crate::fulfillability_matcher_bridge::PyFulfillabilityMatcher::new);

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
        // Contract (post-refactor): the callback receives
        // `(primary_url, secondary_id, quic_port)` and returns either
        // `None` (SLURM mode: the wrapper script already spawned the
        // secondary; Rust owns no child here) or a `SubprocessSpec`
        // carrying argv (+ optional env). Rust then spawns the
        // `std::process::Child` itself and OWNS its lifetime — kill +
        // wait at end of `run()` run against the Rust-side `Child`
        // handles, never re-entering Python for subprocess control.
        // This is the Rust-side enforcement of
        // `feedback_features_in_rust_python_is_bridge`: Python
        // assembles argv (legitimate CLI/config concern), Rust owns the
        // resulting process tree.
        let mut child_processes: Vec<std::process::Child> = Vec::new();
        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let spec_obj = self.spawn_secondary.call1(
                py,
                (&primary_url, &secondary_id, 0u16),
            )?;
            // `None` → SLURM no-op path (`_slurm_already_spawned`).
            // Anything else MUST be a `SubprocessSpec`-shaped object
            // (the Python `dynamic_runner.SubprocessSpec` dataclass);
            // we refuse the legacy `subprocess.Popen` shape loudly
            // rather than fall back to Python-side ownership.
            if spec_obj.is_none(py) {
                tracing::info!(
                    secondary_id = %secondary_id,
                    "spawn_secondary returned None; assuming external spawn (SLURM-style)"
                );
                continue;
            }
            let spec = crate::managers::subprocess_spec::SubprocessSpec::from_pyany(
                spec_obj.bind(py),
            )?;
            let child = spec.spawn().map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "failed to spawn secondary {secondary_id}: {e}"
                ))
            })?;
            tracing::info!(
                secondary_id = %secondary_id,
                pid = child.id(),
                "spawned secondary process (Rust-owned Child)"
            );
            child_processes.push(child);
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

                // Capture the primary's listen endpoint + cert PEM
                // BEFORE moving `server` into the coordinator below.
                // These are the trust anchors threaded through
                // `enable_respawn` and surfaced to each respawned
                // secondary via `SecondarySpawnSpec` (per-spawn, so
                // a future cert rotation reaches downstream
                // respawns).
                //
                // Endpoint format mirrors the QUIC-listen surface in
                // `NetworkServer::bind`: `127.0.0.1:<port>`. In SLURM
                // mode the secondary dials `localhost:<gateway_port>`
                // via the per-secondary reverse tunnel — the gateway-
                // facing port differs but the same PEM authenticates
                // either path. In multi-process local mode the
                // submitter and the spawned subprocess share the same
                // loopback, so this endpoint is already the dial-able
                // address.
                let respawn_primary_endpoint = format!("127.0.0.1:{}", server.port());
                let respawn_primary_pubkey_pem = server.cert_pem().to_string();

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
                    unfulfillable_reinject_max_per_task,
                };

                let mut primary: PrimaryCoordinator<_, _, _, _, RunnerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        peer_transport,
                        ResourceStealingScheduler::memory(),
                        estimator,
                    );

                // Swap in the Python-facing command channel so the
                // `PrimaryHandle` Python is holding talks to the same
                // receiver the operational loop reads from.
                primary.replace_command_channel(command_tx, command_rx);

                // Relay the SLURM-pipeline-parked deployment-mode job
                // manager onto the inner coordinator BEFORE `run()`
                // enters. Same pre-run contract as the other registration
                // setters on `PrimaryCoordinator`: the respawn path
                // reads the field from inside the operational loop, so
                // late installs would be invisible.
                if let Some(jm) = slurm_job_manager {
                    primary.set_slurm_job_manager(jm);
                }

                // Wire the respawn pipeline iff both a budget and a
                // spawner are present. The early-return branch above
                // guarantees they're either both `Some` or both
                // `None`, so a single match-on-Some covers the
                // enabled arm without re-validation. CCD-5: this is
                // the ONLY call site that touches the respawn
                // wiring; no downstream `if policy_enabled` checks
                // live on the hot path.
                if let (Some(spawner), Some(budget)) =
                    (respawn_spawner_arc, respawn_budget)
                {
                    primary.enable_respawn(
                        spawner,
                        budget,
                        respawn_primary_endpoint,
                        respawn_primary_pubkey_pem,
                    );
                }

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE `run()` enters — the coordinator's
                // `register_lifecycle_listener` contract requires
                // pre-run registration because `run()` `mem::take`-s
                // the listener vector into the spawned dispatcher.
                if let Some(listener) = peer_lifecycle_listener {
                    primary.register_lifecycle_listener(listener);
                }

                // Same shape for the task-completion listener: an
                // independent dispatcher pair with the same pre-run
                // registration requirement.
                if let Some(listener) = task_completed_listener {
                    primary.register_task_completed_listener(listener);
                }

                // Same boundary as the lifecycle listener: install
                // the Python fulfillability matcher (if any) BEFORE
                // `run()` enters. The setter is one line; the
                // matcher invocation cadence + state-filter +
                // ReinjectTask fire all live behind the
                // manager-distributed API.
                if let Some(matcher) = fulfillability_matcher {
                    primary.set_fulfillability_matcher(matcher);
                }

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

        // Back with the GIL — terminate secondary processes through
        // the Rust-owned `Child` handles. No re-entry into Python for
        // subprocess control: lifecycle is fully Rust-side after the
        // initial `SubprocessSpec` handoff.
        for mut child in child_processes {
            let pid = child.id();
            if let Err(e) = child.kill() {
                tracing::debug!(pid, error = %e, "child.kill() failed (already exited?)");
            }
            if let Err(e) = child.wait() {
                tracing::debug!(pid, error = %e, "child.wait() failed");
            }
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

// Rust-only surface for the SLURM-pipeline orchestrator. Not exposed
// to Python because the parked handle is the in-process Rust
// `SlurmJobManager` instance — it travels Rust-to-Rust across the
// pipeline → coordinator boundary, never through Python identity.
impl PyPrimaryCoordinator {
    /// Park the deployment-mode job-manager handle so the inner
    /// `PrimaryCoordinator` sees it at `run()` start. Called by
    /// `slurm::pipeline::drive_rust_primary` after `run_preparation`
    /// returns and BEFORE `run()` enters.
    ///
    /// Single concern: relay the opaque handle into the
    /// manager-distributed coordinator. The PyO3 wrapper holds it
    /// between construction and `run()` because the inner
    /// `PrimaryCoordinator` does not exist yet at the call site —
    /// it's built inside the detached tokio runtime.
    pub(crate) fn set_slurm_job_manager_from_rust(
        &mut self,
        jm: Arc<dyn Any + Send + Sync>,
    ) {
        self.slurm_job_manager = Some(jm);
    }
}

