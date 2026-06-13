//! `PyDistributedManager::run` — drives the in-process primary +
//! N secondaries pipeline on a detached tokio runtime over channel
//! transports. Also exposes the `completed` / `failed` / `stranded`
//! getters Python `run_distributed` reads after `run()` returns.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::process::{
    LocalRole, Mesh, MeshHost, Node, NodeRunInputs, PrimaryRunArgs, RunTerminal, SeedSource,
};
use dynrunner_manager_distributed::{
    GracefulAbortTrigger, PrimaryConfig, PrimaryCoordinator, RunError, SecondaryConfig,
    SecondaryCoordinator,
};
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::config::connection::ConnectionMode;
use crate::identifier::RunnerIdentifier;
use crate::managers::secondary::run::{
    PromotedPrimaryRecipeInputs, build_promoted_primary_recipe, build_setup_discovery_fn,
    promoted_command_channel_cell,
};
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;

use super::PyDistributedManager;

#[pymethods]
impl PyDistributedManager {
    /// Run the distributed processing pipeline.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let num_workers = self.num_workers_per_secondary;
        let max_resources_per_secondary = self.max_resources_per_secondary.clone();
        let estimator = self.estimator.clone();
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_path = self.log_path.clone();
        let log_paths = self.log_paths.clone();
        // Single scheduler-tuning snapshot is shared between the
        // in-process primary AND every spawned secondary; cloning into
        // the per-secondary task closure below preserves the same
        // budget shape across the cluster.
        let scheduler_config = self.scheduler_config.clone();
        // Panik-watcher config — same kwarg surface as the standalone
        // primary/secondary pyclasses. Shared verbatim by the
        // in-process primary AND every spawned secondary so a panik
        // file appearing on the host triggers the SAME response on
        // every coordinator in the process; without that the in-
        // process secondaries would silently outlive a primary panik
        // (their workers are spawned in their own pgids and survive
        // their parent's exit).
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval =
            std::time::Duration::from_secs_f64(self.panik_watcher_poll_interval_secs);
        // Compose the per-secondary memprofile output dir once on
        // the GIL thread so the per-secondary spawn closures below
        // receive identical `Option<PathBuf>` values without each
        // re-deriving from `self`. The operator's `output_dir`
        // (always set) wins over the SLURM wrapper bind-mount
        // probe — in-process distributed runs never expose the
        // wrapper but always have a Python-supplied output dir.
        let memprofile_output_dir =
            crate::managers::secondary::run::resolve_secondary_memprofile_dir(
                self.memprofile_enabled,
                Some(self.output_dir.as_path()),
            );
        // Same shape as `PySecondaryCoordinator::run`: derive the
        // memuse log path on the GIL thread so every per-secondary
        // spawn closure clones it as a ready-made
        // `Option<PathBuf>`. Defaults to
        // `{self.output_dir}/memuse.log`; `None` only if
        // `self.output_dir` is itself unset (it isn't — the field
        // is always populated by the constructor).
        let memuse_log_path = dynrunner_manager_local::memuse::derive_memuse_log_path(
            Some(self.output_dir.as_path()),
            None,
        );

        // Pre-compute per-secondary log directories under the GIL
        // before detaching for the tokio runtime. Each secondary gets
        // its own `{secondary_id}` subdirectory so the default
        // `worker_<id>.log` filename never collides across secondaries
        // on a shared mount, and `create_dir_all` errors surface here at
        // run start rather than as silent log loss. (`resolve_log_dir`
        // still imports Python's `datetime` for the `{timestamp}`
        // placeholder, which the default template no longer uses.)
        // `log_path` (not `output_dir`) is the log-mount root — on
        // SLURM deployments it points at `/app/log-network` while
        // `output_dir` is `/app/out-network`. Single-host callers
        // that did not supply a separate log dir get `log_path ==
        // output_dir` from the fallback in `LoadedTaskDefinition`.
        let mut sec_log_dirs: Vec<(String, PathBuf)> = Vec::with_capacity(num_secondaries as usize);
        for i in 0..num_secondaries {
            let sid = format!("sec-{i}");
            let dir = log_paths.resolve_log_dir(py, &log_path, &sid)?;
            std::fs::create_dir_all(&dir).map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "failed to create log directory {dir:?} for {sid}: {e}"
                ))
            })?;
            sec_log_dirs.push((sid, dir));
        }
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        // The PRIMARY's quorum-proceed (straggler) window is DERIVED (unset
        // → 80% of the secondaries' `unconfigured_deadline`, the default IS
        // the cap; explicit → honored; both capped strictly below the
        // deadline). The in-process mesh has no bootstrap dial, so this is
        // the knob's only consumer here. See
        // `dynrunner_manager_distributed::derive_connect_timeout`.
        let dist_connect_timeout = dynrunner_manager_distributed::derive_connect_timeout(
            self.distributed_config.connect_timeout_override(),
            self.distributed_config.unconfigured_deadline(),
        );
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
        let worker_spec = self.worker_spec.clone();
        // Per-type subprocess dispatch: the factory carries the full
        // `TypeRegistry`. `spawn_worker` defaults to `types.first()`
        // for initial pool init (preserves pre-fix single-type
        // behaviour); `spawn_worker_for_type` consults the registry
        // for per-task respawn on TypeId mismatch. Cloned per
        // secondary below in the spawn loop.
        if self.types.first().is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            ));
        }
        let types = self.types.clone();
        let skip_existing = self.skip_existing;
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        let phase_deps = self.phase_deps.clone();
        // The consumer's `may_be_empty` phase opt-out, captured for
        // registration on the in-process primary (the empty-drain
        // proceed-or-fail discriminator).
        let phase_may_be_empty = self.phase_may_be_empty.clone();
        // The shared node-local run-config (the operator's
        // `args.forwarded_argv`). One copy seeds the in-process primary's
        // `PrimaryConfig` and a per-secondary clone seeds each in-process
        // `SecondaryConfig`, so every node answers `RequestRunConfig`
        // identically.
        let forwarded_argv = self.forwarded_argv.clone();
        let source_pre_staged_root = self.source_pre_staged_root.clone();
        // In-process `--source-already-staged` signal. Under mesh-always the
        // in-process setup peer RELOCATES (uniform with SLURM); on the
        // pre-staged path it seeds `DiscoveryDebt=Owed` (via
        // `SeedSource::RelocatedSeed`), and the RELOCATE TARGET (a promoted
        // in-process secondary) runs `discover_on_promotion` itself on the
        // shared host fs (the driver gates on the inherited `Owed` marker). The
        // COLD in-process path (no `--source-already-staged`) discovers the
        // corpus upfront in Python and cold-seeds it, so the snapshot the
        // target inherits carries the tasks and the marker stays `Settled`.
        // Captured as a bool here because `source_pre_staged_root` moves into
        // `PrimaryConfig` inside the detached-runtime closure before the seed
        // is built.
        let source_pre_staged = source_pre_staged_root.is_some();
        // Framework file-staging selector (#489 P3/P4): map the
        // `stage_via_setup_tasks` flag to the typed `StagingStrategy` the
        // `PrimaryConfig` consumes. `Copy`, so it is shared by the bootstrap
        // in-process primary's config AND every per-secondary promote recipe
        // (the relocate target seeds the setup tasks in `discover_on_promotion`).
        let staging_strategy = if self.stage_via_setup_tasks {
            dynrunner_manager_distributed::StagingStrategy::SetupTasks
        } else {
            dynrunner_manager_distributed::StagingStrategy::Disabled
        };

        // Phase 5B: re-acquire the GIL from the coordinator's LocalSet
        // and dispatch to the Python TaskDefinition's `on_phase_*`
        // methods. Built before `py.detach` so the closures can capture
        // ref-bumped `Py<PyAny>` clones.
        let on_phase_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
            crate::managers::lifecycle::make_on_phase_start(self.task_definition.clone_ref(py)),
        );
        // Honest on_phase_end: the in-process primary's hook records a
        // consumer raise into this latch; the SAME latch is installed on
        // the coordinator below so the cascade surfaces a non-zero
        // `RunError::FatalPolicyExit`.
        let phase_hook_raise_latch =
            dynrunner_manager_distributed::PhaseHookRaiseLatch::new();
        let on_phase_end: crate::managers::lifecycle::OnPhaseEnd =
            Box::new(crate::managers::lifecycle::make_on_phase_end_with_raise_latch(
                self.task_definition.clone_ref(py),
                phase_hook_raise_latch.clone(),
            ));
        // The setup peer's own `custom_message_handler` hook ref-bump
        // (F5): a custom message can land during the pre-relocate
        // bootstrap window, so the setup-peer coordinator installs the
        // SAME hook the promoted primary does (built inside the detached
        // runtime from its command sender, below).
        let setup_custom_message_handler_def =
            crate::custom_message_bridge::has_custom_message_handler(
                &self.task_definition.bind(py).clone(),
            )
            .then(|| self.task_definition.clone_ref(py));

        // Per-secondary PROMOTE-recipe inputs, built on the GIL thread so each
        // captures its own `Py<PyAny>` ref-bumps. Under mesh-always the
        // in-process setup peer relocates onto ONE of these secondaries, whose
        // promoted same-peer primary then OWNS the phase machine + fires the
        // `on_phase_*` cascade on the SAME Python `TaskDefinition` the run
        // targets, and (on the pre-staged path) runs `discover_on_promotion`
        // with its own discovery policy. Built per-secondary (any one may be
        // the relocate target) and consumed by the recipe builder in the spawn
        // loop below. The `discover` closure consumes its `Py` handles on its
        // single fire, so each secondary needs its OWN clone.
        let mut sec_phase_lifecycle_callbacks: Vec<(
            crate::managers::lifecycle::OnPhaseStart,
            crate::managers::lifecycle::OnPhaseEnd,
            dynrunner_manager_distributed::PhaseHookRaiseLatch,
        )> = Vec::with_capacity(num_secondaries as usize);
        // Per-secondary RAW discovery handles for the pre-staged path: the
        // `task_definition` + `task_args` Py refs + the staged-corpus root.
        // Captured as bare `Py<PyAny>` (GIL-independent, `Ungil`-safe to move
        // into the detached runtime) — the `SetupDiscovery` policy CLOSURE
        // (not `Ungil`) is built from these INSIDE the per-secondary spawn task
        // via `build_setup_discovery_fn`, mirroring how the SLURM secondary
        // builds its own. `None` on the cold path (no discovery anywhere).
        // (task_definition, task_args, staged-corpus-root) — the raw,
        // `Ungil`-safe discovery handles per secondary; the `SetupDiscovery`
        // closure is built from these INSIDE the detached runtime.
        type SecDiscoveryHandle = (Py<PyAny>, Py<PyAny>, PathBuf);
        let mut sec_discovery_handles: Vec<Option<SecDiscoveryHandle>> =
            Vec::with_capacity(num_secondaries as usize);
        // Per-secondary `custom_message_handler` ref-bumps (F5) for the
        // promote recipes — `None` when the consumer exposes no hook.
        let mut sec_custom_message_handler_defs: Vec<Option<Py<PyAny>>> =
            Vec::with_capacity(num_secondaries as usize);
        for _ in 0..num_secondaries {
            let on_start: crate::managers::lifecycle::OnPhaseStart = Box::new(
                crate::managers::lifecycle::make_on_phase_start(self.task_definition.clone_ref(py)),
            );
            // Honest `on_phase_end`: record a consumer-hook raise into a
            // per-secondary latch the recipe installs on the promoted primary
            // (`set_phase_hook_raise_latch`), so an in-process relocate-target's
            // `on_phase_end` raise surfaces a non-zero `FatalPolicyExit`.
            let raise_latch = dynrunner_manager_distributed::PhaseHookRaiseLatch::new();
            let on_end: crate::managers::lifecycle::OnPhaseEnd =
                Box::new(crate::managers::lifecycle::make_on_phase_end_with_raise_latch(
                    self.task_definition.clone_ref(py),
                    raise_latch.clone(),
                ));
            sec_phase_lifecycle_callbacks.push((on_start, on_end, raise_latch));
            // The promoted in-process primary's `custom_message_handler`
            // hook ref-bump (F5), captured per secondary (any one may be
            // the relocate target) IFF the duck-typed attribute exists.
            // The recipe builds the closure at fire time from the
            // promoted coordinator's own internal command sender.
            sec_custom_message_handler_defs.push(
                crate::custom_message_bridge::has_custom_message_handler(
                    &self.task_definition.bind(py).clone(),
                )
                .then(|| self.task_definition.clone_ref(py)),
            );
            // Pre-staged: capture this secondary's own discovery Py-handle
            // clones (the corpus root IS `source_pre_staged_root` — in-process
            // shares one fs). The relocate target inherits `DiscoveryDebt=Owed`
            // and runs discovery on the host fs. Cold: `None`.
            sec_discovery_handles.push(source_pre_staged_root.clone().map(|root| {
                (
                    self.task_definition.clone_ref(py),
                    self.task_args.clone_ref(py),
                    root,
                )
            }));
        }

        // Take the Python peer-lifecycle listener (if any) out of
        // `self` so it can move into the detached tokio runtime.
        // Wrapped through `PyPeerLifecycleListener::new` into a
        // `Box<dyn LifecycleListener>` at the boundary so the
        // manager-distributed registration API stays
        // PyO3-agnostic. The in-process secondaries do NOT receive
        // the listener (see the field doc on
        // `peer_lifecycle_listener`).
        let peer_lifecycle_listener = self
            .peer_lifecycle_listener
            .take()
            .map(crate::peer_lifecycle_bridge::PyPeerLifecycleListener::new);

        // Same shape for the task-completion listener: independent
        // dispatcher pair on the in-process primary; same
        // pre-`run()` registration contract.
        let task_completed_listener = self
            .task_completed_listener
            .take()
            .map(crate::task_completed_bridge::PyTaskCompletedListener::new);

        // Snapshot the cap, flip `run_started`, and consume the
        // receiver for the detached runtime in one step. The helper
        // owns the single-shot guard and the snapshot ordering; the
        // sender clone returned in `wiring` keeps backing future
        // `handle()` calls. Mirrors `PyPrimaryCoordinator::run`.
        let wiring = self.control_plane.take_for_run()?;
        let unfulfillable_reinject_max_per_task = wiring.cap_snapshot;
        // The ONE Python `PrimaryHandle` command channel, wrapped in the
        // shared take-once cell every promotable secondary's recipe clones.
        // Under mesh-always the setup peer ALWAYS relocates the primary onto
        // an in-process secondary, so the channel must follow the AUTHORITY:
        // the relocate winner claims the pair at promotion
        // (`replace_command_channel`) and services the consumer's
        // `on_phase_end → primary_handle.spawn_tasks` lazy-injection. The
        // setup peer keeps the internal channel `PrimaryCoordinator::new`
        // mints (its pre-relocate window only ever drains its own
        // custom-message-handler commands); handle commands issued before the
        // promotion buffer in the channel and are drained by the promoted
        // primary's loop.
        let python_command_channel =
            promoted_command_channel_cell(wiring.command_tx, wiring.command_rx);

        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut stranded = 0u32;
        // Cluster-collapsed signal carried out of the detached tokio
        // runtime — see `PyPrimaryCoordinator::run` for the full
        // rationale; the in-process distributed manager mirrors the
        // same translation so a collapse here surfaces as a
        // `RuntimeError` to the Python caller of `run_distributed`.
        let mut cluster_collapsed: Option<RunError> = None;
        // Panik outcome carried out of the detached tokio runtime —
        // same shape as `PyPrimaryCoordinator::run`. `Some` iff the
        // in-process primary's `run` returned `RunError::PanikShutdown`.
        let mut panik_shutdown_path: Option<std::path::PathBuf> = None;
        // Pre-phase duplicate-task-id carried out of the detached tokio
        // runtime — same shape as `PyPrimaryCoordinator::run`. `Some`
        // iff the in-process primary's `run` aborted on a #3a duplicate
        // (`RunError::DuplicateTaskIdPrePhase`); the GIL-side tail
        // raises a `PyRuntimeError` so the wrapper does not return
        // exit 0.
        let mut duplicate_task_id_pre_phase: Option<RunError> = None;
        // Cluster-wide `RunAborted` OBSERVED by the setup node after it
        // relocated (the promoted authority broadcast the verdict). Carries
        // the broadcast reason VERBATIM — `RunAborted` is the general abort
        // verdict (a #3a duplicate, a consumer-hook fatal-exit, an
        // empty-drain honesty abort, ...), so it must NOT be re-typed as
        // `DuplicateTaskIdPrePhase` (which appends a fix-your-duplicate-ids
        // lecture that misdirects the operator for every non-duplicate
        // abort). Mirrors `PyPrimaryCoordinator::run`'s `relocated_aborted`.
        let mut relocated_aborted: Option<String> = None;
        // Policy-abort terminal — `Some(RunError::FatalPolicyExit)` iff the
        // node's terminal was a deliberate policy abort (a panicked role task,
        // or an invalid-task fatal-exit). RAISES at the GIL-side tail (never
        // the `Other` swallow). Same shape as `PyPrimaryCoordinator::run`.
        let mut fatal_policy_exit: Option<RunError> = None;
        // Spawn-rejected terminal: a runtime `spawn_tasks` batch was
        // wholesale-rejected so the phase dispatched ZERO tasks. RAISES at
        // the GIL-side tail (never the `Other` swallow). Same shape as
        // `PyPrimaryCoordinator::run`.
        let mut spawn_rejected: Option<RunError> = None;
        // No-relocation-target config error. A LIVE error path under
        // mesh-always: the in-process setup peer relocates (uniform with
        // SLURM), so a fleet where no in-process secondary is eligible
        // (`can_be_primary=false`, all observers, or zero secondaries)
        // surfaces `RunError::NoRelocationTarget`. RAISES at the GIL-side tail
        // rather than silently swallowing.
        let mut no_relocation_target: Option<RunError> = None;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                let mut sec_handles = Vec::new();

                // Arm the operator's SIGUSR2 graceful-abort trigger FIRST —
                // before the mesh build and the fleet bring-up — so a signal
                // sent during the in-process primary's bootstrap window is
                // latched instead of killing the process via the kernel's
                // default disposition. Exactly ONE `user_defined2` stream per
                // process: armed here, injected into the setup-peer primary
                // below (`register_graceful_abort_trigger`); the in-process
                // secondaries never consume it (the primary IS the abort
                // authority). A latched delivery initiates the graceful abort
                // on the primary's first poll, and rides the observer handoff
                // if the setup peer relocates.
                let mut abort_trigger = Some(GracefulAbortTrigger::arm());

                // Build the FULL N+1-node mpsc peer mesh up front via the
                // EXISTING all-to-all builder (`transport-channel::peer_mesh`):
                // one `ChannelPeerTransport` per node — the setup peer
                // (`SETUP_NODE_ID`) PLUS every secondary (`sec-{i}`) — each a
                // first-class member of a fully-connected mpsc mesh. This
                // replaces the old hand-rolled STAR (a `TunneledPeerTransport`
                // primary + per-secondary channels + forwarder tasks that
                // folded ONLY the primary in): under mesh-always the setup peer
                // RELOCATES onto a secondary, and the promoted secondary must
                // reach every other secondary directly — the all-to-all mesh
                // gives every node that reach. The coordinator stays blind to
                // the backend (it sees only `Mesh`/`PeerTransport`); the only
                // difference from the SLURM QUIC mesh is the transport. No new
                // transport-channel machinery — the primitive already exists.
                let mut peer_ids: Vec<String> =
                    Vec::with_capacity(num_secondaries as usize + 1);
                peer_ids.push(dynrunner_core::SETUP_NODE_ID.to_string());
                for (sid, _) in &sec_log_dirs {
                    peer_ids.push(sid.clone());
                }
                let mut mesh_transports =
                    dynrunner_transport_channel::peer_mesh::<RunnerIdentifier>(&peer_ids);
                // Index 0 is the setup peer's transport (peer_ids[0] ==
                // SETUP_NODE_ID); the remaining transports, in `sec_log_dirs`
                // order, are the secondaries'. Drain the secondary transports
                // off the front-trimmed tail so each per-secondary closure owns
                // its own member of the mesh.
                let peer_transport = mesh_transports.remove(0);
                let mut sec_transports = mesh_transports.into_iter();

                for (
                    (
                        (
                            (secondary_id, sec_log),
                            (sec_on_phase_start, sec_on_phase_end, sec_phase_hook_raise_latch),
                        ),
                        sec_discovery_handle,
                    ),
                    sec_custom_message_handler_def,
                ) in sec_log_dirs
                    .into_iter()
                    .zip(sec_phase_lifecycle_callbacks)
                    .zip(sec_discovery_handles)
                    .zip(sec_custom_message_handler_defs)
                {
                    // This secondary's pre-wired all-to-all mesh transport (its
                    // `outgoing` table already reaches the setup peer + every
                    // peer secondary; its inbox is fed by their outboxes).
                    let transport = sec_transports
                        .next()
                        .expect("one mesh transport per secondary (peer_mesh built N+1)");

                    let sec_python = python_executable.clone();
                    let sec_worker_spec = worker_spec.clone();
                    let sec_source = source_dir.clone();
                    let sec_output = output_dir.clone();
                    let sec_log_paths = log_paths.clone();
                    let sec_types = types.clone();
                    let sec_estimator = estimator.clone();
                    let sec_max_resources = max_resources_per_secondary.clone();
                    let sec_scheduler_config = scheduler_config.clone();
                    let sec_panik_paths = panik_watcher_paths.clone();
                    let sec_panik_poll = panik_watcher_poll_interval;
                    let sec_memprofile_output_dir = memprofile_output_dir.clone();
                    let sec_memuse_log_path = memuse_log_path.clone();
                    // Per-secondary clone so the spawned `move` task owns its
                    // own copy; the primary config below still holds the
                    // original to seed itself identically.
                    let sec_forwarded_argv = forwarded_argv.clone();
                    // Second per-secondary clones for the PROMOTE recipe: the
                    // `SecondaryCoordinator` below moves one scheduler/estimator
                    // copy; the recipe (which builds the promoted same-peer
                    // primary on relocation) needs its own.
                    let promote_scheduler_config = scheduler_config.clone();
                    let promote_estimator = estimator.clone();
                    // Every promotable secondary's recipe clones the SAME
                    // take-once cell holding the ONE Python `PrimaryHandle`
                    // command channel; the relocate winner claims the pair so
                    // the handle reaches the live authority.
                    let promote_command_channel = python_command_channel.clone();
                    // The run's phase graph for this secondary's discovery
                    // policy (pre-staged path) — the relocate target pairs it
                    // with the discovered corpus in `discover_on_promotion`.
                    let sec_phase_deps = phase_deps.clone();
                    // Staging/discovery config the PROMOTE recipe threads into
                    // the relocated primary's `PrimaryConfig` so its dispatch
                    // matches the submitter's (the relocate-staging fix). The
                    // in-process setup peer shares one fs, so the pre-staged
                    // root IS `source_pre_staged_root` and the source root IS
                    // `source_dir`. Per-secondary clones because each spawned
                    // `move` task owns its own copy.
                    let promote_pre_staged_root = source_pre_staged_root.clone();
                    let promote_source_dir = Some(source_dir.clone());
                    // The two staging-dispatch flags the PROMOTE recipe stamps,
                    // sourced from the LOCAL PRODUCER (NOT the
                    // `InitialAssignment`-fed cell — a relocate-target's cell is
                    // at `Default` at promotion). Both are already extracted on
                    // this in-process manager: `uses_file_based_items` off the
                    // `task_definition`, `source_pre_staged` =
                    // `source_pre_staged_root.is_some()` (mirroring the
                    // submitter's discriminant). `Copy` bools, captured by the
                    // per-secondary `move` task below.
                    let promote_uses_file_based_items = uses_file_based_items;
                    let promote_pre_staged_mode = source_pre_staged;

                    let handle = tokio::task::spawn_local(async move {
                        // This secondary's all-to-all mpsc mesh transport
                        // (`peer_mesh`, built above): an ordinary first-class
                        // member that reaches the setup peer + every peer
                        // secondary directly — so a promotion lands it on a
                        // routable mesh. Moved in by value; no per-role uplink
                        // leg, no forwarder task.
                        let transport = transport;
                        // The local-slot peer-id for the secondary's `Node`
                        // mesh registration (the same logical id the mesh keys
                        // this secondary under).
                        let sec_id_for_slot = secondary_id.clone();
                        let config = SecondaryConfig {
                            secondary_id,
                            num_workers,
                            max_resources: sec_max_resources,
                            hostname: "localhost".into(),
                            keepalive_interval: dist_keepalive,
                            // In-process mode: primary and
                            // secondaries share filesystem
                            // visibility, so the staging walk's
                            // relative `src_path` (e.g.
                            // `input-0.txt`, derived from
                            // `binary.path` post-strip-prefix)
                            // resolves under the primary's
                            // `source_dir`. Without this set,
                            // `stage_and_register`'s `stage_file`
                            // call rejects every relative
                            // src_path with "no src_network is
                            // configured" and the next
                            // TaskAssignment surfaces as the
                            // legacy "expected StageFile
                            // notification first" failure even
                            // though staging WAS queued — pairs
                            // with the staging-walk fix above:
                            // both are needed for the in-process
                            // pipeline to actually process file-
                            // backed items.
                            src_network: Some(sec_source.clone()),
                            src_tmp: None,
                            peer_timeout: dist_peer_timeout,
                            keepalive_miss_threshold: dist_keepalive_miss_threshold,
                            retry_max_passes: dist_retry_max_passes,
                            oom_retry_max_passes: dist_oom_retry_max_passes,
                            primary_link_failure_threshold: dist_primary_link_failure_threshold,
                            primary_link_failure_window: dist_primary_link_failure_window,
                            // Internal default (no operator kwarg for the
                            // app-silence failover backstop); single source of
                            // truth lives in the distributed crate.
                            primary_silence_backstop:
                                dynrunner_manager_distributed::DEFAULT_PRIMARY_SILENCE_BACKSTOP,
                            unconfigured_deadline: dist_unconfigured_deadline,
                            // Mesh-always: in-process secondaries hold a full
                            // all-to-all mpsc mesh (`peer_mesh` above), so each
                            // is a promotion-eligible compute peer. The setup
                            // peer relocates onto the lowest-id eligible
                            // secondary; `true` is what makes it a valid
                            // `select_relocation_target` candidate AND lights up
                            // its `PromotionSignal` path (paired with the
                            // `promote` recipe below).
                            can_be_primary: true,
                            resource_check_interval: dist_resource_check_interval,
                            log_oom_watcher: dist_log_oom_watcher,
                            promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
                            // In-process distributed manager: the
                            // `ReinjectTask` per-task budget cap, mirrored
                            // from the in-process primary's
                            // `PrimaryConfig` so an externally-issued
                            // `reinject_task` honours the operator's knob
                            // symmetrically regardless of which authority
                            // (live or same-peer) services it. Inert on
                            // a secondary until it holds the primary role
                            // via its same-peer primary.
                            unfulfillable_reinject_max_per_task,
                            // In-process distributed manager runs primary
                            // and secondaries in the same process, so
                            // nesting the workers cgroup would tighten
                            // the cap on the shared address space.
                            // Leave unset; only the network-secondary
                            // path (where the secondary runs in its own
                            // SLURM container) opts in via
                            // `--mem-manager-reserved`.
                            mem_manager_reserved_bytes: None,
                            // Per-secondary memprofile output dir
                            // resolved on the GIL thread above from
                            // the operator's `--memprofile` opt-in
                            // plus `self.output_dir` (always set).
                            // `Some(path)` activates per-task
                            // sampling on the in-process secondary
                            // path symmetrically with the SLURM and
                            // multi-computer-local secondaries.
                            output_dir: sec_memprofile_output_dir.clone(),
                            // Default-on aggregate memuse log under
                            // `{self.output_dir}/memuse.log`. Same
                            // shape every other dispatch path
                            // produces; preserves the
                            // `Option<PathBuf>` test-fixture
                            // flexibility (None = silent).
                            memuse_log_path: sec_memuse_log_path.clone(),
                            // The shared node-local run-config: the
                            // in-process distributed manager dials no
                            // cold-start fetch (every node shares the
                            // submitter's argv directly), so each
                            // in-process secondary seeds the SAME
                            // operator argv the primary holds.
                            forwarded_argv: sec_forwarded_argv,
                        };

                        let estimator = sec_estimator;

                        let factory = SubprocessWorkerFactory {
                            python_executable: sec_python,
                            source_dir: sec_source,
                            output_dir: sec_output,
                            log_dir: sec_log,
                            log_paths: sec_log_paths,
                            // In-process distributed mode shares the submitter's
                            // eagerly-parsed namespace (no cold-start run-config
                            // fetch / deferral): seed each per-secondary cell
                            // once and never swap.
                            types: crate::task_def::shared_registry(sec_types),
                            skip_existing,
                            connection_mode: ConnectionMode::Socketpair,
                            manual_start_worker: false,
                            worker_spec: sec_worker_spec.clone(),
                            child_processes: Vec::new(),
                        };

                        // Wrap the channel-backed mesh transport in the
                        // role-demux `Mesh` and register the Secondary slot,
                        // minting the coordinator's `(client, inbox)` ends + the
                        // `Arc<RoleSlot>` the per-secondary `Node` holds.
                        let mut sec_mesh = Mesh::new(transport);
                        let (sec_slot, sec_client, sec_inbox) = sec_mesh.register_local_role(
                            LocalRole::Secondary,
                            PeerId::from(sec_id_for_slot.as_str()),
                        );

                        let mut secondary: SecondaryCoordinator<_, _, _, RunnerIdentifier> =
                            SecondaryCoordinator::new(
                                config,
                                sec_client,
                                sec_inbox,
                                sec_scheduler_config.build_memory_scheduler(),
                                estimator,
                            );
                        // The egress edge resolves `Destination::Primary` to
                        // the in-process submitter id while the role table is
                        // cold — matching the folded primary mesh-link's key.
                        secondary.set_bootstrap_primary_id(dynrunner_core::SETUP_NODE_ID.to_string());

                        // Per-secondary panik watcher. One watcher per
                        // coordinator is the simplest correct shape: a
                        // single shared `oneshot::Sender` couldn't
                        // fan out to N receivers, and broadcasting
                        // through a different channel type would
                        // complicate the framework API. Polling
                        // overhead at the user-spec'd 10s cadence is
                        // negligible (one stat per path per 10s, per
                        // secondary).
                        let mut panik_watcher =
                            dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                                dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                                    paths: sec_panik_paths,
                                    poll_interval: sec_panik_poll,
                                    // SECONDARY-role spawner (in-
                                    // process, alongside an in-
                                    // process primary). Same
                                    // rationale as the standalone
                                    // secondary in
                                    // `managers/secondary/run.rs`:
                                    // host-side shutdown-manager
                                    // forwards SLURM signals as
                                    // `kill -TERM` into this
                                    // process, and the secondary's
                                    // watcher must route that into
                                    // the panik cascade. NOTE the
                                    // primary running in the SAME
                                    // process has a SEPARATE
                                    // watcher (below) with
                                    // SIGTERM listening OFF —
                                    // primary's shutdown
                                    // semantics are out of scope.
                                    // Because only ONE handler is
                                    // installed process-wide and
                                    // multiple `Signal` instances
                                    // share it, the per-secondary
                                    // watchers in a
                                    // multiple-secondary
                                    // in-process deployment ALL
                                    // see the same SIGTERM and
                                    // ALL fire panik together —
                                    // which is exactly the
                                    // semantics we want: SIGTERM
                                    // is a process-level signal,
                                    // panik is cluster-level,
                                    // every coordinator in this
                                    // process should cascade.
                                    listen_for_sigterm: true,
                                },
                            );
                        if let Some(rx) = panik_watcher.take_signal_rx() {
                            secondary.register_panik_signal_rx(rx);
                        }

                        // Mesh-always: this in-process secondary is a
                        // promotion-eligible compute peer (`can_be_primary =
                        // true`). Compose the `Node` (one role composition per
                        // logical peer); `Node::new` hands out the
                        // `promotion_tx` the secondary signals on a self-named
                        // `PrimaryChanged`, wired via `register_promotion_signal`.
                        // The setup peer relocates onto the lowest-id eligible
                        // secondary, whose `PromotionSignal` fires this path and
                        // makes `Node::run` invoke the `promote` recipe below to
                        // BUILD the snapshot-seeded same-peer primary (the
                        // secondary NEVER constructs a primary — SUPREME-LAW #3).
                        // The pump is hosted on THIS LocalSet (`on_local_set`),
                        // not the dedicated mesh runtime: the in-process channel
                        // mesh is pure mpsc with no socket IO, so there is no
                        // wire QoS for a per-node OS thread to protect.
                        let (node, promotion_tx) =
                            Node::new(MeshHost::on_local_set(sec_mesh));
                        secondary.register_promotion_signal(promotion_tx);

                        // The shared run-config handle the promote recipe reads
                        // at the promotion instant. In-process every node shares
                        // the submitter's argv directly, so this is byte-
                        // identical to the setup peer's `forwarded_argv`.
                        let promote_run_config_handle = secondary.run_config_handle();

                        // Build this secondary's discovery policy INSIDE the
                        // detached runtime (the `SetupDiscovery` CLOSURE is not
                        // `Ungil`, so it cannot cross the `py.detach` boundary —
                        // only the raw `Py` handles did). `Some` on the
                        // pre-staged path; `None` on cold (the marker is
                        // `Settled`, so the driver never consults it).
                        // In-process discovery reads the submitter's
                        // EAGERLY-parsed namespace directly (every node shares
                        // it — no deferred reparse), so it is the
                        // PRE-RESOLVED `SharedRunConfig` (complete from the
                        // start). The `discover_items` driver resolves the same
                        // node-local `resolved_output_root` the SLURM path does.
                        let sec_setup_discovery = sec_discovery_handle.map(|(td, ta, root)| {
                            dynrunner_manager_distributed::SetupDiscovery {
                                discover: build_setup_discovery_fn(
                                    td,
                                    crate::managers::run_config::SharedRunConfig::pre_resolved(ta),
                                    Some(root),
                                ),
                                phase_deps: sec_phase_deps,
                            }
                        });

                        // Build the promoted-primary recipe by REUSING the
                        // SLURM submitter's transport-agnostic recipe builder
                        // (`build_promoted_primary_recipe`). The promoted
                        // same-peer primary OWNS the phase machine (fires the
                        // per-secondary `on_phase_*`) and, on the pre-staged
                        // path, runs `discover_on_promotion` with the
                        // per-secondary discovery policy on the shared host fs.
                        // `command_channel` — the shared take-once cell holding
                        // the ONE Python `PrimaryHandle` command channel. The
                        // relocate winner claims the pair at promotion so the
                        // consumer's `on_phase_end → spawn_tasks` lazy-injection
                        // reaches the live authority (pre-cell, the channel died
                        // with the demoted setup peer and every composite-run
                        // spawn raised "command channel closed").
                        let promote = build_promoted_primary_recipe(PromotedPrimaryRecipeInputs {
                            secondary_id: sec_id_for_slot.clone(),
                            custom_message_handler_def: sec_custom_message_handler_def,
                            keepalive_interval: dist_keepalive,
                            peer_timeout: dist_peer_timeout,
                            keepalive_miss_threshold: dist_keepalive_miss_threshold,
                            retry_max_passes: dist_retry_max_passes,
                            oom_retry_max_passes: dist_oom_retry_max_passes,
                            scheduler_config: promote_scheduler_config,
                            estimator: promote_estimator,
                            command_channel: promote_command_channel,
                            on_phase_start: sec_on_phase_start,
                            on_phase_end: sec_on_phase_end,
                            phase_hook_raise_latch: sec_phase_hook_raise_latch,
                            // In-process: the submitter's `on_run_start` already
                            // fired in THIS process with the one live handle, so
                            // the relocated primary must NOT re-fire it.
                            on_run_start: None,
                            forwarded_argv: promote_run_config_handle,
                            uses_file_based_items: promote_uses_file_based_items,
                            pre_staged_mode: promote_pre_staged_mode,
                            source_pre_staged_root: promote_pre_staged_root,
                            source_dir: promote_source_dir,
                            // Framework file-staging selector (#489 P3/P4): the
                            // relocate-target primary uses the SAME strategy the
                            // in-process setup peer was launched with (`Copy`).
                            staging_strategy,
                            setup_discovery: sec_setup_discovery,
                            // In-process `--multi-computer local`: the
                            // liveness-beacon UDP path is wired only on the
                            // separate-process SLURM secondary today (the
                            // confirmed CPU-starvation-false-death case). The
                            // in-process path keeps the frame-only death-clock
                            // (its pre-existing behaviour) — flagged as a
                            // follow-up (loopback beacon between in-process
                            // threads). No regression: `None` = no beacon rx.
                            liveness_ping_rx: None,
                            // No node beacon on the in-process path (same
                            // follow-up): `None` = no primary→secondaries beacon.
                            peer_liveness_addrs: None,
                            // No runtime-watchdog is spawned on the in-process
                            // `--multi-computer local` path (same follow-up as
                            // the beacon infra above), so there is no reader for
                            // the arm-stats bridge here. Hand the recipe a fresh
                            // standalone cell: the promoted primary's loop still
                            // publishes its arms into it (a cheap, harmless
                            // unread write) and the loop-local
                            // `op_loop_arm_stats` field remains directly
                            // inspectable. Observation-only.
                            op_loop_arm_stats_cell:
                                dynrunner_manager_distributed::oploop_instrumentation::OpLoopArmStatsCell::new(),
                        });

                        let node = node.with_secondary(secondary, sec_slot);
                        let inputs: NodeRunInputs<
                            SubprocessWorkerFactory,
                            dynrunner_scheduler::ResourceStealingScheduler,
                            crate::estimator::PyMemoryEstimatorBridge,
                            RunnerIdentifier,
                        > = NodeRunInputs {
                            secondary_factory: Some(factory),
                            promote: Some(promote),
                            primary_run_args: None,
                            primary_demote_tx: None,
                        };
                        let outcome = node.run(inputs).await;
                        if let RunTerminal::Failed { error } = &outcome.terminal {
                            tracing::error!(error = %error, "in-process secondary node failed");
                        }
                        outcome.completed
                    });

                    sec_handles.push(handle);
                }
                // No manual inbound-sink drop: with the all-to-all `peer_mesh`,
                // the setup peer's inbox is fed only by the secondaries'
                // outboxes (held in their `ChannelPeerTransport.outgoing`). Once
                // every secondary node exits and drops its transport, those
                // senders drop and the setup peer's `recv_peer()` observes
                // `None` — the inbound-closed signal the operational loop's
                // `transport_closed` gate keys off (the relocate target's
                // observer/primary owns the live mesh while the run proceeds).

                let config = PrimaryConfig {
                    node_id: dynrunner_core::SETUP_NODE_ID.into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    // `--source-already-staged` threaded onto the PrimaryConfig
                    // as the staging root. Under mesh-always the in-process
                    // setup peer seeds `DiscoveryDebt=Owed` (the
                    // `SeedSource::RelocatedSeed` below) and RELOCATES; the
                    // relocate target inherits the `Owed` marker via its
                    // snapshot and runs `discover_on_promotion` with its own
                    // discovery policy on the shared host fs. The setup peer
                    // itself never discovers.
                    source_pre_staged_root: source_pre_staged_root.clone(),
                    uses_file_based_items,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
                    oom_retry_max_passes: dist_oom_retry_max_passes,
                    fleet_dead_timeout: std::time::Duration::from_secs(30),
                    mesh_ready_timeout: std::time::Duration::from_secs(60),
                    // Threaded into PrimaryConfig so the manager's
                    // run() has the local source root needed for the
                    // initial staging walk's content-hash + per-
                    // secondary fan-out. The explicit
                    // `queue_initial_staging_from_binaries` call
                    // below pre-populates the queue today; threading
                    // the field uniformly keeps the manager's
                    // future-direction (auto-stage when no caller
                    // pre-queues) wired without each caller re-
                    // implementing the orchestration.
                    source_dir: Some(source_dir.clone()),
                    // Framework file-staging selector (#489 P3/P4): old
                    // StageFile path vs the setup-task model. Mapped from the
                    // `stage_via_setup_tasks` flag above.
                    staging_strategy,
                    // Snapshot taken on the GIL thread (see above) so
                    // the in-process distributed primary honours the
                    // same `unfulfillable_reinject_max_per_task` knob
                    // every other primary path does. The
                    // `PrimaryHandle::set_unfulfillable_reinject_max_per_task`
                    // setter writes through the shared cell pre-run;
                    // post-`mark_run_started` writes raise on the
                    // handle side, so the value frozen here is the
                    // single source of truth for the inner loop.
                    unfulfillable_reinject_max_per_task,
                    // The shared node-local run-config (the operator's
                    // `args.forwarded_argv`), seeded identically on the
                    // in-process primary and every in-process secondary so
                    // the `RequestRunConfig` responder answers uniformly.
                    forwarded_argv,
                    // Staged silence schedule: keepalive-interval-relative
                    // defaults (not surfaced on the Python config today).
                    ..PrimaryConfig::default()
                };

                // Wrap the in-process setup peer's mesh transport in the
                // role-demux `Mesh` and register the Primary slot. Under
                // mesh-always the setup peer RELOCATES the primary onto a
                // compute secondary, so its demote channel is LIVE (Gap C): the
                // relocate's local apply names the target ≠ self, the role-
                // change hook (`register_demote_on_displaced`, wired by
                // `Node::run` from `primary_demote_tx` below) fires the demote
                // signal, and `run_consuming` carries this coordinator out as a
                // standalone observer (`PrimaryRunOutcome::Relocated`). Mirrors
                // the SLURM submitter (`managers/primary/run.rs`).
                let mut pri_mesh = Mesh::new(peer_transport);
                let (pri_slot, pri_client, pri_inbox) = pri_mesh.register_local_role(
                    LocalRole::Primary,
                    PeerId::from(dynrunner_core::SETUP_NODE_ID),
                );
                let (pri_demote_tx, pri_demote_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

                let mut primary = PrimaryCoordinator::new(
                    config,
                    pri_client,
                    pri_inbox,
                    pri_demote_rx,
                    // Mesh-always: the in-process setup peer's seed is
                    // `ColdStart` / `RelocatedSeed` (below) ⇒
                    // `BootstrapRole::SetupPeer`, so `run_pipeline` relocates
                    // the primary onto a compute secondary — uniform with the
                    // SLURM submitter, no construction-time policy.
                    scheduler_config.build_memory_scheduler(),
                    estimator,
                );

                // The setup peer deliberately does NOT take the Python-facing
                // command channel: under mesh-always it relocates the primary
                // role before any phase runs, and a channel consumed here would
                // die with the demoted-to-observer coordinator (the
                // composite-run "command channel closed" regression). The pair
                // lives in `python_command_channel` (the shared take-once cell
                // every promotable secondary's recipe holds) and is claimed by
                // the relocate winner; the setup peer keeps the internal
                // channel `PrimaryCoordinator::new` minted for its own
                // pre-relocate window.

                // Wire the on_phase_end raise-latch (built above, captured
                // by the in-process primary's closure) onto the
                // coordinator so a consumer-hook raise surfaces
                // `FatalPolicyExit`. Same pre-`run()` setter contract.
                primary.set_phase_hook_raise_latch(phase_hook_raise_latch);

                // Install the consumer custom-message hook (F5) on the
                // setup peer too — a custom message can land during the
                // pre-relocate bootstrap window. Built from THIS
                // coordinator's command sender (its INTERNAL channel — the
                // Python channel belongs to the eventual promoted primary),
                // which the setup peer drains during its bootstrap waits.
                if let Some(def) = setup_custom_message_handler_def {
                    match crate::custom_message_bridge::make_custom_message_handler(
                        def,
                        primary.command_sender(),
                    ) {
                        Ok(handler) => primary.set_custom_message_handler(handler),
                        Err(e) => tracing::warn!(
                            error = %e,
                            "custom_message_handler bridge build failed; the \
                             setup peer will consume custom messages unhandled"
                        ),
                    }
                }

                // Register the consumer's `may_be_empty` phase opt-out BEFORE
                // `run()` enters, so the cold-/relocated-seed originator emits
                // it paired with the phase graph (the empty-drain opt-out).
                primary.register_phase_may_be_empty(phase_may_be_empty.iter().cloned());

                // Register the Python peer-lifecycle listener (if any)
                // BEFORE the primary's `run()` enters — the
                // coordinator's `register_lifecycle_listener` contract
                // requires pre-run registration because the listener
                // vector is `mem::take`-d into the spawned dispatcher.
                if let Some(listener) = peer_lifecycle_listener {
                    primary.register_lifecycle_listener(listener);
                }

                // Same shape for the task-completion listener:
                // independent dispatcher pair with the same pre-run
                // registration contract.
                if let Some(listener) = task_completed_listener {
                    primary.register_task_completed_listener(listener);
                }

                // The setup peer registers NO discovery policy: under
                // mesh-always it RELOCATES before `discover_on_promotion` (the
                // `BootstrapRole::SetupPeer` arm fires the relocate before
                // discover). On the pre-staged path the discovery policy lives
                // on the RELOCATE TARGET's promote recipe
                // (`sec_setup_discoveries` → `build_promoted_primary_recipe`
                // above), which inherits the `Owed` marker via its snapshot and
                // walks the shared host fs itself. On the cold path the corpus
                // was cold-seeded upfront and rides the snapshot; the marker is
                // `Settled`, so no discovery runs anywhere.

                // Panik watcher for the in-process primary. Each
                // in-process secondary spawn_local closure above also
                // wires its own watcher — every coordinator on this
                // process polls independently and fires its own
                // teardown when its file appears. Handle held in
                // scope for `Drop::abort()` at loop exit.
                let mut panik_watcher =
                    dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                        dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                            paths: panik_watcher_paths,
                            poll_interval: panik_watcher_poll_interval,
                            // PRIMARY-role spawner: SIGTERM listening
                            // OFF. The host-driven SIGTERM cascade is
                            // a secondary-side concern (SLURM
                            // time-limit applies to allocations
                            // running secondary jobs; the primary
                            // typically runs on the operator host,
                            // not in a SLURM-allocated container).
                            // Primary shutdown is driven by the
                            // sentinel-file path, by orchestrator
                            // teardown, or by panik broadcast from a
                            // secondary that hit SIGTERM.
                            listen_for_sigterm: false,
                        },
                    );
                if let Some(rx) = panik_watcher.take_signal_rx() {
                    primary.register_panik_signal_rx(rx);
                }

                // Hand the entry-armed SIGUSR2 trigger to the setup-peer
                // primary (taken exactly once). Its operational loop's
                // graceful-abort arm consumes it during the pre-relocate
                // window; on relocation `into_observer_handoff` carries it
                // onto the standalone observer.
                if let Some(trigger) = abort_trigger.take() {
                    primary.register_graceful_abort_trigger(trigger);
                }

                // Initial staging is now driven by
                // `PrimaryCoordinator::run` itself: with
                // `PrimaryConfig.source_dir = Some(source_dir)`
                // threaded above, the manager's auto-stage gate
                // (`pending_stage_files.is_empty()` &&
                // `uses_file_based_items` && pre-staged-mode off
                // && source_dir is Some) walks `binaries ×
                // secondary_ids` once secondaries have welcomed
                // and queues the entries before initial
                // assignment. Removes the previous explicit pre-
                // call here in favour of a single source of truth
                // at the manager boundary; consistent with the
                // network-primary path, which also relies on the
                // auto-stage. The SLURM pipeline retains its
                // explicit `queue_initial_staging` because that
                // caller's source-root resolution depends on
                // `--source-already-staged` and other flags
                // unique to it; the gate detects the non-empty
                // queue and skips.

                // Compose the in-process setup peer's `Node` (a pure-primary
                // node — no co-located secondary; the relocate TARGET is a
                // separate secondary node). `Node::run` drives the hosted mesh-pump +
                // runs the primary CONSUMING, so the relocate's demote arm
                // carries this coordinator out as `Relocated { handoff }` (it
                // becomes a standalone observer for the rest of the run).
                // Same `on_local_set` hosting decision as the per-secondary
                // nodes above: a pure-mpsc in-process mesh needs no dedicated
                // mesh runtime thread.
                let (node, _node_promo_tx) = Node::new(MeshHost::on_local_set(pri_mesh));
                let node = node.with_primary(primary, pri_slot);
                // Construct the typed seed at the boundary from the pre-staged
                // signal (a construction-site decision, NOT a runtime flag-if
                // inside the coordinator). Both seeds derive
                // `BootstrapRole::SetupPeer`, so the setup peer relocates either
                // way. Pre-staged: originate ONLY the phase graph +
                // `DiscoveryDebt=Owed`; the relocate target discovers the staged
                // corpus. Cold: the corpus was discovered upfront in Python and
                // is cold-seeded (it rides the target's promotion snapshot).
                let seed = if source_pre_staged {
                    SeedSource::RelocatedSeed { phase_deps }
                } else {
                    SeedSource::ColdStart {
                        binaries: rust_binaries,
                        phase_deps,
                    }
                };
                let inputs: NodeRunInputs<
                    SubprocessWorkerFactory,
                    dynrunner_scheduler::ResourceStealingScheduler,
                    crate::estimator::PyMemoryEstimatorBridge,
                    RunnerIdentifier,
                > = NodeRunInputs {
                    primary_run_args: Some(PrimaryRunArgs {
                        seed,
                        on_phase_start,
                        on_phase_end,
                    }),
                    // Gap C: LIVE demote hook. `Node::run` installs it on the
                    // setup peer's role-change hook
                    // (`register_demote_on_displaced`), so the relocate's local
                    // `PrimaryChanged { Transferred }` apply fires the demote
                    // signal and `run_consuming` relocates this coordinator into
                    // a standalone observer. No co-located secondary, no promote
                    // recipe (this node is the setup peer, never a target).
                    primary_demote_tx: Some(pri_demote_tx),
                    secondary_factory: None,
                    promote: None,
                };
                let outcome = node.run(inputs).await;
                completed = outcome.completed as u32;
                failed = outcome.failed as u32;
                stranded = outcome.stranded as u32;

                // Map the role-agnostic terminal to the GIL-side exit markers
                // (uniform with `PyPrimaryCoordinator::run`).
                match outcome.terminal {
                    RunTerminal::Done => {}
                    RunTerminal::GracefulAbort { reason } => {
                        // Operator-requested graceful abort ran its drain
                        // protocol to the end. A DELIBERATE clean wind-down:
                        // reported loudly (distinct from a silent success),
                        // exits 0 (distinct from the hard-abort raise).
                        tracing::warn!(verdict = %reason, "run gracefully aborted");
                    }
                    RunTerminal::Aborted { reason } => {
                        // A cluster-wide `RunAborted` observed by the setup
                        // node. Carry the broadcast reason VERBATIM — the
                        // verdict already names its own cause (#3a duplicate,
                        // consumer-hook fatal-exit, empty-drain abort, ...);
                        // re-typing it as `DuplicateTaskIdPrePhase` fabricated
                        // a duplicate-task-id diagnosis for every abort.
                        relocated_aborted = Some(reason);
                    }
                    RunTerminal::Panik { matched_path } => {
                        panik_shutdown_path = Some(matched_path);
                    }
                    RunTerminal::Failed { error } => {
                        tracing::error!(error = %error, "in-process primary node failed");
                        match error {
                            RunError::ClusterCollapsed { .. } => {
                                cluster_collapsed = Some(error);
                            }
                            RunError::PanikShutdown { matched_path, .. } => {
                                panik_shutdown_path = Some(matched_path);
                            }
                            e @ RunError::DuplicateTaskIdPrePhase { .. } => {
                                duplicate_task_id_pre_phase = Some(e);
                            }
                            e @ RunError::InvalidComposedGraph { .. } => {
                                // Bring-up fatal: the composed task graph was
                                // invalid (a discovery-batch duplicate identity /
                                // missing dep / cycle). The primary already
                                // broadcast the `RunAborted` verdict; RAISE here
                                // so this node's own exit is non-zero — never the
                                // `Other` swallow's false rc=0 (the run_~1429
                                // false-not-shutdown this fix prevents).
                                fatal_policy_exit = Some(e);
                            }
                            e @ RunError::FatalPolicyExit { .. } => {
                                fatal_policy_exit = Some(e);
                            }
                            e @ RunError::SpawnRejected { .. } => {
                                spawn_rejected = Some(e);
                            }
                            e @ RunError::NoRelocationTarget => {
                                // LIVE under mesh-always: the in-process setup
                                // peer relocates, so a fleet with no eligible
                                // compute secondary surfaces this. RAISE rather
                                // than silently swallow.
                                no_relocation_target = Some(e);
                            }
                            e @ RunError::BringUpFailed { .. } => {
                                // Fleet bring-up failure (0/N welcomes inside
                                // the quorum-proceed window): no fleet, zero
                                // dispatch. RAISE — never the `Other` swallow's
                                // false rc=0 clean teardown (uniform with
                                // `PyPrimaryCoordinator::run`).
                                fatal_policy_exit = Some(e);
                            }
                            e @ (RunError::AbortedByClusterVerdict { .. }
                            | RunError::Deposed { .. }) => {
                                // Run-authority terminals (zombie split-brain
                                // fix): an adopted cluster RunAborted verdict,
                                // or a deposed primary that authored no
                                // verdict. RAISE — never the `Other` swallow's
                                // false rc=0 (uniform with
                                // `PyPrimaryCoordinator::run`).
                                fatal_policy_exit = Some(e);
                            }
                            RunError::GracefulAbort { .. } => {
                                // Unreachable in practice: `Node::run` maps a
                                // primary's GracefulAbort onto its OWN
                                // `RunTerminal::GracefulAbort` (handled above),
                                // never `Failed`. Defensive: treat as the
                                // graceful verdict, never a raise.
                            }
                            RunError::Other(_) => {
                                // The PRESERVED stay-local-primary swallow
                                // (exit 0) — see `PyPrimaryCoordinator::run`.
                            }
                        }
                    }
                }

                // Wait for the per-secondary nodes to finish. Each `Node::run`
                // already ran its own factory's worker-teardown ladder (gated
                // off panik), so there is no aggregated child-process cleanup
                // to do here — the OLD outer `terminate_children` aggregation
                // is now per-node inside `Node::run`.
                for handle in sec_handles {
                    let _ = handle.await;
                }
            }));
        });

        self.completed = completed;
        self.failed = failed;
        self.stranded = stranded;

        if let Some(matched_path) = panik_shutdown_path {
            // GIL is back. Exit(137) — same shape as
            // `PyPrimaryCoordinator::run`. Skips the
            // cluster-collapsed path because a panik shutdown is a
            // strictly-stronger terminal (the operator declared the
            // whole cluster unwanted; partial accounting is
            // irrelevant). The secondaries spawned above have each
            // already run their own panik-react path (kill_all_workers_with_grace)
            // before joining; their workers' pgids are reaped before
            // we exit.
            tracing::error!(
                matched_path = %matched_path.display(),
                "panik shutdown: distributed manager exiting with code 137"
            );
            std::process::exit(137);
        }

        if let Some(err) = duplicate_task_id_pre_phase {
            // Surface the pre-phase duplicate-task-id abort (#3a) — same
            // shape as `PyPrimaryCoordinator::run`. The in-process
            // primary already broadcast `RunAborted`; raise here so the
            // Python wrapper sees a non-zero exit instead of exit 0.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(reason) = relocated_aborted {
            // The setup node observed a cluster-wide `RunAborted`. Raise the
            // broadcast reason verbatim (the in-process manager is a library
            // call, so a PyErr — not the network-submitter's exit(1)); the
            // reason already names the actual cause.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "run aborted cluster-wide: {reason}"
            )));
        }

        if let Some(err) = fatal_policy_exit {
            // A deliberate policy abort (panicked role task / invalid-task
            // fatal-exit) — RAISE, never the `Other` swallow.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = spawn_rejected {
            // A runtime spawn_tasks batch was wholesale-rejected → the phase
            // dispatched ZERO tasks. RAISE so the wrapper sees a non-zero
            // exit instead of the silent rc=0 that masked the dropped work.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

        if let Some(err) = no_relocation_target {
            // LIVE under mesh-always: the in-process setup peer relocates, so a
            // fleet with no eligible compute secondary surfaces this. RAISE so
            // the operator sees the unsupported-topology message.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(err.to_string()));
        }

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
    /// (`total - completed - failed`). Mirrors `RustPrimaryCoordinator.stranded`.
    #[getter]
    fn stranded(&self) -> u32 {
        self.stranded
    }
}
