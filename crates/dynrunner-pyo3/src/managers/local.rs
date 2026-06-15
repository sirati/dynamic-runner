use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{
    COMMAND_CHANNEL_CAPACITY, PhaseId, PrimaryCommand, ResourceKind, ResourceMap, TaskInfo,
    resolve_against_root,
};
use dynrunner_manager_local::{LocalManager, LocalManagerConfig, ProcessingStats};
use tokio::sync::mpsc as tokio_mpsc;

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
use crate::managers::primary_handle::{PyPrimaryHandle, ReinjectCapCell};
use crate::network::gethostname;
use crate::pytypes::{PyFailedTask, PyProcessingStats, PyTaskInfo, extract_binaries};
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{shared_registry, LoadedTaskDefinition, TypeRegistry};
use crate::transport::EitherManagerEnd;

/// The main Python-facing local manager class.
#[pyclass(name = "RustLocalManager")]
pub(crate) struct PyLocalManager {
    python_executable: PathBuf,
    num_workers: u32,
    max_resources: ResourceMap,
    low_resource_thresholds: ResourceMap,
    reuse_workers: bool,
    restart_predicate: Option<Py<PyAny>>,
    retry_max_attempts: u32,
    print_pid: bool,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    scheduler_config: SchedulerConfig,
    /// Panik-watcher paths — same shape as on the distributed
    /// pyclasses. The 2026-05-17 design has "every node polls
    /// independently", and single-host local mode IS a node — so a
    /// per-host panik file trips the watcher and we exit(137) the
    /// same way.
    panik_watcher_paths: Vec<PathBuf>,
    panik_watcher_poll_interval_secs: f64,
    phase_status_log_intervals_secs: Vec<f64>,
    /// Per-phase keepalive watchdog. The map key is the phase name as
    /// reported by `Task.set_phase(...)`; the value is the maximum
    /// silence (no keepalive / phase update) tolerated before the
    /// manager kills and restarts the worker. Empty disables the
    /// watchdog entirely (current default). Forwarded verbatim into
    /// `LocalManagerConfig.stage_timeouts`.
    stage_timeouts_secs: HashMap<String, f64>,
    /// Surfaces `LocalManagerConfig.log_oom_watcher` through the
    /// legacy pyclass so callers that bypass the typed
    /// `LocalManagerConfig` path still pick up the flag.
    log_oom_watcher: bool,
    /// Python-side `--memprofile` opt-in. Pairs with `output_dir`
    /// (already captured above) to drive
    /// `LocalManagerConfig.output_dir` via the shared
    /// `resolve_memprofile_dir` helper. Default `false`; flipped to
    /// `true` by the dispatcher when the operator passes
    /// `--memprofile`. The flag's behaviour lives in Rust — Python
    /// only flips the bool.
    memprofile_enabled: bool,
    types: TypeRegistry,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Phases the consumer declared `PhaseSpec.barrier=False` — applied
    /// to the inner `LocalManager` via `set_no_barrier_phases` BEFORE
    /// `process_binaries` (the local twin of the distributed primary's
    /// `register_phase_no_barrier`). Empty on the common strict-barrier
    /// run.
    phase_no_barrier: Vec<PhaseId>,
    skip_existing: bool,
    estimator: PyMemoryEstimatorBridge,
    connection_mode: ConnectionMode,
    manual_start_worker: bool,
    stats: Option<ProcessingStats>,
    failed_tasks: Vec<dynrunner_core::FailedTask<RunnerIdentifier>>,
    oom_tasks: Vec<dynrunner_core::FailedTask<RunnerIdentifier>>,
    task_payloads: Vec<(TaskInfo<RunnerIdentifier>, Option<Vec<u8>>)>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// and call back into Python from the manager's LocalSet (Phase 5B).
    /// `Py<PyAny>` is `Send + Sync + 'static` so it satisfies the
    /// `FnMut + Send + 'static` bounds on `process_binaries`'s closure
    /// arguments.
    task_definition: Py<PyAny>,

    /// Command-channel sender, minted at `__new__` so `.handle()` is
    /// callable BEFORE `process_binaries`. Clone-source for every
    /// `PyPrimaryHandle` the Python side fetches; the same clone is
    /// also threaded into the inner `LocalManager` at
    /// `process_binaries` time via [`LocalManager::with_command_channel`]
    /// so the receiver-side of the same channel pair lives on the
    /// manager.
    command_tx: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,

    /// Command-channel receiver. `Mutex<Option<...>>` so
    /// `process_binaries` (which holds `&mut self` through the
    /// pyclass method dispatch) can `.lock().take()` the receiver
    /// once and hand it to `LocalManager::with_command_channel`.
    /// `Mutex` (not `RwLock`) because the only access pattern is a
    /// single `take()`; contention is moot.
    command_rx: Mutex<Option<tokio_mpsc::Receiver<PrimaryCommand<RunnerIdentifier>>>>,

    /// Shared per-handle cell for the reinject cap setter. Symmetric
    /// with the in-process distributed manager and the network
    /// primary's coordinator: every `PyPrimaryHandle` minted by
    /// `.handle()` carries a clone of this cell, and every setter
    /// call from Python ripples back to the value
    /// `process_binaries` reads when it builds the
    /// `LocalManagerConfig`. Defaults to `None` (unbounded
    /// reinjections) — flipped only by the operator's CLI knob or
    /// by `PyPrimaryHandle::set_unfulfillable_reinject_max_per_task`.
    reinject_cap: ReinjectCapCell,
}

#[pymethods]
impl PyLocalManager {
    #[new]
    #[pyo3(signature = (
        num_workers,
        max_memory,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        reuse_workers = false,
        restart_predicate = None,
        retry_max_attempts = 1,
        print_pid = false,
        connection_mode = "socketpair",
        socket_dir = None,
        manual_start_worker = false,
        log_paths = None,
        worker_spec = None,
        low_memory_threshold = None,
        scheduler_config = None,
        phase_status_log_intervals_secs = None,
        stage_timeouts_secs = None,
        max_resources = None,
        low_resource_thresholds = None,
        log_oom_watcher = false,
        log_dir = None,
        panik_watcher_paths = None,
        panik_watcher_poll_interval_secs = 10.0,
        memprofile_enabled = false,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        num_workers: u32,
        max_memory: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        reuse_workers: bool,
        restart_predicate: Option<Py<PyAny>>,
        retry_max_attempts: u32,
        print_pid: bool,
        connection_mode: &str,
        socket_dir: Option<String>,
        manual_start_worker: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        low_memory_threshold: Option<u64>,
        scheduler_config: Option<SchedulerConfig>,
        phase_status_log_intervals_secs: Option<Vec<f64>>,
        stage_timeouts_secs: Option<HashMap<String, f64>>,
        max_resources: Option<PyResourceMap>,
        low_resource_thresholds: Option<PyResourceMap>,
        log_oom_watcher: bool,
        log_dir: Option<String>,
        panik_watcher_paths: Option<Vec<PathBuf>>,
        panik_watcher_poll_interval_secs: f64,
        memprofile_enabled: bool,
    ) -> PyResult<Self> {
        let task = LoadedTaskDefinition::from_python(
            py,
            task_definition,
            task_args,
            &source_dir,
            &output_dir,
            log_dir.as_deref(),
            skip_existing,
            log_paths,
        )?;

        // Single-process mode synthesises a `secondary_id` from the
        // hostname (falling back to the literal `"local"`) and feeds
        // it through the same log-dir template the distributed and
        // SLURM modes use. The resulting directory is unique by
        // construction even on a shared mount with other runners,
        // so there is no special-case "single process" branch in the
        // log-path policy.
        let secondary_id = {
            let h = gethostname();
            if h.is_empty() || h == "unknown" {
                "local".to_string()
            } else {
                h
            }
        };
        let log_dir = task
            .log_paths
            .resolve_log_dir(py, &task.log_path, &secondary_id)?;
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "failed to create log directory {log_dir:?}: {e}"
            ))
        })?;

        // Parse connection mode
        let conn_mode = match connection_mode {
            "socketpair" => ConnectionMode::Socketpair,
            "named" => {
                let dir = socket_dir.map(PathBuf::from).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "socket_dir is required when connection_mode is 'named'",
                    )
                })?;
                std::fs::create_dir_all(&dir).ok();
                ConnectionMode::Named { socket_dir: dir }
            }
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown connection_mode: {other:?}, expected 'socketpair' or 'named'"
                )));
            }
        };

        // Normalize the resource budget at the boundary: callers may pass a
        // typed `max_resources` ResourceMap (preferred) or just the legacy
        // scalar `max_memory: u64` (single-key memory). When both are
        // given, the typed map wins. Same shape for `low_resource_thresholds`.
        let max_resources = max_resources
            .map(|m| m.to_rust())
            .unwrap_or_else(|| ResourceMap::from([(ResourceKind::memory(), max_memory)]));
        let low_resource_thresholds =
            low_resource_thresholds
                .map(|m| m.to_rust())
                .unwrap_or_else(|| {
                    ResourceMap::from([(
                        ResourceKind::memory(),
                        low_memory_threshold.unwrap_or(300 * 1024 * 1024),
                    )])
                });

        // Pre-mint the command-channel pair so `.handle()` is
        // callable BEFORE `process_binaries`. Both halves are
        // synchronous to construct (tokio mpsc does not require a
        // running runtime); the receiver moves into the inner
        // `LocalManager` at `process_binaries` time via
        // `command_rx.lock().take()`.
        let (command_tx, command_rx) =
            tokio_mpsc::channel::<PrimaryCommand<RunnerIdentifier>>(COMMAND_CHANNEL_CAPACITY);

        Ok(Self {
            python_executable: task.python_executable,
            num_workers,
            max_resources,
            low_resource_thresholds,
            reuse_workers,
            restart_predicate,
            retry_max_attempts,
            print_pid,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir,
            log_paths: task.log_paths,
            worker_spec,
            scheduler_config: scheduler_config.unwrap_or_default(),
            panik_watcher_paths: panik_watcher_paths.unwrap_or_default(),
            panik_watcher_poll_interval_secs,
            phase_status_log_intervals_secs: phase_status_log_intervals_secs
                .unwrap_or_else(|| vec![60.0, 300.0, 600.0, 1800.0, 3600.0]),
            stage_timeouts_secs: stage_timeouts_secs.unwrap_or_default(),
            log_oom_watcher,
            memprofile_enabled,
            types: task.types,
            phase_deps: task.phase_deps,
            phase_no_barrier: task.phase_no_barrier,
            skip_existing,
            estimator: task.estimator,
            connection_mode: conn_mode,
            manual_start_worker,
            stats: None,
            failed_tasks: Vec::new(),
            oom_tasks: Vec::new(),
            task_payloads: Vec::new(),
            task_definition: task_definition.clone().unbind(),
            command_tx,
            command_rx: Mutex::new(Some(command_rx)),
            reinject_cap: ReinjectCapCell::default(),
        })
    }

    /// Mint a `PrimaryHandle` for this manager. Symmetric with
    /// `PyDistributedManager::handle` / `PyPrimaryCoordinator::handle`
    /// on the network-primary path: every call returns a freshly-built
    /// handle whose `Sender<PrimaryCommand>` clone-shares the
    /// manager's command channel. Callable BEFORE `process_binaries`
    /// — the Python caller passes the handle to its `on_run_start`
    /// hook so the task can drive `spawn_tasks(...)` from inside its
    /// lifecycle.
    ///
    /// Local-mode parity with distributed: `PyPrimaryHandle::
    /// from_sender(self.command_tx.clone(), self.reinject_cap.clone())`
    /// — same constructor, same per-task budget cell semantics. The
    /// only Python-visible difference is that local-mode
    /// `update_preferred_secondaries` is a pool-mirror-only operation
    /// (no peer broadcast); see
    /// `dynrunner_manager_local::manager::command_channel` for the
    /// per-variant handler.
    fn handle(&self) -> PyResult<PyPrimaryHandle> {
        PyPrimaryHandle::from_sender(self.command_tx.clone(), self.reinject_cap.clone())
    }

    /// Process a list of PyTaskInfo objects.
    fn process_binaries(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        // Honour the discovery `skipped_already_done` marker (the uniform
        // discovery-boundary contract — every dispatch path now respects
        // it: the distributed path materialises terminal `SkippedAlreadyDone`
        // ledger entries, the single-process path here filters + counts).
        // A marked item's outputs already exist, so dispatching it would
        // re-run completed work — a correctness regression versus the old
        // consumer-side drop. The marker is a discovery-time routing signal
        // the dispatch entry consumes, NOT a property of the scheduling unit,
        // so the bit never rides into the manager.
        let (mut rust_binaries, already_done_skipped) =
            partition_already_done(extract_binaries(binaries)?);

        // Normalise each `binary.path` to the worker-facing wire id
        // (relative-to-`source_dir`). Out-of-tree paths are left
        // verbatim — the worker opens them via
        // `Path(source_dir).join(<abs>)`, which discards the source
        // prefix and reaches the absolute target.
        for binary in &mut rust_binaries {
            let resolved = resolve_against_root(&binary.path, &self.source_dir);
            if let Some(rel) = resolved.relative {
                binary.path = rel;
            }
        }

        let estimator = self.estimator.clone();
        let scheduler = self.scheduler_config.build_memory_scheduler();

        // Shared derivation: every dispatch path with an
        // `output_dir` defaults memuse logging on under
        // `{output_dir}/memuse.log`. Pre-shared-helper this site
        // hard-coded the join inline and the secondary paths
        // (SLURM / in-process distributed / multi-computer-local
        // subprocess) silently shipped without the log; the
        // `derive_memuse_log_path` helper pins the filename so
        // every caller picks the same shape.
        let memuse_log_path = dynrunner_manager_local::memuse::derive_memuse_log_path(
            Some(self.output_dir.as_path()),
            None,
        );

        let restart_predicate = self.restart_predicate.as_ref().map(|cb| {
            let cb = cb.clone_ref(py);
            let predicate: dynrunner_manager_local::RestartPredicate = Box::new(
                move |ctx: &dynrunner_manager_local::RestartContext<'_>| -> bool {
                    crate::managers::factory_callback::invoke_restart_predicate(&cb, ctx)
                },
            );
            predicate
        });

        // Snapshot the reinject cap and mark the cell as run-started
        // so any subsequent `PyPrimaryHandle::
        // set_unfulfillable_reinject_max_per_task` call raises
        // `PyRuntimeError` (the inner manager owns its own copy of
        // the cap from this moment on, matching the distributed
        // primary's contract).
        let unfulfillable_reinject_max_per_task = self.reinject_cap.snapshot();
        self.reinject_cap.mark_run_started();

        let config = LocalManagerConfig {
            num_workers: self.num_workers,
            max_resources: self.max_resources.clone(),
            reuse_workers: self.reuse_workers,
            restart_predicate,
            retry_max_attempts: self.retry_max_attempts,
            print_pid: self.print_pid,
            memuse_log_path,
            // Phase keys here are the raw strings the worker reports via
            // `Task.set_phase(...)` — the watchdog matches on those, not
            // on `PhaseId` from `get_phases()`. Per-type
            // `TaskTypeSpec.timeout_seconds` is a separate forward-looking
            // field that requires worker→type tracking to enforce; until
            // that follow-up lands, callers wanting timeout enforcement
            // pass `stage_timeouts_secs` on `LocalManagerConfig`.
            stage_timeouts: self
                .stage_timeouts_secs
                .iter()
                .map(|(k, v)| (k.clone(), Duration::from_secs_f64(*v)))
                .collect(),
            low_resource_thresholds: self.low_resource_thresholds.clone(),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: self
                .phase_status_log_intervals_secs
                .iter()
                .map(|s| std::time::Duration::from_secs_f64(*s))
                .collect(),
            log_oom_watcher: self.log_oom_watcher,
            // Composes the memprofile sampler's output directory
            // from the two Python-side inputs (the run-level
            // `output_dir` and the boolean `--memprofile` opt-in).
            // Shared helper at the PyO3 boundary so both this
            // legacy class and the typed `PyLocalManagerConfig`
            // path produce identical results.
            output_dir: crate::config::local_manager::resolve_memprofile_dir(
                self.memprofile_enabled,
                Some(self.output_dir.as_path()),
            ),
            unfulfillable_reinject_max_per_task,
        };

        // Take the receiver out of the manager-side mutex so it can
        // be handed to `LocalManager::with_command_channel`. The
        // sender (`self.command_tx`) stays alive — every existing
        // `PyPrimaryHandle` clone still routes to the same receiver
        // once it moves into the inner manager.
        let command_tx = self.command_tx.clone();
        let command_rx = self
            .command_rx
            .lock()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "RustLocalManager: command_rx mutex poisoned: {e}"
                ))
            })?
            .take()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "RustLocalManager.process_binaries called twice: \
                     command_rx already consumed",
                )
            })?;

        // Per-type subprocess dispatch: the factory carries the full
        // `TypeRegistry`. `spawn_worker` defaults to `types.first()`
        // for initial pool init (preserves pre-fix single-type
        // behaviour); `spawn_worker_for_type` consults the registry
        // for per-task respawn on TypeId mismatch. Multi-phase
        // `TaskDefinition`s with one `TaskTypeSpec` per phase route
        // through the latter automatically via
        // `WorkerPool::ensure_worker_for_type`.
        if self.types.first().is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            ));
        }
        let mut factory = SubprocessWorkerFactory {
            python_executable: self.python_executable.clone(),
            source_dir: self.source_dir.clone(),
            output_dir: self.output_dir.clone(),
            log_dir: self.log_dir.clone(),
            log_paths: self.log_paths.clone(),
            // Local manager parses args eagerly at construction — the registry
            // is final, so seed the shared cell once and never swap.
            types: shared_registry(self.types.clone()),
            skip_existing: self.skip_existing,
            connection_mode: self.connection_mode.clone(),
            manual_start_worker: self.manual_start_worker,
            worker_spec: self.worker_spec.clone(),
            child_processes: Vec::new(),
        };

        let phase_deps = self.phase_deps.clone();
        let phase_no_barrier = self.phase_no_barrier.clone();
        // Panik-watcher config captured before `py.detach`. The
        // LocalManager has no inner `panik_signal_rx` field — there's
        // only one operational loop (`process_binaries`) and the
        // PyO3 wrapper races the panik signal against it directly.
        // Empty paths yields a no-op watcher whose receiver resolves
        // to `Err` (sender dropped); the race arm filters that with
        // `if let Ok(signal) = …` and the select! falls through to
        // the manager-future arm.
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval =
            std::time::Duration::from_secs_f64(self.panik_watcher_poll_interval_secs);
        // Panik-grace window for the worker tree-kill. 5s matches
        // the SubprocessWorkerFactory's `TERMINATE_GRACE` and the
        // secondary's `PANIK_KILL_GRACE`.
        const PANIK_KILL_GRACE: std::time::Duration = std::time::Duration::from_secs(5);
        // GIL-reacquiring closures that dispatch to the Python
        // TaskDefinition's on_phase_start / on_phase_end. Each closure
        // owns its own ref-bumped Py<PyAny> so the manager's lifetime
        // is independent of `self`.
        let on_phase_start =
            crate::managers::lifecycle::make_on_phase_start(self.task_definition.clone_ref(py));
        let on_phase_end =
            crate::managers::lifecycle::make_on_phase_end(self.task_definition.clone_ref(py));

        // Terminal-outcome enum for the local manager's run. Mirrors
        // the structured-outcome pattern the secondary / observer
        // pyclasses use: regular `Done(())` is the happy path,
        // `Panik(PathBuf)` signals the outer scope (GIL re-acquired)
        // to call `exit(137)` after the factory's process-tree
        // teardown has run.
        enum LocalRunOutcome {
            Done(Result<(), String>),
            Panik(std::path::PathBuf),
        }

        // Run the async manager on a current-thread tokio runtime,
        // releasing the GIL during processing.
        let run_outcome: LocalRunOutcome = py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Spawn the panik watcher BEFORE constructing the
                // manager so its receiver is available for the
                // race below. Empty `panik_watcher_paths` yields a
                // never-firing receiver (the spawn helper returns a
                // no-op task), which races as `Err` immediately
                // and falls through to the manager future.
                let mut panik_watcher =
                    dynrunner_manager_distributed::panik_watcher::spawn_panik_watcher(
                        dynrunner_manager_distributed::panik_watcher::PanikWatcherConfig {
                            paths: panik_watcher_paths,
                            poll_interval: panik_watcher_poll_interval,
                            // LOCAL-role spawner (single-process,
                            // no SLURM container, no host
                            // shutdown-manager): SIGTERM listening
                            // OFF per the original feature scope.
                            // The local manager runs on operator
                            // hardware where SIGTERM = "user
                            // pressed Ctrl-C / kill", and we want
                            // the default Unix disposition
                            // (terminate process) to remain
                            // visible rather than be absorbed by
                            // the watcher.
                            listen_for_sigterm: false,
                        },
                    );
                let panik_rx = panik_watcher.take_signal_rx();

                let mut manager: LocalManager<EitherManagerEnd, _, _, _> =
                    LocalManager::with_command_channel(
                        config, scheduler, estimator, command_tx, command_rx,
                    );
                // Register the consumer's `PhaseSpec.barrier=False`
                // opt-in BEFORE `process_binaries` so the pool's
                // initial-state assignment flips no-barrier phases
                // `Blocked → Active`. Same pre-run-setter contract as
                // the distributed-primary `register_phase_no_barrier`
                // call.
                manager.set_no_barrier_phases(phase_no_barrier.iter().cloned());
                // phase_deps comes from LoadedTaskDefinition (5A);
                // on_phase_* closures bridge to Python (5B).
                //
                // Race the panik signal against `process_binaries`
                // in an inner scoped block. The block returns either
                // `Some(panik_path)` (panik fired) or `None` (manager
                // ran to completion and outputs were already collected
                // off `manager`). Done in a scope so the
                // `process_binaries` future and its borrows are
                // dropped before we hand `&mut manager` /
                // `&mut factory` to the post-race teardown step —
                // overlapping the mutable borrows is the rustc
                // pitfall the select! tries to nest in.
                //
                // The LocalManager has no per-loop panik arm — only
                // one entry point, so the race lives here at the
                // PyO3 boundary. On panik we drop the manager future
                // (cancels in-flight worker selects), then fall
                // through to the panik teardown step.
                // Race outcome carries either the manager's terminal
                // `Result<(), String>` or the panik-matched path.
                // The inner scope drops `manager_future` (and its
                // mutable borrows of `manager` / `factory`) before
                // the post-race teardown reads stats / kills workers.
                enum RaceOutcome {
                    ManagerDone(Result<(), String>),
                    Panik(std::path::PathBuf),
                }
                let race: RaceOutcome = {
                    let manager_future = manager.process_binaries(
                        rust_binaries,
                        phase_deps,
                        on_phase_start,
                        on_phase_end,
                        &mut factory,
                    );
                    tokio::pin!(manager_future);

                    tokio::select! {
                        biased;
                        // Bias toward the panik arm so a signal that
                        // resolves in the same tick as a manager
                        // event wins. Operational stop-now must not
                        // be starved by a busy worker event loop.
                        panik = async {
                            match panik_rx {
                                Some(rx) => rx.await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match panik {
                                Ok(signal) => RaceOutcome::Panik(signal.matched_path),
                                // Sender dropped (watcher disabled
                                // or aborted): fall back to running
                                // the manager future to completion.
                                Err(_) => {
                                    let r = (&mut manager_future).await;
                                    RaceOutcome::ManagerDone(r)
                                }
                            }
                        }
                        r = &mut manager_future => {
                            RaceOutcome::ManagerDone(r)
                        }
                    }
                };

                // Manager future is now either completed or dropped;
                // borrows are released. Safe to read `&manager` and
                // `&mut factory`.
                match race {
                    RaceOutcome::ManagerDone(result) => {
                        // Fold the discovery-time already-done skips into the
                        // manager's own `skipped` tally so `ProcessingStats`
                        // is the single accounting sink the operator reads —
                        // the manager never saw these items (they were
                        // partitioned out before dispatch), so the count is
                        // added here, at the one place the discovery partition
                        // is known.
                        let mut stats = manager.stats().clone();
                        stats.skipped += already_done_skipped;
                        self.stats = Some(stats);
                        self.failed_tasks = manager.failed_tasks().to_vec();
                        self.oom_tasks = manager.resource_pressure_tasks().to_vec();
                        self.task_payloads = manager.task_payloads().to_vec();
                        factory.cleanup_all();
                        LocalRunOutcome::Done(result)
                    }
                    RaceOutcome::Panik(matched_path) => {
                        tracing::error!(
                            matched_path = %matched_path.display(),
                            "panik signal observed on local manager; \
                             tearing down worker process trees"
                        );
                        factory.cleanup_all_process_trees(PANIK_KILL_GRACE);
                        LocalRunOutcome::Panik(matched_path)
                    }
                }
            }))
        });

        match run_outcome {
            LocalRunOutcome::Done(result) => {
                result.map_err(pyo3::exceptions::PyRuntimeError::new_err)
            }
            LocalRunOutcome::Panik(matched_path) => {
                tracing::error!(
                    matched_path = %matched_path.display(),
                    "panik shutdown: local manager exiting with code 137"
                );
                std::process::exit(137);
            }
        }
    }

    #[getter]
    fn stats(&self) -> PyResult<PyProcessingStats> {
        let s = self.stats.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("process_binaries not yet called")
        })?;
        Ok(PyProcessingStats {
            completed: s.completed,
            total: s.total,
            errored: s.errored,
            skipped: s.skipped,
        })
    }

    #[getter]
    fn failed_tasks(&self) -> Vec<PyFailedTask> {
        self.failed_tasks
            .iter()
            .map(|t| PyFailedTask {
                binary: PyTaskInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }

    #[getter]
    fn task_results(&self) -> Vec<(PyTaskInfo, Option<Vec<u8>>)> {
        self.task_payloads
            .iter()
            .map(|(bi, data)| (PyTaskInfo::from(bi), data.clone()))
            .collect()
    }

    #[getter]
    fn oom_tasks(&self) -> Vec<PyFailedTask> {
        self.oom_tasks
            .iter()
            .map(|t| PyFailedTask {
                binary: PyTaskInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }
}

/// Partition the marked discovery batch the extract boundary yields into the
/// items the single-process path DISPATCHES (unmarked) and a count of the
/// items it SKIPS (marked `skipped_already_done` — outputs already exist).
///
/// Single concern: the single-process dispatch path's honoring of the uniform
/// `skipped_already_done` discovery-boundary contract. A marked item is NOT
/// returned for dispatch — re-running already-done work is a correctness
/// regression — and is instead tallied into the returned `skipped` count,
/// which the caller folds into the run's `ProcessingStats.skipped`. The bit
/// is consumed HERE and never rides into the manager (it is a discovery-time
/// routing signal, not a property of the scheduling unit).
fn partition_already_done(
    extracted: Vec<(TaskInfo<RunnerIdentifier>, bool)>,
) -> (Vec<TaskInfo<RunnerIdentifier>>, u32) {
    let mut to_dispatch = Vec::with_capacity(extracted.len());
    let mut already_done_skipped: u32 = 0;
    for (task, skipped) in extracted {
        if skipped {
            already_done_skipped += 1;
        } else {
            to_dispatch.push(task);
        }
    }
    (to_dispatch, already_done_skipped)
}

#[cfg(test)]
mod partition_tests {
    //! Pure-Rust unit tests for the single-process `skipped_already_done`
    //! honoring (orchestrator refinement R3). No CPython needed — the
    //! partition operates on the already-extracted `(TaskInfo, bool)` pairs,
    //! so it is tested directly without the `test-with-python` feature.
    use super::*;
    use dynrunner_core::PhaseId;
    use std::path::PathBuf;

    /// A minimal `TaskInfo<RunnerIdentifier>` with id `id`. Only the fields the
    /// partition touches matter; the rest are defaults.
    fn task(id: &str) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tmp/{id}")),
            size: 1,
            identifier: RunnerIdentifier::from(id),
            phase_id: PhaseId::from("p"),
            type_id: dynrunner_core::TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: id.to_string(),
            task_depends_on: Vec::new(),
            preferred_secondaries: Default::default(),
            preferred_version: Default::default(),
            kind: Default::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    /// A marked (`skipped_already_done=true`) item is NOT returned for
    /// dispatch — dispatching it would re-run already-done work — and is
    /// counted in the skipped tally; unmarked items pass through to dispatch
    /// in order.
    #[test]
    fn marked_items_are_skipped_unmarked_dispatched() {
        let extracted = vec![
            (task("a"), false),
            (task("done-1"), true),
            (task("b"), false),
            (task("done-2"), true),
        ];

        let (to_dispatch, skipped) = partition_already_done(extracted);

        assert_eq!(skipped, 2, "two marked items are tallied as skipped");
        let dispatched_ids: Vec<&str> =
            to_dispatch.iter().map(|t| t.task_id.as_str()).collect();
        assert_eq!(
            dispatched_ids,
            vec!["a", "b"],
            "only unmarked items dispatch, in order; marked items are dropped from dispatch"
        );
    }

    /// An all-unmarked batch dispatches everything and skips nothing
    /// (back-compat default — today's behaviour for a producer that never
    /// marks a skip).
    #[test]
    fn all_unmarked_dispatches_all() {
        let extracted = vec![(task("a"), false), (task("b"), false)];
        let (to_dispatch, skipped) = partition_already_done(extracted);
        assert_eq!(skipped, 0);
        assert_eq!(to_dispatch.len(), 2);
    }

    /// An all-marked batch dispatches nothing and skips every item.
    #[test]
    fn all_marked_dispatches_none() {
        let extracted = vec![(task("done-1"), true), (task("done-2"), true)];
        let (to_dispatch, skipped) = partition_already_done(extracted);
        assert_eq!(skipped, 2);
        assert!(
            to_dispatch.is_empty(),
            "a fully-already-done batch dispatches no work"
        );
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Pre-run `handle()` factory contract tests for the in-process
    //! local manager. Mirrors `PyDistributedManager::handle`'s shape
    //! 1:1 — same single concern: can the Python caller fetch a
    //! `PrimaryHandle` BEFORE `process_binaries` enters its detached
    //! tokio runtime?
    //!
    //! Tests require an embedded CPython interpreter (gated behind
    //! the `test-with-python` feature). Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python pylocal_manager`
    //!
    //! Scope: limited to (1) the factory call surface and (2) the
    //! clone-equivalence of two `handle()` results. End-to-end
    //! command dispatch is exercised by the
    //! `primary_handle.rs::primary_handle_spawn_tasks_releases_gil`
    //! test against a stub receiver; the channel wiring on this
    //! manager carries the same `PyPrimaryHandle::from_sender`
    //! constructor, so the same dispatch contract holds transitively.
    use super::*;
    use pyo3::types::{PyAnyMethods, PyModule};

    /// Compile a tiny Python module that exports a `TaskDefinition`-
    /// shaped stub + a default `task_args` Namespace. Same shape as
    /// the distributed-manager test stub.
    fn build_task_definition_module(py: Python<'_>) -> Bound<'_, PyModule> {
        let source = r#"
from types import SimpleNamespace

def estimate_memory(item):
    return 1024 * 1024

_TYPE = SimpleNamespace(
    type_id="t",
    worker_module="stub_worker_module",
    estimator_attr="estimate_memory",
    timeout_seconds=None,
    reserved_memory_per_worker=0,
    max_concurrent=None,
)

_PHASE = SimpleNamespace(
    phase_id="p",
    depends_on=[],
    types=(_TYPE,),
)

class _StubTask:
    uses_file_based_items = False
    estimate_memory = staticmethod(estimate_memory)
    def get_phases(self):
        return (_PHASE,)
    def build_worker_command_args(self, type_id, args, source_dir, output_dir, skip_existing):
        return []

task = _StubTask()
task_args = SimpleNamespace()
"#;
        PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new("stub_local_task_def.py")
                .unwrap()
                .as_c_str(),
            std::ffi::CString::new("stub_local_task_def")
                .unwrap()
                .as_c_str(),
        )
        .expect("compile stub TaskDefinition module")
    }

    /// Construct a `PyLocalManager` with the minimum args required.
    /// Output dir is a tempdir so the per-run log-dir creation
    /// succeeds.
    fn build_manager(py: Python<'_>) -> PyResult<Py<PyLocalManager>> {
        let module = build_task_definition_module(py);
        let task = module.getattr("task")?;
        let task_args = module.getattr("task_args")?;
        // tempdir: PyLocalManager::new creates log_dir under it.
        let tmp = std::env::temp_dir().join(format!("rust_pylocal_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).expect("tmp");
        let source = tmp.join("src");
        let output = tmp.join("out");
        std::fs::create_dir_all(&source).expect("src");
        std::fs::create_dir_all(&output).expect("out");
        let mgr = PyLocalManager::new(
            py,
            /* num_workers */ 1,
            /* max_memory */ 64 * 1024 * 1024,
            /* source_dir */ source.to_string_lossy().into_owned(),
            /* output_dir */ output.to_string_lossy().into_owned(),
            &task,
            &task_args,
            /* skip_existing */ false,
            /* reuse_workers */ true,
            /* restart_predicate */ None,
            /* retry_max_attempts */ 1,
            /* print_pid */ false,
            /* connection_mode */ "socketpair",
            /* socket_dir */ None,
            /* manual_start_worker */ false,
            /* log_paths */ None,
            /* worker_spec */ None,
            /* low_memory_threshold */ None,
            /* scheduler_config */ None,
            /* phase_status_log_intervals_secs */ None,
            /* stage_timeouts_secs */ None,
            /* max_resources */ None,
            /* low_resource_thresholds */ None,
            /* log_oom_watcher */ false,
            /* log_dir */ None,
            /* panik_watcher_paths */ None,
            /* panik_watcher_poll_interval_secs */ 10.0,
            /* memprofile_enabled */ false,
        )?;
        Py::new(py, mgr)
    }

    /// Factory test: `handle()` succeeds BEFORE `process_binaries`.
    #[test]
    fn handle_returns_pyprimaryhandle_before_process_binaries() {
        Python::attach(|py| {
            let mgr = build_manager(py).expect("manager constructs");
            let handle_obj = mgr
                .bind(py)
                .call_method0("handle")
                .expect("handle() must succeed before process_binaries");
            // Downcast to prove the type contract.
            let _handle: pyo3::PyRef<'_, PyPrimaryHandle> = handle_obj
                .cast::<PyPrimaryHandle>()
                .expect("handle() must return a PrimaryHandle pyclass")
                .borrow();
        });
    }

    /// Two `handle()` calls return distinct `PrimaryHandle`
    /// instances backed by the same underlying channel — proven via
    /// `tokio::sync::mpsc::Sender::same_channel`. The factory must
    /// not mint a fresh channel per call.
    #[test]
    fn handle_clones_share_same_command_channel() {
        Python::attach(|py| {
            let mgr = build_manager(py).expect("manager constructs");
            let h1 = mgr.bind(py).call_method0("handle").expect("first handle");
            let h2 = mgr.bind(py).call_method0("handle").expect("second handle");
            let r1 = h1.cast::<PyPrimaryHandle>().unwrap();
            let r2 = h2.cast::<PyPrimaryHandle>().unwrap();
            let r1_sender = r1.borrow().sender.clone();
            let r2_sender = r2.borrow().sender.clone();
            assert!(
                r1_sender.same_channel(&r2_sender),
                "two handles minted from the same manager must clone the same Sender"
            );
            // And the manager's own command_tx must also be the same
            // channel — pin the round trip via the pyclass field.
            let mgr_ref = mgr.borrow(py);
            assert!(
                mgr_ref.command_tx.same_channel(&r1_sender),
                "manager.command_tx must be the same channel as the minted handle"
            );
        });
    }
}
