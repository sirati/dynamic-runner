use dynrunner_core::{Identifier, ResourceMap, TypeId, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{KillReason, ResourcePressureDecision, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc;

use crate::cgroup::{self, NestedCgroupHandle, SubcgroupHandle};
use crate::manager::WorkerFactory;
use crate::worker::{WorkerEvent, WorkerHandle};

/// Per-spawn cgroup-v2 leaf creation helper.
///
/// Single concern: cross the boundary between the pool's stashed
/// parent handle and the per-worker leaf the spawn site needs.
/// Returns `Ok(None)` when no parent handle exists (graceful-
/// fallback environment, or caller opted out at init); the spawn
/// site then proceeds with the inherited cgroup. Errors from the
/// leaf-creation primitive bubble up as a formatted string the
/// surrounding spawn-site uses verbatim — keeping the error-string
/// shape parallel to the existing factory.spawn_worker error path.
fn prepare_worker_leaf(
    cgroup_handle: &Option<NestedCgroupHandle>,
    worker_id: WorkerId,
) -> Result<Option<SubcgroupHandle>, String> {
    cgroup_handle
        .as_ref()
        .map(|h| cgroup::prepare_worker_subgroup(h, worker_id))
        .transpose()
        .map_err(|e| format!("per-worker cgroup setup failed for worker {worker_id}: {e}"))
}

/// Outcome of [`WorkerPool::ensure_worker_for_type`].
///
/// Two-axis discriminator: was the slot already bound to the required
/// type (`AlreadyLoaded`, the dominant single-type fast path), or did
/// the call SIGKILL the prior subprocess and spawn a new one whose
/// readiness is still pending (`RespawnInProgress`)? The caller MUST
/// branch on this so it can bounce in-flight tasks as backpressure
/// rather than try to assign onto a worker that hasn't sent its
/// `Response::Ready` yet.
///
/// See [`WorkerPool::ensure_worker_for_type`]'s rustdoc for the
/// wedge-prevention rationale: pre-fix the wait-for-Ready was
/// synchronous and held the secondary's `select!` arm body open for
/// the entire duration the new subprocess took to start (300+s
/// production wedge on slow Python init). The async-Ready event flow
/// is what removes that wedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureWorkerOutcome {
    /// Same-type fast path — the slot's `loaded_type_id` already
    /// matched `required_type`. No kill, no spawn, no Ready wait. The
    /// caller may immediately proceed with `assign_task`.
    AlreadyLoaded,
    /// Type-shift respawn was issued. The prior subprocess was
    /// SIGKILLed and a fresh one has been spawned for the new type;
    /// a background task is driving `wait_ready` and will emit
    /// [`crate::worker::WorkerEvent::Ready`] (or `Disconnected`)
    /// through the pool's event channel when the new subprocess
    /// reports its protocol-level Ready response. **The slot is NOT
    /// assignable until the Ready event lands.** Callers should treat
    /// this as transient unavailability and route in-flight work
    /// elsewhere (backpressure-bounce to the primary in the
    /// distributed-mode case).
    RespawnInProgress,
}

/// Result of a resource pressure check — tells the caller what happened so it
/// can take manager-specific action (requeue task, report to primary, etc.).
pub enum ResourcePressureResult<I: Identifier> {
    /// A worker was killed. The caller should handle the displaced binary
    /// (e.g. requeue locally or report failure to primary).
    ///
    /// `binary` is boxed because `TaskInfo<I>` is large enough that the
    /// inlined variant blew this enum out to ~236 bytes against
    /// `NoAction`'s zero (clippy::large_enum_variant). Consumers unbox
    /// once when passing the displaced task to its next destination.
    Killed {
        worker_id: WorkerId,
        binary: Option<Box<dynrunner_core::TaskInfo<I>>>,
        reason: KillReason,
    },
    /// No action needed — resources are within limits.
    NoAction,
}

/// Shared worker pool used by both `LocalManager` and `SecondaryCoordinator`.
///
/// Owns the workers, the event channel, and provides lifecycle operations
/// (initialize, restart, OOM check, stop). Does NOT own scheduling decisions
/// or task queues — those remain with the specific manager.
pub struct WorkerPool<M: ManagerEndpoint, I: Identifier> {
    pub workers: Vec<WorkerHandle<M, I>>,
    event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    event_rx: mpsc::UnboundedReceiver<WorkerEvent<I>>,
    /// Nested cgroup-v2 workers subgroup, materialised by
    /// [`Self::initialize`] when the caller supplies a
    /// `mem_manager_reserved_bytes` value AND the runtime environment
    /// supports a delegated cgroup-v2 tree. `None` covers both "caller
    /// opted out" (passed `None` to `initialize`) and "environment
    /// doesn't support nesting" (the [`cgroup::setup_worker_cgroup`]
    /// graceful-fallback case). Kept alive on the pool so the
    /// directory persists for the whole run; dropped when the pool is
    /// dropped, which is fine — the kernel reaps the empty cgroup
    /// directory automatically once all attached pids have exited.
    workers_cgroup: Option<NestedCgroupHandle>,
}

impl<M: ManagerEndpoint + 'static, I: Identifier> Default for WorkerPool<M, I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: ManagerEndpoint + 'static, I: Identifier> WorkerPool<M, I> {
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            workers: Vec::new(),
            event_tx,
            event_rx,
            workers_cgroup: None,
        }
    }

    /// Accessor for the materialised workers/ cgroup handle, if any.
    /// Exposed for tests + diagnostic logging; production callers do
    /// not need this — the pool's per-spawn sites materialise a
    /// per-worker leaf from this parent handle (via
    /// [`cgroup::prepare_worker_subgroup`]) and hand the leaf borrow
    /// directly to [`WorkerFactory::spawn_worker`] for `pre_exec`-
    /// time pid-attachment.
    pub fn workers_cgroup(&self) -> Option<&NestedCgroupHandle> {
        self.workers_cgroup.as_ref()
    }

    /// Shared event sender — needed when constructing WorkerHandles externally.
    pub fn event_tx(&self) -> &mpsc::UnboundedSender<WorkerEvent<I>> {
        &self.event_tx
    }

    /// Receive the next worker event (async, blocks until one arrives).
    pub async fn recv_event(&mut self) -> Option<WorkerEvent<I>> {
        self.event_rx.recv().await
    }

    /// Initialize N workers using the factory, assigning budgets via the scheduler.
    /// Returns an error if any spawn fails — the caller should abort the run.
    ///
    /// `mem_manager_reserved_bytes` controls the nested cgroup-v2
    /// workers subgroup:
    ///
    ///   * `None`: skip nesting entirely. Workers stay in the parent
    ///     leaf and a kernel cgroup-OOM in the parent reaps the
    ///     secondary too (legacy behaviour, preserved for back-compat
    ///     and for tests / in-process channel modes that don't spawn
    ///     subprocesses).
    ///   * `Some(0)`: create the workers subgroup with NO cap
    ///     tightening (the secondary process gets no protected slice).
    ///     Useful for measuring the kernel-OOM-isolation benefit
    ///     without the budget hit.
    ///   * `Some(n)`: create the workers subgroup and set
    ///     `workers/memory.max = parent_memory.max - n`. Reserves
    ///     `n` bytes for the secondary process itself; a workers-
    ///     side memory blow-up trips kernel-OOM on the workers
    ///     subgroup, leaving the secondary alive.
    ///
    /// On graceful-fallback conditions (not under cgroup-v2, missing
    /// memory controller, leaf not writable) the function logs a
    /// `tracing::warn!` line via the [`crate::cgroup`] orchestrator
    /// and proceeds with the flat layout (`workers_cgroup = None`).
    /// Genuine I/O errors propagate as `Err(...)`.
    pub async fn initialize<S: Scheduler<I>>(
        &mut self,
        num_workers: u32,
        max_resources: &ResourceMap,
        scheduler: &S,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
        mem_manager_reserved_bytes: Option<u64>,
    ) -> Result<(), String> {
        // Set up the nested workers cgroup once per pool, BEFORE the
        // spawn loop. If the caller opted out (`None`) or the
        // environment doesn't support nesting (graceful fallback),
        // the per-spawn leaf-creation helper returns `None` and the
        // factory leaves the child in the inherited cgroup (pre-
        // nested layout, unchanged behaviour). Errors here are I/O-
        // level (corrupted /proc or sysfs) and bubble up so the
        // caller can abort the run with a clear cause.
        if let Some(reserved) = mem_manager_reserved_bytes {
            self.workers_cgroup = cgroup::setup_worker_cgroup_default(reserved)
                .map_err(|e| format!("nested workers cgroup setup failed: {e}"))?;
        }

        for i in 0..num_workers {
            let subcgroup = prepare_worker_leaf(&self.workers_cgroup, i)?;
            let (transport, pid) = factory
                .spawn_worker(i, subcgroup.as_ref())
                .map_err(|e| format!("failed to spawn worker {i}: {e}"))?;
            if print_pid && let Some(pid) = pid {
                tracing::info!(worker_id = i, pid, "worker PID");
            }
            let mut handle = WorkerHandle::new(i, transport, self.event_tx.clone());
            handle.pid = pid;
            handle.subcgroup = subcgroup;
            let budget = scheduler.initial_budget(i, max_resources);
            let budget_mb = budget.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
            handle.reserved_budgets = budget;
            tracing::info!(worker_id = i, budget_mb, "worker created");
            self.workers.push(handle);
        }

        self.wait_for_all_ready().await;
        Ok(())
    }

    /// Block until every worker has reported Ready.
    pub async fn wait_for_all_ready(&mut self) {
        loop {
            let all_ready = self.workers.iter().all(|w| w.is_ready());
            if all_ready {
                tracing::info!("all workers ready");
                break;
            }
            for worker in &mut self.workers {
                if !worker.is_ready() {
                    worker.poll_ready().await;
                }
            }
            tokio::task::yield_now().await;
        }
    }

    /// Restart a single worker: stop the old one, spawn a fresh transport,
    /// preserve budget and assignment_failure_count, wait for Ready.
    /// Returns an error if the spawn fails — the caller decides how to react
    /// (typically: log, mark the slot dead, continue with remaining workers).
    ///
    /// Preserves the worker's recorded `loaded_type_id` across the
    /// respawn: if the slot was bound to a particular `TypeId` before
    /// the restart, the replacement is spawned via
    /// `WorkerFactory::spawn_worker_for_type` for the same type so the
    /// fresh subprocess's argv matches. Without this, the next
    /// `ensure_worker_for_type` would see `loaded_type_id == None`,
    /// pessimistically assume mismatch, and trigger a redundant
    /// kill+respawn — turning every per-task-restart cycle into
    /// two spawns instead of one. The fallback to `spawn_worker`
    /// preserves the legacy initial-spawn semantic for slots that
    /// were never bound (e.g. a restart triggered before any
    /// assignment landed).
    pub async fn restart_worker(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        self.replace_worker_slot(worker_id, factory, print_pid, "restart")
            .await?;

        // Wait for ready INLINE. This blocks the calling task until the
        // freshly-respawned worker reports `Response::Ready`. Used only
        // by callers that own their own follow-up sequencing and are NOT
        // driving a `select!`-shaped operational loop — the in-process
        // [`crate::manager::LocalManager`] pipeline, which dispatches the
        // next task immediately after this returns. A `select!`-driven
        // caller (the distributed secondary) MUST use
        // [`Self::restart_worker_async`], which pushes the wait into a
        // background watcher so the operational loop's other arms
        // (keepalive, peer messages, OOM ticks) keep running while a slow
        // subprocess starts.
        loop {
            if self.workers[worker_id as usize].is_ready() {
                break;
            }
            self.workers[worker_id as usize].poll_ready().await;
            tokio::task::yield_now().await;
        }

        tracing::info!(worker_id, "worker restarted and ready");
        Ok(())
    }

    /// Restart a single worker **without blocking the calling task on
    /// the freshly-respawned worker's Ready**.
    ///
    /// Same kill + spawn + budget-preservation sequence as
    /// [`Self::restart_worker`], but instead of the inline wait-for-Ready
    /// loop it spawns a background `wait_ready` watcher that emits the
    /// [`crate::worker::WorkerEvent::Ready`] event through the pool's
    /// shared event channel once the new subprocess reports
    /// `Response::Ready`. The slot is left in `Transitioning`
    /// (`is_idle_state()` / `is_ready()` both false) until that event
    /// lands, so the operational loop's `WorkerEvent::Ready` arm
    /// reclaims the protocol state and re-engages the slot.
    ///
    /// # Wedge prevention (production-bug pin)
    ///
    /// The inline-wait [`Self::restart_worker`], when called from inside
    /// the secondary's `select!`-driven operational loop, held the whole
    /// `select!` open for the entire duration a slow worker subprocess
    /// took to send `Response::Ready` — no keepalive ticks, no router
    /// events, no OOM ticks fired for that window. In production a
    /// `WorkerEvent::Disconnected` at a singleton-typed phase boundary
    /// (a slow build worker that never reached Ready) drove the
    /// operational restart loop into this wait; the new subprocess took
    /// far longer than the keepalive interval to start, so the primary
    /// observed a keepalive gap and declared the busy-but-alive secondary
    /// dead. The async variant is the SAME fix shape
    /// `ensure_worker_for_type_async` applied to the type-shift dispatch
    /// path: the wait rides a background task, the operational loop keeps
    /// its keepalive cadence.
    ///
    /// Preserves `reserved_budgets` and `assignment_failure_count` across
    /// the respawn — same contract as [`Self::restart_worker`].
    pub async fn restart_worker_async(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        self.replace_worker_slot(worker_id, factory, print_pid, "restart (async)")
            .await?;

        // Push the wait-for-Ready into a background task; it emits
        // `WorkerEvent::Ready` (or `Disconnected`) on the pool's shared
        // event channel. The slot stays `Transitioning` until the
        // operational loop's Ready arm reclaims it. Failure here
        // ("not in WaitingForReady") is a programmer error —
        // `replace_worker_slot` installed a freshly-`WorkerHandle::new`-d
        // handle one statement ago — but we propagate it rather than
        // panic so the caller can surface a clean failure.
        self.workers[worker_id as usize].spawn_ready_watcher()?;

        tracing::info!(
            worker_id,
            "worker respawned; wait_ready running in background"
        );
        Ok(())
    }

    /// Shared kill + spawn + budget-preservation core for the two
    /// worker-restart entry points ([`Self::restart_worker`] inline-wait,
    /// [`Self::restart_worker_async`] background-watcher). Stops the old
    /// subprocess, aborts its orphan poll task, recreates the per-worker
    /// cgroup leaf, respawns the SAME `loaded_type_id` (so the argv keeps
    /// matching), and installs a fresh `WorkerHandle` in
    /// `WaitingForReady` carrying the preserved budgets + failure count.
    /// Returns with the slot ready for the caller's chosen wait strategy;
    /// it does NOT wait for `Response::Ready`.
    ///
    /// `label` only flavours the PID log line so the two callers stay
    /// distinguishable in operator logs.
    async fn replace_worker_slot(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
        label: &str,
    ) -> Result<(), String> {
        let old = &mut self.workers[worker_id as usize];
        if !old.is_stopped() {
            old.stop().await;
        }
        // Abort the orphan poll task (if any) BEFORE the slot is
        // replaced. `stop()` only handles the Idle case; a
        // Transitioning slot (poll_loop running) leaves the spawned
        // task detached on handle drop, which can later emit a
        // buffered `Response::Completed` (or pipe-EOF `Disconnected`)
        // on the pool's shared event channel for this worker_id. The
        // secondary's `handle_worker_event` has already cleaned
        // `active_tasks` for the killed task, so the stale event
        // surfaces as `task done task_hash=None` and the dispatcher's
        // accounting silently stalls. See `WorkerHandle::abort_poll_task`
        // for the full rationale.
        old.abort_poll_task();

        let preserved_type = self.workers[worker_id as usize].loaded_type_id.clone();
        // Drop the prior slot's `SubcgroupHandle` BEFORE creating
        // the replacement leaf: `SubcgroupHandle::Drop` best-effort
        // rmdirs the previous `worker-<id>/` directory, and the
        // per-spawn `prepare_worker_subgroup` is idempotent
        // (`mkdir` returns OK on a re-created leaf), so an interim
        // residual directory between drop + recreate is harmless.
        // The explicit take also keeps the slot's `subcgroup`
        // field consistent with the doomed protocol state across
        // the kill+spawn window.
        self.workers[worker_id as usize].subcgroup = None;
        let subcgroup = prepare_worker_leaf(&self.workers_cgroup, worker_id)?;
        let (transport, pid) = match &preserved_type {
            Some(type_id) => factory
                .spawn_worker_for_type(worker_id, type_id, subcgroup.as_ref())
                .map_err(|e| {
                    format!("failed to respawn worker {worker_id} for type {type_id}: {e}")
                })?,
            None => factory
                .spawn_worker(worker_id, subcgroup.as_ref())
                .map_err(|e| format!("failed to respawn worker {worker_id}: {e}"))?,
        };
        if print_pid && let Some(pid) = pid {
            tracing::info!(worker_id, pid, label, "worker PID ({label})");
        }

        let reserved_budgets = self.workers[worker_id as usize].reserved_budgets.clone();
        let failure_count = self.workers[worker_id as usize].assignment_failure_count;

        let mut handle = WorkerHandle::new(worker_id, transport, self.event_tx.clone());
        handle.pid = pid;
        handle.subcgroup = subcgroup;
        handle.reserved_budgets = reserved_budgets;
        handle.assignment_failure_count = failure_count;
        handle.loaded_type_id = preserved_type;
        self.workers[worker_id as usize] = handle;
        Ok(())
    }

    /// Ensure the given worker's subprocess is bound to `required_type`,
    /// **blocking the calling task until the freshly-respawned worker
    /// reports `Response::Ready`**.
    ///
    /// Used by callers that are not driving a `select!`-shaped event
    /// loop — typically the in-process [`crate::manager::LocalManager`]
    /// pipeline, which expects "ensure returned Ok ⇒ slot is
    /// assignable now". The blocking wait is bounded by the
    /// subprocess's own startup time; for the distributed-mode
    /// `select!` callers see
    /// [`Self::ensure_worker_for_type_async`] which does NOT block
    /// the operational loop on the new worker's Ready.
    ///
    /// Same-type fast path: when the recorded `loaded_type_id`
    /// matches `required_type` (the dominant case in a single-type
    /// run), this is a no-op — no kill, no spawn, no Ready wait.
    /// The pre-existing single-type observable behaviour (one
    /// process per slot for the lifetime of the run) is preserved
    /// bit-for-bit.
    ///
    /// Empty-state path: a freshly-initialised worker has
    /// `loaded_type_id == None` because [`Self::initialize`] cannot
    /// generally know which `TypeId` the first assignment will pick.
    /// The mismatch arm fires once on the first assignment per slot,
    /// binding the worker to that type.
    ///
    /// Preserves `reserved_budgets` and `assignment_failure_count`
    /// across the respawn — same contract as [`Self::restart_worker`].
    pub async fn ensure_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        required_type: &TypeId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        let idx = worker_id as usize;
        if self
            .workers
            .get(idx)
            .and_then(|w| w.loaded_type_id.as_ref())
            == Some(required_type)
        {
            return Ok(());
        }

        // Mismatch path — same kill+spawn shape as the async
        // variant, but the wait-for-Ready strategy diverges to
        // preserve the pre-extraction observable behaviour. This
        // path drives `poll_ready` inline, so no
        // [`crate::worker::WorkerEvent::Ready`] lands in the pool's
        // event channel and the operational-loop's Ready arm is
        // NOT triggered downstream. (The async variant emits the
        // event so the distributed-secondary's `select!`-driven
        // handler can reclaim + repoll without blocking other arms;
        // a sync caller — LocalManager pipeline, in-process
        // distributed dispatch where the kill+spawn-bind is fast —
        // owns its own follow-up sequencing and does not need a
        // channel event.)
        let old = &mut self.workers[idx];
        if !old.is_stopped() {
            old.stop().await;
        }
        // Abort the orphan poll task before SIGKILL + replace. See
        // `restart_worker` for the full rationale; same single-concern
        // boundary applies to every worker-replacement lifecycle path
        // on the pool.
        old.abort_poll_task();
        old.kill_subprocess();
        // See `restart_worker` for the drop-before-recreate
        // rationale on the slot's prior `SubcgroupHandle`.
        old.subcgroup = None;

        let subcgroup = prepare_worker_leaf(&self.workers_cgroup, worker_id)?;
        let (transport, pid) = factory
            .spawn_worker_for_type(worker_id, required_type, subcgroup.as_ref())
            .map_err(|e| {
                format!("failed to respawn worker {worker_id} for type {required_type}: {e}")
            })?;
        if print_pid && let Some(pid) = pid {
            tracing::info!(
                worker_id,
                pid,
                type_id = %required_type,
                "worker PID (type-shift respawn, sync)"
            );
        }

        let reserved_budgets = self.workers[idx].reserved_budgets.clone();
        let failure_count = self.workers[idx].assignment_failure_count;

        let mut handle = WorkerHandle::new(worker_id, transport, self.event_tx.clone());
        handle.pid = pid;
        handle.subcgroup = subcgroup;
        handle.reserved_budgets = reserved_budgets;
        handle.assignment_failure_count = failure_count;
        handle.loaded_type_id = Some(required_type.clone());
        self.workers[idx] = handle;

        // Drive `poll_ready` directly — no background task, no
        // event-channel emission. The synchronous loop mirrors the
        // pre-extraction implementation bit-for-bit.
        loop {
            if self.workers[idx].is_ready() {
                break;
            }
            self.workers[idx].poll_ready().await;
            tokio::task::yield_now().await;
        }

        tracing::info!(
            worker_id,
            type_id = %required_type,
            "worker respawned for type-shift and ready (sync)"
        );
        Ok(())
    }

    /// Ensure the given worker's subprocess is bound to `required_type`,
    /// **without blocking the calling task on the freshly-respawned
    /// worker's Ready**.
    ///
    /// Per-type subprocess dispatch primitive: when a multi-phase
    /// `TaskDefinition` declares one `TaskTypeSpec` (and therefore one
    /// `worker_module`) per phase, the worker subprocess that ran
    /// phase N's tasks cannot execute phase N+1's tasks — its argv
    /// loaded the wrong module. This call compares the worker's
    /// recorded `loaded_type_id` against the next task's `type_id`
    /// and, on mismatch, kills + respawns the slot through
    /// [`WorkerFactory::spawn_worker_for_type`].
    ///
    /// # Return outcomes
    ///
    /// * `Ok(EnsureWorkerOutcome::AlreadyLoaded)` — same-type fast
    ///   path. No kill, no spawn, no Ready wait. The slot is
    ///   immediately assignable.
    /// * `Ok(EnsureWorkerOutcome::RespawnInProgress)` — the prior
    ///   subprocess was SIGKILLed and a new one has been spawned for
    ///   `required_type`. The slot is **not yet assignable**: the
    ///   new worker hasn't reported `Response::Ready` yet. The caller
    ///   MUST treat this as "no idle worker available right now" —
    ///   e.g. bounce the task to the primary as backpressure — and
    ///   wait for the standard [`crate::worker::WorkerEvent::Ready`]
    ///   arrival via the pool's event channel before re-trying.
    /// * `Err(_)` — the spawn syscall failed. The slot is in an
    ///   indeterminate state; the caller should requeue the worker
    ///   for restart via the standard `pending_worker_restarts`
    ///   machinery.
    ///
    /// # Wedge prevention (production-bug pin)
    ///
    /// Pre-split, the secondary's distributed-dispatch arm awaited
    /// the new worker's `Response::Ready` inline. When invoked from
    /// inside the secondary's `select!`-driven operational loop, the
    /// await blocked every other arm — keepalives, peer messages,
    /// worker events, OOM ticks — for the entire duration the new
    /// subprocess took to start. In production this manifested as a
    /// 300s tokio-runtime silence on asm-tokenizer's LMU dispatch
    /// when a singleton-typed phase chain (one task per phase, each
    /// phase a distinct `TypeId`) forced a respawn on every phase
    /// boundary and one of the new Python subprocesses took longer
    /// than the primary's keepalive_timeout to send Ready. The
    /// `RespawnInProgress` shape pushes the wait into a background
    /// task that emits its terminal event through the standard event
    /// channel, so the operational loop's other arms keep running
    /// and the primary observes a steady keepalive stream regardless
    /// of how slow the new subprocess is.
    ///
    /// Preserves `reserved_budgets` and `assignment_failure_count`
    /// across the respawn — same contract as [`Self::restart_worker`].
    pub async fn ensure_worker_for_type_async(
        &mut self,
        worker_id: WorkerId,
        required_type: &TypeId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<EnsureWorkerOutcome, String> {
        let idx = worker_id as usize;
        if self
            .workers
            .get(idx)
            .and_then(|w| w.loaded_type_id.as_ref())
            == Some(required_type)
        {
            return Ok(EnsureWorkerOutcome::AlreadyLoaded);
        }

        let old = &mut self.workers[idx];
        if !old.is_stopped() {
            old.stop().await;
        }
        // Abort the orphan poll task before SIGKILL + replace. See
        // `restart_worker` for the full rationale; same single-concern
        // boundary applies to every worker-replacement lifecycle path
        // on the pool.
        old.abort_poll_task();
        // Eagerly SIGKILL the prior subprocess so the type-shift
        // respawn does not race against a still-running worker
        // continuing to load the previous type's worker_module. The
        // restart-pre-respawn SIGKILL on `WorkerHandle` is the same
        // primitive `worker_loop::handle_assignment_failure`'s
        // restart path implicitly relies on via the factory's child
        // tracking; surfacing it explicitly here also covers
        // factories whose `spawn_worker_for_type` overwrites slot
        // tracking without reaping the prior `Child` (no zombie
        // race window).
        old.kill_subprocess();
        // See `restart_worker` for the drop-before-recreate
        // rationale on the slot's prior `SubcgroupHandle`.
        old.subcgroup = None;

        let subcgroup = prepare_worker_leaf(&self.workers_cgroup, worker_id)?;
        let (transport, pid) = factory
            .spawn_worker_for_type(worker_id, required_type, subcgroup.as_ref())
            .map_err(|e| {
                format!("failed to respawn worker {worker_id} for type {required_type}: {e}")
            })?;
        if print_pid && let Some(pid) = pid {
            tracing::info!(
                worker_id,
                pid,
                type_id = %required_type,
                "worker PID (type-shift respawn)"
            );
        }

        let reserved_budgets = self.workers[idx].reserved_budgets.clone();
        let failure_count = self.workers[idx].assignment_failure_count;

        let mut handle = WorkerHandle::new(worker_id, transport, self.event_tx.clone());
        handle.pid = pid;
        handle.subcgroup = subcgroup;
        handle.reserved_budgets = reserved_budgets;
        handle.assignment_failure_count = failure_count;
        handle.loaded_type_id = Some(required_type.clone());
        // Spawn the wait-for-Ready background task BEFORE handing
        // the handle to `self.workers[idx]` so the protocol moves
        // into `Transitioning` and the slot's `is_idle_state()`
        // correctly reports false until the Ready event lands.
        // Failure here ("not in WaitingForReady") is a programmer
        // error — `WorkerHandle::new` constructed the handle in
        // WaitingForReady one statement ago — but we propagate the
        // error rather than panic so the caller can surface a clean
        // failure to the primary.
        handle.spawn_ready_watcher()?;
        self.workers[idx] = handle;

        tracing::info!(
            worker_id,
            type_id = %required_type,
            "worker respawned for type-shift; wait_ready running in background"
        );
        Ok(EnsureWorkerOutcome::RespawnInProgress)
    }

    /// Snapshot the slot's currently-bound `TypeId`, or `None` if the
    /// slot has never been bound to a type (initial pool-init state,
    /// or a restart that lost the binding). Callers can use this to
    /// distinguish first-bind (`None`) from true type-shift
    /// (`Some(T1)` → `Some(T2)`) before invoking
    /// [`Self::ensure_worker_for_type_async`].
    pub fn loaded_type_id(&self, worker_id: WorkerId) -> Option<&TypeId> {
        self.workers
            .get(worker_id as usize)
            .and_then(|w| w.loaded_type_id.as_ref())
    }

    /// Update actual resource usage for all workers from /proc/[pid]/statm.
    pub fn update_all_resource_usage(&mut self) {
        for worker in &mut self.workers {
            worker.update_resource_usage();
        }
    }

    /// Check resource pressure via the scheduler, kill if needed.
    ///
    /// Returns `ResourcePressureResult::Killed` with the displaced binary so
    /// the caller can decide what to do (requeue locally, report to primary, etc.).
    /// The worker is marked as killed but NOT restarted — the caller
    /// must call `restart_worker` if it wants the worker back.
    pub fn check_resource_pressure<S: Scheduler<I>>(
        &mut self,
        scheduler: &S,
        max_resources: &ResourceMap,
        in_pressure_phase: bool,
    ) -> ResourcePressureResult<I> {
        self.update_all_resource_usage();
        let infos = self.budget_infos();
        let decision = scheduler.check_resource_pressure(&infos, max_resources, in_pressure_phase);

        match decision {
            ResourcePressureDecision::Kill { worker_id, reason } => {
                tracing::warn!(
                    worker_id,
                    reason = %reason,
                    in_pressure_phase,
                    "killing worker under resource pressure ({reason})"
                );
                let worker = &mut self.workers[worker_id as usize];
                let binary = worker.current_binary.take().map(Box::new);
                worker.mark_oom_killed();
                ResourcePressureResult::Killed {
                    worker_id,
                    binary,
                    reason,
                }
            }
            ResourcePressureDecision::NoAction => ResourcePressureResult::NoAction,
        }
    }

    /// Build budget info snapshots for all workers.
    pub fn budget_infos(&self) -> Vec<WorkerBudgetInfo<I>> {
        self.workers.iter().map(|w| w.budget_info()).collect()
    }

    /// Stop all workers that aren't already stopped.
    pub async fn stop_all(&mut self) {
        for worker in &mut self.workers {
            if !worker.is_stopped() {
                worker.stop().await;
                tracing::info!(worker_id = worker.worker_id, "worker stopped");
            }
        }
    }

    /// Emergency-stop every worker AND its child process tree with
    /// a SIGTERM → grace → SIGKILL ladder.
    ///
    /// Single concern: "take down every worker pgid the pool owns,
    /// with a bounded escalation so a stuck child can't block the
    /// shutdown". Used by the coordinator panik-react path; the
    /// regular shutdown path goes through `stop_all` (which sends
    /// a clean protocol Stop and lets the worker exit on its own).
    ///
    /// Sequence:
    ///   1. SIGTERM to every worker's process group in one fan-out
    ///      pass. This signals the worker AND every descendant
    ///      sharing its pgid; workers that installed a SIGTERM
    ///      handler (`runtime.py::_install_term_handler`) translate
    ///      it into a clean shutdown.
    ///   2. Sleep `grace` (bounded by the caller — typically a few
    ///      seconds). The sleep is a single `tokio::time::sleep`
    ///      across the pool rather than per-worker, so the total
    ///      wait time is `grace`, not `grace * num_workers`.
    ///   3. SIGKILL to every process group still alive. The
    ///      `process_tree_alive` probe (`kill(-pgid, 0)`) tells us
    ///      whether SIGTERM already drained the group; live ones
    ///      escalate, dead ones are skipped.
    ///
    /// Idempotent: workers without a tracked pid, workers whose
    /// process tree has already exited, and workers where pgid
    /// signalling fails for any other reason are all no-ops on
    /// each step. The pool's `workers` vec is left intact — the
    /// caller is responsible for any subsequent state mutation
    /// (e.g. setting the protocol state to Stopped).
    pub async fn kill_all_workers_with_grace(&self, grace: std::time::Duration) {
        let count = self.workers.len();
        if count == 0 {
            return;
        }
        tracing::info!(
            workers = count,
            grace_ms = grace.as_millis() as u64,
            "kill_all_workers_with_grace: sending SIGTERM to every worker pgid"
        );
        for worker in &self.workers {
            worker.sigterm_process_tree();
        }
        // Single sleep across the pool — wall-clock teardown is
        // bounded by `grace`, not `grace * num_workers`.
        tokio::time::sleep(grace).await;
        let mut escalated = 0usize;
        for worker in &self.workers {
            if worker.process_tree_alive() {
                worker.sigkill_process_tree();
                escalated += 1;
            }
        }
        if escalated > 0 {
            tracing::error!(
                escalated,
                workers = count,
                "kill_all_workers_with_grace: SIGKILL escalation fired \
                 for {escalated}/{count} worker pgid(s) that ignored \
                 SIGTERM within the grace window"
            );
        }
    }
}

#[cfg(test)]
mod orphan_poll_task_tests {
    use super::*;
    use crate::manager::WorkerFactory;
    use crate::worker::WorkerEvent;
    use dynrunner_core::{
        ErrorType, MessageReceiver, MessageSender, ResourceKind, TaskInfo, WorkerId,
    };
    use dynrunner_protocol_manager_worker::{Command, Response};
    use dynrunner_transport_channel::{ChannelManagerEnd, ChannelRunnerEnd, channel_pair};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    fn make_binary(name: &str) -> TaskInfo<TestId> {
        TaskInfo {
            path: std::path::PathBuf::from(name),
            size: 100,
            identifier: TestId(name.into()),
            phase_id: dynrunner_core::PhaseId::from("default"),
            type_id: dynrunner_core::TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: name.into(),
            task_depends_on: Vec::new(),
            preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    /// Fake worker that emits exactly one `Response::Done` per
    /// received `ProcessTask` after an artificial delay, then loops.
    ///
    /// The delay (200ms) is the window the test exploits: a
    /// `restart_worker` call landing during that window must abort
    /// the orphan poll_task BEFORE the buffered Done lands on the
    /// pool's event channel. If `abort_poll_task` is not invoked the
    /// orphan event surfaces and the test fails.
    fn spawn_delayed_done_worker(
        spawn_count: Arc<AtomicU32>,
    ) -> impl FnMut(WorkerId) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        move |_worker_id| {
            spawn_count.fetch_add(1, Ordering::SeqCst);
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner: ChannelRunnerEnd = runner_end;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessTask { .. }) => {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            let _ = runner.send(Response::Done { result_data: None }).await;
                        }
                        None => break,
                    }
                }
            });
            Ok((manager_end, None))
        }
    }

    struct DelayedDoneFactory {
        spawn_count: Arc<AtomicU32>,
    }

    impl WorkerFactory<ChannelManagerEnd> for DelayedDoneFactory {
        fn spawn_worker(
            &mut self,
            worker_id: WorkerId,
            _subcgroup: Option<&SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            spawn_delayed_done_worker(self.spawn_count.clone())(worker_id)
        }
    }

    /// Regression pin: `WorkerPool::restart_worker` MUST abort the
    /// orphan `poll_task` of the slot it is replacing. Without the
    /// abort, the spawned poll_loop continues reading from the
    /// (now-doomed) transport and can land a buffered
    /// `WorkerEvent::TaskCompleted` on the pool's shared event
    /// channel after the slot has been cleared — the secondary's
    /// `handle_worker_event` looks up `active_tasks` (already empty
    /// for the killed task) and the event surfaces as `task done
    /// task_hash=None`, exactly the wedge symptom the asm-tokenizer
    /// 80-task repro produced after the first OOM-last-resort kill.
    ///
    /// Setup: assign one task to a worker whose `Response::Done`
    /// is delayed 200ms. Call `restart_worker` during the delay.
    /// Drain the event channel for 400ms after restart — if the
    /// orphan was aborted, NO TaskCompleted from the original
    /// poll_loop arrives. If `abort_poll_task` is missing, the
    /// original Done lands AFTER restart and the channel produces
    /// a stale TaskCompleted that violates the assertion.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_worker_aborts_orphan_poll_task() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let spawn_count = Arc::new(AtomicU32::new(0));
                let mut factory = DelayedDoneFactory {
                    spawn_count: spawn_count.clone(),
                };
                let scheduler = dynrunner_scheduler::ResourceStealingScheduler::memory();
                let max = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]);

                let mut pool: WorkerPool<ChannelManagerEnd, TestId> = WorkerPool::new();
                pool.initialize(1, &max, &scheduler, &mut factory, false, None)
                    .await
                    .expect("pool init");

                // `wait_for_all_ready` (inside `initialize`) drives
                // `poll_ready` inline and does NOT push Ready to the
                // event channel; nothing to drain here.

                // Assign a task that the worker takes 200ms to complete.
                let binary = make_binary("delayed");
                let est = ResourceMap::from([(ResourceKind::memory(), 100)]);
                pool.workers[0]
                    .assign_task(binary, est, false, std::collections::BTreeMap::new())
                    .await
                    .expect("assign");

                // Immediately restart the worker — the in-flight poll_loop
                // is the orphan we expect to be aborted before it can
                // emit its TaskCompleted event.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                pool.restart_worker(0, &mut factory, false)
                    .await
                    .expect("restart");

                // Window long enough for both (a) the orphan's buffered
                // Response::Done (200ms post-assign) AND (b) any
                // post-restart event from the fresh worker (which has
                // no task assigned).
                let drained =
                    tokio::time::timeout(std::time::Duration::from_millis(400), pool.recv_event())
                        .await;

                match drained {
                    Ok(Some(event)) => panic!(
                        "expected no stale event from the orphan poll_task; \
                     got {event:?}. abort_poll_task is missing from \
                     restart_worker."
                    ),
                    Ok(None) => panic!("event channel closed unexpectedly"),
                    Err(_elapsed) => {
                        // Timeout: nothing arrived in the window. The
                        // orphan was aborted as required.
                    }
                }
            })
            .await;
    }

    /// Regression pin: same orphan-abort contract for
    /// `WorkerPool::ensure_worker_for_type` (sync type-shift
    /// respawn). The pool's three worker-replacement lifecycle
    /// paths (`restart_worker`, `ensure_worker_for_type`,
    /// `ensure_worker_for_type_async`) share the orphan-abort
    /// concern; pinning all three independently catches a future
    /// refactor that wires the abort into one but forgets the
    /// others.
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_worker_for_type_aborts_orphan_poll_task() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let spawn_count = Arc::new(AtomicU32::new(0));
                let scheduler = dynrunner_scheduler::ResourceStealingScheduler::memory();
                let max = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]);

                // Factory with `spawn_worker_for_type` overridden to
                // record the requested TypeId so the sync respawn path
                // observes a "true type-shift" against the slot's
                // initial `loaded_type_id = None` and engages the
                // kill+respawn arm of `ensure_worker_for_type`.
                struct TypeShiftFactory {
                    spawn_count: Arc<AtomicU32>,
                }

                impl WorkerFactory<ChannelManagerEnd> for TypeShiftFactory {
                    fn spawn_worker(
                        &mut self,
                        worker_id: WorkerId,
                        _subcgroup: Option<&SubcgroupHandle>,
                    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                        spawn_delayed_done_worker(self.spawn_count.clone())(worker_id)
                    }
                    fn spawn_worker_for_type(
                        &mut self,
                        worker_id: WorkerId,
                        _type_id: &dynrunner_core::TypeId,
                        subcgroup: Option<&SubcgroupHandle>,
                    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                        self.spawn_worker(worker_id, subcgroup)
                    }
                }

                let mut factory = TypeShiftFactory {
                    spawn_count: spawn_count.clone(),
                };

                let mut pool: WorkerPool<ChannelManagerEnd, TestId> = WorkerPool::new();
                pool.initialize(1, &max, &scheduler, &mut factory, false, None)
                    .await
                    .expect("pool init");

                // `wait_for_all_ready` (inside `initialize`) drives
                // `poll_ready` inline and does NOT push Ready to the
                // event channel; nothing to drain here.

                // Assign a task so poll_loop is spawned (the orphan
                // we test the abort against).
                let binary = make_binary("delayed");
                let est = ResourceMap::from([(ResourceKind::memory(), 100)]);
                pool.workers[0]
                    .assign_task(binary, est, false, std::collections::BTreeMap::new())
                    .await
                    .expect("assign");

                // Trigger the sync type-shift respawn arm. The slot's
                // `loaded_type_id` is None at this point so the arm
                // engages (None != Some("shift")).
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let required = dynrunner_core::TypeId::from("shift");
                pool.ensure_worker_for_type(0, &required, &mut factory, false)
                    .await
                    .expect("ensure_worker_for_type");

                let drained =
                    tokio::time::timeout(std::time::Duration::from_millis(400), pool.recv_event())
                        .await;

                match drained {
                    Ok(Some(event)) => panic!(
                        "expected no stale event from the orphan poll_task; \
                     got {event:?}. abort_poll_task is missing from \
                     ensure_worker_for_type."
                    ),
                    Ok(None) => panic!("event channel closed unexpectedly"),
                    Err(_elapsed) => {
                        // Timeout: nothing arrived. Orphan was aborted.
                    }
                }
            })
            .await;
    }

    /// Regression pin: same orphan-abort contract for
    /// `WorkerPool::ensure_worker_for_type_async`. The async variant
    /// emits a Ready event on the channel when the fresh worker
    /// reaches Ready; the test must therefore distinguish "Ready
    /// from the fresh worker" (allowed) from "stale TaskCompleted
    /// from the orphan" (forbidden).
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_worker_for_type_async_aborts_orphan_poll_task() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let spawn_count = Arc::new(AtomicU32::new(0));
                let scheduler = dynrunner_scheduler::ResourceStealingScheduler::memory();
                let max = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]);

                struct TypeShiftFactory {
                    spawn_count: Arc<AtomicU32>,
                }

                impl WorkerFactory<ChannelManagerEnd> for TypeShiftFactory {
                    fn spawn_worker(
                        &mut self,
                        worker_id: WorkerId,
                        _subcgroup: Option<&SubcgroupHandle>,
                    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                        spawn_delayed_done_worker(self.spawn_count.clone())(worker_id)
                    }
                    fn spawn_worker_for_type(
                        &mut self,
                        worker_id: WorkerId,
                        _type_id: &dynrunner_core::TypeId,
                        subcgroup: Option<&SubcgroupHandle>,
                    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                        self.spawn_worker(worker_id, subcgroup)
                    }
                }

                let mut factory = TypeShiftFactory {
                    spawn_count: spawn_count.clone(),
                };

                let mut pool: WorkerPool<ChannelManagerEnd, TestId> = WorkerPool::new();
                pool.initialize(1, &max, &scheduler, &mut factory, false, None)
                    .await
                    .expect("pool init");

                // `wait_for_all_ready` (inside `initialize`) drives
                // `poll_ready` inline and does NOT push Ready to the
                // event channel; nothing to drain here.

                let binary = make_binary("delayed");
                let est = ResourceMap::from([(ResourceKind::memory(), 100)]);
                pool.workers[0]
                    .assign_task(binary, est, false, std::collections::BTreeMap::new())
                    .await
                    .expect("assign");

                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let required = dynrunner_core::TypeId::from("shift");
                let outcome = pool
                    .ensure_worker_for_type_async(0, &required, &mut factory, false)
                    .await
                    .expect("ensure async");
                // The async variant kills the slot and returns
                // RespawnInProgress without waiting for Ready.
                assert!(matches!(outcome, EnsureWorkerOutcome::RespawnInProgress));

                // Drain events for 400ms — long enough for both the
                // orphan's buffered TaskCompleted (forbidden) AND the
                // fresh worker's Ready (allowed). Assert only the
                // allowed shape lands.
                let deadline = std::time::Instant::now() + std::time::Duration::from_millis(400);
                let mut saw_orphan = false;
                while std::time::Instant::now() < deadline {
                    let next = tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        pool.recv_event(),
                    )
                    .await;
                    match next {
                        Ok(Some(WorkerEvent::Ready { .. })) => {
                            // Fresh worker reached Ready — expected.
                        }
                        Ok(Some(other)) => {
                            // Any other event (TaskCompleted /
                            // Disconnected / PhaseUpdate / Keepalive)
                            // arriving with worker_id=0 right after the
                            // respawn is an orphan event.
                            saw_orphan = true;
                            eprintln!("unexpected orphan event: {other:?}");
                        }
                        Ok(None) => panic!("event channel closed unexpectedly"),
                        Err(_) => continue,
                    }
                }
                assert!(
                    !saw_orphan,
                    "saw stale event(s) from the orphan poll_task; \
                 abort_poll_task is missing from ensure_worker_for_type_async."
                );

                // Avoid an unused-import warning in case the only
                // ErrorType ref above is conditional.
                let _ = ErrorType::Recoverable;
            })
            .await;
    }

    /// Fake worker that delays its `Response::Ready` by `ready_delay`
    /// to mimic a slow subprocess startup (the production "build worker
    /// whose archive-import prelude takes >> the keepalive interval").
    /// After Ready it serves `ProcessTask` immediately so the test never
    /// has to wait twice.
    fn spawn_slow_ready_worker(
        ready_delay: std::time::Duration,
    ) -> impl FnMut(WorkerId) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        move |_worker_id| {
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner: ChannelRunnerEnd = runner_end;
                tokio::time::sleep(ready_delay).await;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessTask { .. }) => {
                            let _ = runner.send(Response::Done { result_data: None }).await;
                        }
                        None => break,
                    }
                }
            });
            Ok((manager_end, None))
        }
    }

    struct SlowReadyFactory {
        ready_delay: std::time::Duration,
    }

    impl WorkerFactory<ChannelManagerEnd> for SlowReadyFactory {
        fn spawn_worker(
            &mut self,
            worker_id: WorkerId,
            _subcgroup: Option<&SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            spawn_slow_ready_worker(self.ready_delay)(worker_id)
        }
        fn spawn_worker_for_type(
            &mut self,
            worker_id: WorkerId,
            _type_id: &dynrunner_core::TypeId,
            subcgroup: Option<&SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            self.spawn_worker(worker_id, subcgroup)
        }
    }

    /// Regression pin (keepalive-starvation wedge): when a worker is
    /// restarted on the secondary's operational `select!` loop and the
    /// replacement subprocess is SLOW to report `Response::Ready`,
    /// [`WorkerPool::restart_worker_async`] MUST return WITHOUT waiting
    /// for that Ready — the wait rides a background watcher that later
    /// emits `WorkerEvent::Ready` on the pool's event channel.
    ///
    /// The inline-wait [`WorkerPool::restart_worker`] used to block the
    /// calling task here for the entire slow-startup window. Inside the
    /// secondary's `select!`, that held EVERY other arm open — the
    /// keepalive tick included — so the primary saw a keepalive gap and
    /// declared the busy-but-alive secondary dead. This test pins the
    /// non-blocking contract two ways:
    ///
    ///   1. `restart_worker_async` returns well before the worker's
    ///      `ready_delay` elapses (the calling task is not blocked).
    ///   2. A CONCURRENT ticker `spawn_local`ed alongside the restart
    ///      keeps making progress while the watcher awaits Ready — the
    ///      runtime is cooperatively scheduled, exactly the property the
    ///      keepalive arm needs. (With the old inline wait the
    ///      `restart_worker(...).await` would not yield until Ready, and
    ///      the assertion-after-await would observe a starved ticker.)
    ///
    /// Pre-fix (`restart_worker_async` calling the inline `poll_ready`
    /// loop instead of `spawn_ready_watcher`) the first assertion fails:
    /// the call would not return until `ready_delay` elapsed.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_worker_async_does_not_block_on_slow_ready() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let scheduler = dynrunner_scheduler::ResourceStealingScheduler::memory();
                let max = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]);

                // Worker startup is far longer than any keepalive-cadence
                // tick a real secondary would run (5s in prod; here we use
                // 600ms so the test stays fast while the gap is
                // unambiguous against the 50ms cadence asserted below).
                let ready_delay = std::time::Duration::from_millis(600);
                let mut factory = SlowReadyFactory { ready_delay };

                let mut pool: WorkerPool<ChannelManagerEnd, TestId> = WorkerPool::new();
                pool.initialize(1, &max, &scheduler, &mut factory, false, None)
                    .await
                    .expect("pool init");

                // A concurrent ticker stands in for the keepalive arm:
                // it must keep firing while the restart's watcher awaits
                // the slow worker's Ready. Shared counter read back after
                // the (non-blocking) restart returns.
                let ticks = Arc::new(AtomicU32::new(0));
                let ticks_for_task = ticks.clone();
                let ticker = tokio::task::spawn_local(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        ticks_for_task.fetch_add(1, Ordering::SeqCst);
                    }
                });

                // Restart NON-BLOCKINGLY. This must return promptly — the
                // wait for the slow worker's Ready is pushed onto the
                // background watcher, NOT awaited here.
                let started = std::time::Instant::now();
                pool.restart_worker_async(0, &mut factory, false)
                    .await
                    .expect("restart_worker_async");
                let restart_return_elapsed = started.elapsed();

                assert!(
                    restart_return_elapsed < ready_delay / 2,
                    "restart_worker_async must return WITHOUT waiting for the slow \
                     worker's Ready (the wait belongs on the background watcher); \
                     it returned after {restart_return_elapsed:?} but the worker's \
                     ready_delay is {ready_delay:?} — the calling task was blocked, \
                     which is the keepalive-starvation wedge."
                );

                // Right after the (prompt) return the slot is still
                // Transitioning — the watcher has not yet observed Ready.
                assert!(
                    !pool.workers[0].is_ready(),
                    "slot must be Transitioning immediately after the async restart; \
                     the watcher emits Ready later via the event channel"
                );

                // Now drive the runtime until the watcher emits Ready.
                // The ticker must have advanced across this window —
                // proving the operational loop's other arms keep running
                // while a slow worker (re)spawns.
                let ev = tokio::time::timeout(
                    ready_delay + std::time::Duration::from_millis(400),
                    pool.recv_event(),
                )
                .await
                .expect("watcher must emit a WorkerEvent within the startup window")
                .expect("event channel must stay open");
                assert!(
                    matches!(ev, WorkerEvent::Ready { worker_id: 0 }),
                    "the background watcher must emit Ready for the restarted slot; got {ev:?}"
                );

                ticker.abort();
                assert!(
                    ticks.load(Ordering::SeqCst) >= 1,
                    "the concurrent ticker (keepalive analog) must keep firing while \
                     the slow worker restarts; a starved ticker means the runtime was \
                     blocked on the worker's Ready"
                );
            })
            .await;
    }
}

#[cfg(test)]
mod cgroup_wiring_tests {
    use super::*;
    use crate::manager::WorkerFactory;
    use dynrunner_core::WorkerId;
    use dynrunner_protocol_manager_worker::ManagerEndpoint;

    /// Per-spawn cgroup-wiring contract: the pool either hands the
    /// factory a fresh per-worker `SubcgroupHandle` (when the parent
    /// `NestedCgroupHandle` exists) or `None` (graceful fallback).
    /// The tracker records whether the borrow it observed was
    /// `Some`/`None` so the test can assert against the
    /// `mem_manager_reserved_bytes` knob's arm. The factory deliberately
    /// errors so `initialize` aborts after the first spawn — enough to
    /// observe the cgroup wiring without running real workers.
    struct CgroupCallTracker {
        last_subcgroup_was_some: Option<bool>,
    }

    impl<M: ManagerEndpoint> WorkerFactory<M> for CgroupCallTracker {
        fn spawn_worker(
            &mut self,
            _worker_id: WorkerId,
            subcgroup: Option<&SubcgroupHandle>,
        ) -> Result<(M, Option<u32>), String> {
            self.last_subcgroup_was_some = Some(subcgroup.is_some());
            Err("test factory: never spawns".into())
        }
    }

    /// `WorkerPool::initialize(None)` skips the cgroup setup entirely
    /// and the per-spawn helper returns `None` so the factory sees
    /// no per-worker leaf.
    #[tokio::test]
    async fn initialize_none_skips_cgroup_setup_and_passes_none_to_spawn() {
        use dynrunner_scheduler::ResourceStealingScheduler;
        use dynrunner_transport_channel::ChannelManagerEnd;

        let mut pool: WorkerPool<ChannelManagerEnd, ()> = WorkerPool::new();
        let mut factory = CgroupCallTracker {
            last_subcgroup_was_some: None,
        };
        let scheduler = ResourceStealingScheduler::memory();
        let max = ResourceMap::new();

        // Spawn will error, but the `subcgroup` borrow MUST have been
        // computed BEFORE the spawn call so we can assert on it via the
        // tracker regardless of the spawn outcome.
        let _ = pool
            .initialize(1, &max, &scheduler, &mut factory, false, None)
            .await;

        assert_eq!(factory.last_subcgroup_was_some, Some(false));
        assert!(pool.workers_cgroup().is_none());
    }

    /// `WorkerPool::initialize(Some(reserved))` invokes the cgroup
    /// orchestrator. In CI / dev environments the orchestrator
    /// typically returns `Ok(None)` via one of the three documented
    /// graceful-fallback predicates (no cgroup-v2 leaf, missing
    /// `memory` controller on the leaf, non-writable
    /// `subtree_control`), in which case the per-spawn helper also
    /// returns `None` and the factory sees `None`. If the host happens
    /// to expose a writable cgroup-v2 leaf with the memory controller
    /// delegated, the orchestrator MAY actually materialise the
    /// workers subgroup AND the per-spawn helper MAY successfully
    /// create a `worker-0/` leaf — in which case the factory sees
    /// `Some(_)`. Either way the factory MUST have observed a value
    /// (the tracker's `last_subcgroup_was_some.is_some()`).
    ///
    /// Real I/O failures (rare; mostly hosts where the kernel
    /// rejects a child-cgroup write because the parent has internal
    /// processes) propagate as `Err(_)` from `initialize` BEFORE the
    /// spawn helper runs. The contract for "graceful degrade is not
    /// an error" is exercised by either branch of the
    /// `last_subcgroup_was_some` flag; the I/O-error arm bubbles up
    /// uniformly with any other spawn failure and is acceptable here.
    #[tokio::test]
    async fn initialize_some_invokes_cgroup_orchestrator() {
        use dynrunner_scheduler::ResourceStealingScheduler;
        use dynrunner_transport_channel::ChannelManagerEnd;

        let mut pool: WorkerPool<ChannelManagerEnd, ()> = WorkerPool::new();
        let mut factory = CgroupCallTracker {
            last_subcgroup_was_some: None,
        };
        let scheduler = ResourceStealingScheduler::memory();
        let max = ResourceMap::new();

        let outcome = pool
            .initialize(
                1,
                &max,
                &scheduler,
                &mut factory,
                false,
                Some(500 * 1024 * 1024),
            )
            .await;

        match outcome {
            // Happy path 1: orchestrator returned Ok (Some or None),
            // the per-spawn helper produced a value, and the factory's
            // spawn errored — we observed the borrow on the first
            // spawn.
            Err(msg) if msg.starts_with("failed to spawn worker") => {
                assert!(factory.last_subcgroup_was_some.is_some());
            }
            // Happy path 2: orchestrator's setup OR the per-spawn
            // leaf creation hit a real I/O error (kernel rejected the
            // workers/ mkdir or the worker-0/ mkdir because processes
            // are in the parent leaf, or the controller-delegate
            // write failed). The factory was NOT called because setup
            // failed early. That's the documented "Err on unexpected
            // I/O" contract.
            Err(msg) if msg.contains("cgroup") => {
                assert!(factory.last_subcgroup_was_some.is_none());
            }
            other => panic!("unexpected initialize outcome: {other:?}"),
        }
    }
}
