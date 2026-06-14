//! Manager-side per-worker handle.
//!
//! Owns the per-worker protocol state machine (assigned at
//! construction) plus the bookkeeping the scheduler reads
//! (reservations, current task, opportunistic flag, phase
//! progress). The handle reads from its transport on a background
//! task and sends [`WorkerEvent`]s into the shared manager channel.

use std::collections::BTreeMap;
use std::time::Instant;

use dynrunner_core::{
    ErrorType, Identifier, ResourceMap, TaskInfo, TaskOutputs, TaskResult, TypeId, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_manager_worker::state::{
    AssignResult, Idle, PollResult, Processing, RunnerProtocol, RunnerProtocolState,
    WaitReadyResult,
};
use dynrunner_scheduler_api::WorkerBudgetInfo;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::cgroup::SubcgroupHandle;

use super::event::WorkerEvent;
use super::exit_status::{WorkerExitStatus, try_reap_subprocess};

/// One queued secondary→worker custom message: `(topic, data)`.
pub type CustomOutboxItem = (String, Vec<u8>);

/// What the per-slot background task returns when it resolves: the
/// recovered protocol state plus — for the per-task poll loop — the
/// custom-message outbox receiver it borrowed for the task's
/// lifetime (`None` for the ready-watcher, which never takes it).
type PollTaskOutput<M> = (
    RunnerProtocolState<M>,
    Option<mpsc::UnboundedReceiver<CustomOutboxItem>>,
);

/// Where a restart-time SIGKILL should land.
///
/// The negative-PID idiom `kill(-pgid, …)` signals every process in
/// a group; the positive-PID `kill(pid, …)` signals one process. The
/// variant decides which, encoding the safety rule that the group
/// form is only ever used when the worker LEADS that group (see
/// [`restart_kill_target`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartKillTarget {
    /// Worker is its own process-group leader; signal the whole
    /// group (worker + abandoned build descendants) via `-pgid`.
    Group(i32),
    /// Worker is NOT a group leader (or pgid unknown); signal only
    /// the worker process, never a group it does not lead.
    Process(i32),
}

impl RestartKillTarget {
    /// The argument to pass to `kill(…)`: negative for the group
    /// form, positive for the bare-PID form.
    fn signal_pid(self) -> i32 {
        match self {
            RestartKillTarget::Group(pgid) => -pgid,
            RestartKillTarget::Process(pid) => pid,
        }
    }

    /// Human-readable label for logging.
    fn kind(self) -> &'static str {
        match self {
            RestartKillTarget::Group(_) => "process-group",
            RestartKillTarget::Process(_) => "bare-pid",
        }
    }
}

/// Decide where a restart-time SIGKILL must land, given the worker's
/// PID and its RESOLVED process group (`None` if `getpgid` failed).
///
/// Pure targeting policy — no syscalls — so the decision can be
/// unit-tested without a fork. Group-kill ONLY when the worker is
/// confirmed its own process-group leader (`pgid == worker_pid`);
/// every other case (non-leader, or unresolvable pgid) falls back to
/// the bare-PID kill. The fallback is the safety floor: signalling a
/// group the worker does not lead could SIGKILL the manager's own
/// group.
fn restart_kill_target(worker_pid: i32, resolved_pgid: Option<i32>) -> RestartKillTarget {
    match resolved_pgid {
        Some(pgid) if pgid == worker_pid => RestartKillTarget::Group(worker_pid),
        _ => RestartKillTarget::Process(worker_pid),
    }
}

/// Manager-side handle for one worker.
///
/// Wraps the ZST protocol state machine plus per-worker metadata used by the
/// scheduler (budget, current task, opportunistic flag, etc.).
///
/// When a task is assigned, the protocol is moved into a spawned background
/// task that reads from the transport and sends `WorkerEvent`s to a shared
/// channel. This avoids head-of-line blocking when polling multiple workers.
pub struct WorkerHandle<M: ManagerEndpoint, I: Identifier> {
    pub worker_id: WorkerId,
    /// Monotonic per-slot subprocess generation. Set once at
    /// construction and immutable for the handle's lifetime; the pool's
    /// replacement edges (`replace_worker_slot`,
    /// `ensure_worker_for_type_async`) construct the successor handle
    /// with `predecessor.generation + 1` so each spawned subprocess in a
    /// slot carries a strictly-increasing id.
    ///
    /// Every [`WorkerEvent`] the handle's poll machinery emits is stamped
    /// with this value (captured at poll-task spawn). A consumer that
    /// keeps the slot's CURRENT generation (the live handle's) can reject
    /// any event whose generation is stale — the buffered-terminal a
    /// type-shift respawn leaves behind on the pool's shared channel
    /// (`abort_poll_task` cannot retract an already-sent message) carries
    /// the OLD generation and is dropped rather than mis-attributed to
    /// the fresh subprocess's task.
    pub generation: u64,
    pub reserved_budgets: ResourceMap,
    pub estimated_resources: ResourceMap,
    pub current_binary: Option<TaskInfo<I>>,
    pub opportunistic: bool,
    pub has_initial_assignment: bool,
    pub idle: bool,
    pub actual_usage: ResourceMap,
    /// Swap component of the last [`Self::update_resource_usage`]
    /// reading, in bytes. The memory kind in `actual_usage` already
    /// INCLUDES this (charged = resident + swap — the decision
    /// input); this field exists so observability (the OOM watcher's
    /// structured log) can report the swap share separately.
    pub actual_swap_bytes: u64,
    pub assignment_failure_count: u32,
    pub pid: Option<u32>,
    /// Per-worker cgroup-v2 leaf the pool's spawn site materialised
    /// before handing the borrow to `WorkerFactory::spawn_worker`'s
    /// `pre_exec` for pid-attachment. `None` when the runtime does
    /// not support delegated cgroup-v2 nesting (graceful-fallback
    /// contract on [`crate::cgroup::prepare_worker_subgroup`]).
    ///
    /// Lifetime is the worker's lifetime: dropping the handle (slot
    /// replacement, pool teardown) runs the `SubcgroupHandle::Drop`
    /// best-effort rmdir on the leaf. The [`Self::subcgroup_dir`]
    /// accessor exposes the leaf path to the memprofile sampler so
    /// it can read `memory.current` / `memory.stat` at task-assigned
    /// time.
    pub subcgroup: Option<SubcgroupHandle>,
    /// `TypeId` the worker subprocess was last spawned (or respawned)
    /// for. `None` until the pool has loaded a type into this slot —
    /// typically because the factory's freshly-initialised spawn did
    /// not advertise which `TypeId` its argv corresponds to, and the
    /// first assignment is what binds the slot.
    ///
    /// The pool's `ensure_worker_for_type` compares this against the
    /// next task's `type_id` and, on mismatch, kills + respawns the
    /// subprocess through `WorkerFactory::spawn_worker_for_type`
    /// before the assignment proceeds. The default-impl `WorkerFactory`
    /// flow (test factories that don't distinguish per-type argv)
    /// still records the requested `TypeId` here so the same-type
    /// fast path stays observably correct without any real spawn.
    pub loaded_type_id: Option<TypeId>,
    /// Whether THIS subprocess generation ever reached `Ready`
    /// (protocol observed Idle at least once). Drives the
    /// startup-crash discrimination: a worker that died WITHOUT ever
    /// reaching Ready is a startup death (broken worker image /
    /// module import error — the #370 production shape), and its
    /// slot's next respawn must be backed off
    /// ([`super::super::pool::WorkerPool::restart_backoff_delay`])
    /// instead of retried at loop speed. A worker that HAD reached
    /// Ready keeps the historical immediate-restart semantics.
    pub ever_ready: bool,
    /// Count of consecutive PREDECESSOR subprocesses in this slot that
    /// died without ever reaching Ready. Maintained at the pool's
    /// replacement edges (carried like `assignment_failure_count`):
    /// the successor handle gets `old.startup_crash_streak + 1` when
    /// the replaced generation never reached Ready, and `0` when it
    /// had. Reaching Ready resets it. Drives the exponential respawn
    /// backoff for a startup-crashing worker.
    pub startup_crash_streak: u32,
    /// Current processing phase name (set by PhaseUpdate messages).
    pub phase: Option<String>,
    /// Timestamp of the last keepalive or phase update.
    pub last_keepalive: Option<Instant>,
    /// When the worker entered its current phase. Reset on PhaseUpdate.
    pub phase_started_at: Option<Instant>,
    /// Index of the next stuck-worker interval to fire from
    /// `LocalManagerConfig::phase_status_log_intervals`.
    pub phase_status_log_idx: usize,
    protocol: RunnerProtocolState<M>,
    /// Shared channel for sending worker events to the manager.
    event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    /// Handle to the background poll task (set while Processing).
    poll_task: Option<JoinHandle<PollTaskOutput<M>>>,
    /// Sender half of the per-subprocess custom-message outbox
    /// ([`Self::send_custom`] queues here). One channel per handle —
    /// i.e. per subprocess generation — so customs queued for a dead
    /// subprocess die with its handle instead of leaking onto the
    /// replacement.
    custom_outbox_tx: mpsc::UnboundedSender<CustomOutboxItem>,
    /// Receiver half. Held here while the slot is Idle /
    /// WaitingForReady (the Idle flush in [`Self::send_custom`] and
    /// [`Self::assign_task`] drains it inline through the typed
    /// `RunnerProtocol::send_custom` allowance); moved into the
    /// per-task poll loop while Processing (the loop's biased select
    /// drains it onto the full-duplex socket mid-task) and recovered
    /// by [`Self::reclaim_protocol`].
    custom_outbox_rx: Option<mpsc::UnboundedReceiver<CustomOutboxItem>>,
}

impl<M: ManagerEndpoint + 'static, I: Identifier> WorkerHandle<M, I> {
    pub fn new(
        worker_id: WorkerId,
        generation: u64,
        transport: M,
        event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    ) -> Self {
        let waiting = RunnerProtocol::connect(transport);
        let (custom_outbox_tx, custom_outbox_rx) = mpsc::unbounded_channel();
        Self {
            worker_id,
            generation,
            reserved_budgets: ResourceMap::new(),
            estimated_resources: ResourceMap::new(),
            current_binary: None,
            opportunistic: false,
            has_initial_assignment: false,
            idle: false,
            actual_usage: ResourceMap::new(),
            actual_swap_bytes: 0,
            assignment_failure_count: 0,
            pid: None,
            subcgroup: None,
            loaded_type_id: None,
            ever_ready: false,
            startup_crash_streak: 0,
            phase: None,
            last_keepalive: None,
            phase_started_at: None,
            phase_status_log_idx: 0,
            protocol: RunnerProtocolState::WaitingForReady(waiting),
            event_tx,
            poll_task: None,
            custom_outbox_tx,
            custom_outbox_rx: Some(custom_outbox_rx),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.protocol.is_idle() || self.protocol.is_processing()
    }

    /// `<workers>/worker-<id>/` path the pool's spawn site
    /// materialised for this worker, when a per-worker cgroup-v2
    /// leaf was created. `None` covers the graceful-fallback case
    /// (host does not support delegated cgroup-v2 nesting).
    ///
    /// Surfaced for the memprofile sampler so it can read
    /// `memory.current` / `memory.stat` from the leaf at
    /// task-assigned time; production code in the pool itself does
    /// not need this — the borrow handed to
    /// `WorkerFactory::spawn_worker` carries everything the
    /// `pre_exec` needs for pid-attachment.
    pub fn subcgroup_dir(&self) -> Option<&std::path::Path> {
        self.subcgroup.as_ref().map(|h| h.cgroup_dir())
    }

    /// Reap the worker subprocess if the framework tracks its PID, and
    /// return its exit disposition. Callers invoke this after observing
    /// a transport-level disconnect (pipe EOF, send-failure) to
    /// discriminate clean-exit vs signal-kill in the manager log.
    ///
    /// Returns `None` when the PID is not tracked, the kernel has not
    /// yet reaped the child (SIGCHLD race), or the child was already
    /// reaped by another path (factory `Child` drop). See
    /// `try_reap_subprocess` for the full set of `None` conditions.
    ///
    /// Non-blocking. Safe to call from the dispatcher's event loop.
    pub fn try_reap_exit(&self) -> Option<WorkerExitStatus> {
        try_reap_subprocess(self.pid)
    }

    /// Fallback reap for the SIGCHLD-race window: when
    /// [`Self::try_reap_exit`] returned `None` for a child the caller
    /// KNOWS is dead (pipe EOF observed), schedule the bounded detached
    /// reaper so the child cannot linger as a zombie just because the
    /// kernel had not finalised it inside the non-blocking retry
    /// budget. No-op without a tracked PID; idempotent against any
    /// other reap path (`ECHILD` exits silently).
    pub fn reap_detached_fallback(&self) {
        if let Some(pid) = self.pid {
            super::exit_status::reap_detached(self.worker_id, pid);
        }
    }

    /// Actively SIGKILL the worker BEFORE its replacement comes up,
    /// rather than relying on the worker to notice EOF on its
    /// transport and exit on its own. Used by the secondary's
    /// restart / type-shift respawn path. Idempotent on absence:
    /// no-op if `pid` is None, the kernel already reaped, or the
    /// process is otherwise gone (ESRCH).
    ///
    /// SIGKILL (not SIGTERM) is intentional here: by the time
    /// this is called, the framework has already decided the
    /// worker is going to be replaced. SIGTERM would invite the
    /// worker's signal handler (which translates SIGTERM into a
    /// `SystemExit("signal 15")` per `runtime.py::_install_term_handler`)
    /// to enter a graceful-exit code path that's slower than
    /// the manager wants to wait. SIGKILL is the "no graceful
    /// shutdown" lever.
    ///
    /// Targets the worker's process GROUP, not just its bare PID,
    /// so the abandoned in-flight build descendant tree (e.g.
    /// `make` → `clang`, spawned under the worker's group) dies
    /// WITH the worker instead of reparenting to PID 1 and
    /// orphaning on every restart. No grace period: we are
    /// abandoning the task, so the build children needn't finish —
    /// this preserves the fast-restart intent, just signalling the
    /// group instead of the lone leader.
    ///
    /// SAFETY: `kill(-pgid, …)` signals the WHOLE group, which is
    /// safe ONLY when the worker is its OWN process-group leader
    /// (the factory contract — `Command::process_group(0)` /
    /// `Popen(start_new_session=True)` — establishes that). A
    /// worker spawned WITHOUT becoming a group leader sits in the
    /// MANAGER's group, so group-signalling its real pgid would
    /// SIGKILL the manager itself. We therefore resolve the real
    /// pgid and only group-kill when `pgid == worker_pid`; any
    /// other case (non-leader, or `getpgid` failure) falls back to
    /// the legacy bare-PID kill. See [`restart_kill_target`].
    #[cfg(unix)]
    pub fn kill_subprocess(&self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::{Pid, getpgid};
        let Some(pid_raw) = self.pid else {
            return;
        };
        let worker_pid = pid_raw as i32;

        // Resolve the worker's REAL process group so we never
        // signal a group the worker does not lead.
        let resolved_pgid = getpgid(Some(Pid::from_raw(worker_pid)))
            .ok()
            .map(|p| p.as_raw());
        let target = restart_kill_target(worker_pid, resolved_pgid);
        if matches!(target, RestartKillTarget::Process(_)) {
            tracing::debug!(
                worker_id = self.worker_id,
                pid = worker_pid,
                resolved_pgid,
                "worker: restart-kill falling back to bare-PID SIGKILL \
                 (worker is not its own process-group leader)"
            );
        }

        match kill(Pid::from_raw(target.signal_pid()), Signal::SIGKILL) {
            Ok(()) => {
                tracing::debug!(
                    worker_id = self.worker_id,
                    pid = worker_pid,
                    target = %target.kind(),
                    "worker: sent SIGKILL before restart"
                );
                // The kill edge OWNS the reap of the worker LEADER:
                // nothing downstream waits this child (the
                // replacement aborts its poll task, so no
                // Disconnected event — and therefore no
                // `try_reap_exit` — ever fires for it). Without
                // this, every type-shift/restart kill leaks a
                // zombie for the life of the manager process (the
                // #370 procfs shape). The group's descendants are
                // not our children, so init reaps them once they
                // die. Detached + bounded, off the runtime thread.
                super::exit_status::reap_detached(self.worker_id, pid_raw);
            }
            Err(nix::errno::Errno::ESRCH) => {
                // Already gone — kernel reaped or process exited
                // on its own. Either way, the goal ("worker is
                // dead before restart") is satisfied.
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = self.worker_id,
                    pid = worker_pid,
                    error = %e,
                    "worker: SIGKILL pre-restart failed; \
                     proceeding with restart anyway"
                );
            }
        }
    }

    #[cfg(not(unix))]
    pub fn kill_subprocess(&self) {}

    /// Send SIGTERM to the worker's entire process group.
    ///
    /// The signed-negative-PID idiom (`kill(-pgid, …)`) delivers the
    /// signal to every process in that group — the worker itself
    /// plus every child it spawned (subprocess pools, helper
    /// processes, etc.). For this to do the right thing the worker
    /// must have been started as its own process-group leader,
    /// which is the contract the factory layer establishes via
    /// `Command::process_group(0)` at spawn time. Workers spawned
    /// by external `WorkerFactory` implementations that don't
    /// enforce that (e.g. the PyO3 `PyCallbackWorkerFactory`,
    /// which delegates spawn to Python) are expected to apply the
    /// equivalent `subprocess.Popen(start_new_session=True)` on
    /// their side; absent that, this SIGTERM only reaches the
    /// worker process itself, matching the legacy
    /// `kill_subprocess` semantic — best-effort, never worse than
    /// the pre-tree-kill behaviour.
    ///
    /// Idempotent on absence: no-op if `pid` is None or the kernel
    /// has already reaped the group (ESRCH).
    ///
    /// Distinct from `kill_subprocess`: that path sends SIGKILL
    /// only to the worker process for the restart-pre-respawn
    /// flow (the worker is going to be replaced; no grace
    /// period). The tree-kill path is the panik / emergency-stop
    /// lever where we DO want the worker's children to receive a
    /// chance to clean up (SIGTERM-first; the pool's
    /// grace-then-SIGKILL escalation lives on `WorkerPool`).
    #[cfg(unix)]
    pub fn sigterm_process_tree(&self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        let Some(pid) = self.pid else {
            return;
        };
        let pgid = Pid::from_raw(-(pid as i32));
        match kill(pgid, Signal::SIGTERM) {
            Ok(()) => {
                tracing::debug!(
                    worker_id = self.worker_id,
                    pgid = pid,
                    "worker: sent SIGTERM to process group"
                );
            }
            Err(nix::errno::Errno::ESRCH) => {
                // Group already gone — kernel reaped the leader
                // and every descendant inherited its termination.
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = self.worker_id,
                    pgid = pid,
                    error = %e,
                    "worker: SIGTERM to process group failed"
                );
            }
        }
    }

    #[cfg(not(unix))]
    pub fn sigterm_process_tree(&self) {}

    /// Send SIGKILL to the worker's entire process group.
    ///
    /// Used as the escalation step after a `sigterm_process_tree`
    /// plus grace-period wait did not bring the group down. Same
    /// negative-pgid idiom as the SIGTERM path; same factory-side
    /// contract about process-group leadership applies. Idempotent
    /// on absence.
    #[cfg(unix)]
    pub fn sigkill_process_tree(&self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        let Some(pid) = self.pid else {
            return;
        };
        let pgid = Pid::from_raw(-(pid as i32));
        match kill(pgid, Signal::SIGKILL) {
            Ok(()) => {
                tracing::debug!(
                    worker_id = self.worker_id,
                    pgid = pid,
                    "worker: sent SIGKILL to process group"
                );
            }
            Err(nix::errno::Errno::ESRCH) => {
                // Group already gone — clean grace-period exit.
            }
            Err(e) => {
                tracing::warn!(
                    worker_id = self.worker_id,
                    pgid = pid,
                    error = %e,
                    "worker: SIGKILL to process group failed"
                );
            }
        }
    }

    #[cfg(not(unix))]
    pub fn sigkill_process_tree(&self) {}

    /// Probe whether the worker's process group still has at
    /// least one live member, by sending signal 0 to the
    /// negative pgid. `kill(-pgid, 0)` returns Ok iff the group
    /// contains at least one process that the caller is
    /// permitted to signal; ESRCH means the entire group has
    /// terminated (i.e. SIGKILL succeeded or the group exited
    /// on its own).
    ///
    /// Used by the pool's grace-then-SIGKILL escalation to
    /// decide whether the escalation is actually needed.
    #[cfg(unix)]
    pub fn process_tree_alive(&self) -> bool {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        let Some(pid) = self.pid else {
            return false;
        };
        let pgid = Pid::from_raw(-(pid as i32));
        match kill(pgid, None) {
            Ok(()) => true,
            Err(nix::errno::Errno::ESRCH) => false,
            Err(_) => false,
        }
    }

    #[cfg(not(unix))]
    pub fn process_tree_alive(&self) -> bool {
        false
    }

    pub fn is_idle_state(&self) -> bool {
        self.protocol.is_idle()
    }

    /// True iff THIS subprocess generation died without ever reaching
    /// `Ready`: the protocol settled in `Stopped`/`Unconnected` while
    /// `ever_ready` is still false. A startup-dead slot can never
    /// become ready by polling — `poll_ready` has nothing left to
    /// drive — so ready-wait loops must treat it as SETTLED (and let
    /// the restart machinery own recovery, on the startup-crash
    /// backoff) instead of spinning on it forever (the #370 busy-loop
    /// shape).
    pub fn is_startup_dead(&self) -> bool {
        !self.ever_ready
            && matches!(
                self.protocol,
                RunnerProtocolState::Stopped(_) | RunnerProtocolState::Unconnected
            )
    }

    pub fn is_processing(&self) -> bool {
        // Transitioning means the protocol is in a spawned poll task
        self.protocol.is_processing() || self.poll_task.is_some()
    }

    pub fn is_stopped(&self) -> bool {
        self.protocol.is_stopped()
    }

    /// Build a snapshot for the scheduler.
    pub fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.reserved_budgets.clone(),
            actual_usage: self.actual_usage.clone(),
            is_idle: self.idle && self.current_binary.is_none(),
            is_opportunistic: self.opportunistic,
            has_initial_assignment: self.has_initial_assignment,
            current_task: self.current_binary.clone(),
            estimated_usage: self.estimated_resources.clone(),
        }
    }

    /// Try to advance from WaitingForReady → Idle.
    pub async fn poll_ready(&mut self) -> Option<WorkerEvent<I>> {
        let waiting = self.protocol.take_waiting()?;
        match waiting.wait_ready().await {
            WaitReadyResult::Ready(idle) => {
                self.protocol = RunnerProtocolState::Idle(idle);
                self.idle = true;
                // This generation reached Ready: it is not a startup
                // death, and the slot's startup-crash streak ends here.
                self.ever_ready = true;
                self.startup_crash_streak = 0;
                Some(WorkerEvent::Ready {
                    worker_id: self.worker_id,
                    generation: self.generation,
                })
            }
            WaitReadyResult::NotYet(w) => {
                self.protocol = RunnerProtocolState::WaitingForReady(w);
                None
            }
            WaitReadyResult::Disconnected(s) => {
                self.protocol = RunnerProtocolState::Stopped(s);
                Some(WorkerEvent::Disconnected {
                    worker_id: self.worker_id,
                    generation: self.generation,
                    // Phase D: framework can't tell from a closed
                    // transport whether the worker process died from
                    // a deterministic bug or from an environment
                    // glitch (OOM-killer, host hiccup). Recoverable
                    // is the safe default — repeated Recoverable
                    // classifications still surface as a permanent
                    // failure once `MAX_RETRY_ATTEMPTS` passes are
                    // exhausted.
                    result: TaskResult::error(
                        ErrorType::Recoverable,
                        "Disconnected before Ready".into(),
                    ),
                    binary: None,
                })
            }
        }
    }

    /// Spawn a background task that drives `wait_ready` to completion
    /// and emits the resulting [`WorkerEvent`] to the shared event
    /// channel.
    ///
    /// # Single concern
    ///
    /// Replace the synchronous poll-loop callers (which block the
    /// owning task while a freshly-spawned worker subprocess takes
    /// arbitrary time to send `Response::Ready`) with the same
    /// event-driven primitive `assign_task` already uses for the
    /// per-task poll loop. The protocol moves into the spawned task;
    /// the JoinHandle is stashed in `poll_task` so
    /// [`reclaim_protocol`] can recover the resulting state when the
    /// terminal event lands.
    ///
    /// # Boundary
    ///
    /// Returns `Ok(())` if the watcher was spawned (the worker was in
    /// `WaitingForReady`); `Err(_)` if the worker is not in that
    /// state — a programmer-error contract violation the caller is
    /// responsible for not triggering. The pool's
    /// `ensure_worker_for_type` is the sole production caller and
    /// constructs the handle in `WaitingForReady` immediately before
    /// the call, satisfying the contract.
    ///
    /// # Wedge prevention (production-bug pin)
    ///
    /// Before this method existed, [`super::super::pool::WorkerPool::ensure_worker_for_type`]
    /// drove `wait_ready` synchronously inside the secondary's
    /// `select!`-driven operational loop. A new worker subprocess
    /// that took 300+ seconds to send `Response::Ready` (e.g. wedged
    /// at Python import time, or a fork that legitimately needed
    /// longer than the keepalive interval to make progress) froze the
    /// entire tokio runtime: no keepalive ticks, no router events, no
    /// worker activity. The primary's keepalive_timeout (300s) was
    /// the only thing that woke things up. By emitting the Ready /
    /// Disconnected event through the existing channel, the
    /// operational loop's other arms keep running and the
    /// post-Ready repoll is wired through the standard
    /// `handle_worker_event` path.
    pub fn spawn_ready_watcher(&mut self) -> Result<(), String> {
        let waiting = self
            .protocol
            .take_waiting()
            .ok_or_else(|| "spawn_ready_watcher requires WaitingForReady state".to_string())?;
        let worker_id = self.worker_id;
        // Capture the slot's generation for the spawned task: immutable
        // for the task's lifetime, so every event it emits carries the
        // generation of the subprocess it is watching.
        let generation = self.generation;
        let tx = self.event_tx.clone();
        let handle = tokio::task::spawn_local(async move {
            let state = match waiting.wait_ready().await {
                WaitReadyResult::Ready(idle) => {
                    // Send first so the operational loop can react
                    // immediately on the next select! iteration even
                    // before `reclaim_protocol` runs. The returned
                    // protocol state is the manager-side handle the
                    // reclaim path will re-install.
                    let _ = tx.send(WorkerEvent::Ready {
                        worker_id,
                        generation,
                    });
                    RunnerProtocolState::Idle(idle)
                }
                WaitReadyResult::NotYet(_) => {
                    // `wait_ready` is documented to return Ready,
                    // Disconnected, or NotYet (the latter only on a
                    // non-Ready response — e.g. the worker sent a
                    // PhaseUpdate before Ready, which the protocol
                    // discards in `wait_ready`'s match arm). For the
                    // background-task shape we have to fold NotYet
                    // into "still waiting". Mark the slot Stopped so
                    // the operational loop's restart machinery can
                    // observe and recover; emit Disconnected as the
                    // wire-level shape for "worker never reached
                    // Ready". This case is genuinely rare and the
                    // alternative (looping back into wait_ready) would
                    // resurrect the wedge this method exists to
                    // prevent — bounded recovery via the restart path
                    // is the correct fail-safe.
                    let _ = tx.send(WorkerEvent::Disconnected {
                        worker_id,
                        generation,
                        result: TaskResult::error(
                            ErrorType::Recoverable,
                            "Worker emitted non-Ready response \
                             before Ready; treating as disconnect"
                                .into(),
                        ),
                        binary: None,
                    });
                    RunnerProtocolState::Unconnected
                }
                WaitReadyResult::Disconnected(stopped) => {
                    let _ = tx.send(WorkerEvent::Disconnected {
                        worker_id,
                        generation,
                        result: TaskResult::error(
                            ErrorType::Recoverable,
                            "Disconnected before Ready".into(),
                        ),
                        binary: None,
                    });
                    RunnerProtocolState::Stopped(stopped)
                }
            };
            // The ready-watcher never borrows the custom outbox.
            (state, None)
        });
        self.poll_task = Some(handle);
        // Protocol is now owned by the spawned task; mark as
        // Transitioning. `is_ready()` returns false in this state
        // (protocol.is_idle / is_processing both false) so the
        // dispatch arm correctly treats the slot as "not yet
        // assignable" until the Ready event arrives and
        // `reclaim_protocol` installs the Idle protocol.
        self.protocol = RunnerProtocolState::Transitioning;
        Ok(())
    }

    /// Queue a secondary→worker custom message for this subprocess.
    ///
    /// Single chokepoint for the `Command::Custom` egress on a slot:
    /// the message lands on the per-subprocess outbox and is
    /// delivered through whichever typed `RunnerProtocol::send_custom`
    /// allowance currently applies —
    ///   * slot Idle → flushed inline here, immediately;
    ///   * slot Processing → the per-task poll loop's biased select
    ///     drains it onto the full-duplex socket mid-task;
    ///   * slot WaitingForReady / Transitioning → queued; flushed at
    ///     the next assign (BEFORE the ProcessTask frame) or the next
    ///     Idle-time send.
    ///
    /// The outbox is per-handle (per subprocess generation): customs
    /// queued for a dead subprocess die with its replaced handle —
    /// the secondary-side stale-generation semantics, applied to the
    /// reply direction.
    ///
    /// Size enforcement lives at the API call sites
    /// (`SecondaryHandle.send_to_worker` / `Task.send_message`); this
    /// chokepoint additionally rejects over-limit payloads
    /// defensively so no internal caller can bypass the contract.
    pub async fn send_custom(&mut self, topic: String, data: Vec<u8>) -> Result<(), String> {
        let limit = dynrunner_protocol_manager_worker::CUSTOM_MESSAGE_MAX_BYTES;
        if data.len() > limit {
            return Err(format!(
                "custom message payload is {} bytes, exceeding the \
                 CUSTOM_MESSAGE_MAX_BYTES limit of {} bytes",
                data.len(),
                limit
            ));
        }
        self.custom_outbox_tx
            .send((topic, data))
            .map_err(|_| "custom outbox closed (worker slot torn down)".to_string())?;
        // Idle slots have no poll task draining the outbox — flush
        // inline through the typed Idle allowance.
        if let Some(mut idle) = self.protocol.take_idle() {
            match self.flush_customs_through(&mut idle).await {
                Ok(()) => {
                    self.protocol = RunnerProtocolState::Idle(idle);
                }
                Err(e) => {
                    // Dead transport: same disposition as a failed
                    // assign send — Stopped, caller restarts.
                    self.protocol = RunnerProtocolState::Stopped(idle.stop().await);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Drain every queued custom message through an Idle protocol.
    /// Shared by the Idle-time [`Self::send_custom`] flush and the
    /// pre-dispatch flush in [`Self::assign_task`].
    async fn flush_customs_through(
        &mut self,
        idle: &mut RunnerProtocol<Idle, M>,
    ) -> Result<(), String> {
        let rx = self
            .custom_outbox_rx
            .as_mut()
            .expect("custom_outbox_rx present whenever the slot is Idle");
        while let Ok((topic, data)) = rx.try_recv() {
            idle.send_custom(topic, data).await?;
        }
        Ok(())
    }

    /// Assign a task to this worker. Transitions Idle → Processing.
    ///
    /// Spawns a background task that reads from the transport and sends
    /// `WorkerEvent`s to the shared event channel. The manager receives
    /// events for all workers from a single channel without blocking.
    ///
    /// `predecessor_outputs` is the dispatch-time-assembled map from
    /// each declared `task_depends_on` predecessor's `task_id` to its
    /// cached [`TaskOutputs`] (gathered by the manager via
    /// [`dynrunner_core::gather_predecessor_outputs`]). Forwarded
    /// verbatim through the protocol layer onto
    /// `Command::ProcessTask.predecessor_outputs`. Pass
    /// `BTreeMap::new()` for tasks with no deps; legacy tasks
    /// continue to ride the bare-path codec form.
    pub async fn assign_task(
        &mut self,
        binary: TaskInfo<I>,
        estimated_resources: ResourceMap,
        opportunistic: bool,
        predecessor_outputs: BTreeMap<String, TaskOutputs>,
    ) -> Result<(), String> {
        let mut idle = self
            .protocol
            .take_idle()
            .ok_or_else(|| "worker not in Idle state".to_string())?;

        // Flush any queued custom messages BEFORE the ProcessTask
        // frame so a custom queued while the slot was between tasks
        // reaches the worker's pre-task read (the `@message_handler`
        // delivery point) instead of being reordered after the
        // dispatch. Errors surface like an assign-send failure: the
        // transport is dead, the slot goes Stopped.
        if let Err(error) = self.flush_customs_through(&mut idle).await {
            // Same disposition as AssignResult::SendFailed: the
            // transport is dead, the slot goes Stopped and the caller
            // routes it through the standard restart machinery.
            self.protocol = RunnerProtocolState::Stopped(idle.stop().await);
            return Err(error);
        }

        let relative_path = binary.path.to_string_lossy().into_owned();
        // Forward the consumer's per-task payload (TaskInfo.payload,
        // an opaque JSON value) to the worker as a serialised
        // string. Null payloads ride the legacy single-line wire
        // path; non-null payloads are wrapped per FR-3 so the
        // worker can read them without an additional filesystem
        // hop.
        let payload = if binary.payload.is_null() {
            None
        } else {
            Some(binary.payload.to_string())
        };
        // Forward the secondary's locally-resolved on-disk location
        // when the file lives outside the worker's configured source
        // dir (extraction-cache hit / pre-staged shared mount). The
        // worker uses this verbatim to open the binary while still
        // seeing `relative_path` as the wire-supplied identifier for
        // output-tree mirroring. `None` here keeps the legacy
        // worker behaviour: open `relative_path` against the
        // configured source dir.
        let resolved_path = binary
            .resolved_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());
        match idle
            .assign_task(relative_path, payload, resolved_path, predecessor_outputs)
            .await
        {
            AssignResult::Assigned(processing) => {
                // Spawn a background task that polls the worker protocol.
                let worker_id = self.worker_id;
                // Capture the slot's generation: immutable for the
                // poll task's lifetime, so every event the poll loop
                // emits (including a terminal buffered past a
                // replacement) carries this subprocess's generation.
                let generation = self.generation;
                let binary_clone = binary.clone();
                let tx = self.event_tx.clone();

                let est_clone = estimated_resources.clone();
                // Move the custom-outbox receiver into the poll task
                // for the task's lifetime: the loop's biased select
                // drains queued secondary→worker customs onto the
                // full-duplex socket while the worker runs.
                // `reclaim_protocol` recovers it at the terminal.
                // The expect is sound: the rx is absent ONLY while a
                // poll task holds it, and the slot can't be Idle then.
                let custom_rx = self
                    .custom_outbox_rx
                    .take()
                    .expect("custom_outbox_rx present whenever the slot is Idle");
                let handle = tokio::task::spawn_local(async move {
                    Self::poll_loop(
                        processing,
                        worker_id,
                        generation,
                        binary_clone,
                        est_clone,
                        tx,
                        custom_rx,
                    )
                    .await
                });

                self.poll_task = Some(handle);
                // Protocol is now owned by the spawned task; mark as Transitioning
                self.protocol = RunnerProtocolState::Transitioning;
                self.current_binary = Some(binary);
                self.estimated_resources = estimated_resources;
                self.opportunistic = opportunistic;
                self.has_initial_assignment = true;
                self.idle = false;
                self.assignment_failure_count = 0;
                Ok(())
            }
            AssignResult::SendFailed { error, protocol } => {
                self.protocol = RunnerProtocolState::Stopped(protocol);
                Err(error)
            }
        }
    }

    /// Background poll loop: reads responses from the transport, sends events
    /// to the shared channel, returns the final protocol state (plus the
    /// custom-outbox receiver it borrowed for the task's lifetime).
    #[allow(clippy::too_many_arguments)] // per-task poll context; see assign_task
    async fn poll_loop(
        mut processing: RunnerProtocol<Processing, M>,
        worker_id: WorkerId,
        generation: u64,
        binary: TaskInfo<I>,
        estimated_resources: ResourceMap,
        tx: mpsc::UnboundedSender<WorkerEvent<I>>,
        mut custom_rx: mpsc::UnboundedReceiver<CustomOutboxItem>,
    ) -> PollTaskOutput<M> {
        loop {
            match processing.poll_status_with_custom_outbox(&mut custom_rx).await {
                PollResult::Completed {
                    result,
                    result_data,
                    protocol,
                } => {
                    let _ = tx.send(WorkerEvent::TaskCompleted {
                        worker_id,
                        generation,
                        result,
                        result_data,
                        binary: Some(binary),
                        estimated_resources,
                    });
                    return (RunnerProtocolState::Idle(protocol), Some(custom_rx));
                }
                PollResult::StillRunning {
                    protocol,
                    phase_update,
                    got_keepalive,
                    custom_message,
                } => {
                    processing = protocol;
                    if let Some(phase) = phase_update {
                        let _ = tx.send(WorkerEvent::PhaseUpdate {
                            worker_id,
                            generation,
                            phase_name: phase,
                        });
                    } else if got_keepalive {
                        let _ = tx.send(WorkerEvent::Keepalive {
                            worker_id,
                            generation,
                        });
                    } else if let Some((topic, data)) = custom_message {
                        let _ = tx.send(WorkerEvent::CustomMessage {
                            worker_id,
                            generation,
                            topic,
                            data,
                        });
                    }
                    // Loop to read the next response
                }
                PollResult::Disconnected { result, protocol } => {
                    let _ = tx.send(WorkerEvent::Disconnected {
                        worker_id,
                        generation,
                        result,
                        binary: Some(binary),
                    });
                    return (RunnerProtocolState::Stopped(protocol), Some(custom_rx));
                }
            }
        }
    }

    /// Reclaim the protocol state from the background poll task after a
    /// terminal event (TaskCompleted or Disconnected) has been received.
    ///
    /// Must be called after receiving a terminal WorkerEvent for this worker.
    pub async fn reclaim_protocol(&mut self) {
        if let Some(handle) = self.poll_task.take() {
            match handle.await {
                Ok((state, custom_rx)) => {
                    // An Idle protocol coming back from the background
                    // task proves this generation reached Ready (the
                    // ready-watcher's Ready path, or a per-task poll
                    // loop's Completed — which implies an earlier
                    // Ready). Ends any startup-crash streak.
                    if state.is_idle() {
                        self.ever_ready = true;
                        self.startup_crash_streak = 0;
                    }
                    self.protocol = state;
                    // Recover the custom-outbox receiver the per-task
                    // poll loop borrowed (None from the ready-watcher,
                    // which never takes it — keep the existing rx).
                    if let Some(rx) = custom_rx {
                        self.custom_outbox_rx = Some(rx);
                    }
                }
                Err(e) => {
                    tracing::error!(
                        worker_id = self.worker_id,
                        error = %e,
                        "poll task panicked"
                    );
                    // Can't recover the transport — mark as stopped with a
                    // placeholder. The manager should restart this worker.
                    self.protocol = RunnerProtocolState::Unconnected;
                }
            }
        }
    }

    /// Send Stop and transition to Stopped.
    pub async fn stop(&mut self) {
        if let Some(idle) = self.protocol.take_idle() {
            let stopped = idle.stop().await;
            self.protocol = RunnerProtocolState::Stopped(stopped);
        }
    }

    /// Abort the background poll task (if any) so it stops emitting
    /// [`WorkerEvent`]s on the pool's shared event channel.
    ///
    /// # Single concern
    ///
    /// The slot is about to be replaced (OOM-restart, type-shift
    /// respawn): the manager is moving on, the prior subprocess is
    /// dead or being killed, and any event the orphan `poll_task`
    /// might still emit (a buffered `Response::Completed` read from
    /// the closing pipe, a `Disconnected` synthesised from pipe-EOF
    /// after `kill_subprocess`) would land on the pool's `event_tx`
    /// with the original `worker_id`.
    ///
    /// `JoinHandle::abort` is BEST-EFFORT: it cancels the spawned
    /// task at its NEXT await point — it CANNOT retract a terminal
    /// the resolved `poll_status` already `tx.send`'d (the send is
    /// synchronous, with no await between resolve and send). Such a
    /// buffered stale terminal survives the abort and carries the OLD
    /// generation; it is neutralized at the CONSUMER by the generation
    /// gate (`WorkerPool::is_stale_event` — every event is stamped
    /// with the emitting subprocess's [`Self::generation`], and the
    /// replacement edge bumps the slot's generation, so the gate drops
    /// the stale event instead of processing it against the fresh
    /// slot's bindings). The protocol state the task was driving is
    /// forfeited (the slot is being replaced anyway). The
    /// `WorkerHandle` itself is unchanged from the caller's
    /// perspective; the pool's replacement code follows with the
    /// usual `kill_subprocess` + new-handle assignment.
    ///
    /// Idempotent: no-op when `poll_task` is `None` (the slot was
    /// not in a Transitioning state, or `reclaim_protocol` already
    /// took the handle).
    pub fn abort_poll_task(&mut self) {
        if let Some(handle) = self.poll_task.take() {
            handle.abort();
        }
    }

    /// Clear current task metadata (after completion or OOM kill).
    pub fn clear_task(&mut self) {
        self.current_binary = None;
        self.estimated_resources = ResourceMap::new();
        self.idle = true;
        self.phase = None;
        self.last_keepalive = None;
        self.phase_started_at = None;
        self.phase_status_log_idx = 0;
    }

    /// Mark this worker as OOM-killed: clear task, mark opportunistic.
    pub fn mark_oom_killed(&mut self) {
        self.current_binary = None;
        self.estimated_resources = ResourceMap::new();
        self.opportunistic = true;
    }

    /// Write a previously-measured [`MemoryCharge`](crate::monitor::MemoryCharge)
    /// (resident + swap; cgroup-leaf-first, `/proc` fallback — see
    /// [`crate::monitor::measure_worker_charge`]) into this slot's
    /// accounting. The memory kind in `actual_usage` carries the
    /// CHARGED bytes so the scheduler's pressure decision sees swap
    /// growth as pressure; the swap component is kept separately for
    /// observability ([`Self::actual_swap_bytes`]).
    ///
    /// The charge is read OFF the async runtime by the self-paced OOM
    /// sweep ([`crate::oom::ChargeSweepInputs::read`]); this slot-write
    /// half runs on the loop with the `&mut pool` borrow, so the
    /// blocking file IO never lands on the operational `select!`.
    pub fn set_memory_charge(&mut self, charge: crate::monitor::MemoryCharge) {
        self.actual_swap_bytes = charge.swap_bytes;
        self.actual_usage = charge.to_resource_map();
    }
}

#[cfg(all(test, unix))]
mod restart_kill_tests {
    use super::{RestartKillTarget, restart_kill_target};

    // ---- Pure targeting policy (no syscalls; always run) ----

    #[test]
    fn leader_targets_its_own_group() {
        // Worker is its own group leader (pgid == pid) → group-kill
        // the whole abandoned tree via the negative pgid.
        let t = restart_kill_target(4242, Some(4242));
        assert_eq!(t, RestartKillTarget::Group(4242));
        assert_eq!(t.signal_pid(), -4242, "group form must signal -pgid");
        assert_eq!(t.kind(), "process-group");
    }

    #[test]
    fn non_leader_falls_back_to_bare_pid() {
        // Worker sits in some OTHER group (e.g. the manager's) →
        // NEVER signal that foreign group; bare-PID fallback only.
        let t = restart_kill_target(4242, Some(100));
        assert_eq!(t, RestartKillTarget::Process(4242));
        assert_eq!(t.signal_pid(), 4242, "bare form must signal +pid");
        assert_eq!(t.kind(), "bare-pid");
    }

    #[test]
    fn unresolvable_pgid_falls_back_to_bare_pid() {
        // getpgid failed (ESRCH / etc.) → we cannot prove leadership,
        // so we must NOT group-kill. Bare-PID fallback.
        let t = restart_kill_target(4242, None);
        assert_eq!(t, RestartKillTarget::Process(4242));
        assert_eq!(t.signal_pid(), 4242);
    }

    // ---- Effectful proof: group-kill reaps the descendant tree ----

    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::sys::wait::{WaitStatus, waitpid};
    use nix::unistd::{ForkResult, Pid, getpgid, getpid, setsid};
    use std::time::{Duration, Instant};

    /// Poll `kill(pid, 0)` until the process is gone (ESRCH) or the
    /// deadline elapses. Returns true iff the process is gone. Used
    /// to observe a NON-child (the grandchild, reparented to init)
    /// disappearing, which `waitpid` cannot do for a non-child.
    fn wait_gone(pid: Pid, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            match kill(pid, None) {
                Err(Errno::ESRCH) => return true,
                _ => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
    }

    /// A leader worker's group-kill must reap the WHOLE tree: the
    /// worker (group leader) AND its grandchild build descendant —
    /// the grandchild must NOT survive as an orphan. This replays
    /// the #500 shape: a worker that is its own group leader with an
    /// in-flight descendant (stand-in for `make`→`clang`).
    #[test]
    fn group_kill_reaps_descendant_no_orphan() {
        // Channel the grandchild PID back to the parent over a pipe;
        // the parent needs it to observe the grandchild dying.
        let (read_fd, write_fd) = nix::unistd::pipe().expect("pipe");

        // SAFETY: between fork() and exec/_exit in the child we use
        // only async-signal-safe calls (setsid, fork, write, pause,
        // _exit) and no heap allocation.
        match unsafe { nix::unistd::fork() }.expect("fork worker") {
            ForkResult::Child => {
                // Become our own session + process-group leader, so
                // pgid == our pid (the factory contract).
                let _ = setsid();
                match unsafe { nix::unistd::fork() } {
                    Ok(ForkResult::Child) => {
                        // Grandchild: the "build" descendant. Sleep
                        // long; the group-kill should take us out.
                        std::thread::sleep(Duration::from_secs(30));
                        unsafe { nix::libc::_exit(0) };
                    }
                    Ok(ForkResult::Parent { child }) => {
                        // Report the grandchild PID to the parent.
                        // `write` takes the owned fd by `AsFd`; the
                        // underlying syscall is async-signal-safe.
                        let bytes = child.as_raw().to_ne_bytes();
                        let _ = nix::unistd::write(&write_fd, &bytes);
                        std::thread::sleep(Duration::from_secs(30));
                        unsafe { nix::libc::_exit(0) };
                    }
                    Err(_) => unsafe { nix::libc::_exit(1) },
                }
            }
            ForkResult::Parent { child } => {
                drop(write_fd); // parent only reads
                let worker_pid = child.as_raw();

                // Read the grandchild PID the worker reported.
                let mut buf = [0u8; 4];
                read_exact(&read_fd, &mut buf);
                drop(read_fd);
                let grandchild = Pid::from_raw(i32::from_ne_bytes(buf));

                // The worker must be its own group leader.
                let pgid = getpgid(Some(child)).expect("getpgid");
                assert_eq!(
                    pgid.as_raw(),
                    worker_pid,
                    "test worker must lead its own group"
                );

                // Targeting policy must pick the group form.
                let target = restart_kill_target(worker_pid, Some(pgid.as_raw()));
                assert_eq!(target, RestartKillTarget::Group(worker_pid));

                // Group-kill exactly as kill_subprocess does.
                kill(Pid::from_raw(target.signal_pid()), Signal::SIGKILL)
                    .expect("group SIGKILL");

                // Reap the worker leader (our child) so it is not a
                // zombie, and confirm it died by signal.
                let status = waitpid(child, None).expect("waitpid worker");
                assert!(
                    matches!(status, WaitStatus::Signaled(_, Signal::SIGKILL, _)),
                    "worker leader must be SIGKILLed, got {status:?}"
                );

                // The grandchild is NOT our child (it reparents to
                // init when the worker dies); observe it disappear.
                assert!(
                    wait_gone(grandchild, Duration::from_secs(5)),
                    "grandchild build descendant ORPHANED — it survived \
                     the worker's group-kill (the #500 leak)"
                );
            }
        }
    }

    /// The non-leader case must NOT signal a foreign group. We model
    /// a worker that is NOT a group leader: a sibling process in the
    /// TEST RUNNER's own group. `kill_subprocess`'s targeting must
    /// pick the bare-PID form for it, so a hypothetical group-kill
    /// would never reach the runner's group. We assert the decision,
    /// and (belt-and-braces) that the runner process itself — a
    /// member of that same group — is untouched.
    #[test]
    fn non_leader_does_not_signal_foreign_group() {
        // Fork a child but do NOT setsid: it inherits the runner's
        // process group, so it is NOT its own group leader.
        match unsafe { nix::unistd::fork() }.expect("fork non-leader") {
            ForkResult::Child => {
                std::thread::sleep(Duration::from_secs(30));
                unsafe { nix::libc::_exit(0) };
            }
            ForkResult::Parent { child } => {
                let worker_pid = child.as_raw();
                let pgid = getpgid(Some(child)).expect("getpgid");

                // The child's real pgid is the RUNNER's group, not
                // its own pid — confirm the precondition.
                assert_ne!(
                    pgid.as_raw(),
                    worker_pid,
                    "non-leader child must inherit the runner's group"
                );
                assert_eq!(
                    pgid.as_raw(),
                    getpgid(Some(getpid())).expect("self getpgid").as_raw(),
                    "non-leader child must share the runner's group"
                );

                // Targeting MUST fall back to bare-PID — never the
                // group form (which would be the runner's group and
                // would SIGKILL the manager in production).
                let target = restart_kill_target(worker_pid, Some(pgid.as_raw()));
                assert_eq!(target, RestartKillTarget::Process(worker_pid));
                assert_eq!(target.signal_pid(), worker_pid);

                // Deliver the bare-PID kill the fallback prescribes;
                // the runner (a co-member of the foreign group) must
                // be alive afterwards.
                kill(Pid::from_raw(target.signal_pid()), Signal::SIGKILL)
                    .expect("bare SIGKILL");
                assert_eq!(
                    kill(getpid(), None),
                    Ok(()),
                    "test runner must survive the non-leader kill"
                );

                let _ = waitpid(child, None);
            }
        }
    }

    /// Blocking read of exactly `buf.len()` bytes from `fd`, looping
    /// over short reads. Test-only helper for the PID handshake.
    /// nix 0.29's `read` takes a `RawFd`; the borrowed raw fd stays
    /// valid because the caller owns the pipe end for the duration.
    fn read_exact(fd: &std::os::fd::OwnedFd, buf: &mut [u8]) {
        use std::os::fd::AsRawFd;
        let raw = fd.as_raw_fd();
        let mut off = 0;
        while off < buf.len() {
            match nix::unistd::read(raw, &mut buf[off..]) {
                Ok(0) => panic!("EOF before grandchild PID handshake completed"),
                Ok(n) => off += n,
                Err(Errno::EINTR) => continue,
                Err(e) => panic!("pipe read failed: {e}"),
            }
        }
    }
}
