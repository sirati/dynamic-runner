use std::fmt;
use std::time::Instant;

use dynrunner_core::{
    TaskInfo, ErrorType, Identifier, ResourceMap, TaskResult, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;

use crate::monitor::{ProcStatmMonitor, ResourceMonitor};
use dynrunner_protocol_manager_worker::state::{
    AssignResult, PollResult, Processing, RunnerProtocol, RunnerProtocolState, WaitReadyResult,
};
use dynrunner_scheduler_api::WorkerBudgetInfo;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Captured exit disposition of a worker subprocess after the framework
/// observed pipe-EOF or send-failure on its transport.
///
/// Exactly one of `code` or `signal` is `Some`: a clean exit has a code,
/// a kill has a signal. `core_dumped` is meaningful only when `signal`
/// is set; otherwise it is `false`.
///
/// The framework treats reap-not-available (no PID, ECHILD, kernel race)
/// as an `Option<WorkerExitStatus>::None` at the use-site — see
/// [`WorkerHandle::try_reap_exit`] for the conditions under which a reap
/// returns `None`.
#[derive(Debug, Clone)]
pub struct WorkerExitStatus {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub signal_name: Option<&'static str>,
    pub core_dumped: bool,
}

impl WorkerExitStatus {
    /// True iff the worker was killed by a signal (vs. exited cleanly).
    /// Operators classify a SIGKILL/SIGTERM disconnect differently from
    /// a non-zero-code exit; this is the discriminator.
    pub fn was_killed(&self) -> bool {
        self.signal.is_some()
    }
}

impl fmt::Display for WorkerExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code, self.signal) {
            (Some(code), _) => write!(f, "exited with code {code}"),
            (_, Some(sig)) => {
                let name = self.signal_name.unwrap_or("?");
                let core = if self.core_dumped { ", core dumped" } else { "" };
                write!(f, "killed by SIG{name} ({sig}){core}")
            }
            (None, None) => write!(f, "unknown disposition"),
        }
    }
}

/// Maximum number of WNOHANG retries when reaping a worker subprocess.
///
/// The framework observes worker death via pipe EOF, which the kernel
/// emits *before* the SIGCHLD that would let `waitpid` return the
/// child's exit status. Without retries, a WNOHANG reap immediately
/// after EOF can return `StillAlive` for an already-dead child. Three
/// retries at 5ms cover the kernel's typical SIGCHLD-delivery window
/// on Linux without making the reap path materially slow.
#[cfg(unix)]
const REAP_RETRY_COUNT: u32 = 3;
#[cfg(unix)]
const REAP_RETRY_DELAY_MS: u64 = 5;

#[cfg(unix)]
fn signal_name_for(sig: i32) -> Option<&'static str> {
    // Map the small set of signals the framework actually expects to
    // see on worker death. Anything else falls back to numeric form
    // via the Display impl. This is intentionally not a generic
    // libc-signal-name lookup — we surface the signals an operator
    // needs to discriminate (SIGKILL: external/OOM, SIGTERM: graceful
    // shutdown, SIGSEGV/SIGABRT/SIGBUS/SIGFPE: deterministic bug,
    // SIGPIPE: peer closed pipe, SIGSYS: seccomp violation).
    match sig {
        1 => Some("HUP"),
        2 => Some("INT"),
        3 => Some("QUIT"),
        4 => Some("ILL"),
        6 => Some("ABRT"),
        7 => Some("BUS"),
        8 => Some("FPE"),
        9 => Some("KILL"),
        11 => Some("SEGV"),
        13 => Some("PIPE"),
        14 => Some("ALRM"),
        15 => Some("TERM"),
        24 => Some("XCPU"),
        25 => Some("XFSZ"),
        31 => Some("SYS"),
        _ => None,
    }
}

/// Non-blocking reap of a worker subprocess that the framework has
/// already observed as dead via pipe EOF or send-failure.
///
/// Returns:
/// - `None` if `pid` is `None` (no PID tracked — e.g. in-process
///   channel worker, factory returned `None`).
/// - `None` if the reap retries exhausted with the kernel still
///   reporting the child alive (SIGCHLD-delivery race or pid mismatch).
/// - `None` if `waitpid` returned `ECHILD` (already reaped by another
///   path, typically the factory dropping its `Child` handle).
/// - `Some(status)` on successful reap.
///
/// **Non-blocking by design:** uses `WNOHANG` with a short retry
/// budget. Blocking `waitpid` from the dispatcher's event loop would
/// freeze the manager if the kernel hadn't actually finalised the
/// child.
#[cfg(unix)]
pub(crate) fn try_reap_subprocess(pid: Option<u32>) -> Option<WorkerExitStatus> {
    use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
    use nix::unistd::Pid;
    let pid = Pid::from_raw(pid? as i32);
    for attempt in 0..=REAP_RETRY_COUNT {
        match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => {
                return Some(WorkerExitStatus {
                    code: Some(code),
                    signal: None,
                    signal_name: None,
                    core_dumped: false,
                });
            }
            Ok(WaitStatus::Signaled(_, sig, core_dumped)) => {
                let sig_num = sig as i32;
                return Some(WorkerExitStatus {
                    code: None,
                    signal: Some(sig_num),
                    signal_name: signal_name_for(sig_num),
                    core_dumped,
                });
            }
            Ok(WaitStatus::StillAlive) => {
                if attempt == REAP_RETRY_COUNT {
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(REAP_RETRY_DELAY_MS));
                continue;
            }
            Ok(_) | Err(_) => return None,
        }
    }
    None
}

#[cfg(not(unix))]
pub(crate) fn try_reap_subprocess(_pid: Option<u32>) -> Option<WorkerExitStatus> {
    None
}

/// Events produced by a worker that the manager reacts to.
#[derive(Debug)]
pub enum WorkerEvent<I: Identifier> {
    Ready {
        worker_id: WorkerId,
    },
    TaskCompleted {
        worker_id: WorkerId,
        result: TaskResult,
        /// Opaque task-specific payload (the bytes after `done:` on the wire).
        /// `None` if the worker sent a bare `done`.
        result_data: Option<Vec<u8>>,
        binary: Option<TaskInfo<I>>,
        estimated_resources: ResourceMap,
    },
    Disconnected {
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<TaskInfo<I>>,
    },
    PhaseUpdate {
        worker_id: WorkerId,
        phase_name: String,
    },
    Keepalive {
        worker_id: WorkerId,
    },
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
    pub reserved_budgets: ResourceMap,
    pub estimated_resources: ResourceMap,
    pub current_binary: Option<TaskInfo<I>>,
    pub opportunistic: bool,
    pub has_initial_assignment: bool,
    pub idle: bool,
    pub actual_usage: ResourceMap,
    pub assignment_failure_count: u32,
    pub pid: Option<u32>,
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
    poll_task: Option<JoinHandle<RunnerProtocolState<M>>>,
}

impl<M: ManagerEndpoint + 'static, I: Identifier> WorkerHandle<M, I> {
    pub fn new(
        worker_id: WorkerId,
        transport: M,
        event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    ) -> Self {
        let waiting = RunnerProtocol::connect(transport);
        Self {
            worker_id,
            reserved_budgets: ResourceMap::new(),
            estimated_resources: ResourceMap::new(),
            current_binary: None,
            opportunistic: false,
            has_initial_assignment: false,
            idle: false,
            actual_usage: ResourceMap::new(),
            assignment_failure_count: 0,
            pid: None,
            phase: None,
            last_keepalive: None,
            phase_started_at: None,
            phase_status_log_idx: 0,
            protocol: RunnerProtocolState::WaitingForReady(waiting),
            event_tx,
            poll_task: None,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.protocol.is_idle() || self.protocol.is_processing()
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

    pub fn is_idle_state(&self) -> bool {
        self.protocol.is_idle()
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
                Some(WorkerEvent::Ready {
                    worker_id: self.worker_id,
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

    /// Assign a task to this worker. Transitions Idle → Processing.
    ///
    /// Spawns a background task that reads from the transport and sends
    /// `WorkerEvent`s to the shared event channel. The manager receives
    /// events for all workers from a single channel without blocking.
    pub async fn assign_task(
        &mut self,
        binary: TaskInfo<I>,
        estimated_resources: ResourceMap,
        opportunistic: bool,
    ) -> Result<(), String> {
        let idle = self
            .protocol
            .take_idle()
            .ok_or_else(|| "worker not in Idle state".to_string())?;

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
        match idle.assign_task(relative_path, payload, resolved_path).await {
            AssignResult::Assigned(processing) => {
                // Spawn a background task that polls the worker protocol.
                let worker_id = self.worker_id;
                let binary_clone = binary.clone();
                let tx = self.event_tx.clone();

                let est_clone = estimated_resources.clone();
                let handle = tokio::task::spawn_local(async move {
                    Self::poll_loop(processing, worker_id, binary_clone, est_clone, tx).await
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
    /// to the shared channel, returns the final protocol state.
    async fn poll_loop(
        mut processing: RunnerProtocol<Processing, M>,
        worker_id: WorkerId,
        binary: TaskInfo<I>,
        estimated_resources: ResourceMap,
        tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    ) -> RunnerProtocolState<M> {
        loop {
            match processing.poll_status().await {
                PollResult::Completed { result, result_data, protocol } => {
                    let _ = tx.send(WorkerEvent::TaskCompleted {
                        worker_id,
                        result,
                        result_data,
                        binary: Some(binary),
                        estimated_resources,
                    });
                    return RunnerProtocolState::Idle(protocol);
                }
                PollResult::StillRunning {
                    protocol,
                    phase_update,
                    got_keepalive,
                } => {
                    processing = protocol;
                    if let Some(phase) = phase_update {
                        let _ = tx.send(WorkerEvent::PhaseUpdate {
                            worker_id,
                            phase_name: phase,
                        });
                    } else if got_keepalive {
                        let _ = tx.send(WorkerEvent::Keepalive { worker_id });
                    }
                    // Loop to read the next response
                }
                PollResult::Disconnected { result, protocol } => {
                    let _ = tx.send(WorkerEvent::Disconnected {
                        worker_id,
                        result,
                        binary: Some(binary),
                    });
                    return RunnerProtocolState::Stopped(protocol);
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
                Ok(state) => {
                    self.protocol = state;
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

    /// Update actual resource usage by reading /proc/[pid]/statm (Linux only).
    pub fn update_resource_usage(&mut self) {
        self.actual_usage = ProcStatmMonitor.measure(self.pid);
    }
}

#[cfg(test)]
mod exit_status_tests {
    use super::*;

    // Display tests: pin the exact log-line text downstream operators
    // grep for. Changes to these formats are breaking changes to the
    // operator workflow, not just internal refactors.

    #[test]
    fn display_exited_zero() {
        let s = WorkerExitStatus {
            code: Some(0),
            signal: None,
            signal_name: None,
            core_dumped: false,
        };
        assert_eq!(s.to_string(), "exited with code 0");
        assert!(!s.was_killed());
    }

    #[test]
    fn display_exited_nonzero() {
        let s = WorkerExitStatus {
            code: Some(137),
            signal: None,
            signal_name: None,
            core_dumped: false,
        };
        // Note: a worker exited with 137 typically means "killed by
        // SIGKILL but reported via a shell wrapper that converted the
        // signal to exit code 128+sig". The framework only sees the
        // shell-reported exit code in that case, not the signal. This
        // is a known-and-accepted blind spot: if a worker is launched
        // under a shell, the shell layer hides signal info.
        assert_eq!(s.to_string(), "exited with code 137");
    }

    #[test]
    fn display_signaled_named() {
        let s = WorkerExitStatus {
            code: None,
            signal: Some(9),
            signal_name: Some("KILL"),
            core_dumped: false,
        };
        assert_eq!(s.to_string(), "killed by SIGKILL (9)");
        assert!(s.was_killed());
    }

    #[test]
    fn display_signaled_unnamed_falls_back_to_question_mark() {
        let s = WorkerExitStatus {
            code: None,
            signal: Some(77),
            signal_name: None,
            core_dumped: false,
        };
        // Numeric signal still surfaces — the operator can look it up.
        // "SIG?" makes the fallback explicit rather than silent.
        assert_eq!(s.to_string(), "killed by SIG? (77)");
    }

    #[test]
    fn display_signaled_with_core_dumped() {
        let s = WorkerExitStatus {
            code: None,
            signal: Some(11),
            signal_name: Some("SEGV"),
            core_dumped: true,
        };
        assert_eq!(s.to_string(), "killed by SIGSEGV (11), core dumped");
    }

    #[test]
    fn try_reap_none_pid_returns_none() {
        // The "no PID tracked" branch — e.g. in-process channel
        // worker, factory returned None — must be a clean None,
        // not a panic.
        assert!(try_reap_subprocess(None).is_none());
    }

    // Live-subprocess reap tests. Spawn a real /bin/true / /bin/sleep,
    // observe the kernel's exit-status reporting via try_reap_subprocess.
    // These tests exercise the actual `waitpid` syscall path on unix
    // (the path operators rely on in production).

    #[cfg(unix)]
    #[test]
    fn try_reap_picks_up_clean_exit() {
        use std::process::Command;
        // `/bin/true` exits with code 0 immediately. Spawn, drop the
        // Child handle (std::process::Child::drop does not reap on
        // unix — the zombie persists), then reap via our path.
        let child = Command::new("true")
            .spawn()
            .expect("spawn `true`");
        let pid = child.id();
        drop(child);
        // Brief wait for the kernel to mark the child as exited.
        // The reap retry loop also rides out this race, but giving
        // it a head start makes the test deterministic on slow CI.
        std::thread::sleep(std::time::Duration::from_millis(25));
        let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
        assert_eq!(status.code, Some(0));
        assert_eq!(status.signal, None);
        assert!(!status.was_killed());
        assert!(!status.core_dumped);
    }

    #[cfg(unix)]
    #[test]
    fn try_reap_picks_up_sigkill() {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        use std::process::Command;
        // Spawn `/bin/sleep 30` and SIGKILL it. The reap must
        // return code=None, signal=Some(9), signal_name=Some("KILL").
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn `sleep 30`");
        let pid = child.id();
        kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("send SIGKILL");
        drop(child);
        std::thread::sleep(std::time::Duration::from_millis(25));
        let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
        assert_eq!(status.code, None);
        assert_eq!(status.signal, Some(9));
        assert_eq!(status.signal_name, Some("KILL"));
        assert!(status.was_killed());
        // SIGKILL does not core-dump by default — assert the
        // formatter does not get this wrong.
        assert!(!status.core_dumped);
        assert_eq!(status.to_string(), "killed by SIGKILL (9)");
    }

    #[cfg(unix)]
    #[test]
    fn try_reap_picks_up_sigterm_with_name() {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        use std::process::Command;
        // Pin SIGTERM specifically because the watchdog's graceful
        // path sends SIGTERM, and operators should be able to
        // discriminate "killed by SIGTERM from watchdog" from
        // "killed by SIGKILL from cgroup-OOM".
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn `sleep 30`");
        let pid = child.id();
        kill(Pid::from_raw(pid as i32), Signal::SIGTERM).expect("send SIGTERM");
        drop(child);
        std::thread::sleep(std::time::Duration::from_millis(25));
        let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
        assert_eq!(status.signal, Some(15));
        assert_eq!(status.signal_name, Some("TERM"));
    }
}
