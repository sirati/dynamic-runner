//! Captured exit disposition of a worker subprocess, plus the
//! non-blocking reaper [`try_reap_subprocess`].
//!
//! The framework observes worker death via pipe EOF; this module
//! turns the kernel's `waitpid` result into a typed
//! [`WorkerExitStatus`] used downstream by [`super::handle`]. Signal
//! names are mapped only for the small set the framework
//! discriminates on (SIGKILL → OOM/external, SIGSEGV/SIGABRT →
//! deterministic bug, etc.); anything else falls back to numeric
//! via the [`Display`] impl.

use std::fmt;

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
