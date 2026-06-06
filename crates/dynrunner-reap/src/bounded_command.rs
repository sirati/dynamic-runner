//! Single concern: run an external command under a wall-clock bound,
//! killing it on expiry.
//!
//! Both reap consumers shell out to external tools on the teardown
//! critical path — the wrapper's in-band reap calls `podman
//! inspect/stop/rm`, the shutdown-manager captures a one-shot `squeue`
//! diagnostic. A stock [`std::process::Command::output`] /
//! [`std::process::Command::status`] is UNBOUNDED: a tool wedged on
//! NFS-backed storage or an unresponsive slurmctld blocks the caller
//! forever. On the teardown path that is fatal — the kill(2) reap that
//! is the hard backstop never runs, or the manager never cleans up and
//! exits, and SLURM's `KillWait` eventually SIGKILLs the wrapper
//! mid-reap (re-creating the original orphan symptom).
//!
//! This module is that ONE bound. It spawns the child, polls
//! [`std::process::Child::try_wait`] on a fixed cadence until either the
//! child exits or `budget` elapses, and on expiry SIGKILLs the child (it
//! is best-effort diagnostic/teardown work — a hung tool is killed, not
//! waited on). The poll cadence is driven by the injected [`Clock`] so
//! the bound is testable without real wall-time.
//!
//! ## Boundary
//!
//! [`run_bounded`] takes a configured [`Command`], a `budget`, a
//! [`Clock`], and whether stdout is wanted; it returns a
//! [`BoundedOutcome`]. Callers know nothing about how the bound is
//! enforced or how the child is killed; this module knows nothing about
//! podman, squeue, or any consumer's command shape.

use std::process::{Command, Stdio};
use std::time::Duration;

use crate::clock::Clock;

/// Poll cadence for the bounded wait. Short enough that a child which
/// exits early is observed promptly, long enough that the busy-loop cost
/// is negligible over a multi-second budget. Independent of any caller
/// poll interval (mirrors the reap state-machine's own 1s wait tick
/// rationale, but tighter because these bounds are short).
const POLL_TICK: Duration = Duration::from_millis(50);

/// The result of a bounded command run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedOutcome {
    /// The child exited on its own within `budget`. `success` mirrors
    /// `ExitStatus::success()`; `stdout` is the captured stdout when the
    /// caller asked for it (empty otherwise).
    Exited { success: bool, stdout: Vec<u8> },
    /// `budget` elapsed with the child still running; it was SIGKILLed.
    /// No output is returned — a timed-out tool produced nothing usable.
    TimedOut,
    /// The child could not be spawned (the diagnostic carries the
    /// `std::io::Error` text).
    SpawnError(String),
}

/// Spawn `cmd` and wait at most `budget` for it to exit, killing it on
/// expiry. stdin is always nulled and stderr always nulled (callers on
/// this path treat both as best-effort silent). When `want_stdout` is
/// true the child's stdout is piped and returned on a clean exit;
/// otherwise stdout is nulled too.
///
/// Never blocks longer than `budget` + one [`POLL_TICK`] (the final
/// post-budget try_wait). On timeout the child is SIGKILLed and reaped
/// so no zombie is left behind.
pub fn run_bounded<C: Clock>(
    mut cmd: Command,
    budget: Duration,
    clock: &C,
    want_stdout: bool,
) -> BoundedOutcome {
    cmd.stdin(Stdio::null()).stderr(Stdio::null());
    cmd.stdout(if want_stdout {
        Stdio::piped()
    } else {
        Stdio::null()
    });

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return BoundedOutcome::SpawnError(e.to_string()),
    };

    let mut elapsed = Duration::ZERO;
    loop {
        match child.try_wait() {
            // Child exited: read its piped stdout (if any) and report.
            Ok(Some(status)) => {
                let stdout = match (want_stdout, child.stdout.take()) {
                    (true, Some(mut handle)) => {
                        use std::io::Read as _;
                        let mut buf = Vec::new();
                        // Best-effort: a read error degrades to empty
                        // stdout, never blocks (the child is already
                        // gone, so the pipe is at EOF or near it).
                        let _ = handle.read_to_end(&mut buf);
                        buf
                    }
                    _ => Vec::new(),
                };
                return BoundedOutcome::Exited {
                    success: status.success(),
                    stdout,
                };
            }
            // Still running.
            Ok(None) => {
                if elapsed >= budget {
                    // Budget exhausted: SIGKILL and reap so the timed-out
                    // tool cannot keep blocking and leaves no zombie.
                    let _ = child.kill();
                    let _ = child.wait();
                    return BoundedOutcome::TimedOut;
                }
                clock.sleep(POLL_TICK);
                elapsed += POLL_TICK;
            }
            // try_wait itself errored (should not happen for a live
            // child); treat as "cannot observe" → kill and report
            // timeout-equivalent so the caller does not hang.
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return BoundedOutcome::TimedOut;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::RealClock;

    /// A fast command that exits 0 within budget is reported `Exited`
    /// with its stdout captured when requested.
    #[test]
    fn fast_command_exits_with_stdout() {
        let mut cmd = Command::new("printf");
        cmd.arg("hello");
        let out = run_bounded(cmd, Duration::from_secs(5), &RealClock, true);
        assert_eq!(
            out,
            BoundedOutcome::Exited {
                success: true,
                stdout: b"hello".to_vec()
            }
        );
    }

    /// A non-zero exit within budget reports `success: false`.
    #[test]
    fn fast_command_nonzero_exit() {
        let mut cmd = Command::new("false");
        let out = run_bounded(
            cmd_silence(&mut cmd),
            Duration::from_secs(5),
            &RealClock,
            false,
        );
        assert_eq!(
            out,
            BoundedOutcome::Exited {
                success: false,
                stdout: Vec::new()
            }
        );
    }

    /// A command that sleeps past the budget is SIGKILLed and reported
    /// `TimedOut` — the load-bearing bound. The budget is short and the
    /// sleep long, so a regression that drops the bound would hang this
    /// test until the runner kills it.
    #[test]
    fn slow_command_times_out_and_is_killed() {
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let start = std::time::Instant::now();
        let out = run_bounded(cmd, Duration::from_millis(200), &RealClock, false);
        let elapsed = start.elapsed();
        assert_eq!(out, BoundedOutcome::TimedOut);
        assert!(
            elapsed < Duration::from_secs(5),
            "must return ~at budget, not wait out the 30s sleep (took {elapsed:?})"
        );
    }

    /// A command that cannot be spawned reports `SpawnError`, never
    /// hangs.
    #[test]
    fn missing_binary_is_spawn_error() {
        let cmd = Command::new("/nonexistent/definitely-not-a-real-binary-xyz");
        let out = run_bounded(cmd, Duration::from_secs(5), &RealClock, false);
        assert!(matches!(out, BoundedOutcome::SpawnError(_)), "got {out:?}");
    }

    /// Helper to keep `false`'s output off the test console without
    /// changing the returned `Command` ownership.
    fn cmd_silence(cmd: &mut Command) -> Command {
        std::mem::replace(cmd, Command::new("true"))
    }
}
