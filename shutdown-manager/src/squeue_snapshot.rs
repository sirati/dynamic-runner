//! Single concern: capture a ONE-SHOT `squeue -u <runtime-user>`
//! diagnostic snapshot at teardown and append its full output to the log,
//! FOR CONTEXT ONLY.
//!
//! This is NOT a trigger and NOT a poll. The pre-2026-05 wrapper watchdog
//! that *polled* `squeue` and forced a kill on a single empty observation
//! was removed precisely because slurmctld transiently emits empty/rc=0
//! results under load (#40 / 684f86ec); re-introducing a polling squeue
//! would resurrect that flakiness. Instead the reaper's authoritative
//! trigger is the wrapper-PID-gone / signal path (see
//! [`crate::poll_loop::ShutdownTrigger`]); this module only takes ONE
//! snapshot of the SLURM view of the runtime user's jobs at the moment of
//! teardown so the operator can correlate "why did teardown fire" against
//! "what did SLURM think the job state was". A transiently-empty snapshot
//! here is harmless — it never decides anything.
//!
//! `-u <user>` (the user's OWN jobs), NOT `-j <job-id>`: the manager has
//! no SLURM job id, and the runtime user is resolvable from the
//! environment. Best-effort throughout — a missing `squeue`, an
//! unresolvable user, a non-zero exit, OR a hung slurmctld each log one
//! note and return; losing a diagnostic snapshot is strictly less bad
//! than aborting the reaper.
//!
//! BOUNDED: this snapshot runs (`main.rs`) BEFORE `final_cleanup`. An
//! unbounded `squeue` against a wedged slurmctld would strand the manager
//! forever — it would never clean up `/tmp` or exit. So the `squeue` (and
//! the `id -un` user-resolution fallback) are run via
//! [`dynrunner_reap::bounded_command::run_bounded`]: spawn, wait at most
//! [`SQUEUE_BUDGET`], SIGKILL on expiry, treat a timeout as "no snapshot".

use std::process::Command;
use std::time::Duration;

use dynrunner_reap::bounded_command::{run_bounded, BoundedOutcome};
use dynrunner_reap::clock::RealClock;

/// Wall-clock bound for the one-shot diagnostic `squeue` (and the `id -un`
/// user-resolution fallback). Short: this is best-effort CONTEXT captured
/// on the teardown critical path, never a decision — a slurmctld that
/// cannot answer in this window is treated as "no snapshot" so the manager
/// proceeds straight to cleanup + exit. 5s comfortably covers a healthy
/// `squeue` while never letting a hung controller strand teardown.
const SQUEUE_BUDGET: Duration = Duration::from_secs(5);

/// Resolve the runtime user, run `squeue -u <user>` once, and append the
/// formatted snapshot to `log`. Best-effort: never panics, never aborts.
pub fn capture<L: FnMut(&str)>(log: &mut L) {
    let Some(user) = resolve_user() else {
        log(
            "squeue snapshot: could not resolve the runtime user (USER/LOGNAME unset \
             and `id -un` unavailable); skipping SLURM-state snapshot",
        );
        return;
    };
    match run_squeue(&user) {
        Ok(output) => log(&format_snapshot(&user, &output)),
        Err(why) => log(&format!(
            "squeue snapshot: `squeue -u {}` unavailable ({}); no SLURM-state context captured",
            user, why
        )),
    }
}

/// Resolve the runtime username: `$USER`, then `$LOGNAME`, then `id -un`.
/// `None` only when all three are unavailable/empty.
fn resolve_user() -> Option<String> {
    if let Some(u) = env_nonempty("USER") {
        return Some(u);
    }
    if let Some(u) = env_nonempty("LOGNAME") {
        return Some(u);
    }
    // Last resort: `id -un`. Absolute path is not used here because this
    // is a pure diagnostic; a PATH miss simply degrades to "no snapshot",
    // never to a wrong reap decision. BOUNDED for the same reason `squeue`
    // is: nothing on the teardown critical path may block unboundedly.
    let mut cmd = Command::new("id");
    cmd.arg("-un");
    let BoundedOutcome::Exited {
        success: true,
        stdout,
    } = run_bounded(cmd, SQUEUE_BUDGET, &RealClock, true)
    else {
        return None;
    };
    let name = String::from_utf8_lossy(&stdout).trim().to_string();
    match name.is_empty() {
        true => None,
        false => Some(name),
    }
}

/// Read an environment variable, returning `None` for unset OR empty.
fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Run `squeue -u <user>` once under a wall-clock bound, capturing stdout.
/// `Ok(stdout)` on a clean exit-0; `Err(diag)` on spawn failure, non-zero
/// exit, OR timeout against a hung slurmctld (the diag is logged as the
/// reason the snapshot is absent). NEVER blocks longer than
/// [`SQUEUE_BUDGET`] — the load-bearing property on the teardown path.
fn run_squeue(user: &str) -> Result<String, String> {
    let mut cmd = Command::new("squeue");
    cmd.arg("-u").arg(user);
    match run_bounded(cmd, SQUEUE_BUDGET, &RealClock, true) {
        BoundedOutcome::Exited {
            success: true,
            stdout,
        } => Ok(String::from_utf8_lossy(&stdout).into_owned()),
        BoundedOutcome::Exited { success: false, .. } => Err("non-zero exit".to_string()),
        BoundedOutcome::TimedOut => Err(format!(
            "timed out after {}s (slurmctld unresponsive); killed",
            SQUEUE_BUDGET.as_secs()
        )),
        BoundedOutcome::SpawnError(e) => Err(format!("spawn error: {}", e)),
    }
}

/// Format the captured snapshot into a single multi-line log entry, FOR
/// CONTEXT only. PURE / testable: no I/O. An empty `squeue` body (the
/// transient-empty case the old poll mis-handled) is reported plainly as
/// "no jobs listed" — it is context, never a decision.
fn format_snapshot(user: &str, squeue_output: &str) -> String {
    let body = squeue_output.trim_end_matches('\n');
    if body.trim().is_empty() {
        return format!(
            "squeue snapshot (context only) for user {} at teardown: <no jobs listed>",
            user
        );
    }
    format!(
        "squeue snapshot (context only) for user {} at teardown:\n{}",
        user, body
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A normal multi-line `squeue` body is appended verbatim under the
    /// context header.
    #[test]
    fn format_includes_full_body() {
        let body = "JOBID PARTITION NAME USER ST TIME NODES NODELIST\n\
                    153731 compute asm kruppb R 1:02 1 node07\n";
        let line = format_snapshot("kruppb", body);
        assert!(line.contains("context only"), "line: {}", line);
        assert!(line.contains("for user kruppb"), "line: {}", line);
        assert!(line.contains("153731 compute asm"), "body verbatim: {}", line);
        // The trailing newline is trimmed (single log entry, no blank tail).
        assert!(!line.ends_with('\n'), "no trailing newline: {:?}", line);
    }

    /// An empty body (the transient-empty case) is reported as "no jobs
    /// listed" — context, never a kill decision.
    #[test]
    fn format_empty_body_is_no_jobs_listed() {
        assert_eq!(
            format_snapshot("kruppb", ""),
            "squeue snapshot (context only) for user kruppb at teardown: <no jobs listed>"
        );
        assert_eq!(
            format_snapshot("kruppb", "\n\n"),
            "squeue snapshot (context only) for user kruppb at teardown: <no jobs listed>"
        );
    }

    /// A header-only body (squeue prints the header even with no jobs) is
    /// preserved verbatim — still context, the operator sees the header.
    #[test]
    fn format_header_only_body_preserved() {
        let body = "JOBID PARTITION NAME USER ST TIME NODES NODELIST\n";
        let line = format_snapshot("kruppb", body);
        assert!(line.contains("JOBID PARTITION"), "header kept: {}", line);
        assert!(!line.contains("<no jobs listed>"), "non-empty body: {}", line);
    }

    /// `env_nonempty` treats unset and empty alike as `None`.
    #[test]
    fn env_nonempty_rejects_empty() {
        // Use a key overwhelmingly unlikely to be set in the test env.
        let key = "DYNRUNNER_SQUEUE_TEST_UNSET_KEY_XYZ";
        // SAFETY: test-local, single-threaded mutation of a unique key.
        unsafe {
            std::env::remove_var(key);
        }
        assert_eq!(env_nonempty(key), None);
        unsafe {
            std::env::set_var(key, "");
        }
        assert_eq!(env_nonempty(key), None);
        unsafe {
            std::env::set_var(key, "alice");
        }
        assert_eq!(env_nonempty(key), Some("alice".to_string()));
        unsafe {
            std::env::remove_var(key);
        }
    }
}
