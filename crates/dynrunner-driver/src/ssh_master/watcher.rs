//! Master-watcher std::thread + the SIGTERM→SIGKILL ladder.
//!
//! The watcher polls `kill(daemon_pid, 0)` once per second so the
//! "master died" log fires for actual daemon death (locked point
//! (f)+(h.1)). The thread is deliberately *not* a tokio task; see
//! the mod-level docs for the PyO3 short-lived-runtime rationale.
//!
//! `terminate_daemon_blocking` is the production kill ladder used
//! by `disconnect_spawn_master` and `Drop`; tests can substitute it
//! via the `test_kill_hook` field on `SshMaster`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::error::{KillLadder, SshMasterError};
use crate::ssh_target::SshTarget;

/// Spawn the master-watcher thread. Returns the join handle. Polls
/// `kill(daemon_pid, 0)` once per second; on ESRCH, sets
/// `invalidated` and emits a `tracing::error!`.
pub(super) fn spawn_master_watcher(
    daemon_pid: u32,
    cancel: Arc<AtomicBool>,
    invalidated: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("dynrunner-ssh-master-watch-{daemon_pid}"))
        .spawn(move || master_watcher_loop(daemon_pid, cancel, invalidated))
        .expect("failed to spawn ssh-master-watch thread")
}

fn master_watcher_loop(daemon_pid: u32, cancel: Arc<AtomicBool>, invalidated: Arc<AtomicBool>) {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);
    let tick = Duration::from_secs(1);
    loop {
        std::thread::sleep(tick);
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        match kill(pid, None) {
            Ok(()) => continue,
            Err(Errno::ESRCH) => {
                invalidated.store(true, Ordering::SeqCst);
                tracing::error!(daemon_pid, "SSH master exited unexpectedly");
                return;
            }
            Err(e) => {
                // EPERM in particular means the daemon is alive but
                // not owned by us (PID reuse onto another user's
                // process — rare, but possible). Treat any other
                // errno as "stop probing" so we don't spin forever
                // on a structural fault.
                tracing::warn!(
                    daemon_pid,
                    error = %e,
                    "SSH master kill(pid,0) probe failed; stopping observer"
                );
                return;
            }
        }
    }
}

/// Sync daemon-teardown ladder: SIGTERM → 200ms grace → SIGKILL
/// → 50ms settle.
///
/// Returns `Err(SshMasterError::UnkillableMaster)` only when even
/// SIGKILL did not result in ESRCH within the post-SIGKILL settle
/// window. This is the only path through which the unkillable
/// condition is surfaced — Drop logs (per locked point (j)) and
/// `disconnect()` returns the variant.
///
/// Sync (not async) for two reasons:
///   1. Drop is sync — async-ifying would require holding a runtime
///      handle on the master, which leaks runtime ownership into
///      the master type.
///   2. The polite `ssh -O exit` already had its sync chance up the
///      stack; this is the fallback ladder, where blocking for at
///      most ~250ms is cheap.
pub(super) fn terminate_daemon_blocking(
    daemon_pid: u32,
    target: &SshTarget,
) -> Result<(), SshMasterError> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);

    // Fast-path: already gone (e.g. `ssh -O exit` worked, or this is
    // a second teardown call after disconnect()).
    if matches!(kill(pid, None), Err(Errno::ESRCH)) {
        return Ok(());
    }

    // SIGTERM, then poll until ESRCH or grace expires.
    if let Err(e) = kill(pid, Signal::SIGTERM)
        && !matches!(e, Errno::ESRCH)
    {
        tracing::warn!(
            daemon_pid,
            error = %e,
            "SIGTERM to SSH master daemon failed"
        );
    }
    let grace = Instant::now() + Duration::from_millis(200);
    let poll = Duration::from_millis(20);
    loop {
        if matches!(kill(pid, None), Err(Errno::ESRCH)) {
            return Ok(());
        }
        if Instant::now() >= grace {
            break;
        }
        std::thread::sleep(poll);
    }

    // Grace expired — SIGKILL. Reaping isn't ours to do (the daemon
    // is reparented to systemd --user / init), so we just signal
    // and poll once more to confirm.
    if let Err(e) = kill(pid, Signal::SIGKILL)
        && !matches!(e, Errno::ESRCH)
    {
        tracing::warn!(
            daemon_pid,
            error = %e,
            "SIGKILL to SSH master daemon failed"
        );
    }
    // Brief post-SIGKILL settle. We don't loop indefinitely:
    // SIGKILL is un-ignorable, and a process surviving SIGKILL is
    // an unrecoverable kernel-level fault we don't want to spin on.
    std::thread::sleep(Duration::from_millis(50));
    if matches!(kill(pid, None), Err(Errno::ESRCH)) {
        return Ok(());
    }
    Err(SshMasterError::UnkillableMaster {
        target: target.clone(),
        last_known_pid: daemon_pid,
        kill_ladder_reached: KillLadder::SigkillButPidStillExists,
    })
}
