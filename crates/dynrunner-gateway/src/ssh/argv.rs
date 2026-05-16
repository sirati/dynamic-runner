//! Argv construction, control-path generation, master-PID parsing
//! and the master watcher / kill-ladder for [`SshGateway`].
//!
//! Pure free functions / module-static state. Imported by
//! `connect_disconnect.rs` (spawn flow) and `mod.rs` (Drop).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::process::Command;

use crate::traits::GatewayError;

use super::SshGateway;

/// with PID, gives a per-instance unique path under /tmp without
/// pulling in a `rand` dep. Acquire-Release isn't needed; only the
/// uniqueness matters, not memory ordering with other state.
static CONTROL_PATH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a master control socket path under `/tmp`.
///
/// Format: `/tmp/dynrunner-m-<pid>-<seq>.sock`. Stays well below the
/// 108-byte `sockaddr_un.sun_path` cap even with 7-digit PIDs and
/// large sequence numbers.
///
/// We deliberately avoid `tempfile::TempDir`: TempDir's Drop unlinks
/// the parent dir on `SshGateway` Drop, which raced the master's own
/// socket cleanup and stranded a master with no path back for
/// `ssh -O exit` (bug (g)). The socket file itself is unlinked by
/// the master on clean exit; on dirty exit a stale socket file is
/// harmless (next connect() generates a new path).
pub(super) fn generate_master_control_path() -> String {
    let pid = std::process::id();
    let seq = CONTROL_PATH_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/dynrunner-m-{pid}-{seq}.sock")
}

/// Build the argv (excluding `ssh` itself) for the master spawn.
///
/// Pure function over the gateway's static config (auth + port flags,
/// control path, registered reverse forwards, ssh target). Pulled out
/// of `connect()` so the contract — specifically the 18h ServerAlive
/// floor — is unit-testable without a live sshd.
pub(super) fn build_master_argv(
    base_args: &[String],
    control_path: &str,
    forwarded_ports: &[(u16, u16)],
    target: &str,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    argv.extend(base_args.iter().cloned());
    // -M: master mode. -N: no remote command. NO `-f`: we don't ask
    // ssh to daemonise via fork-into-init. (That said, with
    // `ControlPersist=yes` OpenSSH always forks a daemon child anyway
    // — the `-f` flag would only suppress the launcher's foreground
    // window, not change the daemon's existence. We track the daemon
    // PID via `ssh -O check` in `connect()` regardless.)
    argv.push("-M".into());
    argv.push("-N".into());
    argv.push("-o".into());
    argv.push(format!("ControlPath={control_path}"));
    for opt in SshGateway::master_only_options() {
        argv.push((*opt).into());
    }
    for &(local_port, remote_port) in forwarded_ports {
        argv.push("-R".into());
        argv.push(format!("0.0.0.0:{remote_port}:localhost:{local_port}"));
    }
    argv.push(target.into());
    argv
}

// ---------------------------------------------------------------------
// Daemon-PID helpers — single concern: track the OpenSSH ControlPersist
// daemon (not the launcher) as the SSH master lifetime anchor. See the
// `# Lifetime model` doc on `SshGateway` for the why.
// ---------------------------------------------------------------------

/// Run `ssh -O check` over the control socket and return the daemon
/// PID. Errors when:
///   - the spawn fails (e.g. `ssh` not in $PATH)
///   - exit status is non-zero (the socket exists but doesn't respond
///     — a real fault, not an interim handshake state)
///   - the output doesn't include `Master running (pid=<N>)` (an
///     OpenSSH version that changed the format)
pub(super) async fn probe_master_pid(
    control_path: &str,
    target: &str,
    base_args: &[String],
) -> Result<u32, GatewayError> {
    let mut cmd = Command::new("ssh");
    for arg in base_args {
        cmd.arg(arg);
    }
    cmd.args([
        "-O",
        "check",
        "-o",
        &format!("ControlPath={control_path}"),
    ]);
    cmd.arg(target);

    let output = cmd.output().await.map_err(|e| {
        GatewayError::CommandFailed(format!("ssh -O check spawn: {e}"))
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(GatewayError::CommandFailed(format!(
            "control socket unresponsive (ssh -O check exited {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        )));
    }
    // OpenSSH writes "Master running (pid=N)" to stdout on success.
    // Some older builds wrote to stderr; concatenate both for safety
    // — single concern: extract a u32 PID from whatever ssh said.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    parse_master_pid(&stdout)
        .or_else(|| parse_master_pid(&stderr))
        .ok_or_else(|| {
            GatewayError::CommandFailed(format!(
                "ssh -O check succeeded but output did not contain \
                 `Master running (pid=N)`: stdout={stdout:?} stderr={stderr:?}"
            ))
        })
}

/// Parse `Master running (pid=<N>)` out of `ssh -O check` output.
///
/// Pure / no-allocation parser, kept private to avoid implying a
/// stable API. Returns `None` when the marker isn't present or when
/// the digit run after `pid=` doesn't fit in `u32`.
pub(super) fn parse_master_pid(s: &str) -> Option<u32> {
    let marker = "Master running (pid=";
    let rest = s.find(marker).map(|i| &s[i + marker.len()..])?;
    let pid_str: String =
        rest.chars().take_while(char::is_ascii_digit).collect();
    if pid_str.is_empty() {
        return None;
    }
    pid_str.parse().ok()
}

/// Watcher loop: poll daemon liveness via `kill(pid, 0)` once per
/// second. Exits silently on `cancel` set; emits one
/// `tracing::error!("SSH master exited unexpectedly")` on observing
/// `ESRCH` (the daemon is gone).
///
/// This runs on a `std::thread`, *not* a tokio task — see the
/// `watcher_thread` field doc on `SshGateway` for why.
pub(super) fn master_watcher_loop(daemon_pid: u32, cancel: Arc<AtomicBool>) {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);
    // 1s cadence: coarse enough to be near-free, fine enough that
    // "master died ~2 min after handshake" is observed within the
    // same minute. We sleep *before* the first probe so we don't
    // race the daemon's coming-up window during connect().
    let tick = std::time::Duration::from_secs(1);
    loop {
        std::thread::sleep(tick);
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        match kill(pid, None) {
            Ok(()) => continue,
            Err(Errno::ESRCH) => {
                tracing::error!(
                    daemon_pid,
                    "SSH master exited unexpectedly"
                );
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

/// Sync daemon-teardown ladder: SIGTERM → 200ms grace → SIGKILL.
///
/// Used by both `disconnect()` (after `ssh -O exit`) and `Drop` (on
/// the panic / forgot-to-disconnect path). Single concern: "given a
/// daemon PID, stop polling until that PID is gone, escalating signal
/// strength on grace expiry". No-ops cleanly when the daemon is
/// already gone (typical post-`ssh -O exit` case) — `kill(pid, 0)`
/// returns ESRCH, the loop exits.
///
/// Sync (not async) for two reasons:
///   1. Drop is sync — async-ifying would require holding a runtime
///      handle on the gateway, which leaks runtime ownership into
///      the gateway type.
///   2. The polite `ssh -O exit` already had its async chance up the
///      stack; this is the fallback ladder, where blocking for at
///      most 200ms+grace is cheap.
pub(super) fn terminate_daemon_blocking(daemon_pid: u32) {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = Pid::from_raw(daemon_pid as i32);

    // Fast-path: already gone (e.g. `ssh -O exit` worked, or this is
    // a second teardown call after disconnect()).
    if matches!(kill(pid, None), Err(Errno::ESRCH)) {
        return;
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
    let grace = std::time::Instant::now()
        + std::time::Duration::from_millis(200);
    let poll = std::time::Duration::from_millis(20);
    loop {
        if matches!(kill(pid, None), Err(Errno::ESRCH)) {
            return;
        }
        if std::time::Instant::now() >= grace {
            break;
        }
        std::thread::sleep(poll);
    }

    // Grace expired — SIGKILL. Reaping isn't ours to do (the daemon
    // is reparented to systemd --user / init), so we just signal and
    // poll once more to confirm. If it's *still* alive, log it: the
    // teardown contract is "best effort", but the operator should
    // know.
    if let Err(e) = kill(pid, Signal::SIGKILL)
        && !matches!(e, Errno::ESRCH)
    {
        tracing::warn!(
            daemon_pid,
            error = %e,
            "SIGKILL to SSH master daemon failed"
        );
    }
    // Brief post-SIGKILL settle. We don't loop: SIGKILL is
    // un-ignorable, and a process surviving SIGKILL is an
    // unrecoverable kernel-level fault we don't want to spin on.
    std::thread::sleep(std::time::Duration::from_millis(50));
    if !matches!(kill(pid, None), Err(Errno::ESRCH)) {
        tracing::error!(
            daemon_pid,
            "SSH master daemon still alive after SIGKILL — operator intervention required"
        );
    }
}
