//! Daemon-PID probing via `ssh -O check`.
//!
//! `probe_master_pid` runs the command synchronously with a short
//! timeout and parses the canonical "Master running (pid=<N>)"
//! marker out of stdout/stderr. Surfaces a typed
//! `SshMasterError::MasterPidProbeFailed` on any failure path so the
//! lifecycle code can branch on it rather than on a string.

use std::process::Command;
use std::time::{Duration, Instant};

use crate::error::SshMasterError;

/// PID. Errors surface as [`SshMasterError::MasterPidProbeFailed`].
pub(crate) fn probe_master_pid(
    control_path: &str,
    target: &str,
    base_args: &[String],
) -> Result<u32, SshMasterError> {
    let mut cmd = Command::new("ssh");
    for arg in base_args {
        cmd.arg(arg);
    }
    cmd.args(["-O", "check", "-o", &format!("ControlPath={control_path}")]);
    cmd.arg(target);

    let output = cmd
        .output()
        .map_err(|_| SshMasterError::MasterPidProbeFailed)?;
    if !output.status.success() {
        return Err(SshMasterError::MasterPidProbeFailed);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    parse_master_pid(&stdout)
        .or_else(|| parse_master_pid(&stderr))
        .ok_or(SshMasterError::MasterPidProbeFailed)
}

/// Same as [`probe_master_pid`] but with a hard timeout: if the
/// `ssh -O check` subprocess hasn't returned within `timeout`, we
/// SIGKILL it and surface [`SshMasterError::MasterPidProbeFailed`].
///
/// Why this exists: `ssh -O check` against a socket that has a
/// listener but doesn't speak the SSH multiplex protocol hangs
/// indefinitely. We observed this in the
/// `adopt_rejects_stale_socket_no_master_behind_it` integration test
/// (a Rust `UnixListener::bind` with no accept loop). The fast-path
/// (a real master responding to the probe) returns in <1ms; the
/// slow-path is a bug-class signal we want to convert into a typed
/// error, not a hang.
///
/// Implementation: spawn the subprocess and poll its `try_wait`
/// status at 50ms cadence, sending SIGKILL on deadline. Sync (we're
/// already in a sync path) and dependency-free.
pub(super) fn probe_master_pid_with_timeout(
    control_path: &str,
    target: &str,
    base_args: &[String],
    timeout: Duration,
) -> Result<u32, SshMasterError> {
    let mut cmd = Command::new("ssh");
    for arg in base_args {
        cmd.arg(arg);
    }
    cmd.args(["-O", "check", "-o", &format!("ControlPath={control_path}")]);
    cmd.arg(target);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|_| SshMasterError::MasterPidProbeFailed)?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // Subprocess exited. Read its output.
                let out = child
                    .wait_with_output()
                    .unwrap_or_else(|_| std::process::Output {
                        status,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    });
                if !out.status.success() {
                    return Err(SshMasterError::MasterPidProbeFailed);
                }
                let stdout = String::from_utf8_lossy(&out.stdout);
                let stderr = String::from_utf8_lossy(&out.stderr);
                return parse_master_pid(&stdout)
                    .or_else(|| parse_master_pid(&stderr))
                    .ok_or(SshMasterError::MasterPidProbeFailed);
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    // Hung subprocess — kill and surface as probe failure.
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(SshMasterError::MasterPidProbeFailed);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return Err(SshMasterError::MasterPidProbeFailed),
        }
    }
}

/// Parse `Master running (pid=<N>)` out of `ssh -O check` output.
pub(super) fn parse_master_pid(s: &str) -> Option<u32> {
    let marker = "Master running (pid=";
    let rest = s.find(marker).map(|i| &s[i + marker.len()..])?;
    let pid_str: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if pid_str.is_empty() {
        return None;
    }
    pid_str.parse().ok()
}
