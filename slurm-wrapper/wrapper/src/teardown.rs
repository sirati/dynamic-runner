//! Single concern: wrapper teardown — forward the SIGCONT nudge to the
//! shutdown manager (generate.rs:306-310) and drain the relay
//! (:404-407). Phase 1 (1I) fills body.

use crate::shutdown_spawn::ShutdownMode;

/// Mirror generate.rs:306-310: `systemctl --user kill --signal=SIGCONT
/// <unit>` for systemd mode, `kill -SIGCONT <pid>` for setsid mode,
/// no-op for `None`.
///
/// SIGCONT cannot be blocked or ignored and does not terminate the
/// target; it only wakes the manager's poll loop so it re-evaluates
/// idle-shutdown. All errors are swallowed, mirroring the bash
/// `2>/dev/null || true`: a missing unit or dead pid means the manager
/// is already gone, which is not the wrapper's concern.
pub fn forward_shutdown_nudge(mode: &ShutdownMode) {
    match mode {
        ShutdownMode::Systemd { unit } => {
            // Mirror `systemctl --user kill --signal=SIGCONT <unit>
            // 2>/dev/null || true`: ignore spawn/exit failures.
            let _ = std::process::Command::new("systemctl")
                .args(["--user", "kill", "--signal=SIGCONT", unit])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        ShutdownMode::Setsid { pid } => {
            // Mirror `kill -SIGCONT <pid> 2>/dev/null || true`.
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(*pid as i32),
                nix::sys::signal::Signal::SIGCONT,
            );
        }
        ShutdownMode::None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_mode_is_noop() {
        forward_shutdown_nudge(&ShutdownMode::None);
    }

    #[test]
    fn setsid_to_self_is_harmless() {
        // SIGCONT to ourselves does not terminate and is a no-op for a
        // running process; this just exercises the kill path.
        forward_shutdown_nudge(&ShutdownMode::Setsid {
            pid: std::process::id(),
        });
    }
}
