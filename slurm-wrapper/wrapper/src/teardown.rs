//! Single concern: wrapper teardown — forward the SIGCONT nudge to the
//! shutdown manager (generate.rs:306-310) and drain the relay
//! (:404-407). Phase 1 (1I) fills body.

use crate::shutdown_spawn::ShutdownMode;

/// Mirror generate.rs:306-310: `systemctl --user kill --signal=SIGCONT
/// <unit>` for systemd mode, `kill -SIGCONT <pid>` for setsid mode,
/// no-op for `None`.
pub fn forward_shutdown_nudge(_mode: &ShutdownMode) {
    todo!("1I: SIGCONT nudge to shutdown manager")
}
