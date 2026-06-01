//! Single concern: the FIFO command-relay service (generate.rs:645-693).
//! HARD external contract — the response-line format
//! `output_N.sock,exit_N.sock,signal_N.sock,<pid>` and the per-command
//! socket naming are consumed by an out-of-repo client. Phase 1 (1F)
//! fills bodies.

use crate::dirs::Layout;

/// Handle to the running relay task. `shutdown` mirrors the bash trap's
/// `kill -TERM $CMD_RELAY_PID; wait` (generate.rs:404-407).
pub struct RelayHandle {
    // Phase 1 (1F) defines internals (task handle, fifo paths).
}

/// `mkfifo` the command socket + response FIFO and spawn the relay loop
/// as a background task.
pub fn spawn(_layout: &Layout) -> std::io::Result<RelayHandle> {
    todo!("1F: mkfifo cmd.sock + relay loop task")
}

impl RelayHandle {
    /// Terminate the relay and wait for it to drain.
    pub async fn shutdown(self) {
        todo!("1F: terminate relay + drain")
    }
}
