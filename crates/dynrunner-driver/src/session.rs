//! [`Session`] — the per-channel command-execution layer over an
//! [`SshMaster`].
//!
//! # Single concern
//!
//! Compose `ssh` / `scp` invocations against an existing
//! [`SshMaster`] without owning master lifetime. Per locked design
//! point (a):
//!
//!   `SshMaster` owns the daemon and the control path.
//!   `Session` adds command execution / scp / sftp on top.
//!
//! Future transports (mosh, aws-ssm, kubectl-exec) will eventually
//! grow their own session types implementing the same trait surface,
//! without touching master lifetime. This is the boundary that
//! enables that.
//!
//! # Holding pattern
//!
//! At extraction time the `dynrunner-gateway` crate's `SshGateway`
//! still implements its own command execution against an
//! internally-spawned master (single struct). The migration of
//! `SshGateway` to use `dynrunner-driver::Session` internally is a
//! follow-up — see locked design "Don't do" point on not refactoring
//! `SshGateway` beyond what's needed. The [`Session`] type here is
//! intentionally minimal: enough to demonstrate the two-type split
//! at the type level (the `two_type_split` test in
//! `tests/ssh_master_unit.rs`) and to host the eventual port of the
//! gateway's command-execution code without re-thinking the
//! boundary.

use std::path::Path;
use std::sync::Arc;

use crate::error::SshMasterError;
use crate::ssh_master::SshMaster;

/// Per-channel command-execution handle.
///
/// Holds an `Arc<SshMaster>` (cheap clone, shareable) and exposes
/// the operations callers used to drive against the gateway:
/// `execute_command`, `transfer_file`, `download_file`. The methods
/// are deliberately sync at the construction site; an async API can
/// layer over them via `tokio::task::spawn_blocking` if a caller
/// needs it.
///
/// **Lifetime contract**: the session does not own master lifetime.
/// Dropping the session is a no-op for the master. Callers wanting
/// the master torn down call `SshMaster::disconnect()` (or drop the
/// `SshMaster` itself for spawn-master).
pub struct Session {
    master: Arc<SshMaster>,
}

impl Session {
    /// Construct a session over an existing master.
    pub fn new(master: Arc<SshMaster>) -> Self {
        Self { master }
    }

    /// Borrow the master. The session does not extend its lifetime.
    pub fn master(&self) -> &SshMaster {
        &self.master
    }

    /// Build the per-call ssh argv prefix shared by all command
    /// invocations: master-derived auth flags + port + control path.
    ///
    /// `pub(crate)` so this module's tests can pin its shape; not
    /// part of the crate's public API surface.
    pub(crate) fn base_args(&self) -> Vec<String> {
        let mut argv = Vec::new();
        if self.master.port() != 22 {
            argv.push("-p".to_string());
            argv.push(self.master.port().to_string());
        }
        for f in self.master.auth_flags() {
            argv.push(f.clone());
        }
        argv.push("-o".to_string());
        argv.push(format!(
            "ControlPath={}",
            self.master.control_path().to_string_lossy()
        ));
        argv.push("-o".to_string());
        argv.push("ControlMaster=no".to_string());
        argv
    }

    /// Execute `cmd` on the remote, optionally `cd`-ing into `cwd`
    /// first. Returns a [`CommandOutcome`] mirroring the
    /// `dynrunner-gateway` `CommandResult` type.
    ///
    /// Sync wrapper around `std::process::Command::output()`. The
    /// existing async path lives in `dynrunner-gateway::ssh::SshGateway`
    /// and continues to work via that crate; the `Session` API here
    /// is the future home for the consolidated implementation.
    pub fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandOutcome, SshMasterError> {
        if self.master.is_invalidated() {
            return Err(self.master_died_err());
        }

        let full_cmd = match cwd {
            Some(dir) => format!("cd {dir} && {cmd}"),
            None => cmd.to_string(),
        };

        let mut c = std::process::Command::new("ssh");
        for arg in self.base_args() {
            c.arg(&arg);
        }
        c.arg(self.master.target().as_str());
        c.arg(&full_cmd);
        let out = c
            .output()
            .map_err(|e| SshMasterError::Other(format!("ssh execute_command spawn: {e}")))?;

        Ok(CommandOutcome {
            return_code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }

    /// Upload `local` to `remote` over the master's control socket
    /// (scp + ControlPath).
    pub fn transfer_file(&self, local: &Path, remote: &str) -> Result<(), SshMasterError> {
        if self.master.is_invalidated() {
            return Err(self.master_died_err());
        }
        let mut c = std::process::Command::new("scp");
        if self.master.port() != 22 {
            c.args(["-P", &self.master.port().to_string()]);
        }
        for f in self.master.auth_flags() {
            c.arg(f);
        }
        c.args([
            "-o",
            &format!(
                "ControlPath={}",
                self.master.control_path().to_string_lossy()
            ),
        ]);
        c.arg(local.to_string_lossy().as_ref());
        c.arg(format!("{}:{remote}", self.master.target().as_str()));
        let out = c
            .output()
            .map_err(|e| SshMasterError::Other(format!("scp spawn: {e}")))?;
        if !out.status.success() {
            return Err(SshMasterError::Other(format!(
                "scp upload failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    }

    /// Download `remote` to `local` over the master's control socket.
    pub fn download_file(&self, remote: &str, local: &Path) -> Result<(), SshMasterError> {
        if self.master.is_invalidated() {
            return Err(self.master_died_err());
        }
        let mut c = std::process::Command::new("scp");
        if self.master.port() != 22 {
            c.args(["-P", &self.master.port().to_string()]);
        }
        for f in self.master.auth_flags() {
            c.arg(f);
        }
        c.args([
            "-o",
            &format!(
                "ControlPath={}",
                self.master.control_path().to_string_lossy()
            ),
        ]);
        c.arg(format!("{}:{remote}", self.master.target().as_str()));
        c.arg(local.to_string_lossy().as_ref());
        let out = c
            .output()
            .map_err(|e| SshMasterError::Other(format!("scp spawn: {e}")))?;
        if !out.status.success() {
            return Err(SshMasterError::Other(format!(
                "scp download failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    }

    fn master_died_err(&self) -> SshMasterError {
        // The session doesn't have access to spawn_timestamp without
        // adding it to the SshMaster public surface; we synthesise a
        // minimal MasterDied with both timestamps as `Instant::now()`.
        // This is acceptable because callers post-invalidation
        // primarily care about the *variant*, not the timing payload.
        let now = std::time::Instant::now();
        SshMasterError::MasterDied {
            target: self.master.target().clone(),
            last_known_pid: self.master.master_pid(),
            spawn_timestamp: now,
            observation_timestamp: now,
        }
    }
}

/// Result of a command executed via [`Session::execute_command`].
/// Mirrors `dynrunner-gateway::CommandResult` so callers porting from
/// the gateway have a familiar shape.
#[derive(Debug, Clone)]
pub struct CommandOutcome {
    pub return_code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandOutcome {
    pub fn success(&self) -> bool {
        self.return_code == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssh_target::SshTarget;

    #[test]
    fn session_holds_master_via_arc() {
        // Smoke: constructing a Session over an Arc<SshMaster> is
        // well-typed — we don't need a real master instance because
        // all we want is the type-level guarantee. We construct a
        // dummy adopt-master-shaped value via the public surface
        // would require a live socket; instead, this test simply
        // pins that `Session::new` accepts `Arc<SshMaster>` and
        // exposes `master()`. The two-type split is asserted in
        // the integration test `two_type_split`.
        // (Compile-only test.)
        fn _types(m: Arc<SshMaster>) -> Session {
            Session::new(m)
        }
        // Avoid warning about unused fn:
        let _ = _types;
        let _ = SshTarget::new("u@h"); // touch the import
    }
}
