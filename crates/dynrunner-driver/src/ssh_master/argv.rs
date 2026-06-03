//! Argv construction + control-path utilities for the master
//! subprocess. Pure functions — no I/O beyond `std::fs::*` on the
//! control-path generator, no async, no side effects on the
//! `SshMaster` struct. Imported by `lifecycle.rs` (spawn) and by
//! the gateway/driver tests via `pub(crate)` re-exports in
//! `mod.rs`.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::SshConfig;
use crate::error::SshMasterError;

/// Master-only `-o` flags. Pinned here (not in operator-owned
/// ssh_config) because they're the framework's lifetime contract for
/// the master process: liveness-floor + log-noise-suppression.
/// `ServerAliveInterval=60 × ServerAliveCountMax=1080 = 18h`
/// keepalive-probe budget (locked design point (f)). NOT overridable
/// — operators wanting different keepalive use `adopt()` with their
/// own master.
fn master_only_options() -> &'static [&'static str] {
    &[
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPersist=yes",
        "-o",
        "ServerAliveInterval=60",
        "-o",
        "ServerAliveCountMax=1080",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "LogLevel=ERROR",
    ]
}

/// Build the auth flags (`-i`, `-F`, `IdentitiesOnly=yes`,
/// `IdentityAgent=none`) for both the master spawn and per-channel
/// ssh subprocesses. Order is part of the contract — see the
/// pre-extraction `dynrunner-gateway::ssh::SshGateway::auth_options`
/// docstring for the agent-leakage rationale.
pub(super) fn build_auth_flags(config: &SshConfig) -> Vec<String> {
    let mut opts = Vec::new();
    if let Some(identity) = &config.identity_file {
        opts.extend([
            "-i".to_string(),
            identity.to_string_lossy().into_owned(),
            "-o".to_string(),
            "IdentitiesOnly=yes".to_string(),
            "-o".to_string(),
            "IdentityAgent=none".to_string(),
        ]);
    }
    if let Some(config_file) = &config.config_file {
        opts.extend(["-F".to_string(), config_file.to_string_lossy().into_owned()]);
    }
    opts
}

/// Construct the always-on argv prefix for ssh subprocesses
/// (`-p` plus auth flags). The master spawn appends `-M -N -o
/// ControlPath=… <master_only_options> [-R …] <target>` after this;
/// per-call ssh/scp invocations from the session layer compose
/// differently.
pub(super) fn build_base_args(port: u16, auth_flags: &[String]) -> Vec<String> {
    let mut args = Vec::new();
    if port != 22 {
        args.push("-p".into());
        args.push(port.to_string());
    }
    args.extend_from_slice(auth_flags);
    args
}

/// Build the argv (excluding `ssh` itself) for the master spawn.
///
/// Pure function — pulled out so the contract (specifically the 18h
/// ServerAlive floor) is unit-testable without a live sshd.
pub(crate) fn build_master_argv(
    base_args: &[String],
    control_path: &str,
    forwarded_ports: &[(u16, u16)],
    target: &str,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();
    argv.extend(base_args.iter().cloned());
    argv.push("-M".into());
    argv.push("-N".into());
    argv.push("-o".into());
    argv.push(format!("ControlPath={control_path}"));
    for opt in master_only_options() {
        argv.push((*opt).into());
    }
    for &(local_port, remote_port) in forwarded_ports {
        argv.push("-R".into());
        argv.push(format!("0.0.0.0:{remote_port}:localhost:{local_port}"));
    }
    argv.push(target.into());
    argv
}

/// Per-process monotonic suffix for control-socket paths. Combined
/// with PID, gives a per-instance unique path under /tmp without
/// pulling in a `rand` dep. Acquire-Release isn't needed; only the
/// uniqueness matters, not memory ordering with other state.
static CONTROL_PATH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a master control socket path under `/tmp`.
///
/// Format: `/tmp/dynrunner-m-<pid>-<seq>.sock`. Stays well below the
/// 108-byte `sockaddr_un.sun_path` cap even with 7-digit PIDs and
/// 19-digit sequence numbers (locked point (e)).
pub(crate) fn generate_master_control_path() -> String {
    let pid = std::process::id();
    let seq = CONTROL_PATH_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/dynrunner-m-{pid}-{seq}.sock")
}

/// Validate that a control-socket path fits in `sockaddr_un.sun_path`.
/// The kernel cap is 108 bytes including the NUL terminator. We
/// require strictly < 108 to leave room for the NUL.
pub(super) fn validate_control_path_len(p: &Path) -> Result<(), SshMasterError> {
    use std::os::unix::ffi::OsStrExt;
    let len = p.as_os_str().as_bytes().len();
    if len < 108 {
        return Ok(());
    }
    Err(SshMasterError::adopt_failed(
        p.to_path_buf(),
        format!("control path is {len} bytes; sockaddr_un.sun_path cap is 108"),
    ))
}
