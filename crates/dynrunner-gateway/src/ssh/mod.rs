//! SSH gateway: persistent-master, multi-channel command/transfer
//! abstraction.
//!
//! Layout:
//! - [`argv`] — argv construction + control-path utilities +
//!   master-PID parser + watcher loop + kill ladder.
//! - [`connect_disconnect`] — the two largest async methods
//!   ([`SshGateway::connect_inner`] / [`SshGateway::disconnect_inner`]).
//! - [`commands`] — command exec, file transfer, port-forwarding,
//!   plus the unified `Gateway` trait impl (delegates `connect` /
//!   `disconnect` to the inherent helpers in `connect_disconnect`).
//!
//! `mod.rs` keeps the struct, the [`Drop`] impl, and the smaller
//! inherent methods (auth + argv-arg builders +
//! `spawn_master_watcher`) that every other sub-module composes
//! against.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::SshConfig;
use crate::path::expand_tilde;
use crate::traits::GatewayError;

mod argv;
mod commands;
mod connect_disconnect;

#[cfg(test)]
mod tests;

use argv::{master_watcher_loop, terminate_daemon_blocking};

/// Gateway for SSH connections using a persistent ControlMaster connection.
///
/// # Lifetime model
///
/// OpenSSH with `ControlPersist=yes` *always* forks-and-detaches at
/// the end of the handshake: the `ssh -M -N` process we spawn (the
/// "launcher") exits 0 within ~120ms via `exit_group(0)`; a daemon
/// child becomes the persistent master, reparented to `systemd --user`
/// (or PID 1 / init), in a different session. The daemon is the
/// process that responds to control-socket commands (`ssh -O exit`
/// / `ssh -O check` / per-channel ssh+scp invocations) — *not* the
/// launcher.
///
/// Tracking the launcher's `tokio::process::Child` as the master
/// lifetime anchor was the bug behind a silent regression of bug (g):
/// `Child::kill()` and `kill_on_drop(true)` operate on the launcher
/// zombie, so dropping `SshGateway` without calling `disconnect()`
/// leaked the daemon. The "master died" observer fired for the
/// launcher's normal voluntary exit ~120ms after handshake, not for
/// daemon health.
///
/// Post-fix: we discover the daemon PID via `ssh -O check` after the
/// control socket appears, reap the launcher zombie immediately, and
/// hold the daemon PID as the lifetime anchor. Drop sends SIGTERM
/// (then SIGKILL after a brief grace) to the daemon directly via
/// `nix::sys::signal::kill`. The watcher polls `kill(daemon_pid, 0)`
/// so the "master died" log fires for actual daemon death.
pub struct SshGateway {
    pub(super) config: SshConfig,
    pub(super) connected: bool,
    pub(super) control_path: Option<String>,
    /// PID of the persistent `ControlPersist` daemon — the
    /// reparented-to-init process that owns the control socket. `None`
    /// before connect(), after disconnect(), and when the
    /// `DYNRUNNER_SSH_CONTROL_PATH` external-master hatch is in use
    /// (the daemon belongs to an upstream driver that did not share
    /// its PID with us).
    pub(super) daemon_pid: Option<u32>,
    /// Cancellation flag for the master-watcher std::thread. Set by
    /// `disconnect()` *before* tearing the daemon down so the
    /// *expected* exit doesn't surface as "died unexpectedly", and by
    /// `Drop` for the same reason on the panic / forgot-to-disconnect
    /// path. The watcher thread observes this on each 1s poll and
    /// exits silently when set.
    pub(super) watcher_cancel: Arc<AtomicBool>,
    /// Handle to the master-watcher std::thread. The thread is
    /// deliberately *not* a tokio task: under PyO3+Python the
    /// `connect()` runtime is short-lived (`block_on_local` builds a
    /// fresh current-thread runtime per call and drops it on return),
    /// so a tokio-spawned watcher would be cancelled the moment
    /// connect() returns. A `std::thread` outlives any per-call
    /// runtime and only exits on `watcher_cancel` or daemon death.
    pub(super) watcher_thread: Option<std::thread::JoinHandle<()>>,
    pub(super) remote_home: Option<String>,
    pub(super) forwarded_ports: Vec<(u16, u16)>,
    /// Whether GatewayPorts is enabled on the remote SSH server.
    /// `None` = unknown, `Some(true)` = enabled, `Some(false)` = disabled.
    pub gateway_ports_enabled: Option<bool>,
    /// True when the master was ADOPTED from the
    /// `DYNRUNNER_SSH_CONTROL_PATH` escape hatch rather than spawned
    /// by us. Owned vs adopted determines disconnect() semantics:
    ///   - owned: `ssh -O exit` + SIGTERM/SIGKILL daemon ladder
    ///     (full teardown of OUR master)
    ///   - adopted: `ssh -O cancel -R` each registered forward
    ///     (undo our additions, leave the master alive for the
    ///     upstream driver that spawned it)
    ///
    /// Pre-fix disconnect() unconditionally ran `ssh -O exit`,
    /// killing the user's pre-spawned master mid-dispatch and
    /// stranding the reverse-forwards along with it. asm-tokenizer
    /// Tier-2 dispatch at 394be31 hit this: SLURM jobs submitted
    /// successfully, then every secondary's connection-back failed
    /// with `connection refused` because the gateway no longer
    /// listened on the forwarded port.
    pub(super) adopted: bool,
}

impl SshGateway {
    pub fn new(config: SshConfig) -> Self {
        Self {
            config,
            connected: false,
            control_path: None,
            daemon_pid: None,
            watcher_cancel: Arc::new(AtomicBool::new(false)),
            watcher_thread: None,
            remote_home: None,
            forwarded_ports: Vec::new(),
            gateway_ports_enabled: None,
            adopted: false,
        }
    }

    /// PID of the framework-spawned SSH master daemon, if any.
    ///
    /// Returns the *daemon* PID — the long-lived `ControlPersist`
    /// process that responds to control-socket commands — discovered
    /// via `ssh -O check` after the handshake completes. *Not* the
    /// PID of the short-lived `ssh -M -N` launcher process we
    /// `spawn()`'d, which exits ~120ms after handshake.
    ///
    /// `None` when the gateway is not connected, or when the
    /// `DYNRUNNER_SSH_CONTROL_PATH` external-master hatch is in use
    /// (the daemon belongs to an upstream driver). Primarily intended
    /// for diagnostics and the integration tests that pin the
    /// Drop-cleans-master (bug (g)) and master-died-observer contracts.
    pub fn master_pid(&self) -> Option<u32> {
        self.daemon_pid
    }

    /// Master-only `-o` flags. Pinned here (not in operator-owned
    /// ssh_config) because they're the framework's lifetime contract
    /// for the master process: liveness-floor + log-noise-suppression.
    /// `ServerAliveInterval=60 × ServerAliveCountMax=1080 = 18h`
    /// keepalive-probe budget. See the migration guide section
    /// "SSH keepalive — anti-leak floor" for the rationale.
    pub(super) fn master_only_options() -> &'static [&'static str] {
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

    pub(super) fn ssh_target(&self) -> String {
        match &self.config.user {
            Some(user) => format!("{user}@{}", self.config.host),
            None => self.config.host.clone(),
        }
    }

    pub(super) fn base_ssh_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if self.config.port != 22 {
            args.push("-p".into());
            args.push(self.config.port.to_string());
        }
        args.extend(self.auth_options());
        args
    }

    /// Explicit-auth flags applied to every ssh/scp invocation.
    ///
    /// `-i` / `IdentitiesOnly=yes` / `IdentityAgent=none` and `-F`
    /// shape *which* credentials ssh considers — orthogonal to `-p`
    /// (port), which is added per ssh/scp by [`Self::base_ssh_args`]
    /// (uses `-p`) or the per-scp builders in `transfer_file` /
    /// `download_file` (use `-P`). Exposed publicly so other
    /// framework-owned ssh subprocesses (e.g. the future Rust port of
    /// `preparation.py`'s reverse tunnel) can mirror the auth contract
    /// without bypassing the gateway.
    ///
    /// `IdentityAgent=none` is bundled with `--ssh-identity-file`
    /// because `IdentitiesOnly=yes` alone does NOT prevent over-
    /// offering on systems where `~/.ssh/config` has
    /// `Match host * → IdentityAgent <socket>` (typical
    /// NixOS+gnome-keyring/1password setups). OpenSSH still enumerates
    /// agent identities ahead of the configured key, and each
    /// enumeration counts against the gateway sshd's `MaxAuthTries` —
    /// so a many-key agent kills the connection at "Too many
    /// authentication failures" before `-i` is reached.
    /// `IdentityAgent=none` is the only flag that fully shuts the
    /// agent out (`-o` settings beat `Match` blocks). Single concern:
    /// "given an explicit identity, no agent may leak in".
    ///
    /// `--ssh-config` alone does NOT add `IdentityAgent=none` — the
    /// user's config-file is authoritative about agent behavior in
    /// that path.
    pub fn auth_options(&self) -> Vec<String> {
        auth_options_for(&self.config)
    }

    pub(super) fn control_args(&self) -> Result<Vec<String>, GatewayError> {
        let cp = self
            .control_path
            .as_ref()
            .ok_or(GatewayError::NotConnected)?;
        Ok(vec![
            "-o".into(),
            format!("ControlPath={cp}"),
            "-o".into(),
            "ControlMaster=no".into(),
        ])
    }

    pub(super) fn expand_remote_path(&self, path: &str) -> String {
        expand_tilde(path, self.remote_home.as_deref())
    }

    /// Spawn the daemon-liveness watcher on a dedicated `std::thread`.
    ///
    /// The thread polls `nix::sys::signal::kill(daemon_pid, None)`
    /// (the existence-probe form: signal 0, no actual signal sent) at
    /// 1s cadence. On `Errno::ESRCH` (process gone) it emits a
    /// `tracing::error!` so any future master-death class bug becomes
    /// observable instead of silent. On `watcher_cancel` set, it
    /// exits silently — `disconnect()` and `Drop` both raise the flag
    /// *before* tearing the daemon down so an expected exit doesn't
    /// surface as "died unexpectedly".
    ///
    /// Why `std::thread` and not `tokio::spawn`: under PyO3+Python the
    /// `connect()` runtime is short-lived — `block_on_local` builds a
    /// fresh current-thread tokio runtime per call and drops it on
    /// return, which cancels every task spawned on it. A tokio-task
    /// watcher would silently die the moment connect() returned to
    /// Python. `std::thread` is decoupled from any runtime lifecycle.
    pub(super) fn spawn_master_watcher(&mut self, daemon_pid: u32) {
        let cancel = Arc::clone(&self.watcher_cancel);
        let handle = std::thread::Builder::new()
            .name(format!("dynrunner-ssh-master-watch-{daemon_pid}"))
            .spawn(move || master_watcher_loop(daemon_pid, cancel))
            .expect("failed to spawn ssh-master-watch thread");
        self.watcher_thread = Some(handle);
    }
}

impl Drop for SshGateway {
    /// Bug (g) safety net: if `disconnect()` was not called before
    /// drop (panic path, error short-circuit between connect() and
    /// disconnect(), test that forgets to await disconnect), tear the
    /// daemon down ourselves via SIGTERM → grace → SIGKILL.
    ///
    /// `Drop` runs synchronously, so we cannot reuse the async
    /// `disconnect()` path — and we must not, because Drop may run
    /// on panic *or* on a runtime that is already being torn down.
    /// `nix::sys::signal::kill` is sync, signals are async-signal-safe,
    /// and the `kill(pid, 0)` existence probe is the canonical
    /// no-runtime-required liveness check.
    ///
    /// Order:
    ///   1. Raise `watcher_cancel` so the watcher thread exits
    ///      silently — the daemon is about to die *expectedly*.
    ///   2. SIGTERM the daemon. Poll `kill(pid, 0)` for up to 200ms.
    ///   3. SIGKILL on grace expiry.
    ///   4. Join the watcher thread (blocks at most ~1s for the next
    ///      poll tick to observe the cancel flag).
    fn drop(&mut self) {
        self.watcher_cancel.store(true, Ordering::SeqCst);
        if let Some(pid) = self.daemon_pid.take() {
            terminate_daemon_blocking(pid);
        }
        if let Some(thread) = self.watcher_thread.take() {
            // Best-effort join. The thread sees the cancel flag on
            // its next 1s poll tick. We swallow `JoinError` because
            // panicking in Drop would mask the underlying error.
            let _ = thread.join();
        }
    }
}

/// The explicit-auth ssh flag chain for `config` — the free-function
/// form of [`SshGateway::auth_options`], for framework-owned ssh
/// subprocesses that target the SAME gateway credentials WITHOUT
/// holding a connected `SshGateway` (e.g. the late-joiner observer's
/// `ssh -L` local-forward tunnels). Single source of truth: the
/// method delegates here, so the two can never drift.
pub fn auth_options_for(config: &SshConfig) -> Vec<String> {
    let mut opts = Vec::new();
    if let Some(identity) = &config.identity_file {
        opts.extend([
            "-i".to_string(),
            identity.clone(),
            "-o".to_string(),
            "IdentitiesOnly=yes".to_string(),
            "-o".to_string(),
            "IdentityAgent=none".to_string(),
        ]);
    }
    if let Some(config_file) = &config.config_file {
        opts.extend(["-F".to_string(), config_file.clone()]);
    }
    opts
}
