use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::process::Command;
use tracing;

use crate::config::SshConfig;
use crate::path::expand_tilde;
use crate::shell::shell_quote;
use crate::traits::{CommandResult, Gateway, GatewayError};

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
    config: SshConfig,
    connected: bool,
    control_path: Option<String>,
    /// PID of the persistent `ControlPersist` daemon — the
    /// reparented-to-init process that owns the control socket. `None`
    /// before connect(), after disconnect(), and when the
    /// `DYNRUNNER_SSH_CONTROL_PATH` external-master hatch is in use
    /// (the daemon belongs to an upstream driver that did not share
    /// its PID with us).
    daemon_pid: Option<u32>,
    /// Cancellation flag for the master-watcher std::thread. Set by
    /// `disconnect()` *before* tearing the daemon down so the
    /// *expected* exit doesn't surface as "died unexpectedly", and by
    /// `Drop` for the same reason on the panic / forgot-to-disconnect
    /// path. The watcher thread observes this on each 1s poll and
    /// exits silently when set.
    watcher_cancel: Arc<AtomicBool>,
    /// Handle to the master-watcher std::thread. The thread is
    /// deliberately *not* a tokio task: under PyO3+Python the
    /// `connect()` runtime is short-lived (`block_on_local` builds a
    /// fresh current-thread runtime per call and drops it on return),
    /// so a tokio-spawned watcher would be cancelled the moment
    /// connect() returns. A `std::thread` outlives any per-call
    /// runtime and only exits on `watcher_cancel` or daemon death.
    watcher_thread: Option<std::thread::JoinHandle<()>>,
    remote_home: Option<String>,
    forwarded_ports: Vec<(u16, u16)>,
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
    /// Pre-fix disconnect() unconditionally ran `ssh -O exit`,
    /// killing the user's pre-spawned master mid-dispatch and
    /// stranding the reverse-forwards along with it. asm-tokenizer
    /// Tier-2 dispatch at 394be31 hit this: SLURM jobs submitted
    /// successfully, then every secondary's connection-back failed
    /// with `connection refused` because the gateway no longer
    /// listened on the forwarded port.
    adopted: bool,
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
    fn master_only_options() -> &'static [&'static str] {
        &[
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=yes",
            "-o", "ServerAliveInterval=60",
            "-o", "ServerAliveCountMax=1080",
            "-o", "TCPKeepAlive=yes",
            "-o", "LogLevel=ERROR",
        ]
    }

    fn ssh_target(&self) -> String {
        match &self.config.user {
            Some(user) => format!("{user}@{}", self.config.host),
            None => self.config.host.clone(),
        }
    }

    fn base_ssh_args(&self) -> Vec<String> {
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
        let mut opts = Vec::new();
        if let Some(identity) = &self.config.identity_file {
            opts.extend([
                "-i".to_string(),
                identity.clone(),
                "-o".to_string(),
                "IdentitiesOnly=yes".to_string(),
                "-o".to_string(),
                "IdentityAgent=none".to_string(),
            ]);
        }
        if let Some(config_file) = &self.config.config_file {
            opts.extend(["-F".to_string(), config_file.clone()]);
        }
        opts
    }

    fn control_args(&self) -> Result<Vec<String>, GatewayError> {
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

    async fn ssh_command(
        &self,
        remote_cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let full_cmd = match cwd {
            Some(dir) => format!("cd {dir} && {remote_cmd}"),
            None => remote_cmd.to_string(),
        };

        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        for arg in self.control_args()? {
            cmd.arg(&arg);
        }
        cmd.arg(self.ssh_target());
        cmd.arg(&full_cmd);

        let output = cmd.output().await?;

        Ok(CommandResult {
            return_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    /// Expand `~` prefix using the detected remote home directory.
    /// When the home has not yet been detected (pre-connect), the path is
    /// returned verbatim — preserving the historical behavior.
    fn expand_remote_path(&self, path: &str) -> String {
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
    fn spawn_master_watcher(&mut self, daemon_pid: u32) {
        let cancel = Arc::clone(&self.watcher_cancel);
        let handle = std::thread::Builder::new()
            .name(format!("dynrunner-ssh-master-watch-{daemon_pid}"))
            .spawn(move || master_watcher_loop(daemon_pid, cancel))
            .expect("failed to spawn ssh-master-watch thread");
        self.watcher_thread = Some(handle);
    }

    async fn check_gateway_ports(&mut self) {
        for &(_, remote_port) in &self.forwarded_ports {
            let result = self
                .ssh_command(
                    &format!("ss -tulpn 2>/dev/null | grep ':{remote_port}'"),
                    None,
                )
                .await;

            match result {
                Ok(cr) if cr.success() && !cr.stdout.is_empty() => {
                    let is_public = cr.stdout.contains("0.0.0.0:")
                        || cr.stdout.contains("*:")
                        || cr.stdout.contains("[::]:");

                    if is_public {
                        tracing::info!(remote_port, "gateway port is publicly accessible");
                        self.gateway_ports_enabled = Some(true);
                    } else {
                        tracing::warn!(
                            remote_port,
                            "gateway port bound to localhost only — GatewayPorts likely disabled"
                        );
                        self.gateway_ports_enabled = Some(false);
                    }
                }
                _ => {
                    tracing::warn!(remote_port, "could not check gateway port binding");
                    self.gateway_ports_enabled = None;
                }
            }
        }
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

impl Gateway for SshGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        tracing::info!(
            host = %self.config.host,
            port = self.config.port,
            user = ?self.config.user,
            "connecting to SSH gateway"
        );

        // External control-path escape hatch: bypass our own master
        // spawn when an upstream driver pre-spawned and points us at
        // its control socket. Useful for harnesses that already manage
        // master lifetime. The `DYNRUNNER_SSH_CONTROL_PATH` env-var
        // hatch is also a workaround for the unresolved 2-min master
        // death symptom under PyO3+Python (see task #17).
        if let Ok(external_cp) = std::env::var("DYNRUNNER_SSH_CONTROL_PATH") {
            if std::path::Path::new(&external_cp).exists() {
                tracing::info!(
                    control_path = %external_cp,
                    "using external SSH master (DYNRUNNER_SSH_CONTROL_PATH); skipping our own spawn"
                );
                self.control_path = Some(external_cp.clone());
                self.connected = true;
                self.adopted = true;

                // Add any registered reverse forwards dynamically via
                // `ssh -O forward -R …`. The external master spawned
                // before our process started doesn't know about
                // forwarded_ports; OpenSSH supports adding them at
                // runtime through the control socket.
                for &(local_port, remote_port) in &self.forwarded_ports.clone() {
                    let mut cmd = Command::new("ssh");
                    for arg in self.base_ssh_args() {
                        cmd.arg(&arg);
                    }
                    cmd.args([
                        "-O",
                        "forward",
                        "-o",
                        &format!("ControlPath={external_cp}"),
                        "-R",
                        &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
                    ]);
                    cmd.arg(self.ssh_target());
                    let output = cmd.output().await?;
                    if !output.status.success() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        return Err(GatewayError::CommandFailed(format!(
                            "Failed to add reverse forward {remote_port}:localhost:{local_port} \
                             to external master: {}",
                            stderr.trim()
                        )));
                    }
                    tracing::info!(local_port, remote_port, "added reverse forward to external master");
                }

                tracing::info!("SSH master connection established");

                // Detect remote home (same as the regular path).
                let home_result = self.ssh_command("echo $HOME", None).await?;
                if home_result.success() {
                    self.remote_home = Some(home_result.stdout.trim().to_string());
                    tracing::info!(home = ?self.remote_home, "detected remote home");
                }
                self.check_gateway_ports().await;
                return Ok(());
            }
        }

        // Stable control socket under /tmp. We deliberately do NOT
        // use `tempfile::TempDir`: TempDir's Drop unlinks the parent
        // dir on `SshGateway` Drop, racing with the master's own
        // socket cleanup and stranding a master with no path back for
        // `ssh -O exit` (bug (g)). The socket file itself is unlinked
        // by the master on clean exit; on dirty exit, the daemon-PID
        // SIGTERM/SIGKILL teardown in Drop takes the master down and
        // a stale socket file is harmless (next connect generates a
        // new path).
        let cp = generate_master_control_path();
        // Pre-flight: if a stale socket from a prior crashed instance
        // happens to collide (PID + sequence makes this near-
        // impossible, but be defensive), clear it so ssh can bind.
        let _ = std::fs::remove_file(&cp);
        self.control_path = Some(cp.clone());

        // Direct tokio spawn of `ssh -M -N` — no `-f`, no `setsid`
        // indirection. NB the spawned process is the *launcher*: with
        // `ControlPersist=yes` (which we pin in master_only_options),
        // OpenSSH always forks a daemon child at end-of-handshake and
        // the launcher exits 0 within ~120ms. The daemon, reparented
        // to systemd --user / init, is the actual long-lived master.
        // We discover its PID via `ssh -O check` once the control
        // socket appears (below) and use *that* PID as the lifetime
        // anchor — `kill_on_drop`/`Child::kill` operate on the
        // launcher zombie and would no-op on the daemon.
        let argv = build_master_argv(
            &self.base_ssh_args(),
            &cp,
            &self.forwarded_ports,
            &self.ssh_target(),
        );
        let mut cmd = Command::new("ssh");
        cmd.args(&argv);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        // Note: NO `kill_on_drop(true)` — the launcher exits
        // voluntarily ~120ms post-handshake, so kill_on_drop is at
        // best a no-op (process already dead) and at worst signals a
        // PID-reuse victim. Daemon teardown goes through
        // `terminate_daemon_blocking(daemon_pid)` in Drop.

        let mut launcher = cmd.spawn().map_err(|e| {
            GatewayError::Other(format!("failed to spawn ssh master: {e}"))
        })?;
        let launcher_pid = launcher.id();

        // Wait for the control socket to appear. 10s timeout — the
        // SSH handshake usually completes in <500ms on a healthy link.
        // While waiting, also poll the launcher: if it exited with
        // *non-zero* status before the socket appeared, the handshake
        // failed — surface immediately. A *zero* exit before the
        // socket appears is benign (the daemon child created the
        // socket but a small ordering window let the launcher's
        // exit_group beat the directory entry showing up to us).
        let socket_path = std::path::Path::new(&cp);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut launcher_exited_with_failure: Option<std::process::ExitStatus> = None;
        while !socket_path.exists() {
            if let Ok(Some(status)) = launcher.try_wait()
                && !status.success()
            {
                launcher_exited_with_failure = Some(status);
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(GatewayError::CommandFailed(
                    "SSH master timed out establishing control socket (10s). \
                     Pass --ssh-config <path> for ssh_config(5) overrides if a \
                     host-key / agent / identity directive needs adjusting."
                        .into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if let Some(status) = launcher_exited_with_failure {
            return Err(GatewayError::CommandFailed(format!(
                "SSH master exited during handshake with status {status:?}. \
                 Pass --ssh-config <path> for ssh_config(5) overrides if a \
                 host-key / agent / identity directive needs adjusting."
            )));
        }

        // Discover the daemon PID via `ssh -O check`. Output shape:
        //   stdout: "Master running (pid=<N>)\n"
        // Exit status non-zero means the control socket exists but
        // doesn't respond — a real fault, not an interim handshake
        // state (we already waited for the socket to appear).
        let daemon_pid = match probe_master_pid(&cp, &self.ssh_target(), &self.base_ssh_args()).await {
            Ok(pid) => pid,
            Err(e) => {
                // Best-effort cleanup so a probe failure doesn't leak
                // the launcher / daemon. The launcher will exit on
                // its own; we *don't* know the daemon PID, so fall
                // back to `ssh -O exit` which goes via the socket and
                // lands at the daemon.
                let mut exit_cmd = Command::new("ssh");
                for arg in self.base_ssh_args() {
                    exit_cmd.arg(&arg);
                }
                exit_cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp}")]);
                exit_cmd.arg(self.ssh_target());
                let _ = exit_cmd.output().await;
                return Err(e);
            }
        };

        // Reap the launcher zombie ASAP. Reading exit status is
        // bookkeeping-only; we don't gate on it. On the rare path
        // where the launcher hasn't exited yet (control socket showed
        // up faster than the launcher's exit_group, possible on
        // loaded systems) `wait()` blocks until it does — typically
        // single-digit ms.
        let _ = launcher.wait().await;

        self.daemon_pid = Some(daemon_pid);
        self.connected = true;
        tracing::info!(
            ?launcher_pid,
            daemon_pid,
            "SSH master connection established"
        );

        // Spawn the "master died" observer: periodic existence-probe
        // on the daemon PID via `nix::sys::signal::kill(pid, None)`,
        // with a `tracing::error!` on ESRCH. Cancelled in disconnect()
        // and Drop *before* we tear the daemon down ourselves so the
        // *expected* exit doesn't surface as "died unexpectedly".
        // This is task #5's acceptance gate (3): any future
        // master-death class bug becomes observable instead of silent.
        self.spawn_master_watcher(daemon_pid);

        // Detect remote home
        let home_result = self.ssh_command("echo $HOME", None).await?;
        if home_result.success() {
            self.remote_home = Some(home_result.stdout.trim().to_string());
            tracing::info!(home = ?self.remote_home, "detected remote home");
        }

        self.check_gateway_ports().await;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        if !self.connected {
            return Ok(());
        }

        // Signal the watcher to exit silently. Anything from this
        // point forward is an *expected* teardown; the observer must
        // not log it as unexpected. The watcher thread observes the
        // flag on its next 1s poll and returns. We join the thread
        // *after* the daemon is down so it has its expected exit
        // condition (either flag-set or daemon-gone, whichever wins).
        self.watcher_cancel.store(true, Ordering::SeqCst);

        if self.adopted {
            // Adopted master: do NOT run `ssh -O exit`, that would
            // kill the upstream driver's master. Instead, undo the
            // forwards WE added during connect() so the master is
            // returned to the upstream driver in the same shape it
            // was passed to us. `ssh -O cancel -R` removes a
            // reverse-forward via the control socket; no other
            // teardown is needed (no daemon_pid for adopted, no
            // watcher_thread either).
            //
            // Cancel-failures are tolerated (warn + continue): the
            // upstream master's lifetime is not the framework's
            // concern, and a left-over forward at most consumes a
            // port on the gateway until the upstream driver exits.
            // We don't return an error from disconnect() because
            // callers treat disconnect() errors as fatal cleanup
            // failures, and that's not what an adopted-cancel
            // mishap is.
            let cp = self.control_path.clone();
            for (local_port, remote_port) in self.forwarded_ports.clone() {
                let mut cmd = Command::new("ssh");
                for arg in self.base_ssh_args() {
                    cmd.arg(&arg);
                }
                if let Some(ref cp) = cp {
                    cmd.args(["-o", &format!("ControlPath={cp}")]);
                }
                cmd.args([
                    "-O",
                    "cancel",
                    "-R",
                    &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
                ]);
                cmd.arg(self.ssh_target());
                match cmd.output().await {
                    Ok(output) if output.status.success() => {
                        tracing::info!(
                            local_port,
                            remote_port,
                            "cancelled reverse forward on adopted master"
                        );
                    }
                    Ok(output) => {
                        tracing::warn!(
                            local_port,
                            remote_port,
                            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                            "ssh -O cancel returned non-zero on adopted master; \
                             upstream driver may need to clean up the forward"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            local_port,
                            remote_port,
                            error = %e,
                            "ssh -O cancel failed to launch on adopted master"
                        );
                    }
                }
            }

            // Mark disconnected; the rest of disconnect() (daemon
            // kill ladder, watcher join) is a no-op for adopted
            // because daemon_pid is None and watcher_thread is None,
            // but skip explicitly so a future code change to either
            // path doesn't accidentally engage on adopted.
            self.connected = false;
            self.control_path = None;
            self.forwarded_ports.clear();
            return Ok(());
        }

        // Owned master path (unchanged): polite `ssh -O exit` first,
        // SIGTERM/SIGKILL ladder fallback.
        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(self.ssh_target());
        let _ = cmd.output().await;

        // Wait for the daemon to actually exit, with SIGTERM/SIGKILL
        // fallback. `terminate_daemon_blocking` is sync — fine because
        // it does at most a 200ms grace poll plus signal sends, and
        // the call site is async-aware (we already awaited
        // `ssh -O exit`'s output above so the polite path had its
        // chance). Centralising the daemon-teardown ladder here means
        // Drop and disconnect() share the same correctness contract.
        if let Some(pid) = self.daemon_pid.take() {
            terminate_daemon_blocking(pid);
        }

        // Join the watcher thread (sync — it observes the cancel
        // flag on its next 1s poll). This is bounded at ~1s.
        if let Some(thread) = self.watcher_thread.take() {
            let _ = thread.join();
        }

        self.connected = false;
        tracing::info!("SSH gateway disconnected");
        Ok(())
    }

    async fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        self.ssh_command(cmd, cwd).await
    }

    async fn transfer_file(
        &self,
        local: &Path,
        remote: &str,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand_remote_path(remote);

        // Pre-flight unlink of the destination. scp opens the dest with
        // O_WRONLY|O_TRUNC, which fails with EACCES when the existing
        // file is read-only — observed when sources are produced by a
        // nix derivation (mode 0444) and a prior scp upload propagated
        // those bits to the dest. `rm -f` no-ops when the dest is
        // absent and only requires write perm on the parent directory,
        // which scp itself already requires. Best-effort: any failure
        // is swallowed so the canonical scp error still surfaces if
        // the real cause is something else (e.g. parent-dir non-writable).
        let _ = self.ssh_command(&format!("rm -f {expanded}"), None).await;

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        for arg in self.auth_options() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(local.to_string_lossy().as_ref());
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::CopyFailed(stderr.into_owned()));
        }

        tracing::debug!(?local, remote = expanded, "file transferred via SCP");
        Ok(())
    }

    async fn download_file(
        &self,
        remote: &str,
        local: &Path,
    ) -> Result<(), GatewayError> {
        if !self.connected {
            return Err(GatewayError::NotConnected);
        }

        let expanded = self.expand_remote_path(remote);

        // Pre-migration Python's ssh_gateway did NOT pre-create the
        // local parent — scp itself would fail and the non-zero exit
        // surfaced as `RuntimeError(f"SCP download failed: ...")`. The
        // Rust port adds a defensive `mkdir -p` on the local parent;
        // route any failure through `CopyFailed` so the observed
        // exception class stays `RuntimeError`, not `OSError`.
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| GatewayError::CopyFailed(format!("SCP download failed: {e}")))?;
        }

        let mut cmd = Command::new("scp");
        if self.config.port != 22 {
            cmd.args(["-P", &self.config.port.to_string()]);
        }
        for arg in self.auth_options() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(format!("{}:{expanded}", self.ssh_target()));
        cmd.arg(local.to_string_lossy().as_ref());

        let output = cmd.output().await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GatewayError::CopyFailed(stderr.into_owned()));
        }

        tracing::debug!(remote = expanded, ?local, "file downloaded via SCP");
        Ok(())
    }

    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        let expanded = self.expand_remote_path(remote);
        let result = self
            .ssh_command(&format!("mkdir -p {expanded}"), None)
            .await?;
        if !result.success() {
            return Err(GatewayError::CommandFailed(format!(
                "mkdir -p failed: {}",
                result.stderr
            )));
        }
        Ok(())
    }

    async fn file_exists(&self, remote: &str) -> Result<bool, GatewayError> {
        let expanded = self.expand_remote_path(remote);
        let result = self
            .ssh_command(&format!("test -e {expanded}"), None)
            .await?;
        Ok(result.success())
    }

    fn setup_port_forwarding(
        &mut self,
        local_port: u16,
        remote_port: u16,
    ) -> Result<(), GatewayError> {
        if self.connected {
            return Err(GatewayError::Other(
                "cannot setup port forwarding after connection established".into(),
            ));
        }
        tracing::info!(local_port, remote_port, "configuring SSH port forwarding");
        self.forwarded_ports.push((local_port, remote_port));
        Ok(())
    }
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
/// large sequence numbers.
///
/// We deliberately avoid `tempfile::TempDir`: TempDir's Drop unlinks
/// the parent dir on `SshGateway` Drop, which raced the master's own
/// socket cleanup and stranded a master with no path back for
/// `ssh -O exit` (bug (g)). The socket file itself is unlinked by
/// the master on clean exit; on dirty exit a stale socket file is
/// harmless (next connect() generates a new path).
fn generate_master_control_path() -> String {
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
fn build_master_argv(
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
async fn probe_master_pid(
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
fn parse_master_pid(s: &str) -> Option<u32> {
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
fn master_watcher_loop(daemon_pid: u32, cancel: Arc<AtomicBool>) {
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
fn terminate_daemon_blocking(daemon_pid: u32) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ssh_config_with(
        identity_file: Option<&str>,
        config_file: Option<&str>,
    ) -> SshConfig {
        SshConfig {
            host: "h".into(),
            port: 22,
            user: None,
            identity_file: identity_file.map(str::to_string),
            config_file: config_file.map(str::to_string),
        }
    }

    #[test]
    fn auth_options_empty_when_unset() {
        let gw = SshGateway::new(ssh_config_with(None, None));
        assert!(gw.auth_options().is_empty());
    }

    #[test]
    fn auth_options_identity_file_emits_full_chain() {
        let gw = SshGateway::new(ssh_config_with(Some("/k/id_ed25519"), None));
        assert_eq!(
            gw.auth_options(),
            vec![
                "-i".to_string(),
                "/k/id_ed25519".to_string(),
                "-o".to_string(),
                "IdentitiesOnly=yes".to_string(),
                "-o".to_string(),
                "IdentityAgent=none".to_string(),
            ]
        );
    }

    #[test]
    fn auth_options_config_file_emits_dash_capital_f() {
        let gw = SshGateway::new(ssh_config_with(None, Some("/etc/ssh/cfg")));
        assert_eq!(
            gw.auth_options(),
            vec!["-F".to_string(), "/etc/ssh/cfg".to_string()],
        );
    }

    #[test]
    fn auth_options_identity_then_config_file_order() {
        // Order matches Python: -i/IdentitiesOnly/IdentityAgent first,
        // then -F. Some downstream consumers (e.g. preparation.py's
        // reverse tunnel) splat this list, so ordering is part of the
        // contract.
        let gw = SshGateway::new(ssh_config_with(Some("/k/id"), Some("/c/f")));
        assert_eq!(
            gw.auth_options(),
            vec![
                "-i".to_string(),
                "/k/id".to_string(),
                "-o".to_string(),
                "IdentitiesOnly=yes".to_string(),
                "-o".to_string(),
                "IdentityAgent=none".to_string(),
                "-F".to_string(),
                "/c/f".to_string(),
            ]
        );
    }

    /// T1: pin the 18h ServerAlive floor in the master spawn argv.
    /// Regression-pin for the anti-leak floor: any change to this
    /// must surface in code review and not be a silent override.
    #[test]
    fn master_argv_pins_18h_serveralive_floor() {
        let argv = build_master_argv(
            &Vec::new(),
            "/tmp/dynrunner-m-test.sock",
            &[],
            "user@host",
        );
        // We assert the *adjacent pair* form: each `-o` must precede
        // its value. `windows(2)` gives us each consecutive pair so we
        // can match `-o ServerAliveInterval=60`.
        let has_pair = |a: &str, b: &str| {
            argv.windows(2).any(|w| w[0] == a && w[1] == b)
        };
        assert!(
            has_pair("-o", "ServerAliveInterval=60"),
            "missing `-o ServerAliveInterval=60`; argv={argv:?}"
        );
        assert!(
            has_pair("-o", "ServerAliveCountMax=1080"),
            "missing `-o ServerAliveCountMax=1080`; argv={argv:?}"
        );
        // 60s × 1080 = 64800s = 18h. Spell the math out so any future
        // edit that changes one without the other fails this test.
        assert_eq!(60u64 * 1080, 64_800);
    }

    #[test]
    fn master_argv_includes_master_mode_flags() {
        let argv = build_master_argv(
            &Vec::new(),
            "/tmp/dynrunner-m-test.sock",
            &[],
            "user@host",
        );
        assert!(argv.contains(&"-M".to_string()));
        assert!(argv.contains(&"-N".to_string()));
        // No `-f`: even though `ControlPersist=yes` causes OpenSSH to
        // fork-and-detach a daemon at handshake-end *anyway* (we
        // track that daemon's PID via `ssh -O check`), `-f` adds an
        // unrelated effect — fork *before* full handshake — that has
        // historically masked auth failures by exiting 0 from the
        // foreground process before the failure surfaced. Pin its
        // absence so a regression doesn't silently re-introduce that
        // failure-masking behaviour.
        assert!(
            !argv.contains(&"-f".to_string()),
            "master argv must not contain `-f` (auth-failure masking); argv={argv:?}"
        );
    }

    #[test]
    fn master_argv_threads_control_path_and_target() {
        let argv = build_master_argv(
            &vec!["-p".to_string(), "2222".to_string()],
            "/tmp/dynrunner-m-42-7.sock",
            &[(1234, 5678)],
            "alice@host",
        );
        assert_eq!(argv.first().map(String::as_str), Some("-p"));
        assert_eq!(argv.get(1).map(String::as_str), Some("2222"));
        assert!(argv.contains(&"ControlPath=/tmp/dynrunner-m-42-7.sock".to_string()));
        assert!(argv.contains(&"-R".to_string()));
        assert!(argv.contains(&"0.0.0.0:5678:localhost:1234".to_string()));
        assert_eq!(argv.last().map(String::as_str), Some("alice@host"));
    }

    /// T2: pin sun_path < 108. The Linux kernel caps Unix socket paths
    /// at `sizeof(sockaddr_un.sun_path) = 108` bytes including the NUL
    /// terminator; ssh silently fails to bind if the control path
    /// exceeds. The path generator must stay well below this bound for
    /// any reasonable PID/sequence combination.
    #[test]
    fn control_path_fits_sockaddr_un() {
        let cp = generate_master_control_path();
        assert!(
            cp.len() < 108,
            "control path is {} bytes ({:?}) — must stay under 108 (sockaddr_un.sun_path cap)",
            cp.len(),
            cp,
        );
        assert!(
            cp.starts_with("/tmp/dynrunner-m-"),
            "control path must live under /tmp with the framework prefix: {cp:?}"
        );
        assert!(
            cp.ends_with(".sock"),
            "control path must end with .sock for grep-ability in operational tooling: {cp:?}"
        );
    }

    #[test]
    fn control_path_unique_across_calls() {
        // Within a single process, the AtomicU64 sequence guarantees
        // distinct paths — pin this to surface any accidental
        // simplification (e.g. fixed name) that re-introduces collision
        // races between concurrent SshGateways.
        let a = generate_master_control_path();
        let b = generate_master_control_path();
        assert_ne!(a, b);
    }

    #[test]
    fn control_path_pessimistic_pid_sequence_still_fits() {
        // Synthesise a worst-case-ish path: 7-digit PID + 19-digit
        // sequence (u64 max ≈ 1.8e19, 19 digits). Even there the
        // total is comfortably below 108 bytes.
        let synthetic = format!("/tmp/dynrunner-m-{}-{}.sock", 9_999_999u32, u64::MAX);
        assert!(
            synthetic.len() < 108,
            "even worst-case PID/seq path is {} bytes ({:?})",
            synthetic.len(),
            synthetic,
        );
    }

    /// `ssh -O check` produces `Master running (pid=<N>)` followed by
    /// a newline. Any digits after `pid=` until the first non-digit
    /// is the PID. The parser must extract it cleanly even when the
    /// line is embedded in surrounding output.
    #[test]
    fn parse_master_pid_extracts_from_canonical_output() {
        assert_eq!(parse_master_pid("Master running (pid=12345)\n"), Some(12345));
    }

    #[test]
    fn parse_master_pid_handles_leading_whitespace_and_extra_lines() {
        let out = "  Master running (pid=42)\nsomething else\n";
        assert_eq!(parse_master_pid(out), Some(42));
    }

    #[test]
    fn parse_master_pid_returns_none_when_marker_absent() {
        // Negative path: the `Stop listening request sent.` reply
        // (which ssh -O stop emits) must NOT be parsed as a PID.
        assert_eq!(parse_master_pid("Stop listening request sent.\n"), None);
        // And: missing `pid=` entirely.
        assert_eq!(parse_master_pid("Master running\n"), None);
        assert_eq!(parse_master_pid(""), None);
    }

    #[test]
    fn parse_master_pid_returns_none_on_non_numeric_pid() {
        // Defence against an OpenSSH version that prints something
        // unexpected after `pid=` — return None, surface the issue
        // up the stack as `CommandFailed`, don't fabricate a PID.
        assert_eq!(parse_master_pid("Master running (pid=abc)"), None);
    }

    #[test]
    fn parse_master_pid_rejects_overflow() {
        // u32 max = 4_294_967_295. Anything wider must yield None
        // rather than silently truncating.
        assert_eq!(
            parse_master_pid("Master running (pid=99999999999999)"),
            None
        );
    }
}
