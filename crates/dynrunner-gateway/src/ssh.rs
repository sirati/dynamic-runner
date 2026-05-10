use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing;

use crate::config::SshConfig;
use crate::filesystem::{DirEntry, Filesystem, FsError};
use crate::path::expand_tilde;
use crate::shell::shell_quote;
use crate::traits::{CommandResult, Gateway, GatewayError};

/// Gateway for SSH connections using a persistent ControlMaster connection.
pub struct SshGateway {
    config: SshConfig,
    connected: bool,
    control_path: Option<String>,
    /// Master `ssh -M -N` child retained as a tokio process handle,
    /// shared with `master_watch`. `kill_on_drop(true)` ensures Drop
    /// without `disconnect()` does not leak the master (bug (g)).
    /// Wrapped in `Arc<Mutex<Option<Child>>>` so the watcher task and
    /// `disconnect()` can both call `try_wait` / `kill` without
    /// fighting over the `&mut Child` exclusive borrow. `None` as the
    /// inner value when the external `DYNRUNNER_SSH_CONTROL_PATH`
    /// hatch is in use — the master belongs to an upstream driver in
    /// that case.
    master_child: Arc<Mutex<Option<Child>>>,
    /// Background task that polls `master_child.try_wait()` and emits
    /// a `tracing::error!` if the master exits unexpectedly. Aborted
    /// in `disconnect()` before we tear the master down ourselves so
    /// the *expected* exit doesn't surface as "died unexpectedly".
    master_watch: Option<JoinHandle<()>>,
    remote_home: Option<String>,
    forwarded_ports: Vec<(u16, u16)>,
    /// Whether GatewayPorts is enabled on the remote SSH server.
    /// `None` = unknown, `Some(true)` = enabled, `Some(false)` = disabled.
    pub gateway_ports_enabled: Option<bool>,
}

impl SshGateway {
    pub fn new(config: SshConfig) -> Self {
        Self {
            config,
            connected: false,
            control_path: None,
            master_child: Arc::new(Mutex::new(None)),
            master_watch: None,
            remote_home: None,
            forwarded_ports: Vec::new(),
            gateway_ports_enabled: None,
        }
    }

    /// PID of the framework-spawned SSH master, if any.
    ///
    /// Returns `None` when the gateway is not connected, when the
    /// `DYNRUNNER_SSH_CONTROL_PATH` external-master hatch is in use
    /// (the master PID belongs to an upstream driver that did not
    /// share it with us), or when the underlying `tokio::process::Child`
    /// has already been reaped. Primarily intended for diagnostics
    /// and the integration tests that pin the Drop-cleans-master
    /// (bug (g)) and master-died-observer contracts.
    pub async fn master_pid(&self) -> Option<u32> {
        let slot = self.master_child.lock().await;
        slot.as_ref().and_then(|c| c.id())
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

    /// Spawn the periodic `try_wait()` poller on the retained master
    /// child. The poller shares ownership of the `Child` via the
    /// `Arc<Mutex<Option<Child>>>`; on observing an exit it logs and
    /// returns. Aborted by `disconnect()` *before* it tears the master
    /// down, so a clean teardown does NOT log "exited unexpectedly".
    fn spawn_master_watcher(&mut self) {
        let child_slot = Arc::clone(&self.master_child);
        let handle = tokio::spawn(async move {
            // Poll cadence: 1s. Coarse enough to be near-free, fine
            // enough that "master died ~2 min after handshake" is
            // observed within the same minute.
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(1));
            // First tick fires immediately; skip it so we don't race
            // the handshake-completion path in connect().
            interval.tick().await;
            loop {
                interval.tick().await;
                let mut slot = child_slot.lock().await;
                let Some(child) = slot.as_mut() else {
                    // Handle was taken by disconnect(); we're done.
                    return;
                };
                // Read the PID *before* `try_wait()`: tokio nulls
                // out `Child::id()` after reaping so the post-reap
                // value is `None`, which makes the diagnostic log
                // less useful.
                let pid = child.id();
                match child.try_wait() {
                    Ok(Some(status)) => {
                        tracing::error!(
                            pid = ?pid,
                            exit_status = ?status,
                            "SSH master exited unexpectedly"
                        );
                        return;
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(
                            pid = ?pid,
                            error = %e,
                            "SSH master try_wait failed; stopping observer"
                        );
                        return;
                    }
                }
            }
        });
        self.master_watch = Some(handle);
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
    /// disconnect(), test that forgets to await disconnect), abort
    /// the master-watcher first. Without this, the watcher task
    /// keeps an Arc reference to `master_child` alive — the inner
    /// `Child`'s `kill_on_drop(true)` never fires until the watcher
    /// itself terminates, leaving the master process running.
    ///
    /// Aborting the watcher drops its Arc, leaving `Self`'s Arc as
    /// the sole owner. When `Self`'s field then drops, the inner
    /// `Mutex<Option<Child>>` drops, the `Child` drops, and tokio
    /// sends SIGKILL.
    fn drop(&mut self) {
        if let Some(watch) = self.master_watch.take() {
            watch.abort();
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
        // by the master on clean exit; on dirty exit, kill_on_drop
        // takes the master down and a stale socket file is harmless
        // (next connect generates a new path).
        let cp = generate_master_control_path();
        // Pre-flight: if a stale socket from a prior crashed instance
        // happens to collide (PID + sequence makes this near-
        // impossible, but be defensive), clear it so ssh can bind.
        let _ = std::fs::remove_file(&cp);
        self.control_path = Some(cp.clone());

        // Direct tokio spawn of `ssh -M -N` — no `-f`, no `setsid`
        // indirection. The `SshGateway`'s lifetime IS the master's
        // lifetime. Reparenting to init was a workaround for "we don't
        // want to manage the lifetime", which we now do.
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
        // Bug (g) fix: if SshGateway drops without disconnect()
        // running first (panic, error-return between connect() and
        // disconnect()), tokio sends SIGKILL to the master so we
        // don't leak orphans.
        cmd.kill_on_drop(true);

        let child = cmd.spawn().map_err(|e| {
            GatewayError::Other(format!("failed to spawn ssh master: {e}"))
        })?;
        let master_pid = child.id();
        {
            let mut slot = self.master_child.lock().await;
            *slot = Some(child);
        }

        // Wait for the control socket to appear. 10s timeout — the
        // SSH handshake usually completes in <500ms on a healthy link.
        let socket_path = std::path::Path::new(&cp);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !socket_path.exists() {
            // If the master died during handshake, surface that
            // immediately rather than waiting out the full 10s.
            {
                let mut slot = self.master_child.lock().await;
                if let Some(child) = slot.as_mut()
                    && let Ok(Some(status)) = child.try_wait()
                {
                    return Err(GatewayError::CommandFailed(format!(
                        "SSH master exited during handshake with status {status:?}. \
                         Pass --ssh-config <path> for ssh_config(5) overrides if a \
                         host-key / agent / identity directive needs adjusting."
                    )));
                }
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

        self.connected = true;
        tracing::info!(?master_pid, "SSH master connection established");

        // Spawn the "master died" observer: periodic try_wait on the
        // retained child, with a tracing::error! on unexpected exit.
        // Aborted in disconnect() before we tear the master down
        // ourselves so the *expected* exit doesn't surface as
        // "died unexpectedly". This is task #5's acceptance gate (3):
        // any future master-death class bug becomes observable
        // instead of silent.
        self.spawn_master_watcher();

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

        // Stop the "master died" observer first. Anything from this
        // point forward is an *expected* teardown; the observer must
        // not log it as unexpected. Abort + drop the JoinHandle is
        // sufficient — the task only borrows the master_child slot
        // briefly, so abort takes effect immediately.
        if let Some(watch) = self.master_watch.take() {
            watch.abort();
        }

        // Politely ask the master to exit via `ssh -O exit` first,
        // which cleans up the control socket and reverse forwards in
        // an orderly fashion.
        let mut cmd = Command::new("ssh");
        for arg in self.base_ssh_args() {
            cmd.arg(&arg);
        }
        if let Some(cp) = &self.control_path {
            cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp}")]);
        }
        cmd.arg(self.ssh_target());
        let _ = cmd.output().await;

        // If the child is still alive after a short grace, SIGKILL
        // via `Child::kill`. We own the master's lifetime now (no `-f`
        // daemonisation, no setsid reparent), so this is a guaranteed
        // teardown.
        let mut slot = self.master_child.lock().await;
        if let Some(mut child) = slot.take() {
            // Up to 1s grace for `ssh -O exit` to land — should be
            // <100ms in practice but we don't want to race the
            // OpenSSH master's own teardown timing.
            let grace = std::time::Instant::now()
                + std::time::Duration::from_millis(1000);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => {
                        if std::time::Instant::now() >= grace {
                            let _ = child.kill().await;
                            let _ = child.wait().await;
                            break;
                        }
                        tokio::time::sleep(
                            std::time::Duration::from_millis(20),
                        )
                        .await;
                    }
                    Err(_) => {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        break;
                    }
                }
            }
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
    // -M: master mode. -N: no remote command. NO `-f`: we manage the
    // master's lifetime via the retained tokio::process::Child, not
    // by daemonising into init.
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

/// Parse the null-delimited output of
/// `find <dir> -mindepth 1 -maxdepth 1 -L -printf "%y\t%s\t%P\0"`
/// into [`DirEntry`] values. The `-L` flag dereferences symlinks so `%y`
/// reports the target kind; broken symlinks come back as `%y=l` and are
/// skipped silently.
fn parse_find_printf(stdout: &str) -> Vec<DirEntry> {
    let mut out = Vec::new();
    for record in stdout.split('\0') {
        if record.is_empty() {
            continue;
        }
        let mut fields = record.splitn(3, '\t');
        let kind = fields.next().unwrap_or("");
        let size_str = fields.next().unwrap_or("0");
        let name = match fields.next() {
            Some(n) if !n.is_empty() => n.to_owned(),
            _ => continue,
        };
        match kind {
            "d" => out.push(DirEntry::Dir { name }),
            "f" => {
                let size: u64 = size_str.parse().unwrap_or(0);
                out.push(DirEntry::File { name, size });
            }
            // 'l' = broken symlink (under -L); other kinds (sockets, fifos,
            // block/char devices) are filtered out the same way.
            _ => {}
        }
    }
    out.sort_by(|a, b| a.name().cmp(b.name()));
    out
}

impl Filesystem for SshGateway {
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        if !self.connected {
            return Err(FsError::NotConnected);
        }
        let expanded = self.expand_remote_path(path);
        let quoted = shell_quote(&expanded);

        // -L follows symlinks for stat: %y reports the target's kind.
        // It's a global option and MUST appear before the path (POSIX +
        // GNU) — placed after the path it parses as a predicate and
        // find exits 1 with "unknown predicate `-L'".
        // %P is the path relative to the find root, which with maxdepth=1
        // is just the entry's basename. \0 separator survives names with
        // newlines/tabs.
        let cmd = format!(
            "find -L {quoted} -mindepth 1 -maxdepth 1 -printf '%y\\t%s\\t%P\\0'"
        );
        let result = self
            .ssh_command(&cmd, None)
            .await
            .map_err(|e| FsError::Other(format!("ssh exec failed: {e}")))?;

        if !result.success() {
            let stderr = result.stderr.trim();
            if stderr.contains("No such file or directory") {
                return Err(FsError::NotFound(expanded));
            }
            if stderr.contains("Not a directory") {
                return Err(FsError::NotADirectory(expanded));
            }
            return Err(FsError::ListingFailed(format!(
                "find exited {}: {stderr}",
                result.return_code,
            )));
        }

        Ok(parse_find_printf(&result.stdout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_find_printf_basic() {
        let stdout = "f\t100\tfile.bin\0d\t4096\tsubdir\0l\t0\tbroken\0";
        let entries = parse_find_printf(stdout);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], DirEntry::File { name: "file.bin".into(), size: 100 });
        assert_eq!(entries[1], DirEntry::Dir { name: "subdir".into() });
    }

    #[test]
    fn parse_find_printf_alphabetical() {
        let stdout = "f\t1\tz\0f\t2\ta\0d\t0\tm\0";
        let entries = parse_find_printf(stdout);
        let names: Vec<_> = entries.iter().map(|e| e.name()).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn parse_find_printf_handles_tabs_in_names() {
        // Tabs inside names: splitn(3) means everything after the second
        // tab is the name. So a name with a tab is preserved.
        let stdout = "f\t10\tname\twith\ttab\0";
        let entries = parse_find_printf(stdout);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name(), "name\twith\ttab");
    }

    #[test]
    fn parse_find_printf_empty() {
        assert!(parse_find_printf("").is_empty());
        assert!(parse_find_printf("\0\0").is_empty());
    }

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
        // No `-f`: we manage the master's lifetime via the retained
        // tokio Child. Pin this so a regression doesn't silently
        // re-introduce daemonisation and orphan masters again.
        assert!(
            !argv.contains(&"-f".to_string()),
            "master argv must not contain `-f` (we own the lifetime); argv={argv:?}"
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
}
