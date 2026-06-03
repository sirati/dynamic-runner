//! `SshGateway::connect_inner` and `SshGateway::disconnect_inner` —
//! the two largest async methods. The public `Gateway` trait impl
//! in `commands.rs` delegates to these inherent methods so the
//! whole trait impl stays in one file while the heavy lifting
//! lives here. Connect spawns the master (or adopts via
//! `DYNRUNNER_SSH_CONTROL_PATH`), discovers its daemon PID, and
//! spawns the watcher; disconnect tears the master down for
//! spawn-owned masters or runs per-forward `ssh -O cancel` for
//! adopt-owned masters.

use std::sync::atomic::Ordering;

use tokio::process::Command;

use crate::traits::GatewayError;

use super::SshGateway;
use super::argv::{
    build_master_argv, generate_master_control_path, probe_master_pid, terminate_daemon_blocking,
};

impl SshGateway {
    pub(super) async fn connect_inner(&mut self) -> Result<(), GatewayError> {
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
        if let Ok(external_cp) = std::env::var("DYNRUNNER_SSH_CONTROL_PATH")
            && std::path::Path::new(&external_cp).exists()
        {
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
                tracing::info!(
                    local_port,
                    remote_port,
                    "added reverse forward to external master"
                );
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

        let mut launcher = cmd
            .spawn()
            .map_err(|e| GatewayError::Other(format!("failed to spawn ssh master: {e}")))?;
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
        let daemon_pid =
            match probe_master_pid(&cp, &self.ssh_target(), &self.base_ssh_args()).await {
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

    pub(super) async fn disconnect_inner(&mut self) -> Result<(), GatewayError> {
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
}
