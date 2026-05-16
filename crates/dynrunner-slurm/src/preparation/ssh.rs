//! Low-level ssh-reverse-tunnel wire-up primitives:
//! [`build_ssh_argv`] (pure argv construction),
//! [`production_spawner`] (the closure passed to
//! [`establish_one_tunnel_inner`](super::establish::establish_one_tunnel_inner)),
//! [`spawn_reverse_tunnel`] (`Command::spawn` of the ssh subprocess),
//! [`verify_tunnel_alive`] (3s sanity-check on the spawned child), and
//! [`terminate_child`] (SIGTERM, 5s wait, SIGKILL). All consumed by
//! the establishment policy in [`establish`](super::establish) and
//! the cleanup path in [`pipeline`](super::pipeline).

use std::future::Future;
use std::time::Duration;

use dynrunner_gateway::shell::shell_join;
use tokio::process::{Child, Command};

use super::options::{PrepError, PreparationOptions};

/// Build the argv for `ssh -N -R <tunnel_port>:localhost:<primary> ...`
/// per the Python implementation's shape — including the auth-options-
/// aware ProxyCommand workaround for OpenSSH 7.3+.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning
/// a real subprocess.
pub(super) fn build_ssh_argv(
    remote_host: &str,
    tunnel_port: u16,
    primary_quic_port: u16,
    opts: &PreparationOptions,
) -> Vec<String> {
    let mut argv: Vec<String> = vec!["ssh".into()];
    argv.extend(opts.auth_options.iter().cloned());

    let jump_target = match &opts.gateway_user {
        Some(u) => format!("{u}@{}", opts.gateway_host),
        None => opts.gateway_host.clone(),
    };

    if !opts.auth_options.is_empty() {
        // -J doesn't propagate -o flags into the inner ssh that
        // it spawns (OpenSSH 7.3+ regression). Use ProxyCommand
        // with the auth flags inline so the inner ssh inherits
        // them as real argv. Same flag list — single source of
        // truth lives on the gateway.
        let mut proxy_parts: Vec<String> = vec!["ssh".into()];
        proxy_parts.extend(opts.auth_options.iter().cloned());
        if opts.gateway_port != 22 {
            proxy_parts.push("-p".into());
            proxy_parts.push(opts.gateway_port.to_string());
        }
        proxy_parts.push("-W".into());
        proxy_parts.push("%h:%p".into());
        proxy_parts.push(jump_target.clone());
        argv.push("-o".into());
        argv.push(format!("ProxyCommand={}", shell_join(&proxy_parts)));
    } else {
        let jump_with_port = if opts.gateway_port != 22 {
            format!("{jump_target}:{}", opts.gateway_port)
        } else {
            jump_target.clone()
        };
        argv.push("-J".into());
        argv.push(jump_with_port);
    }

    argv.push("-R".into());
    argv.push(format!("{tunnel_port}:localhost:{primary_quic_port}"));

    for (local_port, gateway_port) in &opts.extra_port_forwards {
        argv.push("-R".into());
        argv.push(format!("{gateway_port}:localhost:{local_port}"));
    }

    // Remote user defaults to gateway_user, then "root" — matches
    // Python; the actual SLURM compute node typically isn't
    // logged into so this is the master tunnel hop's user.
    let remote_user = opts.gateway_user.as_deref().unwrap_or("root");
    argv.push(format!("{remote_user}@{remote_host}"));
    argv.extend([
        "-N".into(),
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
        // Keepalive tolerance for the per-secondary `-R` reverse-tunnel.
        // ServerAliveInterval=60 + CountMax=1080 = 18h ceiling before
        // SSH considers the session dead — matches the gateway
        // ControlMaster's floor at `dynrunner_gateway::ssh:132-134`.
        // The pre-fix values (30 × 3 = 90 s) killed the tunnel
        // mid-stream during multi-MB nar-file transfers when the
        // worker's sshd was too busy serving the transfer to PONG
        // ServerAlive within the window (asm-dataset-nix R8 LMU repro:
        // 225 MB narfile starts at HTTP 200, dies partway, retries
        // hit "Could not connect" because the `-R` listener is gone
        // — no auto-reconnect path on the SSH side).
        //
        // Detection of genuinely-dead secondaries is the framework's
        // own `primary_link_failure_threshold/_window` (cf.
        // `dynrunner-manager-distributed::secondary::mod.rs:84,92`),
        // not the ssh tunnel's keepalive. Decoupling here just means
        // the ssh tunnel doesn't get killed by transient unresponsive
        // periods that the framework wouldn't have considered fatal.
        "ServerAliveInterval=60".into(),
        "-o".into(),
        "ServerAliveCountMax=1080".into(),
        "-o".into(),
        "TCPKeepAlive=yes".into(),
    ]);
    argv
}

/// Build the production spawner closure passed into
/// [`establish_one_tunnel_inner`]. Captures `(secondary_id, opts,
/// primary_quic_port)` by move so the returned closure is `'static`
/// and the futures it produces own their data — no borrow-lifetime
/// gymnastics at the call site. Each invocation clones the captured
/// state into the produced future (retry attempts get a fresh future
/// each time).
pub(super) fn production_spawner(
    secondary_id: String,
    opts: PreparationOptions,
    primary_quic_port: u16,
) -> impl FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = Result<Child, PrepError>>>> {
    move |host: String, tunnel_port: u16| {
        let secondary_id = secondary_id.clone();
        let opts = opts.clone();
        Box::pin(async move {
            spawn_reverse_tunnel(
                &secondary_id,
                &host,
                tunnel_port,
                primary_quic_port,
                &opts,
            )
            .await
        })
    }
}

/// Spawn the ssh tunnel subprocess from `build_ssh_argv` output.
async fn spawn_reverse_tunnel(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    primary_quic_port: u16,
    opts: &PreparationOptions,
) -> Result<Child, PrepError> {
    let argv = build_ssh_argv(remote_host, tunnel_port, primary_quic_port, opts);

    tracing::info!(
        secondary_id,
        tunnel_port,
        primary_quic_port,
        extras = opts.extra_port_forwards.len(),
        "creating SSH reverse tunnel"
    );
    tracing::debug!(secondary_id, cmd = %shell_join(&argv), "ssh argv");

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    let child = cmd.spawn().map_err(PrepError::Io)?;
    Ok(child)
}

/// Verify the just-spawned ssh process stayed alive past the 3s
/// "established" gate. The corresponding Python idiom is
/// `proc.wait(timeout=3)` raising `TimeoutExpired` on success.
///
/// Operates on a `&mut Child` owned by the caller — no shared-Vec
/// lookup. With ≥2 concurrent watchers this is the only safe shape:
/// using `last_mut()` on a shared `Vec<Child>` would race watcher A
/// onto watcher B's child as soon as their `push` interleaved.
pub(super) async fn verify_tunnel_alive(
    secondary_id: &str,
    child: &mut Child,
) -> Result<(), PrepError> {
    // exit_info encodes alive/dead-with-rc:
    //   Outer None => still alive past 3s (success).
    //   Outer Some(rc_opt) => process exited; rc_opt may be None
    //     (process killed by signal, no exit code).
    let exit_info: Option<Option<i32>> =
        match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
            Err(_elapsed) => None,
            Ok(Ok(status)) => Some(status.code()),
            Ok(Err(e)) => return Err(PrepError::Io(e)),
        };

    match exit_info {
        None => {
            tracing::info!(secondary_id, "SSH tunnel established");
            Ok(())
        }
        Some(rc) => {
            // Drain stderr from the dead child for the error message.
            let stderr = {
                let mut buf = Vec::new();
                if let Some(mut e) = child.stderr.take() {
                    use tokio::io::AsyncReadExt;
                    let _ = e.read_to_end(&mut buf).await;
                }
                String::from_utf8_lossy(&buf).trim().to_string()
            };
            tracing::error!(
                secondary_id,
                rc = ?rc,
                stderr = %stderr,
                "SSH tunnel exited within 3s — forward not established"
            );
            Err(PrepError::TunnelFailed {
                secondary_id: secondary_id.to_owned(),
                rc,
                stderr,
            })
        }
    }
}

/// Send SIGTERM, wait up to 5s, then SIGKILL.
pub(super) async fn terminate_child(child: &mut Child) {
    if let Err(e) = child.start_kill() {
        // Already dead is fine; other errors are logged but don't
        // block the rest of teardown.
        tracing::debug!(error = %e, "start_kill on tunnel subprocess");
    }
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "tunnel subprocess wait failed"),
        Err(_) => {
            tracing::warn!("tunnel subprocess did not exit in 5s; force-killing");
            let _ = child.kill().await;
        }
    }
}
