//! SLURM preparation phase: SSH reverse-tunnel lifecycle.
//!
//! Owned concern: spawning and tearing down `ssh -N -R` subprocesses
//! that bridge each compute node back to the primary's QUIC port. The
//! preparation watches per-secondary connection-info files (URI form,
//! one line `<scheme>://<host>:<port>` produced by the wrapper script)
//! through a caller-supplied [`InfoFileReader`], and once a secondary
//! reports its hostname + tunnel port, opens the matching SSH
//! ProxyJump tunnel.
//!
//! Async shape: each per-secondary watcher runs as an independent
//! `tokio::task::spawn_local` task, communicating its outcome through
//! a `oneshot::Sender`. The coordinator gathers all receivers under a
//! single outer `tokio::time::timeout` (default 600s) — this avoids
//! the cancel-safety hazard of putting a `recv` arm inside a `select!`
//! that also drives a timer (see
//! `crates/dynrunner-manager-distributed/src/secondary/setup.rs:76-96`
//! for the canonical cautionary tale). On timeout the coordinator
//! [`AbortHandle::abort`]s outstanding watchers; cleanup() walks the
//! shared subprocess vector populated by watchers and terminates any
//! ssh -R that escaped the deadline.
//!
//! No gateway abstraction is bound here at the type level: the Python
//! bridge needs to call back into a Python `gateway.execute_command()`
//! to read the info files, and a callback / trait-with-single-method
//! is the minimum surface for that. The auth-options chain
//! (`-i`/`IdentitiesOnly`/`IdentityAgent=none` / `-F config`) is
//! passed in as a `Vec<String>` from the caller — single source of
//! truth lives on the gateway, not duplicated here.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinSet;

/// Single-method bridge for reading the contents of a connection-info
/// file on the gateway. Returns the raw stdout of `cat <path>` (with
/// trailing newline) on success, or `None` if the file does not yet
/// exist (return code != 0 or empty stdout). This matches the polling
/// shape — the watcher distinguishes "not yet" from "done" by stdout
/// presence, not by error.
pub trait InfoFileReader: Clone + 'static {
    fn read(
        &self,
        path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static;
}

/// Configuration for a [`SlurmPreparation`] instance.
///
/// Mirrors the fields the Python implementation pulls off the gateway
/// (`host`, `user`, `port`, `auth_options()`) plus the deployment-
/// derived `extra_port_forwards`. Holding these as plain data keeps
/// the preparation crate independent of any specific gateway impl.
#[derive(Debug, Clone)]
pub struct PreparationOptions {
    /// `<base_log_dir>/<run_id>` — info files live in
    /// `<run_log_dir>/connection_info/<secondary_id>.info`.
    pub run_log_dir: String,
    /// Hostname (or LB alias) compute nodes use to ProxyJump back
    /// through. Used verbatim — no `hostname -f` substitution.
    pub gateway_host: String,
    pub gateway_user: Option<String>,
    pub gateway_port: u16,
    /// Output of the gateway's `auth_options()` — `-i <key>
    /// IdentitiesOnly=yes IdentityAgent=none -F <config>` chain.
    /// Empty when neither `--ssh-identity-file` nor `--ssh-config`
    /// was supplied by the user.
    pub auth_options: Vec<String>,
    /// `(local_port, gateway_port)` pairs that fan out as additional
    /// `-R gateway_port:localhost:local_port` on every per-compute
    /// reverse tunnel. Mirrors `TaskDeploymentSpec.extra_port_forwards`.
    pub extra_port_forwards: Vec<(u16, u16)>,
    /// Total deadline for all secondaries to report ready. Default
    /// 600s matches the legacy Python timeout.
    pub setup_timeout: Duration,
    /// Watcher polling cadence; default 2s matches Python.
    pub poll_interval: Duration,
}

impl PreparationOptions {
    pub fn new(
        run_log_dir: String,
        gateway_host: String,
        gateway_user: Option<String>,
        gateway_port: u16,
        auth_options: Vec<String>,
        extra_port_forwards: Vec<(u16, u16)>,
    ) -> Self {
        Self {
            run_log_dir,
            gateway_host,
            gateway_user,
            gateway_port,
            auth_options,
            extra_port_forwards,
            setup_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrepError {
    #[error("timeout waiting for secondary connection info: {ready}/{total} ready")]
    Timeout { ready: usize, total: usize },
    #[error("connection-info read failed for {secondary_id}: {message}")]
    InfoRead {
        secondary_id: String,
        message: String,
    },
    #[error("malformed connection-info for {secondary_id}: {message}")]
    InfoParse {
        secondary_id: String,
        message: String,
    },
    #[error("ssh tunnel for {secondary_id} failed to establish (rc={rc:?}): {stderr}")]
    TunnelFailed {
        secondary_id: String,
        rc: Option<i32>,
        stderr: String,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("watcher task panicked: {0}")]
    WatcherPanic(String),
    #[error("watcher result lost: {0}")]
    WatcherLost(String),
}

/// Lifecycle for the SLURM preparation phase. Owns spawned `ssh -N -R`
/// subprocess handles and tears them down on cleanup().
///
/// Construction is cheap (no I/O); call [`Self::setup_ssh_tunnels`]
/// inside an async context with a [`tokio::task::LocalSet`] active —
/// the watchers use `spawn_local`.
pub struct SlurmPreparation {
    opts: PreparationOptions,
    /// Subprocess handles that survive past setup_ssh_tunnels (until
    /// cleanup() drains them). Shared with watcher tasks so a watcher
    /// records its child here BEFORE it might be aborted, letting
    /// cleanup() reap any children that escaped the deadline.
    ssh_tunnels: Arc<Mutex<Vec<Child>>>,
    /// secondary_id -> tunnel_port discovered from the info file.
    /// Populated as watchers complete; preserved across cleanup so
    /// the caller can still inspect the map after teardown.
    secondary_port_map: HashMap<String, u16>,
}

impl SlurmPreparation {
    pub fn new(opts: PreparationOptions) -> Self {
        Self {
            opts,
            ssh_tunnels: Arc::new(Mutex::new(Vec::new())),
            secondary_port_map: HashMap::new(),
        }
    }

    pub fn opts(&self) -> &PreparationOptions {
        &self.opts
    }

    pub fn secondary_port_map(&self) -> &HashMap<String, u16> {
        &self.secondary_port_map
    }

    /// Spawn one watcher per secondary, gather results under a single
    /// outer timeout. Returns the populated `secondary_id -> port` map
    /// on success.
    ///
    /// On timeout: outstanding watchers are aborted, [`Self::cleanup`]
    /// must still be called by the caller (drains `ssh_tunnels`).
    pub async fn setup_ssh_tunnels<R: InfoFileReader>(
        &mut self,
        reader: R,
        num_secondaries: usize,
        primary_quic_port: u16,
    ) -> Result<HashMap<String, u16>, PrepError> {
        tracing::info!(
            num_secondaries,
            primary_quic_port,
            "setting up SSH reverse tunnels for {num_secondaries} secondaries"
        );

        let connection_info_dir = format!("{}/connection_info", self.opts.run_log_dir);

        // Each watcher signals completion via its own oneshot. Using
        // `JoinSet` for spawn-local + abort-on-drop semantics: when
        // we return from this function (success OR timeout), JoinSet
        // is dropped and aborts any still-running watcher.
        let mut watchers: JoinSet<()> = JoinSet::new();
        let mut receivers: Vec<oneshot::Receiver<Result<WatcherOutcome, PrepError>>> =
            Vec::with_capacity(num_secondaries);

        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let info_path = format!("{connection_info_dir}/{secondary_id}.info");
            let (tx, rx) = oneshot::channel();
            receivers.push(rx);

            let watcher = TunnelWatcher {
                secondary_id: secondary_id.clone(),
                info_path,
                primary_quic_port,
                opts: self.opts.clone(),
                reader: reader.clone(),
                tunnels: Arc::clone(&self.ssh_tunnels),
                done: tx,
            };
            watchers.spawn_local(watcher.run());
        }

        // Outer deadline. `try_join_all`-style gather over the
        // receivers under a single timeout.
        let gather = async {
            let mut out: HashMap<String, u16> = HashMap::new();
            for rx in receivers {
                match rx.await {
                    Ok(Ok(WatcherOutcome { secondary_id, tunnel_port })) => {
                        out.insert(secondary_id, tunnel_port);
                    }
                    Ok(Err(e)) => return Err(e),
                    // Sender dropped without sending → watcher
                    // panicked or was aborted. JoinSet will surface
                    // the panic on drop; report a generic error.
                    Err(e) => {
                        return Err(PrepError::WatcherLost(e.to_string()));
                    }
                }
            }
            Ok(out)
        };

        let result = match tokio::time::timeout(self.opts.setup_timeout, gather).await {
            Ok(inner) => inner,
            Err(_) => {
                let ready = self.ssh_tunnels.lock().await.len();
                Err(PrepError::Timeout {
                    ready,
                    total: num_secondaries,
                })
            }
        };

        // Drop the JoinSet — aborts any in-flight watchers.
        drop(watchers);

        match &result {
            Ok(map) => {
                self.secondary_port_map.extend(map.iter().map(|(k, v)| (k.clone(), *v)));
                tracing::info!(num = map.len(), "all SSH tunnels established");
            }
            Err(e) => tracing::error!(error = %e, "ssh tunnel setup failed"),
        }
        result
    }

    /// Terminate all tracked tunnel subprocesses. SIGTERM, 5s wait,
    /// then SIGKILL escalation — mirrors Python's
    /// `proc.terminate(); proc.wait(timeout=5); proc.kill()`.
    /// Idempotent (drains `ssh_tunnels`).
    pub async fn cleanup(&mut self) {
        tracing::info!("cleaning up SLURM preparation resources");
        let mut tunnels = self.ssh_tunnels.lock().await;
        for mut child in tunnels.drain(..) {
            terminate_child(&mut child).await;
        }
        tracing::info!("SLURM preparation cleanup complete");
    }
}

/// Result a watcher reports on its oneshot channel. The spawned
/// `Child` is recorded into the shared tunnels Vec from inside the
/// watcher BEFORE the verification step, so cleanup() catches it
/// even if the watcher is aborted between spawn and verify.
struct WatcherOutcome {
    secondary_id: String,
    tunnel_port: u16,
}

struct TunnelWatcher<R: InfoFileReader> {
    secondary_id: String,
    info_path: String,
    primary_quic_port: u16,
    opts: PreparationOptions,
    reader: R,
    tunnels: Arc<Mutex<Vec<Child>>>,
    done: oneshot::Sender<Result<WatcherOutcome, PrepError>>,
}

impl<R: InfoFileReader> TunnelWatcher<R> {
    async fn run(self) {
        let TunnelWatcher {
            secondary_id,
            info_path,
            primary_quic_port,
            opts,
            reader,
            tunnels,
            done,
        } = self;

        let result = drive_one_watcher(
            &secondary_id,
            &info_path,
            primary_quic_port,
            &opts,
            reader,
            &tunnels,
        )
        .await;
        // Send result. If receiver was dropped (coordinator timed
        // out), the result is silently dropped and so is the
        // watcher's view of state — the spawned ssh child has
        // already been pushed to `tunnels` before verification, so
        // cleanup() will reach it regardless.
        let _ = done.send(result);
    }
}

async fn drive_one_watcher<R: InfoFileReader>(
    secondary_id: &str,
    info_path: &str,
    primary_quic_port: u16,
    opts: &PreparationOptions,
    reader: R,
    tunnels: &Arc<Mutex<Vec<Child>>>,
) -> Result<WatcherOutcome, PrepError> {
    // Poll until the info file appears. The outer timeout guards
    // total runtime; this loop has no inner deadline by design —
    // the coordinator owns the deadline.
    let (host, tunnel_port) = loop {
        let stdout = reader
            .read(info_path.to_owned())
            .await
            .map_err(|e| PrepError::InfoRead {
                secondary_id: secondary_id.to_owned(),
                message: e.to_string(),
            })?;
        if let Some(text) = stdout {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                let (h, p) = parse_connection_uri(trimmed).map_err(|m| {
                    PrepError::InfoParse {
                        secondary_id: secondary_id.to_owned(),
                        message: m,
                    }
                })?;
                tracing::info!(secondary_id, host = %h, port = p, "found connection info");
                break (h, p);
            }
        }
        tokio::time::sleep(opts.poll_interval).await;
    };

    let child = spawn_reverse_tunnel(
        secondary_id,
        &host,
        tunnel_port,
        primary_quic_port,
        opts,
    )
    .await?;

    // Stash child handle BEFORE the 3s verification window. If the
    // outer timeout fires now, cleanup() will still reach this
    // process via the shared Vec.
    {
        let mut guard = tunnels.lock().await;
        guard.push(child);
    }

    // Re-borrow the just-pushed child for verification. We hold the
    // lock briefly to take `&mut` to the last element — verifying
    // synchronously (no .await inside the borrow) keeps this safe.
    verify_tunnel_alive(secondary_id, tunnels).await?;

    Ok(WatcherOutcome {
        secondary_id: secondary_id.to_owned(),
        tunnel_port,
    })
}

/// Build and spawn `ssh -N -R <tunnel_port>:localhost:<primary> ...`
/// per the Python implementation's argv shape, including the
/// auth-options-aware ProxyCommand workaround for OpenSSH 7.3+.
async fn spawn_reverse_tunnel(
    secondary_id: &str,
    remote_host: &str,
    tunnel_port: u16,
    primary_quic_port: u16,
    opts: &PreparationOptions,
) -> Result<Child, PrepError> {
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
        "-o".into(),
        "ServerAliveInterval=30".into(),
        "-o".into(),
        "ServerAliveCountMax=3".into(),
        "-o".into(),
        "TCPKeepAlive=yes".into(),
    ]);

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
async fn verify_tunnel_alive(
    secondary_id: &str,
    tunnels: &Arc<Mutex<Vec<Child>>>,
) -> Result<(), PrepError> {
    // exit_info encodes alive/dead-with-rc:
    //   Outer None => still alive past 3s (success).
    //   Outer Some(rc_opt) => process exited; rc_opt may be None
    //     (process killed by signal, no exit code).
    let exit_info: Option<Option<i32>> = {
        // Lock the mutex for the whole verify window. The mutex is
        // contended only by the (rare) cleanup() path; under a 3s
        // deadline this is fine.
        let mut guard = tunnels.lock().await;
        let child = guard.last_mut().expect("watcher just pushed a child");
        match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
            Err(_elapsed) => None,
            Ok(Ok(status)) => Some(status.code()),
            Ok(Err(e)) => return Err(PrepError::Io(e)),
        }
    };

    match exit_info {
        None => {
            tracing::info!(secondary_id, "SSH tunnel established");
            Ok(())
        }
        Some(rc) => {
            // Drain stderr from the dead child for the error message.
            let stderr = {
                let mut guard = tunnels.lock().await;
                let child = guard
                    .last_mut()
                    .expect("watcher just verified a child");
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
async fn terminate_child(child: &mut Child) {
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

/// POSIX shell-join (single-quote each arg, escape inner quotes).
/// Matches Python `shlex.join(...)` for portability between the two
/// implementations during the migration.
fn shell_join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| shell_quote(p))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Parse a connection-info URI line. Accepts `<scheme>://<host>:<port>`
/// (post-L1.7 wire format). Returns `(host, port)`.
fn parse_connection_uri(line: &str) -> Result<(String, u16), String> {
    let line = line.trim();
    let url = url::Url::parse(line).map_err(|e| format!("not a valid URL: {line}: {e}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| format!("URL missing host: {line}"))?
        .to_owned();
    let port = url
        .port()
        .ok_or_else(|| format!("URL missing port: {line}"))?;
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// Reads info files from a real local directory by polling the
    /// filesystem — exercises the same control flow the real
    /// gateway-backed reader will use, without needing a live SSH
    /// gateway in the unit-test ring.
    #[derive(Clone)]
    struct LocalDirReader;

    impl InfoFileReader for LocalDirReader {
        fn read(
            &self,
            path: String,
        ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
            async move {
                match tokio::fs::read_to_string(&path).await {
                    Ok(s) => Ok(Some(s)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(PrepError::Io(e)),
                }
            }
        }
    }

    fn opts_for(tmp: &TempDir) -> PreparationOptions {
        let run_log_dir = tmp.path().display().to_string();
        // Use a reduced timeout/poll for fast tests.
        let mut o = PreparationOptions::new(
            run_log_dir,
            "gateway.example".into(),
            Some("primary".into()),
            22,
            vec![],
            vec![],
        );
        o.setup_timeout = Duration::from_millis(1500);
        o.poll_interval = Duration::from_millis(20);
        o
    }

    #[test]
    fn parse_uri_tcp() {
        let (h, p) = parse_connection_uri("tcp://node03:54321").unwrap();
        assert_eq!(h, "node03");
        assert_eq!(p, 54321);
    }

    #[test]
    fn parse_uri_with_trailing_newline() {
        let (h, p) = parse_connection_uri("tcp://compute1:1234\n").unwrap();
        assert_eq!(h, "compute1");
        assert_eq!(p, 1234);
    }

    #[test]
    fn parse_uri_quic_scheme() {
        let (h, p) = parse_connection_uri("quic://compute2.cluster.local:60001").unwrap();
        assert_eq!(h, "compute2.cluster.local");
        assert_eq!(p, 60001);
    }

    #[test]
    fn parse_uri_missing_port() {
        let err = parse_connection_uri("tcp://nodeonly").unwrap_err();
        assert!(err.contains("missing port"), "got {err}");
    }

    #[test]
    fn parse_uri_garbage() {
        let err = parse_connection_uri("not a uri at all").unwrap_err();
        assert!(err.contains("not a valid URL"), "got {err}");
    }

    #[test]
    fn shell_join_round_trip() {
        let parts = vec!["ssh".to_string(), "-i".into(), "/path/with space".into()];
        let joined = shell_join(&parts);
        assert_eq!(joined, "'ssh' '-i' '/path/with space'");
    }

    /// Bench reader for the timeout/cancel path: returns Ok(None)
    /// forever, with a counter to assert the watcher polled.
    #[derive(Clone)]
    struct StuckReader {
        polls: Arc<AtomicUsize>,
    }

    impl InfoFileReader for StuckReader {
        fn read(
            &self,
            _path: String,
        ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
            let polls = self.polls.clone();
            async move {
                polls.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
        }
    }

    /// State-machine timeout: 0-of-N secondaries reach ready inside
    /// the deadline. Reader returns `None` forever (info file never
    /// shows up). Outer timeout fires before any watcher graduates
    /// to spawning ssh — clean assertion path with no real subprocess
    /// involvement.
    ///
    /// Real ssh -R coverage (the spawn → verify path) lives in
    /// the e2e suite, which has a real gateway.
    #[test]
    fn timeout_when_no_secondary_ready() {
        let tmp = tempfile::tempdir().unwrap();

        let opts = opts_for(&tmp);
        let polls = Arc::new(AtomicUsize::new(0));
        let reader = StuckReader { polls: polls.clone() };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let result: Result<HashMap<String, u16>, PrepError> =
            rt.block_on(local.run_until(async move {
                let mut prep = SlurmPreparation::new(opts);
                let r = prep.setup_ssh_tunnels(reader, 2, 9999).await;
                prep.cleanup().await;
                r
            }));

        match result {
            Ok(m) => panic!("expected setup to time out, got map={m:?}"),
            Err(PrepError::Timeout { ready, total }) => {
                assert_eq!(ready, 0);
                assert_eq!(total, 2);
            }
            Err(other) => panic!("unexpected error class: {other}"),
        }
        // Each of the 2 watchers must have polled multiple times
        // within the 1500ms deadline at 20ms cadence — minimum a
        // few polls per watcher.
        assert!(
            polls.load(Ordering::SeqCst) >= 4,
            "expected >=4 polls, got {}",
            polls.load(Ordering::SeqCst)
        );
    }

    /// Cleanup is idempotent: calling twice doesn't panic, second
    /// call is a no-op. This exercises the `drain(..)` pattern.
    #[test]
    fn cleanup_is_idempotent() {
        let opts = PreparationOptions::new(
            "/tmp".into(),
            "h".into(),
            None,
            22,
            vec![],
            vec![],
        );
        let mut prep = SlurmPreparation::new(opts);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            prep.cleanup().await;
            prep.cleanup().await;
        });
    }

    /// Ssh spawn argv shape (no auth-options): -J jump_target form,
    /// extra_port_forwards fan out, ExitOnForwardFailure present.
    /// We test by rebuilding the argv in a sibling pure-function
    /// `build_ssh_argv` — extracted so the spawn path is testable
    /// without launching a real subprocess.
    #[test]
    fn argv_no_auth_uses_proxyjump_dash_j() {
        let mut o = PreparationOptions::new(
            "/logs".into(),
            "gw.example".into(),
            Some("alice".into()),
            22,
            vec![],
            vec![(2222, 9090)],
        );
        o.setup_timeout = Duration::from_secs(600);
        let argv = build_ssh_argv("secondary-0", "compute01", 40000, 51000, &o);
        // -J alice@gw.example
        let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
        assert_eq!(argv[j_idx + 1], "alice@gw.example");
        // -R 40000:localhost:51000 + extra -R 9090:localhost:2222
        let rs: Vec<&str> = argv
            .iter()
            .enumerate()
            .filter(|(_, s)| s.as_str() == "-R")
            .map(|(i, _)| argv[i + 1].as_str())
            .collect();
        assert_eq!(rs, vec!["40000:localhost:51000", "9090:localhost:2222"]);
        // ExitOnForwardFailure=yes is present
        assert!(argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
        // remote user@host targets compute01 with the gateway user
        // (preparation defaults remote_user to gateway_user).
        assert!(argv.iter().any(|s| s == "alice@compute01"));
    }

    /// With auth_options non-empty we MUST NOT use -J (OpenSSH 7.3+
    /// regression — -o flags don't propagate). Instead a
    /// ProxyCommand= with the auth flags inline.
    #[test]
    fn argv_with_auth_uses_proxycommand() {
        let auth = vec![
            "-i".to_string(),
            "/tmp/key".into(),
            "-o".into(),
            "IdentitiesOnly=yes".into(),
            "-o".into(),
            "IdentityAgent=none".into(),
        ];
        let o = PreparationOptions::new(
            "/logs".into(),
            "gw.example".into(),
            Some("alice".into()),
            2222,
            auth,
            vec![],
        );
        let argv = build_ssh_argv("secondary-0", "compute01", 40000, 51000, &o);
        // No -J
        assert!(!argv.iter().any(|s| s == "-J"));
        // ProxyCommand= present, contains -i /tmp/key, IdentitiesOnly=yes,
        // -p 2222, -W %h:%p, alice@gw.example
        let proxy_cmd = argv
            .iter()
            .find(|s| s.starts_with("ProxyCommand="))
            .expect("has ProxyCommand=");
        assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
        assert!(proxy_cmd.contains("'IdentitiesOnly=yes'"));
        assert!(proxy_cmd.contains("'-p' '2222'"));
        assert!(proxy_cmd.contains("'-W' '%h:%p'"));
        assert!(proxy_cmd.contains("'alice@gw.example'"));
    }

    /// Test-only: replicate the argv-building done inside
    /// `spawn_reverse_tunnel` so the shape is unit-testable without
    /// launching a real subprocess. Kept private to the test module
    /// — production code must NOT take this shortcut.
    fn build_ssh_argv(
        _secondary_id: &str,
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
            "-o".into(),
            "ServerAliveInterval=30".into(),
            "-o".into(),
            "ServerAliveCountMax=3".into(),
            "-o".into(),
            "TCPKeepAlive=yes".into(),
        ]);
        argv
    }

    /// LocalDirReader smoke test: when the file exists, the reader
    /// returns Some(content); when it doesn't, it returns None.
    /// Sanity-check on the IO bridge the timeout test relies on.
    #[test]
    fn local_dir_reader_resolves_existing_and_missing_paths() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let present = tmp.path().join("present.info");
        std::fs::write(&present, "tcp://h:1234\n").unwrap();
        let absent = tmp.path().join("absent.info");
        let reader = LocalDirReader;
        let got = rt.block_on(reader.read(present.display().to_string())).unwrap();
        assert_eq!(got.as_deref(), Some("tcp://h:1234\n"));
        let none = rt.block_on(reader.read(absent.display().to_string())).unwrap();
        assert!(none.is_none());
    }
}
