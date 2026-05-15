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

use dynrunner_gateway::shell::shell_join;
use tokio::process::{Child, Command};
use std::sync::Mutex as StdMutex;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::peer_info::{parse as parse_peer_info, PeerInfoRecord};

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
    /// Policy for the establishment-phase rate-limiter + retry. See
    /// [`EstablishmentPolicy`] for field meanings; default values are
    /// safe for the LMU CIP gateway load-balancer (max 4 concurrent
    /// handshakes, 3 attempts at [0, 5s, 15s] backoff, 90s per-tunnel
    /// wall-clock cap).
    pub establishment: EstablishmentPolicy,
}

/// Policy governing one tunnel's establishment phase — the window
/// between "info file shows up" and "ssh -R verified alive past the
/// 3s gate". Owns:
///
/// 1. **Concurrency cap** (`max_concurrent`): how many `ssh -N -R`
///    handshakes may be in flight at the same instant across all
///    watchers in a single `setup_ssh_tunnels` call. The coordinator
///    materialises this as a [`Semaphore`] shared across watchers;
///    each watcher acquires one permit just before `Command::spawn`
///    and releases it once the 3s verify gate has resolved (success
///    or failure).
///
///    Why: LMU's gateway is a load-balancer alias dispatching across
///    several physical hosts, each running OpenSSH with `MaxStartups`
///    typically defaulting to 10:30:100 (random-drop above 10 unauth'd
///    handshakes). With 15 secondaries all racing to handshake
///    simultaneously, ~1/15 ssh subprocesses intermittently sees
///    `Connection closed by ... rc=255` because the gateway sshd
///    randomly dropped the connection. Capping at 4 keeps the
///    in-flight handshake count well below the default budget while
///    still parallelising the bulk of setup.
///
/// 2. **Retry budget** (`attempts`, `backoff`): per-tunnel retry on
///    handshake-time failure. Only `PrepError::TunnelFailed` is
///    retried (info-parse / IO errors are non-recoverable). After
///    `attempts` total tries the last failure surfaces unchanged.
///    `backoff[i]` is the sleep BEFORE attempt `i+1` (so `backoff[0]`
///    sits between attempts 1 and 2).
///
/// 3. **Per-tunnel wall-clock cap** (`per_tunnel_timeout`): a single
///    establishment (across all retries + their inter-attempt sleeps)
///    is bounded by this deadline so a single chronically-failing
///    tunnel can't soak the whole 600s outer budget. Default 90s
///    matches the consumer requirement.
#[derive(Debug, Clone)]
pub struct EstablishmentPolicy {
    pub max_concurrent: usize,
    pub attempts: usize,
    /// Inter-attempt sleeps. `len() = attempts - 1` is the canonical
    /// shape; if shorter the policy reuses the last entry, if longer
    /// the tail is ignored. Empty disables retry waits.
    pub backoff: Vec<Duration>,
    pub per_tunnel_timeout: Duration,
}

impl EstablishmentPolicy {
    /// Backoff sleep BEFORE attempt index `attempt` (0-based). Returns
    /// `None` for `attempt == 0` (no pre-sleep on the first try).
    /// Indexing semantics: `attempt = i` sleeps `backoff[i - 1]`,
    /// clamping at the last element if `backoff` is shorter.
    pub fn backoff_before(&self, attempt: usize) -> Option<Duration> {
        if attempt == 0 {
            return None;
        }
        let idx = attempt - 1;
        self.backoff
            .get(idx)
            .or_else(|| self.backoff.last())
            .copied()
    }
}

impl Default for EstablishmentPolicy {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            attempts: 3,
            backoff: vec![Duration::from_secs(5), Duration::from_secs(15)],
            per_tunnel_timeout: Duration::from_secs(90),
        }
    }
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
            establishment: EstablishmentPolicy::default(),
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
    /// the caller can still inspect the map after teardown. Wrapped
    /// in `Arc<StdMutex<_>>` so per-tunnel watcher tasks (spawned as
    /// `'static` futures) can clone-and-share the same map, and so
    /// `establish_one_tunnel` can run under `&self` and still record
    /// its outcome — the alternative (`&mut self`) would forbid
    /// concurrent respawn callers from sharing the same manager.
    /// `std::sync::Mutex` (not the tokio variant) because the lock
    /// is never held across an await point.
    secondary_port_map: Arc<StdMutex<HashMap<String, u16>>>,
    /// Shared establishment-phase permit pool. Bounds the number of
    /// in-flight `ssh -N -R` handshakes across all paths that establish
    /// tunnels on this instance — the initial `setup_ssh_tunnels`
    /// loop AND any later `establish_one_tunnel` respawn calls. Built
    /// once at `new()` from `opts.establishment.max_concurrent` so the
    /// rate cap is a per-manager invariant, not a per-call accident.
    /// See `EstablishmentPolicy` for why the cap exists (LMU gateway
    /// load-balancer `MaxStartups` random-drop).
    establish_pool: Arc<Semaphore>,
    /// The primary's QUIC port — destination of every reverse tunnel.
    /// Captured at `setup_ssh_tunnels` entry so per-secondary respawn
    /// callers (`establish_one_tunnel`) can build the same `-R` mapping
    /// without re-threading the value through their call site. `None`
    /// until the first `setup_ssh_tunnels` call records it.
    primary_quic_port: StdMutex<Option<u16>>,
}

impl SlurmPreparation {
    pub fn new(opts: PreparationOptions) -> Self {
        let establish_pool =
            Arc::new(Semaphore::new(opts.establishment.max_concurrent.max(1)));
        Self {
            opts,
            ssh_tunnels: Arc::new(Mutex::new(Vec::new())),
            secondary_port_map: Arc::new(StdMutex::new(HashMap::new())),
            establish_pool,
            primary_quic_port: StdMutex::new(None),
        }
    }

    pub fn opts(&self) -> &PreparationOptions {
        &self.opts
    }

    /// Snapshot of the `secondary_id -> tunnel_port` map. Cloned under
    /// the mutex; the returned `HashMap` is independent of any
    /// subsequent mutations. Synchronous because the underlying
    /// `StdMutex` is never held across an await point.
    pub fn secondary_port_map(&self) -> HashMap<String, u16> {
        self.secondary_port_map
            .lock()
            .expect("secondary_port_map mutex poisoned")
            .clone()
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

        // Capture the primary's QUIC port for later per-secondary
        // respawn calls (`establish_one_tunnel`) — they need the same
        // `-R <tunnel>:localhost:<primary_quic>` mapping as the initial
        // setup. Overwrites any prior value: in practice this is only
        // called once per run, but a re-entry would be against the
        // current QUIC port, not a stale one.
        *self
            .primary_quic_port
            .lock()
            .expect("primary_quic_port mutex poisoned") = Some(primary_quic_port);

        // Each watcher signals completion via its own oneshot. Using
        // `JoinSet` for spawn-local + abort-on-drop semantics: when
        // we return from this function (success OR timeout), JoinSet
        // is dropped and aborts any still-running watcher.
        let mut watchers: JoinSet<()> = JoinSet::new();
        let mut receivers: Vec<
            oneshot::Receiver<Result<(String, u16), PrepError>>,
        > = Vec::with_capacity(num_secondaries);

        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let (tx, rx) = oneshot::channel();
            receivers.push(rx);

            // `establish_one_tunnel` body, lifted into a `'static`
            // future via Arc-cloned state. Same code path the public
            // `establish_one_tunnel` method takes — see its doc for
            // the per-tunnel contract.
            let info_path = format!(
                "{}/connection_info/{secondary_id}.info",
                self.opts.run_log_dir
            );
            let opts = self.opts.clone();
            let reader_clone = reader.clone();
            let tunnels = Arc::clone(&self.ssh_tunnels);
            let establish_pool = Arc::clone(&self.establish_pool);
            // `Arc<StdMutex<...>>` field lets per-watcher tasks share
            // the persistent port map without borrowing `self` (they
            // live on the JoinSet past the borrow).
            let port_map = Arc::clone(&self.secondary_port_map);
            let id_for_task = secondary_id.clone();
            watchers.spawn_local(async move {
                let spawner = production_spawner(id_for_task.clone(), opts.clone(), primary_quic_port);
                let res = establish_one_tunnel_inner(
                    &id_for_task,
                    &info_path,
                    primary_quic_port,
                    &opts,
                    reader_clone,
                    &tunnels,
                    &port_map,
                    &establish_pool,
                    spawner,
                )
                .await;
                let outcome = res.map(|port| (id_for_task.clone(), port));
                // Send result. If receiver was dropped (coordinator timed
                // out), the outcome is silently dropped. Children that
                // passed verification are already in the shared `tunnels`
                // Vec for cleanup(); children that didn't reach
                // verification were owned locally inside
                // establish_one_tunnel_inner and dropped there (SIGKILL
                // via `kill_on_drop`).
                let _ = tx.send(outcome);
            });
        }

        // Outer deadline. `try_join_all`-style gather over the
        // receivers under a single timeout.
        let gather = async {
            let mut out: HashMap<String, u16> = HashMap::new();
            for rx in receivers {
                match rx.await {
                    Ok(Ok((secondary_id, tunnel_port))) => {
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
                // Per-tunnel inner already wrote each (id, port) into
                // the persistent port map under the StdMutex; nothing
                // to extend here. The returned `map` is the per-call
                // snapshot built from oneshot outcomes — by construction
                // it equals what the inner wrote.
                tracing::info!(num = map.len(), "all SSH tunnels established");
            }
            Err(e) => tracing::error!(error = %e, "ssh tunnel setup failed"),
        }
        result
    }

    /// Establish ONE reverse tunnel for a just-spawned secondary,
    /// reusing the same `ssh_tunnels` Vec and rate-limiter pool that
    /// the initial [`Self::setup_ssh_tunnels`] used. Intended for the
    /// respawn path: a single compute node has just re-checked in via
    /// its info file, and the framework needs its tunnel up without
    /// re-running the whole N-secondary setup loop.
    ///
    /// Polls the info file with the configured `poll_interval`, spawns
    /// `ssh -N -R`, verifies past the 3s alive-gate (with the
    /// `EstablishmentPolicy` retry budget + rate-limiter applied), and
    /// pushes the verified `Child` into the shared `ssh_tunnels` Vec.
    /// On success, the discovered `tunnel_port` is recorded in
    /// `secondary_port_map`.
    ///
    /// Precondition: [`Self::setup_ssh_tunnels`] must have been called
    /// at least once on this instance — it captures the primary's QUIC
    /// port, which this method reuses as the `-R` target. Calling
    /// before initial setup returns [`PrepError::TunnelFailed`] with a
    /// "primary QUIC port not yet known" message rather than panicking.
    ///
    /// Caller cleanup: same as `setup_ssh_tunnels` — children added
    /// here are drained by [`Self::cleanup`] on teardown. There is no
    /// dedicated outer timeout for this method; the per-tunnel
    /// wall-clock cap in [`EstablishmentPolicy::per_tunnel_timeout`]
    /// bounds the establishment phase, and the info-file poll loop
    /// runs until success or caller-side abort (drop the future).
    pub async fn establish_one_tunnel<R: InfoFileReader>(
        &self,
        secondary_id: &str,
        reader: R,
    ) -> Result<(), PrepError> {
        let primary_quic_port = self
            .primary_quic_port
            .lock()
            .expect("primary_quic_port mutex poisoned")
            .ok_or_else(|| PrepError::TunnelFailed {
                secondary_id: secondary_id.to_owned(),
                rc: None,
                stderr:
                    "primary QUIC port not yet known — call setup_ssh_tunnels at least once first"
                        .to_owned(),
            })?;

        let info_path = format!(
            "{}/connection_info/{secondary_id}.info",
            self.opts.run_log_dir
        );

        let opts = self.opts.clone();
        let tunnels = Arc::clone(&self.ssh_tunnels);
        let establish_pool = Arc::clone(&self.establish_pool);
        let port_map = Arc::clone(&self.secondary_port_map);
        let id_owned = secondary_id.to_owned();

        let spawner = production_spawner(id_owned.clone(), opts.clone(), primary_quic_port);
        let _port = establish_one_tunnel_inner(
            &id_owned,
            &info_path,
            primary_quic_port,
            &opts,
            reader,
            &tunnels,
            &port_map,
            &establish_pool,
            spawner,
        )
        .await?;
        Ok(())
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

/// Per-tunnel work shared by `setup_ssh_tunnels` (run in parallel
/// across N secondaries inside a JoinSet) and `establish_one_tunnel`
/// (run once for a single respawn).
///
/// Single concern: take one secondary from "info file may or may not
/// be there yet" to "verified ssh -R subprocess in the shared cleanup
/// set, with `(id, port)` recorded in the shared port map". Returns
/// the discovered `tunnel_port` on success.
///
/// Spawner DI: the `spawner` closure is parameterised over
/// `(host: String, tunnel_port: u16) -> Future<Result<Child, PrepError>>`.
/// Production callers pass a closure that builds the `ssh -N -R` argv
/// and runs `Command::spawn`; tests pass a closure that returns a
/// `/bin/sh` child with a configurable outcome, exercising the
/// push-to-Vec / port-map / rate-limiter control flow without ever
/// touching ssh. `String` (not `&str`) so the closure's returned
/// future can own its inputs without borrow-lifetime contortions.
///
/// Cancel-safety: the `Command::spawn` inside the spawner sets
/// `kill_on_drop(true)`, so an outer abort drops the in-progress Child
/// and SIGKILL fires. The Semaphore permit acquired inside
/// `establish_tunnel` is released on drop — no manual re-balance.
//
// `too_many_arguments` is intentional — this helper sits at the
// crate-internal seam between the per-instance manager (which owns
// the shared state) and the per-attempt policy engine. Bundling the
// state into a struct would just shift the verbosity to the
// call-site without changing the parameter count.
#[allow(clippy::too_many_arguments)]
async fn establish_one_tunnel_inner<R, F, Fut>(
    secondary_id: &str,
    info_path: &str,
    primary_quic_port: u16,
    opts: &PreparationOptions,
    reader: R,
    tunnels: &Arc<Mutex<Vec<Child>>>,
    port_map: &Arc<StdMutex<HashMap<String, u16>>>,
    establish_pool: &Arc<Semaphore>,
    mut spawner: F,
) -> Result<u16, PrepError>
where
    R: InfoFileReader,
    F: FnMut(String, u16) -> Fut,
    Fut: Future<Output = Result<Child, PrepError>>,
{
    // Poll until the info file appears. The outer timeout (when
    // called from `setup_ssh_tunnels`) guards total runtime; this
    // loop has no inner deadline by design — the coordinator owns
    // the deadline. `establish_one_tunnel` callers control timeout
    // by dropping the future.
    let (host, tunnel_port) = loop {
        let stdout = reader
            .read(info_path.to_owned())
            .await
            .map_err(|e| PrepError::InfoRead {
                secondary_id: secondary_id.to_owned(),
                message: e.to_string(),
            })?;
        if let Some(text) = stdout {
            let trimmed = text.trim_end_matches('\u{0}');
            // Treat blank/whitespace-only files as "not yet" (writer
            // mkdir's and printfs are not atomic; an empty placeholder
            // can appear before the actual content).
            if !trimmed.trim().is_empty() {
                // Step 7: parse the full v1+v2 record through the
                // canonical `peer_info` module. The watcher only needs
                // the line-1 `(host, port)` to drive the SSH reverse
                // tunnel; the v2 envelope (cert, quic_port, …) is
                // produced for late-joiner consumers (Step 8) and
                // ignored here.
                let record = parse_peer_info(trimmed).map_err(|e| {
                    PrepError::InfoParse {
                        secondary_id: secondary_id.to_owned(),
                        message: e.to_string(),
                    }
                })?;
                let h = record.legacy_uri.host.clone();
                let p = record.legacy_uri.port;
                tracing::info!(
                    secondary_id,
                    host = %h,
                    port = p,
                    version = ?record.version,
                    "found connection info"
                );
                break (h, p);
            }
        }
        tokio::time::sleep(opts.poll_interval).await;
    };
    // `primary_quic_port` is captured by the production spawner
    // closure (see `production_spawner`). For test stubs that don't
    // touch ssh, it's passed in for parity but ignored.
    let _ = primary_quic_port;
    // Delegate spawn + verify + rate-limit + retry to a single
    // helper that owns the establishment concern (see
    // `EstablishmentPolicy` doc-comment). Returns the verified Child
    // already past the 3s alive-gate; this function then moves it
    // into the shared tunnels Vec for cleanup() to reap.
    let child = establish_tunnel(
        secondary_id,
        &opts.establishment,
        establish_pool,
        || spawner(host.clone(), tunnel_port),
    )
    .await?;

    // Commit to the shared tunnel set only after verification —
    // cleanup() now only sees established tunnels.
    {
        let mut guard = tunnels.lock().await;
        guard.push(child);
    }

    // Record the discovered port in the persistent port map. Under
    // a synchronous `StdMutex` — not held across any await.
    port_map
        .lock()
        .expect("secondary_port_map mutex poisoned")
        .insert(secondary_id.to_owned(), tunnel_port);

    Ok(tunnel_port)
}

/// Establish a single SSH reverse tunnel: acquire a semaphore permit,
/// spawn (via the caller-supplied spawner), verify the resulting
/// child survives the 3s alive-gate. On
/// `PrepError::TunnelFailed` (rc=255-class handshake failure), retry
/// up to [`EstablishmentPolicy::attempts`] total times with
/// [`EstablishmentPolicy::backoff`] sleeps in between. The overall
/// per-tunnel deadline is bounded by
/// [`EstablishmentPolicy::per_tunnel_timeout`] so a single chronically
/// failing tunnel can't soak the whole outer `setup_timeout`.
///
/// Single concern: tunnel-establishment policy. The watcher knows
/// nothing about the semaphore or retries — it sees only "give me a
/// verified Child or a terminal error". The `spawner` parameter is
/// pure DI for testability: production passes a closure that builds
/// `ssh -N -R` argv and runs `Command::spawn`; tests can pass a
/// closure that returns a `/bin/sh` child with a configurable
/// success/failure sequence, exercising the rate-limit and retry
/// control flow without ever touching ssh.
///
/// Permit lifetime: acquired before each spawn attempt, released
/// when the per-attempt `_permit` binding drops at the end of each
/// loop iteration (success path: just after verify returns Ok;
/// retry/terminal path: just before the inter-attempt sleep or
/// return). This ensures the 3s verify gate counts against the
/// in-flight handshake budget — without the verify window, a long
/// sequence of failing handshakes would each free their permit
/// instantly and the rate cap would only limit `Command::spawn`
/// turnover, not actual sshd-facing handshake concurrency.
async fn establish_tunnel<F, Fut>(
    secondary_id: &str,
    policy: &EstablishmentPolicy,
    establish_pool: &Arc<Semaphore>,
    mut spawner: F,
) -> Result<Child, PrepError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Child, PrepError>>,
{
    let attempts = policy.attempts.max(1);

    let attempt_all = async {
        let mut last_err: Option<PrepError> = None;
        for attempt in 0..attempts {
            if let Some(sleep) = policy.backoff_before(attempt) {
                tracing::info!(
                    secondary_id,
                    attempt = attempt + 1,
                    total = attempts,
                    backoff_secs = sleep.as_secs_f64(),
                    "retrying SSH tunnel establishment after backoff"
                );
                tokio::time::sleep(sleep).await;
            }

            // Acquire a permit just before the handshake. `acquire`
            // is cancel-safe; the returned permit auto-releases on
            // drop at the end of this loop iteration. `.unwrap()` is
            // sound: the Semaphore is never closed (it lives for the
            // duration of `setup_ssh_tunnels`).
            let _permit = establish_pool.acquire().await.expect("semaphore not closed");

            let mut child = match spawner().await {
                Ok(c) => c,
                // Spawn-time IO error is not a handshake failure —
                // surface immediately without retry; nothing the
                // backoff would help with (binary missing, fd
                // exhaustion, …).
                Err(e) => return Err(e),
            };

            match verify_tunnel_alive(secondary_id, &mut child).await {
                Ok(()) => return Ok(child),
                Err(e @ PrepError::TunnelFailed { .. }) => {
                    // Retryable: log + record, fall through to next
                    // iteration's pre-sleep (if any).
                    tracing::warn!(
                        secondary_id,
                        attempt = attempt + 1,
                        total = attempts,
                        error = %e,
                        "SSH tunnel attempt failed; will retry if budget remains"
                    );
                    last_err = Some(e);
                }
                // Non-retryable verify error (IO etc.) — surface as-is.
                Err(other) => return Err(other),
            }
            // _permit dropped here → release before backoff sleep.
        }
        // Exhausted attempts. last_err is Some(TunnelFailed{..}) by
        // construction (the only path that loops back is the retryable
        // branch above).
        Err(last_err.expect("retry loop ran at least once with TunnelFailed"))
    };

    match tokio::time::timeout(policy.per_tunnel_timeout, attempt_all).await {
        Ok(inner) => inner,
        Err(_) => {
            tracing::error!(
                secondary_id,
                budget_secs = policy.per_tunnel_timeout.as_secs_f64(),
                "SSH tunnel establishment exceeded per-tunnel wall-clock budget"
            );
            Err(PrepError::TunnelFailed {
                secondary_id: secondary_id.to_owned(),
                rc: None,
                stderr: format!(
                    "per-tunnel establishment budget {:?} exhausted before success",
                    policy.per_tunnel_timeout
                ),
            })
        }
    }
}

/// Build the argv for `ssh -N -R <tunnel_port>:localhost:<primary> ...`
/// per the Python implementation's shape — including the auth-options-
/// aware ProxyCommand workaround for OpenSSH 7.3+.
///
/// Pure (no I/O), so the argv shape is unit-testable without spawning
/// a real subprocess.
fn build_ssh_argv(
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
fn production_spawner(
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
async fn verify_tunnel_alive(
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

/// Parse a connection-info URI line. Accepts `<scheme>://<host>:<port>`
/// (post-L1.7 wire format). Returns `(host, port)`.
///
/// Thin shim around [`crate::peer_info::parse_v1_uri`] kept here so
/// existing in-crate tests keep their assertion shape; production
/// callers route through `peer_info` directly.
#[cfg(test)]
fn parse_connection_uri(line: &str) -> Result<(String, u16), String> {
    let uri = crate::peer_info::parse_v1_uri(line).map_err(|e| e.to_string())?;
    Ok((uri.host, uri.port))
}

/// Read a connection-info file from `path` via the supplied
/// [`InfoFileReader`] and parse it as a full v1/v2 [`PeerInfoRecord`].
///
/// Returns `Ok(None)` if the file does not exist yet (matching the
/// watcher's polling semantics). Used by Step 8's late-joiner
/// bootstrap to harvest a directory of records without re-implementing
/// the format. Pure relative to the reader trait — no direct fs/IO.
pub async fn read_peer_info_file<R: InfoFileReader>(
    reader: &R,
    path: String,
) -> Result<Option<PeerInfoRecord>, PrepError> {
    let stdout = reader.read(path.clone()).await?;
    let Some(text) = stdout else {
        return Ok(None);
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    let record = parse_peer_info(&text).map_err(|e| PrepError::InfoParse {
        secondary_id: path,
        message: e.to_string(),
    })?;
    Ok(Some(record))
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
        // Post-Step-7: error message comes from `peer_info::parse_v1_uri`
        // (the shim delegates), shape is `line 1 is not a valid URI: …`
        // — the substring "not a valid URI" is the load-bearing marker.
        assert!(err.contains("not a valid URI"), "got {err}");
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
        let o = PreparationOptions::new(
            "/logs".into(),
            "gw.example".into(),
            Some("alice".into()),
            22,
            vec![],
            vec![(2222, 9090)],
        );
        let argv = build_ssh_argv("compute01", 40000, 51000, &o);
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
        let argv = build_ssh_argv("compute01", 40000, 51000, &o);
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

    /// Multi-watcher race regression: with ≥2 watchers calling
    /// `verify_tunnel_alive` concurrently, each must observe the
    /// outcome of *its own* spawned child — never a sibling's. The
    /// pre-fix shape (`tunnels.lock().last_mut()`) was structurally
    /// vulnerable to this: watcher A could verify watcher B's child
    /// as soon as their `push` order interleaved.
    ///
    /// We exercise the failure branch (each child exits immediately
    /// with a unique stderr message) so the test is fast and the
    /// stderr-attribution is directly observable in the assertion.
    /// Pre-fix, the `last_mut()` lookup made misattribution possible
    /// whenever a sibling's `push` interleaved between this
    /// watcher's push and verify; the test asserts the post-fix
    /// invariant that no such interleaving can ever occur because
    /// each watcher operates on its own owned `Child`.
    #[test]
    fn verify_tunnel_alive_attributes_per_child_under_concurrency() {
        const N: usize = 4;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let outcomes: Vec<(String, PrepError)> = rt.block_on(local.run_until(async move {
            // Spawn N short-lived shells; each emits a marker
            // unique to its index on stderr and exits with rc=1.
            let mut children: Vec<(String, Child)> = Vec::with_capacity(N);
            for i in 0..N {
                let marker = format!("MARK-{i}");
                let mut cmd = Command::new("/bin/sh");
                cmd.arg("-c")
                    .arg(format!("printf '%s' '{marker}' >&2; exit 1"));
                cmd.stdin(std::process::Stdio::null());
                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());
                cmd.kill_on_drop(true);
                let child = cmd.spawn().expect("spawn /bin/sh");
                children.push((format!("secondary-{i}"), child));
            }

            // Verify all in parallel from a JoinSet.
            let mut set: JoinSet<(String, PrepError)> = JoinSet::new();
            for (id, mut child) in children.into_iter() {
                set.spawn_local(async move {
                    let err = verify_tunnel_alive(&id, &mut child)
                        .await
                        .expect_err("dying child must surface TunnelFailed");
                    (id, err)
                });
            }

            let mut out = Vec::with_capacity(N);
            while let Some(joined) = set.join_next().await {
                out.push(joined.expect("watcher panicked"));
            }
            out
        }));

        assert_eq!(outcomes.len(), N);
        for (id, err) in outcomes {
            match err {
                PrepError::TunnelFailed { secondary_id, stderr, .. } => {
                    assert_eq!(secondary_id, id);
                    // Each child's stderr MUST contain its own
                    // marker — pre-fix `last_mut()` could pull
                    // sibling stderr instead.
                    let idx: usize = id
                        .strip_prefix("secondary-")
                        .and_then(|s| s.parse().ok())
                        .expect("id parses");
                    let expected = format!("MARK-{idx}");
                    assert_eq!(
                        stderr, expected,
                        "watcher {id} got cross-attributed stderr {stderr:?}"
                    );
                }
                other => panic!("expected TunnelFailed, got {other}"),
            }
        }
    }

    // ─── Establishment policy: rate-limit + retry coverage ──────────
    //
    // These tests bypass the watcher's info-file polling and exercise
    // `establish_tunnel` directly via dependency injection on the
    // `spawner` closure. The watcher path is covered by the existing
    // `timeout_when_no_secondary_ready` + e2e tests; here we pin the
    // policy-engine semantics (semaphore concurrency cap, retry
    // attempts, terminal-failure surface, per-tunnel wall-clock cap)
    // without launching a real ssh subprocess.

    /// Build a `/bin/sh` child whose stderr emits `marker` and whose
    /// exit code is `rc`. Returns a `Child` that mirrors what
    /// `verify_tunnel_alive` will observe — fast-exit (≪ 3s) ensures
    /// the failure branch trips immediately.
    fn fail_child(marker: &str, rc: i32) -> Child {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("printf '%s' '{marker}' >&2; exit {rc}"));
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        cmd.spawn().expect("spawn /bin/sh")
    }

    /// A child that survives the 3s verify gate. We use `sleep 60`
    /// (and `kill_on_drop(true)` reaps it when the test drops the
    /// Child returned from `establish_tunnel`).
    fn alive_child() -> Child {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("sleep 60");
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        cmd.spawn().expect("spawn /bin/sh sleep")
    }

    /// Establishment-policy test fixture: zero-backoff, 1s per-tunnel
    /// budget so tests stay fast.
    fn fast_policy(max_concurrent: usize, attempts: usize) -> EstablishmentPolicy {
        EstablishmentPolicy {
            max_concurrent,
            attempts,
            backoff: vec![Duration::from_millis(10)],
            per_tunnel_timeout: Duration::from_secs(30),
        }
    }

    /// Retry semantics: a spawner that returns rc=255 on the first
    /// attempt and a long-lived (sleep-60) child on the second must
    /// surface success on the second attempt. Pins option-1: per-
    /// tunnel retry-on-handshake-failure.
    #[test]
    fn establish_tunnel_retries_then_succeeds() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let result: Result<(), PrepError> = rt.block_on(local.run_until(async {
            let pool = Arc::new(Semaphore::new(1));
            let policy = fast_policy(1, 3);
            let attempt_counter = Arc::new(AtomicUsize::new(0));
            let attempts_ref = Arc::clone(&attempt_counter);

            let res = establish_tunnel(
                "secondary-0",
                &policy,
                &pool,
                move || {
                    let i = attempts_ref.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if i == 0 {
                            // First attempt: simulate rc=255 (LMU
                            // gateway random-drop on overloaded sshd).
                            Ok(fail_child("kex_exchange_identification: Connection closed by remote host", 255))
                        } else {
                            // Second attempt: lives past 3s gate.
                            Ok(alive_child())
                        }
                    }
                },
            )
            .await;

            match res {
                Ok(_child) => {
                    assert_eq!(
                        attempt_counter.load(Ordering::SeqCst),
                        2,
                        "expected exactly 2 spawn attempts (1 fail + 1 success)"
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }));
        result.expect("retry-then-success path must yield Ok");
    }

    /// Retry exhaustion: a spawner that always returns rc=255 hits
    /// `attempts` total tries, then surfaces the LAST `TunnelFailed`
    /// — never aborts early, never retries forever.
    #[test]
    fn establish_tunnel_exhausts_attempts_then_fails_loud() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let (err, attempts) = rt.block_on(local.run_until(async {
            let pool = Arc::new(Semaphore::new(1));
            let policy = fast_policy(1, 3);
            let attempt_counter = Arc::new(AtomicUsize::new(0));
            let attempts_ref = Arc::clone(&attempt_counter);

            let res = establish_tunnel(
                "secondary-0",
                &policy,
                &pool,
                move || {
                    let i = attempts_ref.fetch_add(1, Ordering::SeqCst);
                    async move {
                        Ok(fail_child(&format!("ATTEMPT-{i}-FAIL"), 255))
                    }
                },
            )
            .await;

            let err = res.expect_err("3 failing attempts must surface TunnelFailed");
            (err, attempt_counter.load(Ordering::SeqCst))
        }));
        assert_eq!(attempts, 3, "must hit attempts cap exactly");
        match err {
            PrepError::TunnelFailed { secondary_id, rc, stderr } => {
                assert_eq!(secondary_id, "secondary-0");
                // /bin/sh `exit 255` → POSIX raw exit code 255. The
                // exact rc isn't load-bearing (load-bearing is "did
                // we surface the LAST attempt's stderr, not the
                // first?"); pin to a non-None value so a regression
                // that drops rc is caught.
                assert!(rc.is_some(), "rc must be present for spawn-time exit");
                // The surfaced stderr MUST come from the LAST attempt
                // (the latest in the sequence), proving we surface
                // the final failure rather than the first.
                assert_eq!(stderr, "ATTEMPT-2-FAIL");
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }

    /// Stagger semantics: with `max_concurrent = 2` and N=4 concurrent
    /// `establish_tunnel` calls, no more than 2 spawner invocations
    /// may be in flight at any instant. Pins option-2: the semaphore
    /// rate-cap.
    ///
    /// Mechanism: the spawner holds for a fixed wait window before
    /// resolving its future, during which the in-flight counter is
    /// observable. We assert the peak counter stays ≤ max_concurrent
    /// across the whole test.
    #[test]
    fn establish_tunnel_caps_in_flight_spawns_at_max_concurrent() {
        const N: usize = 4;
        const MAX: usize = 2;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let peak: usize = rt.block_on(local.run_until(async {
            let pool = Arc::new(Semaphore::new(MAX));
            // Slightly slower than the verify gate so the permit
            // really IS held during the verify window — the test
            // would still pass with a sub-millisecond spawner but
            // the spirit of the cap is "limit handshake concurrency",
            // not "limit Command::spawn turnover".
            let policy = EstablishmentPolicy {
                max_concurrent: MAX,
                attempts: 1,
                backoff: vec![],
                per_tunnel_timeout: Duration::from_secs(30),
            };
            let in_flight = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));

            let mut set: JoinSet<Result<(), PrepError>> = JoinSet::new();
            for i in 0..N {
                let pool = Arc::clone(&pool);
                let policy = policy.clone();
                let in_flight = Arc::clone(&in_flight);
                let peak = Arc::clone(&peak);
                let id = format!("secondary-{i}");
                set.spawn_local(async move {
                    let in_flight_for_spawner = Arc::clone(&in_flight);
                    let peak_for_spawner = Arc::clone(&peak);
                    let res = establish_tunnel(
                        &id,
                        &policy,
                        &pool,
                        move || {
                            let in_flight = Arc::clone(&in_flight_for_spawner);
                            let peak = Arc::clone(&peak_for_spawner);
                            async move {
                                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                                // Update the peak watermark with the
                                // post-increment value — load-bearing
                                // for the assertion below.
                                peak.fetch_max(now, Ordering::SeqCst);
                                // Hold the permit window long enough to
                                // overlap with sibling spawns. 50ms ×
                                // ceil(N/MAX) = 100ms total run.
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                in_flight.fetch_sub(1, Ordering::SeqCst);
                                // Return a long-lived child so verify
                                // passes — we're testing the permit
                                // gating, not the verify branch.
                                Ok(alive_child())
                            }
                        },
                    )
                    .await;
                    res.map(|_| ())
                });
            }

            // Drain all spawned tasks.
            while let Some(joined) = set.join_next().await {
                joined.expect("watcher join").expect("watcher Ok");
            }
            peak.load(Ordering::SeqCst)
        }));
        assert!(
            peak <= MAX,
            "in-flight spawn count exceeded max_concurrent: peak={peak}, max={MAX}"
        );
        // Sanity: at least MAX must have been simultaneously in
        // flight — otherwise the spawner was so fast the test
        // never actually exercised the cap.
        assert!(
            peak >= MAX,
            "test failed to demonstrate parallelism: peak={peak} < max={MAX}"
        );
    }

    /// Per-tunnel wall-clock cap: a spawner that hangs forever past
    /// the `per_tunnel_timeout` budget must surface `TunnelFailed`
    /// with a budget-exhaustion stderr message.
    #[test]
    fn establish_tunnel_enforces_per_tunnel_wall_clock_budget() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let err: PrepError = rt.block_on(local.run_until(async {
            let pool = Arc::new(Semaphore::new(1));
            let policy = EstablishmentPolicy {
                max_concurrent: 1,
                attempts: 5,
                // Long backoff that the budget should cut short.
                backoff: vec![Duration::from_secs(10)],
                per_tunnel_timeout: Duration::from_millis(200),
            };

            establish_tunnel(
                "secondary-0",
                &policy,
                &pool,
                move || async move {
                    // Each attempt fails fast; the long backoff +
                    // 200ms budget means the timeout fires before
                    // attempt 2 even starts.
                    Ok(fail_child("FAIL", 255))
                },
            )
            .await
            .expect_err("budget exhaustion must surface error")
        }));
        match err {
            PrepError::TunnelFailed { secondary_id, rc, stderr } => {
                assert_eq!(secondary_id, "secondary-0");
                assert_eq!(rc, None, "budget-exhaustion has no spawn rc");
                assert!(
                    stderr.contains("budget"),
                    "expected budget-exhaustion message, got {stderr:?}"
                );
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }

    /// Default policy sanity: the operator-friendly defaults are the
    /// numbers documented in the design (4 concurrent, 3 attempts,
    /// [5s, 15s] backoff, 90s per-tunnel cap). Pinned here so a
    /// careless default-change in `EstablishmentPolicy::default` gets
    /// noticed at review time.
    #[test]
    fn establishment_policy_defaults_match_consumer_contract() {
        let p = EstablishmentPolicy::default();
        assert_eq!(p.max_concurrent, 4);
        assert_eq!(p.attempts, 3);
        assert_eq!(p.backoff, vec![Duration::from_secs(5), Duration::from_secs(15)]);
        assert_eq!(p.per_tunnel_timeout, Duration::from_secs(90));
        // Backoff indexing: attempt 0 has no pre-sleep, attempts 1
        // and 2 use backoff[0] and backoff[1] respectively, anything
        // beyond saturates at the last element.
        assert_eq!(p.backoff_before(0), None);
        assert_eq!(p.backoff_before(1), Some(Duration::from_secs(5)));
        assert_eq!(p.backoff_before(2), Some(Duration::from_secs(15)));
        assert_eq!(p.backoff_before(3), Some(Duration::from_secs(15)));
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

    // ─── establish_one_tunnel coverage ────────────────────────────────
    //
    // These tests exercise `establish_one_tunnel_inner` directly with
    // a stub spawner — same DI seam the `establish_tunnel_*` tests
    // above use. The public `establish_one_tunnel` method is a thin
    // glue layer that hard-codes `production_spawner` and reads
    // `primary_quic_port` from `self`; everything load-bearing for the
    // respawn contract (push-to-Vec, port-map write, rate-limiter
    // sharing) lives in the inner.

    /// Stubbed `InfoFileReader` returning a fixed URI immediately.
    /// Lets the inner skip directly into the spawn+verify phase
    /// without filesystem polling.
    #[derive(Clone)]
    struct CannedUriReader {
        uri: String,
    }

    impl InfoFileReader for CannedUriReader {
        fn read(
            &self,
            _path: String,
        ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
            let uri = self.uri.clone();
            async move { Ok(Some(uri)) }
        }
    }

    /// Push-to-Vec invariant: after `establish_one_tunnel_inner`
    /// returns Ok, the verified `Child` must be in the shared
    /// `ssh_tunnels` Vec and the port-map must carry the discovered
    /// `(id, port)`. Pre-fix this was tangled through `drive_one_watcher`
    /// + `setup_ssh_tunnels`'s post-gather extend; the refactor moves
    /// it inside the inner so any single-tunnel caller observes the
    /// same effect.
    #[test]
    fn establish_one_tunnel_pushes_child_handle() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = opts_for(&tmp);
            let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
            let port_map: Arc<StdMutex<HashMap<String, u16>>> =
                Arc::new(StdMutex::new(HashMap::new()));
            let establish_pool = Arc::new(Semaphore::new(1));
            let reader = CannedUriReader {
                uri: "tcp://compute-77:54321".into(),
            };
            // Stub spawner: returns a long-lived `/bin/sh sleep` child
            // that passes the 3s verify gate. The child is `kill_on_drop`
            // so it's reaped when the test ends.
            let port = establish_one_tunnel_inner(
                "secondary-0",
                "/unused/info_path",
                /* primary_quic_port */ 51000,
                &opts,
                reader,
                &tunnels,
                &port_map,
                &establish_pool,
                |_host, _tunnel_port| async move { Ok(alive_child()) },
            )
            .await
            .expect("establish_one_tunnel_inner must succeed");
            // The discovered port came from the canned URI.
            assert_eq!(port, 54321);
            // Child landed in the shared cleanup Vec.
            let guard = tunnels.lock().await;
            assert_eq!(
                guard.len(),
                1,
                "expected one Child in shared ssh_tunnels Vec, got {}",
                guard.len()
            );
            drop(guard);
            // Port map carries the (id, port) entry.
            let m = port_map.lock().unwrap();
            assert_eq!(m.get("secondary-0").copied(), Some(54321));
        }));
    }

    /// Rate-limiter invariant: two concurrent `establish_one_tunnel_inner`
    /// calls sharing the same `Semaphore` may have at most
    /// `max_concurrent` spawner invocations in flight at any instant.
    /// Mirrors `establish_tunnel_caps_in_flight_spawns_at_max_concurrent`
    /// but goes through the inner helper so the rate cap is verified
    /// at the per-secondary tunnel API surface, not just the policy
    /// engine. Pinned at `MAX = 1` so the two callers must serialise.
    #[test]
    fn establish_one_tunnel_applies_rate_limiter() {
        const N: usize = 2;
        const MAX: usize = 1;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let peak: usize = rt.block_on(local.run_until(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = opts_for(&tmp);
            let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
            let port_map: Arc<StdMutex<HashMap<String, u16>>> =
                Arc::new(StdMutex::new(HashMap::new()));
            let establish_pool = Arc::new(Semaphore::new(MAX));
            let in_flight = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));

            let mut set: JoinSet<Result<u16, PrepError>> = JoinSet::new();
            for i in 0..N {
                let opts = opts.clone();
                let tunnels = Arc::clone(&tunnels);
                let port_map = Arc::clone(&port_map);
                let pool = Arc::clone(&establish_pool);
                let in_flight = Arc::clone(&in_flight);
                let peak = Arc::clone(&peak);
                let id = format!("secondary-{i}");
                let reader = CannedUriReader {
                    uri: format!("tcp://compute-{i}:{}", 60000 + i),
                };
                set.spawn_local(async move {
                    establish_one_tunnel_inner(
                        &id,
                        "/unused/info_path",
                        51000,
                        &opts,
                        reader,
                        &tunnels,
                        &port_map,
                        &pool,
                        move |_host, _tunnel_port| {
                            let in_flight = Arc::clone(&in_flight);
                            let peak = Arc::clone(&peak);
                            async move {
                                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                                peak.fetch_max(now, Ordering::SeqCst);
                                // Hold the permit window long enough
                                // that a sibling spawn — if the cap
                                // were broken — would overlap.
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                in_flight.fetch_sub(1, Ordering::SeqCst);
                                Ok(alive_child())
                            }
                        },
                    )
                    .await
                });
            }
            while let Some(joined) = set.join_next().await {
                joined.expect("inner task join").expect("inner Ok");
            }
            peak.load(Ordering::SeqCst)
        }));
        assert!(
            peak <= MAX,
            "in-flight spawn count exceeded max_concurrent: peak={peak}, max={MAX}"
        );
        // Sanity: at least one spawner ran (proves the test actually
        // exercised the spawn path, not a no-op).
        assert!(peak >= 1, "expected at least one in-flight spawn, got 0");
    }

    /// Calling `establish_one_tunnel` on a fresh manager (before
    /// `setup_ssh_tunnels` has stored the primary QUIC port) must
    /// surface `TunnelFailed` rather than panicking on a missing
    /// precondition. Documents the API contract pinned in the
    /// doc-comment.
    #[test]
    fn establish_one_tunnel_errors_without_prior_setup() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let err: PrepError = rt.block_on(local.run_until(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = opts_for(&tmp);
            let prep = SlurmPreparation::new(opts);
            prep.establish_one_tunnel(
                "secondary-0",
                CannedUriReader {
                    uri: "tcp://h:1".into(),
                },
            )
            .await
            .expect_err("must fail without prior setup_ssh_tunnels")
        }));
        match err {
            PrepError::TunnelFailed { secondary_id, stderr, .. } => {
                assert_eq!(secondary_id, "secondary-0");
                assert!(
                    stderr.contains("primary QUIC port"),
                    "expected precondition stderr, got {stderr:?}"
                );
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }
}
