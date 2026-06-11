//! Per-tunnel establishment policy: the watcher loop and the
//! retry/backoff/per-tunnel-timeout machinery. Shared between the
//! cohort path ([`SlurmPreparation::setup_ssh_tunnels`](super::pipeline::SlurmPreparation::setup_ssh_tunnels))
//! and the single-respawn path
//! ([`SlurmPreparation::establish_one_tunnel`](super::pipeline::SlurmPreparation::establish_one_tunnel)).
//! Ssh wire-up is delegated to a spawner closure (production-bound to
//! [`production_spawner`](super::ssh::production_spawner)); tests
//! supply an in-memory variant.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use tokio::process::Child;
use tokio::sync::Semaphore;

use crate::peer_info::parse as parse_peer_info;

use super::options::{InfoFileReader, PrepError, PreparationOptions};
use super::policy::EstablishmentPolicy;
use super::ssh::{
    BindProbe, TunnelFailureClass, classify_tunnel_failure, terminate_child, verify_tunnel_alive,
};
use super::store::TunnelStore;

/// Per-tunnel work shared by `setup_ssh_tunnels` (run in parallel
/// across N secondaries inside a JoinSet) and `establish_one_tunnel`
/// (run once for a single respawn).
///
/// Single concern: take one secondary from "info file may or may not
/// be there yet" to "verified ssh -R subprocess committed to the
/// caller-supplied [`TunnelStore`], with `(id, port)` recorded in the
/// shared port map". Returns the discovered `tunnel_port` on success.
///
/// Store DI: the verified `Child` is handed to `store.commit(id, child)`
/// — the engine never learns whether that store appends to the shared
/// cohort Vec or replaces a per-secondary registry entry. Cohort + respawn
/// pass a `SharedTunnelVec`; the observer-reconnect path passes a
/// `PerSecondaryTunnelRegistry`. Same control flow, no `if reconnecting`.
///
/// Spawner DI: the `spawner` closure is parameterised over
/// `(host: String, tunnel_port: u16) -> Future<Result<Child, PrepError>>`.
/// Production callers pass a closure that builds the `ssh -N -R` argv
/// and runs `Command::spawn`; tests pass a closure that returns a
/// `/bin/sh` child with a configurable outcome, exercising the
/// store-commit / port-map / rate-limiter control flow without ever
/// touching ssh. `String` (not `&str`) so the closure's returned
/// future can own its inputs without borrow-lifetime contortions.
///
/// Verifier DI: the `verifier` closure (same `(host, tunnel_port)`
/// parameterisation) answers "does the WORKER-side listener for this
/// tunnel actually exist?" after the local alive-gate passes —
/// production-bound to
/// [`production_bind_verifier`](super::ssh::production_bind_verifier),
/// canned in tests. See [`establish_tunnel`] for how each
/// [`BindProbe`] verdict steers the retry loop.
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
pub(super) async fn establish_one_tunnel_inner<R, S, F, Fut, V, VFut>(
    secondary_id: &str,
    info_path: &str,
    primary_quic_port: u16,
    opts: &PreparationOptions,
    reader: R,
    store: &S,
    port_map: &Arc<StdMutex<HashMap<String, u16>>>,
    establish_pool: &Arc<Semaphore>,
    mut spawner: F,
    mut verifier: V,
) -> Result<u16, PrepError>
where
    R: InfoFileReader,
    S: TunnelStore,
    F: FnMut(String, u16) -> Fut,
    Fut: Future<Output = Result<Child, PrepError>>,
    V: FnMut(String, u16) -> VFut,
    VFut: Future<Output = BindProbe>,
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
                let record = parse_peer_info(trimmed).map_err(|e| PrepError::InfoParse {
                    secondary_id: secondary_id.to_owned(),
                    message: e.to_string(),
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
    // already past the 3s alive-gate AND the worker-side bind
    // verification; this function then commits it to the caller's
    // store for cleanup() to reap.
    let child = establish_tunnel(
        secondary_id,
        &opts.establishment,
        establish_pool,
        || spawner(host.clone(), tunnel_port),
        || verifier(host.clone(), tunnel_port),
    )
    .await?;

    // Commit to the caller's tunnel store only after verification —
    // cleanup() now only sees established tunnels. The store decides
    // append-to-Vec (cohort/respawn) vs replace-by-id (reconnect); the
    // engine is blind to which.
    store.commit(secondary_id, child).await;

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
/// child survives the 3s alive-gate, then verify the WORKER-side
/// listener actually exists (via the caller-supplied verifier). On a
/// TRANSIENT-classified `PrepError::TunnelFailed` (pre-banner rc=255
/// drop — see [`classify_tunnel_failure`]) or a
/// definite bind-verification miss, retry
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
/// # Bind verification (the honest closer for the partial-`-R` bind)
///
/// Child survival past the 3s gate plus `ExitOnForwardFailure=yes`
/// does NOT prove the worker-side listener exists: sshd binds the
/// remote forward per address family and reports SUCCESS when at
/// least one family binds (OpenSSH `channel_setup_fwd_listener_tcpip`
/// — a transient v4 collision on the ephemeral tunnel port leaves a
/// v6-only listener while the client survives). So after the gate the
/// `verifier` asks the worker what is actually bound, and each
/// [`BindProbe`] verdict steers the loop:
///
/// * `Listening` — done; only NOW is the tunnel "established" (the
///   caller's store commit, the port map, and therefore the #278 K/N
///   summary all sit downstream of this verdict — they count only
///   VERIFIED tunnels).
/// * `NotListening` — definite miss: the child is terminated and the
///   attempt counts as failed; the retry respawns on the SAME port
///   (the worker's dial target is fixed at worker startup) after the
///   spawner's retry-release reclaims it — see
///   `ReleaseBeforeSpawn::OnRetry` in [`super::ssh`].
/// * `Inconclusive` — probe infrastructure failure (`ss` missing,
///   probe ssh died): WARN and KEEP the gate-verified tunnel; the
///   probe's own availability must never kill tunnels that met the
///   pre-probe standard.
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
pub(super) async fn establish_tunnel<F, Fut, V, VFut>(
    secondary_id: &str,
    policy: &EstablishmentPolicy,
    establish_pool: &Arc<Semaphore>,
    mut spawner: F,
    mut verifier: V,
) -> Result<Child, PrepError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Child, PrepError>>,
    V: FnMut() -> VFut,
    VFut: Future<Output = BindProbe>,
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
            let _permit = establish_pool
                .acquire()
                .await
                .expect("semaphore not closed");

            let mut child = match spawner().await {
                Ok(c) => c,
                // Spawn-time IO error is not a handshake failure —
                // surface immediately without retry; nothing the
                // backoff would help with (binary missing, fd
                // exhaustion, …).
                Err(e) => return Err(e),
            };

            match verify_tunnel_alive(secondary_id, &mut child).await {
                Ok(()) => match verifier().await {
                    BindProbe::Listening { listeners } => {
                        tracing::info!(
                            secondary_id,
                            listeners = ?listeners,
                            "worker-side tunnel listener verified"
                        );
                        return Ok(child);
                    }
                    BindProbe::Inconclusive { reason } => {
                        tracing::warn!(
                            secondary_id,
                            reason = %reason,
                            "could not verify the worker-side tunnel listener; keeping the \
                             gate-verified tunnel (a probe-infrastructure failure must not \
                             kill it). If the listener is genuinely absent the secondary's \
                             dial will keep failing and the lost-visibility reconnect path \
                             rebuilds the tunnel."
                        );
                        return Ok(child);
                    }
                    BindProbe::NotListening => {
                        // Definite miss: ssh survived (so sshd accepted
                        // the forward request) but no loopback listener
                        // exists on the worker — the partial-bind /
                        // wrong-host class. Kill the useless child and
                        // retry: the spawner's retry-release reclaims
                        // the same fixed port before the respawn.
                        tracing::warn!(
                            secondary_id,
                            attempt = attempt + 1,
                            total = attempts,
                            "ssh survived the alive-gate but NO worker-side listener exists \
                             on the tunnel port (sshd reports a -R bind as success when \
                             EITHER address family binds; a transient port collision can eat \
                             the v4 side) — terminating the tunnel and retrying with a \
                             pre-spawn port release"
                        );
                        terminate_child(&mut child).await;
                        last_err = Some(PrepError::TunnelFailed {
                            secondary_id: secondary_id.to_owned(),
                            rc: None,
                            stderr: format!(
                                "ssh tunnel survived the alive-gate but the worker-side \
                                 listener never appeared on the tunnel port (partial -R \
                                 bind); attempt {}/{attempts}",
                                attempt + 1
                            ),
                        });
                    }
                },
                Err(e @ PrepError::TunnelFailed { .. }) => {
                    // Failure classification: only the TRANSIENT class
                    // (pre-banner drop — the probabilistic `MaxStartups`
                    // anatomy) consumes retry budget. A DETERMINISTIC
                    // auth-class refusal (bad/missing key, unknown user
                    // — the provisioning-gap shape) would refuse
                    // identically on every retry, so it surfaces on the
                    // FIRST attempt with the verbatim ssh stderr as the
                    // operator's diagnosis. See `classify_tunnel_failure`.
                    if let PrepError::TunnelFailed { stderr, .. } = &e
                        && classify_tunnel_failure(stderr) == TunnelFailureClass::Deterministic
                    {
                        tracing::error!(
                            secondary_id,
                            error = %e,
                            "SSH tunnel attempt failed with a DETERMINISTIC auth-class error \
                             (credentials / user provisioning / host key) — failing fast \
                             without burning the retry budget; every retry would refuse \
                             identically"
                        );
                        return Err(e);
                    }
                    // Transient: log + record, fall through to next
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
        // construction (the only paths that loop back — the retryable
        // verify branch and the bind-verification miss — both set it).
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
