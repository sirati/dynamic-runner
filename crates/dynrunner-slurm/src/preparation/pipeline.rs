//! [`SlurmPreparation`]: the preparation-phase pipeline. Owns the
//! shared per-cohort state (info-file path map, ssh-tunnel cleanup
//! Vec, semaphore, primary-QUIC-port cell) and offers
//! `setup_ssh_tunnels` (cohort) + `establish_one_tunnel` (respawn) +
//! `cleanup` (teardown). The actual per-tunnel work (info-file polling,
//! retry loop, semaphore acquisition) lives in
//! [`establish`](super::establish); the ssh wire-up primitives live in
//! [`ssh`](super::ssh). Errors and config types are in
//! [`options`](super::options).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::process::Child;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use super::establish::establish_one_tunnel_inner;
use super::options::{InfoFileReader, PrepError, PreparationOptions};
use super::ssh::{LingerRestoreMap, production_spawner, reconnect_spawner, restore_linger};
use super::store::{PerSecondaryTunnelRegistry, SharedTunnelVec, TunnelStore};

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
    ///
    /// The cohort-setup + single-respawn paths commit here (append-only:
    /// each secondary establishes once, no prior child to displace). The
    /// observer-reconnect path uses [`Self::reconnect_tunnels`] instead.
    ssh_tunnels: Arc<Mutex<Vec<Child>>>,
    /// Per-secondary tunnel registry for the OBSERVER-RECONNECT path:
    /// one live `ssh -N -R` `Child` per secondary id, replaced (old
    /// reaped) on each rebuild. Keyed by id so the reconnect cadence can
    /// ask "is this secondary's tunnel still alive?" and NO-OP a re-fire
    /// on a healthy forward — the fix for the rc=255 release+rebind loop
    /// that blinded the observer. Distinct from `ssh_tunnels` because the
    /// reconnect path needs per-id REPLACEMENT (the cohort Vec is
    /// anonymous append-only and would accumulate dead lingerers). Both
    /// are drained at [`Self::cleanup`]. See
    /// [`PerSecondaryTunnelRegistry`].
    reconnect_tunnels: Arc<Mutex<HashMap<String, Child>>>,
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
    /// `host -> was_linger`: each compute node's ORIGINAL logind linger
    /// state, captured at tunnel-establish time (through the spawner) and
    /// drained at [`Self::cleanup`] to restore nodes the run enabled linger
    /// on. Shared into every spawner (initial cohort + respawn + reconnect)
    /// so the whole run's linger lifecycle is one map. See
    /// [`LingerRestoreMap`].
    linger_restore: LingerRestoreMap,
}

impl SlurmPreparation {
    pub fn new(opts: PreparationOptions) -> Self {
        let establish_pool = Arc::new(Semaphore::new(opts.establishment.max_concurrent.max(1)));
        Self {
            opts,
            ssh_tunnels: Arc::new(Mutex::new(Vec::new())),
            reconnect_tunnels: Arc::new(Mutex::new(HashMap::new())),
            secondary_port_map: Arc::new(StdMutex::new(HashMap::new())),
            establish_pool,
            primary_quic_port: StdMutex::new(None),
            linger_restore: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    pub fn opts(&self) -> &PreparationOptions {
        &self.opts
    }

    /// Test backdoor: the observer-reconnect per-secondary tunnel
    /// registry Arc. Lets the gate test pre-seed a live child so it can
    /// assert `reestablish_one_tunnel` no-ops (returns Ok WITHOUT
    /// touching ssh) when the prior child is alive — keyed on the SAME
    /// Arc the method clones. `cfg(test)` so the production surface is
    /// unchanged.
    #[cfg(test)]
    pub(super) fn reconnect_tunnels_for_test(&self) -> Arc<Mutex<HashMap<String, Child>>> {
        Arc::clone(&self.reconnect_tunnels)
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
    ///
    /// `&self` (not `&mut self`): every mutating site below is on an
    /// interior-mutable field (`StdMutex<Option<u16>>`,
    /// `Arc<Mutex<Vec<Child>>>`). The `&self` shape lets respawn
    /// callers share an `Arc<SlurmPreparation>` between the
    /// `setup_ssh_tunnels` initial-cohort loop and a downstream
    /// `establish_one_tunnel` per respawn — see
    /// `crate::respawn::SlurmPreparationTunnelEstablisher`.
    pub async fn setup_ssh_tunnels<R: InfoFileReader>(
        &self,
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
        let mut receivers: Vec<oneshot::Receiver<Result<(String, u16), PrepError>>> =
            Vec::with_capacity(num_secondaries);

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
            let store = SharedTunnelVec::new(Arc::clone(&self.ssh_tunnels));
            let establish_pool = Arc::clone(&self.establish_pool);
            // `Arc<StdMutex<...>>` field lets per-watcher tasks share
            // the persistent port map without borrowing `self` (they
            // live on the JoinSet past the borrow).
            let port_map = Arc::clone(&self.secondary_port_map);
            let linger_restore = Arc::clone(&self.linger_restore);
            let id_for_task = secondary_id.clone();
            watchers.spawn_local(async move {
                let spawner = production_spawner(
                    id_for_task.clone(),
                    opts.clone(),
                    primary_quic_port,
                    linger_restore,
                );
                let res = establish_one_tunnel_inner(
                    &id_for_task,
                    &info_path,
                    primary_quic_port,
                    &opts,
                    reader_clone,
                    &store,
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

        // Single-concern handoff: gather oneshot outcomes under the
        // shared setup deadline, allowing partial success. See
        // `gather_under_deadline` for the per-receiver timeout
        // semantics and the zero-vs-partial fleet failure split.
        let result =
            gather_under_deadline(receivers, num_secondaries, self.opts.setup_timeout).await;

        // Drop the JoinSet — aborts any in-flight watchers.
        drop(watchers);

        match &result {
            Ok(map) => {
                // Per-tunnel inner already wrote each (id, port) into
                // the persistent port map under the StdMutex; nothing
                // to extend here. The returned `map` is the per-call
                // snapshot built from oneshot outcomes — by construction
                // it equals what the inner wrote. May be partial when
                // setup-deadline fired before all watchers reported;
                // the warn! above carries the partial-fleet headline.
                tracing::info!(
                    ready = map.len(),
                    total = num_secondaries,
                    "ssh tunnel setup done"
                );
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
        let store = SharedTunnelVec::new(Arc::clone(&self.ssh_tunnels));
        let establish_pool = Arc::clone(&self.establish_pool);
        let port_map = Arc::clone(&self.secondary_port_map);
        let linger_restore = Arc::clone(&self.linger_restore);
        let id_owned = secondary_id.to_owned();

        let spawner = production_spawner(
            id_owned.clone(),
            opts.clone(),
            primary_quic_port,
            linger_restore,
        );
        let _port = establish_one_tunnel_inner(
            &id_owned,
            &info_path,
            primary_quic_port,
            &opts,
            reader,
            &store,
            &port_map,
            &establish_pool,
            spawner,
        )
        .await?;
        Ok(())
    }

    /// REBUILD a dropped reverse tunnel for an EXISTING secondary whose
    /// `-R` link the (passive, zero-authority) observer lost — the
    /// observer-reconnect path. Identical to [`Self::establish_one_tunnel`]
    /// (re-poll the info file, re-spawn `ssh -N -R`, verify, join the
    /// cleanup set) EXCEPT (1) a LIVENESS GATE that NO-OPs the rebuild
    /// when the secondary's prior tunnel child is still alive, and (2) the
    /// injected spawner first force-releases the stale worker-side `-R
    /// <tunnel_port>` binding before rebinding the SAME port.
    ///
    /// # The liveness gate (defect (a))
    ///
    /// The observer's lost-visibility cadence re-fires this rebuild every
    /// ~60s while visibility stays lost. Blindly running release+rebind on
    /// every tick — even when THIS secondary's `-R` forward is perfectly
    /// healthy — is the bug: the release `fuser -k` either kills the live
    /// forward or (psmisc absent) fails, and the same-port rebind collides
    /// rc=255 against the observer's OWN healthy listener, looping forever
    /// and accumulating dead ssh children. So we FIRST consult the
    /// per-secondary registry: if the prior child for this id is still
    /// running (`try_wait() == None`), the tunnel is fine and we return
    /// `Ok(())` immediately — no release, no rebind, no churn. Only when
    /// the prior child has EXITED (or there was none) do we proceed to the
    /// release+rebind, whose fresh verified child then REPLACES the dead
    /// registry entry (the displaced child reaped). The blindness that
    /// keeps `lost_secs` growing is the SECONDARY's missing QUIC re-dial
    /// (defect (b)), a separate concern in the transport — not this
    /// tunnel's job.
    ///
    /// Why a distinct entry point rather than a flag on
    /// `establish_one_tunnel`: the two paths cross the SAME
    /// establishment seam ([`establish_one_tunnel_inner`]) and differ
    /// in the injected spawner (release-first vs not) AND the tunnel
    /// STORE (per-id registry vs append-only Vec). The respawn path
    /// establishes a tunnel for a node the primary just spawned (no prior
    /// binding can exist on a fresh node), so it must NOT pay the release
    /// round-trip and has no prior child to gate on; the observer-reconnect
    /// path rebuilds an EXISTING node's dropped tunnel. Same inner,
    /// different spawner + store — no duplicated control flow, no
    /// `if reconnecting { … }` special-casing inside the inner.
    ///
    /// Precondition + caller cleanup: identical to
    /// [`Self::establish_one_tunnel`].
    pub async fn reestablish_one_tunnel<R: InfoFileReader>(
        &self,
        secondary_id: &str,
        reader: R,
    ) -> Result<(), PrepError> {
        let store = PerSecondaryTunnelRegistry::new(Arc::clone(&self.reconnect_tunnels));

        // LIVENESS GATE: if this secondary's prior tunnel child is still
        // running, the `-R` forward is healthy — the rebuild is a NO-OP.
        // Returning Ok here is the success signal the cadence was blind to:
        // it stops the release+rebind churn against the observer's own
        // healthy listener. Only an EXITED/absent child warrants a rebuild.
        if store.is_alive(secondary_id).await {
            tracing::debug!(
                secondary_id,
                "observer-reconnect: tunnel child still alive — skipping rebuild (no-op)"
            );
            return Ok(());
        }

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
        let establish_pool = Arc::clone(&self.establish_pool);
        let port_map = Arc::clone(&self.secondary_port_map);
        let linger_restore = Arc::clone(&self.linger_restore);
        let id_owned = secondary_id.to_owned();

        let spawner = reconnect_spawner(
            id_owned.clone(),
            opts.clone(),
            primary_quic_port,
            linger_restore,
        );
        let _port = establish_one_tunnel_inner(
            &id_owned,
            &info_path,
            primary_quic_port,
            &opts,
            reader,
            &store,
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
    /// Idempotent (drains BOTH the cohort/respawn `ssh_tunnels` Vec AND
    /// the observer-reconnect `reconnect_tunnels` registry).
    ///
    /// `&self` mirrors [`Self::setup_ssh_tunnels`]: the only mutation
    /// is draining the interior-mutable child stores.
    ///
    /// Linger: after the tunnels are down, RESTORE each node's logind
    /// linger to off where the run enabled it (whole-run-scoped, race-free
    /// — see [`restore_linger`]). The tunnels are torn down FIRST so the
    /// submitter `-R` login sessions are already gone; the restore then
    /// returns logind to exactly the pre-run state. Best-effort per node.
    pub async fn cleanup(&self) {
        tracing::info!("cleaning up SLURM preparation resources");
        // Drain both tunnel stores through the same `TunnelStore` seam —
        // the cohort/respawn append-only Vec and the reconnect per-id
        // registry both reap their children identically.
        SharedTunnelVec::new(Arc::clone(&self.ssh_tunnels))
            .drain_and_terminate()
            .await;
        PerSecondaryTunnelRegistry::new(Arc::clone(&self.reconnect_tunnels))
            .drain_and_terminate()
            .await;
        restore_linger(&self.linger_restore, &self.opts).await;
        tracing::info!("SLURM preparation cleanup complete");
    }
}

/// Gather per-watcher `oneshot` outcomes under a shared setup
/// deadline, allowing partial success.
///
/// Single concern: turning N oneshot receivers + one deadline into
/// either:
///   * `Ok(partial_or_full_map)` when at least one secondary
///     connected (the late-joiner system handles slots that didn't
///     arrive — see `PeerJoined` cluster mutation),
///   * `Err(PrepError::Timeout { ready: 0, total })` when zero
///     secondaries connected (genuine fleet-failure), or
///   * `Err(other)` when a watcher surfaced an explicit error or
///     dropped its sender without sending (`WatcherLost`).
///
/// Each receiver is awaited via [`tokio::time::timeout_at`] anchored
/// at the same `Instant` so already-completed senders STILL resolve
/// Ok after the deadline (the inner future is polled before the
/// timer is checked), while stalled senders cleanly elapse and leave
/// their map slot absent. Replaces the pre-fix
/// `tokio::time::timeout(setup_timeout, gather)` shape, whose
/// cancellation dropped the partial `HashMap` and crashed callers
/// that could have proceeded on K-of-N.
///
/// Extracted from `setup_ssh_tunnels` so the deadline + partial-
/// fleet semantics can be exercised by `cargo test` without spawning
/// real `ssh` subprocesses — tests drive the senders directly.
pub(super) async fn gather_under_deadline(
    receivers: Vec<oneshot::Receiver<Result<(String, u16), PrepError>>>,
    num_secondaries: usize,
    setup_timeout: Duration,
) -> Result<HashMap<String, u16>, PrepError> {
    let deadline = tokio::time::Instant::now() + setup_timeout;
    let mut out: HashMap<String, u16> = HashMap::new();
    let mut early_err: Option<PrepError> = None;
    for rx in receivers {
        match tokio::time::timeout_at(deadline, rx).await {
            // Watcher delivered an established tunnel.
            Ok(Ok(Ok((secondary_id, tunnel_port)))) => {
                out.insert(secondary_id, tunnel_port);
            }
            // Watcher delivered an explicit error (InfoRead /
            // InfoParse / TunnelFailed / Io). Fail fast — same as
            // the pre-refactor `gather` closure did. A live
            // explicit error from one secondary is a fleet-
            // configuration problem, not a partial-fleet case.
            Ok(Ok(Err(e))) => {
                early_err = Some(e);
                break;
            }
            // Sender dropped without sending → watcher panicked or
            // was aborted before reaching its `tx.send`. Same
            // `WatcherLost` surfacing as before; the JoinSet drop
            // at the call site will surface any panic on top.
            Ok(Err(join_err)) => {
                early_err = Some(PrepError::WatcherLost(join_err.to_string()));
                break;
            }
            // Per-receiver deadline fired — this secondary did not
            // connect in time. Leave the slot absent; the late-
            // joiner path can still attach it via the PeerJoined
            // cluster mutation. Continue to drain siblings whose
            // senders may have raced ahead.
            Err(_elapsed) => {}
        }
    }

    if let Some(e) = early_err {
        Err(e)
    } else if out.is_empty() && num_secondaries > 0 {
        // Genuine fleet-failure: zero secondaries connected
        // inside the deadline. `ssh_tunnels.lock().await.len()`
        // at the call site matches `out.len()` by construction
        // (every successful tunnel pushes into `ssh_tunnels`
        // BEFORE the watcher sends on its oneshot — see
        // `establish_one_tunnel_inner`), so reading from the
        // gathered map here is equivalent to the prior lock read.
        Err(PrepError::Timeout {
            ready: 0,
            total: num_secondaries,
        })
    } else {
        if out.len() < num_secondaries {
            tracing::warn!(
                ready = out.len(),
                total = num_secondaries,
                "setup-deadline timeout: only {} of {} secondaries connected; proceeding with partial fleet; late-joiners can attach via PeerJoined",
                out.len(),
                num_secondaries,
            );
        }
        Ok(out)
    }
}
