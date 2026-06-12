//! [`SlurmPreparation`]: the preparation-phase pipeline. Owns the
//! shared per-cohort state (info-file path map, ssh-tunnel cleanup
//! Vec, semaphore, primary-QUIC-port cell) and offers
//! `run_tunnel_cohort` (incremental cohort — the production bring-up
//! shape) + `setup_ssh_tunnels` (blocking cohort gate) +
//! `establish_one_tunnel` (respawn) + `cleanup` (teardown). The actual
//! per-tunnel work (info-file polling, retry loop, semaphore
//! acquisition) lives in [`establish`](super::establish); the ssh
//! wire-up primitives live in [`ssh`](super::ssh). Errors and config
//! types are in [`options`](super::options).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::process::Child;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinSet;

use super::escalation::{EscalationVerdict, ReconnectEscalation};
use super::establish::establish_one_tunnel_inner;
use super::options::{InfoFileReader, PrepError, PreparationOptions};
use super::ssh::{
    BindProbe, LingerLedger, production_bind_verifier, restore_linger, summarize_linger_enables,
    tunnel_spawner,
};
use super::store::{PerSecondaryTunnelRegistry, SharedTunnelVec, TunnelStore};
use super::summary::{TunnelSetupSummary, secondary_id};

/// Lifecycle for the SLURM preparation phase. Owns spawned `ssh -N -R`
/// subprocess handles and tears them down on cleanup().
///
/// Construction is cheap (no I/O); call [`Self::run_tunnel_cohort`] /
/// [`Self::setup_ssh_tunnels`] inside an async context with a
/// [`tokio::task::LocalSet`] active — the watchers use `spawn_local`.
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
    /// Per-host linger bookkeeping: each compute node's ORIGINAL logind
    /// linger state (captured at tunnel-establish time through the
    /// spawner, drained at [`Self::cleanup`] to restore nodes the run
    /// enabled linger on) plus the enable verdicts (drained by the
    /// post-cohort summary in [`Self::setup_ssh_tunnels`]). Shared into
    /// every spawner (initial cohort + respawn + reconnect) so the whole
    /// run's linger lifecycle is one ledger. See [`LingerLedger`].
    linger_ledger: LingerLedger,
    /// Half-dead-tunnel escalation (#342): per-secondary consecutive
    /// alive-noop reconnect-tick streaks. Consulted by
    /// [`Self::reestablish_one_tunnel`] whenever the liveness gate
    /// no-ops; after K consecutive no-op ticks without recovery the
    /// gate is overridden and the rebuild forced. `StdMutex` — held
    /// only for the synchronous verdict, never across an await.
    reconnect_escalation: StdMutex<ReconnectEscalation>,
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
            linger_ledger: LingerLedger::default(),
            reconnect_escalation: StdMutex::new(ReconnectEscalation::default()),
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
        self.record_primary_quic_port(primary_quic_port);

        // Each watcher signals completion via its own oneshot. Using
        // `JoinSet` for spawn-local + abort-on-drop semantics: when
        // we return from this function (success OR timeout), JoinSet
        // is dropped and aborts any still-running watcher.
        let (watchers, receivers) = self.spawn_cohort_watchers(
            reader,
            num_secondaries,
            primary_quic_port,
            production_spawner_factory(&self.opts, primary_quic_port, &self.linger_ledger),
            production_verifier_factory(&self.opts),
        );

        // Single-concern handoff: gather oneshot outcomes under the
        // shared setup deadline, allowing partial success. See
        // `gather_under_deadline` for the per-receiver timeout
        // semantics and the zero-vs-partial fleet failure split.
        let result =
            gather_under_deadline(receivers, num_secondaries, self.opts.setup_timeout).await;

        // Drop the JoinSet — aborts any in-flight watchers.
        drop(watchers);

        // Cohort linger-enable VERDICT: one summary line from the
        // ledger's recorded outcomes (IMPORTANT only when a node
        // failed). Emitted here — after the gather — so it covers every
        // watcher that got as far as its enable attempt.
        summarize_linger_enables(&self.linger_ledger);

        match &result {
            // Per-tunnel inner already wrote each (id, port) into
            // the persistent port map under the StdMutex; nothing
            // to extend here. The returned `map` is the per-call
            // snapshot built from oneshot outcomes — by construction
            // it equals what the inner wrote. May be partial when
            // setup-deadline fired before all watchers reported.
            Ok(map) => log_cohort_summary(map, num_secondaries),
            Err(e) => tracing::error!(error = %e, "ssh tunnel setup failed"),
        }
        result
    }

    /// Drive the cohort's reverse-tunnel establishment INCREMENTALLY,
    /// to completion or caller-side cancellation — the BRING-UP shape
    /// the SLURM pipeline consumes (via the pyo3 background driver) so
    /// the welcome-accepting primary starts while tunnels are still
    /// materializing.
    ///
    /// Contrast with [`Self::setup_ssh_tunnels`] (the blocking cohort
    /// API): that call returns only when every watcher reported or the
    /// `setup_timeout` deadline fired, and the deadline ABORTS the
    /// still-pending watchers. Used as the pipeline's gate, that shape
    /// serialized bring-up behind the slowest member: one SLURM job
    /// sitting PENDING (its connection-info file never appearing) held
    /// the primary's bind — and with it ALL welcome service — hostage
    /// for the full deadline, so the live members' welcome retries went
    /// unanswered until their own unconfigured give-up
    /// (run_20260612_035452: one pending job starved three live
    /// secondaries into fleet loss).
    ///
    /// Semantics here:
    /// * Per-member watchers run exactly the same establishment path
    ///   (same inner, same stores, same rate-limiter) — each member's
    ///   tunnel commits the moment ITS job materializes, independent of
    ///   the others.
    /// * `setup_timeout` demotes from gate to SUMMARY deadline: when it
    ///   fires, the honest #278 K-of-N summary is logged (or, at zero
    ///   ready, a WARN), but pending watchers KEEP RUNNING — a job the
    ///   queue starts late (or a resubmit) still gets its tunnel + the
    ///   primary's service on arrival. Fleet-failure verdicts belong to
    ///   the primary's quorum-proceed window, which sees actual welcomes.
    /// * The future completes when every watcher finished; with a member
    ///   that never materializes it runs until the caller cancels (drops
    ///   the future — JoinSet abort + `kill_on_drop` reap un-committed
    ///   children; committed ones stay in the shared stores for
    ///   [`Self::cleanup`]).
    pub async fn run_tunnel_cohort<R: InfoFileReader>(
        &self,
        reader: R,
        num_secondaries: usize,
        primary_quic_port: u16,
    ) {
        self.run_tunnel_cohort_inner(
            reader,
            num_secondaries,
            primary_quic_port,
            production_spawner_factory(&self.opts, primary_quic_port, &self.linger_ledger),
            production_verifier_factory(&self.opts),
        )
        .await
    }

    /// DI seam under [`Self::run_tunnel_cohort`]: identical control flow
    /// with the per-member spawner/verifier factories injected, so tests
    /// drive the incremental-cohort semantics without touching ssh.
    pub(super) async fn run_tunnel_cohort_inner<R, MkS, S, SF, MkV, V, VF>(
        &self,
        reader: R,
        num_secondaries: usize,
        primary_quic_port: u16,
        make_spawner: MkS,
        make_verifier: MkV,
    ) where
        R: InfoFileReader,
        MkS: FnMut(&str) -> S,
        S: FnMut(String, u16) -> SF + 'static,
        SF: Future<Output = Result<Child, PrepError>> + 'static,
        MkV: FnMut(&str) -> V,
        V: FnMut(String, u16) -> VF + 'static,
        VF: Future<Output = BindProbe> + 'static,
    {
        tracing::info!(
            num_secondaries,
            primary_quic_port,
            "establishing SSH reverse tunnels for {num_secondaries} secondaries \
             (incremental: each member is served as its job materializes)"
        );

        // Same respawn/reconnect precondition capture as the blocking
        // variant — see the comment in `setup_ssh_tunnels`.
        self.record_primary_quic_port(primary_quic_port);

        let mut watchers = {
            let (watchers, receivers) = self.spawn_cohort_watchers(
                reader,
                num_secondaries,
                primary_quic_port,
                make_spawner,
                make_verifier,
            );

            // SUMMARY deadline (not a gate): reuse the same gather +
            // honest-#278 summary the blocking variant emits, but treat
            // the outcome as pure status — pending watchers are NOT
            // aborted and keep servicing members whose jobs start late.
            let result =
                gather_under_deadline(receivers, num_secondaries, self.opts.setup_timeout).await;
            summarize_linger_enables(&self.linger_ledger);
            match &result {
                Ok(map) => log_cohort_summary(map, num_secondaries),
                Err(e) => tracing::warn!(
                    error = %e,
                    "tunnel-setup summary deadline reached with no member \
                     established; watchers keep polling — late-starting jobs \
                     still get their tunnel on arrival (fleet-failure verdicts \
                     belong to the primary's quorum-proceed window)"
                ),
            }
            watchers
        };

        // Keep servicing until every member established (or the caller
        // cancels by dropping this future). A watcher that already sent
        // its oneshot result is just joined; one whose member never
        // materializes keeps polling — that is the late-join contract.
        while watchers.join_next().await.is_some() {}
        tracing::info!(
            num_secondaries,
            "tunnel cohort establishment finished for all members"
        );
    }

    /// Record the primary's QUIC port — the `-R` target every tunnel
    /// (initial cohort, respawn, reconnect) maps back to. Shared entry
    /// preamble of both cohort variants.
    fn record_primary_quic_port(&self, primary_quic_port: u16) {
        *self
            .primary_quic_port
            .lock()
            .expect("primary_quic_port mutex poisoned") = Some(primary_quic_port);
    }

    /// Spawn one establishment watcher per secondary onto a `JoinSet`
    /// (spawn-local + abort-on-drop semantics) and hand back the per-
    /// watcher oneshot receivers. Shared by the blocking
    /// ([`Self::setup_ssh_tunnels`]) and incremental
    /// ([`Self::run_tunnel_cohort`]) cohort entries — the two differ
    /// only in what they DO with the `JoinSet` after the gather (abort
    /// vs keep servicing), never in how watchers are built.
    ///
    /// `make_spawner` / `make_verifier` are per-member factories
    /// (production: [`tunnel_spawner`] / [`production_bind_verifier`];
    /// tests: canned stubs) so the establishment path stays DI-testable
    /// end to end.
    fn spawn_cohort_watchers<R, MkS, S, SF, MkV, V, VF>(
        &self,
        reader: R,
        num_secondaries: usize,
        primary_quic_port: u16,
        mut make_spawner: MkS,
        mut make_verifier: MkV,
    ) -> (JoinSet<()>, Vec<WatcherReceiver>)
    where
        R: InfoFileReader,
        MkS: FnMut(&str) -> S,
        S: FnMut(String, u16) -> SF + 'static,
        SF: Future<Output = Result<Child, PrepError>> + 'static,
        MkV: FnMut(&str) -> V,
        V: FnMut(String, u16) -> VF + 'static,
        VF: Future<Output = BindProbe> + 'static,
    {
        let mut watchers: JoinSet<()> = JoinSet::new();
        let mut receivers: Vec<oneshot::Receiver<Result<(String, u16), PrepError>>> =
            Vec::with_capacity(num_secondaries);

        for i in 0..num_secondaries {
            let secondary_id = secondary_id(i);
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
            let spawner = make_spawner(&secondary_id);
            let verifier = make_verifier(&secondary_id);
            let id_for_task = secondary_id.clone();
            watchers.spawn_local(async move {
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
                    verifier,
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
        (watchers, receivers)
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
        let linger_ledger = self.linger_ledger.clone();
        let id_owned = secondary_id.to_owned();

        let spawner = tunnel_spawner(
            id_owned.clone(),
            opts.clone(),
            primary_quic_port,
            linger_ledger,
        );
        let verifier = production_bind_verifier(id_owned.clone(), opts.clone());
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
            verifier,
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
    /// # The half-dead escalation (#342)
    ///
    /// The gate's probe is LOCAL (`Child::try_wait`); a tunnel whose local
    /// ssh + master TCP survive while the WORKER-side forward is dead reads
    /// alive forever and would never be rebuilt. So every gate no-op is
    /// fed to the per-secondary [`escalation`](super::escalation) tracker;
    /// after K consecutive alive-noop reconnect ticks without visibility
    /// recovering (the cadence only re-fires while lost), the gate is
    /// overridden and the rebuild forced — see the escalation module doc
    /// for why this cannot regress into the healthy-forward churn.
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
        // healthy listener. Only an EXITED/absent child warrants a rebuild —
        // EXCEPT when the half-dead escalation (#342) overrides: the local
        // child can outlive its WORKER-side forward (master TCP up, forward
        // dead), in which case the gate no-ops forever while visibility
        // never recovers. Each gate no-op is recorded as one alive-noop
        // tick; after K consecutive ticks without recovery (the cadence
        // only re-fires while visibility stays lost) the tunnel is presumed
        // half-dead and the rebuild proceeds anyway: release the worker-side
        // binding, respawn, and let the registry commit replace (and reap)
        // the suspect child. See `escalation` for why this cannot
        // re-introduce the healthy-forward churn the gate exists to stop.
        if store.is_alive(secondary_id).await {
            let verdict = self
                .reconnect_escalation
                .lock()
                .expect("reconnect_escalation mutex poisoned")
                .on_alive_noop(secondary_id, std::time::Instant::now());
            match verdict {
                EscalationVerdict::Tolerate { streak } => {
                    tracing::debug!(
                        secondary_id,
                        alive_noop_streak = streak,
                        "observer-reconnect: tunnel child still alive — skipping rebuild (no-op)"
                    );
                    return Ok(());
                }
                EscalationVerdict::ForceRebuild => {
                    tracing::warn!(
                        secondary_id,
                        "observer-reconnect ESCALATION: tunnel child alive but visibility \
                         did not recover across consecutive reconnect ticks — presuming the \
                         worker-side forward half-dead; forcing release + rebuild (the \
                         suspect child is replaced and reaped on commit)"
                    );
                    // Fall through to the rebuild below.
                }
            }
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
        let linger_ledger = self.linger_ledger.clone();
        let id_owned = secondary_id.to_owned();

        let spawner = tunnel_spawner(
            id_owned.clone(),
            opts.clone(),
            primary_quic_port,
            linger_ledger,
        );
        let verifier = production_bind_verifier(id_owned.clone(), opts.clone());
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
            verifier,
        )
        .await?;
        // A completed rebuild (gate-found-dead or escalation-forced alike)
        // resets the escalation streak: the fresh child gets the full
        // K-tick benefit of the doubt before any future force.
        self.reconnect_escalation
            .lock()
            .expect("reconnect_escalation mutex poisoned")
            .on_rebuilt(&id_owned);
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
        restore_linger(&self.linger_ledger, &self.opts).await;
        tracing::info!("SLURM preparation cleanup complete");
    }
}

/// Boxed per-member spawner shape the production factories hand to
/// `spawn_cohort_watchers` (a nested `impl FnMut -> impl FnMut` return
/// type cannot be written unboxed). `pub(super)` so the cohort tests'
/// stub factories share the shape.
pub(super) type BoxedSpawner =
    Box<dyn FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = Result<Child, PrepError>>>>>;
/// Boxed per-member bind-verifier shape, same rationale.
type BoxedVerifier =
    Box<dyn FnMut(String, u16) -> std::pin::Pin<Box<dyn Future<Output = BindProbe>>>>;

/// One per-member watcher's completion report: `(secondary_id,
/// tunnel_port)` on a verified establishment, the establishment error
/// otherwise. Receiver half handed from `spawn_cohort_watchers` to the
/// gather.
type WatcherReceiver = oneshot::Receiver<Result<(String, u16), PrepError>>;

/// Production per-member spawner factory for the cohort entries: the
/// same [`tunnel_spawner`] closure the per-watcher tasks historically
/// built inline. Captured state is cloned per member, exactly as the
/// inline construction did.
fn production_spawner_factory(
    opts: &PreparationOptions,
    primary_quic_port: u16,
    linger_ledger: &LingerLedger,
) -> impl FnMut(&str) -> BoxedSpawner {
    let opts = opts.clone();
    let linger_ledger = linger_ledger.clone();
    move |secondary_id: &str| {
        Box::new(tunnel_spawner(
            secondary_id.to_owned(),
            opts.clone(),
            primary_quic_port,
            linger_ledger.clone(),
        ))
    }
}

/// Production per-member bind-verifier factory — the
/// [`production_bind_verifier`] half of the same seam.
fn production_verifier_factory(opts: &PreparationOptions) -> impl FnMut(&str) -> BoxedVerifier {
    let opts = opts.clone();
    move |secondary_id: &str| {
        Box::new(production_bind_verifier(
            secondary_id.to_owned(),
            opts.clone(),
        ))
    }
}

/// HONEST cohort summary (#278): report established/expected from what
/// was actually VERIFIED, never a count of spawns. Partial fleets are
/// WARNed with the missing ids named. Single render site for both
/// cohort entries (blocking gate / incremental summary deadline) so
/// the two cannot drift.
fn log_cohort_summary(map: &HashMap<String, u16>, expected: usize) {
    let summary = TunnelSetupSummary::new(map, expected);
    if summary.is_complete() {
        tracing::info!(
            ready = summary.established,
            total = summary.expected,
            "ssh tunnel setup done: {summary}"
        );
    } else {
        tracing::warn!(
            ready = summary.established,
            total = summary.expected,
            missing = ?summary.missing,
            "ssh tunnel setup done with failures: {summary}"
        );
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
