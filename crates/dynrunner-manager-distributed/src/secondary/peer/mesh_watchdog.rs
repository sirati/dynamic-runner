//! Peer-mesh-formation watchdog and the idempotent `MeshReady` reporter.
//!
//! Single concern: decide whether the peer mesh formed within the
//! one-shot watchdog deadline and tell the primary the answer exactly
//! once (mesh formed, mesh degraded, or no peers expected). The full
//! degraded-mode contract is documented on
//! `SecondaryCoordinator::peer_mesh_degraded`; this module owns only
//! the detection + first-report side.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{

    /// One-shot watchdog: 30s after `connect_to_peers` fired with a
    /// non-empty peer list, decide whether the peer mesh formed.
    /// Self-healing if the mesh forms before the deadline
    /// (`peer_count() > 0` suppresses the fault) or partially forms
    /// after the deadline (any incoming peer connection clears
    /// `peer_mesh_check_at`, no fault).
    ///
    /// On confirmed full-mesh failure (deadline elapsed, zero peers)
    /// the run enters DEGRADED mode rather than dying:
    ///   1. `peer_mesh_degraded` is latched true so callers that
    ///      need the mesh (failover election, peer-broadcast
    ///      keepalives) can fail loud or skip — the contract is
    ///      owned by those callers, not by this watchdog.
    ///   2. `MeshReady` is sent with `peer_count=0` so the primary's
    ///      `wait_for_mesh_ready` step releases `PromotePrimary` and
    ///      operational dispatch (over WSS, not the peer mesh) can
    ///      flow. Without this the whole run blocks on the missing
    ///      mesh signal.
    ///   3. `peer_mesh_check_at = None` so the watchdog is one-shot.
    ///
    /// Why not fatal? Operational dispatch primary→secondary uses
    /// WSS, not the QUIC peer mesh. If no failover is ever needed
    /// the run can complete cleanly even with zero peers. The old
    /// fatal behaviour (send `SecondaryFatalError`, set
    /// `fatal_exit`) stranded every remaining task whenever the
    /// peer mesh genuinely couldn't form — see
    /// asm-tokenizer's `--jobs 2` regression where 474 of 484 tasks
    /// were lost ~30s into the run because the watchdog fired even
    /// though primary→secondary dispatch was healthy.
    ///
    /// `peer_count()` already calls `drain_new_connections` so this
    /// reads the freshest state.
    ///
    /// Run-complete short-circuit: once the cluster mirror records
    /// `RunComplete` (either from a peer's broadcast in `dispatch.rs`
    /// or from this node's own promoted-primary broadcast in
    /// `processing.rs`), the peer mesh is irrelevant — failover and
    /// inter-secondary keepalive paths have nothing left to guard.
    /// Disarming the watchdog at run-complete suppresses a misleading
    /// "peer mesh did not form" warn during clean teardown of an
    /// in-process distributed run, where secondaries observe
    /// `RunComplete` ~30s before the watchdog deadline would fire on
    /// the last keepalive tick before exit. Single source of truth:
    /// the read lives in the watchdog itself rather than at every
    /// `cluster_state.apply(RunComplete)` site, so call sites stay
    /// agnostic to peer-mesh policy.
    pub(in crate::secondary) async fn check_peer_mesh_watchdog(&mut self) {
        let deadline = match self.peer_mesh_check_at {
            Some(d) => d,
            None => return,
        };
        if self.cluster_state.run_complete() {
            // Run is over; the mesh fault has nothing to report.
            // Disarm so subsequent ticks are no-ops.
            self.peer_mesh_check_at = None;
            return;
        }
        // peer_count drains new connections internally; calling it
        // BEFORE the deadline check lets a fresh connection clear
        // the watchdog without firing the fault.
        let connected = self.peer_transport.peer_count();
        if connected > 0 {
            self.peer_mesh_check_at = None;
            // Mesh formed for the first time — tell the primary so
            // it can release `PromotePrimary`. Idempotent via
            // `mesh_ready_sent`.
            self.report_mesh_ready_if_needed().await;
            return;
        }
        if std::time::Instant::now() < deadline {
            return;
        }
        // Latch the watchdog off first so it never re-fires.
        self.peer_mesh_check_at = None;
        self.peer_mesh_degraded = true;

        tracing::warn!(
            attempted = self.peer_dial_count,
            connected = 0,
            "peer mesh did not form — failover and inter-secondary \
             keepalive paths are unavailable; run will continue but \
             is fragile (tasks dispatched via primary→secondary WSS \
             still flow)"
        );

        // Report mesh-ready (with peer_count=0) so the primary's
        // `wait_for_mesh_ready` step releases `PromotePrimary`
        // instead of blocking the full mesh-ready timeout on a
        // secondary that will never see peers. Idempotent via
        // `mesh_ready_sent`.
        self.report_mesh_ready_if_needed().await;
    }

    /// Single source of truth for "have we told the primary the
    /// peer-mesh is settled?". Idempotent: the first call that
    /// observes a settled state (mesh formed, watchdog elapsed, or
    /// no peers were ever expected — i.e. single-secondary) emits
    /// `MeshReady` and flips the one-shot guard so subsequent calls
    /// are no-ops.
    ///
    /// Concern owned here, not at call sites: callers (the keepalive
    /// tick's `check_peer_mesh_watchdog` and the operational-loop
    /// entry hook) shouldn't have to know the rules — they just say
    /// "now's a moment the mesh state may have changed; report if
    /// anything to report". This keeps the modular boundary clean
    /// (peer.rs owns peer-mesh status; processing.rs just calls).
    pub(in crate::secondary) async fn report_mesh_ready_if_needed(&mut self) {
        if self.mesh_ready_sent {
            return;
        }
        // Three reportable states, all coalesced by this single
        // helper:
        //   - peer_dial_count == 0: no peers were expected (single-
        //     secondary run, or empty PeerInfo). Mesh is trivially
        //     "ready" the moment we reach the operational loop.
        //   - peer_count > 0: at least one dial landed; mesh has
        //     formed (further peers may keep arriving but the
        //     primary just needs the first non-empty signal).
        //   - peer_mesh_check_at is None AND peer_dial_count > 0:
        //     the watchdog has already cleared the deadline (either
        //     mesh formed, in which case the previous branch fired,
        //     or it elapsed with zero peers). The fully-failed case
        //     still reports so the primary doesn't wait the full
        //     mesh-ready timeout for nothing.
        let connected = self.peer_transport.peer_count() as u32;
        let no_peers_expected = self.peer_dial_count == 0;
        let mesh_formed = connected > 0;
        let watchdog_done =
            self.peer_dial_count > 0 && self.peer_mesh_check_at.is_none();
        if !(no_peers_expected || mesh_formed || watchdog_done) {
            return;
        }
        let msg: DistributedMessage<I> = DistributedMessage::MeshReady {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            peer_count: connected,
        };
        if let Err(e) = self.send_to_current_primary(msg).await {
            // Best-effort: log and flip the flag anyway so we
            // don't busy-retry on every keepalive tick. The
            // primary's wait step will time out (warning, not a
            // hard error — see lifecycle.rs `wait_for_mesh_ready`)
            // and the run continues.
            tracing::warn!(
                error = %e,
                "failed to send MeshReady to primary; primary will fall back to \
                 mesh-ready timeout before promoting primary"
            );
        } else {
            tracing::debug!(
                connected,
                "MeshReady sent to primary"
            );
        }
        self.mesh_ready_sent = true;
    }
}
