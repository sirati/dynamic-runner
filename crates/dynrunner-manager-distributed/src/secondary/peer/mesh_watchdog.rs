//! Peer-mesh-formation watchdog and the idempotent `MeshReady` reporter.
//!
//! Single concern: decide whether the peer mesh formed within the
//! one-shot watchdog deadline and tell the primary the answer exactly
//! once (mesh formed, mesh degraded, or no peers expected). The full
//! degraded-mode contract is documented on
//! `SecondaryCoordinator::peer_mesh_degraded`; this module owns only
//! the detection + first-report side.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
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
        let deadline = match self.mesh.peer_mesh_check_at {
            Some(d) => d,
            None => return,
        };
        if self.cluster_state.run_complete() || self.cluster_state.run_aborted().is_some() {
            // Run is over — completed cleanly OR aborted cluster-wide.
            // Either way the mesh fault has nothing to report:
            // failover and inter-secondary keepalive paths have nothing
            // left to guard once the run is terminating. Disarm so
            // subsequent ticks are no-ops. (`run_aborted` is the
            // failure twin of `run_complete`; both terminate the run,
            // so both disarm the watchdog — single source of truth here
            // rather than at every apply site.)
            self.mesh.peer_mesh_check_at = None;
            return;
        }
        // Real peer-secondary count — drains new connections internally
        // (`real_peer_count` calls `peer_count`), then EXCLUDES the
        // folded primary at this edge: the transport is role-blind
        // (de-role removed the in-transport primary exclusion), so its
        // raw `peer_count` now includes the primary as an ordinary mesh
        // peer. The watchdog asks "did the peer-SECONDARY mesh form?", so
        // it must not count the primary; a primary-only / firewalled
        // fleet (zero real peer-secondaries) would otherwise falsely
        // report "mesh formed". Edge-side exclusion keeps TRANSPORT⊥ROLES
        // (the transport stays role-blind). Read BEFORE the deadline
        // check so an all-expected connection clears the watchdog without
        // firing.
        //
        // FULL-FORMED happy path: clear the watchdog early ONLY when
        // EVERY expected real peer is connected (`connected ==
        // peer_dial_count`). `peer_dial_count` already counts only the
        // PeerInfo secondaries (the primary is NOT in the dial list — see
        // A4), so this is apples-to-apples with `real_peer_count`. A
        // PARTIAL mesh (0 < connected < expected) does NOT clear early:
        // it waits for the deadline, where it is reported as
        // formed-but-not-degraded (still failover-capable with ≥1 peer) —
        // the intended degraded-but-proceed path. (Pre-change this
        // cleared on `connected > 0`; the refinement is "all expected"
        // with timeout as the fallback.)
        let connected = self.real_peer_count();
        if connected == self.mesh.peer_dial_count as usize {
            self.mesh.peer_mesh_check_at = None;
            // Full mesh formed — tell the primary so it can release
            // `PromotePrimary`. Idempotent via `mesh_ready_sent`.
            self.report_mesh_ready_if_needed().await;
            return;
        }
        if std::time::Instant::now() < deadline {
            return;
        }
        // Deadline elapsed without a full mesh. Latch the watchdog off
        // first so it never re-fires.
        self.mesh.peer_mesh_check_at = None;
        // Degraded IFF truly lone: zero real peers connected. Threshold
        // is behaviourally UNCHANGED from before the primary-exclusion
        // edit (`== 0`), only the count now excludes the folded primary.
        // A partial mesh (≥1 real peer) is NOT degraded — two
        // fully-meshed secondaries can still elect (candidate + 1 voter),
        // so failover stays available; only a secondary that is alone
        // (no peer to gather quorum from) latches degraded so
        // `run_election_tick` bails clearly instead of self-promoting
        // solo.
        if connected == 0 {
            self.mesh.degraded = true;
            tracing::warn!(
                attempted = self.mesh.peer_dial_count,
                connected = 0,
                "peer mesh did not form — failover and inter-secondary \
                 keepalive paths are unavailable; run will continue but \
                 is fragile (tasks dispatched via primary→secondary WSS \
                 still flow)"
            );
        } else {
            tracing::warn!(
                attempted = self.mesh.peer_dial_count,
                connected,
                "peer mesh formed only partially before the deadline — \
                 proceeding degraded-but-capable (≥1 peer can still gather \
                 failover quorum); further peers may still arrive"
            );
        }

        // Report mesh-ready (with the real-peer count, which is 0 in the
        // lone case) so the primary's `wait_for_mesh_ready` step releases
        // `PromotePrimary` instead of blocking the full mesh-ready
        // timeout. Fires in EVERY terminal case — full, partial, or lone
        // — so the primary always unblocks. Idempotent via
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
        // Strict-observer suppression: MeshReady is a worker-secondary
        // signal — the primary defers `PromotePrimary` until every
        // worker secondary's mesh has settled. An observer has no
        // workers and is never a promotion candidate, so it must
        // originate NOTHING here (the mesh-ready concern's own role-gate,
        // matching `send_keepalive` / `run_election_tick`).
        if self.config.is_observer {
            return;
        }
        if self.mesh.mesh_ready_sent {
            return;
        }
        // Three reportable states, all coalesced by this single
        // helper:
        //   - peer_dial_count == 0: no peers were expected (single-
        //     secondary run, or empty PeerInfo). Mesh is trivially
        //     "ready" the moment we reach the operational loop.
        //   - real-peer count > 0: at least one peer-SECONDARY dial
        //     landed; mesh has formed (further peers may keep arriving
        //     but the primary just needs the first non-empty signal).
        //   - peer_mesh_check_at is None AND peer_dial_count > 0:
        //     the watchdog has already cleared the deadline (either
        //     mesh formed, in which case the previous branch fired,
        //     or it elapsed with zero peers). The fully-failed case
        //     still reports so the primary doesn't wait the full
        //     mesh-ready timeout for nothing.
        //
        // The count EXCLUDES the folded primary (de-role made the
        // transport role-blind, so its raw `peer_count` includes the
        // primary). Both the `mesh_formed` test and the reported
        // `peer_count` use the real-peer count so a primary-only fleet
        // reads as zero peers, matching the primary's `wait_for_mesh_ready`
        // which counts secondaries.
        let connected = self.real_peer_count() as u32;
        let no_peers_expected = self.mesh.peer_dial_count == 0;
        let mesh_formed = connected > 0;
        let watchdog_done = self.mesh.peer_dial_count > 0 && self.mesh.peer_mesh_check_at.is_none();
        if !(no_peers_expected || mesh_formed || watchdog_done) {
            return;
        }
        let msg: DistributedMessage<I> = DistributedMessage::MeshReady {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            peer_count: connected,
        };
        if let Err(e) = self.send_to_primary(msg).await {
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
            tracing::debug!(connected, "MeshReady sent to primary");
        }
        self.mesh.mesh_ready_sent = true;
    }

    /// Count of connected REAL peer-secondaries — the transport's
    /// connected-peer cardinality with the folded primary excluded.
    ///
    /// The transport is role-blind (de-role removed its in-transport
    /// primary exclusion), so `transport.peer_count()` now counts the
    /// primary as an ordinary mesh peer. The peer-mesh-formation concern
    /// asks specifically about peer-SECONDARY connectivity, so this edge
    /// helper subtracts the primary when it is itself a connected mesh
    /// peer (`has_peer(current_primary)`). Keeping the exclusion at this
    /// edge — not in the transport — preserves TRANSPORT⊥ROLES.
    ///
    /// Robust across the de-role cutover: when the primary is NOT a
    /// transport peer (the pre-de-role world, or any fleet where the
    /// primary link is a separate leg), `has_peer(primary)` is false and
    /// this is exactly `peer_count()`. When it IS a peer (post-de-role),
    /// it is `peer_count() - 1`.
    pub(in crate::secondary) fn real_peer_count(&self) -> usize {
        let raw = self.transport.peer_count();
        let primary_is_peer = self
            .cluster_state
            .current_primary()
            .map(|p| {
                self.transport.has_peer(
                    &dynrunner_protocol_primary_secondary::PeerId::from(p.to_string()),
                )
            })
            .unwrap_or(false);
        raw.saturating_sub(primary_is_peer as usize)
    }
}
