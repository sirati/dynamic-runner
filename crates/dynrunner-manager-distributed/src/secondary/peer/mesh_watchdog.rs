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
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// One-shot watchdog: 30s after `connect_to_peers` fired with a
    /// non-empty peer list, decide whether the peer mesh formed.
    /// Self-healing if the mesh forms before the deadline (an alive
    /// secondary suppresses the fault) or partially forms after the
    /// deadline (any incoming peer connection clears `peer_mesh_check_at`,
    /// no fault).
    ///
    /// "How many peers connected" is the role-aware
    /// [`SecondaryCoordinator::alive_secondary_count`] — alive secondaries
    /// over GLOBAL STATE, filtered POSITIVELY on the secondary capability
    /// (a host running primary+secondary under one peer-id counts; an
    /// observer does not) —
    /// NEVER the transport's role-blind `peer_count()`: post-de-role the
    /// transport counts the folded primary as an ordinary mesh peer, so
    /// asking IT "how many peer-secondaries" would falsely report a lone
    /// secondary as a formed mesh. The role question belongs at this
    /// coordinator edge over global state (TRANSPORT⊥ROLES), not as
    /// transport arithmetic.
    ///
    /// On confirmed full-mesh failure (deadline elapsed, zero peers)
    /// the run enters DEGRADED mode rather than dying:
    ///   1. `peer_mesh_degraded` is latched true so callers that
    ///      need the mesh (failover election, peer-broadcast
    ///      keepalives) can fail loud or skip — the contract is
    ///      owned by those callers, not by this watchdog.
    ///   2. `MeshReady` is sent with `peer_count=0` so the primary's
    ///      `wait_for_mesh_ready` step releases its `PrimaryChanged` announcement and
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
        // Role-aware alive-secondary count over GLOBAL STATE — the
        // watchdog asks "did the peer-SECONDARY mesh form?", so it counts
        // alive secondaries (`alive_secondary_count`: peers that POSITIVELY
        // have a live secondary — keepalive-fresh in this operational
        // regime), NOT the transport's role-blind `peer_count()` (which now
        // counts the folded primary). A primary-only / firewalled fleet
        // (zero alive peer-secondaries) therefore correctly reads zero and
        // does NOT falsely report "mesh formed". Read BEFORE the deadline
        // check so an all-expected set clears the watchdog without firing.
        //
        // FULL-FORMED happy path: clear the watchdog early ONLY when
        // EVERY expected secondary is alive (`connected ==
        // peer_dial_count`). `peer_dial_count` already counts only the
        // PeerInfo secondaries (the primary is NOT in the dial list — see
        // A4), so this is apples-to-apples with `alive_secondary_count`. A
        // PARTIAL mesh (0 < connected < expected) does NOT clear early:
        // it waits for the deadline, where it is reported as
        // formed-but-not-degraded (still failover-capable with ≥1 peer) —
        // the intended degraded-but-proceed path.
        let connected = self.alive_secondary_count();
        if connected == self.mesh.peer_dial_count as usize {
            self.mesh.peer_mesh_check_at = None;
            // Full mesh formed — previously a SILENT success (#362):
            // the watchdog disarmed and nothing named it, so a member
            // whose mesh "formed" only in the CRDT-liveness sense
            // (alive_secondary_count counts keepalive-fresh peers over
            // GLOBAL state, NOT transport legs) left zero trace either
            // way. Name the success AND its decision inputs.
            tracing::info!(
                alive_secondaries = connected,
                expected = self.mesh.peer_dial_count,
                "peer mesh formed (every expected secondary alive) before \
                 the watchdog deadline; disarming the mesh watchdog"
            );
            // Tell the primary so it can release its `PrimaryChanged`
            // announcement. Idempotent via `mesh_ready_sent`.
            self.report_mesh_ready_if_needed().await;
            return;
        }
        if std::time::Instant::now() < deadline {
            return;
        }
        // Deadline elapsed without a full mesh. Latch the watchdog off
        // first so it never re-fires.
        self.mesh.peer_mesh_check_at = None;
        // Degraded IFF truly lone: zero alive secondaries connected. The
        // threshold (`== 0`) is behaviourally unchanged; the count is now
        // the role-aware `alive_secondary_count` over global state rather
        // than transport arithmetic.
        // A partial mesh (≥1 peer) is NOT degraded — two
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
            // The "failover-capable" claim is now BACKED by a real,
            // idle-independent trigger: `run_election_tick`'s leg (C) arms
            // the election when the primary LEAVES the transport
            // `MembershipView` (pump `handle_peer_disconnect`), so a survivor
            // with ≥1 peer genuinely DOES detect primary-death and elect
            // without needing a pending primary-bound send. (Pre-leg-(C) this
            // message overpromised: an IDLE survivor never opened leg (A)'s
            // send-driven window and would silently reconnect-loop a dead
            // primary instead of electing — the message named a capability the
            // arming path could not deliver. Reconciled to match the wired
            // trigger.)
            tracing::warn!(
                attempted = self.mesh.peer_dial_count,
                connected,
                "peer mesh formed only partially before the deadline — \
                 proceeding degraded-but-capable (≥1 peer can gather \
                 failover quorum, and primary-departure arms the election \
                 idle-independently); further peers may still arrive"
            );
        }

        // Report mesh-ready (with the real-peer count, which is 0 in the
        // lone case) so the primary's `wait_for_mesh_ready` step releases
        // its `PrimaryChanged` announcement instead of blocking the full mesh-ready
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
        if self.mesh.mesh_ready_sent {
            return;
        }
        // Three reportable states, all coalesced by this single
        // helper:
        //   - peer_dial_count == 0: no peers were expected (single-
        //     secondary run, or empty PeerInfo). Mesh is trivially
        //     "ready" the moment we reach the operational loop.
        //   - alive-secondary count > 0: at least one peer-SECONDARY is
        //     alive; mesh has formed (further peers may keep arriving
        //     but the primary just needs the first non-empty signal).
        //   - peer_mesh_check_at is None AND peer_dial_count > 0:
        //     the watchdog has already cleared the deadline (either
        //     mesh formed, in which case the previous branch fired,
        //     or it elapsed with zero peers). The fully-failed case
        //     still reports so the primary doesn't wait the full
        //     mesh-ready timeout for nothing.
        //
        // Role-aware count over GLOBAL STATE (`alive_secondary_count`:
        // peers that POSITIVELY have a live secondary), NOT the transport's
        // role-blind `peer_count()`. Both the `mesh_formed` test and the
        // reported `peer_count` use it so a primary-only fleet reads as
        // zero peers, matching the primary's `wait_for_mesh_ready` which
        // counts secondaries.
        let connected = self.alive_secondary_count() as u32;
        let no_peers_expected = self.mesh.peer_dial_count == 0;
        let mesh_formed = connected > 0;
        let watchdog_done = self.mesh.peer_dial_count > 0 && self.mesh.peer_mesh_check_at.is_none();
        if !(no_peers_expected || mesh_formed || watchdog_done) {
            // Not settled yet (peers expected, none alive, watchdog
            // still pending). Called on every keepalive tick, so this
            // stays at DEBUG — but it is no longer a silent return:
            // the inputs that withheld the report are visible.
            tracing::debug!(
                alive_secondaries = connected,
                expected = self.mesh.peer_dial_count,
                watchdog_pending = self.mesh.peer_mesh_check_at.is_some(),
                "MeshReady not reportable yet (mesh unsettled)"
            );
            return;
        }
        // Which settled condition fired — the reporter's decision input,
        // named on the send line so the operator can distinguish a real
        // formed mesh from a watchdog-elapsed (possibly zero-peer) report.
        let trigger = if no_peers_expected {
            "no-peers-expected"
        } else if mesh_formed {
            "mesh-formed"
        } else {
            "watchdog-elapsed"
        };
        let msg: DistributedMessage<I> = DistributedMessage::MeshReady {
            target: None,
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
            // INFO, not debug (#362): this send is the member's half of
            // the pairwise dispatch-readiness confirmation — its absence
            // on a member is exactly the production "no MeshReady"
            // symptom, so the send itself must be operator-visible.
            // `alive_secondaries` counts keepalive-fresh peer
            // SECONDARIES over global state (the reported peer_count),
            // NOT transport connections — a discrepancy between this
            // line and the transport's sweep/registration lines is
            // itself diagnostic.
            tracing::info!(
                alive_secondaries = connected,
                expected = self.mesh.peer_dial_count,
                trigger,
                "MeshReady sent to primary"
            );
        }
        self.mesh.mesh_ready_sent = true;
    }

    /// Re-arm the one-shot reporter for a NEW primary identity and
    /// re-announce immediately if the mesh state is reportable.
    ///
    /// "Mesh-leg confirmed" is PAIRWISE — member ↔ CURRENT primary. The
    /// `mesh_ready_sent` latch records "the primary has been told", so
    /// its scope is the primary IDENTITY, not the process lifetime: a
    /// promoted/relocated primary starts with an EMPTY node-local
    /// confirmed set (`mesh_ready_secondaries` is deliberately not
    /// CRDT-inherited — the predecessor's ledger proves legs to the OLD
    /// node), and without a re-send this member is structurally
    /// unrecoverable into it. The dispatch-readiness gate
    /// (`member_mesh_confirmed`) then withholds the member from every
    /// proactive dispatch — production run_20260610_130116: 10 of 15
    /// members at zero tasks while an injected batch packed onto the
    /// confirmed stragglers.
    ///
    /// Called from the primary-identity-change reaction
    /// (`react_to_primary_identity_change`), i.e. only after a GENUINELY
    /// applied `PrimaryChanged` — stale-epoch NoOps never reach here, so
    /// the unchanged pair is never spammed. The re-send rides
    /// [`Self::report_mesh_ready_if_needed`] unchanged: a settled mesh
    /// (formed, watchdog-terminal, or no-peers-expected) reports
    /// immediately to `Destination::Primary` (re-resolved at the egress
    /// edge — the NEW primary); an UNSETTLED mesh reports nothing now,
    /// and the watchdog's terminal report (latch now clear) reaches
    /// whoever then holds the role — the bring-up announcement flow is
    /// unchanged, just re-pointed. The primary-side `handle_mesh_ready`
    /// insert is unconditional and duplicate-tolerant, so the
    /// re-announce rides the existing late-MeshReady recovery.
    pub(in crate::secondary) async fn rearm_mesh_ready_for_new_primary(&mut self) {
        self.mesh.mesh_ready_sent = false;
        self.report_mesh_ready_if_needed().await;
    }
}
