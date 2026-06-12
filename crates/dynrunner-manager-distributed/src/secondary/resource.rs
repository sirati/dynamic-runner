use dynrunner_core::{ErrorType, Identifier, ResourceKind, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_manager_local::oom::OomWatcher;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, SendTarget, resolve_destination,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

/// Wire marker used when a secondary's worker is killed by a no-fault
/// resource-stealing preempt (`KillReason::is_no_fault()`). The primary
/// recognises this string in [`PrimaryCoordinator::handle_task_failed`]
/// as a backpressure-shaped TaskFailed — re-queue the task at the
/// pool front WITHOUT consuming retry budget. Same shape as the
/// pre-existing `"No idle worker available"` and `"worker pipe broken;
/// respawning"` markers. The string is the public contract between
/// secondary and primary; do not change it without updating the
/// primary's `is_backpressure` predicate in the same commit.
pub const NO_FAULT_PREEMPT_WIRE_MESSAGE: &str = "worker no-fault preempt; resource stealing";

/// How long a SENT confirmable report (a terminal, or an IMPORTANT
/// custom message — F5) waits for the primary's app-level
/// [`DistributedMessage::TerminalAck`] before the reporting concern
/// treats the send as no-route-equivalent and replays it (#352).
///
/// Why 15s:
///   * It must be MEANINGFULLY shorter than the QUIC `max_idle_timeout`
///     (60s) — the blackholed-but-live leg this exists for buffers
///     `send.write_all` locally and returns `Ok` without delivering, and
///     is not pruned from `has_peer` until that idle timeout. At 15s the
///     replay engages within the task window and gets ≥3 attempts before
///     the transport would even notice the dead leg.
///   * It must be FAR above any genuine delivery+ack latency. A healthy
///     ack is one mesh round-trip (sub-second); 15s = 3 production
///     keepalive intervals (5s), so a loaded-but-live primary never
///     produces spurious replays in practice — and a spurious replay is
///     harmless anyway (the authority's hash-keyed terminal idempotence
///     dedupes, and the primary re-acks every landing).
///   * It sits below the primary-link failure window (30s) without
///     feeding it: an ack timeout records NO `record_recv_failure` —
///     delivery bookkeeping, never liveness.
pub(in crate::secondary) const DEFAULT_DELIVERY_ACK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(15);

/// How many replays of ONE confirmable report before the drain
/// escalates the per-attempt WARN to a PERMANENT-failure ERROR (#366) —
/// and re-escalates on every further multiple.
///
/// 8 replay attempts on the capped exponential schedule
/// ([`replay_backoff_delay`]: 15s → 30s → 60s-capped) ≈ 5–6 minutes of
/// continuous non-delivery: far past any transient blip the replay
/// loop exists to ride out (one mesh redial cycle, an election round),
/// yet quick enough that an operator watching a wedging phase barrier
/// gets the "this report is never going to make it" line while the run
/// is still inspectable. The canonical permanent cause is a frame over
/// the mesh wire limit — dropped LOUDLY at the transport egress gate
/// (`dynrunner-transport-quic::framing`) but undeliverable forever, so
/// without this tally its replay churn would look like an ordinary
/// outage WARN every backoff slot.
pub(in crate::secondary) const REPORT_REPLAY_ESCALATION_ATTEMPTS: u32 = 8;

/// Ceiling on the per-report exponential replay backoff
/// ([`replay_backoff_delay`]). An unACKed report keeps retrying forever
/// (delivery is an obligation), but never more often than once per cap
/// once the schedule has grown past it — frequent enough that a
/// restored-but-unnoticed route re-delivers within a minute, sparse
/// enough that a permanently-dead route costs one frame (and at most a
/// couple of log lines) per minute instead of one per loop tick (the
/// ~61/s production replay flood this cap exists to prevent).
pub(in crate::secondary) const REPORT_REPLAY_BACKOFF_CAP: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Aggregation window for the drain's "re-sent" INFO line: re-send
/// activity is tallied and emitted at most once per window (with the
/// suppressed count and the oldest entry's unACKed age), never one line
/// per drain pass — the production flood logged that line 19,437 times
/// in ~5 minutes (~97% of the secondary's log) while a leg was
/// blackholed.
pub(in crate::secondary) const REPORT_REPLAY_LOG_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(30);

/// The per-report replay schedule: how long after its `attempts`-th
/// replay a still-unACKed report waits for the next one. Doubles from
/// `ack_timeout` (`ack_timeout` → 2× → 4× …) and saturates at
/// [`REPORT_REPLAY_BACKOFF_CAP`]; overflow-safe for unbounded attempt
/// counts (the loop retries forever by design).
pub(in crate::secondary) fn replay_backoff_delay(
    ack_timeout: std::time::Duration,
    attempts: u32,
) -> std::time::Duration {
    let factor = 1u32
        .checked_shl(attempts.saturating_sub(1))
        .unwrap_or(u32::MAX);
    ack_timeout
        .saturating_mul(factor)
        .min(REPORT_REPLAY_BACKOFF_CAP)
}

/// One retained CONFIRMABLE report — a terminal-bearing report, or an
/// IMPORTANT custom message (F5) — in the buffered-report-replay queue,
/// tagged with WHY it is retained plus its OWN replay schedule (the
/// entry is the single owner of when it may next be re-sent — the
/// drain merely executes whatever is due).
#[derive(Debug)]
pub(in crate::secondary) struct RetainedReport<I> {
    /// The retained frame, `delivery_seq`-stamped at first send. Every
    /// re-send carries the SAME seq, so whichever landing reaches the
    /// authority matches the ack (and the authority's hash-keyed
    /// terminal idempotence makes any duplicate landing a no-op).
    pub(in crate::secondary) frame: DistributedMessage<I>,
    pub(in crate::secondary) state: RetainedSendState,
    /// How many times the drain has REPLAYED this entry (the first send
    /// at the chokepoint is not a replay). Drives the exponential
    /// backoff ([`replay_backoff_delay`]) and the permanent-failure
    /// escalation ([`REPORT_REPLAY_ESCALATION_ATTEMPTS`], #366). Lives
    /// here — not in a side table — because the entry is updated in
    /// place across replays, so the entry itself is the sticky
    /// identity.
    pub(in crate::secondary) attempts: u32,
    /// The earliest instant the next drain may re-send this entry — the
    /// per-report backoff schedule state. A fresh no-route retention is
    /// due immediately (`now`); a sent-awaiting-ack retention is due
    /// one `delivery_ack_timeout` after the send; each replay pushes it
    /// out by [`replay_backoff_delay`]. The operational loop's replay
    /// wake arm parks on the buffer-wide minimum of this field
    /// ([`SecondaryCoordinator::next_report_replay_due`]) — a
    /// PERSISTENT deadline (stored here, never derived from "now +
    /// interval" at the arm), so sibling select arms firing cannot
    /// push it back.
    pub(in crate::secondary) next_due: std::time::Instant,
    /// When this report was FIRST retained — the entry's unACKed age
    /// anchor for the aggregated drain log ("oldest unACKed Xs").
    /// Never touched by replays.
    pub(in crate::secondary) first_retained_at: std::time::Instant,
}

/// Why a confirmable report is retained in the replay buffer. WHEN it
/// is re-sent is the entry's own schedule
/// ([`RetainedReport::next_due`]); the reason only classifies the
/// retention for logging (the AwaitingAck → past-deadline WARN is the
/// blackholed-leg honesty signal #352 exists for).
#[derive(Debug)]
pub(in crate::secondary) enum RetainedSendState {
    /// The send was ABSORBED on a no-route (the pre-#352 retention
    /// reason): nothing was queued toward the primary. Retained with a
    /// due-NOW schedule slot (the next drain retries immediately, then
    /// backs off).
    NoRoute,
    /// The send returned `Ok` — queued toward a route the membership
    /// view calls live — but the primary's app-level `TerminalAck` has
    /// not yet landed (#352). `Ok` proves nothing about DELIVERY on a
    /// blackholed-but-live QUIC leg, so the frame stays retained; an
    /// ack drops it, and the schedule slot (`next_due`, one ack window
    /// after `sent_at`) makes the drain treat it as no-route-equivalent
    /// (replay, same seq).
    AwaitingAck { sent_at: std::time::Instant },
}

impl RetainedSendState {
    /// Test-visible state predicate (the production drain keys due-ness
    /// on the entry's `next_due`; tests assert on the retention reason
    /// itself).
    #[cfg(test)]
    pub(in crate::secondary) fn is_awaiting_ack(&self) -> bool {
        matches!(self, Self::AwaitingAck { .. })
    }
}

use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// THE egress edge: resolve the role-bearing [`Destination`] this
    /// coordinator owns the facts for, stamp it on the frame (the C3 routing
    /// field the RECEIVER's mesh-pump demuxes by), and queue the frame onto
    /// the one mesh through this coordinator's [`crate::process::MeshClient`].
    /// The coordinator never names a transport and never branches on
    /// locality.
    ///
    /// `resolve_destination` stays AT this coordinator (clarification H1):
    /// its role-specific bootstrap fallback (`current_primary()` warm after
    /// a `PrimaryChanged`, the bootstrap-primary id as the cold-cache
    /// fallback) is the GATE that produces the honest "no route to the
    /// primary" `Err` the failover-health probe in [`Self::send_to_primary`]
    /// keys on. The probe fires in TWO cases, both surfaced here before the
    /// frame is queued:
    ///   - `resolve_destination` returns `None` — no current primary AND no
    ///     bootstrap link, so nothing resolves at all.
    ///   - it resolves to a concrete remote [`SendTarget::Peer`] the
    ///     transport cannot DELIVER to by any path
    ///     (`!self.client.has_route(id)` — no direct connection AND no
    ///     relay forwarder). This is the one-mesh analogue of the
    ///     transport-level `send_to_peer(id) -> NoRoute`: because
    ///     [`crate::process::MeshClient::send`] is QUEUED (it returns `Ok`
    ///     the moment it enqueues, never observing the eventual wire result),
    ///     the no-route signal must be read from the pump-published
    ///     membership/routability view at egress, not awaited from the
    ///     send. The view is
    ///     ≤1-cycle stale + monotone-toward-truth, which is SAFE for the
    ///     probe: it never declares death (the probe only feeds a thresholded
    ///     health window that a successful keepalive resets), and a stale-high
    ///     `has_peer` merely delays the probe by one cycle — the keepalive
    ///     time-axis backstop covers that window.
    ///
    /// # Two `Destination`s: the routing send-target vs the C3 stamp
    ///
    /// The mesh-pump's `dispatch` routes the queued frame by the
    /// `MeshClient::send` `target` (loopback-vs-remote by id); the RECEIVER's
    /// pump demuxes it to a local slot by the frame's STAMPED `target()`.
    /// They are the same for all but the REMOTE-primary case, because
    /// [`Destination::Primary`] is id-less and the mesh cannot route it by
    /// host (the documented C3-seam `Mesh::dispatch` leaves open). So the
    /// egress resolves `Destination::Primary` to its concrete host BEFORE
    /// dispatch (per the `Mesh::dispatch` Primary-arm contract):
    ///   - `SendTarget::Loopback` (a promoted self): send `dst` itself — the
    ///     mesh loopbacks to the local role slot via `deliver_local`. Stamp
    ///     `dst`.
    ///   - `SendTarget::Peer(id)` from `Destination::Primary` (a REMOTE
    ///     primary): send an id-bearing target carrying `id` so the mesh
    ///     routes it by-id over the wire, but STAMP `Destination::Primary` so
    ///     the receiving pump delivers to that host's PRIMARY slot.
    ///   - `SendTarget::Peer(id)` from `Secondary`/`Observer`, and
    ///     `SendTarget::Broadcast`: send `dst` itself; stamp `dst`.
    ///
    /// Nothing is dropped: a self-addressed `Destination::Primary` loopbacks
    /// to the local primary slot; a remote one routes by its resolved host.
    pub(in crate::secondary) async fn send_to(
        &mut self,
        dst: Destination,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // No-route GATE — the failover-health probe substrate. Resolve the
        // role facts this coordinator owns; `None` is "no primary resolvable
        // at all".
        let target = resolve_destination(
            dst.clone(),
            self.cluster_state.current_primary(),
            self.bootstrap_primary_id.as_deref(),
            &self.config.secondary_id,
        )
        .ok_or_else(|| {
            "Destination::Primary unresolvable: no current primary in the role table and no \
             bootstrap primary link — no route to the primary"
                .to_string()
        })?;
        // A resolved remote host the transport cannot DELIVER to by any
        // path is the queued-mesh analogue of the old transport-level
        // NoRoute — surface it as the probe `Err`. `Loopback` (a promoted
        // self) and `Broadcast` never no-route.
        //
        // DELIVERABILITY, not direct membership (BUG 3.3): the gate reads
        // `has_route` — direct connection OR a live, non-blacklisted relay
        // forwarder — NEVER `has_peer` (direct-only). Gating on `has_peer`
        // dropped every primary-bound frame here the moment the direct
        // wire died, even while the transport's relay path could (and on
        // the inbound side DID) deliver: outbound resolution read dead
        // while relayed primary messages kept arriving and resetting the
        // health window — the production "death suspected → message
        // resumed" flip-flop, plus starved TaskRequests/terminal reports
        // that never reached a reachable primary. With `has_route` the
        // frame is queued and relays; the probe arms only when NOTHING
        // can deliver — exactly the state in which no inbound can arrive
        // either, so the two liveness readers agree by construction (one
        // state owner: the transport's Router).
        //
        // DIAGNOSTIC SPLIT (resolution honesty): "not a connected mesh
        // member" conflates two very different states an operator must
        // distinguish at the next incident — the host being ABSENT FROM
        // THE REPLICATED MEMBERSHIP (we believe it removed / never
        // joined: a membership decision) vs the host being a LIVE
        // REPLICATED MEMBER this node merely has NO TRANSPORT WIRE to
        // right now (a transport gap: redial/idle-timeout/blackhole —
        // NOT a removal). The probe semantics are identical (no route
        // either way); only the named cause differs.
        if let SendTarget::Peer(id) = &target
            && !self.client.has_route(id)
        {
            let membership = match self.cluster_state.peer_membership(id.as_str()) {
                crate::cluster_state::PeerMembership::AliveMember => {
                    "host IS a live replicated cluster member but this node \
                     has no transport wire to it right now (transport gap — \
                     redial/relay pending; NOT a membership removal)"
                }
                crate::cluster_state::PeerMembership::RemovedMember => {
                    "host was REMOVED from the replicated membership \
                     (PeerRemoved ledger) and is not wired"
                }
                crate::cluster_state::PeerMembership::NeverJoined => {
                    "host is not in the replicated membership (never joined \
                     / join not yet observed) and is not wired"
                }
            };
            return Err(format!(
                "no route to {id}: {membership}; no relay path through any \
                 connected peer (queued-mesh no-route — failover-health probe)"
            ));
        }
        // The C3 stamp is ALWAYS the role-bearing intent `dst` — it is what
        // the receiver demuxes to a slot. The routing send-target carries the
        // resolved host ONLY for a remote `Destination::Primary` (id-less, so
        // the mesh can't route it by host without the resolution done here).
        let send_target = match (&dst, &target) {
            (Destination::Primary, SendTarget::Peer(id)) => Destination::Secondary(id.clone()),
            _ => dst.clone(),
        };
        // Queue it. `MeshClient::send` is QUEUED (M4): the pump drains it and
        // routes loopback-or-remote against the live slots by `send_target`,
        // and the receiving pump demuxes by the stamped `dst`.
        self.client.send(send_target, msg.with_target(dst))
    }

    /// Send an operational frame to whoever currently holds the
    /// primary role, feeding the failover-health probe on a no-route
    /// result.
    ///
    /// This is the single chokepoint for every primary-bound
    /// operational send (TaskRequest, terminal TaskComplete/TaskFailed,
    /// Keepalive, MeshReady). It addresses [`Destination::Primary`] and
    /// the edge resolver ([`Self::send_to`]) picks the concrete peer —
    /// the current primary, the bootstrap primary while cold, or
    /// loopback for a promoted self; the manager never inspects which.
    ///
    /// # Failover-health probe (the fast path)
    ///
    /// A clean `Err` from the send means "no route to the primary": no
    /// primary resolves at all, OR the resolved host is unreachable by
    /// ANY transport path — no direct connection and no relay forwarder
    /// (the `has_route` deliverability gate in [`Self::send_to`]).
    /// That is the fast-failover signal — it arms the count-axis of
    /// `PrimaryLink` immediately, well before the keepalive time-axis
    /// would. The probe is transport-AGNOSTIC: the manager reacts only
    /// to a send RESULT, never to `peer_count()` or a recv-None branch
    /// or any locality inspection. A successful send resets the health
    /// window via the normal `record_primary_message` path when the
    /// primary's reply / keepalive arrives.
    ///
    /// On a breach `primary_last_seen` is backdated. This is NOT what
    /// trips the local election any more — `run_election_tick`'s fast leg
    /// (A) reads `primary_link.should_arm_failover()` directly. The
    /// backdate is RETAINED for the peer-side confirmation gates that
    /// still key on the `keepalive_interval × keepalive_miss_threshold`
    /// deadline (`record_promotion_vote`'s `primary_silent` + a peer's
    /// own Suspecting quorum tally): on a busy genuine death the link
    /// arms fast, and funnelling the no-route signal into
    /// `primary_last_seen` lets those gates agree immediately rather than
    /// stalling the full ~15s deadline. The backdate (≈20s) is far below
    /// `primary_silence_backstop` (≈120s), so it never trips the
    /// election's patient leg (B).
    pub(in crate::secondary) async fn send_to_primary(
        &mut self,
        mut msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // The frame is consumed by `send_to`; classify it FIRST so a
        // no-route absorb can decide whether to retain it for replay
        // (CONFIRMABLE reports — the terminals AND an IMPORTANT custom
        // message (F5) — must never be lost; see the absorb branch
        // below). The classifier lives on the enum
        // (`requires_delivery_ack`), so this site owns NO message-shape
        // knowledge beyond "must this report provably reach the
        // authority?".
        let is_confirmable = msg.requires_delivery_ack();
        // App-level delivery-confirmation stamp (#352): THIS chokepoint
        // is the single owner of `delivery_seq` assignment — every
        // confirmable primary-bound report gets the next value of the
        // per-secondary monotonic counter on its FIRST pass through
        // here. A replayed frame already carries its seq (the stamp is
        // sticky on the retained copy), so a re-send goes out with the
        // SAME seq and the primary's ack matches whichever landing got
        // through. Non-confirmable sends are never stamped (the accessor
        // is a no-op for them anyway).
        if is_confirmable && msg.delivery_seq().is_none() {
            let seq = self.next_delivery_seq;
            self.next_delivery_seq += 1;
            msg.set_delivery_seq(seq);
        }
        // Causal custom-message watermark stamp (the terminal-ordering
        // gate): a TASK TERMINAL leaving this origin for the first time
        // carries the highest IMPORTANT custom `msg_seq` stamped so far
        // (`next_custom_msg_seq` is the next-to-assign, so `- 1` is the
        // last assigned; 0 = "none sent", no gate). The operational
        // loop's pool-event seam runs the CAUSAL FENCE before any
        // terminal-shaped worker event (`process_worker_pool_event`):
        // first the worker-message PIPELINE FLUSH — a barrier through
        // the dispatcher channel proving the consumer's
        // `worker_message_listener` has run for every custom the worker
        // sent before exiting (their relay commands are in the control
        // queue) — then the control-queue PRE-DRAIN, which
        // `msg_seq`-stamps and sends (or retains) them. So every
        // message the task handed over BEFORE its terminal's triggering
        // event is sequenced by now — the stamp is a true causal
        // watermark, and every seq it covers is guaranteed to resolve
        // at the primary (at-least-once retention while this origin
        // lives; a dead origin's gates are opened by the primary's
        // membership check). `task_hash()` is `Some` exactly for the
        // two terminal variants; the `is_none()` guard keeps the stamp
        // STICKY on the retained replay copy (same contract as
        // `delivery_seq` above).
        if msg.task_hash().is_some() && msg.msgs_posted_through().is_none() {
            msg.set_msgs_posted_through(self.next_custom_msg_seq - 1);
        }
        let report_hash = msg.task_hash().map(str::to_owned);
        let report_seq = msg.delivery_seq();
        let msg_kind = msg.msg_type();
        // Clone ONLY when the frame is confirmable and might need
        // retaining; the common (droppable) path never clones.
        let replay_copy = if is_confirmable {
            Some(msg.clone())
        } else {
            None
        };
        let result = self.send_to(Destination::Primary, msg).await;
        if let Err(ref e) = result {
            // No route to the primary — feed the failover-health probe
            // (shared with the replay drain's re-send edge).
            self.note_primary_no_route(e);
            // FAILOVER-B: a no-route is NOT a run-fatal error — it is a
            // failover SIGNAL, fully recorded above into the primary-link
            // health window. Returning the no-route `Err` here would
            // `?`-propagate up every operational caller
            // (`request_task_for_worker`, the TaskComplete/TaskFailed
            // reports in `worker_event`/`dispatch`) and ABORT the run loop
            // — deliberately killing a VOTER on primary-loss instead of
            // letting `run_election_tick` enter `Suspecting`. A primary
            // death MUST recover via election, never abort. So we ABSORB
            // the no-route into `Ok(())` and let the loop continue so the
            // election (already armed) runs; the secondary holds no
            // authority and owns no requeue.
            //
            // BUFFERED-REPORT-REPLAY (the recovery): a CONFIRMABLE
            // report resolves obligated state at the authority — a
            // terminal (`TaskComplete` / `TaskFailed`, incl. the
            // backpressure-shaped deferred-lost reinject) resolves a task's
            // in-flight entry, so absorbing it without retention strands
            // that task forever (phantom-busy; the phase barrier wedges);
            // an IMPORTANT custom message (F5) is the consumer's
            // must-not-lose payload, contractually delivered
            // at-least-once. So when the absorbed send was confirmable we
            // RETAIN the frame in the reporting concern's replay buffer
            // instead of dropping it; the next drain (the loop's replay
            // wake arm — due immediately for a fresh no-route retention,
            // then per-entry backoff — or the primary-link recovery
            // edge) re-sends it FIFO, retrying forever until delivered
            // — including to a NEW primary after failover, since the
            // drain's egress re-resolves
            // `Destination::Primary` to the current holder at the egress
            // edge. The authority is idempotent on a duplicate landing
            // (hash-keyed `completed_tasks`/`failed_tasks` dedup;
            // backpressure requeue gated on `free_slot_on_terminal`'s
            // held-hash match; the custom inbox's `(origin, msg_seq)`
            // vacant-insert NoOp), so a re-delivery that races an
            // original is at-most-once-effective.
            //
            // Non-confirmable primary-bound sends (keepalives, capacity
            // `TaskRequest`s, `MeshReady`, DROPPABLE customs) are NOT
            // retained: a missed periodic frame is re-emitted on the next
            // tick, and a droppable custom is at-most-once by contract.
            // The gate is `is_confirmable`, computed above off the enum
            // classifier.
            //
            // This is the NO-ROUTE abort — DISTINCT from the
            // `mesh_degraded` split-brain guard in `run_election_tick`,
            // which is preserved: a genuinely-lone (zero-peer) secondary
            // still bails there rather than self-promoting on `quorum=1`.
            //
            // `send_to(Destination::Primary, …)` errors ONLY on no-route
            // (the two branches in `send_to`'s no-route gate; the queued
            // `MeshClient::send` never surfaces a wire-level error here),
            // so absorbing the `Err` discards no other error class. The
            // `Result` return is retained for a future genuinely-fatal
            // primary-bound send class, should one ever exist.
            if let Some(retained) = replay_copy {
                // RETAIN the confirmable report FIFO (push at the back
                // so the buffer stays in arrival order; the drain
                // updates entries in place, never reorders). Due NOW:
                // nothing was queued, so the next drain trigger retries
                // immediately (then the per-entry backoff takes over).
                let now = std::time::Instant::now();
                self.pending_report_replays.push(RetainedReport {
                    frame: retained,
                    state: RetainedSendState::NoRoute,
                    attempts: 0,
                    next_due: now,
                    first_retained_at: now,
                });
                let buffered = self.pending_report_replays.len();
                tracing::warn!(
                    error = %e,
                    msg_kind = ?msg_kind,
                    task_hash = ?report_hash,
                    delivery_seq = ?report_seq,
                    buffered,
                    "primary-bound CONFIRMABLE report absorbed on no-route; \
                     retained for replay ({buffered} buffered)"
                );
            } else {
                tracing::debug!(
                    error = %e,
                    msg_kind = ?msg_kind,
                    "primary-bound droppable send absorbed on no-route \
                     (re-emitted next tick / at-most-once by contract)"
                );
            }
            return Ok(());
        }
        // SENT-BUT-UNACKED retention (#352, the Half-B honesty fix): an
        // `Ok` here only proves the frame was queued toward a route the
        // membership view calls live — on a blackholed-but-not-timed-out
        // QUIC leg `send.write_all` buffers locally and returns Ok while
        // the bytes never arrive, and `has_peer` stays true until the
        // 60s idle timeout (well past the task window). So a
        // confirmable report is RETAINED on success too, in the
        // SAME replay buffer with the `AwaitingAck` retention reason:
        // the primary's app-level `TerminalAck { seq }` is the ONLY
        // event that drops it, and `sent_at` aging past the ack timeout
        // makes the next drain treat it as no-route-equivalent (replay,
        // same seq). NO failover-health input is touched on this path —
        // the ack is delivery bookkeeping, not liveness.
        if let Some(retained) = replay_copy {
            let now = std::time::Instant::now();
            self.pending_report_replays.push(RetainedReport {
                frame: retained,
                state: RetainedSendState::AwaitingAck { sent_at: now },
                attempts: 0,
                // Due one ack window from the send: an ack landing
                // inside it drops the entry; aging past it makes the
                // drain treat the send as no-route-equivalent (#352).
                next_due: now + self.delivery_ack_timeout,
                first_retained_at: now,
            });
            tracing::debug!(
                msg_kind = ?msg_kind,
                task_hash = ?report_hash,
                delivery_seq = ?report_seq,
                buffered = self.pending_report_replays.len(),
                "primary-bound CONFIRMABLE report sent; retained awaiting \
                 the app-level TerminalAck"
            );
        }
        result
    }

    /// Feed one no-route-to-primary observation into the
    /// failover-health probe — the single owner of the
    /// `record_recv_failure` + backdate reaction, shared by the
    /// first-send chokepoint ([`Self::send_to_primary`]) and the replay
    /// drain's re-send edge. `record_recv_failure` anchors the failure
    /// window on the first breach and returns true once the count- or
    /// time-axis threshold is crossed.
    fn note_primary_no_route(&mut self, e: &str) {
        let armed = self.op_mut().primary_link.record_recv_failure();
        if armed {
            tracing::warn!(
                error = %e,
                "no route to primary (resolved primary peer unreachable / no primary \
                 resolvable); failover-health threshold breached — arming election"
            );
            let backdate = self
                .config
                .keepalive_interval
                .saturating_mul(self.config.keepalive_miss_threshold + 1);
            self.op_mut().primary_last_seen = Some(
                std::time::Instant::now()
                    .checked_sub(backdate)
                    .unwrap_or_else(std::time::Instant::now),
            );
        } else {
            tracing::debug!(
                error = %e,
                "no route to primary; recording failover-health probe \
                 (threshold not yet breached)"
            );
        }
    }

    /// Drop the retained confirmable report whose `delivery_seq` the
    /// primary just confirmed (#352) — the app-level delivery proof that
    /// releases the sent-but-unacked retention.
    ///
    /// Exact-seq match (no ack-up-to coalescing): replays re-send the
    /// same seq, possibly to a NEW primary after failover, so cumulative
    /// semantics could falsely confirm an earlier seq that travelled a
    /// different, still-blackholed leg. An ack for an unknown seq is a
    /// benign duplicate (the entry was already acked, or the landing was
    /// a replay whose first ack got through) and is logged at DEBUG.
    ///
    /// Delivery bookkeeping ONLY: no `primary_link` input is read or
    /// written here.
    pub(in crate::secondary) fn ack_delivery(&mut self, seq: u64) {
        let before = self.pending_report_replays.len();
        // Dropping the entry also drops its replay-attempt tally and
        // its backoff schedule — both live ON the entry, so a confirmed
        // delivery leaves no side state behind.
        self.pending_report_replays
            .retain(|entry| entry.frame.delivery_seq() != Some(seq));
        if self.pending_report_replays.len() < before {
            tracing::debug!(
                seq,
                buffered = self.pending_report_replays.len(),
                "report delivery confirmed by primary ack; dropped from \
                 the replay buffer"
            );
        } else {
            tracing::debug!(
                seq,
                "TerminalAck for an unknown delivery_seq (already acked / \
                 duplicate landing); no-op"
            );
        }
    }

    /// Drain the buffered-report-replay queue: re-send every DUE
    /// retained confirmable report FIFO, retrying forever until the
    /// primary's app-level [`DistributedMessage::TerminalAck`] confirms
    /// delivery ([`Self::ack_delivery`] is the only drop site).
    /// Returns the number of reports re-sent (the test observable for
    /// the replay-flood bound).
    ///
    /// The reporting concern's RE-DELIVERY edge. Due-ness is the
    /// entry's OWN schedule ([`RetainedReport::next_due`]): a fresh
    /// no-route retention is due immediately, a sent-awaiting-ack
    /// retention one ack window after its send (the
    /// blackholed-but-live-leg detection, #352), and every replay
    /// pushes the entry's next slot out on the capped exponential
    /// backoff ([`replay_backoff_delay`]: `ack_timeout` → 2× → 4× …
    /// capped). The schedule is what bounds the replay rate when the
    /// ack can never come — pre-backoff a `NoRoute` entry was due on
    /// EVERY drain trigger and a blackholed secondary re-sent the same
    /// seq at operational-loop speed (~61/s, the production replay
    /// flood).
    ///
    /// A due frame is re-sent through [`Self::send_to`] directly (NOT
    /// `send_to_primary` — the chokepoint would retain a SECOND entry;
    /// this drain owns the one existing entry and updates it in place),
    /// still re-resolving `Destination::Primary` to whoever holds the
    /// role NOW (a NEW primary after failover routes automatically) and
    /// carrying the SAME `delivery_seq` (the stamp is sticky — the
    /// idempotency-key contract). A failed re-send feeds the
    /// failover-health probe exactly as a first send does
    /// ([`Self::note_primary_no_route`]). One seq is re-sent at most
    /// ONCE per pass (coalescing — a defensive duplicate entry waits
    /// for the next pass). Never dropped here, never reordered: the
    /// WHOLE buffer is taken (`std::mem::take`) and every entry — kept
    /// or re-sent — re-appends to the now-empty live buffer in
    /// iteration order. Duplicate landings at the authority are safe
    /// (hash-keyed terminal idempotence; the primary re-acks every
    /// landing).
    ///
    /// Called from the two re-delivery triggers: the operational loop's
    /// replay wake arm (parked on [`Self::next_report_replay_due`] —
    /// fires only when an entry is due, never per tick) and the
    /// `record_primary_message` primary-link-recovery edge (via
    /// [`Self::drain_report_replays_now`], which overrides the schedule
    /// for a prompt route-restored retry). A no-op when the buffer is
    /// empty (the steady-state hot path), and silent when nothing is
    /// due.
    pub(in crate::secondary) async fn drain_report_replays(&mut self) -> usize {
        self.drain_report_replays_inner(false).await
    }

    /// Schedule-overriding drain for the ROUTE-RESTORED edge: re-send
    /// every retained report NOW instead of waiting out its backoff
    /// slot. Called from the `record_primary_message` primary-link-
    /// recovery edge — the route to the primary just provably came
    /// back, so a retained report sitting deep inside a (possibly
    /// 60s-capped) slot must not wait it out. Each re-send still
    /// advances the entry's schedule, so a recovery edge that lied
    /// (route immediately gone again) degrades back to the backoff
    /// cadence, not a flood.
    pub(in crate::secondary) async fn drain_report_replays_now(&mut self) -> usize {
        self.drain_report_replays_inner(true).await
    }

    /// The operational loop's replay wake deadline: the EARLIEST
    /// `next_due` across the retained entries, `None` when the buffer
    /// is empty (the wake arm parks). The deadline is PERSISTENT state
    /// (stored on the entries), so the arm re-creating its
    /// `sleep_until` future every loop iteration always re-targets the
    /// same instant — sibling arms firing cannot push it back
    /// (watchdog-fires-under-load law).
    pub(in crate::secondary) fn next_report_replay_due(&self) -> Option<std::time::Instant> {
        self.pending_report_replays
            .iter()
            .map(|entry| entry.next_due)
            .min()
    }

    async fn drain_report_replays_inner(&mut self, force_due: bool) -> usize {
        if self.pending_report_replays.is_empty() {
            return 0;
        }
        // Take the whole buffer; every entry re-appends to the now-empty
        // live buffer in iteration order (kept or re-sent), preserving
        // FIFO order across drains.
        let pending = std::mem::take(&mut self.pending_report_replays);
        let ack_timeout = self.delivery_ack_timeout;
        let now = std::time::Instant::now();
        let mut resent = 0usize;
        // Coalescing: one in-flight replay per seq per pass. After a
        // long outage many entries become due at once — each distinct
        // seq goes out once; a duplicate of an already-sent seq keeps
        // its (overdue) slot and rides the next pass.
        let mut sent_seqs: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for mut entry in pending {
            let seq = entry.frame.delivery_seq();
            let due = force_due || now >= entry.next_due;
            let coalesced = seq.is_some_and(|s| sent_seqs.contains(&s));
            if !due || coalesced {
                // Inside its backoff slot (the common just-sent case —
                // the ack may still be in flight), or its seq already
                // went out this pass: keep waiting.
                self.pending_report_replays.push(entry);
                continue;
            }
            let task_hash = entry.frame.task_hash().map(str::to_owned);
            if let RetainedSendState::AwaitingAck { sent_at } = entry.state
                && now.duration_since(sent_at) >= ack_timeout
            {
                // The blackhole detection edge firing: the transport
                // accepted the send but no ack came back within the
                // window — surface it, this is the honesty signal #352
                // exists for. Bounded per entry by the backoff
                // schedule, never per drain pass.
                tracing::warn!(
                    task_hash = ?task_hash,
                    delivery_seq = ?seq,
                    attempts = entry.attempts,
                    unacked_for_secs = now.duration_since(sent_at).as_secs_f64(),
                    ack_timeout_secs = ack_timeout.as_secs_f64(),
                    "confirmable report sent but UNACKED past the ack timeout \
                     (possible blackholed-but-live leg); treating as \
                     no-route-equivalent and replaying with the same seq"
                );
            }
            // Advance the entry's schedule BEFORE the send: the replay
            // is attempt N regardless of its outcome, and a failed send
            // must consume the slot too (a permanently-dead route backs
            // off instead of flooding).
            entry.attempts = entry.attempts.saturating_add(1);
            entry.next_due = now + replay_backoff_delay(ack_timeout, entry.attempts);
            // Permanent-failure escalation (#366): the per-entry tally
            // crossing the threshold (and every further multiple)
            // escalates to ERROR naming the task and the likely causes.
            if entry.attempts >= REPORT_REPLAY_ESCALATION_ATTEMPTS
                && entry
                    .attempts
                    .is_multiple_of(REPORT_REPLAY_ESCALATION_ATTEMPTS)
            {
                tracing::error!(
                    task_hash = ?task_hash,
                    delivery_seq = ?seq,
                    attempts = entry.attempts,
                    "confirmable report has been replayed {} times \
                     without an ack — PERMANENT delivery failure \
                     suspected. Likely causes: the frame exceeds the \
                     mesh wire limit (look for 'dropping outbound mesh \
                     frame' ERRORs from the transport egress gate) or \
                     the primary link is persistently blackholed. The \
                     task stays unresolved at the authority until this \
                     report lands",
                    entry.attempts
                );
            }
            // Re-send through the egress edge with the SAME stamped
            // frame (same seq — never re-stamp). `Destination::Primary`
            // re-resolves to the CURRENT holder.
            entry.state = match self
                .send_to(Destination::Primary, entry.frame.clone())
                .await
            {
                Ok(()) => RetainedSendState::AwaitingAck { sent_at: now },
                Err(e) => {
                    // Still no route: feed the failover-health probe
                    // (same as a first send) and keep the NoRoute
                    // classification; the advanced schedule above is
                    // what keeps this from flooding.
                    self.note_primary_no_route(&e);
                    RetainedSendState::NoRoute
                }
            };
            if let Some(s) = seq {
                sent_seqs.insert(s);
            }
            resent += 1;
            self.pending_report_replays.push(entry);
        }
        if resent > 0 {
            self.note_replays_for_log(resent, now);
        }
        resent
    }

    /// Aggregated, rate-limited drain log: tally re-sends and emit at
    /// most one INFO per [`REPORT_REPLAY_LOG_WINDOW`] carrying the
    /// suppressed total and the oldest entry's unACKed age — never one
    /// line per drain pass (the production flood logged 19,437 of the
    /// per-pass line in ~5 minutes). A tally accumulated inside a
    /// closed window rides along until the next re-send after the
    /// window opens (replays only stop on ack/route-recovery, at which
    /// point there is nothing left worth logging).
    fn note_replays_for_log(&mut self, resent: usize, now: std::time::Instant) {
        self.replay_log_suppressed += resent;
        let window_open = self
            .replay_log_last_emit
            .is_none_or(|last| now.duration_since(last) >= REPORT_REPLAY_LOG_WINDOW);
        if !window_open {
            return;
        }
        let oldest_unacked_secs = self
            .pending_report_replays
            .iter()
            .map(|entry| now.duration_since(entry.first_retained_at))
            .max()
            .unwrap_or_default()
            .as_secs_f64();
        tracing::info!(
            resent = self.replay_log_suppressed,
            buffered = self.pending_report_replays.len(),
            oldest_unacked_secs,
            window_secs = REPORT_REPLAY_LOG_WINDOW.as_secs(),
            "buffered-report-replay drain re-sent confirmable reports \
             (aggregated over the rate-limit window)"
        );
        self.replay_log_last_emit = Some(now);
        self.replay_log_suppressed = 0;
    }

    /// Report a respawn-HOLD-deferred task whose worker died before it
    /// could run (the worker disconnected between `RespawnInProgress`
    /// and the expected `Ready`, or `assign_task` failed at the
    /// post-Ready dispatch). The task NEVER ran, so it must be requeued
    /// at the authority — not counted as a failure. A backpressure-
    /// shaped `TaskFailed` (`Recoverable` + the `"worker pipe broken;
    /// respawning"` marker the authority's `is_backpressure` predicate
    /// recognises) is the wire contract that drives the requeue +
    /// re-dispatch.
    ///
    /// CLASS-1 own-worker report: the secondary is never the authority,
    /// so this is the SOLE recovery for a lost deferred task — there is
    /// no local pool to requeue into.
    pub(in crate::secondary) async fn report_deferred_task_lost(
        &mut self,
        worker_id: WorkerId,
        file_hash: &str,
    ) -> Result<(), String> {
        let msg = DistributedMessage::TaskFailed {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            task_hash: file_hash.to_string(),
            error_type: ErrorType::Recoverable,
            error_message: "worker pipe broken; respawning".into(),
            // Stamped at the send_to_primary chokepoint (#352).
            delivery_seq: None,
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        };
        self.send_to_primary(msg).await
    }

    /// Report a deferred (first-bind) task whose worker died BY ITS
    /// OWN FAULT before the task could run — the worker was spawned ON
    /// BEHALF of this task and self-destructed (nonzero self-exit,
    /// deterministic-bug signal, OOM kill), so the spawn IS the task's
    /// execution attempt and it COUNTS against the task's retry
    /// budget. The charged twin of [`Self::report_deferred_task_lost`]:
    /// a regular terminal `TaskFailed` whose `error_message` is the
    /// death diagnosis — NEVER a marker the authority's
    /// `is_backpressure` predicate recognises — so the authority's
    /// `failed_tasks` → retry-bucket → permanence accounting sees the
    /// attempt (asm-tokenizer run_20260612_095601: the uncharged
    /// requeue here is what let one broken-args task re-dispatch
    /// 24,323 times with fail counters flat at zero).
    ///
    /// CLASS-1 own-worker report; the authority owns all accounting.
    pub(in crate::secondary) async fn report_deferred_task_failed(
        &mut self,
        worker_id: WorkerId,
        file_hash: &str,
        error_type: ErrorType,
        error_message: String,
    ) -> Result<(), String> {
        let msg = DistributedMessage::TaskFailed {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            task_hash: file_hash.to_string(),
            error_type,
            error_message,
            // Stamped at the send_to_primary chokepoint (#352).
            delivery_seq: None,
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        };
        self.send_to_primary(msg).await
    }

    /// Drain a `pending_first_bind[worker_id]` stash (if any) into the
    /// CHARGED terminal path: the slot's subprocess died by its own
    /// fault while this deferred task was awaiting its `Ready`, so the
    /// loss counts against the task's retry budget (see
    /// [`Self::report_deferred_task_failed`]). The fault-attributed
    /// sibling of [`Self::reinject_pending_first_bind`]; the
    /// `Disconnected` arm picks between the two on the
    /// [`dynrunner_manager_local::oom::DisconnectFault`] verdict.
    /// Returns `true` iff a stash WAS drained (same contract as the
    /// no-fault sibling).
    pub(in crate::secondary) async fn fail_pending_first_bind(
        &mut self,
        worker_id: WorkerId,
        error_type: ErrorType,
        error_message: String,
    ) -> Result<bool, String> {
        if let Some(pending) = self.op_mut().pending_first_bind.remove(&worker_id) {
            let pending_hash = pending.file_hash.clone();
            tracing::warn!(
                worker_id,
                task_hash = %pending_hash,
                error_type = ?error_type,
                error = %error_message,
                "worker died by its own fault while a first-bind task was \
                 deferred awaiting Ready; reporting the deferred task as a \
                 COUNTED failure (charges its retry budget)"
            );
            self.report_deferred_task_failed(worker_id, &pending_hash, error_type, error_message)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Recover whatever a worker slot held before its subprocess is
    /// REPLACED — covering BOTH places a slot's work can live: the
    /// running task in `active_tasks` AND the deferred-awaiting-Ready
    /// task in `pending_first_bind`.
    ///
    /// A worker-replacement edge installs a fresh subprocess (a new
    /// generation) into the slot and bumps the slot generation. Two
    /// structures can still reference the OLD slot occupant. The
    /// `active_tasks` entry is a task the prior subprocess was running:
    /// if left, it is silently abandoned (assigned-never-terminal) and
    /// wedges the phase barrier. The `pending_first_bind` entry is a task
    /// stashed by a first-bind `RespawnInProgress`, deferred until the
    /// (now-replaced) generation's `Ready`: the replacement bumps the
    /// generation, so the prior watcher's `Ready` is stale-dropped by the
    /// generation gate (`handle_worker_event`) — the Ready arm's
    /// `pending_first_bind.remove` never runs, and with no disconnect
    /// either the stash would sit forever (never assigned, never terminal
    /// — the round-2 wedge this sweep closes).
    ///
    /// Both are swept through the SAME backpressure-shaped reinject
    /// contract `report_deferred_task_lost` uses (the authority requeues
    /// then re-dispatches without consuming retry budget). No-op for a
    /// slot holding neither (the common case: replacement of an idle or
    /// already-swept slot).
    ///
    /// INVARIANT this enforces: a slot replacement may never leave a
    /// `pending_first_bind` entry that no future event will touch. Every
    /// replacement edge that bumps the generation funnels through this
    /// chokepoint (the type-shift router edge sweeps the PRIOR stash
    /// before installing the fresh one; the restart loop sweeps before
    /// `restart_worker_async`). The OOM path reports its OWN
    /// `active_tasks` terminal with a resource classification, so it
    /// does not call this whole sweep — but it still drains the deferred
    /// stash via [`Self::reinject_pending_first_bind`] (the deferred task
    /// never ran, so it is backpressure-reinjected, NOT
    /// ResourceExhausted).
    pub(in crate::secondary) async fn sweep_replaced_worker_task(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(), String> {
        let file_hash = self
            .op_mut()
            .active_tasks
            .iter()
            .find(|&(_, &wid)| wid == worker_id)
            .map(|(hash, _)| hash.clone());
        if let Some(hash) = file_hash {
            self.op_mut().active_tasks.remove(&hash);
            tracing::warn!(
                worker_id,
                task_hash = %hash,
                "worker slot replaced while still bound to a task; \
                 sweeping it into reinject (backpressure) so the \
                 replaced generation cannot strand it"
            );
            self.report_deferred_task_lost(worker_id, &hash).await?;
        }
        // Also drain a deferred first-bind stash the replacement would
        // otherwise strand (the stale-Ready round-2 wedge). The drained
        // flag is irrelevant to this sweep's `()` contract — both halves
        // are recovery, not a decision input here.
        self.reinject_pending_first_bind(worker_id).await?;
        Ok(())
    }

    /// Drain a `pending_first_bind[worker_id]` stash (if any) into the
    /// backpressure reinject path before the slot is replaced.
    ///
    /// A deferred first-bind task NEVER RAN — it was stashed awaiting the
    /// slot's `Ready` and the replacement bumped the generation out from
    /// under it. So it is reinjected as backpressure (`Recoverable` +
    /// the `"worker pipe broken; respawning"` marker the authority's
    /// `is_backpressure` predicate recognises), the SAME shape the
    /// `Disconnected` arm's own pre-Ready drain uses — exactly one owner
    /// of "report a lost deferred task". No-op when the slot has no
    /// stash.
    ///
    /// Drained on every generation-bumping replacement edge:
    /// `sweep_replaced_worker_task` (type-shift router, restart loop)
    /// calls it; the OOM path calls it directly. The `Disconnected` arm
    /// pops the stash itself BEFORE flagging the restart, so by the time
    /// the restart loop's `sweep_replaced_worker_task` reaches this drain
    /// the entry is already gone — popped-then-replaced means no
    /// double-report.
    ///
    /// Returns `true` iff a stash WAS drained (a deferred task was found +
    /// reinjected). The `Disconnected` arm reads this to suppress its
    /// "disconnect-with-error resolved to no active task" WARN when the
    /// deferred-stash drain is what resolved the worker — a swept stash is
    /// a recovered task, not a silent loss.
    pub(in crate::secondary) async fn reinject_pending_first_bind(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<bool, String> {
        if let Some(pending) = self.op_mut().pending_first_bind.remove(&worker_id) {
            let pending_hash = pending.file_hash.clone();
            tracing::warn!(
                worker_id,
                task_hash = %pending_hash,
                "worker slot replaced while a first-bind task was deferred \
                 awaiting Ready; reinjecting it (backpressure) so the \
                 stale-dropped Ready cannot strand it"
            );
            self.report_deferred_task_lost(worker_id, &pending_hash)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Route the resource-pressure decision tick through the OOM
    /// watcher (mirrors `LocalManager::check_resource_pressure_via_watcher`).
    /// The watcher invokes `WorkerPool::check_resource_pressure`
    /// internally so it can record kill events for the structured-log
    /// trigger; the secondary-specific kill-outcome handling
    /// (TaskFailed mesh broadcast + worker restart + request new
    /// task) stays here.
    pub(super) async fn check_resource_pressure_via_watcher(
        &mut self,
        watcher: &mut OomWatcher,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let max = self.max_resources();
        // Clone the scheduler before borrowing the operational pool: the
        // pool now lives inside `OperationalState` (reached via
        // `op_mut()`, a full `&mut self` borrow), so a simultaneous
        // `&self.scheduler` shared borrow would conflict. The scheduler
        // is `Clone`-bounded in this impl and cheap to clone (a
        // config-shaped value); cloning once per decision tick keeps the
        // disjoint borrows clean without a manual struct destructure.
        let scheduler = self.scheduler.clone();
        let result = watcher.on_decision(&mut self.op_mut().pool, &scheduler, &max, false);
        self.handle_resource_pressure_result(result, factory).await;
    }

    /// Secondary-specific outcome handler. Pulled out of the prior
    /// `check_resource_pressure` body so both the watcher-driven path
    /// and any future direct caller share the same TaskFailed-broadcast
    /// + restart + request rules.
    ///
    /// Routing is keyed on [`KillReason`]:
    ///
    ///   * No-fault preempt (memory stealing or under-budget) →
    ///     broadcast a backpressure-shaped `TaskFailed` carrying
    ///     [`NO_FAULT_PREEMPT_WIRE_MESSAGE`]. The primary's
    ///     `handle_task_failed` recognises this marker, requeues the
    ///     task at the pool front, and skips the `failed_tasks`
    ///     insert — retry budget is preserved.
    ///   * At-fault OOM (over budget / last resort) → today's path:
    ///     broadcast `TaskFailed { ErrorType::ResourceExhausted(memory) }`.
    ///     Consumes one retry attempt and surfaces in
    ///     `resource_pressure_tasks` for the OOM retry pass.
    ///
    /// Worker restart + new-task request runs in both arms — the
    /// killed worker is gone either way, so the slot needs a fresh
    /// subprocess and a new assignment from the primary.
    async fn handle_resource_pressure_result(
        &mut self,
        result: ResourcePressureResult<I>,
        factory: &mut impl WorkerFactory<M>,
    ) {
        match result {
            ResourcePressureResult::Killed {
                worker_id, reason, ..
            } => {
                // Find and report the task as failed
                let op = self.op_mut();
                let file_hash = op
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.op_mut().active_tasks.remove(&hash);

                    let (error_type, error_message) = if reason.is_no_fault() {
                        (ErrorType::Recoverable, NO_FAULT_PREEMPT_WIRE_MESSAGE.into())
                    } else {
                        (
                            ErrorType::ResourceExhausted(ResourceKind::memory()),
                            reason.as_str().into(),
                        )
                    };

                    let msg = DistributedMessage::TaskFailed {
                        target: None,
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type,
                        error_message,
                        // Stamped at the send_to_primary chokepoint (#352).
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    };
                    // Report to the primary role only. The AUTHORITY
                    // originates the terminal CRDT mutation and
                    // broadcasts it to the mesh, so every peer/observer
                    // mirror converges — the reporting secondary must
                    // NOT broadcast itself (a second CRDT originator
                    // would break the authority's apply-before-dispatch
                    // ordering).
                    let _ = self.send_to_primary(msg).await;
                }

                // The OOM-kill REPLACES the slot (restart below bumps the
                // generation), so a first-bind task deferred awaiting the
                // prior generation's Ready would otherwise be stranded by
                // the stale-dropped Ready. Reinject it as backpressure —
                // the deferred task NEVER RAN, so it is NOT classified
                // ResourceExhausted with the running task above; it rides
                // the generic deferred-lost reinject. The `active_tasks`
                // arm above already reported the running task's terminal
                // with its resource classification, so this drains a
                // DISJOINT structure (a slot in `pending_first_bind` is
                // Transitioning, not running) — no double-report.
                let _ = self.reinject_pending_first_bind(worker_id).await;

                // Restart the worker NON-BLOCKINGLY. This handler runs
                // inside the operational `select!` (the OOM-decision arm),
                // so it must not inline-wait for the new subprocess's
                // Ready — that would hold the `select!` open for the whole
                // slow-worker startup window and starve the keepalive,
                // exactly the wedge `restart_worker_async` exists to
                // avoid. The `WorkerEvent::Ready` arm reclaims the slot
                // and re-issues its `TaskRequest` once the replacement
                // reports `Response::Ready`, so the post-restart repoll
                // rides that arm rather than a (premature) call here.
                if let Err(e) = self
                    .op_mut()
                    .pool
                    .restart_worker_async(worker_id, factory, false)
                    .await
                {
                    tracing::error!(worker_id, error = %e, "secondary OOM-restart failed");
                }
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Send a `TaskRequest` for one idle worker to the current primary
    /// role.
    ///
    /// A pure capacity hint: rate-limited per worker by `primary_link.
    /// should_request_now`, then dispatched through `send_to_primary`
    /// (the [`Destination::Primary`] egress edge resolves the concrete
    /// primary peer — current or bootstrap — and the manager never
    /// branches on locality). Since the P2 transport collapse this no
    /// longer needs a `WorkerFactory`: the request never spawns or
    /// restarts a worker, it only advertises the worker's available
    /// capacity to the authority.
    pub(super) async fn request_task_for_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(), String> {
        if !self.op_mut().primary_link.should_request_now(worker_id) {
            return Ok(());
        }

        let available_memory = if (worker_id as usize) < self.op_mut().pool.workers.len() {
            self.op_mut().pool.workers[worker_id as usize]
                .reserved_budgets
                .get(&dynrunner_core::ResourceKind::memory())
        } else {
            self.config
                .max_resources
                .get(&dynrunner_core::ResourceKind::memory())
                / self.config.num_workers as u64
        };

        let msg = DistributedMessage::TaskRequest {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            available_resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: available_memory,
            }],
        };
        self.op_mut().primary_link.note_request_sent(worker_id);

        self.send_to_primary(msg).await
    }

    /// Periodic safety-net wakeup: walk every idle worker and call
    /// `request_task_for_worker`. The per-worker exponential backoff
    /// (held by `primary_link`, doubling from 1s to a 60s cap) suppresses
    /// requests within the backoff window, so the only fan-out cost is
    /// the in-budget polls — which is precisely the work the kickstart
    /// pattern would have done anyway.
    ///
    /// Only meaningful for the primary failover path (peer
    /// secondaries' workers don't get kickstarted by the primary
    /// when a phase activates) and edge cases on the live-primary path
    /// (a worker that got "no work" between two other workers'
    /// completions and the primary's kickstart targeted only one of
    /// them). Regular live-primary runs see most polls suppressed by
    /// the backoff because the kickstart already covers the path.
    pub(super) async fn repoll_idle_workers(&mut self) {
        let n = self.op_mut().pool.workers.len();
        for wid in 0..n {
            // Re-borrow per iteration: the idle-state read (an `op_mut`
            // borrow) must end before the `request_task_for_worker`
            // await (which re-borrows `op_mut` internally for the
            // rate-limiter + capacity read).
            if self.op_mut().pool.workers[wid].is_idle_state() {
                let _ = self.request_task_for_worker(wid as WorkerId).await;
            }
        }
    }
}
