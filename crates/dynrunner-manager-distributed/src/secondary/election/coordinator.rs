//! `SecondaryCoordinator` methods implementing the failover-election
//! state machine. The pure-data parts of the state machine
//! (`ElectionState`, `ElectionTickActions`, the round-bump helper)
//! live in [`super`]; this file contains only the per-method handlers
//! that mutate the coordinator's election + primary-link fields.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;
use super::{ElectionState, ElectionTickActions, next_round};

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Bump the primary-keepalive timestamp on every primary message and
    /// abandon any in-flight failover election. Called from the dispatch
    /// path before any other handling.
    ///
    /// Who holds the primary role is NOT tracked here — it lives solely
    /// in the replicated `cluster_state` (`current_primary()`, the single
    /// source of "who is primary now"). Routing reads it at the egress
    /// edge (`send_to` → `resolve_destination`); a `PrimaryChanged`
    /// mutation is the only re-point. This method records liveness and
    /// cancels a false-alarm election; it writes no routing target,
    /// because there is no transitional routing target anymore (P2).
    pub(in crate::secondary) fn record_primary_message(&mut self) {
        let secondary_id = self.config.secondary_id.clone();
        let op = self.op_mut();
        op.primary_last_seen = Some(Instant::now());
        // Reset the primary-link health sub-state. A real message
        // arriving on the (possibly-reconnected) transport proves
        // the link is alive again, so any failure-window we'd been
        // tracking should be discarded. Pre-fix the failover
        // arming kept counting probes against a stale window even
        // after a brief flap recovered, so the second flap would
        // arm faster than the first — confusing semantics. The
        // reset closes that loop. Idempotent on a healthy link.
        op.primary_link.record_recv_success();
        // Cancel a failover election in progress: a primary message
        // proves the original primary is still reachable so the
        // election was a false alarm. Revert to Normal. There is no
        // transitional routing target to clear — routing always
        // resolves `Destination::Primary` at the egress edge over
        // `cluster_state.current_primary()`, which still names the
        // original primary (no `PrimaryChanged` committed during the
        // aborted election, per the drop-the-transitional-hint design).
        if matches!(
            op.election,
            ElectionState::Suspecting { .. }
                | ElectionState::Voting { .. }
                | ElectionState::Candidate { .. }
        ) {
            // Visibility on election recovery: pre-fix the
            // transition from Suspecting/Voting/Candidate back to
            // Normal was silent — an operator tailing the log saw
            // the entering-Suspecting WARN but no resolution
            // signal when keepalives resumed. With this log they
            // can grep "election recovered" to confirm a
            // transient blip rather than chase a phantom election
            // failure.
            tracing::info!(
                secondary = %secondary_id,
                from = ?std::mem::discriminant(&op.election),
                "election recovered: primary message resumed, reverting to Normal"
            );
            op.election = ElectionState::Normal;
        }
    }

    /// The live mesh peers for failover quorum/candidate reasoning: the
    /// keys of `peer_keepalives` MINUS the current primary's host id.
    ///
    /// A co-located primary+secondary host emits a `Secondary` keepalive
    /// that lands in `peer_keepalives` even though its id is the current
    /// primary (the recognition arm tracks a multi-role host as BOTH). That
    /// entry is correct as peer-mesh liveness but must NEVER inflate
    /// election counts — the primary is the role being failed-over FROM, not
    /// a peer that could vote for or become a candidate. Excluding it here
    /// is the single quorum-side counterpart to the peer-timeout sweep's
    /// own current-primary skip (`check_peer_timeouts`), both keyed on the
    /// single source of "who is primary now" (`current_primary()`).
    pub(in crate::secondary) fn live_peer_ids(&self) -> impl Iterator<Item = &String> {
        let current_primary = self.cluster_state.current_primary();
        // `peer_keepalives` now lives in `OperationalState`; outside
        // `Operational` there are no peer keepalives to enumerate (the
        // election that calls this only runs `Operational`), so an empty
        // iterator is the faithful pre-`Operational` answer. Both
        // borrows (`cluster_state` + the operational state) are shared
        // `&self` reads of disjoint fields, so they coexist.
        self.op_ref()
            .into_iter()
            .flat_map(|op| op.peer_keepalives.keys())
            .filter(move |id| Some(id.as_str()) != current_primary)
    }

    /// The role-aware "alive secondaries" set — the single coordinator
    /// answer to "which peers run a LIVE SECONDARY right now", filtered
    /// POSITIVELY on the secondary capability. This is the role-aware
    /// count the PeerMesh deliberately CANNOT answer: the transport
    /// exposes only role-blind `peer_count()` (raw connection
    /// cardinality), so the peer-mesh-formation watchdog and the
    /// cold-start "is any secondary reachable" branch read THIS, computed
    /// over global state — never `transport.peer_count()`/`has_peer`.
    ///
    /// POSITIVE FILTER — never a role negation. A host runs any
    /// independent subset of {primary, secondary, observer} under one
    /// peer-id, so "is an alive secondary" is answered by the secondary
    /// capability itself, NOT by `!primary && !observer`. Consequently a
    /// co-located primary+secondary host COUNTS (it positively has a
    /// secondary), an observer is absent because it has no secondary (it
    /// emits no `Secondary` keepalive / advertises no workers), and a
    /// primary-only host is absent for the same positive reason. This is
    /// exactly where the failover-quorum notion DIFFERS: `live_peer_ids`
    /// additionally subtracts `current_primary` (you cannot elect the node
    /// being failed-over-from to replace itself), an election-specific
    /// rule that does NOT belong to the "is a secondary present" question.
    ///
    /// Two positive liveness signals, one per lifecycle regime — selected
    /// by which ledger physically exists, NOT by an arbitrary flag:
    ///   - OPERATIONAL (the `OperationalState` exists): `peer_keepalives`
    ///     keys. A peer is in this map IFF it emitted a `Secondary`
    ///     keepalive — positive proof it runs a live secondary (the
    ///     co-located primary included; see the recognition arm in
    ///     `peer/message_handler.rs`). A peer swept by `check_peer_timeouts`
    ///     is correctly gone.
    ///   - SETUP / pre-operational (no `OperationalState`, so no
    ///     `peer_keepalives`): replicated membership via
    ///     [`ClusterState::alive_secondary_members`] (advertised
    ///     worker-secondary capacity AND live). It grows as each peer's
    ///     `PeerJoined` + `SecondaryCapacity` land during setup — the
    ///     faithful "has any secondary joined" answer for the cold-start
    ///     branch, where keepalives do not yet flow.
    pub(in crate::secondary) fn alive_secondary_ids(&self) -> Vec<String> {
        if let Some(op) = self.op_ref() {
            op.peer_keepalives.keys().cloned().collect()
        } else {
            self.cluster_state
                .alive_secondary_members()
                .map(str::to_owned)
                .collect()
        }
    }

    /// Count of [`Self::alive_secondary_ids`] — the role-aware
    /// alive-secondary cardinality. The peer-mesh-formation watchdog and
    /// the cold-start "is any secondary reachable" branch read this; it
    /// NEVER derives from `transport.peer_count()`.
    pub(in crate::secondary) fn alive_secondary_count(&self) -> usize {
        self.alive_secondary_ids().len()
    }

    /// Advance the election state. Called once per processing-loop tick.
    /// Returns the broadcast/self-send messages the loop should flush.
    pub(in crate::secondary) fn run_election_tick(&mut self) -> ElectionTickActions<I> {
        let mut actions = ElectionTickActions::default();
        if matches!(
            self.op_ref().map(|op| &op.election),
            Some(ElectionState::Promoted)
        ) {
            return actions;
        }

        // Snapshot the reads that touch `cluster_state` / `live_peer_ids`
        // (both `&self`-method/`cluster_state` borrows) up front, BEFORE
        // binding the `&mut OperationalState` for the state-machine match
        // below. `election`, `primary_last_seen`, and `peer_keepalives`
        // now all live inside `OperationalState`; the match needs a `&mut`
        // borrow of that state, which cannot coexist with the `&self`
        // borrows `live_peer_ids()` / `cluster_state.role_table()` take.
        // `peer_keepalives` does not change within a single tick (the
        // election reads it, never mutates it here), so a single snapshot
        // of the live-peer set + the observer set is faithful to the
        // original per-arm re-reads. `current_primary_id` is the silent
        // primary's diagnostic identity + the `TimeoutQuery` target.
        let current_primary_id = self.cluster_state.current_primary().map(str::to_owned);
        let live_peers: Vec<String> = self.live_peer_ids().cloned().collect();
        let observers: std::collections::HashSet<String> = self
            .cluster_state
            .role_table()
            .observers
            .iter()
            .cloned()
            .collect();
        let mesh_degraded = self.mesh.degraded;
        let secondary_id = self.config.secondary_id.clone();

        // Honest liveness — by SOURCE, not by a bare receive-staleness
        // clock. The decision to suspect the primary and start a failover
        // election is the OR of two predicates, each honest about a
        // distinct death mode, so the secondary rides out a transient
        // keepalive blip exactly as the primary side does (the QUIC layer
        // keeps a quiet-but-live link up to `max_idle_timeout` ≈60s):
        //
        //   (A) genuine-dead-link (fast): `primary_link.should_arm_failover()`.
        //       The primary-link health window arms ONLY when a
        //       primary-bound send returns a no-route `Err` via
        //       `send_to_primary` (the connection is closed / no primary
        //       resolves). A live-but-app-quiet QUIC connection still
        //       enqueues sends `Ok` (mpsc to a live writer task), so this
        //       leg stays SILENT during a blip and fires only on a genuine
        //       link death — the honest fast signal, identical to the one
        //       `check_primary_link_threshold` already polls each keepalive
        //       tick.
        //   (B) wedged-primary backstop (patient): `primary_last_seen`
        //       staleness past `primary_silence_backstop` (≈2 min). Covers
        //       the ONLY case (A) cannot — a primary alive at QUIC but
        //       wedged at the application layer (it sends nothing yet its
        //       connection stays routable, so no send ever errors).
        //
        // The bare `keepalive_interval × keepalive_miss_threshold` (≈15s)
        // receive-staleness trigger is GONE: it could not distinguish a
        // blip from a dead primary, so it spuriously elected at 15s during
        // a blip while the primary side patiently waited. Lengthening it to
        // 2 min would instead delay EVERY genuine failover by 2 min; the
        // (A)+(B) split keeps fast failover for a real dead link and is
        // patient only when death is genuinely indistinguishable from a blip.
        //
        // `primary_last_seen` is refreshed by `record_primary_message`,
        // driven by the role-tagged recognition path in `handle_inbound`'s
        // Keepalive arm: a `Primary`-tagged keepalive whose originator IS
        // the current primary refreshes it, so a promoted peer's primary
        // keepalives feed leg (B) exactly like the co-located primary's
        // traffic once did. A co-located primary's Secondary keepalive lands
        // in `peer_keepalives` (it is a live mesh peer) but is excluded from
        // quorum/candidate counts by `live_peer_ids`, so peer liveness and
        // primary liveness stay cleanly separate.
        //
        // Bind the operational state by direct field projection (borrows
        // only `self.lifecycle`, leaving `self.config` / `self.fatal_exit`
        // / `self.mesh` reachable as disjoint fields). The
        // `cluster_state` / `live_peer_ids` reads were already snapshotted
        // above, so nothing in the match below needs a `&self` method.
        let backstop = self.config.primary_silence_backstop;
        // The standard death deadline (`keepalive_interval ×
        // keepalive_miss_threshold`, ≈15s). It NO LONGER gates
        // `need_election` (the honest (A)+(B) split above replaced the bare
        // staleness trigger); it survives ONLY as the per-peer agreement
        // threshold the Suspecting-quorum tally compares each
        // `TimeoutResponse` age against — a SEPARATE predicate about
        // whether a PEER also sees the primary as silent, not about whether
        // WE should start an election.
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let op = self
            .lifecycle
            .operational_mut()
            .expect("run_election_tick reached before Operational — type invariant violation");

        // (A) genuine-dead-link — the primary link's own no-route arming.
        let link_dead = op.primary_link.should_arm_failover();
        // (B) wedged-primary backstop — patient receive-staleness, ONLY for
        // an app-silent primary whose link never armed leg (A).
        let primary_silence_exceeded = op
            .primary_last_seen
            .map(|t| Instant::now().duration_since(t) > backstop)
            .unwrap_or(false);

        let need_election = link_dead || primary_silence_exceeded;

        match &op.election {
            ElectionState::Normal if need_election => {
                // Degraded-mesh failover guard: the election protocol
                // needs a peer mesh to gather quorum responses
                // (`TimeoutQuery` / `TimeoutResponse` /
                // `PromotionVote` / `PromotionConfirm` all flow over
                // peer_transport). With zero peers, the next
                // Suspecting tick would self-promote on `quorum=1` —
                // the same secondary that just lost its only primary
                // would unilaterally claim authority. That's worse
                // than failing loud: there's no other surviving
                // node to coordinate with, so the cluster is
                // already unsalvageable. Bail with a clear reason
                // instead of pretending the election succeeded.
                // Who the silent primary was (co-located or a promoted
                // peer), snapshotted above from the single source of "who
                // is primary now" — BOTH the diagnostic identity and the
                // `TimeoutQuery::query_node_id` the peers reply about. It
                // drives no election decision (that is `primary_silent`
                // alone).
                if mesh_degraded {
                    let reason = format!(
                        "peer mesh required for failover but not \
                         available: primary death suspected \
                         (link_dead={link_dead}, \
                         primary_silence_exceeded={primary_silence_exceeded}) \
                         and no peers connected to elect a new primary; \
                         exiting",
                    );
                    tracing::error!(
                        secondary = %secondary_id,
                        link_dead,
                        primary_silence_exceeded,
                        primary = ?current_primary_id,
                        "{reason}"
                    );
                    // `fatal_exit` is a flat coordinator write-latch (a
                    // disjoint field from `self.lifecycle`, so writable
                    // while `op` is borrowed). The loop reads it once per
                    // iteration and exits non-zero.
                    self.fatal_exit = Some(reason);
                    return actions;
                }
                tracing::warn!(
                    secondary = %secondary_id,
                    link_dead,
                    primary_silence_exceeded,
                    primary = ?current_primary_id,
                    "primary death suspected (dead link or app-silence backstop); \
                     entering Suspecting"
                );
                op.election = ElectionState::Suspecting {
                    since: Instant::now(),
                    responses: HashMap::new(),
                };
                if let Some(query_node_id) = current_primary_id {
                    actions.broadcast.push(DistributedMessage::TimeoutQuery {
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        query_node_id,
                    });
                }
            }
            ElectionState::Suspecting { since, responses } => {
                // Wait at least `keepalive_interval` to gather peer responses
                // before counting votes. This is the natural window for a
                // peer to reply once it has also seen the primary go silent.
                if since.elapsed() < self.config.keepalive_interval {
                    return actions;
                }
                // `live_peers` was snapshotted from `live_peer_ids`, which
                // already excludes the current primary, so its length IS
                // the live-peer count.
                let peer_count = live_peers.len();
                // Each `TimeoutResponse.last_keepalive` is now a monotonic AGE
                // (seconds since the responder last saw the queried node, on the
                // responder's own monotonic clock — see the `TimeoutQuery` arm),
                // NOT an absolute wall-clock timestamp. A peer "agrees" the
                // primary is silent iff that age exceeds the death deadline; a
                // `None` (never seen) also agrees. Comparing a relative age means
                // there is NO cross-node wall-clock subtraction, so a coordinated
                // suspend/resume cannot fabricate a false quorum on primary death.
                let agreeing = responses
                    .values()
                    .filter(|age| {
                        age.map(|secs| secs > deadline.as_secs_f64())
                            .unwrap_or(true)
                    })
                    .count()
                    + 1; // include us
                let quorum = peer_count.div_ceil(2) + 1;
                if agreeing < quorum {
                    tracing::info!(agreeing, quorum, "no quorum on primary death; waiting");
                    return actions;
                }
                // Filter observer peers from candidate selection. An
                // observer in the alive set with a lex-low ID would
                // otherwise be deferred-to by non-observer peers; the
                // observer (a standalone ObserverCoordinator) is never a
                // candidate, so deferring to it would stall the cluster.
                //
                // Read source: `cluster_state.role_table().observers`
                // is the replicated single source of truth. Populated by
                // the `ClusterMutation::PeerJoined { is_observer: true }`
                // apply rule — originated by the primary in
                // `primary/peer_setup.rs::send_peer_lists` and replicated
                // to every node via the standard CRDT broadcast path. This
                // peer-side filter is the sole observer guard: a compute
                // SecondaryCoordinator is never itself an observer (the
                // observer role IS the ObserverCoordinator), so `self` is
                // always a legitimate candidate and is included
                // unconditionally.
                // `observers` + `live_peers` were snapshotted above (both
                // were `&self`/`cluster_state` reads that cannot coexist
                // with the `&mut op` borrow held by this match).
                let lowest_alive = live_peers
                    .iter()
                    .filter(|id| !observers.contains(*id))
                    .chain(std::iter::once(&secondary_id))
                    .min()
                    .cloned();
                let we_lead = lowest_alive
                    .as_ref()
                    .map(|id| id == &secondary_id)
                    .unwrap_or(false);
                let round = next_round(&op.election);
                if we_lead {
                    tracing::info!(round, "self-promoting");
                    op.election = ElectionState::Candidate {
                        round,
                        confirms: HashSet::from([secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    // No transitional self-as-primary routing target —
                    // authority is committed only once this candidate
                    // wins quorum (`record_promotion_confirm` reaches
                    // `Promoted`). The failover re-point — broadcasting +
                    // applying `PrimaryChanged { new = self }` so surviving
                    // secondaries' `cluster_state.current_primary()` moves
                    // onto this winner's mesh peer-id — is the composed
                    // runtime's terminal action on that transition, not a
                    // transitional Voting-time hint.
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: secondary_id.clone(),
                        vote_round: round,
                    });
                } else if let Some(candidate) = lowest_alive {
                    tracing::info!(%candidate, round, "deferring to lowest-live-id peer");
                    // No transitional routing target (see above).
                    op.election = ElectionState::Voting { round, candidate };
                }
            }
            ElectionState::Candidate { round, started, .. } => {
                // Conservative timeout: if no quorum within 5 keepalive
                // intervals, restart the election with a higher round.
                let timeout = self.config.keepalive_interval.saturating_mul(5);
                if started.elapsed() > timeout {
                    let next = round + 1;
                    tracing::warn!(round, "candidate timed out, retrying with round {next}");
                    op.election = ElectionState::Candidate {
                        round: next,
                        confirms: HashSet::from([secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: secondary_id.clone(),
                        vote_round: next,
                    });
                }
            }
            _ => {}
        }
        actions
    }

    /// Handle an incoming `TimeoutResponse` from a peer (called from
    /// `handle_peer_message`). Stores the response in the Suspecting bucket;
    /// the next tick will tally.
    ///
    /// `last_keepalive` is the responder's monotonic AGE (seconds since it last
    /// saw the queried node, on the responder's own clock), not an absolute
    /// timestamp — the Suspecting tally compares it directly to the death
    /// deadline. `None` = never seen.
    pub(in crate::secondary) fn record_timeout_response(
        &mut self,
        peer: String,
        last_keepalive: Option<f64>,
    ) {
        if let ElectionState::Suspecting { responses, .. } = &mut self.op_mut().election {
            responses.insert(peer, last_keepalive);
        }
    }

    /// Handle an incoming `PromotionVote` from a peer. We confirm only if
    /// the candidate is the lowest-live-id we know about and we also see
    /// the primary as silent. Returns the message to send back, if any.
    pub(in crate::secondary) fn record_promotion_vote(
        &mut self,
        candidate: String,
        round: u32,
    ) -> Option<DistributedMessage<I>> {
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let primary_silent = self
            .op_ref()
            .and_then(|op| op.primary_last_seen)
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(true);
        if !primary_silent {
            return None;
        }
        // `live_peer_ids` is a `&self` read (cluster_state + op_ref); take
        // the `lowest` owned value here, then borrow `op` mutably below
        // for the election write — the two borrows don't overlap.
        let lowest = self
            .live_peer_ids()
            .chain(std::iter::once(&self.config.secondary_id))
            .min()
            .cloned();
        if lowest.as_ref() != Some(&candidate) {
            return None;
        }
        // Adopt this candidate as our voting target; transition out of
        // Suspecting/Candidate so we don't double-vote. No transitional
        // routing target is written — `Role::Primary` re-points only on
        // the winner's authoritative `PrimaryChanged` (P2).
        let sender_id = self.config.secondary_id.clone();
        let op = self.op_mut();
        let already_voting_for_this_round = matches!(
            &op.election,
            ElectionState::Voting { round: r, candidate: c }
                if *r == round && c == &candidate
        );
        if !already_voting_for_this_round {
            op.election = ElectionState::Voting {
                round,
                candidate: candidate.clone(),
            };
        }
        Some(DistributedMessage::PromotionConfirm {
            sender_id,
            timestamp: timestamp_now(),
            new_primary_id: candidate,
            vote_round: round,
        })
    }

    /// Handle an incoming `PromotionConfirm`. If we're the candidate of the
    /// matching round and we've collected a majority, promote ourselves.
    /// Returns true if we just became the new primary.
    pub(in crate::secondary) fn record_promotion_confirm(
        &mut self,
        peer: String,
        target: String,
        round: u32,
    ) -> bool {
        if target != self.config.secondary_id {
            return false;
        }
        // `live_peer_ids` is a `&self` read; take the owned count before
        // borrowing `op` mutably for the confirm tally + transition.
        let peer_count = self.live_peer_ids().count();
        let quorum = peer_count.div_ceil(2) + 1;
        let op = self.op_mut();
        let promoted = match &mut op.election {
            ElectionState::Candidate {
                round: r, confirms, ..
            } if *r == round => {
                confirms.insert(peer);
                confirms.len() >= quorum
            }
            _ => false,
        };
        if promoted {
            tracing::info!(round, "won election — taking over as primary");
            // The election state machine only records the terminal
            // `Promoted` state transition. Activating the co-located
            // primary — `PrimaryCoordinator::activate_local_primary`,
            // which on this seeded-resume path hydrates the pool from the
            // replicated CRDT and enters the operational loop — is the
            // terminal action the composed runtime drives off this
            // transition; the secondary carries no self-promotion mirror
            // to flip on here.
            op.election = ElectionState::Promoted;
        }
        promoted
    }

    /// Terminal action for THIS node winning its failover election:
    /// originate the single primary-activation frame
    /// `ClusterMutation::PrimaryChanged { new = self, epoch =
    /// primary_epoch + 1 }`, apply it locally so this winner's OWN apply
    /// hook fires, and broadcast it so every surviving peer re-points.
    ///
    /// Unified onto the `PrimaryChanged` apply hook
    /// (`apply_cluster_mutations` → `apply_primary_changed`): there is no
    /// separate role-flip wire frame and no separate direct activation
    /// call — the single `PrimaryChanged` frame carries both. The flow:
    ///   1. **Local apply** via `apply_cluster_mutations` — the winner's
    ///      own apply hook (`apply_primary_changed`) runs, signalling that
    ///      this node is now the named primary (the build of the
    ///      `PrimaryCoordinator` on the promotion event is the Phase-C
    ///      `Process` concern — see the C4 seam in `apply_primary_changed`)
    ///      and resetting this node's election to `Normal` (a primary now
    ///      exists — no lingering `Promoted`). Idempotent if the broadcast
    ///      is later echoed back through the mesh receive path.
    ///   2. **Broadcast** the same `PrimaryChanged` onto the mesh
    ///      (`Destination::All`). Surviving secondaries apply it via the
    ///      SAME hook, moving their `cluster_state.current_primary()` onto
    ///      this winner's mesh peer-id. Their next `Destination::Primary`
    ///      send then resolves to this winner at the egress edge — the
    ///      entire failover re-route. `epoch + 1` strictly supersedes the
    ///      prior identity (last-writer-wins on epoch), so a delayed
    ///      lower-epoch broadcast cannot un-elect this winner.
    ///
    /// The OLD primary, observing this `PrimaryChanged`, becomes a pure
    /// observer (it no longer holds `Role::Primary`) — R3's observe path.
    /// This is the same originate-apply-broadcast shape the live primary's
    /// `apply_and_broadcast_cluster_mutations` uses; the only difference
    /// is the local apply routes through `apply_cluster_mutations` so the
    /// secondary-side wake hook fires.
    pub(in crate::secondary) async fn fire_local_promotion(&mut self) {
        let epoch = self.cluster_state.primary_epoch() + 1;
        let mutation = dynrunner_protocol_primary_secondary::ClusterMutation::PrimaryChanged {
            new: self.config.secondary_id.clone(),
            epoch,
            // Election win (`new == self`): this winner names ITSELF the
            // primary. The bootstrap-transfer reason is set only by the
            // submitter's relocate path naming a DIFFERENT chosen peer.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        };

        // (1) Apply locally through the unified hook so THIS winner's own
        // apply hook fires (activate co-located primary + reset election).
        self.apply_cluster_mutations(vec![mutation.clone()]);

        // (2) Broadcast the re-point so surviving peers apply the same
        // frame through the same hook. epoch+1 strictly supersedes the
        // prior primary identity.
        let msg = DistributedMessage::ClusterMutation {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            mutations: vec![mutation],
        };
        if let Err(e) = self
            .send_to(dynrunner_protocol_primary_secondary::Destination::All, msg)
            .await
        {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                error = %e,
                "PrimaryChanged(new=self) failover broadcast failed; \
                 surviving secondaries will re-point on the next election \
                 round or via CRDT snapshot reconciliation"
            );
        }
    }
}
