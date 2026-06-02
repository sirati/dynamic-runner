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

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;
use super::{next_round, primary_node_id, ElectionState, ElectionTickActions};

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
    /// in the replicated `cluster_state` (mirrored into the transport's
    /// `RoleCache` by the `PrimaryChanged` apply hook, which is the
    /// single source of "who is primary now" and the only re-route).
    /// This method records liveness and cancels a false-alarm election;
    /// it writes no routing target, because there is no transitional
    /// routing target anymore (P2).
    pub(in crate::secondary) fn record_primary_message(&mut self) {
        self.primary_last_seen = Some(Instant::now());
        // Reset the primary-link health sub-state. A real message
        // arriving on the (possibly-reconnected) transport proves
        // the link is alive again, so any failure-window we'd been
        // tracking should be discarded. Pre-fix the failover
        // arming kept counting probes against a stale window even
        // after a brief flap recovered, so the second flap would
        // arm faster than the first — confusing semantics. The
        // reset closes that loop. Idempotent on a healthy link.
        self.primary_link.record_recv_success();
        // Cancel a failover election in progress: a primary message
        // proves the original primary is still reachable so the
        // election was a false alarm. Revert to Normal. There is no
        // transitional routing target to clear — routing always
        // resolves `Role::Primary` through the RoleCache, which still
        // names the original primary (no `PromotePrimary` committed
        // during the aborted election, per the drop-the-transitional-
        // hint design).
        if matches!(
            self.election,
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
                secondary = %self.config.secondary_id,
                from = ?std::mem::discriminant(&self.election),
                "election recovered: primary message resumed, reverting to Normal"
            );
            self.election = ElectionState::Normal;
        }
    }

    /// Advance the election state. Called once per processing-loop tick.
    /// Returns the broadcast/self-send messages the loop should flush.
    pub(in crate::secondary) fn run_election_tick(&mut self) -> ElectionTickActions<I> {
        let mut actions = ElectionTickActions::default();
        // Strict-observer suppression: an observer is a passive bystander
        // with ZERO authority/responsibility. It never participates in
        // failover — it doesn't suspect a silent primary, doesn't
        // broadcast TimeoutQuery / PromotionVote, and can never become a
        // candidate. Its only cue is the replicated `run_complete()`.
        // This is the failover concern's own role-gate (the election
        // module OWNS failover), matching `send_keepalive`'s and
        // `report_mesh_ready_if_needed`'s self-gates — not a scattered
        // cross-concern branch.
        if self.config.is_observer {
            return actions;
        }
        if matches!(self.election, ElectionState::Promoted) {
            return actions;
        }

        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let primary_silent = self
            .primary_last_seen
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(false);

        // Cascade election trigger: if a primary peer was promoted
        // (current_primary names a peer, not ourselves) and that
        // peer's keepalives have gone stale, we need to run a fresh
        // election regardless of whether the local-machine primary
        // is still alive. Pre-fix, the election only fired on
        // local-primary silence — so when a promoted peer like
        // sec-0 died but the local primary was still streaming
        // keepalives to other secondaries, those secondaries
        // observed the disconnect, dropped sec-0 from
        // peer_keepalives, and then sat idle forever because
        // primary_silent stayed false. Dataset's K=2 run hit
        // exactly this: 4 surviving secondaries with no primary,
        // no election attempt, no logged "primary assigned" lines
        // on any of them.
        //
        // A peer is considered the silent primary when:
        //   - the replicated `current_primary` is set (a promotion
        //     happened) — read from `cluster_state`, the single source
        //     of "who is primary now".
        //   - the id is NOT ourselves (we'd be Promoted, handled above)
        //   - its peer_keepalives entry is missing OR its
        //     timestamp is older than `deadline`
        // Clone the id out of the cluster_state borrow before the
        // `peer_keepalives` read so the two borrows don't overlap.
        let current_primary_id = self.cluster_state.current_primary().map(str::to_owned);
        let primary_peer_silent = current_primary_id
            .as_deref()
            .filter(|id| *id != self.config.secondary_id.as_str())
            .map(|id| {
                self.peer_keepalives
                    .get(id)
                    .map(|t| (timestamp_now() - t) > deadline.as_secs_f64())
                    .unwrap_or(true)
            })
            .unwrap_or(false);

        let need_election = primary_silent || primary_peer_silent;

        match &self.election {
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
                if self.peer_mesh_degraded {
                    let reason = format!(
                        "peer mesh required for failover but not \
                         available: primary went silent (primary_silent={}, \
                         primary_peer_silent={}) and no peers connected to \
                         elect a new primary; exiting",
                        primary_silent, primary_peer_silent,
                    );
                    tracing::error!(
                        secondary = %self.config.secondary_id,
                        primary_silent,
                        primary_peer_silent,
                        primary_peer = ?current_primary_id,
                        "{reason}"
                    );
                    self.fatal_exit = Some(reason);
                    return actions;
                }
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    miss_threshold = self.config.keepalive_miss_threshold,
                    primary_silent,
                    primary_peer_silent,
                    primary_peer = ?current_primary_id,
                    "primary or primary peer missed keepalives; \
                     entering Suspecting"
                );
                self.election = ElectionState::Suspecting {
                    since: Instant::now(),
                    responses: HashMap::new(),
                };
                actions.broadcast.push(DistributedMessage::TimeoutQuery {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    query_node_id: primary_node_id(),
                });
            }
            ElectionState::Suspecting { since, responses } => {
                // Wait at least `keepalive_interval` to gather peer responses
                // before counting votes. This is the natural window for a
                // peer to reply once it has also seen the primary go silent.
                if since.elapsed() < self.config.keepalive_interval {
                    return actions;
                }
                let peer_count = self.peer_keepalives.len();
                let agreeing = responses
                    .values()
                    .filter(|last| {
                        last.map(|t| (timestamp_now() - t) > deadline.as_secs_f64())
                            .unwrap_or(true)
                    })
                    .count()
                    + 1; // include us
                let quorum = peer_count.div_ceil(2) + 1;
                if agreeing < quorum {
                    tracing::info!(
                        agreeing,
                        quorum,
                        "no quorum on primary death; waiting"
                    );
                    return actions;
                }
                // Task #36 / Step 7: filter observer peers from
                // candidate selection. An observer in the alive set
                // with a lex-low ID would otherwise be deferred-to by
                // non-observer peers; the observer would then refuse
                // self-promotion (#35 self-skip), stalling the
                // cluster. Filtering at the peer-side complements
                // the observer's self-exclusion: both sides agree
                // observers can't be candidates. The #35 self-only
                // guard below (`is_observer && we_lead`) becomes
                // belt-and-suspenders once `RoleTable.observers` is
                // populated end-to-end, but stays in place for the
                // pre-PeerInfo window (election can run before the
                // first PeerInfo arrives in adversarial timings).
                //
                // Read source: `cluster_state.role_table().observers`
                // is the replicated single source of truth (Step 7,
                // Decision G). Populated by the `ClusterMutation::
                // PeerJoined { is_observer: true }` apply rule —
                // originated by the primary in
                // `primary/peer_setup.rs::send_peer_lists` and
                // replicated to every node via the standard CRDT
                // broadcast path.
                let observers = &self.cluster_state.role_table().observers;
                let lowest_alive = self
                    .peer_keepalives
                    .keys()
                    .filter(|id| !observers.contains(*id))
                    .chain(
                        std::iter::once(&self.config.secondary_id)
                            .filter(|_| !self.config.is_observer),
                    )
                    .min()
                    .cloned();
                let we_lead = lowest_alive
                    .as_ref()
                    .map(|id| id == &self.config.secondary_id)
                    .unwrap_or(false);
                let round = next_round(&self.election);
                // Observer self-exclusion (#35): even when our id is
                // the lex-lowest in the alive set, an observer MUST
                // NOT self-promote. Observers are non-candidates by
                // design — they receive cluster updates but cannot
                // host the primary role (no workers, no dispatch
                // authority). The full fortification (peers also
                // skipping observers when picking `lowest_alive`)
                // needs `PeerConnectionInfo.is_observer`, tracked as
                // a follow-up; until that lands the observer
                // self-policing is the load-bearing guard.
                //
                // If we_lead but is_observer, we defer to the NEXT-
                // lowest-id peer that ISN'T us — that peer will
                // self-promote on its own tick. If we're the only
                // alive secondary (peer_keepalives empty), there's
                // no candidate at all and we stay Voting (effectively
                // waiting for another secondary to come online); the
                // peer_mesh_degraded guard above catches the
                // pathological "alone and primary's dead" case.
                if self.config.is_observer && we_lead {
                    let next_lowest = self
                        .peer_keepalives
                        .keys()
                        .min()
                        .cloned();
                    tracing::info!(
                        observer = %self.config.secondary_id,
                        ?next_lowest,
                        round,
                        "observer would have self-promoted by lowest-id but \
                         is non-candidate; deferring to next-lowest peer \
                         (peers without observer-awareness will need to \
                         self-promote on their own ticks)"
                    );
                    if let Some(candidate) = next_lowest {
                        // No transitional routing target: `Role::Primary`
                        // is re-pointed only by the winner's
                        // authoritative `PrimaryChanged` (applied on the
                        // `PromotePrimary` it broadcasts after winning),
                        // never by an in-flight Voting transition. See
                        // the P2 drop-the-transitional-hint design.
                        self.election = ElectionState::Voting { round, candidate };
                    }
                    // No next_lowest = we're the only one alive AND
                    // we're an observer. Don't transition; let the
                    // peer-mesh-degraded path catch this in a future
                    // tick (or a new secondary arrival fixes it).
                } else if we_lead {
                    tracing::info!(round, "self-promoting");
                    self.election = ElectionState::Candidate {
                        round,
                        confirms: HashSet::from([self.config.secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    // No transitional self-as-primary routing target —
                    // authority is committed only once this candidate
                    // wins quorum and broadcasts `PromotePrimary`
                    // (R4's `activate_local_primary`), whose
                    // `PrimaryChanged` apply re-points every node's
                    // RoleCache.
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: self.config.secondary_id.clone(),
                        vote_round: round,
                    });
                } else if let Some(candidate) = lowest_alive {
                    tracing::info!(%candidate, round, "deferring to lowest-live-id peer");
                    // No transitional routing target (see above).
                    self.election = ElectionState::Voting { round, candidate };
                }
            }
            ElectionState::Candidate { round, started, .. } => {
                // Conservative timeout: if no quorum within 5 keepalive
                // intervals, restart the election with a higher round.
                let timeout = self.config.keepalive_interval.saturating_mul(5);
                if started.elapsed() > timeout {
                    let next = round + 1;
                    tracing::warn!(round, "candidate timed out, retrying with round {next}");
                    self.election = ElectionState::Candidate {
                        round: next,
                        confirms: HashSet::from([self.config.secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: self.config.secondary_id.clone(),
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
    pub(in crate::secondary) fn record_timeout_response(
        &mut self,
        peer: String,
        last_keepalive: Option<f64>,
    ) {
        if let ElectionState::Suspecting { responses, .. } = &mut self.election {
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
        let primary_silent = self
            .primary_last_seen
            .map(|t| {
                Instant::now().duration_since(t)
                    > self
                        .config
                        .keepalive_interval
                        .saturating_mul(self.config.keepalive_miss_threshold)
            })
            .unwrap_or(true);
        if !primary_silent {
            return None;
        }
        let lowest = self
            .peer_keepalives
            .keys()
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
        let already_voting_for_this_round = matches!(
            &self.election,
            ElectionState::Voting { round: r, candidate: c }
                if *r == round && c == &candidate
        );
        if !already_voting_for_this_round {
            self.election = ElectionState::Voting {
                round,
                candidate: candidate.clone(),
            };
        }
        Some(DistributedMessage::PromotionConfirm {
            sender_id: self.config.secondary_id.clone(),
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
        let peer_count = self.peer_keepalives.len();
        let quorum = peer_count.div_ceil(2) + 1;
        let promoted = match &mut self.election {
            ElectionState::Candidate {
                round: r,
                confirms,
                ..
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
            // primary (seeding its pool from the replicated CRDT and
            // entering its operational loop) is R4's terminal action,
            // wired through the unified composition — the secondary
            // carries no self-promotion mirror to flip on here.
            self.election = ElectionState::Promoted;
        }
        promoted
    }

    /// Whether the secondary has been elected primary in this run.
    #[allow(dead_code)]
    pub(in crate::secondary) fn is_promoted(&self) -> bool {
        matches!(self.election, ElectionState::Promoted)
    }

    /// Whether we're still in the normal pre-election state.
    #[allow(dead_code)]
    pub(in crate::secondary) fn election_is_normal(&self) -> bool {
        self.election.is_normal()
    }
}
