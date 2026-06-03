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
use super::{ElectionState, ElectionTickActions, next_round, primary_node_id};

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

    /// The node id whose keepalives count as PRIMARY-liveness assertions
    /// for failover, as a TOTAL function — recognition never has a "no
    /// primary" hole.
    ///
    /// This is the RECOGNITION concern, deliberately decoupled from the
    /// ROUTING concern. Routing (how to physically reach the primary) reads
    /// `role_table.primary` / the transport `RoleCache`, which is COLD on
    /// bootstrap on purpose so traffic flows over the uplink — that COLD
    /// state mirrors as `cluster_state.current_primary() == None`. But the
    /// bootstrap primary's IDENTITY is not unknown: it is the well-known
    /// canonical `primary_node_id()` constant — the same value election
    /// stamps into `TimeoutQuery::query_node_id` and the bootstrap primary
    /// stamps onto every keepalive it broadcasts. So recognition resolves
    /// `current_primary()` when a failover has named a concrete winner, and
    /// otherwise falls back to that canonical bootstrap identity. The
    /// `None`-means-routes-via-uplink artifact must NOT leak into
    /// recognition, or the bootstrap primary's keepalives are never
    /// recognized and primary liveness stays parasitic on dispatch traffic.
    ///
    /// The fallback is DISABLED once a failover commits a concrete primary
    /// (`current_primary() == Some(winner)`): a zombie/demoted old bootstrap
    /// primary's keepalives (still stamped `primary_node_id()`) then no
    /// longer match, so they correctly fall through to `peer_keepalives`.
    pub(in crate::secondary) fn recognized_primary_id(&self) -> String {
        self.cluster_state
            .current_primary()
            .map(str::to_owned)
            .unwrap_or_else(primary_node_id)
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
        // `primary_silent` is the SOLE liveness predicate for whoever
        // currently holds the primary role — co-located OR a promoted
        // peer. `primary_last_seen` is refreshed by
        // `record_primary_message`, and post-A-M0a the recognition path
        // in `handle_inbound`'s Keepalive arm routes EVERY keepalive
        // whose originator IS the current primary (resolved via
        // `cluster_state.current_primary()`, the single source of "who
        // is primary now") through `record_primary_message`. So a
        // promoted peer's keepalives refresh `primary_last_seen` exactly
        // like the co-located primary's dispatch traffic once did, and a
        // genuinely-dead promoted primary trips `primary_silent` once its
        // keepalives stop — there is no longer a separate
        // promoted-peer-primary liveness axis to track.
        //
        // This subsumes the former cascade trigger (`primary_peer_silent`,
        // which read the promoted primary's `peer_keepalives` entry):
        // that branch is both REDUNDANT (the recognition path now keeps
        // `primary_last_seen` fresh for a promoted peer) and BROKEN (post-
        // A-M0a the current primary's keepalives no longer populate
        // `peer_keepalives`, so its `unwrap_or(true)` fired against a
        // HEALTHY just-promoted primary and stormed `TimeoutQuery`,
        // risking double-promotion). The cascade (promoted-peer-died)
        // case Dataset's K=2 run hit is now covered by `primary_silent`
        // via the A-M0a recognition path.
        let primary_silent = self
            .primary_last_seen
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(false);

        let need_election = primary_silent;

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
                // Diagnostic only: who the silent primary was (co-located
                // or a promoted peer). Read inline from the single source
                // of "who is primary now"; it drives no decision — the
                // decision is `primary_silent` alone.
                let current_primary_id = self.cluster_state.current_primary();
                if self.peer_mesh_degraded {
                    let reason = format!(
                        "peer mesh required for failover but not \
                         available: primary went silent (primary_silent={}) \
                         and no peers connected to elect a new primary; \
                         exiting",
                        primary_silent,
                    );
                    tracing::error!(
                        secondary = %self.config.secondary_id,
                        primary_silent,
                        primary = ?current_primary_id,
                        "{reason}"
                    );
                    self.fatal_exit = Some(reason);
                    return actions;
                }
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    miss_threshold = self.config.keepalive_miss_threshold,
                    primary_silent,
                    primary = ?current_primary_id,
                    "primary missed keepalives; entering Suspecting"
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
                    tracing::info!(agreeing, quorum, "no quorum on primary death; waiting");
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
                    let next_lowest = self.peer_keepalives.keys().min().cloned();
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
                    // wins quorum (`record_promotion_confirm` reaches
                    // `Promoted`). The failover re-point — broadcasting
                    // `PromotePrimary { new = self }` so surviving
                    // secondaries' role cache moves off the dead uplink
                    // onto this winner's mesh peer — is the composed
                    // runtime's terminal action on that transition, not a
                    // transitional Voting-time hint.
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
            self.election = ElectionState::Promoted;
        }
        promoted
    }

    /// Fire the promotion-activation gate for the co-located parked
    /// primary (fire-once). Wakes `PrimaryCoordinator::run_parked` into
    /// its seeded resume (`activate_local_primary` → hydrate-from-CRDT →
    /// operational loop).
    ///
    /// `take()` makes this idempotent across the TWO paths that reach
    /// the terminal `Promoted` state: winning this node's OWN election
    /// (`record_promotion_confirm` → true, via `fire_local_promotion`)
    /// and being NAMED primary by a `PromotePrimary` broadcast (the
    /// `dispatch/router` arm). Only the first consumes the sender; the
    /// second is a no-op on the gate. No-op entirely when no co-located
    /// primary was composed (`promote_activation_tx` is `None`).
    pub(in crate::secondary) fn activate_co_located_primary(&mut self) {
        if let Some(tx) = self.promote_activation_tx.take() {
            // Hand the parked primary a SNAPSHOT of this secondary's
            // continuously-mirrored cluster_state — the seed it
            // `restore`s before `hydrate_from_cluster_state`. The parked
            // primary's own cluster_state is empty (the role-aware tap
            // does not feed it CRDT mirror frames), so this snapshot IS
            // the ledger it resumes from.
            let snapshot = self.cluster_state.snapshot();
            if tx.send(snapshot).is_err() {
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    "promotion-activation gate receiver dropped before firing \
                     — the parked co-located primary is gone"
                );
            } else {
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "fired promotion-activation gate with cluster_state snapshot; \
                     co-located primary will restore + hydrate and take authority"
                );
            }
        }
    }

    /// Terminal action for THIS node reaching the `Promoted` state: wake
    /// the co-located parked primary into its seeded resume and re-point
    /// every surviving secondary's `Role::Primary` onto this winner.
    ///
    /// Two side effects, both idempotent on a second call:
    ///   1. Fire the promotion-activation gate (`promote_activation_tx`,
    ///      registered by the composed runtime). The parked
    ///      `PrimaryCoordinator::run_parked` is waiting on the matching
    ///      oneshot; firing it triggers `activate_local_primary`
    ///      (hydrate-from-CRDT seeded resume) + the operational loop.
    ///      `take()` makes this fire-once: the own-election-win path
    ///      (`record_promotion_confirm` → true) and the peer-named path
    ///      (the `PromotePrimary { new = self }` router arm) both call
    ///      here, but only the first consumes the sender.
    ///   2. Broadcast `PromotePrimary { new = self, epoch =
    ///      primary_epoch + 1 }` onto the mesh. Surviving secondaries
    ///      apply it as `ClusterMutation::PrimaryChanged`, whose
    ///      write-through role-change hook re-points their transport's
    ///      `Role::Primary` cache off the dead original-primary uplink
    ///      onto this winner's mesh peer-id — the entire failover
    ///      re-route, owned by the transport layer. `epoch + 1` strictly
    ///      supersedes the prior identity (last-writer-wins on epoch),
    ///      so a delayed lower-epoch broadcast cannot un-elect this
    ///      winner.
    ///
    /// The OLD primary, observing this `PrimaryChanged`, becomes a pure
    /// observer (it no longer holds `Role::Primary`) — R3's observe path.
    ///
    /// No-op on the gate when no co-located primary was composed
    /// (Rust-only tests / legacy callers): `promote_activation_tx` is
    /// `None`; the broadcast still fires so the mesh learns the new
    /// authority.
    pub(in crate::secondary) async fn fire_local_promotion(&mut self) {
        // (1) Wake the parked co-located primary (fire-once).
        self.activate_co_located_primary();

        // (2) Broadcast the failover re-point. epoch+1 strictly
        // supersedes the prior primary identity.
        let epoch = self.cluster_state.primary_epoch() + 1;
        let msg = DistributedMessage::PromotePrimary {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            new_primary_id: self.config.secondary_id.clone(),
            epoch,
            // Failover after primary loss: the ledger is CRDT-merged
            // from the in-flight broadcasts, NOT a fresh setup-defer
            // discovery. `required_setup = false`.
            required_setup: false,
        };
        if let Err(e) = self
            .transport
            .send(
                dynrunner_protocol_primary_secondary::Address::Broadcast(
                    dynrunner_protocol_primary_secondary::Scope::Mesh,
                ),
                msg,
            )
            .await
        {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                error = %e,
                "PromotePrimary(new=self) failover broadcast failed; \
                 surviving secondaries will re-point on the next election \
                 round or via CRDT snapshot reconciliation"
            );
        }
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
