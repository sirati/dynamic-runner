//! Failover election (F2): primary-death detection on the secondary side
//! plus the lowest-live-ID + quorum election that promotes one of the
//! surviving peers to the new primary role.
//!
//! State machine:
//!
//! ```text
//!   Normal ──primary missed N keepalives──> Suspecting
//!   Suspecting ──quorum agrees + we are lowest-live-id──> Candidate
//!   Suspecting ──quorum agrees + a peer is lower──> Voting
//!   Candidate  ──majority confirms──> Promoted
//!   Voting     ──saw winner's first task list──> Normal (new primary tracked)
//!   any state  ──primary keepalive arrives──> Normal
//! ```
//!
//! Each tick of the secondary's processing loop calls
//! [`SecondaryCoordinator::run_election_tick`] which advances the state
//! machine based on the elapsed time and the messages received from peers
//! in `handle_peer_message`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::timestamp_now;

/// The election state machine. Variants carry only the fields they need so
/// the rest of the secondary doesn't have to pattern-match optionals.
pub(super) enum ElectionState {
    /// Primary is alive (or we haven't started suspecting yet).
    Normal,
    /// We've stopped seeing primary keepalives. Querying peers about
    /// primary liveness; collecting their `TimeoutResponse` answers.
    Suspecting {
        since: Instant,
        responses: HashMap<String, Option<f64>>,
    },
    /// We are the candidate. Waiting for majority `PromotionConfirm`.
    Candidate {
        round: u32,
        confirms: HashSet<String>,
        started: Instant,
    },
    /// A peer with lower id is candidate. Waiting for the take-over to
    /// happen (we'll see the new primary start sending messages).
    Voting {
        round: u32,
        candidate: String,
    },
    /// We've taken over and are now acting as the primary. Stops further
    /// election ticks.
    Promoted,
}

impl ElectionState {
    fn is_normal(&self) -> bool {
        matches!(self, ElectionState::Normal)
    }
}

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Bump the primary-keepalive timestamp on every primary message and
    /// abandon any in-flight failover election. Called from the dispatch
    /// path before any other handling.
    ///
    /// The current primary identity is NOT cleared here — once set
    /// (either by an explicit `PromotePrimary` from the live primary
    /// or by the failover-election outcome) it names the current
    /// cluster primary and survives subsequent keepalives from the
    /// now-demoted local primary. Pre-fix this method cleared it on
    /// every primary keepalive, so non-primary secondaries that
    /// learned the new primary's id from `PromotePrimary` lost the
    /// routing target the moment the next local-primary keepalive
    /// arrived; their `send_to_current_primary` then routed via
    /// `primary_transport` back to the demoted local primary instead
    /// of directly to the SLURM-primary peer. The local primary's
    /// `handle_task_request` relayed TaskRequests onward, papering
    /// over the bug as long as the local primary's transport stayed
    /// alive — but when that transport closed (laptop suspend, SSH
    /// tunnel idle close) the relay vanished and the entire fleet
    /// stalled. Surfaced in dataset's K=2 hello run after 95b9f32:
    /// the synchronous PromotePrimary state-sync was correct, but the
    /// very next primary keepalive clobbered the routing target
    /// back to None.
    pub(super) fn record_primary_message(&mut self) {
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
        // election was a false alarm. The transitional routing target
        // we set inside the election (pointing at the in-flight
        // candidate) is also stale — clear it so routing goes back to
        // the live primary via `primary_transport`.
        //
        // For Normal/Promoted states we MUST NOT touch the routing
        // target:
        //   - Normal-with-Some(primary) means an explicit handoff is
        //     in effect (PromotePrimary, or a completed
        //     Voting → Normal election outcome). Demoted local-primary
        //     keepalives still arrive in this state but no longer name
        //     the routing target.
        //   - Promoted means we ARE the primary; no override applies
        //     to ourselves.
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
            self.primary_link.set_current_primary(None);
        }
    }

    /// Advance the election state. Called once per processing-loop tick.
    /// Returns the broadcast/self-send messages the loop should flush.
    pub(super) fn run_election_tick(&mut self) -> ElectionTickActions<I> {
        let mut actions = ElectionTickActions::default();
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
        //   - current_primary is set (a promotion happened)
        //   - the id is NOT ourselves (we'd be Promoted, handled above)
        //   - its peer_keepalives entry is missing OR its
        //     timestamp is older than `deadline`
        let primary_peer_silent = self
            .primary_link
            .current_primary()
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
                        primary_peer = ?self.primary_link.current_primary(),
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
                    primary_peer = ?self.primary_link.current_primary(),
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
                        self.primary_link.set_current_primary(Some(candidate.clone()));
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
                    self.primary_link
                        .set_current_primary(Some(self.config.secondary_id.clone()));
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: self.config.secondary_id.clone(),
                        vote_round: round,
                    });
                } else if let Some(candidate) = lowest_alive {
                    tracing::info!(%candidate, round, "deferring to lowest-live-id peer");
                    self.primary_link.set_current_primary(Some(candidate.clone()));
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
    pub(super) fn record_timeout_response(
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
    pub(super) fn record_promotion_vote(
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
        // Suspecting/Candidate so we don't double-vote.
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
            self.primary_link.set_current_primary(Some(candidate.clone()));
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
    pub(super) fn record_promotion_confirm(
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
            self.election = ElectionState::Promoted;
            self.is_primary = true;
            // Hydrate `primary_pending` from the continuously-replicated
            // `cluster_state`. Pre-Phase-B promotion drained a cached
            // `FullTaskList` snapshot — a consume-once data path with
            // its own race conditions (no broadcast observed yet ⇒
            // empty new primary). Post-Phase-B every node has been
            // mirroring the ledger from run start, so the new primary
            // simply reads its own already-current state.
            self.populate_primary_from_cluster_state();
        }
        promoted
    }

    /// Whether the secondary has been elected primary in this run.
    #[allow(dead_code)]
    pub(super) fn is_promoted(&self) -> bool {
        matches!(self.election, ElectionState::Promoted)
    }

    /// Whether we're still in the normal pre-election state.
    #[allow(dead_code)]
    pub(super) fn election_is_normal(&self) -> bool {
        self.election.is_normal()
    }
}

/// Output of one election tick: messages the caller should flush onto the
/// peer transport before the next iteration.
pub(super) struct ElectionTickActions<I: Identifier> {
    pub(super) broadcast: Vec<DistributedMessage<I>>,
}

impl<I: Identifier> Default for ElectionTickActions<I> {
    fn default() -> Self {
        Self {
            broadcast: Vec::new(),
        }
    }
}

/// Conventional id used inside `TimeoutQuery::query_node_id` when the
/// queried party is the primary. Keeps every secondary using the same
/// string so peers can match queries up.
pub(super) fn primary_node_id() -> String {
    "primary".into()
}

fn next_round(state: &ElectionState) -> u32 {
    match state {
        ElectionState::Voting { round, .. } | ElectionState::Candidate { round, .. } => round + 1,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    //! Failover scenarios (b), (c), (d) from the migration plan, exercised
    //! at the election state-machine level. The full multi-process
    //! integration tests over channels would require post-promotion task
    //! takeover (re-distributing pending work from the dead primary), which
    //! is not yet implemented in pure Rust — these tests cover the
    //! detection + voting algorithm itself.
    //!
    //! Scenario (a) — secondary dies → primary requeues — is covered in
    //! `crate::primary::heartbeat::tests`.

    use super::super::test_helpers::{election_config, make_secondary};
    use super::*;
    use std::time::Duration;

    /// The death deadline given the helper's keepalive_interval (50ms) and
    /// keepalive_miss_threshold (2). 100ms exact; sleep slightly over.
    const PAST_DEATH: Duration = Duration::from_millis(110);
    /// One full keepalive interval, the gather window for `Suspecting` to
    /// progress to a vote.
    const ONE_INTERVAL: Duration = Duration::from_millis(60);

    /// Scenario (b): primary stops sending keepalives. The lowest-id
    /// secondary observes the death, runs the election, collects quorum,
    /// and promotes itself.
    #[tokio::test(flavor = "current_thread")]
    async fn primary_dies_lowest_id_promotes() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.peer_keepalives.insert("sec-b".into(), 0.0);
        sec.peer_keepalives.insert("sec-c".into(), 0.0);
        sec.record_primary_message();

        tokio::time::sleep(PAST_DEATH).await;

        // First tick: enter Suspecting and broadcast TimeoutQuery.
        let actions = sec.run_election_tick();
        assert!(matches!(sec.election, ElectionState::Suspecting { .. }));
        assert!(actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })));

        // Wait the gather window so the Suspecting tick is eligible to vote.
        tokio::time::sleep(ONE_INTERVAL).await;

        // Peers report primary silent (None means "haven't seen recently").
        sec.record_timeout_response("sec-b".into(), None);
        sec.record_timeout_response("sec-c".into(), None);

        // Second tick: tally quorum, transition Suspecting → Candidate
        // (sec-a is the lowest id), and broadcast PromotionVote.
        let actions = sec.run_election_tick();
        assert!(matches!(sec.election, ElectionState::Candidate { .. }));
        assert!(actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { .. })));

        // One peer confirms — combined with the candidate's own vote that
        // is the quorum (peer_count=2 → quorum=2).
        let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
        assert!(promoted, "majority confirm should promote");
        assert!(matches!(sec.election, ElectionState::Promoted));
    }

    /// Scenario (c): with four peers including self, one peer is dead at
    /// the same time as the primary. The election still has quorum from
    /// the remaining three live secondaries.
    #[tokio::test(flavor = "current_thread")]
    async fn double_failure_election_still_succeeds() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.peer_keepalives.insert("sec-b".into(), 0.0);
        sec.peer_keepalives.insert("sec-c".into(), 0.0);
        sec.peer_keepalives.insert("sec-d".into(), 0.0); // will not respond
        sec.record_primary_message();

        tokio::time::sleep(PAST_DEATH).await;
        sec.run_election_tick();
        tokio::time::sleep(ONE_INTERVAL).await;

        // Only b and c respond; d is silent.
        sec.record_timeout_response("sec-b".into(), None);
        sec.record_timeout_response("sec-c".into(), None);

        sec.run_election_tick();
        assert!(
            matches!(sec.election, ElectionState::Candidate { .. }),
            "quorum (3 of 4) reached even with one peer dead"
        );

        // Confirm quorum for promotion: peer_count=3 → quorum=3, candidate
        // counts itself, needs two peer confirms.
        sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
        let promoted = sec.record_promotion_confirm("sec-c".into(), "sec-a".into(), 1);
        assert!(promoted, "two peer confirms + self = quorum");
        assert!(matches!(sec.election, ElectionState::Promoted));
    }

    /// Once promoted, a secondary's `primary_pending` pool is hydrated
    /// from its replicated `cluster_state` mirror. Validates the
    /// post-promotion takeover wiring (originally #34, post-Phase-B
    /// re-grounded onto the CRDT-replicated ledger).
    #[tokio::test(flavor = "current_thread")]
    async fn promotion_hydrates_primary_tasks_from_cluster_state() {
        use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
        use dynrunner_protocol_primary_secondary::ClusterMutation;
        use std::collections::HashSet;
        use std::path::PathBuf;

        let mut sec = make_secondary(election_config("sec-a"));
        sec.peer_keepalives.insert("sec-b".into(), 0.0);

        // Pre-seed cluster_state as if the live primary's
        // `seed_cluster_state` broadcast had already arrived: one
        // `TaskAdded` plus an empty phase_deps map so the
        // (synthesised) "default" phase has zero parents and the pool
        // is immediately Active.
        let task: TaskInfo<super::super::test_helpers::TestId> = TaskInfo {
            path: PathBuf::from("/tmp/bin1"),
            size: 100,
            identifier: super::super::test_helpers::TestId("bin1".into()),
            phase_id: PhaseId::from("default"),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        };
        sec.cluster_state.apply(ClusterMutation::PhaseDepsSet {
            deps: std::collections::HashMap::new(),
        });
        sec.cluster_state.apply(ClusterMutation::TaskAdded {
            hash: "hash_bin1".into(),
            task,
        });

        // Simulate the candidate path: become candidate, then receive
        // confirm to flip to Promoted. peer_count=1, quorum=2, so we need
        // confirms from {self, sec-b} to promote.
        sec.election = ElectionState::Candidate {
            round: 1,
            confirms: HashSet::from(["sec-a".to_string()]),
            started: std::time::Instant::now(),
        };
        sec.is_primary = false; // not yet
        let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
        assert!(promoted, "majority confirm should promote");

        assert!(matches!(sec.election, ElectionState::Promoted));
        assert!(sec.is_primary, "promotion sets is_primary");
        assert_eq!(
            sec.primary_pending_len(),
            1,
            "cluster_state should have hydrated one pending binary into the pool"
        );
    }

    /// `record_primary_message` resets election state and clears any
    /// remembered new-primary-peer routing target — the live primary is
    /// alive again, so TaskRequest routes back to primary_transport.
    #[tokio::test(flavor = "current_thread")]
    async fn primary_recovery_clears_routing_target() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.primary_link.set_current_primary(Some("sec-c".into()));
        sec.election = ElectionState::Voting {
            round: 1,
            candidate: "sec-c".into(),
        };
        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Normal));
        assert!(
            sec.primary_link.current_primary().is_none(),
            "live primary message should clear the routing target"
        );
    }

    /// `Promoted` state survives a `record_primary_message`: once we've
    /// taken over, a stray late message from the dead primary doesn't
    /// dethrone us.
    #[tokio::test(flavor = "current_thread")]
    async fn promoted_state_survives_late_primary_message() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.election = ElectionState::Promoted;
        sec.is_primary = true;
        sec.primary_link.set_current_primary(Some("sec-a".into()));

        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Promoted));
        assert!(sec.is_primary);
    }

    /// Regression: PromotePrimary's routing target survives
    /// subsequent live-primary keepalives. Pre-fix
    /// `record_primary_message` unconditionally cleared the
    /// current-primary identity whenever the live primary kept
    /// sending keepalives, so `send_to_current_primary` on
    /// non-primary secondaries fell back to `primary_transport`
    /// (the demoted local primary) instead of unicasting to the
    /// SLURM-primary peer.
    /// Dispatch worked only as long as the local primary's
    /// `handle_task_request` relay path stayed alive; once its
    /// transport closed (laptop suspend / SSH idle) the relay
    /// vanished and TaskRequests stopped reaching the SLURM-primary,
    /// idling the entire fleet. Surfaced in dataset's K=2 hello run
    /// after 95b9f32 — synchronous PromotePrimary state-sync was
    /// correct but the very next keepalive clobbered the routing
    /// target.
    #[tokio::test(flavor = "current_thread")]
    async fn promote_primary_routing_survives_keepalive() {
        let mut sec = make_secondary(election_config("sec-b"));
        // Receive PromotePrimary naming a peer (sec-a) as the
        // SLURM-primary; sec-b is a regular peer.
        let promote = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 1,
            required_setup: false,
        };
        sec.dispatch_message(promote)
            .await
            .expect("PromotePrimary handler succeeds");
        assert_eq!(sec.primary_link.current_primary(), Some("sec-a"));
        assert!(!sec.is_primary);
        // The (still-alive, now-demoted) local primary keeps sending
        // keepalives. Pre-fix this would have flipped the routing
        // target back to None.
        sec.record_primary_message();
        assert_eq!(
            sec.primary_link.current_primary(),
            Some("sec-a"),
            "live-primary keepalive must not clobber the explicit handoff target",
        );
    }

    /// Regression: pre-designated primary's election state stays
    /// Promoted even when the local-machine primary's keepalives go
    /// silent. Pre-fix the `PromotePrimary` handler set
    /// `is_primary=true` but left `election=Normal`, so the
    /// keepalive-tick path's `if Promoted return` early-return did
    /// nothing for the pre-designated primary — the local primary's
    /// transport going silent post-promotion (its observer-mode
    /// demotion) drove the SLURM-primary itself into Suspecting and
    /// then Candidate, dropping its in-flight ledger via a self-re-
    /// promotion cascade. Surfaced in tokenizer's v6 trace.
    ///
    /// Drives the real `dispatch_message` PromotePrimary arm so the
    /// test would fail without the dispatch.rs fix that syncs
    /// `election` with `is_primary`.
    #[tokio::test(flavor = "current_thread")]
    async fn pre_designated_primary_ignores_silent_local_primary() {
        let mut sec = make_secondary(election_config("sec-a"));
        // Pre-promotion: Normal state, is_primary defaults false.
        assert!(matches!(sec.election, ElectionState::Normal));
        assert!(!sec.is_primary);

        // Receive PromotePrimary naming this node — exercises the
        // dispatch.rs handler that must set both fields in lockstep.
        let promote = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 1,
            required_setup: false,
        };
        sec.dispatch_message(promote)
            .await
            .expect("PromotePrimary handler succeeds");
        assert!(sec.is_primary);
        assert!(matches!(sec.election, ElectionState::Promoted));

        // Local primary stops sending keepalives — its observer-mode
        // demotion is benign post-promotion.
        sec.primary_last_seen = Some(
            std::time::Instant::now() - std::time::Duration::from_secs(60),
        );

        // Pre-fix this would have entered Suspecting and started a
        // self-re-promotion cascade. Post-fix the early-return fires.
        let actions = sec.run_election_tick();
        assert!(actions.broadcast.is_empty());
        assert!(matches!(sec.election, ElectionState::Promoted));
        assert!(sec.is_primary);
    }

    /// Phase P: PromotePrimary clears any per-worker backoff accrued
    /// against the previous primary. Without this, idle workers sit
    /// through a stale window before re-issuing at the new primary,
    /// reproducing the dispatch-silence symptom from the trace at
    /// `feb1052`.
    #[tokio::test(flavor = "current_thread")]
    async fn promote_primary_clears_per_worker_backoff() {
        let mut sec = make_secondary(election_config("sec-b"));
        // Simulate per-worker backoff accrued against the old primary.
        sec.primary_link.note_request_sent(0);
        sec.primary_link.note_request_sent(1);
        assert!(!sec.primary_link.should_request_now(0));
        assert!(!sec.primary_link.should_request_now(1));

        let promote = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 1,
            required_setup: false,
        };
        sec.dispatch_message(promote)
            .await
            .expect("PromotePrimary handler succeeds");

        // Both workers can fire a fresh request immediately at the
        // new primary.
        assert!(sec.primary_link.should_request_now(0));
        assert!(sec.primary_link.should_request_now(1));
    }

    /// Phase P: PromotePrimary feeds (epoch, primary) into the
    /// replicated `cluster_state`, where last-writer-wins on
    /// `(epoch, primary_id)` makes a stale lower-epoch broadcast a
    /// no-op against an already-installed higher-epoch promotion.
    #[tokio::test(flavor = "current_thread")]
    async fn promote_primary_applies_primary_changed_with_epoch() {
        let mut sec = make_secondary(election_config("sec-b"));

        let high = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-c".into(),
            epoch: 5,
            required_setup: false,
        };
        sec.dispatch_message(high).await.unwrap();
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-c"));
        assert_eq!(sec.cluster_state.primary_epoch(), 5);
        assert_eq!(sec.primary_link.current_primary(), Some("sec-c"));

        // A late lower-epoch broadcast must not clobber the higher
        // epoch already installed.
        let stale = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 2,
            required_setup: false,
        };
        sec.dispatch_message(stale).await.unwrap();
        assert_eq!(
            sec.cluster_state.current_primary(),
            Some("sec-c"),
            "stale lower-epoch PromotePrimary must not supersede higher epoch"
        );
        assert_eq!(sec.cluster_state.primary_epoch(), 5);
    }

    /// Scenario (d): two peers detect primary death simultaneously and both
    /// would-be-candidates start voting. The lowest-id rule + quorum
    /// resolves to a single winner; the higher-id peer defers to Voting
    /// instead of becoming Candidate.
    #[tokio::test(flavor = "current_thread")]
    async fn split_brain_lowest_id_wins() {
        let mut sec_a = make_secondary(election_config("sec-a"));
        sec_a.peer_keepalives.insert("sec-b".into(), 0.0);
        sec_a.peer_keepalives.insert("sec-c".into(), 0.0);
        sec_a.record_primary_message();

        let mut sec_b = make_secondary(election_config("sec-b"));
        sec_b.peer_keepalives.insert("sec-a".into(), 0.0);
        sec_b.peer_keepalives.insert("sec-c".into(), 0.0);
        sec_b.record_primary_message();

        tokio::time::sleep(PAST_DEATH).await;

        // Both detect primary death simultaneously and enter Suspecting.
        sec_a.run_election_tick();
        sec_b.run_election_tick();

        tokio::time::sleep(ONE_INTERVAL).await;

        // Both gather peer responses.
        sec_a.record_timeout_response("sec-b".into(), None);
        sec_a.record_timeout_response("sec-c".into(), None);
        sec_b.record_timeout_response("sec-a".into(), None);
        sec_b.record_timeout_response("sec-c".into(), None);

        // Tally + decide: sec-a is lowest in its peer set → Candidate.
        // sec-b sees sec-a as lowest in its peer set → Voting.
        sec_a.run_election_tick();
        sec_b.run_election_tick();

        assert!(
            matches!(sec_a.election, ElectionState::Candidate { .. }),
            "sec-a (lowest id) should self-promote"
        );
        match &sec_b.election {
            ElectionState::Voting { candidate, .. } => assert_eq!(candidate, "sec-a"),
            other => panic!("sec-b should defer to sec-a, got {:?}", std::mem::discriminant(other)),
        }

        // sec-b confirms sec-a; quorum 2 (peer_count=2). sec-a + sec-b = 2.
        let promoted = sec_a.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
        assert!(promoted);
        assert!(matches!(sec_a.election, ElectionState::Promoted));
        assert!(
            !matches!(sec_b.election, ElectionState::Promoted),
            "sec-b must NOT also promote — split-brain prevented"
        );
    }

    /// Observer self-exclusion (#35): a secondary with
    /// `is_observer = true` MUST NOT self-promote even when its id
    /// is lex-lowest in the alive set. Setup: "obs-a" (observer)
    /// and "sec-b" (regular), obs-a is lex-lowest. Primary goes
    /// silent. obs-a's election tick should defer to sec-b
    /// (next-lowest) instead of entering Candidate state.
    ///
    /// This is the load-bearing observer guard until the peer-side
    /// `lowest_alive` filter lands (which requires extending
    /// PeerConnectionInfo with is_observer and broadcasting via
    /// PeerInfo — tracked as a follow-up). Without this guard, an
    /// observer in the alive set with a lex-low id would
    /// self-promote despite having no workers and no dispatch
    /// authority — the cluster would then stall because the
    /// "promoted" node can't actually do anything.
    #[tokio::test(flavor = "current_thread")]
    async fn observer_never_self_promotes_even_when_lowest_id() {
        use super::super::test_helpers::{election_config, make_secondary};

        let mut cfg = election_config("obs-a");
        cfg.is_observer = true;
        let mut sec = make_secondary(cfg);

        // sec-b is the next-lowest. obs-a is lex-lowest in the alive
        // set (obs-a < sec-b lexicographically). Pre-fix this would
        // make obs-a self-promote on its election tick.
        sec.peer_keepalives.insert("sec-b".into(), timestamp_now());
        sec.record_primary_message();

        // Primary goes silent.
        tokio::time::sleep(PAST_DEATH).await;
        sec.run_election_tick();
        tokio::time::sleep(ONE_INTERVAL).await;

        // sec-b's TimeoutResponse so obs-a's Suspecting tick has
        // peer agreement to reach quorum.
        sec.record_timeout_response("sec-b".into(), None);

        let actions = sec.run_election_tick();

        // Observer MUST NOT enter Candidate state.
        assert!(
            !matches!(sec.election, ElectionState::Candidate { .. }),
            "observer entered Candidate state despite is_observer=true — \
             observer self-exclusion guard regressed. Election state: \
             {:?}",
            std::mem::discriminant(&sec.election),
        );

        // No PromotionVote broadcast either — observers must not
        // even campaign.
        assert!(
            !actions
                .broadcast
                .iter()
                .any(|m| matches!(m, DistributedMessage::PromotionVote { .. })),
            "observer broadcast a PromotionVote — must not campaign"
        );

        // Routing target should point at sec-b (the next-lowest
        // non-observer peer), so the observer's send_to_current_primary
        // routes correctly once sec-b self-promotes on its own tick.
        match &sec.election {
            ElectionState::Voting { candidate, .. } => {
                assert_eq!(
                    candidate, "sec-b",
                    "observer must defer to next-lowest non-observer peer"
                );
            }
            other => panic!(
                "observer should be in Voting state pointing at sec-b, got \
                 discriminant={:?}",
                std::mem::discriminant(other)
            ),
        }
    }
}

#[cfg(test)]
mod observer_peer_side_tests {
    use super::super::test_helpers::{election_config, make_secondary};
    use super::*;
    use std::time::Duration;

    const PAST_DEATH: Duration = Duration::from_millis(110);
    const ONE_INTERVAL: Duration = Duration::from_millis(60);

    /// #36 peer-side filter: a NON-observer secondary observing an
    /// observer in `peer_keepalives` MUST NOT defer to it as the
    /// `lowest_alive` candidate, even when the observer's id is
    /// lex-lowest. Pre-#36 the non-observer would have picked the
    /// observer as candidate and the cluster would stall (observer
    /// refuses self-promotion per #35).
    ///
    /// Setup: sec-b (non-observer) sees obs-a (recorded in the
    /// replicated `RoleTable.observers` via `PeerJoined { is_observer:
    /// true }`). obs-a is lex-lowest. After primary silence + quorum,
    /// sec-b must SELF-PROMOTE (since the only other peer is filtered).
    #[tokio::test(flavor = "current_thread")]
    async fn non_observer_filters_observer_from_lowest_alive() {
        use dynrunner_protocol_primary_secondary::ClusterMutation;
        let mut sec = make_secondary(election_config("sec-b"));
        // obs-a is registered as a peer AND marked observer.
        sec.peer_keepalives.insert("obs-a".into(), timestamp_now());
        sec.cluster_state.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
        });
        sec.record_primary_message();

        tokio::time::sleep(PAST_DEATH).await;
        sec.run_election_tick();
        tokio::time::sleep(ONE_INTERVAL).await;

        // obs-a doesn't respond to TimeoutQuery (observers can
        // respond, but this test pins the case where they don't —
        // the filter must still work). sec-b alone is enough for
        // quorum because peer_count is 1 (just obs-a), quorum =
        // (1+1)/2 + 1 = 2, agreeing = 1 (self) + 0 = 1.
        // For this test we have to either: (a) lower the threshold,
        // or (b) bypass the quorum check.
        //
        // Simpler: drive obs-a TimeoutResponse so quorum is met,
        // then assert filter behavior.
        sec.record_timeout_response("obs-a".into(), None);

        let actions = sec.run_election_tick();

        // sec-b MUST be in Candidate state (self-promoted), NOT
        // Voting for obs-a. The lowest_alive filter saw only sec-b
        // (after dropping obs-a) so sec-b is the lex-lowest and
        // self-promotes.
        assert!(
            matches!(sec.election, ElectionState::Candidate { .. }),
            "non-observer sec-b should self-promote (peer-filter dropped \
             obs-a from lowest_alive); got state={:?}",
            std::mem::discriminant(&sec.election)
        );
        assert!(
            actions
                .broadcast
                .iter()
                .any(|m| matches!(m, DistributedMessage::PromotionVote {
                    candidate_id, ..
                } if candidate_id == "sec-b")),
            "expected PromotionVote naming sec-b (self); broadcasts: \
             {} messages",
            actions.broadcast.len()
        );
    }

    /// Defensive guard test: a PromotePrimary naming an observer is
    /// rejected loud rather than installed in the routing target.
    /// Should not happen if peers honour the filter, but the
    /// rejection protects against forgeries and misconfigured peers.
    #[tokio::test(flavor = "current_thread")]
    async fn promote_primary_naming_observer_is_rejected() {
        use dynrunner_protocol_primary_secondary::ClusterMutation;
        let mut sec = make_secondary(election_config("sec-b"));
        sec.cluster_state.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
        });

        let promote = DistributedMessage::PromotePrimary::<
            super::super::test_helpers::TestId,
        > {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "obs-a".into(),
            epoch: 1,
            required_setup: false,
        };
        let result = sec.dispatch_message(promote).await;

        // Handler returns Ok(()) (silently rejects) — we don't
        // upgrade to Err because Err propagates to the processing
        // loop and exits the secondary, which is overkill for a
        // single bad PromotePrimary message. The rejection is
        // logged as error-level, which suffices.
        assert!(result.is_ok());

        // Routing target NOT installed; cluster_state primary
        // unchanged.
        assert!(
            sec.primary_link.current_primary().map(|s| s != "obs-a").unwrap_or(true),
            "primary_link.current_primary should NOT be obs-a after \
             rejected PromotePrimary"
        );
        assert!(
            sec.cluster_state
                .current_primary()
                .map(|s| s != "obs-a")
                .unwrap_or(true),
            "cluster_state should NOT install obs-a as primary"
        );
        assert!(
            !sec.is_primary,
            "sec-b should NOT have flipped to primary role"
        );
    }

    /// Step 7 / Decision G end-to-end: the `ClusterMutation::
    /// PeerJoined { is_observer: true }` apply rule is the SAME
    /// source of truth that both `lowest_alive` filtering and the
    /// defensive PromotePrimary rejection read. Without storage
    /// relocation, the deleted `peer_observers` HashSet would have
    /// produced identical results; with it, callers consult
    /// `cluster_state.role_table().observers` instead.
    ///
    /// This test pins:
    ///   (a) Reads via the role-table see the observer set populated
    ///       by `PeerJoined` (the production path is
    ///       `primary/peer_setup.rs::send_peer_lists` originating the
    ///       mutation alongside the PeerInfo broadcast).
    ///   (b) `lowest_alive` filter excludes the observer just as the
    ///       deleted `peer_observers` HashSet would have.
    ///   (c) The defensive PromotePrimary rejection also reads from
    ///       the role-table, refusing to install an observer as
    ///       primary even if the broadcast tries to.
    ///
    /// Concrete behaviour-preservation gate for the
    /// peer_observers→role_table.observers migration: same inputs
    /// produce the same outputs as before.
    #[tokio::test(flavor = "current_thread")]
    async fn role_table_observers_drives_filter_and_promote_rejection() {
        use dynrunner_protocol_primary_secondary::ClusterMutation;

        let mut sec = make_secondary(election_config("sec-b"));
        // Production path: `PeerJoined { is_observer: true }` apply
        // populates the role table.
        sec.cluster_state.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
        });
        sec.peer_keepalives.insert("obs-a".into(), timestamp_now());
        sec.record_primary_message();

        // (a) Role-table read sees the observer.
        assert!(sec
            .cluster_state
            .role_table()
            .observers
            .contains("obs-a"));

        // (b) Election filter excludes the observer from
        // lowest_alive — sec-b ends up self-promoting after the
        // primary times out (the only other peer is filtered).
        tokio::time::sleep(PAST_DEATH).await;
        sec.run_election_tick();
        tokio::time::sleep(ONE_INTERVAL).await;
        sec.record_timeout_response("obs-a".into(), None);
        let actions = sec.run_election_tick();
        assert!(
            matches!(sec.election, ElectionState::Candidate { .. }),
            "sec-b should self-promote (lowest_alive filter dropped obs-a)"
        );
        assert!(
            actions
                .broadcast
                .iter()
                .any(|m| matches!(m, DistributedMessage::PromotionVote {
                    candidate_id, ..
                } if candidate_id == "sec-b")),
            "expected PromotionVote naming sec-b"
        );

        // (c) Defensive PromotePrimary rejection: a spurious
        // PromotePrimary naming the observer is silently rejected
        // (logged at error level) without flipping role.
        let promote = DistributedMessage::PromotePrimary::<
            super::super::test_helpers::TestId,
        > {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "obs-a".into(),
            epoch: 99,
            required_setup: false,
        };
        sec.dispatch_message(promote)
            .await
            .expect("PromotePrimary handler returns Ok even when rejecting");
        assert!(
            sec.cluster_state
                .current_primary()
                .map(|s| s != "obs-a")
                .unwrap_or(true),
            "observer must NOT be installed as primary"
        );
    }
}
