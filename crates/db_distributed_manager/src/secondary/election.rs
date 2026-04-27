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

use db_comm_api_base::Identifier;
use db_manager_runner_comm::ManagerEndpoint;
use db_primary_secondary_comm::{DistributedMessage, PeerTransport, PrimaryTransport};
use db_scheduler_api::{ResourceEstimator, Scheduler};

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
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator + Clone,
    I: Identifier,
{
    /// Bump the primary-keepalive timestamp on every primary message and
    /// reset election state if we're not yet the new primary. Called from
    /// the dispatch path before any other handling. Also clears any
    /// remembered new-primary-peer routing target — the original primary
    /// is alive, so TaskRequest goes back to `primary_transport`.
    pub(super) fn record_primary_message(&mut self) {
        self.primary_last_seen = Some(Instant::now());
        if !matches!(self.election, ElectionState::Promoted) {
            self.election = ElectionState::Normal;
            self.slurm_primary_peer_id = None;
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

        match &self.election {
            ElectionState::Normal if primary_silent => {
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    miss_threshold = self.config.keepalive_miss_threshold,
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
                let quorum = (peer_count + 1) / 2 + 1;
                if agreeing < quorum {
                    tracing::info!(
                        agreeing,
                        quorum,
                        "no quorum on primary death; waiting"
                    );
                    return actions;
                }
                let lowest_alive = self
                    .peer_keepalives
                    .keys()
                    .chain(std::iter::once(&self.config.secondary_id))
                    .min()
                    .cloned();
                let we_lead = lowest_alive
                    .as_ref()
                    .map(|id| id == &self.config.secondary_id)
                    .unwrap_or(false);
                let round = next_round(&self.election);
                if we_lead {
                    tracing::info!(round, "self-promoting");
                    self.election = ElectionState::Candidate {
                        round,
                        confirms: HashSet::from([self.config.secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    self.slurm_primary_peer_id = Some(self.config.secondary_id.clone());
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: self.config.secondary_id.clone(),
                        vote_round: round,
                    });
                } else if let Some(candidate) = lowest_alive {
                    tracing::info!(%candidate, round, "deferring to lowest-live-id peer");
                    self.slurm_primary_peer_id = Some(candidate.clone());
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
            self.slurm_primary_peer_id = Some(candidate.clone());
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
        let quorum = (peer_count + 1) / 2 + 1;
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
            self.is_slurm_primary = true;
            self.populate_slurm_from_cache();
        }
        promoted
    }

    /// On promotion, hydrate `slurm_pending_binaries` from whatever the
    /// most recent live primary broadcast in `FullTaskList`. If no
    /// broadcast was ever observed (e.g. the election fired before the
    /// primary even sent one), the new primary starts with an empty
    /// pending list — peers will still request work and the primary will
    /// reply "no tasks available", which is the safe degrade.
    pub(super) fn populate_slurm_from_cache(&mut self) {
        if let Some((all_tasks, completed)) = self.cached_full_task_list.take() {
            tracing::info!(
                total = all_tasks.len(),
                completed = completed.len(),
                "post-promotion: hydrating SLURM-primary pending list from cached FullTaskList"
            );
            self.populate_slurm_tasks(all_tasks, completed);
        } else {
            tracing::warn!(
                "post-promotion: no cached FullTaskList; new primary starts with empty pending list"
            );
        }
    }

    /// Whether the secondary has been elected primary in this run.
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

    /// Once promoted, a secondary's `slurm_pending_binaries` is hydrated
    /// from the cached `FullTaskList` it observed earlier from the live
    /// primary. Validates the post-promotion takeover wiring (#34).
    #[tokio::test(flavor = "current_thread")]
    async fn promotion_hydrates_slurm_tasks_from_cache() {
        use db_comm_api_base::ResourceMap;
        use db_primary_secondary_comm::{DistributedBinaryInfo, TaskInfo};
        use std::collections::HashSet;

        let mut sec = make_secondary(election_config("sec-a"));
        sec.peer_keepalives.insert("sec-b".into(), 0.0);

        // Pre-seed the cache as if the live primary had broadcast
        // FullTaskList earlier in the run.
        sec.cached_full_task_list = Some((
            vec![TaskInfo {
                local_path: "bin1".into(),
                binary_info: DistributedBinaryInfo {
                    path: "bin1".into(),
                    size: 100,
                    identifier: super::super::test_helpers::TestId("bin1".into()),
                },
                hash: "hash_bin1".into(),
                file_path: None,
            }],
            HashSet::new(),
        ));

        // Simulate the candidate path: become candidate, then receive
        // confirm to flip to Promoted. peer_count=1, quorum=2, so we need
        // confirms from {self, sec-b} to promote.
        sec.election = ElectionState::Candidate {
            round: 1,
            confirms: HashSet::from(["sec-a".to_string()]),
            started: std::time::Instant::now(),
        };
        sec.is_slurm_primary = false; // not yet
        let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
        assert!(promoted, "majority confirm should promote");

        assert!(matches!(sec.election, ElectionState::Promoted));
        assert!(sec.is_slurm_primary, "promotion sets is_slurm_primary");
        assert_eq!(
            sec.slurm_pending_binaries.len(),
            1,
            "cache should have hydrated one pending binary"
        );
        assert!(
            sec.cached_full_task_list.is_none(),
            "cache is consumed on hydration"
        );
        let _ = ResourceMap::new(); // silence unused import on some configs
    }

    /// `record_primary_message` resets election state and clears any
    /// remembered new-primary-peer routing target — the live primary is
    /// alive again, so TaskRequest routes back to primary_transport.
    #[tokio::test(flavor = "current_thread")]
    async fn primary_recovery_clears_routing_target() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.slurm_primary_peer_id = Some("sec-c".into());
        sec.election = ElectionState::Voting {
            round: 1,
            candidate: "sec-c".into(),
        };
        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Normal));
        assert!(
            sec.slurm_primary_peer_id.is_none(),
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
        sec.is_slurm_primary = true;
        sec.slurm_primary_peer_id = Some("sec-a".into());

        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Promoted));
        assert!(sec.is_slurm_primary);
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
}
