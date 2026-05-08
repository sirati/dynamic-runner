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

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport, PrimaryTransport};
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
    PT: PrimaryTransport<I>,
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
        use dynrunner_core::{PhaseId, TaskInfo, TypeId};
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
}
