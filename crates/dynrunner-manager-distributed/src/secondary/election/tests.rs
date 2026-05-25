#![cfg(test)]

    //! Failover scenarios (b), (c), (d) from the migration plan, exercised
    //! at the election state-machine level. The full multi-process
    //! integration tests over channels would require post-promotion task
    //! takeover (re-distributing pending work from the dead primary), which
    //! is not yet implemented in pure Rust — these tests cover the
    //! detection + voting algorithm itself.
    //!
    //! Scenario (a) — secondary dies → primary requeues — is covered in
    //! `crate::primary::heartbeat::tests`.

    use super::super::test_helpers::{election_config, make_secondary, FakeWorkerFactory};
    use super::super::wire::timestamp_now;
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
            task_id: "bin1".into(),
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
        sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
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
        sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
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
        sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
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
        sec.dispatch_message(high, &mut None, &mut FakeWorkerFactory).await.unwrap();
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
        sec.dispatch_message(stale, &mut None, &mut FakeWorkerFactory).await.unwrap();
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
