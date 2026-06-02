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

    /// `record_primary_message` resets the failover election state to
    /// Normal — the live primary is alive again, so the secondary stops
    /// suspecting / voting. (Post-unification "who is primary" is the
    /// transport RoleCache, not a PrimaryLink locality field; a live
    /// keepalive resets the ELECTION, never the replicated primary
    /// identity.)
    #[tokio::test(flavor = "current_thread")]
    async fn primary_recovery_resets_election_state() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.election = ElectionState::Voting {
            round: 1,
            candidate: "sec-c".into(),
        };
        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Normal));
    }

    /// `Promoted` state survives a `record_primary_message`: once we've
    /// taken over, a stray late message from the dead primary doesn't
    /// dethrone us.
    #[tokio::test(flavor = "current_thread")]
    async fn promoted_state_survives_late_primary_message() {
        let mut sec = make_secondary(election_config("sec-a"));
        sec.election = ElectionState::Promoted;

        sec.record_primary_message();
        assert!(matches!(sec.election, ElectionState::Promoted));
    }

    /// Regression: PromotePrimary's routing target survives
    /// subsequent live-primary keepalives. Pre-fix
    /// `record_primary_message` unconditionally cleared the
    /// current-primary identity whenever the live primary kept
    /// sending keepalives, so `send_to_primary` on
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
        sec.dispatch_message(promote, &mut FakeWorkerFactory)
            .await
            .expect("PromotePrimary handler succeeds");
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
        // The (still-alive, now-demoted) local primary keeps sending
        // keepalives. A live-primary keepalive resets the election but
        // must NOT clobber the replicated primary identity (the
        // PrimaryChanged apply is last-writer-wins on epoch).
        sec.record_primary_message();
        assert_eq!(
            sec.cluster_state.current_primary(),
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
        // Pre-promotion: Normal state, this node is not yet primary.
        assert!(matches!(sec.election, ElectionState::Normal));
        assert!(sec.cluster_state.current_primary().is_none());

        // Receive PromotePrimary naming this node — exercises the
        // dispatch.rs handler that installs the role into the CRDT
        // (which drives the transport RoleCache write-through hook) AND
        // flips the election state to Promoted in lockstep.
        let promote = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 1,
            required_setup: false,
        };
        sec.dispatch_message(promote, &mut FakeWorkerFactory)
            .await
            .expect("PromotePrimary handler succeeds");
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
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
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
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
        sec.dispatch_message(promote, &mut FakeWorkerFactory)
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
        sec.dispatch_message(high, &mut FakeWorkerFactory).await.unwrap();
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-c"));
        assert_eq!(sec.cluster_state.primary_epoch(), 5);
        assert_eq!(sec.cluster_state.current_primary(), Some("sec-c"));

        // A late lower-epoch broadcast must not clobber the higher
        // epoch already installed.
        let stale = DistributedMessage::PromotePrimary {
            sender_id: "primary".into(),
            timestamp: 0.0,
            new_primary_id: "sec-a".into(),
            epoch: 2,
            required_setup: false,
        };
        sec.dispatch_message(stale, &mut FakeWorkerFactory).await.unwrap();
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

    /// Observer non-participation (pure-observer role): a secondary
    /// with `is_observer = true` is a passive bystander with ZERO
    /// authority/responsibility — it does NOT participate in failover
    /// at all. Even when its id is lex-lowest in the alive set and the
    /// primary goes silent, its election tick is a complete no-op: it
    /// never suspects, never broadcasts a TimeoutQuery / PromotionVote,
    /// never enters Candidate or Voting. The cluster's failover is
    /// driven entirely by the NON-observer peers (sec-b self-elects on
    /// its own tick); the observer just observes the resulting
    /// `PrimaryChanged`. This is the "observer originates NOTHING"
    /// invariant applied to the failover concern.
    #[tokio::test(flavor = "current_thread")]
    async fn observer_election_tick_is_a_no_op_even_when_lowest_id() {
        use super::super::test_helpers::{election_config, make_secondary};

        let mut cfg = election_config("obs-a");
        cfg.is_observer = true;
        let mut sec = make_secondary(cfg);

        // obs-a is lex-lowest in the alive set (obs-a < sec-b). Under
        // the OLD model this would drive an election; under the
        // pure-observer model it originates nothing.
        sec.peer_keepalives.insert("sec-b".into(), timestamp_now());
        sec.record_primary_message();

        // Primary goes silent — a worker secondary would suspect here.
        tokio::time::sleep(PAST_DEATH).await;
        let actions_1 = sec.run_election_tick();
        tokio::time::sleep(ONE_INTERVAL).await;
        sec.record_timeout_response("sec-b".into(), None);
        let actions_2 = sec.run_election_tick();

        // The observer stayed Normal across both ticks and originated
        // NOTHING.
        assert!(
            matches!(sec.election, ElectionState::Normal),
            "observer must stay Normal (no failover participation); got {:?}",
            std::mem::discriminant(&sec.election),
        );
        assert!(
            actions_1.broadcast.is_empty() && actions_2.broadcast.is_empty(),
            "observer election tick must originate no broadcast",
        );
    }
