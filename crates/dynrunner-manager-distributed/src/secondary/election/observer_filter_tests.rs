#![cfg(test)]

use super::super::test_helpers::{FakeWorkerFactory, election_config, make_secondary};
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
    sec.enter_operational_for_test();
    // obs-a is registered as a peer AND marked observer.
    sec.op_mut()
        .peer_keepalives
        .insert("obs-a".into(), std::time::Instant::now());
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-a".into(),
        is_observer: true,
        can_be_primary: false,
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
        matches!(sec.op_mut().election, ElectionState::Candidate { .. }),
        "non-observer sec-b should self-promote (peer-filter dropped \
             obs-a from lowest_alive); got state={:?}",
        std::mem::discriminant(&sec.op_mut().election)
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

/// Defensive guard test: a `PrimaryChanged` naming an observer is
/// rejected loud rather than installed in the routing target.
/// Should not happen if peers honour the filter, but the
/// rejection protects against forgeries and misconfigured peers.
#[tokio::test(flavor = "current_thread")]
async fn primary_changed_naming_observer_is_rejected() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-b"));
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-a".into(),
        is_observer: true,
        can_be_primary: false,
    });

    let promote = DistributedMessage::ClusterMutation::<super::super::test_helpers::TestId> {
        sender_id: "primary".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "obs-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    let result = sec.dispatch_message(promote, &mut FakeWorkerFactory).await;

    // Handler returns Ok(()) (silently rejects) — we don't
    // upgrade to Err because Err propagates to the processing
    // loop and exits the secondary, which is overkill for a
    // single bad announcement. The rejection is logged as
    // error-level inside the apply hook, which suffices.
    assert!(result.is_ok());

    // The replicated CRDT — the single source of "who is primary"
    // post-unification — must NOT install the observer as primary.
    assert!(
        sec.cluster_state
            .current_primary()
            .map(|s| s != "obs-a")
            .unwrap_or(true),
        "cluster_state should NOT install obs-a as primary"
    );
}

/// Step 7 / Decision G end-to-end: the `ClusterMutation::
/// PeerJoined { is_observer: true }` apply rule is the SAME
/// source of truth that both `lowest_alive` filtering and the
/// defensive PrimaryChanged rejection read. Without storage
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
///   (c) The defensive PrimaryChanged rejection also reads from
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
    sec.enter_operational_for_test();
    // Production path: `PeerJoined { is_observer: true }` apply
    // populates the role table.
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-a".into(),
        is_observer: true,
        can_be_primary: false,
    });
    sec.op_mut()
        .peer_keepalives
        .insert("obs-a".into(), std::time::Instant::now());
    sec.record_primary_message();

    // (a) Role-table read sees the observer.
    assert!(sec.cluster_state.role_table().observers.contains("obs-a"));

    // (b) Election filter excludes the observer from
    // lowest_alive — sec-b ends up self-promoting after the
    // primary times out (the only other peer is filtered).
    tokio::time::sleep(PAST_DEATH).await;
    sec.run_election_tick();
    tokio::time::sleep(ONE_INTERVAL).await;
    sec.record_timeout_response("obs-a".into(), None);
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Candidate { .. }),
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

    // (c) Defensive PrimaryChanged rejection: a spurious
    // PrimaryChanged naming the observer is silently rejected
    // (logged at error level) without flipping role.
    let promote = DistributedMessage::ClusterMutation::<super::super::test_helpers::TestId> {
        sender_id: "primary".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "obs-a".into(),
            epoch: 99,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler returns Ok even when rejecting");
    assert!(
        sec.cluster_state
            .current_primary()
            .map(|s| s != "obs-a")
            .unwrap_or(true),
        "observer must NOT be installed as primary"
    );
}
