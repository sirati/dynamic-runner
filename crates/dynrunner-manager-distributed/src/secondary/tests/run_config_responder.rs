#![cfg(test)]

//! The secondary's `RequestRunConfig` PURE responder.
//!
//! The secondary answers a `RequestRunConfig` from its node-local
//! `forwarded_argv` and unicasts exactly ONE `RunConfig` back — read-only
//! peer gossip, available on the secondary role so a cold-start fetch is
//! answerable before any primary exists / promotes. The responder is PURE:
//! it MUST NOT originate `PeerJoined`, MUST NOT send any welcome, and MUST
//! NOT touch the replicated ledger. These tests pin that contract (the
//! seeded argv round-trips token-for-token; zero mutation/welcome frames;
//! the replicated state fingerprint + role table are unchanged) and the
//! graceful empty-pre-seed case.

use super::super::SecondaryConfig;
use super::super::test_helpers::{
    FakeWorkerFactory, RecordingPeer, SecondaryHarness, TestId, election_config,
    make_secondary_recording,
};
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType};
use std::cell::RefCell;
use std::rc::Rc;

/// Build an operational secondary over a `RecordingPeer` mesh stub, seeded
/// with `forwarded_argv`. Returns the harness + the shared peer-bus log so
/// a test can drain the queued egress and assert on what was fanned out.
#[allow(clippy::type_complexity)]
fn operational_secondary_with_argv(
    secondary_id: &str,
    forwarded_argv: Vec<String>,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    let config = SecondaryConfig {
        forwarded_argv,
        ..election_config(secondary_id)
    };
    // `peer_count = 1` so the synthetic membership (the recorder models a
    // healthy mesh keyed `primary` + `peer-0`) lets the unicast reply to the
    // requester (`peer-0`, a connected member — exactly what a joining /
    // cold-start-fetching peer is by the time it asks) route through the
    // egress no-route gate rather than being dropped.
    let (mut sec, log) = make_secondary_recording(config, 1);
    sec.enter_operational_for_test();
    (sec, log)
}

/// Pull every `RunConfig` frame's `forwarded_argv` out of the recorded
/// peer-bus traffic (the unicast reply lands here via `send_to_peer`).
fn run_config_argvs(
    log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
) -> Vec<Vec<String>> {
    log.borrow()
        .iter()
        .filter_map(|msg| match msg {
            DistributedMessage::RunConfig { forwarded_argv, .. } => Some(forwarded_argv.clone()),
            _ => None,
        })
        .collect()
}

/// Count the frames of a given `MessageType` in the recorded traffic.
fn count_of(log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>, kind: MessageType) -> usize {
    log.borrow()
        .iter()
        .filter(|msg| msg.msg_type() == kind)
        .count()
}

/// Seeded argv round-trips token-for-token, and the responder is PURE:
/// exactly ONE `RunConfig`, ZERO `ClusterMutation`/`SecondaryWelcome`
/// frames, and the replicated state fingerprint + role table are unchanged.
#[tokio::test(flavor = "current_thread")]
async fn run_config_responder_is_pure_and_round_trips_argv() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let seeded = vec![
                "--jobs".to_string(),
                "8".to_string(),
                "--log-oom-watcher".to_string(),
            ];
            let (mut sec, log) =
                operational_secondary_with_argv("responder", seeded.clone());

            // Capture the replicated-state fingerprint + the role-table
            // projection BEFORE the request: a pure responder leaves both
            // untouched (covers roster / quorum / secondary_capacities /
            // peer_state convergence-bearing state in one fold + the
            // node-local role projection).
            let digest_before = sec.cluster_state.digest();
            let role_table_before = sec.cluster_state.role_table().clone();

            let req = DistributedMessage::RequestRunConfig {
                target: None,
                sender_id: "peer-0".into(),
                timestamp: 0.0,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("RequestRunConfig handler succeeds");
            // Flush the queued unicast reply onto the RecordingPeer log
            // (MeshClient::send is queued).
            sec.drain_egress().await;

            // Exactly ONE RunConfig, carrying the seeded argv verbatim.
            let argvs = run_config_argvs(&log);
            assert_eq!(
                argvs.len(),
                1,
                "responder must emit exactly one RunConfig; got {argvs:?}"
            );
            assert_eq!(
                argvs[0], seeded,
                "the RunConfig must carry the seeded forwarded_argv token-for-token"
            );

            // PURE: no membership/authority frames originated.
            assert_eq!(
                count_of(&log, MessageType::ClusterMutation),
                0,
                "pure responder must NOT originate any ClusterMutation (no PeerJoined)"
            );
            assert_eq!(
                count_of(&log, MessageType::SecondaryWelcome),
                0,
                "pure responder must NOT send a SecondaryWelcome"
            );

            // Replicated state + role projection unchanged.
            assert_eq!(
                sec.cluster_state.digest(),
                digest_before,
                "pure responder must NOT mutate the replicated ledger \
                 (roster / quorum / secondary_capacities convergence state)"
            );
            assert_eq!(
                sec.cluster_state.role_table(),
                &role_table_before,
                "pure responder must NOT touch the role table / peer_state projection"
            );
        })
        .await;
}

/// Empty pre-seed → empty argv (graceful): a node that never received a
/// run-config still answers, with an empty `forwarded_argv`.
#[tokio::test(flavor = "current_thread")]
async fn run_config_responder_empty_preseed_answers_empty() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = operational_secondary_with_argv("responder", Vec::new());

            let req = DistributedMessage::RequestRunConfig {
                target: None,
                sender_id: "peer-0".into(),
                timestamp: 0.0,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("RequestRunConfig handler succeeds");
            sec.drain_egress().await;

            let argvs = run_config_argvs(&log);
            assert_eq!(
                argvs.len(),
                1,
                "responder must still emit exactly one RunConfig on empty pre-seed; got {argvs:?}"
            );
            assert!(
                argvs[0].is_empty(),
                "empty pre-seed must answer with an empty forwarded_argv; got {:?}",
                argvs[0]
            );
        })
        .await;
}
