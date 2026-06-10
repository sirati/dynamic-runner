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
use super::super::lifecycle::SecondaryLifecycle;
use super::super::test_helpers::{
    FakeWorkerFactory, RecordingPeer, SecondaryHarness, TestId, election_config,
    make_secondary_recording,
};
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType};
use dynrunner_transport_channel::{ChannelManagerEnd, channel_pair};
use std::cell::RefCell;
use std::rc::Rc;

/// A `WorkerFactory` that records each `spawn_worker` into a shared ordering
/// log, then behaves like `FakeWorkerFactory` (Ready → Done echo). Paired
/// with a recording finalize closure to prove the finalize fires BEFORE the
/// first worker spawn reads `cmd_args`.
struct OrderingFactory {
    order: Rc<RefCell<Vec<&'static str>>>,
}

impl dynrunner_manager_local::WorkerFactory<ChannelManagerEnd> for OrderingFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: dynrunner_core::WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.order.borrow_mut().push("spawn");
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(async move {
            use dynrunner_core::{MessageReceiver, MessageSender};
            use dynrunner_protocol_manager_worker::{Command, Response};
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) | None => break,
                    // Test fixtures ignore consumer custom messages.
                    Some(Command::Custom { .. }) => {}
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner.send(Response::Done { result_data: None }).await;
                    }
                }
            }
        });
        Ok((manager_end, None))
    }
}

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
fn run_config_argvs(log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>) -> Vec<Vec<String>> {
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
            let (mut sec, log) = operational_secondary_with_argv("responder", seeded.clone());

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

/// An inbound `RunConfig` PUSH from the primary STORES its `forwarded_argv`
/// into the secondary's node-local copy, overwriting the (empty boot-CLI)
/// pre-seed. This is the deferred-delivery half of the primary push: the
/// secondary booted with only its boot-critical args, and the primary
/// unicasts the consumer's run-config the moment it welcomes this node.
/// After the push the stored copy is what the run path reads AND what THIS
/// node re-serves on a peer's `RequestRunConfig`.
#[tokio::test(flavor = "current_thread")]
async fn inbound_run_config_push_stores_forwarded_argv() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Boot with an EMPTY pre-seed (the cold-start boot CLI omits the
            // run-config) so the assertion pins that the push REPLACES it.
            let (mut sec, _log) = operational_secondary_with_argv("sec-0", Vec::new());
            assert!(
                sec.forwarded_argv.lock().unwrap().is_empty(),
                "precondition: cold-start secondary boots with empty forwarded_argv"
            );

            let pushed = vec![
                "--task".to_string(),
                "tokenize".to_string(),
                "--platform".to_string(),
                "x86".to_string(),
            ];
            let push = DistributedMessage::RunConfig {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                forwarded_argv: pushed.clone(),
            };
            sec.dispatch_message(push, &mut FakeWorkerFactory)
                .await
                .expect("inbound RunConfig handler succeeds");

            assert_eq!(
                *sec.forwarded_argv.lock().unwrap(),
                pushed,
                "the pushed RunConfig must be stored token-for-token as the \
                 secondary's node-local forwarded_argv"
            );

            // The stored value now round-trips through the responder: a peer's
            // RequestRunConfig is answered with the PUSHED argv (proves the
            // push and the re-serve share the one node-local copy).
            let req = DistributedMessage::RequestRunConfig {
                target: None,
                sender_id: "peer-0".into(),
                timestamp: 0.0,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("RequestRunConfig handler succeeds");
            sec.drain_egress().await;
            let argvs = run_config_argvs(&_log);
            assert_eq!(
                argvs.len(),
                1,
                "responder must emit exactly one RunConfig after the push; got {argvs:?}"
            );
            assert_eq!(
                argvs[0], pushed,
                "the re-served RunConfig must carry the PUSHED forwarded_argv"
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

/// Move a `Connecting` coordinator into `AwaitingPrimary` (the state
/// `enter_configuring_on_first_primary_frame` requires), mirroring the
/// `Connecting → AwaitingPrimary` transition the run loop drives on the dial.
fn arm_awaiting_primary(sec: &mut SecondaryHarness<RecordingPeer<TestId>>) {
    let lifecycle = std::mem::replace(&mut sec.lifecycle, SecondaryLifecycle::connecting());
    sec.lifecycle = lifecycle.enter_awaiting_primary();
}

/// (step-8a) The run-config finalize fires BEFORE `initialize_workers` reads
/// `cmd_args`: driving `enter_configuring_on_first_primary_frame` records the
/// finalize event strictly before the first worker spawn.
#[tokio::test(flavor = "current_thread")]
async fn finalize_fires_before_initialize_workers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(election_config("sec-0"), 0);
            arm_awaiting_primary(&mut sec);

            // The push delivered (so the backstop short-circuits), carrying a
            // value the finalize will observe.
            sec.store_pushed_run_config(vec!["--platform".into(), "x64".into()]);

            let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
            let order_for_finalize = order.clone();
            let seen_argv: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
            let seen_for_finalize = seen_argv.clone();
            sec.register_finalize_run_config(Box::new(move |delivered: Vec<String>| {
                order_for_finalize.borrow_mut().push("finalize");
                *seen_for_finalize.borrow_mut() = delivered;
                Box::pin(async { Ok(()) })
            }));

            let mut factory = OrderingFactory {
                order: order.clone(),
            };
            sec.enter_configuring_on_first_primary_frame(&mut factory)
                .await
                .expect("enter_configuring must succeed");

            assert_eq!(
                *order.borrow(),
                vec!["finalize", "spawn"],
                "the finalize must fire BEFORE the worker pool's first spawn reads cmd_args"
            );
            assert_eq!(
                *seen_argv.borrow(),
                vec!["--platform".to_string(), "x64".to_string()],
                "the finalize must receive the DELIVERED forwarded_argv off the shared handle"
            );
        })
        .await;
}

/// (step-8c) No finalize registered (the legacy Rust-only fixture / out-of-tree
/// direct-driver path) → `enter_configuring` still spawns workers, and the
/// shared run-config handle is untouched (the seam is skipped). Note the
/// `args=` consumer path (compiler_suit) registers an IDENTITY finalizer
/// instead, exercising the Some-but-no-op seam; this fixture pins the genuine
/// `None`-skips-the-seam path.
#[tokio::test(flavor = "current_thread")]
async fn no_finalize_registered_is_faithful_no_op() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(election_config("sec-0"), 0);
            arm_awaiting_primary(&mut sec);

            let argv_before = sec.forwarded_argv.lock().unwrap().clone();

            // No `register_finalize_run_config` call: the inert path.
            let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
            let mut factory = OrderingFactory {
                order: order.clone(),
            };
            sec.enter_configuring_on_first_primary_frame(&mut factory)
                .await
                .expect("enter_configuring must succeed with no finalize registered");

            // Workers still spawn; the run-config handle is byte-identical.
            assert_eq!(
                *order.borrow(),
                vec!["spawn"],
                "with no finalize the pool still spawns (the seam is inert, not blocking)"
            );
            assert_eq!(
                *sec.forwarded_argv.lock().unwrap(),
                argv_before,
                "a no-finalize run must leave the run-config handle byte-identical"
            );
        })
        .await;
}

/// (step-8b) The shared run-config handle the PROMOTION RECIPE reads reflects
/// the DELIVERED argv (post-push), not the stale boot seed. The recipe
/// (built in the pyo3 wrapper) captures a clone of THIS handle and reads it at
/// promotion, threading the value into the promoted `PrimaryConfig.forwarded_argv`
/// — so this pins the staleness fix at the layer the handle lives in.
#[tokio::test(flavor = "current_thread")]
async fn run_config_handle_reflects_delivered_argv_for_promotion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Boot with an EMPTY seed (the cold-start case the bug bit: the
            // pre-push recipe capture would have frozen this empty value).
            let (sec, _log) = make_secondary_recording(election_config("sec-0"), 0);
            let recipe_handle = sec.run_config_handle();
            assert!(
                recipe_handle.lock().unwrap().is_empty(),
                "precondition: the recipe handle starts at the empty boot seed"
            );

            let mut sec = sec;
            let delivered = vec![
                "--jobs".to_string(),
                "8".to_string(),
                "--name-regex".to_string(),
                "foo.*".to_string(),
            ];
            sec.store_pushed_run_config(delivered.clone());

            // The handle the recipe holds now reflects the DELIVERED argv —
            // the promoted PrimaryConfig.forwarded_argv would carry this, NOT
            // the stale empty seed.
            assert_eq!(
                *recipe_handle.lock().unwrap(),
                delivered,
                "the promotion recipe's shared handle must reflect the delivered \
                 (post-push) argv, not the stale boot seed"
            );
        })
        .await;
}
