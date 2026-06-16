//! #308 — the secondary side of the per-task reconciliation probe: the
//! `TaskHoldQuery` responder.
//!
//! The responder answers from this node's LIVE own-worker bookkeeping
//! (`SecondaryLifecycle::holds_task`): the generation-aware
//! `active_tasks` map PLUS the `pending_first_bind` deferrals (a
//! respawn-HOLD task is genuinely held — parked, not lost). A hash in
//! NEITHER is a positive `held = false` denial, which the primary acts
//! on (fail + requeue) — so the truthfulness of both branches is what
//! this family pins. The reply rides `send_to_primary`
//! (`Destination::Primary`), observable in the recorded wire log.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, election_config, make_secondary_recording_with_membership,
};
use super::super::{PendingAffineDependent, *};
use super::processing::make_binary;
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// Build a `PendingAffineDependent` for work task `B` (`work_hash`) parked
/// behind a local SecondaryAffine import on `worker_id` (#497).
fn make_affine_dependent(work_hash: &str, worker_id: u32) -> PendingAffineDependent<TestId> {
    PendingAffineDependent {
        work_hash: work_hash.to_string(),
        worker_id,
        binary: make_binary(work_hash, 50),
        estimated: dynrunner_core::ResourceMap::new(),
        predecessor_outputs: Default::default(),
    }
}

/// Collect every `TaskHoldResponse` in the recorded wire log as
/// `(task_hash, held)` pairs.
fn hold_responses(
    log: &std::rc::Rc<
        std::cell::RefCell<Vec<DistributedMessage<super::super::test_helpers::TestId>>>,
    >,
) -> Vec<(String, bool)> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TaskHoldResponse {
                task_hash, held, ..
            } => Some((task_hash.clone(), *held)),
            _ => None,
        })
        .collect()
}

async fn probe(
    secondary: &mut super::super::test_helpers::SecondaryHarness<
        super::super::test_helpers::RecordingPeer<super::super::test_helpers::TestId>,
    >,
    task_hash: &str,
) {
    let query = DistributedMessage::TaskHoldQuery {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        task_hash: task_hash.into(),
    };
    secondary
        .handle_inbound(query, &mut FakeWorkerFactory)
        .await;
    secondary.drain_egress().await;
}

/// One operational secondary answering three probes: a hash bound in
/// `active_tasks` (held), a hash parked in `pending_first_bind` (held —
/// a respawn-HOLD deferral is live bookkeeping), and a hash in neither
/// (the positive denial the primary requeues on). Each reply echoes the
/// queried hash verbatim.
#[tokio::test(flavor = "current_thread")]
async fn responder_answers_from_active_tasks_and_pending_first_bind() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // A task bound in the generation-aware active_tasks map …
            secondary.active_tasks_mut().insert("h-active".into(), 0);
            // … and one parked in the first-bind deferral (respawn-HOLD).
            secondary.op_mut().pending_first_bind.insert(
                0,
                PendingFirstBind {
                    binary: make_binary("fb-task", 50),
                    file_hash: "h-firstbind".into(),
                    estimated: dynrunner_core::ResourceMap::new(),
                    predecessor_outputs: Default::default(),
                },
            );

            probe(&mut secondary, "h-active").await;
            probe(&mut secondary, "h-firstbind").await;
            probe(&mut secondary, "h-unknown").await;

            assert_eq!(
                hold_responses(&log),
                vec![
                    ("h-active".to_string(), true),
                    ("h-firstbind".to_string(), true),
                    ("h-unknown".to_string(), false),
                ],
                "active_tasks => held; pending_first_bind => held; \
                 neither => the positive denial"
            );
        })
        .await;
}

/// #497 — a work task parked in `affine_running` (deferred behind this
/// node's local SecondaryAffine import) answers `held = true`: it was
/// assigned to this node and its dispatch is merely withheld until the
/// import releases, so it is genuinely held own-worker bookkeeping. Without
/// this branch the parked dependent answers the positive denial, the
/// primary requeues it onto the SAME affine secondary, it defers again, and
/// the reconciliation probe loops forever (the never-registering-task leak).
///
/// REVERT-CHECK: drop the `affine_running` branch from `holding_worker` and
/// this asserts `held = false` (the looping pre-fix shape).
#[tokio::test(flavor = "current_thread")]
async fn responder_answers_held_for_affine_running_parked_dependent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Park work task B (h-parked) behind a local import gate
            // (gate-I), bound to worker 0 — exactly what `ensure_affine_import`
            // does on a defer.
            secondary
                .op_mut()
                .affine_running
                .entry("gate-I".into())
                .or_default()
                .push(make_affine_dependent("h-parked", 0));

            probe(&mut secondary, "h-parked").await;
            probe(&mut secondary, "h-unknown").await;

            assert_eq!(
                hold_responses(&log),
                vec![
                    ("h-parked".to_string(), true),
                    ("h-unknown".to_string(), false),
                ],
                "a dependent parked in affine_running is HELD (it is owned, \
                 just deferred); an unknown hash is the positive denial"
            );
        })
        .await;
}

/// The responder must not leak liveness/election side effects: the
/// probe is accounting reconciliation only. After answering a denial,
/// the secondary's own bookkeeping is untouched (no requeue happens
/// HERE — the verdict and its consequences are the primary's).
#[tokio::test(flavor = "current_thread")]
async fn denial_has_no_local_side_effects() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            secondary.active_tasks_mut().insert("h-other".into(), 0);

            probe(&mut secondary, "h-unknown").await;

            assert_eq!(hold_responses(&log), vec![("h-unknown".to_string(), false)]);
            // The unrelated held task is untouched by answering a probe
            // about a different hash.
            assert!(
                secondary.op_mut().active_tasks.contains_key("h-other"),
                "answering a probe must not mutate own-worker bookkeeping"
            );
        })
        .await;
}
