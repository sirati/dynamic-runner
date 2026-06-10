//! App-level delivery confirmation for terminal-bearing primary-bound
//! sends (#352) — the blackholed-but-live-leg repro.
//!
//! The defect (production-observed; Half B of the half-joined fix): a
//! registered, live QUIC connection whose stream is blackholed —
//! `send.write_all` buffers locally and returns `Ok`; the route is not
//! pruned from `has_peer` until the 60s idle timeout (well past the
//! task window) — so `send_to_primary` "succeeds", the terminal-replay
//! buffer never engaged, and the terminal was silently lost (primary
//! phantom-busy; the phase barrier wedges).
//!
//! The fix under test: every terminal-bearing report is
//! `delivery_seq`-stamped at the `send_to_primary` chokepoint and stays
//! RETAINED (`AwaitingAck`) after a transport-`Ok` send; the primary's
//! ingest echoes a `TerminalAck { seq }` per landing, which is the ONLY
//! drop site; an un-acked send ages past `delivery_ack_timeout` and is
//! treated as no-route-equivalent — replayed (same seq) through the
//! EXISTING absorb→replay machinery.
//!
//! The `RecordingPeer` harness models the blackhole exactly: every send
//! is recorded and "succeeds" (membership healthy → no no-route), but
//! nothing ever answers — no ack arrives until the test injects one.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary_recording_with_membership,
};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use std::time::Duration;

/// Count the terminal frames for `hash` in the recorded wire log,
/// returning their `delivery_seq`s in send order.
fn sent_seqs_for_hash(
    log: &std::rc::Rc<
        std::cell::RefCell<Vec<DistributedMessage<super::super::test_helpers::TestId>>>,
    >,
    hash: &str,
) -> Vec<Option<u64>> {
    log.borrow()
        .iter()
        .filter(|m| m.task_hash() == Some(hash))
        .map(|m| m.delivery_seq())
        .collect()
}

/// THE blackhole repro: sends "succeed" but never deliver (no ack) →
/// the terminal is retained → the ack timeout elapses → the replay
/// fires with the SAME seq → a now-delivering route's ack lands → the
/// retention is released and replay stops. Pre-#352 the first
/// transport-`Ok` dropped the frame on the floor forever.
#[tokio::test(flavor = "current_thread")]
async fn blackholed_leg_unacked_terminal_replays_with_same_seq_until_acked() {
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
            // Sub-second ack deadline so the test drives the timeout edge
            // without wall-clock cost; production default is 15s (see
            // `DEFAULT_DELIVERY_ACK_TIMEOUT`).
            secondary.delivery_ack_timeout = Duration::from_millis(50);

            // The send SUCCEEDS (route up, membership healthy — the
            // blackholed leg looks exactly like this) …
            secondary
                .report_deferred_task_lost(0, "bh-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            let first = sent_seqs_for_hash(&log, "bh-hash");
            assert_eq!(
                first.len(),
                1,
                "exactly one terminal on the wire after the first send"
            );
            let seq = first[0].expect("the chokepoint must stamp delivery_seq");

            // … and the frame is RETAINED awaiting the ack, not dropped.
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "a transport-Ok terminal send must stay retained until acked"
            );
            assert!(secondary.pending_report_replays[0].state.is_awaiting_ack());

            // Inside the ack window a drain re-sends NOTHING (the ack may
            // simply still be in flight — no spurious replays).
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert_eq!(
                sent_seqs_for_hash(&log, "bh-hash").len(),
                1,
                "no replay before the ack timeout elapses"
            );

            // The blackhole: no ack ever arrives. Past the deadline the
            // drain treats the send as no-route-equivalent and replays —
            // with the SAME seq.
            tokio::time::sleep(Duration::from_millis(60)).await;
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            let after_replay = sent_seqs_for_hash(&log, "bh-hash");
            assert_eq!(
                after_replay.len(),
                2,
                "the un-acked terminal must replay after the ack timeout"
            );
            assert_eq!(
                after_replay[1],
                Some(seq),
                "the replay must carry the SAME delivery_seq (the primary's \
                 hash-keyed idempotence + per-landing ack key on it)"
            );
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "still exactly one retained entry across the replay"
            );

            // A now-delivering route: the primary's ack lands through the
            // real inbound arm and releases the retention.
            secondary
                .handle_inbound(
                    DistributedMessage::TerminalAck {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        seq,
                    },
                    &mut factory,
                )
                .await;
            assert!(
                secondary.pending_report_replays.is_empty(),
                "the TerminalAck is the (only) drop site"
            );

            // And the replay machinery goes quiet: another full timeout +
            // drain produces no further wire traffic.
            tokio::time::sleep(Duration::from_millis(60)).await;
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert_eq!(
                sent_seqs_for_hash(&log, "bh-hash").len(),
                2,
                "no replays after the ack confirmed delivery"
            );
        })
        .await;
}

/// A promptly-acked terminal never replays: the ack inside the window
/// releases the retention, and a later drain past the (former) deadline
/// re-sends nothing.
#[tokio::test(flavor = "current_thread")]
async fn acked_terminal_leaves_buffer_and_never_replays() {
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
            secondary.delivery_ack_timeout = Duration::from_millis(50);

            secondary
                .report_deferred_task_lost(0, "acked-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            let seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("stamped");

            // Healthy-path ack, well inside the window.
            secondary
                .handle_inbound(
                    DistributedMessage::TerminalAck {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        seq,
                    },
                    &mut factory,
                )
                .await;
            assert!(secondary.pending_report_replays.is_empty());

            // A duplicate ack (re-acked replay landing whose first ack
            // already cleared the entry) is a benign no-op.
            secondary.ack_delivery(seq);
            assert!(secondary.pending_report_replays.is_empty());

            tokio::time::sleep(Duration::from_millis(60)).await;
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert_eq!(
                sent_seqs_for_hash(&log, "acked-hash").len(),
                1,
                "an acked terminal must never replay"
            );
        })
        .await;
}

/// Non-terminal primary-bound sends are untouched by the ack machinery:
/// a successful capacity `TaskRequest` is neither stamped nor retained.
#[tokio::test(flavor = "current_thread")]
async fn successful_non_terminal_send_is_not_stamped_or_retained() {
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

            secondary.request_task_for_worker(0).await.unwrap();
            secondary.drain_egress().await;
            assert!(
                secondary.pending_report_replays.is_empty(),
                "a non-terminal send must not be retained on success"
            );
            let requests: Vec<_> = log
                .borrow()
                .iter()
                .filter(|m| matches!(m, DistributedMessage::TaskRequest { .. }))
                .cloned()
                .collect();
            assert_eq!(requests.len(), 1, "the TaskRequest reached the wire");
            assert_eq!(
                requests[0].delivery_seq(),
                None,
                "non-terminal frames are never delivery_seq-stamped"
            );
        })
        .await;
}

/// The ack machinery is delivery bookkeeping, NEVER liveness: a full
/// blackhole cycle (send Ok → ack timeout → replay, repeatedly) leaves
/// every failover-arming input untouched — `record_recv_failure` is
/// only ever fed by the (unchanged) no-route absorb, so the
/// primary-link health window stays pristine and no election arms.
#[tokio::test(flavor = "current_thread")]
async fn ack_timeout_replay_never_feeds_failover_arming() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;
            secondary.delivery_ack_timeout = Duration::from_millis(20);

            secondary
                .report_deferred_task_lost(0, "liveness-hash")
                .await
                .unwrap();
            // Three full timeout+replay cycles on the blackholed leg.
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(25)).await;
                secondary.drain_report_replays().await;
                secondary.drain_egress().await;
            }
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "the un-acked terminal stays retained across the cycles"
            );

            let op = secondary.op_mut();
            assert!(
                !op.primary_link.is_link_failing(),
                "an ack timeout must record NO primary-link failure \
                 (delivery bookkeeping, not liveness)"
            );
            assert!(
                !op.primary_link.should_arm_failover(),
                "failover arming inputs must be byte-identical to pre-#352 \
                 on the all-sends-succeed path"
            );
            assert!(
                matches!(op.election, super::super::election::ElectionState::Normal),
                "no election state change off the ack machinery"
            );
        })
        .await;
}

/// The permanent-failure detector (#366): each replay of one entry
/// bumps the tally carried ON the entry itself (`attempts`, updated in
/// place across replays — also the backoff-schedule driver), and the
/// ack drops the entry, tally and all.
#[tokio::test(flavor = "current_thread")]
async fn timed_out_replays_tally_per_entry_and_ack_clears_the_tally() {
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
            secondary.delivery_ack_timeout = Duration::from_millis(20);

            secondary
                .report_deferred_task_lost(0, "tally-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            let seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("stamped");
            assert_eq!(
                secondary.pending_report_replays[0].attempts, 0,
                "the first SEND is not a replay; the tally starts on the \
                 first timed-out re-send"
            );

            // Three timed-out replay rounds → tally 3 on the entry. The
            // sleeps track the exponential schedule (slots of 20, 40,
            // 80ms after replays 1, 2, 3).
            for (round, sleep_ms) in [(1u32, 30u64), (2, 30), (3, 50)] {
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                secondary.drain_report_replays().await;
                secondary.drain_egress().await;
                assert_eq!(
                    secondary.pending_report_replays[0].attempts, round,
                    "each timed-out replay must bump the entry's tally"
                );
            }
            assert_eq!(
                sent_seqs_for_hash(&log, "tally-hash").len(),
                4,
                "first send + 3 replays on the wire"
            );

            // The ack releases the retention — the tally lives on the
            // entry, so it goes with it (no side state).
            secondary
                .handle_inbound(
                    DistributedMessage::TerminalAck {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        seq,
                    },
                    &mut factory,
                )
                .await;
            assert!(secondary.pending_report_replays.is_empty());
        })
        .await;
}
