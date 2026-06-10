//! Per-report replay backoff — the self-inflicted replay-flood repro
//! (asm-dataset test-env, run_20260610_221140).
//!
//! Production storm: a secondary's mesh leg to the primary blackholed,
//! its confirmable terminal report aged past `delivery_ack_timeout` and
//! was correctly treated as no-route-equivalent — but the drain then
//! re-sent the SAME `delivery_seq` on EVERY operational-loop iteration
//! (~61 replays/second, 19,437 "drain re-sent" INFO lines in ~5
//! minutes) because a `NoRoute`-retained entry carried no schedule: it
//! was due on every drain pass.
//!
//! The contract under test: replay scheduling lives ON each retained
//! entry (`attempts` + `next_due`), replays follow an exponential
//! schedule (`ack_timeout` → 2× → 4× … capped at
//! [`REPORT_REPLAY_BACKOFF_CAP`]), one seq is re-sent at most once per
//! drain pass, an ack stops everything, and the route-restored edge
//! (`drain_report_replays_now`) retries promptly without waiting out
//! the backoff slot.

#![cfg(test)]

use super::super::resource::{
    DEFAULT_DELIVERY_ACK_TIMEOUT, REPORT_REPLAY_BACKOFF_CAP, replay_backoff_delay,
};
use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary_recording_with_membership,
};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use std::time::Duration;

/// THE storm repro (RED pre-fix): one confirmable report retained on an
/// unreachable primary + a spinning operational loop (modelled as a
/// tight drain loop, the per-iteration cadence the production loop
/// drove) must replay on the backoff schedule, NOT once per drain pass.
///
/// Pre-fix a `NoRoute` entry was due on EVERY pass: over a 400ms spin
/// at ~2ms per pass the drain re-sent ~200 times (the production
/// ~61/s shape). Post-fix the schedule allows the initial due-now
/// re-send plus replays at `ack_timeout`, 2×, 4×, … — at most 5 in
/// this window.
#[tokio::test(flavor = "current_thread")]
async fn unreachable_primary_replays_bounded_by_backoff_not_drain_cadence() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;
            secondary.delivery_ack_timeout = Duration::from_millis(40);

            // Unreachable destination: the primary is absent from the
            // membership view, so every send no-routes (the storm's
            // steady state — the ACK can never come).
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();
            secondary
                .report_deferred_task_lost(0, "storm-hash")
                .await
                .unwrap();
            assert_eq!(secondary.pending_report_replays.len(), 1);

            // The spinning operational loop: ~2ms per iteration for
            // ~400ms, draining every pass exactly as the per-tick drain
            // call did in production.
            let start = std::time::Instant::now();
            let mut replays = 0usize;
            while start.elapsed() < Duration::from_millis(400) {
                replays += secondary.drain_report_replays().await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }

            // Backoff schedule bound (ack_timeout 40ms, spin 400ms):
            // due-now initial re-send, then 40, 80, 160, 320ms → ≤ 5
            // replays. Pre-fix: one per pass ≈ 200 (the storm).
            assert!(
                replays <= 5,
                "an unACKed report on an unreachable route must replay on \
                 the exponential backoff schedule, not once per drain pass \
                 (the ~61/s production storm); got {replays} replays in 400ms"
            );
            // And it is still retained (never dropped) for when the
            // route comes back.
            assert_eq!(secondary.pending_report_replays.len(), 1);
        })
        .await;
}

/// Blackholed-but-live leg (sends succeed, no ack ever): replays are
/// spaced by the DOUBLING schedule, not the flat ack-timeout — and every
/// replay carries the IDENTICAL `delivery_seq` (the idempotency-key
/// contract: never re-stamp).
#[tokio::test(flavor = "current_thread")]
async fn blackholed_leg_replays_double_their_spacing_and_keep_the_seq() {
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
            secondary.delivery_ack_timeout = Duration::from_millis(40);

            secondary
                .report_deferred_task_lost(0, "double-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            let seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("stamped");

            // Spin a tight drain loop for ~400ms. Schedule: first send at
            // t=0 (already on the wire), replay 1 at 40ms, replay 2 at
            // +80ms (=120), replay 3 at +160ms (=280) → 4 frames total;
            // a flat 40ms cadence would have produced ~10.
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_millis(400) {
                secondary.drain_report_replays().await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            secondary.drain_egress().await;

            let sent: Vec<Option<u64>> = log
                .borrow()
                .iter()
                .filter(|m| m.task_hash() == Some("double-hash"))
                .map(|m| m.delivery_seq())
                .collect();
            assert!(
                (3..=5).contains(&sent.len()),
                "doubling schedule over 400ms at ack_timeout=40ms must \
                 produce ~4 wire sends (first + replays at 40/120/280ms); \
                 got {}",
                sent.len()
            );
            assert!(
                sent.iter().all(|s| *s == Some(seq)),
                "every replay must carry the IDENTICAL delivery_seq \
                 (idempotency key — never re-stamp); got {sent:?}"
            );
        })
        .await;
}

/// An ACK arriving mid-schedule stops all replays: nothing further hits
/// the wire no matter how long the loop keeps spinning.
#[tokio::test(flavor = "current_thread")]
async fn ack_arrival_stops_scheduled_replays() {
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
            secondary.delivery_ack_timeout = Duration::from_millis(30);

            secondary
                .report_deferred_task_lost(0, "ackstop-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            let seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("stamped");

            // Let one replay fire, then ack.
            tokio::time::sleep(Duration::from_millis(40)).await;
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
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
            let on_wire = log
                .borrow()
                .iter()
                .filter(|m| m.task_hash() == Some("ackstop-hash"))
                .count();

            // Keep spinning well past several backoff slots: no further
            // sends.
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_millis(150) {
                secondary.drain_report_replays().await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            secondary.drain_egress().await;
            assert_eq!(
                log.borrow()
                    .iter()
                    .filter(|m| m.task_hash() == Some("ackstop-hash"))
                    .count(),
                on_wire,
                "an acked report must never replay again"
            );
        })
        .await;
}

/// Route-restored prompt retry: `drain_report_replays_now` (the
/// primary-link-recovery edge's drain) re-sends a retained report
/// IMMEDIATELY, ignoring its next-backoff slot — and the re-send still
/// carries the same seq.
#[tokio::test(flavor = "current_thread")]
async fn route_restored_drain_retries_promptly_ignoring_backoff_slot() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;
            secondary.delivery_ack_timeout = Duration::from_secs(15);

            // Route down; retain; burn the due-now slot with one failed
            // drain pass so the entry sits deep inside a 15s backoff slot.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();
            secondary
                .report_deferred_task_lost(0, "restore-hash")
                .await
                .unwrap();
            secondary.drain_report_replays().await;
            assert!(
                log.borrow().is_empty(),
                "nothing reaches the wire while the route is down"
            );
            let seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("stamped");

            // Route back up. The schedule-respecting drain must NOT send
            // (the next slot is ~15s away) …
            membership
                .borrow_mut()
                .push(dynrunner_protocol_primary_secondary::PeerId::from("setup"));
            secondary.publish_membership();
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert!(
                log.borrow().is_empty(),
                "the schedule-respecting drain honours the backoff slot"
            );

            // … but the route-restored edge retries promptly.
            secondary.drain_report_replays_now().await;
            secondary.drain_egress().await;
            let sent: Vec<Option<u64>> = log
                .borrow()
                .iter()
                .filter(|m| m.task_hash() == Some("restore-hash"))
                .map(|m| m.delivery_seq())
                .collect();
            assert_eq!(
                sent,
                vec![Some(seq)],
                "the route-restored drain must re-send promptly (not wait \
                 out the backoff slot), once, with the same seq"
            );
        })
        .await;
}

/// Coalescing: two retained entries carrying the SAME seq (the
/// defensive duplicate case) produce exactly ONE wire send in a single
/// drain pass — one in-flight replay per seq.
#[tokio::test(flavor = "current_thread")]
async fn same_seq_is_resent_at_most_once_per_drain_pass() {
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
            secondary.delivery_ack_timeout = Duration::from_millis(30);

            secondary
                .report_deferred_task_lost(0, "dupe-hash")
                .await
                .unwrap();
            secondary.drain_egress().await;
            assert_eq!(log.borrow().len(), 1, "first send on the wire");

            // Inject a duplicate retained entry with the same frame (and
            // thus the same seq), both overdue.
            let dupe = secondary.pending_report_replays[0].frame.clone();
            let overdue = std::time::Instant::now();
            secondary.pending_report_replays[0].next_due = overdue;
            let state = super::super::resource::RetainedSendState::NoRoute;
            secondary
                .pending_report_replays
                .push(super::super::resource::RetainedReport {
                    frame: dupe,
                    state,
                    attempts: 0,
                    next_due: overdue,
                    first_retained_at: overdue,
                });

            let resent = secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert_eq!(
                resent, 1,
                "one drain pass must re-send a given seq at most once \
                 (coalescing), even with duplicate retained entries"
            );
            assert_eq!(
                log.borrow()
                    .iter()
                    .filter(|m| m.task_hash() == Some("dupe-hash"))
                    .count(),
                2,
                "first send + exactly one coalesced replay on the wire"
            );
        })
        .await;
}

/// The schedule itself: `ack_timeout` → 2× → 4× …, capped at
/// [`REPORT_REPLAY_BACKOFF_CAP`], and overflow-safe for absurd attempt
/// counts.
#[test]
fn backoff_schedule_doubles_from_ack_timeout_and_caps() {
    let t = DEFAULT_DELIVERY_ACK_TIMEOUT; // 15s
    assert_eq!(replay_backoff_delay(t, 1), t);
    assert_eq!(replay_backoff_delay(t, 2), t * 2);
    assert_eq!(replay_backoff_delay(t, 3), REPORT_REPLAY_BACKOFF_CAP);
    assert_eq!(replay_backoff_delay(t, 4), REPORT_REPLAY_BACKOFF_CAP);
    // Overflow-safe far past the cap threshold.
    assert_eq!(replay_backoff_delay(t, 200), REPORT_REPLAY_BACKOFF_CAP);
    // A sub-second test-sized timeout doubles cleanly below the cap.
    let small = Duration::from_millis(40);
    assert_eq!(replay_backoff_delay(small, 1), small);
    assert_eq!(replay_backoff_delay(small, 2), small * 2);
    assert_eq!(replay_backoff_delay(small, 3), small * 4);
}

/// The wake deadline the operational loop parks on: `None` when the
/// buffer is empty, otherwise the EARLIEST `next_due` across entries —
/// the persistent (entry-stored) deadline that makes the replay select
/// arm fire on time no matter how often sibling arms win iterations.
#[tokio::test(flavor = "current_thread")]
async fn next_report_replay_due_is_min_over_entries() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;
            secondary.delivery_ack_timeout = Duration::from_millis(50);

            assert!(
                secondary.next_report_replay_due().is_none(),
                "an empty buffer parks the wake arm (no deadline)"
            );

            // Route down → a due-now NoRoute entry.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();
            secondary
                .report_deferred_task_lost(0, "due-hash-1")
                .await
                .unwrap();
            let first_due = secondary
                .next_report_replay_due()
                .expect("a retained entry must publish a wake deadline");
            assert!(
                first_due <= std::time::Instant::now(),
                "a fresh NoRoute retention is due immediately"
            );

            // A second, later entry must not move the earliest deadline.
            secondary
                .report_deferred_task_lost(0, "due-hash-2")
                .await
                .unwrap();
            assert_eq!(
                secondary.next_report_replay_due(),
                Some(
                    secondary
                        .pending_report_replays
                        .iter()
                        .map(|e| e.next_due)
                        .min()
                        .unwrap()
                ),
                "the wake deadline is the minimum next_due over all entries"
            );
        })
        .await;
}
