//! #586 — arm-priority discipline regression tests.
//!
//! The secondary `process_tasks` loop has 16 `select!` arms; the 20Hz
//! OOM-sweep timer (a `tokio::time::sleep_until(next_sweep_due)` future
//! that re-arms 50ms after each completed sweep) was — pre-#586 —
//! sharing uniform-random arm priority with `ARM_INBOX`. Under
//! steady-state report arrival rates (~7-9 inbox frames/sec on the
//! consumer's run_20260615_192743), the sweep timer fired ~18.5×/sec
//! and won ~67% of races against the data path, structurally starving
//! terminal-report reconciliation (30→60→120s unacked reports → fleet-
//! wide buffered-report-replay → idled secondaries).
//!
//! The fix: add `biased;` + place `ARM_INBOX` BEFORE `ARM_MEM_CHECK` in
//! source order. The tests below verify the `tokio::select! biased;`
//! semantics that the real loop now depends on.
//!
//! ── Tests ──
//!
//! - [`biased_select_inbox_wins_when_ready`] — the core invariant: when
//!   the inbox arm has a ready frame AND the OOM-sweep deadline has
//!   passed, the inbox arm wins every race. No probabilistic bound — a
//!   STRICT 100% win rate under continuous arrivals.
//!
//! - [`biased_select_oom_fires_when_inbox_empty`] — the dual: with the
//!   inbox empty, the OOM-sweep arm still fires at its 50ms cadence.
//!   biased+inbox-first does NOT starve the forensic timer when the
//!   inbox is idle (which is the steady state OUTSIDE the consumer's
//!   degraded window).
//!
//! - [`biased_select_inbox_wait_bounded_by_sweep_body_time`] — under
//!   continuous arrivals AND an active sweep, the inbox is delayed at
//!   most by the in-flight sweep arm body's wall-clock (since arm
//!   bodies run to completion before the next select! poll). This is
//!   the owner-refined T4: not a probabilistic share but a strict
//!   bound. The sweep body's spawn_blocking().await is the only way an
//!   inbox arrival can wait, and it is bounded.
//!
//! Tested IN ISOLATION — these don't drive the real loop (which would
//! pull in ~3000 lines of fixture). They test the `tokio::select!
//! biased;` macro semantics the real loop now relies on, with the
//! arm shape that mirrors the real loop's ARM_INBOX-vs-ARM_MEM_CHECK
//! pair.

use std::time::Duration;

use tokio::sync::mpsc;

/// The core invariant: under `biased; arm_inbox >>> sleep_until`, the
/// inbox arm wins EVERY race when it has a ready frame. Asserts ZERO
/// sweep-arm wins across a 50-iteration race where every iteration has
/// both arms ready.
#[tokio::test(flavor = "current_thread")]
async fn biased_select_inbox_wins_when_ready() {
    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
    // Pre-fill the inbox so the recv arm is ALWAYS ready at every
    // select! poll. 50 frames is plenty for the race window.
    for i in 0..50u32 {
        tx.send(i).expect("send");
    }
    drop(tx); // No more arrivals; loop exits when inbox empties.

    // OOM-sweep deadline already in the past — its sleep_until resolves
    // immediately, so on EVERY iteration the sweep arm is "ready" the
    // same instant the inbox arm is. Without `biased;` tokio would
    // pick one at random ~50/50; WITH `biased;` and inbox-first, inbox
    // must win every single race.
    let mut next_sweep_due =
        tokio::time::Instant::now() - Duration::from_millis(10);
    let sweep_interval = Duration::from_millis(50);

    let mut inbox_wins = 0usize;
    let mut sweep_wins = 0usize;
    loop {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    Some(_) => inbox_wins += 1,
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(next_sweep_due) => {
                sweep_wins += 1;
                next_sweep_due = tokio::time::Instant::now() + sweep_interval;
            }
        }
    }

    assert_eq!(inbox_wins, 50, "inbox should win every iteration");
    assert_eq!(
        sweep_wins, 0,
        "sweep must NEVER win when inbox is ready under `biased;` + inbox-first"
    );
}

/// The dual invariant: with the inbox idle, the OOM-sweep arm fires at
/// its expected cadence. biased+inbox-first does not silence the
/// forensic timer; it only prevents it from beating a ready inbox.
#[tokio::test(flavor = "current_thread")]
async fn biased_select_oom_fires_when_inbox_empty() {
    let (_tx, mut rx) = mpsc::unbounded_channel::<u32>();
    // Inbox empty for the whole window — recv() pends forever. Only the
    // sweep arm has a path to firing.
    let mut next_sweep_due = tokio::time::Instant::now();
    let sweep_interval = Duration::from_millis(50);
    let window = Duration::from_millis(310);

    let start = tokio::time::Instant::now();
    let mut sweeps = 0usize;
    while tokio::time::Instant::now().duration_since(start) < window {
        tokio::select! {
            biased;
            _msg = rx.recv() => {
                // Channel closed (no senders alive after drop in this
                // test setup would yield None) — not expected in the
                // window. The single `_tx` we kept ensures recv() pends.
                unreachable!("inbox is idle");
            }
            _ = tokio::time::sleep_until(next_sweep_due) => {
                sweeps += 1;
                next_sweep_due = tokio::time::Instant::now() + sweep_interval;
            }
        }
    }

    // 310ms window, 50ms cadence with await-before-resleep ≈ 6 sweeps.
    // Scheduler jitter could shift by ±1; bound generously.
    assert!(
        (5..=8).contains(&sweeps),
        "sweep cadence under empty inbox should be ~6 fires/300ms, got {sweeps}"
    );
}

/// Owner-refined T4: under continuous arrivals AND an in-flight sweep,
/// the inbox arm's wait-time is bounded by the sweep arm body's
/// wall-clock. The arm-body runs to completion before the next select!
/// poll (tokio select! does not pre-empt arm bodies), so an inbox
/// arrival that lands DURING a sweep body is delayed at most until that
/// body completes — never the next sweep, never two sweeps.
///
/// This test simulates a slow sweep body (50ms of work) and a
/// continuous inbox stream; asserts every inbox frame is consumed
/// within `sweep_body_time + epsilon` of arrival.
#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn biased_select_inbox_wait_bounded_by_sweep_body_time() {
    let (tx, mut rx) =
        mpsc::unbounded_channel::<tokio::time::Instant>();

    // Producer task: emit a frame every 5ms for 400ms. Each frame
    // carries the instant it was queued so the consumer can measure
    // wait-time per arrival.
    let producer = tokio::spawn(async move {
        for _ in 0..80usize {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if tx.send(tokio::time::Instant::now()).is_err() {
                break;
            }
        }
    });

    // Simulated sweep body cost: 30ms of off-thread work (mirrors the
    // real loop's spawn_blocking().await for cgroup reads on ~14
    // workers).
    let sweep_body_cost = Duration::from_millis(30);
    let sweep_interval = Duration::from_millis(50);
    let mut next_sweep_due = tokio::time::Instant::now();

    let mut max_wait = Duration::ZERO;
    let mut frames = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    Some(queued_at) => {
                        let wait = queued_at.elapsed();
                        if wait > max_wait {
                            max_wait = wait;
                        }
                        frames += 1;
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(next_sweep_due) => {
                // Sweep body runs to completion BEFORE the next
                // select! poll — exactly as the real loop's
                // spawn_blocking().await behaves.
                tokio::time::sleep(sweep_body_cost).await;
                next_sweep_due = tokio::time::Instant::now() + sweep_interval;
            }
        }
    }

    let _ = producer.await;

    // Each frame should have waited NO MORE than sweep_body_cost + a
    // small jitter (scheduling overhead under current_thread). The
    // PRE-#586 unbiased shape would have allowed a frame to wait
    // through MULTIPLE sweeps stacking up (since uniform random had
    // sweep winning ~67% of races vs inbox); biased + inbox-first
    // bounds the wait to ONE in-flight body at most.
    let bound = sweep_body_cost + Duration::from_millis(20);
    assert!(
        max_wait <= bound,
        "inbox wait {max_wait:?} exceeded sweep_body+jitter bound {bound:?} \
         (#586 regression: biased + inbox-first must bound inbox wait \
         to the in-flight sweep body's wall-clock)"
    );
    assert!(
        frames >= 50,
        "expected >=50 frames consumed in the 500ms window, got {frames}"
    );
}
