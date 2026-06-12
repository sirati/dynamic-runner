//! Per-task re-dispatch backoff (`pending_pool/backoff.rs`):
//! `requeue`/`reinject` bump a streak whose FIRST re-entry is free and
//! whose subsequent re-entries stamp an exponential not-before window
//! the dispatch read paths honour; terminals clear the streak; and
//! `next_dispatch_backoff_expiry` exposes only strictly-future wakes.
//!
//! Production shape pinned here (asm-tokenizer run_20260612_095601):
//! a task whose every dispatch bounced was requeued and re-assigned
//! at memory speed — 27k+ assignments inside single-second windows —
//! because a requeued item was always immediately dispatch-eligible.

use std::time::Duration;

use super::{pool_with, t};

/// A test-scale backoff: base 30ms doubling to a 240ms cap, so a
/// whole exponential ladder fits inside a fast unit test.
const BASE: Duration = Duration::from_millis(30);
const CAP: Duration = Duration::from_millis(240);

#[test]
fn first_requeue_is_free_second_is_hidden_until_backoff_expires() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // First re-entry: one bounce is not a spin — the item stays
    // immediately dispatchable (dead-host requeues and drain-edge
    // retry passes rely on this).
    let item = p.pop_for_worker(1).expect("fresh item dispatches");
    p.requeue(item);
    let item = p
        .pop_for_worker(1)
        .expect("first requeue must stay immediately dispatchable");

    // Second consecutive re-entry: the brake engages. Queued (the
    // phase machine still counts it) but hidden from both read paths.
    p.requeue(item);
    assert_eq!(p.len(), 1, "requeued item is queued");
    assert!(
        p.pop_for_worker(1).is_none(),
        "backed-off item must not pop"
    );
    assert!(
        p.view_for_worker(1, None).is_empty(),
        "backed-off item must not appear in a worker view"
    );

    // After the window expires it dispatches again.
    std::thread::sleep(BASE + Duration::from_millis(10));
    assert!(
        !p.view_for_worker(1, None).is_empty(),
        "expired backoff must make the item visible again"
    );
    assert!(
        p.pop_for_worker(1).is_some(),
        "expired backoff must make the item poppable again"
    );
}

#[test]
fn reinject_shares_the_same_streak() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // Attempt 1 bounces (requeue, streak 1 — free), attempt 2 fails
    // pending retry and the drain-edge bucket reinjects (streak 2 —
    // braked): a counted retry after a bounce must wait its window.
    let item = p.pop_for_worker(1).expect("fresh item dispatches");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    p.on_item_failed_pending_retry(&item.phase_id.clone(), &item.task_id.clone());
    p.reinject(item);

    assert!(
        p.pop_for_worker(1).is_none(),
        "a repeat re-entry via reinject must wait out its backoff window"
    );
    std::thread::sleep(BASE + Duration::from_millis(10));
    assert!(
        p.pop_for_worker(1).is_some(),
        "the retry dispatches once its window expires"
    );
}

#[test]
fn backoff_doubles_per_requeue_and_saturates_at_cap() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // Streaks 1..=6 give 0/30/60/120/240/240ms. The free first
    // re-entry exposes NO wake; from streak 2 the strictly-future
    // expiry stamp tracks the doubling ladder.
    let mut item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "the free first re-entry must not park a wake"
    );
    item = p.pop_for_worker(1).expect("free first re-entry");

    let mut expected = vec![30u64, 60, 120, 240, 240];
    expected.reverse();
    while let Some(want_ms) = expected.pop() {
        let before = std::time::Instant::now();
        p.requeue(item);
        let due = p
            .next_dispatch_backoff_expiry()
            .expect("a queued backed-off item must expose a wake");
        // `due = stamp_instant + delay` with `stamp_instant >= before`,
        // so the measured window is `delay` plus a tiny stamping skew.
        let window = due.duration_since(before);
        let want = Duration::from_millis(want_ms);
        assert!(
            window >= want && window <= want + Duration::from_millis(20),
            "streak window must be ~{want_ms}ms, got {window:?}"
        );
        // Wait it out and take the item again for the next round.
        std::thread::sleep(window + Duration::from_millis(10));
        item = p.pop_for_worker(1).expect("eligible again after window");
    }
}

#[test]
fn an_eligible_sibling_behind_a_backed_off_front_item_still_dispatches() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(Duration::from_secs(60), Duration::from_secs(60));
    p.extend([t("P", "T", "", 10), t("P", "T", "", 20)])
        .expect("valid extend");

    // Take + requeue the first item TWICE (the second re-entry stamps
    // the long backoff): `requeue` pushes it to the FRONT of the
    // bucket both times, so the hidden item now heads the bucket.
    let first = p.pop_for_worker(1).expect("first item");
    let first_id = first.task_id.clone();
    p.requeue(first);
    let first = p.pop_for_worker(1).expect("free first re-entry");
    assert_eq!(first.task_id, first_id);
    p.requeue(first);

    // The sibling behind it must still dispatch.
    let next = p.pop_for_worker(1).expect("sibling dispatches");
    assert_ne!(
        next.task_id, first_id,
        "the backed-off front item must not block (or be) the dispatch"
    );
    assert!(
        p.pop_for_worker(1).is_none(),
        "only the backed-off item remains; it must stay hidden"
    );
}

#[test]
fn terminal_clears_the_streak() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // Two requeues grow the streak to 2 (a third re-entry would wait
    // 60ms).
    let item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    let phase = item.phase_id.clone();
    let id = item.task_id.clone();
    p.requeue(item);
    std::thread::sleep(BASE + Duration::from_millis(10));
    let item = p.pop_for_worker(1).expect("eligible again");

    // Terminal success: the streak resets. A FRESH lifecycle of the
    // same task id (reinject after terminal) starts back at the free
    // first re-entry, not at streak 3.
    p.on_item_finished(&phase, Some(&id));
    p.reinject(item);
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "post-terminal streak must restart at the free first re-entry"
    );
    assert!(
        p.pop_for_worker(1).is_some(),
        "the post-terminal reinject is immediately dispatchable"
    );
}

#[test]
fn next_expiry_is_strictly_future_and_empties_when_nothing_parked() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "no backoff parked on a fresh pool"
    );

    let item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    p.requeue(item);
    let due = p.next_dispatch_backoff_expiry().expect("wake parked");
    assert!(
        due > std::time::Instant::now() - Duration::from_millis(1),
        "wake must not be in the past"
    );

    // Once the window expires the wake disappears (the item is simply
    // eligible) — a loop parked on this accessor can never hot-fire.
    std::thread::sleep(BASE + Duration::from_millis(10));
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "an expired stamp must not surface as a wake"
    );
    assert!(
        p.pop_for_worker(1).is_some(),
        "and the item is dispatch-eligible"
    );
}
