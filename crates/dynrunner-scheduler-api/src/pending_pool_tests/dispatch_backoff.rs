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
fn next_expiry_is_strictly_future_then_re_polls_until_dispatched() {
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

    // Once the window expires the task is eligible again, but the wake
    // does NOT vanish: while the task remains queued-and-undispatched
    // (the recheck has not taken it), the accessor keeps surfacing a
    // BOUNDED re-poll wake so a missed dispatch is retried instead of
    // stranding (the #640 deadlock). The level persists until the task
    // is actually taken.
    std::thread::sleep(BASE + Duration::from_millis(10));
    let repoll = p
        .next_dispatch_backoff_expiry()
        .expect("expired-but-undispatched task must keep a re-poll wake");
    assert!(
        repoll > std::time::Instant::now(),
        "re-poll wake is bounded into the future (no hot-spin)"
    );
    // The task IS dispatch-eligible now (the gate is open).
    let item = p.pop_for_worker(1).expect("eligible after window");
    // Now that it has actually been taken, the level clears.
    let _ = item;
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "a taken task no longer needs a re-poll wake"
    );
}

/// Bug B — the #640 dispatch deadlock. Production sequence (asm-
/// tokenizer 25-min strand, in_flight=0, task_backoff arm count static
/// at 1): a task is requeued under backoff; its window expires; the
/// single post-expiry recheck MISSES (no idle worker / the only
/// candidate skipped) so the task is NOT taken; pre-#640 the secondary
/// re-poll would have re-triggered dispatch but #640 removed it and
/// there is no primary dispatch-sweep — so a wake that returned `None`
/// on expiry parked the op-loop arm on `pending()` forever and the
/// eligible-but-undispatched task stranded. The level-trigger fix makes
/// `next_dispatch_backoff_expiry` keep returning a BOUNDED re-poll wake
/// across the missed recheck until the task is actually taken.
#[test]
fn expired_task_re_polls_across_a_missed_recheck_until_dispatched() {
    // A fast re-poll cadence so the missed-recheck loop is observable
    // in a unit test, but still strictly > now (no hot-spin).
    const REPOLL: Duration = Duration::from_millis(20);
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.set_dispatch_repoll_interval(REPOLL);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // Requeue under backoff: streak 2, a real (future) window.
    let item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    p.requeue(item);
    assert!(
        p.next_dispatch_backoff_expiry().is_some(),
        "a backed-off task parks a wake"
    );

    // Advance past the stamp.
    std::thread::sleep(BASE + Duration::from_millis(10));

    // The wake fires (op-loop emits TasksAdded). Simulate the recheck
    // MISSING: NO `pop_for_worker` here — no worker took the task. The
    // accessor must NOT return None (that was the deadlock); it must
    // surface a bounded re-poll wake so the arm re-fires.
    let now = std::time::Instant::now();
    let w1 = p
        .next_dispatch_backoff_expiry()
        .expect("missed recheck must keep a re-poll wake, not strand");
    assert!(
        w1 > now,
        "re-poll wake must be strictly future (bounded, not now-raw): no hot-spin"
    );
    assert!(
        w1 <= now + REPOLL + Duration::from_millis(20),
        "re-poll wake must be BOUNDED to ~one interval, not the full backoff cap"
    );

    // Recheck misses AGAIN: still no worker. The level persists.
    std::thread::sleep(REPOLL + Duration::from_millis(5));
    let now2 = std::time::Instant::now();
    let w2 = p
        .next_dispatch_backoff_expiry()
        .expect("a still-undispatched task keeps re-polling");
    assert!(w2 > now2, "second re-poll wake is also bounded-future");

    // Finally a worker frees up and the task IS dispatched on this
    // tick — proving it was never stranded, only awaiting a worker.
    let taken = p
        .pop_for_worker(1)
        .expect("the eligible task dispatches once a worker is free");
    let _ = taken;

    // Once taken, the level clears — the arm parks (None) instead of
    // hot-spinning on an already-dispatched task.
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "a dispatched task must end the re-poll level"
    );
}

/// A fresh requeue while a task is in the expired-but-undispatched
/// re-poll state supersedes the re-poll: the task is re-stamped under a
/// new future window, so the wake is that future stamp (not an
/// immediate re-poll), and the task is hidden again until it expires.
#[test]
fn requeue_supersedes_an_expired_re_poll_state() {
    const REPOLL: Duration = Duration::from_millis(20);
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.set_dispatch_repoll_interval(REPOLL);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    let item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    p.requeue(item);
    std::thread::sleep(BASE + Duration::from_millis(10));

    // Observe the expiry → enters the re-poll level.
    assert!(
        p.next_dispatch_backoff_expiry().is_some(),
        "expired task is in re-poll level"
    );

    // The task is taken, fails, and is requeued again (streak 3): a new
    // future window. The wake must now be a strictly-future stamp well
    // beyond the short re-poll interval, and the task hidden again.
    let item = p.pop_for_worker(1).expect("eligible, taken");
    p.requeue(item);
    let now = std::time::Instant::now();
    let due = p
        .next_dispatch_backoff_expiry()
        .expect("re-stamped task parks a future wake");
    assert!(
        due > now + REPOLL,
        "a fresh requeue must supersede the short re-poll with its real backoff window"
    );
    assert!(
        p.pop_for_worker(1).is_none(),
        "the re-stamped task is hidden again until its new window expires"
    );
}

/// `clear_dispatch_backoff` (the settle-when-untracked seam) forgets a
/// task's backoff streak + its expired-but-undispatched re-poll state
/// WITHOUT the rest of the terminal bookkeeping — so a genuine terminal
/// whose hash holds no local residue still stops the Bug-B level-trigger
/// re-firing for the now-settled hash.
#[test]
fn clear_dispatch_backoff_stops_the_level_trigger() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // Drive the task into the expired-but-undispatched re-poll level.
    let item = p.pop_for_worker(1).expect("fresh item");
    p.requeue(item);
    let item = p.pop_for_worker(1).expect("free first re-entry");
    let id = item.task_id.clone();
    p.requeue(item);
    std::thread::sleep(BASE + Duration::from_millis(10));
    assert!(
        p.next_dispatch_backoff_expiry().is_some(),
        "the expired task is in the re-poll level (a wake is parked)"
    );

    // A genuine terminal settles the hash but finds no residue: clear the
    // backoff directly. The level-trigger must then stop firing.
    p.clear_dispatch_backoff(&id);
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "clearing the backoff for a settled hash must end the re-poll level"
    );
}

/// Bug B — the load-bearing production sequence (run_20260617_220927):
/// the pre-mesh BACKPRESSURE loop. The mesh-gate dispatches ~10-15s
/// before the mesh forms, so every dispatch attempt is backpressured
/// ("not ready to accept") → re-requeue (a NEW, growing backoff stamp)
/// → the backoff arm must re-fire → dispatch → backpressure again →
/// ... until a worker readies and accepts. The level-trigger must
/// CONVERGE through this repeated-backpressure window: a wake every
/// cycle (never parking on `pending()` while the task is eligible-but-
/// undispatched), spaced by the per-streak backoff (NOT a hot-spin —
/// the re-fire cadence rides the growing backoff window, not the poll
/// instant).
#[test]
fn backpressure_loop_converges_via_growing_backoff_no_hot_spin() {
    let mut p = pool_with(&["P"], &[]);
    p.set_dispatch_backoff_params(BASE, CAP);
    p.extend([t("P", "T", "", 10)]).expect("valid extend");

    // First dispatch attempt → backpressure → requeue (streak 1, FREE:
    // one bounce is not a spin, so it stays immediately dispatchable).
    let item = p.pop_for_worker(1).expect("fresh item dispatches");
    p.requeue(item);
    // The free re-entry is immediately poppable (no wake yet) — the
    // dispatch is re-attempted and backpressured again, so requeue: NOW
    // the brake engages (streak 2, the first real backoff window).
    let item = p
        .pop_for_worker(1)
        .expect("free first re-entry stays immediately dispatchable");
    p.requeue(item);

    // Simulate N repeated-backpressure cycles. Each cycle: the worker is
    // "not ready" so the dispatch is backpressured and the item is
    // re-requeued under a GROWING backoff. After each re-requeue the
    // backoff arm must see a strictly-future wake (the new window),
    // spaced by the streak's backoff — never a parked `pending()`, never
    // an instant re-fire.
    const CYCLES: usize = 4;
    let mut prev_window = Duration::ZERO;
    for cycle in 0..CYCLES {
        // The window for THIS streak (2,3,4,5 → 30,60,120,240ms).
        let before = std::time::Instant::now();
        let due = p
            .next_dispatch_backoff_expiry()
            .expect("an eligible-but-backed-off task must keep a wake every cycle");
        let window = due.duration_since(before);
        assert!(
            window > Duration::ZERO,
            "cycle {cycle}: the wake must be strictly future (no hot-spin)"
        );
        // The cadence rides the GROWING per-streak backoff, not the poll
        // instant: each cycle's window is >= the previous (until cap).
        assert!(
            window + Duration::from_millis(5) >= prev_window,
            "cycle {cycle}: re-fire spacing must follow the growing backoff, \
             got {window:?} after {prev_window:?}"
        );
        prev_window = window;

        // Wait out the window; the item is now eligible. The worker is
        // STILL not ready (pre-mesh): dispatch is backpressured →
        // re-requeue under the next, larger backoff.
        std::thread::sleep(window + Duration::from_millis(10));
        let item = p
            .pop_for_worker(1)
            .expect("eligible after its window — the dispatch is attempted");
        // Backpressure: the worker declined; requeue (next streak).
        p.requeue(item);
    }

    // The mesh has now formed: wait out the final window and a worker
    // ACCEPTS. The task converges to dispatched and the level clears.
    let due = p
        .next_dispatch_backoff_expiry()
        .expect("still parked on the final backoff window");
    std::thread::sleep(due.saturating_duration_since(std::time::Instant::now()) + Duration::from_millis(10));
    let accepted = p
        .pop_for_worker(1)
        .expect("once a worker is ready the backpressure-looped task dispatches");
    let _ = accepted;
    assert!(
        p.next_dispatch_backoff_expiry().is_none(),
        "a converged (taken) task must not keep a wake — the loop ends, no hot-spin"
    );
}
