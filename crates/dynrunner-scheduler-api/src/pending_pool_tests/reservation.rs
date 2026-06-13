//! Bring-up FORMATION-WINDOW reservation overlay tests (#494): the pool
//! tags each queued task with the member it is reserved for, scopes a
//! member's visibility to its share while the window is open, folds a
//! DEAD member's share onto the survivors, and self-closes when the last
//! reserved task drains or redistributes.

use super::{phase, pool_with, t};

/// Build a task in phase `P`, free-pool, with an explicit stable
/// `task_id` so the test can construct the reservation plan by identity.
fn task(id: &str) -> dynrunner_core::TaskInfo<()> {
    let mut item = t("P", "T", "", 10);
    item.task_id = id.into();
    item
}

/// The `(phase_id, task_id)` reservation key for a `P`-phase task id.
fn key(id: &str) -> super::super::ReservationKey {
    (phase("P"), id.into())
}

/// A `holder_confirmed` closure where NO holder has confirmed — the
/// strict formation case where each unconfirmed member's share is
/// protected.
fn none_confirmed(_: &str) -> bool {
    false
}

/// With NO window open the overlay admits everyone — the local
/// single-node manager / steady-state path is wholly unaffected.
#[test]
fn closed_window_admits_every_member() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b")]).expect("valid extend");
    assert!(!p.reservation_active());
    let a = task("a");
    assert!(p.reservation_admits("sec-0", &a, &none_confirmed));
    assert!(p.reservation_admits("sec-9", &a, &none_confirmed));
}

/// While the holders are UNCONFIRMED, an open window scopes a reserved
/// task to its HOLDER only (protecting the still-forming member's share);
/// an UNRESERVED task (omitted from the plan — a streamed/late task)
/// admits everyone even while the window is open.
#[test]
fn open_window_scopes_reserved_to_unconfirmed_holder() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b"), task("streamed")])
        .expect("valid extend");
    // Reserve a→sec-0, b→sec-1; leave `streamed` unreserved.
    p.open_reservation([(key("a"), "sec-0".into()), (key("b"), "sec-1".into())]);
    assert!(p.reservation_active());

    let a = task("a");
    let b = task("b");
    let streamed = task("streamed");

    // Neither holder confirmed → each share is protected from the other.
    // a is sec-0's only.
    assert!(p.reservation_admits("sec-0", &a, &none_confirmed));
    assert!(!p.reservation_admits("sec-1", &a, &none_confirmed));
    // b is sec-1's only.
    assert!(p.reservation_admits("sec-1", &b, &none_confirmed));
    assert!(!p.reservation_admits("sec-0", &b, &none_confirmed));
    // streamed (unreserved) admits anyone.
    assert!(p.reservation_admits("sec-0", &streamed, &none_confirmed));
    assert!(p.reservation_admits("sec-7", &streamed, &none_confirmed));
}

/// A CONFIRMED holder's reserved overflow is FREE for the formed fleet —
/// a confirmed holder had its dibs, so any member (a co-confirmed peer,
/// OR a mid-run joiner) may take it. This is what keeps a single
/// bring-up member's whole-pool reservation from starving a mid-run
/// joiner (the `midrun_join` regression).
#[test]
fn confirmed_holders_overflow_is_free_for_the_fleet() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b")]).expect("valid extend");
    p.open_reservation([(key("a"), "sec-0".into()), (key("b"), "sec-1".into())]);

    let a = task("a");
    let b = task("b");

    // sec-0 confirmed, sec-1 still forming.
    let confirmed_sec0 = |h: &str| h == "sec-0";
    // a (sec-0's, holder confirmed) → free for a joiner.
    assert!(p.reservation_admits("sec-0", &a, &confirmed_sec0));
    assert!(p.reservation_admits("joiner", &a, &confirmed_sec0));
    // b (sec-1's, holder UNCONFIRMED) → still protected: only sec-1 sees it.
    assert!(p.reservation_admits("sec-1", &b, &confirmed_sec0));
    assert!(!p.reservation_admits("sec-0", &b, &confirmed_sec0));
    assert!(!p.reservation_admits("joiner", &b, &confirmed_sec0));
}

/// `open_reservation` with an EMPTY plan does not open a window (nothing
/// to scope).
#[test]
fn empty_plan_does_not_open_window() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a")]).expect("valid extend");
    p.open_reservation(std::iter::empty());
    assert!(!p.reservation_active());
}

/// Taking a reserved task drops its holder; when the LAST reserved task
/// drains the window self-closes (no explicit close call).
#[test]
fn drain_self_closes_window() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b")]).expect("valid extend");
    p.open_reservation([(key("a"), "sec-0".into()), (key("b"), "sec-1".into())]);
    assert!(p.reservation_active());

    // sec-0 takes its reserved `a` — view scoping is the coordinator's
    // job, so here we take directly. `pop_for_worker` pulls FIFO; with
    // both reserved, the first pop drains one, the second the other.
    let _first = p.pop_for_worker(0).expect("a dispatchable");
    assert!(
        p.reservation_active(),
        "still one reserved task held; window stays open"
    );
    let _second = p.pop_for_worker(1).expect("b dispatchable");
    assert!(
        !p.reservation_active(),
        "last reserved task drained — window self-closes"
    );
}

/// A DEAD member's still-queued reserved share folds round-robin onto the
/// supplied fallback survivors.
#[test]
fn redistribute_folds_dead_share_onto_survivors() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b"), task("c"), task("d")])
        .expect("valid extend");
    // Reserve all four to the soon-dead sec-0.
    p.open_reservation([
        (key("a"), "sec-0".into()),
        (key("b"), "sec-0".into()),
        (key("c"), "sec-0".into()),
        (key("d"), "sec-0".into()),
    ]);

    // sec-0 dies; fold its share onto sec-1, sec-2 (round-robin).
    p.redistribute_member("sec-0", &["sec-1".into(), "sec-2".into()]);
    assert!(p.reservation_active(), "the share moved, not vanished");

    let (a, b, c, d) = (task("a"), task("b"), task("c"), task("d"));
    // Round-robin over [sec-1, sec-2] in queued order a,b,c,d:
    // a→sec-1, b→sec-2, c→sec-1, d→sec-2. No task is still sec-0's.
    // Holders treated as unconfirmed so the strict-holder routing shows.
    assert!(
        p.reservation_admits("sec-1", &a, &none_confirmed)
            && !p.reservation_admits("sec-2", &a, &none_confirmed)
    );
    assert!(
        p.reservation_admits("sec-2", &b, &none_confirmed)
            && !p.reservation_admits("sec-1", &b, &none_confirmed)
    );
    assert!(
        p.reservation_admits("sec-1", &c, &none_confirmed)
            && !p.reservation_admits("sec-2", &c, &none_confirmed)
    );
    assert!(
        p.reservation_admits("sec-2", &d, &none_confirmed)
            && !p.reservation_admits("sec-1", &d, &none_confirmed)
    );
    // sec-0 (dead) sees none of them.
    assert!(!p.reservation_admits("sec-0", &a, &none_confirmed));
}

/// CASCADE: two members die in turn; the second death folds its
/// (already-redistributed-onto) share onto the then-current survivors.
#[test]
fn redistribute_is_cascade_safe() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b")]).expect("valid extend");
    p.open_reservation([(key("a"), "sec-0".into()), (key("b"), "sec-1".into())]);

    // sec-0 dies → a folds onto the survivors [sec-1, sec-2].
    p.redistribute_member("sec-0", &["sec-1".into(), "sec-2".into()]);
    let a = task("a");
    assert!(
        p.reservation_admits("sec-1", &a, &none_confirmed),
        "a went to sec-1"
    );

    // sec-1 dies → both a (now sec-1's) and b (sec-1's) fold onto [sec-2].
    p.redistribute_member("sec-1", &["sec-2".into()]);
    let b = task("b");
    assert!(
        p.reservation_admits("sec-2", &a, &none_confirmed)
            && p.reservation_admits("sec-2", &b, &none_confirmed),
        "both fold onto the lone survivor sec-2"
    );
    assert!(
        !p.reservation_admits("sec-1", &a, &none_confirmed)
            && !p.reservation_admits("sec-1", &b, &none_confirmed)
    );
}

/// LONE-SURVIVOR edge: redistribute with an EMPTY fallback list unreserves
/// the dead member's share (admits everyone) rather than stranding it on
/// a member that can no longer take it. With nothing left reserved the
/// window self-closes.
#[test]
fn redistribute_with_no_survivors_unreserves() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a")]).expect("valid extend");
    p.open_reservation([(key("a"), "sec-0".into())]);

    p.redistribute_member("sec-0", &[]);
    let a = task("a");
    assert!(
        p.reservation_admits("sec-7", &a, &none_confirmed),
        "no survivor to hold it — it admits everyone, never stranded"
    );
    assert!(
        !p.reservation_active(),
        "nothing left reserved — window self-closes"
    );
}

/// A DRAINED reserved task (already popped by its confirmed holder) is
/// gone from the queue, so a later death of a DIFFERENT member does not
/// resurrect or mis-route it — redistribute only touches still-queued
/// reservations.
#[test]
fn redistribute_ignores_already_drained_tasks() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([task("a"), task("b")]).expect("valid extend");
    p.open_reservation([(key("a"), "sec-0".into()), (key("b"), "sec-1".into())]);

    // sec-0 confirmed and drained its `a`.
    let _ = p.pop_for_worker(0).expect("a dispatchable");
    assert!(p.reservation_active(), "b still reserved to sec-1");

    // sec-1 dies → only its still-queued `b` folds onto [sec-2]; the
    // already-drained `a` is untouched.
    p.redistribute_member("sec-1", &["sec-2".into()]);
    let b = task("b");
    assert!(
        p.reservation_admits("sec-2", &b, &none_confirmed)
            && !p.reservation_admits("sec-1", &b, &none_confirmed)
    );
}
