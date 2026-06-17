//! Inbound-filter seam tests: the verdict an installed opaque closure
//! returns (Drop / Bounce / Accept), and the no-filter pass-through
//! default. Drive `process_inbound` (async path, can send for Bounce)
//! and `process_inbound_sync` (no send) so both seams stay covered.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use super::*;
use crate::messages::DistributedMessage;
use crate::relay::router::Verdict;
use crate::relay::testing::new_log;

// ── no filter installed → pure pass-through (Accept-all) ──

#[test]
fn no_filter_delivers_unchanged() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    let outcome = router.process_inbound(
        keepalive("a"),
        &mut conns,
        clocks_at(Instant::now(), 1.0),
    );
    match outcome {
        InboundOutcome::Deliver { msg, .. } => assert_eq!(msg.sender_id(), "a"),
        other => panic!("expected pass-through Deliver: {other:?}"),
    }
    // Nothing dispatched: a no-op filter never touches the send path.
    assert!(log.borrow().is_empty());
}

// ── Drop → discarded, not delivered, not sent ──

#[test]
fn drop_verdict_discards_package() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    router.install_filter(|_pkg: DistributedMessage<()>| Verdict::Drop);
    let outcome = router.process_inbound(
        keepalive("a"),
        &mut conns,
        clocks_at(Instant::now(), 1.0),
    );
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
    assert!(log.borrow().is_empty(), "Drop must not send anything");
}

// ── Accept → delivered onward (and may transform) ──

#[test]
fn accept_verdict_delivers_package() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    // Identity Accept.
    router.install_filter(|pkg: DistributedMessage<()>| Verdict::Accept(pkg));
    let outcome = router.process_inbound(
        keepalive("a"),
        &mut conns,
        clocks_at(Instant::now(), 1.0),
    );
    match outcome {
        InboundOutcome::Deliver { msg, .. } => assert_eq!(msg.sender_id(), "a"),
        other => panic!("expected Accept→Deliver: {other:?}"),
    }
    assert!(log.borrow().is_empty(), "Accept must not send");
}

#[test]
fn accept_verdict_can_transform_package() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    // Replace the delivered package with a different one.
    router.install_filter(|_pkg: DistributedMessage<()>| Verdict::Accept(keepalive("rewritten")));
    let outcome = router.process_inbound(
        keepalive("a"),
        &mut conns,
        clocks_at(Instant::now(), 1.0),
    );
    match outcome {
        InboundOutcome::Deliver { msg, .. } => assert_eq!(msg.sender_id(), "rewritten"),
        other => panic!("expected transformed Deliver: {other:?}"),
    }
}

// ── Bounce → not delivered; carried package sent back to ORIGIN ──

#[test]
fn bounce_verdict_sends_reply_to_inbound_origin() {
    let log = new_log::<()>();
    // "self" has a direct link to the inbound origin "a".
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    // The reply package the caller constructs carries its OWN
    // sender_id ("self") — the bounce must still go to "a", the
    // sender of the inbound package, NOT to the reply's sender_id.
    router.install_filter(|_pkg: DistributedMessage<()>| Verdict::Bounce(keepalive("self")));
    let outcome = router.process_inbound(
        keepalive("a"),
        &mut conns,
        clocks_at(Instant::now(), 1.0),
    );
    // Not delivered to the consumer.
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
    // Sent back to the ORIGIN "a" via the existing send path.
    let entries = log.borrow();
    assert_eq!(entries.len(), 1, "bounce must send exactly once");
    assert_eq!(entries[0].addressee, "a");
    assert_eq!(entries[0].msg.sender_id(), "self");
}

// ── filter is an opaque, stateful FnMut closure ──

#[test]
fn filter_is_opaque_stateful_closure() {
    let log = new_log::<()>();
    let mut conns = conns_with_log(&["a"], &log);
    let mut router = Router::<()>::new("self".into());
    // Carry mutable state across packages: drop the first, accept
    // the rest. Proves FnMut + that the closure persists on the
    // Router across calls (the variable `seen` is moved INTO the
    // filter and lives there).
    let calls = Arc::new(AtomicU32::new(0));
    let calls_in = calls.clone();
    let mut seen = 0u32;
    router.install_filter(move |pkg: DistributedMessage<()>| {
        calls_in.fetch_add(1, Ordering::SeqCst);
        seen += 1;
        if seen == 1 {
            Verdict::Drop
        } else {
            Verdict::Accept(pkg)
        }
    });

    let now = Instant::now();
    let first = router.process_inbound(keepalive("a"), &mut conns, clocks_at(now, 1.0));
    assert!(
        matches!(first, InboundOutcome::Handled { .. }),
        "first package dropped by stateful filter"
    );
    let second = router.process_inbound(keepalive("a"), &mut conns, clocks_at(now, 2.0));
    assert!(
        matches!(second, InboundOutcome::Deliver { .. }),
        "second package accepted by stateful filter"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "filter invoked once per delivered package"
    );
}

// ── sync path: Drop / Accept behave; Bounce cannot send ──

#[test]
fn sync_path_applies_drop_and_accept() {
    let mut router = Router::<()>::new("self".into());
    router.install_filter(|pkg: DistributedMessage<()>| {
        if pkg.sender_id() == "drop-me" {
            Verdict::Drop
        } else {
            Verdict::Accept(pkg)
        }
    });
    let now = Instant::now();
    let dropped = router.process_inbound_sync(keepalive("drop-me"), clocks_at(now, 1.0));
    assert!(matches!(dropped, InboundOutcome::Handled { .. }));
    let accepted = router.process_inbound_sync(keepalive("keep"), clocks_at(now, 1.0));
    match accepted {
        InboundOutcome::Deliver { msg, .. } => assert_eq!(msg.sender_id(), "keep"),
        other => panic!("expected sync Accept→Deliver: {other:?}"),
    }
}

#[test]
fn sync_path_bounce_is_dropped_not_delivered() {
    let mut router = Router::<()>::new("self".into());
    router.install_filter(|_pkg: DistributedMessage<()>| Verdict::Bounce(keepalive("self")));
    let outcome = router.process_inbound_sync(keepalive("a"), clocks_at(Instant::now(), 1.0));
    // Sync path holds no connections, so Bounce cannot send: the
    // package is neither delivered nor sent.
    assert!(matches!(
        outcome,
        InboundOutcome::Handled {
            redial_target: None
        }
    ));
}
