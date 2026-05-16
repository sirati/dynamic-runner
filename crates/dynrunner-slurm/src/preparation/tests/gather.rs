//! Tests for `gather_under_deadline` — the per-receiver-timeout
//! state machine that turns N watcher `oneshot` outcomes + one
//! shared deadline into either a (possibly partial) port map or a
//! terminal error.
//!
//! Single concern: deadline + partial-fleet semantics. Drives the
//! senders directly so the tests don't depend on `production_spawner`
//! (real `ssh`) or the info-file polling loop. The companion
//! pipeline-level test `timeout_when_no_secondary_ready` covers the
//! same zero-fleet branch through the public `setup_ssh_tunnels`
//! API — both kept so a regression that re-introduces the
//! `tokio::time::timeout(setup_timeout, gather)` shape (which would
//! drop partial state on cancellation) fails BOTH layers.

use std::time::Duration;

use tokio::sync::oneshot;

use crate::preparation::options::PrepError;
use crate::preparation::pipeline::gather_under_deadline;

/// Helper: spawn a tokio task that waits `delay` then sends `outcome`
/// on the supplied sender. Mirrors what `setup_ssh_tunnels`'s
/// per-secondary watcher does — but the sender is the only part of
/// the watcher state machine that `gather_under_deadline` observes,
/// so the rest is irrelevant here.
fn spawn_sender_after(
    tx: oneshot::Sender<Result<(String, u16), PrepError>>,
    delay: Duration,
    outcome: Result<(String, u16), PrepError>,
) {
    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let _ = tx.send(outcome);
    });
}

/// Partial-fleet path: 4 secondaries expected, 2 deliver successfully
/// before the deadline, 2 hold their senders open past the deadline
/// (mimicking compute nodes whose info file never appeared). The
/// call must return `Ok(map)` with `map.len() == 2`, and a warn-level
/// log must be emitted carrying the "proceeding with partial fleet"
/// headline so operators see the degradation.
///
/// This is the primary regression-pin for the bug fix: the prior
/// implementation used `tokio::time::timeout(setup_timeout, gather)`,
/// which dropped the partial `HashMap` on cancellation and returned
/// `Err(Timeout)` here.
#[tokio::test(flavor = "current_thread")]
#[tracing_test::traced_test]
async fn partial_fleet_succeeds_with_warning() {
    let mut receivers = Vec::with_capacity(4);

    // Two will deliver well before the 500ms deadline.
    let (tx0, rx0) = oneshot::channel();
    receivers.push(rx0);
    spawn_sender_after(
        tx0,
        Duration::from_millis(50),
        Ok(("secondary-0".to_string(), 60000)),
    );

    let (tx1, rx1) = oneshot::channel();
    receivers.push(rx1);
    spawn_sender_after(
        tx1,
        Duration::from_millis(50),
        Ok(("secondary-1".to_string(), 60001)),
    );

    // Two are held — the senders are kept alive but never fire.
    // Storing them in a Vec keeps them live across the await; if
    // they were dropped the receivers would surface `WatcherLost`
    // (RecvError) instead of timing out, which is a different
    // code path.
    let (tx2, rx2) = oneshot::channel::<Result<(String, u16), PrepError>>();
    receivers.push(rx2);
    let (tx3, rx3) = oneshot::channel::<Result<(String, u16), PrepError>>();
    receivers.push(rx3);
    let _held = [tx2, tx3];

    let result =
        gather_under_deadline(receivers, /*num_secondaries=*/ 4, Duration::from_millis(500))
            .await;

    let map = result.expect("partial fleet must succeed, not error");
    assert_eq!(
        map.len(),
        2,
        "expected 2 of 4 secondaries in returned map, got {map:?}"
    );
    assert_eq!(map.get("secondary-0").copied(), Some(60000));
    assert_eq!(map.get("secondary-1").copied(), Some(60001));
    // The 2 stalled secondaries are absent from the map — late-
    // joiner system's responsibility from here.
    assert!(!map.contains_key("secondary-2"));
    assert!(!map.contains_key("secondary-3"));

    // Warn log must fire with the partial-fleet headline so the
    // degradation is operator-visible. Substring-match on a stable
    // phrase from the warn! call — full message is allowed to
    // evolve, the "proceeding with partial fleet" wording is the
    // load-bearing operator-facing signal.
    assert!(
        logs_contain("proceeding with partial fleet"),
        "expected partial-fleet warn log"
    );
}

/// Zero-fleet path: 2 secondaries expected, neither delivers before
/// the deadline. Returns `Err(PrepError::Timeout { ready: 0,
/// total: 2 })` — kept as a genuine fleet-failure case so callers
/// can still distinguish "nobody came" from "some came".
///
/// Regression-pin: ensures the partial-fleet rework did NOT
/// accidentally turn the zero case into `Ok(empty_map)`.
#[tokio::test(flavor = "current_thread")]
async fn zero_fleet_still_errors() {
    let (tx0, rx0) = oneshot::channel::<Result<(String, u16), PrepError>>();
    let (tx1, rx1) = oneshot::channel::<Result<(String, u16), PrepError>>();
    // Hold senders so the receivers stall on the deadline (rather
    // than tripping the `WatcherLost` branch on sender drop).
    let _held = [tx0, tx1];

    let result = gather_under_deadline(
        vec![rx0, rx1],
        /*num_secondaries=*/ 2,
        Duration::from_millis(200),
    )
    .await;

    match result {
        Err(PrepError::Timeout { ready, total }) => {
            assert_eq!(ready, 0);
            assert_eq!(total, 2);
        }
        Ok(m) => panic!("expected Timeout, got Ok({m:?})"),
        Err(other) => panic!("expected Timeout, got {other}"),
    }
}

/// Fail-fast on explicit inner error: one watcher surfaces
/// `PrepError::InfoRead` immediately. The call returns that error
/// without waiting for the other watcher (no swallowing or
/// downgrade-to-partial behaviour).
///
/// Regression-pin: ensures the fail-fast semantics of the
/// pre-refactor `gather` closure survived the extraction. A
/// fleet-configuration problem ought to surface at the caller, not
/// be silently masked by a one-of-N partial map.
#[tokio::test(flavor = "current_thread")]
async fn fail_fast_on_explicit_inner_error() {
    let (tx0, rx0) = oneshot::channel();
    spawn_sender_after(
        tx0,
        Duration::from_millis(10),
        Err(PrepError::InfoRead {
            secondary_id: "secondary-0".to_string(),
            message: "synthetic".to_string(),
        }),
    );
    // A second receiver whose sender is HELD past the assertion
    // window — load-bearing for the assertion that gather aborts
    // on the explicit error WITHOUT waiting for siblings. If gather
    // mistakenly waited, the test would hang on this rx1 until the
    // outer `setup_timeout` fired (which is the bug we're guarding
    // against — fail-fast must not be downgraded into "wait then
    // return partial").
    let (tx1, rx1) = oneshot::channel::<Result<(String, u16), PrepError>>();
    let _held = [tx1];

    let start = std::time::Instant::now();
    let result =
        gather_under_deadline(vec![rx0, rx1], /*num_secondaries=*/ 2, Duration::from_secs(30))
            .await;
    let elapsed = start.elapsed();

    // Must return quickly — well under the 30s deadline. Generous
    // bound (500ms) to absorb runtime scheduling jitter on slow
    // CI; what's load-bearing is "did not wait for the deadline".
    assert!(
        elapsed < Duration::from_millis(500),
        "fail-fast violated: gather took {elapsed:?} (deadline was 30s); did it wait for the held sibling?",
    );

    match result {
        Err(PrepError::InfoRead { secondary_id, message }) => {
            assert_eq!(secondary_id, "secondary-0");
            assert_eq!(message, "synthetic");
        }
        other => panic!("expected fail-fast InfoRead error, got {other:?}"),
    }
}

/// `WatcherLost` surfacing: a sender that is dropped without sending
/// (mimicking a watcher panic or abort before reaching its
/// `tx.send`) must produce `PrepError::WatcherLost`. Pins the
/// behaviour of the `Ok(Err(join_err))` arm — important because
/// `setup_ssh_tunnels` aborts the `JoinSet` on its own return path,
/// and any silent swallowing of dropped senders would mask a real
/// crash in the watcher.
#[tokio::test(flavor = "current_thread")]
async fn dropped_sender_surfaces_watcher_lost() {
    let (tx0, rx0) = oneshot::channel::<Result<(String, u16), PrepError>>();
    // Drop without sending — receiver will surface RecvError.
    drop(tx0);
    let (tx1, rx1) = oneshot::channel();
    spawn_sender_after(
        tx1,
        Duration::from_millis(10),
        Ok(("secondary-1".to_string(), 60001)),
    );

    let result =
        gather_under_deadline(vec![rx0, rx1], /*num_secondaries=*/ 2, Duration::from_secs(5))
            .await;

    match result {
        Err(PrepError::WatcherLost(_)) => {}
        other => panic!("expected WatcherLost, got {other:?}"),
    }
}
