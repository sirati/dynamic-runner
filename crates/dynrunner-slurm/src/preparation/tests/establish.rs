//! Tests for the establishment policy engine: rate-limit, retry,
//! per-tunnel wall-clock cap. Exercise `establish_tunnel` directly
//! via dependency injection on the spawner closure (no real ssh).
//! Also includes the `EstablishmentPolicy` defaults contract test.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::preparation::establish::establish_tunnel;
use crate::preparation::options::PrepError;
use crate::preparation::policy::EstablishmentPolicy;

/// Build a `/bin/sh` child whose stderr emits `marker` and whose
/// exit code is `rc`. Returns a `Child` that mirrors what
/// `verify_tunnel_alive` will observe — fast-exit (≪ 3s) ensures
/// the failure branch trips immediately. `pub(super)` so the
/// `respawn` test file can model the worker-side "port still in use"
/// rc=255 failure without duplicating the helper.
pub(super) fn fail_child(marker: &str, rc: i32) -> Child {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("printf '%s' '{marker}' >&2; exit {rc}"));
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    cmd.spawn().expect("spawn /bin/sh")
}

/// A child that survives the 3s verify gate. We use `sleep 60`
/// (and `kill_on_drop(true)` reaps it when the test drops the
/// Child returned from `establish_tunnel`). `pub(super)` so the
/// `respawn` test file can drive `establish_one_tunnel_inner` with
/// the same fake-success child without duplicating the helper.
pub(super) fn alive_child() -> Child {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg("sleep 60");
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    cmd.spawn().expect("spawn /bin/sh sleep")
}

/// Establishment-policy test fixture: zero-backoff, 1s per-tunnel
/// budget so tests stay fast.
fn fast_policy(max_concurrent: usize, attempts: usize) -> EstablishmentPolicy {
    EstablishmentPolicy {
        max_concurrent,
        attempts,
        backoff: vec![Duration::from_millis(10)],
        per_tunnel_timeout: Duration::from_secs(30),
    }
}

/// Retry semantics: a spawner that returns rc=255 on the first
/// attempt and a long-lived (sleep-60) child on the second must
/// surface success on the second attempt. Pins option-1: per-
/// tunnel retry-on-handshake-failure.
#[test]
fn establish_tunnel_retries_then_succeeds() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let result: Result<(), PrepError> = rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = fast_policy(1, 3);
        let attempt_counter = Arc::new(AtomicUsize::new(0));
        let attempts_ref = Arc::clone(&attempt_counter);

        let res = establish_tunnel("secondary-0", &policy, &pool, move || {
            let i = attempts_ref.fetch_add(1, Ordering::SeqCst);
            async move {
                if i == 0 {
                    // First attempt: simulate rc=255 (LMU
                    // gateway random-drop on overloaded sshd).
                    Ok(fail_child(
                        "kex_exchange_identification: Connection closed by remote host",
                        255,
                    ))
                } else {
                    // Second attempt: lives past 3s gate.
                    Ok(alive_child())
                }
            }
        })
        .await;

        match res {
            Ok(_child) => {
                assert_eq!(
                    attempt_counter.load(Ordering::SeqCst),
                    2,
                    "expected exactly 2 spawn attempts (1 fail + 1 success)"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }));
    result.expect("retry-then-success path must yield Ok");
}

/// Retry exhaustion: a spawner that always returns rc=255 hits
/// `attempts` total tries, then surfaces the LAST `TunnelFailed`
/// — never aborts early, never retries forever.
#[test]
fn establish_tunnel_exhausts_attempts_then_fails_loud() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let (err, attempts) = rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = fast_policy(1, 3);
        let attempt_counter = Arc::new(AtomicUsize::new(0));
        let attempts_ref = Arc::clone(&attempt_counter);

        let res = establish_tunnel("secondary-0", &policy, &pool, move || {
            let i = attempts_ref.fetch_add(1, Ordering::SeqCst);
            async move { Ok(fail_child(&format!("ATTEMPT-{i}-FAIL"), 255)) }
        })
        .await;

        let err = res.expect_err("3 failing attempts must surface TunnelFailed");
        (err, attempt_counter.load(Ordering::SeqCst))
    }));
    assert_eq!(attempts, 3, "must hit attempts cap exactly");
    match err {
        PrepError::TunnelFailed {
            secondary_id,
            rc,
            stderr,
        } => {
            assert_eq!(secondary_id, "secondary-0");
            // /bin/sh `exit 255` → POSIX raw exit code 255. The
            // exact rc isn't load-bearing (load-bearing is "did
            // we surface the LAST attempt's stderr, not the
            // first?"); pin to a non-None value so a regression
            // that drops rc is caught.
            assert!(rc.is_some(), "rc must be present for spawn-time exit");
            // The surfaced stderr MUST come from the LAST attempt
            // (the latest in the sequence), proving we surface
            // the final failure rather than the first.
            assert_eq!(stderr, "ATTEMPT-2-FAIL");
        }
        other => panic!("expected TunnelFailed, got {other}"),
    }
}

/// Stagger semantics: with `max_concurrent = 2` and N=4 concurrent
/// `establish_tunnel` calls, no more than 2 spawner invocations
/// may be in flight at any instant. Pins option-2: the semaphore
/// rate-cap.
///
/// Mechanism: the spawner holds for a fixed wait window before
/// resolving its future, during which the in-flight counter is
/// observable. We assert the peak counter stays ≤ max_concurrent
/// across the whole test.
#[test]
fn establish_tunnel_caps_in_flight_spawns_at_max_concurrent() {
    const N: usize = 4;
    const MAX: usize = 2;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let peak: usize = rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(MAX));
        // Slightly slower than the verify gate so the permit
        // really IS held during the verify window — the test
        // would still pass with a sub-millisecond spawner but
        // the spirit of the cap is "limit handshake concurrency",
        // not "limit Command::spawn turnover".
        let policy = EstablishmentPolicy {
            max_concurrent: MAX,
            attempts: 1,
            backoff: vec![],
            per_tunnel_timeout: Duration::from_secs(30),
        };
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut set: JoinSet<Result<(), PrepError>> = JoinSet::new();
        for i in 0..N {
            let pool = Arc::clone(&pool);
            let policy = policy.clone();
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            let id = format!("secondary-{i}");
            set.spawn_local(async move {
                let in_flight_for_spawner = Arc::clone(&in_flight);
                let peak_for_spawner = Arc::clone(&peak);
                let res = establish_tunnel(&id, &policy, &pool, move || {
                    let in_flight = Arc::clone(&in_flight_for_spawner);
                    let peak = Arc::clone(&peak_for_spawner);
                    async move {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        // Update the peak watermark with the
                        // post-increment value — load-bearing
                        // for the assertion below.
                        peak.fetch_max(now, Ordering::SeqCst);
                        // Hold the permit window long enough to
                        // overlap with sibling spawns. 50ms ×
                        // ceil(N/MAX) = 100ms total run.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        // Return a long-lived child so verify
                        // passes — we're testing the permit
                        // gating, not the verify branch.
                        Ok(alive_child())
                    }
                })
                .await;
                res.map(|_| ())
            });
        }

        // Drain all spawned tasks.
        while let Some(joined) = set.join_next().await {
            joined.expect("watcher join").expect("watcher Ok");
        }
        peak.load(Ordering::SeqCst)
    }));
    assert!(
        peak <= MAX,
        "in-flight spawn count exceeded max_concurrent: peak={peak}, max={MAX}"
    );
    // Sanity: at least MAX must have been simultaneously in
    // flight — otherwise the spawner was so fast the test
    // never actually exercised the cap.
    assert!(
        peak >= MAX,
        "test failed to demonstrate parallelism: peak={peak} < max={MAX}"
    );
}

/// Per-tunnel wall-clock cap: a spawner that hangs forever past
/// the `per_tunnel_timeout` budget must surface `TunnelFailed`
/// with a budget-exhaustion stderr message.
#[test]
fn establish_tunnel_enforces_per_tunnel_wall_clock_budget() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let err: PrepError = rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = EstablishmentPolicy {
            max_concurrent: 1,
            attempts: 5,
            // Long backoff that the budget should cut short.
            backoff: vec![Duration::from_secs(10)],
            per_tunnel_timeout: Duration::from_millis(200),
        };

        establish_tunnel("secondary-0", &policy, &pool, move || async move {
            // Each attempt fails fast; the long backoff +
            // 200ms budget means the timeout fires before
            // attempt 2 even starts.
            Ok(fail_child("FAIL", 255))
        })
        .await
        .expect_err("budget exhaustion must surface error")
    }));
    match err {
        PrepError::TunnelFailed {
            secondary_id,
            rc,
            stderr,
        } => {
            assert_eq!(secondary_id, "secondary-0");
            assert_eq!(rc, None, "budget-exhaustion has no spawn rc");
            assert!(
                stderr.contains("budget"),
                "expected budget-exhaustion message, got {stderr:?}"
            );
        }
        other => panic!("expected TunnelFailed, got {other}"),
    }
}

/// Default policy sanity: the operator-friendly defaults are the
/// numbers documented in the design (4 concurrent, 3 attempts,
/// [5s, 15s] backoff, 90s per-tunnel cap). Pinned here so a
/// careless default-change in `EstablishmentPolicy::default` gets
/// noticed at review time.
#[test]
fn establishment_policy_defaults_match_consumer_contract() {
    let p = EstablishmentPolicy::default();
    assert_eq!(p.max_concurrent, 4);
    assert_eq!(p.attempts, 3);
    assert_eq!(
        p.backoff,
        vec![Duration::from_secs(5), Duration::from_secs(15)]
    );
    assert_eq!(p.per_tunnel_timeout, Duration::from_secs(90));
    // Backoff indexing: attempt 0 has no pre-sleep, attempts 1
    // and 2 use backoff[0] and backoff[1] respectively, anything
    // beyond saturates at the last element.
    assert_eq!(p.backoff_before(0), None);
    assert_eq!(p.backoff_before(1), Some(Duration::from_secs(5)));
    assert_eq!(p.backoff_before(2), Some(Duration::from_secs(15)));
    assert_eq!(p.backoff_before(3), Some(Duration::from_secs(15)));
}
