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
use crate::preparation::ssh::BindProbe;

/// Canned always-Listening bind verifier (zero-arg shape for
/// `establish_tunnel`). The pre-existing policy tests use it so their
/// concern — rate-limit / retry / budget — stays isolated from bind
/// verification.
pub(super) fn listening_verifier() -> impl FnMut() -> std::future::Ready<BindProbe> {
    || {
        std::future::ready(BindProbe::Listening {
            listeners: vec!["127.0.0.1:40000".into(), "[::1]:40000".into()],
        })
    }
}

/// Canned always-Listening bind verifier in the `(host, tunnel_port)`
/// shape `establish_one_tunnel_inner` takes. Shared with the respawn
/// tests.
pub(super) fn listening_inner_verifier()
-> impl FnMut(String, u16) -> std::future::Ready<BindProbe> {
    |_host, _tunnel_port| {
        std::future::ready(BindProbe::Listening {
            listeners: vec!["127.0.0.1:40000".into(), "[::1]:40000".into()],
        })
    }
}

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

        let res = establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || {
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
            },
            listening_verifier(),
        )
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

        let res = establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || {
                let i = attempts_ref.fetch_add(1, Ordering::SeqCst);
                async move { Ok(fail_child(&format!("ATTEMPT-{i}-FAIL"), 255)) }
            },
            listening_verifier(),
        )
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
                }, listening_verifier())
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

        establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || async move {
                // Each attempt fails fast; the long backoff +
                // 200ms budget means the timeout fires before
                // attempt 2 even starts.
                Ok(fail_child("FAIL", 255))
            },
            listening_verifier(),
        )
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

/// FAILURE CLASSIFICATION fail-fast: an rc=255 whose stderr carries
/// auth-class evidence ("Permission denied" — the wrong/missing
/// key/user PROVISIONING shape) is DETERMINISTIC: every retry would
/// refuse identically, so the establishment must surface the error
/// after exactly ONE attempt — no retry budget burned, and the
/// operator sees the verbatim stderr immediately (asm-dataset
/// run_20260611: 3 silent retries turned a 10-second "Permission
/// denied" diagnosis into two failed dispatch attempts).
#[test]
fn establish_tunnel_fails_fast_on_auth_class_stderr() {
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

        let res = establish_tunnel(
            "secondary-4",
            &policy,
            &pool,
            move || {
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                async move {
                    Ok(fail_child(
                        "runuser@gateway.example: Permission denied (publickey,password).",
                        255,
                    ))
                }
            },
            listening_verifier(),
        )
        .await;

        let err = res.expect_err("auth-class failure must surface an error");
        (err, attempt_counter.load(Ordering::SeqCst))
    }));
    assert_eq!(
        attempts, 1,
        "a deterministic auth-class failure must NOT be retried"
    );
    match err {
        PrepError::TunnelFailed {
            secondary_id,
            rc,
            stderr,
        } => {
            assert_eq!(secondary_id, "secondary-4");
            assert!(rc.is_some());
            // The verbatim ssh stderr is the operator's diagnosis —
            // it must survive into the surfaced error untouched.
            assert_eq!(
                stderr,
                "runuser@gateway.example: Permission denied (publickey,password)."
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

/// THE partial-`-R`-bind closer (production anatomy: ssh survives the
/// 3s gate, `ExitOnForwardFailure=yes` never trips, yet the worker's
/// `127.0.0.1:<tunnel_port>` listener NEVER exists): a definite
/// `NotListening` bind-probe verdict must (1) TERMINATE the useless
/// gate-surviving child, (2) consume one attempt and RESPAWN, and
/// (3) succeed once the respawned tunnel verifies. The dead first
/// child is asserted via its reaped pid; the returned child is the
/// second spawn.
#[test]
fn bind_verification_failure_kills_child_and_respawns() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = fast_policy(1, 3);
        let pids = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let pids_for_spawner = Arc::clone(&pids);
        let probe_count = Arc::new(AtomicUsize::new(0));
        let probe_ref = Arc::clone(&probe_count);

        let child = establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || {
                let pids = Arc::clone(&pids_for_spawner);
                async move {
                    // Every spawn survives the gate — the FAILURE in
                    // this scenario is invisible to the gate (that is
                    // the bug's whole anatomy).
                    let child = alive_child();
                    pids.lock().unwrap().push(child.id().expect("live child has a pid"));
                    Ok(child)
                }
            },
            move || {
                let i = probe_ref.fetch_add(1, Ordering::SeqCst);
                std::future::ready(if i == 0 {
                    // First tunnel: sshd bound only [::1] elsewhere /
                    // nothing dialable — definite miss.
                    BindProbe::NotListening
                } else {
                    BindProbe::Listening {
                        listeners: vec!["127.0.0.1:40000".into()],
                    }
                })
            },
        )
        .await
        .expect("respawn after a bind-verification miss must succeed");

        let pids = pids.lock().unwrap().clone();
        assert_eq!(pids.len(), 2, "one miss + one verified spawn");
        assert_eq!(
            probe_count.load(Ordering::SeqCst),
            2,
            "each gate-surviving spawn is bind-verified exactly once"
        );
        // The first (unverified) child was terminated AND reaped —
        // its /proc entry is gone. The returned child is the second.
        assert!(
            !std::path::Path::new(&format!("/proc/{}", pids[0])).exists(),
            "the gate-surviving but unverified child must be killed"
        );
        assert_eq!(child.id(), Some(pids[1]), "the verified child is the respawn");
    }));
}

/// A PERSISTENT bind-verification miss (the squatter never clears)
/// exhausts the attempt budget and fails loud with the partial-bind
/// message — and every gate-surviving child was killed along the way
/// (no leaked half-tunnels for cleanup() to find uncommitted).
#[test]
fn bind_verification_failure_exhausts_attempts_then_fails_loud() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = fast_policy(1, 3);
        let pids = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
        let pids_for_spawner = Arc::clone(&pids);

        let err = establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || {
                let pids = Arc::clone(&pids_for_spawner);
                async move {
                    let child = alive_child();
                    pids.lock().unwrap().push(child.id().expect("live child has a pid"));
                    Ok(child)
                }
            },
            || std::future::ready(BindProbe::NotListening),
        )
        .await
        .expect_err("a never-verifying tunnel must fail after the attempt budget");

        let pids = pids.lock().unwrap().clone();
        assert_eq!(pids.len(), 3, "must spawn exactly `attempts` times");
        for pid in &pids {
            assert!(
                !std::path::Path::new(&format!("/proc/{pid}")).exists(),
                "every unverified child must be killed (pid {pid} survives)"
            );
        }
        match err {
            PrepError::TunnelFailed { secondary_id, rc, stderr } => {
                assert_eq!(secondary_id, "secondary-0");
                assert_eq!(rc, None, "a bind-verification miss has no exit code");
                assert!(
                    stderr.contains("worker-side listener never appeared"),
                    "error must name the partial-bind anatomy, got {stderr:?}"
                );
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }));
}

/// An `Inconclusive` probe (ssh to the worker failed / `ss` missing)
/// must KEEP the gate-verified tunnel — the probe's own
/// infrastructure failing must never kill tunnels that met the
/// pre-probe standard (a node without iproute2 would otherwise lose
/// every tunnel forever).
#[test]
fn bind_probe_inconclusive_keeps_gate_verified_tunnel() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let pool = Arc::new(Semaphore::new(1));
        let policy = fast_policy(1, 3);
        let spawns = Arc::new(AtomicUsize::new(0));
        let spawns_ref = Arc::clone(&spawns);

        let child = establish_tunnel(
            "secondary-0",
            &policy,
            &pool,
            move || {
                spawns_ref.fetch_add(1, Ordering::SeqCst);
                async move { Ok(alive_child()) }
            },
            || {
                std::future::ready(BindProbe::Inconclusive {
                    reason: "`ss` unavailable on the worker (iproute2 missing)".into(),
                })
            },
        )
        .await
        .expect("an inconclusive probe must not fail the tunnel");

        assert_eq!(spawns.load(Ordering::SeqCst), 1, "no respawn on inconclusive");
        assert!(child.id().is_some(), "the gate-verified child is kept alive");
    }));
}
