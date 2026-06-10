//! Tests for the single-respawn `establish_one_tunnel` entry point.
//! Drive `establish_one_tunnel_inner` directly via the same spawner
//! DI seam the `establish_tunnel_*` tests use, plus a public-API
//! smoke check that `establish_one_tunnel` errors before
//! `setup_ssh_tunnels` has populated the primary-QUIC-port cell.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::process::Child;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::preparation::SlurmPreparation;
use crate::preparation::establish::establish_one_tunnel_inner;
use crate::preparation::options::{InfoFileReader, PrepError};
use crate::preparation::store::SharedTunnelVec;

use super::establish::{alive_child, fail_child, listening_inner_verifier};
use super::opts_for;

/// Stubbed `InfoFileReader` returning a fixed URI immediately.
/// Lets the inner skip directly into the spawn+verify phase
/// without filesystem polling.
#[derive(Clone)]
struct CannedUriReader {
    uri: String,
}

impl InfoFileReader for CannedUriReader {
    fn read(
        &self,
        _path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
        let uri = self.uri.clone();
        async move { Ok(Some(uri)) }
    }
}

/// Push-to-Vec invariant: after `establish_one_tunnel_inner`
/// returns Ok, the verified `Child` must be in the shared
/// `ssh_tunnels` Vec and the port-map must carry the discovered
/// `(id, port)`. Pre-fix this was tangled through `drive_one_watcher`
/// and `setup_ssh_tunnels`'s post-gather extend; the refactor moves
/// it inside the inner so any single-tunnel caller observes the
/// same effect.
#[test]
fn establish_one_tunnel_pushes_child_handle() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
        let store = SharedTunnelVec::new(Arc::clone(&tunnels));
        let port_map: Arc<StdMutex<HashMap<String, u16>>> = Arc::new(StdMutex::new(HashMap::new()));
        let establish_pool = Arc::new(Semaphore::new(1));
        let reader = CannedUriReader {
            uri: "tcp://compute-77:54321".into(),
        };
        // Stub spawner: returns a long-lived `/bin/sh sleep` child
        // that passes the 3s verify gate. The child is `kill_on_drop`
        // so it's reaped when the test ends.
        let port = establish_one_tunnel_inner(
            "secondary-0",
            "/unused/info_path",
            /* primary_quic_port */ 51000,
            &opts,
            reader,
            &store,
            &port_map,
            &establish_pool,
            |_host, _tunnel_port| async move { Ok(alive_child()) },
            listening_inner_verifier(),
        )
        .await
        .expect("establish_one_tunnel_inner must succeed");
        // The discovered port came from the canned URI.
        assert_eq!(port, 54321);
        // Child landed in the shared cleanup Vec.
        let guard = tunnels.lock().await;
        assert_eq!(
            guard.len(),
            1,
            "expected one Child in shared ssh_tunnels Vec, got {}",
            guard.len()
        );
        drop(guard);
        // Port map carries the (id, port) entry.
        let m = port_map.lock().unwrap();
        assert_eq!(m.get("secondary-0").copied(), Some(54321));
    }));
}

/// Rate-limiter invariant: two concurrent `establish_one_tunnel_inner`
/// calls sharing the same `Semaphore` may have at most
/// `max_concurrent` spawner invocations in flight at any instant.
/// Mirrors `establish_tunnel_caps_in_flight_spawns_at_max_concurrent`
/// but goes through the inner helper so the rate cap is verified
/// at the per-secondary tunnel API surface, not just the policy
/// engine. Pinned at `MAX = 1` so the two callers must serialise.
#[test]
fn establish_one_tunnel_applies_rate_limiter() {
    const N: usize = 2;
    const MAX: usize = 1;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let peak: usize = rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
        let port_map: Arc<StdMutex<HashMap<String, u16>>> = Arc::new(StdMutex::new(HashMap::new()));
        let establish_pool = Arc::new(Semaphore::new(MAX));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut set: JoinSet<Result<u16, PrepError>> = JoinSet::new();
        for i in 0..N {
            let opts = opts.clone();
            let store = SharedTunnelVec::new(Arc::clone(&tunnels));
            let port_map = Arc::clone(&port_map);
            let pool = Arc::clone(&establish_pool);
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            let id = format!("secondary-{i}");
            let reader = CannedUriReader {
                uri: format!("tcp://compute-{i}:{}", 60000 + i),
            };
            set.spawn_local(async move {
                establish_one_tunnel_inner(
                    &id,
                    "/unused/info_path",
                    51000,
                    &opts,
                    reader,
                    &store,
                    &port_map,
                    &pool,
                    move |_host, _tunnel_port| {
                        let in_flight = Arc::clone(&in_flight);
                        let peak = Arc::clone(&peak);
                        async move {
                            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            // Hold the permit window long enough
                            // that a sibling spawn — if the cap
                            // were broken — would overlap.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            in_flight.fetch_sub(1, Ordering::SeqCst);
                            Ok(alive_child())
                        }
                    },
                    listening_inner_verifier(),
                )
                .await
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.expect("inner task join").expect("inner Ok");
        }
        peak.load(Ordering::SeqCst)
    }));
    assert!(
        peak <= MAX,
        "in-flight spawn count exceeded max_concurrent: peak={peak}, max={MAX}"
    );
    // Sanity: at least one spawner ran (proves the test actually
    // exercised the spawn path, not a no-op).
    assert!(peak >= 1, "expected at least one in-flight spawn, got 0");
}

/// Observer-reconnect SEAM: release-then-rebind-same-port SUCCEEDS,
/// whereas the pre-fix reuse-the-same-port-without-release FAILS.
///
/// This is the unit pin of the BUG-fix DECISION, driven through the
/// SAME [`establish_one_tunnel_inner`] seam the production
/// `reestablish_one_tunnel` uses, with the spawner shaped exactly like
/// the production `reconnect_spawner` (a release-action followed by a
/// spawn-action). It models the worker-side state directly — no real
/// ssh — via a shared `port_in_use` flag:
///
///   * The worker's sshd still holds the stale `-R <tunnel_port>`
///     listener after an ungraceful drop (`port_in_use = true`).
///   * A spawn while the port is in use returns rc=255 "remote port
///     forwarding failed (port in use)" — exactly the production
///     `ExitOnForwardFailure=yes` rc=255 the bug reports.
///   * A RELEASE clears the binding (`port_in_use = false`); the next
///     spawn (same port) then survives the 3s verify gate.
///
/// The two arms below share the worker-state fixture and the
/// establishment policy. The ONLY difference is whether the spawner
/// performs the release first — proving the fix is the release step,
/// not anything else in the establishment path.
#[test]
fn reconnect_release_then_rebind_same_port_succeeds_vs_reuse_fails() {
    use std::sync::atomic::AtomicBool;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();

    // A spawn-action that consults the shared worker-side port state.
    // While the port is still bound it fails with the SAME rc=255
    // "port in use" the production tunnel hits; once released it
    // returns a long-lived child that passes the verify gate. The
    // `release` flag, when set by the spawner, models the production
    // `reconnect_spawner` clearing the stale binding before the spawn.
    fn spawn_for(port_in_use: &Arc<AtomicBool>) -> Child {
        if port_in_use.load(Ordering::SeqCst) {
            fail_child(
                "Warning: remote port forwarding failed for listen port",
                255,
            )
        } else {
            alive_child()
        }
    }

    let (with_fix, without_fix): (Result<u16, PrepError>, Result<u16, PrepError>) =
        rt.block_on(local.run_until(async {
            let tmp = tempfile::tempdir().unwrap();
            let opts = opts_for(&tmp);

            // ---- ARM 1: reconnect spawner (release THEN rebind) ----
            let port_in_use = Arc::new(AtomicBool::new(true)); // stale binding present
            let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
            let store = SharedTunnelVec::new(Arc::clone(&tunnels));
            let port_map: Arc<StdMutex<HashMap<String, u16>>> =
                Arc::new(StdMutex::new(HashMap::new()));
            let pool = Arc::new(Semaphore::new(1));
            let reader = CannedUriReader {
                uri: "tcp://compute-9:40000".into(),
            };
            let piu = Arc::clone(&port_in_use);
            let with_fix = establish_one_tunnel_inner(
                "secondary-0",
                "/unused",
                51000,
                &opts,
                reader,
                &store,
                &port_map,
                &pool,
                move |_host, _port| {
                    let piu = Arc::clone(&piu);
                    async move {
                        // reconnect_spawner shape: RELEASE the stale
                        // binding, then spawn the SAME-port rebind.
                        piu.store(false, Ordering::SeqCst);
                        Ok(spawn_for(&piu))
                    }
                },
                listening_inner_verifier(),
            )
            .await;

            // ---- ARM 2: plain spawner (reuse, NO release) ----
            let port_in_use2 = Arc::new(AtomicBool::new(true)); // stale binding present
            let tunnels2: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
            let store2 = SharedTunnelVec::new(Arc::clone(&tunnels2));
            let port_map2: Arc<StdMutex<HashMap<String, u16>>> =
                Arc::new(StdMutex::new(HashMap::new()));
            let pool2 = Arc::new(Semaphore::new(1));
            let reader2 = CannedUriReader {
                uri: "tcp://compute-9:40000".into(),
            };
            let piu2 = Arc::clone(&port_in_use2);
            let without_fix = establish_one_tunnel_inner(
                "secondary-0",
                "/unused",
                51000,
                &opts,
                reader2,
                &store2,
                &port_map2,
                &pool2,
                move |_host, _port| {
                    let piu2 = Arc::clone(&piu2);
                    // production_spawner shape: no release — the stale
                    // binding stays, every attempt collides.
                    async move { Ok(spawn_for(&piu2)) }
                },
                listening_inner_verifier(),
            )
            .await;

            (with_fix, without_fix)
        }));

    // With the fix (release-then-rebind), the SAME port re-establishes.
    let port = with_fix.expect("release-then-rebind on the same port must succeed");
    assert_eq!(port, 40000, "the rebind reuses the worker's fixed port");

    // Without the release (the pre-fix reuse path), every attempt hits
    // rc=255 "port in use" and the rebuild fails — the exact symptom
    // the bug reports (lost_secs monotonic, never recovers).
    match without_fix {
        Err(PrepError::TunnelFailed { rc, stderr, .. }) => {
            assert_eq!(rc, Some(255), "reuse-without-release fails with rc=255");
            assert!(
                stderr.contains("remote port forwarding failed"),
                "expected the port-in-use failure, got {stderr:?}"
            );
        }
        other => panic!("expected reuse-without-release to fail rc=255, got {other:?}"),
    }
}

/// Defect (a) GATE: `reestablish_one_tunnel` NO-OPs (returns Ok WITHOUT
/// touching ssh) when the secondary's prior tunnel child is still alive —
/// the fix for the rc=255 release+rebind loop that re-fired every ~60s
/// cadence tick against the observer's OWN healthy listener.
///
/// The proof is structural: we seed a LIVE child into the per-secondary
/// registry on a FRESH manager (one that never ran `setup_ssh_tunnels`,
/// so the primary-QUIC-port cell is UNSET). If the gate did NOT fire, the
/// method would fall through to the precondition check and return the
/// "primary QUIC port not yet known" `TunnelFailed`. It returning `Ok(())`
/// instead can ONLY mean the liveness gate short-circuited before any
/// release/rebind — exactly the no-op the cadence needs.
#[test]
fn reestablish_is_noop_when_tunnel_child_alive() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let prep = SlurmPreparation::new(opts);

        // Seed a live (long-running) child into the reconnect registry
        // for secondary-0 — models a HEALTHY `-R` forward whose ssh
        // subprocess is still running.
        {
            let registry = prep.reconnect_tunnels_for_test();
            registry
                .lock()
                .await
                .insert("secondary-0".to_string(), alive_child());
        }

        // No `setup_ssh_tunnels` ran ⇒ primary QUIC port is UNSET. The
        // gate must fire first and return Ok — never reaching the
        // precondition error, never spawning ssh.
        let res = prep
            .reestablish_one_tunnel(
                "secondary-0",
                CannedUriReader {
                    uri: "tcp://h:1".into(),
                },
            )
            .await;
        assert!(
            res.is_ok(),
            "a live tunnel child must make reestablish a no-op (gate before any rebuild), got {res:?}"
        );
    }));
}

/// #342 ESCALATION through the production seam: K (=3, the default)
/// consecutive `reestablish_one_tunnel` calls whose liveness gate says
/// alive-noop FORCE the rebuild on the Kth call — and the force resets
/// the streak so the following call no-ops again (no tick-after-tick
/// churn after a failed force).
///
/// Structural proof, same trick as
/// [`reestablish_is_noop_when_tunnel_child_alive`]: the manager is
/// FRESH (primary-QUIC-port cell UNSET), so reaching the rebuild path
/// can only surface the "primary QUIC port not yet known"
/// `TunnelFailed` — it never touches ssh. Calls 1–2 returning `Ok(())`
/// prove the gate tolerated; call 3 returning THAT error proves the
/// escalation overrode the gate and entered the rebuild; call 4
/// returning `Ok(())` again proves the firing reset the streak.
/// (The successful-force end state — the suspect child REPLACED and
/// reaped by the fresh commit — is the registry's commit-replace
/// contract, pinned in `tests/store.rs::commit_replaces_entry_and_reaps_displaced`.)
#[test]
fn reestablish_escalates_past_alive_gate_after_consecutive_noops() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let prep = SlurmPreparation::new(opts);

        // A LIVE child in the reconnect registry for secondary-0 —
        // models the half-dead tunnel: local ssh alive, worker-side
        // forward (unobservable here) dead, visibility never recovering
        // so the cadence keeps re-firing this call.
        {
            let registry = prep.reconnect_tunnels_for_test();
            registry
                .lock()
                .await
                .insert("secondary-0".to_string(), alive_child());
        }
        let reader = CannedUriReader {
            uri: "tcp://h:1".into(),
        };

        // Ticks 1 + 2: the gate tolerates (alive-noop, Ok).
        for tick in 1..=2 {
            let res = prep.reestablish_one_tunnel("secondary-0", reader.clone()).await;
            assert!(
                res.is_ok(),
                "tick {tick}: below the threshold the gate must still no-op, got {res:?}"
            );
        }

        // Tick 3: the escalation forces the rebuild — the call falls
        // through the gate and hits the unset-QUIC-port precondition,
        // proving the rebuild path was entered without any ssh.
        let forced = prep
            .reestablish_one_tunnel("secondary-0", reader.clone())
            .await;
        match forced {
            Err(PrepError::TunnelFailed { stderr, .. }) => assert!(
                stderr.contains("primary QUIC port"),
                "expected the rebuild-path precondition error, got {stderr:?}"
            ),
            other => panic!(
                "tick 3 must force past the alive gate into the rebuild path, got {other:?}"
            ),
        }

        // Tick 4: the force reset the streak — back to a tolerated no-op.
        let res = prep.reestablish_one_tunnel("secondary-0", reader).await;
        assert!(
            res.is_ok(),
            "the tick after a force must start a fresh streak (no churn), got {res:?}"
        );
    }));
}

/// Calling `establish_one_tunnel` on a fresh manager (before
/// `setup_ssh_tunnels` has stored the primary QUIC port) must
/// surface `TunnelFailed` rather than panicking on a missing
/// precondition. Documents the API contract pinned in the
/// doc-comment.
#[test]
fn establish_one_tunnel_errors_without_prior_setup() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let err: PrepError = rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let opts = opts_for(&tmp);
        let prep = SlurmPreparation::new(opts);
        prep.establish_one_tunnel(
            "secondary-0",
            CannedUriReader {
                uri: "tcp://h:1".into(),
            },
        )
        .await
        .expect_err("must fail without prior setup_ssh_tunnels")
    }));
    match err {
        PrepError::TunnelFailed {
            secondary_id,
            stderr,
            ..
        } => {
            assert_eq!(secondary_id, "secondary-0");
            assert!(
                stderr.contains("primary QUIC port"),
                "expected precondition stderr, got {stderr:?}"
            );
        }
        other => panic!("expected TunnelFailed, got {other}"),
    }
}

/// HONEST-SUMMARY guarantee at the establishment seam: a tunnel whose
/// bind verification never passes is NEVER committed to the tunnel
/// store and NEVER recorded in the port map — so the #278
/// `TunnelSetupSummary` (derived from the port map) counts only
/// VERIFIED tunnels and the production "All N SSH tunnels established"
/// headline can no longer cover a listener-less fleet.
#[test]
fn unverified_tunnel_is_never_committed_or_counted() {
    use crate::preparation::ssh::BindProbe;
    use crate::preparation::summary::TunnelSetupSummary;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        let tmp = tempfile::tempdir().unwrap();
        let mut opts = opts_for(&tmp);
        // Fast policy: the persistent-miss path must not pay the
        // production backoffs.
        opts.establishment.attempts = 2;
        opts.establishment.backoff = vec![Duration::from_millis(10)];

        let tunnels: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
        let store = SharedTunnelVec::new(Arc::clone(&tunnels));
        let port_map: Arc<StdMutex<HashMap<String, u16>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let pool = Arc::new(Semaphore::new(1));
        let reader = CannedUriReader {
            uri: "tcp://compute-3:42655".into(),
        };

        let res = establish_one_tunnel_inner(
            "secondary-0",
            "/unused",
            51000,
            &opts,
            reader,
            &store,
            &port_map,
            &pool,
            // Gate-surviving spawns: the failure is bind-level only.
            |_host, _port| async move { Ok(alive_child()) },
            |_host, _port| std::future::ready(BindProbe::NotListening),
        )
        .await;
        assert!(res.is_err(), "a never-verifying tunnel must surface an error");

        // NOT committed: cleanup() has nothing to reap from this id.
        assert!(
            tunnels.lock().await.is_empty(),
            "an unverified tunnel must never reach the store"
        );
        // NOT counted: the port map is the summary's ground truth.
        let map = port_map.lock().unwrap().clone();
        assert!(map.is_empty(), "an unverified tunnel must never enter the port map");
        let summary = TunnelSetupSummary::new(&map, 1);
        assert!(!summary.is_complete());
        assert_eq!(summary.established, 0, "the K/N summary counts only verified tunnels");
        assert_eq!(summary.missing, vec!["secondary-0".to_string()]);
    }));
}
