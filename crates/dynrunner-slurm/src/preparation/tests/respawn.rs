//! Tests for the single-respawn `establish_one_tunnel` entry point.
//! Drive `establish_one_tunnel_inner` directly via the same spawner
//! DI seam the `establish_tunnel_*` tests use, plus a public-API
//! smoke check that `establish_one_tunnel` errors before
//! `setup_ssh_tunnels` has populated the primary-QUIC-port cell.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use tokio::process::Child;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::preparation::establish::establish_one_tunnel_inner;
use crate::preparation::options::{InfoFileReader, PrepError};
use crate::preparation::SlurmPreparation;

use super::establish::alive_child;
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
        let port_map: Arc<StdMutex<HashMap<String, u16>>> =
            Arc::new(StdMutex::new(HashMap::new()));
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
            &tunnels,
            &port_map,
            &establish_pool,
            |_host, _tunnel_port| async move { Ok(alive_child()) },
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
        let port_map: Arc<StdMutex<HashMap<String, u16>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let establish_pool = Arc::new(Semaphore::new(MAX));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut set: JoinSet<Result<u16, PrepError>> = JoinSet::new();
        for i in 0..N {
            let opts = opts.clone();
            let tunnels = Arc::clone(&tunnels);
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
                    &tunnels,
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
        PrepError::TunnelFailed { secondary_id, stderr, .. } => {
            assert_eq!(secondary_id, "secondary-0");
            assert!(
                stderr.contains("primary QUIC port"),
                "expected precondition stderr, got {stderr:?}"
            );
        }
        other => panic!("expected TunnelFailed, got {other}"),
    }
}
