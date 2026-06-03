//! The registered `on_cluster_state_refresh` callback fires on the
//! `process_tasks` periodic tick with the LIVE, post-apply
//! `cluster_state`.
//!
//! Single concern of this file: pin the live mid-run refresh seam
//! (`register_cluster_state_refresh` + the periodic-tick arm in
//! `process_tasks`). The observer holds `&mut cluster_state` for the
//! whole run, so a concurrently-running consumer (the PyO3 observer's
//! live-snapshot feed) can only observe the freshening CRDT through this
//! callback. The test:
//!   1. seeds the CRDT (a restored snapshot with a known terminal count),
//!   2. registers a CAPTURING callback that records what it observed,
//!   3. drives `process_tasks` under a PAUSED clock and advances past the
//!      refresh interval so the tick fires,
//!   4. terminates the loop deterministically via the fatal-exit signal,
//!   5. asserts the callback fired AND observed the seeded counts (proving
//!      it read the live, post-restore ledger — not a stale/empty view).
//!
//! The downstream projection (`StatsSnapshot::from_cluster_state`) and
//! the `SharedSnapshotSource` publish live in the PyO3 layer; this crate
//! only knows it hands `&ClusterState` to a registered closure, so the
//! callback here captures raw CRDT reads (`outcome_counts().succeeded`)
//! rather than a PyO3 projection — keeping the test inside this crate's
//! dependency boundary.
//!
//! Determinism: `start_paused` + explicit `advance` means the periodic
//! tick elapses on the virtual clock with no wall-clock race; the
//! fatal-exit signal gives a deterministic loop exit rather than racing a
//! timeout.

#![cfg(test)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use dynrunner_core::TaskInfo;

use super::super::test_helpers::TestId;
use super::super::test_helpers::{FakeWorkerFactory, election_config, make_secondary};

/// A snapshot with `n_completed` completed tasks under a designated
/// primary. No `RunComplete` is set, so a coordinator that restores this
/// stays in `process_tasks` rather than exiting immediately — letting the
/// periodic refresh tick fire. `outcome_counts().succeeded` over the
/// restored ledger equals `n_completed`.
fn snapshot_with_completed(
    n_completed: usize,
) -> crate::cluster_state::ClusterStateSnapshot<TestId> {
    use crate::cluster_state::TaskState;
    let mk_task = |ident: &str| TaskInfo {
        path: PathBuf::from(format!("/tmp/{ident}")),
        size: 100,
        identifier: TestId(ident.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: ident.into(),
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let mut tasks = HashMap::new();
    for i in 0..n_completed {
        let id = format!("done-{i}");
        tasks.insert(id.clone(), TaskState::Completed { task: mk_task(&id) });
    }
    crate::cluster_state::ClusterStateSnapshot {
        tasks,
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 3,
        phase_deps: HashMap::new(),
        observers: std::iter::once("observer-1".to_string()).collect(),
        can_be_primary: Default::default(),
        peer_holdings: HashMap::new(),
        task_outputs: HashMap::new(),
        secondary_capacities: HashMap::new(),
    }
}

/// The registered refresh callback fires on the periodic tick with the
/// live, post-restore `cluster_state` — observing the seeded terminal
/// count, not a stale/empty view. Driven on a paused clock so the
/// `CLUSTER_STATE_REFRESH_INTERVAL` (30s) elapses deterministically.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn refresh_callback_fires_on_tick_with_live_cluster_state() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = election_config("observer-1");
            config.is_observer = true;
            config.num_workers = 0;
            let mut sec = make_secondary(config);

            // Seed the CRDT with 3 completed tasks; no RunComplete, so the
            // loop stays in `process_tasks` and the refresh tick gets a
            // chance to fire. The restore also latches
            // `setup_phase_completed = true`, so the run loop skips the
            // setup handshake and enters `process_tasks` directly.
            sec.restore_from_snapshot_and_skip_setup(snapshot_with_completed(3));
            assert_eq!(
                sec.cluster_state.outcome_counts().succeeded,
                3,
                "precondition: the restored ledger reports 3 completed",
            );

            // Capturing callback: records every `succeeded` count it
            // observed off the LIVE borrow. `Rc<RefCell<_>>` because the
            // callback and the assertion both run on this single-threaded
            // LocalSet.
            let observed: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
            let observed_in_cb = observed.clone();
            sec.register_cluster_state_refresh(Box::new(move |cs| {
                observed_in_cb
                    .borrow_mut()
                    .push(cs.outcome_counts().succeeded);
            }));

            // Fatal-exit signal: the deterministic loop terminator. We
            // fire it AFTER advancing past the refresh interval so the
            // refresh tick has fired first; the loop then latches
            // `fatal_exit` and returns `Err`.
            let (fatal_tx, fatal_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            sec.register_fatal_exit_signal_rx(fatal_rx);

            // Drive the run loop on its own task so we can advance the
            // virtual clock around it.
            let run_handle = tokio::task::spawn_local(async move {
                let mut factory = FakeWorkerFactory;
                let result = sec.run_until_setup_or_done(&mut factory).await;
                (sec, result)
            });

            // Yield so the spawned loop reaches its first `select!` await
            // and the refresh interval is armed (the immediate tick was
            // consumed by `reset()` at loop entry).
            tokio::task::yield_now().await;

            // Advance past the 30s refresh interval so the periodic tick
            // fires the callback. A small margin over 30s covers the
            // interval's full period.
            tokio::time::advance(Duration::from_secs(31)).await;
            tokio::task::yield_now().await;

            // The callback must have fired at least once by now, observing
            // the live (post-restore) count.
            assert!(
                !observed.borrow().is_empty(),
                "the refresh callback should have fired after the 30s tick \
                 elapsed; it did not",
            );
            assert!(
                observed.borrow().iter().all(|&n| n == 3),
                "every refresh-callback invocation must observe the live \
                 post-restore count (succeeded == 3); got: {:?}",
                observed.borrow(),
            );

            // Terminate the loop deterministically.
            fatal_tx
                .send("test teardown".to_string())
                .expect("fatal-exit receiver still live");
            tokio::task::yield_now().await;

            let (_sec, result) = tokio::time::timeout(Duration::from_secs(5), run_handle)
                .await
                .expect("run loop did not return within budget after fatal-exit")
                .expect("run loop task panicked");
            assert!(
                result.is_err(),
                "the fatal-exit signal should drive the loop to an Err exit",
            );
        })
        .await;
}

/// Without a registered callback the periodic tick is a no-op: the loop
/// keeps running and exits cleanly only via its own terminal path. Pins
/// the `None` branch of the refresh arm (a regular secondary / Rust-only
/// caller that never registers a consumer must not be affected).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn no_callback_registered_is_inert() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = election_config("observer-2");
            config.is_observer = true;
            config.num_workers = 0;
            let mut sec = make_secondary(config);
            sec.restore_from_snapshot_and_skip_setup(snapshot_with_completed(1));

            // NO `register_cluster_state_refresh` — the slot stays `None`.
            let (fatal_tx, fatal_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            sec.register_fatal_exit_signal_rx(fatal_rx);

            let run_handle = tokio::task::spawn_local(async move {
                let mut factory = FakeWorkerFactory;
                sec.run_until_setup_or_done(&mut factory).await
            });

            tokio::task::yield_now().await;
            // Advance past the refresh interval; the inert tick fires and
            // does nothing.
            tokio::time::advance(Duration::from_secs(31)).await;
            tokio::task::yield_now().await;

            fatal_tx
                .send("test teardown".to_string())
                .expect("fatal-exit receiver still live");
            tokio::task::yield_now().await;

            let result = tokio::time::timeout(Duration::from_secs(5), run_handle)
                .await
                .expect("run loop did not return within budget")
                .expect("run loop task panicked");
            assert!(
                result.is_err(),
                "with no callback the loop still exits via the fatal-exit path",
            );
        })
        .await;
}
