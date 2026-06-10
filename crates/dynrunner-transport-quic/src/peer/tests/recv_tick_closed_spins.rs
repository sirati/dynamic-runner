//! REPRO PROBE (livelock RCA): does a CLOSED `reconnect_tick_rx` make
//! `PeerNetwork::recv_peer` freeze the single-thread runtime?
//!
//! ANSWER (this test): NO. This probe was written to test the leading
//! hypothesis for the asm-dataset `run_20260610_050030` livelock and
//! **falsifies it**. Kept as a regression + a signpost so the next
//! investigator does not re-chase this dead end.
//!
//! # The production failure being investigated
//!
//! A relocated-primary host process (`python -m
//! dynamic_runner._secondary_bootstrap`, secondary-0) froze at 03:07:54: the
//! MAIN python thread pegged 99% CPU in PURE USERSPACE (utime ≫ stime, 23591
//! nonvoluntary vs 244 voluntary ctx switches — it NEVER entered the kernel
//! and NEVER yielded). 12 already-arrived remote `TaskComplete`s were never
//! ingested; the 10-min stats cadence went dead; the off-thread liveness
//! beacon kept flowing (so no failover). 50+ min frozen until scancel.
//!
//! # The hypothesis this probe tests
//!
//! The whole stack runs on ONE current-thread tokio runtime + `LocalSet`
//! (`crates/dynrunner-pyo3/src/managers/secondary/run.rs`:
//! `rt.block_on(local.run_until(...))` under `py.detach`). `recv_peer`
//! (`peer/transport_impl.rs:101`) is an inner `loop { tokio::select! { ... } }`
//! over SIX mpsc receivers; five keep a network-held sender clone, but the
//! sixth — `reconnect_tick_rx` — has its ONLY sender (non-`cfg(test)`) MOVED
//! into the spawned 5-second reconnect-tick task (`peer/mod.rs:243`). If that
//! task ends, `reconnect_tick_rx` closes. A closed `UnboundedReceiver::recv()`
//! resolves `None` synchronously; the arm body yields `None`; the inner loop
//! re-polls. The hypothesis: that is a non-yielding busy-spin (matching the
//! /proc signature) that starves the operational loop's `inbox.recv()` so the
//! 12 completions never ingest.
//!
//! # Why the hypothesis is FALSE (what this test proves)
//!
//! tokio's **cooperative-scheduling budget** instruments `mpsc::recv()`: after
//! a bounded number of immediately-ready polls, `recv()` itself returns
//! `Pending` (budget-forced) EVEN on a closed channel, so the inner `select!`
//! parks and other tasks run; the budget resets and it spins again briefly.
//! The closed-channel arm therefore produces a coop-THROTTLED spin that DOES
//! yield — it burns some CPU but does NOT monopolise the single executor
//! thread. This test demonstrates that a co-scheduled mpsc-consumer sibling
//! (modelling the operational loop's `inbox.recv()`) drains EVERY item even
//! with `reconnect_tick_rx` closed — i.e. the closed channel does NOT produce
//! the production full-freeze. The real production spin must be a
//! NON-cooperative loop (one that never awaits a coop-instrumented resource —
//! e.g. a synchronous `try_recv`-drain loop fed a self-requeueing command, or
//! Python bytecode under the GIL), which this probe rules `recv_peer` OUT of.
//!
//! Run with `--ignored` to execute the probe (it is `#[ignore]` so it does
//! not gate CI — it asserts a NEGATIVE result that documents a falsification,
//! not an invariant the code must preserve).

use std::sync::{Arc, Mutex};

use dynrunner_protocol_primary_secondary::PeerTransport;
use tokio::sync::mpsc;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

use super::super::PeerNetwork;
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};

/// Items the producer feeds the consumer sibling. A healthy (or merely
/// coop-throttled) executor drains all of them in the window; only a TRULY
/// non-cooperative monopolising spin would starve them.
const ITEMS: usize = 50_000;

/// Drive a `recv_peer` (which never resolves) concurrently with an
/// mpsc-consumer sibling on the SAME current-thread runtime; return how many
/// of [`ITEMS`] the sibling drains before the window closes.
///
/// `close_tick`: when true, replace `reconnect_tick_rx` with the receiver of
/// a fresh channel whose sender is dropped immediately — the exact state the
/// moved-in tick-task sender leaves behind when that task ends, so
/// `reconnect_tick_rx.recv()` resolves `None` immediately and forever.
async fn items_drained_under_recv_peer(close_tick: bool) -> usize {
    let mut peer: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();

    if close_tick {
        let (tick_tx, tick_rx) = mpsc::unbounded_channel::<()>();
        drop(tick_tx);
        peer.reconnect_tick_rx = tick_rx;
    }

    let (work_tx, mut work_rx) = mpsc::unbounded_channel::<()>();
    for _ in 0..ITEMS {
        work_tx.send(()).unwrap();
    }
    drop(work_tx);

    let consumer = tokio::task::spawn_local(async move {
        let mut drained = 0usize;
        while work_rx.recv().await.is_some() {
            drained += 1;
        }
        drained
    });

    tokio::select! {
        _ = peer.recv_peer() => {
            panic!("recv_peer unexpectedly resolved — neither arm should yield a delivered frame");
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {}
    }

    consumer.abort();
    consumer.await.unwrap_or_default()
}

/// THE PROBE. Closing `reconnect_tick_rx` does NOT freeze the runtime: tokio
/// coop throttling lets the co-scheduled mpsc consumer drain every item even
/// while `recv_peer`'s closed arm re-polls. This is the FALSIFICATION of the
/// "closed `reconnect_tick_rx` → production freeze" hypothesis — `recv_peer`
/// is ruled out as the non-cooperative spinner.
#[tokio::test(flavor = "current_thread")]
#[ignore = "RCA probe: asserts a NEGATIVE (falsification) result, not a CI invariant"]
async fn closed_reconnect_tick_does_not_freeze_runtime_under_coop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Control: channel open → arm parks → consumer drains all.
            let healthy = items_drained_under_recv_peer(false).await;
            assert_eq!(
                healthy, ITEMS,
                "control (open tick channel): recv_peer parks, consumer drains all {ITEMS}",
            );

            // Probe: channel closed. If the closed arm produced a NON-yielding
            // monopolising spin (the hypothesis), the consumer would be
            // starved (< ITEMS). It is NOT — coop throttling lets it finish.
            let with_closed_tick = items_drained_under_recv_peer(true).await;
            assert_eq!(
                with_closed_tick, ITEMS,
                "FALSIFICATION: a closed `reconnect_tick_rx` does NOT monopolise the \
                 single-thread executor — tokio coop throttling lets the co-scheduled \
                 mpsc consumer drain all {ITEMS} items. The production full-freeze is \
                 therefore NOT explained by this closed-channel arm; recv_peer is ruled \
                 out as the non-cooperative spinner. (Observed: {with_closed_tick}.)",
            );
        })
        .await;
}

/// Drive `recv_peer` with a CLOSED `reconnect_tick_rx` for a window, capturing
/// the framework log, and return how many of [`ITEMS`] a co-scheduled consumer
/// sibling drains plus how many times the tick-arm close WARN fired.
///
/// This is the GATED variant of [`items_drained_under_recv_peer`]: it always
/// closes the tick channel and additionally observes the one-shot gate's WARN
/// via the shared capture layer.
async fn drained_and_warns_with_closed_tick() -> (usize, usize) {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: Arc::clone(&records),
    };
    let subscriber = Registry::default().with(layer);
    // Thread-local default for the duration of this test — the
    // `current_thread` runtime + LocalSet share this thread, so the
    // spawn_local'd consumer and recv_peer see it; the guard clears it on
    // function exit.
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut peer: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();

    // Close the tick channel: replace the receiver with one whose sole sender
    // is dropped — the exact state the moved-in tick-task sender leaves when
    // the 5s tick task ends. `recv()` now resolves `None` forever.
    let (tick_tx, tick_rx) = mpsc::unbounded_channel::<()>();
    drop(tick_tx);
    peer.reconnect_tick_rx = tick_rx;

    let (work_tx, mut work_rx) = mpsc::unbounded_channel::<()>();
    for _ in 0..ITEMS {
        work_tx.send(()).unwrap();
    }
    drop(work_tx);

    let consumer = tokio::task::spawn_local(async move {
        let mut drained = 0usize;
        while work_rx.recv().await.is_some() {
            drained += 1;
        }
        drained
    });

    tokio::select! {
        _ = peer.recv_peer() => {
            panic!("recv_peer unexpectedly resolved — no arm should yield a delivered frame");
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {}
    }

    consumer.abort();
    let drained = consumer.await.unwrap_or_default();

    let warns = records
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.message.contains("reconnect-tick channel closed"))
        .count();
    (drained, warns)
}

/// THE GATE TEST. With the one-shot `tick_closed` gate in `recv_peer`, a closed
/// `reconnect_tick_rx` fires its `None` branch EXACTLY ONCE (one WARN) and then
/// the arm is DISABLED for the rest of the call — so `recv_peer` PARKS on its
/// remaining (all-quiet) arms instead of re-polling the always-ready closed
/// receiver. The load-bearing observable is the WARN count: without the gate
/// the closed arm would re-fire every loop turn (and only tokio coop throttling
/// — not the code — would keep it from a hard freeze, the dead end the
/// falsification probe above documents). With the gate the arm is structurally
/// silenced after one report. The drained count confirms the co-scheduled
/// consumer is not starved.
#[tokio::test(flavor = "current_thread")]
async fn closed_reconnect_tick_arm_is_gated_off_after_one_warn() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (drained, warns) = drained_and_warns_with_closed_tick().await;
            assert_eq!(
                warns, 1,
                "the one-shot gate must fire the close WARN EXACTLY once and then \
                 disable the arm; a count != 1 means the arm is either never gated \
                 (it would re-warn every loop turn, the busy-spin) or never reached. \
                 (Observed: {warns} warns.)",
            );
            assert_eq!(
                drained, ITEMS,
                "with the tick arm gated off, recv_peer parks on its quiet arms and the \
                 co-scheduled consumer drains all {ITEMS} items. (Observed: {drained}.)",
            );
        })
        .await;
}
