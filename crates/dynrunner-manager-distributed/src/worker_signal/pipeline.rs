//! Batched worker-signal drain helper.
//!
//! Single concern: coalesce a burst of [`WorkerMgmtSignal`]s into a
//! single [`WorkerSignalBatch`] so worker management's operational
//! `select!` arm reacts once per burst instead of once per signal.
//! When phase/task management emits a flurry of signals (e.g. a wave of
//! `TasksAdded` as a phase queues work), the drain helper hands worker
//! management one batch covering the whole burst.
//!
//! Module boundary:
//! - Input: a `tokio::sync::mpsc::UnboundedReceiver<WorkerMgmtSignal>`,
//!   owned by worker management's operational loop. The emit side's
//!   sender lives on `ClusterState` (installed via
//!   `install_worker_mgmt_sender`, mirroring `lifecycle_tx`).
//! - Output: an `Option<WorkerSignalBatch>` — `Some(batch)` after a
//!   burst ends (50ms idle since the last recv); `None` on
//!   `rx.recv() == None` (every sender dropped — worker-management exit
//!   cue, the arm goes silent for the remainder of the loop).
//! - The helper does NOT itself act on any signal — the caller (worker
//!   management's loop) does, because acting on a signal requires
//!   `&mut self` against worker-management state the drain helper has
//!   no reference to.
//!
//! Why an async helper (not a spawned task): worker management must
//! react against its own `&mut self` state (worker registry, dispatch
//! queue), so the drain lives inside its operational `select!` rather
//! than on a detached task — keeping the reaction next to the
//! state it mutates without cross-task synchronization.
//!
//! Unlike the matcher pipeline — whose batch keeps only the *latest*
//! holdings snapshot because the matcher's answer is a function of the
//! current cluster view — worker signals are discrete events that must
//! each be acted on (a `RunShouldFail` cannot be discarded because a
//! later `TasksAdded` arrived). The batch therefore carries EVERY
//! signal in the burst, in arrival order.

use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use super::signal::WorkerMgmtSignal;

/// Idle-window after which the pipeline considers a burst of worker
/// signals complete and flushes the batch. 50ms mirrors the matcher
/// pipeline's window: short enough that a single phase-transition's
/// signals flush in well under one heartbeat tick, long enough to
/// swallow a multi-signal burst into one reaction.
pub const WORKER_SIGNAL_BATCH_IDLE_WINDOW: Duration = Duration::from_millis(50);

/// One collapsed batch of worker-management signals, ready to be
/// processed by worker management's operational-loop reaction.
///
/// `signals` holds every signal in the burst, in arrival order. Unlike
/// the matcher batch (which discards all but the latest snapshot), no
/// signal is dropped — each is a discrete event worker management must
/// see.
#[derive(Clone, Debug)]
pub struct WorkerSignalBatch {
    /// Every signal in the burst, in arrival order.
    pub signals: Vec<WorkerMgmtSignal>,
}

/// Drain `rx` for one batch: await the first signal, then keep
/// `tokio::time::timeout(idle_window, rx.recv())`-looping while signals
/// continue to arrive; on timeout (idle window elapsed since the last
/// recv), return every accumulated signal as a [`WorkerSignalBatch`].
///
/// Returns `None` only when `rx.recv() == None` BEFORE the first signal
/// of a (would-be) batch — i.e. every sender has been dropped. The
/// caller then disables this arm of the `select!` for the rest of the
/// loop (same shape as the matcher pipeline's "all senders dropped"
/// handling).
///
/// `idle_window` is a parameter (not a hard-coded constant in the body)
/// so tests can tighten the window without busy-waiting; production
/// code passes [`WORKER_SIGNAL_BATCH_IDLE_WINDOW`].
pub async fn drain_worker_signal_batch(
    rx: &mut UnboundedReceiver<WorkerMgmtSignal>,
    idle_window: Duration,
) -> Option<WorkerSignalBatch> {
    // Block until the first signal of the burst. `None` here means all
    // senders dropped before any signal landed — the caller's
    // termination cue.
    let first = rx.recv().await?;
    let mut signals = vec![first];

    // Drain follow-up signals that arrive within the idle window of the
    // previous one. `tokio::time::timeout(idle, recv())` returns
    // `Err(Elapsed)` on idle-window expiry — that's the batch boundary.
    // `Ok(Some(...))` extends the burst; `Ok(None)` means the channel
    // closed mid-burst, which we treat as end-of-batch (the partial
    // batch is still valid).
    loop {
        match tokio::time::timeout(idle_window, rx.recv()).await {
            Ok(Some(signal)) => {
                signals.push(signal);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Some(WorkerSignalBatch { signals })
}

#[cfg(test)]
mod tests {
    use dynrunner_core::PhaseId;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    /// A burst of signals arriving within the idle window collapses into
    /// one batch that preserves EVERY signal in arrival order — no
    /// signal is discarded (unlike the matcher's latest-only collapse).
    /// Pins the burst-coalescing contract.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_coalesces_burst_preserving_all_signals_in_order() {
        let (tx, mut rx) = unbounded_channel::<WorkerMgmtSignal>();

        let s1 = WorkerMgmtSignal::TasksAdded;
        let s2 = WorkerMgmtSignal::PhaseStartedNeedsWorkers {
            phase: PhaseId::from("phase-a"),
            min: 3,
        };
        let s3 = WorkerMgmtSignal::RunShouldFail {
            reason: "boom".to_string(),
        };

        // Three sends arrive faster than the idle window can elapse.
        tx.send(s1.clone()).unwrap();
        tx.send(s2.clone()).unwrap();
        tx.send(s3.clone()).unwrap();
        // Drop sender so once the burst completes the helper would
        // return None on the next recv — but the idle-window timeout
        // fires first because the sends are already queued.
        drop(tx);

        let batch = drain_worker_signal_batch(&mut rx, Duration::from_millis(50))
            .await
            .expect("burst should produce a batch");
        assert_eq!(batch.signals, vec![s1, s2, s3]);
    }

    /// A closed channel (no signals ever) returns `None` so the caller
    /// can disable the `select!` arm. Pins the termination cue.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_returns_none_on_closed_channel_with_no_signals() {
        let (tx, mut rx) = unbounded_channel::<WorkerMgmtSignal>();
        drop(tx);
        let result = drain_worker_signal_batch(&mut rx, Duration::from_millis(50)).await;
        assert!(result.is_none());
    }
}
