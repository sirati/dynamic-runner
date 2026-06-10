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
//! - Output: an `Option<WorkerSignalBatch>` — `Some(batch)` carrying the
//!   first signal to arrive plus everything queued behind it; `None` on
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
//! # Cancellation safety is LOAD-BEARING
//!
//! The sole production caller is one arm of the operational loop's
//! `select!`, so this future is dropped every time a SIBLING arm wins
//! an iteration. The helper is therefore built so that a signal is
//! consumed off the channel ONLY on the poll that COMPLETES the
//! future: one cancel-safe `recv().await` for the first signal,
//! followed by a strictly synchronous `try_recv` sweep — no await
//! point exists between consumption and return. The previous
//! generation of this helper awaited a 50ms idle window AFTER
//! consuming (holding the batch in a future-local Vec); a competing
//! arm readying inside that window — on a busy production mesh,
//! near-always — cancelled the future and DESTROYED the consumed
//! signals. The lost `TasksAdded` of a mid-run injected batch starved
//! proactive dispatch entirely, and the secondaries' 60s-capped
//! `TaskRequest` re-polls then packed the whole batch onto the
//! first-polling secondaries (asm-tokenizer run_20260610_145529,
//! 14/12/2 with twelve zeros after a 60-90s stall). Pinned by
//! `primary::tests::injected_spread::
//! injection_during_cascade_dispatches_despite_busy_inbox`.
//!
//! Burst coalescing is preserved: every real burst is emitted
//! synchronously inside another arm's handler (a phase transition, a
//! spawn apply), so by the time this arm is next polled the whole
//! burst is already queued and the `try_recv` sweep collects it into
//! one batch. A burst spanning awaits simply yields two reactions —
//! the recheck is idempotent, so reacting twice is correct where
//! reacting zero times was the bug.
//!
//! Unlike the matcher pipeline — whose batch keeps only the *latest*
//! holdings snapshot because the matcher's answer is a function of the
//! current cluster view — worker signals are discrete events that must
//! each be acted on (a `RunShouldFail` cannot be discarded because a
//! later `TasksAdded` arrived). The batch therefore carries EVERY
//! signal in the burst, in arrival order.

use tokio::sync::mpsc::UnboundedReceiver;

use super::signal::WorkerMgmtSignal;

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

/// Receive one batch CANCEL-SAFELY: await the first signal (the only
/// await point — `UnboundedReceiver::recv` consumes a signal only on
/// the poll that completes it), then synchronously sweep everything
/// already queued behind it into the same [`WorkerSignalBatch`]. The
/// future is Ready on the very poll that consumed the first signal, so
/// a `select!` that drops this future mid-flight can never destroy a
/// consumed signal — the cancellation-safety contract the module doc
/// names as load-bearing.
///
/// Returns `None` only when `rx.recv() == None` BEFORE the first signal
/// of a (would-be) batch — i.e. every sender has been dropped. The
/// caller then disables this arm of the `select!` for the rest of the
/// loop (same shape as the matcher pipeline's "all senders dropped"
/// handling).
pub async fn recv_worker_signal_batch(
    rx: &mut UnboundedReceiver<WorkerMgmtSignal>,
) -> Option<WorkerSignalBatch> {
    // Block until the first signal of the burst. `None` here means all
    // senders dropped before any signal landed — the caller's
    // termination cue.
    let first = rx.recv().await?;
    let mut signals = vec![first];

    // Synchronous sweep of the rest of the burst — every signal queued
    // by the time this arm was polled (the real burst shape: emitted
    // back-to-back inside another arm's handler). NO await from here to
    // the return, or a cancellation could destroy the consumed batch.
    while let Ok(signal) = rx.try_recv() {
        signals.push(signal);
    }
    Some(WorkerSignalBatch { signals })
}

/// Non-blocking drain: collect every signal CURRENTLY queued on `rx`
/// into one batch WITHOUT awaiting anything at all. `Some(batch)` iff at
/// least one signal was already queued; `None` when the channel was
/// momentarily empty (or closed) — there is nothing to react to right
/// now.
///
/// Why separate from [`recv_worker_signal_batch`]: that helper is the
/// operational loop's PARKED arm — it blocks until the first signal
/// arrives, then sweeps the queued burst. This helper is the SYNCHRONOUS
/// completion-gate pre-drain: when the loop is about to declare the run
/// complete, a `RunShouldFail` / `PolicyFatalExit` emitted onto the bus
/// in the SAME iteration that finished the last task is already queued
/// but has not yet been selected by the parked arm. Draining it here
/// (with no await) lets the loop observe the fatal break outcome before
/// the clean-completion exit wins.
pub fn try_collect_worker_signal_batch(
    rx: &mut UnboundedReceiver<WorkerMgmtSignal>,
) -> Option<WorkerSignalBatch> {
    let mut signals = Vec::new();
    while let Ok(signal) = rx.try_recv() {
        signals.push(signal);
    }
    if signals.is_empty() {
        None
    } else {
        Some(WorkerSignalBatch { signals })
    }
}

#[cfg(test)]
mod tests {
    use dynrunner_core::PhaseId;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    /// A queued burst collapses into one batch that preserves EVERY
    /// signal in arrival order — no signal is discarded (unlike the
    /// matcher's latest-only collapse). Pins the burst-coalescing
    /// contract.
    #[tokio::test(flavor = "current_thread")]
    async fn recv_coalesces_queued_burst_preserving_all_signals_in_order() {
        let (tx, mut rx) = unbounded_channel::<WorkerMgmtSignal>();

        let s1 = WorkerMgmtSignal::TasksAdded;
        let s2 = WorkerMgmtSignal::PhaseStartedNeedsWorkers {
            phase: PhaseId::from("phase-a"),
            min: 3,
        };
        let s3 = WorkerMgmtSignal::RunShouldFail {
            reason: "boom".to_string(),
        };

        // The whole burst is queued before the helper is polled — the
        // real burst shape (emitted back-to-back inside another arm's
        // handler).
        tx.send(s1.clone()).unwrap();
        tx.send(s2.clone()).unwrap();
        tx.send(s3.clone()).unwrap();
        drop(tx);

        let batch = recv_worker_signal_batch(&mut rx)
            .await
            .expect("burst should produce a batch");
        assert_eq!(batch.signals, vec![s1, s2, s3]);
    }

    /// A closed channel (no signals ever) returns `None` so the caller
    /// can disable the `select!` arm. Pins the termination cue.
    #[tokio::test(flavor = "current_thread")]
    async fn recv_returns_none_on_closed_channel_with_no_signals() {
        let (tx, mut rx) = unbounded_channel::<WorkerMgmtSignal>();
        drop(tx);
        let result = recv_worker_signal_batch(&mut rx).await;
        assert!(result.is_none());
    }

    /// THE cancellation-safety contract: a `select!` iteration that
    /// drops this future because a sibling arm won must not consume a
    /// single signal. The helper completes on the SAME poll that
    /// consumes the first signal, so a cancelled (dropped-while-
    /// pending) future has by construction consumed nothing — the
    /// signal is still on the channel for the next iteration. The
    /// pre-fix idle-window drain failed exactly this: it consumed,
    /// then parked, and the drop destroyed the consumed batch.
    #[tokio::test(flavor = "current_thread")]
    async fn recv_cancelled_while_pending_consumes_nothing() {
        let (tx, mut rx) = unbounded_channel::<WorkerMgmtSignal>();

        // Poll once while the channel is empty (pending), then DROP the
        // future — the select!-loses-the-race shape. A zero timeout
        // polls the inner future exactly once (tokio's `Timeout` polls
        // the value before the deadline) and elapses while it is
        // pending.
        {
            let fut = recv_worker_signal_batch(&mut rx);
            tokio::pin!(fut);
            assert!(
                tokio::time::timeout(std::time::Duration::ZERO, fut.as_mut())
                    .await
                    .is_err(),
                "empty channel must leave the helper pending"
            );
            // fut dropped here (cancellation).
        }

        // A signal emitted after the cancelled poll must be received in
        // full by the next invocation — nothing was consumed or lost.
        tx.send(WorkerMgmtSignal::TasksAdded).unwrap();
        let batch = recv_worker_signal_batch(&mut rx)
            .await
            .expect("the signal must survive the cancelled prior poll");
        assert_eq!(batch.signals, vec![WorkerMgmtSignal::TasksAdded]);
    }
}
