//! Batched matcher-trigger drain helper.
//!
//! Single concern: collapse a burst of [`MatcherTriggerEvent`]s into a
//! single [`MatcherBatch`] so the operational `select!` arm walks
//! `Unfulfillable` tasks once per burst instead of once per event.
//! Pre-batching the consumer matcher fired N times when an observer
//! broadcast N outpaths in quick succession; this helper drops that
//! to a single fire per Unfulfillable task per burst.
//!
//! Module boundary:
//! - Input: a `tokio::sync::mpsc::UnboundedReceiver<MatcherTriggerEvent>`,
//!   owned by the operational loop's local state. The apply path's
//!   sender side lives on `ClusterState` (installed via
//!   `install_matcher_trigger_sender`, mirroring `lifecycle_tx`).
//! - Output: an `Option<MatcherBatch>` — `Some(batch)` after a burst
//!   ends (50ms idle since the last recv); `None` on `rx.recv() == None`
//!   (every sender dropped — coordinator exit cue, the arm goes silent
//!   for the remainder of the loop).
//! - The helper does NOT itself call the matcher or `cluster_state` —
//!   the caller (operational loop) does both, because both require
//!   `&mut self` and the apply-path borrow rules already constrain
//!   them to the same task.
//!
//! Why an async helper (not a spawn_local task):
//! - The peer-lifecycle dispatcher fans events out to listeners that
//!   own no shared state, so it can run on its own task. The matcher,
//!   in contrast, must walk `cluster_state.tasks` and fire
//!   `PrimaryCommand::ReinjectTask` against the coordinator's command
//!   channel — both naturally live behind `&mut self` on the
//!   operational loop's borrow. Putting the drain helper in the
//!   `select!` keeps the matcher invocation co-located with the
//!   coordinator without cross-task synchronization.

use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use super::event::MatcherTriggerEvent;

/// Idle-window after which the pipeline considers a burst of trigger
/// events complete and flushes the batch to the matcher. 50ms is short
/// enough that a single observer's holdings broadcast (one or two
/// quick applies) flushes in well under one heartbeat tick, and long
/// enough to swallow the 50-outpath bursts the brief calls out
/// (the matcher fires once instead of 50 times).
pub const MATCHER_BATCH_IDLE_WINDOW: Duration = Duration::from_millis(50);

/// One collapsed batch of trigger events, ready to be processed by the
/// operational-loop's matcher walk.
///
/// `holdings` is the most-recent snapshot in the burst — earlier
/// snapshots from the same burst are discarded because the matcher's
/// "do you want me to reinject?" answer is a function of the *current*
/// cluster view, not the trajectory.
#[derive(Clone, Debug)]
pub struct MatcherBatch {
    /// The snapshot from the last event in the burst.
    pub holdings: std::collections::HashMap<String, std::collections::HashSet<String>>,
}

/// Drain `rx` for one batch: await the first event, then keep
/// `tokio::time::timeout(idle_window, rx.recv())`-looping while events
/// continue to arrive; on timeout (idle window elapsed since the last
/// recv), return the most-recent snapshot as a [`MatcherBatch`].
///
/// Returns `None` only when `rx.recv() == None` BEFORE the first event
/// of a (would-be) batch — i.e. every sender has been dropped. The
/// caller then disables this arm of the `select!` for the rest of
/// the loop (same shape as the command-channel arm's
/// "all senders dropped" handling in `lifecycle.rs`).
///
/// `idle_window` is a parameter (not a hard-coded constant in the body)
/// so tests can tighten the window without busy-waiting; production
/// code passes [`MATCHER_BATCH_IDLE_WINDOW`].
pub async fn drain_matcher_batch(
    rx: &mut UnboundedReceiver<MatcherTriggerEvent>,
    idle_window: Duration,
) -> Option<MatcherBatch> {
    // Block until the first event of the burst. `None` here means
    // all senders dropped before any event landed — the caller's
    // termination cue.
    let first = rx.recv().await?;
    let mut latest = first.holdings;

    // Drain follow-up events that arrive within the idle window of
    // the previous one. `tokio::time::timeout(idle, recv())` returns
    // `Err(Elapsed)` on idle-window expiry — that's the batch
    // boundary. `Ok(Some(...))` extends the burst; `Ok(None)` means
    // the channel closed mid-burst, which we treat as end-of-batch
    // (the partial batch is still valid).
    loop {
        match tokio::time::timeout(idle_window, rx.recv()).await {
            Ok(Some(event)) => {
                latest = event.holdings;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    Some(MatcherBatch { holdings: latest })
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    /// Two events arriving within the idle window collapse into one
    /// batch; the batch's holdings are the LATEST snapshot (the earlier
    /// snapshot is discarded). Pins the burst-collapsing contract.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_collapses_burst_to_latest_snapshot() {
        let (tx, mut rx) = unbounded_channel::<MatcherTriggerEvent>();
        let mut h1 = HashMap::new();
        h1.insert("peer-a".to_string(), HashSet::from(["out-1".to_string()]));
        let mut h2 = HashMap::new();
        h2.insert(
            "peer-a".to_string(),
            HashSet::from(["out-1".to_string(), "out-2".to_string()]),
        );

        // Two sends arrive faster than the idle window can elapse.
        tx.send(MatcherTriggerEvent { holdings: h1 }).unwrap();
        tx.send(MatcherTriggerEvent { holdings: h2.clone() }).unwrap();
        // Drop sender so once the burst completes the helper would
        // return None on the next recv — but the idle-window timeout
        // fires first because the two sends are already in the queue.
        drop(tx);

        let batch = drain_matcher_batch(&mut rx, Duration::from_millis(50))
            .await
            .expect("burst should produce a batch");
        assert_eq!(batch.holdings, h2);
    }

    /// A closed channel (no events ever) returns `None` so the caller
    /// can disable the `select!` arm. Pins the termination cue.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_returns_none_on_closed_channel_with_no_events() {
        let (tx, mut rx) = unbounded_channel::<MatcherTriggerEvent>();
        drop(tx);
        let result = drain_matcher_batch(&mut rx, Duration::from_millis(50)).await;
        assert!(result.is_none());
    }
}
