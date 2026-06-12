//! Peer-lifecycle dispatcher listener that forwards the RESPAWN-RELEVANT
//! lifecycle events to the respawn pipeline's operational-loop arm.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};

/// [`LifecycleListener`] that forwards the respawn-relevant subset of
/// [`PeerLifecycleEvent`]s onto the supplied sender.
///
/// Single concern: forward exactly the events the respawn arm can act on.
/// The listener does not consult the budget, mint ids, or invoke the
/// spawner; it owns only the sender and the shared "awaiting a join" gate.
/// Interpretation lives on the operational `select!` arm
/// (`dispatch_respawn_lifecycle`), the only site with
/// `&mut PrimaryCoordinator`: `Removed` becomes a spawn request; `Added`
/// reconciles the pending-replacement bookkeeping (a re-admitted original
/// revokes its still-pending replacement; a joining replacement clears its
/// entry).
///
/// Relevance gate: a `Removed` event is ALWAYS forwarded — a death is a
/// spawn trigger regardless of pipeline state. An `Added` event is
/// forwarded ONLY while the `awaiting_join` gate reads `true`, i.e. while
/// at least one replacement is pending — the sole window in which
/// `reconcile_replacements_on_join` can do anything. With no replacement
/// pending, every `Added` is a guaranteed no-op, so dropping it here keeps
/// the respawn arm parked (the membership-join busy-arm fix) WITHOUT
/// breaking the apply-order contract: whenever the order actually matters
/// (a replacement IS pending) the gate is `true` and BOTH event kinds ride
/// the same channel in apply order, exactly as before. The gate is set at
/// accept time (the `pending_replacements` insert) BEFORE the spawn future
/// runs, so the replacement's own — far later — `PeerJoined` always finds
/// it open.
///
/// Channel shape: the channel is unbounded
/// (`tokio::sync::mpsc::UnboundedSender::send` is sync and infallible
/// on the value side), so the dispatcher task (which calls `on_event`
/// synchronously) NEVER blocks and NEVER drops. Mass-death-grace
/// finalize bursts that previously blew past the legacy bounded cap
/// of 256 now enqueue every death; the operational-loop arm drains
/// at the rate of one `dispatch_respawn_lifecycle` per iteration, and
/// the total-budget cap on `RespawnBudget::max_total` ensures only the
/// first N drain past acceptance — the rest land as
/// `RejectTotalBudget` decisions, keeping memory bounded.
pub fn respawn_dispatcher_listener(
    event_tx: tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>,
    awaiting_join: Arc<AtomicBool>,
) -> Box<dyn LifecycleListener> {
    Box::new(RespawnDispatcherListener {
        event_tx,
        awaiting_join,
    })
}

struct RespawnDispatcherListener {
    event_tx: tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>,
    awaiting_join: Arc<AtomicBool>,
}

impl LifecycleListener for RespawnDispatcherListener {
    fn on_event(&self, event: &PeerLifecycleEvent) {
        // Drop `Added` events while no replacement is pending — they
        // cannot do respawn work, so forwarding them only busy-wakes the
        // respawn arm. `Removed` is always relevant.
        if let PeerLifecycleEvent::Added { .. } = event
            && !self.awaiting_join.load(Ordering::Relaxed)
        {
            return;
        }
        // `UnboundedSender::send` only fails when every receiver has
        // been dropped — i.e. the operational loop is gone. Log at
        // debug level: this happens during normal teardown when the
        // lifecycle dispatcher outlives the operational loop by a
        // tick. There is no actionable failure here.
        if let Err(e) = self.event_tx.send(event.clone()) {
            tracing::debug!(
                target: "dynrunner_respawn",
                event = ?event,
                error = %e,
                "respawn lifecycle channel closed; receiver gone",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_lifecycle::RemovalCause;
    use tokio::sync::mpsc::unbounded_channel;

    fn removed() -> PeerLifecycleEvent {
        PeerLifecycleEvent::Removed {
            id: "secondary-0".into(),
            cause: RemovalCause::KeepaliveMiss,
        }
    }
    fn added() -> PeerLifecycleEvent {
        PeerLifecycleEvent::Added {
            id: "secondary-0".into(),
            is_observer: false,
        }
    }

    /// A `Removed` is always respawn-relevant — it forwards whether or not
    /// a replacement is pending (a closed gate must never swallow a death).
    #[test]
    fn removed_forwards_regardless_of_the_gate() {
        for gate_open in [false, true] {
            let (tx, mut rx) = unbounded_channel();
            let gate = Arc::new(AtomicBool::new(gate_open));
            let listener = respawn_dispatcher_listener(tx, gate);
            listener.on_event(&removed());
            assert_eq!(
                rx.try_recv().ok(),
                Some(removed()),
                "a removal must forward with gate_open={gate_open}",
            );
        }
    }

    /// An `Added` forwards ONLY while the gate is open (a replacement is
    /// pending). A closed gate drops it — the busy-arm fix: an idle
    /// pipeline never wakes the respawn arm on a membership join.
    #[test]
    fn added_forwards_only_when_a_replacement_is_pending() {
        // Gate closed: dropped.
        let (tx, mut rx) = unbounded_channel();
        let gate = Arc::new(AtomicBool::new(false));
        let listener = respawn_dispatcher_listener(tx, Arc::clone(&gate));
        listener.on_event(&added());
        assert!(
            rx.try_recv().is_err(),
            "an `Added` must be dropped while no replacement is pending",
        );

        // Gate flips open (a death just dispatched a replacement): now it
        // forwards, so the reconcile path observes the join in apply order.
        gate.store(true, Ordering::Relaxed);
        listener.on_event(&added());
        assert_eq!(
            rx.try_recv().ok(),
            Some(added()),
            "an `Added` must forward while a replacement is pending",
        );
    }
}
