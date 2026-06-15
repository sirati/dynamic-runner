//! Peer-lifecycle dispatcher listener that forwards the RESPAWN-RELEVANT
//! lifecycle events to the respawn pipeline's operational-loop arm.

use crate::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};

/// [`LifecycleListener`] that forwards the respawn-relevant subset of
/// [`PeerLifecycleEvent`]s onto the supplied sender.
///
/// Single concern: forward exactly the events the respawn arm can act on.
/// The listener does not consult the budget, mint ids, or invoke the
/// spawner; it owns only the sender. Interpretation lives on the
/// operational `select!` arm (`dispatch_respawn_lifecycle`), the only site
/// with `&mut PrimaryCoordinator`: `Removed` becomes a spawn request.
/// `Added` events are NEVER respawn-relevant — the historical
/// "reconcile against pending replacements" path was deleted along with
/// the per-replacement revoke surface (the slurm-authoritative quantity
/// gate in `handler::dispatch_respawn_request` is what now prevents the
/// redundant-replacement scenario). Dropping `Added` here keeps the
/// respawn arm parked between deaths.
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
) -> Box<dyn LifecycleListener> {
    Box::new(RespawnDispatcherListener { event_tx })
}

struct RespawnDispatcherListener {
    event_tx: tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>,
}

impl LifecycleListener for RespawnDispatcherListener {
    fn on_event(&self, event: &PeerLifecycleEvent) {
        // Drop `Added` events unconditionally — the respawn pipeline no
        // longer reconciles joins (the revoke surface was removed; the
        // quantity gate carries that load now). Only `Removed` reaches
        // the operational arm.
        if let PeerLifecycleEvent::Added { .. } = event {
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

    /// A `Removed` is always respawn-relevant — it forwards onto the
    /// operational arm.
    #[test]
    fn removed_forwards() {
        let (tx, mut rx) = unbounded_channel();
        let listener = respawn_dispatcher_listener(tx);
        listener.on_event(&removed());
        assert_eq!(rx.try_recv().ok(), Some(removed()));
    }

    /// An `Added` is NEVER respawn-relevant after the revoke surface was
    /// removed — the listener drops it so the respawn arm parks instead
    /// of busy-waking on membership joins.
    #[test]
    fn added_is_dropped() {
        let (tx, mut rx) = unbounded_channel();
        let listener = respawn_dispatcher_listener(tx);
        listener.on_event(&added());
        assert!(rx.try_recv().is_err(), "an `Added` must never forward");
    }
}
