//! Peer-lifecycle dispatcher listener that forwards lifecycle events
//! to the respawn pipeline's operational-loop arm.

use crate::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};

/// [`LifecycleListener`] that forwards every [`PeerLifecycleEvent`]
/// onto the supplied sender, verbatim.
///
/// Single concern: pure forwarding. The listener does not consult
/// the budget, does not mint ids, does not invoke the spawner, and
/// owns no state beyond the sender. Interpretation lives on the
/// operational `select!` arm (`dispatch_respawn_lifecycle`), which is
/// the only site with `&mut PrimaryCoordinator`: `Removed` becomes a
/// spawn request; `Added` reconciles the pending-replacement
/// bookkeeping (a re-admitted original revokes its still-pending
/// replacement; a joining replacement clears its entry). Both event
/// kinds MUST ride the same channel so the loop observes them in
/// apply order — a replacement-joined / original-re-admitted
/// interleaving is resolved by whichever `PeerJoined` applied first.
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
