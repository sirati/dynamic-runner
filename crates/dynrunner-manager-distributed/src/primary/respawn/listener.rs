//! Peer-lifecycle dispatcher listener that enqueues respawn requests.

use crate::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};

use super::types::RespawnRequest;

/// [`LifecycleListener`] that converts `PeerLifecycleEvent::Removed`
/// into a [`RespawnRequest`] on the supplied sender.
///
/// Single concern: pure transformation. The listener does not consult
/// the budget, does not mint ids, does not invoke the spawner, and
/// owns no state beyond the sender. Everything else lives on the
/// operational `select!` arm, which is the only site with `&mut
/// PrimaryCoordinator`.
///
/// `Added` events are dropped silently — the respawn pipeline only
/// reacts to deaths. A future telemetry listener (also registered
/// off-apply) can observe `Added` independently without changing this
/// listener.
///
/// Channel shape: the request channel is unbounded
/// (`tokio::sync::mpsc::UnboundedSender::send` is sync and infallible
/// on the value side), so the dispatcher task (which calls `on_event`
/// synchronously) NEVER blocks and NEVER drops. Mass-death-grace
/// finalize bursts that previously blew past the legacy bounded cap
/// of 256 now enqueue every death; the operational-loop arm drains
/// at the rate of one `dispatch_respawn_request` per iteration, and
/// the total-budget cap on `RespawnBudget::max_total` ensures only the
/// first N drain past acceptance — the rest land as
/// `RejectTotalBudget` decisions, keeping memory bounded.
pub fn respawn_dispatcher_listener(
    request_tx: tokio::sync::mpsc::UnboundedSender<RespawnRequest>,
) -> Box<dyn LifecycleListener> {
    Box::new(RespawnDispatcherListener { request_tx })
}

struct RespawnDispatcherListener {
    request_tx: tokio::sync::mpsc::UnboundedSender<RespawnRequest>,
}

impl LifecycleListener for RespawnDispatcherListener {
    fn on_event(&self, event: &PeerLifecycleEvent) {
        match event {
            PeerLifecycleEvent::Removed { id, cause } => {
                let req = RespawnRequest {
                    original_id: id.clone(),
                    cause: cause.clone(),
                };
                // `UnboundedSender::send` only fails when every
                // receiver has been dropped — i.e. the operational
                // loop is gone. Log at debug level: this happens
                // during normal teardown when the lifecycle
                // dispatcher outlives the operational loop by a
                // tick. There is no actionable failure here.
                if let Err(e) = self.request_tx.send(req) {
                    tracing::debug!(
                        target: "dynrunner_respawn",
                        peer_id = %id,
                        cause = ?cause,
                        error = %e,
                        "respawn request channel closed; receiver gone",
                    );
                }
            }
            PeerLifecycleEvent::Added { .. } => {
                // Added events are out of scope for the respawn
                // pipeline; a separate listener can observe them
                // without this one needing to know.
            }
        }
    }
}
