//! Shutdown hook: stop all worker subprocesses cleanly.
//!
//! Single concern: hand off the pool's bulk-stop to the worker
//! lifecycle layer. The body is trivial because the actual
//! teardown work lives inside `WorkerPool::stop_all` — this layer
//! exists only to expose the call as an inherent method on
//! `SecondaryCoordinator` for the run-finalisation site.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(in crate::secondary) async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}
