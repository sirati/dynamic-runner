//! Failover detection: per-secondary heartbeat tracking, dead-secondary
//! requeue, and `TimeoutDetected` broadcast (F1).
//!
//! The primary updates a `last_keepalive` timestamp every time it observes
//! a `Keepalive` message from a secondary. On a periodic tick the
//! operational loop calls [`SecondaryHeartbeatReport::collect`] to fold the
//! map into a [`SecondaryHeartbeatReport`]; for every secondary in the
//! `dead` list the loop calls [`PrimaryCoordinator::requeue_dead_secondary`]
//! to take its in-flight tasks back into the pending pool, evict its
//! per-worker tracking, drop the connection state, and notify surviving
//! peers via `TimeoutDetected`.

use std::time::Instant;

use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{DistributedMessage, SecondaryTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

/// Outcome of a single periodic heartbeat sweep.
pub(super) struct SecondaryHeartbeatReport {
    /// Secondaries whose last keepalive is older than the configured death
    /// threshold. Each entry is `(secondary_id, last_keepalive_seen)`.
    pub(super) dead: Vec<DeadSecondary>,
}

pub(super) struct DeadSecondary {
    pub(super) secondary_id: String,
    pub(super) last_keepalive: Instant,
}

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator, I: Identifier>
    PrimaryCoordinator<T, S, E, I>
{
    /// Update the keepalive timestamp for a known secondary. No-op if the
    /// secondary id isn't registered (e.g. a stray message after death).
    pub(super) fn record_keepalive(&mut self, secondary_id: &str) {
        if self.secondaries.contains_key(secondary_id) {
            self.secondary_keepalives
                .insert(secondary_id.into(), Instant::now());
        }
    }

    /// Seed the keepalive timestamp at welcome time so the death deadline
    /// counts from when we first heard from the secondary, not from
    /// process start.
    pub(super) fn seed_keepalive(&mut self, secondary_id: &str) {
        self.secondary_keepalives
            .insert(secondary_id.into(), Instant::now());
    }

    /// Inspect every tracked secondary and decide which ones missed too many
    /// keepalives to still be considered alive.
    pub(super) fn collect_heartbeat_report(&self) -> SecondaryHeartbeatReport {
        let now = Instant::now();
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let mut dead = Vec::new();
        for (id, last) in &self.secondary_keepalives {
            if !self.secondaries.contains_key(id) {
                continue;
            }
            if now.duration_since(*last) > deadline {
                dead.push(DeadSecondary {
                    secondary_id: id.clone(),
                    last_keepalive: *last,
                });
            }
        }
        SecondaryHeartbeatReport { dead }
    }

    /// Send a `Keepalive` to every connected secondary. Secondaries use this
    /// to detect primary death (F2): if a secondary stops seeing primary
    /// keepalives for `keepalive_miss_threshold` intervals, it kicks off the
    /// failover election. Called from the operational loop on the same
    /// cadence as `collect_heartbeat_report`.
    pub(super) async fn broadcast_primary_keepalive(&mut self) {
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        if secondary_ids.is_empty() {
            return;
        }
        let msg = DistributedMessage::<I>::Keepalive {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.node_id.clone(),
            active_workers: self.workers.iter().filter(|w| !w.is_idle).count() as u32,
        };
        for secondary_id in secondary_ids {
            if let Err(e) = self.transport.send_to(&secondary_id, msg.clone()).await {
                tracing::debug!(
                    secondary = %secondary_id,
                    error = %e,
                    "primary keepalive delivery failed"
                );
            }
        }
    }

    /// Take in-flight tasks back, drop the secondary from the routable set,
    /// and broadcast a `TimeoutDetected` to every surviving secondary so
    /// they can prune the dead peer from their own peer maps.
    pub(super) async fn requeue_dead_secondary(
        &mut self,
        dead: DeadSecondary,
    ) -> Result<(), String> {
        let DeadSecondary {
            secondary_id,
            last_keepalive,
        } = dead;
        let last_seen_secs = last_keepalive.elapsed().as_secs_f64();
        tracing::warn!(
            secondary = %secondary_id,
            last_seen_s = last_seen_secs,
            keepalive_miss_threshold = self.config.keepalive_miss_threshold,
            "secondary missed keepalives; requeueing in-flight tasks"
        );

        let mut requeued = 0usize;
        let mut survivors_workers = Vec::with_capacity(self.workers.len());
        for mut worker in std::mem::take(&mut self.workers) {
            if worker.secondary_id == secondary_id {
                if let Some(binary) = worker.current_task.take() {
                    self.pending_binaries.push(binary);
                    requeued += 1;
                }
                worker.estimated_resources = ResourceMap::new();
                worker.is_idle = true;
                // Drop this worker — its host is dead.
                continue;
            }
            survivors_workers.push(worker);
        }
        self.workers = survivors_workers;

        self.secondaries.remove(&secondary_id);
        self.secondary_keepalives.remove(&secondary_id);

        if self
            .slurm_primary_id
            .as_ref()
            .map(|id| id == &secondary_id)
            .unwrap_or(false)
        {
            // The SLURM-primary just died; clear the pointer so caller can
            // promote a survivor before sending another FullTaskList.
            self.slurm_primary_id = None;
        }

        // Notify every surviving secondary so they prune the dead peer.
        let timeout_msg = DistributedMessage::<I>::TimeoutDetected {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            timed_out_secondary_id: secondary_id.clone(),
            last_seen: timestamp_now() - last_seen_secs,
        };
        let surviving: Vec<String> = self.secondaries.keys().cloned().collect();
        for peer_id in surviving {
            if let Err(e) = self
                .transport
                .send_to(&peer_id, timeout_msg.clone())
                .await
            {
                tracing::warn!(peer = %peer_id, error = %e, "TimeoutDetected delivery failed");
            }
        }

        tracing::info!(
            secondary = %secondary_id,
            requeued_tasks = requeued,
            surviving_secondaries = self.secondaries.len(),
            pending = self.pending_binaries.len(),
            "dead secondary cleaned up"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use dynrunner_core::{BinaryInfo, ResourceMap};
    use dynrunner_protocol_primary_secondary::DistributedMessage;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc as tokio_mpsc;

    use crate::primary::{PrimaryConfig, PrimaryCoordinator, RemoteWorkerState};
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::ResourceEstimator;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[derive(Clone)]
    struct FixedEstimator;
    impl ResourceEstimator for FixedEstimator {
        fn estimate(&self, _size: u64) -> ResourceMap {
            ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1)])
        }
    }

    fn config(keepalive_interval: Duration, miss_threshold: u32) -> PrimaryConfig {
        PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval,
            keepalive_miss_threshold: miss_threshold,
        }
    }

    fn empty_transport() -> (
        ChannelSecondaryTransportEnd<TestId>,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ) {
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let (sec_tx, sec_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("dead-sec".into(), sec_tx);
        (
            ChannelSecondaryTransportEnd {
                outgoing,
                incoming_rx,
            },
            sec_rx,
            incoming_tx,
        )
    }

    /// Build a primary with one registered secondary that owns one in-flight
    /// task; advance time past the death threshold; verify the heartbeat
    /// report flags the secondary as dead and `requeue_dead_secondary`
    /// requeues the task and drops the worker.
    #[tokio::test(flavor = "current_thread")]
    async fn dead_secondary_requeues_in_flight_task() {
        let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );

        // Register the secondary at the connection level — the heartbeat
        // tracker only flags secondaries it knows about.
        let conn = SecondaryConnection::new("dead-sec".into()).receive_welcome(
            1,
            vec![],
            "host".into(),
            0,
            None,
        );
        primary.secondaries.insert(
            "dead-sec".into(),
            SecondaryConnectionState::Handshaking(conn),
        );
        primary.seed_keepalive("dead-sec");

        // Stage one in-flight task on a single virtual worker.
        let in_flight = BinaryInfo {
            path: std::path::PathBuf::from("victim.bin"),
            size: 100,
            identifier: TestId("victim".into()),
        };
        primary.workers.push(RemoteWorkerState {
            worker_id: 0,
            secondary_id: "dead-sec".into(),
            resource_budgets: ResourceMap::new(),
            current_task: Some(in_flight.clone()),
            estimated_resources: ResourceMap::new(),
            is_idle: false,
        });

        // Sleep past `keepalive_interval * miss_threshold` so the deadline
        // expires, then collect the report.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let report = primary.collect_heartbeat_report();
        assert_eq!(report.dead.len(), 1);
        assert_eq!(report.dead[0].secondary_id, "dead-sec");

        for dead in report.dead {
            primary.requeue_dead_secondary(dead).await.unwrap();
        }

        assert_eq!(primary.workers.len(), 0, "dead worker should be evicted");
        assert_eq!(primary.pending_binaries.len(), 1, "in-flight task requeued");
        assert_eq!(primary.pending_binaries[0].identifier.0, "victim");
        assert!(!primary.secondaries.contains_key("dead-sec"));
    }

    /// A secondary that's still sending keepalives stays in the routable
    /// set even when other secondaries die.
    #[tokio::test(flavor = "current_thread")]
    async fn live_secondary_is_not_falsely_declared_dead() {
        let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );

        let conn = SecondaryConnection::new("dead-sec".into()).receive_welcome(
            1,
            vec![],
            "host".into(),
            0,
            None,
        );
        primary.secondaries.insert(
            "dead-sec".into(),
            SecondaryConnectionState::Handshaking(conn),
        );
        primary.seed_keepalive("dead-sec");

        // Bump the keepalive within the deadline window so the heartbeat
        // report should leave it alone.
        tokio::time::sleep(Duration::from_millis(60)).await;
        primary.record_keepalive("dead-sec");
        tokio::time::sleep(Duration::from_millis(60)).await;
        let report = primary.collect_heartbeat_report();
        assert_eq!(report.dead.len(), 0);
    }
}

