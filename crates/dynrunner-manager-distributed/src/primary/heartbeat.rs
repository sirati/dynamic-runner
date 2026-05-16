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

use std::time::{Duration, Instant};

use dynrunner_core::{BoundedString, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    Address, ClusterMutation, DistributedMessage, PeerTransport, RemovalCause, Scope,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::{PendingMassDeath, PrimaryCoordinator};
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

impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<T, P, S, E, I>
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
    ///
    /// Only secondaries in the Operational state are subject to the
    /// heartbeat threshold. Pre-Operational states (Handshaking,
    /// InitialAssigning) are still finishing setup and the secondary's
    /// own main loop — which sends keepalives — hasn't started yet
    /// (see `secondary/processing.rs` where `keepalive_interval.tick()`
    /// fires only post-`wait_for_setup`). Applying the threshold
    /// during setup falsely declares a slow-to-handshake secondary
    /// dead at the operational-loop transition: e.g. a SLURM
    /// secondary that took 38s for container startup, SSH-tunnel, and
    /// handshake gets dropped immediately on the first heartbeat
    /// tick, despite being healthy and processing tasks.
    pub(super) fn collect_heartbeat_report(&self) -> SecondaryHeartbeatReport {
        let now = Instant::now();
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let mut dead = Vec::new();
        for (id, last) in &self.secondary_keepalives {
            let state = match self.secondaries.get(id) {
                Some(s) => s,
                None => continue,
            };
            if !matches!(
                state,
                crate::state::SecondaryConnectionState::Operational(_)
            ) {
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
    ///
    /// Keepalive rides `peer_transport.send(Address::Broadcast(
    /// Scope::AllSecondaries), msg)` since Step 6 — the Step 5b
    /// `TunneledPeerTransport` made the primary a real mesh member, so
    /// the mesh-level broadcast reaches every connected secondary
    /// (over the same per-secondary tunnel writers the legacy
    /// `transport.broadcast` previously used directly, just routed via
    /// the mesh abstraction now). Bug class #3 collapses by virtue of
    /// the dead per-peer writer to the promoted peer no longer being
    /// invoked: post-demotion the new primary's writer is gone from
    /// the shared writer table, so the broadcast iterates over the
    /// LIVE peers only.
    ///
    /// `Scope::AllSecondaries` is the right scope (not `Scope::Mesh`):
    /// the primary itself is not in its own writer table — every
    /// shared-outgoing entry is a secondary — so the two scopes resolve
    /// identically in the current `TunneledPeerTransport` impl. The
    /// distinction is semantic: "AllSecondaries" is what F2 actually
    /// needs, regardless of which `PeerTransport` impl carries it.
    pub(super) async fn broadcast_primary_keepalive(&mut self) {
        if self.secondaries.is_empty() {
            return;
        }
        let msg = DistributedMessage::<I>::Keepalive {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.node_id.clone(),
            active_workers: self.workers.iter().filter(|w| !w.is_idle).count() as u32,
        };
        if let Err(error) = self
            .peer_transport
            .send(Address::Broadcast(Scope::AllSecondaries), msg)
            .await
        {
            // Keepalive failures are debug-level: a secondary mid-
            // disconnect generates one of these per tick until the
            // heartbeat-monitor declares it dead. Surfacing each at
            // warn would spam the log on an already-handled state
            // transition. (Pre-Step-6 the legacy
            // `transport.broadcast` returned per-secondary failure
            // tuples; `peer_transport.send` collapses them into a
            // single Err — the heartbeat-monitor is the
            // per-secondary signal, not this log line.)
            tracing::debug!(
                error = %error,
                "primary keepalive delivery failed"
            );
        }
    }

    /// Take in-flight tasks back, drop the secondary from the routable set,
    /// originate a `ClusterMutation::PeerRemoved` carrying `cause` (the
    /// primary is the sole authoritative author of `PeerRemoved` — every
    /// invocation of this hook fires the mutation post-`secondaries.remove`
    /// so receivers learn about the death via the replicated ledger), and
    /// broadcast a `TimeoutDetected` to every surviving secondary so they
    /// can prune the dead peer from their own peer maps.
    pub(super) async fn requeue_dead_secondary(
        &mut self,
        dead: DeadSecondary,
        cause: RemovalCause,
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
        // Snapshot the dead workers' global ids first; we need them to
        // call `pool.release_worker` AFTER requeue (release_worker
        // clears the affinity record, requeue uses it for routing —
        // and even if `requeue` doesn't read it, it's the documented
        // ordering in the brief).
        let dead_global_ids: Vec<u32> = self
            .workers
            .iter()
            .filter(|w| w.secondary_id == secondary_id)
            .map(|w| w.worker_id)
            .collect();
        for mut worker in std::mem::take(&mut self.workers) {
            if worker.secondary_id == secondary_id {
                if let Some(binary) = worker.current_task.take() {
                    // requeue lands the item at the FRONT of its
                    // (phase, type, affinity) bucket and decrements
                    // the phase's in-flight count.
                    self.pool_mut().requeue(binary);
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
        // Now clear pool-side affinity for the dead workers so any
        // bucket they pinned is free for survivors.
        for wid in dead_global_ids {
            self.pool_mut().release_worker(wid);
        }

        self.secondaries.remove(&secondary_id);
        self.secondary_keepalives.remove(&secondary_id);

        // Authoritative origination: the primary is the sole writer of
        // `PeerRemoved` for a dead secondary. Goes through the canonical
        // `apply_and_broadcast_cluster_mutations` helper so the local
        // CRDT mirror flips in the same call as the wire fan-out and
        // the apply+filter semantics stay consistent with every other
        // primary-originated mutation. Secondaries do NOT broadcast
        // `PeerRemoved`; they observe and apply ours.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerRemoved {
            id: secondary_id.clone(),
            cause,
        }])
        .await;

        if self
            .primary_id
            .as_ref()
            .map(|id| id == &secondary_id)
            .unwrap_or(false)
        {
            // The primary just died; clear the pointer so the caller
            // can promote a survivor. State transfer is no longer
            // needed at promotion — every secondary's continuously-
            // replicated `cluster_state` mirror already holds the
            // ledger.
            self.primary_id = None;
        }

        // Notify every surviving secondary so they prune the dead peer.
        // Not a broadcast: the dead secondary was just removed from
        // `self.secondaries` and we want to skip it explicitly. The
        // transport's own connection map may still hold a (about-to-die)
        // sender for the dead one until its wire-level handler exits,
        // and broadcasting would deliver a self-referential TimeoutDetected
        // if the heartbeat-monitor's call was a false positive. Iterating
        // the post-removal survivors avoids that race.
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
            pending = self.pool().len(),
            "dead secondary cleaned up"
        );
        Ok(())
    }

    /// Drive one heartbeat-tick cycle: resolve any deferred mass-death
    /// pending state, then look at the fresh report and decide whether
    /// new deaths look correlated (mass event, defer them) or
    /// independent (requeue immediately).
    ///
    /// Replaces the legacy "for every dead in report.dead, requeue
    /// immediately" path so a transient gateway-side tunnel collapse —
    /// which makes every connected secondary appear silent at the
    /// same tick — doesn't cause the primary to evict the entire
    /// fleet and burn the retry budget on a recoverable network blip.
    ///
    /// Mass-death detection rule: if EVERY currently-alive (i.e.
    /// `secondaries.len() - pending_mass_death.len()`) secondary
    /// appears in the new dead list AND the count meets
    /// `mass_death_min_count` AND `mass_death_grace > 0`, defer the
    /// requeue. Otherwise (subset death, or singleton, or feature
    /// disabled) requeue per-secondary as before.
    pub(super) async fn process_heartbeat_tick(&mut self) -> Result<(), String> {
        let now = Instant::now();
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);

        // Step 1: resolve secondaries already in mass-death-deferred state.
        // Each pending entry is either:
        //   (a) recovered — its keepalive timestamp advanced past the
        //       defer-time value AND is fresh (within `deadline`). Drop
        //       from pending; secondary is alive again.
        //   (b) grace expired — `mass_death_grace` elapsed since defer
        //       without recovery. Escalate to actual death via
        //       `requeue_dead_secondary`.
        //   (c) still pending — neither recovered nor expired. Leave
        //       alone for the next tick.
        let mut to_resolve: Vec<String> = Vec::new();
        let mut to_finalize: Vec<DeadSecondary> = Vec::new();
        for (id, pending) in &self.pending_mass_death {
            let live_keepalive = self.secondary_keepalives.get(id).copied();
            let recovered = live_keepalive
                .map(|t| t > pending.last_keepalive_at_defer && now.duration_since(t) <= deadline)
                .unwrap_or(false);
            if recovered {
                to_resolve.push(id.clone());
            } else if now.duration_since(pending.deferred_at) >= self.config.mass_death_grace {
                to_finalize.push(DeadSecondary {
                    secondary_id: id.clone(),
                    last_keepalive: pending.last_keepalive_at_defer,
                });
            }
        }
        for id in to_resolve {
            self.pending_mass_death.remove(&id);
            tracing::info!(
                secondary = %id,
                "mass-death deferred secondary recovered; un-deferring"
            );
        }
        for dead in to_finalize {
            let id = dead.secondary_id.clone();
            self.pending_mass_death.remove(&id);
            tracing::warn!(
                secondary = %id,
                grace_s = self.config.mass_death_grace.as_secs_f64(),
                "mass-death grace expired without keepalive recovery; \
                 escalating to actual death"
            );
            self.requeue_dead_secondary(dead, RemovalCause::MassDeathEscalation)
                .await?;
        }

        // Step 2: process newly-dead secondaries (fresh entries from
        // `collect_heartbeat_report` not already in the pending set).
        let report = self.collect_heartbeat_report();
        let new_dead: Vec<DeadSecondary> = report
            .dead
            .into_iter()
            .filter(|d| !self.pending_mass_death.contains_key(&d.secondary_id))
            .collect();
        if new_dead.is_empty() {
            return Ok(());
        }

        // "Mass event" iff every currently-alive secondary appears in
        // the new dead list, gated by `mass_death_min_count` to keep
        // singleton/dual-secondary runs from biasing toward correlated
        // inference. `alive_count` excludes already-deferred peers
        // (they're "dead from the alive set's perspective" too).
        let alive_count = self.secondaries.len().saturating_sub(self.pending_mass_death.len());
        let mass_event = self.config.mass_death_grace > Duration::ZERO
            && new_dead.len() >= self.config.mass_death_min_count as usize
            && new_dead.len() == alive_count;
        if mass_event {
            tracing::warn!(
                count = new_dead.len(),
                grace_s = self.config.mass_death_grace.as_secs_f64(),
                "every connected secondary went silent at the same heartbeat tick; \
                 inferring correlated cause (likely gateway-side tunnel collapse) \
                 and deferring requeue. Tasks remain in-flight; secondaries that \
                 reconnect during the grace window are silently un-deferred."
            );
            for dead in new_dead {
                self.pending_mass_death.insert(
                    dead.secondary_id.clone(),
                    PendingMassDeath {
                        deferred_at: now,
                        last_keepalive_at_defer: dead.last_keepalive,
                    },
                );
            }
        } else {
            // Independent / partial death. Per-secondary requeue as
            // before — these really are dead, not a correlated blip.
            for dead in new_dead {
                self.requeue_dead_secondary(dead, RemovalCause::KeepaliveMiss)
                    .await?;
            }
        }
        Ok(())
    }

    /// Handle a `SecondaryFatalError` from a secondary that's about to
    /// exit non-zero. Treat it as a dead-secondary notification: log
    /// at ERROR with the secondary's reason, then run the standard
    /// requeue path so any in-flight tasks return to the pool and the
    /// surviving fleet learns about the death (via `TimeoutDetected`)
    /// without waiting for the keepalive miss threshold to elapse.
    ///
    /// The fatal sender will be terminating its process anyway, so
    /// the requeue is the right cleanup — there's no recovery to
    /// attempt on the primary side, just bookkeeping. Uses the most
    /// recent observed keepalive as `last_keepalive` so the log line
    /// is meaningful; falls back to `Instant::now()` if the secondary
    /// never sent a keepalive (handshake-time fault).
    pub(super) async fn handle_secondary_fatal_error(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let DistributedMessage::SecondaryFatalError {
            secondary_id,
            error,
            ..
        } = msg
        else {
            return Ok(());
        };
        tracing::error!(
            secondary = %secondary_id,
            error = %error,
            "secondary reported fatal error; treating as dead and requeueing in-flight tasks"
        );
        let last_keepalive = self
            .secondary_keepalives
            .get(&secondary_id)
            .copied()
            .unwrap_or_else(Instant::now);
        let dead = DeadSecondary {
            secondary_id,
            last_keepalive,
        };
        // `BoundedString::from` truncates oversized inputs at the
        // 1 KiB cap that `RemovalCause::FatalError` carries, so a
        // misbehaving secondary cannot force unbounded allocation on
        // receivers via the cause payload.
        let cause = RemovalCause::FatalError(BoundedString::from(error));
        self.requeue_dead_secondary(dead, cause).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use dynrunner_core::{
        BoundedString, PhaseId, ResourceMap, SoftPreferredSecondaries, TaskInfo, TypeId,
    };
    use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, RemovalCause};
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc as tokio_mpsc;

    use crate::primary::{PrimaryConfig, PrimaryCoordinator, RemoteWorkerState};
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::{PendingPool, ResourceEstimator};

    /// Test fixture: install an empty pool with a single "default" phase
    /// onto a freshly-constructed primary. Mirrors what `run()` does in
    /// production; tests that exercise post-initialisation paths
    /// (heartbeat re-queue, etc.) need this so `pool_mut()` doesn't
    /// panic.
    fn install_default_pool<T, P, S, E>(
        primary: &mut PrimaryCoordinator<T, P, S, E, TestId>,
    ) where
        T: dynrunner_protocol_primary_secondary::SecondaryTransport<TestId>,
        P: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
        S: dynrunner_scheduler_api::Scheduler<TestId>,
        E: ResourceEstimator<TestId>,
    {
        let phase = PhaseId::from("default");
        let pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[derive(Clone)]
    struct FixedEstimator;
    impl ResourceEstimator<TestId> for FixedEstimator {
        fn estimate(&self, _task: &dynrunner_core::TaskInfo<TestId>) -> ResourceMap {
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
            source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            // Default OFF in legacy heartbeat tests — they assert the
            // `requeue_dead_secondary` immediate path. Tests that
            // exercise the mass-death path build their own config.
            mass_death_grace: Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
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
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut primary);

        // Register the secondary at the connection level. Drive
        // through the full handshake → operational state machine
        // because the heartbeat-monitor only applies the deadline
        // to Operational secondaries (pre-Operational means setup
        // is still in progress; the secondary's own keepalive
        // sender hasn't started yet, so falsely declaring dead
        // would drop a healthy node mid-setup).
        let conn = SecondaryConnection::new("dead-sec".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            "dead-sec".into(),
            SecondaryConnectionState::Operational(conn),
        );
        primary.seed_keepalive("dead-sec");

        // Stage one in-flight task on a single virtual worker.
        let in_flight = TaskInfo {
            path: std::path::PathBuf::from("victim.bin"),
            size: 100,
            identifier: TestId("victim".into()),
            phase_id: PhaseId::from("default"),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
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
            primary
                .requeue_dead_secondary(dead, RemovalCause::KeepaliveMiss)
                .await
                .unwrap();
        }

        assert_eq!(primary.workers.len(), 0, "dead worker should be evicted");
        // After requeue, the in-flight item is back in the pool (queued),
        // not in_flight.
        assert_eq!(primary.pool().len(), 1, "in-flight task requeued");
        let requeued: Vec<_> = primary.pool().iter().collect();
        assert_eq!(requeued[0].identifier.0, "victim");
        assert!(!primary.secondaries.contains_key("dead-sec"));
    }

    /// Multi-secondary transport variant that pre-registers two
    /// secondaries on the outgoing map. Used by the mass-death tests
    /// because the singleton `empty_transport` only knows about
    /// `dead-sec`, and `requeue_dead_secondary` walks the outgoing
    /// table to fan `TimeoutDetected` to survivors.
    fn two_secondary_transport() -> (
        ChannelSecondaryTransportEnd<TestId>,
        Vec<tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ) {
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let (a_tx, a_rx) = tokio_mpsc::unbounded_channel();
        let (b_tx, b_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-a".into(), a_tx);
        outgoing.insert("sec-b".into(), b_tx);
        (
            ChannelSecondaryTransportEnd {
                outgoing,
                incoming_rx,
            },
            vec![a_rx, b_rx],
            incoming_tx,
        )
    }

    /// Helper: register a secondary in Operational state with a single
    /// in-flight task. Mirrors the setup pattern of
    /// `dead_secondary_requeues_in_flight_task` but parametrised by id
    /// so the mass-death tests can stage two of them.
    fn register_operational_secondary<T, P, S, E>(
        primary: &mut PrimaryCoordinator<T, P, S, E, TestId>,
        secondary_id: &str,
        worker_id: u32,
        in_flight_label: &str,
    ) where
        T: dynrunner_protocol_primary_secondary::SecondaryTransport<TestId>,
        P: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
        S: dynrunner_scheduler_api::Scheduler<TestId>,
        E: ResourceEstimator<TestId>,
    {
        let conn = SecondaryConnection::new(secondary_id.into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            secondary_id.into(),
            SecondaryConnectionState::Operational(conn),
        );
        primary.seed_keepalive(secondary_id);
        primary.workers.push(RemoteWorkerState {
            worker_id,
            secondary_id: secondary_id.into(),
            resource_budgets: ResourceMap::new(),
            current_task: Some(TaskInfo {
                path: std::path::PathBuf::from(format!("{in_flight_label}.bin")),
                size: 100,
                identifier: TestId(in_flight_label.into()),
                phase_id: PhaseId::from("default"),
                type_id: TypeId::from("default"),
                affinity_id: None,
                payload: serde_json::Value::Null,
                task_id: None,
                task_depends_on: vec![],
                preferred_secondaries: SoftPreferredSecondaries::default(),
                resolved_path: None,
            }),
            estimated_resources: ResourceMap::new(),
            is_idle: false,
        });
    }

    fn config_with_mass_death(
        keepalive_interval: Duration,
        miss_threshold: u32,
        grace: Duration,
        min_count: u32,
    ) -> PrimaryConfig {
        let mut cfg = config(keepalive_interval, miss_threshold);
        cfg.mass_death_grace = grace;
        cfg.mass_death_min_count = min_count;
        cfg
    }

    /// When EVERY connected secondary appears dead at the same
    /// heartbeat tick (and there are at least `mass_death_min_count`
    /// of them), the framework infers a correlated cause and DEFERS
    /// the requeue. Tasks remain in-flight; `pending_mass_death`
    /// tracks the deferred set. Pre-fix the primary requeued every
    /// secondary immediately, evicted the entire fleet, and burned
    /// the retry budget on what was actually a transient gateway-side
    /// blip — observed in tokenizer's cohort-5 dispatch where 197
    /// in-flight tasks were lost to a 15-second tunnel hiccup.
    #[tokio::test(flavor = "current_thread")]
    async fn mass_death_defers_requeue_when_all_secondaries_silent() {
        let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_secs(60),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        // Sleep past the deadline so both appear in the dead list.
        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.process_heartbeat_tick().await.unwrap();

        // BOTH secondaries deferred — pending_mass_death population
        // matches the connected fleet, no requeue happened, no
        // workers evicted, pool still empty (tasks remain in-flight
        // on `workers[].current_task`).
        assert_eq!(primary.pending_mass_death.len(), 2);
        assert!(primary.pending_mass_death.contains_key("sec-a"));
        assert!(primary.pending_mass_death.contains_key("sec-b"));
        assert_eq!(primary.workers.len(), 2, "no workers evicted");
        assert_eq!(primary.pool().len(), 0, "no tasks requeued");
        assert_eq!(primary.secondaries.len(), 2, "secondaries still registered");
    }

    /// During mass-death grace, a secondary whose keepalive resumes
    /// is silently un-deferred — no requeue, no logged death. The
    /// other deferred peer stays pending until it either recovers or
    /// the grace expires.
    #[tokio::test(flavor = "current_thread")]
    async fn mass_death_recovery_during_grace_undefers_secondary() {
        let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_secs(60),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.process_heartbeat_tick().await.unwrap();
        assert_eq!(primary.pending_mass_death.len(), 2, "both deferred");

        // sec-a's keepalive resumes — simulate by recording a fresh one.
        primary.record_keepalive("sec-a");
        primary.process_heartbeat_tick().await.unwrap();

        // sec-a un-deferred (back in the live fleet), sec-b still
        // deferred. No requeue happened for either.
        assert!(!primary.pending_mass_death.contains_key("sec-a"));
        assert!(primary.pending_mass_death.contains_key("sec-b"));
        assert_eq!(primary.workers.len(), 2, "no workers evicted");
        assert_eq!(primary.pool().len(), 0, "no tasks requeued");
    }

    /// A single-secondary death is NOT mass-death; the legacy
    /// per-secondary requeue path runs unchanged. Guards against
    /// over-eager mass detection swallowing every death.
    #[tokio::test(flavor = "current_thread")]
    async fn solo_death_with_live_peers_takes_legacy_requeue_path() {
        let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_secs(60),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        // Only sec-a is past the deadline; sec-b is still fresh.
        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.record_keepalive("sec-b");
        primary.process_heartbeat_tick().await.unwrap();

        // sec-a went through the legacy path (requeue + evict + drop
        // from secondaries); sec-b is unaffected. Mass-death pending
        // stays empty — the rule didn't trip.
        assert_eq!(primary.pending_mass_death.len(), 0);
        assert!(!primary.secondaries.contains_key("sec-a"));
        assert!(primary.secondaries.contains_key("sec-b"));
        assert_eq!(primary.pool().len(), 1, "sec-a's task requeued");
        assert_eq!(primary.workers.len(), 1, "only sec-b's worker remains");
    }

    /// `mass_death_grace = ZERO` reverts to legacy "requeue every
    /// dead secondary immediately" behaviour even when every connected
    /// peer dies at the same tick — the disable knob.
    #[tokio::test(flavor = "current_thread")]
    async fn mass_death_disabled_when_grace_is_zero() {
        let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::ZERO,
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.process_heartbeat_tick().await.unwrap();

        // Both requeued immediately — no deferral.
        assert_eq!(primary.pending_mass_death.len(), 0);
        assert_eq!(primary.workers.len(), 0, "all workers evicted");
        assert_eq!(primary.pool().len(), 2, "both tasks requeued");
        assert!(primary.secondaries.is_empty());
    }

    /// Drain `rx` non-blockingly and return every `PeerRemoved` mutation
    /// observed in any `DistributedMessage::ClusterMutation` batch. The
    /// primary's `apply_and_broadcast_cluster_mutations` helper fans the
    /// broadcast across the transport's outgoing channel map, so any
    /// receiver wired to that map sees the same payload. Used by the
    /// PeerRemoved-origination tests to inspect the mutation primary
    /// authored on death.
    fn collect_peer_removed(
        rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    ) -> Vec<(String, RemovalCause)> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
                for m in mutations {
                    if let ClusterMutation::PeerRemoved { id, cause } = m {
                        out.push((id, cause));
                    }
                }
            }
        }
        out
    }

    /// Independent / partial-death path: a single secondary misses the
    /// keepalive threshold while peers stay alive. The primary
    /// originates one `PeerRemoved { cause: KeepaliveMiss }` per dead
    /// secondary. Pins the call-site cause wiring (`process_heartbeat_tick`
    /// else-branch).
    #[tokio::test(flavor = "current_thread")]
    async fn requeue_dead_secondary_emits_peer_removed_with_keepalive_miss_cause() {
        let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_secs(60),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        // Only sec-a misses the deadline (sec-b is refreshed below), so
        // the mass-death rule does NOT trip and the else-branch runs.
        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.record_keepalive("sec-b");
        primary.process_heartbeat_tick().await.unwrap();

        // Drain BOTH receivers — broadcast goes to every entry in the
        // outgoing map. Either drain sees the same PeerRemoved payload;
        // we read sec-b's because the dead one's channel may still be
        // sending its TimeoutDetected first.
        let removed_a = collect_peer_removed(&mut sec_rxs[0]);
        let removed_b = collect_peer_removed(&mut sec_rxs[1]);
        let merged = if !removed_b.is_empty() { removed_b } else { removed_a };
        assert_eq!(
            merged.len(),
            1,
            "exactly one PeerRemoved must originate per single death; got {merged:?}",
        );
        assert_eq!(merged[0].0, "sec-a");
        assert_eq!(merged[0].1, RemovalCause::KeepaliveMiss);
    }

    /// Mass-death finalize path: every connected secondary goes silent
    /// at the same tick → defer; after the grace window elapses without
    /// recovery, the primary escalates each deferred entry to actual
    /// death and originates `PeerRemoved { cause: MassDeathEscalation }`.
    /// Pins the call-site cause wiring (mass-death finalize loop).
    ///
    /// Real-time sleeps (not paused tokio time) because the heartbeat
    /// path measures via `std::time::Instant::now`, which
    /// `tokio::time::advance` doesn't move.
    #[tokio::test(flavor = "current_thread")]
    async fn requeue_dead_secondary_emits_peer_removed_with_mass_death_escalation_cause() {
        let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_millis(200),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        // First tick: both silent past the deadline → deferred, no
        // PeerRemoved authored yet (the entry-deferral path is silent
        // per the operative rule).
        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.process_heartbeat_tick().await.unwrap();
        assert_eq!(primary.pending_mass_death.len(), 2, "both deferred");
        assert!(
            collect_peer_removed(&mut sec_rxs[0]).is_empty(),
            "entry-deferral must not author PeerRemoved (operative rule)"
        );
        assert!(
            collect_peer_removed(&mut sec_rxs[1]).is_empty(),
            "entry-deferral must not author PeerRemoved (operative rule)"
        );

        // Sleep past the grace window without recovery → finalize.
        tokio::time::sleep(Duration::from_millis(250)).await;
        primary.process_heartbeat_tick().await.unwrap();

        // One PeerRemoved per finalized secondary, all carrying
        // MassDeathEscalation. Both receivers receive each broadcast
        // (broadcast iterates the outgoing map), so reading either is
        // sufficient — drain both and merge.
        let mut removed = collect_peer_removed(&mut sec_rxs[0]);
        removed.extend(collect_peer_removed(&mut sec_rxs[1]));
        // De-dup by id (each finalize broadcasts once; both channels
        // see the same broadcast).
        removed.sort_by(|a, b| a.0.cmp(&b.0));
        removed.dedup();
        assert_eq!(
            removed.len(),
            2,
            "one PeerRemoved per finalized secondary; got {removed:?}"
        );
        for (_, cause) in &removed {
            assert_eq!(*cause, RemovalCause::MassDeathEscalation);
        }
        let ids: std::collections::HashSet<&str> =
            removed.iter().map(|(id, _)| id.as_str()).collect();
        assert!(ids.contains("sec-a"));
        assert!(ids.contains("sec-b"));
    }

    /// Fatal-error path: a secondary explicitly reports a fatal error.
    /// The primary originates `PeerRemoved { cause: FatalError(<msg>) }`
    /// using `BoundedString::from(error)`. Oversized error strings are
    /// truncated at the 1 KiB cap that `RemovalCause::FatalError`
    /// carries, so a misbehaving secondary can't force unbounded
    /// allocation on receivers.
    #[tokio::test(flavor = "current_thread")]
    async fn requeue_dead_secondary_emits_peer_removed_with_fatal_error_cause() {
        let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config(Duration::from_millis(50), 2),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        // Build an oversized error payload so the truncation guarantee
        // is exercised end-to-end (not just in the BoundedString unit
        // test).
        let huge = "x".repeat(4096);
        let fatal = DistributedMessage::<TestId>::SecondaryFatalError {
            sender_id: "sec-a".into(),
            timestamp: 0.0,
            secondary_id: "sec-a".into(),
            error: huge,
        };
        primary.handle_secondary_fatal_error(fatal).await.unwrap();

        let mut removed = collect_peer_removed(&mut sec_rxs[0]);
        removed.extend(collect_peer_removed(&mut sec_rxs[1]));
        removed.sort_by(|a, b| a.0.cmp(&b.0));
        removed.dedup();
        assert_eq!(removed.len(), 1, "exactly one PeerRemoved authored");
        assert_eq!(removed[0].0, "sec-a");
        match &removed[0].1 {
            RemovalCause::FatalError(s) => {
                // BoundedString<1024> truncates at construction; the
                // oversized input must be capped on the wire payload.
                assert_eq!(
                    s.as_ref().len(),
                    1024,
                    "FatalError diagnostic must be truncated to 1024 bytes; \
                     got {} bytes",
                    s.as_ref().len()
                );
                let expected: String = std::iter::repeat('x').take(1024).collect();
                assert_eq!(s.as_ref(), expected);
            }
            other => panic!("expected FatalError cause; got {other:?}"),
        }
        // Silence unused-import warning for BoundedString — the
        // truncation invariant is checked via length above, but the
        // type itself is the load-bearing piece for that invariant.
        let _: BoundedString<1024> = BoundedString::from("anchor");
    }

    /// Negative pin (operative rule: "PeerRemoved fires only post-
    /// mass-death-grace"): while a secondary is deferred during the
    /// mass-death grace window, NO `PeerRemoved` mutation is authored.
    /// The hook fires only on the finalize path (covered by the
    /// `MassDeathEscalation` test above); a recovery during the grace
    /// window drops the deferred entry silently.
    #[tokio::test(flavor = "current_thread")]
    async fn mass_death_grace_entry_deferral_does_not_fire_peer_removed() {
        let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                config_with_mass_death(
                    Duration::from_millis(50),
                    2,
                    Duration::from_secs(60),
                    2,
                ),
                transport,
                dynrunner_transport_quic::NoPeerTransport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
        install_default_pool(&mut primary);
        register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

        tokio::time::sleep(Duration::from_millis(200)).await;
        primary.process_heartbeat_tick().await.unwrap();
        assert_eq!(
            primary.pending_mass_death.len(),
            2,
            "both deferred — neither requeued nor evicted"
        );

        // The entry-deferral path is silent: no PeerRemoved on EITHER
        // receiver. If one ever fires here we'd duplicate-author with
        // the finalize path AND break the recovery contract (a peer
        // that recovers during grace must look as if it never died).
        let from_a = collect_peer_removed(&mut sec_rxs[0]);
        let from_b = collect_peer_removed(&mut sec_rxs[1]);
        assert!(
            from_a.is_empty() && from_b.is_empty(),
            "entry-deferral must not author PeerRemoved; a={from_a:?} b={from_b:?}"
        );

        // Recovery during grace also stays silent: drop the pending
        // entry, no PeerRemoved on either channel.
        primary.record_keepalive("sec-a");
        primary.process_heartbeat_tick().await.unwrap();
        assert!(!primary.pending_mass_death.contains_key("sec-a"));
        let from_a = collect_peer_removed(&mut sec_rxs[0]);
        let from_b = collect_peer_removed(&mut sec_rxs[1]);
        assert!(
            from_a.is_empty() && from_b.is_empty(),
            "grace-window recovery must not author PeerRemoved; \
             a={from_a:?} b={from_b:?}"
        );
    }

    /// A secondary that's still sending keepalives stays in the routable
    /// set even when other secondaries die.
    #[tokio::test(flavor = "current_thread")]
    async fn live_secondary_is_not_falsely_declared_dead() {
        let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );

        let conn = SecondaryConnection::new("dead-sec".into()).receive_welcome(
            1,
            vec![],
            "host".into(),
            0,
            None,
            false,
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

