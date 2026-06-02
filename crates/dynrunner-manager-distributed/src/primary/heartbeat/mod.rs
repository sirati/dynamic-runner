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

use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    Address, ClusterMutation, DistributedMessage, PeerTransport, RemovalCause, Scope,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::{PendingMassDeath, PrimaryCoordinator};
use super::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

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

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
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
            active_workers: self.workers.iter().filter(|w| !w.is_idle()).count() as u32,
        };
        if let Err(error) = self
            .transport
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
        // Recover EVERY in-flight task targeting the dead secondary
        // through the single hash-keyed ledger: requeue each (front of
        // its bucket, phase in-flight counter decremented, type slot
        // released) and drop the ledger entry. This covers both
        // locally-dispatched tasks (a slot held them) AND inherited
        // (pre-owned, no-slot) tasks the dead secondary owned —
        // mirroring the reference dead-peer recovery. The held slots
        // are then removed below; the ledger is the source of truth for
        // the requeue so the two can't diverge.
        let requeue_mutations = self.recover_inflight_for_dead_secondary(&secondary_id);
        let requeued = requeue_mutations.len();
        // Drop every worker hosted by the dead secondary — its host is
        // gone. The slot state is discarded with the worker; the task
        // it held (if any) was already requeued via the ledger above.
        self.workers
            .retain(|w| w.secondary_id != secondary_id);
        // Now clear pool-side affinity for the dead workers so any
        // bucket they pinned is free for survivors.
        for wid in dead_global_ids {
            self.pool_mut().release_worker(wid);
        }

        self.secondaries.remove(&secondary_id);
        self.secondary_keepalives.remove(&secondary_id);

        // Authoritative origination, one batch: the dead secondary's
        // in-flight tasks transition `InFlight → Pending` in the CRDT
        // (the `TaskRequeued` mutations the recovery just produced, in
        // lockstep with the local pool requeue above so a stale
        // `InFlight` can't survive and strand the task on failover) and
        // the secondary itself is marked removed (`PeerRemoved`). Both
        // go through the canonical `apply_and_broadcast_cluster_mutations`
        // helper so the local CRDT mirror flips in the same call as the
        // wire fan-out and the apply+filter semantics stay consistent
        // with every other primary-originated mutation. The primary is
        // the sole writer of both; secondaries observe and apply ours.
        let mut recovery_mutations = requeue_mutations;
        recovery_mutations.push(ClusterMutation::PeerRemoved {
            id: secondary_id.clone(),
            cause,
        });
        self.apply_and_broadcast_cluster_mutations(recovery_mutations)
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
                .send(Address::Peer(peer_id.clone()), timeout_msg.clone())
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

        // A dead secondary's in-flight tasks were just requeued into
        // the pool — a pool-entry edge. Surviving free workers only
        // emit `TaskRequest` after they finish a task; the workers free
        // on survivors right now have nothing in flight to complete, so
        // without a nudge the requeued tasks would sit in the pool
        // forever. EMIT a `TasksAdded` onto the decoupled
        // worker-management bus rather than calling dispatch directly
        // (the dispatch-decoupling law) — mirroring the emit at every
        // other pool-entry / worker-free edge (`handle_task_complete`,
        // `handle_task_failed`, the retry bucket). The operational
        // loop's worker-management arm coalesces it into one batched
        // recheck (which, on a real `TasksAdded`, bypasses the
        // per-secondary backoff so a survivor that was transiently
        // backpressured is still a target).
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
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
mod tests;

