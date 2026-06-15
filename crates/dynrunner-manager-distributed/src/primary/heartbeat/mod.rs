//! Failover detection: per-secondary heartbeat tracking, the honest
//! staged silence-declaration policy, dead-secondary requeue, and
//! `TimeoutDetected` broadcast (F1).
//!
//! The primary updates a `last_keepalive` timestamp every time it observes
//! a `Keepalive` message from a secondary. On a periodic tick the
//! operational loop calls [`PrimaryCoordinator::collect_heartbeat_report`]
//! to fold the map into a [`SecondaryHeartbeatReport`] of RAW per-secondary
//! silence ages — a single death clock — and hands it to
//! [`PrimaryCoordinator::decide_dead_secondaries`].
//!
//! The declaration policy is a single ordered schedule of multiples of
//! `keepalive_interval` (the one cadence authority): WARN stages are
//! LOG-ONLY and fire once per stage, while the last entry — the HARD
//! backstop (≈2m at the 5s default) — declares the secondary dead and
//! requeues its in-flight tasks REGARDLESS of dispatch state. The backstop
//! is required: a purely starvation-driven declaration would never empty
//! `secondaries`, so the fleet-dead arm would never arm and a fully-silent
//! fleet would hang forever.
//!
//! The schedule consumes the wall-clock evidence age while the decider's
//! own ticks run on cadence, and the starvation-honest JUDGED clock (a
//! difference of `OwnTickHealth::judged_elapsed` readings, per-member
//! anchored by [`SilenceJudgedMark`]) while the decider is CHRONICALLY
//! starved — the bounded escalation of the tick-lag deferral, see
//! [`PrimaryCoordinator::process_heartbeat_tick`].
//!
//! Both declaration paths — the hard backstop here and the lazy on-demand
//! requeue at the dispatch altitude (`only_silent_held_work_remains` →
//! `declare_silent_secondaries_dead`) — funnel through
//! [`PrimaryCoordinator::declare_silent_secondaries_dead`], which wraps
//! the [`PrimaryCoordinator::requeue_dead_secondary`] primitive (it takes
//! the in-flight tasks back into the pending pool, evicts per-worker
//! tracking, drops the connection state, and notifies surviving peers via
//! `TimeoutDetected`).

mod collective_silence;
mod ingest_gate;

pub(super) use collective_silence::CollectiveSilenceGate;
use collective_silence::SilenceObservation;
pub(super) use ingest_gate::IngestEdgeGate;

use std::time::{Duration, Instant};

use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, KeepaliveRole, PeerId, RemovalCause,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::PrimaryCoordinator;
use super::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

/// Minimum spacing between two primary egress-keepalive failure WARNs (see
/// [`PrimaryCoordinator::keepalive_egress_warn`]). 60s matches the
/// established throttle cadence used across the crate's recurring-fault
/// reports, so a persistent mute primary surfaces ~once a minute with the
/// suppressed-occurrence count rather than once per keepalive tick.
pub(super) const KEEPALIVE_EGRESS_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Outcome of a single periodic heartbeat sweep: the RAW per-secondary
/// silence ages, one entry per Operational secondary the primary is
/// tracking. There is no binary dead/alive partition here — the single
/// death clock is the continuous silence age, fed to
/// [`PrimaryCoordinator::decide_dead_secondaries`] which applies the staged
/// schedule.
pub(super) struct SecondaryHeartbeatReport {
    /// Per-secondary continuous-silence observations.
    pub(super) silences: Vec<SecondarySilence>,
}

/// One secondary's continuous silence at the moment of the sweep.
pub(super) struct SecondarySilence {
    pub(super) secondary_id: String,
    /// The most recent keepalive timestamp observed for the secondary.
    pub(super) last_keepalive: Instant,
    /// `now - last_keepalive` at sweep time — the secondary's continuous
    /// silence age, the single clock the staged schedule reads.
    pub(super) silence: Duration,
}

/// A stage of the ordered, keepalive-interval-relative silence schedule.
///
/// `Warn(i)` is the i-th WARN stage (LOG-ONLY, fire-once); `Hard` is the
/// terminal backstop that declares the secondary dead. The ordering
/// `Warn(0) < Warn(1) < … < Hard` is by ascending multiple of
/// `keepalive_interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Warn(usize),
    Hard,
}

/// PURE: classify a continuous silence into the highest schedule stage it
/// has crossed, or `None` if it has not crossed the first stage yet.
///
/// `silence` is the member's continuous-silence age on whichever clock
/// the caller judges by (the wall-clock evidence age on a healthy
/// decider; the starvation-honest judged-clock difference under the
/// chronic-starvation escalation — see [`SilenceJudgedMark`]);
/// `keepalive_interval` scales the schedule; `warn_multiples` are the
/// ascending WARN-stage multiples and `hard_multiple` is the terminal
/// backstop multiple. No `&self`, no I/O — a property-testable
/// classifier. The schedule entries are read in place (the caller owns
/// the config) so the silence-age arithmetic lives in exactly one spot.
pub(super) fn silence_stage(
    silence: Duration,
    keepalive_interval: Duration,
    warn_multiples: &[u32],
    hard_multiple: u32,
) -> Option<Stage> {
    let crossed = |multiple: u32| silence > keepalive_interval.saturating_mul(multiple);
    if crossed(hard_multiple) {
        return Some(Stage::Hard);
    }
    // Highest WARN stage whose threshold the silence has crossed. The
    // multiples are ascending, so the last crossed index is the answer.
    warn_multiples
        .iter()
        .enumerate()
        .rev()
        .find(|(_, m)| crossed(**m))
        .map(|(i, _)| Stage::Warn(i))
}

pub(super) struct DeadSecondary {
    pub(super) secondary_id: String,
    pub(super) last_keepalive: Instant,
}

/// Per-member judged-silence mark: the member's last evidence-of-life
/// instant paired with the [`crate::own_tick_health::OwnTickHealth`]
/// judged-clock reading when a sweep first observed that evidence.
///
/// The chronic-starvation escalation's per-member state: while the
/// decider's own ticks are chronically lagged, wall-clock silence
/// (`now - last_evidence`) is inflated by the decider's OWN stall, so the
/// escalated sweep judges `judged_now - judged_at_evidence` instead — a
/// difference of judged-clock readings, which accrues at most one
/// starvation threshold per lagged round and therefore never exceeds the
/// wall silence (an escalated sweep can only be MORE conservative than a
/// wall-clock sweep, never less). Maintained on every sweep; an evidence
/// advance resets the mark, so a member with fresh frames each round
/// measures zero judged silence regardless of how stretched the rounds
/// are.
pub(in crate::primary) struct SilenceJudgedMark {
    /// The evidence instant the mark was last reset on (the same union
    /// clock `collect_heartbeat_report` reports).
    pub(super) last_evidence: Instant,
    /// `OwnTickHealth::judged_elapsed()` at the sweep that observed
    /// `last_evidence` as fresh.
    pub(super) judged_at_evidence: Duration,
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Update the keepalive timestamp for a known secondary. No-op if the
    /// secondary id isn't registered (e.g. a stray message after death).
    ///
    /// A fresh keepalive ends the current silence streak, so the
    /// per-secondary staged-WARN state resets here: the next streak
    /// re-warns from the first stage.
    pub(super) fn record_keepalive(&mut self, secondary_id: &str) {
        if self.secondaries.contains_key(secondary_id) {
            self.secondary_keepalives
                .insert(secondary_id.into(), Instant::now());
            self.silence_warn_stage.remove(secondary_id);
        }
    }

    /// Test accessor: the death-clock timestamp recorded for `secondary_id`,
    /// or `None` if no keepalive has refreshed it. Used by the §14 BUG-1 gate
    /// to prove a same-peer secondary's `All` keepalive reached the local
    /// primary slot and refreshed its death clock (the multi-role host does
    /// NOT declare its own same-peer secondary dead).
    #[cfg(test)]
    pub(crate) fn last_keepalive_for_test(&self, secondary_id: &str) -> Option<Instant> {
        self.secondary_keepalives.get(secondary_id).copied()
    }

    /// Seed the keepalive timestamp at welcome time so the death deadline
    /// counts from when we first heard from the secondary, not from
    /// process start. A welcome starts a fresh silence streak, so the
    /// staged-WARN state resets too.
    pub(super) fn seed_keepalive(&mut self, secondary_id: &str) {
        self.secondary_keepalives
            .insert(secondary_id.into(), Instant::now());
        self.silence_warn_stage.remove(secondary_id);
        // A welcome is an incarnation boundary: drop any stale judged-
        // silence mark so the new incarnation's judged clock starts from
        // its own fresh evidence (the next sweep re-seeds it).
        self.silence_judged_marks.remove(secondary_id);
        // The fresh incarnation re-earns the setup exemption: its own
        // keepalive emitter has not started yet, so the pre-Operational
        // gate must spare it again until its first proven keepalive.
        self.keepalive_proven.remove(secondary_id);
    }

    /// Record PROOF that `msg`'s sender runs its operational main loop:
    /// a mesh `Keepalive` carrying the SECONDARY emitter role is sent
    /// only by a secondary's post-`wait_for_setup` keepalive arm, so its
    /// arrival is ground truth that setup completed on the member's own
    /// node — whatever this primary's connection typestate for it says.
    /// Called from the `dispatch_message` Keepalive arm (after the
    /// preamble's `record_keepalive` refreshed the death clock).
    ///
    /// The proof BOUNDS the silence sweep's pre-Operational setup
    /// exemption (see [`Self::collect_heartbeat_report`]): once a member
    /// has provably emitted, it is silence-judged like any Operational
    /// member, so a member whose primary-side connection state wedged
    /// pre-Operational cannot die silence-invisible (the
    /// run_20260611_214327 face: a wire-dead member was never declared
    /// because a duplicate welcome had regressed its state). No-op for
    /// unknown senders (a stray frame after death) and for non-secondary
    /// emitter roles (a primary keepalive proves a different loop).
    pub(super) fn note_secondary_keepalive_frame(&mut self, msg: &DistributedMessage<I>) {
        if let DistributedMessage::Keepalive {
            sender_id,
            emitter_role: KeepaliveRole::Secondary,
            ..
        } = msg
            && self.secondaries.contains_key(sender_id.as_str())
        {
            self.keepalive_proven.insert(sender_id.clone());
        }
    }

    /// Fold the per-secondary keepalive map into a sweep of RAW silence
    /// ages — one entry per Operational secondary. The staged schedule in
    /// [`Self::decide_dead_secondaries`] reads these ages; this method does
    /// NOT itself partition dead/alive (the single death clock is the
    /// continuous silence age, not a binary dead-at-Nx list).
    ///
    /// Secondaries in a pre-Operational state (Handshaking,
    /// InitialAssigning) are exempt WHILE their keepalive emitter has not
    /// provably started: they are still finishing setup and the
    /// secondary's own main loop — which sends keepalives — hasn't
    /// started yet (see `secondary/processing.rs` where
    /// `keepalive_interval.tick()` fires only post-`wait_for_setup`).
    /// Subjecting them to the silence schedule falsely declares a
    /// slow-to-handshake secondary dead at the operational-loop
    /// transition: e.g. a SLURM secondary that took 38s for container
    /// startup, SSH-tunnel, and handshake would be dropped immediately on
    /// the first heartbeat tick, despite being healthy and processing
    /// tasks.
    ///
    /// The exemption is BOUNDED by proof, not by state alone
    /// (`keepalive_proven` — see [`Self::note_secondary_keepalive_frame`]):
    /// a member whose mesh keepalives have arrived has demonstrably
    /// completed setup on its own node, so it is judged like any
    /// Operational member even if this primary's connection typestate for
    /// it reads pre-Operational. Pre-bound, a member whose state had been
    /// regressed out of `Operational` (the duplicate-welcome wedge —
    /// run_20260611_214327) was skipped here on EVERY sweep: its death
    /// produced no WARN stage, no hard backstop, no `PeerRemoved` —
    /// respawn unreachable and its in-flight tasks stranded forever. A
    /// removal deferral must never be able to outlive a dead peer.
    ///
    /// FLOOD IMMUNITY — the INGEST-clock union: each secondary's
    /// last-evidence-of-life is the LATEST of (a) its last PROCESSED
    /// frame (`secondary_keepalives`, refreshed at dispatch + by the
    /// liveness-beacon arm), (b) its last frame to ENTER this
    /// primary's inbox (`RoleInbox::last_ingest_from`, recorded at the
    /// slot's delivery choke point BEFORE the frame waits in the
    /// channel), and (c) its last frame to ARRIVE at this node's
    /// TRANSPORT (`RoleInbox::last_transport_arrival_from`, recorded by
    /// the connection read loops BEFORE the frame waits in the
    /// transport's inbound queue). Under inbox starvation (the
    /// run_20260610_221140 face: depth 52654, keepalive arm starved)
    /// the processed clock inflates while the peers' keepalives sit
    /// QUEUED — pre-union the sweep declared LIVE peers dead off this
    /// node's own busyness. And under MESH-PUMP starvation (the
    /// run_20260611_115429 face: a snapshot-flooded egress starved the
    /// pump's ingress arm) frames never even reached the slot's choke
    /// point, so clock (b) lied too — only the transport-arrival clock,
    /// written by reader tasks that keep running while the pump
    /// starves, still measures the PEER's silence and never our
    /// backlog.
    pub(super) fn collect_heartbeat_report(&self) -> SecondaryHeartbeatReport {
        let now = Instant::now();
        let mut silences = Vec::new();
        for (id, last) in &self.secondary_keepalives {
            let state = match self.secondaries.get(id) {
                Some(s) => s,
                None => continue,
            };
            // A member the replicated membership ledger has
            // AUTHORITATIVELY removed is never silence-judged: it is not
            // SILENT, it is GONE. A graceful departure
            // (`PeerRemoved { SelfDeparture }`, originated by the leaving
            // node and applied to `cluster_state`) stops the member's
            // keepalives by design, so its silence age inflates legitimately
            // — but the apply path that flips the membership ledger to
            // `RemovedMember` does NOT reap this primary-local roster cache
            // (`self.secondaries` / `secondary_keepalives` are written only by
            // the bootstrap handshake + the hydrate rebuild). Pre-filter the
            // departed-but-still-cached member here, off the SAME authoritative
            // ledger the hydrate rebuild reads (`live_known_secondaries`), so
            // the silence schedule never reaches the keepalive-miss removal +
            // task requeue for a deliberately-departed member (the
            // run_20260612_094056 face: a member departed cleanly, a promotion
            // followed, and the promoted primary silence-removed-with-requeue
            // the gone member two minutes later). A re-admission flips the same
            // entry back to `AliveMember` and the member is judged again.
            if self.cluster_state.peer_membership(id)
                == crate::cluster_state::PeerMembership::RemovedMember
            {
                continue;
            }
            if !matches!(
                state,
                crate::state::SecondaryConnectionState::Operational(_)
            ) && !self.keepalive_proven.contains(id)
            {
                continue;
            }
            let last_evidence = [
                self.inbox.last_ingest_from(id),
                self.inbox.last_transport_arrival_from(id),
            ]
            .into_iter()
            .flatten()
            .fold(*last, Instant::max);
            silences.push(SecondarySilence {
                secondary_id: id.clone(),
                last_keepalive: last_evidence,
                silence: now.saturating_duration_since(last_evidence),
            });
        }
        SecondaryHeartbeatReport { silences }
    }

    /// Send a `Keepalive` to every connected secondary. Secondaries use this
    /// to detect primary death (F2): if a secondary stops seeing primary
    /// keepalives for `keepalive_miss_threshold` intervals, it kicks off the
    /// failover election. Called from the operational loop on the same
    /// cadence as `collect_heartbeat_report`.
    ///
    /// Keepalive rides `self.send_to(Destination::All, msg)` — the
    /// single mesh broadcast. The primary is a real mesh member, so the
    /// fan-out reaches every connected secondary (over the same
    /// per-secondary tunnel writers the legacy `transport.broadcast`
    /// used). Bug class #3 collapses by virtue of the dead per-peer
    /// writer to the promoted peer no longer being invoked: post-demotion
    /// the new primary's writer is gone from the shared writer table, so
    /// the broadcast iterates over the LIVE peers only.
    ///
    /// The former `Scope::AllSecondaries` ("exclude the current primary
    /// from the fan-out") collapses into `Destination::All`: the primary
    /// is not in its own writer table, so the broadcast already excludes
    /// it. "Don't send to self" is the implicit loopback rule, not a
    /// role-flavoured broadcast scope.
    ///
    /// Deliberately NOT gated on `self.secondaries` being non-empty: the
    /// keepalive's audience is the MESH MEMBERS the transport knows, not
    /// the worker-bearing secondary roster. The historical empty-roster
    /// early-return (from the pre-mesh era, when the send loop literally
    /// iterated `self.secondaries.keys()` and an empty roster meant zero
    /// recipients) silenced the ONLY frame that refreshes a peer's
    /// `primary_last_seen` clock and cancels elections — so a promoted
    /// primary in a slow bring-up window (members connected, no welcome /
    /// hydrate registered yet) fed spurious primary-silence suspicion.
    /// With zero members the mesh fan is simply a no-op.
    ///
    /// OBSERVER members additionally get the keepalive DIRECTED
    /// (`Destination::Observer(id)` per roster observer — see
    /// [`PrimaryCoordinator::send_to_each_observer`]): the `All` fan is
    /// the transport's direct-leg broadcast, which a relay-only observer
    /// (a late joiner behind a gateway leg, or an observer whose direct
    /// leg died) never receives — the production face where an observer
    /// ingested live CRDT gossip while declaring the named primary
    /// silent for 600s. The keepalive is the frame the observer's
    /// `primary_last_seen` clock keys on, so its delivery must be
    /// independent of broadcast reachability; the directed edge relays
    /// through a connected sibling. A direct-leg observer's duplicate is
    /// an idempotent clock refresh.
    ///
    /// EGRESS-HEALTH NARRATION: both fan paths funnel their delivery
    /// failures into ONE throttled WARN (`keepalive_egress_warn`, ~60s).
    /// A primary whose keepalive sends all fail is MUTE — invisible to its
    /// own peers, whose `primary_last_seen` clocks then run down toward a
    /// spurious failover election — so the failure path is loud rather
    /// than swallowed at debug. The WARN names how many sends failed and
    /// to whom (mesh broadcast + named observers) plus the count of
    /// per-tick failures suppressed since the last emit; a clean tick is
    /// silent and the per-send debug lines stay for fine-grained tracing.
    pub(super) async fn broadcast_primary_keepalive(&mut self) {
        let msg = DistributedMessage::<I>::Keepalive {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.node_id.clone(),
            active_workers: self.workers.iter().filter(|w| !w.is_idle()).count() as u32,
            emitter_role: KeepaliveRole::Primary,
        };
        // Fan over BOTH egress paths and collect their failures into one
        // narration: the directed observer fan
        // (`Destination::Observer(id)` per roster observer) and the mesh
        // `Destination::All` broadcast. Both are this primary's keepalive
        // egress; a mute primary whose sends all fail is INVISIBLE to
        // itself and silently feeds every peer's primary-silence
        // suspicion, so the failure path is loud — but throttled.
        let mut failed_observers = self.send_to_each_observer(msg.clone()).await;
        let broadcast_err = self.send_to(Destination::All, msg).await.err();

        // Per-send debug lines stay for fine-grained tracing: a secondary
        // mid-disconnect generates one per tick until the heartbeat-monitor
        // declares it dead, so the throttled WARN above carries the
        // operator-facing summary while these remain quiet by default.
        // (The legacy `transport.broadcast` returned per-secondary failure
        // tuples; `self.client.send` collapses the mesh fan into a single
        // Err — the heartbeat-monitor is the per-secondary signal, not
        // this line.)
        if let Some(error) = &broadcast_err {
            tracing::debug!(error = %error, "primary keepalive mesh broadcast failed");
        }

        // One throttled WARN naming how many keepalive sends failed and to
        // whom. Emitted only when AT LEAST one egress path failed; a clean
        // tick stays quiet. The throttle suppresses per-tick spam on an
        // already-handled disconnect while keeping a persistent mute
        // primary loud (~once a minute, carrying the suppressed count).
        let any_failed = broadcast_err.is_some() || !failed_observers.is_empty();
        if any_failed && let Some(suppressed) = self.keepalive_egress_warn.permit() {
            failed_observers.sort();
            tracing::warn!(
                mesh_broadcast_failed = broadcast_err.is_some(),
                failed_observers = ?failed_observers,
                failed_observer_count = failed_observers.len(),
                suppressed_since_last_warn = suppressed,
                "primary keepalive egress failing; this primary may be MUTE \
                 to its peers (their primary-silence clocks are not being \
                 refreshed)"
            );
        }
    }

    /// Rebuild the PRIMARY→secondaries liveness-beacon target set from the
    /// current secondary roster and publish it into the dedicated beacon
    /// thread's [`crate::liveness::BeaconTarget`] cell. The transport-
    /// INDEPENDENT twin of [`Self::broadcast_primary_keepalive`]: that fans a
    /// mesh `Keepalive` to every secondary over the (build-starvable) tokio
    /// runtime; this hands the off-runtime beacon thread the same recipient
    /// set so the primary keeps asserting liveness even when its runtime is
    /// CPU-starved by a co-located build and the mesh keepalive freezes.
    ///
    /// The recipient set is the current secondary roster (`self.secondaries`,
    /// EXCLUDING this primary's own node id — a primary never beacons itself),
    /// each id resolved to its raw beacon `SocketAddr` through the shared
    /// `peer_liveness_addrs` book. A secondary whose address is unknown (the
    /// book lacks it) is simply absent from the set (no beacon to it this
    /// round — strictly better than beaconing a bogus address; the mesh-frame
    /// keepalive still reaches it). Called on every roster change (welcome,
    /// hydrate-on-promotion, dead-secondary requeue) so the set tracks the
    /// live recipients. The beacon thread re-reads the set each tick, so a
    /// roster change repoints it with zero beacon-side knowledge.
    ///
    /// Resolves via the address BOOK rather than `SecondaryConnection`'s own
    /// `ipv4`/`liveness_port` because a PROMOTED primary's hydrated roster
    /// carries neither (it is rebuilt from the address-less CRDT) — the book,
    /// populated by the co-located secondary from `PeerInfo`, is the promoted
    /// primary's only source of its secondaries' beacon addresses.
    ///
    /// OBSERVERS ARE DELIBERATELY NOT BEACON TARGETS. An observer process
    /// binds NO [`crate::liveness::LivenessListener`] (only the secondary
    /// run path does — see `pyo3::managers::secondary::run`), advertises
    /// `liveness_port: None`, and detects primary silence purely from mesh
    /// frames — its `primary_last_seen` clock keys on the Primary-role
    /// `Keepalive` and the `PrimaryChanged` re-point (observer coordinator
    /// `on_keepalive` / `on_cluster_mutation`), NOT a UDP beacon. The
    /// observer's primary-liveness channel is therefore the DIRECTED mesh
    /// keepalive ([`PrimaryCoordinator::send_to_each_observer`], which the
    /// transport relays even to a relay-only observer), and a beacon to an
    /// observer would land on a port nothing listens on. The exclusion is
    /// already STRUCTURAL — `self.secondaries` carries observer entries, but
    /// an observer has no `peer_liveness_addrs` book entry (it advertised no
    /// port), so the `filter_map` resolve drops it — and it is by design: the
    /// off-runtime beacon thread is the build-starvation fallback for the
    /// WORKER-secondary mesh-keepalive freeze, a hazard an observer (no local
    /// worker pool, runtime never build-starved) does not have.
    pub(super) fn publish_beacon_targets(&self) {
        let own_id = self.config.node_id.as_str();
        let addrs: Vec<std::net::SocketAddr> = self
            .secondaries
            .keys()
            .filter(|id| id.as_str() != own_id)
            .filter_map(|id| self.peer_liveness_addrs.get(id))
            .collect();
        self.beacon_target.publish_set(addrs);
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
        // Pre-start fence A (#530a): every task this peer-removal just
        // requeued may be re-dispatched onto another member while the
        // "dead" peer is actually still alive (a false-dead recovery —
        // the run_20260612 #518 wasted-compute window). Record the
        // supplanted holder identity NOW, BEFORE the `PeerRemoved`
        // below kills the live `peer_member_gen` for this id: the
        // generation stamped on the hint is the LIVE incarnation at
        // requeue time, which is exactly the value the receiver
        // compares against the supplanted holder's CURRENT
        // `peer_member_gen` (re-admission would have bumped it). Only
        // genuine `TaskRequeued` mutations carry a fence — a
        // setup-task's `TaskFailed` (non-reassignable) is a real
        // terminal, no redirect dispatch follows.
        let supplanted_gen = self.cluster_state.peer_member_gen(&secondary_id);
        for mutation in &requeue_mutations {
            if let ClusterMutation::TaskRequeued { hash, .. } = mutation {
                self.supplanted_holders
                    .insert(hash.clone(), (secondary_id.clone(), supplanted_gen));
            }
        }
        let requeued = requeue_mutations.len();
        // Drop every worker hosted by the dead secondary — its host is
        // gone. The slot state is discarded with the worker; the task
        // it held (if any) was already requeued via the ledger above.
        self.workers.retain(|w| w.secondary_id != secondary_id);
        // Now clear pool-side affinity for the dead workers so any
        // bucket they pinned is free for survivors.
        for wid in dead_global_ids {
            self.pool_mut().release_worker(wid);
        }
        // #494 bring-up reservation: this is the GENUINE member-removal
        // path, the one redistribute trigger. If a formation-window
        // reservation is still open and this dead member held a share it
        // never drained, fold it onto the surviving fleet round-robin (the
        // dead member is already retained out of `self.workers` above, so
        // the survivor order excludes it). A no-op once the window has
        // closed — a steady-state death just requeues, no redistribute.
        self.redistribute_reservation_for_dead_member(&secondary_id);

        self.secondaries.remove(&secondary_id);
        self.secondary_keepalives.remove(&secondary_id);
        // The secondary is gone; drop any staged-WARN state, the
        // judged-silence mark, and the keepalive proof so a re-welcomed
        // id (respawn reusing the slot) starts a fresh streak with the
        // setup exemption re-earned.
        self.silence_warn_stage.remove(&secondary_id);
        self.silence_judged_marks.remove(&secondary_id);
        self.keepalive_proven.remove(&secondary_id);

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
            // Kill the id's CURRENT membership incarnation: a removal
            // that was already superseded by a re-admission (a stale
            // lower generation) loses at every receiver instead of
            // re-burying the re-admitted live peer.
            member_gen: self.cluster_state.peer_member_gen(&secondary_id),
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
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            timed_out_secondary_id: secondary_id.clone(),
            last_seen: timestamp_now() - last_seen_secs,
        };
        let surviving: Vec<String> = self.secondaries.keys().cloned().collect();
        for peer_id in surviving {
            if let Err(e) = self
                .send_to(
                    Destination::Secondary(PeerId::from(peer_id.clone())),
                    timeout_msg.clone(),
                )
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

    /// Drive one heartbeat-tick cycle: collect the fresh silence sweep
    /// and hand it to the staged dead-secondary declaration policy —
    /// UNLESS the decider itself is provably unhealthy, on either axis:
    ///
    /// - **locally starved** (the tick's own inter-tick gap stretched
    ///   far past the keepalive cadence: the runtime was frozen/starved
    ///   and every age it would measure is inflated by our own stall,
    ///   not the peers' silence), or
    /// - **ingest-backlogged** (the sweep runs on cadence but the mesh
    ///   pump is not moving inbound frames: the transport's arrival
    ///   clock shows frames undrained across sweeps — see
    ///   [`IngestEdgeGate`] — so the staleness INPUTS are suspect even
    ///   though the sweep itself is timely; a buried peer's keepalives
    ///   may sit unattributed in the same backed-up queue).
    ///
    /// A deferred sweep is SKIPPED — named, never silent — and the next
    /// on-cadence tick after the node is healthy again decides
    /// honestly. A genuine death is thereby delayed only while the
    /// decider itself is compromised; a false mass-removal of live
    /// peers off this node's own backlog is impossible.
    ///
    /// The tick-lag deferral is BOUNDED: on a CHRONICALLY starved node
    /// (every inter-tick gap lagged, for a streak spanning the hard
    /// silence window — the run_20260611_200548 face) the primitive
    /// escalates instead of deferring forever, and the sweep resumes on
    /// its starvation-honest judged clock (each lagged round contributes
    /// at most one starvation threshold of judgeable silence — see
    /// [`SilenceJudgedMark`]). A genuinely dead member is then still
    /// removed within a bounded number of lagged rounds, while a member
    /// with fresh evidence each round measures zero judged silence and is
    /// never falsely declared.
    pub(super) async fn process_heartbeat_tick(&mut self) -> Result<(), String> {
        let now = Instant::now();
        // Own-tick-health gate — the shared authority (mesh ⊥ role: the
        // SAME primitive the secondary's election/peer-liveness judgments
        // consume). A lagged sweep means THIS node's runtime was
        // frozen/starved, so every silence age it would measure is inflated
        // by our own stall, not the peers' silence; skip the sweep (named by
        // the primitive's throttled WARN) and decide honestly on the next
        // on-cadence tick. The primitive also re-bases its trustworthy floor
        // on the lag, but the primary defers the WHOLE sweep rather than
        // clamping per-peer — a deferred sweep authors no removal at all.
        // Under the CHRONIC escalation the verdict flips to `false` and the
        // sweep below judges on the judged clock instead (see the method
        // doc).
        if self.own_tick_health.observe_tick(now) {
            return Ok(());
        }
        // Decider-health gate on the staleness INPUTS: fold this
        // sweep's transport edge-clock sample into the gate (it owns
        // the deferral narration) and author no removal while it
        // defers. Same threshold family as the tick-lag guard above —
        // `STARVATION_TICK_MULTIPLE` keepalive intervals — far below the
        // hard death backstop, so a healthy pump never feels it.
        // Transports without edge clocks return `None` here and the gate
        // stays inactive (clock (c) of the union is then absent too — the
        // sweep decides off clocks (a)+(b) and the tick-lag guard alone).
        if let Some(edges) = self.inbox.transport_ingest_edges() {
            let pending_threshold = self
                .config
                .keepalive_interval
                .saturating_mul(crate::own_tick_health::STARVATION_TICK_MULTIPLE);
            if self
                .ingest_gate
                .observe(&edges, now, pending_threshold)
                .is_some()
            {
                return Ok(());
            }
        }
        let report = self.collect_heartbeat_report();
        self.decide_dead_secondaries(report).await
    }

    /// Apply the staged silence schedule to one heartbeat sweep.
    ///
    /// For every reported secondary, classify its continuous silence into
    /// the highest schedule stage it has crossed (the PURE
    /// [`silence_stage`] helper):
    ///
    /// - a WARN stage logs ONCE per stage (the per-secondary
    ///   `silence_warn_stage` counter tracks how many WARN stages have
    ///   already fired for the current streak) and does NOT declare death;
    /// - the HARD backstop declares the secondary dead and requeues its
    ///   in-flight tasks REGARDLESS of dispatch state, via
    ///   [`Self::declare_silent_secondaries_dead`].
    ///
    /// The hard backstop is the load-bearing forward-progress guarantee: a
    /// purely starvation-driven declaration would never empty
    /// `secondaries`, so the fleet-dead arm would never arm and a fully-
    /// silent fleet would hang forever.
    ///
    /// SELF-SUSPECT gate (the third decider-health guard — see
    /// [`CollectiveSilenceGate`]): before executing the hard
    /// declarations, the sweep's per-member classification is folded
    /// into the collective-silence tracker. When EVERY remote judged
    /// member is silent simultaneously (and there are at least two),
    /// the parsimonious hypothesis is that THIS node's wire is deaf —
    /// not that N peers died independently (the run_20260612_043357
    /// face: a saturated primary's QUIC legs collapsed and it declared
    /// all three live remotes dead) — so the declarations are deferred:
    /// WARN stages still narrate, but no removal is authored until a
    /// remote frame proves the wire or the gate's bounded escalation
    /// window elapses (the hard backstop stays load-bearing for a
    /// genuinely all-dead fleet). The co-located same-peer member is
    /// not wire evidence and never counts toward the inference.
    ///
    /// [`Self::declare_silent_secondaries_dead`] wraps the existing
    /// [`Self::requeue_dead_secondary`] primitive, which already emits
    /// `WorkerMgmtSignal::TasksAdded` after requeueing — so this method
    /// does NOT re-nudge the worker-management bus.
    /// [`Self::handle_secondary_fatal_error`] is a SIBLING path
    /// (`FatalError`), NOT routed through here.
    async fn decide_dead_secondaries(
        &mut self,
        report: SecondaryHeartbeatReport,
    ) -> Result<(), String> {
        let interval = self.config.keepalive_interval;
        let warn_multiples = self.config.silence_warn_multiples.clone();
        let hard_multiple = self.config.silence_hard_multiple;
        // Chronic-starvation escalation (see `process_heartbeat_tick`):
        // while active, judge each member on the starvation-honest judged
        // clock instead of the wall-clock evidence age.
        let chronic = self.own_tick_health.in_chronic_starvation();
        let judged_now = self.own_tick_health.judged_elapsed();

        let mut hard_dead: Vec<DeadSecondary> = Vec::new();
        let mut observations: Vec<SilenceObservation> = Vec::new();
        for s in report.silences {
            // Maintain the judged mark on EVERY sweep (not just escalated
            // ones) so the escalation has per-member history the moment it
            // engages. An evidence advance resets the mark — a member with
            // fresh frames each round measures zero judged silence.
            let mark = self
                .silence_judged_marks
                .entry(s.secondary_id.clone())
                .or_insert(SilenceJudgedMark {
                    last_evidence: s.last_keepalive,
                    judged_at_evidence: judged_now,
                });
            if s.last_keepalive > mark.last_evidence {
                mark.last_evidence = s.last_keepalive;
                mark.judged_at_evidence = judged_now;
            }
            let silence = if chronic {
                judged_now.saturating_sub(mark.judged_at_evidence)
            } else {
                s.silence
            };
            let stage = silence_stage(silence, interval, &warn_multiples, hard_multiple);
            observations.push(SilenceObservation {
                // The co-located same-peer member's frames ride the
                // in-process loopback — they prove nothing about the
                // wire, so it never counts toward (or against) the
                // collective-silence inference.
                remote: s.secondary_id != self.config.node_id,
                silent: stage.is_some(),
                hard: matches!(stage, Some(Stage::Hard)),
            });
            match stage {
                None => continue,
                Some(Stage::Hard) => {
                    hard_dead.push(DeadSecondary {
                        secondary_id: s.secondary_id,
                        last_keepalive: s.last_keepalive,
                    });
                }
                Some(Stage::Warn(idx)) => {
                    self.log_silence_warn_once(&s.secondary_id, idx, silence);
                }
            }
        }

        // Self-suspect gate: fold this sweep's classification in; while
        // it defers, author NO removal (the WARN stages above already
        // narrated the silences; the gate's own WARN names the
        // suspicion). The escalation window is the hard silence window —
        // the same bound the chronic tick-lag escalation uses, derived
        // from the one cadence authority rather than a new config knob.
        let escalation_window = interval.saturating_mul(hard_multiple);
        if self
            .collective_silence_gate
            .observe(&observations, Instant::now(), escalation_window)
            .is_some()
        {
            return Ok(());
        }

        self.declare_silent_secondaries_dead(hard_dead, RemovalCause::KeepaliveMiss)
            .await
    }

    /// Fire the WARN log for `stage_idx` of `secondary_id` AT MOST ONCE per
    /// silence streak. The per-secondary `silence_warn_stage` counter holds
    /// the number of WARN stages already logged this streak; a stage only
    /// logs when its index reaches the counter, after which the counter
    /// advances. Reset to zero (entry removed) on keepalive recovery,
    /// welcome, and requeue, so a fresh streak re-warns from stage 0.
    ///
    /// Single concern: the fire-once bookkeeping for the staged WARN log;
    /// the schedule itself lives in config and the classification in the
    /// pure [`silence_stage`].
    fn log_silence_warn_once(&mut self, secondary_id: &str, stage_idx: usize, silence: Duration) {
        let warned = self
            .silence_warn_stage
            .get(secondary_id)
            .copied()
            .unwrap_or(0);
        // `stage_idx` is the HIGHEST stage crossed; fire every not-yet-
        // logged stage up to and including it so a tick that skips past
        // several stages (a long inter-tick gap) still logs each once.
        if stage_idx < warned {
            return;
        }
        for idx in warned..=stage_idx {
            tracing::warn!(
                secondary = %secondary_id,
                silence_s = silence.as_secs_f64(),
                stage = idx,
                "secondary silent past WARN stage; not yet declared dead"
            );
        }
        self.silence_warn_stage
            .insert(secondary_id.into(), stage_idx + 1);
    }

    /// The set of Operational secondaries currently SILENT — those whose
    /// continuous silence has crossed at least the first schedule stage
    /// (the same pure [`silence_stage`] classification the heartbeat tick
    /// uses, so "silent" means one death clock, not a second threshold).
    ///
    /// Owned by the liveness module; the only liveness fact the dispatch-
    /// altitude oracle ([`PrimaryCoordinator::only_silent_held_work_remains`])
    /// consumes. Stays `pub(super)` so the silent-id set never leaks past
    /// the `primary` module boundary into dispatch.
    pub(super) fn silent_secondary_ids(&self) -> std::collections::HashSet<String> {
        // Decider-health gates, mirrored from `process_heartbeat_tick` /
        // `decide_dead_secondaries`: while the ingest path is backlogged
        // OR the self-suspect collective-silence gate defers, every
        // staleness reading is suspect, so the dispatch-altitude
        // early-requeue path (the only other author of staleness-based
        // `PeerRemoved`s, via `only_silent_held_work_remains` →
        // `declare_silent_secondaries_dead`) must see NO silent peers
        // either. Reads the most recent sweep's verdicts (at most one
        // tick stale — the same staleness class as the keepalive clocks
        // this method samples anyway).
        if self.ingest_gate.deferring().is_some()
            || self.collective_silence_gate.deferring().is_some()
        {
            return std::collections::HashSet::new();
        }
        let report = self.collect_heartbeat_report();
        // Same judged-clock substitution as `decide_dead_secondaries`,
        // read-only (this method is `&self`; the marks are maintained by
        // the per-tick sweep, so a reading here is at most one tick stale
        // — the same staleness class as the keepalive clocks). A member
        // without a mark, or whose evidence advanced past it, measures
        // zero judged silence (not silent — conservative).
        let chronic = self.own_tick_health.in_chronic_starvation();
        let judged_now = self.own_tick_health.judged_elapsed();
        // Exclude the recognized primary's own same-peer secondary by IDENTITY
        // (an `id != current_primary` cut owned here, for the early-requeue
        // concern only). The EARLY dispatch-altitude requeue acts on first-stage silence,
        // so during a transient self-keepalive gap — when the host's own
        // secondary is still processing but momentarily silent — reporting self
        // here would yank the self's LIVE in-flight task before the next
        // keepalive refreshes the clock and before the hard backstop. The hard
        // backstop (`decide_dead_secondaries`) is deliberately left unfiltered.
        let current_primary = self.cluster_state.current_primary();
        report
            .silences
            .into_iter()
            .filter(|s| {
                let silence = if chronic {
                    match self.silence_judged_marks.get(&s.secondary_id) {
                        Some(mark) if s.last_keepalive <= mark.last_evidence => {
                            judged_now.saturating_sub(mark.judged_at_evidence)
                        }
                        // Fresh evidence past the mark, or no mark yet:
                        // not silent on the judged clock.
                        _ => Duration::ZERO,
                    }
                } else {
                    s.silence
                };
                silence_stage(
                    silence,
                    self.config.keepalive_interval,
                    &self.config.silence_warn_multiples,
                    self.config.silence_hard_multiple,
                )
                .is_some()
            })
            .filter(|s| Some(s.secondary_id.as_str()) != current_primary)
            .map(|s| s.secondary_id)
            .collect()
    }

    /// Declare every secondary in `dead` dead and requeue its in-flight
    /// tasks, routing each through the existing [`Self::requeue_dead_secondary`]
    /// primitive. THE single command both declaration paths funnel through:
    ///
    /// - the hard backstop in [`Self::decide_dead_secondaries`] (fires
    ///   regardless of dispatch state, at the ≈2m bound), and
    /// - the lazy on-demand requeue at the dispatch altitude
    ///   ([`PrimaryCoordinator::only_silent_held_work_remains`] →
    ///   this method), which fires EARLIER than the backstop only when an
    ///   idle worker has nothing but silent-held work left.
    ///
    /// Dispatch sees ONLY this method and the oracle; the silent-id set is
    /// otherwise private to the liveness module. `requeue_dead_secondary`
    /// owns the `TasksAdded` re-nudge, so this method does not touch the
    /// bus.
    pub(super) async fn declare_silent_secondaries_dead(
        &mut self,
        dead: Vec<DeadSecondary>,
        cause: RemovalCause,
    ) -> Result<(), String> {
        for d in dead {
            self.requeue_dead_secondary(d, cause.clone()).await?;
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
            target: None,
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
