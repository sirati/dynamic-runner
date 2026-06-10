//! `SecondaryCoordinator` methods implementing the failover-election
//! state machine. The pure-data parts of the state machine
//! (`ElectionState`, `ElectionTickActions`, the round-bump helper)
//! live in [`super`]; this file contains only the per-method handlers
//! that mutate the coordinator's election + primary-link fields.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;
use super::{ElectionState, ElectionTickActions, failover_quorum, next_round, push_timeout_query};

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Bump the primary-keepalive timestamp on every primary message and
    /// abandon any in-flight failover election. Called from the dispatch
    /// path before any other handling.
    ///
    /// Who holds the primary role is NOT tracked here — it lives solely
    /// in the replicated `cluster_state` (`current_primary()`, the single
    /// source of "who is primary now"). Routing reads it at the egress
    /// edge (`send_to` → `resolve_destination`); a `PrimaryChanged`
    /// mutation is the only re-point. This method records liveness and
    /// cancels a false-alarm election; it writes no routing target,
    /// because there is no transitional routing target anymore (P2).
    ///
    /// Returns `true` iff this call RECOVERED an in-flight failover
    /// election (reverted Suspecting/Voting/Candidate → Normal) — the
    /// "primary message resumed" edge. The async
    /// [`Self::record_primary_message_if_from_primary`] wrapper keys the
    /// buffered-terminal-replay drain off this edge: a resumed primary
    /// link is the moment the route is most likely back, so any terminal
    /// retained during the outage is re-delivered immediately rather than
    /// waiting for the next loop tick.
    pub(in crate::secondary) fn record_primary_message(&mut self) -> bool {
        let secondary_id = self.config.secondary_id.clone();
        let op = self.op_mut();
        op.primary_last_seen = Some(Instant::now());
        // Reset the primary-link health sub-state. A real message
        // arriving on the (possibly-reconnected) transport proves
        // the link is alive again, so any failure-window we'd been
        // tracking should be discarded. Pre-fix the failover
        // arming kept counting probes against a stale window even
        // after a brief flap recovered, so the second flap would
        // arm faster than the first — confusing semantics. The
        // reset closes that loop. Idempotent on a healthy link.
        op.primary_link.record_recv_success();
        // Cancel a failover election in progress: a primary message
        // proves the original primary is still reachable so the
        // election was a false alarm. Revert to Normal. There is no
        // transitional routing target to clear — routing always
        // resolves `Destination::Primary` at the egress edge over
        // `cluster_state.current_primary()`, which still names the
        // original primary (no `PrimaryChanged` committed during the
        // aborted election, per the drop-the-transitional-hint design).
        if matches!(
            op.election,
            ElectionState::Suspecting { .. }
                | ElectionState::Voting { .. }
                | ElectionState::Candidate { .. }
        ) {
            // Visibility on election recovery: pre-fix the
            // transition from Suspecting/Voting/Candidate back to
            // Normal was silent — an operator tailing the log saw
            // the entering-Suspecting WARN but no resolution
            // signal when keepalives resumed. With this log they
            // can grep "election recovered" to confirm a
            // transient blip rather than chase a phantom election
            // failure.
            tracing::info!(
                secondary = %secondary_id,
                from = ?std::mem::discriminant(&op.election),
                "election recovered: primary message resumed, reverting to Normal"
            );
            op.election = ElectionState::Normal;
            return true;
        }
        false
    }

    /// Identity-gated wrapper over [`Self::record_primary_message`]: refresh
    /// the primary-liveness anchor and cancel a false-alarm election ONLY
    /// when the inbound frame's `sender_id` IS the current primary
    /// (`cluster_state.current_primary()`, the single source of "who is
    /// primary now").
    ///
    /// THE GATE — single source. "This frame proves the current primary is
    /// alive" is true iff its origin is the current primary; a frame from any
    /// OTHER mesh member (a peer secondary's anti-entropy `StateDigest`, the
    /// submitter's `RequestClusterSnapshot` / `RequestRunConfig`, a relayed
    /// `ClusterMutation`) is NOT a primary-liveness signal and must NEVER
    /// reset the election. Pre-fix, the operational dispatch path called the
    /// un-gated [`Self::record_primary_message`] for EVERY inbound frame — a
    /// stale assumption from the pre-mesh model, where the primary uplink was
    /// a physically primary-only transport so "any frame on it" was genuine
    /// primary traffic. After the one-mesh cutover the same dispatcher is
    /// reached for frames from ANY peer, so the un-gated reset let a lone
    /// survivor's own in-flight self-promotion be cancelled by a peer/submitter
    /// frame — the single-survivor election never converged (it flapped
    /// Suspecting/Candidate↔Normal forever). This is the identity-keyed
    /// counterpart to leg (C)'s remote-primary self-skip and the quorum side's
    /// `live_peer_ids` current-primary exclusion: every "is this the primary"
    /// decision keys on `current_primary()`, never on which transport delivered
    /// the frame.
    ///
    /// A frame whose `sender_id` names THIS node (a same-peer / self-promoted
    /// primary's own keepalive, recognized because `current_primary()` names
    /// self) still resets via this gate — exactly the
    /// `self_named_primary_resets_election_to_normal` contract — because the
    /// predicate is "origin == current primary", and a self-named primary IS
    /// the current primary.
    ///
    /// `async` because it owns the buffered-terminal-replay drain on the
    /// primary-link-recovery edge: when this call reverts an in-flight
    /// failover election (the "primary message resumed" edge that
    /// [`Self::record_primary_message`] returns `true` on), the route to the
    /// primary is most likely back, so any terminal-bearing report retained
    /// during the outage is re-delivered RIGHT NOW — schedule-overriding
    /// (`drain_report_replays_now`), ahead of the entry's next backoff slot
    /// and of the operational loop's replay wake arm. The drain is FIFO +
    /// retry-forever; a frame that re-absorbs (route not actually back yet)
    /// simply re-buffers on its advanced backoff slot and the next trigger
    /// retries it.
    pub(in crate::secondary) async fn record_primary_message_if_from_primary(
        &mut self,
        sender_id: &str,
    ) {
        if self.cluster_state.current_primary() == Some(sender_id) {
            let link_recovered = self.record_primary_message();
            if link_recovered {
                // Primary-link-recovery edge: re-deliver any retained
                // terminal reports immediately — schedule-overriding
                // (`_now`), because the route just provably came back
                // and a retained report may be sitting deep inside a
                // capped backoff slot. No-op when nothing was buffered
                // (the steady-state case).
                self.drain_report_replays_now().await;
            }
        }
    }

    /// Is `node_id`'s transport-INDEPENDENT liveness beacon still fresh?
    ///
    /// The UNION counterpart to the mesh-frame liveness legs, keyed on ANY
    /// node id (the current primary at the election-arming sites, the
    /// queried node at the `TimeoutQuery` responder, the candidate at the
    /// Voting re-evaluation). Reads this
    /// node's [`crate::liveness::BeaconLiveness`] POLL view (published by the
    /// `LivenessListener` per decoded beacon datagram) and judges its
    /// staleness on the SAME death deadline the mesh-frame quorum gates use
    /// (`keepalive_interval × keepalive_miss_threshold`), so the beacon and
    /// the frame are weighed on one yardstick. A never-seen beacon (`None`)
    /// is NOT fresh — it must never spuriously suppress a genuine election
    /// (#317): before a node has proven liveness on the beacon path the
    /// union degrades to the mesh-frame view alone. The beacon fires on
    /// `keepalive_interval`, so `keepalive_miss_threshold` consecutive
    /// missed beacons is the same "the node went silent" bound as for
    /// frames — a starved-but-alive node keeps its dedicated-thread
    /// beacon flowing well inside it.
    pub(in crate::secondary) fn node_beacon_fresh(&self, node_id: &str) -> bool {
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        self.beacon_liveness
            .last_seen(node_id)
            .map(|t| Instant::now().duration_since(t) <= deadline)
            .unwrap_or(false)
    }

    /// Is `id` currently a connected member of the transport mesh
    /// (`MeshClient::has_peer` over the published `MembershipView`)?
    ///
    /// SINGLE SOURCE for the transport-membership read every
    /// death/liveness seam takes (the `live_peer_ids` quorum
    /// intersection, the leg-(C) arming observation in
    /// [`Self::primary_departed_membership`], the deferrer-side
    /// death-evidence sites): the mesh-pump republishes the view on every
    /// peer connect/disconnect (`handle_peer_disconnect` → live
    /// `transport.connected_ids()` re-read), so this flips `false` within
    /// one pump cycle of a peer's QUIC teardown — the fast,
    /// idle-independent, role-independent transport signal (mesh ⊥ role).
    pub(in crate::secondary) fn is_mesh_member(&self, id: &str) -> bool {
        self.client
            .has_peer(&dynrunner_protocol_primary_secondary::address::PeerId::from(id))
    }

    /// Leg (C) of the primary-death evidence: has the CURRENT PRIMARY
    /// departed the transport mesh membership? The idle-independent
    /// primary-death observation, SINGLE-SOURCED here for BOTH sides of
    /// the failover protocol:
    ///   - the ARMING side: `run_election_tick`'s `need_election`
    ///     disjunction (the "election arms on current_primary leaving
    ///     membership" leg), and
    ///   - the DEFERRER side (#331): `record_promotion_vote`'s
    ///     independent-death-observation gate, so a peer asked to confirm
    ///     a candidate stops deferring the moment it has watched the
    ///     primary's QUIC connection tear down, instead of withholding its
    ///     confirm until its own frame-silence clock crosses the ~15s
    ///     death deadline — the deferrer-side twin that lets failover
    ///     converge sub-deadline. (The `TimeoutQuery` responder's
    ///     equivalent observation is [`Self::queried_node_liveness_age`],
    ///     keyed on the QUERIED id rather than the primary role.)
    ///
    /// Reads the transport `MembershipView` (NOT the CRDT `is_peer_alive`
    /// ledger): a dead primary cannot originate its own `PeerRemoved`, and
    /// no surviving secondary originates one for it — the CRDT membership
    /// stays stale on primary death, while the transport view is the
    /// direct, role-independent death observation (mesh ⊥ role). The
    /// mesh-pump republishes the view on the primary's QUIC teardown
    /// (`handle_peer_disconnect`), so `has_peer` flips `false` within one
    /// pump cycle — regardless of whether THIS node ever issued a
    /// primary-bound send.
    ///
    /// SELF-EXCLUSION: a `current_primary` naming THIS node (a same-peer /
    /// self-promoted primary co-located with the secondary role) is NEVER
    /// a membership-departure signal — a node is structurally absent from
    /// its OWN transport `connected_ids` (the view enumerates REMOTE
    /// peers), so `has_peer(self)` is always `false` while the local
    /// primary is perfectly alive. Reading that as "primary left" would
    /// make a promoted/co-located node spuriously act against itself, so
    /// the observation is keyed only on a REMOTE primary identity —
    /// exactly the single-source self-skip `live_peer_ids` /
    /// `check_peer_timeouts` apply for the quorum side.
    ///
    /// SEEN-GATE (`primary_last_seen.is_some()`): fires only for a primary
    /// this node has actually SEEN (received ≥1 message from) and that is
    /// now absent from membership — a genuine death of a previously-live
    /// primary. The gate suppresses the relocation bring-up window, where
    /// a freshly-named compute primary is in `current_primary()` but this
    /// survivor's transport has not yet dialled it (so `has_peer` is
    /// transiently `false`): there `primary_last_seen` is still `None` (no
    /// primary message yet, and neither backdate site has run — both
    /// require a prior no-route), so the observation correctly stays
    /// silent until the primary has proven liveness once.
    ///
    /// DEBOUNCE: a transient single-cycle membership flicker on a
    /// still-live primary is self-cancelling, on both consuming sides —
    /// the next primary keepalive routes through `record_primary_message`,
    /// which reverts an armed Suspecting back to Normal; and a confirm the
    /// deferrer would have lent during the blip is simply never sent once
    /// `has_peer` reads `true` again on the next call. The beacon union is
    /// deliberately NOT folded in here: each consumer applies its own
    /// `node_beacon_fresh` suppression at its decision site, exactly as
    /// the arming side always has.
    pub(in crate::secondary) fn primary_departed_membership(&self) -> bool {
        let seen = self
            .op_ref()
            .map(|op| op.primary_last_seen.is_some())
            .unwrap_or(false);
        seen && self
            .cluster_state
            .current_primary()
            .filter(|id| *id != self.config.secondary_id.as_str())
            .map(|id| !self.is_mesh_member(id))
            .unwrap_or(false)
    }

    /// The liveness EVIDENCE this node reports about `query_node_id` in a
    /// `TimeoutResponse`: `Some(age)` — seconds since its last `Secondary`
    /// keepalive, on THIS node's monotonic clock — while the queried node
    /// is a live mesh member; `None` when this node has NO liveness
    /// evidence for it: never saw a keepalive (the pre-existing meaning),
    /// OR — the responder-side membership observation (#331) — saw it and
    /// then watched it DEPART the transport membership with its beacon
    /// also silent. The querier's Suspecting tally counts a `None` as
    /// agreement that the queried node is silent, so a responder that
    /// observed the death at the transport level lends its agreement
    /// IMMEDIATELY instead of deferring until its keepalive age crosses
    /// the ~15s death deadline. This is the same
    /// keepalive ∩ membership rule `live_peer_ids` applies to the quorum
    /// denominator: a departed peer's lingering `peer_keepalives` entry is
    /// a corpse entry (the 300s reaper just hasn't fired), not liveness —
    /// reporting its raw age as if it were liveness evidence is what made
    /// every survivor's agreement wait out the silence window.
    ///
    /// Guards, mirroring [`Self::primary_departed_membership`]'s shape:
    ///   - SELF-EXCLUSION: this node is structurally absent from its own
    ///     membership view, so a query about THIS node's id never reads as
    ///     departed.
    ///   - SEEN-GATE: the departed branch requires a `peer_keepalives`
    ///     entry (seen-then-departed); a never-seen node reports `None`
    ///     exactly as before, with no membership read.
    ///   - BEACON UNION: a membership-departed node whose
    ///     transport-INDEPENDENT beacon is still fresh (a CPU-starved but
    ///     alive primary whose QUIC connection idle-timed-out) is NOT
    ///     death-observed — its keepalive age is reported as before, so a
    ///     peer's spurious election cannot gather agreement through us.
    ///
    /// All three guards ARE the single-source seen-peer death-evidence rule
    /// [`Self::peer_death_observed`]; this method only adds the age-report
    /// shape on top of it.
    pub(in crate::secondary) fn queried_node_liveness_age(
        &self,
        query_node_id: &str,
    ) -> Option<f64> {
        // SELF-QUERY: first-hand liveness. A node asked about ITSELF is the
        // one responder with direct evidence, and that evidence is "alive
        // RIGHT NOW" (age 0). Without this arm the lookup below read the
        // node's own id out of `peer_keepalives` — structurally absent (a
        // node does not receive its own broadcasts) — and reported `None`,
        // which the querier's Suspecting tally counts as AGREEMENT: the
        // suspected node itself lent a death-quorum vote AGAINST ITSELF.
        if query_node_id == self.config.secondary_id {
            return Some(0.0);
        }
        let age = self
            .op_ref()
            .and_then(|op| op.peer_keepalives.get(query_node_id))
            .map(|t| t.elapsed().as_secs_f64());
        if self.peer_death_observed(query_node_id) {
            None
        } else {
            age
        }
    }

    /// SINGLE SOURCE for the seen-PEER death-evidence rule: has this node
    /// independently OBSERVED peer `id`'s death at the transport level?
    /// True iff ALL of:
    ///   - SEEN-GATE: `id` has a `peer_keepalives` entry (this node saw it
    ///     alive at least once) — a never-seen peer is never death-observed,
    ///     so a not-yet-dialled bring-up window cannot false-fire;
    ///   - SELF-EXCLUSION: `id` is not THIS node — a node is structurally
    ///     absent from its own membership view (`connected_ids` enumerates
    ///     REMOTE peers), so reading that absence as death would be wrong;
    ///   - MEMBERSHIP DEPARTURE: `id` is gone from the live transport
    ///     `MembershipView` ([`Self::is_mesh_member`] — the fast,
    ///     idle-independent signal the mesh-pump republishes on QUIC
    ///     teardown);
    ///   - BEACON-SILENT: `id`'s transport-INDEPENDENT beacon is also not
    ///     fresh ([`Self::node_beacon_fresh`]) — a CPU-starved-but-alive
    ///     node whose QUIC connection idle-timed-out keeps beaconing and is
    ///     NOT death-observed.
    ///
    /// LIVE READ, not a latch (the #331 no-churn shape): every call reads
    /// the CURRENT membership view + beacon view, so a transient
    /// departure+rejoin blip reads alive again at the next evaluation —
    /// nothing is latched.
    ///
    /// Consumed by BOTH peer-keyed death observations so they cannot drift:
    ///   - the `TimeoutQuery` responder ([`Self::queried_node_liveness_age`]),
    ///     which reports `None` (no liveness evidence) for a death-observed
    ///     queried node, and
    ///   - the Voting-state candidate-liveness re-evaluation in
    ///     [`Self::run_election_tick`] (#361), which releases a vote lent
    ///     to a candidate this node has watched die.
    ///
    /// The PRIMARY-keyed twin is [`Self::primary_departed_membership`]
    /// (seen-gate on `primary_last_seen`, beacon union applied per
    /// consumer); this rule is keyed on an arbitrary PEER id with the
    /// seen-gate on that peer's own `peer_keepalives` entry.
    pub(in crate::secondary) fn peer_death_observed(&self, id: &str) -> bool {
        let seen = self
            .op_ref()
            .map(|op| op.peer_keepalives.contains_key(id))
            .unwrap_or(false);
        seen && id != self.config.secondary_id
            && !self.is_mesh_member(id)
            && !self.node_beacon_fresh(id)
    }

    /// The live mesh peers for failover quorum/candidate reasoning: the
    /// keys of `peer_keepalives`, MINUS the current primary's host id, AND
    /// INTERSECTED with the live transport `MembershipView` — a peer counts
    /// toward quorum only if it is BOTH keepalive-tracked AND a currently
    /// connected mesh member.
    ///
    /// A host that runs primary+secondary under one peer-id emits a
    /// `Secondary` keepalive that lands in `peer_keepalives` even though its
    /// id is the current primary (the recognition arm tracks a multi-role
    /// host as BOTH). That
    /// entry is correct as peer-mesh liveness but must NEVER inflate
    /// election counts — the primary is the role being failed-over FROM, not
    /// a peer that could vote for or become a candidate. Excluding it here
    /// is the single quorum-side counterpart to the peer-timeout sweep's
    /// own current-primary skip (`check_peer_timeouts`), both keyed on the
    /// single source of "who is primary now" (`current_primary()`).
    ///
    /// TWO honest death signals, mirroring the primary-side union legs the
    /// arm path uses (`run_election_tick`): a peer is dropped from this
    /// quorum denominator the instant EITHER signal says it is gone —
    ///   - the FAST `MembershipView` departure (`client.has_peer`): when a
    ///     peer's QUIC connection tears down the mesh-pump's
    ///     `handle_peer_disconnect` republishes the view WITHOUT it within
    ///     one pump cycle — the SAME idle-independent signal leg (C) reads
    ///     for the primary. This is what lets a lone survivor's quorum shrink
    ///     to the truly-live fleet PROMPTLY on a simultaneous peer loss,
    ///     instead of waiting out the slow `peer_timeout` (300s prod) reaper
    ///     — the lone-survivor wedge fix. In the supported topology
    ///     (reliable, non-firewalled inter-compute networking) a peer absent
    ///     from membership is DEAD, not partitioned, so dropping it is
    ///     correct; a TRULY-lone (never-meshed) secondary is still stopped
    ///     UPSTREAM by the formation-time `mesh_degraded` guard, which is
    ///     about never-having-meshed, not meshed-then-departed.
    ///   - the SLOW `peer_keepalives` reaper (`check_peer_timeouts`): the
    ///     backstop for a peer still nominally a transport member but
    ///     application-silent past `peer_timeout`.
    ///
    /// The intersection means a freshly-departed-but-not-yet-reaped peer
    /// (the live wedge: gone from membership, still in `peer_keepalives`
    /// because the 300s reaper has not fired) is correctly excluded NOW.
    ///
    /// SCOPE — candidate/voter ELIGIBILITY, not the quorum DENOMINATOR.
    /// This set answers "who could vote for or become a candidate", so the
    /// current primary is role-excluded unconditionally. The failover-quorum
    /// DENOMINATOR is the SEPARATE [`Self::failover_quorum_peer_count`],
    /// which adds the current primary back while the mesh membership still
    /// lists it: a member-listed primary is not GONE, merely role-excluded,
    /// and treating its exclusion as fleet shrinkage is what let a deposed
    /// half-partitioned ex-primary compute `failover_quorum(0) == 1` and
    /// metronomically self-promote (the asm-dataset primary ping-pong).
    pub(in crate::secondary) fn live_peer_ids(&self) -> impl Iterator<Item = &String> {
        let current_primary = self.cluster_state.current_primary();
        // `peer_keepalives` now lives in `OperationalState`; outside
        // `Operational` there are no peer keepalives to enumerate (the
        // election that calls this only runs `Operational`), so an empty
        // iterator is the faithful pre-`Operational` answer. All the
        // reads here (`cluster_state`, the operational state, the
        // membership view behind `is_mesh_member`) are shared `&self`
        // borrows, so they coexist.
        self.op_ref()
            .into_iter()
            .flat_map(|op| op.peer_keepalives.keys())
            .filter(move |id| Some(id.as_str()) != current_primary)
            // The live transport membership view, read through the same
            // single-source seam (`is_mesh_member`) leg (C) and the
            // deferrer-side death-evidence sites use.
            .filter(move |id| self.is_mesh_member(id))
    }

    /// Does the CURRENT primary still count toward the failover-quorum
    /// DENOMINATOR? `true` iff `current_primary()` names a REMOTE peer
    /// that is still a connected mesh member (`is_mesh_member` — the same
    /// single-source membership seam every death-evidence site reads).
    ///
    /// The distinction this draws (the fix for the asm-dataset election
    /// ping-pong): a peer can be ABSENT from [`Self::live_peer_ids`] for
    /// two very different reasons —
    ///   (a) it is GONE (membership-departed / death-observed): genuine
    ///       fleet shrinkage, and the quorum correctly adapts down; or
    ///   (b) it is the current primary, ROLE-excluded by the
    ///       minus-current-primary rule while the mesh membership still
    ///       lists it: NOT fleet shrinkage — the node is alive and merely
    ///       ineligible to vote/be a candidate about its own death.
    /// Treating (b) as shrinkage let a deposed ex-primary whose only OTHER
    /// peer sat behind an asymmetric dead leg compute an empty live set,
    /// take `failover_quorum(0) == 1`, and self-promote with zero peer
    /// agreement. Counting the member-listed primary keeps the quorum at
    /// `failover_quorum(1) == 2` there — unreachable without real
    /// agreement — while a primary that GENUINELY died still drops out
    /// within one mesh-pump cycle of its QUIC teardown (membership
    /// departure), so #317/#332 lone-/below-majority-survivor convergence
    /// is untouched.
    ///
    /// SELF-EXCLUSION mirrors [`Self::primary_departed_membership`]: a
    /// `current_primary` naming THIS node is structurally absent from its
    /// own membership view, and a node never quorum-counts itself through
    /// this path (the tallies add self separately).
    pub(in crate::secondary) fn current_primary_in_quorum(&self) -> bool {
        self.cluster_state
            .current_primary()
            .filter(|id| *id != self.config.secondary_id.as_str())
            .map(|id| self.is_mesh_member(id))
            .unwrap_or(false)
    }

    /// The failover-quorum DENOMINATOR: the candidate/voter-eligible live
    /// peers ([`Self::live_peer_ids`]) PLUS the current primary while the
    /// mesh membership still lists it ([`Self::current_primary_in_quorum`]).
    /// SINGLE SOURCE for both tally sites (the Suspecting agreement tally
    /// and the `PromotionConfirm` tally in [`Self::record_promotion_confirm`])
    /// so the two cannot desync; both feed it to the single-source
    /// [`super::failover_quorum`] rule.
    pub(in crate::secondary) fn failover_quorum_peer_count(&self) -> usize {
        self.live_peer_ids().count() + usize::from(self.current_primary_in_quorum())
    }

    /// The role-aware "alive secondaries" set — the single coordinator
    /// answer to "which peers run a LIVE SECONDARY right now", filtered
    /// POSITIVELY on the secondary capability. This is the role-aware
    /// count the PeerMesh deliberately CANNOT answer: the transport
    /// exposes only role-blind `peer_count()` (raw connection
    /// cardinality), so the peer-mesh-formation watchdog and the
    /// cold-start "is any secondary reachable" branch read THIS, computed
    /// over global state — never `transport.peer_count()`/`has_peer`.
    ///
    /// POSITIVE FILTER — never a role negation. A host runs any
    /// independent subset of {primary, secondary, observer} under one
    /// peer-id, so "is an alive secondary" is answered by the secondary
    /// capability itself, NOT by `!primary && !observer`. Consequently a
    /// host running primary+secondary under one peer-id COUNTS (it
    /// positively has a secondary), an observer is absent because it has no
    /// secondary (it
    /// emits no `Secondary` keepalive / advertises no workers), and a
    /// primary-only host is absent for the same positive reason. This is
    /// exactly where the failover-quorum notion DIFFERS: `live_peer_ids`
    /// additionally subtracts `current_primary` (you cannot elect the node
    /// being failed-over-from to replace itself), an election-specific
    /// rule that does NOT belong to the "is a secondary present" question.
    ///
    /// Two positive liveness signals, one per lifecycle regime — selected
    /// by which ledger physically exists, NOT by an arbitrary flag:
    ///   - OPERATIONAL (the `OperationalState` exists): `peer_keepalives`
    ///     keys. A peer is in this map IFF it emitted a `Secondary`
    ///     keepalive — positive proof it runs a live secondary (a
    ///     same-peer primary included; see the recognition arm in
    ///     `peer/message_handler.rs`). A peer swept by `check_peer_timeouts`
    ///     is correctly gone.
    ///   - SETUP / pre-operational (no `OperationalState`, so no
    ///     `peer_keepalives`): replicated membership via
    ///     [`ClusterState::alive_secondary_members`] (advertised
    ///     worker-secondary capacity AND live). It grows as each peer's
    ///     `PeerJoined` + `SecondaryCapacity` land during setup — the
    ///     faithful "has any secondary joined" answer for the cold-start
    ///     branch, where keepalives do not yet flow.
    pub(in crate::secondary) fn alive_secondary_ids(&self) -> Vec<String> {
        if let Some(op) = self.op_ref() {
            op.peer_keepalives.keys().cloned().collect()
        } else {
            self.cluster_state
                .alive_secondary_members()
                .map(str::to_owned)
                .collect()
        }
    }

    /// Count of [`Self::alive_secondary_ids`] — the role-aware
    /// alive-secondary cardinality. The peer-mesh-formation watchdog and
    /// the cold-start "is any secondary reachable" branch read this; it
    /// NEVER derives from `transport.peer_count()`.
    pub(in crate::secondary) fn alive_secondary_count(&self) -> usize {
        self.alive_secondary_ids().len()
    }

    /// Advance the election state. Called once per processing-loop tick.
    /// Returns the broadcast/self-send messages the loop should flush.
    pub(in crate::secondary) fn run_election_tick(&mut self) -> ElectionTickActions<I> {
        let mut actions = ElectionTickActions::default();
        if matches!(
            self.op_ref().map(|op| &op.election),
            Some(ElectionState::Promoted)
        ) {
            return actions;
        }

        // Snapshot the reads that touch `cluster_state` / `live_peer_ids`
        // (both `&self`-method/`cluster_state` borrows) up front, BEFORE
        // binding the `&mut OperationalState` for the state-machine match
        // below. `election`, `primary_last_seen`, and `peer_keepalives`
        // now all live inside `OperationalState`; the match needs a `&mut`
        // borrow of that state, which cannot coexist with the `&self`
        // borrows `live_peer_ids()` / `cluster_state.role_table()` take.
        // `peer_keepalives` does not change within a single tick (the
        // election reads it, never mutates it here), so a single snapshot
        // of the live-peer set + the observer set is faithful to the
        // original per-arm re-reads. `current_primary_id` is the silent
        // primary's diagnostic identity + the `TimeoutQuery` target.
        let current_primary_id = self.cluster_state.current_primary().map(str::to_owned);
        // (C) primary DEPARTED the transport mesh — the idle-independent
        // primary-death signal. That is the gap legs (A)/(B) cannot cover
        // for an IDLE survivor: (A) opens its health window only on a
        // `send_to_primary` no-route (an idle node issues none), and (B)
        // is the patient ~120s backstop; a node with no pending send to
        // the dead primary would otherwise loop the transport's
        // reconnect-ticker silently and never elect. The observation
        // itself — the `MembershipView` read, the self-exclusion, the
        // `primary_last_seen` seen-gate — is the single-source
        // [`Self::primary_departed_membership`], SHARED with the
        // deferrer-side consumer (`record_promotion_vote`) so the arming
        // and deferring sides cannot drift on what "the primary left the
        // mesh" means. Snapshotted here as a `&self` read alongside
        // `current_primary_id`, before the `&mut op` borrow below.
        let primary_left_membership_after_seen = self.primary_departed_membership();
        // UNION counterpart of the mesh-frame death legs: is the current
        // primary's transport-INDEPENDENT beacon still flowing? Snapshotted
        // here as a `&self` read of `beacon_liveness` (alongside
        // `current_primary_id` / `primary_left_membership_after_seen`), before the
        // `&mut op` borrow below. A CPU-starved-but-alive primary (its
        // single-threaded runtime pegged by a co-located build) freezes its
        // OUTBOUND mesh keepalive AND lets its QUIC connection idle-timeout —
        // tripping legs (A)/(C) — yet its dedicated-thread beacon keeps
        // asserting liveness. `true` here suppresses the spurious election;
        // a GENUINE primary death (no beacon AND no frame) leaves this
        // `false`, so the election still arms promptly (the #317 path).
        let primary_beacon_fresh = current_primary_id
            .as_deref()
            .map(|id| self.node_beacon_fresh(id))
            .unwrap_or(false);
        // (#361) Voting-state candidate-liveness re-evaluation, snapshotted
        // as a `&self` read before the `&mut op` borrow below: is the
        // CANDIDATE this node lent its vote to death-observed — departed
        // the transport membership with its beacon also silent, via the
        // SAME single-source seen-peer evidence rule the `TimeoutQuery`
        // responder applies (`peer_death_observed`)? Pre-#361 the Voting
        // state had NO tick arm at all: a voter whose candidate died
        // mid-election waited indefinitely for an external event (another
        // candidate's `PromotionVote`, or a primary frame), so on a small
        // fleet where the dead candidate was the only one the election
        // wedged forever. `false` whenever the election is not in Voting.
        let voting_candidate_dead = match self.op_ref().map(|op| &op.election) {
            Some(ElectionState::Voting { candidate, .. }) => self.peer_death_observed(candidate),
            _ => false,
        };
        let live_peers: Vec<String> = self.live_peer_ids().cloned().collect();
        // The quorum DENOMINATOR + its primary-inclusion evidence,
        // snapshotted as `&self` reads alongside `live_peers`. The
        // denominator differs from `live_peers.len()` exactly when the
        // current primary is still a connected mesh member: role-excluded
        // from candidacy, but NOT gone — see `current_primary_in_quorum`.
        let quorum_peer_count = self.failover_quorum_peer_count();
        let primary_counted_in_quorum = self.current_primary_in_quorum();
        // Leg-3 gate (deposed-primary re-assertion): a deposed ex-primary
        // may not take the lone-survivor in-tick self-quorum. A plain
        // coordinator-field read, snapshotted with the rest.
        let deposed_primary = self.deposed_primary;
        let observers: std::collections::HashSet<String> = self
            .cluster_state
            .role_table()
            .observers
            .iter()
            .cloned()
            .collect();
        let mesh_degraded = self.mesh.degraded;
        let secondary_id = self.config.secondary_id.clone();

        // Honest liveness — by SOURCE, not by a bare receive-staleness
        // clock. The decision to suspect the primary and start a failover
        // election is the OR of three predicates, each honest about a
        // distinct death mode, so the secondary rides out a transient
        // keepalive blip exactly as the primary side does (the QUIC layer
        // keeps a quiet-but-live link up to `max_idle_timeout` ≈60s):
        //
        //   (A) genuine-dead-link (fast, SEND-driven): `primary_link.should_arm_failover()`.
        //       The primary-link health window arms ONLY when a
        //       primary-bound send returns a no-route `Err` via
        //       `send_to_primary` (the connection is closed / no primary
        //       resolves). A live-but-app-quiet QUIC connection still
        //       enqueues sends `Ok` (mpsc to a live writer task), so this
        //       leg stays SILENT during a blip and fires only on a genuine
        //       link death — the honest fast signal, identical to the one
        //       `check_primary_link_threshold` already polls each keepalive
        //       tick. Structurally BLIND to an IDLE survivor that issues no
        //       primary-bound send (keepalives fan `Destination::All`, never
        //       no-route) — that gap is leg (C).
        //   (B) wedged-primary backstop (patient): `primary_last_seen`
        //       staleness past `primary_silence_backstop` (≈2 min). Covers
        //       the case neither (A) nor (C) can — a primary alive at QUIC
        //       (still a mesh member) but wedged at the application layer (it
        //       sends nothing yet its connection stays routable, so no send
        //       ever errors and it never leaves membership).
        //   (C) primary-left-membership (fast, IDLE-INDEPENDENT):
        //       `primary_left_membership_after_seen`. The mesh-pump
        //       republishes the `MembershipView` on the primary's QUIC
        //       teardown (`handle_peer_disconnect`), so `MeshClient::has_peer`
        //       flips `false` without any send from this node. This is the
        //       leg an IDLE survivor relies on: it neither issues the
        //       send (A) needs nor waits the ~120s (B) takes. Gated on
        //       `primary_last_seen.is_some()` so a not-yet-dialled relocation
        //       target does not false-arm (see the snapshot above).
        //
        // The bare `keepalive_interval × keepalive_miss_threshold` (≈15s)
        // receive-staleness trigger is GONE: it could not distinguish a
        // blip from a dead primary, so it spuriously elected at 15s during
        // a blip while the primary side patiently waited. Lengthening it to
        // 2 min would instead delay EVERY genuine failover by 2 min; the
        // (A)+(C) fast legs keep fast failover for a real dead/departed link
        // and (B) is patient only when death is genuinely indistinguishable
        // from a blip (still-routable, still-a-member, just silent).
        //
        // `primary_last_seen` is refreshed by `record_primary_message`,
        // driven by the role-tagged recognition path in `handle_inbound`'s
        // Keepalive arm: a `Primary`-tagged keepalive whose originator IS
        // the current primary refreshes it, so a promoted peer's primary
        // keepalives feed leg (B) exactly like a same-peer primary's
        // traffic once did. A same-peer primary's Secondary keepalive lands
        // in `peer_keepalives` (it is a live mesh peer) but is excluded from
        // quorum/candidate counts by `live_peer_ids`, so peer liveness and
        // primary liveness stay cleanly separate.
        //
        // Bind the operational state by direct field projection (borrows
        // only `self.lifecycle`, leaving `self.config` / `self.fatal_exit`
        // / `self.mesh` reachable as disjoint fields). The
        // `cluster_state` / `live_peer_ids` reads were already snapshotted
        // above, so nothing in the match below needs a `&self` method.
        let backstop = self.config.primary_silence_backstop;
        // The standard death deadline (`keepalive_interval ×
        // keepalive_miss_threshold`, ≈15s). It NO LONGER gates
        // `need_election` (the honest (A)+(B) split above replaced the bare
        // staleness trigger); it survives ONLY as the per-peer agreement
        // threshold the Suspecting-quorum tally compares each
        // `TimeoutResponse` age against — a SEPARATE predicate about
        // whether a PEER also sees the primary as silent, not about whether
        // WE should start an election.
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let op = self
            .lifecycle
            .operational_mut()
            .expect("run_election_tick reached before Operational — type invariant violation");

        // (A) genuine-dead-link — the primary link's own no-route arming.
        let link_dead = op.primary_link.should_arm_failover();
        // (B) wedged-primary backstop — patient receive-staleness, ONLY for
        // an app-silent primary whose link never armed leg (A).
        let primary_silence_exceeded = op
            .primary_last_seen
            .map(|t| Instant::now().duration_since(t) > backstop)
            .unwrap_or(false);
        // (C) primary departed the transport mesh — snapshotted above via
        // the single-source `primary_departed_membership()` (which carries
        // the `primary_last_seen.is_some()` seen-gate, the self-exclusion,
        // and the blip self-cancellation rationale).

        // The UNION death-clock (mirroring the primary-side reaper, where a
        // secondary is reaped iff BOTH its beacon and its frames are silent):
        // the mesh-frame disjunction declares the primary suspect, but the
        // election arms only when the primary's BEACON is ALSO silent. A
        // primary whose runtime is starved by a co-located build still emits
        // its dedicated-thread beacon, so `primary_beacon_fresh` short-circuits
        // the spurious failover; a genuine death (beacon also silent) arms it.
        let mesh_says_dead =
            link_dead || primary_silence_exceeded || primary_left_membership_after_seen;
        let need_election = mesh_says_dead && !primary_beacon_fresh;

        match &op.election {
            ElectionState::Normal if need_election => {
                // Degraded-mesh failover guard: the election protocol
                // needs a peer mesh to gather quorum responses
                // (`TimeoutQuery` / `TimeoutResponse` /
                // `PromotionVote` / `PromotionConfirm` all flow over
                // peer_transport). With zero peers, the next
                // Suspecting tick would self-promote on `quorum=1` —
                // the same secondary that just lost its only primary
                // would unilaterally claim authority. That's worse
                // than failing loud: there's no other surviving
                // node to coordinate with, so the cluster is
                // already unsalvageable. Bail with a clear reason
                // instead of pretending the election succeeded.
                // Who the silent primary was (a same-peer primary or a
                // promoted peer), snapshotted above from the single source of "who
                // is primary now" — BOTH the diagnostic identity and the
                // `TimeoutQuery::query_node_id` the peers reply about. It
                // drives no election decision (that is `primary_silent`
                // alone).
                if mesh_degraded {
                    let reason = format!(
                        "peer mesh required for failover but not \
                         available: primary death suspected \
                         (link_dead={link_dead}, \
                         primary_silence_exceeded={primary_silence_exceeded}, \
                         primary_left_membership={primary_left_membership_after_seen}) \
                         and no peers connected to elect a new primary; \
                         exiting",
                    );
                    tracing::error!(
                        secondary = %secondary_id,
                        link_dead,
                        primary_silence_exceeded,
                        primary_left_membership = primary_left_membership_after_seen,
                        primary = ?current_primary_id,
                        "{reason}"
                    );
                    // `fatal_exit` is a flat coordinator write-latch (a
                    // disjoint field from `self.lifecycle`, so writable
                    // while `op` is borrowed). The loop reads it once per
                    // iteration and exits non-zero.
                    self.fatal_exit = Some(reason);
                    return actions;
                }
                tracing::warn!(
                    secondary = %secondary_id,
                    link_dead,
                    primary_silence_exceeded,
                    primary_left_membership = primary_left_membership_after_seen,
                    primary = ?current_primary_id,
                    "primary death suspected (dead link, app-silence backstop, \
                     or primary departed the mesh); entering Suspecting"
                );
                op.election = ElectionState::Suspecting {
                    since: Instant::now(),
                    responses: HashMap::new(),
                };
                push_timeout_query(&mut actions.broadcast, &secondary_id, current_primary_id);
            }
            ElectionState::Suspecting { since, responses } => {
                // Wait at least `keepalive_interval` to gather peer responses
                // before counting votes. This is the natural window for a
                // peer to reply once it has also seen the primary go silent.
                if since.elapsed() < self.config.keepalive_interval {
                    return actions;
                }
                // The quorum DENOMINATOR (`failover_quorum_peer_count`,
                // snapshotted above): the candidate-eligible `live_peers`
                // PLUS the current primary while the mesh membership still
                // lists it. A member-listed primary is role-excluded from
                // candidacy but NOT gone, so it must not shrink the
                // denominator — that shrinkage is what let a deposed
                // ex-primary behind an asymmetric dead leg reach
                // `failover_quorum(0) == 1` and self-promote solo.
                let peer_count = quorum_peer_count;
                // Each `TimeoutResponse.last_keepalive` is now a monotonic AGE
                // (seconds since the responder last saw the queried node, on the
                // responder's own monotonic clock — see the `TimeoutQuery` arm),
                // NOT an absolute wall-clock timestamp. A peer "agrees" the
                // primary is silent iff that age exceeds the death deadline; a
                // `None` (never seen) also agrees. Comparing a relative age means
                // there is NO cross-node wall-clock subtraction, so a coordinated
                // suspend/resume cannot fabricate a false quorum on primary death.
                let agreeing = responses
                    .values()
                    .filter(|age| {
                        age.map(|secs| secs > deadline.as_secs_f64())
                            .unwrap_or(true)
                    })
                    .count()
                    + 1; // include us
                // Single-source quorum rule (`election::failover_quorum`):
                // adapts to the CURRENT live-peer set (`peer_count` from
                // `live_peer_ids`, which shrinks symmetrically on a
                // partition — the 2-node-trap fix), never a fixed
                // `config.num_secondaries`. Observers are absent from
                // `peer_count` by construction (no `Secondary` keepalive).
                let quorum = failover_quorum(peer_count);
                if agreeing < quorum {
                    // RE-POLL while waiting for quorum. The `TimeoutQuery` is
                    // a CONTINUOUS liveness question, not a one-shot: under an
                    // abrupt (non-graceful) primary crash the survivors evict
                    // the dead primary from their views at DIFFERENT instants,
                    // so a peer queried before IT has observed the death
                    // answers "primary still fresh" (age < deadline) — a
                    // disagreeing response. Pre-fix the query fired exactly
                    // once on entering Suspecting, so that single stale answer
                    // was cached forever (`record_timeout_response` only
                    // overwrites a peer's entry when a NEW response arrives,
                    // and none ever would): `agreeing` stayed pinned below
                    // quorum and the candidate wedged in Suspecting at round 1
                    // — never advancing, never self-promoting. Re-emitting the
                    // query each waiting tick makes every peer re-answer with
                    // its CURRENT view, so once a peer observes the death its
                    // refreshed (agreeing) response replaces the stale one and
                    // quorum converges — no dependence on all survivors
                    // observing the departure at the same instant. The target
                    // is the same silent primary the entering-Suspecting query
                    // named (`current_primary_id`, snapshotted above). Cheap:
                    // one broadcast per keepalive tick only while a genuine
                    // election is unresolved. Shares the SINGLE query-builder
                    // (`push_timeout_query`) the entering-Suspecting emit uses
                    // so the two sites cannot drift.
                    push_timeout_query(&mut actions.broadcast, &secondary_id, current_primary_id);
                    // Names the denominator evidence (silent-branch rule):
                    // `primary_counted_in_quorum == true` says the refusal
                    // to proceed is (at least partly) because the suspected
                    // primary is still a connected mesh member —
                    // membership-says-alive, so unilateral progress is
                    // forbidden — as opposed to genuinely missing peer
                    // agreement on a death-observed primary.
                    tracing::info!(
                        agreeing,
                        quorum,
                        live_peers = live_peers.len(),
                        primary_counted_in_quorum,
                        "no quorum on primary death; re-polling peers"
                    );
                    return actions;
                }
                // Filter observer peers from candidate selection. An
                // observer in the alive set with a lex-low ID would
                // otherwise be deferred-to by non-observer peers; the
                // observer (a standalone ObserverCoordinator) is never a
                // candidate, so deferring to it would stall the cluster.
                //
                // Read source: `cluster_state.role_table().observers`
                // is the replicated single source of truth. Populated by
                // the `ClusterMutation::PeerJoined { is_observer: true }`
                // apply rule — originated by the primary in
                // `primary/peer_setup.rs::send_peer_lists` and replicated
                // to every node via the standard CRDT broadcast path. This
                // peer-side filter is the sole observer guard: a compute
                // SecondaryCoordinator is never itself an observer (the
                // observer role IS the ObserverCoordinator), so `self` is
                // always a legitimate candidate and is included
                // unconditionally.
                // `observers` + `live_peers` were snapshotted above (both
                // were `&self`/`cluster_state` reads that cannot coexist
                // with the `&mut op` borrow held by this match).
                let lowest_alive = live_peers
                    .iter()
                    .filter(|id| !observers.contains(*id))
                    .chain(std::iter::once(&secondary_id))
                    .min()
                    .cloned();
                let we_lead = lowest_alive
                    .as_ref()
                    .map(|id| id == &secondary_id)
                    .unwrap_or(false);
                let round = next_round(&op.election);
                if we_lead {
                    // The candidate counts ITSELF as one confirm. When the
                    // live-peer fleet is empty (`peer_count == 0`, so
                    // `quorum == failover_quorum(0) == 1`) that single self-
                    // confirm ALREADY meets quorum: there is no peer whose
                    // `PromotionConfirm` could ever arrive to drive
                    // `record_promotion_confirm`, so awaiting one would wedge
                    // the candidate at round 1 (timeout → retry round+1
                    // forever) and the lone survivor would never converge.
                    // Commit the promotion in-tick using the SAME terminal
                    // transition (`Promoted` + caller-driven
                    // `fire_local_promotion`) the peer-confirm path reaches,
                    // keyed on the SAME single-source `failover_quorum` rule.
                    // This is NOT the split-brain `quorum == 1` case the
                    // `mesh_degraded` guard above blocks — that guard already
                    // fatal-exited a TRULY lone (never-meshed) secondary before
                    // any tally; reaching here means the mesh DID form (this
                    // survivor was failover-capable) and its peers have since
                    // departed, which is exactly the relocation-completeness
                    // case a single survivor must win.
                    let self_confirm: HashSet<String> = HashSet::from([secondary_id.clone()]);
                    if self_confirm.len() >= quorum && !deposed_primary {
                        tracing::info!(
                            round,
                            quorum,
                            "self-promoting: single-survivor self-quorum already met — \
                             committing promotion (no peer confirm to await)"
                        );
                        op.election = ElectionState::Promoted;
                        actions.promoted = true;
                    } else {
                        if self_confirm.len() >= quorum {
                            // The self-quorum WAS met but this node is a
                            // DEPOSED ex-primary (leg 3): the fleet elected
                            // around it once, so its lone-survivor view is
                            // suspect (the production ping-pong was exactly
                            // this node behind an asymmetric dead leg).
                            // Refuse the in-tick commit and campaign as a
                            // Candidate instead — promotion then requires a
                            // real peer `PromotionConfirm` (positive
                            // agreement), which `record_promotion_confirm`
                            // grants on the first one at this quorum.
                            tracing::warn!(
                                round,
                                quorum,
                                live_peers = live_peers.len(),
                                primary_counted_in_quorum,
                                "refusing lone-survivor self-promotion: this \
                                 node is a DEPOSED ex-primary; campaigning as \
                                 Candidate and awaiting positive peer agreement"
                            );
                        } else {
                            tracing::info!(round, "self-promoting");
                        }
                        op.election = ElectionState::Candidate {
                            round,
                            confirms: self_confirm,
                            started: Instant::now(),
                        };
                        // No transitional self-as-primary routing target —
                        // authority is committed only once this candidate
                        // wins quorum (`record_promotion_confirm` reaches
                        // `Promoted`). The failover re-point — broadcasting +
                        // applying `PrimaryChanged { new = self }` so surviving
                        // secondaries' `cluster_state.current_primary()` moves
                        // onto this winner's mesh peer-id — is the composed
                        // runtime's terminal action on that transition, not a
                        // transitional Voting-time hint.
                        actions.broadcast.push(DistributedMessage::PromotionVote {
                            target: None,
                            sender_id: secondary_id.clone(),
                            timestamp: timestamp_now(),
                            candidate_id: secondary_id.clone(),
                            vote_round: round,
                        });
                    }
                } else if let Some(candidate) = lowest_alive {
                    tracing::info!(%candidate, round, "deferring to lowest-live-id peer");
                    // No transitional routing target (see above).
                    op.election = ElectionState::Voting { round, candidate };
                }
            }
            ElectionState::Candidate { round, started, .. } => {
                // Conservative timeout: if no quorum within 5 keepalive
                // intervals, restart the election with a higher round.
                let timeout = self.config.keepalive_interval.saturating_mul(5);
                if started.elapsed() > timeout {
                    let next = round + 1;
                    tracing::warn!(round, "candidate timed out, retrying with round {next}");
                    op.election = ElectionState::Candidate {
                        round: next,
                        confirms: HashSet::from([secondary_id.clone()]),
                        started: Instant::now(),
                    };
                    actions.broadcast.push(DistributedMessage::PromotionVote {
                        target: None,
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: secondary_id.clone(),
                        vote_round: next,
                    });
                }
            }
            // (#361) The CANDIDATE this node lent its vote to has died
            // mid-election — death-observed via the snapshotted
            // single-source `peer_death_observed` rule (membership
            // departure ∩ beacon-silent ∩ seen-gate, self-excluded — the
            // same evidence the `TimeoutQuery` responder reports on).
            // Waiting any longer is a wedge: the dead candidate will never
            // broadcast the winning `PrimaryChanged`, and on a small fleet
            // there may be no OTHER candidate whose `PromotionVote` could
            // re-pull this voter. RELEASE the vote — revert to Suspecting
            // — and let the normal election machinery re-run: the re-poll
            // gathers fresh peer agreement and the next tally's
            // lowest-live-id selection no longer sees the departed
            // candidate (`live_peer_ids` intersects the same membership
            // view), so a SURVIVOR emerges as the new candidate — possibly
            // this node, possibly alone via the single-survivor
            // self-quorum.
            //
            // NO CHURN on a blip: the observation is a LIVE `has_peer` +
            // beacon read at tick time (non-latching, the #331 shape), so
            // a transient departure+rejoin, or a CPU-starved-but-alive
            // candidate still beaconing, reads alive again and the guard
            // never fires — a live candidate's election proceeds untouched
            // through the `_` arm below.
            //
            // The `mesh_degraded` lone-secondary guard needs no mirror
            // here: `degraded` latches only at FORMATION time (zero peers
            // ever meshed — see `mesh_watchdog`), and a node in Voting
            // necessarily meshed (the vote/quorum traffic arrived over the
            // mesh), so the re-entered Suspecting machinery runs strictly
            // downstream of that guard exactly as any armed election does.
            ElectionState::Voting { round, candidate } if voting_candidate_dead => {
                tracing::warn!(
                    secondary = %secondary_id,
                    candidate = %candidate,
                    round,
                    "candidate death observed mid-election (departed the mesh, \
                     beacon silent); releasing vote and re-entering Suspecting"
                );
                op.election = ElectionState::Suspecting {
                    since: Instant::now(),
                    responses: HashMap::new(),
                };
                // Re-ask the fleet about the still-silent primary — the
                // same entering-Suspecting emit the Normal arm fires,
                // through the single query-builder so the sites cannot
                // drift.
                push_timeout_query(&mut actions.broadcast, &secondary_id, current_primary_id);
            }
            _ => {}
        }
        actions
    }

    /// Handle an incoming `TimeoutResponse` from a peer (called from
    /// `handle_peer_message`). Stores the response in the Suspecting bucket;
    /// the next tick will tally.
    ///
    /// `last_keepalive` is the responder's monotonic AGE (seconds since it last
    /// saw the queried node, on the responder's own clock), not an absolute
    /// timestamp — the Suspecting tally compares it directly to the death
    /// deadline. `None` = never seen.
    pub(in crate::secondary) fn record_timeout_response(
        &mut self,
        peer: String,
        last_keepalive: Option<f64>,
    ) {
        if let ElectionState::Suspecting { responses, .. } = &mut self.op_mut().election {
            responses.insert(peer, last_keepalive);
        }
    }

    /// Handle an incoming `PromotionVote` from a peer. We confirm only if
    /// the candidate is the lowest-live-id we know about and we have
    /// independently observed the primary as dead — silent frames OR a
    /// transport membership departure (#331). Returns the message to send
    /// back, if any.
    pub(in crate::secondary) fn record_promotion_vote(
        &mut self,
        candidate: String,
        round: u32,
    ) -> Option<DistributedMessage<I>> {
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let frame_silent = self
            .op_ref()
            .and_then(|op| op.primary_last_seen)
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(true);
        // Deferrer-side leg (C) (#331): the current primary DEPARTING the
        // transport membership is this node's own, independent death
        // observation — the same single-source
        // [`Self::primary_departed_membership`] the arming side reads.
        // Pre-fix the deferrer's only mesh-frame evidence was
        // `frame_silent` (its own receive-staleness crossing the ~15s
        // death deadline), so even though every survivor WATCHED the
        // primary's QUIC teardown within one pump cycle, each still
        // WITHHELD its confirm until its own silence clock ran out —
        // bounding failover convergence below by the deadline. With the
        // membership observation the deferrer lends its confirm the
        // moment it has independently seen the death; a transient blip
        // self-debounces exactly as on the arming side (seen-gate,
        // `has_peer` reading `true` again by the next vote, and the
        // unchanged beacon union below).
        let membership_departed = self.primary_departed_membership();
        // UNION with the beacon (mirror the arm-side `need_election`): a peer
        // that armed an election against a CPU-starved-but-alive primary
        // would otherwise pull our confirm on the frame-silence alone. The
        // primary's beacon proves it is alive, so refuse to confirm while the
        // beacon flows — a peer's spurious election cannot reach quorum
        // through us. A genuine death (beacon also silent) leaves
        // `primary_silent` true, so #317 convergence is intact.
        let current_primary = self.cluster_state.current_primary().map(str::to_owned);
        let beacon_fresh = current_primary
            .as_deref()
            .map(|id| self.node_beacon_fresh(id))
            .unwrap_or(false);
        let primary_silent = (frame_silent || membership_departed) && !beacon_fresh;
        if !primary_silent {
            return None;
        }
        // FIRST-HAND REACHABILITY VETO: never lend a confirm to a candidate
        // THIS node currently has no route to (absent from the live
        // transport `MembershipView` — the same single-source membership
        // seam every death-evidence site reads). A vote for an unreachable
        // candidate is a vote for a primary this voter could never follow.
        // Structurally the lowest-live-id check below already implies this
        // (`live_peer_ids` ⊆ membership, so a non-member can never be the
        // computed lowest) — this names the decision and its evidence
        // instead of rejecting through a silent id mismatch (and keeps the
        // veto in force even if candidate selection evolves). `candidate ==
        // self` is exempt: a node is structurally absent from its OWN
        // membership view, and reaching itself needs no route.
        if candidate != self.config.secondary_id && !self.is_mesh_member(&candidate) {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                candidate = %candidate,
                round,
                "vetoing PromotionVote: no route to the candidate (absent \
                 from this voter's live transport membership)"
            );
            return None;
        }
        // `live_peer_ids` is a `&self` read (cluster_state + op_ref); take
        // the `lowest` owned value here, then borrow `op` mutably below
        // for the election write — the two borrows don't overlap.
        let lowest = self
            .live_peer_ids()
            .chain(std::iter::once(&self.config.secondary_id))
            .min()
            .cloned();
        if lowest.as_ref() != Some(&candidate) {
            return None;
        }
        // Adopt this candidate as our voting target; transition out of
        // Suspecting/Candidate so we don't double-vote. No transitional
        // routing target is written — `Role::Primary` re-points only on
        // the winner's authoritative `PrimaryChanged` (P2).
        let sender_id = self.config.secondary_id.clone();
        let op = self.op_mut();
        let already_voting_for_this_round = matches!(
            &op.election,
            ElectionState::Voting { round: r, candidate: c }
                if *r == round && c == &candidate
        );
        if !already_voting_for_this_round {
            op.election = ElectionState::Voting {
                round,
                candidate: candidate.clone(),
            };
        }
        Some(DistributedMessage::PromotionConfirm {
            target: None,
            sender_id,
            timestamp: timestamp_now(),
            new_primary_id: candidate,
            vote_round: round,
        })
    }

    /// Handle an incoming `PromotionConfirm`. If we're the candidate of the
    /// matching round and we've collected a majority, promote ourselves.
    /// Returns true if we just became the new primary.
    pub(in crate::secondary) fn record_promotion_confirm(
        &mut self,
        peer: String,
        target: String,
        round: u32,
    ) -> bool {
        if target != self.config.secondary_id {
            return false;
        }
        // The quorum DENOMINATOR (`failover_quorum_peer_count` — the same
        // single source the Suspecting tally reads, so the two sites cannot
        // desync): candidate-eligible live peers PLUS a still-member-listed
        // current primary. A `&self` read; take the owned count before
        // borrowing `op` mutably for the confirm tally + transition.
        let peer_count = self.failover_quorum_peer_count();
        // Same single-source quorum rule as the Suspecting tally
        // (`election::failover_quorum`) — the two sites read ONE function so
        // the majority arithmetic cannot desync on a live failover.
        let quorum = failover_quorum(peer_count);
        let op = self.op_mut();
        let promoted = match &mut op.election {
            ElectionState::Candidate {
                round: r, confirms, ..
            } if *r == round => {
                confirms.insert(peer);
                confirms.len() >= quorum
            }
            _ => false,
        };
        if promoted {
            tracing::info!(round, "won election — taking over as primary");
            // The election state machine only records the terminal
            // `Promoted` state transition. Activating the same-peer
            // primary — `PrimaryCoordinator::activate_local_primary`,
            // which on this seeded-resume path hydrates the pool from the
            // replicated CRDT and enters the operational loop — is the
            // terminal action the composed runtime drives off this
            // transition; the secondary carries no self-promotion mirror
            // to flip on here.
            op.election = ElectionState::Promoted;
        }
        promoted
    }

    /// Terminal action for THIS node winning its failover election:
    /// originate the single primary-activation frame
    /// `ClusterMutation::PrimaryChanged { new = self, epoch =
    /// primary_epoch + 1 }`, apply it locally so this winner's OWN apply
    /// hook fires, and broadcast it so every surviving peer re-points.
    ///
    /// Unified onto the `PrimaryChanged` apply hook
    /// (`apply_cluster_mutations` → `apply_primary_changed`): there is no
    /// separate role-flip wire frame and no separate direct activation
    /// call — the single `PrimaryChanged` frame carries both. The flow:
    ///   1. **Local apply** via `apply_cluster_mutations` — the winner's
    ///      own apply hook (`apply_primary_changed`) runs, signalling that
    ///      this node is now the named primary (the build of the
    ///      `PrimaryCoordinator` on the promotion event is the Phase-C
    ///      `Process` concern — see the C4 seam in `apply_primary_changed`)
    ///      and resetting this node's election to `Normal` (a primary now
    ///      exists — no lingering `Promoted`). Idempotent if the broadcast
    ///      is later echoed back through the mesh receive path.
    ///   2. **Broadcast** the same `PrimaryChanged` onto the mesh
    ///      (`Destination::All`). Surviving secondaries apply it via the
    ///      SAME hook, moving their `cluster_state.current_primary()` onto
    ///      this winner's mesh peer-id. Their next `Destination::Primary`
    ///      send then resolves to this winner at the egress edge — the
    ///      entire failover re-route. `epoch + 1` strictly supersedes the
    ///      prior identity (last-writer-wins on epoch), so a delayed
    ///      lower-epoch broadcast cannot un-elect this winner.
    ///
    /// The OLD primary, observing this `PrimaryChanged`, becomes a pure
    /// observer (it no longer holds `Role::Primary`) — R3's observe path.
    /// This is the same originate-apply-broadcast shape the live primary's
    /// `apply_and_broadcast_cluster_mutations` uses; the only difference
    /// is the local apply routes through `apply_cluster_mutations` so the
    /// secondary-side wake hook fires.
    pub(in crate::secondary) async fn fire_local_promotion(&mut self) {
        let epoch = self.cluster_state.primary_epoch() + 1;
        let mutation = dynrunner_protocol_primary_secondary::ClusterMutation::PrimaryChanged {
            new: self.config.secondary_id.clone(),
            epoch,
            // Election win (`new == self`): this winner names ITSELF the
            // primary. The bootstrap-transfer reason is set only by the
            // submitter's relocate path naming a DIFFERENT chosen peer.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        };

        // (1) Apply locally through the unified hook so THIS winner's own
        // apply hook fires (Phase-C seam: signal `Process` to build the
        // primary on the self-named promotion + reset election).
        self.apply_cluster_mutations(vec![mutation.clone()]);

        // (2) Broadcast the re-point so surviving peers apply the same
        // frame through the same hook. epoch+1 strictly supersedes the
        // prior primary identity.
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            mutations: vec![mutation],
        };
        if let Err(e) = self
            .send_to(dynrunner_protocol_primary_secondary::Destination::All, msg)
            .await
        {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                error = %e,
                "PrimaryChanged(new=self) failover broadcast failed; \
                 surviving secondaries will re-point on the next election \
                 round or via CRDT snapshot reconciliation"
            );
        }
    }
}
