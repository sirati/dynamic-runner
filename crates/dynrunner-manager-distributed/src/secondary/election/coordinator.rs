//! `SecondaryCoordinator` methods implementing the failover-election
//! state machine. The pure-data parts of the state machine
//! (`ElectionState`, `ElectionTickActions`, the round-bump helper)
//! live in [`super`]; this file contains only the per-method handlers
//! that mutate the coordinator's election + primary-link fields.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;
use super::{ElectionState, ElectionTickActions, next_round};

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
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
    pub(in crate::secondary) fn record_primary_message(&mut self) {
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
        }
    }

    /// The live mesh peers for failover quorum/candidate reasoning: the
    /// keys of `peer_keepalives` MINUS the current primary's host id.
    ///
    /// A co-located primary+secondary host emits a `Secondary` keepalive
    /// that lands in `peer_keepalives` even though its id is the current
    /// primary (the recognition arm tracks a multi-role host as BOTH). That
    /// entry is correct as peer-mesh liveness but must NEVER inflate
    /// election counts — the primary is the role being failed-over FROM, not
    /// a peer that could vote for or become a candidate. Excluding it here
    /// is the single quorum-side counterpart to the peer-timeout sweep's
    /// own current-primary skip (`check_peer_timeouts`), both keyed on the
    /// single source of "who is primary now" (`current_primary()`).
    pub(in crate::secondary) fn live_peer_ids(&self) -> impl Iterator<Item = &String> {
        let current_primary = self.cluster_state.current_primary();
        // `peer_keepalives` now lives in `OperationalState`; outside
        // `Operational` there are no peer keepalives to enumerate (the
        // election that calls this only runs `Operational`), so an empty
        // iterator is the faithful pre-`Operational` answer. Both
        // borrows (`cluster_state` + the operational state) are shared
        // `&self` reads of disjoint fields, so they coexist.
        self.op_ref()
            .into_iter()
            .flat_map(|op| op.peer_keepalives.keys())
            .filter(move |id| Some(id.as_str()) != current_primary)
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
    /// co-located primary+secondary host COUNTS (it positively has a
    /// secondary), an observer is absent because it has no secondary (it
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
    ///     keepalive — positive proof it runs a live secondary (the
    ///     co-located primary included; see the recognition arm in
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
        // Strict-observer suppression: an observer is a passive bystander
        // with ZERO authority/responsibility. It never participates in
        // failover — it doesn't suspect a silent primary, doesn't
        // broadcast TimeoutQuery / PromotionVote, and can never become a
        // candidate. Its only cue is the replicated `run_complete()`.
        // This is the failover concern's own role-gate (the election
        // module OWNS failover), matching `send_keepalive`'s and
        // `report_mesh_ready_if_needed`'s self-gates — not a scattered
        // cross-concern branch.
        if self.config.is_observer {
            return actions;
        }
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
        let live_peers: Vec<String> = self.live_peer_ids().cloned().collect();
        let observers: std::collections::HashSet<String> =
            self.cluster_state.role_table().observers.iter().cloned().collect();
        let mesh_degraded = self.mesh.degraded;
        let secondary_id = self.config.secondary_id.clone();
        let is_observer = self.config.is_observer;

        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        // `primary_silent` is the SOLE liveness predicate for whoever
        // currently holds the primary role — co-located OR a promoted
        // peer. `primary_last_seen` is refreshed by
        // `record_primary_message`, driven by the role-tagged recognition
        // path in `handle_inbound`'s Keepalive arm: a `Primary`-tagged
        // keepalive whose originator IS the current primary (resolved via
        // `cluster_state.current_primary()`, the single source of "who is
        // primary now") refreshes `primary_last_seen`. So a promoted peer's
        // primary keepalives refresh it exactly like the co-located
        // primary's dispatch traffic once did, and a genuinely-dead primary
        // trips `primary_silent` once its primary keepalives stop — there is
        // no longer a separate promoted-peer-primary liveness axis to track.
        //
        // This subsumes the former cascade trigger (`primary_peer_silent`,
        // which read the promoted primary's `peer_keepalives` entry): that
        // branch is both REDUNDANT (the recognition path keeps
        // `primary_last_seen` fresh via the Primary keepalive) and BROKEN
        // (it would fire against a HEALTHY just-promoted primary and storm
        // `TimeoutQuery`, risking double-promotion). The cascade
        // (promoted-peer-died) case Dataset's K=2 run hit is covered by
        // `primary_silent`. A co-located primary's Secondary keepalive DOES
        // land in `peer_keepalives` (it is a live mesh peer), but that
        // entry is excluded from quorum/candidate counts by `live_peer_ids`,
        // so peer liveness and primary liveness stay cleanly separate.
        // Bind the operational state by direct field projection (borrows
        // only `self.lifecycle`, leaving `self.config` / `self.fatal_exit`
        // / `self.mesh` reachable as disjoint fields). The
        // `cluster_state` / `live_peer_ids` reads were already snapshotted
        // above, so nothing in the match below needs a `&self` method.
        let op = self
            .lifecycle
            .operational_mut()
            .expect("run_election_tick reached before Operational — type invariant violation");

        let primary_silent = op
            .primary_last_seen
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(false);

        let need_election = primary_silent;

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
                // Who the silent primary was (co-located or a promoted
                // peer), snapshotted above from the single source of "who
                // is primary now" — BOTH the diagnostic identity and the
                // `TimeoutQuery::query_node_id` the peers reply about. It
                // drives no election decision (that is `primary_silent`
                // alone).
                if mesh_degraded {
                    let reason = format!(
                        "peer mesh required for failover but not \
                         available: primary went silent (primary_silent={}) \
                         and no peers connected to elect a new primary; \
                         exiting",
                        primary_silent,
                    );
                    tracing::error!(
                        secondary = %secondary_id,
                        primary_silent,
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
                    miss_threshold = self.config.keepalive_miss_threshold,
                    primary_silent,
                    primary = ?current_primary_id,
                    "primary missed keepalives; entering Suspecting"
                );
                op.election = ElectionState::Suspecting {
                    since: Instant::now(),
                    responses: HashMap::new(),
                };
                if let Some(query_node_id) = current_primary_id {
                    actions.broadcast.push(DistributedMessage::TimeoutQuery {
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        query_node_id,
                    });
                }
            }
            ElectionState::Suspecting { since, responses } => {
                // Wait at least `keepalive_interval` to gather peer responses
                // before counting votes. This is the natural window for a
                // peer to reply once it has also seen the primary go silent.
                if since.elapsed() < self.config.keepalive_interval {
                    return actions;
                }
                // `live_peers` was snapshotted from `live_peer_ids`, which
                // already excludes the current primary, so its length IS
                // the live-peer count.
                let peer_count = live_peers.len();
                let agreeing = responses
                    .values()
                    .filter(|last| {
                        last.map(|t| (timestamp_now() - t) > deadline.as_secs_f64())
                            .unwrap_or(true)
                    })
                    .count()
                    + 1; // include us
                let quorum = peer_count.div_ceil(2) + 1;
                if agreeing < quorum {
                    tracing::info!(agreeing, quorum, "no quorum on primary death; waiting");
                    return actions;
                }
                // Task #36 / Step 7: filter observer peers from
                // candidate selection. An observer in the alive set
                // with a lex-low ID would otherwise be deferred-to by
                // non-observer peers; the observer would then refuse
                // self-promotion (#35 self-skip), stalling the
                // cluster. Filtering at the peer-side complements
                // the observer's self-exclusion: both sides agree
                // observers can't be candidates. The #35 self-only
                // guard below (`is_observer && we_lead`) becomes
                // belt-and-suspenders once `RoleTable.observers` is
                // populated end-to-end, but stays in place for the
                // pre-PeerInfo window (election can run before the
                // first PeerInfo arrives in adversarial timings).
                //
                // Read source: `cluster_state.role_table().observers`
                // is the replicated single source of truth (Step 7,
                // Decision G). Populated by the `ClusterMutation::
                // PeerJoined { is_observer: true }` apply rule —
                // originated by the primary in
                // `primary/peer_setup.rs::send_peer_lists` and
                // replicated to every node via the standard CRDT
                // broadcast path.
                // `observers` + `live_peers` were snapshotted above (both
                // were `&self`/`cluster_state` reads that cannot coexist
                // with the `&mut op` borrow held by this match).
                let lowest_alive = live_peers
                    .iter()
                    .filter(|id| !observers.contains(*id))
                    .chain(std::iter::once(&secondary_id).filter(|_| !is_observer))
                    .min()
                    .cloned();
                let we_lead = lowest_alive
                    .as_ref()
                    .map(|id| id == &secondary_id)
                    .unwrap_or(false);
                let round = next_round(&op.election);
                // Observer self-exclusion (#35): even when our id is
                // the lex-lowest in the alive set, an observer MUST
                // NOT self-promote. Observers are non-candidates by
                // design — they receive cluster updates but cannot
                // host the primary role (no workers, no dispatch
                // authority). The full fortification (peers also
                // skipping observers when picking `lowest_alive`)
                // needs `PeerConnectionInfo.is_observer`, tracked as
                // a follow-up; until that lands the observer
                // self-policing is the load-bearing guard.
                //
                // If we_lead but is_observer, we defer to the NEXT-
                // lowest-id peer that ISN'T us — that peer will
                // self-promote on its own tick. If we're the only
                // alive secondary (peer_keepalives empty), there's
                // no candidate at all and we stay Voting (effectively
                // waiting for another secondary to come online); the
                // peer_mesh_degraded guard above catches the
                // pathological "alone and primary's dead" case.
                if is_observer && we_lead {
                    let next_lowest = live_peers.iter().min().cloned();
                    tracing::info!(
                        observer = %secondary_id,
                        ?next_lowest,
                        round,
                        "observer would have self-promoted by lowest-id but \
                         is non-candidate; deferring to next-lowest peer \
                         (peers without observer-awareness will need to \
                         self-promote on their own ticks)"
                    );
                    if let Some(candidate) = next_lowest {
                        // No transitional routing target: `Role::Primary`
                        // is re-pointed only by the winner's
                        // authoritative `PrimaryChanged` (broadcast +
                        // applied after winning), never by an in-flight
                        // Voting transition. See the P2
                        // drop-the-transitional-hint design.
                        op.election = ElectionState::Voting { round, candidate };
                    }
                    // No next_lowest = we're the only one alive AND
                    // we're an observer. Don't transition; let the
                    // peer-mesh-degraded path catch this in a future
                    // tick (or a new secondary arrival fixes it).
                } else if we_lead {
                    tracing::info!(round, "self-promoting");
                    op.election = ElectionState::Candidate {
                        round,
                        confirms: HashSet::from([secondary_id.clone()]),
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
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: secondary_id.clone(),
                        vote_round: round,
                    });
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
                        sender_id: secondary_id.clone(),
                        timestamp: timestamp_now(),
                        candidate_id: secondary_id.clone(),
                        vote_round: next,
                    });
                }
            }
            _ => {}
        }
        actions
    }

    /// Handle an incoming `TimeoutResponse` from a peer (called from
    /// `handle_peer_message`). Stores the response in the Suspecting bucket;
    /// the next tick will tally.
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
    /// the candidate is the lowest-live-id we know about and we also see
    /// the primary as silent. Returns the message to send back, if any.
    pub(in crate::secondary) fn record_promotion_vote(
        &mut self,
        candidate: String,
        round: u32,
    ) -> Option<DistributedMessage<I>> {
        let deadline = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold);
        let primary_silent = self
            .op_ref()
            .and_then(|op| op.primary_last_seen)
            .map(|t| Instant::now().duration_since(t) > deadline)
            .unwrap_or(true);
        if !primary_silent {
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
        // `live_peer_ids` is a `&self` read; take the owned count before
        // borrowing `op` mutably for the confirm tally + transition.
        let peer_count = self.live_peer_ids().count();
        let quorum = peer_count.div_ceil(2) + 1;
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
            // `Promoted` state transition. Activating the co-located
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

    /// Build + start the co-located [`crate::primary::PrimaryCoordinator`]
    /// ON DEMAND, the moment this node becomes the primary. The single
    /// activation mechanism BOTH handoff sides converge on: a failover-self
    /// election win (the apply hook on a `PrimaryChanged { reason:
    /// Election, new = self }`) and a bootstrap transfer naming this node
    /// (the setup-FSM Transferred transition). There is NO pre-parked
    /// object and NO gate — the coordinator does not exist until this runs.
    ///
    /// Capability is the explicit replicated marker, read here, never
    /// inferred: the build proceeds ONLY when
    /// `cluster_state.can_be_primary(self)` is true. That marker is the
    /// single source of truth (set by the peer at join, updatable by a
    /// client via `SetCanBePrimary`), so the selection / election guarantee
    /// "this node can host a primary" is checked against global state, not
    /// re-derived from membership.
    ///
    /// On a capable node the activator closure
    /// ([`super::super::PrimaryActivator`], registered by the runtime) is
    /// `take()`-n — making activation fire-once across the paths that name
    /// this node (an own-election self-apply and a peer-echoed re-announce
    /// converge on one build) — and invoked with a SNAPSHOT of this
    /// secondary's continuously-mirrored `cluster_state` (the freshly-built
    /// primary's own ledger is empty, so this snapshot IS the seeded resume
    /// it `restore`s). The returned `JoinHandle` is stored for the runtime
    /// to join at wind-down.
    ///
    /// The build forks on `(can_be_primary(self), activator registered?)`,
    /// LOUD on a genuine contract violation (never a silent no-op — the
    /// failure mode that produced THE HANG is deleted by construction):
    ///   * `(true, Some)` — capable + wired ⇒ BUILD.
    ///   * `(true, None)` — marked capable but the runtime registered no
    ///     activator: a programmer wiring error. Latch `fatal_exit` + log
    ///     loud so the run aborts rather than stranding with no primary.
    ///   * `(false, Some)` — the node was NAMED primary yet its replicated
    ///     marker says it cannot host one (against selection's guarantee —
    ///     e.g. a client cleared the marker after wiring). Latch
    ///     `fatal_exit` + log loud; refuse to build an unmarked authority.
    ///   * `(false, None)` — never marked AND never wired: a Rust-only test
    ///     / legacy single-`run()` caller / `disable_peer_overlay` host that
    ///     was never selected as a hand-off target and runs no on-demand
    ///     authority. A BENIGN no-op — the `PrimaryChanged` broadcast still
    ///     fires uncontested; this is NOT a violation.
    pub(in crate::secondary) fn activate_co_located_primary_on_demand(&mut self) {
        // Idempotency: a second apply of the same naming finds the build
        // already done — a clean no-op, never a double-build.
        if self.activated_primary_handle.is_some() {
            tracing::debug!(
                secondary = %self.config.secondary_id,
                "co-located primary already activated on demand; ignoring \
                 repeat activation"
            );
            return;
        }

        let capable = self.cluster_state.can_be_primary(&self.config.secondary_id);
        match (capable, self.primary_activator.take()) {
            (true, Some(activator)) => {
                // Capable + wired: build. Hand the freshly-built primary a
                // SNAPSHOT of this secondary's continuously-mirrored
                // cluster_state — the seed it `restore`s before
                // `hydrate_from_cluster_state`.
                let snapshot = self.cluster_state.snapshot();
                let handle = activator(snapshot);
                self.activated_primary_handle = Some(handle);
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "built + started co-located primary on demand with \
                     cluster_state snapshot; it will restore + hydrate and \
                     take authority"
                );
            }
            (true, None) => {
                let reason = format!(
                    "node {} is marked can_be_primary but no primary-activator \
                     was registered — the runtime failed to wire on-demand \
                     construction; cannot build the co-located primary",
                    self.config.secondary_id
                );
                tracing::error!(secondary = %self.config.secondary_id, "{reason}");
                self.fatal_exit = Some(reason);
            }
            (false, Some(_)) => {
                // Activator was wired but the marker says incapable (a
                // client cleared it): refuse to build an unmarked authority.
                let reason = format!(
                    "node {} was named primary but cluster_state.can_be_primary \
                     is unset — selection/election must never name a peer whose \
                     capability marker is cleared; refusing to build to avoid \
                     a silent split-brain",
                    self.config.secondary_id
                );
                tracing::error!(secondary = %self.config.secondary_id, "{reason}");
                self.fatal_exit = Some(reason);
            }
            (false, None) => {
                // Never marked, never wired — a legacy / Rust-only / no-mesh
                // path that runs no on-demand authority. Benign: the
                // `PrimaryChanged` broadcast still fires uncontested.
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    "named primary but no on-demand activator wired and \
                     can_be_primary unset (legacy / no-mesh path); not building \
                     a co-located primary"
                );
            }
        }
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
    ///      own apply hook runs, BUILDING the co-located primary on demand
    ///      (`activate_co_located_primary_on_demand`, fire-once via the
    ///      activator `take()`) and resetting this node's election to
    ///      `Normal` (a primary now exists on this host — no lingering
    ///      `Promoted`). The fire-once is idempotent if the broadcast is
    ///      later echoed back through the mesh receive path. No build when
    ///      the node's `can_be_primary` marker is unset (Rust-only tests /
    ///      legacy callers, which never set it): the broadcast still fires
    ///      so the mesh learns the new authority.
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
        // apply hook fires (activate co-located primary + reset election).
        self.apply_cluster_mutations(vec![mutation.clone()]);

        // (2) Broadcast the re-point so surviving peers apply the same
        // frame through the same hook. epoch+1 strictly supersedes the
        // prior primary identity.
        let msg = DistributedMessage::ClusterMutation {
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
