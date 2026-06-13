//! Shared helpers used by the dispatch router AND by sibling secondary
//! subsystems (notably `wait_for_setup`'s receive loop, which applies
//! `ClusterMutation` batches with identical semantics to the
//! operational router; the early-staging path that runs before
//! per-task assignments; and the unresolvable-task fail-loud guard
//! that both `dispatch_message` and `handle_initial_assignment` need).
//!
//! Single concern: provide the apply / stage / fail-loud primitives
//! the router and its setup-time counterpart share, so each rule has
//! exactly one writer.

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::election::ElectionState;
use super::super::wire::timestamp_now;
use crate::cluster_state::ApplyOutcome;
use crate::process::PromotionSignal;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Mirror a batch of `ClusterMutation`s into the local replicated
    /// CRDT AND react to a primary-identity change that names THIS node.
    /// Shared between the operational `dispatch_message` /
    /// `handle_inbound` arms, the peer-mesh relay, and `wait_for_setup`'s
    /// receive loop — every site observes the same wire variant and must
    /// apply with identical semantics. CRDT idempotency makes repeated
    /// apply safe (duplicates and late-after-terminal arrivals NoOp by
    /// precondition).
    ///
    /// For every non-`PrimaryChanged` variant this is a PURE CRDT mirror:
    /// the secondary holds no authority and no dispatch pool, so there is
    /// no pool-growth side effect. The authoritative dispatch-pool
    /// coherence (re-injecting freshly-`Pending` tasks surfaced by a
    /// `TasksSpawned` apply) is the `PrimaryCoordinator`'s concern, driven
    /// on the authority's own pool. A non-authority node simply converges
    /// its CRDT mirror; it never decides what to dispatch from it.
    ///
    /// `PrimaryChanged` is the SINGLE primary-activation frame. Applying
    /// it is also the ONE activation path: this hook runs
    /// [`Self::apply_primary_changed`] per such mutation so a
    /// `PrimaryChanged { new = self }` arriving over ANY receive path
    /// (operational dispatch, peer relay, or setup-time) advances the
    /// CRDT primary identity and — on a self-named promotion — leaves the
    /// Phase-C seam that SIGNALS `Process` to build the primary (it does
    /// NOT build one here), then resets the failover election. It keys on
    /// identity, not on election history — a node that never
    /// suspected/voted still reacts when named.
    ///
    /// Returns `true` iff a `PrimaryChanged` genuinely advanced the
    /// primary identity (an `Applied`, not a stale-epoch NoOp or an
    /// observer rejection). The async operational receive arms react to
    /// that signal with [`Self::react_to_primary_identity_change`] — the
    /// single owner of the per-primary state refresh. The reaction sends
    /// and touches the worker pool, so it is the caller's (async,
    /// operational) concern — this sync hook only reports that the
    /// identity moved.
    pub(in crate::secondary) fn apply_cluster_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) -> bool {
        let count = mutations.len();
        let mut primary_changed = false;
        for m in mutations {
            match m {
                // `reason` (Election vs Transferred) is the Phase-C signal
                // discriminant carried through to `Process`; the build of the
                // primary on a self-named promotion is the Phase-C `Process`
                // concern, not done here (see `apply_primary_changed`). The
                // central CRDT epoch-LWW apply itself stays reason-blind.
                ClusterMutation::PrimaryChanged { new, epoch, reason } => {
                    primary_changed |= self.apply_primary_changed(new, epoch, reason);
                }
                other => {
                    self.cluster_state.apply(other);
                }
            }
        }
        tracing::debug!(
            secondary = %self.config.secondary_id,
            applied = count,
            "mirrored cluster mutations into local CRDT"
        );
        primary_changed
    }

    /// React to a GENUINELY applied primary-identity change (the `true`
    /// return of [`Self::apply_cluster_mutations`]): refresh every piece
    /// of per-primary-pointed state this secondary holds, so the new
    /// primary is treated as the fresh pair it is. ONE owner for the
    /// reaction — both operational receive arms (the primary-link
    /// dispatcher's `ClusterMutation` arm and the peer-mesh relay's)
    /// call this instead of each knowing the pieces:
    ///
    ///   1. **Pairwise mesh re-announce.** The one-shot `MeshReady`
    ///      reporter is re-armed and re-announces to the NEW primary
    ///      ([`Self::rearm_mesh_ready_for_new_primary`]): the primary's
    ///      mesh-confirmation set is node-local and starts EMPTY at
    ///      promotion/relocation, and without the re-send this member is
    ///      structurally unrecoverable into it — the dispatch-readiness
    ///      gate (`member_mesh_confirmed`) then withholds the member
    ///      from every proactive dispatch (the production
    ///      run_20260610_130116 injected-batch pack).
    ///   2. **Worker-pull revive.** Backoff accrued against the PRIOR
    ///      primary is stale the moment the role flips
    ///      (`reset_all_backoff` — keyed off the backoff maps, not the
    ///      pool, so it fires even before `initialize_workers`), and
    ///      every idle worker re-issues its `TaskRequest` immediately
    ///      (`repoll_idle_workers`, `Destination::Primary` re-resolved
    ///      at the egress edge) instead of sitting out a stale window
    ///      (the dispatch-silence symptom).
    ///   3. **Retained-report re-drive.** A confirmable report retained
    ///      during the prior primary's outage re-resolves
    ///      `Destination::Primary` at its egress on every drain, so it
    ///      WOULD route to the new holder — but its `next_due` is a
    ///      backoff slot timed against the gone primary (capped at 60s),
    ///      so a member that already KNOWS the new primary would
    ///      otherwise sit out that slot before re-sending (the production
    ///      `15+30+60+60+60` replay-backoff stall). The identity advance
    ///      is the same "the target just changed, re-deliver NOW" edge as
    ///      `record_primary_message`'s route-recovery drain, so this fires
    ///      the SAME schedule-overriding `drain_report_replays_now` — the
    ///      retained reports land at the new primary within one reaction
    ///      instead of waiting out a stale backoff. A re-absorb (route not
    ///      actually up yet) simply re-buffers on the advanced slot.
    ///
    /// The re-announce is queued BEFORE the repoll so the new primary
    /// hears this member's confirmation ahead of its first
    /// request-driven pulls; the retained-report re-drive runs LAST so the
    /// re-announce + repoll are already queued when the reports go out.
    pub(in crate::secondary) async fn react_to_primary_identity_change(&mut self) {
        self.rearm_mesh_ready_for_new_primary().await;
        self.op_mut().primary_link.reset_all_backoff();
        self.repoll_idle_workers().await;
        self.drain_report_replays_now().await;
    }

    /// The unified primary-activation apply hook for a
    /// `ClusterMutation::PrimaryChanged { new, epoch }` observed on any
    /// receive path. The SINGLE place the secondary reacts to a
    /// primary-identity change:
    ///
    ///   1. **Observer-not-primary guard.** An observer cannot host the
    ///      primary role (no workers, no dispatch authority). If `new`
    ///      names any peer in the replicated `RoleTable.observers`, REJECT
    ///      loud and do NOT install it as `current_primary`. This guard
    ///      protects the single-source-of-truth `current_primary()`
    ///      against a forged or racy announcement naming an observer. (A
    ///      compute SecondaryCoordinator is never itself an observer — the
    ///      observer role IS the ObserverCoordinator — so the self case
    ///      cannot arise.)
    ///   2. **Epoch-LWW apply.** The CRDT `PrimaryChanged` arm is
    ///      last-writer-wins on `(epoch, primary_id)`, so a stale
    ///      lower-epoch announcement NoOps against an already-installed
    ///      higher epoch. Every side effect below is gated on the apply
    ///      actually advancing state (`Applied`), so a no-op announcement
    ///      neither wakes nor resets.
    ///   3. **Self-named → signal + reset.** When `new` is THIS node and
    ///      not an observer, the primary build on the promotion event is
    ///      the Phase-C `Process` concern (the C4 seam — the secondary
    ///      SIGNALS `Process` to construct the `PrimaryCoordinator`; it
    ///      never builds one itself), and this node resets its failover
    ///      election to `Normal` (a primary now exists — no lingering
    ///      Promoted to name).
    ///   4. **Peer-named → reset.** When `new` is a PEER, a primary now
    ///      exists, so any in-flight failover election on this node is
    ///      stale: reset it to `Normal`.
    ///
    /// Returns `true` iff the apply genuinely advanced the primary
    /// identity (`Applied`); `false` on an observer rejection or a
    /// stale-epoch NoOp. The worker-pull revive is the caller's concern
    /// (see [`Self::apply_cluster_mutations`]).
    fn apply_primary_changed(
        &mut self,
        new: String,
        epoch: u64,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason,
    ) -> bool {
        // (1) Observer guard — reject naming an observer before the apply
        // moves `current_primary`.
        let observers = &self.cluster_state.role_table().observers;
        let names_observer = observers.contains(&new);
        if names_observer {
            tracing::error!(
                secondary = %self.config.secondary_id,
                target = %new,
                epoch,
                target_in_role_table_observers = observers.contains(&new),
                "REJECTED PrimaryChanged naming an observer — observers \
                 cannot host the primary role (no workers, no dispatch \
                 authority). Ignoring; the cluster's election should retry \
                 with the observer filtered out."
            );
            return false;
        }

        // Deposition observation, captured BEFORE the apply moves
        // `current_primary`: did THIS node hold the primary role going
        // into this advance? Consumed by `on_primary_identity_advanced`
        // to latch `deposed_primary` (see the field doc in
        // `secondary/mod.rs`).
        let was_primary_before =
            self.cluster_state.current_primary() == Some(self.config.secondary_id.as_str());

        // (2) Epoch-LWW apply. Side effects below only on a genuine
        // identity advance.
        let outcome = self.cluster_state.apply(ClusterMutation::PrimaryChanged {
            new: new.clone(),
            epoch,
            // The central CRDT apply is reason-blind (epoch-LWW only), so
            // the value carried here is immaterial to the resulting state.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::default(),
        });
        if outcome == ApplyOutcome::NoOp {
            tracing::debug!(
                new_primary = %new,
                epoch,
                "ignoring stale PrimaryChanged superseded by higher epoch"
            );
            return false;
        }

        self.on_primary_identity_advanced(&new, epoch, reason, was_primary_before);
        true
    }

    /// The post-advance tail of a GENUINE primary-identity change — the
    /// single seam every advance path runs, however the fact arrived:
    /// the live `PrimaryChanged` apply ([`Self::apply_primary_changed`])
    /// AND the anti-entropy snapshot heal
    /// ([`Self::restore_cluster_snapshot_frame`]). Keying the seam on the
    /// identity advance (not on the wire variant) is what makes the
    /// relocation announcement RECOVERABLE: a secondary that missed the
    /// one-shot broadcast still promotes / follows when the fact reaches
    /// it through a snapshot pull.
    ///
    /// `was_primary_before` is the caller's pre-advance observation of
    /// `current_primary() == self` (both advance paths capture it BEFORE
    /// their apply/restore moves the identity): it drives the
    /// `deposed_primary` latch — set when this node is deposed (it WAS the
    /// primary and the advance names a peer), cleared whenever an advance
    /// names this node again. The latch gates the election's lone-survivor
    /// fast path (see the field doc in `secondary/mod.rs`).
    fn on_primary_identity_advanced(
        &mut self,
        new: &str,
        epoch: u64,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason,
        was_primary_before: bool,
    ) {
        if new == self.config.secondary_id {
            // Named primary again through an applied advance: any earlier
            // deposition is superseded — this node holds the role
            // legitimately (an election win carries peer agreement; a
            // relocation carries the submitter's authority).
            self.deposed_primary = false;
            // (3) This node is the new primary.
            //
            // C4 promotion/transfer signal. The build of the
            // `PrimaryCoordinator` on this promotion event is the `Node`'s
            // concern — the secondary SIGNALS the `Node` (which constructs
            // the snapshot-seeded primary on the event, threading this
            // secondary's `WorkerFactory` to the spawn site) and NEVER
            // builds a primary itself (SUPREME-LAW #3). FIRE the typed
            // `PromotionSignal { reason, epoch, snapshot }`: `reason`
            // (Election vs Transferred) lets the `Node` branch the
            // build/seed path; `epoch` carries the role-table generation the
            // promotion was raised at; `snapshot` is THIS host's converged
            // `cluster_state` captured RIGHT HERE — atomically with the
            // signal, inside the same `&mut self` apply that just advanced
            // the CRDT identity (the `apply` above). Capturing it on the
            // signal (not via a shared-mutable cell the `Node` reads later)
            // keeps the seed coherent with its trigger and owned (`Send`):
            // the `Node` threads it straight to the
            // `PromotedPrimaryBuilder`, which calls
            // `seed_from_promotion_snapshot`. Best-effort — a dropped
            // receiver (or an unwired coordinator: Rust-only unit fixtures)
            // means no `Node` is listening, which the CRDT mutation above
            // has already recorded, so the test still observes the identity
            // advance.
            if let Some(tx) = &self.promotion_tx {
                let snapshot = self.cluster_state.snapshot();
                // Settled-CRDT base captured atomically with the fat
                // snapshot: the slim index + read fds onto this host's
                // spill file. The built primary installs it before
                // restoring `snapshot`, inheriting the join-fixed-point
                // slice without replaying fat bodies (hydrate-from-index).
                let settled_base = self.cluster_state.settled_base_clone();
                if tx
                    .send(PromotionSignal {
                        reason,
                        epoch,
                        snapshot,
                        settled_base,
                    })
                    .is_err()
                {
                    tracing::debug!(
                        secondary = %self.config.secondary_id,
                        epoch,
                        "promotion signal receiver dropped (node winding down); \
                         CRDT primary identity already advanced"
                    );
                }
            } else {
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    epoch,
                    "self-named PrimaryChanged with no promotion signal wired \
                     (unit fixture); CRDT primary identity advanced, no primary built"
                );
            }
            // Reset the election: a primary now exists, so there is no
            // lingering Promoted state.
            self.reset_election_to_normal();
        } else {
            // (4) A peer is the new primary, so any in-flight election on
            // this node is stale: a primary now exists. Reset it.
            if was_primary_before {
                // THIS node just got DEPOSED: it held the primary role and
                // the fleet advanced the identity onto a peer. Latch it —
                // the election's lone-survivor fast path is forbidden to a
                // deposed ex-primary (its mesh view is suspect; the fleet
                // elected around it), so any re-candidacy must gather
                // positive peer agreement.
                self.deposed_primary = true;
                tracing::warn!(
                    secondary = %self.config.secondary_id,
                    new_primary = %new,
                    epoch,
                    "this node was DEPOSED as primary; lone-survivor \
                     self-promotion is disabled until a peer-agreed \
                     re-election (deposed_primary latched)"
                );
            }
            self.reset_election_to_normal();
        }
        tracing::info!(
            new_primary = %new,
            epoch,
            "primary role changed"
        );
        // Re-point the liveness beacon at the new primary's advertised
        // liveness address. The beacon thread reads the published target
        // each tick, so this is the single place a failover redirects it —
        // with zero beacon-side election knowledge.
        self.republish_beacon_target();
    }

    /// Restore one inbound `SnapshotStreamPackage` frame into the local
    /// mirror AND run the primary-identity seam if the heal advanced the
    /// primary fact.
    ///
    /// Shared between the operational dispatch router's package arm and
    /// `wait_for_setup`'s receive loop (the same single-writer
    /// discipline as [`Self::apply_cluster_mutations`]): both sites must
    /// restore with identical semantics, and BOTH must observe a healed
    /// primary-identity advance — pre-fix the restore was a silent lattice
    /// merge, so a snapshot that newly named THIS node primary (the missed
    /// relocation announcement healing through anti-entropy) never fired
    /// the `PromotionSignal` and a peer-named heal never reset the
    /// election. The decode failure stays WARN-and-keep (the steady-state
    /// discriminator: the next digest round re-pulls) and does NOT advance
    /// the resume cursor, so the re-pull resumes from before the bad span.
    ///
    /// Each package is a PARTIAL snapshot; `restore` is the idempotent
    /// lattice merge, so packages interleave safely with live broadcasts
    /// and the primary-identity fact (it rides the stream's HEAD package)
    /// heals on the first package, before the bulk lands.
    ///
    /// The healed `reason` is [`PrimaryChangeReason::default`]: a snapshot
    /// carries no origination reason (the CRDT is reason-blind), and the
    /// `Node`'s promotion build threads only the snapshot — the reason is
    /// advisory routing metadata.
    ///
    /// Returns `true` iff the restore genuinely advanced the primary
    /// identity (`(current_primary, primary_epoch)` moved); operational
    /// callers react with [`Self::react_to_primary_identity_change`]
    /// (async, pool-touching — the caller's concern, exactly as for
    /// [`Self::apply_cluster_mutations`]).
    pub(in crate::secondary) fn restore_snapshot_stream_frame(
        &mut self,
        sender_id: &str,
        stream_id: &str,
        cursor: Option<&str>,
        payload: &str,
        done: bool,
    ) -> bool {
        let snap = match crate::cluster_state::decode_stream_payload::<I>(payload) {
            Ok(snap) => snap,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    stream_id = %stream_id,
                    "SnapshotStreamPackage decode failed in the steady-state \
                     anti-entropy sink; dropped a malformed package (the \
                     next peer StateDigest broadcast will re-trigger \
                     reconciliation via the reactive digest arm)"
                );
                return false;
            }
        };
        self.inbound_snapshots
            .note_package(sender_id, stream_id, cursor, done);
        let before = (
            self.cluster_state.current_primary().map(str::to_owned),
            self.cluster_state.primary_epoch(),
        );
        // Same pre-advance deposition observation the live apply captures.
        let was_primary_before =
            before.0.as_deref() == Some(self.config.secondary_id.as_str());
        self.cluster_state.restore(snap);
        let after_primary = self.cluster_state.current_primary().map(str::to_owned);
        let after = (after_primary.clone(), self.cluster_state.primary_epoch());
        if after != before
            && let Some(new) = after_primary
        {
            let epoch = self.cluster_state.primary_epoch();
            self.on_primary_identity_advanced(
                &new,
                epoch,
                dynrunner_protocol_primary_secondary::PrimaryChangeReason::default(),
                was_primary_before,
            );
            return true;
        }
        false
    }

    /// Anti-entropy receive side: compare `digest` (a peer's broadcast)
    /// against the local replica and pull a snapshot from the proven-ahead
    /// SENDER iff this replica is behind. The decision (compare + target
    /// selection + request construction) lives in `crate::anti_entropy` so
    /// primary / secondary / observer share ONE policy; this helper owns
    /// only the `send_to` edge. Shared between the operational dispatch
    /// router's `StateDigest` arm and `wait_for_setup`'s receive loop —
    /// pre-`Operational` participation is what lets a setup-wedged
    /// secondary recover a missed relocation announcement (the pull's
    /// packages heal via [`Self::restore_snapshot_stream_frame`]).
    pub(in crate::secondary) async fn reconcile_state_digest(
        &mut self,
        sender_id: &str,
        sender_is_observer: bool,
        digest: &dynrunner_protocol_primary_secondary::StateDigest,
    ) {
        let local = self.cluster_state.digest();
        let requester = crate::anti_entropy::RequesterIdentity {
            node_id: &self.config.secondary_id,
            // Wire role advertisement: a compute SecondaryCoordinator
            // is never an observer (the observer role IS the
            // standalone ObserverCoordinator), so the anti-entropy
            // requester always declares `false`.
            is_observer: false,
            can_be_primary: self.config.can_be_primary,
        };
        if let Some((destination, request)) = crate::anti_entropy::reconcile_against_peer(
            &local,
            digest,
            sender_id,
            sender_is_observer,
            &requester,
            &mut self.inbound_snapshots,
            timestamp_now(),
        ) {
            if let Err(e) = self.send_to(destination, request).await {
                tracing::debug!(
                    error = %e,
                    peer = %sender_id,
                    "anti-entropy: snapshot pull request send failed; \
                     a later digest round retries"
                );
            } else {
                tracing::debug!(
                    peer = %sender_id,
                    "anti-entropy: local replica behind peer digest; \
                     requested snapshot pull"
                );
            }
        }
    }

    /// Test hook: synchronously drain every pending snapshot-stream wake
    /// token, emitting + sending each package — what the process loop's
    /// stream arm does one-per-wakeup, compressed for loop-less
    /// responder tests.
    #[cfg(test)]
    pub(crate) async fn drive_snapshot_streams_for_test(&mut self) {
        while let Some(stream_id) = self.snapshot_streams.try_next_wake() {
            if let Some((dst, frame)) =
                self.snapshot_streams
                    .emit_next(&stream_id, &self.cluster_state, timestamp_now())
            {
                let _ = self.send_to(dst, frame).await;
            }
        }
    }

    /// Publish the CURRENT primary's liveness `SocketAddr` into the
    /// beacon-target cell the dedicated beacon thread reads. Resolves
    /// `cluster_state.current_primary()` against `peer_liveness_addrs`
    /// (populated from `PeerInfo`); `None` (no primary yet, or its
    /// liveness address not yet learned) makes the beacon a no-op until a
    /// later `PeerInfo`/`PrimaryChanged` resolves it. Called on every
    /// primary-identity advance AND whenever the peer-address view is
    /// rebuilt, so the target stays current across both axes.
    pub(in crate::secondary) fn republish_beacon_target(&mut self) {
        let addr = self
            .cluster_state
            .current_primary()
            .and_then(|primary_id| self.peer_liveness_addrs.get(primary_id));
        self.beacon_target.publish_one(addr);
    }

    /// Rebuild the id→liveness-`SocketAddr` view from a `PeerInfo` roster
    /// and re-point the beacon. For each peer that advertised a
    /// `liveness_port` AND a parseable `ipv4`, record `(ipv4:port)`; a
    /// peer missing either is simply absent from the view (the beacon
    /// no-ops if it becomes primary without an advertised address —
    /// strictly better than beaconing a bogus address, and the union
    /// death-clock still carries it via mesh frames). IPv4 is the beacon
    /// transport (the QUIC mesh's primary LAN family); ipv6-only peers are
    /// not beaconed in this pass.
    pub(in crate::secondary) fn ingest_peer_liveness_addrs(
        &mut self,
        peers: &[dynrunner_protocol_primary_secondary::PeerConnectionInfo],
    ) {
        // The address-book owns the `PeerInfo` → `ipv4:port` parse + filter
        // (a peer missing either field is absent — strictly better than a
        // bogus address). Writing the SHARED cell makes the same book
        // readable by the co-located promoted primary's beacon-target
        // builder, not just this secondary's `republish_beacon_target`.
        self.peer_liveness_addrs.ingest(peers);
        self.republish_beacon_target();
    }

    /// A clone of the node-scoped peer→liveness-address book. The run
    /// boundary hands this to the promoted-primary recipe so the primary's
    /// beacon-target builder can resolve its secondaries' beacon addresses
    /// (the promoted primary observes no `PeerInfo` of its own). Mirrors
    /// `beacon_target()` / `set_beacon_liveness` as a shared-cell accessor.
    pub fn peer_liveness_addrs(&self) -> crate::liveness::PeerLivenessAddrs {
        self.peer_liveness_addrs.clone()
    }

    /// Reset the failover election to `Normal` on a genuine primary-identity
    /// advance.
    ///
    /// OPERATIONAL: revert `OperationalState.election` to `Normal` — a primary
    /// now exists, so any in-flight election is stale.
    ///
    /// SETUP-PHASE (#420 face (c)): a `wait_for_setup` secondary that ARMED a
    /// setup-phase election (its `setup_election` holder is `Some`) is the
    /// LOSER of that election when this advance names a PEER — DROP the holder
    /// entirely. This is the loser contract: the elected primary's re-sent
    /// setup trio (its `PromotedDestination` pre-loop chain) completes this
    /// node's handshake and spawns its workers while it stays in
    /// `wait_for_setup`; the transient election state is discarded so its
    /// silence clock re-arms fresh against the new primary and no stale
    /// candidacy lingers. (A self-named advance — this node WON — never reaches
    /// the loser drop: `fire_local_promotion` already left the holder
    /// `Promoted` and the winner exits setup via the `PromotionSignal`, so the
    /// drop here is harmless either way.) A pre-`Operational` secondary that
    /// never armed an election holds neither — both branches no-op.
    fn reset_election_to_normal(&mut self) {
        if let Some(op) = self.lifecycle.operational_mut() {
            op.election = ElectionState::Normal;
        } else {
            self.setup_election = None;
        }
    }

    /// Run a `stage_file` copy + register the result in
    /// `extraction_cache`. Shared between the standalone
    /// `DistributedMessage::StageFile` arm in `dispatch_message`
    /// (post-setup re-staging) and the inline `staged_files` records
    /// of `InitialAssignment` (processed by `handle_initial_assignment`
    /// before any per-task assignment runs). Failures are logged and
    /// swallowed — the next TaskAssignment for the same hash will
    /// surface as a TaskFailed via `report_unresolvable_task` rather
    /// than wedging the staging path itself.
    ///
    /// `file_hash` is the cache lookup key (must match the
    /// `TaskAssignment.file_hash` the secondary will see later);
    /// `content_hash` is what `stage_file` verifies against after
    /// the copy. The two were previously a single `file_hash`
    /// field — the conflation always made verification mismatch
    /// (16-char identifier hex vs 64-char content SHA256 hex).
    pub(in crate::secondary) fn stage_and_register(
        &mut self,
        file_hash: &str,
        content_hash: &str,
        src_path: &str,
        dest_path: &str,
    ) {
        let src_tmp = self.extraction_cache.tmp_dir().to_path_buf();
        match super::super::staging::stage_file(
            self.config.src_network.as_deref(),
            &src_tmp,
            src_path,
            dest_path,
            content_hash,
        ) {
            Ok(outcome) => {
                self.extraction_cache.register_path(file_hash, outcome.dest);
                tracing::info!(
                    file_hash = %file_hash,
                    "staged file registered"
                );
            }
            Err(e) => {
                tracing::error!(
                    file_hash = %file_hash,
                    error = %e,
                    "stage_file failed; the next TaskAssignment for this hash will be reported as TaskFailed"
                );
            }
        }
    }

    /// Fail-loud guard for "the worker has no plausible way to open
    /// this binary". Both `dispatch_message` (operational
    /// TaskAssignment) and `handle_initial_assignment`
    /// (InitialAssignment in the setup phase) need the same check —
    /// without it, a missed-resolution silently passes the primary's
    /// filesystem-view path through to the worker, which crashes at
    /// exec time and the primary re-enqueues as Recoverable.
    ///
    /// Returns `Ok(true)` when the task is unresolvable: a
    /// `TaskFailed` NonRecoverable was sent to the primary and the
    /// caller MUST skip the worker assignment. Returns `Ok(false)`
    /// when resolution either succeeded or the path can plausibly
    /// resolve at the worker (in-process distributed mode where
    /// primary and secondary share a filesystem view); the caller
    /// should proceed with the assignment.
    ///
    /// Two ways the worker can succeed without `resolved_path`:
    ///   - the secondary has a staging directory (`src_network`
    ///     set) AND the file landed there — covered by
    ///     `resolved_path.is_some()`.
    ///   - the secondary shares a filesystem view with the primary
    ///     AND `local_path` is the primary's absolute path
    ///     (in-process distributed mode); for that to be plausible
    ///     `local_path` must at minimum be absolute.
    pub(in crate::secondary) async fn report_unresolvable_task(
        &mut self,
        worker_id: u32,
        file_hash: &str,
        local_path: &str,
        resolved_path: &Option<std::path::PathBuf>,
    ) -> Result<bool, String> {
        let local_path_is_relative = std::path::Path::new(local_path).is_relative();
        if resolved_path.is_none() && (self.config.src_network.is_some() || local_path_is_relative)
        {
            // Report against the ORIGINAL wire `worker_id`: this value is
            // only echoed back to the primary in the `TaskFailed` frame, it
            // never indexes the pool here. The prior
            // `worker_id.min(pool.workers.len() - 1)` clamp touched the pool
            // purely to "correct" the reported id — which (a) underflowed on
            // a 0-worker `Operational`/`Configuring` node (`0u32 - 1`) and
            // (b) silently retargeted an out-of-range id onto the last slot.
            // The wire id is the faithful thing to report (the router's
            // backpressure path reports the same un-clamped wire id), so
            // drop the clamp and the pool touch entirely.
            let msg = DistributedMessage::TaskFailed {
                target: None,
                sender_id: self.config.secondary_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: self.config.secondary_id.clone(),
                worker_id,
                task_hash: file_hash.into(),
                error_type: ErrorType::NonRecoverable,
                error_message: format!(
                    "file_hash {file_hash} not pre-staged at {local_path}; \
                     expected StageFile notification first"
                ),
                // Stamped at the send_to_primary chokepoint (#352).
                delivery_seq: None,
                // Stamped at the send_to_primary chokepoint (ordering gate).
                msgs_posted_through: None,
            };
            self.send_to_primary(msg).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Store an inbound run-config PUSH from the primary.
    ///
    /// The primary unicasts a `RunConfig` the moment it welcomes this
    /// secondary (`PrimaryCoordinator::push_run_config_to`), carrying the
    /// consumer's `forwarded_argv` the boot CLI omits. Shared by the
    /// operational dispatch router AND `wait_for_setup`'s receive loop —
    /// the push can land in EITHER window (it fires right after welcome, so
    /// it usually arrives mid-setup), and both sites must store it with
    /// identical semantics, so the write lives here with exactly one
    /// writer.
    ///
    /// Pure node-local launch constant, NOT lattice data: it overwrites
    /// `self.forwarded_argv` (last-writer-wins; a duplicate push or a later
    /// `RequestRunConfig` answer carries the same value) and never touches
    /// `cluster_state`. The stored copy is what the run path reads and what
    /// THIS node re-serves on a peer's `RequestRunConfig` / threads into a
    /// promoted `PrimaryConfig`.
    pub(in crate::secondary) fn store_pushed_run_config(&mut self, forwarded_argv: Vec<String>) {
        let argv_len = forwarded_argv.len();
        // SINGLE writer to the shared handle. Last-writer-wins: a duplicate
        // push or a later `RequestRunConfig` answer carries the same value.
        *self
            .forwarded_argv
            .lock()
            .expect("forwarded_argv mutex poisoned") = forwarded_argv;
        // Latch the delivery so the finalize backstop can tell "the push
        // landed (possibly empty)" from "no run-config has arrived yet" —
        // an empty argv is a valid landing (compiler_suit-shape), so the
        // latch, not emptiness, is the discriminator.
        self.forwarded_argv_was_pushed = true;
        tracing::debug!(
            secondary = %self.config.secondary_id,
            argv_len,
            "received pushed run-config from primary"
        );
    }
}
