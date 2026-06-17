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

use dynrunner_core::{ErrorType, Identifier, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{AssignedTaskRef, ClusterMutation, DistributedMessage};
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
        // Pre-scan the batch for own-originated `CustomMessagePosted`
        // entries: the appearance of `CustomMessagePosted { origin =
        // self.id, seq }` in a ClusterMutation broadcast received by
        // this node is the durability proof an IMPORTANT-custom
        // retention entry waits for under the #541 drop trigger
        // (`AwaitingCrdtConvergence`). The primary's `Destination::All`
        // fan is origin-EXCLUDED on the PRIMARY role — secondaries are
        // included, so an origin secondary ALWAYS receives its own
        // CustomMessagePosted back if the mesh-pump actually fanned out
        // (the post-#539 hard-crash window is exactly the case where
        // the local apply ran but the fan-out did not, so this
        // observation never happens there and the retention stays
        // live). The scan reads `self.config.secondary_id` to compare
        // origin, then collects the seqs into a single sweep through
        // `pending_report_replays` — the retention bookkeeping is
        // local to the secondary; the apply loop below is the
        // standard CRDT mirror, unchanged.
        //
        // Idempotent under duplicate broadcasts (the same
        // CustomMessagePosted arriving twice): the second sweep finds
        // no matching seq because the first already dropped it.
        let own_id = self.config.secondary_id.as_str();
        let own_posted_seqs: Vec<u64> = mutations
            .iter()
            .filter_map(|m| match m {
                ClusterMutation::CustomMessagePosted { origin, seq, .. }
                    if origin == own_id =>
                {
                    Some(*seq)
                }
                _ => None,
            })
            .collect();
        if !own_posted_seqs.is_empty() {
            self.drop_retentions_on_own_custom_observation(&own_posted_seqs);
        }
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
        // Open the failover slot-reconfirmation window: the new primary
        // holds stale `InFlight` guesses for inherited slots, so the
        // periodic keepalive re-poll (`repoll_idle_workers_periodic`) must
        // run until every idle worker re-confirms its slot. The IMMEDIATE
        // repoll just below is the first reconfirming wave; the window keeps
        // the periodic driver alive for any worker that misses it (e.g. a
        // worker that frees AFTER this reaction).
        self.op_mut().primary_link.arm_failover_reconfirm();
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
        // `secondary/mod.rs`). The same pre-advance read also names the
        // FORMER primary (the node relocating away on a `Transferred`
        // advance), threaded so the built primary can hold delivery for it.
        let former_primary = self.cluster_state.current_primary().map(str::to_owned);
        let was_primary_before =
            former_primary.as_deref() == Some(self.config.secondary_id.as_str());

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

        self.on_primary_identity_advanced(
            &new,
            epoch,
            reason,
            was_primary_before,
            former_primary.as_deref(),
        );
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
    ///
    /// `former_primary` is the caller's pre-advance `current_primary()`
    /// (captured at the SAME point as `was_primary_before`, before the
    /// apply/restore moved the identity). When this advance is a graceful
    /// `Transferred` relocation naming THIS node, that former primary is
    /// the node relocating away to become a standalone observer — carried
    /// onto the `PromotionSignal`'s `relocating_from` so the built primary
    /// can hold its terminal-verdict delivery for that still-arriving
    /// observer (see [`super::super::super::node::PromotionSignal`]).
    fn on_primary_identity_advanced(
        &mut self,
        new: &str,
        epoch: u64,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason,
        was_primary_before: bool,
        former_primary: Option<&str>,
    ) {
        // Stamp the post-failover SETTLE-WINDOW anchor on EVERY genuinely-
        // applied advance (self- or peer-named): this is the moment a new
        // epoch's `MeshReady` reconfirmation begins, and `run_election_tick`
        // suppresses election RE-arming for a short window after it so the
        // reconfirmation can complete before another election can fire (the
        // amplifier that turns one transient failover into a self-sustaining
        // epoch cascade — see the field doc in `secondary/mod.rs`). Reached
        // only on an `Applied` advance (the caller gates on it), so a
        // stale-epoch NoOp never refreshes the window.
        self.last_primary_change_at = Some(std::time::Instant::now());
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
                // GRACEFUL relocation only: the former primary is handing
                // its role to this node and becomes a standalone observer,
                // so carry it as the built primary's pending observer. A
                // `Election` failover names no `relocating_from` — the
                // former primary CRASHED, so the built primary must not
                // wait for it (it will never announce as an observer).
                let relocating_from = match reason {
                    dynrunner_protocol_primary_secondary::PrimaryChangeReason::Transferred => {
                        former_primary.map(str::to_owned)
                    }
                    dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election => None,
                };
                if tx
                    .send(PromotionSignal {
                        reason,
                        epoch,
                        snapshot,
                        settled_base,
                        relocating_from,
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
                before.0.as_deref(),
            );
            return true;
        }
        false
    }

    /// Anti-entropy receive side: compare `digest` (a peer's broadcast)
    /// against the local replica and, iff this replica is behind, NOTE the
    /// divergence to the disciplined PULL driver (`pull_coordinator`)
    /// instead of firing an eager immediate snapshot pull at the sender.
    /// `note_behind` is IDEMPOTENT — a divergence noticed while a probe→pull
    /// cycle is already in flight is a NoOp — so a perpetually-behind
    /// replica under churn initiates pulls at the cooldown rate, NOT one per
    /// inbound digest (the #491 snapshot-package storm the eager
    /// `reconcile_against_peer` caused). The cold (Idle) note SCHEDULES a
    /// staggered probe (per-node jitter, #504) and returns `None`; the probe
    /// itself fires from the pull arm's `tick` at the staggered deadline, and
    /// selection + the actual pull happen on the pull arm's timers. The digest
    /// beacon + `is_behind` DETECTION are unchanged; only the eager per-digest
    /// pull is replaced.
    ///
    /// Shared between the operational dispatch router's `StateDigest` arm
    /// and `wait_for_setup`'s receive loop — pre-`Operational` participation
    /// still triggers the heal; the pull arm runs in the operational loop,
    /// and a pre-operational note simply primes the FSM (the staggered probe
    /// is emitted by the pull arm's `tick` once the loop runs).
    pub(in crate::secondary) async fn reconcile_state_digest(
        &mut self,
        sender_id: &str,
        sender_is_observer: bool,
        digest: &dynrunner_protocol_primary_secondary::StateDigest,
    ) {
        let _ = (sender_id, sender_is_observer);
        let local = self.cluster_state.digest();
        if !local.is_behind(digest) {
            // Converged on every field the peer reports — nothing to pull
            // (the self-quiescing steady state).
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .note_behind(std::time::Instant::now())
        {
            // `note_behind` SCHEDULES a staggered probe and returns `None`
            // (the probe fires from the pull arm's `tick` at the per-node
            // jitter deadline, #504); this drive stays as the directive sink
            // in case the FSM ever returns one synchronously again.
            self.drive_pull_directive(directive).await;
        }
    }

    /// Translate ONE [`crate::pull_coordinator::PullDirective`] into this
    /// secondary's `send_to` edge — the role-owned wire-touch half of the
    /// disciplined pull (the FSM + selection live in `pull_coordinator`;
    /// the frame construction + role-typing in
    /// `pull_coordinator::pull_probe` / `pull_request`; this method only
    /// owns the `send_to`). A `Probe` broadcasts the local digest to direct
    /// neighbours; a `PullFrom` issues the resume-from-cursor
    /// `RequestSnapshotStream` to the chosen target and hands the minted
    /// stream id back to the coordinator so a later `PullFail` matches it.
    pub(in crate::secondary) async fn drive_pull_directive(
        &mut self,
        directive: crate::pull_coordinator::PullDirective,
    ) {
        match directive {
            crate::pull_coordinator::PullDirective::Probe => {
                let digest = self.cluster_state.digest();
                let frame = crate::pull_coordinator::pull_probe(
                    &self.config.secondary_id,
                    timestamp_now(),
                    digest,
                );
                let _ = self
                    .send_to(
                        dynrunner_protocol_primary_secondary::Destination::All,
                        frame,
                    )
                    .await;
            }
            crate::pull_coordinator::PullDirective::PullFrom {
                target_id,
                target_is_observer,
                target_range_digest,
            } => {
                // P1: compute the divergent range-set against the chosen
                // responder's piggybacked range digest, so the request
                // streams only the divergent buckets. The fold is the
                // cluster_state's; the compare is the pull-model vocabulary.
                let task_ranges = crate::pull_coordinator::divergent_ranges_for_pull(
                    &self.cluster_state.tasks_range_digest(),
                    &target_range_digest,
                );
                let (dst, frame, stream_id) = crate::pull_coordinator::pull_request(
                    &self.config.secondary_id,
                    // A compute secondary is never an observer; its
                    // primary-capability rides the request.
                    false,
                    self.config.can_be_primary,
                    &target_id,
                    target_is_observer,
                    task_ranges,
                    &mut self.inbound_snapshots,
                    timestamp_now(),
                );
                if self.send_to(dst, frame).await.is_ok() {
                    // Bind the in-flight pull's stream id so a `PullFail`
                    // for exactly this attempt advances the target.
                    self.pull_coordinator.note_pull_stream(&stream_id);
                }
            }
        }
    }

    /// Answer an inbound `PullProbe`: reply with this node's current inbox
    /// depth + the responder-side `ahead` bit (does this replica hold ledger
    /// data the prober lacks, computed from the digest the probe carried).
    /// Direct-only reply (the prober's 30s re-probe recovers a lost one).
    pub(in crate::secondary) async fn handle_pull_probe(
        &mut self,
        prober_id: &str,
        prober_digest: &dynrunner_protocol_primary_secondary::StateDigest,
    ) {
        let local = self.cluster_state.digest();
        let ahead = crate::pull_coordinator::probe_reply_ahead(&local, prober_digest);
        // P1: piggyback this responder's task-ledger range digest so the
        // prober can compute the divergent buckets without a second
        // round-trip (folded once per inbound probe — single-flight +
        // cooldown-bounded, so far below the killed per-digest storm).
        let range_digest = self.cluster_state.tasks_range_digest();
        // The prober declared its own role on the probe? The probe carries
        // no role bit (a compute secondary's probe), so reply typed
        // `Secondary(prober)` — the prober's id==self ingress fan absorbs a
        // mis-type harmlessly, and a compute prober is the common case. An
        // observer prober's reply mis-type is covered the same way the eager
        // path's was (the receiver-side id==self fan).
        let (dst, frame) = crate::pull_coordinator::pull_probe_reply(
            &self.config.secondary_id,
            timestamp_now(),
            prober_id,
            false,
            self.inbox.depth() as u64,
            ahead,
            range_digest,
        );
        let _ = self.send_to(dst, frame).await;
    }

    /// Record an inbound `PullProbeReply` into the pull driver. A reply
    /// addressed to a different requester (not us) is ignored; a usable
    /// (ahead) reply may resolve the pull target (the first-answer fallback
    /// once the window has elapsed), in which case the returned directive is
    /// driven onto the wire.
    pub(in crate::secondary) async fn handle_pull_probe_reply(
        &mut self,
        responder_id: &str,
        requester: &str,
        inbox_size: u64,
        ahead: bool,
        range_digest: Box<dynrunner_protocol_primary_secondary::RangeDigest>,
    ) {
        if requester != self.config.secondary_id {
            return;
        }
        let reply = crate::pull_coordinator::ProbeReply {
            responder_id,
            // Role hint for the eventual pull's typing: the reply frame
            // carries no responder-role bit, so default `Secondary`; an
            // observer responder's pull is covered by the receiver id==self
            // fan, as in the eager path.
            responder_is_observer: false,
            inbox_size,
            ahead,
            // P1: the responder's piggybacked range digest, retained on the
            // candidate so the pull to it streams only the divergent buckets.
            range_digest,
        };
        if let Some(directive) = self
            .pull_coordinator
            .on_probe_reply(std::time::Instant::now(), &reply)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Record an inbound `PullFail` (a chosen target's direct leg to us
    /// dropped) into the pull driver and drive the fall-to-next-target
    /// directive if one is produced. A fail for a requester that is not us,
    /// or for a stream that is not in flight, is ignored by the driver.
    pub(in crate::secondary) async fn handle_pull_fail(
        &mut self,
        requester: &str,
        stream_id: &str,
    ) {
        if requester != self.config.secondary_id {
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .on_fail(std::time::Instant::now(), stream_id)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Test-only: drive a complete pull-PROBE answer so an integration test
    /// can advance the disciplined pull from "probe emitted" to "snapshot
    /// pull issued" deterministically, without a real 1-second wait. It
    /// feeds a single AHEAD `PullProbeReply` from `donor` at a synthetic
    /// `now` PAST the selection window — exactly the production first-answer
    /// fallback (a reply that lands after an empty window commits on
    /// arrival) — and drives the resulting `RequestSnapshotStream`. The role
    /// edge is identical to the live `handle_pull_probe_reply` path; only
    /// the clock is synthetic. Call AFTER a digest dispatch has emitted the
    /// probe (the FSM must be Probing).
    #[cfg(test)]
    pub(in crate::secondary) async fn complete_pull_probe_for_test(
        &mut self,
        donor_id: &str,
        donor_is_observer: bool,
    ) {
        let reply = crate::pull_coordinator::ProbeReply {
            responder_id: donor_id,
            responder_is_observer: donor_is_observer,
            inbox_size: 0,
            ahead: true,
            // The probe→pull mechanics under test do not depend on the
            // delta content; the default (all-zero) range digest yields the
            // all-ranges full pull, which is what this helper exercises.
            range_digest: Box::new(dynrunner_protocol_primary_secondary::RangeDigest::default()),
        };
        // A synthetic `now` strictly past the selection window guarantees the
        // first-answer fallback commits on this reply. Derive it from the
        // coordinator's OWN Probing wake deadline (`since + SELECTION_WINDOW`)
        // rather than `Instant::now()`, so it is robust to a probe whose
        // `since` was stamped at a future-relative staggered `fire_at` (#504):
        // `wake_deadline + 1ms` is always strictly past the window for the
        // probe in flight. Falls back to a now-relative window if the FSM
        // somehow armed no deadline (defensive — the caller guarantees Probing).
        let past_window = self
            .pull_coordinator
            .wake_deadline()
            .map(|d| d + std::time::Duration::from_millis(1))
            .unwrap_or_else(|| {
                std::time::Instant::now()
                    + crate::pull_coordinator::SELECTION_WINDOW
                    + std::time::Duration::from_millis(1)
            });
        if let Some(directive) = self.pull_coordinator.on_probe_reply(past_window, &reply) {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Test-only: fire the STAGGERED first probe (#504). `note_behind` defers
    /// the probe to the pull arm's `tick` at a per-node jitter deadline; a
    /// loop-less unit test has no pull arm, so this drives the coordinator's
    /// `tick` at its `wake_deadline` (the deferred probe's `fire_at`) — exactly
    /// what the operational loop's pull arm does — emitting the `Probe` and
    /// entering `Probing`, then sends it via the same `drive_pull_directive`
    /// edge. Call AFTER a digest dispatch has noted the divergence (the FSM is
    /// `ProbePending`); a NoOp if no probe is pending.
    #[cfg(test)]
    pub(in crate::secondary) async fn fire_staggered_probe_for_test(&mut self) {
        let Some(fire_at) = self.pull_coordinator.wake_deadline() else {
            return;
        };
        for directive in self.pull_coordinator.tick(fire_at) {
            self.drive_pull_directive(directive).await;
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
    /// Returns `Ok(true)` when the task is unresolvable: a `TaskFailed`
    /// was sent to the primary and the caller MUST skip the worker
    /// assignment. Returns `Ok(false)` when resolution either succeeded
    /// or the path can plausibly resolve at the worker (in-process
    /// distributed mode where primary and secondary share a filesystem
    /// view); the caller should proceed with the assignment.
    ///
    /// Two ways the worker can succeed without `resolved_path`:
    ///   - the secondary has a staging directory (`src_network`
    ///     set) AND the file landed there — covered by
    ///     `resolved_path.is_some()`.
    ///   - the secondary shares a filesystem view with the primary
    ///     AND `local_path` is the primary's absolute path
    ///     (in-process distributed mode); for that to be plausible
    ///     `local_path` must at minimum be absolute.
    ///
    /// The failure is classified by WHY it is unresolvable — the same
    /// `src_network` predicate that gates the report:
    ///   - `src_network.is_some()` — staging IS configured but THIS
    ///     node never staged the file (a per-node bind-mount lacks a
    ///     file a sibling holds; a respawn/partial-stage topology; a
    ///     StageFile-vs-assignment race). The file is REINJECTABLE
    ///     ELSEWHERE, so the failure is `Recoverable`: the primary
    ///     re-injects it into the pool (where a staged peer can pick it
    ///     up) and the per-phase `retry_max_passes` budget BOUNDS it —
    ///     a task placeable nowhere fails-final after the budget, never
    ///     silently lost and never looped. A `NonRecoverable` here was
    ///     the #495 silent-loss bug: the primary does not re-route a
    ///     NonRecoverable, so a not-staged-HERE task was lost cluster-
    ///     wide (274/660 lost, run falsely "complete").
    ///   - `src_network` unset + a RELATIVE `local_path` — the worker
    ///     can never open it AND no peer is configured differently
    ///     (the wire path is identical for every node in non-staging
    ///     mode), so rerouting would only bounce it to identically-
    ///     unconfigured peers. This is a genuine misconfiguration and
    ///     stays `NonRecoverable` (the historical fail-loud behaviour).
    pub(in crate::secondary) async fn report_unresolvable_task(
        &mut self,
        worker_id: u32,
        file_hash: &str,
        local_path: &str,
        resolved_path: &Option<std::path::PathBuf>,
    ) -> Result<bool, String> {
        let local_path_is_relative = std::path::Path::new(local_path).is_relative();
        let staging_configured = self.config.src_network.is_some();
        if resolved_path.is_none() && (staging_configured || local_path_is_relative) {
            // Staging configured ⇒ reinjectable to a staged peer
            // (Recoverable); otherwise a relative path the worker can
            // never open and no peer resolves differently (NonRecoverable).
            let error_type = if staging_configured {
                ErrorType::Recoverable
            } else {
                ErrorType::NonRecoverable
            };
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
                error_type,
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

    /// HONOR the primary's assigned `worker_id` or BOUNCE the assignment
    /// (#517). THE single dispatch-target selection seam, shared by the
    /// operational router (`assign_resolved_task`) and the setup-time
    /// initial-assignment loop (`handle_initial_assignment`) so neither can
    /// diverge into the pre-#517 any-idle re-pick.
    ///
    /// The secondary holds NO scheduling authority (the dispatch-decoupling
    /// law): it must run the task on the EXACT worker the primary chose, or
    /// not at all. So:
    ///   - the requested slot exists and is idle ⇒ `Ok(Some(worker_id))`:
    ///     the caller dispatches onto it;
    ///   - the requested slot is NOT idle (busy with an incumbent, in a
    ///     respawn transition) OR out of range OR the pool is empty
    ///     (0-worker observer / late-joiner) ⇒ `Ok(None)`: the caller does
    ///     NOT dispatch. We bounce a typed
    ///     [`DistributedMessage::IllegallyAssignedToNonidleWorker`] to the
    ///     primary so it reconciles its diverged `(secondary, worker_id)`
    ///     occupancy and REQUEUES the task. This is NOT a `TaskFailed`: the
    ///     task is never accounted as a failure (no retry-budget burn).
    ///
    /// SAFETY (preserves cabd34ab): `pool.workers.get(worker_id)` is the
    /// Option path — an out-of-range id or a 0-worker pool resolves to
    /// `None` and bounces, never a `len() - 1` underflow and never an
    /// unconditional index. There is NO `.or_else(any-idle)` re-pick.
    ///
    /// `incumbent` is filled from the requested slot's `current_binary`
    /// (the task it is actually running) when present — the busy case —
    /// and is `None` for the degenerate non-idle cases that carry no
    /// running task (out-of-range / 0-worker / mid-respawn transition).
    pub(in crate::secondary) async fn select_honored_target_or_bounce(
        &mut self,
        worker_id: WorkerId,
        assigned: AssignedTaskRef<I>,
    ) -> Result<Option<WorkerId>, String> {
        // Honor: dispatch ONLY onto the exact requested slot, and only if
        // it is idle. `.get()` keeps an out-of-range id / 0-worker pool a
        // clean `None` (no index, no clamp) — the cabd34ab safety. Reach the
        // pool via `pool_mut()` (NOT `op_mut()`): this seam runs from BOTH
        // the operational router AND the setup-time initial-assignment loop
        // (lifecycle `Configuring`), and only `pool_mut()` carries the pool
        // in both states.
        let slot = self.pool_mut().workers.get(worker_id as usize);
        if slot.is_some_and(|w| w.is_idle_state()) {
            return Ok(Some(worker_id));
        }

        // Not idle: derive the incumbent (the task the slot is actually
        // running) from the worker's own `current_binary`, if any. Absent
        // for out-of-range / 0-worker / mid-respawn — those carry no
        // incumbent but still bounce + requeue. Clone the identifier OUT of
        // the pool borrow FIRST (dropping the borrow) so the subsequent
        // `holding_hash_for_worker` immutable self-borrow doesn't overlap.
        let incumbent_identifier: Option<I> = self
            .pool_mut()
            .workers
            .get(worker_id as usize)
            .and_then(|w| w.current_binary.as_ref())
            .map(|task| task.identifier.clone());
        let incumbent: Option<AssignedTaskRef<I>> = incumbent_identifier.map(|task_id| {
            AssignedTaskRef {
                // Reverse-resolve the incumbent's wire hash from the
                // own-worker bookkeeping (`active_tasks: hash -> worker`),
                // the same single truth source `holding_worker` reads;
                // fall back to an empty hash only if the slot is busy
                // without an `active_tasks` entry (a transition window).
                hash: self.holding_hash_for_worker(worker_id).unwrap_or_default(),
                task_id,
            }
        });

        // The bounce is an EXPECTED, no-loss in-flight reconciliation, not a
        // fault: at scale the primary's optimistic per-(secondary, worker_id)
        // dispatch races the secondary's physical respawn/requeue-rebind, so a
        // handful of these per second is steady-state churn (#531 RCA: 414
        // events at 154-worker saturation, all cleanly requeued). DEBUG keeps
        // normal logs clean while retaining the full structured forensics. The
        // PATHOLOGICAL-loop signal (a genuine repeated same-(secondary,worker)
        // bounce — #518 H3.3) is surfaced by the primary's rate-limited WARN on
        // the reconcile path (`handle_illegally_assigned`), the single choke
        // point every bounce passes through.
        match &incumbent {
            Some(inc) => tracing::debug!(
                secondary_id = %self.config.secondary_id,
                worker_id,
                assigned_hash = %assigned.hash,
                incumbent_hash = %inc.hash,
                "primary illegally assigned task to a worker that is BUSY \
                 running another task; bouncing for re-dispatch (NOT failing) \
                 — the secondary never re-picks another worker"
            ),
            None => tracing::debug!(
                secondary_id = %self.config.secondary_id,
                worker_id,
                assigned_hash = %assigned.hash,
                "primary assigned task to a worker slot that cannot take it \
                 (out-of-range id / 0-worker pool / mid-respawn); bouncing for \
                 re-dispatch (NOT failing) — the secondary never re-picks"
            ),
        }

        let msg = DistributedMessage::IllegallyAssignedToNonidleWorker {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            assigned,
            incumbent,
        };
        self.send_to_primary(msg).await?;
        Ok(None)
    }

    /// The wire hash of the task currently running on `worker_id`, read off
    /// the own-worker `active_tasks` map (`hash -> worker`) by reverse
    /// lookup — the same single truth source `holding_worker` consults. A
    /// helper so the illegal-assignment bounce names the incumbent's hash
    /// without re-hashing the binary.
    fn holding_hash_for_worker(&self, worker_id: WorkerId) -> Option<String> {
        match &self.lifecycle {
            super::super::SecondaryLifecycle::Configuring(cfg) => cfg
                .active_tasks
                .iter()
                .find(|&(_, &wid)| wid == worker_id)
                .map(|(hash, _)| hash.clone()),
            super::super::SecondaryLifecycle::Operational(op) => op
                .active_tasks
                .iter()
                .find(|&(_, &wid)| wid == worker_id)
                .map(|(hash, _)| hash.clone()),
            _ => None,
        }
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
