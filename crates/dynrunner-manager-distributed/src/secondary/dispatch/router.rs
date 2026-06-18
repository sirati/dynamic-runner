//! Inbound primary→secondary message router.
//!
//! Single concern: receive one `DistributedMessage` arriving over the
//! primary transport and route it to the per-message handler. The
//! body is intentionally one large `match` because the wire enum is
//! flat and every arm shares the same "record primary-message
//! liveness, then handle" pre-amble (`record_primary_message`). Arms
//! that mutate cluster-replicated state delegate to helpers in
//! [`super::helpers`] so the apply rule has a single writer.
//!
//! Length exception: this file is over the 500-line threshold because
//! the router IS the single concern; splitting individual arms into
//! free functions would require threading every destructured field
//! through a method signature for no behavioural gain. Documented in
//! `secondary/dispatch/mod.rs`.

use std::collections::BTreeMap;

use dynrunner_core::{ErrorType, Identifier, ResourceMap, TaskInfo, TaskOutputs, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    AssignedTaskRef, ClusterMutation, Destination, DistributedMessage, PeerId,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::{distributed_to_binary, timestamp_now};

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Wire-frame dispatcher for the frame types the role-aware
    /// `handle_inbound` base does not own directly (TaskAssignment,
    /// StageFile, RequestSnapshotStream, SnapshotStreamPackage, PeerInfo,
    /// ClusterMutation, plus the test-reachable TaskComplete/TaskFailed
    /// arms). The secondary holds NO authority: every arm here is either
    /// own-worker management, a CRDT mirror apply, or a CLASS-1 report to
    /// the primary role.
    ///
    /// State contract (NOT a compile-time guarantee). The pool-touching
    /// arms (`TaskAssignment`, the `ClusterMutation` repoll/backoff resets
    /// on a `PrimaryChanged`) reach the worker pool / operational fields
    /// through the
    /// `op_mut()` / `pool_mut()` typed accessors. Those accessors are
    /// `#[track_caller]` `.expect(...)` RUNTIME asserts on the lifecycle
    /// state, NOT a type-level "unrepresentable by construction" — making
    /// the bad call truly uncompilable would require threading
    /// coordinator-level state through `OperationalState`. What guarantees
    /// the accessors are only reached when `Operational` is CALL-SITE
    /// ROUTING: `dispatch_message` runs only on the operational inbound
    /// path (post-`enter_operational`); the pre-`Operational` setup
    /// handlers route elsewhere and never enter this dispatcher. A
    /// 0-worker `Operational` node (late-joiner / observer / phase-end
    /// observer) is a VALID state on this path, so the `TaskAssignment`
    /// arm selects the dispatch target as an `Option` (`.get()` /
    /// `position()`, never bounds arithmetic or an unconditional index):
    /// an empty pool is simply the degenerate case of "no idle worker",
    /// reported back to the primary as backpressure like any other.
    pub(in crate::secondary) async fn dispatch_message(
        &mut self,
        msg: DistributedMessage<I>,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        // A frame from the CURRENT PRIMARY resets the election state and
        // bumps the keepalive timestamp (F2) — gated on sender identity, NOT
        // on which transport delivered it. Post one-mesh cutover this
        // dispatcher is reached for frames from ANY peer (a peer secondary's
        // anti-entropy, the submitter's snapshot/run-config requests, a
        // relayed mutation), so an UN-gated reset would cancel a lone
        // survivor's own in-flight self-promotion on a non-primary frame and
        // the single-survivor election would never converge. The gate keys on
        // `current_primary()` — the same single source every other
        // "is this the primary" decision uses.
        self.record_primary_message_if_from_primary(msg.sender_id())
            .await;

        match msg {
            DistributedMessage::TaskAssignment {
                worker_id,
                file_hash,
                binary_info,
                zip_file,
                local_path,
                predecessor_outputs,
                supplanted_holder,
                secondary_id_member_gen,
                ..
            } => {
                // Run-terminal gate (asm-dataset run_20260611_112116,
                // secondary-11's zombie): once the replicated `RunAborted`
                // verdict is latched, NO work-starting edge may run — not
                // the ordinary assign, and not the first-bind / type-shift
                // respawn `ensure_worker_for_type_async` triggers below
                // (production respawned workers "for type-shift" 3+ minutes
                // post-abort). The frame is dropped, not bounced: the run
                // is over cluster-wide, this loop's own tail exits on the
                // same latch within this iteration, and no reply is owed —
                // the authority that sent it exits on the same verdict.
                if let Some(reason) = self.cluster_state.run_aborted() {
                    tracing::info!(
                        reason = %reason,
                        task_hash = %file_hash,
                        worker_id,
                        "TaskAssignment ignored: the replicated run-terminal \
                         verdict is latched (post-abort dispatch); this node \
                         exits on the same latch"
                    );
                    return Ok(());
                }
                // Duplicate-assignment recognition (the post-failover assign
                // loop). A hash this node ALREADY HOLDS in live own-worker
                // bookkeeping (`holding_worker` — the same single truth
                // source the #308 probe responder answers from: the
                // generation-aware `active_tasks` plus the respawn-HOLD
                // `pending_first_bind` deferrals) must NEVER enter the
                // idle-target selection below: with no idle worker it would
                // bounce as the GENERIC "No idle worker available"
                // backpressure — which the authority classifies as requeue,
                // sustaining an indefinite assign → bounce → requeue loop
                // against the still-running task — and with an idle worker
                // present the fallback would DOUBLE-RUN the hash on this
                // node and clobber its `active_tasks` entry. Answer with the
                // already-held coherence report instead, naming the REAL
                // holding worker: the authority keeps the task in flight on
                // this holder (its optimistic dispatch commit is the correct
                // record) and the eventual real terminal settles it. Reached
                // when the authority's replicated ledger lost the `InFlight`
                // fact (the originating primary died between the assignment
                // send and its `TaskAssigned` broadcast landing) or a
                // false-dead recovery requeued a live holder's work.
                if let Some(holding_wid) = self.lifecycle.holding_worker(&file_hash) {
                    tracing::info!(
                        task_hash = %file_hash,
                        requested_worker_id = worker_id,
                        holding_worker_id = holding_wid,
                        "TaskAssignment for a hash this node is already \
                         running (duplicate dispatch — the authority's ledger \
                         lost the in-flight fact); answering already-held so \
                         it re-converges to InFlight on this holder"
                    );
                    let msg = DistributedMessage::TaskFailed {
                        target: None,
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: holding_wid,
                        task_hash: file_hash,
                        error_type: ErrorType::Recoverable,
                        error_message: super::TASK_ALREADY_HELD_WIRE_MESSAGE.into(),
                        // Stamped at the send_to_primary chokepoint (#352).
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    };
                    // Report to the primary role only — the authority owns
                    // mesh propagation, same contract as the backpressure
                    // report below.
                    self.send_to_primary(msg).await?;
                    return Ok(());
                }
                // Pre-start fence B (#530b): addressee-incarnation gate.
                // The primary stamped this dispatch with the addressee's
                // `peer_member_gen` AS IT KNEW IT; the receiver compares
                // against its OWN current gen and rejects a stale lease (a
                // re-removal-and-re-admission crossed the dispatch in
                // flight, so the lease is wholly invalid — the work is owed
                // to the previous incarnation, not this one). Checked
                // BEFORE the supplanted-holder fence because a stale-
                // addressee lease is cheaper to reject (one map lookup, no
                // CRDT comparison) and the lease is wholly invalid in this
                // case — no fence A inspection is meaningful for a frame
                // already aimed at the wrong incarnation. Symmetric to the
                // secondary→primary `InFlightRoster` gen-staleness gate
                // (the primary→secondary direction); a pre-#530 sender that
                // omits the field falls through (the gate is open — the
                // pre-existing behaviour).
                if let Some(lease_gen) = secondary_id_member_gen {
                    let my_gen = self
                        .cluster_state
                        .peer_member_gen(&self.config.secondary_id);
                    if lease_gen != my_gen {
                        tracing::warn!(
                            task_hash = %file_hash,
                            lease_gen,
                            my_gen,
                            "TaskAssignment addressee-incarnation fence (#530b): \
                             the dispatch names a stale incarnation of this \
                             secondary (a re-removal-and-re-admission crossed \
                             the dispatch in flight); rejecting to the primary \
                             so the task is requeued under the live incarnation"
                        );
                        let msg = DistributedMessage::TaskFailed {
                            target: None,
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: file_hash.clone(),
                            error_type: ErrorType::Recoverable,
                            error_message: super::TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE.into(),
                            // Stamped at the send_to_primary chokepoint (#352).
                            delivery_seq: None,
                            // Stamped at the send_to_primary chokepoint (ordering gate).
                            msgs_posted_through: None,
                        };
                        self.send_to_primary(msg).await?;
                        return Ok(());
                    }
                }

                // Pre-start fence A (#530a): supplanted-holder gate. The
                // dispatch carries a hint identifying the ORIGINAL holder
                // and its `peer_member_gen` AT REMOVAL TIME, if any. When
                // the supplanted holder is alive again at gen ≥ supplanted
                // gen (its peer-removal was false-dead — the run_20260612
                // #518 wasted-compute window), this node REFUSES to start a
                // duplicate copy. The reply routes through the same
                // already-held-style primitive so the primary's
                // `handle_task_failed` classifier recognises the marker
                // and reconciles + withdraws via the existing
                // `note_task_already_held` → `reconcile_authoritative_holder`
                // path; the LIVE original holder remains authoritative.
                //
                // A pre-#530 sender that omits the hint falls through (the
                // pre-existing #518 inflight-reconcile post-start dedup
                // contract is preserved). A hint whose holder is no longer a
                // live, present member — the death was genuine (no
                // re-admission), OR the holder deliberately DEPARTED (its
                // `Departed` tombstone is the convergent signal; a graceful
                // self-departure leaves `peer_state` Alive, so the liveness
                // bit alone would wrongly keep a leaving holder
                // "authoritative") — also falls through: the dispatch is
                // legitimate.
                if let Some((peer, supplanted_gen)) = &supplanted_holder {
                    let alive = self.cluster_state.is_peer_alive(peer)
                        && !self.cluster_state.is_member_departed(peer);
                    let live_gen = self.cluster_state.peer_member_gen(peer);
                    if alive && live_gen >= *supplanted_gen {
                        tracing::warn!(
                            task_hash = %file_hash,
                            supplanted_peer = %peer,
                            supplanted_gen = *supplanted_gen,
                            live_gen,
                            "TaskAssignment supplanted-holder fence (#530a): \
                             the original holder is alive again at gen >= the \
                             supplanted gen; refusing to start a duplicate \
                             copy; replying so the primary reconciles and \
                             withdraws (the live original holder is \
                             authoritative)"
                        );
                        let msg = DistributedMessage::TaskFailed {
                            target: None,
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: file_hash.clone(),
                            error_type: ErrorType::Recoverable,
                            error_message:
                                super::TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE.into(),
                            // Stamped at the send_to_primary chokepoint (#352).
                            delivery_seq: None,
                            // Stamped at the send_to_primary chokepoint (ordering gate).
                            msgs_posted_through: None,
                        };
                        self.send_to_primary(msg).await?;
                        return Ok(());
                    }
                }

                // Resolve binary path via the three-mode helper
                // (uses_file_based_items / pre_staged_mode / default
                // extraction-cache). See `resolve_for_dispatch` for
                // the mode decision tree — every dispatch + assign
                // site on the secondary funnels through it so the
                // three modes stay in lockstep.
                let zip_ref = zip_file.as_deref().filter(|z| !z.is_empty());
                let resolved_path = self.resolve_for_dispatch(zip_ref, &local_path, &file_hash);

                // Fail loudly when the worker has no plausible way to
                // open the binary, instead of silently passing
                // through the primary's absolute path and crashing
                // at exec time (which the primary then re-enqueues
                // as Recoverable, producing an infinite
                // dispatch / re-enqueue loop — observed at ~12ms
                // cadence for 6 binaries on a misconfigured SLURM
                // dispatch).
                //
                // Two ways the worker can succeed without resolution:
                //   - the secondary has a staging directory
                //     (`src_network` set) AND the file landed there;
                //     covered by `resolved_path.is_some()` above.
                //   - the secondary shares a filesystem view with the
                //     primary AND `local_path` is the primary's
                //     absolute path (the in-process distributed
                //     manager's mode); for that to be plausible we at
                //     minimum need `local_path` to be absolute.
                //
                // So the failure conditions are:
                //   `resolved_path.is_none()` AND (
                //       `src_network.is_some()`             // staging configured but missed
                //       OR `local_path` is relative          // can't possibly resolve relatively
                //   )
                // The second predicate is what catches the
                // `in_docker` misdetection failure mode: pipeline
                // sends relative paths in SLURM mode, the secondary
                // detected `src_network=None` due to a runtime
                // sentinel mismatch, the old guard missed it, and
                // workers spun on the primary-filesystem-view
                // relative path.
                if self
                    .report_unresolvable_task(worker_id, &file_hash, &local_path, &resolved_path)
                    .await?
                {
                    return Ok(());
                }

                // Hydrate from the wire info first (preserves
                // phase/type/affinity/payload), then surface the
                // locally-resolved on-disk path via the dedicated
                // `resolved_path` field. `binary.path` stays as the
                // wire-supplied identifier so consumers'
                // `task.relative_path` keeps its mirror-against-
                // source-tree meaning regardless of where the
                // secondary's extraction cache landed the file.
                let mut binary = distributed_to_binary(&binary_info);
                if let Some(path) = resolved_path {
                    binary.resolved_path = Some(path);
                }
                let estimated = self.estimator.estimate(&binary);

                self.assign_resolved_task(
                    worker_id,
                    binary,
                    estimated,
                    predecessor_outputs,
                    file_hash,
                    factory,
                )
                .await
            }
            DistributedMessage::StageFile {
                secondary_id,
                file_hash,
                content_hash,
                src_path,
                dest_path,
                ..
            } => {
                // Only act if addressed to us. The wire is broadcast-shaped
                // but each StageFile names exactly one secondary.
                if secondary_id != self.config.secondary_id {
                    tracing::debug!(
                        target = %secondary_id,
                        self_id = %self.config.secondary_id,
                        "ignoring StageFile addressed to another secondary"
                    );
                    return Ok(());
                }
                self.stage_and_register(&file_hash, &content_hash, &src_path, &dest_path);
                Ok(())
            }
            DistributedMessage::TaskComplete { task_hash, .. } => {
                // A `TaskComplete` REPORT frame is a peer's own-worker
                // terminal report to the authority — not a CRDT
                // mutation. A non-authority node has nothing to do with
                // it: the authoritative terminal state arrives as a
                // separate `ClusterMutation::TaskCompleted` broadcast
                // that the `ClusterMutation` arm mirrors idempotently.
                // The secondary keeps NO per-node terminal set, so this
                // arm is a pure observation no-op.
                //
                // Reachable only via a direct `dispatch_message` call
                // (tests); the operational inbound stream routes
                // TaskComplete through the role-aware `handle_inbound`
                // arm (own-worker reporter path).
                tracing::trace!(task_hash, "observed TaskComplete report (no-op)");
                Ok(())
            }
            DistributedMessage::TaskFailed { task_hash, .. } => {
                // Same as the TaskComplete arm: a `TaskFailed` REPORT
                // frame carries no authority for a non-authority node.
                // The authoritative terminal state (and any retry
                // cascade) is owned by the primary and mirrored to this
                // node's `cluster_state` via a `ClusterMutation`
                // broadcast. Pure observation no-op.
                tracing::trace!(task_hash, "observed TaskFailed report (no-op)");
                Ok(())
            }
            // #518 worker-source-of-truth: the primary (which just
            // re-admitted this falsely-removed-but-alive member) asks for
            // the tasks this node's workers are ACTUALLY running, so it can
            // dedup the requeued copies it dispatched onto other members.
            // Answer off the live `active_tasks` bookkeeping.
            DistributedMessage::RequestInFlightRoster { .. } => {
                self.report_inflight_roster().await;
                Ok(())
            }
            // #518: the primary directs this member to stand down a
            // DUPLICATE copy (the original holder re-admitted). Drop a
            // not-yet-started deferral; a copy already running is left to
            // complete (no mid-run abort) and the primary's terminal-dedup
            // absorbs it. See `withdraw_task`.
            DistributedMessage::WithdrawTask { .. } => {
                self.withdraw_task(&msg);
                Ok(())
            }
            DistributedMessage::RequestSnapshotStream {
                sender_id,
                stream_id,
                resume_after,
                task_ranges,
                is_observer,
                can_be_primary,
                ..
            } => {
                // Any peer can answer — `cluster_state` is replicated,
                // so any responder's stream is a valid bootstrap
                // source. The merge semantics on the receiver
                // (`ClusterState::restore`) reconcile partial /
                // overlapping packages, so duplicate streams from
                // multiple peers are harmless.
                //
                // The arm only REGISTERS (or resumes) the stream — one
                // sorted key list + the small tally capture, never a
                // ledger copy or a monolithic serialization. The
                // packages are produced one per loop wakeup by the
                // process-loop's stream arm (`snapshot_streams.
                // next_wake` → `emit_next`), each typed off the
                // requester's self-declared role and addressed by its
                // id — resolvable for a rosterless joiner over its
                // direct leg.
                self.snapshot_streams.accept_request(
                    &self.cluster_state,
                    &sender_id,
                    is_observer,
                    &stream_id,
                    resume_after.as_deref(),
                    &task_ranges,
                );
                // Explicit `PeerJoined` origination on late-joiner accept.
                //
                // Late-joiners enter the cluster by sending
                // `RequestSnapshotStream`; the responder is the first
                // existing member to observe the joiner. Apply locally
                // and broadcast over the canonical origination path so
                // receivers learn about the joiner via the
                // `apply_peer_joined` rule, idempotent under duplicate
                // broadcasts from concurrent responders.
                //
                // The joiner's ACTUAL role rides the request frame's
                // `is_observer` field — the joiner declares its own role
                // when it calls `join_running_cluster`. A worker joins
                // with `false`, an observer with `true`. This carries
                // the truth into `apply_peer_joined`, whose observer
                // flag is an upward-only ratchet: a `false` for a
                // re-bootstrapping worker is a NoOp against an existing
                // `false` entry (no mis-upgrade), and a `true` for an
                // observer correctly populates `RoleTable.observers`.
                // Only `PeerRemoved` ever clears the observer flag, so
                // the ratchet never regresses a genuine observer.
                // The id's CURRENT membership incarnation: a secondary
                // responder never bumps the generation (the primary's
                // frame-ingest re-admission seam is the sole authority
                // for re-admitting a removed id), so a join for a
                // still-removed id is the sticky NoOp.
                let member_gen = self.cluster_state.peer_member_gen(&sender_id);
                let _ = self
                    .apply_and_broadcast_mutations(vec![ClusterMutation::PeerJoined {
                        peer_id: sender_id,
                        is_observer,
                        // The late-joiner declared its own primary-capability
                        // on the snapshot request (twin of `is_observer`); a
                        // re-bootstrapping compute secondary carries `true`,
                        // an observer late-joiner `false`. Record that truth
                        // in the replicated `RoleTable.can_be_primary`.
                        can_be_primary,
                        // Stamped at the origination choke point
                        // (`apply_and_broadcast_mutations` → `stamp_versions`).
                        cap_version: Default::default(),
                        member_gen,
                    }])
                    .await;
                Ok(())
            }
            DistributedMessage::SnapshotStreamPackage {
                sender_id,
                stream_id,
                cursor,
                payload,
                done,
                ..
            } => {
                // Lattice-merge into the local mirror via the shared
                // single-writer restore helper (`ClusterState::restore` +
                // the primary-identity seam — see
                // `restore_snapshot_stream_frame`; `wait_for_setup`'s
                // receive loop shares it). Each package is a PARTIAL
                // snapshot; the merge is idempotent on duplicates and
                // safe under concurrent live broadcasts (joiner may have
                // already applied mutations a package also contains;
                // the merge keeps the strictly stronger of each).
                //
                // Per-frame fatality discriminator (D-C / D3): this arm is
                // the STEADY-STATE anti-entropy / late-heal pull sink — it
                // runs inside the operational loop (`dispatch_message`), NOT
                // the bootstrap constructor. A malformed package here is
                // WARN-and-keep, NOT fatal: the secondary re-converges
                // REACTIVELY — the next peer `StateDigest` broadcast feeds the
                // digest arm below, which re-pulls iff the replica is still
                // behind (resuming from the last good cursor), so one bad
                // frame cannot wedge or corrupt the replica. (The secondary
                // has NO AE-3 timer cadence — that recovery-tick cadence is
                // the OBSERVER's; here the inbound digest arm is the heal
                // trigger.) The BOOTSTRAP decode (cold-join constructor)
                // stays FATAL — a malformed INITIAL payload genuinely
                // leaves the node with no starting state and must
                // hard-fail there. The discriminator is WHICH FUNCTION the
                // decode lives in (steady-state loop arm = WARN, bootstrap
                // constructor = fatal); there is no latch.
                //
                // A heal that genuinely advanced the primary identity gets
                // the SAME per-primary state refresh the live
                // `ClusterMutation::PrimaryChanged` apply gets (the
                // `react_to_primary_identity_change` single owner) — a
                // snapshot-healed primary flip must re-announce MeshReady /
                // reset backoff / repoll exactly like a broadcast-delivered
                // one.
                if self.restore_snapshot_stream_frame(
                    &sender_id,
                    &stream_id,
                    cursor.as_deref(),
                    &payload,
                    done,
                ) {
                    self.react_to_primary_identity_change().await;
                }
                // The terminal package ends the disciplined pull's in-flight
                // cycle: return the driver to Idle so a converged node goes
                // quiescent and a still-behind node (a WARN-dropped package)
                // re-probes on its NEXT divergence detection rather than
                // waiting out the rebalance. A NoOp for a bootstrap-stream
                // `done` (its id never matches the pull driver's in-flight
                // stream).
                if done {
                    self.pull_coordinator.on_pull_done(&stream_id);
                }
                Ok(())
            }
            DistributedMessage::StateDigest {
                digest,
                sender_id,
                sender_is_observer,
                ..
            } => {
                // Anti-entropy receive: compare the peer's digest against
                // ours and pull a snapshot iff we are behind, via the
                // shared single-writer helper (`reconcile_state_digest`;
                // `wait_for_setup`'s receive loop shares it). The pull's
                // reply heals via the `ClusterSnapshot` recv arm above
                // (idempotent restore), so a converged second round matches
                // and pulls nothing (self-quiescing). The sender's declared
                // role rides the frame so the pull is typed off it.
                self.reconcile_state_digest(&sender_id, sender_is_observer, &digest)
                    .await;
                Ok(())
            }
            // Pull-model PROBE from a behind peer: answer with our inbox
            // depth + the responder-side `ahead` bit. Direct-neighbours-only
            // (the ingress never re-broadcast this inbound `All`), handled
            // locally, never relayed onward.
            DistributedMessage::PullProbe {
                sender_id, digest, ..
            } => {
                self.handle_pull_probe(&sender_id, &digest).await;
                Ok(())
            }
            // Pull-model PROBE REPLY: a direct neighbour answered our probe.
            // Fed to the single-flight pull driver (smallest-inbox-ahead
            // selection + first-answer fallback).
            DistributedMessage::PullProbeReply {
                sender_id,
                requester,
                inbox_size,
                ahead,
                range_digest,
                ..
            } => {
                self.handle_pull_probe_reply(
                    &sender_id,
                    &requester,
                    inbox_size,
                    ahead,
                    range_digest,
                )
                .await;
                Ok(())
            }
            // Pull-model FAIL: the chosen target could not serve our pull
            // (its direct leg to us dropped — delivered INDIRECTLY via the
            // relay). Fall to the next candidate.
            DistributedMessage::PullFail {
                requester,
                stream_id,
                ..
            } => {
                self.handle_pull_fail(&requester, &stream_id).await;
                Ok(())
            }
            DistributedMessage::ClusterMutation { mutations, .. } => {
                // `apply_cluster_mutations` mirrors the batch and, for a
                // `PrimaryChanged`, runs the unified primary-activation
                // hook (Phase-C seam: signal `Process` to build the primary
                // on a self-named promotion + reset election + observer
                // guard). It returns whether a primary-identity change was
                // genuinely applied; when it was, refresh every piece of
                // per-primary-pointed state (MeshReady re-announce to the
                // new primary + worker-pull revive + immediate repoll) via
                // the single-owner reaction —
                // `react_to_primary_identity_change` documents the pieces.
                if self.apply_cluster_mutations(mutations) {
                    self.react_to_primary_identity_change().await;
                }
                Ok(())
            }
            DistributedMessage::PeerInfo { peers, .. } => {
                // Observer-set replication no longer rides PeerInfo:
                // the primary originates one
                // `ClusterMutation::PeerJoined { is_observer: true }`
                // per observer secondary alongside its PeerInfo
                // broadcast, and the standard `apply_cluster_mutations`
                // path on the `ClusterMutation` arm above is the
                // sole writer to `RoleTable.observers`. A runtime
                // PeerInfo re-broadcast therefore has nothing to do
                // at the receiver in this step's scope: mid-run mesh
                // expansion (Step 8/9) and any wider peer-lifecycle
                // handling (Batch D) will route through their own
                // paths. The `PeerConnectionInfo.is_observer` field
                // remains on the wire frame for backwards
                // compatibility but is not consumed here.
                //
                // The liveness-beacon path DOES consume PeerInfo: rebuild
                // the id→liveness-address view + re-point the beacon, so a
                // mid-run roster update (a peer's liveness port changed /
                // newly advertised) is reflected. Single concern: address
                // capture for the beacon; no role/CRDT side effect.
                //
                // Receipt narration (#362): an operational-phase PeerInfo
                // was previously handled in TOTAL silence here, while its
                // real effect — the mesh-pump re-running the peer-dial
                // sweep off the same frame, re-dialing any missing legs —
                // happened invisibly. That made a roster re-broadcast an
                // unnameable rescue: members whose first dials failed were
                // healed by it, members it never reached stayed dead, and
                // neither could be seen in the logs. Name the receipt; the
                // transport's "peer-dial sweep" line names the dials.
                tracing::info!(
                    peers = peers.len(),
                    "peer list received (operational); the mesh-pump re-runs \
                     the peer-dial sweep off it"
                );
                // #658 scancel-self-heal-pending notice: peers present no
                // `slurm_job_id` because that field is wire-only at this rev
                // (always `None`) — its value-source population (Layer 4)
                // and the consensus-restart scancel self-heal (Layer 5) are
                // both unimplemented. This is NOT a version mismatch; the
                // condition holds for every same-version peer fleet-wide.
                // So the operator MUST know that, until those layers land, a
                // consensus-declared-dead mid-run peer's SLURM job will not
                // be auto-scancelled and must be scancelled manually. Emit
                // the notice ONCE per run (gated on
                // `consensus_scancel_pending_warned`) — a per-peer,
                // per-frame WARN would just repeat the same pending-feature
                // statement N times.
                if !self.consensus_scancel_pending_warned
                    && peers.iter().any(|peer| {
                        peer.slurm_job_id.is_none()
                            && !peer.secondary_id.is_empty()
                            && peer.secondary_id != self.config.secondary_id
                    })
                {
                    self.consensus_scancel_pending_warned = true;
                    tracing::warn!(
                        target: "dynrunner_consensus",
                        "scancel self-heal not yet implemented: peer \
                         slurm_job_id population (Layer 4) and the \
                         consensus-restart scancel self-heal (Layer 5) are \
                         unshipped, so a consensus-declared-dead mid-run \
                         peer's SLURM job will NOT be auto-scancelled — \
                         scancel such a peer's SLURM job manually until those \
                         layers land (this is expected on current builds, not \
                         a version mismatch)"
                    );
                }
                self.ingest_peer_liveness_addrs(&peers);
                Ok(())
            }
            DistributedMessage::RunConfig { forwarded_argv, .. } => {
                // Inbound run-config PUSH from the primary (see
                // `store_pushed_run_config` for the full rationale). Reached
                // when the push lands after this secondary is already
                // operational; the usual landing window is mid-setup
                // (`wait_for_setup`), which shares the same helper.
                self.store_pushed_run_config(forwarded_argv);
                Ok(())
            }
            DistributedMessage::RequestRunConfig { sender_id, .. } => {
                // PURE read-only run-config responder. Answer a joining /
                // respawned / cold-start-fetching peer from this node's
                // node-local `forwarded_argv` and unicast exactly ONE
                // `RunConfig` back (its return address rides `sender_id`,
                // mirroring the snapshot responder's reply edge). Unlike the
                // `RequestSnapshotStream` arm above, it does NOT originate
                // `PeerJoined`, does NOT send any welcome, and never touches
                // roster / capacity / CRDT: the run-config is a node-local
                // launch constant, not lattice data, so answering for it is
                // read-only peer gossip — NOT authority (a secondary holds
                // none; the work-split is preserved). Available on the
                // secondary role so a cold-start fetch is answerable before
                // any primary exists / promotes. A send failure is logged
                // best-effort, exactly as the snapshot responder treats its
                // own; the requester's bounded recv wait falls back to its
                // own deadline.
                let response = DistributedMessage::RunConfig {
                    target: None,
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    forwarded_argv: self
                        .forwarded_argv
                        .lock()
                        .expect("forwarded_argv mutex poisoned")
                        .clone(),
                };
                if let Err(e) = self
                    .send_to(
                        Destination::Secondary(PeerId::from(sender_id.clone())),
                        response,
                    )
                    .await
                {
                    tracing::warn!(
                        target = %sender_id,
                        error = %e,
                        "failed to deliver RunConfig response"
                    );
                }
                Ok(())
            }
            DistributedMessage::SetupAssignment { task_hash, .. } => {
                // The primary directed a `TaskKind::Setup` task to this node's
                // in-process executor (this node is the task's affinity
                // member). Runs poolless (a setup task never touches the
                // worker pool), so it is valid on a 0-worker Operational node.
                // The executor body + the terminal report to the primary live
                // in `secondary::setup_exec`; this is the one-line delegate.
                self.execute_setup_assignment(task_hash).await
            }
            // #556 mesh-consensus inbound. Each arm hands the frame to the
            // FSM via the thin wiring layer (`secondary::consensus::wiring`)
            // and dispatches any emitted frames through the existing
            // egress. `SuspectPeers` / `RestartRequest` come FROM the
            // primary (observe its epoch on arrival); `PeerProbe` /
            // `PeerProbeAck` are secondary-to-secondary frames the mesh
            // routes between probers and suspects. The FSM owns every
            // stale-round / stale-epoch defensive drop.
            DistributedMessage::SuspectPeers {
                consensus_id,
                primary_epoch,
                member_gen,
                suspected,
                ..
            } => {
                self.handle_consensus_suspect_peers(
                    consensus_id,
                    primary_epoch,
                    member_gen,
                    suspected,
                )
                .await;
                Ok(())
            }
            DistributedMessage::RestartRequest {
                consensus_id,
                primary_epoch,
                member_gen,
                candidates,
                ..
            } => {
                self.handle_consensus_restart_request(
                    consensus_id,
                    primary_epoch,
                    member_gen,
                    candidates,
                )
                .await;
                Ok(())
            }
            DistributedMessage::PeerProbe {
                sender_id,
                consensus_id,
                probed_id,
                ..
            } => {
                self.handle_consensus_probe(&sender_id, consensus_id, &probed_id)
                    .await;
                Ok(())
            }
            DistributedMessage::PeerProbeAck {
                sender_id,
                consensus_id,
                prober_id,
                ..
            } => {
                self.handle_consensus_probe_ack(&sender_id, consensus_id, &prober_id)
                    .await;
                Ok(())
            }
            // The secondary never legitimately RECEIVES `ResolvedPeer` /
            // `RestartConfirm` (those are secondary-to-primary replies).
            // A landing here is either a wire-routing bug or a
            // co-located-loopback echo; drop silently.
            DistributedMessage::ResolvedPeer { .. }
            | DistributedMessage::RestartConfirm { .. } => {
                tracing::debug!(
                    msg_type = ?msg.msg_type(),
                    "#556 consensus reply variant addressed to secondary; \
                     dropping (wire-routing edge)"
                );
                Ok(())
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }

    /// Bind a RESOLVED work-task binary onto an idle worker on this node:
    /// select the dispatch target slot, ensure the per-type subprocess, and
    /// assign the task (or report backpressure when no idle slot is free).
    ///
    /// Extracted verbatim from the `TaskAssignment` arm so it has EXACTLY ONE
    /// definition: the arm calls it directly for the normal (non-affine-gated)
    /// path, and the SecondaryAffine release
    /// ([`Self::dispatch_released_affine_dependent`]) calls it to dispatch a
    /// dependent `B` the gate withheld until its per-secondary import finished.
    /// The release path reaches the SAME selection + per-type-ensure + assign
    /// logic — no second dispatch path, no duplicated worker-binding logic.
    ///
    /// `worker_id` is the primary's REQUESTED slot (preferred if idle, else any
    /// idle worker). `binary` is the wire-hydrated `TaskInfo` with its on-disk
    /// `resolved_path` already folded in; `estimated`/`predecessor_outputs` are
    /// forwarded verbatim. `file_hash` is the `active_tasks` key + the wire
    /// task hash. The run-aborted gate is upstream of every caller (the
    /// `TaskAssignment` arm and the affine release both check it before
    /// reaching here).
    pub(in crate::secondary) async fn assign_resolved_task(
        &mut self,
        worker_id: WorkerId,
        binary: TaskInfo<I>,
        estimated: ResourceMap,
        predecessor_outputs: BTreeMap<String, TaskOutputs>,
        file_hash: String,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        // HONOR the primary's assigned `worker_id` — never re-pick (#517).
        // The secondary holds no scheduling authority: it dispatches onto
        // the EXACT slot the primary chose IFF that slot is idle, else it
        // bounces a typed `IllegallyAssignedToNonidleWorker` so the primary
        // reconciles its diverged occupancy + requeues (NOT a failure). The
        // shared `select_honored_target_or_bounce` owns the honor-or-bounce
        // decision AND the bounce send (the SAME seam the setup-time
        // initial-assignment loop uses), so the two can never diverge into
        // the pre-#517 any-idle re-pick. SAFETY (preserves cabd34ab): the
        // `.get()` Option path inside makes an out-of-range id / 0-worker
        // pool a clean bounce — no `len() - 1` underflow, no unconditional
        // index. `None` ⇒ the task was bounced for re-dispatch and this node
        // dispatches nothing.
        let assigned_ref = AssignedTaskRef {
            hash: file_hash.clone(),
            task_id: binary.identifier.clone(),
        };
        let target_wid = self
            .select_honored_target_or_bounce(worker_id, assigned_ref)
            .await?;

        if let Some(target_wid) = target_wid {
            let estimated_mb =
                estimated.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
            let log_task_hash = file_hash.clone();
            // Per-type subprocess dispatch via the async-event
            // flow for both first-bind (`loaded_type_id == None`)
            // AND true type-shift (`Some(T1) → Some(T2)`). The
            // same-type fast path short-circuits inside the
            // pool with `EnsureWorkerOutcome::AlreadyLoaded`;
            // the respawn path returns `RespawnInProgress`
            // and the dispatch arm stashes the binary in
            // `pending_first_bind` for the
            // `WorkerEvent::Ready` handler to pick up.
            //
            // Pre-fix the first-bind path called the
            // synchronous `ensure_worker_for_type`, which
            // drives `poll_ready` inline. Inside this
            // `select!` arm body the inline wait blocked
            // every other arm — peer-bus relays, keepalive,
            // worker events, OOM ticks — for the entire
            // duration the freshly-spawned worker subprocess
            // took to send `Response::Ready`. The bug
            // manifested in production as a 300+s tokio-
            // runtime silence on the asm-tokenizer LMU
            // dispatch when one of the Python workers took
            // longer than `keepalive_timeout` to import its
            // task module.
            //
            // The earlier type-shift fix (commit 7862339)
            // closed the wedge for `Some(T1) → Some(T2)` by
            // routing the task through the same wire-bounce
            // contract the live primary's
            // `handle_primary_peer_rejection` recognises.
            // That works architecturally but biases
            // distribution on small workloads (peer
            // secondaries pay one wire round-trip per
            // type-bind, the promoted-primary's self-
            // assigns reach the same-type fast path within
            // sub-millisecond of Ready). Storing the binary
            // in `pending_first_bind` keeps it pinned to the
            // worker the primary picked: no wire round-trip,
            // no fairness regression, and the loss path
            // (`WorkerEvent::Disconnected`) still recovers
            // via the backpressure marker if the worker
            // never reaches Ready.
            let ensure_result: Result<(), String> = match self
                .op_mut()
                .pool
                .ensure_worker_for_type_async(target_wid, &binary.type_id, factory, false)
                .await
            {
                Ok(dynrunner_manager_local::EnsureWorkerOutcome::AlreadyLoaded) => Ok(()),
                Ok(dynrunner_manager_local::EnsureWorkerOutcome::RespawnInProgress) => {
                    // Respawn-HOLD (#58): the per-type subprocess
                    // for `target_wid` is mid-kill+spawn (the
                    // pool kicked off a background `wait_ready`
                    // task). DEFER this task rather than drop it
                    // or busy-bounce it to the authority: stash
                    // the resolved binary in `pending_first_bind`
                    // keyed by the worker; the
                    // `WorkerEvent::Ready` handler picks it up and
                    // calls `assign_task` once the slot is
                    // observably Idle with the new type bound. No
                    // drop, no tight retry loop. The loss path
                    // (`WorkerEvent::Disconnected` before Ready)
                    // reports the deferred task back to the
                    // authority as backpressure via
                    // `report_deferred_task_lost`.
                    tracing::debug!(
                        worker_id = target_wid,
                        type_id = %binary.type_id,
                        file_hash = %file_hash,
                        "type-bind respawn in progress; deferring task until \
                         worker Ready (respawn-HOLD)"
                    );
                    // The type-shift respawn just REPLACED the slot's
                    // subprocess (a new generation). Sweep any task
                    // still bound to this slot in `active_tasks` into
                    // the reinject path so the replaced generation
                    // cannot strand it (assigned-never-terminal). The
                    // deferred task we are about to stash lives in
                    // `pending_first_bind`, NOT `active_tasks`, so the
                    // sweep never touches it. No-op when the slot was
                    // idle/already-swept (the common case — the
                    // dispatch target was selected idle). This is the
                    // belt-and-braces companion to the generation
                    // gate: the gate stops a stale terminal from
                    // mis-attributing; this stops a replacement from
                    // abandoning a still-bound task.
                    //
                    // ORDERING: the sweep runs after the generation
                    // bump, in the SAME select-arm body — the event
                    // channel cannot drain between bump and sweep
                    // within one loop iteration. Keep them adjacent;
                    // even if a future refactor yielded to the event
                    // arm in between, the bumped generation makes the
                    // gate drop a draining stale terminal first.
                    self.sweep_replaced_worker_task(target_wid).await?;
                    self.op_mut().pending_first_bind.insert(
                        target_wid,
                        super::super::PendingFirstBind {
                            binary,
                            file_hash,
                            estimated,
                            predecessor_outputs,
                        },
                    );
                    return Ok(());
                }
                Err(e) => Err(e),
            };
            if let Err(e) = ensure_result {
                tracing::warn!(
                    worker_id = target_wid,
                    error = %e,
                    type_id = %binary.type_id,
                    "ensure_worker_for_type failed for peer-assigned task; queuing respawn"
                );
                self.schedule_worker_restart(target_wid);
                let task_failed = DistributedMessage::TaskFailed {
                    target: None,
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: self.config.secondary_id.clone(),
                    worker_id: target_wid,
                    task_hash: file_hash.clone(),
                    error_type: ErrorType::Recoverable,
                    error_message: format!("No idle worker available (respawn failed): {e}"),
                    // Stamped at the send_to_primary chokepoint (#352).
                    delivery_seq: None,
                    // Stamped at the send_to_primary chokepoint (ordering gate).
                    msgs_posted_through: None,
                };
                // Report to the primary role only; the authority
                // owns mesh propagation (it originates the CRDT
                // mutation and broadcasts it). A reporting
                // secondary that also broadcast would be a second
                // CRDT originator.
                self.send_to_primary(task_failed).await?;
                return Ok(());
            }
            // Snapshot the assigned binary for the sampler
            // hook before the move into `assign_task`. The
            // hook only reads `task_id` so cloning the
            // whole `TaskInfo` once is cheap relative to
            // the assignment-write hot path.
            let binary_for_hook = binary.clone();
            let worker = &mut self.op_mut().pool.workers[target_wid as usize];
            match worker
                .assign_task(binary, estimated, false, predecessor_outputs)
                .await
            {
                Ok(()) => {
                    self.notify_sampler_assigned(target_wid, &binary_for_hook);
                    self.op_mut().active_tasks.insert(file_hash, target_wid);
                    self.op_mut().primary_link.reset_backoff(target_wid);
                    tracing::info!(
                        worker_id = target_wid,
                        task_id = ?binary_for_hook.task_id,
                        phase = %binary_for_hook.phase_id,
                        task_type = %binary_for_hook.type_id,
                        task_hash = %log_task_hash,
                        estimated_mb,
                        "assigned task from primary"
                    );
                }
                Err(e) => {
                    // Worker subprocess likely died between
                    // tasks. Reap so the log carries the
                    // actual signal/code rather than the
                    // pipe-level "Broken pipe" string. See
                    // `WorkerHandle::try_reap_exit` for None
                    // conditions.
                    let exit_status =
                        self.op_mut().pool.workers[target_wid as usize].try_reap_exit();
                    tracing::warn!(
                        worker_id = target_wid,
                        error = %e,
                        exit_status = exit_status.as_ref().map(|s| s.to_string()),
                        "peer-assign failed; queuing worker respawn + requeuing task at primary"
                    );
                    // Bug B: queue the worker for respawn
                    // at the next `process_tasks` tick. The
                    // dead pipe stays dead until the manager
                    // brings a replacement up; pre-fix the
                    // SLURM-secondary path silently abandoned
                    // the slot.
                    self.schedule_worker_restart(target_wid);
                    // Bug C: the task hasn't been attempted
                    // — the pipe-write never landed. Send
                    // the primary a backpressure-shaped
                    // TaskFailed (`Recoverable` + the marker
                    // message the primary's
                    // `handle_primary_peer_rejection` path
                    // recognises via `is_backpressure`) so
                    // the primary requeues the binary into
                    // its pool and re-dispatches once a peer
                    // signals capacity. Pre-fix this sent
                    // `NonRecoverable + e` which marked the
                    // un-attempted task as terminal failed;
                    // combined with Bug B (no respawn) this
                    // lost every subsequent task assigned
                    // to the dead slot.
                    let msg = DistributedMessage::TaskFailed {
                        target: None,
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: target_wid,
                        task_hash: file_hash,
                        error_type: ErrorType::Recoverable,
                        error_message: "worker pipe broken; respawning".into(),
                        // Stamped at the send_to_primary chokepoint (#352).
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    };
                    self.send_to_primary(msg).await?;
                }
            }
        }
        // `target_wid == None`: `select_honored_target_or_bounce` ALREADY
        // bounced the typed `IllegallyAssignedToNonidleWorker` (the
        // requested slot was busy / out-of-range / 0-worker) — the task is
        // back at the authority for reconcile + requeue, NOT failed. Nothing
        // more to send here. The pre-#517 "No idle worker available"
        // TaskFailed bounce is GONE from this path: it existed only to cover
        // the re-pick fallback's exhaustion, and the typed bounce subsumes
        // it (the authority reconciles its occupancy instead of merely
        // requeuing onto a model that keeps re-assigning the busy slot).
        Ok(())
    }
}
