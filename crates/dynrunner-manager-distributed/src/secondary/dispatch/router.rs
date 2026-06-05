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

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::{distributed_to_binary, timestamp_now};
use crate::cluster_state::ClusterStateSnapshot;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Wire-frame dispatcher for the frame types the role-aware
    /// `handle_inbound` base does not own directly (TaskAssignment,
    /// StageFile, RequestClusterSnapshot, ClusterSnapshot, PeerInfo,
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
        // Any message from the primary side resets the election state and
        // bumps the keepalive timestamp (F2).
        self.record_primary_message();

        match msg {
            DistributedMessage::TaskAssignment {
                worker_id,
                file_hash,
                binary_info,
                zip_file,
                local_path,
                predecessor_outputs,
                ..
            } => {
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

                // Select the dispatch target worker SAFELY. Prefer the
                // primary's requested slot IF it is a valid, idle worker;
                // otherwise fall back to any idle worker. Both `.get()` and
                // `position()` return `None` for an empty pool, so a
                // 0-worker `Operational` node (late-joiner / observer /
                // phase-end observer — a VALID state on this path) is just
                // the degenerate case of "no idle worker": no out-of-range
                // `len() - 1` arithmetic, no unconditional index into the
                // pool. An out-of-range `worker_id` likewise resolves to
                // `None` on the preference and falls back to an idle worker
                // — never silently clamped onto the wrong (last) slot.
                let pool = &self.op_mut().pool;
                let target_wid: Option<u32> = pool
                    .workers
                    .get(worker_id as usize)
                    .filter(|w| w.is_idle_state())
                    .map(|_| worker_id)
                    .or_else(|| {
                        pool.workers
                            .iter()
                            .position(|w| w.is_idle_state())
                            .map(|i| i as u32)
                    });

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
                        self.op_mut().pending_worker_restarts.insert(target_wid);
                        let task_failed = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id: target_wid,
                            task_hash: file_hash.clone(),
                            error_type: ErrorType::Recoverable,
                            error_message: format!(
                                "No idle worker available (respawn failed): {e}"
                            ),
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
                                task_id = ?binary_info.task_id,
                                phase = %binary_info.phase_id,
                                task_type = %binary_info.type_id,
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
                            self.op_mut().pending_worker_restarts.insert(target_wid);
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
                                sender_id: self.config.secondary_id.clone(),
                                timestamp: timestamp_now(),
                                secondary_id: self.config.secondary_id.clone(),
                                worker_id: target_wid,
                                task_hash: file_hash,
                                error_type: ErrorType::Recoverable,
                                error_message: "worker pipe broken; respawning".into(),
                            };
                            self.send_to_primary(msg).await?;
                        }
                    }
                } else {
                    // No idle worker to take the task — including the
                    // degenerate 0-worker pool (a late-joiner / phase-end
                    // observer), which selected `None` above without any
                    // bounds arithmetic or index. Report backpressure to
                    // the primary keyed by the ORIGINAL wire `worker_id`
                    // (there is no chosen slot).
                    tracing::warn!(worker_id, "no idle worker available for task assignment");
                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: file_hash,
                        error_type: ErrorType::Recoverable,
                        error_message: "No idle worker available".into(),
                    };
                    // Report to the primary only — the authority owns
                    // mesh propagation. Routing across a primary
                    // changeover is opaque: `send_to_primary`
                    // (`Destination::Primary`) resolves at the egress
                    // edge to whichever node currently holds the role.
                    // The promoted peer's `handle_primary_peer_rejection`
                    // recovery is hash-keyed, so the single report
                    // suffices.
                    self.send_to_primary(msg).await?;
                }
                Ok(())
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
            DistributedMessage::RequestClusterSnapshot {
                sender_id,
                is_observer,
                can_be_primary,
                ..
            } => {
                // Any peer can answer — `cluster_state` is replicated,
                // so any responder's snapshot is a valid bootstrap
                // payload. The merge semantics on the receiver
                // (`ClusterState::restore`) reconcile partial /
                // overlapping snapshots, so a duplicate response from
                // multiple peers is harmless.
                //
                // Wire-side `snapshot_json` carries the snapshot
                // serialized via serde_json so the protocol envelope
                // stays free of the manager-distributed crate's
                // `ClusterStateSnapshot<I>` (which is the right-side
                // dependency direction; the protocol crate must not
                // depend on the manager crate).
                let snapshot = self.cluster_state.snapshot();
                let snapshot_json = serde_json::to_string(&snapshot)
                    .map_err(|e| format!("snapshot serialization: {e}"))?;
                let response = DistributedMessage::ClusterSnapshot {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    snapshot_json,
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
                        "failed to deliver ClusterSnapshot response"
                    );
                }
                // Explicit `PeerJoined` origination on late-joiner accept.
                //
                // Late-joiners enter the cluster by sending
                // `RequestClusterSnapshot`; the responder is the first
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
                    }])
                    .await;
                Ok(())
            }
            DistributedMessage::ClusterSnapshot { snapshot_json, .. } => {
                // Lattice-merge into the local mirror via
                // `ClusterState::restore`. Idempotent on duplicates
                // and safe under concurrent live broadcasts (joiner
                // may have already applied mutations the snapshot also
                // contains; the merge keeps the strictly stronger of
                // each).
                //
                // Per-frame fatality discriminator (D-C / D3): this arm is
                // the STEADY-STATE anti-entropy / late-heal pull sink — it
                // runs inside the operational loop (`dispatch_message`), NOT
                // the bootstrap constructor. A malformed snapshot here is
                // WARN-and-keep, NOT fatal: the secondary re-converges
                // REACTIVELY — the next peer `StateDigest` broadcast feeds the
                // digest arm below, which re-pulls a fresh snapshot iff the
                // replica is still behind, so one bad frame cannot wedge or
                // corrupt the replica. (The secondary has NO AE-3 timer
                // cadence — that recovery-tick cadence is the OBSERVER's;
                // here the inbound digest arm is the heal trigger.) The
                // BOOTSTRAP decode (cold-join
                // constructor) stays FATAL — a malformed INITIAL snapshot
                // genuinely leaves the node with no starting state and must
                // hard-fail there. The discriminator is WHICH FUNCTION the
                // decode lives in (steady-state loop arm = WARN, bootstrap
                // constructor = fatal); there is no latch.
                match serde_json::from_str::<ClusterStateSnapshot<I>>(&snapshot_json) {
                    Ok(snap) => {
                        self.cluster_state.restore(snap);
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "ClusterSnapshot decode failed in the steady-state \
                             anti-entropy sink; dropped a malformed snapshot (the \
                             next peer StateDigest broadcast will re-trigger \
                             reconciliation via the reactive digest arm)"
                        );
                    }
                }
                Ok(())
            }
            DistributedMessage::StateDigest {
                digest, sender_id, ..
            } => {
                // Anti-entropy receive: compare the peer's digest against
                // ours and pull a snapshot iff we are behind. The decision
                // (compare + target selection + request construction) lives
                // in `crate::anti_entropy` so primary / secondary / observer
                // share ONE policy; this arm only owns the `send_to` edge.
                // The pull's reply heals via the existing `ClusterSnapshot`
                // recv arm above (idempotent `restore`), so a converged
                // second round matches and pulls nothing (self-quiescing).
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
                    &digest,
                    &sender_id,
                    &requester,
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
                Ok(())
            }
            DistributedMessage::ClusterMutation { mutations, .. } => {
                // `apply_cluster_mutations` mirrors the batch and, for a
                // `PrimaryChanged`, runs the unified primary-activation
                // hook (wake co-located primary + reset election + observer
                // guard). It returns whether a primary-identity change was
                // genuinely applied; when it was, revive the worker-pull
                // rate-limiter — backoff accrued against the PRIOR primary
                // is stale the moment the role changes, so without this the
                // repoll is suppressed by `should_request_now` and idle
                // workers sit through a stale window before re-issuing at
                // the new primary (the dispatch-silence symptom). Keyed off
                // the backoff maps (not the pool) so it fires even before
                // `initialize_workers`. Then immediate repoll so every idle
                // worker re-issues its `TaskRequest` against the freshly-
                // identified primary (`Destination::Primary` re-resolved at
                // the egress edge) instead of waiting a keepalive interval.
                if self.apply_cluster_mutations(mutations) {
                    self.op_mut().primary_link.reset_all_backoff();
                    self.repoll_idle_workers().await;
                }
                Ok(())
            }
            DistributedMessage::PeerInfo { peers: _, .. } => {
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
                Ok(())
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }
}
