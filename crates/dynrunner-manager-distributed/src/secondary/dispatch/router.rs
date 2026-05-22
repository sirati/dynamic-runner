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

use dynrunner_core::{ErrorType, Identifier, MessageReceiver, MessageSender};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::cluster_state::ClusterStateSnapshot;
use crate::primary::PrimaryCommand;
use super::super::election::ElectionState;
use super::super::wire::{distributed_to_binary, timestamp_now};
use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the TaskComplete / TaskFailed arms so a callback-
    /// issued `spawn_tasks` applies inline. Off-loop callers pass
    /// `&mut None`.
    pub(in crate::secondary) async fn dispatch_message(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
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
                let resolved_path =
                    self.resolve_for_dispatch(zip_ref, &local_path, &file_hash);

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
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);

                // Find the target worker — prefer the requested one, fall back to any idle
                let target_wid = if self.pool.workers[wid as usize].is_idle_state() {
                    wid
                } else {
                    self.pool.workers
                        .iter()
                        .position(|w| w.is_idle_state())
                        .map(|i| i as u32)
                        .unwrap_or(wid)
                };

                if self.pool.workers[target_wid as usize].is_idle_state() {
                    let estimated_mb = estimated.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
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
                        .pool
                        .ensure_worker_for_type_async(target_wid, &binary.type_id, factory, false)
                        .await
                    {
                        Ok(dynrunner_manager_local::EnsureWorkerOutcome::AlreadyLoaded) => Ok(()),
                        Ok(dynrunner_manager_local::EnsureWorkerOutcome::RespawnInProgress) => {
                            tracing::debug!(
                                worker_id = target_wid,
                                type_id = %binary.type_id,
                                file_hash = %file_hash,
                                "type-bind respawn issued; stashing task in pending_first_bind \
                                 until WorkerEvent::Ready arrives on the event channel"
                            );
                            // The slot is occupied by a Transitioning
                            // worker with the new type bound; do NOT
                            // queue another restart through
                            // `pending_worker_restarts` — that would
                            // SIGKILL the just-spawned subprocess and
                            // start a third respawn cycle.
                            self.pending_first_bind.insert(
                                target_wid,
                                super::super::PendingFirstBind {
                                    binary,
                                    file_hash,
                                    estimated,
                                    source: super::super::BindSource::PeerAssigned,
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
                        self.pending_worker_restarts.insert(target_wid);
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
                        // Mirror the "no idle worker available" arm:
                        // unicast to the current primary, broadcast to
                        // peers (belt-and-suspenders for primary
                        // changeover mid-flight).
                        self.send_to_current_primary(task_failed.clone()).await?;
                        let _ = self.peer_transport.broadcast(task_failed).await;
                        return Ok(());
                    }
                    let worker = &mut self.pool.workers[target_wid as usize];
                    match worker
                        .assign_task(binary, estimated, false, predecessor_outputs)
                        .await
                    {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, target_wid);
                            self.primary_link.reset_backoff(target_wid);
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
                                self.pool.workers[target_wid as usize].try_reap_exit();
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
                            self.pending_worker_restarts.insert(target_wid);
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
                                error_message:
                                    "worker pipe broken; respawning".into(),
                            };
                            self.send_to_current_primary(msg).await?;
                        }
                    }
                } else {
                    tracing::warn!(
                        worker_id = target_wid,
                        "no idle worker available for task assignment"
                    );
                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: target_wid,
                        task_hash: file_hash,
                        error_type: ErrorType::Recoverable,
                        error_message: "No idle worker available".into(),
                    };
                    // Route to whoever currently holds primary
                    // authority; broadcast to peers as belt-and-
                    // suspenders so the promoted peer's
                    // `handle_primary_peer_rejection` always sees the
                    // bounce even if the unicast loses to a primary
                    // changeover mid-flight. Idempotent — the
                    // primary's recovery path is hash-keyed and
                    // no-ops on the second call.
                    self.send_to_current_primary(msg.clone()).await?;
                    let _ = self.peer_transport.broadcast(msg).await;
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
            DistributedMessage::PromotePrimary { new_primary_id, epoch, required_setup, .. } => {
                // Task #36 / Step 7 defensive guard: an observer MUST
                // NOT be promoted. If we receive a PromotePrimary
                // naming an observer (either us, or a peer in the
                // replicated `RoleTable.observers` set), reject loud
                // — this should not happen now that `is_observer`
                // rides PeerInfo end-to-end into `role_table.observers`
                // (Step 7 closed the wire-level race window the
                // prior comment flagged: "PeerInfo broadcast carrying
                // is_observer was lost"). The rejection remains as
                // belt-and-suspenders against a misconfigured peer
                // or a forged PromotePrimary on the wire.
                let observers = &self.cluster_state.role_table().observers;
                let names_observer = (self.config.is_observer
                    && new_primary_id == self.config.secondary_id)
                    || observers.contains(&new_primary_id);
                if names_observer {
                    tracing::error!(
                        secondary = %self.config.secondary_id,
                        target = %new_primary_id,
                        epoch,
                        self_is_observer = self.config.is_observer,
                        target_in_role_table_observers = observers.contains(&new_primary_id),
                        "REJECTED PromotePrimary naming an observer — observers \
                         cannot host primary role (no workers, no dispatch \
                         authority). With Step 7's `is_observer` ride-along on \
                         PeerInfo, this should only fire for a forged \
                         PromotePrimary or a peer that promoted before \
                         processing the latest PeerInfo broadcast. Ignoring; \
                         the cluster's election should retry with the observer \
                         filtered out."
                    );
                    return Ok(());
                }
                // Apply to the replicated ledger first: last-writer-
                // wins on (epoch, primary_id) makes a stale lower-
                // epoch broadcast a no-op against an already-installed
                // higher-epoch promotion (e.g. a delayed bootstrap
                // PromotePrimary arriving after a failover round).
                // If the ledger rejects it, every other side-effect
                // must short-circuit too — otherwise the routing
                // target diverges from the cluster's authoritative
                // primary identity.
                let outcome = self.cluster_state.apply(ClusterMutation::PrimaryChanged {
                    new: new_primary_id.clone(),
                    epoch,
                });
                if outcome == crate::cluster_state::ApplyOutcome::NoOp {
                    tracing::debug!(
                        new_primary = %new_primary_id,
                        epoch,
                        "ignoring stale PromotePrimary superseded by higher epoch"
                    );
                    return Ok(());
                }
                let became_primary =
                    new_primary_id == self.config.secondary_id && !self.is_primary;
                self.is_primary = new_primary_id == self.config.secondary_id;
                if became_primary {
                    // Stamp the promotion instant so the alive-demoted
                    // natural-quiesce branch in `process_tasks` can
                    // enforce its minimum-elapsed-time gate. See the
                    // `promoted_at` field doc on `SecondaryCoordinator`
                    // for the full rationale. Stamped here BEFORE
                    // `populate_primary_from_cluster_state` so the
                    // grace counts from the role flip, not from the
                    // hydration write (which is sub-millisecond after
                    // but conceptually distinct — a future hydration
                    // refactor should not change the grace start).
                    self.promoted_at = Some(std::time::Instant::now());
                }
                // `on_primary_changed` updates the routing target AND
                // resets per-worker backoff so idle workers fire a
                // fresh `TaskRequest` at the new primary on their next
                // tick instead of sitting through stale windows that
                // accrued against the old primary. This is the
                // cancel-and-reissue primitive the trace at `feb1052`
                // exposed as missing — pre-Phase-P only the routing
                // target moved, leaving the new primary's local
                // workers silent for the residual window.
                self.primary_link.on_primary_changed(new_primary_id.clone());
                if became_primary {
                    if required_setup {
                        // Setup-promote path: the submitter deferred
                        // all run-setup work (discovery, ledger seed,
                        // initial assignment) to us. The cluster ledger
                        // is intentionally empty at this point —
                        // there's nothing to hydrate from. Set the
                        // setup_pending flag so the outer process-tasks
                        // loop yields back to the PyO3 wrapper, which
                        // re-acquires the GIL, runs Python's
                        // `task.discover_items` against our locally
                        // bind-mounted staged source, and calls
                        // `ingest_setup_discovery` to feed the result
                        // back into the ledger. Hydration is deferred
                        // until that call clears the flag.
                        //
                        // NOTE: do NOT call
                        // `populate_primary_from_cluster_state` here —
                        // there is no state to populate from yet, and
                        // a no-op pool would set `primary_pending =
                        // None` which is indistinguishable from
                        // "pool build failed" downstream.
                        self.setup_pending = true;
                        tracing::info!(
                            epoch,
                            "promoted with required_setup=true — yielding to wrapper for discovery"
                        );
                    } else {
                        // Atomic role-flip on continuously-replicated
                        // state: the new primary's pending pool is
                        // hydrated from `cluster_state` directly, no
                        // wire round-trip. Pre-Phase-B this happened
                        // either via FullTaskList arrival (race-prone:
                        // the trace at `feb1052` showed dispatch silence
                        // for 1.6s because PromotePrimary preceded the
                        // payload) or via a cached snapshot
                        // (`populate_primary_from_cache`, which had its
                        // own consume-once footgun).
                        self.populate_primary_from_cluster_state();
                    }
                }
                if self.is_primary {
                    // Sync the election state machine with the role
                    // change so `run_election_tick`'s
                    // `if Promoted return` early-return guards this
                    // node too. Pre-fix the pre-designated primary
                    // had `is_primary=true` but `election=Normal`,
                    // so the keepalive-tick path entered Suspecting
                    // the moment local-primary keepalives went
                    // silent (which is benign post-promotion: the
                    // local primary has demoted itself per
                    // `lifecycle.rs`'s observer-mode contract).
                    // Self-suspect cascaded into self-re-promotion,
                    // hydrated the new pool from a stale
                    // initial-assignment-time snapshot, and dropped
                    // every in-flight task. Surfaced in tokenizer's
                    // v6 trace.
                    self.election = ElectionState::Promoted;
                    tracing::info!(epoch, "this secondary has been promoted to primary");
                } else {
                    tracing::info!(
                        new_primary = %new_primary_id,
                        epoch,
                        "another secondary promoted to primary"
                    );
                }
                // Immediate repoll: every idle worker re-issues its
                // pending TaskRequest at the freshly-identified
                // primary instead of waiting up to a keepalive
                // interval for the periodic `repoll_idle_workers`
                // tick. The race this closes:
                //
                //   1. Process-tasks entry's for-loop sends an
                //      initial TaskRequest for each idle worker. At
                //      that point `primary_link.current_primary() ==
                //      None`, so the request routes via
                //      `primary_transport.send` to the original
                //      submitter (the still-live demoted local
                //      primary).
                //   2. The demoted local's `handle_task_request`
                //      skips the local-assign branch (`!self.demoted`
                //      gate) and tries to relay via
                //      `peer_transport.send(Address::Role(Primary),
                //      msg)`. Pre-PromotePrimary the role-table cache
                //      is empty — the relay drops with the warn line
                //      "Address::Role(Primary) unresolvable:
                //      role-table cache empty for this role".
                //   3. `note_request_sent` already bumped the
                //      requesting worker's backoff window; the
                //      worker is now silent until the next
                //      `repoll_idle_workers` tick — up to a full
                //      `keepalive_interval` (5s on the production
                //      default).
                //   4. Meanwhile the promoted secondary's own two
                //      workers self-assign synchronously from its
                //      newly-hydrated `primary_pending` in the same
                //      process-tasks for-loop and burn through small
                //      workloads (e.g. 20 binaries × ~0.1s each)
                //      well inside the 5s window — peer secondaries
                //      observe zero TaskAssignments.
                //
                // Repolling here (after `on_primary_changed` has
                // cleared per-worker backoff AND installed the new
                // routing target) gives every idle worker an
                // immediate retry against the fresh route. For the
                // promoted secondary the repoll self-assigns from
                // its own pool; for peers it routes via
                // `peer_transport.send_to_peer(promoted_id, msg)`.
                //
                // Gated on `!self.setup_pending` because the
                // setup-promote-promoted path is about to yield to
                // the wrapper for discovery — the local pool is
                // empty and there's no primary to poll yet. The
                // wrapper-driven re-entry into `process_tasks`
                // will run the entry for-loop (`request_task_for_worker`
                // for every idle worker) after the pool hydrates,
                // covering that case without needing a repoll here.
                //
                // 20/0/0/0-style distribution on small workloads was
                // a pre-existing efficiency artifact masked by the
                // larger run-completion bug in `a78c89c` — see
                // `b1ecc53`'s commit message for the run-complete
                // hang fix. This repoll closes the structural race
                // independent of that fix.
                if !self.setup_pending {
                    self.repoll_idle_workers(factory).await;
                }
                Ok(())
            }
            DistributedMessage::TaskComplete {
                task_hash,
                ..
            } => {
                // The local primary forwards observed TaskCompletes
                // to every secondary so each one's `completed_tasks`
                // set stays current — matters on local-death-then-
                // failover, where the elected secondary's
                // `populate_primary_from_cluster_state` consults
                // `self.completed_tasks` to rebuild the primary view.
                // (Peer broadcast covers the common case but is
                // best-effort; this primary-side forward is the
                // reliable backstop.) Idempotent: a forward of our
                // own completion just re-inserts the hash that's
                // already there.
                self.completed_tasks.insert(task_hash.clone());
                self.note_primary_item_completed(&task_hash, command_rx).await;
                Ok(())
            }
            DistributedMessage::TaskFailed {
                task_hash,
                error_type,
                ..
            } => {
                // Same forwarding rationale as TaskComplete; only
                // act on terminal (non-Recoverable) failures since
                // Recoverable retry is owned by the primary
                // (see `note_primary_item_failed`) and a future
                // TaskComplete or terminal TaskFailed will arrive.
                if !matches!(error_type, ErrorType::Recoverable) {
                    self.completed_tasks.insert(task_hash.clone());
                    // Use the failure-aware variant for symmetry
                    // with the other TaskFailed sites (peer.rs,
                    // processing.rs); for non-Recoverable inputs
                    // this is identical to `note_primary_item_completed`
                    // (no entry added to `primary_failed`).
                    self.note_primary_item_failed(&task_hash, &error_type, command_rx).await;
                    // Kickstart our own idle workers so any
                    // reinjected items the per-phase retry-bucket
                    // cascade (inside `note_primary_item_failed`)
                    // produced reach a worker on this tick. Same
                    // shape and rationale as the other TaskFailed
                    // sites — peer workers self-recover on their
                    // own keepalive tick.
                    self.repoll_idle_workers(factory).await;
                }
                Ok(())
            }
            DistributedMessage::RequestClusterSnapshot { sender_id, .. } => {
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
                    .peer_transport
                    .send_to_peer(&sender_id, response)
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
                // existing member to observe the joiner. By current
                // design late-joiners are observers (`is_observer =
                // true`), so the CRDT mutation carries that flag. Apply
                // locally and broadcast over the same canonical path the
                // post-promotion originator uses — receivers learn about
                // the joiner via the widened `apply_peer_joined` rule
                // (`peer_state` entry + observer-set projection),
                // idempotent under duplicate broadcasts from concurrent
                // responders. The CRDT-merge contract handles the rare
                // race where two peers both answered the same join.
                let _ = self
                    .apply_and_broadcast_mutations(vec![
                        ClusterMutation::PeerJoined {
                            peer_id: sender_id,
                            is_observer: true,
                        },
                    ])
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
                match serde_json::from_str::<ClusterStateSnapshot<I>>(&snapshot_json) {
                    Ok(snap) => {
                        self.cluster_state.restore(snap);
                        if self.is_primary {
                            // The post-bootstrap primary rebuilds its
                            // pending pool from the merged ledger.
                            self.populate_primary_from_cluster_state();
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "failed to deserialize ClusterSnapshot payload"
                        );
                    }
                }
                Ok(())
            }
            DistributedMessage::ClusterMutation { mutations, .. } => {
                self.apply_cluster_mutations(mutations);
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
            DistributedMessage::TaskRequest {
                secondary_id,
                worker_id,
                available_resources,
                ..
            } if self.is_primary => {
                let available_memory = available_resources.iter()
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                self.handle_primary_task_request(secondary_id, worker_id, available_memory, factory)
                    .await
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }
}
