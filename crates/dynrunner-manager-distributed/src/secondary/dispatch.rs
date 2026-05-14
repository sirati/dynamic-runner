use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterStateSnapshot;
use super::SecondaryCoordinator;
use super::election::ElectionState;
use super::wire::{distributed_to_binary, timestamp_now};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
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

                let worker = &mut self.pool.workers[target_wid as usize];
                if worker.is_idle_state() {
                    let estimated_mb = estimated.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
                    let log_task_hash = file_hash.clone();
                    match worker.assign_task(binary, estimated, false).await {
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
                self.note_primary_item_completed(&task_hash);
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
                    self.note_primary_item_failed(&task_hash, &error_type);
                    // Drain-check is harmless even when no entry
                    // was added (no-op when ledger is empty); kept
                    // for symmetry with the other TaskFailed sites
                    // so future maintainers don't have to remember
                    // a per-site filter.
                    self.primary_drain_check_and_retry().await;
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
            DistributedMessage::PeerInfo { peers, .. } => {
                // Step 7 (Decision G): a runtime PeerInfo re-broadcast
                // (the only way observers join a running cluster
                // post-Step 8) lands the updated observer set into
                // the replicated `RoleTable.observers`. Source-of-
                // truth is identical to the setup-time path in
                // `secondary/setup.rs::wait_for_setup` — both call
                // `cluster_state.set_observers(_)` with the union of
                // `is_observer=true` peers from the broadcast. The
                // set is Replace-shaped so a re-broadcast that drops
                // a peer correctly removes it from observers.
                //
                // We deliberately do NOT call
                // `peer_transport.connect_to_peers(peers)` here —
                // mid-run mesh expansion is Step 8/9 territory and
                // adds the dial path through a different entry
                // point. For Step 7's scope, the observer-set
                // replication is the only piece runtime PeerInfo
                // needs to perform.
                let observers: std::collections::HashSet<String> = peers
                    .iter()
                    .filter(|p| p.is_observer)
                    .map(|p| p.secondary_id.clone())
                    .collect();
                self.cluster_state.set_observers(observers);
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
                self.handle_primary_task_request(secondary_id, worker_id, available_memory)
                    .await
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }

    /// Apply a batch of `ClusterMutation`s against the local mirror.
    /// Shared between the operational `dispatch_message` arm and
    /// `wait_for_setup`'s receive loop — both sites observe the same
    /// wire variant and must apply with identical semantics. CRDT
    /// idempotency makes repeated apply safe (duplicates and
    /// late-after-terminal arrivals NoOp by precondition).
    pub(super) fn apply_cluster_mutations(&mut self, mutations: Vec<ClusterMutation<I>>) {
        let count = mutations.len();
        for m in mutations {
            self.cluster_state.apply(m);
        }
        tracing::debug!(
            secondary = %self.config.secondary_id,
            applied = count,
            "applied cluster mutations"
        );
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
    pub(super) fn stage_and_register(
        &mut self,
        file_hash: &str,
        content_hash: &str,
        src_path: &str,
        dest_path: &str,
    ) {
        let src_tmp = self.extraction_cache.tmp_dir().to_path_buf();
        match super::staging::stage_file(
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
    pub(super) async fn report_unresolvable_task(
        &mut self,
        worker_id: u32,
        file_hash: &str,
        local_path: &str,
        resolved_path: &Option<std::path::PathBuf>,
    ) -> Result<bool, String> {
        let local_path_is_relative = std::path::Path::new(local_path).is_relative();
        if resolved_path.is_none()
            && (self.config.src_network.is_some() || local_path_is_relative)
        {
            let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
            let msg = DistributedMessage::TaskFailed {
                sender_id: self.config.secondary_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: self.config.secondary_id.clone(),
                worker_id: wid,
                task_hash: file_hash.into(),
                error_type: ErrorType::NonRecoverable,
                error_message: format!(
                    "file_hash {file_hash} not pre-staged at {local_path}; \
                     expected StageFile notification first"
                ),
            };
            self.send_to_current_primary(msg).await?;
            return Ok(true);
        }
        Ok(false)
    }
}
