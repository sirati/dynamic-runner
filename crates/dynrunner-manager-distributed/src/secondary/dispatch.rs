use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


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
                // phase/type/affinity/payload), then override the path
                // if extraction-cache resolution found a local copy.
                let mut binary = distributed_to_binary(&binary_info);
                if let Some(path) = resolved_path {
                    binary.path = path;
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
                            self.reset_request_backoff(target_wid);
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
                            tracing::error!(
                                worker_id = target_wid,
                                error = %e,
                                "failed to assign task"
                            );
                            let msg = DistributedMessage::TaskFailed {
                                sender_id: self.config.secondary_id.clone(),
                                timestamp: timestamp_now(),
                                secondary_id: self.config.secondary_id.clone(),
                                worker_id: target_wid,
                                task_hash: file_hash,
                                error_type: "NonRecoverable".into(),
                                error_message: e,
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
                        error_type: "Recoverable".into(),
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
            DistributedMessage::PromotePrimary { new_primary_id, .. } => {
                self.is_primary = new_primary_id == self.config.secondary_id;
                // Point send_to_current_primary at the new primary
                // for ALL nodes (the new primary itself, and every
                // other secondary). Without this, non-primary nodes
                // kept routing operational messages via
                // `primary_transport` (the local-machine primary).
                // The local primary's `handle_task_request` then
                // relayed TaskRequests onward to `primary_id` (the
                // SLURM-primary), so dispatch worked as long as the
                // local primary's transport stayed up. When the local
                // primary's transport closed (laptop suspend, SSH
                // tunnel idle close — the tokenizer/dataset trigger),
                // the relay vanished, TaskRequests never reached the
                // SLURM-primary, and workers idled forever. After
                // this fix, secondaries route directly via the peer
                // mesh and don't depend on the local primary's
                // transport being alive. `send_to_current_primary`
                // already handles the self-loopback case
                // (`current_primary == self`) by falling through to
                // `primary_transport`, so setting the field
                // unconditionally is correct for both branches.
                self.primary_peer_id = Some(new_primary_id.clone());
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
                    tracing::info!("this secondary has been promoted to primary");
                } else {
                    tracing::info!(
                        new_primary = %new_primary_id,
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
                // to every secondary so each one's cached completion
                // set stays current — matters on local-death-then-
                // failover, where the elected secondary's
                // populate_primary_tasks filters items against
                // self.completed_tasks (peer broadcast covers the
                // common case but is best-effort; this primary-side
                // forward is the reliable backstop). Idempotent:
                // a forward of our own completion just re-inserts
                // the hash that's already there.
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
                if error_type != "Recoverable" {
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
            DistributedMessage::FullTaskList {
                all_tasks,
                completed_tasks,
                pending_tasks,
                phase_deps,
                ..
            } => {
                let completed_set: HashSet<String> = completed_tasks.into_iter().collect();
                tracing::info!(
                    total = all_tasks.len(),
                    completed = completed_set.len(),
                    pending = pending_tasks.len(),
                    phases = phase_deps.len(),
                    "received full task list"
                );

                // Cache on every secondary: if we get promoted later we
                // can rebuild the primary `PendingPool` from this
                // snapshot (the live primary may by then be dead, so we
                // can't ask for it again).
                self.cached_full_task_list = Some((
                    all_tasks.clone(),
                    completed_set.clone(),
                    phase_deps.clone(),
                ));

                if self.is_primary {
                    self.populate_primary_tasks(all_tasks, completed_set, phase_deps);
                }
                Ok(())
            }
            DistributedMessage::ClusterMutation { mutations, .. } => {
                self.apply_cluster_mutations(mutations);
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
                error_type: "NonRecoverable".into(),
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
