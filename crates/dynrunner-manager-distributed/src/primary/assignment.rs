use std::collections::HashMap;

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, PeerId, StagedFileRecord, WorkerReadyInfo, ZipBinaryEntry,
    ZipFileAssignment,
};
use dynrunner_scheduler_api::{AssignmentDecision, ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};
use super::PrimaryCoordinator;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    pub(super) async fn perform_initial_assignment(
        &mut self,
    ) -> Result<InitialAssignmentOutcome, String> {
        tracing::info!("performing initial assignment");

        // Group pending StageFile records by recipient so they can
        // ride inline in each secondary's InitialAssignment. Done up
        // front (rather than during the per-secondary loop below) so
        // we drain `self.pending_stage_files` once and don't hold
        // overlapping borrows on `self`.
        let mut staged_per_secondary: HashMap<String, Vec<StagedFileRecord>> = HashMap::new();
        for (secondary_id, file_hash, content_hash, src_path, dest_path) in
            std::mem::take(&mut self.pending_stage_files)
        {
            staged_per_secondary
                .entry(secondary_id)
                .or_default()
                .push(StagedFileRecord {
                    file_hash,
                    content_hash,
                    src_path,
                    dest_path,
                });
        }

        // V5: the roster READ routes through the replicated
        // `cluster_state.known_secondaries()` — the CRDT-derived known set —
        // NOT `self.secondaries` (which keeps transport-handle metadata
        // only). Name-sorted so the per-secondary `InitialAssignment`
        // fan-out + Operational transition below are deterministic across
        // runs (important for repro / log-diffing). The matching `self.workers`
        // roster was already built — round-robin name-sorted, the SAME
        // ordering — by `reconstruct_workers_from_cluster_state` (the SOLE
        // roster builder, V2) at the run-init call site just before this.
        let mut secondary_ids: Vec<String> = self
            .cluster_state
            .known_secondaries()
            .map(String::from)
            .collect();
        secondary_ids.sort();

        // V2: `perform_initial_assignment` no longer BUILDS the roster — it
        // is a pure scheduler over the existing `self.workers` (rebuilt by
        // `reconstruct_workers_from_cluster_state`, which is the sole builder
        // and produces the identical round-robin name-sorted shape this loop
        // used to construct). The round-robin `self.workers.push` block that
        // lived here (duplicating `reconstruct_workers_from_cluster_state`)
        // was deleted; this function now reads `self.workers` and never
        // constructs it.

        // Perform initial assignment for each worker. The pool is
        // pre-sorted by `run()` (size DESC) and bucketed by
        // `(phase, type, affinity)`; per-worker visibility is the
        // `view_for_worker` slice the scheduler chooses from.
        //
        // Worker visit order is `dispatch_order` — the ONE owner of the
        // dispatch-target ordering policy, shared with the operational
        // recheck (`dispatch_to_idle_workers`) — so the initial batch
        // interleaves grants across secondaries (least-projected-load
        // round-robin) instead of relying on the roster Vec's layout
        // for spread. On the cold all-idle roster the order coincides
        // with the round-robin construction order; on a roster carrying
        // inherited occupancy (promotion/resume) it correctly
        // deprioritizes already-loaded secondaries, where the raw
        // `0..len` scan it replaces ignored load entirely.
        let mut assignments_per_secondary: HashMap<String, Vec<(u32, TaskInfo<I>, ResourceMap)>> =
            HashMap::new();
        let mut total_assigned_resources = ResourceMap::new();

        for worker_idx in super::lifecycle::dispatch_order(&self.workers) {
            let worker_info = self.workers[worker_idx].budget_info();
            let max_res = self.workers[worker_idx].resource_budgets.clone();
            let global_wid = self.workers[worker_idx].worker_id;
            // Soft preference tie-break: tasks whose
            // `preferred_secondaries` lists this worker's secondary
            // sort first within their priority class. The predicate
            // is applied AFTER `cap_filter_view` — caps are hard,
            // preferences are advisory. See
            // `primary::preferred_secondaries` for the helper's
            // contract.
            let secondary_id = self.workers[worker_idx].secondary_id.clone();
            let preference_predicate =
                super::preferred_secondaries::apply_preferred_secondaries_predicate::<I>(
                    &secondary_id,
                );
            let view = self.cap_filter_view(
                self.pool()
                    .view_for_worker(global_wid, Some(&preference_predicate)),
            );
            if view.is_empty() {
                continue;
            }
            let decision = self.scheduler.assign_initial(
                &worker_info,
                view.as_slice(),
                &total_assigned_resources,
                &max_res,
                &self.estimator,
            );

            if let AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                let binary = self.pool_mut().take_from_view(view, binary_index);
                total_assigned_resources.add(&estimated_usage);

                let secondary_id = self.workers[worker_idx].secondary_id.clone();
                // Secondary-local worker id (the wire `worker_id`).
                let local_worker_id = self.local_worker_id_in_secondary(worker_idx);

                // Type-slot reserve + slot `Idle -> Assigned{task_hash}`
                // + ledger insert, committed together at the moment of
                // initial dispatch. The wire `InitialAssignment` is
                // built+sent below in the per-secondary fan-out loop; the
                // ledger/slot must already reflect the assignment so a
                // completion that races back is attributed by hash.
                let task_hash = compute_task_hash(&binary);
                self.commit_assignment(
                    worker_idx,
                    binary.clone(),
                    task_hash,
                    estimated_usage.clone(),
                );

                assignments_per_secondary
                    .entry(secondary_id)
                    .or_default()
                    .push((local_worker_id, binary, estimated_usage));
            }
        }

        // Send InitialAssignment to EVERY connected secondary, even
        // those that got no initial work — `wait_for_setup` on the
        // secondary side is gated on PeerInfo + InitialAssignment +
        // TransferComplete, so omitting InitialAssignment for an
        // empty-batch secondary leaves it permanently stuck waiting
        // for a message that never arrives. Symptom: a 4-secondary
        // run with a single phase-3 item logs `assigned=0 remaining=1`,
        // primary sends InitialAssignment only to the lucky secondary,
        // the other 3 hang in wait_for_setup until the heartbeat-
        // monitor declares them dead 15s later.
        //
        // For empty-batch secondaries the payload's zip_files,
        // workers_ready, and staged_files are all empty vectors — the
        // secondary just enters process_tasks and starts requesting
        // work normally. The PrimaryConfig flags
        // (`pre_staged_mode`, `uses_file_based_items`) still need to
        // be carried so the secondary's dispatch behaviour matches
        // the primary's.
        for secondary_id in &secondary_ids {
            let empty_assignments: Vec<(u32, TaskInfo<I>, ResourceMap)> = Vec::new();
            let assignments = assignments_per_secondary
                .get(secondary_id)
                .unwrap_or(&empty_assignments);
            let zip_files = if assignments.is_empty() {
                Vec::new()
            } else {
                vec![ZipFileAssignment {
                    zip_name: String::new(),
                    binaries: assignments
                        .iter()
                        .map(|(_, binary, _)| ZipBinaryEntry {
                            local_path: self.config.wire_local_path(binary),
                            binary_info: binary_to_distributed(binary),
                            hash: compute_task_hash(binary),
                        })
                        .collect(),
                }]
            };

            let workers_ready: Vec<WorkerReadyInfo> = assignments
                .iter()
                .map(|(worker_id, _, est_res)| WorkerReadyInfo {
                    worker_id: *worker_id,
                    resource_budgets: est_res
                        .iter()
                        .map(|(kind, amount)| dynrunner_core::ResourceAmount {
                            kind: kind.clone(),
                            amount,
                        })
                        .collect(),
                })
                .collect();

            let staged_files = staged_per_secondary
                .remove(secondary_id)
                .unwrap_or_default();
            let msg = DistributedMessage::InitialAssignment {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: secondary_id.clone(),
                zip_files,
                workers_ready,
                staged_files,
                pre_staged_mode: self.config.source_pre_staged_root.is_some(),
                uses_file_based_items: self.config.uses_file_based_items,
            };
            // A failed send here is a CLUSTER COLLAPSE, not a transient: the
            // destination is a concrete `Secondary(id)` (always resolvable —
            // it carries its own host), so the only way `send_to` errors is
            // the mesh-pump's egress receiver being dropped (the Node winding
            // down / the mesh gone). That is the egress-side twin of the
            // operational loop's `recv() -> None` collapse criterion — the
            // SAME mesh-pump, observed from the send side. Rather than
            // `?`-escape as a raw `RunError::Other` (which bypasses the
            // strand-classification that runs only AFTER assignment, in
            // `run_operational_and_finalize`), surface the typed collapse so
            // the caller routes it into the SOLE classification site
            // (`finalize_terminal_accounting`): the full pool is stranded, the
            // honest `RunAborted` terminal is broadcast, and the run returns
            // `ClusterCollapsed` — identical to a secondary dying mid-loop.
            // Short-circuit the fan-out: no further sends, no
            // `originate_task_assigned` (no replicated `InFlight` to
            // compensate), no `Operational` transition.
            if self
                .send_to(
                    Destination::Secondary(PeerId::from(secondary_id.clone())),
                    msg,
                )
                .await
                .is_err()
            {
                tracing::error!(
                    secondary_id = %secondary_id,
                    "initial-assignment send failed: mesh-pump gone (cluster collapse); \
                     routing through the strand-classification finalize tail"
                );
                return Ok(InitialAssignmentOutcome::ClusterCollapsed);
            }

            // Send succeeded: originate the CRDT `Pending → InFlight`
            // transition for each task in this secondary's initial
            // batch (the single origination point, shared with the live
            // dispatch sites). After the send so a delivery failure —
            // which `?`-aborts initial assignment — never leaves a
            // replicated `InFlight` to compensate. Collect the
            // (hash, worker) pairs owned BEFORE the mut-self call so the
            // immutable borrow of `assignments_per_secondary` is dropped.
            let assigned_inflight: Vec<(String, u32)> = assignments
                .iter()
                .map(|(worker_id, binary, _)| (compute_task_hash(binary), *worker_id))
                .collect();
            for (task_hash, worker_id) in assigned_inflight {
                // Operator-facing per-task INFO, same shape/fields as the
                // two live dispatch sites (`lifecycle/dispatch.rs`,
                // `task/request.rs`): one line per task naming which
                // secondary/worker took it. The aggregate "initial
                // assignment complete" emit below carries only the TOTAL,
                // so without this the initial batch records no per-task
                // "assigned" line — breaking the assigned-vs-terminal set
                // forensics (obs-3) that diffs each "task assigned" hash
                // against its terminal report.
                tracing::info!(
                    secondary = %secondary_id,
                    worker_id,
                    task_hash = %task_hash,
                    "task assigned"
                );
                self.originate_task_assigned(task_hash, secondary_id.clone(), worker_id)
                    .await;
            }
        }

        // Transition all to Operational. At the same moment, reset
        // each secondary's keepalive clock so the heartbeat-monitor's
        // first deadline check after operational-loop start measures
        // "time since the secondary became operational", not "time
        // since welcome arrived" (which can include 30+s of
        // container startup + SSH tunnel + handshake on slow
        // clusters). Without this reset a secondary whose setup
        // took longer than `keepalive_miss_threshold *
        // keepalive_interval` would be falsely declared dead on the
        // first tick, even though its own keepalive sender (which
        // only spins up post-`wait_for_setup`) was about to start
        // ticking.
        for secondary_id in &secondary_ids {
            if let Some(state) = self.secondaries.remove(secondary_id) {
                let new_state = match state {
                    SecondaryConnectionState::InitialAssigning(conn) => {
                        SecondaryConnectionState::Operational(conn.assignments_sent())
                    }
                    other => other,
                };
                self.secondaries.insert(secondary_id.clone(), new_state);
            }
            self.seed_keepalive(secondary_id);
        }

        let assigned: usize = assignments_per_secondary.values().map(|v| v.len()).sum();
        // Phase-preparation / task-spawning important event: the initial
        // per-secondary assignment has placed `assigned` tasks across the
        // fleet (`remaining` still queued for the operational loop's
        // TaskRequest cycle). This is the single point at which initial
        // tasks have been spawned/assigned for the run, so it carries the
        // count at the importance target — the dual-sink surfaces it on
        // stdio under `--important-stdio-only`. Structured fields, not
        // prose (mirrors `retry_bucket`'s `count`-bearing emit). One emit
        // with the TOTAL, after the per-secondary fan-out — never inside
        // the per-recipient loop.
        tracing::info!(
            target: super::important_events::IMPORTANT_TARGET,
            assigned,
            remaining = self.pool().len(),
            "initial assignment complete"
        );

        Ok(InitialAssignmentOutcome::Completed)
    }

    // ── Phase 6: Transfer Complete ──
}

/// The terminal outcome of [`PrimaryCoordinator::perform_initial_assignment`].
///
/// Single concern: tell the caller whether the initial per-secondary
/// assignment completed normally or hit a cluster-collapse send failure
/// (the mesh-pump gone), so the caller can route a collapse into the
/// SAME strand-classification path the operational loop uses instead of
/// `?`-escaping as a raw `RunError::Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InitialAssignmentOutcome {
    /// Every connected secondary received its `InitialAssignment`; the
    /// pre-loop chain continues normally (transfer-complete, op-loop).
    Completed,
    /// A send to a secondary failed because the mesh-pump's egress
    /// receiver was dropped — the cluster is collapsing. The caller must
    /// skip straight to the strand-classification finalize tail.
    ClusterCollapsed,
}
