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

        // BATCH-PAYLOAD ELIGIBILITY: only members this primary itself
        // walked through the live bring-up handshake to `InitialAssigning`
        // (welcome → cert → peer-listed → peers-ready) are PROVABLY parked
        // in `wait_for_setup` awaiting THIS batch — the only receiver state
        // whose setup loop consumes an `InitialAssignment` payload. A
        // member whose typestate was RECONSTRUCTED from the replicated
        // ledger (`reconstruct_secondaries_from_cluster_state`'s
        // metadata-only `Operational` seed — the promotion/relocate path)
        // is unprovable: an OPERATIONAL survivor's frame router has no
        // `InitialAssignment` arm (the frame debug-drops unhandled), so a
        // task committed onto it sits replicated-`InFlight` with no holder
        // until the reconciliation probe's ~600s denial recovers it.
        // Committing payloads on that unprovable assumption was the
        // post-failover wedge; the failure directions are asymmetric, so
        // when unsure we must NOT commit:
        //   * an operational member skipped here is dispatched through the
        //     OPERATIONAL path (`dispatch_to_idle_workers` /
        //     `handle_task_request`) behind the real gates —
        //     mesh-confirmation, backpressure backoff, already-held
        //     recognition — at its `MeshReady` confirmation edge;
        //   * a setup-parked member skipped here still gets its gate
        //     RELEASED by the empty fan below (unchanged), goes
        //     operational, reports `MeshReady`, and pulls/receives work
        //     the same way. The duplicate-welcome trio re-serve
        //     (`re_serve_setup_on_duplicate_welcome`) remains the
        //     retransmit backstop.
        let batch_eligible: std::collections::HashSet<String> = self
            .secondaries
            .iter()
            .filter(|(_, s)| matches!(s, SecondaryConnectionState::InitialAssigning(_)))
            .map(|(id, _)| id.clone())
            .collect();

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
            // Payload-eligibility membership gate (see `batch_eligible`
            // above): never commit a task onto a member that won't consume
            // the `InitialAssignment` it would ride.
            if !batch_eligible.contains(&self.workers[worker_idx].secondary_id) {
                continue;
            }
            // #519 per-decision bias: runs only for a worker that reaches
            // view-construction (a real dispatch decision), after the
            // payload-eligibility skip — a skipped worker must not advance
            // the counter or consume a toggle flip. The call folds the
            // decision-count bump + every-W gate re-eval + toggle flip;
            // returns `false` while disarmed (pre-#519 view).
            let prefer_dependency = self.prefer_dependency_for_decision();
            let worker_info = self.workers[worker_idx].budget_info();
            let max_res = self.workers[worker_idx].resource_budgets.clone();
            // The ONE dispatch-shape view pipeline (soft preferred-
            // secondaries tie-break → strict gate → per-type cap filter →
            // the graceful-abort freeze), shared with the two operational
            // dispatch sites. This call previously re-spelled the
            // soft-predicate + cap-filter steps inline (duplicated logic);
            // routing through the single owner is behaviour-identical here
            // — the strict preferred-secondaries gate is active only in
            // OOM-bucket `single_worker_mode`, which
            // `hydrate_from_cluster_state` resets to `false` before this
            // pre-loop site can run — and it puts the initial assignment
            // behind the SAME graceful-abort scheduling gate as every
            // other dispatch path (load-bearing on the promoted-primary
            // path: a freeze inherited via the promotion snapshot must
            // also stop the post-promotion initial assignment).
            let view = self.dispatch_view_for_worker(worker_idx, prefer_dependency);
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
                // Owned consumption ticket — the view's last use,
                // releasing the pool borrow for the take below.
                let selection = view.select(binary_index);
                let binary = self.pool_mut().take_selected(selection);
                total_assigned_resources.add(&estimated_usage);

                let secondary_id = self.workers[worker_idx].secondary_id.clone();
                // Secondary-local worker id (the wire `worker_id`).
                let local_worker_id = self.local_worker_id_in_secondary(worker_idx);

                // Type-slot reserve + slot `Idle -> Assigned{task_hash}`
                // + ledger insert, committed together at the moment of
                // initial dispatch. The wire `InitialAssignment` is
                // built+sent below in the per-secondary fan-out loop; the
                // ledger/slot must already reflect the assignment so a
                // completion that races back is attributed by hash. Workers
                // are all-idle by construction here (cold roster), so the
                // enforced idle-guard (#517) refuses only on a broken
                // invariant: requeue + un-count + skip rather than dispatch
                // a task the model can't track (the silent-overwrite
                // backstop).
                let task_hash = compute_task_hash(&binary);
                if !self.commit_assignment(
                    worker_idx,
                    binary.clone(),
                    task_hash,
                    estimated_usage.clone(),
                ) {
                    self.pool_mut().requeue(binary);
                    total_assigned_resources.sub(&estimated_usage);
                    continue;
                }

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
                .send_initial_assignment_to(secondary_id, zip_files, workers_ready, staged_files)
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
            self.mark_member_operational(secondary_id);
        }

        let assigned: usize = assignments_per_secondary.values().map(|v| v.len()).sum();
        let remaining = self.pool().len();
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
            remaining,
            "initial assignment complete"
        );

        // The pool still holds work the batch did NOT place — on the
        // promoted path that is the whole inherited pool (no member was
        // batch-eligible), on the live path the over-capacity remainder.
        // EMIT one `TasksAdded` so the operational dispatch recheck
        // (`dispatch_to_idle_workers`, behind the mesh-confirmation /
        // backoff / already-held gates) owns it from here: the pre-loop
        // `wait_for_mesh_ready` in-wait servicing or the operational-loop
        // entry sweep drains the bus, and each member's `MeshReady`
        // confirmation edge fires its own wakeup as it lands. Decoupled
        // emit, never a direct dispatch call (the dispatch-decoupling
        // law). Emitted AFTER the fan-out above, so a co-located member's
        // gate-release trio is queued on its FIFO loopback BEFORE any
        // recheck can push a `TaskAssignment` at it.
        if remaining > 0 {
            self.cluster_state
                .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
        }

        Ok(InitialAssignmentOutcome::Completed)
    }

    /// The SOLE per-member `InitialAssignment` construction + send site,
    /// shared by the run-start batch fan-out
    /// ([`Self::perform_initial_assignment`]) and the mid-run incremental
    /// serve (`peer_setup::serve_setup_on_cert_exchange`'s post-run-start
    /// variant, which passes empty payloads — a mid-run joiner holds no
    /// pre-assigned work and pulls via `TaskRequest` like every member
    /// post-start). The `PrimaryConfig` dispatch flags (`pre_staged_mode` /
    /// `uses_file_based_items`) are stamped HERE so every recipient's
    /// dispatch resolver agrees with the primary regardless of which path
    /// served it.
    ///
    /// The `Err` arm is uniformly the mesh-pump-gone collapse (a directed
    /// `Destination::Secondary(id)` send has no other error mode — see the
    /// batch call site's comment); each caller chooses its own collapse
    /// routing.
    pub(super) async fn send_initial_assignment_to(
        &mut self,
        secondary_id: &str,
        zip_files: Vec<ZipFileAssignment<I>>,
        workers_ready: Vec<WorkerReadyInfo>,
        staged_files: Vec<StagedFileRecord>,
    ) -> Result<(), String> {
        let msg = DistributedMessage::InitialAssignment {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: secondary_id.to_string(),
            zip_files,
            workers_ready,
            staged_files,
            pre_staged_mode: self.config.source_pre_staged_root.is_some(),
            uses_file_based_items: self.config.uses_file_based_items,
        };
        self.send_to(
            Destination::Secondary(PeerId::from(secondary_id.to_string())),
            msg,
        )
        .await
    }

    /// The ONE per-member operational typestate walk:
    /// `InitialAssigning → Operational` ("its `InitialAssignment` has
    /// been sent"), plus the keepalive re-seed. No-op state-wise from
    /// any other state; the keepalive seed always runs (matching the
    /// historical batch, which seeded every CRDT-known id whether or
    /// not a connection typestate existed for it).
    pub(super) fn mark_member_operational(&mut self, secondary_id: &str) {
        if let Some(state) = self.secondaries.remove(secondary_id) {
            let new_state = match state {
                SecondaryConnectionState::InitialAssigning(conn) => {
                    SecondaryConnectionState::Operational(conn.assignments_sent())
                }
                other => other,
            };
            self.secondaries
                .insert(secondary_id.to_string(), new_state);
        }
        self.seed_keepalive(secondary_id);
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
