use std::collections::HashMap;

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, PeerId, PeerTransport, StagedFileRecord, WorkerReadyInfo,
    ZipBinaryEntry, ZipFileAssignment,
};
use dynrunner_scheduler_api::{AssignmentDecision, ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};
use super::{PrimaryCoordinator, RemoteWorkerState};

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
    pub(super) async fn perform_initial_assignment(&mut self) -> Result<(), String> {
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

        // Build self.workers in ROUND-ROBIN order across secondaries
        // (interleaved) rather than contiguous-per-secondary, so the
        // downstream `for worker_idx in 0..self.workers.len()` loop —
        // which assigns one task per worker in order — distributes
        // initial assignments one-per-secondary-per-round. Pre-fix
        // (contiguous-per-secondary) meant tasks < total_workers
        // would fill the early secondaries fully before any task
        // reached the later ones; with N=4 secondaries × 2 workers
        // and 4 tasks, two secondaries got 2 tasks each and two got
        // none in the initial batch. The operational loop's
        // TaskRequest cycle self-balances post-initial, but the
        // startup transient was visible in small-task-set runs.
        //
        // Secondary iteration order is now NAME-SORTED (was
        // HashMap-random) so initial assignment is deterministic
        // across runs — important for repro of subtle scheduler
        // behaviour and for log-diffing.
        let mut secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        secondary_ids.sort();

        // Per-secondary metadata snapshot (num_workers + max_res),
        // pulled up here so the round-robin loop below holds no
        // overlapping borrows on `self`.
        struct SecondaryMeta {
            id: String,
            num_workers: u32,
            max_res: dynrunner_core::ResourceMap,
        }
        let secondary_meta: Vec<SecondaryMeta> = secondary_ids
            .iter()
            .map(|sid| {
                let state = self.secondaries.get(sid).unwrap();
                let ram_bytes = state
                    .resources()
                    .iter()
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                SecondaryMeta {
                    id: sid.clone(),
                    num_workers: state.num_workers(),
                    max_res: dynrunner_core::ResourceMap::from([(
                        dynrunner_core::ResourceKind::memory(),
                        ram_bytes,
                    )]),
                }
            })
            .collect();

        let max_workers_per_secondary = secondary_meta
            .iter()
            .map(|m| m.num_workers)
            .max()
            .unwrap_or(0);

        // Heterogeneous worker counts: a secondary that runs out of
        // workers in earlier rounds is just skipped — `if round <
        // meta.num_workers` keeps the round-robin tight to the
        // remaining set. Example with sec_a=3, sec_b=2, sec_c=4:
        //   round 0: [a/w0, b/w0, c/w0]
        //   round 1: [a/w1, b/w1, c/w1]
        //   round 2: [a/w2,       c/w2]     (b skipped)
        //   round 3: [             c/w3]    (a, b skipped)
        // Resulting self.workers preserves the "earliest available
        // worker per round" semantic without bunching the
        // higher-count secondary's tail at the end.
        let mut global_worker_id: u32 = 0;
        for round in 0..max_workers_per_secondary {
            for meta in &secondary_meta {
                if round < meta.num_workers {
                    let budget = self.scheduler.initial_budget(round, &meta.max_res);
                    self.workers.push(RemoteWorkerState {
                        worker_id: global_worker_id,
                        secondary_id: meta.id.clone(),
                        resource_budgets: budget,
                        state: super::SlotState::Idle,
                    });
                    global_worker_id += 1;
                }
            }
        }

        // Perform initial assignment for each worker. The pool is
        // pre-sorted by `run()` (size DESC) and bucketed by
        // `(phase, type, affinity)`; per-worker visibility is the
        // `view_for_worker` slice the scheduler chooses from.
        let mut assignments_per_secondary: HashMap<String, Vec<(u32, TaskInfo<I>, ResourceMap)>> =
            HashMap::new();
        let mut total_assigned_resources = ResourceMap::new();

        for worker_idx in 0..self.workers.len() {
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
            self.send_to(
                Destination::Secondary(PeerId::from(secondary_id.clone())),
                msg,
            )
            .await?;

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
        //
        // Defer-mode asymmetry (scope boundary): this event fires ONLY on
        // the non-defer, submitter-side initial-assignment path. In
        // setup-defer mode (`--source-already-staged` /
        // `required_setup_on_promote`) `perform_initial_assignment` is not
        // called at all — `emit_setup_defer_handshake` runs instead, and the
        // real task discovery + assignment happens post-relocation on the
        // promoted primary. That promoted-side spawning is NOT surfaced
        // here, by design: this is a submitter-local important-stdio site,
        // and the promoted primary is constructed by `Process` on the
        // promotion event (a separate coordinator) whose own assignment is
        // out of this emitter's reach. So under setup-defer the operator sees no
        // "initial assignment complete" from the submitter — the boundary
        // is honest, not a gap to paper over here.
        tracing::info!(
            target: super::important_events::IMPORTANT_TARGET,
            assigned,
            remaining = self.pool().len(),
            "initial assignment complete"
        );

        Ok(())
    }

    /// Setup-defer mode handshake (replaces `perform_initial_assignment`
    /// when the submitter has chosen to delegate task discovery + ledger
    /// seed to the chosen secondary). Emits one degenerate
    /// `InitialAssignment` per connected secondary — empty `zip_files`,
    /// empty `workers_ready`, empty `staged_files`, `pre_staged_mode =
    /// true`, `uses_file_based_items` carried through — so each
    /// secondary's `wait_for_setup` loop sees the expected
    /// PeerInfo + InitialAssignment + TransferComplete triple and falls
    /// through to operational mode. The `pre_staged_mode: true` field on
    /// that `InitialAssignment` is the discriminator (the secondary's
    /// `setup_discovery_pending` latch) that flips the post-handshake
    /// secondary into setup-pending mode instead of the usual
    /// hydrate-from-cluster-state path.
    ///
    /// Also performs the InitialAssigning → Operational typestate
    /// transition and seeds the per-secondary keepalive clock — the
    /// same bookkeeping `perform_initial_assignment` does on the legacy
    /// path. Without those, the local primary's heartbeat monitor
    /// would never observe its secondaries as `Operational` (it gates
    /// dead-detection on that variant) and post-demote keepalive
    /// timestamps would start unset.
    ///
    /// No pool / worker allocation: in setup-defer mode the local
    /// primary's `all_binaries` is empty, no `self.workers` are built,
    /// and assignment decisions are deferred to the promoted secondary
    /// after it broadcasts its `TaskAdded` mutations.
    pub(super) async fn emit_setup_defer_handshake(&mut self) -> Result<(), String> {
        tracing::info!("emitting setup-defer handshake (empty InitialAssignment per secondary)");

        let mut secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        secondary_ids.sort();

        for secondary_id in &secondary_ids {
            let msg = DistributedMessage::InitialAssignment {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: secondary_id.clone(),
                zip_files: Vec::new(),
                workers_ready: Vec::new(),
                staged_files: Vec::new(),
                pre_staged_mode: true,
                uses_file_based_items: self.config.uses_file_based_items,
            };
            self.send_to(
                Destination::Secondary(PeerId::from(secondary_id.clone())),
                msg,
            )
            .await?;
        }

        // InitialAssigning → Operational + seed keepalive, identical to
        // the legacy `perform_initial_assignment` tail. The connection
        // states must reach Operational so the heartbeat monitor (which
        // gates dead-detection on this variant) can observe per-secondary
        // liveness from this point onward.
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

        Ok(())
    }

    // ── Phase 6: Transfer Complete ──
}
