use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId, RemovalCause,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Apply a replicated `DistributedMessage::ClusterMutation` batch.
    ///
    /// Single concern: keep the demoted primary's CRDT mirror — and the
    /// accounting sets the operational loop's exit-counter check
    /// reads — converged with the cluster's view, even when the live
    /// primary authority has handed off to a promoted secondary.
    ///
    /// Pre-fix the primary's `dispatch_message` had no arm for
    /// `MessageType::ClusterMutation`: any mutation broadcast addressed
    /// at the demoted primary fell through the catch-all. The
    /// operational loop's `completed + failed >= total` exit check
    /// reads `self.completed_tasks` / `self.failed_tasks`, which on a
    /// demoted primary are fed only by direct `TaskComplete` /
    /// `TaskFailed` forwards reaching the local primary's transport.
    /// Cross-secondary completions on the new primary's pool never
    /// arrived as direct forwards (the new primary doesn't loopback
    /// peer-observed completions to the demoted primary's transport),
    /// so the counter never reached the total and the run loop sat
    /// forever — the asm-dataset-nix R2 / T3 hang.
    ///
    /// Mirrors `secondary::dispatch::apply_cluster_mutations` in shape
    /// (idempotent fan-out over a `Vec<ClusterMutation>`); diverges in
    /// that the primary additionally maintains `completed_tasks` /
    /// `failed_tasks` because those are the sets the lifecycle
    /// exit-counter reads. The CRDT idempotency on `cluster_state`
    /// makes repeated apply safe; `HashSet::insert` is idempotent on
    /// the accounting side.
    /// Open (or resume) a snapshot STREAM answering a late-joiner's /
    /// re-bootstrapping / behind peer's `RequestSnapshotStream` from the
    /// primary's authoritative ledger.
    ///
    /// Single concern: the snapshot-RPC responder on the primary side.
    /// The primary's `cluster_state` is the authoritative copy of every
    /// replicated field, so its stream is the strongest possible
    /// bootstrap source. The handler only REGISTERS the stream (one
    /// sorted key list + the small tally capture — never a ledger copy
    /// or a monolithic serialization); the packages are produced one
    /// per operational-loop wakeup by the loop's stream arm
    /// (`snapshot_streams.next_wake` → `emit_next`), so a 100 MB ledger
    /// never holds the loop.
    ///
    /// Mirrors the secondary responder exactly, and additionally
    /// originates the requester's `PeerJoined` over the canonical
    /// broadcast path so every replica learns about the joiner. The
    /// joiner's declared `is_observer` / `can_be_primary` ride the
    /// request frame and are recorded truthfully (the same role-carrying
    /// contract the secondary responder honours — `apply_peer_joined`'s
    /// observer ratchet keeps a re-bootstrapping worker from
    /// mis-upgrading to observer).
    pub(crate) async fn handle_request_snapshot_stream(&mut self, msg: DistributedMessage<I>) {
        // ANY routing stamp is accepted: the `target` is the wire
        // envelope's ingress-demux header, never request semantics. Every
        // coordinator-egress pull arrives STAMPED (`Some(Destination::
        // Primary)` from the observer's bootstrap recovery, `Some(
        // Destination::Secondary(<this host>))` from an anti-entropy pull
        // the mesh-ingress fan-fallback handed to this slot), while a raw
        // transport-level joiner request arrives un-stamped. A previous
        // `target: None` pattern here SILENTLY dropped every stamped
        // shape, so the primary never served anti-entropy / recovery
        // pulls — only a healthy primary answering the joiner's raw
        // requests masked it.
        let DistributedMessage::RequestSnapshotStream {
            target: _,
            sender_id,
            stream_id,
            resume_after,
            task_ranges,
            is_observer,
            can_be_primary,
            ..
        } = msg
        else {
            // Unreachable from the `connect.rs` msg-type dispatch; loud,
            // never a silent drop of a snapshot pull.
            tracing::error!(
                kind = ?msg.msg_type(),
                "handle_request_snapshot_stream reached with a \
                 non-RequestSnapshotStream frame; dropping (dispatch bug)"
            );
            return;
        };
        self.snapshot_streams.accept_request(
            &self.cluster_state,
            &sender_id,
            is_observer,
            &stream_id,
            resume_after.as_deref(),
            &task_ranges,
        );
        // Originate the requester's `PeerJoined` over the canonical
        // local-apply + broadcast path. The joiner declared its own
        // role + capability on the request frame; record that truth in
        // the replicated `RoleTable` (idempotent / set-once on re-apply,
        // so a duplicate from a concurrent secondary responder NoOps).
        // The requester's CURRENT membership incarnation: a frame from a
        // previously-removed id already routed through the dispatch
        // preamble's re-admission seam (which bumped the generation), so
        // this read observes the post-re-admission value; for a fresh
        // late-joiner it is 0.
        let member_gen = self.cluster_state.peer_member_gen(&sender_id);
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
            peer_id: sender_id,
            is_observer,
            can_be_primary,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
            member_gen,
        }])
        .await;
    }

    /// Test hook: synchronously drain every pending snapshot-stream wake
    /// token, emitting + sending each package — what the operational
    /// loop's stream arm does one-per-wakeup, compressed for loop-less
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

    /// Answer a peer's `RequestRunConfig` from this primary's node-local
    /// `forwarded_argv`.
    ///
    /// Single concern: the run-config-RPC responder on the primary side. It
    /// is a PURE READ-ONLY responder — it reads `self.forwarded_argv` and
    /// unicasts exactly ONE `RunConfig` back to the requester (its return
    /// address rides `sender_id`, mirroring the snapshot responder's reply
    /// edge). Unlike `handle_request_snapshot_stream`, it does NOT
    /// originate `PeerJoined`, does NOT send `SecondaryWelcome`, and never
    /// touches roster / quorum / capacity / CRDT: the run-config is a
    /// node-local launch constant, not lattice data, and answering for it
    /// is read-only peer gossip, NOT primary authority (the work-split is
    /// preserved). The SAME read-only responder lives on the secondary
    /// router so a cold-start fetch is answerable before any primary
    /// exists / promotes.
    ///
    /// A send failure is logged best-effort, exactly as the snapshot
    /// responder treats its own; the requester's bounded recv wait falls
    /// back to its own deadline.
    pub(crate) async fn handle_request_run_config(&mut self, msg: DistributedMessage<I>) {
        // ANY routing stamp is accepted — same contract as the snapshot
        // responder above: the setup backstop sends this request through
        // its egress edge, which stamps `Some(Destination::Primary)`; a
        // previous `target: None` pattern silently dropped every such
        // in-band backstop fetch.
        let DistributedMessage::RequestRunConfig {
            target: _,
            sender_id,
            ..
        } = msg
        else {
            // Unreachable from the `connect.rs` msg-type dispatch; loud,
            // never a silent drop of a run-config fetch.
            tracing::error!(
                kind = ?msg.msg_type(),
                "handle_request_run_config reached with a \
                 non-RequestRunConfig frame; dropping (dispatch bug)"
            );
            return;
        };
        let response = DistributedMessage::RunConfig {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            forwarded_argv: self.forwarded_argv.clone(),
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
                "failed to deliver RunConfig response from primary"
            );
        }
    }

    /// Push this primary's node-local `forwarded_argv` to a freshly-
    /// welcomed secondary over its EXISTING mesh connection.
    ///
    /// Single concern: the run-config DELIVERY edge on the welcome-accept
    /// path. The welcomed secondary booted with only its boot-critical CLI
    /// args; it parses the consumer's run-config (`--task`, task filters)
    /// only AFTER it is connected, so the primary unicasts the same
    /// `RunConfig` frame the `RequestRunConfig` responder serves — but
    /// proactively, the moment the secondary is recorded as connected,
    /// rather than waiting for the secondary to ask. This reuses the
    /// already-established connection (the welcome/cert handshake that
    /// precedes it is untouched); it is purely a new SEND site for the
    /// existing message.
    ///
    /// Like the responder, this is read-only peer gossip: it reads
    /// `self.forwarded_argv` and never touches roster / capacity / CRDT.
    /// A send failure is logged best-effort — the secondary's own
    /// `RequestRunConfig` fallback (and bounded recv waits) still cover the
    /// rare drop.
    pub(crate) async fn push_run_config_to(&mut self, secondary_id: String) {
        let response = DistributedMessage::RunConfig {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            forwarded_argv: self.forwarded_argv.clone(),
        };
        if let Err(e) = self
            .send_to(
                Destination::Secondary(PeerId::from(secondary_id.clone())),
                response,
            )
            .await
        {
            tracing::warn!(
                target = %secondary_id,
                error = %e,
                "failed to push RunConfig to welcomed secondary"
            );
        }
    }

    /// Anti-entropy receive: compare a peer's `StateDigest` against the
    /// primary's own and, iff the primary is somehow behind, NOTE the
    /// divergence to the disciplined PULL driver (`pull_coordinator`)
    /// instead of firing an eager immediate pull. `note_behind` is
    /// IDEMPOTENT (single-flight), so even a primary that briefly diverges
    /// initiates at most one probe→pull cycle at the cooldown rate, not one
    /// per digest. The authoritative primary is essentially never behind a
    /// follower's digest, so this is almost always a NoOp (a matching digest
    /// returns early); it exists for the freshly-promoted primary still
    /// warming its mirror. The digest beacon + `is_behind` DETECTION are
    /// unchanged; only the eager pull is replaced.
    pub(crate) async fn handle_state_digest(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::StateDigest {
            target: None,
            digest,
            ..
        } = msg
        else {
            return;
        };
        let local = self.cluster_state.digest();
        if !local.is_behind(&digest) {
            // Converged on the peer's digest — nothing to pull (the
            // steady-state authoritative-primary path).
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .note_behind(std::time::Instant::now())
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Translate ONE [`crate::pull_coordinator::PullDirective`] into the
    /// primary's `send_to` edge — the role-owned wire-touch half of the
    /// disciplined pull (FSM + selection in `pull_coordinator`; frame
    /// construction + role-typing in `pull_coordinator::pull_probe` /
    /// `pull_request`; this owns only `send_to`). The primary declares
    /// `is_observer: false`, `can_be_primary: true` on its pulls.
    pub(crate) async fn drive_pull_directive(
        &mut self,
        directive: crate::pull_coordinator::PullDirective,
    ) {
        match directive {
            crate::pull_coordinator::PullDirective::Probe => {
                let digest = self.cluster_state.digest();
                let frame = crate::pull_coordinator::pull_probe(
                    &self.config.node_id,
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
                // P1: narrow the request to the buckets divergent from the
                // chosen responder (compare the responder's piggybacked range
                // digest against our own).
                let task_ranges = crate::pull_coordinator::divergent_ranges_for_pull(
                    &self.cluster_state.tasks_range_digest(),
                    &target_range_digest,
                );
                let (dst, frame, stream_id) = crate::pull_coordinator::pull_request(
                    &self.config.node_id,
                    false,
                    true,
                    &target_id,
                    target_is_observer,
                    task_ranges,
                    &mut self.inbound_snapshots,
                    timestamp_now(),
                );
                if self.send_to(dst, frame).await.is_ok() {
                    self.pull_coordinator.note_pull_stream(&stream_id);
                }
            }
        }
    }

    /// Answer an inbound `PullProbe`: reply with the primary's inbox depth
    /// + the responder-side `ahead` bit. Direct-only reply.
    pub(crate) async fn handle_pull_probe(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::PullProbe {
            sender_id, digest, ..
        } = msg
        else {
            return;
        };
        let local = self.cluster_state.digest();
        let ahead = crate::pull_coordinator::probe_reply_ahead(&local, &digest);
        // P1: piggyback this node's task-ledger range digest so the prober
        // computes the divergent buckets without a second round-trip.
        let range_digest = self.cluster_state.tasks_range_digest();
        let (dst, frame) = crate::pull_coordinator::pull_probe_reply(
            &self.config.node_id,
            timestamp_now(),
            &sender_id,
            false,
            self.inbox.depth() as u64,
            ahead,
            range_digest,
        );
        let _ = self.send_to(dst, frame).await;
    }

    /// Record an inbound `PullProbeReply` into the pull driver.
    pub(crate) async fn handle_pull_probe_reply(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::PullProbeReply {
            sender_id,
            requester,
            inbox_size,
            ahead,
            range_digest,
            ..
        } = msg
        else {
            return;
        };
        if requester != self.config.node_id {
            return;
        }
        let reply = crate::pull_coordinator::ProbeReply {
            responder_id: &sender_id,
            responder_is_observer: false,
            inbox_size,
            ahead,
            range_digest,
        };
        if let Some(directive) = self
            .pull_coordinator
            .on_probe_reply(std::time::Instant::now(), &reply)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Record an inbound `PullFail` and fall to the next pull target.
    pub(crate) async fn handle_pull_fail(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::PullFail {
            requester,
            stream_id,
            ..
        } = msg
        else {
            return;
        };
        if requester != self.config.node_id {
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .on_fail(std::time::Instant::now(), &stream_id)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Settle the primary's LOCAL execution caches with a terminal fact
    /// the replicated ledger has accepted for `task_hash` — the single
    /// CRDT-terminal convergence path shared by the received-mutation
    /// ingest below and the inherited-slot reconciliation's terminal veto
    /// (`handle_task_request`).
    ///
    /// A terminal normally arrives as a WIRE frame (`TaskComplete` /
    /// `TaskFailed`), whose handler frees the holding slot and runs the
    /// phase cascade itself. A terminal that arrives ONLY through the
    /// replicated ledger (a peer's `TaskCompleted`/`TaskFailed`
    /// ClusterMutation — e.g. a concurrent/deposed primary's broadcast —
    /// or snapshot-restore residue) used to leave every local cache
    /// stale: the slot stayed phantom-busy, the in-flight ledger entry
    /// survived, the pool's phase counter never drained, and the
    /// accounting mirrors never recorded the outcome. The
    /// run_20260610_221140 requeue-vs-complete race is the production
    /// blast: the stale inherited slot was later "reconciled" and the
    /// COMPLETED task re-executed.
    ///
    /// What settles, in order:
    /// 1. The local execution residue — EITHER the holding slot + ledger
    ///    entry + type slot ([`Self::free_slot_on_terminal_by_hash`]) OR
    ///    a queued pool copy left by an earlier failover-recovery requeue
    ///    ([`Self::reclaim_requeued_on_terminal`]).
    /// 2. The accounting mirrors (`completed_tasks` / `failed_tasks`),
    ///    classified exactly as the hydrate-time projection: `Completed`/
    ///    `SkippedAlreadyDone`/`Unfulfillable`/`InvalidTask` → completed
    ///    (counter), `Failed { kind }` → failed (unless already
    ///    completed — completion supersedes). Unconditional + idempotent.
    /// 3. IFF step 1 found residue: the phase cascade — `note_item_completed`
    ///    (with the task id, resolving dependents) for a success-like
    ///    terminal, `note_item_failed` for a failure-like one — plus the
    ///    decoupled `TasksAdded` emit (a slot freed / a phase may have
    ///    advanced). Gating the cascade on found-residue keeps a
    ///    re-delivered or higher-version re-failure apply (B1) from
    ///    double-firing the phase machine.
    ///
    /// Returns `true` iff step 1 found residue (the caller's signal that
    /// the terminal was locally accounted by THIS call).
    pub(crate) async fn settle_local_state_on_crdt_terminal(
        &mut self,
        task_hash: &str,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> bool {
        use crate::cluster_state::TaskState;
        // Classify off the post-apply authoritative state. (Owned copies so
        // the `&self.cluster_state` borrow drops before the mutations.)
        // `success_like` drives the cascade (dependents resolve only on a
        // genuinely-done prereq); `counter_completed` follows the
        // hydrate-time projection (`Unfulfillable`/`InvalidTask` count in
        // the completed mirror — disjoint from `failed_tasks`).
        let (counter_completed, failed_kind, success_like) =
            match self.cluster_state.task_view(task_hash) {
                Some(crate::cluster_state::TaskView::Live(state)) => match state {
                    // `SetupCompleted` is success-like for the cascade (its
                    // dependents resolve) and goes in the completed mirror
                    // for dep pre-resolution — disjoint from `failed_tasks`,
                    // exactly like `Completed`/`SkippedAlreadyDone`. (The
                    // separate `setup_succeeded` OUTCOME bucket is derived
                    // from the CRDT; this node-local mirror is a dep-
                    // resolution set, not the success count.)
                    TaskState::Completed { .. }
                    | TaskState::SkippedAlreadyDone { .. }
                    | TaskState::SetupCompleted { .. } => (true, None, true),
                    TaskState::Failed { kind, .. } => (false, Some(kind.clone()), false),
                    TaskState::Unfulfillable { .. } | TaskState::InvalidTask { .. } => {
                        (true, None, false)
                    }
                    // Not terminal: nothing to settle.
                    _ => return false,
                },
                // A SETTLED (spilled) hash classifies off its slim index
                // class — the identical projection as the fat arms above.
                // Load-bearing: the spill sweep may evict a terminal
                // between the apply that fired this event and this
                // dispatcher-side settle; reading `task_state` here would
                // see `None` and LEAK the residue (slot / requeue copy).
                Some(crate::cluster_state::TaskView::Settled(entry)) => {
                    use crate::cluster_state::SettledClass;
                    match &entry.class {
                        SettledClass::Completed
                        | SettledClass::SkippedAlreadyDone
                        | SettledClass::SetupCompleted => (true, None, true),
                        SettledClass::FailedFinal(kind) => (false, Some(kind.clone()), false),
                        SettledClass::InvalidTask => (true, None, false),
                    }
                }
                // Unknown hash: nothing to settle.
                None => return false,
            };

        // 1. Local execution residue: a held slot, or a queued requeue copy.
        let residue: Option<(dynrunner_core::PhaseId, String)> = self
            .free_slot_on_terminal_by_hash(task_hash)
            .map(|entry| (entry.phase, entry.task.task_id.clone()))
            .or_else(|| {
                self.reclaim_requeued_on_terminal(task_hash)
                    .map(|t| (t.phase_id.clone(), t.task_id.clone()))
            });

        // 2. Accounting mirrors (idempotent; completion supersedes failure,
        // mirroring `handle_task_complete` / the hydrate projection).
        if counter_completed {
            self.failed_tasks.remove(task_hash);
            self.completed_tasks.insert(task_hash.to_string());
        } else if let Some(kind) = failed_kind.clone()
            && !self.completed_tasks.contains(task_hash)
        {
            self.failed_tasks.insert(task_hash.to_string(), kind);
        }
        // Pre-start fence A side-map drop (#530a): a CRDT-terminal settle is
        // still a terminal — the hash is done from the run's perspective and
        // no further dispatch will fence on it. Symmetric with the wire-side
        // drops in `handle_task_complete` / `handle_task_failed` so the side-
        // map cannot outlive the task on any settle path.
        self.drop_supplanted_holder(task_hash);

        // 3. Phase cascade — only when THIS call actually released local
        // residue (otherwise the wire handler already ran it).
        let Some((phase, task_id)) = residue else {
            return false;
        };
        if success_like {
            self.note_item_completed(&phase, Some(task_id.as_str()), command_rx)
                .await;
        } else {
            // Failure-like terminal. A `Failed { kind }` forwards the kind
            // so the pool records the retry-pending failure marker — the
            // mirror-path twin of the wire handler's routing (a relayed
            // terminal must un-wedge blocked dependents exactly like a
            // directly-delivered one). The `Unfulfillable` / `InvalidTask`
            // states surface `failed_kind = None` here, which keeps the
            // legacy dormancy: their dependents stay blocked for the
            // operator-resolvable reinject path.
            self.note_item_failed(
                &phase,
                Some(task_id.as_str()),
                failed_kind.as_ref(),
                command_rx,
            )
            .await;
        }
        // A slot freed / a phase may have advanced: decoupled recheck emit,
        // never a direct dispatch call (the dispatch-decoupling law).
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        true
    }

    /// Ingest one anti-entropy `SnapshotStreamPackage` on the primary.
    ///
    /// Single concern: the snapshot-restore edge of the primary's
    /// anti-entropy receive side. [`Self::handle_state_digest`] REQUESTS a
    /// stream whenever a peer's digest proves this node behind (a higher
    /// `primary_epoch`, a latched run verdict, missing task terminals, …);
    /// this arm ingests each package incrementally. Pre-fix the primary's
    /// `dispatch_message` had NO arm for the snapshot reply — the reply
    /// fell through the unhandled-type catch-all, so the pull could never
    /// converge: a DEPOSED primary starved of direct `PrimaryChanged`
    /// announcements (the dead-leg topology) kept authoring forever even
    /// when a peer's digest told it the cluster had moved on (the
    /// run_20260610_221140 zombie split-brain).
    ///
    /// Each package is a PARTIAL snapshot and `restore` is the idempotent
    /// lattice merge, so packages interleave safely with live mutation
    /// broadcasts (no consistent cut needed): the head package adopts a
    /// higher-epoch `(current_primary, primary_epoch)` register (firing
    /// the role-change hooks — the BUG-6 displaced hook is what demotes a
    /// zombie out of `run_consuming`) and latches the sticky
    /// `run_complete`/`run_aborted` verdicts; the task packages
    /// union-merge the ledger bulk. Pool/slot coherence for restored task
    /// terminals is NOT re-derived here wholesale — the live convergence
    /// seams (`settle_local_state_on_crdt_terminal` via the mutation
    /// ingest + the inherited-slot terminal veto) cover the per-task
    /// residue, and a restore that deposes this primary drops the whole
    /// pipeline anyway.
    ///
    /// ANY routing stamp is accepted (the stamp is the ingress-demux
    /// header, never reply semantics — the same never-drop-on-stamp rule
    /// as the request handler). A malformed package is WARN-dropped
    /// WITHOUT advancing the resume cursor, so the next digest round's
    /// re-pull resumes from before the bad span.
    pub(crate) fn handle_snapshot_stream_package(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::SnapshotStreamPackage {
            target: _,
            sender_id,
            stream_id,
            cursor,
            payload,
            done,
            ..
        } = msg
        else {
            return;
        };
        match crate::cluster_state::decode_stream_payload::<I>(&payload) {
            Ok(snap) => {
                self.cluster_state.restore(snap);
                self.inbound_snapshots
                    .note_package(&sender_id, &stream_id, cursor.as_deref(), done);
                if done {
                    tracing::info!(
                        peer = %sender_id,
                        stream_id = %stream_id,
                        primary_epoch = self.cluster_state.primary_epoch(),
                        run_aborted = self.cluster_state.run_aborted().is_some(),
                        "anti-entropy: snapshot stream pulled from peer fully restored"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    peer = %sender_id,
                    stream_id = %stream_id,
                    error = %e,
                    "SnapshotStreamPackage decode failed on the primary's \
                     anti-entropy sink; dropped a malformed package (the \
                     next digest round re-pulls from the last good cursor)"
                );
            }
        }
        // End the disciplined pull's in-flight cycle on the terminal package
        // (whether or not it decoded) so the driver returns to Idle: the
        // primary goes quiescent once converged, and re-probes on the next
        // divergence rather than after the rebalance. A NoOp for a
        // bootstrap-stream `done`.
        if done {
            self.pull_coordinator.on_pull_done(&stream_id);
        }
    }

    pub(crate) async fn handle_cluster_mutation(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        if let DistributedMessage::ClusterMutation {
            target: None,
            mutations,
            ..
        } = msg
        {
            // Note whether any ledger-growing mutation rides in this
            // batch BEFORE moving the mutations into apply, so the
            // post-apply `total_tasks` refresh below absorbs runtime
            // task injection (`TaskAdded` / `TasksSpawned`) a peer
            // originated. Only those two variants grow the ledger;
            // refreshing for the hot terminal path (TaskCompleted /
            // TaskFailed) would be a wasted read.
            let has_task_added = mutations.iter().any(|m| {
                matches!(
                    m,
                    ClusterMutation::TaskAdded { .. } | ClusterMutation::TasksSpawned { .. }
                )
            });
            // Discovery-seed surface, distinct from the runtime-spawn
            // surface above. `TaskAdded` is originated ONLY as the
            // initial ledger seed — `seed_cluster_state` (run start) —
            // always paired with a `PhaseDepsSet`, never as an
            // incremental mid-run add (that is `TasksSpawned`,
            // which auto-resumes through the pool's dep machine via
            // `newly_pending`). A `TaskAdded` therefore marks the FIRST
            // ledger growth this node's pool sees. Snapshot pre-apply,
            // act post-apply (same shape as `has_task_added`).
            let carries_discovery_task_added = mutations
                .iter()
                .any(|m| matches!(m, ClusterMutation::TaskAdded { .. }));
            // Collect any PeerJoined ids riding in the batch BEFORE
            // moving the mutations into apply. After the batch
            // applies, each joined id may have resolved a previously-
            // unknown entry in some task's `preferred_secondaries`,
            // so we forget its dedup state and re-run validation.
            // Same shape as `has_task_added`: snapshot pre-apply,
            // act post-apply.
            let joined_peer_ids: Vec<String> = mutations
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::PeerJoined { peer_id, .. } => Some(peer_id.clone()),
                    _ => None,
                })
                .collect();
            // Receive-side pool growth surface: every `TasksSpawned`
            // entry the apply rule classifies as freshly `Pending`
            // (no deps, or all deps already `Completed`) is cloned
            // into `newly_pending`. A primary that still locally owns
            // a pool (`self.pending.is_some()`) reinjects each entry
            // so the pool stays coherent with the CRDT ledger across
            // wire-received batches.
            //
            // This matters for the re-promotion path: a demoted
            // primary applying a promoted-secondary's TasksSpawned
            // broadcast keeps the post-spawn tasks dispatchable in
            // its local pool, so a later re-election (the demoted
            // primary becomes live again) finds the pool already
            // aligned with the cluster's view. Without it, re-
            // election would resurrect the pool from its pre-spawn
            // snapshot and the post-spawn tasks would never
            // dispatch on this node.
            //
            // `resumed` is not consumed here for the same reason the
            // promoted-secondary's receive path discards it — the
            // pool's own dep machinery dispatches Blocked entries on
            // the prereq's `on_item_finished` event.
            let mut resumed: Vec<TaskInfo<I>> = Vec::new();
            let mut newly_pending: Vec<TaskInfo<I>> = Vec::new();
            // Worker-roster growth surface (wire-receive twin of the
            // originator-side detection in
            // `apply_and_broadcast_cluster_mutations`): track whether THIS
            // batch genuinely applied a `SecondaryCapacity` — i.e. a
            // worker became ready and a new idle slot now exists in the
            // ledger. Read the per-mutation `ApplyOutcome` inline (the
            // set-once capacity apply returns `Applied` only on the first
            // record for an id, `NoOp` on every redundant re-emit /
            // snapshot replay), so the rebuild fires exactly once per new
            // secondary. Acted on post-loop, after the roster source
            // (`cluster_state.secondary_capacities`) has grown.
            let mut capacity_grew = false;
            // Terminal-ingest surface: every TaskCompleted/TaskFailed this
            // batch GENUINELY applied (the CRDT transitioned — a duplicate
            // delivery NoOps and is skipped) must settle the primary's
            // local execution caches (slot / in-flight ledger / pool /
            // accounting mirrors) exactly like a wire-delivered terminal.
            // Collected during the apply loop, settled after it (the
            // settle's cascade takes `&mut self`).
            let mut applied_terminal_hashes: Vec<String> = Vec::new();
            // Clean-departure reap surface (wire-receive twin of the
            // keepalive-death reaper): a peer that leaves the mesh
            // gracefully broadcasts its OWN `PeerRemoved { SelfDeparture }`,
            // which `apply_peer_removed` converges into the membership
            // ledger (Departed tombstone, peer kept Alive) — but that apply
            // does NOT touch the primary-local roster caches (`self.
            // secondaries` / `secondary_keepalives` / ...), and the
            // keepalive sweep deliberately skips a departed member, so the
            // local-cache reaper (`requeue_dead_secondary`) never fires for
            // it. Left uncaught, `peer_roster()` keeps re-listing the
            // departed peer in every `PeerInfo`, so the transport never
            // sees it drop and redials the dead address forever. Collect
            // each id whose `SelfDeparture` GENUINELY applied (a re-delivery
            // NoOps and is skipped), then evict it from the local caches
            // after the loop. Snapshot-during-apply / act-post-loop, the
            // same shape as the terminal-ingest surface above.
            let mut departed_self_ids: Vec<String> = Vec::new();
            for m in mutations {
                let is_capacity = matches!(m, ClusterMutation::SecondaryCapacity { .. });
                let terminal_hash = match &m {
                    ClusterMutation::TaskCompleted { hash, .. }
                    | ClusterMutation::TaskFailed { hash, .. } => Some(hash.clone()),
                    _ => None,
                };
                // A gracefully-departing peer's own self-authored removal.
                // ONLY `SelfDeparture` is observed from the wire here: every
                // other `PeerRemoved` cause is primary-authored (and the
                // author already reaped its local caches via
                // `requeue_dead_secondary`), and the primary's own
                // `RosterReemit` is for a peer it has already evicted.
                let departed_self_id = match &m {
                    ClusterMutation::PeerRemoved {
                        id,
                        cause: RemovalCause::SelfDeparture(_),
                        ..
                    } => Some(id.clone()),
                    _ => None,
                };
                let outcome =
                    self.cluster_state
                        .apply_with_resumed_blocked(m, &mut resumed, &mut newly_pending);
                if outcome == crate::cluster_state::ApplyOutcome::Applied {
                    if is_capacity {
                        capacity_grew = true;
                    }
                    if let Some(hash) = terminal_hash {
                        applied_terminal_hashes.push(hash);
                    }
                    if let Some(id) = departed_self_id {
                        departed_self_ids.push(id);
                    }
                }
            }
            // Settle each genuinely-applied terminal against the local
            // execution state (the run_20260610_221140 requeue-vs-complete
            // race: a CRDT-delivered terminal must free a phantom-busy
            // inherited slot / reclaim an erroneously-requeued pool copy,
            // never leave the completed work re-dispatchable).
            for hash in applied_terminal_hashes {
                self.settle_local_state_on_crdt_terminal(&hash, command_rx)
                    .await;
            }
            // Reap the local roster caches of every peer whose graceful
            // `SelfDeparture` just applied. Re-check `is_member_departed`
            // at reap time: a re-admission at a HIGHER generation (a
            // `PeerJoined` later in THIS batch, or one this node had
            // already converged) supersedes the tombstone, and that
            // re-admitted peer must STAY in the roster — so it is not
            // reaped. Skip this node's own id defensively (the co-resident-
            // primary case is excluded at the leaving node's drain gate, so
            // the primary never authors a `SelfDeparture` for its own node,
            // but a stray self-removal must never evict the primary itself).
            for id in departed_self_ids {
                if id == self.config.node_id {
                    continue;
                }
                if !self.cluster_state.is_member_departed(&id) {
                    continue;
                }
                tracing::info!(
                    secondary = %id,
                    "peer departed gracefully (SelfDeparture); evicting it \
                     from the local roster caches so it drops out of the \
                     next PeerInfo and the transport stops redialing it"
                );
                self.evict_secondary_local_caches(&id);
            }
            // Pool-coherence after a ledger-growing apply. Two
            // mutually-exclusive surfaces, both gated on this node still
            // owning a pool (`self.pending.is_some()`):
            //
            //   * Discovery seed at a quiescent pool (REBUILD). A
            //     `TaskAdded` Applied AND the pool currently holds no
            //     queued dispatchable work — the pool was built before
            //     discovery seeded the ledger and is drained/empty (the
            //     pre-staged `--source-already-staged` path: phases sit
            //     `Active`-empty or `Drained`, every declared phase with
            //     zero items). `TaskAdded` does NOT feed `newly_pending` (only
            //     `TasksSpawned` does, in `apply_tasks.rs`), so a plain
            //     reinject never runs for it and the discovered tasks
            //     would stay in the CRDT ledger un-dispatchable. REBUILD
            //     the pool from the now-seeded `cluster_state` via the
            //     `hydrate_from_cluster_state` primitive — it reads the
            //     batch's just-applied `phase_deps`, classifies each
            //     discovery phase Active/Blocked, queues every
            //     freshly-`Pending` discovery task, and `cascade_drain_done`s
            //     a declared-but-empty phase to `Done` (the activated-primary
            //     semantics; no `on_phase_end` re-fires). This re-activates
            //     the drained pool — the integrated form of the same
            //     dispatch-enablement the runtime-spawn reinject below gives
            //     an ACTIVE phase. The predicate is one-shot by
            //     construction: the rebuild queues the seed, so the pool
            //     then holds dispatchable work and a re-delivered batch
            //     (idempotent `TaskAdded` NoOp) does not re-trigger.
            //
            //   * Runtime spawn into the live pool (INCREMENTAL reinject).
            //     `TasksSpawned` entries the apply rule classified freshly
            //     `Pending` (no deps, or all deps already `Completed`) ride
            //     `newly_pending`. A primary that still owns a pool
            //     reinjects each so the pool stays coherent with the CRDT
            //     ledger across wire-received batches. This matters for the
            //     re-promotion path: a demoted primary applying a promoted-
            //     secondary's TasksSpawned broadcast keeps the post-spawn
            //     tasks dispatchable, so a later re-election finds the pool
            //     already aligned with the cluster's view (without it,
            //     re-election would resurrect the pool from its pre-spawn
            //     snapshot and the post-spawn tasks would never dispatch).
            //
            // The rebuild SUBSUMES the incremental reinject for the seed
            // batch (the rebuilt pool already holds every discovery task),
            // so the two are mutually exclusive. Each emits a decoupled
            // `TasksAdded` worker-mgmt signal so the recheck dispatches the
            // new work — never a direct dispatch call (the dispatch-
            // decoupling law).
            if self.pending.is_some() {
                if carries_discovery_task_added && !self.pool().has_queued_dispatchable() {
                    tracing::info!(
                        crdt_tasks = self.cluster_state.task_count(),
                        "discovery TaskAdded seeded the ledger at a quiescent \
                         pool; rebuilding from cluster_state so the discovered \
                         tasks become dispatchable"
                    );
                    // A wire-received discovery batch that seeded a duplicate
                    // `(phase_id, task_id)` identity into the ledger (the
                    // hash-keyed dedup cannot collapse it) surfaces here as a
                    // hydrate `Err`. The operational loop IS running at this
                    // MID-RUN rebuild, so this is the #3b class: route it
                    // through `invalidate_all_pending` — the SAME run-wide
                    // invalidation the within-batch-duplicate spawn path uses
                    // (latch + broadcast `RunAborted` FIRST, freeze dispatch via
                    // the worker-management bus, then wipe the not-yet-terminal
                    // ledger). NOT the bring-up `abort_run_on_invalid_composition`
                    // (that one's typed return is the exit; here the running
                    // loop's worker-mgmt drain owns the shutdown). Return early:
                    // with `pending = None` there is no pool to drain/dispatch.
                    if let Err(e) = self.hydrate_from_cluster_state() {
                        self.invalidate_all_pending(format!(
                            "invalid composed task graph in cluster_state \
                             (mid-run discovery rebuild): {e}"
                        ))
                        .await;
                        return;
                    }
                    // `hydrate_from_cluster_state` no longer self-drains empty
                    // phases (the primary's coordinator owns the narrated
                    // cascade at run-entry). At this MID-RUN rebuild there is
                    // no run-entry cascade to follow, so drain trivially-empty
                    // phases here for dependent visibility — the same silent
                    // cascade the secondary-hydration port performed in-line
                    // before. Callback narration is owned by the operational
                    // loop's `process_phase_lifecycle`.
                    crate::secondary::origination::cascade_drain_done(self.pool_mut());
                    self.cluster_state
                        .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
                } else {
                    let reinjected_any = !newly_pending.is_empty();
                    for task in newly_pending {
                        tracing::debug!(
                            phase = %task.phase_id,
                            task_id = ?task.task_id,
                            "pool: reinject freshly-Pending task from \
                             wire-received TasksSpawned"
                        );
                        self.pool_mut().reinject(std::sync::Arc::new(task));
                    }
                    if reinjected_any {
                        self.cluster_state
                            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
                    }
                }
            }
            if !joined_peer_ids.is_empty() {
                // A peer that just joined may have resolved a
                // previously-unknown `preferred_secondaries` id; drop
                // each joined id from the warn dedup set so the next
                // validation cycle re-evaluates from scratch, then
                // walk every replicated task in `cluster_state` (the
                // authoritative post-apply view — on a demoted /
                // setup-promoted primary `all_binaries` is empty and
                // `cluster_state.iter_all()` is the only source).
                for id in &joined_peer_ids {
                    self.preferred_secondaries_validator.forget(id);
                }
                let known: std::collections::HashSet<&str> =
                    self.secondaries.keys().map(|s| s.as_str()).collect();
                let tasks: Vec<TaskInfo<I>> = self
                    .cluster_state
                    .iter_all()
                    .map(|(_, t)| t)
                    .collect();
                self.preferred_secondaries_validator
                    .validate(tasks.iter(), &known);
            }
            if has_task_added {
                // Refresh `total_tasks` from the post-apply CRDT view
                // so runtime task injection (`TaskAdded` / `TasksSpawned`)
                // a peer originated is absorbed into the authoritative
                // primary's count. Idempotent against duplicate-Add via
                // the CRDT's presence semantics: re-applying a TaskAdded
                // for a hash already in the ledger leaves `task_count`
                // unchanged.
                self.total_tasks = self.cluster_state.task_count();
            }
            if capacity_grew {
                // A worker became ready: a `SecondaryCapacity` this batch
                // genuinely applied grew the replicated roster. Rebuild the
                // local worker cache from the now-grown capacity set and
                // emit `TasksAdded` so the dispatch recheck re-evaluates the
                // new idle slot against the ready pool — the worker-ready
                // half of state-based dispatch (the wire-receive twin of the
                // originator-side reaction). Owned by worker management; the
                // apply path here only DETECTS the growth.
                self.react_to_capacity_growth();
            }
        }
    }
}
