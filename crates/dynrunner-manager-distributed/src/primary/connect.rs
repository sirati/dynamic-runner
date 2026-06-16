use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::primary::error::RunError;
use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

/// True iff `msg` is an IMPORTANT [`DistributedMessage::CustomMessage`] —
/// the F5 class whose pre-handler ack would open the ack-then-die-before-
/// CRDT-commit wedge (#539). Used by `dispatch_message` to skip the
/// pre-handler `ack_delivery_report` for this one class; the matching
/// post-`CustomMessagePosted`-apply ack lives inside `handle_custom_message`.
///
/// Free function (not a method): it inspects only the variant + `important`
/// bit, never coordinator state, so it stays generic-free and the
/// `dispatch_message` call site reads as one boolean classifier.
fn is_important_custom_message<I: Identifier>(msg: &DistributedMessage<I>) -> bool {
    matches!(
        msg,
        DistributedMessage::CustomMessage {
            important: true,
            ..
        }
    )
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Returns the structured [`RunError`] directly (not the helper-level
    /// `Result<(), String>` shape): the zero-welcome timeout is a KNOWN
    /// must-raise run terminal ([`RunError::BringUpFailed`]) and must not
    /// be flattened through `From<String>` into the swallow-eligible
    /// `Other`. The transport/dispatch error paths inside keep their
    /// generic `Other` typing via the blanket `From` on `?`.
    pub(super) async fn wait_for_connections(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), RunError> {
        tracing::info!("waiting for {} secondaries", self.config.num_secondaries);

        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let expected = self.config.num_secondaries as usize;

        // Whether the wait exits via the quorum-proceed timeout arm
        // (k<expected welcomed) rather than the full-fleet break. Drives
        // the bring-up milestone's full-vs-quorum phrasing after the loop
        // — set ONCE in the timeout arm so the single post-loop emit reads
        // it without re-deriving the count comparison.
        let mut proceeded_at_quorum = false;

        // Setup-liveness beacon: the SAME jittered anti-entropy digest
        // cadence every other waiting state already runs (the secondary's
        // `wait_for_setup`, this primary's `operational_loop`, the observer
        // tail) — `wait_for_connections` was the one wait WITHOUT it, which
        // made an assembling primary INVISIBLE to its fleet for up to the
        // whole `connect_timeout` straggler window. The broadcast digest is
        // the "still assembling the fleet" liveness signal the secondaries'
        // re-armable setup deadline keys on (see
        // `secondary::setup_deadline`): every welcomed/announce-received
        // secondary that can hear the primary keeps waiting while it
        // assembles, so the quorum-proceed lands on a LIVE fleet — the
        // asm-dataset LMU death (15/15 secondaries expired inside the
        // primary's silent 600s straggler window, the quorum-proceed at
        // 11:16:10 relocating into a fleet dead for 20s) cannot recur. It
        // ALSO doubles as the standard anti-entropy EMIT half, healing
        // mid-setup divergence exactly as the sibling waits do.
        //
        // `interval_at(now + period, period)` schedules the FIRST tick at
        // `t + period` (not t=0), so the select loop starts immediately
        // without blocking outside the loop. Using `interval(period)` with
        // a pre-loop `tick().await` to discard the t=0 tick was correct
        // semantically but created a real-time race: the `tick().await`
        // held execution outside the select for up to `period` (15–25 s),
        // during which `connect_timeout` could expire even though secondaries
        // had already sent their welcome messages into the buffered channel —
        // on re-entry both the inbox arm and the (already-expired) deadline
        // arm were simultaneously ready, and tokio's pseudo-random select
        // arbitration could pick the timeout arm first, producing a spurious
        // "0/N sent SecondaryWelcome" BringUpFailed under parallel test load.
        // `interval_at` keeps the same 15–25 s cadence with no pre-loop block.
        let period = crate::anti_entropy::tick_period(&self.config.node_id);
        let mut assembly_beacon = tokio::time::interval_at(
            tokio::time::Instant::now() + period,
            period,
        );
        assembly_beacon.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Check if all secondaries have completed cert exchange
            let cert_done = self
                .secondaries
                .values()
                .filter(|s| s.is_at_least_cert_exchanged())
                .count();
            if cert_done >= expected {
                break;
            }

            // Cancellation safety: `transport.recv_peer` is the unified
            // inbound demux — the relocated `NetworkServer` `select!`
            // over the cancel-safe inbound + registration mpscs.
            // `sleep_until` is one-shot and cancel-safe. If
            // `sleep_until` wins it's because the deadline expired and
            // we error out anyway. The demux applies any pending writer
            // registration before surfacing a frame, so the welcome
            // this loop counts arrives with its secondary already
            // registered (FIFO welcome → registration → cert-exchange).
            tokio::select! {
                msg = self.inbox.recv() => {
                    match msg {
                        // Pre-operational-loop site. Threading
                        // `command_rx` through so an `on_phase_end`
                        // callback fired by an in-cascade TaskComplete
                        // can queue a `PrimaryCommand::SpawnTasks` and
                        // the cascade's per-iteration drain step
                        // applies it inline before the wait returns.
                        // Without this, the spawn lands on the channel
                        // but `operational_loop`'s `completed+failed >=
                        // total_tasks` exit check trips on entry before
                        // the select! polls the command arm — the
                        // spawn is dropped and the run completes with
                        // the post-spawn tasks never dispatching. The
                        // PyO3 `PrimaryHandle` IS reachable before
                        // operational-loop entry (it shares the
                        // pre-`run` `command_sender()` clone), and
                        // `on_phase_end` fires off any TaskComplete
                        // that arrives during connect — both producers
                        // converge on the same channel here.
                        // A `SecondaryWelcome` handled here originates the
                        // secondary's `SecondaryCapacity`, which (via
                        // `react_to_capacity_growth`) rebuilds the roster and
                        // queues a `TasksAdded` on the bus. Deliberately do
                        // NOT drain+dispatch inline at THIS wait: connect runs
                        // BEFORE `perform_initial_assignment`, which is the
                        // designated first-dispatch (a deterministic
                        // round-robin over the full roster). A live dispatch
                        // here would drain the pool ahead of it and break the
                        // initial-assignment shape. The queued `TasksAdded`
                        // stays on the bus and is serviced by the
                        // post-assignment `wait_for_mesh_ready` drain / the
                        // operational-loop entry sweep — the roster is current,
                        // dispatch is just ordered after initial assignment.
                        Some(m) => self.dispatch_message(m, command_rx).await?,
                        None => return Err("transport closed".into()),
                    }
                }
                // Setup-liveness beacon tick (see the arming comment above
                // the loop): narrate assembly progress + broadcast this
                // primary's digest so the fleet's re-armable setup
                // deadlines observe a LIVE assembling primary. Pure EMIT of
                // the role-agnostic anti-entropy frame; the receive-side
                // compare+pull is the `StateDigest` arm of
                // `dispatch_message`. `interval.tick` is cancel-safe
                // (tokio docs).
                _ = assembly_beacon.tick() => {
                    tracing::info!(
                        welcomed = cert_done,
                        expected,
                        "still assembling the fleet ({cert_done}/{expected} \
                         welcomed); broadcasting setup-liveness digest"
                    );
                    let digest = self.cluster_state.digest();
                    let frame = crate::anti_entropy::digest_broadcast(
                        &self.config.node_id,
                        super::wire::timestamp_now(),
                        digest,
                        // A PrimaryCoordinator is never an observer.
                        false,
                    );
                    if let Err(error) = self
                        .send_to(
                            dynrunner_protocol_primary_secondary::Destination::All,
                            frame,
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %error,
                            "setup-liveness digest broadcast failed; the next \
                             tick retries"
                        );
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    // Quorum-on-timeout: if at least one secondary has
                    // completed cert-exchange, proceed with what we
                    // have rather than failing the entire dispatch.
                    // Real-world flakes (cp corruption from gateway →
                    // compute-node /tmp, podman-load races, /tmp full
                    // killing the wrapper, single-node SLURM
                    // scheduling glitches) routinely take down 1-of-N
                    // secondaries; pre-fix the all-or-nothing
                    // handshake meant a 5-job dispatch dies if 1 job
                    // fails. With quorum, we drop the missing
                    // secondaries from `num_secondaries` AND from
                    // `self.secondaries` (so initial assignment +
                    // worker-budget accounting reflect the actual
                    // fleet, AND `peer_setup` doesn't try to send
                    // PeerInfo to a half-handshaked secondary whose
                    // wire connection's writer task already
                    // exited — that surfaces as "channel closed"
                    // on the send_to and bubbles up as a
                    // primary-coordinator-failure). Failover
                    // requires N>=1; zero connected is still a
                    // hard error.
                    if cert_done == 0 {
                        // Structured run terminal, NOT a bare string: a
                        // 0/N bring-up is a run-level FATAL the PyO3
                        // boundary must RAISE — typing it `Other` (the
                        // old `From<String>` flattening) made the
                        // boundary swallow it into a clean
                        // "Completed: 0 / Failed: 0" rc=0 teardown
                        // (run_20260611_131736).
                        return Err(RunError::BringUpFailed {
                            reason: format!(
                                "timeout waiting for secondaries: 0/{expected} sent \
                                 SecondaryWelcome (transport-level connection accept \
                                 happens lazily on first message, so 0/N here can mean \
                                 either no peer ever connected OR connections completed \
                                 handshake but never sent Welcome — in the latter case \
                                 the per-connection accept handler should have logged a \
                                 'peer connected but did not send SecondaryWelcome \
                                 within Ns; closing as non-conformant' line in the \
                                 transport log; that points at the consumer's \
                                 worker_module not completing the runner protocol's \
                                 Ready handshake)"
                            ),
                        });
                    }
                    // Drop secondaries that are present in the registry
                    // but didn't make it to cert-exchanged (Handshaking
                    // or earlier). They're stale entries — peer_setup
                    // and downstream phases must NOT iterate over them.
                    let to_drop: Vec<String> = self.secondaries
                        .iter()
                        .filter(|(_, s)| !s.is_at_least_cert_exchanged())
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in &to_drop {
                        self.secondaries.remove(id);
                    }
                    let missing: Vec<String> = (0..expected)
                        .map(|i| format!("secondary-{i}"))
                        .filter(|sid| !self.secondaries.contains_key(sid))
                        .collect();
                    tracing::warn!(
                        connected = cert_done,
                        expected,
                        dropped_partial = ?to_drop,
                        missing_no_welcome = ?missing
                            .iter()
                            .filter(|id| !to_drop.contains(id))
                            .collect::<Vec<_>>(),
                        "connect_timeout reached with partial fleet; proceeding \
                         with quorum — missing/partial secondaries are dropped \
                         from this dispatch (run continues at reduced parallelism, \
                         no tasks lost)"
                    );
                    self.config.num_secondaries = cert_done as u32;
                    proceeded_at_quorum = true;
                    break;
                }
            }
        }

        // Whole-cluster-ready milestone (the LLM-wake "all secondaries
        // connected" bring-up event): every connected secondary has
        // completed cert-exchange, so the mesh is formed and the run can
        // proceed. Emitted at the importance target so the dual-sink
        // routes it to stdio under `--important-stdio-only` while the full
        // log keeps it too.
        //
        // The two loop-exit paths the operator must be able to tell apart:
        // a FULL fleet (every requested secondary welcomed) vs a
        // QUORUM-proceed (the straggler window expired with k<requested,
        // the missing ones already dropped from the dispatch). Both report
        // k/n so an LLM woken on stdio sees the actual fleet size; the
        // quorum case additionally says it is proceeding at reduced
        // parallelism. `expected` is the ORIGINAL requested count captured
        // before the timeout arm overwrote `self.config.num_secondaries`.
        let connected = self.secondaries.len();
        if proceeded_at_quorum {
            tracing::info!(
                target: super::important_events::IMPORTANT_TARGET,
                connected,
                requested = expected,
                "secondaries connected at quorum: {connected}/{expected} \
                 welcomed; proceeding at reduced parallelism",
            );
        } else {
            tracing::info!(
                target: super::important_events::IMPORTANT_TARGET,
                connected,
                requested = expected,
                "all secondaries connected: {connected}/{expected}",
            );
        }
        Ok(())
    }

    /// Central message dispatcher — routes incoming messages by type.
    ///
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the TaskComplete / TaskFailed cascade so a
    /// callback-issued `spawn_tasks` applies inline before the next
    /// `drain_empty_active_phases` poll. The pre-loop waits
    /// (`wait_for_connections`, `wait_for_mesh_ready`) pass the LIVE
    /// `command_rx` (`Some`): PyPrimaryHandle is already reachable before
    /// operational-loop entry (it shares the pre-`run` `command_sender()`
    /// clone), so a callback-queued command drains inline during those
    /// waits. Only the post-loop caller (`drain_pending_messages`) passes
    /// `&mut None`: the loop has already exited and won't re-enter, so no
    /// in-runtime callback path needs draining.
    pub(super) async fn dispatch_message(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        // RE-ADMISSION seam (BEFORE the heartbeat bump, so a re-admitted
        // sender's triggering frame lands in its restored death clock): a
        // frame from a member the replicated ledger holds REMOVED is
        // proof of life — re-admit it at the next membership generation.
        // No-op for live / never-joined / self senders. See
        // `primary::readmission`.
        self.maybe_readmit_sender(&msg).await;

        // Every cross-secondary message bumps the per-secondary heartbeat,
        // not just `Keepalive`. A secondary that's actively processing
        // tasks shouldn't be falsely declared dead just because keepalives
        // are sparser than task traffic.
        self.record_keepalive(msg.sender_id());

        // App-level delivery confirmation (#352): echo a `TerminalAck`
        // for EVERY `delivery_seq`-stamped confirmable landing (a
        // terminal, or a droppable consumer frame) BEFORE the handlers
        // run — including landings their dedup gates will drop (a
        // duplicate means the original ack was lost or the replay raced
        // it; not re-acking would replay forever). A no-op for every
        // other frame, so the handlers stay seq-oblivious.
        //
        // IMPORTANT custom messages (F5) are EXCLUDED from this
        // pre-handler ack (#539 — the ack-then-die-before-CRDT-commit
        // wedge): acking before `apply_and_broadcast(CustomMessagePosted)`
        // lets the origin drop the message from its retain buffer the
        // instant the ack lands, and a primary that dies between the ack
        // and the wire fan-out strands the message — no peer's CRDT has
        // the Unhandled entry, the origin can no longer replay, and any
        // subsequent task terminal stamped at that origin with
        // `msgs_posted_through >= seq` parks forever in the
        // terminal-ordering gate (`terminal_gate.rs`). The ack for an
        // important landing is sent below, AFTER `handle_custom_message`
        // has run its `apply_and_broadcast(CustomMessagePosted)` — the
        // post-ack retention drop is then safe under the CRDT's
        // anti-entropy backstop (the local apply makes the entry
        // available to any peer's snapshot pull).
        let important_custom = is_important_custom_message(&msg);
        let deferred_ack: Option<(u64, String)> = if important_custom {
            // Snapshot the ack ingredients before `msg` is moved into the
            // handler (the destructure consumes the variant). `None` for
            // an unstamped / no-reporter landing — the same skip rule
            // `ack_delivery_report` enforces internally.
            match (msg.delivery_seq(), msg.delivery_reporter()) {
                (Some(seq), Some(reporter)) => Some((seq, reporter.to_string())),
                _ => None,
            }
        } else {
            self.ack_delivery_report(&msg).await;
            None
        };

        match msg.msg_type() {
            MessageType::SecondaryWelcome => self.handle_welcome(msg).await,
            MessageType::CertExchange => self.handle_cert_exchange(msg).await,
            MessageType::TaskRequest => self.handle_task_request(msg, command_rx).await?,
            // Wire task terminals route through the terminal-ordering
            // gate (`terminal_gate.rs`): a terminal whose origin's
            // causally-prior IMPORTANT custom messages (the
            // `msgs_posted_through` stamp) are not yet resolved is
            // PARKED and re-admitted on the custom-message dispatch
            // cadence — phase-end derives from terminals, so it can
            // never overtake the messages the phase's own task sent.
            MessageType::TaskComplete | MessageType::TaskFailed => {
                self.ingest_task_terminal(msg, command_rx).await
            }
            // Off-primary setup-task executor's terminal report (the
            // setup-task counterpart of a worker terminal). Routed to the
            // dedicated setup-terminal handler — a setup task has no worker
            // slot, so it does NOT go through the worker terminal gate. The
            // handler originates the authoritative CRDT terminal
            // (`SetupCompleted` / `TaskFailed { NonRecoverable }`). See
            // `primary::setup_dispatch`.
            MessageType::SetupTerminal => self.handle_setup_terminal(msg, command_rx).await,
            // Consumer custom message (F5): droppable → direct handler
            // dispatch; important → CRDT-post FIRST then ack + handler
            // dispatch (the ack lives inside `handle_custom_message` for
            // an important landing — see the pre-match `if` above for
            // the #539 rationale).
            MessageType::CustomMessage => self.handle_custom_message(msg, command_rx).await,
            // Reconciliation-probe verdict arm (#308): a holder
            // secondary's answer to this primary's `TaskHoldQuery`.
            // `held` re-arms the per-task deadline; `not held` fails +
            // requeues the lost task through the backpressure-shaped
            // `handle_task_failed` path. See
            // `primary::reconciliation_probe`.
            MessageType::TaskHoldResponse => self.handle_task_hold_response(msg, command_rx).await,
            // Affine-deferral reports (#497): a secondary parks a work task
            // behind its local SecondaryAffine import and REPORTS the queued
            // state; on import completion it self-dispatches the task and
            // REPORTS the release. The secondary reports, the PRIMARY
            // originates the CRDT transitions (the work-split law) — and,
            // critically, the queued handler DROPS the parked dependent from
            // `self.in_flight` so the reconciliation probe (which views only
            // the ledger) stops looping on a task no holder will report a
            // terminal for. Without these arms both reports hit the
            // catch-all below, stranding the task InFlight forever and
            // re-originating it every ~600s reconciliation cycle (the
            // unbounded coordinator leak). See `primary::task::affine_deferral`.
            MessageType::TaskQueuedAfterLocalDependency => {
                self.handle_task_queued_after_local_dependency(msg).await
            }
            MessageType::LocalDependencyReleased => {
                self.handle_local_dependency_released(msg).await
            }
            // Remote respawn-execution outcomes: the provider-host
            // observer's answer to this primary's RespawnSpawnRequest /
            // RespawnRevokeRequest. Completes the matching waiter inside
            // the RemoteSecondarySpawner's retry loop. See
            // `primary::respawn::remote`.
            MessageType::RespawnSpawnResult | MessageType::RespawnRevokeResult => {
                self.handle_respawn_exec_result(msg)
            }
            MessageType::MeshReady => self.handle_mesh_ready(msg),
            // Observer-requested graceful abort: the ONE management command
            // a zero-authority observer may send. The handler originates the
            // replicated `GracefulAbortRequested` sticky latch (idempotent —
            // a re-sent request against an already-latched freeze NoOps).
            // See `lifecycle::graceful_abort`.
            MessageType::GracefulAbortRequest => self.handle_graceful_abort_request(msg).await,
            // The death clock was refreshed by the preamble's
            // `record_keepalive`; additionally record the PROOF that the
            // sender's operational main loop runs (a secondary-role
            // keepalive is emitted only post-`wait_for_setup`), which
            // bounds the silence sweep's pre-Operational setup exemption.
            MessageType::Keepalive => self.note_secondary_keepalive_frame(&msg),
            MessageType::SecondaryFatalError => self.handle_secondary_fatal_error(msg).await?,
            // Replicated cluster ledger maintenance. Without this arm
            // the demoted local primary cannot observe completions
            // forwarded only via the CRDT bus (the cross-secondary
            // case after promotion); see `handle_cluster_mutation`
            // for the full rationale and the asm-dataset-nix R2 / T3
            // hang it pins.
            MessageType::ClusterMutation => self.handle_cluster_mutation(msg, command_rx).await,
            // Snapshot-RPC responder. A late-joiner / re-bootstrapping
            // peer (or a recovering observer) unicasts
            // `RequestSnapshotStream` to `Destination::Primary`; the
            // primary's `cluster_state` is the authoritative copy, so its
            // stream is the strongest bootstrap source. The handler only
            // REGISTERS the stream; the operational loop's stream arm
            // produces one bounded package per wakeup. See
            // `handle_request_snapshot_stream`.
            MessageType::RequestSnapshotStream => self.handle_request_snapshot_stream(msg).await,
            // Run-config-RPC responder. A joining / respawned peer (or one
            // re-fetching after promotion) unicasts `RequestRunConfig`; the
            // primary answers from its node-local `forwarded_argv`. PURE
            // read-only responder — no PeerJoined / welcome / CRDT write
            // (unlike the snapshot arm above). See `handle_request_run_config`.
            MessageType::RequestRunConfig => self.handle_request_run_config(msg).await,
            // Anti-entropy receive arm. A peer's periodic `StateDigest`;
            // the primary compares it against its own and pulls a snapshot
            // only if it is somehow behind (almost always a NoOp on the
            // authoritative primary). See `handle_state_digest`.
            MessageType::StateDigest => self.handle_state_digest(msg).await,
            // Pull-model PROBE from a behind peer: answer with our inbox
            // depth + the responder-side `ahead` bit. Direct-neighbours-only
            // (the ingress never re-broadcast this inbound `All`), handled
            // locally, never relayed onward.
            MessageType::PullProbe => self.handle_pull_probe(msg).await,
            // Pull-model PROBE REPLY → the single-flight pull driver
            // (smallest-inbox-ahead selection + first-answer fallback).
            MessageType::PullProbeReply => self.handle_pull_probe_reply(msg).await,
            // Pull-model FAIL (chosen target's direct leg to us dropped,
            // delivered INDIRECTLY via the relay) → fall to the next target.
            MessageType::PullFail => self.handle_pull_fail(msg).await,
            // Anti-entropy pull-reply arm. One package of the stream this
            // primary's own `handle_state_digest` requested from a
            // proven-ahead peer. Pre-fix this fell through the catch-all
            // and the pull never converged — the deposed-zombie
            // starvation (see `handle_snapshot_stream_package`).
            MessageType::SnapshotStreamPackage => self.handle_snapshot_stream_package(msg),
            // #517 illegal-assignment bounce: a secondary refused to run a
            // task because the assigned worker slot was NOT idle, and never
            // re-picked. This is NOT a `TaskFailed` (it never routes through
            // the terminal gate / `handle_task_failed` — no failure
            // accounting), it is an occupancy-DIVERGENCE report: reconcile
            // this primary's `(secondary, worker_id)` model so it stops
            // believing the slot is idle, then requeue the bounced task for
            // a genuinely-idle worker. See `handle_illegally_assigned`.
            MessageType::IllegallyAssignedToNonidleWorker => {
                self.handle_illegally_assigned(msg).await
            }
            // #518 cross-member dedup: a just-re-admitted member reported
            // the tasks its workers are AUTHORITATIVELY running. For each,
            // recognise the member as the holder and withdraw any requeued
            // duplicate copy on another member. See `handle_inflight_roster`.
            MessageType::InFlightRoster => self.handle_inflight_roster(msg).await,
            // #556 mesh-consensus replies from non-suspected secondaries.
            // `ResolvedPeer` is positive liveness evidence for a suspect
            // (the witness heard back from the probed peer); `RestartConfirm`
            // is the round-2 commit reply on whether to proceed with
            // mesh-declaring the candidate batch dead. Both feed the
            // FSM via the thin wiring layer; the FSM owns the tally + the
            // round verdict. A frame that arrives outside the FSM's
            // in-flight round (stale `consensus_id`) is dropped inside
            // the FSM — no special-case here.
            MessageType::ResolvedPeer => {
                if let DistributedMessage::ResolvedPeer {
                    consensus_id,
                    observer_id,
                    resolved,
                    ..
                } = msg
                {
                    self.apply_consensus_resolved(
                        consensus_id,
                        &observer_id,
                        &resolved,
                    )
                    .await;
                }
            }
            MessageType::RestartConfirm => {
                if let DistributedMessage::RestartConfirm {
                    consensus_id,
                    responder_id,
                    still_suspicious,
                    resolved_since,
                    ..
                } = msg
                {
                    self.apply_consensus_confirm(
                        consensus_id,
                        &responder_id,
                        still_suspicious,
                        resolved_since,
                    )
                    .await;
                }
            }
            // The primary never legitimately RECEIVES `SuspectPeers` /
            // `RestartRequest` (those are primary-emitted) or
            // `PeerProbe` / `PeerProbeAck` (those are secondary-to-secondary).
            // A landing here is either a wire-routing bug or a
            // co-located-loopback echo and is dropped silently — same
            // shape as the historical `other` catchall.
            MessageType::SuspectPeers
            | MessageType::RestartRequest
            | MessageType::PeerProbe
            | MessageType::PeerProbeAck => {
                tracing::debug!(
                    msg_type = ?msg.msg_type(),
                    "#556 consensus frame addressed to primary that the \
                     primary does not consume; dropping (wire-routing edge)"
                );
            }
            other => {
                tracing::debug!(?other, "unhandled message type");
            }
        }
        // Deferred ack for the IMPORTANT custom-message landing (#539):
        // `handle_custom_message` has applied `CustomMessagePosted`
        // locally + queued its wire fan-out, so the entry is now in the
        // local CRDT and anti-entropy carries it to any peer's snapshot
        // pull. Acking here lets the origin's retain buffer drop the
        // message — safe by construction, because the cluster (this
        // primary's `cluster_state` + the in-flight broadcast) is the
        // durable record from this point on. A duplicate landing
        // (replay racing the original) already NoOp'd inside the
        // CustomMessagePosted apply, and we still re-ack so the
        // origin's retention window can collapse.
        if let Some((seq, reporter)) = deferred_ack {
            self.send_terminal_ack_to(seq, &reporter).await;
        }
        Ok(())
    }

    /// Send a `TerminalAck { seq }` to `reporter`. The shared egress used
    /// by both [`Self::ack_delivery_report`] (the variant-agnostic ingest
    /// echo) and the post-handler important-custom ack (#539). Best
    /// effort: a failed send is logged at DEBUG and the reporter's
    /// ack-timeout replay re-lands the report, which gets re-acked.
    async fn send_terminal_ack_to(&mut self, seq: u64, reporter: &str) {
        let ack = DistributedMessage::TerminalAck {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: super::wire::timestamp_now(),
            seq,
        };
        if let Err(e) = self
            .send_to(
                dynrunner_protocol_primary_secondary::Destination::Secondary(
                    dynrunner_protocol_primary_secondary::PeerId::from(reporter),
                ),
                ack,
            )
            .await
        {
            tracing::debug!(
                secondary = %reporter,
                seq,
                error = %e,
                "TerminalAck send failed (best-effort; the reporter's \
                 ack-timeout replay re-lands the report and gets re-acked)"
            );
        }
    }

    /// Echo the app-level delivery confirmation (#352) for one
    /// confirmable report landing — a terminal, or an IMPORTANT custom
    /// message (F5): a `TerminalAck { seq }` unicast back to the
    /// report's ORIGINATING secondary.
    ///
    /// Gated on the frame carrying BOTH a `delivery_seq` (a pre-field /
    /// unstamped sender gets no ack and keeps the pre-#352 no-route-only
    /// replay behaviour; a DROPPABLE custom is never stamped) and a
    /// `delivery_reporter` (non-confirmable frames have neither) — i.e.
    /// a no-op for everything but a stamped `TaskComplete` /
    /// `TaskFailed` / important `CustomMessage`.
    ///
    /// Addressed to the frame's `secondary_id` (the originator holding
    /// the retention buffer), NOT the wire `sender_id`: a relayed or
    /// peer-forwarded landing must still clear the originator's buffer.
    /// The ack is acked-per-LANDING and exact-seq (no ack-up-to
    /// coalescing — replays re-send the same seq, possibly across a
    /// failover, so cumulative semantics could falsely confirm a seq
    /// that travelled a different, still-blackholed leg). Dedup of the
    /// duplicate landings themselves stays with the proven hash-keyed
    /// terminal idempotence in the handlers — no per-secondary seq state
    /// is kept here, so a freshly-promoted primary acks replays
    /// correctly with zero handoff.
    ///
    /// A send failure is best-effort (DEBUG): the reporter's ack-timeout
    /// replay re-lands the report and this site re-acks it.
    pub(super) async fn ack_delivery_report(&mut self, msg: &DistributedMessage<I>) {
        let (Some(seq), Some(reporter)) = (msg.delivery_seq(), msg.delivery_reporter()) else {
            return;
        };
        self.send_terminal_ack_to(seq, reporter).await;
    }

    /// Record a secondary's `MeshReady` report. The
    /// `wait_for_mesh_ready` step blocks on this set covering every
    /// connected secondary before it lets the `PrimaryChanged`
    /// announcement fire. A stray `MeshReady` after the wait already
    /// cleared is
    /// idempotent — the set just stays full and the message is a
    /// no-op.
    /// # Sole writer of the mesh-confirmation set
    ///
    /// `mesh_ready_secondaries` is the single source of truth for "has
    /// this member confirmed its peer-mesh leg formed". This is its ONLY
    /// insertion site, and it is UNCONDITIONAL — a `MeshReady` that lands
    /// AFTER `wait_for_mesh_ready` already proceeded on the timeout (a
    /// straggler that finished its dials late) STILL flips the member
    /// confirmed here, so the dispatch gate keyed on
    /// [`Self::member_mesh_confirmed`] recovers it into the assignable
    /// set. Late join must recover — there is no one-shot guard.
    pub(super) fn handle_mesh_ready(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::MeshReady {
            target: None,
            secondary_id,
            peer_count,
            ..
        } = msg
        {
            tracing::debug!(
                secondary = %secondary_id,
                peer_count,
                "secondary reports mesh ready"
            );
            let newly_confirmed = self.mesh_ready_secondaries.insert(secondary_id.clone());
            if newly_confirmed {
                // Name the CONSEQUENCE the silent-branch rule asks for: the
                // dispatch gate that withheld work from this member
                // (`member_mesh_confirmed` returned false for it) now lets
                // it through — a late MeshReady recovers a member that the
                // mesh-ready timeout left unassignable.
                tracing::info!(
                    secondary = %secondary_id,
                    "member mesh leg confirmed; it is now assignable to proactive dispatch"
                );
                // Re-arm the gate's once-per-spell veto WARN: if this
                // member ever regresses to unconfirmed (a promoted
                // primary's empty set), the next veto names it again.
                self.mesh_gate_veto_warned.remove(&secondary_id);
                // CONFIRMATION-EDGE DISPATCH WAKEUP. A member becoming
                // confirmed enlarges the assignable set — the exact dual
                // of `react_to_capacity_growth`'s worker-ready signal —
                // so dispatch must re-evaluate NOW, not on the next
                // unrelated event. Emit `TasksAdded` on the
                // worker-management bus: the operational loop's
                // worker-management arm (or the pre-loop inline drain
                // during `wait_for_mesh_ready`) runs the recheck and the
                // work the gate withheld flows at this edge. Decoupling
                // law: bus signal only, never a direct dispatch call.
                self.cluster_state
                    .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
            }
        }
    }

    /// THE single dispatch-readiness predicate every assignment path
    /// consults: may proactive dispatch push work to `secondary_id`, or
    /// is it a half-joined member whose mesh leg never confirmed?
    ///
    /// Returns `true` (assignable) iff the member's frames provably
    /// reach this primary:
    ///
    ///   * the member is CO-LOCATED with this primary (its peer-id IS
    ///     `config.node_id` — the same-host worker-secondary of a
    ///     promoted/compute-peer primary). Its frames ride the
    ///     in-process loopback (`Mesh::deliver_local`), so there is no
    ///     wire leg for a `MeshReady` to prove — co-location is
    ///     structural confirmation. This mirrors the self-exclusion
    ///     `wait_for_mesh_ready` applies to its expected set ("a node
    ///     never emits MeshReady ABOUT ITSELF to itself") — demanding a
    ///     self round-trip left the lone-survivor acting primary
    ///     (run_20260612_035452) vetoing the ONLY dispatchable workers
    ///     until the co-located secondary finished consuming the setup
    ///     trio and its loopbacked report landed; OR
    ///   * the member has confirmed its peer-mesh leg formed
    ///     (`mesh_ready_secondaries` — a `MeshReady` was received from
    ///     it).
    ///
    /// Nothing else counts: keepalives are liveness, not leg
    /// confirmation, and "never dispatched to" is not proof of a
    /// working leg.
    ///
    /// # Single owner of the dispatch readiness gate
    ///
    /// A member that never confirmed its mesh leg is half-joined: its
    /// terminals ride a half-formed mesh egress leg that silently
    /// swallows them (route present at the transport `has_peer` view,
    /// send returns `Ok`, but the frame never reaches the authority —
    /// the run_20260610_105906 strand). Pushing work to it strands each
    /// task and wedges the phase barrier. So it is withheld from EVERY
    /// proactive path until a `MeshReady` lands.
    ///
    /// # No first-dispatch exemption (#360)
    ///
    /// This predicate originally exempted a member's FIRST dispatch
    /// (tracked in a `members_dispatched_to` set), on the theory that
    /// the bring-up dispatch is what drives a late member operational so
    /// it can emit `MeshReady`. That theory was stale: a member reaches
    /// its operational loop by consuming the setup trio — and the
    /// `InitialAssignment` half of the trio is sent by
    /// `perform_initial_assignment` to EVERY known secondary (empty
    /// batches included), a direct fan-out that never consults this
    /// gate. Once operational, `MeshReady` emission is fully
    /// dispatch-independent (operational-entry hook, mesh watchdog,
    /// keepalive tick). A `TaskAssignment` pushed at a member still
    /// stuck in `wait_for_setup`, by contrast, is DROPPED by its setup
    /// loop ("unexpected message during setup") while the primary
    /// records the task `InFlight` — the exemption could only strand,
    /// never recover. In production (run_20260610_144905) it admitted
    /// the first work onto unconfirmed secondary-2; the type-shift
    /// first-bind continuation kept serving it and the terminal
    /// swallowed on the half-formed leg, in the same second this gate
    /// was vetoing further pushes. There is no residual window that
    /// needs it: a healthy-uplink member that somehow never confirmed
    /// still PULLS work through the request-driven path (its
    /// `TaskRequest`'s arrival is its own delivery proof), and the
    /// assigned=0 bring-up recovery is the confirmation-edge wakeup in
    /// [`Self::handle_mesh_ready`] — the withheld work flows the moment
    /// the member's `MeshReady` lands, with no unrelated event needed.
    ///
    /// The SOLE writer of `mesh_ready_secondaries` is
    /// [`Self::handle_mesh_ready`] (unconditional, so a late `MeshReady`
    /// recovers the member into the assignable set). The set is
    /// deliberately NOT cleared on a member's removal: a re-admission is
    /// keyed on FRAME INGEST from that member (its frames demonstrably
    /// reached this primary again), which is the same delivery-proof
    /// class as a `TaskRequest`'s arrival — so a re-admitted member's
    /// surviving confirmation is backed by fresh evidence, while the
    /// member itself (never knowing it was removed) would never re-send
    /// a `MeshReady` that a cleared entry would wait on.
    ///
    /// Read by [`Self::should_skip_worker_for_dispatch`] — the single
    /// owner of the per-worker dispatch-skip decision — so BOTH
    /// operational dispatch paths (`dispatch_to_idle_workers` and
    /// `handle_task_request`) gate on this ONE predicate without either
    /// site knowing the mesh-readiness rule.
    pub(super) fn member_mesh_confirmed(&self, secondary_id: &str) -> bool {
        secondary_id == self.config.node_id || self.mesh_ready_secondaries.contains(secondary_id)
    }

    pub(super) async fn handle_welcome(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::SecondaryWelcome {
            target: None,
            secondary_id,
            resources,
            worker_count,
            hostname,
            is_observer,
            can_be_primary,
            ..
        } = msg
        {
            let ram_bytes = resources
                .iter()
                .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                .map(|r| r.amount)
                .unwrap_or(0);

            // Idempotence on the connection TYPESTATE: the secondary's
            // setup loop retries its welcome on a capped backoff until it
            // observes the setup trio (the retry is load-bearing —
            // run_20260611_005927), so a duplicate welcome routinely lands
            // AFTER the one-shot bring-up walk already advanced this member
            // past `Handshaking`. Re-inserting a fresh `Handshaking` state
            // would REGRESS a walked (possibly Operational) member — and the
            // batch walk phases never re-run, so nothing would ever walk it
            // back: the member would sit pre-Operational forever, invisible
            // to the silence schedule's state gate while still working tasks
            // (the run_20260611_214327 wedge: a wire-dead member was never
            // declared). Keep the walked state; every side effect below is
            // idempotent (set-once capacity, generation-gated `PeerJoined`)
            // or deliberately re-sent (the run-config push — the retry
            // contract). A fresh or still-handshaking member registers
            // exactly as before.
            let fresh_member = matches!(
                self.secondaries.get(&secondary_id),
                None | Some(SecondaryConnectionState::AwaitingWelcome(_))
                    | Some(SecondaryConnectionState::Handshaking(_))
            );
            if fresh_member {
                tracing::info!(
                    secondary = %secondary_id,
                    workers = worker_count,
                    ram_gb = ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                    is_observer,
                    "secondary connected"
                );
            } else {
                tracing::debug!(
                    secondary = %secondary_id,
                    "duplicate welcome from an already-walked member \
                     (handshake retry); connection state retained"
                );
                // A duplicate welcome from a member WITHOUT operational
                // proof is a trio-retransmit request: its setup gate has
                // not released, so a served frame was lost in flight. The
                // re-serve rule (and the serve itself) is owned by
                // `peer_setup`; this site only reports the event.
                self.re_serve_setup_on_duplicate_welcome(&secondary_id)
                    .await;
            }

            // Capture the advertised capacity before `resources` is
            // moved into the per-secondary connection state below, so
            // the same welcome originates the static `SecondaryCapacity`
            // record into the replicated ledger (see the broadcast
            // batch below). `worker_count` is `Copy`; `resources` is
            // cloned once.
            let advertised_resources = resources.clone();
            if fresh_member {
                let conn = SecondaryConnection::new(secondary_id.clone());
                let conn = conn.receive_welcome(
                    worker_count,
                    resources,
                    hostname,
                    0,
                    None,
                    is_observer,
                    can_be_primary,
                );
                self.secondaries.insert(
                    secondary_id.clone(),
                    SecondaryConnectionState::Handshaking(conn),
                );
                self.seed_keepalive(&secondary_id);
            }

            // Explicit `PeerJoined` origination on accept.
            //
            // The widened apply rule (`apply_peer_joined`) tracks full
            // peer-lifecycle in `peer_state` and ratchets the observer
            // projection from `is_observer = true` broadcasts. Prior to
            // this site, non-observer secondary membership was implicit
            // (everyone learned from the `PeerInfo` broadcast in
            // `send_peer_lists`); observer membership was originated from
            // the same site. Making the welcome accept originate the
            // `PeerJoined` mutation gives the CRDT a per-join entry the
            // moment a secondary is recorded as connected, regardless of
            // its observer flag — `send_peer_lists` continues to emit
            // its observer batch idempotently (the `apply_peer_joined`
            // rule short-circuits NoOp on re-applies for an already-
            // alive id whose observer projection isn't changing).
            // Originate the static `SecondaryCapacity` record alongside
            // `PeerJoined`, carrying the `worker_count` + advertised
            // resources the welcome announced (historically dropped
            // here — the worker roster was 100% primary-local, so a
            // promoted primary started `alive_worker_count() == 0`).
            // Set-once apply: the idempotent re-emits this site shares
            // with `PeerJoined` (e.g. via `send_peer_lists`) NoOp on
            // re-application.
            self.apply_and_broadcast_cluster_mutations(vec![
                ClusterMutation::PeerJoined {
                    peer_id: secondary_id.clone(),
                    is_observer,
                    // The connecting secondary advertised its own
                    // primary-capability in the `SecondaryWelcome` (under
                    // mesh-always a network compute secondary ⇒ true; only an
                    // observer / the in-process same-host secondary ⇒ false).
                    // Record that truth in the replicated
                    // `RoleTable.can_be_primary` so the bootstrap-relocation /
                    // promotion selection reads the explicit marker.
                    can_be_primary,
                    // Stamped at the origination choke point
                    // (`apply_locally_for_broadcast` → `stamp_versions`).
                    cap_version: Default::default(),
                    // The id's CURRENT membership incarnation (0 for a
                    // fresh id; a welcome from a just-re-admitted id
                    // carries the bumped generation the dispatch
                    // preamble's re-admission seam already applied, so
                    // this join is the idempotent NoOp echo).
                    member_gen: self.cluster_state.peer_member_gen(&secondary_id),
                },
                ClusterMutation::SecondaryCapacity {
                    secondary: secondary_id.clone(),
                    worker_count,
                    resources: advertised_resources,
                },
            ])
            .await;

            // Post-welcome run-config delivery. The secondary booted with
            // only its boot-critical CLI args and parses the consumer's
            // run-config (`--task`, task filters) AFTER it connects, so the
            // primary proactively unicasts the EXISTING `RunConfig` frame
            // over the connection it just welcomed — the secondary need not
            // ask first. Pure SEND of the existing message; the welcome /
            // cert handshake above is unchanged.
            self.push_run_config_to(secondary_id).await;
        }
    }

    pub(super) async fn handle_cert_exchange(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::CertExchange {
            target: None,
            secondary_id,
            public_cert_pem,
            ipv4_address,
            ipv6_address,
            quic_port,
            liveness_port,
            ..
        } = msg
            && let Some(state) = self.secondaries.remove(&secondary_id)
        {
            // The inner branch carries an else, so we keep it nested
            // rather than chaining into the outer `&& let`.
            if let SecondaryConnectionState::Handshaking(conn) = state {
                let conn = conn.receive_cert_exchange(
                    public_cert_pem,
                    ipv4_address,
                    ipv6_address,
                    quic_port,
                    liveness_port,
                );
                self.secondaries.insert(
                    secondary_id.clone(),
                    SecondaryConnectionState::CertExchanging(conn),
                );
                tracing::debug!(secondary = %secondary_id, "cert exchange received");
                // Incremental setup delivery: the member is servable the
                // moment its cert/addresses land — notify the setup-
                // delivery owner (`peer_setup`) so the grown roster is
                // broadcast NOW and this member's typestate is walked,
                // instead of holding it (and every earlier-welcomed
                // member) hostage until the whole fleet has welcomed.
                // Fires only on this Handshaking → CertExchanging edge:
                // a duplicate cert-exchange (the load-bearing handshake
                // retry) finds the member already walked and lands in
                // the `else` arm below, so retries never re-broadcast.
                self.serve_setup_on_cert_exchange(&secondary_id).await;
            } else {
                self.secondaries.insert(secondary_id, state);
            }
        }
    }

    // ── Phase 3: Send Peer Lists ──
}
