use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::primary::error::RunError;
use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

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
        // mid-setup divergence exactly as the sibling waits do. `Skip` +
        // dropped-first-tick mirrors the operational arm.
        let mut assembly_beacon =
            tokio::time::interval(crate::anti_entropy::tick_period(&self.config.node_id));
        assembly_beacon.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        assembly_beacon.tick().await;

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
        // terminal, or an important custom message — F5), BEFORE the
        // handlers run — including landings their dedup gates will drop
        // (a duplicate means the original ack was lost or the replay
        // raced it; not re-acking would replay forever). A no-op for
        // every other frame, so the handlers stay seq-oblivious.
        self.ack_delivery_report(&msg).await;

        match msg.msg_type() {
            MessageType::SecondaryWelcome => self.handle_welcome(msg).await,
            MessageType::CertExchange => self.handle_cert_exchange(msg),
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
            // Consumer custom message (F5): droppable → direct handler
            // dispatch; important → CRDT-post + the handler-dispatch
            // decision. The ack echo for an important landing already
            // ran above (`ack_delivery_report` — every
            // `delivery_seq`-stamped landing, including dedup-dropped
            // duplicates).
            MessageType::CustomMessage => self.handle_custom_message(msg, command_rx).await,
            // Reconciliation-probe verdict arm (#308): a holder
            // secondary's answer to this primary's `TaskHoldQuery`.
            // `held` re-arms the per-task deadline; `not held` fails +
            // requeues the lost task through the backpressure-shaped
            // `handle_task_failed` path. See
            // `primary::reconciliation_probe`.
            MessageType::TaskHoldResponse => self.handle_task_hold_response(msg, command_rx).await,
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
            // `RequestClusterSnapshot` to `Destination::Primary`; the
            // primary's `cluster_state` is the authoritative copy, so its
            // snapshot is the strongest bootstrap payload. Pre-fix only
            // the secondary router answered this — a request addressed at
            // the primary fell through the catch-all and timed out. See
            // `handle_request_cluster_snapshot`.
            MessageType::RequestClusterSnapshot => self.handle_request_cluster_snapshot(msg).await,
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
            // Anti-entropy pull-reply arm. The snapshot this primary's own
            // `handle_state_digest` requested from a proven-ahead peer.
            // Pre-fix this fell through the catch-all and the pull never
            // converged — the deposed-zombie starvation (see
            // `handle_cluster_snapshot`).
            MessageType::ClusterSnapshot => self.handle_cluster_snapshot(msg),
            other => {
                tracing::debug!(?other, "unhandled message type");
            }
        }
        Ok(())
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
        let reporter = reporter.to_string();
        let ack = DistributedMessage::TerminalAck {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: super::wire::timestamp_now(),
            seq,
        };
        if let Err(e) = self
            .send_to(
                dynrunner_protocol_primary_secondary::Destination::Secondary(
                    dynrunner_protocol_primary_secondary::PeerId::from(reporter.as_str()),
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
    /// Returns `true` (assignable) iff the member has confirmed its
    /// peer-mesh leg formed (`mesh_ready_secondaries` — a `MeshReady`
    /// was received from it). Nothing else counts: keepalives are
    /// liveness, not leg confirmation, and "never dispatched to" is not
    /// proof of a working leg.
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
    /// recovers the member into the assignable set).
    ///
    /// Read by [`Self::should_skip_worker_for_dispatch`] — the single
    /// owner of the per-worker dispatch-skip decision — so BOTH
    /// operational dispatch paths (`dispatch_to_idle_workers` and
    /// `handle_task_request`) gate on this ONE predicate without either
    /// site knowing the mesh-readiness rule.
    pub(super) fn member_mesh_confirmed(&self, secondary_id: &str) -> bool {
        self.mesh_ready_secondaries.contains(secondary_id)
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

    pub(super) fn handle_cert_exchange(&mut self, msg: DistributedMessage<I>) {
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
            } else {
                self.secondaries.insert(secondary_id, state);
            }
        }
    }

    // ── Phase 3: Send Peer Lists ──
}
