use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    pub(super) async fn wait_for_connections(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        tracing::info!("waiting for {} secondaries", self.config.num_secondaries);

        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let expected = self.config.num_secondaries as usize;

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
                        return Err(format!(
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
                        ));
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
                    break;
                }
            }
        }

        // Whole-cluster-ready milestone (the LLM-wake "connected to
        // gateway" event): every connected secondary has completed
        // cert-exchange, so the mesh is formed and the run can proceed.
        // Emitted at the importance target so the dual-sink routes it
        // to stdio under `--important-stdio-only` while the full log
        // keeps it too.
        tracing::info!(
            target: super::important_events::IMPORTANT_TARGET,
            secondaries = self.secondaries.len(),
            "all secondaries connected",
        );
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
        // Every cross-secondary message bumps the per-secondary heartbeat,
        // not just `Keepalive`. A secondary that's actively processing
        // tasks shouldn't be falsely declared dead just because keepalives
        // are sparser than task traffic.
        self.record_keepalive(msg.sender_id());

        match msg.msg_type() {
            MessageType::SecondaryWelcome => self.handle_welcome(msg).await,
            MessageType::CertExchange => self.handle_cert_exchange(msg),
            MessageType::TaskRequest => self.handle_task_request(msg).await?,
            MessageType::TaskComplete => self.handle_task_complete(msg, command_rx).await,
            MessageType::TaskFailed => self.handle_task_failed(msg, command_rx).await,
            MessageType::MeshReady => self.handle_mesh_ready(msg),
            MessageType::Keepalive => { /* tracked above, no further action */ }
            MessageType::SecondaryFatalError => self.handle_secondary_fatal_error(msg).await?,
            // Replicated cluster ledger maintenance. Without this arm
            // the demoted local primary cannot observe completions
            // forwarded only via the CRDT bus (the cross-secondary
            // case after promotion); see `handle_cluster_mutation`
            // for the full rationale and the asm-dataset-nix R2 / T3
            // hang it pins.
            MessageType::ClusterMutation => self.handle_cluster_mutation(msg).await,
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
            other => {
                tracing::debug!(?other, "unhandled message type");
            }
        }
        Ok(())
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
            }
        }
    }

    /// THE single dispatch-readiness predicate every assignment path
    /// consults: may proactive dispatch assign MORE work to
    /// `secondary_id`, or is it a half-joined member whose mesh leg never
    /// confirmed?
    ///
    /// Returns `true` (assignable) unless the member is BOTH:
    ///   1. already dispatched-to ([`Self::members_dispatched_to`] — it
    ///      got at least one task, so its first/bootstrap dispatch already
    ///      happened), AND
    ///   2. has NOT confirmed its peer-mesh leg formed
    ///      (`mesh_ready_secondaries` — no `MeshReady` received).
    ///
    /// # Single owner of the dispatch readiness gate
    ///
    /// A member that got work but never confirmed its mesh leg is
    /// half-joined: its terminals ride a half-formed mesh egress leg that
    /// silently swallows them (route present at the transport `has_peer`
    /// view, send returns `Ok`, but the frame never reaches the authority
    /// — the run_20260610_105906 strand). Piling MORE work on it strands
    /// each task and wedges the phase barrier. So it is withheld from
    /// EVERY operational path until a `MeshReady` lands.
    ///
    /// The first/bootstrap dispatch is NEVER gated (condition 1): the
    /// bring-up recovery during `wait_for_mesh_ready`
    /// (`drain_and_react_to_pending_worker_signals`) dispatches to a member
    /// that joined after the initial-assignment snapshot, and that dispatch
    /// is precisely what drives it operational so it CAN emit the
    /// `MeshReady` the wait blocks on — gating it would re-create the
    /// assigned=0 deadlock. The terminal of that first dispatch, if the leg
    /// IS half-formed, is caught by the secondary-side
    /// buffered-terminal-replay (the Half-B honesty fix), so the
    /// first-dispatch exemption never loses a terminal.
    ///
    /// The SOLE writer of `mesh_ready_secondaries` is
    /// [`Self::handle_mesh_ready`] (unconditional, so a late `MeshReady`
    /// recovers the member into the assignable set). The SOLE writer of
    /// `members_dispatched_to` is `originate_task_assigned`.
    ///
    /// Read by [`Self::should_skip_worker_for_dispatch`] — the single
    /// owner of the per-worker dispatch-skip decision — so BOTH
    /// operational dispatch paths (`dispatch_to_idle_workers` and
    /// `handle_task_request`) gate on this ONE predicate without either
    /// site knowing the mesh-readiness rule.
    pub(super) fn member_mesh_confirmed(&self, secondary_id: &str) -> bool {
        // Not-yet-dispatched member: its first (bootstrap) dispatch is
        // always allowed.
        if !self.members_dispatched_to.contains(secondary_id) {
            return true;
        }
        // Already got work: must have confirmed its mesh leg to get more.
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
            tracing::info!(
                secondary = %secondary_id,
                workers = worker_count,
                ram_gb = ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                is_observer,
                "secondary connected"
            );

            // Capture the advertised capacity before `resources` is
            // moved into the per-secondary connection state below, so
            // the same welcome originates the static `SecondaryCapacity`
            // record into the replicated ledger (see the broadcast
            // batch below). `worker_count` is `Copy`; `resources` is
            // cloned once.
            let advertised_resources = resources.clone();
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
