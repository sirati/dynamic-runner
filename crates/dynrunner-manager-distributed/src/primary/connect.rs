use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, MessageType, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
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
                msg = self.transport.recv_peer() => {
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
    /// `drain_empty_active_phases` poll. Pre-loop callers
    /// (`wait_for_connections`, `wait_for_mesh_ready`) and post-loop
    /// callers (`drain_pending_messages`) pass `&mut None`: at those
    /// moments PyPrimaryHandle is either dormant (run hasn't entered
    /// the operational loop yet) or the loop has already exited and
    /// won't re-enter, so no in-runtime callback path needs draining.
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
    pub(super) fn handle_mesh_ready(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::MeshReady {
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
            self.mesh_ready_secondaries.insert(secondary_id);
        }
    }

    pub(super) async fn handle_welcome(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::SecondaryWelcome {
            secondary_id,
            resources,
            worker_count,
            hostname,
            is_observer,
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
            let conn =
                conn.receive_welcome(worker_count, resources, hostname, 0, None, is_observer);
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
                },
                ClusterMutation::SecondaryCapacity {
                    secondary: secondary_id,
                    worker_count,
                    resources: advertised_resources,
                },
            ])
            .await;
        }
    }

    pub(super) fn handle_cert_exchange(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::CertExchange {
            secondary_id,
            public_cert_pem,
            ipv4_address,
            ipv6_address,
            quic_port,
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
