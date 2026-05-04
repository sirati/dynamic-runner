
use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, MessageType,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn wait_for_connections(&mut self) -> Result<(), String> {
        tracing::info!("waiting for {} secondaries", self.config.num_secondaries);

        let deadline = tokio::time::Instant::now() + self.config.connect_timeout;
        let expected = self.config.num_secondaries as usize;

        loop {
            // Check if all secondaries have completed cert exchange
            let cert_done = self.secondaries.values()
                .filter(|s| s.is_at_least_cert_exchanged())
                .count();
            if cert_done >= expected {
                break;
            }

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => return Err("transport closed".into()),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(format!(
                        "timeout waiting for secondaries: {}/{} sent SecondaryWelcome \
                         (transport-level connection accept happens lazily on first \
                         message, so 0/N here can mean either no peer ever connected \
                         OR connections completed handshake but never sent Welcome — \
                         in the latter case the per-connection accept handler \
                         should have logged a 'peer connected but did not send \
                         SecondaryWelcome within Ns; closing as non-conformant' \
                         line in the transport log; that points at the consumer's \
                         worker_module not completing the runner protocol's Ready \
                         handshake)",
                        self.secondaries.len(),
                        expected
                    ));
                }
            }
        }

        tracing::info!("all {} secondaries connected", self.secondaries.len());
        Ok(())
    }

    /// Central message dispatcher — routes incoming messages by type.
    pub(super) async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // Every cross-secondary message bumps the per-secondary heartbeat,
        // not just `Keepalive`. A secondary that's actively processing
        // tasks shouldn't be falsely declared dead just because keepalives
        // are sparser than task traffic.
        self.record_keepalive(msg.sender_id());

        match msg.msg_type() {
            MessageType::SecondaryWelcome => self.handle_welcome(msg),
            MessageType::CertExchange => self.handle_cert_exchange(msg),
            MessageType::TaskRequest => self.handle_task_request(msg).await?,
            MessageType::TaskComplete => self.handle_task_complete(msg).await,
            MessageType::TaskFailed => self.handle_task_failed(msg).await,
            MessageType::Keepalive => { /* tracked above, no further action */ }
            other => {
                tracing::debug!(?other, "unhandled message type");
            }
        }
        Ok(())
    }

    pub(super) fn handle_welcome(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::SecondaryWelcome {
            secondary_id,
            resources,
            worker_count,
            hostname,
            ..
        } = msg
        {
            let ram_bytes = resources.iter()
                .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                .map(|r| r.amount)
                .unwrap_or(0);
            tracing::info!(
                secondary = %secondary_id,
                workers = worker_count,
                ram_gb = ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                "secondary connected"
            );

            let conn = SecondaryConnection::new(secondary_id.clone());
            let conn = conn.receive_welcome(worker_count, resources, hostname, 0, None);
            self.secondaries.insert(
                secondary_id.clone(),
                SecondaryConnectionState::Handshaking(conn),
            );
            self.seed_keepalive(&secondary_id);
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
        {
            if let Some(state) = self.secondaries.remove(&secondary_id) {
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
    }

    // ── Phase 3: Send Peer Lists ──

}
