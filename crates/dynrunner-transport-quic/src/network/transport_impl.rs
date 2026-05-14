//! `MessageReceiver` + `SecondaryTransport` impls for `NetworkServer`.
//! Inherent methods stay in `mod.rs`; this file is purely the
//! trait-glue layer.

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::{DistributedMessage, SecondaryTransport};

use super::NetworkServer;

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for NetworkServer<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        // Drain any new connections before checking for messages
        self.drain_new_connections();

        // Use select to also drain new connections that arrive while waiting
        loop {
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.drain_new_connections();
                    // Inbound fan-out: when paired with a
                    // `TunneledPeerTransport` via `attach_tunnel`,
                    // clone-forward each message into the peer
                    // view's queue. This is the SOLE site that
                    // taps inbound — `transport.recv()` callers
                    // remain the canonical inbound consumer
                    // (Step 5b leaves that contract intact); the
                    // peer queue accumulates harmlessly until
                    // Step 6 lands the demoted-primary
                    // `select! { peer_transport.recv_peer() }`
                    // arm that drains it.
                    if let (Some(m), Some(tap)) = (msg.as_ref(), self.inbound_tap()) {
                        // `Clone` is bounded on the impl so this is
                        // a plain message clone (one mpsc-frame
                        // allocation); `DistributedMessage` is the
                        // same shape every other peer-mesh transport
                        // already clones on broadcast.
                        let _ = tap.send(DistributedMessage::clone(m));
                    }
                    return msg;
                }
                accepted = self.new_conn_rx.recv() => {
                    if let Some(accepted) = accepted {
                        tracing::info!(secondary = %accepted.secondary_id, "secondary registered (during recv)");
                        // Mirror into the shared peer view's
                        // writer table when paired — same
                        // contract as `drain_new_connections`,
                        // applied to the in-recv accept path so
                        // the two registration sites stay in
                        // lockstep.
                        if let Some(shared) = self.shared_outgoing_for_tap() {
                            shared
                                .borrow_mut()
                                .insert(accepted.secondary_id.clone(), accepted.outgoing_tx.clone());
                        }
                        self.connections.insert(accepted.secondary_id, accepted.outgoing_tx);
                    }
                }
            }
        }
    }
}

impl<I: Identifier> SecondaryTransport<I> for NetworkServer<I> {
    async fn send_to(
        &mut self,
        secondary_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // Drain any pending new connections first
        self.drain_new_connections();

        if let Some(tx) = self.connections.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())
        } else {
            Err(format!("no connection for secondary '{secondary_id}'"))
        }
    }

    async fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), Vec<(String, String)>> {
        self.drain_new_connections();
        let mut errors = Vec::new();
        for (secondary_id, tx) in &self.connections {
            if let Err(e) = tx.send(msg.clone()) {
                errors.push((secondary_id.clone(), e.to_string()));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
