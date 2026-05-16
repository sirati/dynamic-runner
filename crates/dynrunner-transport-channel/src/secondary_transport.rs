//! Primary ↔ Secondary channel transports.
//!
//! `ChannelSecondaryTransportEnd` is the primary-side aggregator
//! that holds one outgoing sender per secondary and a single
//! incoming receiver fanning in from all of them.
//! `ChannelPrimaryTransportEnd` is the secondary-side single-peer
//! handle.

use std::collections::HashMap;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{DistributedMessage, SecondaryTransport};
use tokio::sync::mpsc;

/// Channel-based transport for the primary side of distributed coordination.
///
/// Holds per-secondary outgoing senders and a single incoming receiver
/// that aggregates messages from all secondaries.
pub struct ChannelSecondaryTransportEnd<I: Identifier> {
    pub outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    pub incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelSecondaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.recv().await
    }
}

impl<I: Identifier> SecondaryTransport<I> for ChannelSecondaryTransportEnd<I> {
    async fn send_to(&mut self, secondary_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), Vec<(String, String)>> {
        let mut errors = Vec::new();
        for (secondary_id, tx) in &self.outgoing {
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

/// Channel-based transport for the secondary side of distributed coordination.
///
/// Sends to the primary and receives from it via unbounded mpsc channels.
pub struct ChannelPrimaryTransportEnd<I: Identifier> {
    pub tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    pub rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.recv().await
    }
}
