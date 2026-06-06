use std::collections::HashMap;
use std::marker::PhantomData;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType};
use tokio::sync::mpsc;

/// A message tagged with its source.
#[derive(Debug)]
pub struct RoutedMessage<I: Identifier> {
    pub message: DistributedMessage<I>,
    pub source_id: String,
}

/// Routes incoming distributed messages to typed mpsc channels.
///
/// Instead of Python's dynamic `register_handler(type_string, callback)`,
/// the Rust router uses a fixed set of typed channels. The coordinator
/// holds the receivers; the router holds senders.
pub struct MessageRouter<I: Identifier> {
    pub node_id: String,
    senders: HashMap<MessageType, mpsc::UnboundedSender<RoutedMessage<I>>>,
    _marker: PhantomData<I>,
}

impl<I: Identifier> MessageRouter<I> {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            senders: HashMap::new(),
            _marker: PhantomData,
        }
    }

    /// Register a channel for a specific message type.
    /// Returns the receiving end.
    pub fn register(&mut self, msg_type: MessageType) -> mpsc::UnboundedReceiver<RoutedMessage<I>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.insert(msg_type, tx);
        rx
    }

    /// Route an incoming message to its registered channel.
    /// Returns false if no handler is registered for this message type.
    pub fn route(&self, message: DistributedMessage<I>) -> bool {
        let msg_type = message.msg_type();
        let source_id = message.sender_id().to_string();
        if let Some(tx) = self.senders.get(&msg_type) {
            let _ = tx.send(RoutedMessage { message, source_id });
            true
        } else {
            tracing::warn!(?msg_type, "no handler registered for message type");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_protocol_primary_secondary::KeepaliveRole;
    use serde::{Deserialize, Serialize};

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[test]
    fn route_to_registered_channel() {
        let mut router: MessageRouter<TestId> = MessageRouter::new("primary".into());
        let mut rx = router.register(MessageType::Keepalive);

        let msg = DistributedMessage::Keepalive {
            target: None,
            sender_id: "sec-0".into(),
            timestamp: 1.0,
            secondary_id: "sec-0".into(),
            active_workers: 2,
            emitter_role: KeepaliveRole::Secondary,
        };

        assert!(router.route(msg));
        let routed = rx.try_recv().unwrap();
        assert_eq!(routed.source_id, "sec-0");
        match routed.message {
            DistributedMessage::Keepalive { active_workers, .. } => {
                assert_eq!(active_workers, 2);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn unregistered_type_returns_false() {
        let router: MessageRouter<TestId> = MessageRouter::new("primary".into());
        let msg = DistributedMessage::Keepalive {
            target: None,
            sender_id: "sec-0".into(),
            timestamp: 1.0,
            secondary_id: "sec-0".into(),
            active_workers: 2,
            emitter_role: KeepaliveRole::Secondary,
        };
        assert!(!router.route(msg));
    }
}
