//! Outbound dispatch capability for one peer connection.
//!
//! The relay router owns peer-mesh routing decisions but never holds
//! transport-specific state. To dispatch a message it borrows a
//! per-peer connection record from the transport's connection map and
//! invokes [`OutboundChannel::dispatch`] on it. The transport supplies
//! the connection-record type; this trait is the single boundary
//! between routing decisions and transport-specific send mechanics.
//!
//! Both the QUIC `PeerNetwork` and the in-process
//! `ChannelPeerTransport` already store
//! `tokio::sync::mpsc::UnboundedSender<DistributedMessage<I>>` as the
//! value type in their connection maps, so the blanket impl below is
//! the only one required for current consumers. A new transport gains
//! relay parity by impl'ing this trait for whatever its connection
//! record happens to be — no copy-paste of the dispatch state machine.
//!
//! # Error semantics
//!
//! `dispatch` is consumed-on-`Err` — there is no retry. The `Err`
//! arm signals that the underlying connection is dead so the caller
//! can drop the connection record from its map and surface the
//! failure as a routing decision (relay or no-route on the next
//! attempt).

use crate::messages::DistributedMessage;
use dynrunner_core::Identifier;

/// Capability to dispatch one message to the peer this channel
/// addresses. See module docs for error semantics.
pub trait OutboundChannel<I: Identifier> {
    fn dispatch(&self, msg: DistributedMessage<I>) -> Result<(), ()>;
}

impl<I: Identifier> OutboundChannel<I>
    for tokio::sync::mpsc::UnboundedSender<DistributedMessage<I>>
{
    fn dispatch(&self, msg: DistributedMessage<I>) -> Result<(), ()> {
        self.send(msg).map_err(|_| ())
    }
}
