use dynrunner_core::{Identifier, MessageReceiver, MessageSender};

use crate::DistributedMessage;

/// Transport trait for the primary side: can send to specific secondaries, receive from any.
///
/// The addressing (`send_to` with a secondary ID) is a protocol-level concern
/// that sits on top of the base `MessageSender`/`MessageReceiver` traits.
pub trait SecondaryTransport<I: Identifier>:
    MessageReceiver<DistributedMessage<I>>
{
    /// Send a message to a specific secondary.
    fn send_to(
        &mut self,
        secondary_id: &str,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

/// Transport trait for the secondary side: send to / receive from the primary.
///
/// This is a simple bidirectional channel, equivalent to
/// `MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>`.
pub trait PrimaryTransport<I: Identifier>:
    MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>
{
}

impl<T, I> PrimaryTransport<I> for T
where
    I: Identifier,
    T: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
{
}

/// Transport trait for peer-to-peer communication between secondaries.
///
/// Supports broadcasting to all connected peers, sending to a specific peer,
/// and receiving messages from any peer.
pub trait PeerTransport<I: Identifier> {
    /// Broadcast a message to all connected peers.
    fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Send a message to a specific peer.
    fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next message from any peer.
    fn recv_peer(&mut self) -> impl std::future::Future<Output = Option<DistributedMessage<I>>>;

    /// Try to receive a message without blocking. Returns `None` if no message is available.
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>>;

    /// The number of connected peers.
    fn peer_count(&self) -> usize;

    /// Connect to peers from the peer list received from primary.
    fn connect_to_peers(
        &mut self,
        peers: &[crate::PeerConnectionInfo],
    ) -> impl std::future::Future<Output = ()>;
}
