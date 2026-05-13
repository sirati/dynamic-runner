use dynrunner_core::{Identifier, MessageReceiver, MessageSender};

use crate::address::{Address, Role, RoleChangeHookRegistrar, Scope};
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

    /// Broadcast a message to every connected secondary.
    ///
    /// Implementations must drain pending new connections before iterating so a
    /// secondary whose handshake completed since the last poll is not silently
    /// skipped. Per-peer failures are returned as `(secondary_id, error)`
    /// pairs; the broadcast itself succeeds (`Ok(())`) when every peer's
    /// outgoing channel accepted the message. Callers choose the log severity
    /// for partial failures (e.g. `debug` for high-cadence keepalives, `warn`
    /// for low-cadence control messages).
    fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), Vec<(String, String)>>>;
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

    /// Send a message via role-aware addressing.
    ///
    /// Default implementation routes the well-known address shapes to
    /// the existing primitives:
    ///   - `Address::Peer(id)` → `send_to_peer(id, msg)`
    ///   - `Address::Broadcast(Scope::Mesh)` → `broadcast(msg)`
    ///
    /// `Address::Role(_)` and `Address::Broadcast(Scope::AllSecondaries)`
    /// return `Err` with a clear "not yet supported" message — Steps 2-3
    /// of the unification refactor will implement them on top of the
    /// `RoleTable` cache. Until then, callers MUST keep using the
    /// existing send_to_peer/broadcast for their concerns; this entry
    /// point is the migration target, not yet load-bearing.
    fn send(
        &mut self,
        addr: Address,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>> {
        async move {
            match addr {
                Address::Peer(id) => self.send_to_peer(&id, msg).await,
                Address::Broadcast(Scope::Mesh) => self.broadcast(msg).await,
                Address::Role(role) => Err(format!(
                    "Address::Role({role:?}) not yet supported (Step 3 of unification refactor); \
                     callers must continue using send_to_peer/broadcast"
                )),
                Address::Broadcast(Scope::AllSecondaries) => Err(
                    "Address::Broadcast(AllSecondaries) not yet supported (Step 3); \
                     callers must continue using SecondaryTransport::broadcast"
                        .into(),
                ),
            }
        }
    }

    /// Attach this transport's write-through role cache to the
    /// authoritative [`RoleTable`] owner. The registrar is the
    /// downstream `ClusterState` (or a test fixture implementing
    /// [`RoleChangeHookRegistrar`]).
    ///
    /// Default impl is a no-op: transports that don't keep a
    /// role-cache (e.g. `NoPeerTransport`, or the channel transport
    /// in tests that never exercise role addressing) compile cleanly
    /// without overriding. Real transports override to register a
    /// hook that writes their local `HashMap<Role, String>` cache
    /// whenever the authoritative table mutates — that's how Step 3
    /// gets a lock-free read of "who is primary now" on the send
    /// hot path.
    ///
    /// The registration is one-shot; callers invoke this once at
    /// coordinator construction.
    fn register_with_cluster_state(&self, _registrar: &mut dyn RoleChangeHookRegistrar) {}

    /// Look up the current id of whoever holds `role` per this
    /// transport's local write-through cache.
    ///
    /// Default impl returns `None` — transports without a cache
    /// silently report "no holder", which is the safe answer
    /// upstream (Step 3's role dispatch will surface `None` as a
    /// no-route error, not a mis-send).
    ///
    /// Real transports override to read their cached map populated
    /// by the hook registered via [`Self::register_with_cluster_state`].
    /// The returned `String` is a clone — the cache stays locked for
    /// the minimum window.
    fn peer_for_role(&self, _role: &Role) -> Option<String> {
        None
    }
}
