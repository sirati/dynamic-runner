/// Generic typed message sender.
///
/// Transport crates implement this for the relevant message types.
/// How serialization is handled is up to the transport — channel transports
/// pass messages directly, byte-oriented transports use the codec utilities
/// provided by the protocol comm API crates.
pub trait MessageSender<M> {
    fn send(
        &mut self,
        msg: M,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

/// Generic typed message receiver.
///
/// Returns `None` when the connection is closed.
///
/// # Cancellation safety
///
/// Implementations **must** be cancellation-safe: if the future returned
/// by `recv` is dropped before completing (e.g. because a sibling arm of
/// `tokio::select!` won), no message is permanently lost — the next call
/// to `recv` resumes from the same logical position.
///
/// The recommended way to satisfy this contract is the bridge pattern
/// already used by every concrete impl in this workspace: the public
/// `recv` is a thin wrapper around `tokio::sync::mpsc::Receiver::recv`
/// (documented cancel-safe), and a per-connection reader task owns the
/// underlying byte/socket stream and pushes decoded messages into the
/// channel. Dropping a `recv` future then drops only the mpsc-side
/// future; the reader task keeps reading regardless. See
/// `dynrunner-transport-quic`'s `network/client.rs`,
/// `network/accept.rs`, and `peer/handler.rs` for concrete examples.
///
/// Direct `&mut self` await over a network read inside a `select!` arm
/// is unsafe unless the underlying read primitive is itself documented
/// cancel-safe (e.g. quinn `RecvStream::read`); the bridge pattern
/// avoids relying on that invariant.
pub trait MessageReceiver<M> {
    fn recv(&mut self) -> impl std::future::Future<Output = Option<M>>;
}
