/// Generic typed message sender.
///
/// Transport crates implement this for the relevant message types.
/// How serialization is handled is up to the transport — channel transports
/// pass messages directly, byte-oriented transports use the codec utilities
/// provided by the protocol comm API crates.
///
/// # Flush semantics
///
/// `send` only guarantees the message has been accepted by the sender —
/// for direct-wire transports (e.g. `WssConnection::send`, the channel
/// transports in `dynrunner-transport-channel`) that's identical to "on
/// the wire / delivered to the in-process receiver". For bridged
/// transports that hand the message to a writer task via an internal
/// mpsc (e.g. `NetworkClient` in `dynrunner-transport-quic::network`),
/// `send.await` returns as soon as the mpsc enqueue succeeds; the wire
/// write happens asynchronously inside the writer task.
///
/// `flush` is the rendezvous primitive that lets a caller wait until
/// every previously-enqueued message has been written to the underlying
/// wire (or wire-equivalent for in-process transports). It is the
/// contract that lets a shutdown path emit a final message and observe
/// it as delivered before tearing down its runtime — without it, the
/// `Drop` of a bridged transport aborts its writer task and any messages
/// still queued at the mpsc are silently lost.
///
/// The default implementation is a no-op `Ok(())`. Direct-wire transports
/// (where every `send.await` already returns post-wire) inherit that
/// default. Bridged transports MUST override it with a rendezvous that
/// observes the writer task has drained the queue up to and including
/// any messages enqueued before the `flush` call.
pub trait MessageSender<M> {
    fn send(&mut self, msg: M) -> impl std::future::Future<Output = Result<(), String>>;

    /// Block until every previously-enqueued send has reached the
    /// underlying wire (or wire-equivalent for in-process transports).
    ///
    /// Returns `Ok(())` on a clean drain; returns `Err` if the
    /// writer task has exited / the connection is torn down (in which
    /// case any not-yet-flushed messages are lost — callers should
    /// log and proceed).
    ///
    /// The default no-op is correct for transports where `send.await`
    /// already returns post-wire (`WssConnection`,
    /// `ChannelPrimaryTransportEnd`, etc.). Bridged transports
    /// (`NetworkClient`) override with a rendezvous on the writer task.
    fn flush(&mut self) -> impl std::future::Future<Output = Result<(), String>> {
        async { Ok(()) }
    }
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
