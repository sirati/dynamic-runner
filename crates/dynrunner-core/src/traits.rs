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
pub trait MessageReceiver<M> {
    fn recv(&mut self) -> impl std::future::Future<Output = Option<M>>;
}
