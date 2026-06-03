//! The two trait shapes that gate the setup-phase channel: a
//! per-secondary [`SetupBootstrap`] (1:1 wire to the primary) and a
//! 1:N [`SetupBootstrapBroadcast`] (primary's fan-out + multiplexed
//! recv). Wire only — see the [`super::adapters`] module for the
//! impls that wrap the existing operational transport.

use dynrunner_core::Identifier;

use crate::setup_bootstrap::message::SetupBootstrapMessage;

/// Secondary-side bootstrap channel: a 1:1 wire to the primary.
///
/// The secondary's setup sequence is:
///   1. `send(SecondaryWelcome{...})`
///   2. `send(CertExchange{...})`
///   3. `recv()` until [`SetupBootstrapMessage::PeerInfo`] arrives
///
/// After step 3 the bootstrap retires and operational messaging takes
/// over on [`PeerTransport`]. The trait method count stays at two; if
/// a future change wants to thread additional setup-phase frames
/// through here, the right move is to add a [`SetupBootstrapMessage`]
/// variant — not a new trait method — so the narrow-type guarantee
/// stays load-bearing.
///
/// [`PeerTransport`]: crate::PeerTransport
pub trait SetupBootstrap<I: Identifier> {
    /// Send one setup-phase frame to the primary.
    fn send(
        &mut self,
        msg: SetupBootstrapMessage,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next setup-phase frame from the primary, or `None`
    /// if the underlying wire closed.
    ///
    /// Non-setup frames that arrive during the setup window (extremely
    /// rare — the setup phase completes in milliseconds) are logged at
    /// `warn` and skipped; the next call returns the next setup-eligible
    /// frame. The caller does not see operational traffic through this
    /// surface — that's the structural guarantee Step 10 enforces.
    fn recv(&mut self) -> impl std::future::Future<Output = Option<SetupBootstrapMessage>>;
}

/// Primary-side bootstrap channel: a 1:N fan-out to every connected
/// secondary plus a recv multiplexed across them.
///
/// The primary's setup sequence is:
///   - `recv()` until every secondary has emitted `SecondaryWelcome` +
///     `CertExchange` (orchestrated by
///     `dynrunner_manager_distributed::primary::connect::wait_for_connections`)
///   - `broadcast(PeerInfo{...})` once enough secondaries have completed
///     cert exchange (orchestrated by
///     `dynrunner_manager_distributed::primary::peer_setup::send_peer_lists`)
///
/// Asymmetric to [`SetupBootstrap`] because the primary doesn't
/// per-secondary unicast at setup — `PeerInfo` is a fan-out, and
/// `SecondaryWelcome` / `CertExchange` arrive from N secondaries onto
/// the same recv loop.
pub trait SetupBootstrapBroadcast<I: Identifier> {
    /// Fan-out a setup-phase frame to every connected secondary.
    /// Partial-failure summary is preserved by the underlying
    /// [`SecondaryTransport::broadcast`]; the adapter folds it into a
    /// `String` for the trait shape.
    fn broadcast(
        &mut self,
        msg: SetupBootstrapMessage,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next setup-phase frame from any connected secondary,
    /// or `None` if the underlying wire closed. Non-setup frames are
    /// logged at `warn` and skipped (same semantics as
    /// [`SetupBootstrap::recv`]).
    fn recv(&mut self) -> impl std::future::Future<Output = Option<SetupBootstrapMessage>>;
}
