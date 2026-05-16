//! Public types crossing the announcer's API boundary.
//!
//! Single concern: define the three types that the announcer task, the
//! producer of `AnnounceTrigger` (the role-change hook), and the
//! transport-side `AnnouncerSender` implementor all share. None of
//! these types own behaviour beyond what `derive` macros provide; the
//! task body and the sender impl live in sibling modules.

/// Unit signal pushed onto the announcer's channel by the role-change
/// hook (and by the initial post-restore fire). Carries no payload —
/// the announcer reads the current epoch from the shared mirror at
/// send time, so the trigger is a pure "go ahead" notification and
/// stale captures are impossible by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnounceTrigger;

/// Typed body that crosses the announcer → transport boundary. Mirrors
/// the wire variant `ClusterMutation::PeerResourceHoldingsUpdated {
/// peer_id, holdings, epoch }` field-for-field so the production
/// [`PeerMeshAnnouncerSender`](super::sender::PeerMeshAnnouncerSender)
/// impl rewraps it into the real `ClusterMutation` with a mechanical
/// field-by-field copy. The body type itself is the announcer's
/// stable internal payload representation, decoupling the observer's
/// task lifecycle from any downstream wire-variant rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerResourceHoldingsUpdatedPayload {
    pub peer_id: String,
    pub holdings: Vec<String>,
    pub epoch: u64,
}

/// Transport-side delivery boundary the announcer talks through.
///
/// Single concern: convert a typed announcement body into a single
/// best-effort attempt at delivering it to whichever peer currently
/// holds the primary role. Returning `Err(msg)` triggers the
/// announcer's retry-with-backoff loop; the error is logged at the
/// announcer level but not otherwise inspected.
///
/// The trait is `&mut self` rather than `&self` because the
/// production impl owns an outbox channel handle that may need
/// mutable access on send; the channel handle itself can be cloned
/// before construction to deal with multi-call concurrency.
pub trait AnnouncerSender {
    fn send_holdings(
        &mut self,
        body: &PeerResourceHoldingsUpdatedPayload,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}
