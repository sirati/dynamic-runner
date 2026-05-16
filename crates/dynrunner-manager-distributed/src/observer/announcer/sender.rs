//! Production `AnnouncerSender` impl that bridges the announcer task
//! to the coordinator's `peer_transport` through an outbox channel.
//!
//! # Why an outbox, not `Arc<Mutex<P>>`
//!
//! `PeerTransport`'s methods take `&mut self`, the trait isn't object-
//! safe (each method returns an `impl Future`), and the coordinator
//! owns the concrete `P` by value. Sharing the transport across two
//! tasks would require either an invasive type-signature change on
//! every coordinator-wide site that names the transport, or a global
//! `tokio::sync::Mutex<P>` that serialises every send against the
//! coordinator's own. The outbox+reply pattern keeps `peer_transport`
//! pinned to the run loop (where `&mut self` already lives) and the
//! announcer's send concern stays expressible as the existing
//! [`AnnouncerSender`](super::types::AnnouncerSender) trait —
//! `send_holdings` returns `Result<(), String>`, which the
//! retry-with-backoff loop in
//! [`run_observer_announcer`](super::task::run_observer_announcer)
//! interprets exactly as before.

use super::types::{AnnouncerSender, PeerResourceHoldingsUpdatedPayload};

/// One queued send request flowing from the spawned announcer task
/// into the [`crate::SecondaryCoordinator`]'s operational `select!`
/// loop. The coordinator owns `peer_transport` by value and its
/// methods take `&mut self`, so the announcer (a sibling task) cannot
/// touch the transport directly; instead it posts an item onto the
/// outbox and awaits the loop's send outcome through `reply`.
///
/// # Single concern of this struct
///
/// Carry one in-flight send request across the announcer →
/// coordinator boundary. The reply oneshot is the per-call
/// flow-control signal: the announcer's `send_holdings` awaits it
/// before returning, so the trait's `Result<(), String>` contract is
/// preserved even though the actual transport call happens in a
/// different task.
pub struct AnnouncerOutboxItem<I: dynrunner_core::Identifier> {
    pub msg: dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    pub reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

/// Production [`AnnouncerSender`] impl that forwards every
/// announcement onto a coordinator-side outbox. The coordinator's
/// operational `select!` drains the outbox and issues the actual
/// `peer_transport.send(Address::Role(Role::Primary), msg)` call,
/// reporting the outcome back via `reply`.
///
/// # Module boundary
///
/// - **Held by**: the observer's announcer task, one instance per
///   [`run_observer_announcer`](super::task::run_observer_announcer)
///   invocation.
/// - **Talks to**: the coordinator's announcer-outbox `mpsc::Sender`,
///   cloned in at construction.
/// - **Does NOT touch**: `peer_transport` directly. The outbox is the
///   only boundary it crosses.
///
/// # Wire shape
///
/// Each `send_holdings(body)` call rewraps `body` (field-for-field)
/// into:
///
/// ```ignore
/// DistributedMessage::ClusterMutation {
///     sender_id: <observer's secondary_id>,
///     timestamp: <now>,
///     mutations: vec![
///         ClusterMutation::PeerResourceHoldingsUpdated {
///             peer_id: body.peer_id,
///             holdings: body.holdings,
///             epoch:    body.epoch,
///         },
///     ],
/// }
/// ```
///
/// The coordinator sends this with `Address::Role(Role::Primary)`,
/// which the `PeerTransport::send` default impl wraps in a
/// `RoleAddressed` envelope and routes through the write-through
/// `RoleTable` cache (Step 3 / Step 4 of the unification refactor).
/// Cache-cold `Address::Role` lookups error with a typed message —
/// that surfaces back through `reply` as `Err`, which trips the
/// retry-with-backoff loop in
/// [`run_observer_announcer`](super::task::run_observer_announcer);
/// the next retry will see a populated cache once `PromotePrimary`
/// has applied.
pub struct PeerMeshAnnouncerSender<I: dynrunner_core::Identifier> {
    /// Sender-id stamped onto every `DistributedMessage::ClusterMutation`
    /// the announcer emits. Equal to the observer's `secondary_id` —
    /// the same value that lands in `PeerResourceHoldingsUpdatedPayload::peer_id`.
    /// Cached at construction so each `send_holdings` call doesn't
    /// have to re-borrow it through the outbox.
    sender_id: String,
    /// Cloned coordinator-side outbox handle. The coordinator drops
    /// the matching receiver only on shutdown; the announcer observes
    /// a `send`-error in that case and surfaces it as the loop's
    /// retry trigger (which never converges, but the announcer
    /// task's `abort()` from the observer wire-up site terminates the
    /// task itself before that becomes observable).
    outbox_tx: tokio::sync::mpsc::Sender<AnnouncerOutboxItem<I>>,
}

impl<I: dynrunner_core::Identifier> PeerMeshAnnouncerSender<I> {
    /// Construct the production sender. The `sender_id` is stamped
    /// onto every wire envelope and is the observer's `secondary_id`;
    /// `outbox_tx` is cloned from the coordinator's
    /// `announcer_outbox_tx` field at attach time.
    pub fn new(
        sender_id: String,
        outbox_tx: tokio::sync::mpsc::Sender<AnnouncerOutboxItem<I>>,
    ) -> Self {
        Self {
            sender_id,
            outbox_tx,
        }
    }
}

/// Inline timestamp source for the production sender. Lifted out of
/// the inline `send_holdings` body so the test below can pin the
/// exact wire-frame shape without observing the system clock — the
/// timestamp field is `f64` and the test asserts only that it's a
/// non-NaN finite value, never an exact equality.
fn announcer_timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl<I: dynrunner_core::Identifier> AnnouncerSender for PeerMeshAnnouncerSender<I> {
    async fn send_holdings(
        &mut self,
        body: &PeerResourceHoldingsUpdatedPayload,
    ) -> Result<(), String> {
        use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
        let mutation = ClusterMutation::<I>::PeerResourceHoldingsUpdated {
            peer_id: body.peer_id.clone(),
            holdings: body.holdings.clone(),
            epoch: body.epoch,
        };
        let msg = DistributedMessage::<I>::ClusterMutation {
            sender_id: self.sender_id.clone(),
            timestamp: announcer_timestamp_now(),
            mutations: vec![mutation],
        };
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // Outbox close (receiver dropped) maps to the same error
        // shape `send_holdings` already documents — the retry loop
        // will sleep and re-try. The retry never converges in that
        // shutdown case; the announcer task's abort-on-exit (held by
        // the observer wire-up site) is what terminates it.
        self.outbox_tx
            .send(AnnouncerOutboxItem {
                msg,
                reply: reply_tx,
            })
            .await
            .map_err(|e| format!("announcer outbox closed: {e}"))?;
        // The coordinator's drain arm awaits `peer_transport.send`
        // and forwards its `Result<(), String>` outcome verbatim. A
        // dropped `reply_tx` (drain-arm panic or coordinator shutdown
        // mid-send) surfaces as `RecvError`, which also trips the
        // retry loop.
        reply_rx
            .await
            .map_err(|e| format!("announcer outbox reply dropped: {e}"))?
    }
}
