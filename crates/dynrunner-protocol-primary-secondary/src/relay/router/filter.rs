//! Inbound-package filter: the verdict an opaque closure returns for
//! each package the [`Router`] is about to deliver onward.
//!
//! The filter is installed on the [`Router`] (the single inbound
//! dispatch seam both transports delegate to) via
//! [`Router::install_filter`]. It runs at the exact point a package
//! would become [`crate::relay::InboundOutcome::Deliver`] — i.e. on
//! packages destined for THIS node's consumer, never on the router's
//! own Relay / RelayBackoff control frames (those are consumed one
//! layer below, before the filter sees anything).
//!
//! # The mesh knows nothing of what the filter decides
//!
//! The closure is `FnMut(DistributedMessage<I>) -> Verdict<I>`: it
//! takes ownership of the package and returns one of three verdicts.
//! The Router applies the verdict mechanically and has zero knowledge
//! of WHY — there is no primary / CRDT / demote awareness here. A
//! caller that wants to bounce primary-addressed packets with a
//! redirect builds that redirect package itself and returns it inside
//! [`Verdict::Bounce`]; the Router only sends/delivers/drops.
//!
//! [`Router`]: super::Router

use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;

/// What the installed inbound filter decided for one package the
/// [`Router`](super::Router) was about to deliver to the local
/// consumer.
///
/// Default behaviour with no filter installed is equivalent to every
/// package yielding `Accept(package)` — pure pass-through.
pub enum Verdict<I: Identifier> {
    /// Discard the package: it is neither delivered to the consumer
    /// nor sent anywhere.
    Drop,
    /// Do NOT deliver; instead send the carried package back to the
    /// ORIGINAL sender of the inbound package, reusing the Router's
    /// existing send path (direct-or-relay). The carried package is
    /// the caller's to construct — commonly a redirect/error reply.
    Bounce(DistributedMessage<I>),
    /// Deliver the carried package onward to the consumer. In the
    /// common case this is the same package unchanged (identity),
    /// but a filter may transform it.
    Accept(DistributedMessage<I>),
}

/// The boxed closure stored on the Router. `FnMut` so a filter may
/// carry mutable state across packages; `Send` so it can live on the
/// transport task that owns the Router (and thus persist after
/// whatever installed it is gone).
pub type InboundFilter<I> = Box<dyn FnMut(DistributedMessage<I>) -> Verdict<I> + Send>;
