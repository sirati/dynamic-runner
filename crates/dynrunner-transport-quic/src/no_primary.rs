//! No-op primary transport for peer-only mesh participants.
//!
//! # Concern
//!
//! Single primitive for "this node never speaks the primary-secondary
//! wire protocol — every meaningful exchange travels via the peer
//! mesh". Sibling stub to [`crate::NoPeerTransport`]: where
//! `NoPeerTransport` is for single-secondary deployments that don't
//! need a peer overlay, `NoPrimaryTransport` is for late-joining
//! observer dispatchers that join the mesh via
//! [`dynrunner_protocol_primary_secondary::PeerTransport::join_running_cluster`]
//! and rely exclusively on the peer mesh (including
//! `Address::Role(Role::Primary)` over a Step-5b
//! `TunneledPeerTransport`) for outbound, plus the snapshot RPC for
//! initial state.
//!
//! # Why a stub (not a real `NetworkClient`)
//!
//! The late-joiner observer has no `primary_url` to dial — peer-info
//! files (Step 7's connection_info dir) carry SECONDARY connection
//! data, not the primary's listening URL (the primary publishes that
//! via the gateway / reverse-tunnel path the dispatcher already owns
//! at construction time). The observer's design contract is "join
//! by peer mesh, not by primary handshake": the
//! `restore_from_snapshot_and_skip_setup` entry on
//! `SecondaryCoordinator` is the load-bearing latch that skips the
//! welcome / cert-exchange / wait-for-setup phases (the ONLY phases
//! that send through `primary_transport`), and the steady-state
//! processing loop never invokes `primary_transport.send` from an
//! observer (every `.send` site is gated on either
//! `is_primary` — observers can never be primary, per
//! `SecondaryConfig.is_observer` — or on completion/setup paths a
//! zero-worker observer never reaches; the audit trail is in
//! `secondary/processing.rs:380`, `secondary/resource.rs:168`,
//! `secondary/setup.rs:57/80`, `secondary/primary.rs:814`).
//!
//! On the recv side the processing loop's `select!` arm
//! (`processing.rs:75`) is gated by `!self.primary_disconnected`; with
//! this stub the recv future is `pending::<None>()` and the arm
//! simply never fires, which is the desired observability shape (no
//! spurious "primary disconnected" cascades, no synthesized
//! disconnect on a stub that has nothing to disconnect from).
//!
//! # Lifetime
//!
//! Lives here for the duration of Step 9 (late-joiner CLI) through
//! Step 11 (PrimaryTransport / SecondaryTransport trait deletion in
//! the transport-unification refactor). Once Step 11 lands, the
//! observer's `SecondaryCoordinator` no longer carries a generic
//! `PT: PrimaryTransport<I>` parameter at all and this stub becomes
//! unreachable code — at which point it deletes alongside the trait.

use std::future;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// No-op stand-in for a [`PrimaryTransport`] when the node participates
/// in the cluster purely via the peer mesh (late-joining observer).
///
/// # Behaviour
///
/// - `send(msg)` returns `Ok(())` immediately, discarding `msg`. Every
///   call site for `primary_transport.send` in the secondary coordinator
///   either swallows errors (`let _ = …`) or is gated behind state
///   (`is_primary`, `setup_phase_completed=false`, etc.) that an
///   observer cannot reach. Returning `Err` here would surface false
///   positives in those `Err`-aware sites; `Ok(())` matches the
///   observer's contract ("the call was harmless; the message simply
///   has no recipient on the primary-handshake channel because that
///   channel doesn't exist for me").
/// - `recv()` returns a `pending::<None>()` future: the processing
///   loop's `select!` arm has nothing to fire on, every iteration is
///   driven by the peer-transport / worker-pool / timer arms instead.
///   Yielding `None` instead would synthesise a primary-disconnect
///   that the `handle_primary_disconnect` path would then react to,
///   surfacing failover cascades on a transport that was never
///   connected in the first place.
///
/// # Cancel safety
///
/// `recv` returns a `pending` future that has no observable state
/// transitions — dropping it on `select!` arm cancellation is a no-op,
/// satisfying the [`MessageReceiver`] cancel-safety contract by
/// vacuous truth.
///
/// [`PrimaryTransport`]: dynrunner_protocol_primary_secondary::PrimaryTransport
pub struct NoPrimaryTransport;

impl<I: Identifier> MessageSender<DistributedMessage<I>> for NoPrimaryTransport {
    async fn send(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> {
        Ok(())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for NoPrimaryTransport {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        future::pending().await
    }
}
