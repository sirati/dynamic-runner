//! The observer's transport-recovery port.
//!
//! # Single concern
//!
//! ONE concern: the seam the zero-authority observer crosses to ask
//! "rebuild my path to these peers" WITHOUT owning any of the recovery
//! mechanism. The observer's transport (the relocated submitter's
//! [`dynrunner_transport_tunnel::TunneledPeerTransport`]) reaches the
//! compute mesh over per-secondary ssh reverse (`-R`) tunnels: the
//! compute peers DIAL the submitter through those tunnels (the submitter
//! never dials out — `connect_to_peers` is a no-op there, and there is
//! no QUIC reconnect ticker on that transport). So when a `-R` tunnel
//! drops (an external ssh blip / `ServerAliveCountMax` exhaustion), the
//! compute peer can no longer reach the observer and the observer's
//! transport has NO way to re-establish the link on its own.
//!
//! The owner directive (2026-06-09): "an observer that looses connection
//! should try to build a new ssh tunnel and reconnect of cource. if all
//! connection is lost they do not shut down, they report that connection
//! is lost and try reconnecting after a minute."
//!
//! # The boundary (modularity: observer triggers, the tunnel layer owns `-R`)
//!
//! The observer coordinator does NOT own ssh management. It holds a
//! [`TunnelReconnector`] and, on lost visibility (the
//! [`super::lost_visibility::LostVisibilityReporter`]'s retry cadence),
//! calls [`TunnelReconnector::reconnect`] with the peer ids it expects to
//! be reachable (its CRDT roster). The CONCRETE rebuild of the `-R`
//! tunnel — info-file polling, the `ssh -N -R` argv, the rate-limiter,
//! the cleanup-set bookkeeping — is owned ENTIRELY by the provider layer
//! (`dynrunner-slurm`'s `SlurmPreparation::establish_one_tunnel`, wrapped
//! into a [`TunnelReconnector`] impl and wired at the pyo3 boundary). The
//! observer sees only "rebuild the path to these ids"; it never names ssh.
//!
//! This mirrors the respawn boundary
//! ([`crate::primary::respawn::SecondarySpawner`]): the trait lives here
//! in `manager-distributed`, the SLURM binding lives in `dynrunner-slurm`,
//! and the role layer depends on neither the ssh primitives nor the
//! concrete preparation struct. A node whose transport CAN re-establish
//! the link itself (the late-joiner over a real `PeerNetwork`, which has
//! its own QUIC reconnect ticker + dial path) simply has NO reconnector —
//! the observer holds `Option<Arc<dyn TunnelReconnector>>` and the absent
//! case is the "transport heals itself" path.

use std::sync::Arc;

/// Port the observer crosses to rebuild its transport path to a set of
/// peers it has lost visibility to.
///
/// `#[async_trait(?Send)]` because the production binding drives an
/// `ssh -N -R` subprocess whose future is not `Send` (the same provider
/// physics that forces the `?Send` bound on
/// [`crate::primary::respawn::SecondarySpawner`]). The observer run loop
/// is `LocalSet`-bound for exactly this reason. The trait object stays
/// `Send + Sync` so an `Arc<dyn TunnelReconnector>` is moveable.
///
/// Single concern: "given the ids I have lost, restore my path to them".
/// The implementation owns HOW (rebuild the `-R` per id); the observer
/// owns only WHEN (lost visibility) and WHO (its roster). The call MUST
/// be non-blocking for the observer loop — a production impl spawns the
/// per-id rebuild work detached and returns immediately, so the observer
/// keeps observing/narrating while the rebuild proceeds and its
/// visibility flips back once a compute peer re-dials over the rebuilt
/// tunnel.
#[async_trait::async_trait(?Send)]
pub trait TunnelReconnector: Send + Sync {
    /// Attempt to restore the transport path to each id in `peer_ids`.
    /// Best-effort + idempotent: rebuilding a tunnel whose link is
    /// already healthy is harmless, and a failed rebuild is retried on
    /// the next lost-visibility cadence tick — never surfaced as a run
    /// error (an observer carries zero authority).
    async fn reconnect(&self, peer_ids: &[String]);
}

/// A reconnector handle the observer holds. `None` on a path whose
/// transport re-establishes its own links (the late-joiner's
/// `PeerNetwork` QUIC reconnect ticker); `Some` on the relocated
/// submitter→observer path whose `TunneledPeerTransport` needs the `-R`
/// rebuilt out-of-band.
pub type ReconnectorHandle = Option<Arc<dyn TunnelReconnector>>;
