//! SLURM binding of the observer's transport-recovery port.
//!
//! Single concern: implement
//! [`dynrunner_manager_distributed::observer::TunnelReconnector`] over the
//! SAME [`SlurmPreparation`] the initial setup + the respawn path use, so a
//! relocated submitter→observer can rebuild its dropped `-R` reverse
//! tunnels on lost visibility. Each lost peer id maps to one
//! [`SlurmPreparation::establish_one_tunnel`] call — the existing primitive
//! that re-polls the connection-info file, re-spawns `ssh -N -R`, and joins
//! the shared cleanup set, all without the caller knowing ssh.
//!
//! # Why the observer needs this (and the respawn `SecondarySpawner` does not cover it)
//!
//! The respawn pipeline rebuilds a tunnel for a NEW secondary the primary
//! deliberately re-spawned. This port rebuilds the EXISTING per-secondary
//! `-R` tunnels when they drop out from under a passive observer (an ssh
//! blip / `ServerAliveCountMax` exhaustion — no auto-reconnect on the ssh
//! side). The observer carries zero authority; it never re-spawns SLURM
//! jobs. It only restores the wire its compute peers dial it over. So this
//! is its own port, reusing the same underlying establishment primitive.
//!
//! # Boundary
//!
//! `dynrunner-manager-distributed` owns the trait + the "when/who" (the
//! observer triggers on lost visibility with its CRDT roster);
//! `dynrunner-slurm` owns the "how" (the `ssh -N -R` rebuild). The pyo3
//! layer wires the production binding onto the submitter primary via
//! `set_tunnel_reconnector`, exactly as it wires the respawn spawner via
//! `enable_respawn`. The role layer never names ssh.

use std::sync::Arc;

use async_trait::async_trait;
use dynrunner_manager_distributed::observer::TunnelReconnector;

use crate::preparation::{InfoFileReader, SlurmPreparation};

/// Production binding of [`TunnelReconnector`] to
/// [`SlurmPreparation::establish_one_tunnel`]. Holds the SAME
/// `Arc<SlurmPreparation>` the cohort-setup + respawn paths share (so a
/// rebuilt tunnel re-joins the same `ssh_tunnels` cleanup set + reuses the
/// rate-limiter), plus the [`InfoFileReader`] the trait method's signature
/// does not carry.
pub struct SlurmPreparationTunnelReconnector<R: InfoFileReader + Send + Sync> {
    preparation: Arc<SlurmPreparation>,
    info_reader: R,
}

impl<R: InfoFileReader + Send + Sync> SlurmPreparationTunnelReconnector<R> {
    /// Construct an observer-reconnect binding over a shared
    /// `SlurmPreparation`. Precondition (inherited from
    /// [`SlurmPreparation::establish_one_tunnel`]): `setup_ssh_tunnels`
    /// must have run at least once on this instance so the primary's QUIC
    /// port is captured.
    pub fn new(preparation: Arc<SlurmPreparation>, info_reader: R) -> Self {
        Self {
            preparation,
            info_reader,
        }
    }
}

#[async_trait(?Send)]
impl<R: InfoFileReader + Send + Sync + 'static> TunnelReconnector
    for SlurmPreparationTunnelReconnector<R>
{
    async fn reconnect(&self, peer_ids: &[String]) {
        // Rebuild each lost peer's `-R` reverse tunnel. Best-effort +
        // idempotent: a peer whose tunnel is already healthy simply
        // re-polls its info file and re-establishes (the observer's loop
        // retries on the next ~60s cadence tick if any fail). Per-id
        // failures are logged, never propagated — an observer carries zero
        // authority and a tunnel rebuild is never a run error.
        //
        // `reestablish_one_tunnel` (NOT `establish_one_tunnel`): on an
        // UNGRACEFUL drop (SIGKILL / NIC blip / crash) the worker's sshd
        // still holds the old `-R <tunnel_port>` listener with no
        // FIN/RST to release it, so re-spawning `ssh -R <same_port>`
        // fails every attempt with rc=255 "remote port forwarding failed
        // (port in use)". The reestablish path first force-releases that
        // stale worker-side binding, then rebinds the SAME port (the
        // port is the worker's fixed listen port — a fresh port would
        // break the worker's `localhost:<tunnel_port>` dial with no
        // re-coordination path). The graceful-close path already has the
        // port free, so the release is a harmless no-op there.
        for peer_id in peer_ids {
            match self
                .preparation
                .reestablish_one_tunnel(peer_id, self.info_reader.clone())
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        secondary_id = %peer_id,
                        "observer rebuilt reverse tunnel (BUG-B reconnect)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        secondary_id = %peer_id,
                        error = %e,
                        "observer reverse-tunnel rebuild failed; will retry on the next \
                         lost-visibility cadence tick"
                    );
                }
            }
        }
    }
}
