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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preparation::{PrepError, PreparationOptions, SlurmPreparation};
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Reader whose info files NEVER appear (every rebuild sticks in the
    /// poll loop forever — the dead-peer shape) while recording which
    /// ids' paths were polled at all.
    #[derive(Clone, Default)]
    struct RecordingStuckReader {
        polled: Arc<Mutex<HashSet<String>>>,
    }

    impl crate::preparation::InfoFileReader for RecordingStuckReader {
        fn read(
            &self,
            path: String,
        ) -> impl std::future::Future<Output = Result<Option<String>, PrepError>> + 'static
        {
            let polled = Arc::clone(&self.polled);
            async move {
                polled.lock().expect("polled mutex").insert(path);
                Ok(None)
            }
        }
    }

    /// The mass-disconnect survivor-starvation pin (specimen 1 of
    /// run_20260611_200548): the per-id rebuilds must run CONCURRENTLY.
    /// With a roster whose ids all stick in their (unbounded) info-file
    /// poll, a sequential walk never gets past the FIRST id — the
    /// remaining ids' info files are never even read, which is exactly
    /// how six dead peers starved the five live survivors' rebuilds for
    /// minutes per cadence tick. Concurrent rebuilds poll every id's
    /// info file within the first poll periods.
    #[tokio::test(flavor = "current_thread")]
    async fn rebuilds_run_concurrently_not_sequentially() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut opts = PreparationOptions::new(
                    "/tmp/dynrunner-obsreconnect-test".into(),
                    "gw.invalid".into(),
                    None,
                    22,
                    vec![],
                    vec![],
                );
                opts.poll_interval = Duration::from_millis(5);
                let prep = Arc::new(SlurmPreparation::new(opts));
                let reader = RecordingStuckReader::default();
                // Seed the captured primary QUIC port (the reestablish
                // precondition) without establishing anything: a
                // zero-secondary cohort touches no ssh.
                prep.setup_ssh_tunnels(reader.clone(), 0, 51200)
                    .await
                    .expect("zero-secondary setup seeds the primary port");

                let reconnector =
                    SlurmPreparationTunnelReconnector::new(Arc::clone(&prep), reader.clone());
                let ids: Vec<String> = vec!["sec-0".into(), "sec-1".into(), "sec-2".into()];
                tokio::task::spawn_local(async move {
                    reconnector.reconnect(&ids).await;
                });

                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    let n = reader.polled.lock().expect("polled mutex").len();
                    if n >= 3 {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "only {n} of 3 ids' info files were ever polled — the \
                         rebuild walk is sequential and the first (stuck/dead) \
                         id starves every other rebuild"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
    }
}
