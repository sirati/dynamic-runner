//! Reverse-SSH-tunnel port for the respawn flow.
//!
//! Owns the [`TunnelEstablisher`] trait — the seam the spawner uses to
//! ask "bring up the reverse tunnel for this secondary id" without
//! depending on the concrete `SlurmPreparation` struct — and the
//! production binding [`SlurmPreparationTunnelEstablisher`] that wires
//! that trait through to
//! [`SlurmPreparation::establish_one_tunnel`]. Tests in
//! [`tests`](super::tests) supply an in-memory stub to drive the
//! spawner contract without spinning up a real `ssh -N -R`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::preparation::{InfoFileReader, PrepError, SlurmPreparation};

/// Port that brings up the reverse SSH tunnel for a just-spawned
/// secondary. Production wires this to
/// [`SlurmPreparation::establish_one_tunnel`] via the blanket impl in
/// this module; tests pass an in-memory stub so the spawner contract
/// can be exercised without `ssh -N -R`.
///
/// Single concern: "given a new secondary id, ensure the reverse
/// tunnel is up". The spawner does not need to know about the
/// connection-info polling, the `Semaphore` rate-limiter, or the
/// shared `ssh_tunnels` cleanup Vec — those are owned by the
/// production implementation.
///
/// `&self` (not `&mut self`) matches the underlying
/// `establish_one_tunnel` shape, which already runs under `&self`
/// thanks to the `Arc<StdMutex<...>>` shared state inside
/// `SlurmPreparation`.
pub trait TunnelEstablisher: Send + Sync {
    fn establish_one_tunnel<'a>(
        &'a self,
        secondary_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PrepError>> + 'a>>;
}

/// Production binding of [`TunnelEstablisher`] to
/// [`SlurmPreparation::establish_one_tunnel`]. Captures the
/// `InfoFileReader` once (the trait method's signature does not carry
/// the reader, so it has to live on the binding) and re-uses the
/// shared `Arc<SlurmPreparation>` so respawn tunnels join the same
/// cleanup set as the initial cohort.
pub struct SlurmPreparationTunnelEstablisher<R: InfoFileReader + Send + Sync> {
    preparation: Arc<SlurmPreparation>,
    info_reader: R,
}

impl<R: InfoFileReader + Send + Sync> SlurmPreparationTunnelEstablisher<R> {
    pub fn new(preparation: Arc<SlurmPreparation>, info_reader: R) -> Self {
        Self {
            preparation,
            info_reader,
        }
    }
}

impl<R: InfoFileReader + Send + Sync> TunnelEstablisher for SlurmPreparationTunnelEstablisher<R> {
    fn establish_one_tunnel<'a>(
        &'a self,
        secondary_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PrepError>> + 'a>> {
        let reader = self.info_reader.clone();
        Box::pin(async move {
            self.preparation
                .establish_one_tunnel(secondary_id, reader)
                .await
        })
    }
}
