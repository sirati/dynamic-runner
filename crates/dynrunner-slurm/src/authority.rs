//! Off-loop slurm-authoritative life-state probe.
//!
//! # Concern
//! ONE: ask SLURM (squeue + sacct) whether a secondary's job is still alive,
//! and surface a 3-state answer to the framework. The result is the latest
//! answer SLURM gave; staleness/freshness is handled at the snapshot layer
//! (see `dynrunner_manager_distributed::authority_snapshot`).
//!
//! # Module boundary
//! The `PeerLifeState` enum and `SlurmAuthorityProbe` trait live in
//! `dynrunner-manager-distributed::authority_snapshot` (the framework owns
//! the contract). This module supplies the SLURM-provider IMPL of that
//! trait, `SlurmJobManagerProbe`, which queries the shared
//! `SlurmJobManager` for each `secondary_id → job_id → squeue/sacct`
//! life-state read.
//!
//! # Why off-loop
//! The coordinator's `select!` loop can wedge in any arm (apply_spawn_tasks
//! under #66 affine-ON is the documented case, #547). Inline-async queries
//! that run on the coordinator loop would never fire during the very incident
//! they're meant to neutralise. The probe runs on its OWN `tokio::spawn`'d
//! task; consumers read a lock-free SNAPSHOT.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::authority_snapshot::{PeerLifeState, SlurmAuthorityProbe};

use crate::job_manager::{JobStatus, SlurmJobManager};

/// Probe binding over a shared `SlurmJobManager`. The manager owns the
/// `secondary_id → job_id` table; this binding consults `get_job_status`
/// (squeue) and falls through to `sacct_terminal_state_pub` when squeue
/// has no row. Lock discipline: only synchronous probes happen under the
/// manager mutex (cheap map reads); each gateway round-trip re-acquires
/// the lock so concurrent submissions are not starved by the probe.
pub struct SlurmJobManagerProbe<G: Gateway> {
    manager: Arc<Mutex<SlurmJobManager<G>>>,
}

impl<G: Gateway + Send + Sync + 'static> SlurmJobManagerProbe<G> {
    pub fn new(manager: Arc<Mutex<SlurmJobManager<G>>>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl<G: Gateway + Send + Sync + 'static> SlurmAuthorityProbe for SlurmJobManagerProbe<G> {
    async fn peer_life(&self, secondary_id: &str) -> PeerLifeState {
        // Resolve secondary_id → job_id under the lock, then release the
        // lock before the squeue round-trip so concurrent submissions are
        // not starved by the probe.
        let job_id = {
            let mgr = self.manager.lock().await;
            match mgr.secondary_jobs_get(secondary_id) {
                Some(id) => id.clone(),
                None => return PeerLifeState::Unknown,
            }
        };
        let mgr = self.manager.lock().await;
        match mgr.get_job_status(&job_id).await {
            Ok(info) => match info.state_kind {
                Some(JobStatus::Pending)
                | Some(JobStatus::Running)
                | Some(JobStatus::Unknown(_)) => PeerLifeState::Alive,
                Some(JobStatus::Completed)
                | Some(JobStatus::Failed)
                | Some(JobStatus::Cancelled) => PeerLifeState::Gone,
                None => match mgr.sacct_terminal_state_pub(&job_id).await {
                    Some(_) => PeerLifeState::Gone,
                    None => PeerLifeState::Unknown,
                },
            },
            Err(_) => PeerLifeState::Unknown,
        }
    }

    async fn probe_all(&self) -> std::collections::HashMap<String, PeerLifeState> {
        let entries: Vec<(String, String)> = {
            let mgr = self.manager.lock().await;
            mgr.secondary_jobs_snapshot()
        };
        let mut out = std::collections::HashMap::with_capacity(entries.len());
        for (secondary_id, _job_id) in entries {
            let life = self.peer_life(&secondary_id).await;
            out.insert(secondary_id, life);
        }
        out
    }
}
