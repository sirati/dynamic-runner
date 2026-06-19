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
        // Delegate to the classified path (the batched #675 squeue probe)
        // and discard the pending-Resources count: the per-secondary
        // verdict is identical between the two, so this both reuses the
        // single batched `squeue -j <list>` call and avoids a duplicate
        // classification loop.
        self.probe_all_classified().await.0
    }

    /// Probe every secondary AND count how many are PENDING with reason
    /// "Resources". The reason field is the squeue `%r` column; "Resources"
    /// is the SLURM scheduler's signal that the job cannot schedule because
    /// the partition has insufficient capacity (nodes, CPUs, memory) to
    /// satisfy the request at any point with the current allocation. Jobs
    /// pending for any other reason (Priority, Dependency, QOSMaxCpuPerUser,
    /// …) are NOT counted — they may schedule once higher-priority work
    /// clears, so they are NOT unschedulable on the partition's capacity.
    async fn probe_all_classified(
        &self,
    ) -> (std::collections::HashMap<String, PeerLifeState>, usize) {
        let entries: Vec<(String, String)> = {
            let mgr = self.manager.lock().await;
            mgr.secondary_jobs_snapshot()
        };
        // Batch the per-secondary squeue probes into ONE
        // `squeue -j <id1>,<id2>,…` round-trip (#675). The returned map
        // carries the SAME per-job `JobStatusInfo` the per-job path would,
        // with a job ABSENT from the queue mapping to the missing snapshot
        // (`state_kind == None`) — IDENTICAL to the per-job probe's
        // empty/non-zero result. Classification below is therefore
        // unchanged per job; only the number of squeue calls (N→1) differs.
        let job_ids: Vec<String> = entries.iter().map(|(_, job_id)| job_id.clone()).collect();
        let statuses = {
            let mgr = self.manager.lock().await;
            mgr.get_job_status_batch(&job_ids).await
        };
        let mut out = std::collections::HashMap::with_capacity(entries.len());
        let mut pending_resources: usize = 0;
        for (secondary_id, job_id) in entries {
            // A transport `Err` on the batched query degrades the whole
            // probe to Unknown — the same direction the per-job path took
            // when its single `get_job_status` returned `Err`.
            let info = match &statuses {
                Ok(map) => map.get(&job_id),
                Err(_) => None,
            };
            let life = match info {
                Some(info) => match info.state_kind {
                    Some(crate::job_manager::JobStatus::Pending) => {
                        // "Resources" is the reason SLURM prints when the
                        // job cannot schedule on the partition's capacity.
                        if info.reason == "Resources" {
                            pending_resources += 1;
                        }
                        PeerLifeState::Alive
                    }
                    Some(crate::job_manager::JobStatus::Running)
                    | Some(crate::job_manager::JobStatus::Unknown(_)) => PeerLifeState::Alive,
                    Some(crate::job_manager::JobStatus::Completed)
                    | Some(crate::job_manager::JobStatus::Failed)
                    | Some(crate::job_manager::JobStatus::Cancelled) => PeerLifeState::Gone,
                    None => {
                        // squeue has no row — check sacct for terminal state.
                        let mgr2 = self.manager.lock().await;
                        match mgr2.sacct_terminal_state_pub(&job_id).await {
                            Some(_) => PeerLifeState::Gone,
                            None => PeerLifeState::Unknown,
                        }
                    }
                },
                None => PeerLifeState::Unknown,
            };
            out.insert(secondary_id, life);
        }
        (out, pending_resources)
    }
}
