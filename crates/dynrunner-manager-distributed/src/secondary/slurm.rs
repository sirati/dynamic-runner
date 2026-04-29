use std::collections::HashSet;

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, PeerTransport, PrimaryTransport,
    TaskListEntry,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) fn populate_slurm_tasks(
        &mut self,
        all_tasks: Vec<TaskListEntry<I>>,
        completed: HashSet<String>,
    ) {
        self.slurm_completed = completed.clone();
        self.slurm_pending_binaries.clear();

        for task in all_tasks {
            if completed.contains(&task.hash)
                || self.completed_tasks.contains(&task.hash)
                || self.active_tasks.contains_key(&task.hash)
            {
                continue;
            }

            let path = task.file_path.as_deref().unwrap_or(&task.local_path);

            // Try to resolve via extraction cache first
            let resolved = self
                .extraction_cache
                .resolve_binary(None, path, &task.hash);

            let binary_path = resolved.unwrap_or_else(|| std::path::PathBuf::from(path));

            // Hydrate phase/type/affinity/payload from the wire and
            // override the resolved path. Single source of truth for
            // wire→TaskInfo lives in `DistributedBinaryInfo::to_task_info`.
            let mut binary = task.binary_info.to_task_info();
            binary.path = binary_path;
            self.slurm_pending_binaries.push(binary);
        }

        // Sort by size descending for better packing
        self.slurm_pending_binaries.sort_by(|a, b| b.size.cmp(&a.size));

        tracing::info!(
            pending = self.slurm_pending_binaries.len(),
            completed = self.slurm_completed.len(),
            "populated SLURM-primary task list"
        );
    }

    /// Handle a task request from a peer when acting as SLURM-primary.
    /// Finds a suitable task and sends a TaskAssignment back.
    pub(super) async fn handle_slurm_task_request(
        &mut self,
        requesting_secondary_id: String,
        worker_id: WorkerId,
        available_memory: u64,
    ) -> Result<(), String> {
        if self.slurm_pending_binaries.is_empty() {
            tracing::debug!(
                secondary = %requesting_secondary_id,
                worker_id,
                "no pending tasks for SLURM-primary assignment"
            );
            return Ok(());
        }

        // Remove any tasks that have been completed since population
        self.slurm_pending_binaries.retain(|b| {
            let hash = format!("{:016x}", {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                b.path.hash(&mut h);
                b.identifier.hash(&mut h);
                h.finish()
            });
            !self.completed_tasks.contains(&hash)
        });

        if self.slurm_pending_binaries.is_empty() {
            return Ok(());
        }

        // Find a task that fits the available memory
        let mut assigned_idx = None;
        for (i, binary) in self.slurm_pending_binaries.iter().enumerate() {
            let estimated = self.estimator.estimate(binary);
            if estimated.get(&dynrunner_core::ResourceKind::memory()) <= available_memory {
                assigned_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = assigned_idx {
            let binary = self.slurm_pending_binaries.remove(idx);
            let file_hash = format!("{:016x}", {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                binary.path.hash(&mut hasher);
                binary.identifier.hash(&mut hasher);
                hasher.finish()
            });

            if requesting_secondary_id == self.config.secondary_id {
                // Assign directly to local worker (avoid recursive dispatch_message cycle)
                let resolved = self
                    .extraction_cache
                    .resolve_binary(None, &binary.path.to_string_lossy(), &file_hash);
                let actual_binary = match resolved {
                    Some(path) => {
                        let mut b = binary.clone();
                        b.path = path;
                        b
                    }
                    None => binary.clone(),
                };
                let estimated = self.estimator.estimate(&actual_binary);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
                if self.pool.workers[wid as usize].is_idle_state() {
                    match self.pool.workers[wid as usize]
                        .assign_task(actual_binary, estimated, false)
                        .await
                    {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, wid);
                            self.reset_request_backoff(wid);
                        }
                        Err(e) => {
                            tracing::error!(worker_id = wid, error = %e, "failed to assign SLURM task locally");
                        }
                    }
                }
            } else {
                // Send TaskAssignment to peer
                let msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: requesting_secondary_id.clone(),
                    worker_id,
                    zip_file: None,
                    binary_info: DistributedBinaryInfo::from_task_info(&binary),
                    local_path: binary.path.to_string_lossy().into_owned(),
                    file_hash,
                };
                let _ = self
                    .peer_transport
                    .send_to_peer(&requesting_secondary_id, msg)
                    .await;
            }

            tracing::info!(
                secondary = %requesting_secondary_id,
                worker_id,
                binary = ?binary.identifier,
                remaining = self.slurm_pending_binaries.len(),
                "SLURM-primary assigned task"
            );
        }

        Ok(())
    }
}
