//! Primary-side helper for emitting StageFile notifications.
//!
//! The primary does NOT transfer file payloads itself: a separate
//! pipeline (packaging.preparation, SSH copy, etc.) places the file
//! on the shared drive (`src_network`) — or out-of-band on the
//! secondary's host — and then asks us to tell the secondary "this
//! file is now available at `<src_path>`; please stage it to
//! `<dest_path>` so the next TaskAssignment for it resolves cleanly."

use db_comm_api_base::Identifier;
use db_primary_secondary_comm::{DistributedMessage, SecondaryTransport};
use db_scheduler_api::{ResourceEstimator, Scheduler};

use super::wire::timestamp_now;
use super::PrimaryCoordinator;

impl<T, S, E, I> PrimaryCoordinator<T, S, E, I>
where
    T: SecondaryTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator,
    I: Identifier,
{
    /// Send a `StageFile` notification to a specific secondary.
    ///
    /// `src_path` is interpreted by the secondary relative to its
    /// configured `src_network` (when relative) or as an absolute
    /// path (out-of-band SSH-staged source). `dest_path` is always
    /// relative to the secondary's `src_tmp`.
    pub async fn notify_stage_file(
        &mut self,
        secondary_id: &str,
        file_hash: String,
        src_path: String,
        dest_path: String,
    ) -> Result<(), String> {
        let msg = DistributedMessage::StageFile {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: secondary_id.to_string(),
            file_hash,
            src_path,
            dest_path,
        };
        self.transport.send_to(secondary_id, msg).await
    }

    /// Drain `self.pending_stage_files` and emit each as a StageFile
    /// wire message via `notify_stage_file`. Logs (but does not
    /// propagate) per-message send errors so a single dead secondary
    /// does not abort the whole startup.
    pub(super) async fn flush_pending_stage_files(&mut self) -> Result<(), String> {
        let pending = std::mem::take(&mut self.pending_stage_files);
        if pending.is_empty() {
            return Ok(());
        }
        let count = pending.len();
        for (secondary_id, file_hash, src_path, dest_path) in pending {
            if let Err(e) = self
                .notify_stage_file(&secondary_id, file_hash.clone(), src_path, dest_path)
                .await
            {
                tracing::error!(
                    secondary_id = %secondary_id,
                    file_hash = %file_hash,
                    error = %e,
                    "failed to flush queued StageFile"
                );
            }
        }
        tracing::info!(count, "flushed queued StageFile notifications");
        Ok(())
    }
}
