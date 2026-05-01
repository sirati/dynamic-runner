//! Primary-side helper for emitting StageFile notifications.
//!
//! The primary does NOT transfer file payloads itself: a separate
//! pipeline (packaging.preparation, SSH copy, etc.) places the file
//! on the shared drive (`src_network`) — or out-of-band on the
//! secondary's host — and then asks us to tell the secondary "this
//! file is now available at `<src_path>`; please stage it to
//! `<dest_path>` so the next TaskAssignment for it resolves cleanly."

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, SecondaryTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::wire::timestamp_now;
use super::PrimaryCoordinator;

impl<T, S, E, I> PrimaryCoordinator<T, S, E, I>
where
    T: SecondaryTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
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

}
