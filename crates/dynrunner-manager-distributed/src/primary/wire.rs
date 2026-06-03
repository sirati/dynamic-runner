use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::DistributedBinaryInfo;

// `compute_task_hash` is the wire-canonical task content hash; it now
// lives in `dynrunner-core` so both the distributed manager (this
// crate) and the local manager (`dynrunner-manager-local`) share one
// definition. Re-exported here under the historical path so existing
// callers (`crate::primary::wire::compute_task_hash`) compile
// unchanged.
pub use dynrunner_core::compute_task_hash;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub(super) fn binary_to_distributed<I: Identifier>(
    binary: &TaskInfo<I>,
) -> DistributedBinaryInfo<I> {
    DistributedBinaryInfo::from_task_info(binary)
}
