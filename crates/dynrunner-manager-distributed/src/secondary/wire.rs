use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::DistributedBinaryInfo;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Hydrate a `TaskInfo<I>` from the wire-side `DistributedBinaryInfo<I>`.
/// Thin wrapper over `DistributedBinaryInfo::to_task_info` — kept here as
/// an alias so existing call sites (and the secondary's import surface)
/// stay unchanged.
pub(super) fn distributed_to_binary<I: Identifier>(info: &DistributedBinaryInfo<I>) -> TaskInfo<I> {
    info.to_task_info()
}
