use dynrunner_core::{TaskInfo, Identifier};
use dynrunner_protocol_primary_secondary::DistributedBinaryInfo;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub(super) fn binary_to_distributed<I: Identifier>(
    binary: &TaskInfo<I>,
) -> DistributedBinaryInfo<I> {
    DistributedBinaryInfo {
        path: binary.path.to_string_lossy().into_owned(),
        size: binary.size,
        identifier: binary.identifier.clone(),
    }
}

pub fn compute_task_hash<I: Identifier>(binary: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
