use db_comm_api_base::{BinaryInfo, Identifier};
use db_primary_secondary_comm::DistributedBinaryInfo;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub(super) fn binary_to_distributed<I: Identifier>(
    binary: &BinaryInfo<I>,
) -> DistributedBinaryInfo<I> {
    DistributedBinaryInfo {
        path: binary.path.to_string_lossy().into_owned(),
        size: binary.size,
        identifier: binary.identifier.clone(),
    }
}

pub(super) fn compute_task_hash<I: Identifier>(binary: &BinaryInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
