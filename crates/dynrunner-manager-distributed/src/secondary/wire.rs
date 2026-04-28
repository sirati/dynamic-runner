use db_comm_api_base::{BinaryInfo, Identifier};
use db_primary_secondary_comm::DistributedBinaryInfo;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub(super) fn distributed_to_binary<I: Identifier>(info: &DistributedBinaryInfo<I>) -> BinaryInfo<I> {
    BinaryInfo {
        path: std::path::PathBuf::from(&info.path),
        size: info.size,
        identifier: info.identifier.clone(),
    }
}

