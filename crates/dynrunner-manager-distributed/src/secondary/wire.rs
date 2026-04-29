use dynrunner_core::{BinaryInfo, Identifier, PhaseId, TypeId};
use dynrunner_protocol_primary_secondary::DistributedBinaryInfo;

pub(super) fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

// TODO(phases-4b): wire phase_id/type_id/affinity_id/payload through
// DistributedBinaryInfo so the secondary preserves them across the network.
pub(super) fn distributed_to_binary<I: Identifier>(info: &DistributedBinaryInfo<I>) -> BinaryInfo<I> {
    BinaryInfo {
        path: std::path::PathBuf::from(&info.path),
        size: info.size,
        identifier: info.identifier.clone(),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
    }
}

