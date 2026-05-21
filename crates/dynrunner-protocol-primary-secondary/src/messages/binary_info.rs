//! Per-task wire descriptor + supporting types
//! (`DistributedBinaryInfo`, `ZipFileAssignment`, `ZipBinaryEntry`,
//! `StagedFileRecord`) along with the serde-default helpers
//! (`default_phase_id_string` / `default_type_id_string` /
//! `default_payload_json` / `default_uses_file_based_items`) that keep
//! the wire format backward-compatible with pre-Phase-4b senders.

use dynrunner_core::{Identifier, SoftPreferredSecondaries, TaskDep, TaskInfo};
use serde::{Deserialize, Serialize};

/// Binary info as serialized in distributed messages.
///
/// Generic over the identifier type `I`. The identifier fields are flattened
/// into the JSON object to maintain backward compatibility with the Python
/// wire format.
///
/// Carries the `(phase_id, type_id, affinity_id, payload_json)` tags so the
/// receiving secondary can hydrate its in-process `TaskInfo<I>` with the
/// actual phase/type/affinity from the primary's `PendingPool` rather than
/// resetting to defaults. `payload_json` is a stringified `serde_json::Value`
/// — keeping it a `String` on the wire decouples the protocol crate from
/// the runner's choice of opaque payload representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct DistributedBinaryInfo<I> {
    pub path: String,
    pub size: u64,
    /// Wire identity. Pre-B2 this was a flattened struct of typed fields
    /// (e.g. {binary_name, platform, …}); post-B2 the runner treats every
    /// identifier as an opaque key (`Arc<str>` Rust-side), so the field
    /// is just `identifier`.
    pub identifier: I,
    /// Phase tag (`PhaseId` Rust-side). Defaults to `"default"` for
    /// pre-Phase-4b senders that didn't include the field.
    #[serde(default = "default_phase_id_string")]
    pub phase_id: String,
    /// Type tag (`TypeId` Rust-side). Defaults to `"default"` for
    /// pre-Phase-4b senders.
    #[serde(default = "default_type_id_string")]
    pub type_id: String,
    /// Optional soft-affinity tag (`AffinityId` Rust-side). `None` means
    /// the item belongs to the free pool.
    #[serde(default)]
    pub affinity_id: Option<String>,
    /// Opaque per-item payload, stringified JSON. Defaults to JSON
    /// `null` for pre-Phase-4b senders. The framework never inspects
    /// the contents — it's pass-through metadata for the worker.
    #[serde(default = "default_payload_json")]
    pub payload_json: String,
    /// Optional consumer-supplied task id (see `TaskInfo::task_id`).
    /// Defaults to `None` for pre-task-deps senders so the wire
    /// stays backward-compatible.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Per-edge dep records of prerequisites (see
    /// `TaskInfo::task_depends_on`). Defaults to empty for pre-task-deps
    /// senders. Wire backcompat: `TaskDep`'s `#[serde(untagged)]`
    /// deserializer accepts both legacy bare-string elements (which decode
    /// as `inherit_outputs: false`) and the full struct shape, so pre-keyed-
    /// outputs payloads that emitted `["foo", "bar"]` continue to load.
    #[serde(default)]
    pub task_depends_on: Vec<TaskDep>,
    /// Soft hint of preferred secondaries (see
    /// [`TaskInfo::preferred_secondaries`]). Carried verbatim across
    /// the wire so the receiving secondary's hydrated `TaskInfo`
    /// keeps the same preference list. The `#[serde(default,
    /// skip_serializing_if = "…is_empty")]` pair keeps the wire
    /// backward-compatible with peers that don't emit the field, and
    /// omits it from the wire in the common empty case.
    #[serde(default, skip_serializing_if = "SoftPreferredSecondaries::is_empty")]
    pub preferred_secondaries: SoftPreferredSecondaries,
}

fn default_phase_id_string() -> String {
    "default".into()
}

fn default_type_id_string() -> String {
    "default".into()
}

fn default_payload_json() -> String {
    "null".into()
}

pub(crate) fn default_uses_file_based_items() -> bool {
    true
}

impl<I: Identifier> DistributedBinaryInfo<I> {
    /// Build the wire-side info from an in-process `TaskInfo<I>`.
    ///
    /// Owns the reverse transformation in [`Self::to_task_info`]; managers
    /// (primary, secondary, promoted-secondary) all funnel through
    /// these two methods so the phase/type/affinity/payload tags stay in
    /// lockstep across the wire.
    pub fn from_task_info(task: &TaskInfo<I>) -> Self {
        Self {
            path: task.path.to_string_lossy().into_owned(),
            size: task.size,
            identifier: task.identifier.clone(),
            phase_id: task.phase_id.as_str().to_owned(),
            type_id: task.type_id.as_str().to_owned(),
            affinity_id: task.affinity_id.as_ref().map(|a| a.as_str().to_owned()),
            // payload is opaque to the framework — round-trip the JSON
            // representation verbatim. `to_string` on `serde_json::Value`
            // is infallible.
            payload_json: task.payload.to_string(),
            task_id: task.task_id.clone(),
            task_depends_on: task.task_depends_on.clone(),
            preferred_secondaries: task.preferred_secondaries.clone(),
        }
    }

    /// Hydrate an in-process `TaskInfo<I>` from this wire-side info.
    ///
    /// A malformed `payload_json` (shouldn't happen — senders always emit
    /// valid JSON via `Value::to_string`) decodes as JSON `null` rather
    /// than failing the dispatch path; the per-item payload is opaque to
    /// the framework so the worst case is the worker sees an unexpected
    /// payload.
    pub fn to_task_info(&self) -> TaskInfo<I> {
        use dynrunner_core::{AffinityId, PhaseId, TypeId};
        let payload = serde_json::from_str::<serde_json::Value>(&self.payload_json)
            .unwrap_or(serde_json::Value::Null);
        TaskInfo {
            path: std::path::PathBuf::from(&self.path),
            size: self.size,
            identifier: self.identifier.clone(),
            phase_id: PhaseId::from(self.phase_id.as_str()),
            type_id: TypeId::from(self.type_id.as_str()),
            affinity_id: self.affinity_id.as_deref().map(AffinityId::from),
            payload,
            task_id: self.task_id.clone(),
            task_depends_on: self.task_depends_on.clone(),
            preferred_secondaries: self.preferred_secondaries.clone(),
            resolved_path: None,
        }
    }
}

/// Zip file with assigned binaries for initial assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct ZipFileAssignment<I> {
    pub zip_name: String,
    pub binaries: Vec<ZipBinaryEntry<I>>,
}

/// A single binary entry within a zip assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct ZipBinaryEntry<I> {
    pub local_path: String,
    pub binary_info: DistributedBinaryInfo<I>,
    pub hash: String,
}

/// A pre-staging record carried inline in `InitialAssignment` so the
/// secondary can register files in its `ExtractionCache`
/// atomically with processing the assignment. Avoids the
/// StageFile-vs-InitialAssignment race that the standalone
/// `DistributedMessage::StageFile` path otherwise opens during
/// setup: the secondary's `wait_for_setup` loop matches only on
/// `PeerInfo` / `InitialAssignment` / `TransferComplete` and would
/// drop a separately-sent `StageFile` arriving in the same window.
/// Per-secondary addressing is implicit from the enclosing
/// `InitialAssignment.secondary_id`.
///
/// `file_hash` is the task identifier (path/identifier-derived,
/// matches `TaskAssignment.file_hash` so the
/// `ExtractionCache` lookup keys line up). `content_hash` is the
/// SHA256 of the file contents the primary expects the secondary
/// to land at `src_tmp/<dest_path>` after copying from
/// `src_network/<src_path>` (or from an absolute `src_path`); the
/// secondary verifies and rejects a copy whose hash doesn't match.
/// Decoupling the two means the cache key stays cheap (no file IO
/// at every `compute_task_hash` site) while the staging path keeps
/// its integrity check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedFileRecord {
    pub file_hash: String,
    pub content_hash: String,
    pub src_path: String,
    pub dest_path: String,
}
