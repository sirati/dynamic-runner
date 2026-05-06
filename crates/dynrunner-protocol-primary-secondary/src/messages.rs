use std::collections::HashMap;

use dynrunner_core::{Identifier, PhaseId, ResourceAmount, TaskInfo};
use serde::{Deserialize, Serialize};

/// All distributed message types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    // Primary <-> Secondary
    SecondaryWelcome,
    Entropy,
    CertExchange,
    PeerInfo,
    InitialAssignment,
    TaskRequest,
    TaskAssignment,
    TransferComplete,
    StageFile,
    PromotePrimary,
    FullTaskList,
    MeshReady,
    // Secondary <-> Secondary (peer-to-peer)
    TaskComplete,
    TaskFailed,
    Keepalive,
    TimeoutDetected,
    TimeoutQuery,
    TimeoutResponse,
    PromotionVote,
    PromotionConfirm,
    /// Secondary signalling an unrecoverable local fault (e.g. peer
    /// mesh fully failed to form). Sent once, immediately before the
    /// secondary process exits non-zero. Primary treats the sender as
    /// dead and runs the standard requeue path.
    SecondaryFatalError,
    /// Wire-only envelope: peer-to-peer relay when the direct A↔B
    /// link is unreachable but A↔C↔B is. The application layer never
    /// observes this variant; `PeerTransport::recv_peer` unwraps it
    /// or forwards it transparently.
    RelayMessage,
}

/// Worker readiness information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReadyInfo {
    pub worker_id: u32,
    pub resource_budgets: Vec<ResourceAmount>,
}

/// Peer connection information sent in PeerInfo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConnectionInfo {
    pub secondary_id: String,
    pub cert: String,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub port: u16,
}

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
    /// Task ids of prerequisites (see `TaskInfo::task_depends_on`).
    /// Defaults to empty for pre-task-deps senders.
    #[serde(default)]
    pub task_depends_on: Vec<String>,
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

fn default_uses_file_based_items() -> bool {
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

/// Wire-format entry in a `FullTaskList` broadcast. Distinct from
/// `dynrunner_core::TaskInfo` (the in-process content type) — this is the
/// flat, hash-keyed, file-path-aware shape that primaries and secondaries
/// exchange over the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct TaskListEntry<I> {
    pub local_path: String,
    pub binary_info: DistributedBinaryInfo<I>,
    pub hash: String,
    /// Original file path for resolution on primary.
    #[serde(default)]
    pub file_path: Option<String>,
}

/// The typed message enum. Each variant carries exactly the payload
/// from the Python protocol, with `sender_id` and `timestamp` common fields.
///
/// Generic over the identifier type `I` for binary info in task-related
/// messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "msg_type", rename_all = "snake_case")]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub enum DistributedMessage<I> {
    SecondaryWelcome {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        resources: Vec<ResourceAmount>,
        worker_count: u32,
        hostname: String,
    },
    Entropy {
        sender_id: String,
        timestamp: f64,
        entropy_hex: String,
    },
    CertExchange {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        public_cert_pem: String,
        ipv4_address: Option<String>,
        ipv6_address: Option<String>,
        quic_port: u16,
    },
    PeerInfo {
        sender_id: String,
        timestamp: f64,
        peers: Vec<PeerConnectionInfo>,
    },
    InitialAssignment {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        zip_files: Vec<ZipFileAssignment<I>>,
        workers_ready: Vec<WorkerReadyInfo>,
        /// Files the secondary should register in its
        /// ExtractionCache before processing per-task assignments
        /// (replaces the separate StageFile messages that previously
        /// raced this one). Defaults to empty for backward
        /// compatibility with primaries pre-dating the inline-staging
        /// fix; the receiver treats the absence as "no inline records,
        /// fall back to whatever standalone StageFile messages
        /// arrived earlier".
        #[serde(default)]
        staged_files: Vec<StagedFileRecord>,
        /// Pre-staged source mode: when true, the secondary skips the
        /// hash-based extraction-cache lookup for incoming
        /// TaskAssignments and resolves files directly via
        /// `src_network/<local_path>`. Set by the primary when the
        /// run was launched with `--source-already-staged`; that
        /// mode bind-mounts the source data into the container at
        /// `/app/src-network` (= secondary's `src_network`) and
        /// skips the StageFile-driven copy + verify pass entirely.
        /// The hash machinery is a network-transfer dedup
        /// optimisation; with no transfer there's nothing to
        /// dedup, and the bind-mount IS the contract.
        /// Defaults to false for backward compatibility.
        #[serde(default)]
        pre_staged_mode: bool,
        /// Whether items dispatched to this secondary are backed by
        /// real files on the secondary's filesystem. When false
        /// (`TaskDefinition.uses_file_based_items=False`), the
        /// framework passes `local_path` to the worker as an opaque
        /// identifier — no `stat()`, no content hash, no
        /// extraction-cache resolution. Workers that read their
        /// payload via JSON/stdin/comm-fd (rather than opening a
        /// file at TaskInfo.path) declare this so the framework
        /// doesn't perform load-bearing IO on a path the worker
        /// never touches.
        ///
        /// Defaults to TRUE for backward compatibility (older
        /// primaries don't send the field; receiver assumes
        /// file-based, which is the historical contract).
        #[serde(default = "default_uses_file_based_items")]
        uses_file_based_items: bool,
    },
    TaskRequest {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        available_resources: Vec<ResourceAmount>,
    },
    TaskAssignment {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        zip_file: Option<String>,
        binary_info: DistributedBinaryInfo<I>,
        local_path: String,
        file_hash: String,
    },
    TransferComplete {
        sender_id: String,
        timestamp: f64,
        total_files: u64,
        total_bytes: u64,
    },
    /// Per-file staging notification: tells `secondary_id` to copy the
    /// file from `src_path` (relative to the secondary's `src_network`,
    /// or absolute if out-of-band-staged) to `dest_path` (relative to
    /// `src_tmp`), then hash-verify against `content_hash` and register
    /// the resulting local path in the `ExtractionCache` keyed by
    /// `file_hash`. The runner does NOT transfer file payloads —
    /// the assumption is shared storage; this message just tells the
    /// secondary "the file is now available, copy it locally".
    /// `file_hash` and `content_hash` are independent: the former is
    /// the task identifier (path/identifier-derived; the cache lookup
    /// key that must equal `TaskAssignment.file_hash`), the latter is
    /// the SHA256 of the file contents (used only for the integrity
    /// check on the copy).
    StageFile {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        file_hash: String,
        content_hash: String,
        src_path: String,
        dest_path: String,
    },
    PromotePrimary {
        sender_id: String,
        timestamp: f64,
        new_primary_id: String,
    },
    /// Secondary -> Primary: "my peer-mesh has finished forming
    /// (or was empty / fully failed to form)". Emitted once per
    /// secondary, after `connect_to_peers` has either landed at
    /// least one peer connection or the per-secondary peer-mesh
    /// watchdog has elapsed; for single-secondary runs (no peers
    /// to dial) it fires immediately at operational-loop entry.
    /// The primary defers `PromotePrimary` until every secondary
    /// has reported, so the promoted secondary never
    /// becomes authoritative against an empty peer mesh — closing
    /// the 750µs ↔ 30s gap where pre-mesh-formation messages
    /// would be sent into a void. `peer_count` carries the
    /// observed peer-connection count at signal time (0 in the
    /// single-secondary or fully-failed cases).
    MeshReady {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        peer_count: u32,
    },
    FullTaskList {
        sender_id: String,
        timestamp: f64,
        all_tasks: Vec<TaskListEntry<I>>,
        completed_tasks: Vec<String>,
        #[serde(default)]
        pending_tasks: Vec<String>,
        /// Run-level phase dependency graph: `child -> [parents]`.
        /// Travels alongside the task list so a promoted secondary can
        /// rebuild a `PendingPool` with the same phase machine the live
        /// primary used. Defaults to an empty map for backward
        /// compatibility with primaries that pre-date the dep wiring;
        /// the receiver treats every phase as having zero deps in
        /// that case.
        #[serde(default)]
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    },
    TaskComplete {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        task_hash: String,
        #[serde(default)]
        result_data: Option<Vec<u8>>,
    },
    TaskFailed {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        task_hash: String,
        error_type: String,
        error_message: String,
    },
    Keepalive {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        active_workers: u32,
    },
    TimeoutDetected {
        sender_id: String,
        timestamp: f64,
        timed_out_secondary_id: String,
        last_seen: f64,
    },
    TimeoutQuery {
        sender_id: String,
        timestamp: f64,
        /// Node id of the suspected-dead party. May be a secondary (when
        /// the querier is the primary or a peer auditing a secondary) or
        /// the primary's node id (when secondaries are checking primary
        /// liveness during failover detection).
        query_node_id: String,
    },
    TimeoutResponse {
        sender_id: String,
        timestamp: f64,
        /// Echoes the `query_node_id` from the corresponding TimeoutQuery
        /// so concurrent queries can be matched up by the aggregator.
        query_node_id: String,
        last_keepalive: Option<f64>,
    },
    PromotionVote {
        sender_id: String,
        timestamp: f64,
        candidate_id: String,
        vote_round: u32,
    },
    PromotionConfirm {
        sender_id: String,
        timestamp: f64,
        new_primary_id: String,
        vote_round: u32,
    },
    /// Secondary -> Primary unrecoverable-fault notification. The
    /// secondary sets this just before exiting non-zero, so the
    /// primary can drop it from the routable set + requeue any
    /// in-flight tasks rather than waiting on the keepalive miss
    /// threshold. `error` is a free-form human-readable description
    /// of the fault (e.g. "peer mesh fully failed to form: 0 of N
    /// peers reachable; cluster routing impossible").
    SecondaryFatalError {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        error: String,
    },
    /// Wire-only relay envelope. A peer that can't reach `target_id`
    /// directly wraps the message in this variant and sends it to a
    /// reachable forwarder; the forwarder unwraps if it's the target,
    /// or appends itself to `path` and forwards to another non-`path`
    /// peer if not.
    ///
    /// `path` records every peer the message has visited (sender plus
    /// each forwarder, in order). Loop prevention: a forwarder MUST
    /// pick a candidate that is not in `path`, not the target itself,
    /// and not its own id. If no such candidate exists, the relay is
    /// dropped with a warn — multi-hop backtracking ("ask previous to
    /// choose another peer") would need a stateful round-trip
    /// protocol and is intentionally deferred to a follow-up; the
    /// path field is the wire shape the future protocol will plug
    /// into without another schema bump.
    ///
    /// Application code never observes `Relay` — `recv_peer` strips
    /// the envelope before delivery.
    Relay {
        sender_id: String,
        timestamp: f64,
        target_id: String,
        path: Vec<String>,
        inner: Box<DistributedMessage<I>>,
    },
}

impl<I> DistributedMessage<I> {
    pub fn sender_id(&self) -> &str {
        match self {
            Self::SecondaryWelcome { sender_id, .. }
            | Self::Entropy { sender_id, .. }
            | Self::CertExchange { sender_id, .. }
            | Self::PeerInfo { sender_id, .. }
            | Self::InitialAssignment { sender_id, .. }
            | Self::TaskRequest { sender_id, .. }
            | Self::TaskAssignment { sender_id, .. }
            | Self::TransferComplete { sender_id, .. }
            | Self::StageFile { sender_id, .. }
            | Self::PromotePrimary { sender_id, .. }
            | Self::FullTaskList { sender_id, .. }
            | Self::MeshReady { sender_id, .. }
            | Self::TaskComplete { sender_id, .. }
            | Self::TaskFailed { sender_id, .. }
            | Self::Keepalive { sender_id, .. }
            | Self::TimeoutDetected { sender_id, .. }
            | Self::TimeoutQuery { sender_id, .. }
            | Self::TimeoutResponse { sender_id, .. }
            | Self::PromotionVote { sender_id, .. }
            | Self::PromotionConfirm { sender_id, .. }
            | Self::SecondaryFatalError { sender_id, .. }
            | Self::Relay { sender_id, .. } => sender_id,
        }
    }

    pub fn timestamp(&self) -> f64 {
        match self {
            Self::SecondaryWelcome { timestamp, .. }
            | Self::Entropy { timestamp, .. }
            | Self::CertExchange { timestamp, .. }
            | Self::PeerInfo { timestamp, .. }
            | Self::InitialAssignment { timestamp, .. }
            | Self::TaskRequest { timestamp, .. }
            | Self::TaskAssignment { timestamp, .. }
            | Self::TransferComplete { timestamp, .. }
            | Self::StageFile { timestamp, .. }
            | Self::PromotePrimary { timestamp, .. }
            | Self::FullTaskList { timestamp, .. }
            | Self::MeshReady { timestamp, .. }
            | Self::TaskComplete { timestamp, .. }
            | Self::TaskFailed { timestamp, .. }
            | Self::Keepalive { timestamp, .. }
            | Self::TimeoutDetected { timestamp, .. }
            | Self::TimeoutQuery { timestamp, .. }
            | Self::TimeoutResponse { timestamp, .. }
            | Self::PromotionVote { timestamp, .. }
            | Self::PromotionConfirm { timestamp, .. }
            | Self::SecondaryFatalError { timestamp, .. }
            | Self::Relay { timestamp, .. } => *timestamp,
        }
    }

    pub fn msg_type(&self) -> MessageType {
        match self {
            Self::SecondaryWelcome { .. } => MessageType::SecondaryWelcome,
            Self::Entropy { .. } => MessageType::Entropy,
            Self::CertExchange { .. } => MessageType::CertExchange,
            Self::PeerInfo { .. } => MessageType::PeerInfo,
            Self::InitialAssignment { .. } => MessageType::InitialAssignment,
            Self::TaskRequest { .. } => MessageType::TaskRequest,
            Self::TaskAssignment { .. } => MessageType::TaskAssignment,
            Self::TransferComplete { .. } => MessageType::TransferComplete,
            Self::StageFile { .. } => MessageType::StageFile,
            Self::PromotePrimary { .. } => MessageType::PromotePrimary,
            Self::FullTaskList { .. } => MessageType::FullTaskList,
            Self::MeshReady { .. } => MessageType::MeshReady,
            Self::TaskComplete { .. } => MessageType::TaskComplete,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
            Self::Keepalive { .. } => MessageType::Keepalive,
            Self::TimeoutDetected { .. } => MessageType::TimeoutDetected,
            Self::TimeoutQuery { .. } => MessageType::TimeoutQuery,
            Self::TimeoutResponse { .. } => MessageType::TimeoutResponse,
            Self::PromotionVote { .. } => MessageType::PromotionVote,
            Self::PromotionConfirm { .. } => MessageType::PromotionConfirm,
            Self::SecondaryFatalError { .. } => MessageType::SecondaryFatalError,
            Self::Relay { .. } => MessageType::RelayMessage,
        }
    }
}
