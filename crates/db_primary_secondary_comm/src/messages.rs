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
    PromotePrimary,
    FullTaskList,
    // Secondary <-> Secondary (peer-to-peer)
    TaskComplete,
    TaskFailed,
    Keepalive,
    TimeoutDetected,
    TimeoutQuery,
    TimeoutResponse,
    PromotionVote,
    PromotionConfirm,
    // Host <-> Container
    ExecuteCommand,
    CommandResult,
}

/// Worker readiness information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReadyInfo {
    pub worker_id: u32,
    pub memory_budget: u64,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct DistributedBinaryInfo<I> {
    pub path: String,
    pub size: u64,
    #[serde(flatten)]
    pub identifier: I,
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

/// Task info in full task list.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct TaskInfo<I> {
    pub local_path: String,
    pub binary_info: DistributedBinaryInfo<I>,
    pub hash: String,
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
        ram_bytes: u64,
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
    },
    TaskRequest {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        available_memory: u64,
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
    PromotePrimary {
        sender_id: String,
        timestamp: f64,
        new_primary_id: String,
    },
    FullTaskList {
        sender_id: String,
        timestamp: f64,
        all_tasks: Vec<TaskInfo<I>>,
        completed_tasks: Vec<String>,
    },
    TaskComplete {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        task_hash: String,
        warnings: u32,
        filtered: u32,
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
        query_secondary_id: String,
    },
    TimeoutResponse {
        sender_id: String,
        timestamp: f64,
        query_secondary_id: String,
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
    ExecuteCommand {
        sender_id: String,
        timestamp: f64,
        command: String,
        command_id: String,
    },
    CommandResult {
        sender_id: String,
        timestamp: f64,
        command_id: String,
        return_code: i32,
        stdout: String,
        stderr: String,
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
            | Self::PromotePrimary { sender_id, .. }
            | Self::FullTaskList { sender_id, .. }
            | Self::TaskComplete { sender_id, .. }
            | Self::TaskFailed { sender_id, .. }
            | Self::Keepalive { sender_id, .. }
            | Self::TimeoutDetected { sender_id, .. }
            | Self::TimeoutQuery { sender_id, .. }
            | Self::TimeoutResponse { sender_id, .. }
            | Self::PromotionVote { sender_id, .. }
            | Self::PromotionConfirm { sender_id, .. }
            | Self::ExecuteCommand { sender_id, .. }
            | Self::CommandResult { sender_id, .. } => sender_id,
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
            | Self::PromotePrimary { timestamp, .. }
            | Self::FullTaskList { timestamp, .. }
            | Self::TaskComplete { timestamp, .. }
            | Self::TaskFailed { timestamp, .. }
            | Self::Keepalive { timestamp, .. }
            | Self::TimeoutDetected { timestamp, .. }
            | Self::TimeoutQuery { timestamp, .. }
            | Self::TimeoutResponse { timestamp, .. }
            | Self::PromotionVote { timestamp, .. }
            | Self::PromotionConfirm { timestamp, .. }
            | Self::ExecuteCommand { timestamp, .. }
            | Self::CommandResult { timestamp, .. } => *timestamp,
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
            Self::PromotePrimary { .. } => MessageType::PromotePrimary,
            Self::FullTaskList { .. } => MessageType::FullTaskList,
            Self::TaskComplete { .. } => MessageType::TaskComplete,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
            Self::Keepalive { .. } => MessageType::Keepalive,
            Self::TimeoutDetected { .. } => MessageType::TimeoutDetected,
            Self::TimeoutQuery { .. } => MessageType::TimeoutQuery,
            Self::TimeoutResponse { .. } => MessageType::TimeoutResponse,
            Self::PromotionVote { .. } => MessageType::PromotionVote,
            Self::PromotionConfirm { .. } => MessageType::PromotionConfirm,
            Self::ExecuteCommand { .. } => MessageType::ExecuteCommand,
            Self::CommandResult { .. } => MessageType::CommandResult,
        }
    }
}
