import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class MessageType(Enum):
    """Message types for distributed protocol"""

    # Primary <-> Secondary
    SECONDARY_WELCOME = "secondary_welcome"
    ENTROPY = "entropy"
    CERT_EXCHANGE = "cert_exchange"
    PEER_INFO = "peer_info"
    INITIAL_ASSIGNMENT = "initial_assignment"
    TASK_REQUEST = "task_request"
    TASK_ASSIGNMENT = "task_assignment"
    TRANSFER_COMPLETE = "transfer_complete"
    PROMOTE_PRIMARY = "promote_primary"
    FULL_TASK_LIST = "full_task_list"

    # Secondary <-> Secondary (peer-to-peer)
    TASK_COMPLETE = "task_complete"
    TASK_FAILED = "task_failed"
    KEEPALIVE = "keepalive"
    TIMEOUT_DETECTED = "timeout_detected"
    TIMEOUT_QUERY = "timeout_query"
    TIMEOUT_RESPONSE = "timeout_response"
    PROMOTION_VOTE = "promotion_vote"
    PROMOTION_CONFIRM = "promotion_confirm"

    # Host <-> Container
    EXECUTE_COMMAND = "execute_command"
    COMMAND_RESULT = "command_result"


@dataclass
class Message:
    """Base message class"""

    msg_type: MessageType
    sender_id: str
    timestamp: float

    def to_json(self) -> str:
        """Serialize message to JSON"""
        data = asdict(self)
        data["msg_type"] = self.msg_type.value
        return json.dumps(data)

    @classmethod
    def from_json(cls, data: str) -> "Message":
        """Deserialize message from JSON"""
        obj = json.loads(data)
        msg_type = MessageType(obj["msg_type"])

        # Map message types to classes
        msg_classes = {
            MessageType.SECONDARY_WELCOME: SecondaryWelcomeMessage,
            MessageType.ENTROPY: EntropyMessage,
            MessageType.CERT_EXCHANGE: CertExchangeMessage,
            MessageType.PEER_INFO: PeerInfoMessage,
            MessageType.INITIAL_ASSIGNMENT: InitialAssignmentMessage,
            MessageType.TASK_REQUEST: TaskRequestMessage,
            MessageType.TASK_ASSIGNMENT: TaskAssignmentMessage,
            MessageType.TRANSFER_COMPLETE: TransferCompleteMessage,
            MessageType.PROMOTE_PRIMARY: PromotePrimaryMessage,
            MessageType.FULL_TASK_LIST: FullTaskListMessage,
            MessageType.TASK_COMPLETE: TaskCompleteMessage,
            MessageType.TASK_FAILED: TaskFailedMessage,
            MessageType.KEEPALIVE: KeepaliveMessage,
            MessageType.TIMEOUT_DETECTED: TimeoutDetectedMessage,
            MessageType.TIMEOUT_QUERY: TimeoutQueryMessage,
            MessageType.TIMEOUT_RESPONSE: TimeoutResponseMessage,
            MessageType.PROMOTION_VOTE: PromotionVoteMessage,
            MessageType.PROMOTION_CONFIRM: PromotionConfirmMessage,
            MessageType.EXECUTE_COMMAND: ExecuteCommandMessage,
            MessageType.COMMAND_RESULT: CommandResultMessage,
        }

        msg_class = msg_classes.get(msg_type, Message)
        return msg_class(**obj)


@dataclass
class SecondaryWelcomeMessage(Message):
    """Secondary announces capabilities to primary"""

    secondary_id: str
    ram_bytes: int
    worker_count: int
    hostname: str

    def __init__(
        self, sender_id: str, timestamp: float, secondary_id: str, ram_bytes: int, worker_count: int, hostname: str
    ):
        super().__init__(MessageType.SECONDARY_WELCOME, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.ram_bytes = ram_bytes
        self.worker_count = worker_count
        self.hostname = hostname


@dataclass
class EntropyMessage(Message):
    """Primary sends entropy for certificate generation"""

    entropy_hex: str

    def __init__(self, sender_id: str, timestamp: float, entropy_hex: str):
        super().__init__(MessageType.ENTROPY, sender_id, timestamp)
        self.entropy_hex = entropy_hex


@dataclass
class CertExchangeMessage(Message):
    """Secondary sends public certificate and IP addresses"""

    secondary_id: str
    public_cert_pem: str
    ipv4_address: str | None
    ipv6_address: str | None
    quic_port: int

    def __init__(
        self,
        sender_id: str,
        timestamp: float,
        secondary_id: str,
        public_cert_pem: str,
        ipv4_address: str | None,
        ipv6_address: str | None,
        quic_port: int,
    ):
        super().__init__(MessageType.CERT_EXCHANGE, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.public_cert_pem = public_cert_pem
        self.ipv4_address = ipv4_address
        self.ipv6_address = ipv6_address
        self.quic_port = quic_port


@dataclass
class PeerInfoMessage(Message):
    """Primary relays peer connection info to all secondaries"""

    peers: list[dict[str, Any]]  # List of {secondary_id, cert, ipv4, ipv6, port}

    def __init__(self, sender_id: str, timestamp: float, peers: list[dict[str, Any]]):
        super().__init__(MessageType.PEER_INFO, sender_id, timestamp)
        self.peers = peers


@dataclass
class WorkerReadyInfo:
    """Worker readiness information"""

    worker_id: int
    memory_budget: int


@dataclass
class InitialAssignmentMessage(Message):
    """Primary sends initial task assignment to secondary"""

    secondary_id: str
    zip_files: list[dict[str, Any]]  # List of {zip_name, binaries: [{local_path, binary_info, hash}]}
    workers_ready: list[WorkerReadyInfo]

    def __init__(
        self,
        sender_id: str,
        timestamp: float,
        secondary_id: str,
        zip_files: list[dict[str, Any]],
        workers_ready: list[dict[str, Any]],
    ):
        super().__init__(MessageType.INITIAL_ASSIGNMENT, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.zip_files = zip_files
        self.workers_ready = [WorkerReadyInfo(**w) for w in workers_ready]


@dataclass
class TaskRequestMessage(Message):
    """Secondary requests new task from primary"""

    secondary_id: str
    worker_id: int
    available_memory: int

    def __init__(self, sender_id: str, timestamp: float, secondary_id: str, worker_id: int, available_memory: int):
        super().__init__(MessageType.TASK_REQUEST, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.worker_id = worker_id
        self.available_memory = available_memory


@dataclass
class TaskAssignmentMessage(Message):
    """Primary/SLURM-primary assigns task to secondary worker"""

    secondary_id: str
    worker_id: int
    zip_file: str | None
    binary_info: dict[str, Any]
    local_path: str
    file_hash: str

    def __init__(
        self,
        sender_id: str,
        timestamp: float,
        secondary_id: str,
        worker_id: int,
        zip_file: str | None,
        binary_info: dict[str, Any],
        local_path: str,
        file_hash: str,
    ):
        super().__init__(MessageType.TASK_ASSIGNMENT, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.worker_id = worker_id
        self.zip_file = zip_file
        self.binary_info = binary_info
        self.local_path = local_path
        self.file_hash = file_hash


@dataclass
class TransferCompleteMessage(Message):
    """Primary notifies all secondaries that file transfer is complete"""

    total_files: int
    total_bytes: int

    def __init__(self, sender_id: str, timestamp: float, total_files: int, total_bytes: int):
        super().__init__(MessageType.TRANSFER_COMPLETE, sender_id, timestamp)
        self.total_files = total_files
        self.total_bytes = total_bytes


@dataclass
class PromotePrimaryMessage(Message):
    """Primary promotes a secondary to SLURM-primary role"""

    new_primary_id: str

    def __init__(self, sender_id: str, timestamp: float, new_primary_id: str):
        super().__init__(MessageType.PROMOTE_PRIMARY, sender_id, timestamp)
        self.new_primary_id = new_primary_id


@dataclass
class FullTaskListMessage(Message):
    """Primary sends complete task list to all secondaries"""

    all_tasks: list[dict[str, Any]]  # List of all task info
    completed_tasks: list[str]  # List of completed task hashes

    def __init__(self, sender_id: str, timestamp: float, all_tasks: list[dict[str, Any]], completed_tasks: list[str]):
        super().__init__(MessageType.FULL_TASK_LIST, sender_id, timestamp)
        self.all_tasks = all_tasks
        self.completed_tasks = completed_tasks


@dataclass
class TaskCompleteMessage(Message):
    """Secondary notifies peers of task completion"""

    secondary_id: str
    worker_id: int
    task_hash: str
    warnings: int
    filtered: int

    def __init__(
        self,
        sender_id: str,
        timestamp: float,
        secondary_id: str,
        worker_id: int,
        task_hash: str,
        warnings: int,
        filtered: int,
    ):
        super().__init__(MessageType.TASK_COMPLETE, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.worker_id = worker_id
        self.task_hash = task_hash
        self.warnings = warnings
        self.filtered = filtered


@dataclass
class TaskFailedMessage(Message):
    """Secondary notifies peers of task failure"""

    secondary_id: str
    worker_id: int
    task_hash: str
    error_type: str
    error_message: str

    def __init__(
        self,
        sender_id: str,
        timestamp: float,
        secondary_id: str,
        worker_id: int,
        task_hash: str,
        error_type: str,
        error_message: str,
    ):
        super().__init__(MessageType.TASK_FAILED, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.worker_id = worker_id
        self.task_hash = task_hash
        self.error_type = error_type
        self.error_message = error_message


@dataclass
class KeepaliveMessage(Message):
    """Secondary sends keepalive to peers"""

    secondary_id: str
    active_workers: int

    def __init__(self, sender_id: str, timestamp: float, secondary_id: str, active_workers: int):
        super().__init__(MessageType.KEEPALIVE, sender_id, timestamp)
        self.secondary_id = secondary_id
        self.active_workers = active_workers


@dataclass
class TimeoutDetectedMessage(Message):
    """Secondary notifies peers of detected timeout"""

    timed_out_secondary_id: str
    last_seen: float

    def __init__(self, sender_id: str, timestamp: float, timed_out_secondary_id: str, last_seen: float):
        super().__init__(MessageType.TIMEOUT_DETECTED, sender_id, timestamp)
        self.timed_out_secondary_id = timed_out_secondary_id
        self.last_seen = last_seen


@dataclass
class TimeoutQueryMessage(Message):
    """Query peers for last keepalive from specific secondary"""

    query_secondary_id: str

    def __init__(self, sender_id: str, timestamp: float, query_secondary_id: str):
        super().__init__(MessageType.TIMEOUT_QUERY, sender_id, timestamp)
        self.query_secondary_id = query_secondary_id


@dataclass
class TimeoutResponseMessage(Message):
    """Response to timeout query"""

    query_secondary_id: str
    last_keepalive: float | None

    def __init__(self, sender_id: str, timestamp: float, query_secondary_id: str, last_keepalive: float | None):
        super().__init__(MessageType.TIMEOUT_RESPONSE, sender_id, timestamp)
        self.query_secondary_id = query_secondary_id
        self.last_keepalive = last_keepalive


@dataclass
class PromotionVoteMessage(Message):
    """Vote for new SLURM-primary"""

    candidate_id: str
    vote_round: int

    def __init__(self, sender_id: str, timestamp: float, candidate_id: str, vote_round: int):
        super().__init__(MessageType.PROMOTION_VOTE, sender_id, timestamp)
        self.candidate_id = candidate_id
        self.vote_round = vote_round


@dataclass
class PromotionConfirmMessage(Message):
    """Confirm new SLURM-primary election"""

    new_primary_id: str
    vote_round: int

    def __init__(self, sender_id: str, timestamp: float, new_primary_id: str, vote_round: int):
        super().__init__(MessageType.PROMOTION_CONFIRM, sender_id, timestamp)
        self.new_primary_id = new_primary_id
        self.vote_round = vote_round


@dataclass
class ExecuteCommandMessage(Message):
    """Request host to execute command (container -> host)"""

    command: str
    command_id: str

    def __init__(self, sender_id: str, timestamp: float, command: str, command_id: str):
        super().__init__(MessageType.EXECUTE_COMMAND, sender_id, timestamp)
        self.command = command
        self.command_id = command_id


@dataclass
class CommandResultMessage(Message):
    """Host sends command execution result (host -> container)"""

    command_id: str
    return_code: int
    stdout: str
    stderr: str

    def __init__(self, sender_id: str, timestamp: float, command_id: str, return_code: int, stdout: str, stderr: str):
        super().__init__(MessageType.COMMAND_RESULT, sender_id, timestamp)
        self.command_id = command_id
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
