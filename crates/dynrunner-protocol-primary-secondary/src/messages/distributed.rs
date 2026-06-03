//! `DistributedMessage<I>` — the typed wire enum carrying every wire
//! frame the runner exchanges, plus its `sender_id` / `timestamp` /
//! `msg_type` accessors. Generic over the identifier type `I` so
//! task-related variants can carry runner-specific opaque keys without
//! pulling crate-level details into the wire shape.

use std::collections::BTreeMap;

use dynrunner_core::{ErrorType, ResourceAmount, TaskOutputs};
use serde::{Deserialize, Serialize};

use crate::address::Role;
use crate::cluster_mutation::ClusterMutation;
use crate::messages::binary_info::{DistributedBinaryInfo, StagedFileRecord, ZipFileAssignment};
use crate::messages::peer_info::{PeerConnectionInfo, WorkerReadyInfo};

/// The typed message enum. Each variant carries exactly the payload
/// from the Python protocol, with `sender_id` and `timestamp` common fields.
///
/// Generic over the identifier type `I` for binary info in task-related
/// messages.
///
/// The `clippy::large_enum_variant` lint is suppressed at the enum
/// level. The largest variant is `TaskAssignment`, dominated by its
/// inline `binary_info` (DistributedBinaryInfo) payload — strings,
/// identifier, dep records, soft-affinity hints, ~280 bytes total — the
/// natural hot-path dispatch shape. Boxing `binary_info` would push
/// every dispatch and destructure site through indirection for a
/// one-time stack-size win, and that field is the bulk of the size;
/// per-variant fields stay inline to match the rest of the enum. The
/// existing Box wrappers on `Relay.inner` and `RoleAddressed.payload`
/// exist for recursive-shape reasons (a self-referential variant must
/// be boxed), not for size containment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "msg_type", rename_all = "snake_case")]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
#[allow(clippy::large_enum_variant)]
pub enum DistributedMessage<I> {
    SecondaryWelcome {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        resources: Vec<ResourceAmount>,
        worker_count: u32,
        hostname: String,
        /// Observer-mode flag (task #36). The primary records this
        /// on the per-secondary connection state and propagates it
        /// via PeerInfo's `PeerConnectionInfo.is_observer` so other
        /// secondaries can filter the observer from `lowest_alive`
        /// candidate selection. `#[serde(default)]` keeps pre-#36
        /// senders wire-compatible.
        #[serde(default)]
        is_observer: bool,
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
        #[serde(default = "crate::messages::binary_info::default_uses_file_based_items")]
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
        /// Predecessor `TaskOutputs` keyed by predecessor task id —
        /// the channel by which the primary delivers a dependent
        /// task's predecessor results to the secondary that will
        /// run it. The secondary forwards the map into the worker's
        /// `Command::ProcessTask`; the framework never inspects keys
        /// or values, they round-trip through serde-JSON only.
        ///
        /// `#[serde(default)]` keeps pre-keyed-outputs senders
        /// wire-compatible (their `TaskAssignment` omits the field
        /// and decodes as an empty map).
        /// `#[serde(skip_serializing_if = "BTreeMap::is_empty")]`
        /// elides the field on the wire when the dependent has no
        /// recorded predecessor outputs, matching the
        /// `preferred_secondaries` / `task_depends_on` "optional
        /// fields elide when default" idiom in this crate. The
        /// no-dep common case keeps the same byte representation
        /// it had pre-feature.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        predecessor_outputs: BTreeMap<String, TaskOutputs>,
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
        /// Monotonic epoch for the role-flip. Receivers feed this
        /// into the replicated `ClusterState::PrimaryChanged`
        /// mutation, which is last-writer-wins on (epoch,
        /// primary_id). Higher epochs strictly win, matching the
        /// election protocol's tiebreaker semantics: a partition
        /// that re-elects with a higher epoch supersedes the
        /// previous identity unconditionally.
        epoch: u64,
        /// "You also own setup". Set by the submitter primary when
        /// it skipped its own discovery/upload/seed pass (because
        /// `--source-already-staged` told it the source data is on
        /// the cluster-shared filesystem, not on its local host).
        /// The receiving secondary, when it becomes primary, runs
        /// `task.discover_items` against its own bind-mounted
        /// source path, broadcasts the resulting `TaskAdded`
        /// mutations, and only then hydrates its pending pool from
        /// the now-seeded ledger.
        ///
        /// This is the SOLE wire signal that distinguishes the
        /// three reasons a secondary becomes primary:
        ///   1. Legacy bootstrap (submitter did setup):
        ///      `required_setup=false`, ledger already seeded.
        ///   2. Setup-promote (this field set):
        ///      `required_setup=true`, secondary runs discovery.
        ///   3. Failover after primary loss (election emits the
        ///      PromotePrimary from a peer): `required_setup=false`,
        ///      ledger is CRDT-merged from in-flight broadcasts.
        ///
        /// "Is `cluster_state` empty?" is NOT a sufficient
        /// discriminator: failover-at-startup can legitimately
        /// observe an empty ledger and must not be misclassified
        /// as a setup promotion. The wire flag is the only
        /// reliable signal because only the submitter primary knows
        /// at promote-time whether it skipped its own setup pass.
        ///
        /// `#[serde(default)]` keeps pre-fix wire-senders forward-
        /// compatible: their `PromotePrimary` decodes with
        /// `required_setup=false` and takes the legacy path
        /// unchanged.
        #[serde(default)]
        required_setup: bool,
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
    /// Late-joiner / reconnecting node asks any connected peer for a
    /// full snapshot of the current `ClusterState`. Any peer can
    /// respond — state is replicated, so any responder suffices. The
    /// originator targets a specific peer via the unicast transport;
    /// no broadcast (one snapshot is enough).
    RequestClusterSnapshot {
        sender_id: String,
        timestamp: f64,
        /// The joiner's own role. The snapshot responder is the first
        /// existing member to observe a late-joiner; it broadcasts a
        /// `ClusterMutation::PeerJoined { is_observer }` so every peer
        /// learns about the new member. The joiner declares its own
        /// role here so the responder broadcasts the TRUTH rather than
        /// assuming observer — a hardcoded `true` mis-ratcheted a
        /// re-bootstrapping worker up to observer via
        /// `apply_peer_joined`'s upward-only observer ratchet.
        /// `#[serde(default)]` keeps pre-field senders wire-compatible
        /// (they decode as `false` — a worker, the conservative
        /// non-ratcheting default).
        #[serde(default)]
        is_observer: bool,
    },
    /// Response carrying a full `ClusterStateSnapshot` the receiver
    /// merges into its local mirror via `ClusterState::restore`. The
    /// CRDT-merge semantics make the merge idempotent, safe under
    /// concurrent live broadcasts, and resilient to dropped /
    /// duplicate snapshots.
    ///
    /// `snapshot_json` carries the snapshot serialized as JSON (the
    /// same encoding `ClusterStateSnapshot<I>` uses through serde).
    /// Wire-side erasure of the generic `I` parameter keeps the
    /// envelope concrete for routing while preserving exact-roundtrip
    /// semantics on the receiver, which decodes back into
    /// `ClusterStateSnapshot<I>` once `I` is known in context.
    ClusterSnapshot {
        sender_id: String,
        timestamp: f64,
        snapshot_json: String,
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
        error_type: ErrorType,
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
    /// Replicated cluster-state mutations. Receivers apply each
    /// mutation in order against their local
    /// `dynrunner_manager_distributed::cluster_state::ClusterState`.
    /// Carrying a `Vec` instead of a single mutation lets the
    /// originator batch the bulk-load case (thousands of `TaskAdded`
    /// at run start) into one wire message; single-mutation events
    /// (one `TaskAssigned` per dispatch) use a one-element vec.
    ClusterMutation {
        sender_id: String,
        timestamp: f64,
        mutations: Vec<ClusterMutation<I>>,
    },
    /// Wire-only relay envelope. A peer that can't reach `target_id`
    /// directly wraps the message in this variant and sends it to a
    /// reachable forwarder; the forwarder unwraps if it's the target,
    /// or appends itself to `path` and forwards to another non-`path`
    /// peer if not.
    ///
    /// `path` records every peer the message has visited (originator
    /// plus each forwarder, in order). Loop prevention: a forwarder
    /// MUST pick a candidate that is not in `path`, not the target
    /// itself, and not its own id. If no such candidate exists, the
    /// forwarder sends a [`RelayBackoff`] back to its predecessor
    /// (the last entry in `path` from its view) so the predecessor
    /// can mark this forwarder tried and pick another candidate.
    /// `relay_id` is the originator's monotonic counter; the
    /// cluster-wide key is `(sender_id, relay_id)` so collisions
    /// between independent originators are impossible.
    ///
    /// Application code never observes `Relay` — `recv_peer` strips
    /// the envelope before delivery.
    Relay {
        sender_id: String,
        timestamp: f64,
        target_id: String,
        relay_id: u64,
        path: Vec<String>,
        inner: Box<DistributedMessage<I>>,
    },
    /// Wire-only signal from a forwarder back to its predecessor in
    /// the relay path: "I (`sender_id`) could not forward
    /// `relay_id` — mark me tried and try another peer."
    /// Predecessor looks up its `outgoing_relays[(orig_sender,
    /// relay_id)]` state, removes `sender_id` from candidates, and
    /// either retries with the next-lowest-id reachable peer or
    /// propagates the backoff one step further back. The originator
    /// drops the relay with a warn when its own candidates exhaust.
    RelayBackoff {
        sender_id: String,
        timestamp: f64,
        original_sender: String,
        relay_id: u64,
    },
    /// Envelope used for role-based addressing. The sender resolves
    /// `intended_role` to a specific peer-id via its local RoleTable
    /// cache and ships this envelope to that peer. The receiver checks
    /// its OWN cache; if it agrees it holds `intended_role`, it unwraps
    /// and processes `payload`. If not, the receiver's relay-and-hint
    /// path (Step 4) forwards `payload` to whoever IT thinks holds the
    /// role AND sends a `RoleMisaddressHint` back to `sender_id` to
    /// warm the sender's cache.
    ///
    /// `attempts` is the relay-hop counter — if it exceeds a safety
    /// bound (e.g. 3), the receiver drops the envelope rather than
    /// relay further. This caps relay storms when the cluster is in
    /// disagreement about who holds the role.
    ///
    /// `payload` is boxed because `DistributedMessage` is variant-
    /// sized; an unboxed recursive enum would blow up the
    /// discriminant size and force every other variant to carry the
    /// max-variant overhead. Same pattern as `Relay { inner: Box<_> }`.
    ///
    /// `attempts` carries `#[serde(default)]` so pre-Step-3 wire
    /// senders that omit the field decode as `attempts=0` — same
    /// backcompat shape we use for `is_observer`, `required_setup`,
    /// and the Phase-4b binary-info tags.
    RoleAddressed {
        sender_id: String,
        timestamp: f64,
        intended_role: Role,
        payload: Box<DistributedMessage<I>>,
        #[serde(default)]
        attempts: u8,
    },
    /// Sent back to a `RoleAddressed` sender whose cache was stale.
    /// The receiver tells the sender "you thought I was `role`, but the
    /// actual holder is `holder_id`". The sender writes `holder_id` into
    /// its own RoleTable cache for `role`, so the next send via
    /// `Address::Role(role)` routes correctly on the first hop. Purely
    /// cache-warming; the original payload was already forwarded by the
    /// relaying receiver (Step 4), so the sender does NOT re-send.
    RoleMisaddressHint {
        sender_id: String,
        timestamp: f64,
        role: Role,
        holder_id: String,
    },
}
