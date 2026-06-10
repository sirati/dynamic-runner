//! `DistributedMessage<I>` — the typed wire enum carrying every wire
//! frame the runner exchanges, plus its `sender_id` / `timestamp` /
//! `msg_type` accessors. Generic over the identifier type `I` so
//! task-related variants can carry runner-specific opaque keys without
//! pulling crate-level details into the wire shape.

use std::collections::BTreeMap;

use dynrunner_core::{ErrorType, ResourceAmount, TaskOutputs};
use serde::{Deserialize, Serialize};

use crate::address::Destination;
use crate::cluster_mutation::ClusterMutation;
use crate::messages::binary_info::{DistributedBinaryInfo, StagedFileRecord, ZipFileAssignment};
use crate::messages::peer_info::{PeerConnectionInfo, WorkerReadyInfo};
use crate::messages::state_digest::StateDigest;

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
/// existing `Box` wrapper on `Relay.inner` exists for recursive-shape
/// reasons (a self-referential variant must be boxed), not for size
/// containment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "msg_type", rename_all = "snake_case")]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
#[allow(clippy::large_enum_variant)]
pub enum DistributedMessage<I> {
    SecondaryWelcome {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Whether this secondary can host the primary role on promotion —
        /// under mesh-always (pillar 1) a network compute secondary always
        /// holds a peer mesh, so its host can build a `PrimaryCoordinator`
        /// when it is named primary (only an observer / the in-process
        /// same-host secondary advertises `false`). The primary translates
        /// this into the secondary's replicated `RoleTable.can_be_primary`
        /// entry (the `PeerJoined { can_be_primary }` it originates on
        /// welcome-accept), which the bootstrap-relocation / promotion
        /// selection reads
        /// as the single authoritative capability marker. Mirrors the
        /// `is_observer` advertisement exactly. `#[serde(default)]` keeps
        /// pre-field senders wire-compatible (they decode as `false` — the
        /// conservative "submitter stays primary" value).
        #[serde(default)]
        can_be_primary: bool,
    },
    Entropy {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        entropy_hex: String,
    },
    CertExchange {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        public_cert_pem: String,
        ipv4_address: Option<String>,
        ipv6_address: Option<String>,
        quic_port: u16,
        /// UDP port this node's liveness-beacon listener is bound on, to
        /// be paired with `ipv4_address`/`ipv6_address` by a peer's beacon
        /// once this node becomes primary (the "primary on ANY peer"
        /// invariant — every primary-capable node advertises one).
        /// Separate from `quic_port` (quinn owns that UDP socket).
        /// `#[serde(default, skip_serializing_if)]` keeps pre-beacon wire
        /// bytes unchanged while it is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        liveness_port: Option<u16>,
    },
    PeerInfo {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        peers: Vec<PeerConnectionInfo>,
    },
    InitialAssignment {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        available_resources: Vec<ResourceAmount>,
    },
    TaskAssignment {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        file_hash: String,
        content_hash: String,
        src_path: String,
        dest_path: String,
    },
    /// Secondary -> Primary: "my peer-mesh has finished forming
    /// (or was empty / fully failed to form)". Emitted once per
    /// secondary, after `connect_to_peers` has either landed at
    /// least one peer connection or the per-secondary peer-mesh
    /// watchdog has elapsed; for single-secondary runs (no peers
    /// to dial) it fires immediately at operational-loop entry.
    /// The primary defers its bootstrap primary announcement until
    /// every secondary has reported, so a newly-named primary never
    /// becomes authoritative against an empty peer mesh — closing
    /// the 750µs ↔ 30s gap where pre-mesh-formation messages
    /// would be sent into a void. `peer_count` carries the
    /// observed peer-connection count at signal time (0 in the
    /// single-secondary or fully-failed cases).
    MeshReady {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// The joiner's own primary-capability marker — twin of
        /// `is_observer`. A late-joining compute secondary that can host
        /// the primary on demand declares `true`; an observer late-joiner
        /// declares `false`. The snapshot responder broadcasts a
        /// `ClusterMutation::PeerJoined { can_be_primary }` carrying this
        /// truth so the replicated `RoleTable.can_be_primary` records the
        /// joiner's capability. `#[serde(default)]` keeps pre-field
        /// senders wire-compatible (decode as `false`).
        #[serde(default)]
        can_be_primary: bool,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        snapshot_json: String,
    },
    /// Joining / reconnecting / respawned secondary asks any connected
    /// peer for the cluster-wide run configuration (the consumer's
    /// `forwarded_argv`). The run-config is replicated, so any peer can
    /// answer — the originator targets a specific peer via the unicast
    /// transport; no broadcast (one response is enough). Carries no
    /// payload beyond the routing/common fields: the request says only
    /// "send me the run-config".
    RequestRunConfig {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
    },
    /// Response to `RequestRunConfig`: the cluster-wide run configuration
    /// the requester splices onto its own argv to reconstruct the
    /// identical run-config the consumer would have passed on the launch
    /// command line. A SINGLE authoritative field — `forwarded_argv` — is
    /// the run-config; there is no second derived/split copy.
    ///
    /// `#[serde(default)]` keeps pre-field senders wire-compatible: a
    /// frame omitting `forwarded_argv` entirely decodes as an empty vec
    /// (the conservative "no extra run-config" value).
    RunConfig {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        #[serde(default)]
        forwarded_argv: Vec<String>,
    },
    /// Periodic anti-entropy fingerprint. Every role broadcasts its
    /// [`StateDigest`] on the convergence cadence; a receiver compares
    /// the carried digest against its own (`StateDigest::is_behind`) and,
    /// when it finds the sender holds ledger data it is missing, pulls a
    /// full snapshot via the existing `RequestClusterSnapshot` →
    /// `ClusterSnapshot` → `restore()` path. The digest is the DETECTOR
    /// only — it carries no task payloads (just per-field counts + `u64`
    /// folds) and triggers no merge by itself, so steady-state cost is a
    /// fixed handful of integers per node per period and a converged mesh
    /// exchanges digests that match and pull nothing (self-quiescing).
    ///
    /// The digest is identifier-erased by construction (every member is a
    /// `u64`/`bool` summary, never an `I`-typed payload), so the frame
    /// carries the concrete [`StateDigest`] inline rather than a
    /// JSON-erased string the way `ClusterSnapshot` carries its
    /// `I`-parametric payload.
    StateDigest {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        digest: StateDigest,
    },
    TaskComplete {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        task_hash: String,
        #[serde(default)]
        result_data: Option<Vec<u8>>,
        /// App-level delivery-confirmation sequence id (#352). Stamped by
        /// the reporting secondary's `send_to_primary` chokepoint
        /// (per-secondary monotonic) on every terminal-bearing
        /// primary-bound send; the primary's ingest answers each landing
        /// with a [`DistributedMessage::TerminalAck`] carrying it back.
        /// A replay re-sends the SAME seq, so the ack matches whichever
        /// landing got through. `None` for a pre-field sender (no ack is
        /// emitted, restoring the pre-#352 no-route-only replay
        /// behaviour). `#[serde(default, skip_serializing_if)]` keeps
        /// the wire bytes byte-identical while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_seq: Option<u64>,
    },
    TaskFailed {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        worker_id: u32,
        task_hash: String,
        error_type: ErrorType,
        error_message: String,
        /// App-level delivery-confirmation sequence id (#352) — see
        /// [`DistributedMessage::TaskComplete`]'s twin field for the
        /// full contract. Stamped at the secondary's `send_to_primary`
        /// chokepoint; acked per landing by the primary's ingest;
        /// replays carry the SAME seq. `None` for pre-field senders;
        /// the wire bytes stay byte-identical while `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_seq: Option<u64>,
    },
    /// Primary -> reporting secondary: app-level delivery confirmation
    /// for ONE terminal-bearing report landing (#352).
    ///
    /// A registered-but-blackholed QUIC leg buffers `send.write_all`
    /// locally and returns `Ok` without delivering, and is not pruned
    /// until the 60s idle timeout — so a transport-level "send
    /// succeeded" cannot prove a terminal reached the authority. This
    /// frame is the application-level proof: the primary's ingest emits
    /// exactly one `TerminalAck { seq }` back to the report's
    /// ORIGINATING secondary (the frame's `secondary_id`, NOT the wire
    /// `sender_id` — a relayed/forwarded landing must still clear the
    /// originator's retention buffer) for EVERY terminal landing that
    /// carries a `delivery_seq`, INCLUDING dedup-dropped duplicates
    /// (the duplicate means the original ack was lost or raced; not
    /// re-acking would replay forever). The seq is acked EXACTLY (no
    /// ack-up-to coalescing): replays re-send the same seq across
    /// failover to a new primary, so cumulative semantics could
    /// falsely confirm an earlier seq that travelled a different,
    /// still-blackholed leg; per-landing exact acks are unconditionally
    /// sound and terminals are low-rate, so there is nothing to batch.
    ///
    /// Delivery bookkeeping ONLY — never a liveness signal: the
    /// receiving secondary drops the matching retention-buffer entry
    /// and nothing else (no `primary_link` input is touched on either
    /// presence or absence of an ack).
    TerminalAck {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The confirmed report's `delivery_seq`, echoed verbatim.
        seq: u64,
    },
    /// Primary -> holder secondary: per-task reconciliation probe
    /// (#308). Asks "do you still hold `task_hash` in any live
    /// bookkeeping?" — emitted by the primary's per-task deadline
    /// tracker once the task has been in flight too long with no
    /// terminal. The holder answers with a
    /// [`DistributedMessage::TaskHoldResponse`].
    ///
    /// Accounting reconciliation ONLY — never a liveness signal: a
    /// missing response is left to the existing silent-secondary
    /// machinery (the probe re-arms and takes no action).
    TaskHoldQuery {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The in-flight ledger key being reconciled (the same hash the
        /// `TaskAssignment` carried as `file_hash` and a terminal
        /// carries as `task_hash`).
        task_hash: String,
    },
    /// Holder secondary -> primary: the answer to a
    /// [`DistributedMessage::TaskHoldQuery`]. `held = true` means the
    /// task is genuinely in some live bookkeeping on the responder
    /// (the generation-aware `active_tasks` map or the deferred
    /// `pending_first_bind` set) — the primary re-arms its deadline (a
    /// long task survives unlimited probe rounds). `held = false` is
    /// the responder's positive denial: it will never produce a
    /// terminal for this task, so the primary fails + requeues it
    /// through the backpressure-shaped path.
    TaskHoldResponse {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Echoes the query's `task_hash` so the primary matches the
        /// answer to its outstanding probe.
        task_hash: String,
        /// Whether the responder holds the task in any live bookkeeping.
        held: bool,
    },
    Keepalive {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        active_workers: u32,
        /// Role whose liveness this keepalive asserts. A multi-role host
        /// runs under one peer-id, so the keepalive must say which role's
        /// liveness it is asserting: a `Primary` keepalive refreshes the
        /// receiver's primary-liveness tracking, a `Secondary` keepalive
        /// refreshes its peer-mesh liveness — without this tag the two
        /// signals collapse onto one entry and a multi-role node is
        /// dropped from `peer_keepalives`, corrupting election quorum.
        ///
        /// `#[serde(default)]` decodes a missing field as `Secondary`,
        /// keeping pre-field senders wire-compatible (their keepalives
        /// were the secondary-mesh kind, so `Secondary` is the faithful
        /// historical interpretation).
        #[serde(default)]
        emitter_role: KeepaliveRole,
    },
    TimeoutDetected {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        timed_out_secondary_id: String,
        last_seen: f64,
    },
    TimeoutQuery {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Node id of the suspected-dead party. May be a secondary (when
        /// the querier is the primary or a peer auditing a secondary) or
        /// the primary's node id (when secondaries are checking primary
        /// liveness during failover detection).
        query_node_id: String,
    },
    TimeoutResponse {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Echoes the `query_node_id` from the corresponding TimeoutQuery
        /// so concurrent queries can be matched up by the aggregator.
        query_node_id: String,
        /// AGE in seconds since the responder last saw the queried node,
        /// measured on the responder's own monotonic clock (NOT an absolute
        /// wall-clock timestamp). `None` = the responder has never seen it.
        /// Relative-age keying makes failover-quorum tallies immune to a
        /// coordinated suspend/resume wall-clock jump — there is no cross-node
        /// absolute-clock subtraction.
        last_keepalive: Option<f64>,
    },
    PromotionVote {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        candidate_id: String,
        vote_round: u32,
    },
    PromotionConfirm {
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
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
        /// Mesh routing target (Phase-C C3): the resolved role-bearing
        /// [`Destination`] the egress stamps so the receiving mesh-pump
        /// demuxes the frame to the right local role-slot WITHOUT a
        /// content classifier. `None` on a freshly-constructed frame; the
        /// egress stamps `Some(resolved)` once the coordinators are
        /// rewired. `#[serde(default, skip_serializing_if)]` keeps the
        /// wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        original_sender: String,
        relay_id: u64,
    },
}

/// Which role's liveness a [`DistributedMessage::Keepalive`] asserts.
///
/// A host runs any subset of {primary, secondary, observer} under one
/// peer-id, so a bare keepalive cannot say whether it is a
/// primary-liveness or a peer-mesh-liveness signal. Stamping the emitter
/// role makes the two distinguishable on the wire: the primary keepalive
/// emitter stamps [`Primary`](KeepaliveRole::Primary); the secondary
/// keepalive emitter stamps [`Secondary`](KeepaliveRole::Secondary).
///
/// `Secondary` is the `#[default]` so a keepalive whose `emitter_role`
/// field is absent (a pre-field wire sender) decodes as the
/// secondary-mesh kind — the faithful historical interpretation, since
/// every legacy keepalive fed peer-mesh liveness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeepaliveRole {
    Primary,
    #[default]
    Secondary,
}
