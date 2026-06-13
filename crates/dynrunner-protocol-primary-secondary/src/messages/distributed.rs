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
    /// Observer -> Primary: "gracefully abort the run". The ONE
    /// management command a zero-authority observer may send: the
    /// primary reacts by originating
    /// `ClusterMutation::GracefulAbortRequested` (the replicated sticky
    /// freeze latch — see that variant's doc for the full drain
    /// protocol). The request itself carries no authority: a primary
    /// that already latched the freeze NoOps it (idempotent under
    /// operator re-triggering / at-least-once delivery), and a request
    /// that never reaches a live primary simply has no effect until the
    /// operator re-sends it. A typed wire variant (NOT a string-topic
    /// custom message), so receivers dispatch on the discriminant.
    GracefulAbortRequest {
        /// Mesh routing target (Phase-C C3): same contract as every
        /// other variant's `target` field — `None` on a freshly-
        /// constructed frame, stamped by the egress edge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
    },
    /// Late-joiner / reconnecting / behind node asks one peer to STREAM
    /// it the replicated `ClusterState` as a sequence of bounded
    /// [`Self::SnapshotStreamPackage`] frames. Any peer can respond —
    /// state is replicated, so any responder suffices. The originator
    /// targets a specific peer via the unicast transport; no broadcast.
    /// Replaces the monolithic request/snapshot frame pair: the answer
    /// is produced one bounded package per responder-loop wakeup, so a
    /// 100 MB ledger never serializes (or transmits) as one frame.
    RequestSnapshotStream {
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
        /// Requester-minted stream correlation id (unique per pull:
        /// `<requester-id>/<counter>` — node ids are cluster-unique and
        /// the counter is per-process, so the pair is unique without an
        /// RNG, which this deterministic runtime deliberately avoids).
        /// Every `SnapshotStreamPackage` answering this request carries
        /// it back, and a RESUME re-request repeats it so a responder
        /// still holding the stream repositions instead of restarting.
        stream_id: String,
        /// Resume cursor: the canonical task key (the sorted-key total
        /// order every responder shares) up to which the requester has
        /// already applied packages. `None` asks for the stream from the
        /// start; `Some(k)` asks for task entries STRICTLY AFTER `k`.
        /// Any responder can serve a resume — the cursor is a ledger
        /// key, not a responder-local offset. `#[serde(default)]` keeps
        /// pre-field senders wire-compatible (decode as `None` — a
        /// from-the-start stream, the conservative shape).
        #[serde(default)]
        resume_after: Option<String>,
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
    /// One package of a snapshot STREAM answering
    /// [`Self::RequestSnapshotStream`]: a partial `ClusterStateSnapshot`
    /// the receiver merges via the ONE idempotent `ClusterState::restore`
    /// lattice. The stream replaces the monolithic snapshot frame: the
    /// CRDT join is idempotent / commutative / monotone, so NO consistent
    /// cut is needed — each package is a valid partial snapshot on its
    /// own, packages interleave safely with live mutation broadcasts, and
    /// anything stale behind the stream's cursor converges through
    /// gossip / anti-entropy.
    ///
    /// `payload` carries the partial snapshot as base64-wrapped CBOR
    /// (CBOR for compact, escape-free encode/decode; base64 because the
    /// wire envelope is JSON, which has no raw-bytes representation).
    /// Wire-side erasure of the generic `I` parameter keeps the envelope
    /// concrete for routing — the receiver decodes back into
    /// `ClusterStateSnapshot<I>` once `I` is known in context (the same
    /// dependency-direction rule the old JSON field followed: this crate
    /// never names `ClusterStateSnapshot`).
    SnapshotStreamPackage {
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
        /// The correlation id of the `RequestSnapshotStream` this package
        /// answers (requester-minted; see that field's doc).
        stream_id: String,
        /// Responder-side package counter within the stream (0-based,
        /// monotone per send). Diagnostics + done-accounting only — the
        /// receiver's merge is order-independent (idempotent lattice), so
        /// a gap never blocks application; the resume cursor rides the
        /// canonical TASK KEY, not this counter.
        seq: u64,
        /// The canonical-order resume cursor AFTER applying this package:
        /// the highest task key the package carries, stamped by the
        /// responder so receivers track resume progress WITHOUT decoding
        /// the opaque payload (the bootstrap collector lives in this
        /// crate, which must stay free of `ClusterStateSnapshot`).
        /// `None` for packages carrying no task entries (the small-field
        /// head / tail packages). A receiver re-requesting an interrupted
        /// stream echoes its last seen cursor as `resume_after`.
        cursor: Option<String>,
        /// Base64-wrapped CBOR of one partial `ClusterStateSnapshot`.
        payload: String,
        /// `true` on the stream's final package. A receiver that saw
        /// `done` has the full snapshot as of the stream's start (modulo
        /// the documented tail rules); one that never sees it re-requests
        /// with its resume cursor on the pull cadence.
        done: bool,
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
    /// snapshot stream via the existing `RequestSnapshotStream` →
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
        /// The sender's SELF-DECLARED role (the same `is_observer` bit it
        /// stamps on a `RequestSnapshotStream`), carried so a peer that
        /// pulls a snapshot FROM this sender (the proven-ahead responder)
        /// types the pull's [`Destination`] off the sender's role —
        /// `Observer(id)` for an observer sender, `Secondary(id)` for a
        /// compute peer — instead of guessing `Secondary` for every
        /// target. This is the inverse of the snapshot-RPC reply policy
        /// (`reply_destination`): there the request carries the requester's
        /// role and the reply is typed off it; here the digest carries the
        /// sender's role and the pull is typed off it. Without it, a pull
        /// directed at an observer is mis-typed `Secondary`, missing the
        /// observer's ingress role-slot demux (served by the fan, but with
        /// a per-pull WARN). `#[serde(default)]` decodes a pre-field peer
        /// as `false` (the conservative compute-role shape — the receiver-
        /// side id==self fan covers the residual mis-type without noise).
        #[serde(default)]
        sender_is_observer: bool,
    },
    /// Pull-model PROBE: a node that detected it is behind broadcasts this
    /// to its DIRECT mesh neighbours (never relayed onward) to discover the
    /// least-loaded peer that holds ledger data it lacks. Carries the
    /// requester's own [`StateDigest`] so each responder can compute, on its
    /// own side, whether it is AHEAD of the requester (it holds data the
    /// requester is missing) — the cheap correctness filter that stops the
    /// requester ever burning a pull cycle on a peer that cannot help. Every
    /// direct neighbour answers with a [`Self::PullProbeReply`]. This is the
    /// single-flight, load-balanced replacement for the eager per-digest
    /// immediate pull (`anti_entropy::reconcile_against_peer`): a node runs
    /// at most one probe→pull cycle at a time regardless of how many
    /// divergent digests it sees, so a perpetually-behind replica under
    /// churn initiates pulls at the cooldown rate, NOT one per inbound
    /// digest (the #491 snapshot-package storm).
    ///
    /// All fields are `#[serde(default)]`-wire-compatible: a pre-field peer
    /// that cannot decode the new `msg_type` tag fails the decode loudly-
    /// but-gracefully through the framed-IO pumps (same contract as every
    /// other variant added to this enum), and a future field added here
    /// decodes as its zero value on a peer that predates it.
    PullProbe {
        /// Mesh routing target (Phase-C C3) — same contract as every other
        /// variant. A `PullProbe` is sent to [`Destination::All`]; the
        /// receiving mesh-pump's `route_incoming` local-fans an inbound
        /// `All` frame and NEVER re-broadcasts it, so the probe reaches
        /// only the sender's DIRECT neighbours, exactly as the protocol
        /// requires (direct-neighbours-only, never relayed onward).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The requester's own current digest. The responder computes
        /// `ahead = self_digest.is_behind(requester_digest) == false &&
        /// requester_digest.is_behind(self_digest)` — i.e. the responder
        /// holds ledger data the requester lacks — and stamps the result on
        /// its [`Self::PullProbeReply`]'s `ahead` bit. Computed
        /// responder-side so the requester need not carry every peer's
        /// digest; the probe carries the requester's digest exactly once.
        digest: StateDigest,
    },
    /// Pull-model PROBE REPLY: a direct neighbour's answer to a
    /// [`Self::PullProbe`], reporting its current inbox depth and whether it
    /// is AHEAD of the requester. The requester collects replies for a 1s
    /// selection window, filters to the `ahead` responders (Decision A: the
    /// ahead-filter — never pull from a peer that cannot help), and picks
    /// the one with the SMALLEST `inbox_size` as its pull target (the
    /// least-loaded ahead peer answers fastest and starves no one). If the
    /// 1s window elapses with zero replies, the FIRST subsequent reply is
    /// chosen. The chosen target then receives a `RequestSnapshotStream`
    /// (resume-from-last-good cursor); subsequent chunks target the SAME
    /// peer until a 1-minute rebalance re-probes.
    ///
    /// Sent DIRECTLY back to the requester (`Destination::Secondary(id)` /
    /// `Destination::Observer(id)` — the requester's id, typed off its
    /// declared role); never relayed (a reply that cannot reach the
    /// requester directly is simply lost, and the requester's 30s re-probe
    /// recovers).
    PullProbeReply {
        /// Mesh routing target (Phase-C C3) — same contract as every other
        /// variant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The requester this reply answers (the `sender_id` of the
        /// [`Self::PullProbe`] that triggered it). The requester matches
        /// replies to its outstanding probe by this; a reply whose
        /// `requester` is not this node's id is ignored. Named `requester`
        /// (not `target`) because the wire envelope's routing-header field
        /// is already `target` (the C3 stamp); this is the application-level
        /// addressee, the probe originator.
        requester: String,
        /// The responder's current role-inbox depth (queued frames) — the
        /// load signal the requester selects the SMALLEST of among the
        /// `ahead` responders. A deep inbox means the responder is already
        /// backed up; pulling from it would worsen its starvation.
        inbox_size: u64,
        /// Whether the responder holds ledger data the requester lacks
        /// (`requester_digest.is_behind(responder_digest)`), computed
        /// responder-side from the digest the [`Self::PullProbe`] carried.
        /// Only `ahead` responders are pull candidates (Decision A); if no
        /// reply is `ahead`, the requester pulls from no one this cycle (it
        /// is caught up, or the others are behind too — the next probe
        /// re-evaluates). `#[serde(default)]` decodes a pre-field peer as
        /// `false` — the conservative "cannot help" shape, so a legacy
        /// responder is never selected as a pull target.
        #[serde(default)]
        ahead: bool,
    },
    /// Pull-model FAIL: the chosen pull TARGET could not serve the
    /// requester's `RequestSnapshotStream` because the DIRECT link to the
    /// requester dropped in the meantime. Unlike a [`Self::PullProbeReply`]
    /// (direct-only, lost if the leg is gone), a `PullFail` rides the
    /// existing relay-toward-the-role-holder path so it is delivered
    /// INDIRECTLY when the direct leg to the requester is gone — that is
    /// precisely the case it signals. On receipt the requester drops the
    /// dead target and falls to the NEXT target in its smallest-inbox-
    /// ordered candidate list (the FAIL→next-target fallback).
    PullFail {
        /// Mesh routing target (Phase-C C3) — same contract as every other
        /// variant. Stamped `Destination::Secondary(requester)` /
        /// `Observer(requester)`; the ingress role-miss relay forwards it
        /// toward the requester's recognized holder when the direct leg is
        /// gone (the indirect-delivery contract this frame exists for).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The requester this fail is addressed to (the originator of the
        /// `RequestSnapshotStream` the responder could not answer). Named
        /// `requester` (not `target`) for the same reason as
        /// [`Self::PullProbeReply::requester`]: the routing-header field is
        /// already `target`.
        requester: String,
        /// The `stream_id` of the `RequestSnapshotStream` that failed, so
        /// the requester correlates the fail to its in-flight pull and
        /// abandons exactly that attempt before falling to the next target.
        stream_id: String,
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
        /// Per-origin CAUSAL custom-message watermark (the
        /// message-vs-phase-end ordering gate): the highest IMPORTANT
        /// `CustomMessage::msg_seq` the reporting secondary had stamped
        /// when this terminal first left it. The primary DEFERS
        /// processing this terminal until the origin's replicated
        /// custom-inbox terminal watermark covers the stamp — every
        /// important message the consumer handed to
        /// `SecondaryHandle.send_to_primary` BEFORE the terminal went
        /// out is Handled/Failed-resolved first — so phase-end, which
        /// derives from terminals, can never overtake the messages the
        /// task causally sent. IMPORTANT-only by construction
        /// (droppables are never counted in the `msg_seq` space), so a
        /// legitimately-lost droppable can never wedge the gate.
        /// Stamped once at the secondary's `send_to_primary` chokepoint
        /// (sticky across replays, like `delivery_seq`). `None` for a
        /// pre-field sender — the gate is open (no causal claim).
        /// `#[serde(default, skip_serializing_if)]` keeps the wire
        /// bytes byte-identical while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        msgs_posted_through: Option<u64>,
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
        /// Per-origin CAUSAL custom-message watermark — see
        /// [`DistributedMessage::TaskComplete`]'s twin field for the
        /// full ordering-gate contract. A failed task's causally-prior
        /// important messages gate its terminal identically (the
        /// consumer's phase-end barrier must observe them either way).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        msgs_posted_through: Option<u64>,
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
    /// Secondary -> Primary consumer-defined message (F5). The framework
    /// never interprets `topic` or `data` — they are the consumer's
    /// routing key + payload, delivered to the primary-side
    /// `custom_message_handler` hook.
    ///
    /// Two delivery classes, selected by `important`:
    ///   * `important = false` (DROPPABLE): fire-and-forget through the
    ///     secondary's `send_to_primary` chokepoint — no retention, no
    ///     CRDT residency, at-most-once. Lost on failover by design.
    ///   * `important = true`: stamped with `delivery_seq` at the
    ///     chokepoint and RETAINED exactly like a terminal report (the
    ///     #352 machinery): unacked frames replay with the SAME seq
    ///     through the re-resolving [`Destination::Primary`], so a
    ///     failover mid-flight re-lands at the NEW primary. The primary
    ///     acks per landing with the existing
    ///     [`DistributedMessage::TerminalAck`] and originates the
    ///     `CustomMessagePosted` / `CustomMessageHandled` CRDT mutations
    ///     (see `ClusterMutation`) so a promoted primary replays every
    ///     not-yet-handled message at hydrate.
    CustomMessage {
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
        /// ORIGINATOR identity (NOT the wire `sender_id` — a relayed /
        /// peer-forwarded landing carries a forwarder's `sender_id`
        /// while the originator's retention buffer still waits). The
        /// `(origin_secondary_id, msg_seq)` pair is the message's
        /// cluster-wide IDEMPOTENCY KEY: a transport replay re-posts
        /// the same key and the CRDT apply NoOps it.
        origin_secondary_id: String,
        /// Per-origin monotonic message sequence, stamped by the
        /// originating secondary's custom-message send entry point.
        /// With `origin_secondary_id` it forms the idempotency key.
        /// Distinct from `delivery_seq` (the #352 retention/ack key):
        /// `msg_seq` identifies the MESSAGE across the cluster;
        /// `delivery_seq` identifies one retention-buffer entry on the
        /// originator.
        msg_seq: u64,
        /// Consumer routing key — the framework never interprets it.
        topic: String,
        /// Consumer payload, ≤ [`crate::CUSTOM_MESSAGE_MAX_BYTES`]
        /// (enforced at the send entry points, tolerated by the wire).
        data: Vec<u8>,
        /// Delivery class — see the variant doc.
        important: bool,
        /// App-level delivery-confirmation sequence id (#352),
        /// IMPORTANT-only: stamped at the secondary's `send_to_primary`
        /// chokepoint (the same per-secondary monotonic counter the
        /// terminal reports use); the primary's ingest answers each
        /// landing with a [`DistributedMessage::TerminalAck`] carrying
        /// it back. A replay re-sends the SAME seq. Always `None` on a
        /// droppable frame (never stamped — droppables are not
        /// retained). `#[serde(default, skip_serializing_if)]` keeps
        /// the wire bytes byte-identical while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_seq: Option<u64>,
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
    /// Primary -> provider-host observer: "submit a replacement
    /// secondary for me". The respawn DECISION (budget, id mint,
    /// replicated-ledger spend) lives on the primary — wherever it runs;
    /// the physical spawn PROVIDER (the SLURM job-manager + gateway +
    /// tunnel pool, or the multi-process child registry) lives in the
    /// SUBMITTER process only (mesh node-id `"setup"`), which keeps it
    /// across its own primary→observer demotion. A relocated/promoted
    /// primary therefore delegates execution over the mesh with this
    /// frame; a primary with a LOCAL provider never sends it.
    ///
    /// Carries exactly the `SecondarySpawnSpec` the provider trait
    /// consumes. `new_secondary_id` is the cluster-unique id the primary
    /// minted — it doubles as the request's correlation AND idempotency
    /// key: a re-sent request (retry while the observer was unreachable,
    /// or a lost result) re-uses the SAME id and the observer-side
    /// execution arm dedupes on it, so one id can never double-submit.
    RespawnSpawnRequest {
        /// Mesh routing target (Phase-C C3) — same contract as on every
        /// other variant. `#[serde(default, skip_serializing_if)]` keeps
        /// the wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The freshly-minted replacement id (correlation + idempotency
        /// key — see the variant doc).
        new_secondary_id: String,
        /// `SecondarySpawnSpec::primary_endpoint`, relayed verbatim.
        /// The SLURM provider ignores it (the respawned secondary
        /// fetches its run config over the mesh); carried for the
        /// provider-trait spec shape, not interpreted in transit.
        primary_endpoint: String,
        /// `SecondarySpawnSpec::primary_pubkey_pem`, relayed verbatim
        /// (same forward-compat contract as `primary_endpoint`).
        primary_pubkey_pem: String,
        /// `SecondarySpawnSpec::dead_member_id` — the id of the dead
        /// member the replacement stands in for, relayed verbatim so an
        /// observer-hosted provider can resolve its SLURM node (job id →
        /// squeue/sacct) and exclude that node from the replacement's
        /// sbatch (the in-process provider reads it off the spec
        /// directly). `None` when there is no dead member to key on;
        /// `#[serde(default, skip_serializing_if)]` keeps the wire bytes
        /// unchanged in that case.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dead_member_id: Option<String>,
    },
    /// Provider-host observer -> primary: the outcome of one
    /// [`Self::RespawnSpawnRequest`], correlated by `new_secondary_id`.
    /// `error = None` is success; `Some(reason)` feeds the primary's
    /// existing respawn failure logging/budget exactly as a local
    /// provider `Err` does. Sent once per completed execution AND
    /// re-sent from the observer's outcome cache when a duplicate
    /// request for the same id lands (the lost-result replay).
    RespawnSpawnResult {
        /// Mesh routing target (Phase-C C3) — same contract as on every
        /// other variant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Echoes the request's `new_secondary_id` verbatim.
        new_secondary_id: String,
        /// `None` = the provider spawned successfully; `Some` carries
        /// the provider's error string.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Primary -> provider-host observer: "revoke the replacement
    /// previously requested for `new_secondary_id`" — the remote leg of
    /// [`SecondarySpawner::revoke`]'s re-admission revocation (the
    /// member the replacement was spawned for came back alive before
    /// the replacement joined). Idempotent at the provider by the
    /// trait's contract (Submitted→scancel / not-yet-submitted→tombstone),
    /// so re-sends and request/revoke races need no observer-side dedup.
    RespawnRevokeRequest {
        /// Mesh routing target (Phase-C C3) — same contract as on every
        /// other variant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// The replacement id whose pending submission is revoked.
        new_secondary_id: String,
    },
    /// Provider-host observer -> primary: the outcome of one
    /// [`Self::RespawnRevokeRequest`], correlated by `new_secondary_id`.
    /// `error = None` = revoked (or quietly already-gone, per the
    /// provider contract); `Some(reason)` = the provider could not reach
    /// its backend — the primary logs loudly and the provider-side
    /// run-teardown sweep remains the reclamation backstop.
    RespawnRevokeResult {
        /// Mesh routing target (Phase-C C3) — same contract as on every
        /// other variant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Echoes the request's `new_secondary_id` verbatim.
        new_secondary_id: String,
        /// `None` = revoke succeeded (or was a quiet no-op).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
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
    /// Wire-only signal from the NON-dial-owning side of a
    /// member↔member mesh leg to the leg's DIAL OWNER (the
    /// lower-id-dials rule fixes ownership): "my end of our wire is
    /// dead — prune your entry for me and re-dial." Sent on the
    /// requester's reconnect-tick cadence while its leg to the
    /// recipient is tracked-disconnected, and — because the direct leg
    /// is down by definition — typically delivered via [`Self::Relay`].
    ///
    /// This closes the half-open hole the lower-id-dials rule leaves
    /// open: when the dial owner's side of the wire still looks healthy
    /// (its frames keep flowing one way) it never observes a disconnect
    /// and never re-dials, while the non-owner side structurally never
    /// dials. The request is authoritative evidence from the other end
    /// that the wire is useless; the owner force-prunes its stale
    /// connection entry (so the fresh dial's registration is not
    /// dedup-dropped against it) and dials immediately.
    ///
    /// Application code never observes `RedialRequest` — the transport's
    /// recv path consumes it. It restores the TRANSPORT PIPE only and
    /// feeds no liveness/failover input; the requester keeps narrating
    /// the outage through its reconnect tracker's milestone WARNs.
    RedialRequest {
        /// Mesh routing target (Phase-C C3) — same contract as on every
        /// other variant. `#[serde(default, skip_serializing_if)]` keeps
        /// the wire bytes unchanged while the field is `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Consecutive reconnect ticks the requester has observed the
        /// leg dead (its tracker's attempt count). The dial owner's
        /// GRACE gate keys on this: a low count may be the
        /// mesh-forming / first-frame-identification window, where the
        /// owner's wire is healthy and its own next regular frame will
        /// identify it at the requester's accept loop — force-pruning
        /// there kills a good wire (and any frames queued on it). Only
        /// a PERSISTING request (count past the grace threshold) proves
        /// the wire dead from the requester's side despite the owner
        /// having spoken on it. `#[serde(default)]` keeps pre-field
        /// senders decodable (count 0 → inside the grace window).
        #[serde(default)]
        attempts: u32,
    },
    /// Wire-only chunk of ONE oversized framework frame (#367 — the
    /// 67k-task `ClusterSnapshot` that exceeded the wire cap and was
    /// serialize-dropped in a loop, starving anti-entropy / rejoin
    /// forever). A frame whose serialized size exceeds the transport's
    /// wire limit AND whose type is [`Self::chunk_eligible`] is split by
    /// the SENDING framing layer into N of these (each individually
    /// under the cap) and reassembled by the RECEIVING framing layer
    /// before delivery; the application layer never observes the
    /// variant (the same contract as `Relay` / `RedialRequest`, one
    /// layer further down — chunks are consumed at the framed-IO pumps,
    /// below even the Router).
    ///
    /// NOT a relaxation of the wire cap for consumer payloads: the
    /// eligibility classifier is a closed framework-frame allowlist
    /// (`ClusterSnapshot`, possibly `Relay`-wrapped), and the receiver
    /// re-checks eligibility after reassembly, so an oversized
    /// `TaskComplete`/`CustomMessage` can neither be chunked nor smuggled
    /// through reassembly (#364/#366 semantics stay).
    ///
    /// Reassembly contract (see `chunking::ChunkReassembler`):
    /// `transfer_id` identifies the transfer on a connection;
    /// `index`/`total` order the slices; `checksum` (FNV-1a-64 of the
    /// COMPLETE reassembled payload) proves integrity. Chunks of one
    /// transfer are written back-to-back on one ordered leg, so a gap /
    /// supersede / checksum mismatch is a transfer-fatal fault: the
    /// receiver abandons the partial with ONE loud WARN and the
    /// higher-level trigger (the anti-entropy digest cadence) is the
    /// bounded retry. A pre-field receiver fails LOUDLY-but-gracefully:
    /// the unknown `msg_type` tag is a decode error, which the framed-IO
    /// pumps surface at ERROR and resolve through the NORMAL disconnect
    /// path (never a panic).
    FrameChunk {
        /// Mesh routing target — present for wire-shape uniformity with
        /// every other variant; chunks travel leg-level (below routing),
        /// so the egress never stamps it. `#[serde(default,
        /// skip_serializing_if)]` keeps the wire bytes minimal.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Destination>,
        sender_id: String,
        timestamp: f64,
        /// Sender-local monotonic transfer id: all chunks of one
        /// oversized frame carry the same value; a new transfer on the
        /// same leg carries a strictly different one (supersede
        /// detection).
        transfer_id: u64,
        /// Zero-based slice index within the transfer.
        index: u32,
        /// Total number of slices in the transfer (≥ 1, constant across
        /// the transfer).
        total: u32,
        /// FNV-1a-64 over the COMPLETE reassembled payload bytes.
        checksum: u64,
        /// This slice's bytes, base64-encoded (standard alphabet, with
        /// padding). Base64 is byte-boundary-safe at any split point and
        /// inflates 4/3 — vs serde_json's ~4x number-array encoding of
        /// `Vec<u8>` and the unbounded re-escaping cost of nesting JSON
        /// text in a JSON string.
        payload_b64: String,
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
