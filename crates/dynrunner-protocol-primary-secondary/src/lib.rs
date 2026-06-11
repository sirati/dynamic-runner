pub mod address;
pub mod cluster_mutation;

/// Maximum `data` payload of a consumer custom message (F5 / F2),
/// secondary→primary AND worker↔secondary alike: 100 KiB. Enforced at
/// the SEND entry points (the Python APIs raise `ValueError` naming the
/// size and the limit; the Rust send seams reject before framing) and
/// comfortably tolerated by the wire. The framework never interprets the
/// payload — the limit exists so a consumer stream stays signal-sized
/// (the #364 wedged-IPC class is structurally impossible at 100 KiB).
pub const CUSTOM_MESSAGE_MAX_BYTES: usize = 100 * 1024;
pub mod chunking;
pub mod codec;
pub mod freshness;
pub mod messages;
pub mod relay;
pub mod removal_cause;
pub mod setup_bootstrap;
pub mod transport;

pub use address::{
    Destination, PeerId, RoleChangeHookRegistrar, RoleTable, SendTarget, resolve_destination,
};
pub use cluster_mutation::{
    ClusterMutation, DiscoveryDebt, PrimaryChangeReason, SecondaryCapacityRecord,
};
pub use chunking::{
    AbandonedTransfer, ChunkIngest, ChunkOutcome, ChunkReassembler, split_frame,
};
pub use codec::{decode_frame, deserialize_message, serialize_message};
pub use freshness::{FreshnessClock, InboundClosed, InboundTap, IngestEdges};
pub use messages::*;
pub use relay::{
    BackoffDecision, Clocks, InboundOutcome, MSG_DIRECT_RESTORED, MSG_DROPPED_AT_ORIGINATOR,
    MSG_RELAY_ENGAGED, OutboundChannel, OutgoingRelay, PeerRouteState, REDIAL_COOLDOWN,
    RELAY_LOG_TARGET, RouteDecision, RouteVia, Router, RoutingError, SendOutcome, forward_step,
    handle_backoff, pick_relay, route_exists, route_send,
};
pub use removal_cause::RemovalCause;
pub use setup_bootstrap::{
    PrimaryPeerSetupBootstrap, SecondarySetupBootstrap, SetupBootstrap, SetupBootstrapBroadcast,
    SetupBootstrapMessage,
};
pub use transport::{DEFAULT_JOIN_TIMEOUT, JoinError, PeerTransport, SecondaryTransport};
