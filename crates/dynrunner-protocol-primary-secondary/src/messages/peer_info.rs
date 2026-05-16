//! Setup-phase peer descriptor structs: `WorkerReadyInfo` (per-worker
//! resource budgets) and `PeerConnectionInfo` (cert / addresses / port
//! / observer flag carried in `PeerInfo` broadcasts).

use dynrunner_core::ResourceAmount;
use serde::{Deserialize, Serialize};

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
    /// Observer-mode flag (task #36). When true, this peer cannot
    /// become primary and has no workers — election code on
    /// receiving secondaries MUST exclude it from `lowest_alive`
    /// candidate selection. Without this filter, an observer with
    /// a lex-low ID would be deferred-to by other peers, then
    /// refuse self-promotion (per `secondary::election`'s observer
    /// guard added in #35), stalling the cluster.
    ///
    /// `#[serde(default)]` keeps pre-#36 wire-senders compatible:
    /// pre-#36 PeerInfo broadcasts omit the field, deserialize
    /// defaults to `false`, regular secondaries continue to
    /// participate as election candidates without operator-visible
    /// change.
    #[serde(default)]
    pub is_observer: bool,
}
