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
    /// Observer-mode flag. When true, this peer cannot become primary
    /// and has no workers — election code on receiving secondaries MUST
    /// exclude it from `lowest_alive` candidate selection. Without this
    /// filter, an observer with a lex-low ID would be deferred-to by
    /// other peers, then refuse self-promotion, stalling the cluster.
    ///
    /// This is a WIRE role advertisement only; there is NO observer MODE
    /// on a coordinator — the observer role IS the standalone
    /// `ObserverCoordinator`, which advertises `true` here on join.
    ///
    /// `#[serde(default)]` keeps pre-#36 wire-senders compatible:
    /// pre-#36 PeerInfo broadcasts omit the field, deserialize
    /// defaults to `false`, regular secondaries continue to
    /// participate as election candidates without operator-visible
    /// change.
    #[serde(default)]
    pub is_observer: bool,

    /// UDP port this peer's liveness beacon is reachable on, paired with
    /// [`Self::ipv4`] / [`Self::ipv6`]. The liveness beacon is a
    /// transport-INDEPENDENT keepalive path: the primary binds a dedicated
    /// `UdpSocket` on this port and a busy secondary's dedicated beacon
    /// thread sends to `(ipv4, liveness_port)` on a cadence the secondary's
    /// tokio runtime cannot starve (the runtime-CPU-starvation false-death
    /// fix). It is a SEPARATE UDP socket from [`Self::port`] (the QUIC mesh
    /// port quinn owns), reached over the SAME advertised LAN address the
    /// mesh dials — never the bootstrap tunnel.
    ///
    /// `None` when the peer advertises no beacon (an older sender, or a
    /// role that runs no listener); receivers simply skip beaconing to it.
    /// `#[serde(default)]` keeps pre-beacon wire-senders compatible: the
    /// omitted field deserializes to `None`.
    #[serde(default)]
    pub liveness_port: Option<u16>,
}
