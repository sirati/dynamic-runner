pub mod cluster_state;
pub mod state;
pub mod message_router;
pub mod primary;
pub mod secondary;
pub mod zip_extract;

pub use primary::{PrimaryCoordinator, PrimaryConfig};
pub use primary::wire::compute_task_hash;
pub use zip_extract::compute_file_hash;
pub use secondary::{SecondaryCoordinator, SecondaryConfig, PeerCertInfo};
// Re-export transport traits from the comm API crate for convenience.
pub use dynrunner_protocol_primary_secondary::{PrimaryTransport, SecondaryTransport};
pub use state::{
    SecondaryConnection, AwaitingWelcome, Handshaking, CertExchanging, PeerDiscovery,
    InitialAssigning, Operational, ShuttingDown, SecondaryConnectionState,
};
pub use message_router::{MessageRouter, RoutedMessage};
pub use cluster_state::{ApplyOutcome, ClusterState, StateCounts, TaskState};
