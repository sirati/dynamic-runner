pub mod state;
pub mod message_router;
pub mod primary;
pub mod secondary;
pub mod zip_extract;

pub use primary::{PrimaryCoordinator, PrimaryConfig};
pub use primary::wire::compute_task_hash;
pub use secondary::{SecondaryCoordinator, SecondaryConfig, PeerCertInfo};
// Re-export transport traits from the comm API crate for convenience.
pub use db_primary_secondary_comm::{PrimaryTransport, SecondaryTransport};
pub use state::{
    SecondaryConnection, AwaitingWelcome, Handshaking, CertExchanging, PeerDiscovery,
    InitialAssigning, Operational, ShuttingDown, SecondaryConnectionState,
};
pub use message_router::{MessageRouter, RoutedMessage};
