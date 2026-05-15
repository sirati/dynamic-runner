pub mod cluster_state;
pub mod fulfillability_matcher;
pub mod observer;
pub mod peer_lifecycle;
pub mod state;
pub mod message_router;
pub mod primary;
pub mod secondary;
pub mod zip_extract;

pub use primary::{PrimaryCoordinator, PrimaryConfig, RunError};
pub use primary::staging::{compute_initial_staging_entries, StagingError};
pub use primary::wire::compute_task_hash;
pub use zip_extract::compute_file_hash;
pub use secondary::{SecondaryCoordinator, SecondaryConfig, PeerCertInfo, RunOutcome};
// Re-export transport traits from the comm API crate for convenience.
pub use dynrunner_protocol_primary_secondary::SecondaryTransport;
pub use state::{
    SecondaryConnection, AwaitingWelcome, Handshaking, CertExchanging, PeerDiscovery,
    InitialAssigning, Operational, ShuttingDown, SecondaryConnectionState,
};
pub use message_router::{MessageRouter, RoutedMessage};
pub use cluster_state::{
    ApplyOutcome, ClusterState, OutcomeSummary, RoleChangeHook, StateCounts, TaskState,
};
// Re-export the role-table types so downstream crates don't have to
// reach into the protocol crate to type the cache shape.
pub use dynrunner_protocol_primary_secondary::{RoleChangeHookRegistrar, RoleTable};
// Re-export observer-only surface for PyO3 and consumer wire-up. The
// announcer task body + the lifecycle attach helper live behind one
// import path so consumers don't have to descend into the submodule.
pub use observer::{
    attach_observer_announcer, run_observer_announcer, AnnounceTrigger, AnnouncerHandle,
    AnnouncerSender, PeerResourceHoldingsUpdatedPayload,
};
