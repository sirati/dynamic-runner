pub mod anti_entropy;
pub mod cluster_state;
pub mod discovery;
pub mod fulfillability_matcher;
pub mod message_router;
pub mod observer;
pub mod panik_watcher;
pub mod peer_lifecycle;
pub mod primary;
pub mod process;
pub(crate) mod run_narrator;
pub mod secondary;
pub mod state;
pub mod task_completed;
pub mod worker_signal;
pub mod zip_extract;

#[cfg(test)]
mod test_capture;

pub use primary::staging::{StagingEntry, StagingError, compute_initial_staging_entries};
pub use primary::wire::compute_task_hash;
pub use primary::{
    PhaseHookRaiseLatch, PrimaryConfig, PrimaryCoordinator, PrimaryRunOutcome, RunError,
};
pub use discovery::{SetupDiscovery, SetupDiscoveryFn};
pub use secondary::{
    DEFAULT_PRIMARY_SILENCE_BACKSTOP, FinalizeRunConfigFn, PeerCertInfo, RunOutcome,
    SecondaryConfig, SecondaryCoordinator, SecondaryTerminal,
};
pub use zip_extract::compute_file_hash;
// Re-export transport traits from the comm API crate for convenience.
pub use cluster_state::{
    ApplyOutcome, ClusterState, OutcomeSummary, PhaseRollup, RoleChangeHook, StateCounts, TaskState,
};
pub use dynrunner_protocol_primary_secondary::SecondaryTransport;
pub use message_router::{MessageRouter, RoutedMessage};
pub use state::{
    AwaitingWelcome, CertExchanging, Handshaking, InitialAssigning, Operational, PeerDiscovery,
    SecondaryConnection, SecondaryConnectionState, ShuttingDown,
};
// Re-export the role-table types so downstream crates don't have to
// reach into the protocol crate to type the cache shape.
pub use dynrunner_protocol_primary_secondary::{RoleChangeHookRegistrar, RoleTable};
// Re-export observer-only surface for PyO3 and consumer wire-up. The
// announcer task body + the lifecycle attach helper live behind one
// import path so consumers don't have to descend into the submodule.
pub use observer::{
    AnnounceTrigger, AnnouncerHandle, AnnouncerSender, ErrorAggregationPolicy,
    InvalidTaskMonitorPolicy, PeerResourceHoldingsUpdatedPayload, Reporter, SharedSnapshotSource,
    StatsSnapshot, TokioClock, attach_observer_announcer, run_observer_announcer, run_reporter,
};
