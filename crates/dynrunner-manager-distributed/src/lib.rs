pub mod affine_action;
pub mod anti_entropy;
pub mod cluster_state;
pub(crate) mod collection_stats;
pub mod pull_coordinator;
pub mod settled_spill;
pub mod snapshot_stream;
pub mod discovery;
pub mod fulfillability_matcher;
pub mod graceful_abort_trigger;
pub mod liveness;
pub mod message_router;
pub mod observer;
pub mod oploop_instrumentation;
pub(crate) mod own_tick_health;
pub mod panik_watcher;
pub mod peer_credentials;
pub mod peer_lifecycle;
pub mod primary;
pub mod process;
pub(crate) mod run_narrator;
pub mod runtime_watchdog;
pub(crate) mod warn_throttle;
pub mod secondary;
pub mod setup_exec;
pub mod state;
pub mod upload_action;
pub mod task_completed;
pub mod worker_messages;
pub mod worker_signal;
pub mod zip_extract;

#[cfg(test)]
mod test_capture;

pub use primary::staging::{StagingEntry, StagingError, compute_initial_staging_entries};
pub use primary::wire::compute_task_hash;
pub use primary::{
    PhaseHookRaiseLatch, PrimaryConfig, PrimaryCoordinator, PrimaryRunOutcome, RunError,
    StagingAugmentation, StagingStrategy, augment_batch_for_staging, derive_connect_timeout,
};
pub use discovery::{SetupDiscovery, SetupDiscoveryFn};
// The operator's SIGUSR2 graceful-abort trigger — armed once at process
// entry and consumed by whichever role loop (primary or observer) is active
// (the PyO3 entry paths arm it; see the module header).
pub use graceful_abort_trigger::GracefulAbortTrigger;
pub use secondary::{
    DEFAULT_PRIMARY_SILENCE_BACKSTOP, FinalizeRunConfigFn, PeerCertInfo, RunOutcome,
    SecondaryConfig, SecondaryCoordinator, SecondaryTerminal, StagingDispatchContext,
};
pub use zip_extract::compute_file_hash;
// Re-export transport traits from the comm API crate for convenience.
pub use cluster_state::{
    ApplyOutcome, ClusterState, OutcomeSummary, PhaseRollup, RoleChangeHook, SettledStore,
    StateCounts, TaskState,
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
    InvalidTaskMonitorPolicy, PeerResourceHoldingsUpdatedPayload, ReconnectorHandle, Reporter,
    SharedSnapshotSource, StatsSnapshot, TokioClock, TunnelReconnector, attach_observer_announcer,
    run_observer_announcer, run_reporter,
};
// Re-export the upload-action port (#336 P1) for the PyO3 binding + consumer
// wire-up — same one-import-path convenience as `TunnelReconnector`.
pub use upload_action::{UploadAction, UploadActionHandle, UploadError};
// Re-export the import-action port (#497 P4) for the PyO3 binding + consumer
// wire-up — the per-secondary SecondaryAffine import seam.
pub use affine_action::{ImportAction, ImportActionHandle, ImportError};
