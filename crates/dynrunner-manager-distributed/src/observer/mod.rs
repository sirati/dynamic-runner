//! Observer-mode coordinator components.
//!
//! Single concern of this module: house the components that exist for
//! observer-mode operation — a node that holds the replicated CRDT and
//! carries zero authority. These are role-agnostic about WHICH process
//! is observing: both the secondary late-joiner (a peer-mesh-only
//! participant that joined via the snapshot RPC) AND the
//! relocated-submitter primary tail (the bootstrap-primary that handed
//! its role to a compute secondary and runs the relocation observer tail)
//! consume them. They have no place inside the generic `secondary/` tree.
//!
//!   * [`announcer`]        — the resource-holdings broadcaster.
//!   * [`reporting`]        — the CRDT-derived periodic-stats + idle
//!     reporter (the operator's "wake-an-LLM" feed).
//!   * [`failure_response`] — the terminal-failure policies (error
//!     aggregation + the invalid_task fatal-exit monitor).
//!   * [`lost_visibility`]  — the report-lost-and-keep-observing state
//!     machine (visibility loss is reported + retried, NEVER a run verdict).
//!
//! See each submodule's header for its concern.

pub mod announcer;
pub mod coordinator;
pub mod failure_response;
pub mod lifecycle;
pub mod lost_visibility;
pub mod reporting;

pub use announcer::{
    AnnounceTrigger, AnnouncerOutboxItem, AnnouncerSender, PeerMeshAnnouncerSender,
    PeerResourceHoldingsUpdatedPayload, run_observer_announcer,
};
pub use coordinator::{
    ObserverConfig, ObserverCoordinator, ObserverHandoff, ObserverTerminal,
    build_cold_join_observer,
};
pub use failure_response::{ErrorAggregationPolicy, InvalidTaskMonitorPolicy};
pub use lifecycle::{AnnouncerHandle, attach_observer_announcer};
pub use lost_visibility::{LostVisibilityReporter, Visibility};
pub use reporting::{Reporter, SharedSnapshotSource, StatsSnapshot, TokioClock, run_reporter};
