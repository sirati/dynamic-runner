//! Observer-specific subsystems.
//!
//! Single concern of this module: house the components that exist
//! ONLY for observer-mode coordinators (peer-mesh-only participants
//! that joined via the late-joiner snapshot RPC) and therefore have
//! no place inside the generic `secondary/` tree. The first such
//! component is [`announcer`], the resource-holdings broadcaster.
//!
//! See each submodule's header for its concern.

pub mod announcer;
pub mod lifecycle;

pub use announcer::{
    run_observer_announcer, AnnounceTrigger, AnnouncerOutboxItem, AnnouncerSender,
    PeerMeshAnnouncerSender, PeerResourceHoldingsUpdatedPayload,
};
pub use lifecycle::{attach_observer_announcer, AnnouncerHandle};
