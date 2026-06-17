//! Task state-change event module (the #520 observer-narration primitive).
//!
//! Single concern: the *shape* of the per-transition event the CRDT merge
//! join ([`crate::cluster_state::ClusterState::merge_task_state`]) enqueues
//! onto the state-change channel. Unlike [`crate::task_completed`] —
//! which carries a listener trait + a fan-out dispatcher because it has
//! several heterogeneous Policy consumers — this channel has exactly ONE
//! consumer (the observer's operator narrator), which drains it directly in
//! its run loop (the `respawn_exec`-outbox shape). So this module owns only
//! the event value type; the emit side lives on `ClusterState`
//! (`emit_task_state_change_event` / `install_task_state_change_sender`,
//! the same install/emit pair every other apply-path channel uses) and the
//! consume side lives in the observer.
//!
//! The CCD-9 invariant holds at the boundary exactly as for
//! [`crate::task_completed`]: the apply/merge path NEVER invokes the
//! consumer directly — it only `tx.send()`s onto the channel; consumption
//! happens strictly off-apply on the observer's loop.

pub mod event;

pub use event::{TaskStateChange, TaskStateChangeEvent, TaskTxnId};
