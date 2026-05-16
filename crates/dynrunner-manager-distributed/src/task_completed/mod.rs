//! Task-completion dispatch module.
//!
//! Three concerns, one per submodule (mirrors
//! [`crate::peer_lifecycle`] exactly; the two modules differ only in
//! the mutation family that triggers them):
//!
//! - [`event`] defines the [`TaskCompletedEvent`] value type that
//!   flows from the apply path
//!   (`ClusterState::emit_task_completed_event`) across the
//!   dispatcher channel to consumers.
//! - [`listener`] defines the [`TaskCompletedListener`] trait every
//!   consumer (PyO3 bridge adapter, future Rust-side phase
//!   orchestrators, telemetry taps) implements. The trait is
//!   intentionally synchronous + fast: the dispatcher holds no lock
//!   between fires, and listeners that want to do real work
//!   re-enqueue onto their own runtime.
//! - [`dispatcher`] is the loop that drains the mpsc receiver end the
//!   apply path writes through and fans each event out to every
//!   registered listener. Spawned once per coordinator at `run()`
//!   start and exits when the channel closes (coordinator dropped).
//!
//! The CCD-9 invariant lives at the module boundary: the apply path
//! NEVER invokes a listener directly — it only `tx.send()`s onto the
//! channel. Listener execution happens on the dispatcher task, which
//! runs strictly off-apply, so a slow / panicking / Python-GIL-blocked
//! listener cannot stall `ClusterState::apply`.

pub mod dispatcher;
pub mod event;
pub mod listener;

pub use dispatcher::run_task_completed_dispatcher;
pub use event::TaskCompletedEvent;
pub use listener::TaskCompletedListener;
