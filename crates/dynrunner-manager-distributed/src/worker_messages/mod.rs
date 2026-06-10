//! Worker custom-message dispatch module.
//!
//! Three concerns, one per submodule (mirrors
//! [`crate::task_completed`] exactly; the two modules differ in the
//! trigger — a node-local worker wire frame here vs a CRDT apply
//! there):
//!
//! - [`event`] defines the [`WorkerCustomMessage`] value type that
//!   flows from the secondary's worker-event bridge
//!   (`secondary/processing/worker_event.rs`, the
//!   `WorkerEvent::CustomMessage` arm) across the dispatcher channel
//!   to consumers.
//! - [`listener`] defines the [`WorkerMessageListener`] trait every
//!   consumer (PyO3 bridge adapter — the consumer's duck-typed
//!   `worker_message_listener` TaskDefinition attribute — telemetry
//!   taps) implements. The trait is intentionally synchronous + fast:
//!   the dispatcher holds no lock between fires, and listeners that
//!   want to do real work re-enqueue onto their own runtime.
//! - [`dispatcher`] is the loop that drains the mpsc receiver end the
//!   bridge writes through and fans each event out to every
//!   registered listener. Spawned once per coordinator at run start
//!   and exits when the channel closes (coordinator dropped).
//!
//! The CCD-9-shaped invariant lives at the module boundary: the
//! worker-event bridge NEVER invokes a listener directly — it only
//! `tx.send()`s onto the channel. Listener execution happens on the
//! dispatcher task, which runs strictly off the operational loop, so
//! a slow / panicking / Python-GIL-blocked listener cannot stall the
//! secondary's `process_tasks` select.
//!
//! The REPLY direction (consumer → worker) is the separate
//! [`crate::secondary::SecondaryControlCommand`] ingress: the
//! listener's `SecondaryHandle.send_to_worker` queues a control
//! command the operational loop drains — the listener never touches
//! the pool.

pub mod dispatcher;
pub mod event;
pub mod listener;

pub use dispatcher::run_worker_message_dispatcher;
pub use event::WorkerCustomMessage;
pub use listener::WorkerMessageListener;
