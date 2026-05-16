//! Mesh-partition end-to-end scenarios for the relay-routing
//! state machine ([`dynrunner_protocol_primary_secondary::Router`]).
//!
//! ## Driver
//!
//! Sequential cooperative pump. We never use `tokio::join!` or
//! `select!` over multiple transports' `recv_peer`: that attempts
//! overlapping `&mut self` borrows on transport state. Instead we
//! round-robin call `tokio::time::timeout(short, recv_peer)` on
//! each transport. `recv_peer` is the only path that actually
//! forwards `Relay` envelopes (the sync `try_recv_peer` drops them
//! with a warn — see `Router::process_inbound_sync`). If the
//! timeout fires while `recv_peer` is awaiting an empty inbox the
//! future is cancellation-safe (only `await` point is
//! `mpsc::UnboundedReceiver::recv` and `process_inbound` is
//! synchronous). The pump terminates on either (a) the per-test
//! watch closure succeeding or (b) a 5s wall-clock deadline. The
//! wall-clock deadline is independent of any `tokio::time::pause()`
//! virtual clock — a paused-clock test still aborts on a real bug.
//!
//! The pump captures the delivered messages into a per-peer
//! `Vec<DistributedMessage>` and lets the watch closure inspect
//! those records.
//!
//! ## Cooldown-gate scenarios (#7, #8, #9)
//!
//! [`dynrunner_protocol_primary_secondary::Clocks::now`] is a
//! `std::time::Instant`. `tokio::time::pause` does **not** affect
//! `std::time::Instant::now()`, so we can't drive the cooldown gate
//! via a paused tokio clock through the transport — the
//! transport's `now_clocks()` shim reads the real monotonic clock
//! unconditionally. To exercise the cooldown gate deterministically
//! those scenarios bypass the transport and drive the [`Router`]
//! directly with synthesized [`Clocks`] values. The same
//! `Router::send_to_peer` / `Router::process_inbound` code path the
//! transport delegates to is exercised; the only thing we skip is
//! the trivial `now_clocks()` wrapper.
//!
//! ## Layout
//!
//! Test cases live in sibling `mesh_partition/<sub>.rs` files
//! grouped by concern:
//! - `helpers.rs`: shared fixtures (TestId, mesh builder, pump
//!   driver, tracing capture layer).
//! - `relay_basics.rs`: scenarios 1-6 — relay selection, partition
//!   alternates, heal, dead-end propagation, multi-hop, mid-relay
//!   sever.
//! - `cooldown_and_blacklist.rs`: scenarios 7-9 — cooldown gate,
//!   receiver-side relay observation, blacklist persistence.

#[path = "mesh_partition/helpers.rs"]
mod helpers;

#[path = "mesh_partition/cooldown_and_blacklist.rs"]
mod cooldown_and_blacklist;
#[path = "mesh_partition/relay_basics.rs"]
mod relay_basics;
