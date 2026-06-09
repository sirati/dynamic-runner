//! Transport-INDEPENDENT liveness beacon.
//!
//! # Why this module exists
//!
//! The whole distributed runtime — the role select loops, the mesh pump,
//! the QUIC writer tasks, and quinn's UDP driver — runs on ONE
//! `current_thread` tokio `LocalSet`. When a secondary's co-resident,
//! CPU-bound build subprocess (e.g. a long QEMU cross-arch nix build on a
//! `--cores=1` allocation) pegs the core, the kernel deschedules that
//! single runtime thread for the build's duration. EVERY periodic emit
//! freezes — including the secondary→primary app keepalive the primary
//! reaps on — so the primary false-declares the (alive, busy) node dead:
//! requeue thrash, duplicate compute, permanent slot loss.
//!
//! No tokio-side fix survives this: a timer task, a multi-thread runtime,
//! or renicing the worker all fail (same cgroup CPU quota; the build often
//! runs under an untouchable daemon; a rootless job lacks `CAP_SYS_NICE`).
//! The only liveness path that survives a fully-pegged core is one OFF the
//! tokio runtime entirely.
//!
//! # Bidirectional — every role emits its own liveness, every tracker unions it
//!
//! Liveness on the independent path is SYMMETRIC: every node emits its own
//! liveness, and every node's liveness-tracker consults the beacon as a
//! UNION with its mesh-frame view. The CPU-starvation hazard is the same on
//! BOTH sides — a relocated/promoted primary's NODE keeps its co-located
//! worker-secondary (it runs builds and starves its single-threaded
//! runtime exactly like any compute node), so the primary's OUTBOUND mesh
//! keepalive freezes too, and the secondaries would false-elect a successor
//! against a still-alive primary.
//!
//! - [`LivenessBeacon`] (emitter, both roles): a dedicated OS thread + its
//!   own `std::net::UdpSocket` that sends one tiny [`datagram`] to every
//!   address in its [`BeaconTarget`] set each interval. A SECONDARY's
//!   beacon targets the ONE current primary; a PRIMARY's beacon targets ALL
//!   its live secondaries (the [`BeaconTarget`] set generalizes the single
//!   target — same mechanism, 1 vs N addresses). A mostly-sleeping thread
//!   firing a few syscalls is promptly scheduled by CFS even on a pegged
//!   core, so it keeps asserting liveness while the main runtime is starved.
//! - [`LivenessListener`] (receiver, every primary-capable node): a recv
//!   task on the node's HEALTHY runtime that decodes datagrams and feeds two
//!   subscribers — a PUSH `mpsc` of node-ids for the primary's reaper
//!   (folded into the per-secondary death-clock as a UNION with the inbound
//!   frame), and a [`BeaconLiveness`] POLL view for the secondary's
//!   failover-detector (the primary's beacon freshness, unioned with the
//!   mesh-frame legs so a CPU-starved-but-beaconing primary is NOT declared
//!   dead).
//!
//! The beacon's target is a runtime-published [`BeaconTarget`] cell, re-read
//! each tick so a failover (secondary side) or a roster change (primary
//! side) repoints the beacon with zero election/membership knowledge on the
//! beacon side. Every target address is a peer's advertised LAN address +
//! `PeerConnectionInfo.liveness_port` — the same LAN path the QUIC mesh
//! uses, never the bootstrap tunnel.

mod address_book;
mod beacon;
mod datagram;
mod freshness;
mod listener;
mod target;

pub use address_book::PeerLivenessAddrs;
pub use beacon::LivenessBeacon;
pub use freshness::BeaconLiveness;
pub use listener::LivenessListener;
pub use target::BeaconTarget;
