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
//! # The two halves
//!
//! - [`LivenessBeacon`] (secondary): a dedicated OS thread + its own
//!   `std::net::UdpSocket` that sends one tiny [`datagram`] to the current
//!   primary's liveness address every interval. A mostly-sleeping thread
//!   firing a single syscall is promptly scheduled by CFS even on a pegged
//!   core, so it keeps asserting liveness while the main runtime is
//!   starved.
//! - [`LivenessListener`] (primary): a recv task on the primary's HEALTHY
//!   runtime (the primary owns no local worker pool → never build-starved)
//!   that decodes datagrams and forwards the asserting node-id to the
//!   operational loop, which folds it into the per-secondary death-clock
//!   as a UNION with the existing inbound-frame refresh.
//!
//! The beacon's target is a runtime-published [`BeaconTarget`] cell (the
//! current primary's advertised liveness `SocketAddr`), re-read each tick
//! so a failover repoints the beacon with zero election knowledge on the
//! beacon side. The address is the primary's advertised LAN address +
//! `PeerConnectionInfo.liveness_port` — the same LAN path the QUIC mesh
//! uses, never the bootstrap tunnel.

mod beacon;
mod datagram;
mod listener;
mod target;

pub use beacon::LivenessBeacon;
pub use listener::LivenessListener;
pub use target::BeaconTarget;
