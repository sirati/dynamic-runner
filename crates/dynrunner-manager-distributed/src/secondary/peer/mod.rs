//! Peer-mesh concerns of the `SecondaryCoordinator`.
//!
//! # Sub-module layout
//!
//! - [`message_handler`] — inbound peer-message dispatch
//!   (`handle_peer_message`).
//! - [`mesh_watchdog`] — one-shot mesh-formation watchdog plus the
//!   idempotent `MeshReady`-to-primary reporter
//!   (`check_peer_mesh_watchdog`, `report_mesh_ready_if_needed`).
//! - [`keepalive_timeouts`] — periodic keepalive-timeout sweep that
//!   also recovers `primary_in_flight` tasks targeting the timed-out
//!   peer (`check_peer_timeouts`).
//!
//! Each submodule defines its methods as `impl<...> SecondaryCoordinator`
//! blocks; the parent `secondary` module just declares this `peer`
//! module and gets every method automatically through the inherent-impl
//! merge.

mod keepalive_timeouts;
mod mesh_watchdog;
mod message_handler;
