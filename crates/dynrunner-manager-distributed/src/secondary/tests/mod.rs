//! Tests for the secondary coordinator.
//!
//! Split per-concern:
//!
//! - [`processing`] — basic worker-processing integration tests
//!   (initial-assignment + dispatch loop + StageFile pre-flight).
//! - [`cascade`] — `cascade_drain_done` pool-phase regression test.
//! - [`peer_mesh_watchdog`] — peer-mesh-formation watchdog
//!   (degraded-mode + healthy-mesh non-regression).
//! - [`r1`] — R1 promotion-threshold tests + cold-start no-primary
//!   tests + post-promotion peer-message dispatch test.
//! - [`setup_promote_discriminator`] — Step 10's `required_setup`
//!   discriminator across the three promotion-reason cases.
//! - [`late_joiner_observer`] — late-joiner observer-mode scenario.
//! - [`late_joiner_accept_emits_peer_joined`] — receive-side
//!   PeerJoined emission contract for the late-joiner accept path.
//! - [`observer_announcer_wireup`] — observer announcer production
//!   wiring contract.

#![cfg(test)]

mod cascade;
mod late_joiner_accept_emits_peer_joined;
mod late_joiner_observer;
mod observer_announcer_wireup;
mod peer_mesh_watchdog;
mod processing;
mod r1;
mod setup_promote_discriminator;
