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
//! - [`promoted_primary_quiesce_gate`] — T11 regression: gate
//!   the promoted-primary natural-quiesce branch on a settle
//!   window so a partial CRDT mirror doesn't broadcast a
//!   spurious `RunComplete`.
//! - [`late_joiner_observer`] — late-joiner observer-mode scenario.
//! - [`late_joiner_accept_emits_peer_joined`] — receive-side
//!   PeerJoined emission contract for the late-joiner accept path.
//! - [`observer_announcer_wireup`] — observer announcer production
//!   wiring contract.
//! - [`phase_lifecycle_callback`] — promoted-secondary fires
//!   `on_phase_end` through `note_primary_item_completed` /
//!   `note_primary_item_failed`'s drain cascade (Pins the
//!   single-process / SLURM gap reported by consumer:
//!   `on_phase_end` was silent on the post-promotion path).
//! - [`retry_bucket_cascade`] — promoted-secondary's per-phase
//!   Recoverable + OOM retry-bucket cascade. Mirrors the live-
//!   primary's `primary/tests/retry.rs` shape against the
//!   secondary's `primary_failed` / `primary_retry_passes_used`
//!   state via the shared core in
//!   `primary/retry_bucket.rs::try_phase_retry_bucket_core`.

#![cfg(test)]

mod cascade;
mod command_channel;
mod late_joiner_accept_emits_peer_joined;
mod late_joiner_observer;
mod observer_announcer_wireup;
mod panik_integration;
mod peer_mesh_watchdog;
mod phase_lifecycle_callback;
mod processing;
mod promoted_primary_quiesce_gate;
mod r1;
mod retry_bucket_cascade;
mod setup_promote_discriminator;
