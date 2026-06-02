//! Tests for the secondary coordinator.
//!
//! Post-unification the secondary is a PURE NON-AUTHORITY node: it
//! manages its own workers, mirrors the replicated CRDT, and reports
//! its own workers' outcomes to whoever holds the primary role. It
//! never dispatches authoritatively, never originates a terminal CRDT
//! mutation, and never drives a phase machine. The tests here exercise
//! exactly that surface; every authority-mirror test (command_channel,
//! phase_lifecycle_callback, keyed_outputs_apply_race,
//! retry_bucket_cascade, cascade, singleton_type_shift,
//! result_data_plumbing, promoted_primary_quiesce_gate) was retired
//! with the authority mirror it tested.
//!
//! Setup-discovery is NOT an authority-mirror concern and is restored
//! in its correct shape: the secondary is the PRODUCER of the discovery
//! result (it mounts the corpus), broadcasting `PhaseDepsSet + TaskAdded`
//! onto the mesh for the co-located authoritative primary to pick up. It
//! holds no dispatch authority over the discovered tasks — see
//! [`setup_discovery_yield`].
//!
//! Split per-concern:
//!
//! - [`processing`] — basic worker-processing integration tests
//!   (initial-assignment + dispatch loop + StageFile pre-flight).
//! - [`peer_mesh_watchdog`] — peer-mesh-formation watchdog
//!   (degraded-mode + healthy-mesh non-regression).
//! - [`r1`] — R1 promotion-threshold tests + cold-start no-primary
//!   tests + post-promotion peer-message dispatch test.
//! - [`cluster_state_refresh`] — the registered
//!   `on_cluster_state_refresh` callback fires on the `process_tasks`
//!   periodic tick with the live, post-apply `cluster_state`.
//! - [`keepalive_recognition`] — primary-vs-peer keepalive routing: a
//!   current-primary keepalive refreshes `primary_last_seen`; any other
//!   peer's keepalive feeds `peer_keepalives`.
//! - [`late_joiner_observer`] — late-joiner observer-mode scenario.
//! - [`late_joiner_accept_emits_peer_joined`] — receive-side
//!   PeerJoined emission contract for the late-joiner accept path,
//!   carrying the joiner's ACTUAL role.
//! - [`observer_announcer_wireup`] — observer announcer production
//!   wiring contract (the separate resource-provider capability).
//! - [`panik_integration`] — panik self-departure + worker teardown.
//! - [`memprofile_hook`] — sampler lifecycle hooks.
//! - [`pure_observer`] — the strict pure-observer role: originates
//!   NOTHING, holds the FULL CRDT, exits ONLY on `run_complete()`;
//!   late-joining observer AND worker each get the full snapshot with
//!   the CORRECT role; N concurrent observers.
//! - [`setup_discovery_yield`] — the pre-staged-mode `SetupPending`
//!   yield discriminator + the fire-once latch (`ingest_setup_discovery`
//!   broadcasts `PhaseDepsSet + TaskAdded`, seeds the local ledger, and
//!   suppresses re-yield even on an empty discovery).

#![cfg(test)]

mod cluster_state_refresh;
mod keepalive_recognition;
mod late_joiner_accept_emits_peer_joined;
mod late_joiner_observer;
mod memprofile_hook;
mod observer_announcer_wireup;
mod panik_integration;
mod peer_mesh_watchdog;
mod processing;
mod pure_observer;
mod r1;
mod setup_discovery_yield;
