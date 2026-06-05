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
//! - [`honest_liveness`] — `run_election_tick`'s honest-by-source
//!   `need_election`: a transient blip (route up, staleness < backstop)
//!   does NOT elect, a dead link arms fast via leg (A), a wedged-but-
//!   routable primary elects via the patient backstop (leg B), and a
//!   resumed primary message cancels an in-flight election.
//! - [`keepalive_recognition`] — primary-vs-peer keepalive routing: a
//!   current-primary keepalive refreshes `primary_last_seen`; any other
//!   peer's keepalive feeds `peer_keepalives`.
//! - [`keepalive_emission`] — `send_keepalive` fans ONE keepalive out
//!   exactly once (reaching the meshed primary once, not twice), fires
//!   even when the mesh is degraded, and is suppressed for observers.
//! - [`late_joiner_observer`] — late-joiner observer-mode scenario.
//! - [`late_joiner_accept_emits_peer_joined`] — receive-side
//!   PeerJoined emission contract for the late-joiner accept path,
//!   carrying the joiner's ACTUAL role.
//! - [`observer_announcer_wireup`] — observer announcer production
//!   wiring contract (the separate resource-provider capability).
//! - [`panik_integration`] — panik self-departure + worker teardown.
//! - [`memprofile_hook`] — sampler lifecycle hooks.
//! - [`setup_discovery_yield`] — the pre-staged-mode `SetupPending`
//!   yield discriminator + the fire-once latch (`ingest_setup_discovery`
//!   broadcasts `PhaseDepsSet + TaskAdded`, seeds the local ledger, and
//!   suppresses re-yield even on an empty discovery).
//! - [`colocated_broadcast_loopback`] — the secondary's `Destination::All`
//!   egress also reaches a co-located primary's inbound (CH2): the
//!   symmetric broadcast-loopback leg. Every self-originated broadcast
//!   (TaskAdded batch + Keepalive) is looped back when `current_primary()
//!   == self`, gated off otherwise; fixes the pre-staged silent zero-run.
//! - [`colocated_loopback_resumes_across_setup_pending`] — a co-located
//!   secondary observes its own primary's `RunComplete` over the loopback
//!   AFTER a `SetupPending` yield/re-entry; the loopback receiver is
//!   resumable per-run state, not a fire-once latch.
//! - [`panik_resumes_across_setup_pending`] — a regular pre-staged
//!   secondary acts on a panik signal delivered AFTER a `SetupPending`
//!   yield/re-entry; the panik receiver is resumable per-run state, not a
//!   fire-once latch.

#![cfg(test)]

mod anti_entropy_heal;
mod colocated_broadcast_loopback;
mod colocated_loopback_resumes_across_setup_pending;
mod honest_liveness;
mod keepalive_emission;
mod keepalive_recognition;
mod late_joiner_accept_emits_peer_joined;
mod late_joiner_observer;
mod memprofile_hook;
mod observer_announcer_wireup;
mod panik_integration;
mod panik_resumes_across_setup_pending;
mod peer_mesh_watchdog;
mod processing;
mod r1;
mod setup_discovery_yield;
