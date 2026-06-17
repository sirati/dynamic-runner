//! Primary coordinator module.
//!
//! The orchestration entrypoint is split across several sibling modules:
//!
//! - [`coordinator`] — the `PrimaryCoordinator<T, P, S, E, I>` struct
//!   plus the `new()` constructor, the `run()` cleanup wrapper, the
//!   `run_pipeline()` body, and the small `note_item_*`/
//!   `process_phase_lifecycle` helpers shared by the wire handlers.
//!   Held as a single file because the inherent impl is one cohesive
//!   concern: drive the operational loop while owning every piece of
//!   coordinator state. Each set of wire-handlers (lifecycle.rs,
//!   task.rs, heartbeat.rs, …) lives in its own sibling module
//!   already.
//! - [`config`] — `PrimaryConfig` + `Default` + `wire_local_path`,
//!   plus the `OnPhaseStart` / `OnPhaseEnd` lifecycle-callback type
//!   aliases.
//! - [`error`] — the structured `RunError` enum and `From<String>` /
//!   `From<&str>` blanket impls.
//!
//! The sibling concerns each own their wire arms:
//! [`assignment`], [`command_channel`], [`connect`],
//! [`fulfillability_matcher`], [`heartbeat`], [`lifecycle`],
//! [`peer_setup`], [`preferred_secondaries`], [`respawn`], [`staging`],
//! [`task`], [`wire`].

mod affine_dispatch;
mod affine_scheduler;
mod assignment;
mod bringup_reservation;
mod command_channel;
mod config;
mod connect;
pub mod consensus;
mod coordinator;
mod custom_message;
mod discovery;
mod error;
mod estimate_escalation;
mod fulfillability_matcher;
mod heartbeat;
mod hydrate;
mod important_events;
mod ingest;
mod lifecycle;
mod peer_setup;
mod readmission;
pub mod preferred_secondaries;
mod reconciliation_probe;
pub mod respawn;
pub(crate) mod retry_bucket;
mod secondary_id;
mod setup_dispatch;
mod setup_staging;
mod spawn_queue;
pub mod staging;
pub(crate) mod task;
mod terminal_gate;
pub mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use command_channel::{
    COMMAND_CHANNEL_CAPACITY, PrimaryCommand, SpawnError, validate_spawn_tasks,
};
pub use config::{
    DEFAULT_TASK_RECONCILIATION_TIMEOUT, OnCustomMessage, OnPhaseEnd, OnPhaseStart,
    PhaseHookRaiseLatch, PrimaryConfig, derive_connect_timeout,
};
pub use setup_staging::{StagingAugmentation, StagingStrategy, augment_batch_for_staging};
pub use coordinator::{PrimaryCoordinator, PrimaryRunOutcome};
pub use error::RunError;

// Submodule-visible coordinator-state types. `pub(crate)` so test-only
// modules under `crate::primary::heartbeat::tests` (etc.) can construct
// these directly without going through the wire-message path; the
// `pub(super) struct` declaration on the coordinator side keeps the
// fields scoped to siblings within `primary/`.
pub(crate) use coordinator::{RemoteWorkerState, SlotProvenance, SlotState};

/// Settle window the authority sleeps after broadcasting a run-terminal
/// CRDT mutation (`RunComplete` / `RunAborted`) before the dispatcher
/// tears down its transport. Without it, a fast dispatcher exit races
/// the broadcast and some peers miss the signal — leftover SLURM jobs in
/// CG state for wrappers whose secondaries never saw it. 500ms is far
/// more than the QUIC delivery latency of an in-process / podman-bridge
/// mesh; the cost on a happy-path exit is negligible. Shared by the
/// `RunComplete` path (`coordinator`) and the `RunAborted` path
/// (`ingest`) so the two stay in lockstep.
pub(crate) const PRIMARY_BROADCAST_SETTLE: std::time::Duration =
    std::time::Duration::from_millis(500);

/// Upper bound on how long the terminal-verdict broadcast holds the
/// authority alive RE-BROADCASTING while an OBSERVER leg the roster names
/// is transiently unreachable (#415 face (b1)).
///
/// The fixed [`PRIMARY_BROADCAST_SETTLE`] is sized for the QUIC delivery
/// latency of a HEALTHY mesh; it cannot deliver to a peer whose leg is
/// DOWN at broadcast time. A zero-authority observer never times out on
/// visibility loss (it exits ONLY on observing the CRDT terminal — BUG-B),
/// so a terminal that misses its leg is uniquely unrecoverable once the
/// fleet tears down (the observed run_20260611_155305 blackout: the
/// fleet-wide `-R` drop coincided with run-end; the verdict never reached
/// the observer, and after teardown there was no peer left to
/// anti-entropy-pull from). The compute peers, by contrast, self-heal a
/// missed terminal — they fail over / time out and release their slot —
/// so only the observer leg needs this grace.
///
/// Bounded so a genuinely-gone observer (its host died) cannot stall the
/// fleet teardown forever: past this cap the authority gives up and tears
/// down, exactly as before. 60s comfortably covers the observed
/// secondary→observer `-R` re-fold (the bootstrap-redial backoff caps at
/// 30s and the observed re-establish landed in ~27-31s) without holding a
/// finished run open for an unbounded stretch.
pub(crate) const TERMINAL_OBSERVER_DELIVERY_GRACE: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Upper bound on how long a RELOCATED primary holds its terminal-verdict
/// broadcast waiting for the node that relocated its role away (the
/// `PromotionSignal::relocating_from`, recorded as the coordinator's
/// `pending_observer`) to ANNOUNCE itself as a standalone observer.
///
/// Distinct from [`TERMINAL_OBSERVER_DELIVERY_GRACE`]: that grace covers a
/// KNOWN observer (already in the role-table projection) whose transport leg
/// is transiently DOWN and re-folding — the observed `-R` re-establish takes
/// tens of seconds, so its cap is 60s. THIS grace covers the much faster
/// in-process / same-mesh swap→announce hop: the relocating-away node's mesh
/// leg SURVIVES the primary→observer retag (the slot's stable channel), so it
/// only has to finish `ObserverCoordinator::from_handoff` and fire its
/// bootstrap snapshot request — sub-second on a healthy node. 5s comfortably
/// covers that with margin while staying well under the relocation e2e's 20s
/// convergence bound; on cap expiry the primary PROCEEDS to teardown (a
/// becoming-observer that died mid-swap is the only way the cap is reached,
/// and a dead node has nothing to deliver to).
pub(crate) const PENDING_OBSERVER_ANNOUNCE_GRACE: std::time::Duration =
    std::time::Duration::from_secs(5);
