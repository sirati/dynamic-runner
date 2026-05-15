//! Scaffolding for secondary respawn.
//!
//! Single concern: own the types and trait that describe how a
//! replacement secondary is requested, spawned, budgeted, and reported
//! back to the operational loop. Per-provider implementations
//! (multi-process, SLURM) live in sibling files and depend only on
//! this module's API surface; the operational loop owns the
//! `JoinSet<RespawnOutcome>` field declared on `PrimaryCoordinator`
//! and drains it in its `select!`. No call site outside this module
//! needs to know the internals of any specific spawner.
//!
//! The crossing-the-boundary surface is the [`SecondarySpawner`]
//! trait plus the value types [`SecondarySpawnSpec`], [`SpawnError`],
//! [`RespawnBudget`], [`RespawnOutcome`], and [`RespawnEvent`].
//! Everything else (per-task tracking, primary-internal helpers,
//! CLI flag plumbing) lands in sibling subtasks.

use crate::peer_lifecycle::PeerLifecycleEvent;
use dynrunner_protocol_primary_secondary::RemovalCause;

// `PeerLifecycleEvent` is re-imported to anchor the dispatcher's
// upstream — the respawn pipeline ultimately reacts to `PeerRemoved`
// lifecycle events emitted by the existing peer-lifecycle module.
// Touching the import at module load keeps the symbol live for
// downstream subtasks that will wire the closure-shaped dispatcher
// without inflating the public API of this scaffolding step.
#[allow(dead_code)]
const _PEER_LIFECYCLE_EVENT_TYPE_ANCHOR: Option<PeerLifecycleEvent> = None;

/// Specification handed to the spawner when the primary requests a
/// replacement secondary. Carries the primary's pubkey so the spawned
/// secondary can authenticate inbound connections.
#[derive(Clone, Debug)]
pub struct SecondarySpawnSpec {
    pub new_secondary_id: String,
    pub primary_endpoint: String,
    pub primary_pubkey_pem: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("spawn provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("spawn timed out")]
    Timeout,
    #[error("spawn failed: {0}")]
    Other(String),
}

/// Async trait for the per-provider spawner. Multi-process and SLURM
/// implementations live in sibling files.
///
/// `#[async_trait(?Send)]` because the SLURM impl drives
/// `ssh -N -R` subprocess spawn through a closure whose future is not
/// `Send` (the closure returns `Pin<Box<dyn Future + 'static>>` — see
/// `dynrunner_slurm::preparation::production_spawner`). The operational
/// loop on `PrimaryCoordinator` already runs inside a
/// `tokio::task::LocalSet` for the same reason (the SLURM preparation
/// pipeline uses `spawn_local` for per-tunnel watchers), so dropping
/// the `Send` bound on the returned future does not constrain the
/// integration site — it just lifts a constraint the provider physics
/// can't satisfy. The trait object itself stays `Send + Sync` so
/// `Arc<dyn SecondarySpawner>` is moveable across `select!` arms.
#[async_trait::async_trait(?Send)]
pub trait SecondarySpawner: Send + Sync {
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError>;
}

#[derive(Clone, Debug)]
pub struct RespawnBudget {
    pub max_per_secondary: u32,
    pub max_total: u32,
    pub cooldown: std::time::Duration,
}

impl Default for RespawnBudget {
    fn default() -> Self {
        Self {
            max_per_secondary: 3,
            max_total: 10,
            cooldown: std::time::Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RespawnOutcome {
    pub original_id: String,
    pub new_id: String,
    pub cause: RemovalCause,
    pub result: Result<(), String>,
}

/// Track family of respawned secondaries — `original_id` lets the
/// budget look at the chain to apply per-secondary caps.
#[derive(Clone, Debug)]
pub struct RespawnEvent {
    pub original_id: String,
    pub new_id: String,
    pub cause: RemovalCause,
    pub at: std::time::SystemTime,
}

/// Maximum number of [`RespawnEvent`]s retained on the coordinator's
/// `respawn_events` ring. Sized for operator forensics across the
/// lifetime of a single run; the ring drops oldest on overflow.
pub const RESPAWN_EVENTS_CAP: usize = 1024;

/// Push `ev` onto a `respawn_events` ring, evicting the oldest entry
/// when the ring is already at [`RESPAWN_EVENTS_CAP`]. The single
/// concern of this helper is bounded FIFO semantics; the operational
/// loop (the only legitimate caller) does not need to know the cap.
pub(crate) fn push_event(ring: &mut std::collections::VecDeque<RespawnEvent>, ev: RespawnEvent) {
    if ring.len() == RESPAWN_EVENTS_CAP {
        ring.pop_front();
    }
    ring.push_back(ev);
}

#[cfg(test)]
mod tests {
    //! Contract-level constructor smoke tests. Full integration
    //! (spawner ↔ dispatcher ↔ JoinSet drain) lands in sibling F6.

    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn spawn_spec_constructs() {
        let spec = SecondarySpawnSpec {
            new_secondary_id: "sec-replacement-1".to_owned(),
            primary_endpoint: "127.0.0.1:5555".to_owned(),
            primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\n...\n".to_owned(),
        };
        assert_eq!(spec.new_secondary_id, "sec-replacement-1");
        assert_eq!(spec.primary_endpoint, "127.0.0.1:5555");
        assert!(spec.primary_pubkey_pem.starts_with("-----BEGIN"));
    }

    #[test]
    fn spawn_error_renders_human_strings() {
        let provider_unavail = SpawnError::ProviderUnavailable("slurm not configured".to_owned());
        assert_eq!(
            format!("{provider_unavail}"),
            "spawn provider unavailable: slurm not configured",
        );
        let timeout = SpawnError::Timeout;
        assert_eq!(format!("{timeout}"), "spawn timed out");
        let other = SpawnError::Other("exec failed".to_owned());
        assert_eq!(format!("{other}"), "spawn failed: exec failed");
    }

    #[test]
    fn respawn_budget_default_matches_spec() {
        let b = RespawnBudget::default();
        assert_eq!(b.max_per_secondary, 3);
        assert_eq!(b.max_total, 10);
        assert_eq!(b.cooldown, Duration::from_secs(30));
    }

    #[test]
    fn respawn_outcome_constructs_with_ok_and_err() {
        let ok = RespawnOutcome {
            original_id: "sec-a".to_owned(),
            new_id: "sec-a-replacement".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
            result: Ok(()),
        };
        assert!(ok.result.is_ok());

        let err = RespawnOutcome {
            original_id: "sec-b".to_owned(),
            new_id: "sec-b-replacement".to_owned(),
            cause: RemovalCause::MassDeathEscalation,
            result: Err("spawn failed".to_owned()),
        };
        assert!(matches!(err.result, Err(ref s) if s == "spawn failed"));
    }

    #[test]
    fn respawn_event_constructs() {
        let ev = RespawnEvent {
            original_id: "sec-a".to_owned(),
            new_id: "sec-a-replacement".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
            at: SystemTime::now(),
        };
        assert_eq!(ev.original_id, "sec-a");
        assert_eq!(ev.new_id, "sec-a-replacement");
        assert!(matches!(ev.cause, RemovalCause::KeepaliveMiss));
    }

    #[test]
    fn respawn_event_ringbuffer_drops_oldest_at_1024_cap() {
        use std::collections::VecDeque;

        let mut ring: VecDeque<RespawnEvent> = VecDeque::new();
        // Push exactly one more than the cap; the very first event
        // (`new_id = "new-0"`) must be evicted, and the buffer must
        // remain at the cap with the freshest event at the back.
        for i in 0..=RESPAWN_EVENTS_CAP {
            push_event(
                &mut ring,
                RespawnEvent {
                    original_id: format!("orig-{i}"),
                    new_id: format!("new-{i}"),
                    cause: RemovalCause::KeepaliveMiss,
                    at: SystemTime::now(),
                },
            );
        }
        assert_eq!(ring.len(), RESPAWN_EVENTS_CAP);
        assert_eq!(ring.front().unwrap().new_id, "new-1");
        assert_eq!(
            ring.back().unwrap().new_id,
            format!("new-{}", RESPAWN_EVENTS_CAP),
        );
    }
}
