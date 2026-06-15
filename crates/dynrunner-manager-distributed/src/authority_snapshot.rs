//! Sync consumer-facing snapshot of slurm-authoritative life-state.
//!
//! # Concern
//! ONE: expose a lock-free, synchronously-readable view of the latest
//! authoritative probe answers. Consumers (collective_silence gate, sticky-
//! removal tiebreak, respawn quantity gate) read without entering an async
//! context — the probe lives off-loop on its own `tokio::spawn`'d task;
//! the snapshot is published via `ArcSwap`.
//!
//! # Module boundary
//! This module owns the SYNCHRONOUS read trait + the snapshot publisher.
//! The async PROBE that actually queries slurm lives in
//! `dynrunner_slurm::authority` (the SLURM provider's concern) and
//! implements the `SlurmAuthorityProbe` trait defined here. The split
//! keeps `dynrunner-manager-distributed` free of any SLURM dependency:
//! the framework crate names the contract, the provider crate names the
//! implementation. `dynrunner-slurm` already depends on
//! `dynrunner-manager-distributed`, so the trait flows naturally one way.
//!
//! # Staleness handling — FAIL-CLOSED
//! Snapshot carries `last_updated: Instant`. Past
//! `STALENESS_BOUND_MULTIPLE * probe_interval`, EVERY id's `peer_life`
//! returns Unknown and `secondary_active_or_queued_count` returns None.
//! All consumers are conservatively safe under stale: gate keeps
//! deferring, sticky keeps the dead-mark, quantity gate refuses respawn.
//!
//! # Why the lag is a FEATURE
//! When 10 secondaries appear silent simultaneously and the framework
//! declares all 10 dead, the snapshot still shows their slurm jobs Alive
//! (probe hasn't re-run). Each respawn dispatch consults the quantity gate,
//! sees count==initial, REFUSES TO FIRE. Only after the next probe — when
//! slurm confirms Gone — does respawn fire. This lag is the mechanism by
//! which false-deaths-from-local-deafness produce no respawn cascade.
//! DO NOT remove the lag; it is load-bearing (#543).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;

/// Authoritative life-state of one secondary's slurm job, as the off-loop
/// probe reports it. Fail-safe: `Unknown` is the conservative answer
/// whenever the probe could not read evidence either way. Every consumer
/// must treat `Unknown` as "no positive evidence" (the quantity gate
/// refuses, the sticky-removal tiebreak keeps the dead-mark, the
/// collective-silence gate keeps deferring).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerLifeState {
    /// Slurm reports the job PENDING / RUNNING (alive in the queue).
    Alive,
    /// Slurm reports a terminal state (COMPLETED / FAILED / CANCELLED /
    /// TIMEOUT / OOM / NODE_FAIL / BOOT_FAIL / DEADLINE) — the job has
    /// left the queue. Squeue-empty + sacct-terminal counts as `Gone`.
    Gone,
    /// No mapping (id not known to the manager), gateway transport failed,
    /// or squeue empty + sacct silent. Read consumers fail-closed.
    Unknown,
}

/// Async surface the off-loop updater task calls once per probe interval.
/// Implemented by the SLURM provider's `SlurmJobManagerProbe` (see
/// `dynrunner_slurm::authority`).
#[async_trait]
pub trait SlurmAuthorityProbe: Send + Sync {
    /// The latest authoritative state of `secondary_id`'s slurm job.
    async fn peer_life(&self, secondary_id: &str) -> PeerLifeState;
    /// Probe every secondary the manager has ever submitted a job for.
    /// The returned map is what the snapshot publisher installs.
    async fn probe_all(&self) -> HashMap<String, PeerLifeState>;
}

/// Snapshot is "stale" past `STALENESS_BOUND_MULTIPLE * probe_interval`.
/// At 4× the cadence, three back-to-back missed probes is the threshold
/// (the same shape every other "missed cadence" gate uses).
pub const STALENESS_BOUND_MULTIPLE: u32 = 4;

#[derive(Debug, Clone)]
struct SnapshotInner {
    map: HashMap<String, PeerLifeState>,
    last_updated: Instant,
}

/// The synchronous read surface every safety-net consumer uses. Two
/// distinct queries because the consumers ask distinct questions:
///
/// - `peer_life(id)`: the tiebreaks (collective_silence escalation,
///   sticky-removal reversal) ask about ONE peer.
/// - `secondary_active_or_queued_count()`: the quantity gate asks "is
///   the fleet below initial count?" The answer is the count of jobs
///   the snapshot currently classifies `Alive`; `None` means we have
///   no positive evidence (snapshot stale or unable to read).
///
/// Both fail-closed under stale snapshots: `Unknown` / `None`.
pub trait SlurmAuthoritativeSnapshot: Send + Sync {
    fn peer_life(&self, secondary_id: &str) -> PeerLifeState;
    fn secondary_active_or_queued_count(&self) -> Option<usize>;
}

/// `ArcSwap`-backed snapshot publisher. Constructed by the deployment
/// layer; an updater task (`spawn_authority_updater`) republishes the
/// map at `probe_interval`. Readers see the latest published map without
/// any lock (one atomic load), and a fresh publish never blocks readers.
pub struct OffLoopAuthoritySnapshot {
    inner: Arc<ArcSwap<SnapshotInner>>,
    staleness_bound: Duration,
}

impl OffLoopAuthoritySnapshot {
    /// Construct a snapshot whose staleness bound is `STALENESS_BOUND_MULTIPLE
    /// * probe_interval`. The initial published value is the empty map at
    /// `Instant::now()` so the first reader before any probe runs sees a
    /// fresh-but-empty snapshot (each id reads `Unknown`, count reads
    /// `Some(0)` — consistent with "no slurm jobs we've recorded yet").
    pub fn new(probe_interval: Duration) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(SnapshotInner {
                map: HashMap::new(),
                last_updated: Instant::now(),
            })),
            staleness_bound: probe_interval.saturating_mul(STALENESS_BOUND_MULTIPLE),
        }
    }

    /// A publisher handle for the off-loop updater. Cheap clones (one
    /// `Arc::clone`); the snapshot keeps its OWN `Arc` so the publisher
    /// can republish while the snapshot is shared with consumers.
    pub fn updater_handle(&self) -> AuthorityUpdaterHandle {
        AuthorityUpdaterHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.inner.load().last_updated) > self.staleness_bound
    }
}

impl SlurmAuthoritativeSnapshot for OffLoopAuthoritySnapshot {
    fn peer_life(&self, secondary_id: &str) -> PeerLifeState {
        if self.is_stale(Instant::now()) {
            return PeerLifeState::Unknown;
        }
        self.inner
            .load()
            .map
            .get(secondary_id)
            .copied()
            .unwrap_or(PeerLifeState::Unknown)
    }

    fn secondary_active_or_queued_count(&self) -> Option<usize> {
        if self.is_stale(Instant::now()) {
            return None;
        }
        let snap = self.inner.load();
        Some(snap.map.values().filter(|s| matches!(s, PeerLifeState::Alive)).count())
    }
}

/// Publisher side of the snapshot. The updater task `publish()`es a
/// fresh map every probe round; readers see it on the next load.
#[derive(Clone)]
pub struct AuthorityUpdaterHandle {
    inner: Arc<ArcSwap<SnapshotInner>>,
}

impl AuthorityUpdaterHandle {
    /// Atomically install a fresh probe result. `last_updated` is set to
    /// `Instant::now()` at publish time, so the snapshot's staleness gate
    /// reads from the moment THIS publish landed (not the moment the
    /// probe started its round-trip).
    pub fn publish(&self, map: HashMap<String, PeerLifeState>) {
        self.inner.store(Arc::new(SnapshotInner {
            map,
            last_updated: Instant::now(),
        }));
    }
}

/// Spawn an off-loop task that probes at `probe_interval` and republishes
/// the map. The returned `JoinHandle` belongs to the deployment layer —
/// hold it for the run's lifetime so the probe stops at shutdown.
///
/// `MissedTickBehavior::Skip` so a slow probe round-trip does not stack
/// late ticks (the next tick fires on schedule, not back-to-back to catch
/// up). Stale answers are handled by the snapshot's staleness gate, not
/// by tighter-but-uneven probing.
pub fn spawn_authority_updater(
    probe: Arc<dyn SlurmAuthorityProbe>,
    publisher: AuthorityUpdaterHandle,
    probe_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(probe_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let map = probe.probe_all().await;
            publisher.publish(map);
        }
    })
}

/// Inert snapshot for non-SLURM deployments (multi-process, in-memory
/// channel mesh). Every id reads `Unknown`, count reads `None` — every
/// consumer fail-closes on those values, so the tiebreaks become no-ops
/// and the quantity gate refuses respawn UNLESS a real snapshot is wired.
///
/// The expected wire-up on non-SLURM is to inject this snapshot but
/// pair it with a different gate (the multi-process spawner is
/// reclaimed by Drop sweeps, where the over-allocation hazard never
/// materialises in the first place). On SLURM, `OffLoopAuthoritySnapshot`
/// replaces this inert one at construction.
pub struct NoSlurmAuthoritySnapshot;

impl SlurmAuthoritativeSnapshot for NoSlurmAuthoritySnapshot {
    fn peer_life(&self, _secondary_id: &str) -> PeerLifeState {
        PeerLifeState::Unknown
    }
    fn secondary_active_or_queued_count(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    /// Deterministic snapshot for unit tests. `map` answers `peer_life`;
    /// `count` answers `secondary_active_or_queued_count`. The two are
    /// independently controllable so a test can pin one without
    /// implying the other.
    pub struct StaticSnapshot {
        pub map: HashMap<String, PeerLifeState>,
        pub count: Option<usize>,
    }
    impl SlurmAuthoritativeSnapshot for StaticSnapshot {
        fn peer_life(&self, secondary_id: &str) -> PeerLifeState {
            self.map.get(secondary_id).copied().unwrap_or(PeerLifeState::Unknown)
        }
        fn secondary_active_or_queued_count(&self) -> Option<usize> {
            self.count
        }
    }
}
