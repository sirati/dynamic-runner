//! Wire/value types and traits for the respawn pipeline.
//!
//! Pure data + the [`SecondarySpawner`] trait. No methods on
//! `PrimaryCoordinator` here; budget evaluation lives in [`super::budget`]
//! and the operational-loop wiring lives in [`super::handler`].

use dynrunner_protocol_primary_secondary::RemovalCause;

/// Specification handed to the spawner when the primary requests a
/// replacement secondary. Carries the primary's pubkey so the spawned
/// secondary can authenticate inbound connections.
#[derive(Clone, Debug)]
pub struct SecondarySpawnSpec {
    pub new_secondary_id: String,
    pub primary_endpoint: String,
    pub primary_pubkey_pem: String,
    /// The DEAD member's id (`secondary-N`) the replacement is standing
    /// in for, when this is a replacement spawn. A provider that places
    /// jobs on named nodes (the SLURM spawner) resolves it to the dead
    /// member's SLURM node — from SLURM's own vocabulary (job id →
    /// squeue/sacct), NOT a mesh-advertised hostname — and excludes that
    /// node so the replacement never re-inherits a NODE_FAIL/hardware-
    /// faulty node. `None` when there is no dead member to key on; the
    /// provider then places without constraint. Best-effort: correctness
    /// never depends on it (an unresolvable id just omits `--exclude`).
    pub dead_member_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("spawn provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("spawn timed out")]
    Timeout,
    #[error("spawn revoked: replacement no longer needed")]
    Revoked,
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

    /// Best-effort revocation of a replacement previously requested via
    /// [`Self::spawn`] for `new_secondary_id`, issued when the
    /// replacement became redundant BEFORE it joined the membership
    /// (the member it replaces was re-admitted — provably alive, so
    /// the still-pending replacement is a resource squatter).
    ///
    /// Contract: idempotent and race-tolerant. The provider may be
    /// called before, during, or after its own submission completes,
    /// and the underlying allocation may already be gone — all of
    /// those are quiet successes. `Err` is reserved for the provider
    /// being UNABLE TO REACH its backend (e.g. gateway/ssh transport
    /// failure), so the caller can log loudly; the spawned resource is
    /// still reclaimed by the provider's run-teardown sweep (the SLURM
    /// provider's `cleanup()` scancels every id on `job_ids`).
    ///
    /// Default: no-op. Providers whose replacements are reclaimed by
    /// run teardown alone (e.g. the multi-process provider's
    /// `tracked_children` Drop sweep) need no eager revocation.
    async fn revoke(&self, new_secondary_id: &str) -> Result<(), SpawnError> {
        tracing::debug!(
            new_secondary_id,
            "spawner provider has no eager revocation; the replacement \
             is reclaimed at run teardown only",
        );
        Ok(())
    }
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

/// The replicated caps (`ClusterMutation::RespawnPolicySet`, read off
/// the CRDT at hydrate) ARE a budget — this is the one conversion site,
/// so the promoted primary's re-arm and any future reader agree on the
/// `cooldown_ms` → `Duration` translation.
impl From<crate::cluster_state::ReplicatedRespawnPolicy> for RespawnBudget {
    fn from(p: crate::cluster_state::ReplicatedRespawnPolicy) -> Self {
        Self {
            max_per_secondary: p.max_per_secondary,
            max_total: p.max_total,
            cooldown: std::time::Duration::from_millis(p.cooldown_ms),
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

/// Typed translation of a `Removed`-shaped lifecycle observation,
/// built by the operational-loop router
/// (`dispatch_respawn_lifecycle`) from the forwarded
/// [`PeerLifecycleEvent`](crate::peer_lifecycle::PeerLifecycleEvent)
/// and consumed by `dispatch_respawn_request`.
///
/// Single concern: name the inputs of one respawn decision (who died,
/// why) without leaking any coordinator-side state into the
/// peer-lifecycle listener. The listener cannot hold `&mut
/// PrimaryCoordinator` (it runs on the peer-lifecycle dispatcher
/// task, which has no access to the coordinator's `respawn_tasks`
/// JoinSet or the `next_secondary_id` allocator); the operational
/// loop owns the budget check, the id mint, the spawner invocation,
/// and the JoinSet push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RespawnRequest {
    pub original_id: String,
    pub cause: RemovalCause,
}

// Note: the dispatcher → operational-loop channel carries the FULL
// `PeerLifecycleEvent` stream (the respawn pipeline consumes `Removed`
// as a spawn request and `Added` as the replacement-reconciliation
// signal — a re-admitted original revokes its still-pending
// replacement; a joining replacement clears its bookkeeping) and is
// UNBOUNDED. The historical bounded capacity (`RESPAWN_REQUEST_CHANNEL_CAPACITY
// = 256`) drop-on-full path lost deaths during mass-death-grace
// finalize bursts and broke the budget accounting (a dropped request
// is invisible to `respawn_events`, so the family-budget counter
// never increments and the operator-visible `respawn_budget_exhausted`
// line never fires for the lost peer). The apply-path lifecycle
// channel uses `tokio::sync::mpsc::unbounded_channel` for the same
// reason: the producer is the synchronous lifecycle dispatcher
// `on_event` arm, which must NEVER block; the consumer is the
// operational-loop `select!` arm, which drains at the rate of one
// `dispatch_respawn_lifecycle` per iteration. Memory is bounded by the
// total-budget cap (`RespawnBudget::max_total`, default 10) — once
// the operational loop has reconciled `max_total` requests every
// subsequent enqueue gets a `RejectTotalBudget` decision and the
// queue empties (and `Added` events are reconciled inline, never
// queued beyond the drain rate).

/// Decision returned by [`RespawnBudget::should_respawn`].
///
/// `Accept` is the success arm; the three `Reject*` variants carry
/// the reason so the operational-loop arm can emit a distinct
/// structured-log event per case (the operator forensics surface).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RespawnDecision {
    Accept,
    RejectFamilyBudget,
    RejectTotalBudget,
    RejectCooldown,
}
