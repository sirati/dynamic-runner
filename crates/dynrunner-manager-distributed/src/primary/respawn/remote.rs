//! Remote execution backend for the respawn pipeline.
//!
//! # Single concern
//!
//! Carry one respawn EXECUTION (spawn / revoke) across the mesh to the
//! process that physically hosts the provider, and bring its outcome
//! back. The respawn DECISION (budget admission, id mint, replicated
//! ledger spend, pending-replacement reconciliation) stays entirely in
//! `dispatch_respawn_request` / `reconcile_replacements_on_join` and is
//! untouched: this module is the SECOND backend behind the SAME
//! [`SecondarySpawner`] trait the local providers implement, so the
//! handler never knows local from remote.
//!
//! # Why a remote backend exists
//!
//! The physical provider (the SLURM GIL job-manager + gateway + tunnel
//! pool, or the multi-process child registry) lives in the SUBMITTER
//! process only — mesh node-id [`dynrunner_core::SETUP_NODE_ID`] — and
//! cannot move. Under mesh-always the primary ALWAYS relocates off the
//! submitter, so every relocated/promoted primary holds the decision
//! with no local provider. The submitter keeps the provider across its
//! own primary→observer demotion (the provider belongs to the PROCESS,
//! not the role — it rides the `ObserverHandoff`), so the remote leg's
//! destination is `Destination::Observer("setup")`: host-id-stable,
//! role-demuxed at the receiving pump.
//!
//! # Delivery semantics
//!
//! - Requests are RETRIED on a bounded-backoff cadence while the
//!   provider host is unreachable — loudly (one WARN per re-send),
//!   never silently dropped, never fatal. The retry future lives on the
//!   coordinator's `respawn_tasks` JoinSet exactly like a local spawn,
//!   so run teardown drains it.
//! - The primary-minted `new_secondary_id` is the correlation AND
//!   idempotency key: a re-send carries the SAME id, and the
//!   observer-side execution arm dedupes on it (in-flight → ignore;
//!   done → replay the cached outcome), so one id can never
//!   double-submit (the #399 fresh-id discipline, extended across the
//!   wire).
//! - Across primary FAILOVER no in-flight request is replayed: the
//!   accepted event is on the replicated ledger (recorded at dispatch,
//!   before the spawn future runs), so the new primary's budget already
//!   accounts for it; the submission itself either landed (the
//!   replacement joins as ordinary capacity and the new primary's
//!   `PeerJoined` reconciliation adopts it) or died with the request
//!   (the family's next death re-enters the decision under the
//!   inherited budget). This mirrors the LOCAL provider's failover
//!   semantics exactly — no new replicated state is invented.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use tokio::sync::oneshot;

use crate::process::MeshClient;

use super::types::{SecondarySpawnSpec, SecondarySpawner, SpawnError};

/// First re-send delay when no result has come back; doubles per
/// attempt up to [`SPAWN_RESEND_CAP`]. The first window also covers the
/// provider's normal execution time (sbatch + tunnel can take this
/// long), so a healthy round-trip usually completes inside attempt 1.
const SPAWN_RESEND_INITIAL: Duration = Duration::from_secs(10);
/// Re-send backoff cap. Spawn requests retry INDEFINITELY at this
/// cadence (never dropped, never fatal — the budget already spent, the
/// id already minted; only delivery is pending). Run teardown aborts
/// the retry future via the `respawn_tasks` drain.
const SPAWN_RESEND_CAP: Duration = Duration::from_secs(60);
/// Revoke re-send cadence + bounded attempt count. Revocation is
/// best-effort by the trait contract: after the attempts exhaust the
/// caller gets the same `Err` a local provider returns on an
/// unreachable backend (logged loudly; the provider-side run-teardown
/// sweep remains the reclamation backstop).
const REVOKE_RESEND_INTERVAL: Duration = Duration::from_secs(10);
const REVOKE_MAX_ATTEMPTS: u32 = 6;

/// Which pending table a request correlates through. Spawn and revoke
/// results for the SAME `new_secondary_id` must never cross-complete
/// (a revoke can race its spawn), so the two are distinct keyspaces.
#[derive(Clone, Copy, Debug)]
enum ExecKind {
    Spawn,
    Revoke,
}

#[derive(Default)]
struct PendingMaps {
    spawn: HashMap<String, oneshot::Sender<Result<(), String>>>,
    revoke: HashMap<String, oneshot::Sender<Result<(), String>>>,
}

/// Correlation table between in-flight remote executions and the
/// result frames that complete them. ONE shared handle, two holders:
/// the [`RemoteSecondarySpawner`] registers a waiter per request; the
/// coordinator's frame-ingest arm (`handle_respawn_exec_result`)
/// completes it when the observer's result frame lands.
///
/// `std::sync::Mutex` — every critical section is a synchronous map
/// probe, never held across an await (the same discipline as the SLURM
/// provider's `replacement_jobs`). Growth is bounded by the respawn
/// budget (`max_total` concurrent ids at the theoretical worst).
#[derive(Clone, Default)]
pub struct RemoteRespawnPending {
    inner: Arc<Mutex<PendingMaps>>,
}

impl RemoteRespawnPending {
    /// Register a waiter for `new_secondary_id`'s result. Replaces any
    /// stale entry for the same key (its receiver is gone — a prior
    /// waiter that was aborted; the replaced sender's send becomes a
    /// no-op).
    fn register(&self, kind: ExecKind, new_secondary_id: &str) -> PendingWaiter {
        let (tx, rx) = oneshot::channel();
        let mut maps = self.inner.lock().expect("remote-respawn pending poisoned");
        match kind {
            ExecKind::Spawn => maps.spawn.insert(new_secondary_id.to_owned(), tx),
            ExecKind::Revoke => maps.revoke.insert(new_secondary_id.to_owned(), tx),
        };
        PendingWaiter {
            rx,
            kind,
            key: new_secondary_id.to_owned(),
            pending: self.clone(),
        }
    }

    fn complete(&self, kind: ExecKind, new_secondary_id: &str, result: Result<(), String>) -> bool {
        let sender = {
            let mut maps = self.inner.lock().expect("remote-respawn pending poisoned");
            match kind {
                ExecKind::Spawn => maps.spawn.remove(new_secondary_id),
                ExecKind::Revoke => maps.revoke.remove(new_secondary_id),
            }
        };
        match sender {
            // A send error means the waiter future was dropped (run
            // teardown aborted it) — the entry is already removed, so
            // this is a quiet success for bookkeeping purposes.
            Some(tx) => {
                let _ = tx.send(result);
                true
            }
            None => false,
        }
    }

    fn remove(&self, kind: ExecKind, new_secondary_id: &str) {
        let mut maps = self.inner.lock().expect("remote-respawn pending poisoned");
        match kind {
            ExecKind::Spawn => maps.spawn.remove(new_secondary_id),
            ExecKind::Revoke => maps.revoke.remove(new_secondary_id),
        };
    }

    /// Complete the spawn waiter for `new_secondary_id` with the
    /// observer-reported outcome. `false` = no waiter (an outcome for a
    /// request this primary never sent — a failover leftover or a
    /// duplicate-result replay; the caller logs at debug).
    pub(crate) fn complete_spawn(&self, new_secondary_id: &str, result: Result<(), String>) -> bool {
        self.complete(ExecKind::Spawn, new_secondary_id, result)
    }

    /// Revoke twin of [`Self::complete_spawn`].
    pub(crate) fn complete_revoke(
        &self,
        new_secondary_id: &str,
        result: Result<(), String>,
    ) -> bool {
        self.complete(ExecKind::Revoke, new_secondary_id, result)
    }
}

/// One registered waiter. Removes its own pending entry when dropped
/// WITHOUT being completed (the waiter future was aborted mid-await),
/// so the table never accumulates dead senders.
struct PendingWaiter {
    rx: oneshot::Receiver<Result<(), String>>,
    kind: ExecKind,
    key: String,
    pending: RemoteRespawnPending,
}

impl Drop for PendingWaiter {
    fn drop(&mut self) {
        // Idempotent: a completed waiter's entry was already removed by
        // `complete`; this only reaps the aborted-mid-await case.
        self.pending.remove(self.kind, &self.key);
    }
}

/// The remote [`SecondarySpawner`] backend: sends typed request frames
/// to the provider-host process and awaits the correlated result frame
/// the coordinator's ingest completes. See the module doc for the
/// delivery/idempotency contract.
pub struct RemoteSecondarySpawner<I: Identifier> {
    /// The primary's own queued mesh egress (a cheap clone — every
    /// clone shares the one pump queue).
    client: MeshClient<I>,
    /// The provider-host peer (the submitter process,
    /// [`dynrunner_core::SETUP_NODE_ID`] — host-id-stable across its
    /// primary→observer demotion).
    provider_host: PeerId,
    /// This primary's node id, stamped as `sender_id` on every request.
    local_id: String,
    /// The shared correlation table (the coordinator holds the
    /// completing end).
    pending: RemoteRespawnPending,
}

impl<I: Identifier> RemoteSecondarySpawner<I> {
    pub(crate) fn new(
        client: MeshClient<I>,
        provider_host: PeerId,
        local_id: String,
        pending: RemoteRespawnPending,
    ) -> Self {
        Self {
            client,
            provider_host,
            local_id,
            pending,
        }
    }

    /// Queue one request frame to the provider host, stamped with the
    /// role-bearing routing target the receiving pump demuxes against
    /// its observer slot. An `Err` means the LOCAL mesh-pump is gone
    /// (this node is winding down) — the one unretriable send failure.
    fn send_request(&self, frame: DistributedMessage<I>) -> Result<(), String> {
        let dst = Destination::Observer(self.provider_host.clone());
        self.client.send(dst.clone(), frame.with_target(dst))
    }
}

#[async_trait::async_trait(?Send)]
impl<I: Identifier> SecondarySpawner for RemoteSecondarySpawner<I> {
    /// Send the spawn request and await the observer's result,
    /// re-sending on a bounded-backoff cadence (loud WARN per re-send)
    /// until a result lands. Indefinite by design: the budget is spent
    /// and the id minted, so delivery is the only pending step; the
    /// `respawn_tasks` drain aborts this future at run teardown.
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        let mut waiter = self.pending.register(ExecKind::Spawn, &spec.new_secondary_id);
        let mut delay = SPAWN_RESEND_INITIAL;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let frame = DistributedMessage::RespawnSpawnRequest {
                target: None,
                sender_id: self.local_id.clone(),
                timestamp: crate::primary::wire::timestamp_now(),
                new_secondary_id: spec.new_secondary_id.clone(),
                primary_endpoint: spec.primary_endpoint.clone(),
                primary_pubkey_pem: spec.primary_pubkey_pem.clone(),
            };
            if let Err(e) = self.send_request(frame) {
                // Local pump gone — the node is winding down; nothing
                // further can be delivered from this process.
                return Err(SpawnError::ProviderUnavailable(format!(
                    "local mesh egress closed: {e}"
                )));
            }
            match tokio::time::timeout(delay, &mut waiter.rx).await {
                Ok(Ok(Ok(()))) => return Ok(()),
                Ok(Ok(Err(provider_err))) => {
                    // The observer-side provider failed — surfaced to
                    // the budget/logging exactly as a local provider
                    // `Err` (the `respawn_failed` event).
                    return Err(SpawnError::Other(provider_err));
                }
                Ok(Err(_recv_gone)) => {
                    // Sender vanished without a result: only possible if
                    // a concurrent re-registration replaced the entry —
                    // a logic error worth surfacing, not retrying.
                    return Err(SpawnError::Other(
                        "remote respawn waiter displaced before a result arrived".to_string(),
                    ));
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        target: "dynrunner_respawn",
                        new_secondary_id = %spec.new_secondary_id,
                        provider_host = %self.provider_host,
                        attempt,
                        next_resend_s = delay.min(SPAWN_RESEND_CAP).as_secs(),
                        event = "respawn_remote_resend",
                        "no spawn result from the provider host yet; re-sending \
                         the request (idempotent — the observer dedupes on the \
                         replacement id). The request is queued, not dropped.",
                    );
                    delay = (delay * 2).min(SPAWN_RESEND_CAP);
                }
            }
        }
    }

    /// Send the revoke request and await the observer's result, with
    /// bounded re-sends. Exhaustion returns the same
    /// `ProviderUnavailable` a local provider returns on an unreachable
    /// backend: the caller logs loudly, and the provider-side
    /// run-teardown sweep (the job id is on the PROVIDER host's
    /// `job_ids` ledger — submission happened there) reclaims the job.
    async fn revoke(&self, new_secondary_id: &str) -> Result<(), SpawnError> {
        let mut waiter = self.pending.register(ExecKind::Revoke, new_secondary_id);
        for attempt in 1..=REVOKE_MAX_ATTEMPTS {
            let frame = DistributedMessage::RespawnRevokeRequest {
                target: None,
                sender_id: self.local_id.clone(),
                timestamp: crate::primary::wire::timestamp_now(),
                new_secondary_id: new_secondary_id.to_owned(),
            };
            if let Err(e) = self.send_request(frame) {
                return Err(SpawnError::ProviderUnavailable(format!(
                    "local mesh egress closed: {e}"
                )));
            }
            match tokio::time::timeout(REVOKE_RESEND_INTERVAL, &mut waiter.rx).await {
                Ok(Ok(Ok(()))) => return Ok(()),
                Ok(Ok(Err(provider_err))) => return Err(SpawnError::Other(provider_err)),
                Ok(Err(_recv_gone)) => {
                    return Err(SpawnError::Other(
                        "remote revoke waiter displaced before a result arrived".to_string(),
                    ));
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        target: "dynrunner_respawn",
                        new_secondary_id,
                        provider_host = %self.provider_host,
                        attempt,
                        max_attempts = REVOKE_MAX_ATTEMPTS,
                        event = "respawn_remote_revoke_resend",
                        "no revoke result from the provider host yet; re-sending \
                         (idempotent at the provider)",
                    );
                }
            }
        }
        Err(SpawnError::ProviderUnavailable(format!(
            "no revoke result from provider host {} after {} attempts",
            self.provider_host, REVOKE_MAX_ATTEMPTS
        )))
    }
}

// ── Coordinator-side result ingest ─────────────────────────────────
//
// The completing end of the correlation table: the operational loop's
// inbox arm routes `RespawnSpawnResult` / `RespawnRevokeResult` frames
// here (via `dispatch_message`), and the matching waiter inside the
// remote spawner's retry loop resolves. Lives in this file so the two
// halves of one correlation contract are read together.

use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

impl<S, E, I> crate::primary::PrimaryCoordinator<S, E, I>
where
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Complete the pending remote execution a respawn result frame
    /// correlates to. An unmatched result (no waiter) is logged at
    /// debug — it is either a duplicate-result replay (the observer
    /// re-sends from its outcome cache on a duplicate request) or a
    /// leftover addressed to a predecessor primary that died mid-spawn;
    /// both are benign by the idempotency contract.
    pub(crate) fn handle_respawn_exec_result(&mut self, msg: DistributedMessage<I>) {
        let Some(pending) = self.remote_respawn_pending.as_ref() else {
            tracing::warn!(
                target: "dynrunner_respawn",
                kind = ?msg.msg_type(),
                sender = %msg.sender_id(),
                "respawn result frame received but no remote respawn backend \
                 is wired on this primary (local-provider topology, or policy \
                 disabled); dropping",
            );
            return;
        };
        match msg {
            DistributedMessage::RespawnSpawnResult {
                new_secondary_id,
                error,
                ..
            } => {
                let result = match error {
                    None => Ok(()),
                    Some(e) => Err(e),
                };
                if !pending.complete_spawn(&new_secondary_id, result) {
                    tracing::debug!(
                        target: "dynrunner_respawn",
                        new_secondary_id = %new_secondary_id,
                        "spawn result without a pending waiter (duplicate \
                         replay or predecessor-primary leftover); ignored",
                    );
                }
            }
            DistributedMessage::RespawnRevokeResult {
                new_secondary_id,
                error,
                ..
            } => {
                let result = match error {
                    None => Ok(()),
                    Some(e) => Err(e),
                };
                if !pending.complete_revoke(&new_secondary_id, result) {
                    tracing::debug!(
                        target: "dynrunner_respawn",
                        new_secondary_id = %new_secondary_id,
                        "revoke result without a pending waiter (duplicate \
                         replay or predecessor-primary leftover); ignored",
                    );
                }
            }
            other => {
                tracing::debug!(
                    target: "dynrunner_respawn",
                    kind = ?other.msg_type(),
                    "non-result frame routed to the respawn result handler; ignored",
                );
            }
        }
    }
}
