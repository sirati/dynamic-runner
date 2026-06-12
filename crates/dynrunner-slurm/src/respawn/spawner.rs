//! [`SlurmSecondarySpawner`]: the SLURM-provider implementation of
//! [`SecondarySpawner`] from `dynrunner_manager_distributed`. Holds
//! the shared [`SlurmJobManager`] (under `tokio::sync::Mutex`), the
//! [`TunnelEstablisher`] port (defined in
//! [`tunnel`](super::tunnel)), a per-spec
//! [`WrapperScriptGenerator`] closure, and the node-local
//! secondary-id → sbatch-job-id bookkeeping that backs
//! [`SecondarySpawner::revoke`] (cancel a replacement made redundant
//! by its original's re-admission). See the module-level docs in
//! [`super`] for the design rationale.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};
use tokio::sync::Mutex;

use crate::job_manager::{CancelOutcome, SlurmJobManager};

use super::tunnel::TunnelEstablisher;

/// Per-replacement bookkeeping entry in
/// [`SlurmSecondarySpawner::replacement_jobs`]. The two states close
/// the revoke-vs-submit race without the revoker ever waiting on the
/// submitting task:
///
/// - `Submitted(job_id)`: sbatch returned this id; a `revoke` for the
///   secondary id takes the entry and scancels it.
/// - `Revoked`: a `revoke` arrived BEFORE sbatch completed (or even
///   before `spawn` ran). The submitting task observes the tombstone
///   right after `submit_job` returns and scancels its own freshly-
///   minted job immediately, skipping the tunnel step.
#[derive(Debug)]
enum ReplacementJob {
    Submitted(String),
    Revoked,
}

/// Closure that synthesises a SLURM wrapper-script body for a given
/// respawn spec. Returns the script content (not a path); the
/// [`SlurmJobManager::submit_job`] call below pipes it to sbatch over
/// STDIN (`printf '%s' '<body>' | sbatch --parsable …`) — no gateway-
/// side `job_<job_name>.sh` file is written.
///
/// `Send + Sync` because the spawner is shared via `Arc<dyn
/// SecondarySpawner>` and the trait method takes `&self`.
pub type WrapperScriptGenerator =
    Arc<dyn Fn(&SecondarySpawnSpec) -> Result<String, String> + Send + Sync>;

/// SLURM provider implementation of [`SecondarySpawner`].
///
/// Composes the existing SLURM collaborators (`SlurmJobManager`,
/// the `TunnelEstablisher` port, and the wrapper-script generator)
/// into the trait surface that `dynrunner-manager-distributed`
/// consumes.
pub struct SlurmSecondarySpawner<G: Gateway, T: TunnelEstablisher> {
    /// `SlurmJobManager` shared with whatever else drives sbatch on
    /// this run (typically the pipeline's initial-setup path). Wrapped
    /// in `tokio::sync::Mutex` because `submit_job` takes `&mut self`
    /// (it pushes onto `job_ids`); the lock is held across an await.
    job_manager: Arc<Mutex<SlurmJobManager<G>>>,
    /// Reverse-tunnel port. Production binds to `SlurmPreparation`;
    /// tests bind to an in-memory recorder. The spawner sees only
    /// "give me a tunnel for this id".
    tunnel_establisher: Arc<T>,
    /// Per-spec wrapper-script body generator. Bound at wire-up time
    /// so `spawn()` stays parameterised purely over the spec.
    wrapper_script_generator: WrapperScriptGenerator,
    /// `run_log_dir` forwarded into `submit_job` for the
    /// `--output=`/`--error=` paths. Same shape the initial-setup path
    /// uses; captured here so the per-respawn call site doesn't have
    /// to re-derive it.
    run_log_dir: String,
    /// Node-local secondary-id → replacement-job bookkeeping backing
    /// [`SecondarySpawner::revoke`]. Shared (`Arc`) with the detached
    /// inner submit task, which records the sbatch job id here (or
    /// honours a `Revoked` tombstone — see [`ReplacementJob`]).
    /// `std::sync::Mutex` — every critical section is a synchronous
    /// map probe, never held across an await. Entries whose
    /// replacement joins the run and is never revoked stay until drop;
    /// growth is bounded by the respawn budget, the same story as
    /// `SlurmJobManager::job_ids`.
    replacement_jobs: Arc<StdMutex<HashMap<String, ReplacementJob>>>,
}

impl<G: Gateway, T: TunnelEstablisher> SlurmSecondarySpawner<G, T> {
    /// Construct a SLURM spawner. In production, all collaborators
    /// must already be initialised — in particular, the
    /// `TunnelEstablisher` must wrap a `SlurmPreparation` that has
    /// had `setup_ssh_tunnels` called at least once so its
    /// `primary_quic_port` is captured (see
    /// [`SlurmPreparation::establish_one_tunnel`] for why).
    pub fn new(
        job_manager: Arc<Mutex<SlurmJobManager<G>>>,
        tunnel_establisher: Arc<T>,
        wrapper_script_generator: WrapperScriptGenerator,
        run_log_dir: String,
    ) -> Self {
        Self {
            job_manager,
            tunnel_establisher,
            wrapper_script_generator,
            run_log_dir,
            replacement_jobs: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

#[async_trait(?Send)]
impl<G, T> SecondarySpawner for SlurmSecondarySpawner<G, T>
where
    G: Gateway + Send + Sync + 'static,
    T: TunnelEstablisher + 'static,
{
    /// Orphan-safety shape: the actual sbatch + job_ids.push + tunnel
    /// setup runs inside `tokio::task::spawn_local`, NOT inline on the
    /// caller's future. The `spawn_local` task is parented to the
    /// surrounding `LocalSet` (the operational loop's `run_until`),
    /// not to the coordinator's `respawn_tasks: JoinSet<RespawnOutcome>`
    /// which gets `.shutdown().await`-d on teardown. The outer
    /// `spawn()` future awaits a `oneshot::Receiver`; dropping the
    /// receiver does NOT abort the inner task — the sbatch finishes,
    /// `submit_job` pushes the job_id onto `job_ids`, and the
    /// coordinator's later `cleanup()` `scancel`s the orphan.
    ///
    /// This closes two hazard windows the brief identified:
    /// (a) sbatch submitted but `job_ids.push` aborted → orphan with
    ///     no scancel record.
    /// (b) sbatch recorded but tunnel-setup aborted → SLURM job runs
    ///     orphaned in the queue.
    /// Both are fixed by making the (sbatch + push + tunnel) sequence
    /// inseparable from a JoinSet-shutdown perspective.
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        // (1) Synthesise the wrapper script body for this respawn id.
        // The generator owns the deployment-context capture; any
        // failure (template-render, missing config, …) lands here as
        // an opaque string so the trait's `SpawnError` variants stay
        // provider-agnostic. Wrapper-gen failure is a synchronous
        // local-state error; no sbatch has touched the gateway yet,
        // so we surface it directly without taking the inner-task
        // detour.
        let wrapper_script = (self.wrapper_script_generator)(&spec)
            .map_err(|e| SpawnError::Other(format!("wrapper-gen: {e}")))?;

        // Snapshot the collaborator handles the inner task needs.
        // Cloning is cheap (Arc clones); the inner task moves them in
        // so it has no lifetime dependency on `self`.
        let job_manager = Arc::clone(&self.job_manager);
        let tunnel_establisher = Arc::clone(&self.tunnel_establisher);
        let run_log_dir = self.run_log_dir.clone();
        let secondary_id = spec.new_secondary_id.clone();
        // The dead member's node, carried on the spec — excluded from the
        // replacement's sbatch when known so it never lands back on a
        // NODE_FAIL/faulty node. `None` places without constraint.
        let exclude_node = spec.exclude_node.clone();
        let replacement_jobs = Arc::clone(&self.replacement_jobs);

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), SpawnError>>();

        // Spawn the inseparable (sbatch + job_ids.push + tunnel) work
        // onto the surrounding `LocalSet`. The task is detached from
        // the outer JoinSet so a `.shutdown().await` on
        // `respawn_tasks` only aborts the await below; the inner
        // task keeps running until sbatch returns AND the job_id has
        // been pushed onto `job_manager.job_ids` (so the coordinator's
        // later `cleanup()` can `scancel` the orphan).
        tokio::task::spawn_local(async move {
            // (2) Submit a 1-node sbatch. `submit_job` pipes the
            // wrapper body to sbatch over STDIN, invokes sbatch, and
            // pushes the returned job_id onto its `job_ids` Vec in a
            // single `&mut self` borrow.
            // Holding the Mutex across the whole call serialises
            // concurrent respawns through the same manager — matches
            // the existing pipeline contract.
            let job_id = {
                let mut mgr = job_manager.lock().await;
                match mgr
                    .submit_job(
                        &wrapper_script,
                        &secondary_id,
                        &secondary_id,
                        1,
                        &run_log_dir,
                        exclude_node.as_deref(),
                    )
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        // sbatch failed → no job_id was minted, no
                        // push happened, no orphan exists. Clear any
                        // `Revoked` tombstone a concurrent revoke may
                        // have parked for us (there is no job for it
                        // to apply to), surface the error, and stop.
                        replacement_jobs
                            .lock()
                            .expect("replacement_jobs mutex poisoned")
                            .remove(&secondary_id);
                        let _ = tx.send(Err(SpawnError::Other(format!("sbatch: {e}"))));
                        return;
                    }
                }
            };

            // Record the freshly-minted job id under this replacement's
            // secondary id — the bookkeeping `revoke` consults — unless
            // a revoke already tombstoned the entry (the replacement
            // became redundant while sbatch was in flight). In the
            // tombstoned case the job is scancel'd right here by its
            // own submitter and the tunnel step is skipped: nothing
            // will ever connect to a cancelled job.
            let already_revoked = {
                let mut jobs = replacement_jobs
                    .lock()
                    .expect("replacement_jobs mutex poisoned");
                match jobs.get(&secondary_id) {
                    Some(ReplacementJob::Revoked) => {
                        jobs.remove(&secondary_id);
                        true
                    }
                    _ => {
                        jobs.insert(
                            secondary_id.clone(),
                            ReplacementJob::Submitted(job_id.clone()),
                        );
                        false
                    }
                }
            };
            if already_revoked {
                tracing::info!(
                    new_secondary_id = %secondary_id,
                    job_id = %job_id,
                    "respawn revoked while sbatch was in flight; \
                     cancelling the freshly-submitted job",
                );
                // Best-effort: on a transport failure the job_id is
                // already on `job_manager.job_ids` (submit_job pushed
                // it), so the coordinator's `cleanup()` scancels it at
                // run teardown — the same orphan safety net the abort
                // path relies on.
                let mgr = job_manager.lock().await;
                if let Err(e) = mgr.cancel_job(&job_id).await {
                    tracing::warn!(
                        new_secondary_id = %secondary_id,
                        job_id = %job_id,
                        error = %e,
                        "could not scancel the revoked-in-flight respawn job; \
                         it remains on job_ids for the run-teardown sweep",
                    );
                }
                let _ = tx.send(Err(SpawnError::Revoked));
                return;
            }

            tracing::info!(
                new_secondary_id = %secondary_id,
                job_id = %job_id,
                "respawn sbatch submitted; awaiting tunnel",
            );

            // (3) Bring up the reverse tunnel for the new secondary
            // via the `TunnelEstablisher` port. Failure here is NOT
            // an orphan condition for sbatch: the job_id has already
            // landed on `job_ids` (above), so `cleanup()` will
            // `scancel` it on coordinator drop. The error is still
            // surfaced to the budget so a flapping tunnel doesn't
            // silently re-submit.
            let tunnel_outcome = tunnel_establisher.establish_one_tunnel(&secondary_id).await;
            match tunnel_outcome {
                Ok(()) => {
                    tracing::info!(
                        new_secondary_id = %secondary_id,
                        job_id = %job_id,
                        "respawn tunnel established",
                    );
                    let _ = tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = tx.send(Err(SpawnError::Other(format!("tunnel: {e}"))));
                }
            }
        });

        // Outer future awaits the oneshot. If the JoinSet drops this
        // future, the `rx` is dropped — the inner task's `tx.send`
        // becomes a no-op (oneshot::Sender::send returns Err if rx is
        // gone, which we ignore via `let _ =`). The inner task
        // continues to completion regardless, so the orphan-safety
        // invariant holds.
        match rx.await {
            Ok(result) => result,
            Err(_recv_err) => Err(SpawnError::Other(
                "spawn inner task dropped its sender before completion".to_string(),
            )),
        }
    }

    /// Best-effort scancel of the replacement job submitted for
    /// `new_secondary_id` (the member it replaces was re-admitted
    /// before the replacement joined — the queued/running job is a
    /// redundant allocation squatter).
    ///
    /// Race tolerance per the trait contract:
    /// - job already submitted → take the recorded id and `scancel`
    ///   it. A job the controller no longer knows (finished/purged) is
    ///   a quiet no-op (`CancelOutcome::AlreadyGone`, debug log).
    /// - submission still in flight (or `spawn` not yet polled) → park
    ///   a `Revoked` tombstone; the submitting task scancels its own
    ///   job the moment sbatch returns (see `spawn`).
    /// - `Err` ONLY on a gateway transport failure: scancel never ran.
    ///   The job id is still on `job_manager.job_ids` (submit_job
    ///   pushed it), so the run-teardown `cleanup()` sweep scancels it
    ///   regardless — the caller logs loudly, nothing is leaked past
    ///   the run.
    async fn revoke(&self, new_secondary_id: &str) -> Result<(), SpawnError> {
        let submitted_job_id = {
            let mut jobs = self
                .replacement_jobs
                .lock()
                .expect("replacement_jobs mutex poisoned");
            match jobs.remove(new_secondary_id) {
                Some(ReplacementJob::Submitted(job_id)) => Some(job_id),
                // Already tombstoned by an earlier revoke (put it
                // back — idempotent) or not submitted yet: park the
                // tombstone for the submitting task.
                Some(ReplacementJob::Revoked) | None => {
                    jobs.insert(new_secondary_id.to_owned(), ReplacementJob::Revoked);
                    tracing::debug!(
                        new_secondary_id,
                        "revoke arrived before sbatch completed; tombstoned — \
                         the submitting task will scancel its own job",
                    );
                    None
                }
            }
        };
        let Some(job_id) = submitted_job_id else {
            return Ok(());
        };
        let mgr = self.job_manager.lock().await;
        match mgr.cancel_job(&job_id).await {
            Ok(CancelOutcome::Cancelled) => {
                tracing::info!(
                    new_secondary_id,
                    job_id = %job_id,
                    "revoked redundant respawn job (scancel issued)",
                );
                Ok(())
            }
            Ok(CancelOutcome::AlreadyGone) => {
                tracing::debug!(
                    new_secondary_id,
                    job_id = %job_id,
                    "revoked respawn job was already gone; nothing to cancel",
                );
                Ok(())
            }
            Err(e) => Err(SpawnError::Other(format!(
                "scancel for revoked respawn job {job_id}: {e}"
            ))),
        }
    }
}
