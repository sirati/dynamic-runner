//! [`SlurmSecondarySpawner`]: the SLURM-provider implementation of
//! [`SecondarySpawner`] from `dynrunner_manager_distributed`. Holds
//! the shared [`SlurmJobManager`] (under `tokio::sync::Mutex`), the
//! [`TunnelEstablisher`] port (defined in
//! [`tunnel`](super::tunnel)), and a per-spec
//! [`WrapperScriptGenerator`] closure. See the module-level docs in
//! [`super`] for the design rationale.

use std::sync::Arc;

use async_trait::async_trait;
use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};
use tokio::sync::Mutex;

use crate::job_manager::SlurmJobManager;

use super::tunnel::TunnelEstablisher;

/// Closure that synthesises a SLURM wrapper-script body for a given
/// respawn spec. Returns the script content (not a path); the
/// [`SlurmJobManager::submit_job`] call below writes it to the gateway
/// at `<root_folder>/job_<job_name>.sh` and submits via
/// `sbatch --parsable`.
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

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), SpawnError>>();

        // Spawn the inseparable (sbatch + job_ids.push + tunnel) work
        // onto the surrounding `LocalSet`. The task is detached from
        // the outer JoinSet so a `.shutdown().await` on
        // `respawn_tasks` only aborts the await below; the inner
        // task keeps running until sbatch returns AND the job_id has
        // been pushed onto `job_manager.job_ids` (so the coordinator's
        // later `cleanup()` can `scancel` the orphan).
        tokio::task::spawn_local(async move {
            // (2) Submit a 1-node sbatch. `submit_job` writes the
            // wrapper, invokes sbatch, and pushes the returned job_id
            // onto its `job_ids` Vec in a single `&mut self` borrow.
            // Holding the Mutex across the whole call serialises
            // concurrent respawns through the same manager — matches
            // the existing pipeline contract.
            let job_id = {
                let mut mgr = job_manager.lock().await;
                match mgr
                    .submit_job(&wrapper_script, &secondary_id, 1, &run_log_dir)
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        // sbatch failed → no job_id was minted, no
                        // push happened, no orphan exists. Surface
                        // the error and stop.
                        let _ = tx.send(Err(SpawnError::Other(format!("sbatch: {e}"))));
                        return;
                    }
                }
            };

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
            let tunnel_outcome = tunnel_establisher
                .establish_one_tunnel(&secondary_id)
                .await;
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
}

