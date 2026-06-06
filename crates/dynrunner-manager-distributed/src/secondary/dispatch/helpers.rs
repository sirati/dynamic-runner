//! Shared helpers used by the dispatch router AND by sibling secondary
//! subsystems (notably `wait_for_setup`'s receive loop, which applies
//! `ClusterMutation` batches with identical semantics to the
//! operational router; the early-staging path that runs before
//! per-task assignments; and the unresolvable-task fail-loud guard
//! that both `dispatch_message` and `handle_initial_assignment` need).
//!
//! Single concern: provide the apply / stage / fail-loud primitives
//! the router and its setup-time counterpart share, so each rule has
//! exactly one writer.

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::election::ElectionState;
use super::super::wire::timestamp_now;
use crate::cluster_state::ApplyOutcome;
use crate::process::PromotionSignal;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Mirror a batch of `ClusterMutation`s into the local replicated
    /// CRDT AND react to a primary-identity change that names THIS node.
    /// Shared between the operational `dispatch_message` /
    /// `handle_inbound` arms, the peer-mesh relay, and `wait_for_setup`'s
    /// receive loop — every site observes the same wire variant and must
    /// apply with identical semantics. CRDT idempotency makes repeated
    /// apply safe (duplicates and late-after-terminal arrivals NoOp by
    /// precondition).
    ///
    /// For every non-`PrimaryChanged` variant this is a PURE CRDT mirror:
    /// the secondary holds no authority and no dispatch pool, so there is
    /// no pool-growth side effect. The authoritative dispatch-pool
    /// coherence (re-injecting freshly-`Pending` tasks surfaced by a
    /// `TasksSpawned` apply) is the `PrimaryCoordinator`'s concern, driven
    /// on the authority's own pool. A non-authority node simply converges
    /// its CRDT mirror; it never decides what to dispatch from it.
    ///
    /// `PrimaryChanged` is the SINGLE primary-activation frame. Applying
    /// it is also the ONE activation path: this hook runs
    /// [`Self::apply_primary_changed`] per such mutation so a
    /// `PrimaryChanged { new = self }` arriving over ANY receive path
    /// (operational dispatch, peer relay, or setup-time) advances the
    /// CRDT primary identity and — on a self-named promotion — leaves the
    /// Phase-C seam that SIGNALS `Process` to build the primary (it does
    /// NOT build one here), then resets the failover election. It keys on
    /// identity, not on election history — a node that never
    /// suspected/voted still reacts when named.
    ///
    /// Returns `true` iff a `PrimaryChanged` genuinely advanced the
    /// primary identity (an `Applied`, not a stale-epoch NoOp or an
    /// observer rejection). The async operational receive arms react to
    /// that signal by reviving the worker-pull rate-limiter (backoff
    /// accrued against the PRIOR primary is stale the moment the role
    /// changes) and immediately re-polling idle workers so a freshly-
    /// identified primary gets TaskRequests without a keepalive-interval
    /// delay. Reviving the pull semantic touches the worker pool, so it
    /// is the caller's (async, operational) concern — this sync hook only
    /// reports that the identity moved.
    pub(in crate::secondary) fn apply_cluster_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) -> bool {
        let count = mutations.len();
        let mut primary_changed = false;
        for m in mutations {
            match m {
                // `reason` (Election vs Transferred) is the Phase-C signal
                // discriminant carried through to `Process`; the build of the
                // primary on a self-named promotion is the Phase-C `Process`
                // concern, not done here (see `apply_primary_changed`). The
                // central CRDT epoch-LWW apply itself stays reason-blind.
                ClusterMutation::PrimaryChanged { new, epoch, reason } => {
                    primary_changed |= self.apply_primary_changed(new, epoch, reason);
                }
                other => {
                    self.cluster_state.apply(other);
                }
            }
        }
        tracing::debug!(
            secondary = %self.config.secondary_id,
            applied = count,
            "mirrored cluster mutations into local CRDT"
        );
        primary_changed
    }

    /// The unified primary-activation apply hook for a
    /// `ClusterMutation::PrimaryChanged { new, epoch }` observed on any
    /// receive path. The SINGLE place the secondary reacts to a
    /// primary-identity change:
    ///
    ///   1. **Observer-not-primary guard.** An observer cannot host the
    ///      primary role (no workers, no dispatch authority). If `new`
    ///      names any peer in the replicated `RoleTable.observers`, REJECT
    ///      loud and do NOT install it as `current_primary`. This guard
    ///      protects the single-source-of-truth `current_primary()`
    ///      against a forged or racy announcement naming an observer. (A
    ///      compute SecondaryCoordinator is never itself an observer — the
    ///      observer role IS the ObserverCoordinator — so the self case
    ///      cannot arise.)
    ///   2. **Epoch-LWW apply.** The CRDT `PrimaryChanged` arm is
    ///      last-writer-wins on `(epoch, primary_id)`, so a stale
    ///      lower-epoch announcement NoOps against an already-installed
    ///      higher epoch. Every side effect below is gated on the apply
    ///      actually advancing state (`Applied`), so a no-op announcement
    ///      neither wakes nor resets.
    ///   3. **Self-named → signal + reset.** When `new` is THIS node and
    ///      not an observer, the primary build on the promotion event is
    ///      the Phase-C `Process` concern (the C4 seam — the secondary
    ///      SIGNALS `Process` to construct the `PrimaryCoordinator`; it
    ///      never builds one itself), and this node resets its failover
    ///      election to `Normal` (a primary now exists — no lingering
    ///      Promoted to name).
    ///   4. **Peer-named → reset.** When `new` is a PEER, a primary now
    ///      exists, so any in-flight failover election on this node is
    ///      stale: reset it to `Normal`.
    ///
    /// Returns `true` iff the apply genuinely advanced the primary
    /// identity (`Applied`); `false` on an observer rejection or a
    /// stale-epoch NoOp. The worker-pull revive is the caller's concern
    /// (see [`Self::apply_cluster_mutations`]).
    fn apply_primary_changed(
        &mut self,
        new: String,
        epoch: u64,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason,
    ) -> bool {
        // (1) Observer guard — reject naming an observer before the apply
        // moves `current_primary`.
        let observers = &self.cluster_state.role_table().observers;
        let names_observer = observers.contains(&new);
        if names_observer {
            tracing::error!(
                secondary = %self.config.secondary_id,
                target = %new,
                epoch,
                target_in_role_table_observers = observers.contains(&new),
                "REJECTED PrimaryChanged naming an observer — observers \
                 cannot host the primary role (no workers, no dispatch \
                 authority). Ignoring; the cluster's election should retry \
                 with the observer filtered out."
            );
            return false;
        }

        // (2) Epoch-LWW apply. Side effects below only on a genuine
        // identity advance.
        let outcome = self.cluster_state.apply(ClusterMutation::PrimaryChanged {
            new: new.clone(),
            epoch,
            // The central CRDT apply is reason-blind (epoch-LWW only), so
            // the value carried here is immaterial to the resulting state.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::default(),
        });
        if outcome == ApplyOutcome::NoOp {
            tracing::debug!(
                new_primary = %new,
                epoch,
                "ignoring stale PrimaryChanged superseded by higher epoch"
            );
            return false;
        }

        if new == self.config.secondary_id {
            // (3) This node is the new primary.
            //
            // C4 promotion/transfer signal. The build of the
            // `PrimaryCoordinator` on this promotion event is the `Node`'s
            // concern — the secondary SIGNALS the `Node` (which constructs
            // the snapshot-seeded primary on the event, threading this
            // secondary's `WorkerFactory` to the spawn site) and NEVER
            // builds a primary itself (SUPREME-LAW #3). FIRE the typed
            // `PromotionSignal { reason, epoch, snapshot }`: `reason`
            // (Election vs Transferred) lets the `Node` branch the
            // build/seed path; `epoch` carries the role-table generation the
            // promotion was raised at; `snapshot` is THIS host's converged
            // `cluster_state` captured RIGHT HERE — atomically with the
            // signal, inside the same `&mut self` apply that just advanced
            // the CRDT identity (the `apply` above). Capturing it on the
            // signal (not via a shared-mutable cell the `Node` reads later)
            // keeps the seed coherent with its trigger and owned (`Send`):
            // the `Node` threads it straight to the
            // `PromotedPrimaryBuilder`, which calls
            // `seed_from_promotion_snapshot`. Best-effort — a dropped
            // receiver (or an unwired coordinator: Rust-only unit fixtures)
            // means no `Node` is listening, which the CRDT mutation above
            // has already recorded, so the test still observes the identity
            // advance.
            if let Some(tx) = &self.promotion_tx {
                let snapshot = self.cluster_state.snapshot();
                if tx.send(PromotionSignal { reason, epoch, snapshot }).is_err() {
                    tracing::debug!(
                        secondary = %self.config.secondary_id,
                        epoch,
                        "promotion signal receiver dropped (node winding down); \
                         CRDT primary identity already advanced"
                    );
                }
            } else {
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    epoch,
                    "self-named PrimaryChanged with no promotion signal wired \
                     (unit fixture); CRDT primary identity advanced, no primary built"
                );
            }
            // Reset the election: a primary now exists, so there is no
            // lingering Promoted state.
            self.reset_election_to_normal();
        } else {
            // (4) A peer is the new primary, so any in-flight election on
            // this node is stale: a primary now exists. Reset it.
            self.reset_election_to_normal();
        }
        tracing::info!(
            new_primary = %new,
            epoch,
            "primary role changed"
        );
        true
    }

    /// Reset the failover election to `Normal` iff this node has reached
    /// `Operational` (the only lifecycle state that carries an
    /// `ElectionState`). Pre-`Operational` receive paths
    /// (`wait_for_setup`) hold no election, so this is a no-op there —
    /// using `operational_mut()` rather than `op_mut()` keeps the
    /// pre-`Operational` apply path panic-free.
    fn reset_election_to_normal(&mut self) {
        if let Some(op) = self.lifecycle.operational_mut() {
            op.election = ElectionState::Normal;
        }
    }

    /// Run a `stage_file` copy + register the result in
    /// `extraction_cache`. Shared between the standalone
    /// `DistributedMessage::StageFile` arm in `dispatch_message`
    /// (post-setup re-staging) and the inline `staged_files` records
    /// of `InitialAssignment` (processed by `handle_initial_assignment`
    /// before any per-task assignment runs). Failures are logged and
    /// swallowed — the next TaskAssignment for the same hash will
    /// surface as a TaskFailed via `report_unresolvable_task` rather
    /// than wedging the staging path itself.
    ///
    /// `file_hash` is the cache lookup key (must match the
    /// `TaskAssignment.file_hash` the secondary will see later);
    /// `content_hash` is what `stage_file` verifies against after
    /// the copy. The two were previously a single `file_hash`
    /// field — the conflation always made verification mismatch
    /// (16-char identifier hex vs 64-char content SHA256 hex).
    pub(in crate::secondary) fn stage_and_register(
        &mut self,
        file_hash: &str,
        content_hash: &str,
        src_path: &str,
        dest_path: &str,
    ) {
        let src_tmp = self.extraction_cache.tmp_dir().to_path_buf();
        match super::super::staging::stage_file(
            self.config.src_network.as_deref(),
            &src_tmp,
            src_path,
            dest_path,
            content_hash,
        ) {
            Ok(outcome) => {
                self.extraction_cache.register_path(file_hash, outcome.dest);
                tracing::info!(
                    file_hash = %file_hash,
                    "staged file registered"
                );
            }
            Err(e) => {
                tracing::error!(
                    file_hash = %file_hash,
                    error = %e,
                    "stage_file failed; the next TaskAssignment for this hash will be reported as TaskFailed"
                );
            }
        }
    }

    /// Fail-loud guard for "the worker has no plausible way to open
    /// this binary". Both `dispatch_message` (operational
    /// TaskAssignment) and `handle_initial_assignment`
    /// (InitialAssignment in the setup phase) need the same check —
    /// without it, a missed-resolution silently passes the primary's
    /// filesystem-view path through to the worker, which crashes at
    /// exec time and the primary re-enqueues as Recoverable.
    ///
    /// Returns `Ok(true)` when the task is unresolvable: a
    /// `TaskFailed` NonRecoverable was sent to the primary and the
    /// caller MUST skip the worker assignment. Returns `Ok(false)`
    /// when resolution either succeeded or the path can plausibly
    /// resolve at the worker (in-process distributed mode where
    /// primary and secondary share a filesystem view); the caller
    /// should proceed with the assignment.
    ///
    /// Two ways the worker can succeed without `resolved_path`:
    ///   - the secondary has a staging directory (`src_network`
    ///     set) AND the file landed there — covered by
    ///     `resolved_path.is_some()`.
    ///   - the secondary shares a filesystem view with the primary
    ///     AND `local_path` is the primary's absolute path
    ///     (in-process distributed mode); for that to be plausible
    ///     `local_path` must at minimum be absolute.
    pub(in crate::secondary) async fn report_unresolvable_task(
        &mut self,
        worker_id: u32,
        file_hash: &str,
        local_path: &str,
        resolved_path: &Option<std::path::PathBuf>,
    ) -> Result<bool, String> {
        let local_path_is_relative = std::path::Path::new(local_path).is_relative();
        if resolved_path.is_none() && (self.config.src_network.is_some() || local_path_is_relative)
        {
            // Report against the ORIGINAL wire `worker_id`: this value is
            // only echoed back to the primary in the `TaskFailed` frame, it
            // never indexes the pool here. The prior
            // `worker_id.min(pool.workers.len() - 1)` clamp touched the pool
            // purely to "correct" the reported id — which (a) underflowed on
            // a 0-worker `Operational`/`Configuring` node (`0u32 - 1`) and
            // (b) silently retargeted an out-of-range id onto the last slot.
            // The wire id is the faithful thing to report (the router's
            // backpressure path reports the same un-clamped wire id), so
            // drop the clamp and the pool touch entirely.
            let msg = DistributedMessage::TaskFailed {
                target: None,
                sender_id: self.config.secondary_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: self.config.secondary_id.clone(),
                worker_id,
                task_hash: file_hash.into(),
                error_type: ErrorType::NonRecoverable,
                error_message: format!(
                    "file_hash {file_hash} not pre-staged at {local_path}; \
                     expected StageFile notification first"
                ),
            };
            self.send_to_primary(msg).await?;
            return Ok(true);
        }
        Ok(false)
    }
}
