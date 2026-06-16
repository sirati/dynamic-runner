//! The SecondaryAffine ready-resolution ORIGINATOR detection (#497).
//!
//! Single concern: deciding WHEN a `TaskKind::SecondaryAffine` gate `I`
//! has become dependency-SATISFIED and therefore must transition to the
//! terminal `TaskState::AffineReady` (the READY-not-EXECUTED resolution).
//! A SecondaryAffine task is a primary-side dependency GATE — the primary
//! NEVER executes it; it merely unblocks its dependents the moment its OWN
//! deps are done. This module owns the read-only detection over the
//! replicated ledger; the apply of the resulting
//! [`ClusterMutation::AffineReady`] (the actual `Pending → AffineReady`
//! transition + the dependent resume) lives in sibling `apply.rs`, and the
//! BROADCAST of the originated mutations is the primary's concern.
//!
//! # The WHEN (two firing surfaces, ONE rule)
//!
//! A gate `I` becomes ready iff it is currently `Pending` in the ledger
//! with EVERY dep terminal. That single condition is reached two ways, and
//! this detector treats them identically (it reads the post-apply ledger,
//! not the surface that produced it):
//!
//!   1. **At SPAWN** — a SecondaryAffine task with ZERO deps (or all deps
//!      already terminal) is born `Pending` all-resolved by the spawn
//!      classifier, so its dependents are unblocked from t=0 with no upload
//!      needed (the owner-emphasised no-dep case).
//!   2. **On RESUME** — `I`'s upload dep resolves; the `TaskCompleted` /
//!      `SetupCompleted` apply arm's `resume_blocked_on` transitions `I`
//!      `Blocked → Pending`, at which point its deps are all terminal.
//!
//! The caller hands this detector the `TaskInfo`s that JUST became Pending
//! in an apply pass (the spawn surface AND the resumed surface); the
//! detector filters to the SecondaryAffine gates whose ledger state is
//! `Pending` with all deps resolved and returns one `AffineReady{hash}` per
//! gate. The "all deps resolved" re-check against the live ledger is
//! LOAD-BEARING: a `resume_blocked_on` transitions a `Blocked` entry to
//! `Pending` on its FIRST matching prereq, but a gate with a SECOND
//! still-unresolved dep must NOT be declared ready yet — the re-check
//! catches that case (the next prereq's completion re-runs the detector).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::ClusterMutation;

use super::{ClusterState, TaskState};

impl<I: Identifier> ClusterState<I> {
    /// For every hash in `candidate_hashes` whose ledger entry is a
    /// `Pending` `TaskKind::SecondaryAffine` gate with ALL deps resolved,
    /// produce a [`ClusterMutation::AffineReady`] the originator broadcasts
    /// (which transitions `Pending → AffineReady` + unblocks the gate's
    /// dependents). The SINGLE owner of the READY-not-EXECUTED detection —
    /// both the spawn seam and the resume seam call it on their respective
    /// just-became-Pending surfaces.
    ///
    /// Read-only: it inspects the ledger and returns mutations; it does NOT
    /// apply them (apply is sibling `apply.rs`'s concern). A hash that is
    /// not a `Pending` SecondaryAffine gate (a Work/Setup task, a gate not
    /// yet Pending, a gate with an unresolved dep, an unknown hash) yields
    /// nothing — so passing the full just-became-Pending surface is safe
    /// and the caller never pre-filters by kind.
    pub(crate) fn affine_ready_mutations_for(
        &self,
        candidate_hashes: impl IntoIterator<Item = String>,
    ) -> Vec<ClusterMutation<I>> {
        candidate_hashes
            .into_iter()
            .filter(|hash| self.is_pending_resolved_affine_gate(hash))
            .map(|hash| ClusterMutation::AffineReady { hash })
            .collect()
    }

    /// Scan the WHOLE logical ledger and produce one
    /// [`ClusterMutation::AffineReady`] per `Pending` SecondaryAffine gate
    /// that is already dependency-resolved — the SEED-surface twin of
    /// [`Self::affine_ready_mutations_for`].
    ///
    /// The delta surface (`affine_ready_mutations_for` fed `became_pending`)
    /// fires the originator on the gates that JUST became Pending in an
    /// apply pass (the resume + `TasksSpawned` spawn surfaces). But a gate
    /// seeded directly into the ledger via `TaskAdded` — the cold-seed,
    /// discover-on-promotion, and promotion-snapshot originators — enters
    /// `Pending` WITHOUT riding any apply-pass delta surface (`TaskAdded`'s
    /// apply arm deliberately does NOT feed the pool-growth
    /// `newly_pending_from_spawn` channel — the receive side rebuilds the
    /// whole pool for a `TaskAdded` batch instead — so its freshly-Pending
    /// gates never reach `became_pending`). A no-dep gate (or one whose
    /// deps are already terminal at seed time, e.g. a pre-succeeded staging
    /// setup task) is therefore born `Pending`-all-resolved yet never
    /// transitions to `AffineReady`, leaving its dependents Blocked forever.
    ///
    /// This scan closes that gap by reading the post-seed/post-hydrate
    /// ledger directly: the SAME [`Self::is_pending_resolved_affine_gate`]
    /// detection rule the delta surface uses (one detection owner), applied
    /// over every fat entry. A `Pending` gate is never settled (only
    /// terminals settle), so iterating the fat map is complete. Idempotent:
    /// a gate already `AffineReady` is not `Pending`, so it is skipped; the
    /// scan can run on every seed-convergence pass without re-emitting.
    pub(crate) fn affine_ready_mutations_for_ledger(&self) -> Vec<ClusterMutation<I>> {
        let candidates: Vec<String> = self
            .tasks_iter()
            .filter_map(|(hash, state)| match state {
                TaskState::Pending { .. } if state.def().kind.is_secondary_affine() => {
                    Some(hash.clone())
                }
                _ => None,
            })
            .collect();
        self.affine_ready_mutations_for(candidates)
    }

    /// Whether `hash`'s ledger entry is a `Pending` SecondaryAffine gate
    /// with every dep terminal — the exact READY-not-EXECUTED firing
    /// condition. A settled entry can never be `Pending` (only terminals
    /// settle), so the live-state read suffices for the gate itself; the
    /// per-dep terminality consults the full logical ledger via
    /// [`Self::task_view`] (fat or settled).
    fn is_pending_resolved_affine_gate(&self, hash: &str) -> bool {
        let Some(state @ TaskState::Pending { .. }) = self.task_state(hash) else {
            return false;
        };
        let def = state.def();
        if !def.kind.is_secondary_affine() {
            return false;
        }
        def.task_depends_on.iter().all(|dep| {
            self.task_hash_for_dep(&dep.phase_id, dep.task_id.as_str())
                .and_then(|dep_hash| self.task_view(dep_hash))
                .is_some_and(|view| view.is_terminal())
        })
    }
}
