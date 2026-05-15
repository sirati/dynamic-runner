//! Fulfillability-matcher trait.
//!
//! Single concern: the boundary the coordinator crosses to ask a
//! consumer-supplied predicate "given THIS just-failed unfulfillable
//! task and THIS snapshot of peer holdings, do you want me to
//! auto-reinject?". One method, no side effects expected from the
//! callee — the coordinator owns the `PrimaryCommand::ReinjectTask`
//! fire on `true`. A return value of `false` (or a panic/exception
//! caught by the bridge) means "leave the task Unfulfillable".
//!
//! The trait is generic over the identifier `I` so it can borrow
//! `&TaskInfo<I>` directly without erasing the type — the PyO3 bridge
//! handles the view-construction at the boundary so trait-object
//! callers (`Box<dyn FulfillabilityMatcher<I>>`) work uniformly.
//!
//! Send-only (not Sync) because the matcher is owned by a single
//! consumer at a time — the operational `select!` arm. No shared
//! observers; one matcher per coordinator.

use std::collections::{HashMap, HashSet};

use dynrunner_core::TaskInfo;

/// Consumer-supplied predicate the matcher pipeline calls once per
/// `TaskState::Unfulfillable` task per batch of holdings updates.
///
/// Return semantics:
/// - `true` → coordinator auto-fires `PrimaryCommand::ReinjectTask{hash}`
///   (shares the `unfulfillable_reinject_remaining` budget with
///   consumer-explicit calls; budget exhaustion is logged at warn).
/// - `false` → task stays `Unfulfillable`; subsequent batches will
///   re-invoke the matcher.
///
/// Implementations should be fast: the matcher invocation runs on the
/// operational `select!` arm, blocking other arms (transport, command
/// channel, heartbeat) for the duration. Heavy work (network calls,
/// disk I/O, expensive Python) belongs off-thread; the matcher's job
/// is a cheap "does the cluster now hold what this task needs?" check.
///
/// Panic / error policy: the pipeline catches errors from the bridge
/// (logged at `warn`) and treats them as `false` for the offending
/// task. Other tasks in the same batch are unaffected. The matcher
/// is never disabled by an error; the next batch re-invokes it.
pub trait FulfillabilityMatcher<I>: Send {
    /// Predicate body. `task` is borrowed read-only out of the
    /// cluster-state ledger; `reason` is the `TaskState::Unfulfillable`
    /// reason string from the same entry; `holdings` is the snapshot
    /// from the most-recent `MatcherTriggerEvent` in this batch.
    ///
    /// Borrowed `&` everywhere: the trait method must not move out of
    /// any argument so the caller can re-invoke with the same snapshot
    /// for the next Unfulfillable task in the batch without cloning.
    fn should_reinject(
        &self,
        hash: &str,
        task: &TaskInfo<I>,
        reason: &str,
        holdings: &HashMap<String, HashSet<String>>,
    ) -> bool;
}
