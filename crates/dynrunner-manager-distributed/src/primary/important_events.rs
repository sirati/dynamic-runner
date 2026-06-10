//! Primary-side "important" (LLM-wake-worthy) event emission.
//!
//! # Concern
//!
//! Single concern: own the crate-local *importance marker* — the
//! [`IMPORTANT_TARGET`] tracing target — and the ONE event whose
//! occurrence point is an apply-path mutation rather than a single
//! call site: **primary changed**. Every other primary-side important
//! event (connected-to-gateway, phase-start, initial-setup-done,
//! error-/OOM-retry) is a direct `tracing::info!(target:
//! IMPORTANT_TARGET, …)` at its own occurrence point; only
//! "primary changed" is observed indirectly, so it subscribes to the
//! existing [`crate::RoleChangeHook`] fabric here.
//!
//! # Why a subscription, not an inline log
//!
//! `ClusterMutation::PrimaryChanged` is applied on the CRDT apply path
//! (`cluster_state::apply`), which already fires
//! `fire_role_change_hooks` after the role table mutates. Logging at
//! that apply site would couple the CRDT-apply concern (whose single
//! job is to converge replicated state) to the unrelated
//! logging/observability concern. Instead the apply path stays
//! untouched and this module *subscribes* a hook — exactly the fabric
//! the transport write-through cache and the observer announcer
//! already ride.
//!
//! # Importance marker
//!
//! The marker is the fixed tracing target string `"dynrunner_important"`
//! (mirrored by `dynrunner-pyo3`'s `logging::IMPORTANT_TARGET` and the
//! Python child logger `dynamic_runner.important`). The dual-sink in
//! `dynrunner-pyo3` routes this target to stdio under
//! `--important-stdio-only`. Emitting at this target is the only thing
//! a call site needs to know — the stdio gate is one filter, never a
//! per-call-site `if`.

use std::sync::Mutex;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{RoleChangeHookRegistrar, RoleTable};

use crate::ClusterState;

/// Tracing target marking an event as "important" (LLM-wake-worthy).
///
/// Re-exported from the single cross-crate source of truth
/// ([`dynrunner_core::IMPORTANT_TARGET`]) and mirrored by the Python child
/// logger `dynamic_runner.important`. The primary's several emit sites
/// share this one const rather than repeating the literal.
pub(crate) use dynrunner_core::IMPORTANT_TARGET;

/// Register a [`crate::RoleChangeHook`] on `cluster_state` that emits
/// the **primary changed** important event whenever the replicated
/// `RoleTable.primary` actually transitions to a new holder.
///
/// `fire_role_change_hooks` fires on EVERY role-table mutation —
/// `PrimaryChanged` AND observer-set churn (`PeerJoined { is_observer:
/// true }`) AND the late-joiner snapshot restore. The hook therefore
/// tracks the last-seen primary and emits only when `table.primary`
/// genuinely changed, so observer churn and idempotent re-delivery
/// never produce a spurious "primary changed" line.
///
/// Idempotent apply (lower epoch / duplicate at the same epoch) is
/// already filtered upstream: those return `ApplyOutcome::NoOp` and do
/// NOT fire the hook at all (see the `PrimaryChanged` apply arm), so
/// the delta check here only ever filters the legitimate cross-event
/// fires (observer churn / restore-with-unchanged-primary).
///
/// Registered once, at primary-coordinator construction, on the node
/// that emits the other primary-side important events; a promoted
/// secondary runs its own same-peer primary coordinator, so the hook
/// rides the promotion automatically.
pub(crate) fn register_primary_changed_important_hook<I: Identifier>(
    cluster_state: &mut ClusterState<I>,
) {
    // Seed with the table's current primary so a hook registered after
    // an initial `PrimaryChanged` (none in the bootstrap path, but
    // robust against reuse) does not re-announce an unchanged holder.
    let last_seen: Mutex<Option<String>> = Mutex::new(cluster_state.role_table().primary.clone());
    cluster_state.register_role_change_hook(Box::new(move |table: &RoleTable| {
        let mut guard = last_seen.lock().unwrap_or_else(|e| e.into_inner());
        if *guard != table.primary {
            *guard = table.primary.clone();
            if let Some(primary) = guard.as_deref() {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    primary = %primary,
                    "primary changed",
                );
            }
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_state::ApplyOutcome;
    use crate::test_capture::{ImportantCapture, important_only};
    use dynrunner_core::RunnerIdentifier;
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
    use tracing::subscriber::with_default;
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    /// The hook fires exactly once per genuine primary transition and
    /// stays silent on observer-set churn and on idempotent re-delivery.
    #[test]
    fn primary_changed_emits_one_important_event_per_transition() {
        let capture = ImportantCapture::default();
        let subscriber = Registry::default().with(capture.clone().with_filter(important_only()));

        with_default(subscriber, || {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            register_primary_changed_important_hook(&mut state);

            // No primary yet → no event.
            assert!(capture.messages().is_empty());

            // First real change → one event.
            assert_eq!(
                state.apply(ClusterMutation::PrimaryChanged {
                    new: "secondary-3".into(),
                    epoch: 1,
                    reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
                }),
                ApplyOutcome::Applied
            );

            // Observer-set churn fires the role-change hooks but does
            // NOT change the primary → no extra event.
            assert_eq!(
                state.apply(ClusterMutation::PeerJoined {
                    peer_id: "obs-1".into(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                }),
                ApplyOutcome::Applied
            );

            // A genuine failover to a new primary → one more event.
            assert_eq!(
                state.apply(ClusterMutation::PrimaryChanged {
                    new: "secondary-7".into(),
                    epoch: 2,
                    reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
                }),
                ApplyOutcome::Applied
            );

            // Idempotent re-delivery (same holder, same epoch) is a
            // NoOp upstream and fires no hook → no extra event.
            assert_eq!(
                state.apply(ClusterMutation::PrimaryChanged {
                    new: "secondary-7".into(),
                    epoch: 2,
                    reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
                }),
                ApplyOutcome::NoOp
            );
        });

        let msgs = capture.messages();
        assert_eq!(
            msgs.len(),
            2,
            "expected exactly two primary-changed events, got {msgs:?}"
        );
        assert!(
            msgs.iter().all(|m| m.contains("primary changed")),
            "{msgs:?}"
        );
    }
}
