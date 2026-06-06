//! Composition-input helpers for [`super::Node::run`].
//!
//! # Concern
//!
//! ONE concern: derive the per-run composition inputs the [`super::Node::run`]
//! loop needs before spawning roles — the fallback primary args for a node
//! that composed a primary but drives no pipeline, and the process's own
//! host peer-id read off whichever role is live.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::PeerId;

use super::super::run_inputs::PrimaryRunArgs;

/// Empty primary args (no binaries / deps / narration) — the fallback when a
/// primary entry is live but no run args were supplied (a node that composed
/// a primary but drives no pipeline, e.g. a unit fixture).
pub(super) fn empty_primary_args<I: Identifier>() -> PrimaryRunArgs<I> {
    PrimaryRunArgs {
        binaries: Vec::new(),
        phase_deps: std::collections::HashMap::new(),
        on_phase_start: Box::new(|_| {}),
        on_phase_end: Box::new(|_, _, _, _| {}),
    }
}

/// The first live role's host peer-id (every local slot shares it).
pub(super) fn first_live_peer_id<I, P, S, O>(
    primary: &Option<super::super::node::RoleEntry<P, I>>,
    secondary: &Option<super::super::node::RoleEntry<S, I>>,
    observer: &Option<super::super::node::RoleEntry<O, I>>,
) -> PeerId
where
    I: Identifier,
{
    if let Some(e) = primary {
        return e.slot.peer_id().clone();
    }
    if let Some(e) = secondary {
        return e.slot.peer_id().clone();
    }
    if let Some(e) = observer {
        return e.slot.peer_id().clone();
    }
    PeerId::from("")
}
