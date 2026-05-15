//! Matcher-trigger event value type.
//!
//! Single concern: the shape of the value that flows from the
//! cluster-state apply path (on every applied
//! `ClusterMutation::PeerResourceHoldingsUpdated`, E1) to the matcher
//! pipeline. The pipeline collapses bursts of these events into a
//! single matcher pass per `Unfulfillable` task — so the payload here
//! is just a snapshot of the holdings AS OF the apply that fired it.
//! The collapsing logic keeps only the most-recent snapshot; the
//! matcher always reads against the freshest view of the cluster's
//! resource holdings, never against a stale intermediate.
//!
//! The trigger is opaque to the apply path — the apply rule doesn't
//! decide which tasks the matcher will re-check, it only signals that
//! the holdings landscape moved. The pipeline owns the per-Unfulfillable
//! walk against the (separately accessible) `cluster_state.tasks` map.

use std::collections::{HashMap, HashSet};

/// One holdings-changed signal. Sent by the cluster-state apply path
/// whenever a `PeerResourceHoldingsUpdated` mutation lands (E1).
///
/// The `holdings` snapshot is the apply rule's post-state view of
/// `cluster_state.peer_holdings`, captured at emit time so the pipeline
/// reads a consistent snapshot without re-acquiring the cluster-state
/// borrow. Collapses with later events under the 50ms-idle batching
/// rule in [`super::pipeline`] — the matcher sees only the freshest
/// snapshot in a burst.
#[derive(Clone, Debug)]
pub struct MatcherTriggerEvent {
    /// `peer_id` → set of outpath strings the peer currently advertises.
    /// Cloned out of the cluster-state apply path so the pipeline owns
    /// the snapshot independently. `String` (not `Arc<str>`) at this
    /// boundary because the PyO3 bridge needs an owned shape to build
    /// a Python dict anyway.
    pub holdings: HashMap<String, HashSet<String>>,
}
