//! Resource accounting types: scalar [`ResourceAmount`], multi-kind
//! [`ResourceMap`], and the soft-secondary-preference newtype
//! [`SoftPreferredSecondaries`].

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::identifiers::ResourceKind;

/// Soft hint of preferred secondaries (by peer name / id) for a task.
///
/// "Soft" is load-bearing: the scheduler MAY honour this list when picking
/// a secondary, but is not obliged to — if no preferred peer is available
/// the task still dispatches to whichever secondary the scheduler picks.
/// A future "strict" requirement (must run on one of these peers, fail
/// otherwise) MUST be a sibling type (e.g. `StrictRequiredSecondaries`),
/// NOT a boolean flag on this type. The newtype boundary exists to keep
/// soft and strict semantics from collapsing into one fragile field.
///
/// The wire shape is `#[serde(transparent)]` so the on-wire form is
/// indistinguishable from a bare `Vec<String>` — that's what makes
/// `#[serde(default, skip_serializing_if = "…is_empty")]` on the host
/// field safely backward-compatible with pre-this-change peers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SoftPreferredSecondaries(pub Vec<String>);

impl SoftPreferredSecondaries {
    pub fn new(secondaries: Vec<String>) -> Self {
        Self(secondaries)
    }

    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A quantity of a specific resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAmount {
    pub kind: ResourceKind,
    pub amount: u64,
}

/// A map of resource kinds to quantities.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceMap(BTreeMap<ResourceKind, u64>);

impl ResourceMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn get(&self, kind: &ResourceKind) -> u64 {
        self.0.get(kind).copied().unwrap_or(0)
    }

    pub fn insert(&mut self, kind: ResourceKind, amount: u64) {
        self.0.insert(kind, amount);
    }

    pub fn contains_key(&self, kind: &ResourceKind) -> bool {
        self.0.contains_key(kind)
    }

    /// Iterate by reference (the kind is `Arc<str>`-backed and cheap to clone
    /// when the consumer needs ownership).
    pub fn iter(&self) -> impl Iterator<Item = (&ResourceKind, u64)> + '_ {
        self.0.iter().map(|(k, &v)| (k, v))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Add all amounts from `other` to self.
    pub fn add(&mut self, other: &ResourceMap) {
        for (kind, amount) in other.iter() {
            *self.0.entry(kind.clone()).or_insert(0) += amount;
        }
    }

    /// Convert to a `Vec<ResourceAmount>` for wire serialization.
    pub fn to_resource_amounts(&self) -> Vec<ResourceAmount> {
        self.0
            .iter()
            .map(|(kind, &amount)| ResourceAmount {
                kind: kind.clone(),
                amount,
            })
            .collect()
    }

    /// Subtract all amounts in `other` from self (saturating).
    pub fn sub(&mut self, other: &ResourceMap) {
        for (kind, amount) in other.iter() {
            let entry = self.0.entry(kind.clone()).or_insert(0);
            *entry = entry.saturating_sub(amount);
        }
    }
}

impl<const N: usize> From<[(ResourceKind, u64); N]> for ResourceMap {
    fn from(arr: [(ResourceKind, u64); N]) -> Self {
        Self(BTreeMap::from(arr))
    }
}

impl FromIterator<(ResourceKind, u64)> for ResourceMap {
    fn from_iter<T: IntoIterator<Item = (ResourceKind, u64)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl fmt::Display for ResourceMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        write!(f, "{{")?;
        for (kind, amount) in self.iter() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{kind}: {amount}")?;
            first = false;
        }
        write!(f, "}}")
    }
}
