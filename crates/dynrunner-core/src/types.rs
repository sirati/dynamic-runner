use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type WorkerId = u32;

/// A kind of resource that can be scheduled and monitored.
///
/// Opaque string newtype: Rust treats every kind interchangeably and never
/// privileges any particular name. The set of valid kinds is decided by the
/// task definition (typically Python registers `"memory"` etc. as it goes).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ResourceKind(Arc<str>);

impl ResourceKind {
    pub fn new<S: Into<Arc<str>>>(name: S) -> Self {
        Self(name.into())
    }

    /// Convenience constructor for the conventional memory kind.
    pub fn memory() -> Self {
        Self::new("memory")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ResourceKind {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for ResourceKind {
    fn from(s: String) -> Self {
        Self::new(s)
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

/// Trait alias for types that can serve as a binary identifier.
///
/// Any type implementing these bounds can be used as the identifier
/// in `BinaryInfo<I>`. The concrete identifier (e.g. with fields like
/// `binary_name`, `platform`, `compiler`, etc.) is defined by the
/// task-specific crate (e.g. `db_python_provider`).
pub trait Identifier:
    Clone + Debug + Hash + Eq + Serialize + for<'de> Deserialize<'de> + Send + 'static
{
}

impl<T> Identifier for T where
    T: Clone + Debug + Hash + Eq + Serialize + for<'de> Deserialize<'de> + Send + 'static
{
}

/// String-based runner identifier.
///
/// Task definitions Python-side compose a stable, unique string per work item
/// (e.g. `"<binary_name>/<platform>/<compiler>/<version>/<opt_level>"` for the
/// tokenizer task) and pass it through Rust as an `Arc<str>`. The runtime
/// uses string equality for identity — no hashing collisions, no opaque
/// `PyObject` round-tripping. The Python wrapper layer caches the dataclass
/// instances so Rust→Python returns can be translated back to typed objects.
pub type RunnerIdentifier = std::sync::Arc<str>;

/// Information about a binary to be processed.
///
/// Generic over the identifier type `I` so different task definitions
/// can use different identifier structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct BinaryInfo<I> {
    pub path: PathBuf,
    pub size: u64,
    pub identifier: I,
}

pub type TaskInput<I> = BinaryInfo<I>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    ResourceExhausted(ResourceKind),
    NonRecoverable,
    Recoverable,
}

impl ErrorType {
    /// Wire encoding (owned string because `ResourceExhausted` carries a
    /// task-defined kind name we have to interpolate). The legacy `oom`
    /// shorthand is preserved for the conventional `"memory"` kind.
    pub fn wire_value(&self) -> String {
        match self {
            ErrorType::ResourceExhausted(kind) if kind.as_str() == "memory" => "oom".into(),
            ErrorType::ResourceExhausted(kind) => format!("resource_exhausted:{kind}"),
            ErrorType::NonRecoverable => "non_recoverable".into(),
            ErrorType::Recoverable => "recoverable".into(),
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        if s == "oom" {
            return Some(ErrorType::ResourceExhausted(ResourceKind::memory()));
        }
        if let Some(kind) = s.strip_prefix("resource_exhausted:") {
            return Some(ErrorType::ResourceExhausted(ResourceKind::new(kind)));
        }
        match s {
            "non_recoverable" => Some(ErrorType::NonRecoverable),
            "recoverable" => Some(ErrorType::Recoverable),
            _ => None,
        }
    }
}

/// Outcome of one worker task. Generic over `R` so a task definition can
/// attach a typed payload (e.g. tokenizer counts, GPU profile data) — the
/// runner itself never inspects `R`.
#[derive(Debug, Clone)]
pub struct TaskResult<R = ()> {
    pub success: bool,
    pub error_type: Option<ErrorType>,
    pub error_message: Option<String>,
    /// Task-defined payload returned on success. `None` on failure or for
    /// tasks that don't produce a payload.
    pub result: Option<R>,
}

impl<R> TaskResult<R> {
    /// Successful completion with no payload.
    pub fn ok() -> Self {
        Self {
            success: true,
            error_type: None,
            error_message: None,
            result: None,
        }
    }

    /// Successful completion carrying a typed payload.
    pub fn ok_with(result: R) -> Self {
        Self {
            success: true,
            error_type: None,
            error_message: None,
            result: Some(result),
        }
    }

    pub fn error(error_type: ErrorType, message: String) -> Self {
        Self {
            success: false,
            error_type: Some(error_type),
            error_message: Some(message),
            result: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FailedTask<I> {
    pub binary: BinaryInfo<I>,
    pub error_type: ErrorType,
    pub error_message: String,
    pub retry_count: u32,
}



#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
