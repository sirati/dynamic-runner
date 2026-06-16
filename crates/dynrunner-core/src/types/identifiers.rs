//! Opaque `Arc<str>` newtype identifiers plus the [`Identifier`] trait alias.
//!
//! Each identifier is just a thin newtype around `Arc<str>` so the same id can
//! be cheaply shared across thousands of items. The framework treats every
//! value interchangeably; the set of valid names is decided by the task
//! definition (typically Python registers them as it goes).

use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

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

/// Opaque identifier for one phase declared by a TaskDefinition.
///
/// Phases group items that share an ordering barrier; the framework
/// gates dispatch at phase boundaries based on `PhaseSpec.depends_on`.
/// `PhaseId` is just a thin newtype around `Arc<str>` so the same
/// id can be cheaply shared across thousands of items.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PhaseId(Arc<str>);

impl PhaseId {
    pub fn new<S: Into<Arc<str>>>(name: S) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PhaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for PhaseId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

/// `Default` is the empty `PhaseId` — used exclusively as the
/// `TaskDep` migration sentinel: a legacy un-phased dep decodes to this
/// value and the snapshot-restore migration shim replaces it with the
/// enclosing task's real phase. It is never a valid runtime phase (a
/// real phase is non-empty), so it is safe as the "needs migration"
/// marker.
impl Default for PhaseId {
    fn default() -> Self {
        Self::new("")
    }
}

impl From<String> for PhaseId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Opaque identifier for one task type within a phase.
///
/// A `TaskTypeSpec` binds a `TypeId` to a worker entry-point and
/// per-type memory estimator; the framework dispatches items to the
/// worker matching their `type_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct TypeId(Arc<str>);

impl TypeId {
    pub fn new<S: Into<Arc<str>>>(name: S) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TypeId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for TypeId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Opaque identifier for a soft worker-pinning class.
///
/// Items sharing an `AffinityId` prefer the same worker so kernel
/// page-cache reuse is realized; pinning is soft — a worker never
/// refuses work to stay busy when its bucket is empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct AffinityId(Arc<str>);

impl AffinityId {
    pub fn new<S: Into<Arc<str>>>(name: S) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AffinityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for AffinityId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for AffinityId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Trait alias for types that can serve as a binary identifier.
///
/// Any type implementing these bounds can be used as the identifier
/// in `TaskInfo<I>`. The concrete identifier (e.g. with fields like
/// `binary_name`, `platform`, `compiler`, etc.) is defined by the
/// task-specific crate (e.g. `dynrunner_pyo3`).
pub trait Identifier:
    Clone + Debug + Hash + Eq + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static
{
}

impl<T> Identifier for T where
    T: Clone + Debug + Hash + Eq + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static
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
