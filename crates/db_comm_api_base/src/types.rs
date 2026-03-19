use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub type WorkerId = u32;

/// A kind of resource that can be scheduled and monitored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum ResourceKind {
    Memory,
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceKind::Memory => write!(f, "memory"),
        }
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

    pub fn get(&self, kind: ResourceKind) -> u64 {
        self.0.get(&kind).copied().unwrap_or(0)
    }

    pub fn insert(&mut self, kind: ResourceKind, amount: u64) {
        self.0.insert(kind, amount);
    }

    pub fn contains_key(&self, kind: ResourceKind) -> bool {
        self.0.contains_key(&kind)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ResourceKind, u64)> + '_ {
        self.0.iter().map(|(&k, &v)| (k, v))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Add all amounts from `other` to self.
    pub fn add(&mut self, other: &ResourceMap) {
        for (kind, amount) in other.iter() {
            *self.0.entry(kind).or_insert(0) += amount;
        }
    }

    /// Convert to a `Vec<ResourceAmount>` for wire serialization.
    pub fn to_resource_amounts(&self) -> Vec<ResourceAmount> {
        self.0.iter().map(|(&kind, &amount)| ResourceAmount { kind, amount }).collect()
    }

    /// Subtract all amounts in `other` from self (saturating).
    pub fn sub(&mut self, other: &ResourceMap) {
        for (kind, amount) in other.iter() {
            let entry = self.0.entry(kind).or_insert(0);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    ResourceExhausted(ResourceKind),
    NonRecoverable,
    Recoverable,
}

impl ErrorType {
    pub fn wire_value(&self) -> &'static str {
        match self {
            ErrorType::ResourceExhausted(ResourceKind::Memory) => "oom",
            ErrorType::NonRecoverable => "non_recoverable",
            ErrorType::Recoverable => "recoverable",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "oom" | "resource_exhausted:memory" => {
                Some(ErrorType::ResourceExhausted(ResourceKind::Memory))
            }
            "non_recoverable" => Some(ErrorType::NonRecoverable),
            "recoverable" => Some(ErrorType::Recoverable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub success: bool,
    pub error_type: Option<ErrorType>,
    pub error_message: Option<String>,
    pub warnings: u32,
    pub filtered: u32,
}

impl TaskResult {
    pub fn ok(warnings: u32, filtered: u32) -> Self {
        Self {
            success: true,
            error_type: None,
            error_message: None,
            warnings,
            filtered,
        }
    }

    pub fn error(error_type: ErrorType, message: String) -> Self {
        Self {
            success: false,
            error_type: Some(error_type),
            error_message: Some(message),
            warnings: 0,
            filtered: 0,
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
mod tests {
    use super::*;

    /// Minimal test identifier for unit tests.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[test]
    fn error_type_wire_roundtrip() {
        for et in [
            ErrorType::ResourceExhausted(ResourceKind::Memory),
            ErrorType::NonRecoverable,
            ErrorType::Recoverable,
        ] {
            let wire = et.wire_value();
            let parsed = ErrorType::from_wire(wire).unwrap();
            assert_eq!(et, parsed);
        }
    }

    #[test]
    fn error_type_from_wire_unknown_returns_none() {
        assert!(ErrorType::from_wire("garbage").is_none());
    }

    #[test]
    fn error_type_from_wire_forward_compat() {
        let parsed = ErrorType::from_wire("resource_exhausted:memory").unwrap();
        assert_eq!(parsed, ErrorType::ResourceExhausted(ResourceKind::Memory));
    }

    #[test]
    fn task_result_ok() {
        let r = TaskResult::ok(3, 5);
        assert!(r.success);
        assert!(r.error_type.is_none());
        assert_eq!(r.warnings, 3);
        assert_eq!(r.filtered, 5);
    }

    #[test]
    fn task_result_error() {
        let r = TaskResult::error(
            ErrorType::ResourceExhausted(ResourceKind::Memory),
            "out of memory".into(),
        );
        assert!(!r.success);
        assert_eq!(
            r.error_type,
            Some(ErrorType::ResourceExhausted(ResourceKind::Memory))
        );
    }

    #[test]
    fn binary_info_generic() {
        let bi = BinaryInfo {
            path: PathBuf::from("/tmp/test"),
            size: 1024,
            identifier: TestId("test-binary".into()),
        };
        assert_eq!(bi.size, 1024);
        assert_eq!(bi.identifier.0, "test-binary");
    }

    #[test]
    fn resource_map_basic() {
        let mut map = ResourceMap::new();
        assert!(map.is_empty());
        assert_eq!(map.get(ResourceKind::Memory), 0);

        map.insert(ResourceKind::Memory, 1024);
        assert!(!map.is_empty());
        assert_eq!(map.get(ResourceKind::Memory), 1024);
        assert!(map.contains_key(ResourceKind::Memory));
    }

    #[test]
    fn resource_map_from_array() {
        let map = ResourceMap::from([(ResourceKind::Memory, 2048)]);
        assert_eq!(map.get(ResourceKind::Memory), 2048);
    }

    #[test]
    fn resource_map_display() {
        let map = ResourceMap::from([(ResourceKind::Memory, 1024)]);
        assert_eq!(format!("{map}"), "{memory: 1024}");
    }
}
