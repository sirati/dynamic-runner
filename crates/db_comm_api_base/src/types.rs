use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub type WorkerId = u32;
pub type MemoryBytes = u64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    OutOfMemory,
    NonRecoverable,
    Recoverable,
}

impl ErrorType {
    pub fn wire_value(&self) -> &'static str {
        match self {
            ErrorType::OutOfMemory => "oom",
            ErrorType::NonRecoverable => "non_recoverable",
            ErrorType::Recoverable => "recoverable",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "oom" => Some(ErrorType::OutOfMemory),
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
            ErrorType::OutOfMemory,
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
    fn task_result_ok() {
        let r = TaskResult::ok(3, 5);
        assert!(r.success);
        assert!(r.error_type.is_none());
        assert_eq!(r.warnings, 3);
        assert_eq!(r.filtered, 5);
    }

    #[test]
    fn task_result_error() {
        let r = TaskResult::error(ErrorType::OutOfMemory, "out of memory".into());
        assert!(!r.success);
        assert_eq!(r.error_type, Some(ErrorType::OutOfMemory));
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
}
