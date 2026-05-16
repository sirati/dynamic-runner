//! Error/result types attached to a task outcome: [`ErrorType`], the typed
//! payload-bearing [`TaskResult`], and the bookkeeping [`FailedTask`] record.

use serde::{Deserialize, Serialize};

use crate::bounded_string::BoundedString;

use super::identifiers::ResourceKind;
use super::task::TaskInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    ResourceExhausted(ResourceKind),
    NonRecoverable,
    Recoverable,
    /// A task cannot run right now because a required resource (e.g. a
    /// toolchain outpath) is not held by any cluster peer. Semantically
    /// a sub-class of `NonRecoverable` from the secondary's perspective:
    /// the worker cannot make progress without the resource. Distinct
    /// from `NonRecoverable` because the failure is *reinjectable* once
    /// the resource reappears — future scheduler logic uses this tag to
    /// move the task back to a pending state instead of terminal-fail.
    /// `reason` is a free-form diagnostic capped at 2 KiB so a buggy or
    /// malicious peer cannot inflate per-message memory cost.
    Unfulfillable { reason: BoundedString<2048> },
}

impl ErrorType {
    /// Wire encoding (owned string because `ResourceExhausted` carries a
    /// task-defined kind name we have to interpolate). The legacy `oom`
    /// shorthand is preserved for the conventional `"memory"` kind.
    ///
    /// `Unfulfillable` uses an `unfulfillable:<reason>` tag. The reason
    /// is already capped at 2 KiB by `BoundedString<2048>`; no further
    /// escaping is applied because the text codec terminates frames on
    /// `\n` and the reason is the last field — newlines in `reason`
    /// would break framing. Callers must ensure reasons contain no
    /// newlines (the text codec is line-oriented; the richer JSON codec
    /// in `protocol-primary-secondary` has no such restriction).
    pub fn wire_value(&self) -> String {
        match self {
            ErrorType::ResourceExhausted(kind) if kind.as_str() == "memory" => "oom".into(),
            ErrorType::ResourceExhausted(kind) => format!("resource_exhausted:{kind}"),
            ErrorType::NonRecoverable => "non_recoverable".into(),
            ErrorType::Recoverable => "recoverable".into(),
            ErrorType::Unfulfillable { reason } => format!("unfulfillable:{reason}"),
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        if s == "oom" {
            return Some(ErrorType::ResourceExhausted(ResourceKind::memory()));
        }
        if let Some(kind) = s.strip_prefix("resource_exhausted:") {
            return Some(ErrorType::ResourceExhausted(ResourceKind::new(kind)));
        }
        if let Some(reason) = s.strip_prefix("unfulfillable:") {
            return Some(ErrorType::Unfulfillable {
                reason: reason.to_owned().into(),
            });
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
    pub binary: TaskInfo<I>,
    pub error_type: ErrorType,
    pub error_message: String,
    pub retry_count: u32,
}
