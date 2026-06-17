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
    Unfulfillable {
        reason: BoundedString<2048>,
    },
    /// A task is structurally invalid and can NEVER run — e.g. it names
    /// a dependency that does not exist in the run, or a duplicate
    /// `(phase_id, task_id)` was submitted. Unlike `Unfulfillable`
    /// (which is *reinjectable* once a resource reappears) this is a
    /// terminal, NON-reinjectable failure: no future cluster state makes
    /// the task runnable. `reason` is a free-form diagnostic capped at
    /// 2 KiB (same bound + same no-newline framing contract as
    /// `Unfulfillable`, see `wire_value` below) so a buggy or malicious
    /// peer cannot inflate per-message memory cost.
    InvalidTask {
        reason: BoundedString<2048>,
    },
}

/// How a failed task's [`ErrorType`] routes through the failure
/// pipeline — the SINGLE classification of an error's permanence, owned
/// here alongside `ErrorType` so no consumer re-derives it with an ad-hoc
/// `match`. The three callers (the live wire-failure path, the CRDT-apply
/// mirror, and the audit/forward path) route on this verb instead of each
/// carving out their own carve-outs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    /// The failure MIGHT clear on a re-run: the per-phase retry buckets
    /// decide (`Recoverable` → error-retry bucket; memory
    /// `ResourceExhausted` → OOM bucket). Routes to the soft / retry-
    /// pending path; permanence is decided later at the phase drain edge.
    Retryable,
    /// The failure can NEVER clear on a re-run — it is terminal NOW.
    /// `NonRecoverable`, `InvalidTask` (structurally invalid, see the
    /// variant doc), and a NON-memory `ResourceExhausted` (no retry
    /// bucket accepts it, so a soft marker would only wedge it) all land
    /// here. Routes to the immediate-permanence path: into `failed_tasks`
    /// with an immediate dependent cascade.
    Permanent,
    /// The failure is operator-reinjectable: `Unfulfillable` — the task
    /// could not run because a required resource is held by no peer, but
    /// becomes runnable again if that resource reappears. Routes to the
    /// dormancy path (`on_item_finished(None)`); its dependents stay
    /// Blocked awaiting the reinject, NOT cascaded.
    Reinjectable,
}

impl ErrorType {
    /// Classify this error by PERMANENCE — the one place the failure
    /// pipeline's routing decision is derived. See [`RetryClass`].
    ///
    /// A NON-memory `ResourceExhausted` is `Permanent`, not `Retryable`:
    /// only the memory kind has a retry bucket (the OOM bucket), so any
    /// other resource kind has no reinjection path and a soft marker would
    /// merely hold its dependents hostage at the drain edge forever.
    pub fn retry_class(&self) -> RetryClass {
        match self {
            ErrorType::Recoverable => RetryClass::Retryable,
            ErrorType::ResourceExhausted(kind) if *kind == ResourceKind::memory() => {
                RetryClass::Retryable
            }
            ErrorType::ResourceExhausted(_)
            | ErrorType::NonRecoverable
            | ErrorType::InvalidTask { .. } => RetryClass::Permanent,
            ErrorType::Unfulfillable { .. } => RetryClass::Reinjectable,
        }
    }

    /// Wire encoding (owned string because `ResourceExhausted` carries a
    /// task-defined kind name we have to interpolate). The legacy `oom`
    /// shorthand is preserved for the conventional `"memory"` kind.
    ///
    /// `Unfulfillable` uses an `unfulfillable:<reason>` tag and
    /// `InvalidTask` an `invalid_task:<reason>` tag. The reason is
    /// already capped at 2 KiB by `BoundedString<2048>`; no further
    /// escaping is applied because the text codec terminates frames on
    /// `\n` and the reason is the last field — newlines in `reason`
    /// would break framing. Callers must ensure reasons contain no
    /// newlines (the text codec is line-oriented; the richer JSON codec
    /// in `protocol-primary-secondary` has no such restriction). A colon
    /// inside `reason` is safe: `from_wire` strips only the tag prefix
    /// and keeps the remainder verbatim.
    pub fn wire_value(&self) -> String {
        match self {
            ErrorType::ResourceExhausted(kind) if kind.as_str() == "memory" => "oom".into(),
            ErrorType::ResourceExhausted(kind) => format!("resource_exhausted:{kind}"),
            ErrorType::NonRecoverable => "non_recoverable".into(),
            ErrorType::Recoverable => "recoverable".into(),
            ErrorType::Unfulfillable { reason } => format!("unfulfillable:{reason}"),
            ErrorType::InvalidTask { reason } => format!("invalid_task:{reason}"),
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
        if let Some(reason) = s.strip_prefix("invalid_task:") {
            return Some(ErrorType::InvalidTask {
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
