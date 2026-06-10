use std::collections::BTreeMap;

use dynrunner_core::{ErrorType, TaskOutputs};

/// API-level hard cap on one custom-message payload (`data`), in
/// bytes — 100 KiB, both directions (`Response::Custom` worker →
/// secondary and `Command::Custom` secondary → worker).
///
/// Enforced at BOTH consumer call sites with an error naming the
/// actual size and this limit (Python `Task.send_message` and
/// `SecondaryHandle.send_to_worker` raise `ValueError`; the Rust
/// send chokepoints reject before framing) and merely TOLERATED by
/// the wire: the framed b64-JSON line for a max-size payload is
/// well under [`crate::framing::MAX_RESPONSE_FRAME_BYTES`], so a
/// frame that slips past the API check can never wedge the
/// manager-side reader (#364 class). Exported to Python through
/// `dynamic_runner._native.CUSTOM_MESSAGE_MAX_BYTES`, mirroring
/// `PUBLISH_STRING_MAX_BYTES`.
pub const CUSTOM_MESSAGE_MAX_BYTES: usize = 100 * 1024;

#[derive(Debug, Clone)]
pub enum Command {
    Stop,
    /// Dispatch a task to the worker.
    ///
    /// `relative_path` is the worker-facing identifier the framework
    /// passes through verbatim — for file-based tasks it's a real
    /// path the worker opens; for `uses_file_based_items=False` tasks
    /// (FR-2) it's an opaque key the worker resolves however it
    /// wants.
    ///
    /// `payload` is the opaque per-item data the consumer attached to
    /// the original `TaskInfo.payload` (JSON value, serialised as a
    /// string for transport). `None` means "the task carries no
    /// payload"; the wire then collapses to the historical
    /// `<path>\n` line. `Some(json)` ships the wrapped form so a
    /// worker that opted into payload data (FR-3 use case) can
    /// process the task without an additional filesystem read.
    ///
    /// `resolved_path` is the locally-resolved absolute on-disk
    /// location of the file when the secondary's extraction cache /
    /// pre-staged shared mount placed it somewhere other than
    /// `<source-dir>/<relative_path>`. `None` means "the worker
    /// should open `relative_path` against its configured source
    /// dir as before". `Some(p)` means "the file lives at `p`;
    /// `relative_path` remains the wire-supplied identifier used
    /// for output-tree mirroring". Decouples the two concerns that
    /// pre-fix collided in `relative_path` — see TaskInfo's
    /// `resolved_path` doc-comment for the why.
    ///
    /// `predecessor_outputs` carries the keyed outputs each
    /// declared predecessor committed (keyed by predecessor
    /// `task_id`). Empty map means "no predecessor outputs
    /// attached" — the historical case for tasks with no deps or
    /// deps whose producers committed nothing. On the wire the
    /// field is omitted entirely when empty, preserving byte-
    /// identical bare-path form for legacy tasks.
    ProcessTask {
        relative_path: String,
        payload: Option<String>,
        resolved_path: Option<String>,
        predecessor_outputs: BTreeMap<String, TaskOutputs>,
    },
    /// Secondary → worker consumer custom message.
    ///
    /// `topic` is a consumer routing key the framework never
    /// interprets; `data` is the opaque payload (≤
    /// [`CUSTOM_MESSAGE_MAX_BYTES`], enforced at the API call sites,
    /// tolerated by the wire). Legal while the worker is `Idle` AND
    /// while it is `Processing` (the unix socket is full-duplex;
    /// bytes buffer until the worker polls) — a typed allowance on
    /// `RunnerProtocol`, see `state.rs::send_custom`. Delivery to
    /// consumer code is explicit-poll: `Task.poll_messages()` drains
    /// buffered frames mid-task; between tasks queued customs route
    /// to the optional module-level `@message_handler`.
    Custom { topic: String, data: Vec<u8> },
}

#[derive(Debug, Clone)]
pub enum Response {
    Ready,
    Done {
        result_data: Option<Vec<u8>>,
    },
    Error {
        error_type: ErrorType,
        message: String,
    },
    /// Worker reported an exception with full type + message +
    /// traceback (formerly `PickledError`). Carries plain strings —
    /// no Python objects are deserialised.
    ///
    /// `error_type` is optional for wire backwards compatibility. The
    /// historical wire form (a worker-process-internal exception with
    /// no category) maps to `None`, which the runner treats as
    /// `NonRecoverable` (worker process is presumed corrupt → restart
    /// signal). Consumers that catch user-task exceptions and want
    /// to surface the traceback WITHOUT triggering a restart now set
    /// `error_type = Some(ErrorType::Recoverable)`; the runner then
    /// treats the failure exactly like a `Response::Error` of the
    /// same category, except `error_message` is enriched with the
    /// formatted traceback (`"{exception_type}: {message}\n{traceback}"`).
    /// `Some(NonRecoverable)` is identical to `None`; `Some(Oom)`
    /// behaves like `Response::Error { error_type: Oom, .. }`.
    WorkerException {
        exception_type: String,
        message: String,
        traceback: String,
        error_type: Option<ErrorType>,
    },
    PhaseUpdate {
        phase_name: String,
    },
    Keepalive,
    /// Worker → secondary consumer custom message (the streamed-spawn
    /// hot path: a worker streams descriptor batches / progress pings
    /// mid-task without waiting for the terminal `done`).
    ///
    /// `topic` is a consumer routing key the framework never
    /// interprets; `data` is the opaque payload (≤
    /// [`CUSTOM_MESSAGE_MAX_BYTES`], enforced at `Task.send_message`,
    /// tolerated by the wire). A NON-TERMINAL response: the manager
    /// FSM classifies it alongside `Keepalive`/`PhaseUpdate`
    /// (`PollResult::StillRunning`), so mid-task emission can never
    /// perturb terminal attribution (#364 class).
    Custom { topic: String, data: Vec<u8> },
}
