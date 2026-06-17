//! PyO3 type wrappers crossing the Rust <-> Python FFI boundary.
//!
//! Each sub-file owns one concern:
//!   - [`path_str`] — `PyPathStr`: accept `str | os.PathLike` in pyclass
//!     fields without forcing every config to know about path coercion.
//!   - [`identifier`] — `PyBinaryIdentifier` + the 5-field key encoding
//!     (`join_identifier`/`split_identifier`) + the Python-object
//!     resolver (`identifier_from_pyobj`).
//!   - [`task_info`] — `PyTaskInfo` + the From-impls that cross the
//!     `&PyTaskInfo <-> TaskInfo<RunnerIdentifier>` boundary, plus the
//!     pure-Rust round-trip tests that exercise them.
//!   - [`task_view`] — `PyTaskInfoView` read-only view passed to
//!     consumer-installed fulfillability matchers.
//!   - [`results`] — `PyProcessingStats` + `PyFailedTask` result types.
//!   - [`extract`] — `extract_binaries` / `task_to_pytask` bridge
//!     helpers that walk a `PyList` of Python `TaskInfo`-shaped objects
//!     and produce the Rust-side `Vec<TaskInfo<_>>`.
//!   - [`upload_root`] — `PyUploadRoot`: the FFI surface of the framework
//!     mount-root selector (`dynrunner_core::UploadRoot`) a consumer uses
//!     to choose WHICH framework mount an attached file uploads under.

mod extract;
mod identifier;
mod path_str;
mod results;
mod task_info;
mod task_view;
mod upload_root;

pub(crate) use extract::{extract_binaries, task_to_pytask};
pub(crate) use identifier::{PyBinaryIdentifier, identifier_from_pyobj};
pub(crate) use path_str::PyPathStr;
pub(crate) use results::{PyFailedTask, PyProcessingStats};
pub(crate) use task_info::PyTaskInfo;
pub(crate) use task_view::PyTaskInfoView;
pub(crate) use upload_root::PyUploadRoot;
