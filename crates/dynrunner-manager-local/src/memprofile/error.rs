//! Shared error type for the memprofile module.
//!
//! Single concern: a typed error the sampler can match on to decide
//! whether to drop a sample (parse / transient I/O) or escalate. Both
//! the cgroup-v2 sysfs reader and the zstd-framed JSONL writer return
//! this type so the sampler tick loop has one error surface to handle.
//!
//! Variants are intentionally minimal — new variants are added by the
//! submodule that needs them (e.g. the writer may add a compression
//! variant when zstd lands). Keeping the enum lean here means callers
//! don't grow stale match arms before the upstream code exists.
//!
//! Both variants carry the offending `PathBuf` so a `tracing::warn!`
//! line from the sampler can name the file the kernel served (or
//! failed to serve) without the caller having to thread the path
//! through itself.

use std::io;
use std::path::PathBuf;

/// Errors produced by the memprofile module.
#[derive(Debug, thiserror::Error)]
pub enum MemProfileError {
    /// An I/O syscall against a memprofile sysfs / output file failed.
    /// The wrapped `io::Error` carries the OS errno; `path` names the
    /// file we were trying to touch so the sampler can log it
    /// structurally.
    #[error("memprofile io at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// A memprofile file was read successfully but its contents did
    /// not parse. `line` is `Some(1-indexed)` for line-oriented files
    /// (`memory.stat`) and `None` for whole-file scalars
    /// (`memory.current`, `memory.swap.current`).
    #[error("memprofile parse at {path} (line {line:?}): {message}")]
    Parse {
        path: PathBuf,
        line: Option<usize>,
        message: String,
    },

    /// A sample could not be serialised to JSON on the write path.
    /// Separate from [`Self::Parse`] (which is read-side) so callers
    /// matching on the variant can distinguish "kernel handed us
    /// something we couldn't decode" from "we tried to encode our
    /// own struct and it failed".
    #[error("memprofile serialize at {path}: {message}")]
    Serialize { path: PathBuf, message: String },
}

impl MemProfileError {
    /// Construct an [`MemProfileError::Io`] from a path and the
    /// underlying `io::Error`. Crate-private so call sites stay
    /// inside the module.
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Construct an [`MemProfileError::Parse`] from a path, an
    /// optional 1-indexed line number, and a human-readable message.
    /// Crate-private for the same reason as [`Self::io`].
    pub(crate) fn parse(
        path: impl Into<PathBuf>,
        line: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        Self::Parse {
            path: path.into(),
            line,
            message: message.into(),
        }
    }

    /// Construct an [`MemProfileError::Serialize`] from a path and
    /// the serde-side failure message.
    pub(crate) fn serialize(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::Serialize {
            path: path.into(),
            message: message.into(),
        }
    }
}
