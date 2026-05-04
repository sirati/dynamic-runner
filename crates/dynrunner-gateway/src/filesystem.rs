//! Read-only filesystem listing abstraction.
//!
//! Separate from [`Gateway`](crate::Gateway) because traversal is a different
//! concern from command execution + file transfer. Backends (local,
//! SSH-via-`find`, future object stores) implement [`Filesystem`]; the
//! `dynrunner-discovery` crate consumes it.

use std::future::Future;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirEntry {
    /// Regular file (or symlink resolving to one).
    File { name: String, size: u64 },
    /// Directory (or symlink resolving to one).
    Dir { name: String },
}

impl DirEntry {
    pub fn name(&self) -> &str {
        match self {
            DirEntry::File { name, .. } | DirEntry::Dir { name } => name.as_str(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("not connected")]
    NotConnected,
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("not a directory: {0}")]
    NotADirectory(String),
    #[error("listing failed: {0}")]
    ListingFailed(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Read-only directory listing.
///
/// Implementations resolve symlinks (a symlink to a regular file appears
/// as [`DirEntry::File`], a symlink to a directory as [`DirEntry::Dir`]),
/// include hidden entries, skip broken symlinks silently, and return
/// entries sorted alphabetically by name.
pub trait Filesystem: Send + Sync {
    fn list_dir(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<DirEntry>, FsError>> + Send;
}
