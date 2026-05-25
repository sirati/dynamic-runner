//! Sampler errors.
//!
//! SIBLING-DEP: this is a minimal stub introduced by the writer
//! work; the full enum (`Parse`, additional variants, exhaustive
//! conversions) lands with the cgroup-reader / sampler work. The
//! coordinator reconciles on merge by keeping the richer definition.
//! Only the public constructors `MemProfileError::io` and
//! `MemProfileError::parse` are load-bearing for callers — anything
//! the sibling work adds must preserve those two entry points.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum MemProfileError {
    #[error("memprofile io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("memprofile parse error at {path}{}: {detail}", line.map(|l| format!(":{l}")).unwrap_or_default())]
    Parse {
        path: PathBuf,
        line: Option<usize>,
        detail: String,
    },
}

impl MemProfileError {
    pub fn io(path: PathBuf, source: std::io::Error) -> Self {
        Self::Io { path, source }
    }

    pub fn parse(path: PathBuf, line: Option<usize>, detail: impl Into<String>) -> Self {
        Self::Parse {
            path,
            line,
            detail: detail.into(),
        }
    }
}
