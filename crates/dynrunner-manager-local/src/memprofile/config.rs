//! Sampler config.

use std::path::PathBuf;
use std::time::Duration;

/// Configuration for [`super::MemProfileSampler`].
///
/// `output_dir` is the run-level directory (e.g.
/// `{python.output_dir}/memprofile/`). Per-task files are constructed
/// as `{output_dir}/{task_id}.worker-{N}.memprofile.jsonl.zst`, where
/// `task_id` may contain slashes for asm-tokenizer — the writer's
/// `create_dir_all` materialises the nested parents.
///
/// `sample_interval` defaults to `Duration::from_secs(1)` per the
/// peer-confirmed cadence; exposed for tests that need a tighter loop.
#[derive(Debug, Clone)]
pub struct MemProfileConfig {
    pub output_dir: PathBuf,
    pub sample_interval: Duration,
}

impl MemProfileConfig {
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            sample_interval: Duration::from_secs(1),
        }
    }
}
