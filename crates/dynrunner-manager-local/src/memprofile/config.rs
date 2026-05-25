//! Sampler config.

use std::path::PathBuf;
use std::time::Duration;

/// Compile-time constant for the container-internal path the SLURM
/// wrapper bind-mounts the gateway-shared output filesystem into.
///
/// When `--memprofile` is set on a SLURM secondary WITHOUT an explicit
/// operator-supplied output override, the secondary uses this path so
/// the resulting `.jsonl.zst` files land on the gateway-shared output
/// drive (alongside the rest of the run's artifacts).
///
/// Source of truth for the bind-mount target itself lives in
/// `crates/dynrunner-slurm/src/wrapper_script/generate.rs` (search
/// for the `/app/out-network` literal in the `-v` block). Changing
/// either constant without the other breaks the artifact-retrieval
/// contract — the wrapper would bind-mount one path while the
/// secondary wrote to another.
pub const SLURM_SECONDARY_OUTPUT_DIR: &str = "/app/out-network";

/// Sub-directory the plumbing layer joins onto the operator's run
/// `output_dir` before passing it to the sampler. Lifted to a single
/// constant so both the local-mode resolver and the secondary-mode
/// resolver pin the same on-disk layout (`{output_dir}/memprofile/...`).
pub const MEMPROFILE_SUBDIR: &str = "memprofile";

/// Filename for the per-secondary aggregate memuse log appended to
/// at every task completion. Single source of truth so every
/// dispatch path (LocalManager direct, SLURM secondary, in-process
/// distributed secondary, multi-computer-local secondary) picks
/// the same filename without re-stating the literal at each call
/// site. See `crate::memuse::log_resource_usage` for the writer.
pub const MEMUSE_LOG_FILENAME: &str = "memuse.log";

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
