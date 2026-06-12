//! Module-internal tests for the SLURM preparation pipeline, split by
//! concern into sub-files. Each sub-file owns the test fixtures it
//! alone needs (e.g. `pipeline.rs`'s `LocalDirReader`,
//! `respawn.rs`'s `CannedUriReader`); only the truly shared
//! `opts_for` helper lives here.

use std::time::Duration;

use tempfile::TempDir;

use super::options::PreparationOptions;

mod cohort;
mod escalation;
mod establish;
mod gather;
mod pipeline;
mod respawn;
mod ssh;
mod store;
mod summary;
mod uri_parsing;

/// Construct a `PreparationOptions` with `setup_timeout` / `poll_interval`
/// shortened so the timeout tests don't hold the suite hostage. Shared
/// across the pipeline / establish / respawn test files.
pub(super) fn opts_for(tmp: &TempDir) -> PreparationOptions {
    let run_log_dir = tmp.path().display().to_string();
    let mut o = PreparationOptions::new(
        run_log_dir,
        "gateway.example".into(),
        Some("primary".into()),
        22,
        vec![],
        vec![],
    );
    o.setup_timeout = Duration::from_millis(1500);
    o.poll_interval = Duration::from_millis(20);
    o
}
