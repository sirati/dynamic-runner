//! Naming of the submitter's LOCAL per-run state dir — the single
//! owner of the `/tmp/db-runner-cert-<run_id>` convention.
//!
//! # Concern
//!
//! One concern: pure path derivation for the local (submitter-side)
//! run-state home. Two writers/readers share it:
//!
//! - the SLURM pipeline (`run_pipeline.rs`) creates the dir at run
//!   start and points the submitter primary's
//!   `PrimaryConfig.peer_credentials_path` into it;
//! - the late-joiner observer derives the SAME path from the
//!   `--observer-join-from-peer-info-dir` value (whose parent dir is
//!   named after the run id, `{base_log_dir}/{run_id}/connection_info`)
//!   to pick the persisted credentials up without a new operator flag.
//!
//! No filesystem access here — existence probing belongs to the
//! callers (the late-joiner treats an absent file as "no credentials,
//! WSS fallback", never an error).

use std::path::{Path, PathBuf};

/// The submitter's local per-run state dir: `/tmp/db-runner-cert-<run_id>`.
///
/// Historical name: the dir was introduced for run-local cert material
/// and is exactly where the run's mesh credentials belong — LOCAL
/// state, never the shared cluster-visible `connection_info/` dir.
pub(crate) fn cert_dir_for_run(run_id: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/db-runner-cert-{run_id}"))
}

/// The peer-credentials file inside a run's local cert dir — the
/// roster's cert pins persisted by the submitter primary
/// (`dynrunner_manager_distributed::peer_credentials`).
pub(crate) fn peer_credentials_path(cert_dir: &Path) -> PathBuf {
    cert_dir.join("peer_credentials.json")
}

/// Derive the run id from a `connection_info` dir path, per the
/// pipeline's `{base_log_dir}/{run_id}/connection_info` convention:
/// the PARENT dir's name, accepted only when it matches the
/// `run_YYYYMMDD_HHMMSS` shape `_make_run_id` produces (so an operator
/// pointing at some unrelated dir never makes the late-joiner probe a
/// bogus `/tmp` path).
pub(crate) fn derive_run_id_from_info_dir(info_dir: &Path) -> Option<String> {
    let parent = info_dir.parent()?.file_name()?.to_str()?;
    let digits = parent.strip_prefix("run_")?;
    // `run_` + 8 date digits + `_` + 6 time digits.
    let (date, time) = digits.split_once('_')?;
    if date.len() == 8
        && time.len() == 6
        && date.chars().all(|c| c.is_ascii_digit())
        && time.chars().all(|c| c.is_ascii_digit())
    {
        Some(parent.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pipeline-side and joiner-side derivations meet on the same
    /// file for the same run.
    #[test]
    fn info_dir_derivation_meets_pipeline_naming() {
        let info_dir = Path::new("/cluster/logs/run_20260611_200548/connection_info");
        let run_id = derive_run_id_from_info_dir(info_dir).expect("conventional path derives");
        assert_eq!(run_id, "run_20260611_200548");
        assert_eq!(
            peer_credentials_path(&cert_dir_for_run(&run_id)),
            PathBuf::from("/tmp/db-runner-cert-run_20260611_200548/peer_credentials.json")
        );
    }

    /// Unconventional paths derive nothing (no bogus /tmp probes).
    #[test]
    fn non_run_shaped_parents_derive_none() {
        for p in [
            "/data/some_dir/connection_info",
            "/data/run_x/connection_info",
            "/data/run_2026_0611/connection_info",
            "/data/run_20260611200548/connection_info", // missing the date_time split
            "/data/run_20260611_20054x/connection_info",
            "connection_info", // no parent name
        ] {
            assert_eq!(
                derive_run_id_from_info_dir(Path::new(p)),
                None,
                "path {p} must not derive a run id"
            );
        }
    }
}
