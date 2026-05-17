//! Module-internal tests for `job_manager`, split by concern.
//!
//! - [`legacy`] — the pre-existing image-staging and `submit_job`
//!   regression tests (one big flat file, kept verbatim to avoid
//!   semantic drift during the directory split).
//! - [`shutdown_binary`] — exercises
//!   [`SlurmJobManager::upload_shutdown_manager_binary`] across the
//!   three documented branches (env var unset → skip; set + missing
//!   → error; set + present → transfer_file + chmod 755 + remote
//!   path recorded on the manager).

mod legacy;
mod shutdown_binary;
