//! Module-internal tests for `job_manager`, split by concern.
//!
//! - [`legacy`] — the pre-existing image-staging and `submit_job`
//!   regression tests (one big flat file, kept verbatim to avoid
//!   semantic drift during the directory split).
//! - [`shutdown_binary`] — exercises
//!   [`SlurmJobManager::upload_shutdown_manager_binary_from`] across
//!   its two documented branches (missing source → error; present
//!   source → transfer_file + chmod 755 + remote path recorded on
//!   the manager) plus the manager → wrapper-renderer wiring.

mod legacy;
mod shutdown_binary;
