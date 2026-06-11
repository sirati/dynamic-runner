//! Module-internal tests for `job_manager`, split by concern.
//!
//! - [`legacy`] — the pre-existing image-staging and `submit_job`
//!   regression tests (one big flat file, kept verbatim to avoid
//!   semantic drift during the directory split).
//! - [`binary_upload`] — exercises the shared hash-conditional gate
//!   (`upload_binary_hash_conditional`): a matching remote hash skips
//!   the transfer; a different hash or an absent remote transfers.
//! - [`shutdown_binary`] — exercises
//!   [`SlurmJobManager::upload_shutdown_manager_binary_from`] across
//!   its two documented branches (missing source → error; present
//!   source → transfer_file + chmod 755 + remote path recorded on
//!   the manager) plus the manager → wrapper-renderer wiring.
//! - [`important_events`] — pins that the image build/transfer step
//!   emits its building-image + uploading-image events on the
//!   `crate::IMPORTANT_TARGET` tracing marker.
//! - [`cancel_verify`] — pins the post-`scancel` verification sweep in
//!   `cancel_all_jobs`: a job that scancel exits 0 on but stays in the
//!   queue (the run_20260611_182745 race) is re-scancelled and, if it
//!   outlives the budget, WARN-flagged with its id.

mod binary_upload;
mod cancel_verify;
mod important_events;
mod legacy;
mod shutdown_binary;
