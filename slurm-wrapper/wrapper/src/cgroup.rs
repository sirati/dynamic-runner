//! Single concern: SLURM job-cgroup MEMBERSHIP — discover the cgroup the
//! wrapper itself lives in (the slurmstepd per-job cgroup) and pull a
//! child process (conmon) into it so SLURM's authoritative
//! `proctrack/cgroup` end-of-job sweep reaps that process as a matter of
//! course.
//!
//! Two primitives, matching the two sub-mechanisms of design §4(a):
//!
//!   * [`current_job_cgroup`] — parse `/proc/self/cgroup` (cgroup-v2 single
//!     line `0::<path>`) to the path of the wrapper's own cgroup. This is
//!     the value passed to podman `--cgroup-parent` (a1) AND the cgroup
//!     whose `cgroup.procs` we write for the adopt backstop (a2).
//!
//!   * [`adopt_into_self_cgroup`] — write a PID into the wrapper's OWN
//!     cgroup's `cgroup.procs`. This is the (a2) backstop: you can always
//!     move a process into a cgroup you are already a member of (no
//!     `mkdir`, no delegation needed), so conmon — which double-forked
//!     into `user.slice` — is pulled back into the job cgroup
//!     unconditionally, even on a non-delegated cluster (Krater) where
//!     `--cgroup-parent` (a1) cannot create a child cgroup.
//!
//! [`cgroup_parent_probe`] decides whether a1 is even attemptable: it
//! tries to `mkdir` + `rmdir` a throwaway child cgroup under the job
//! cgroup; success means cgroup-v2 delegation is present and podman can
//! create the container's cgroup beneath `--cgroup-parent`. On failure
//! (the no-delegation case) the caller omits the podman flags and relies
//! solely on the (a2) adopt + the in-band reap.
//!
//! Boundary: callers get a `CgroupPath` (the job-cgroup path string) and
//! call `adopt`/`probe`; they know nothing about `/proc` parsing or the
//! `/sys/fs/cgroup` layout. The `/proc` and `/sys/fs/cgroup` roots are
//! injectable so the parse + adopt are unit-testable against fixtures
//! (mirrors `signals.rs`'s `proc_base` pattern).

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::LOG_TARGET;

/// The default cgroup-v2 mount root. Injectable in tests.
const CGROUP_FS_ROOT: &str = "/sys/fs/cgroup";

/// The wrapper's own cgroup, as discovered from `/proc/self/cgroup`. The
/// inner value is the cgroup path WITHOUT the `/sys/fs/cgroup` prefix —
/// e.g. `/system.slice/slurmstepd.scope/job_153731/step_batch/...` — i.e.
/// exactly the cgroup-v2 `0::<path>` field. This is the form podman
/// `--cgroup-parent` expects, and joining it onto the cgroup-fs root
/// yields the filesystem directory whose `cgroup.procs` we write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupPath(pub String);

impl CgroupPath {
    /// The cgroup-v2 relative path string, suitable for podman
    /// `--cgroup-parent`.
    pub fn as_parent_arg(&self) -> &str {
        &self.0
    }

    /// The filesystem directory of this cgroup under `cgroup_fs_root`.
    fn dir_under(&self, cgroup_fs_root: &Path) -> PathBuf {
        // The path begins with `/`; strip it so `join` nests under the
        // fs root rather than resetting to the absolute path.
        let rel = self.0.trim_start_matches('/');
        cgroup_fs_root.join(rel)
    }
}

/// Parse the wrapper's own cgroup-v2 path from `proc_base/self/cgroup`.
/// Returns `None` when the file is missing, or has no cgroup-v2 (`0::`)
/// line (a pure cgroup-v1 host — neither (a1) nor (a2) applies there, the
/// in-band reap carries the load). PURE w.r.t. the injected base dir.
pub fn current_job_cgroup(proc_base: &Path) -> Option<CgroupPath> {
    let raw = std::fs::read_to_string(proc_base.join("self").join("cgroup")).ok()?;
    parse_cgroup_v2_line(&raw)
}

/// Production entry: read the real `/proc/self/cgroup`.
pub fn current_job_cgroup_real() -> Option<CgroupPath> {
    current_job_cgroup(Path::new("/proc"))
}

/// Extract the cgroup-v2 controller's path from the contents of a
/// `/proc/<pid>/cgroup` file. The cgroup-v2 entry is the line with an
/// empty hierarchy-id and empty controller list: `0::<path>`. PURE.
fn parse_cgroup_v2_line(contents: &str) -> Option<CgroupPath> {
    for line in contents.lines() {
        // Each line is `<hierarchy-id>:<controllers>:<path>`. The unified
        // (v2) hierarchy is `0::<path>` — hierarchy-id 0, empty
        // controllers. Split into at most 3 parts so a `:` inside the
        // path (legal) is preserved.
        let mut parts = line.splitn(3, ':');
        let hid = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        if hid == "0" && controllers.is_empty() {
            let trimmed = path.trim();
            if trimmed.is_empty() {
                return None;
            }
            return Some(CgroupPath(trimmed.to_string()));
        }
    }
    None
}

/// Write `pid` into the wrapper's OWN cgroup's `cgroup.procs`, moving it
/// into the job cgroup. This is the (a2) delegation-independent backstop:
/// moving a process into a cgroup you are already a member of needs no
/// `mkdir` and no delegation. PURE w.r.t. the injected cgroup-fs root.
///
/// Best-effort by contract at the call site (the in-band reap is the
/// guarantee), but the `io::Result` is surfaced so the caller can log the
/// outcome forensically.
pub fn adopt_into_cgroup(
    cgroup_fs_root: &Path,
    job_cgroup: &CgroupPath,
    pid: u32,
) -> std::io::Result<()> {
    let procs = job_cgroup.dir_under(cgroup_fs_root).join("cgroup.procs");
    // cgroup.procs adoption is a single `write(2)` of the decimal PID. It
    // must be a fresh open + write (append semantics are kernel-defined
    // for this file), so use `OpenOptions::append` to avoid truncation
    // surprises, then write the bare PID.
    let mut f = std::fs::OpenOptions::new().append(true).open(&procs)?;
    write!(f, "{}", pid)
}

/// Production adopt: write into the real `/sys/fs/cgroup` tree, logging
/// the outcome. Returns whether the adoption write succeeded.
pub fn adopt_into_self_cgroup(job_cgroup: &CgroupPath, pid: u32) -> bool {
    match adopt_into_cgroup(Path::new(CGROUP_FS_ROOT), job_cgroup, pid) {
        Ok(()) => {
            tracing::info!(
                target: LOG_TARGET,
                pid,
                cgroup = %job_cgroup.0,
                "adopted conmon into the wrapper's own (job) cgroup via cgroup.procs"
            );
            true
        }
        Err(e) => {
            tracing::warn!(
                target: LOG_TARGET,
                pid,
                cgroup = %job_cgroup.0,
                error = %e,
                "could not adopt conmon into the job cgroup (cgroup.procs write failed); \
                 relying on the in-band reap to kill it"
            );
            false
        }
    }
}

/// Probe whether podman's `--cgroup-parent` (a1) is attemptable: try to
/// create a throwaway child cgroup under the job cgroup and immediately
/// remove it. Success means cgroup-v2 delegation is present (the user can
/// `mkdir` under the job cgroup), so podman can create the container's
/// cgroup beneath `--cgroup-parent`. Failure (the Krater no-delegation
/// case) means a1 would fail outright, so the caller omits the flags and
/// relies on (a2) adopt + the in-band reap. PURE w.r.t. the injected
/// cgroup-fs root.
pub fn cgroup_parent_probe(cgroup_fs_root: &Path, job_cgroup: &CgroupPath) -> bool {
    let probe_dir = job_cgroup
        .dir_under(cgroup_fs_root)
        .join(".dynrunner-cgparent-probe");
    match std::fs::create_dir(&probe_dir) {
        Ok(()) => {
            // Clean up the throwaway child immediately; an empty cgroup
            // dir is removed with rmdir (std remove_dir), never recursive.
            let _ = std::fs::remove_dir(&probe_dir);
            true
        }
        Err(_) => false,
    }
}

/// Production probe against the real `/sys/fs/cgroup`.
pub fn cgroup_parent_probe_real(job_cgroup: &CgroupPath) -> bool {
    cgroup_parent_probe(Path::new(CGROUP_FS_ROOT), job_cgroup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_v2_unified_line() {
        let contents = "0::/system.slice/slurmstepd.scope/job_153731/step_batch\n";
        let cg = parse_cgroup_v2_line(contents).expect("v2 line must parse");
        assert_eq!(cg.0, "/system.slice/slurmstepd.scope/job_153731/step_batch");
        assert_eq!(
            cg.as_parent_arg(),
            "/system.slice/slurmstepd.scope/job_153731/step_batch"
        );
    }

    #[test]
    fn parses_v2_line_among_v1_lines() {
        // A hybrid host lists v1 controllers first, then the v2 unified
        // line. We must pick the `0::` line, not a v1 one.
        let contents = "\
12:pids:/user.slice\n\
3:memory:/user.slice/user-1000.slice\n\
0::/system.slice/slurmstepd.scope/job_42\n";
        let cg = parse_cgroup_v2_line(contents).expect("v2 line present");
        assert_eq!(cg.0, "/system.slice/slurmstepd.scope/job_42");
    }

    #[test]
    fn pure_v1_host_has_no_v2_line() {
        let contents = "\
12:pids:/user.slice\n\
3:memory:/user.slice\n";
        assert_eq!(parse_cgroup_v2_line(contents), None);
    }

    #[test]
    fn v2_root_path_is_some_root() {
        // The unified root is `0::/` — a valid (if unusual) cgroup path.
        let cg = parse_cgroup_v2_line("0::/\n").expect("root parses");
        assert_eq!(cg.0, "/");
    }

    #[test]
    fn current_job_cgroup_reads_injected_proc() {
        let tmp = tempfile::tempdir().unwrap();
        let proc = tmp.path();
        let self_dir = proc.join("self");
        std::fs::create_dir_all(&self_dir).unwrap();
        std::fs::write(self_dir.join("cgroup"), b"0::/system.slice/job_7\n").unwrap();
        let cg = current_job_cgroup(proc).expect("must parse injected proc");
        assert_eq!(cg.0, "/system.slice/job_7");
    }

    #[test]
    fn current_job_cgroup_missing_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(current_job_cgroup(tmp.path()), None);
    }

    #[test]
    fn adopt_writes_pid_into_self_cgroup_procs() {
        // Fabricate a cgroup-fs tree: <root>/<job-cgroup>/cgroup.procs.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let job = CgroupPath("/system.slice/job_7".to_string());
        let dir = root.join("system.slice/job_7");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cgroup.procs"), b"").unwrap();

        adopt_into_cgroup(root, &job, 4242).expect("adopt write must succeed");
        let written = std::fs::read_to_string(dir.join("cgroup.procs")).unwrap();
        assert_eq!(written, "4242", "the bare PID must be written to cgroup.procs");
    }

    #[test]
    fn adopt_missing_procs_file_is_err() {
        // No cgroup.procs present → the open fails → Err surfaced so the
        // caller logs and falls back to the in-band reap.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let job = CgroupPath("/system.slice/job_7".to_string());
        // Create the dir but NOT the cgroup.procs file.
        std::fs::create_dir_all(root.join("system.slice/job_7")).unwrap();
        assert!(adopt_into_cgroup(root, &job, 9).is_err());
    }

    #[test]
    fn cgroup_parent_probe_succeeds_when_mkdir_allowed() {
        // A writable job-cgroup dir (delegation present) → probe creates
        // and removes the throwaway child → true. Models the delegated
        // (slurm-test-env with Delegate=yes) case.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let job = CgroupPath("/job_7".to_string());
        std::fs::create_dir_all(root.join("job_7")).unwrap();
        assert!(cgroup_parent_probe(root, &job));
        // The throwaway child must be cleaned up.
        assert!(!root.join("job_7/.dynrunner-cgparent-probe").exists());
    }

    #[test]
    fn cgroup_parent_probe_fails_when_parent_absent() {
        // No job-cgroup dir (models the no-delegation / unreadable case)
        // → create_dir fails → probe false → caller omits a1 and uses a2.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let job = CgroupPath("/job_does_not_exist".to_string());
        assert!(!cgroup_parent_probe(root, &job));
    }
}
