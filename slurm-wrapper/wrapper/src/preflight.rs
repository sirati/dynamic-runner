//! Single concern: pre-flight orphan-container sweep (generate.rs:452-489).
//! Honours DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1.
//!
//! "Orphan" means PROVABLY ABANDONED: a per-job scratch root whose
//! owning wrapper no longer holds the [`crate::scratch_lock`] liveness
//! lock. A root whose lock is HELD belongs to a LIVE sibling job on
//! this node — e.g. the flapped-but-alive original secondary whose
//! replacement job (the member-respawn pipeline) landed here — and
//! sweeping it rips the rootfs out from under a running secondary
//! (asm-dataset run_20260611_115429: respawned workers died with exec
//! ENOENT / missing `libc.so.6` / missing PATH tools while the
//! secondary's own mapped pages kept it alive). Roots WITHOUT a lock
//! marker (pre-fix wrappers, the original 2026-05-16 conmon-orphan
//! incident shape) keep being swept exactly as before.

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;

const LOG_TARGET: &str = "slurm-wrapper";

/// Graceful-stop (-t 10) + `rm -af` orphan podman containers under
/// `/tmp/*/storage` (owned by this user, NOT liveness-locked) and the
/// user-default storage. `podman` is the resolved absolute path from
/// `bin_resolve`.
///
/// Port of the bash heredoc (generate.rs:452-489) plus the liveness
/// gate: every podman invocation swallows its errors (mirror of
/// `2>/dev/null || true`), the per-job `/tmp/*/storage` roots are
/// skipped unless they are directories owned by the current euid
/// (mirror of `[ -d ]` + `[ -O ]`) AND not held live by a running
/// wrapper ([`crate::scratch_lock::is_live`]), and the 10s
/// graceful-stop window precedes the unconditional `rm -af`.
pub fn run(podman: &str) {
    if std::env::var("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN").as_deref() == Ok("1") {
        tracing::info!(
            target: LOG_TARGET,
            "Pre-flight podman cleanup: skipped (DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1)"
        );
        return;
    }

    tracing::info!(
        target: LOG_TARGET,
        "Pre-flight: scanning for leftover podman containers..."
    );

    // Phase 1: orphan per-job storage roots under /tmp/.
    let mut found = sweep_scratch_roots(podman, Path::new("/tmp"));

    // Phase 2: user-default rootless storage.
    let default_running = run_podman_capture(Command::new(podman).arg("ps").arg("-q"));
    let default_ids = parse_container_ids(&default_running);
    if !default_ids.is_empty() {
        found = true;
        tracing::info!(
            target: LOG_TARGET,
            "Pre-flight: stopping containers in default storage: {}",
            default_ids.join(" ")
        );
        let mut cmd = Command::new(podman);
        cmd.arg("stop").arg("-t").arg("10").args(&default_ids);
        run_podman_swallow(&mut cmd);
    }
    run_podman_swallow(Command::new(podman).arg("rm").arg("-af"));

    if found {
        tracing::info!(target: LOG_TARGET, "Pre-flight: cleaned up leftover containers");
    } else {
        tracing::info!(target: LOG_TARGET, "Pre-flight: no leftover containers");
    }
}

/// Phase 1 of the sweep: enumerate `<scan_root>/*/storage` per-job
/// podman roots owned by the current euid, SKIP the ones whose owning
/// wrapper is alive ([`crate::scratch_lock::is_live`] — see the module
/// doc for the live-sibling incident this gates), graceful-stop any
/// running containers in the dead ones, then `rm -af` to release
/// storage layers + bind-mount references. Returns whether any
/// running container was found (feeds the operator summary line).
///
/// `scan_root` is `/tmp` in production; parameterised so tests drive
/// the sweep against a tempdir with a fake podman.
fn sweep_scratch_roots(podman: &str, scan_root: &Path) -> bool {
    let mut found = false;
    let euid = nix::unistd::geteuid();
    if let Ok(entries) = std::fs::read_dir(scan_root) {
        for entry in entries.flatten() {
            let storage = entry.path().join("storage");
            // Mirror `[ -d "$orphan_storage" ] || continue`.
            let meta = match std::fs::metadata(&storage) {
                Ok(m) if m.is_dir() => m,
                _ => continue,
            };
            // Mirror `[ -O "$orphan_storage" ] || continue`: owned by euid.
            if meta.uid() != euid.as_raw() {
                continue;
            }
            // LIVENESS GATE: a held wrapper.lock means a RUNNING wrapper
            // owns this scratch root — it is a live sibling job, not an
            // orphan. Stopping/removing its containers would gut the
            // rootfs under its live secondary (run_20260611_115429).
            if crate::scratch_lock::is_live(&entry.path()) {
                tracing::info!(
                    target: LOG_TARGET,
                    "Pre-flight: skipping LIVE sibling scratch root {} \
                     (wrapper.lock held by a running wrapper)",
                    entry.path().display()
                );
                continue;
            }
            // runroot = "${orphan_storage%/storage}/run" == "<entry>/run".
            let runroot = entry.path().join("run");
            let storage = storage.to_string_lossy();
            let runroot = runroot.to_string_lossy();

            // Running containers: graceful stop with 10s grace.
            let running = scoped_ps(podman, &storage, &runroot);
            let ids = parse_container_ids(&running);
            if !ids.is_empty() {
                found = true;
                tracing::info!(
                    target: LOG_TARGET,
                    "Pre-flight: stopping containers in {storage}: {}",
                    ids.join(" ")
                );
                scoped_stop(podman, &storage, &runroot, &ids);
            }
            // All containers (including stopped/exited): release storage
            // layers + bind-mount references held by exited containers.
            scoped_rm_af(podman, &storage, &runroot);
        }
    }
    found
}

/// `<podman> --root <storage> --runroot <runroot> --cgroup-manager=cgroupfs ps -q`.
fn scoped_ps(podman: &str, storage: &str, runroot: &str) -> String {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("ps")
        .arg("-q");
    run_podman_capture(&mut cmd)
}

/// `<podman> --root ... --runroot ... --cgroup-manager=cgroupfs stop -t 10 <ids...>`.
fn scoped_stop(podman: &str, storage: &str, runroot: &str, ids: &[String]) {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("stop")
        .arg("-t")
        .arg("10")
        .args(ids);
    run_podman_swallow(&mut cmd);
}

/// `<podman> --root ... --runroot ... --cgroup-manager=cgroupfs rm -af`.
fn scoped_rm_af(podman: &str, storage: &str, runroot: &str) {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .arg("rm")
        .arg("-af");
    run_podman_swallow(&mut cmd);
}

/// Run a podman command, capturing stdout as UTF-8. Mirror of
/// `$(... 2>/dev/null || true)`: any failure yields an empty string.
fn run_podman_capture(cmd: &mut Command) -> String {
    match cmd.output() {
        Ok(out) => String::from_utf8_lossy(&out.stdout).into_owned(),
        Err(_) => String::new(),
    }
}

/// Run a podman command for its side effect, swallowing all errors.
/// Mirror of `... 2>/dev/null || true`.
fn run_podman_swallow(cmd: &mut Command) {
    let _ = cmd.output();
}

/// Split podman `ps -q` stdout into individual container ids. Splits on
/// ASCII whitespace and drops empties, mirroring the unquoted shell
/// expansion `$orphan_running` that word-splits ids into separate args.
fn parse_container_ids(stdout: &str) -> Vec<String> {
    stdout
        .split_ascii_whitespace()
        .map(|s| s.to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_container_ids_splits_and_trims() {
        assert_eq!(
            parse_container_ids("a1\nb2\n  c3 \n"),
            vec!["a1".to_string(), "b2".to_string(), "c3".to_string()]
        );
    }

    #[test]
    fn parse_container_ids_empty_inputs() {
        assert_eq!(parse_container_ids(""), Vec::<String>::new());
        assert_eq!(parse_container_ids("   \n\t  \n"), Vec::<String>::new());
    }

    /// RAII guard: restore (or clear) an env var on drop so tests stay
    /// isolated regardless of the global env state.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn run_returns_on_disable_env() {
        let _guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "1");
        // The disable path must return without invoking podman or panicking.
        run("podman");
    }

    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;

    /// Write an executable fake-podman that appends each invocation's
    /// argv (one line, space-joined) to `calls_log` and answers `ps -q`
    /// with one fake container id (so the stop path is exercised for
    /// swept roots). Returns the script path.
    fn write_fake_podman(dir: &Path, calls_log: &Path) -> PathBuf {
        let path = dir.join("fake-podman");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            "#!/usr/bin/env bash\n\
             echo \"$@\" >> {log}\n\
             for a in \"$@\"; do\n\
               if [ \"$a\" = ps ]; then echo deadbeef; fi\n\
             done",
            log = calls_log.display()
        )
        .unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Create a per-job scratch root `<scan>/<name>` with the
    /// `storage/` + `run/` shape the sweep keys on.
    fn make_scratch_root(scan: &Path, name: &str) -> PathBuf {
        let root = scan.join(name);
        std::fs::create_dir_all(root.join("storage")).unwrap();
        std::fs::create_dir_all(root.join("run")).unwrap();
        root
    }

    /// THE production pin (asm-dataset run_20260611_115429): a scratch
    /// root whose wrapper is ALIVE (liveness lock held) must NOT be
    /// touched by the sweep — no `ps`, no `stop`, no `rm` against its
    /// storage. Pre-fix the sweep classified every euid-owned
    /// `/tmp/*/storage` as an orphan and stop/`rm -af`-ed the LIVE
    /// sibling's container, gutting the rootfs under its running
    /// secondary (respawned workers: exec ENOENT / missing libc.so.6 /
    /// missing PATH tools). An orphan root in the SAME sweep (lock
    /// marker present but released — its wrapper died) must still get
    /// the full stop + rm treatment (the original 2026-05-16
    /// orphan-accumulation incident must stay fixed).
    #[test]
    fn sweep_skips_live_sibling_root_and_cleans_orphan() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // LIVE sibling: liveness lock HELD (a running wrapper).
        let live = make_scratch_root(&scan, "asm-live1234");
        let _live_guard = crate::scratch_lock::acquire(&live).expect("acquire live lock");

        // ORPHAN: marker present but its owner died (lock released).
        let orphan = make_scratch_root(&scan, "asm-dead5678");
        drop(crate::scratch_lock::acquire(&orphan).expect("acquire+release orphan lock"));

        let found = sweep_scratch_roots(&podman.to_string_lossy(), &scan);

        let calls = std::fs::read_to_string(&calls_log).unwrap_or_default();
        let live_storage = live.join("storage");
        assert!(
            !calls.contains(&live_storage.to_string_lossy().into_owned()),
            "the sweep must NEVER touch a LIVE sibling's storage root \
             (gutting it kills the running secondary's exec context); \
             podman calls:\n{calls}",
        );
        let orphan_storage = orphan.join("storage").to_string_lossy().into_owned();
        let orphan_lines: Vec<&str> = calls
            .lines()
            .filter(|l| l.contains(&orphan_storage))
            .collect();
        assert!(
            orphan_lines.iter().any(|l| l.contains(" stop ")),
            "the orphan root must still be graceful-stopped; calls:\n{calls}",
        );
        assert!(
            orphan_lines.iter().any(|l| l.contains(" rm ")),
            "the orphan root must still be rm -af'd; calls:\n{calls}",
        );
        assert!(found, "the orphan's running container counts as found");
    }

    /// A root with NO liveness marker at all (a wrapper from before
    /// this fix, or the true-orphan shape the sweep was built for)
    /// keeps being swept — the gate must not regress the original
    /// orphan cleanup.
    #[test]
    fn sweep_still_cleans_markerless_root() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        let orphan = make_scratch_root(&scan, "asm-prefix9abc");

        sweep_scratch_roots(&podman.to_string_lossy(), &scan);

        let calls = std::fs::read_to_string(&calls_log).unwrap_or_default();
        let storage = orphan.join("storage").to_string_lossy().into_owned();
        assert!(
            calls.lines().any(|l| l.contains(&storage) && l.contains(" rm ")),
            "a markerless (pre-fix / true-orphan) root must still be swept; \
             calls:\n{calls}",
        );
    }
}
