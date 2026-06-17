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

/// Outcome of the Phase-1 per-job-root sweep.
///
/// Carries two independent facts the Phase-2 (default-storage) policy
/// and the operator summary line each need:
///
/// - `found_running` — at least one running container was stopped in a
///   swept (dead) root; feeds the "cleaned up leftover containers"
///   summary.
/// - `live_sibling_present` — at least one scratch root probed LIVE
///   (a held `wrapper.lock`). When set, the node hosts a running
///   sibling secondary, so the UNSCOPED Phase-2 `podman rm -af` against
///   the user-default rootless storage MUST be suppressed: a
///   default-storage container the live sibling owns cannot be
///   distinguished from a true orphan there, and tearing it down guts
///   the sibling's rootfs exactly as the Phase-1 gate prevents for
///   custom-root jobs. Deferring the default-storage cleanup to a later
///   preflight that runs with no live sibling is strictly safer than
///   gutting a live one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SweepOutcome {
    found_running: bool,
    live_sibling_present: bool,
}

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
///
/// `own_scratch_root` is THIS wrapper's own per-job scratch root
/// (`layout.rndtmp`). The wrapper holds its OWN `wrapper.lock` for the
/// whole run (see `main::run` / [`crate::scratch_lock`]), so the
/// liveness probe would read its own held lock back as a "live sibling"
/// — the scan MUST exclude the one root it itself owns (see
/// [`sweep_scratch_roots`]).
pub fn run(podman: &str, own_scratch_root: &Path) {
    run_in(podman, Path::new("/tmp"), own_scratch_root);
}

/// Body of [`run`], parameterised over the scratch-scan root so the
/// end-to-end test can point Phase 1 at a tempdir of fake per-job roots
/// (the `/tmp` literal `run` passes in production is not test-writable
/// with controlled contents). Phase 2 (default storage) is unscoped and
/// driven purely by the supplied `podman` binary, so a fake-podman in
/// the test observes both phases through one call log.
///
/// `own_scratch_root` is excluded from the Phase-1 scan (the wrapper
/// holds its own liveness lock — see [`sweep_scratch_roots`]).
fn run_in(podman: &str, scan_root: &Path, own_scratch_root: &Path) {
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

    // Phase 1: orphan per-job storage roots (liveness-gated per root).
    // The outcome also reports whether ANY root probed live.
    let sweep = sweep_scratch_roots(podman, scan_root, own_scratch_root);

    // Phase 2: user-default rootless storage. UNSCOPED (`podman` with no
    // `--root`/`--runroot`), so unlike Phase 1 it cannot attribute a
    // default-storage container to a particular scratch root — a `stop`
    // + `rm -af` here hits EVERY default-storage container the run user
    // owns on this node. When a live sibling job is present
    // (`sweep.live_sibling_present`), one of those may belong to it, and
    // tearing it down guts the sibling's rootfs exactly as the Phase-1
    // gate prevents for custom-root jobs (run_20260611_175319 — the
    // respawned-worker torn-PATH tear). Defer the default-storage
    // cleanup: a later preflight running with no live sibling sweeps the
    // genuine orphans, while a live sibling is never gutted.
    let mut found = sweep.found_running;
    if sweep.live_sibling_present {
        // NARRATE the deferral: silent suppression reads as "swept clean"
        // in forensics, and this class already cost two RCA rounds partly
        // because the sweep's actions weren't logged. Surface how many
        // default-storage containers were left untouched and why.
        let default_running = run_podman_capture(Command::new(podman).arg("ps").arg("-q"));
        let deferred = parse_container_ids(&default_running).len();
        // The literal token `default-storage sweep` appears in BOTH
        // Phase-2 outcome lines (this deferral and the sweep narration in
        // the else-arm) so ONE forensic grep finds whichever fired — the
        // #430 RCA grep'd for exactly that token and its zero hits were
        // misread as "the gate never engaged".
        tracing::warn!(
            target: LOG_TARGET,
            deferred_default_storage_containers = deferred,
            "Pre-flight: a LIVE sibling job holds a scratch-root liveness lock on \
             this node; SKIPPING the default-storage sweep against the user-default \
             rootless storage ({deferred} container(s) left untouched). The unscoped \
             `podman rm -af` would gut a default-storage container the live sibling \
             may own; these orphans are swept by a later preflight with no live \
             sibling.",
        );
    } else {
        // No live sibling: safe to sweep the default storage as before.
        let default_running = run_podman_capture(Command::new(podman).arg("ps").arg("-q"));
        let default_ids = parse_container_ids(&default_running);
        if !default_ids.is_empty() {
            found = true;
            tracing::info!(
                target: LOG_TARGET,
                "Pre-flight: default-storage sweep: stopping running containers in \
                 the user-default rootless storage: {}",
                default_ids.join(" ")
            );
            let mut cmd = Command::new(podman);
            cmd.arg("stop").arg("-t").arg("10").args(&default_ids);
            run_podman_swallow(&mut cmd);
        }
        // NARRATE the destructive op BEFORE it runs (the #421 lesson:
        // a silent destructive path is itself a bug). This `rm -af` is
        // unscoped — it removes EVERY container (running or exited) the
        // run user owns in the default rootless storage on this node.
        tracing::info!(
            target: LOG_TARGET,
            "Pre-flight: default-storage sweep: removing ALL containers in the \
             user-default rootless storage (`podman rm -af`; no scratch root on \
             this node holds a live wrapper.lock)",
        );
        run_podman_swallow(Command::new(podman).arg("rm").arg("-af"));
    }

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
///
/// `own_scratch_root` is THIS wrapper's own per-job scratch root
/// (`layout.rndtmp`), excluded UP FRONT: the wrapper holds its own
/// `wrapper.lock` for the whole run, and a held POSIX advisory lock
/// conflicts even across two file descriptors in the SAME process — so
/// without this exclusion the wrapper's own [`crate::scratch_lock::is_live`]
/// probe reads its OWN held lock back as a "live sibling", spuriously
/// setting `live_sibling_present` and suppressing the Phase-2
/// default-storage sweep (run_20260612_094056: a self-lock false
/// positive deferred the sweep that would have cleared a stale libpod
/// DB). The own root is neither an orphan nor a sibling — it is us —
/// so it is skipped before the metadata / ownership / liveness checks.
///
/// Returns a [`SweepOutcome`]: whether a running container was stopped
/// in a swept root, AND whether any root probed LIVE — the latter
/// gates the Phase-2 default-storage sweep (see [`SweepOutcome`]).
fn sweep_scratch_roots(
    podman: &str,
    scan_root: &Path,
    own_scratch_root: &Path,
) -> SweepOutcome {
    let mut outcome = SweepOutcome::default();
    let euid = nix::unistd::geteuid();
    if let Ok(entries) = std::fs::read_dir(scan_root) {
        for entry in entries.flatten() {
            // SELF-EXCLUSION: never probe / sweep the root this wrapper
            // itself owns. Its `wrapper.lock` is held by THIS process, so
            // the liveness probe below would read it back as a live
            // sibling (advisory locks conflict across fds within one
            // process) and suppress the Phase-2 sweep on a false
            // positive. Compared by path identity (see `same_root`).
            if same_root(&entry.path(), own_scratch_root) {
                continue;
            }
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
            // rootfs under its live secondary (run_20260611_115429). The
            // same live fact also disarms the UNSCOPED Phase-2 sweep (a
            // live sibling may own a default-storage container too — see
            // `SweepOutcome::live_sibling_present`).
            if crate::scratch_lock::is_live(&entry.path()) {
                outcome.live_sibling_present = true;
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
                outcome.found_running = true;
                tracing::info!(
                    target: LOG_TARGET,
                    "Pre-flight: stopping containers in {storage}: {}",
                    ids.join(" ")
                );
                scoped_stop(podman, &storage, &runroot, &ids);
            }
            // All containers (including stopped/exited): release storage
            // layers + bind-mount references held by exited containers.
            // NARRATE the destructive op BEFORE it runs (the #421 lesson:
            // a silent destructive path is itself a bug) — this used to be
            // the one sweep step with zero forensic trace unless a running
            // container also happened to be stopped above.
            tracing::info!(
                target: LOG_TARGET,
                "Pre-flight: removing ALL containers in orphan scratch root \
                 {storage} (`podman rm -af`; liveness probe found no held \
                 wrapper.lock — the owning wrapper is dead)",
            );
            scoped_rm_af(podman, &storage, &runroot);
        }
    }
    outcome
}

/// Build a `podman` invocation scoped to a per-job scratch root:
/// `--root <storage> --runroot <runroot> --cgroup-manager=cgroupfs`,
/// with `XDG_RUNTIME_DIR=<runroot>`.
///
/// The `XDG_RUNTIME_DIR` override is HERMETIC-LOAD-CRITICAL, not
/// cosmetic. libpod derives its `tmp_dir` from `$XDG_RUNTIME_DIR`
/// (`<XDG_RUNTIME_DIR>/libpod/tmp`), NOT from `--runroot`, and STAMPS
/// that `tmp_dir` into the state DB it creates inside the graphroot
/// (`<storage>/libpod/...`). A scan invocation that scopes `--root`/
/// `--runroot` to a per-job root but inherits the wrapper's AMBIENT
/// `XDG_RUNTIME_DIR` (the default `/run/user/<uid>`) therefore writes a
/// DB recording the DEFAULT tmp dir into THAT root's graphroot. When the
/// root's OWNING wrapper later runs its image load — correctly scoped
/// with `XDG_RUNTIME_DIR=<its run/>` — podman opens the poisoned DB and
/// trips the consistency check (`database tmp dir
/// "/run/user/<uid>/libpod/tmp" does not match our tmp dir
/// "<root>/run/libpod/tmp"`), killing the load (run_20260612_094056:
/// secondary dead at startup). Pinning `XDG_RUNTIME_DIR=<runroot>` makes
/// the recorded tmp dir match the scoped runroot, so a scan can never
/// poison a (sibling's) graphroot DB — mirrors `teardown::podman_base`
/// and `image::run_load`, the other scoped per-job podman invocations.
fn scoped_base(podman: &str, storage: &str, runroot: &str) -> Command {
    let mut cmd = Command::new(podman);
    cmd.arg("--root")
        .arg(storage)
        .arg("--runroot")
        .arg(runroot)
        .arg("--cgroup-manager=cgroupfs")
        .env("XDG_RUNTIME_DIR", runroot);
    cmd
}

/// `<scoped_base> ps -q`.
fn scoped_ps(podman: &str, storage: &str, runroot: &str) -> String {
    let mut cmd = scoped_base(podman, storage, runroot);
    cmd.arg("ps").arg("-q");
    run_podman_capture(&mut cmd)
}

/// `<scoped_base> stop -t 10 <ids...>`.
fn scoped_stop(podman: &str, storage: &str, runroot: &str, ids: &[String]) {
    let mut cmd = scoped_base(podman, storage, runroot);
    cmd.arg("stop").arg("-t").arg("10").args(ids);
    run_podman_swallow(&mut cmd);
}

/// `<scoped_base> rm -af`.
fn scoped_rm_af(podman: &str, storage: &str, runroot: &str) {
    let mut cmd = scoped_base(podman, storage, runroot);
    cmd.arg("rm").arg("-af");
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

/// Do `a` and `b` name the same scratch root? Used to exclude the
/// wrapper's OWN root from the sweep (see [`sweep_scratch_roots`]).
///
/// Compares the canonicalised paths when BOTH resolve (handles `/tmp`
/// being a symlink, or either side carrying `.`/`..`/trailing-slash
/// noise), falling back to a lexical equality when canonicalisation
/// fails for either side (e.g. the own root was already removed). The
/// fallback never widens the match — only the exact same path string
/// is treated as self when canonicalisation is unavailable.
fn same_root(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// `DYNRUNNER_DISABLE_PREFLIGHT_PODMAN` is process-global; the test
    /// runner schedules tests in parallel threads, so every test that
    /// SETS or READS it (the disable-escape probe + the two `run_in`
    /// end-to-end tests) must serialise through this lock. Without it a
    /// concurrent `run_returns_on_disable_env` leaves the var at `1`
    /// while a `run_in` test reads it and skips the whole sweep
    /// (empty call log → spurious failure).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "1");
        // The disable path must return without invoking podman or panicking.
        run("podman", Path::new(NO_OWN_ROOT));
    }

    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;

    /// Write an executable fake-podman that appends each invocation's
    /// argv (one line, space-joined) to `calls_log` and answers `ps -q`
    /// with one fake container id (so the stop path is exercised for
    /// swept roots). Returns the script path.
    ///
    /// Each invocation ALSO appends one line `<XDG_RUNTIME_DIR>\t<argv>`
    /// to a sibling `<calls_log>.env` file (see `env_log_path`) so the
    /// hermetic-load test can correlate the per-call `XDG_RUNTIME_DIR`
    /// with that call's `--runroot`. `calls_log` itself stays a pure
    /// argv-per-line log so the existing argv-substring assertions are
    /// byte-for-byte unaffected.
    fn write_fake_podman(dir: &Path, calls_log: &Path) -> PathBuf {
        let path = dir.join("fake-podman");
        let env_log = env_log_path(calls_log);
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            "#!/usr/bin/env bash\n\
             echo \"$@\" >> {log}\n\
             printf '%s\\t%s\\n' \"${{XDG_RUNTIME_DIR:-<unset>}}\" \"$*\" >> {env}\n\
             for a in \"$@\"; do\n\
               if [ \"$a\" = ps ]; then echo deadbeef; fi\n\
             done",
            log = calls_log.display(),
            env = env_log.display(),
        )
        .unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// Sibling of the argv `calls_log` carrying one
    /// `<XDG_RUNTIME_DIR>\t<argv>` line per fake-podman invocation.
    fn env_log_path(calls_log: &Path) -> PathBuf {
        let mut s = calls_log.as_os_str().to_owned();
        s.push(".env");
        PathBuf::from(s)
    }

    /// Create a per-job scratch root `<scan>/<name>` with the
    /// `storage/` + `run/` shape the sweep keys on.
    fn make_scratch_root(scan: &Path, name: &str) -> PathBuf {
        let root = scan.join(name);
        std::fs::create_dir_all(root.join("storage")).unwrap();
        std::fs::create_dir_all(root.join("run")).unwrap();
        root
    }

    /// Run a fake-podman-driven sweep/run `op`, retrying (with a FRESH
    /// `calls_log` each attempt) until `done(&calls)` holds or a short
    /// deadline elapses; returns the final `calls` log.
    ///
    /// Test-harness hygiene only (default PARALLEL execution): the sweep
    /// `exec`s the fake-podman SCRIPT this test wrote. An UNRELATED concurrent
    /// test's `Command` `fork` can duplicate the still-open WRITE fd of its
    /// own freshly-written fake-podman into its child; until that child
    /// `exec`s, exec'ing that inode fails `ETXTBSY` ("Text file busy"). The
    /// sweep SWALLOWS podman errors by design, so an ETXTBSY'd scoped call
    /// simply logs nothing and a positive "the orphan was swept" assertion
    /// transiently fails. The condition is transient (clears when the foreign
    /// child execs) and CANNOT arise in production (no concurrent process
    /// writes the podman binary), so retrying the sweep only waits it out —
    /// it never masks a real regression (a genuinely-skipped root never logs
    /// the call no matter how many times the sweep runs).
    fn run_until_calls<F>(op: F, calls_log: &Path, done: impl Fn(&str) -> bool) -> String
    where
        F: Fn(),
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let _ = std::fs::remove_file(calls_log);
            let _ = std::fs::remove_file(env_log_path(calls_log));
            op();
            let calls = std::fs::read_to_string(calls_log).unwrap_or_default();
            if done(&calls) || std::time::Instant::now() >= deadline {
                return calls;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// An `own_scratch_root` that is NOT one of the scanned roots — for
    /// the legacy tests that exercise sibling/orphan handling and do not
    /// involve the wrapper's own root. A path that does not exist
    /// canonicalises-to-fallback in `same_root` and never matches a real
    /// scanned entry, so the self-exclusion is a no-op for these tests.
    const NO_OWN_ROOT: &str = "/nonexistent/preflight-test-own-root";

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

        // Run the sweep, retrying (fresh calls log per attempt) until the dead
        // orphan is acted upon. TWO transient parallel-harness interferences
        // can spare the orphan on any single pass; both are one-sided and
        // cannot mask a real regression (a genuinely-live root is never
        // falsely reported dead — a fork only ADDS a lock holder — and a
        // genuinely-skipped root never logs the call):
        //   - `is_live`'s probe momentarily `flock`s the DEAD orphan; a
        //     concurrent test's `fork` can dup that fd and pin the OFD until
        //     it execs, so the probe spuriously reports the orphan LIVE.
        //   - the swallowed scoped `exec` of the fake-podman script can
        //     `ETXTBSY` (see `run_until_calls`), logging nothing.
        // The live-untouched assertion below is checked against the clean
        // single pass `run_until_calls` returns.
        let orphan_storage = orphan.join("storage").to_string_lossy().into_owned();
        let outcome = std::cell::Cell::new(SweepOutcome::default());
        let calls = run_until_calls(
            || {
                outcome.set(sweep_scratch_roots(
                    &podman.to_string_lossy(),
                    &scan,
                    Path::new(NO_OWN_ROOT),
                ));
            },
            &calls_log,
            |c| {
                c.lines()
                    .any(|l| l.contains(&orphan_storage) && l.contains(" stop "))
            },
        );
        let outcome = outcome.get();

        let live_storage = live.join("storage");
        assert!(
            !calls.contains(&live_storage.to_string_lossy().into_owned()),
            "the sweep must NEVER touch a LIVE sibling's storage root \
             (gutting it kills the running secondary's exec context); \
             podman calls:\n{calls}",
        );
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
        assert!(
            outcome.found_running,
            "the orphan's running container counts as found"
        );
        assert!(
            outcome.live_sibling_present,
            "the held-lock live root must be reported so Phase 2's unscoped \
             default-storage sweep is suppressed",
        );
    }

    /// THE defect-(a) pin (run_20260612_094056): the wrapper holds its
    /// OWN `wrapper.lock` for the whole run, then runs the preflight in
    /// the SAME process. A POSIX advisory lock conflicts across two file
    /// descriptors even within one process, so the liveness probe reads
    /// the wrapper's own held lock back as a "live sibling" — pre-fix
    /// this set `live_sibling_present` and suppressed the Phase-2
    /// default-storage sweep on a false positive. With only the OWN root
    /// present (and its lock genuinely held), the scan must EXCLUDE it:
    /// `live_sibling_present` stays false and the own root is never
    /// touched.
    ///
    /// Revert-check: drop the `same_root` self-exclusion in
    /// `sweep_scratch_roots` and the held own-lock re-probes live →
    /// `live_sibling_present` flips true, failing this test.
    #[test]
    fn sweep_excludes_own_held_root() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // The wrapper's OWN scratch root, with its `wrapper.lock` HELD
        // for the life of the run (exactly as `main::run` does before
        // calling `preflight::run`).
        let own = make_scratch_root(&scan, "asm-tokenizer-6c5ea8a6");
        let _own_guard = crate::scratch_lock::acquire(&own).expect("hold own lock");

        let outcome = sweep_scratch_roots(&podman.to_string_lossy(), &scan, &own);

        assert!(
            !outcome.live_sibling_present,
            "the wrapper's OWN held root must NOT count as a live sibling \
             (self read back through a second fd); else the Phase-2 \
             default-storage sweep is suppressed on a false positive",
        );
        let calls = std::fs::read_to_string(&calls_log).unwrap_or_default();
        let own_storage = own.join("storage").to_string_lossy().into_owned();
        assert!(
            !calls.contains(&own_storage),
            "the wrapper's own scratch root must never be probed/swept; \
             calls:\n{calls}",
        );
    }

    /// Self-exclusion must NOT weaken protection for a GENUINE foreign
    /// sibling: the OWN root is excluded, but a DIFFERENT held-lock root
    /// still reports `live_sibling_present` (the run_20260611_115429
    /// live-sibling guard stays intact).
    #[test]
    fn sweep_excludes_own_but_keeps_foreign_sibling_live() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // OWN root (held) — excluded.
        let own = make_scratch_root(&scan, "asm-tokenizer-6c5ea8a6");
        let _own_guard = crate::scratch_lock::acquire(&own).expect("hold own lock");
        // A GENUINE foreign sibling (held) — must still gate Phase 2.
        let sibling = make_scratch_root(&scan, "asm-tokenizer-deadbeef");
        let _sib_guard = crate::scratch_lock::acquire(&sibling).expect("hold sibling lock");

        let outcome = sweep_scratch_roots(&podman.to_string_lossy(), &scan, &own);

        assert!(
            outcome.live_sibling_present,
            "a genuine foreign held-lock sibling must still be reported \
             live so Phase 2 is suppressed — self-exclusion must not \
             disarm the live-sibling guard",
        );
        let calls = std::fs::read_to_string(&calls_log).unwrap_or_default();
        assert!(
            !calls.contains(&own.join("storage").to_string_lossy().into_owned()),
            "own root never touched; calls:\n{calls}",
        );
        assert!(
            !calls.contains(&sibling.join("storage").to_string_lossy().into_owned()),
            "a live foreign sibling's root is never touched either; \
             calls:\n{calls}",
        );
    }

    /// THE defect-(b) pin (run_20260612_094056 hermetic load): every
    /// SCOPED Phase-1 podman invocation against a (sibling's) per-job
    /// graphroot must run with `XDG_RUNTIME_DIR=<that root's run/>`.
    /// libpod derives its `tmp_dir` from `$XDG_RUNTIME_DIR` (NOT from
    /// `--runroot`) and stamps it into the state DB it writes inside the
    /// graphroot. A scan that scoped `--root`/`--runroot` but inherited
    /// the wrapper's AMBIENT default `XDG_RUNTIME_DIR` poisoned the
    /// scanned graphroot's DB with the default tmp dir; the root's owning
    /// wrapper then tripped `database tmp dir ... does not match` on its
    /// correctly-scoped load. Asserts the per-call XDG matches the call's
    /// own `--runroot`.
    ///
    /// Revert-check: drop the `.env("XDG_RUNTIME_DIR", runroot)` from
    /// `scoped_base` and the swept root's calls log `<default-or-unset>`
    /// instead of `<root>/run`, failing the match assertion.
    #[test]
    fn scoped_scan_pins_xdg_runtime_dir_to_runroot() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // A markerless orphan root: swept (ps + stop + rm), so every
        // scoped helper runs against it.
        let orphan = make_scratch_root(&scan, "asm-tokenizer-cafe1234");
        let want_runroot = orphan.join("run").to_string_lossy().into_owned();

        // Retry until the orphan is actually swept (a scoped call carrying
        // its runroot is logged): the fake-podman exec can transiently
        // ETXTBSY under the parallel harness (see `run_until_calls`).
        run_until_calls(
            || {
                sweep_scratch_roots(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
            },
            &calls_log,
            |_| {
                std::fs::read_to_string(env_log_path(&calls_log))
                    .unwrap_or_default()
                    .lines()
                    .any(|l| l.contains(&want_runroot))
            },
        );

        // The fake podman logged `<XDG_RUNTIME_DIR>\t<argv>` per call.
        let env_log = std::fs::read_to_string(env_log_path(&calls_log)).unwrap_or_default();

        // Every SCOPED call (one carrying `--runroot <orphan>/run`) must
        // have run with `XDG_RUNTIME_DIR == <orphan>/run`.
        let scoped: Vec<&str> = env_log
            .lines()
            .filter(|l| l.contains(&want_runroot))
            .collect();
        assert!(
            !scoped.is_empty(),
            "the orphan root must have been swept by scoped calls; \
             env log:\n{env_log}",
        );
        for line in &scoped {
            let (xdg, _argv) = line.split_once('\t').expect("xdg<TAB>argv");
            assert_eq!(
                xdg, want_runroot,
                "a scoped scan call must run with XDG_RUNTIME_DIR=<runroot> \
                 so libpod records a tmp dir consistent with the scoped \
                 runroot (else it poisons the graphroot DB); line:\n{line}",
            );
        }
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

        let storage = orphan.join("storage").to_string_lossy().into_owned();
        let calls = run_until_calls(
            || {
                sweep_scratch_roots(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
            },
            &calls_log,
            |c| c.lines().any(|l| l.contains(&storage) && l.contains(" rm ")),
        );
        assert!(
            calls.lines().any(|l| l.contains(&storage) && l.contains(" rm ")),
            "a markerless (pre-fix / true-orphan) root must still be swept; \
             calls:\n{calls}",
        );
    }

    /// A `stop`/`rm` call is "default-storage" when it carries NO
    /// `--root`/`--runroot` (the unscoped Phase-2 shape). The scoped
    /// Phase-1 calls always prefix those flags, so their absence on a
    /// `stop`/`rm` line uniquely identifies a default-storage teardown.
    ///
    /// The fake podman logs argv space-joined, so the verb is the FIRST
    /// token on the default-storage lines (`stop -t 10 …`, `rm -af`) and
    /// a leading-space match would miss them — split into tokens and
    /// look for the verb as token[0].
    fn is_default_storage_teardown(line: &str) -> bool {
        let mut toks = line.split_whitespace();
        let verb = toks.next();
        matches!(verb, Some("stop") | Some("rm"))
            && !line.contains("--root")
            && !line.contains("--runroot")
    }

    /// THE root-cause pin for run_20260611_175319 (respawned-worker
    /// torn-PATH). Replays the observed sequence END-TO-END through the
    /// real preflight `run_in`:
    ///
    ///   * a LIVE sibling secondary holds its `wrapper.lock`, AND
    ///   * the fake podman reports a RUNNING default-storage container
    ///     (`ps -q` → an id) — standing in for the live sibling's
    ///     own default-storage container.
    ///
    /// The unscoped Phase-2 sweep (`podman stop` + `podman rm -af`, no
    /// `--root`) would tear that container down and gut the live
    /// sibling's rootfs — exactly the PATH/libc.so.6/nix-store tear the
    /// consumer hit. With the live-sibling suppression, preflight must
    /// issue NO default-storage `stop`/`rm` at all (the container stays
    /// alive) and must NARRATE the deferral.
    ///
    /// Revert-check: drop the `if sweep.live_sibling_present` guard in
    /// `run_in` (back to the always-rm shape) and the default-storage
    /// `rm -af` reappears in the call log, failing the survive assert.
    #[test]
    fn run_suppresses_default_storage_sweep_when_live_sibling_present() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        // The fake podman answers EVERY `ps` with a container id, so the
        // unscoped Phase-2 `ps -q` reports a running default-storage
        // container — the one a non-suppressed sweep would gut.
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // LIVE sibling: scratch root with a HELD wrapper.lock.
        let live = make_scratch_root(&scan, "asm-live9999");
        let _live_guard = crate::scratch_lock::acquire(&live).expect("acquire live lock");

        // `run_in` reads the process-global disable env; serialise + pin
        // it OFF so the ambient state / a parallel disable-probe test
        // cannot skip the sweep out from under this test.
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _disable_guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "0");

        run_in(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));

        let calls = std::fs::read_to_string(&calls_log).unwrap_or_default();

        // The live sibling's scratch root is never touched (Phase-1 gate).
        let live_storage = live.join("storage").to_string_lossy().into_owned();
        assert!(
            !calls.contains(&live_storage),
            "Phase 1 must skip the live sibling's scratch root; calls:\n{calls}",
        );
        // CORE: no UNSCOPED default-storage stop/rm — the live sibling's
        // default-storage container survives the preflight.
        let default_teardowns: Vec<&str> = calls
            .lines()
            .filter(|l| is_default_storage_teardown(l))
            .collect();
        assert!(
            default_teardowns.is_empty(),
            "with a live sibling present the unscoped default-storage \
             stop/rm MUST be suppressed (it would gut the sibling's \
             rootfs); offending calls: {default_teardowns:?}\nfull log:\n{calls}",
        );
    }

    use std::cell::RefCell;
    use std::sync::Arc;

    thread_local! {
        /// The buffer the GLOBAL capture subscriber writes THIS thread's
        /// events to while a `capture_logs` is active on it; `None`
        /// otherwise (events are then discarded). Per-thread so parallel
        /// tests never cross-contaminate each other's captured logs.
        static CAPTURE_BUF: RefCell<Option<Arc<Mutex<Vec<u8>>>>> = const { RefCell::new(None) };
    }

    /// Writer the global capture subscriber routes every event through:
    /// it forwards bytes to the calling thread's `CAPTURE_BUF` when set,
    /// and drops them otherwise. One process-wide subscriber owns this,
    /// so callsite interest is cached ONCE as enabled.
    struct ThreadRouter;
    impl std::io::Write for ThreadRouter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            CAPTURE_BUF.with(|c| {
                if let Some(buf) = c.borrow().as_ref() {
                    buf.lock().unwrap_or_else(|e| e.into_inner()).extend_from_slice(b);
                }
            });
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadRouter {
        type Writer = ThreadRouter;
        fn make_writer(&'a self) -> ThreadRouter {
            ThreadRouter
        }
    }

    /// Install the process-wide capture subscriber EXACTLY once.
    ///
    /// Why global (not thread-local `with_default`): `tracing` caches
    /// each callsite's INTEREST globally, computed from the registered
    /// subscriber's level hint. A thread-local `with_default` is NOT
    /// consulted by that cache, so when any parallel test hits a sweep
    /// narration callsite FIRST under the no-op global default, the
    /// callsite is cached as `never` and a later thread-local capture
    /// silently sees nothing (the narration asserts then flake under
    /// `--test-threads>1`). A single permissive (TRACE-level) global
    /// subscriber caches every callsite as ENABLED; the per-thread
    /// `CAPTURE_BUF` then decides which thread's events to retain.
    fn ensure_capture_subscriber() {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(ThreadRouter)
                .finish();
            // Ignore the result: a test binary that somehow already has a
            // global default still captures via the thread-local writer if
            // that default is ours; the narration tests assert on the
            // buffer, so a lost race here surfaces as an explicit failure,
            // never a silent pass.
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
    }

    /// Capture every tracing line emitted by `f` (TRACE and up) into a
    /// `String`, routing only THIS thread's events to the returned
    /// buffer (see `ensure_capture_subscriber` / `CAPTURE_BUF`).
    fn capture_logs<F: FnOnce()>(f: F) -> String {
        ensure_capture_subscriber();
        let buf = Arc::new(Mutex::new(Vec::new()));
        // Bind this thread's capture buffer for the duration of `f`,
        // restoring the prior binding on exit (supports nesting / repeat).
        let prev = CAPTURE_BUF.with(|c| c.borrow_mut().replace(buf.clone()));
        f();
        CAPTURE_BUF.with(|c| *c.borrow_mut() = prev);
        let bytes = buf.lock().unwrap_or_else(|e| e.into_inner()).clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// OBSERVABILITY PIN (#421's explicit lesson, re-bitten in the
    /// run_20260611_202345 RCA): every destructive storage operation must
    /// NARRATE what it is about to destroy and why, BEFORE it runs. The
    /// Phase-1 per-root `rm -af` against a swept (probe-dead) scratch
    /// root used to be completely silent — a sweep that gutted a root
    /// left zero forensic trace unless a *running* container also
    /// happened to be stopped. The sweep must log the orphan root's
    /// storage path and the liveness rationale.
    #[test]
    fn sweep_narrates_orphan_root_rm() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // Markerless orphan root: swept, including the silent-by-default
        // `rm -af`.
        let orphan = make_scratch_root(&scan, "asm-deadbeef");

        let logs = capture_logs(|| {
            sweep_scratch_roots(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
        });

        let storage = orphan.join("storage").to_string_lossy().into_owned();
        let rm_lines: Vec<&str> = logs
            .lines()
            .filter(|l| l.contains("rm -af") && l.contains(&storage))
            .collect();
        assert!(
            !rm_lines.is_empty(),
            "the Phase-1 orphan-root `rm -af` must narrate the storage \
             root it is about to destroy and why; logs:\n{logs}",
        );
        assert!(
            rm_lines.iter().any(|l| l.contains("wrapper.lock")),
            "the narration must carry the liveness rationale \
             (no held wrapper.lock); logs:\n{logs}",
        );
    }

    /// OBSERVABILITY PIN: the Phase-2 unscoped `rm -af` against the
    /// user-default rootless storage must narrate itself, and BOTH
    /// Phase-2 outcome lines (the sweep and the live-sibling deferral)
    /// must carry the literal grep token `default-storage sweep`. The
    /// #430 forensics grep'd the run log tree for exactly that token and
    /// got zero hits — which was treated as "the sweep never ran /
    /// never deferred", when in fact NEITHER line contained the token
    /// (the rm was silent; the WARN said "user-default rootless storage
    /// sweep"). A destructive path whose log lines can't be found by the
    /// obvious grep is indistinguishable from an unlogged one.
    #[test]
    fn run_narrates_default_storage_sweep_with_grep_token() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _disable_guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "0");

        // No live sibling: the Phase-2 sweep RUNS and must say so.
        let logs = capture_logs(|| {
            run_in(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
        });
        assert!(
            logs.lines()
                .any(|l| l.contains("default-storage sweep") && l.contains("rm -af")),
            "the Phase-2 default-storage `rm -af` must narrate itself \
             with the grep token `default-storage sweep`; logs:\n{logs}",
        );
    }

    /// OBSERVABILITY PIN (deferral side of the grep token): when a live
    /// sibling suppresses the Phase-2 sweep, the WARN must ALSO carry
    /// the `default-storage sweep` token so one grep finds both
    /// outcomes.
    #[test]
    fn run_deferral_warn_carries_grep_token() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // LIVE sibling: scratch root with a HELD wrapper.lock.
        let live = make_scratch_root(&scan, "asm-live7777");
        let _live_guard = crate::scratch_lock::acquire(&live).expect("acquire live lock");

        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _disable_guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "0");

        let logs = capture_logs(|| {
            run_in(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
        });
        assert!(
            logs.lines()
                .any(|l| l.contains("default-storage sweep") && l.contains("SKIPPING")),
            "the live-sibling deferral WARN must carry the grep token \
             `default-storage sweep`; logs:\n{logs}",
        );
    }

    /// Counterpart: with NO live sibling, `run_in` still performs the
    /// default-storage sweep (`rm -af`) — the suppression must not
    /// regress true-orphan cleanup on a node with no live job.
    #[test]
    fn run_sweeps_default_storage_when_no_live_sibling() {
        let tmp = tempfile::tempdir().unwrap();
        let scan = tmp.path().join("scan");
        std::fs::create_dir_all(&scan).unwrap();
        let calls_log = tmp.path().join("calls.log");
        let podman = write_fake_podman(tmp.path(), &calls_log);

        // No scratch roots at all → no live sibling. Serialise + pin the
        // disable env OFF (see the live-sibling test).
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _disable_guard = EnvGuard::set("DYNRUNNER_DISABLE_PREFLIGHT_PODMAN", "0");

        // Retry until the default-storage teardown is logged: the fake-podman
        // exec can transiently ETXTBSY under the parallel harness (see
        // `run_until_calls`). With no live sibling the sweep is deterministic,
        // so a clean attempt always logs it.
        let calls = run_until_calls(
            || {
                run_in(&podman.to_string_lossy(), &scan, Path::new(NO_OWN_ROOT));
            },
            &calls_log,
            |c| c.lines().any(is_default_storage_teardown),
        );
        assert!(
            calls.lines().any(is_default_storage_teardown),
            "with no live sibling the default-storage sweep must still run \
             (true-orphan cleanup); calls:\n{calls}",
        );
    }
}
