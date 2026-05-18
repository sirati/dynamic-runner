//! Single concern: invoking the `podman` CLI behind a trait so the
//! state machine in `poll_loop` is generic over the backend.
//!
//! All real-world invocations go through the same root/runroot/
//! cgroup-manager prefix; the production impl owns that prefix once.
//! Tests use [`MockBackend`] in `tests/common`, never spawning real
//! podman processes.
//!
//! Errors are intentionally collapsed to `bool` at the trait surface
//! for the *signalling* methods (`kill_pid1`, `stop`, `rm_all`, ...):
//! every caller in this binary treats those failures as "best effort,
//! move on". The exception is [`PodmanBackend::remove_tmp_tree`], whose
//! caller (`cleanup::remove_tmp_prefix`) needs the captured stderr in
//! the manager's log to diagnose why `/tmp/asm-*` cleanup fails — it
//! therefore returns `Result<(), String>` with stderr/argv/exit packed
//! into the error string. Despite living on the same trait,
//! `remove_tmp_tree` is a host-side `rm -rf` and does NOT shell out
//! through podman at all (see its method doc for the why).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Backend abstraction. Production: [`RealPodman`]. Tests: a mock that
/// records calls in order.
pub trait PodmanBackend {
    /// `podman container exists <NAME>` — true iff exit status 0.
    fn container_exists(&self, name: &str) -> bool;

    /// `podman exec <NAME> kill -<SIGNAL> <pid>` — true iff exit 0.
    /// Used to signal the secondary process inside the container by its
    /// pid-1 child PID.
    fn exec_signal(&self, name: &str, pid: u32, signal: &str) -> bool;

    /// `podman exec <NAME> pgrep -P 1 -o` — return the oldest child of
    /// PID 1 inside the container, or `None` if pgrep finds nothing
    /// (or the exec fails — caller treats both alike, see state-machine
    /// commentary).
    fn exec_pgrep_first_child(&self, name: &str) -> Option<u32>;

    /// `podman kill --signal <SIGNAL> <NAME>` — signals pid 1 of the
    /// container itself. Belt-and-suspenders for the case the user
    /// process never spawned a child, or pgrep missed it.
    fn kill_pid1(&self, name: &str, signal: &str) -> bool;

    /// `podman stop -t <grace_secs> <NAME>` — graceful stop, falling
    /// back to SIGKILL after `grace_secs`.
    fn stop(&self, name: &str, grace_secs: u32) -> bool;

    /// `podman rm -af` — remove all containers under this storage
    /// root, releasing layer references. Idempotent.
    fn rm_all(&self) -> bool;

    /// `podman unshare <rm-path> <abs-path> -rf` — recursive
    /// remove of the tmp tree via a rootless-podman user-namespace
    /// where kruppb maps to uid 0. The unshare invocation is
    /// **without** `--root`/`--runroot`: the userns entry is what
    /// we need; the storage-driver context is not.
    ///
    /// Why not plain host `rm`: rootless-podman overlay storage
    /// contains nix-store-pattern read-only directories (mode
    /// `r-xr-xr-x` even on dirs owned by kruppb). `unlinkat(2)`
    /// requires write permission on the *parent directory* of the
    /// entry being removed; host kruppb lacks that write bit and
    /// gets EACCES. Inside `podman unshare`, kruppb maps to uid 0,
    /// which bypasses the dir-write-bit via `CAP_DAC_OVERRIDE`
    /// (empirically confirmed by peer asm-tokenizer 2026-05-18
    /// 12:37 — bare `rm /tmp/asm-XXX -rf` EACCES'd on
    /// `nix/store/.../libtsan.so.2.0.0`; `podman unshare rm
    /// /tmp/asm-XXX -rf` cleared the same residue cleanly).
    ///
    /// Why no `--root`/`--runroot`: the earlier shape
    /// `podman --root=X --runroot=Y unshare rm X -rf` failed with
    /// `EBUSY: Device or resource busy` on `X/storage/overlay`
    /// because the unshare's storage driver — initialized exactly
    /// because we passed `--root=X` — holds an internal lock on
    /// its own root directory. Dropping those flags eliminates the
    /// busy-lock without losing the userns entry: the userns
    /// mapping comes from `containers.conf` defaults and rootless-
    /// podman state, not from `--root`.
    ///
    /// Net argv: `podman unshare <rm-path> <abs-path> -rf`. Built
    /// from a fresh `Command::new(&self.podman_path)` — NOT the
    /// `self.cmd()` helper that prepends `--root`/`--runroot`/
    /// `--cgroup-manager`, because we explicitly do NOT want those.
    ///
    /// HARD SAFETY CONTRACT (enforced by [`validate_safe_tmp_path`]
    /// inside [`RealPodman::remove_tmp_tree`], invoked before exec).
    /// The argv-shape validation is what protects against
    /// catastrophic path bugs, not the absent unshare wrapping:
    ///
    ///   - path canonicalizes (symlinks resolved, `..` collapsed) —
    ///     also proves the path exists;
    ///   - canonical path is absolute;
    ///   - canonical path is strictly under `/tmp/` (and not `/tmp/`
    ///     itself) — a symlink whose target leaves `/tmp/` is
    ///     rejected by this check after canonicalize;
    ///   - canonical path does NOT traverse `/home/`;
    ///   - canonical path matches a strict character whitelist
    ///     `[a-zA-Z0-9./_-]` — rejects `*`, `'`, shell metas, NUL,
    ///     whitespace.
    ///
    /// Argv is `rm <abs-path> -rf` (path BEFORE flags). Reason: if
    /// any future arg-construction bug drops the path slot, `rm -rf`
    /// alone has no operand and is a safe no-op exit-error; the
    /// reversed shape `rm -rf <abs-path>` would let a dropped path
    /// expose `-rf` to a subsequent arg if argv ever gets composed
    /// dynamically.
    ///
    /// On failure returns `Err(stderr)` where the string carries the
    /// captured stderr plus the argv and exit status — `/tmp/asm-*`
    /// directories silently piling up on workers is a real recurring
    /// symptom and the next repro must tell us why.
    fn remove_tmp_tree(&self, path: &Path) -> Result<(), String>;
}

/// Production backend. Holds the podman binary path, the `rm`
/// binary path, AND the storage/runroot prefix so callers do not
/// have to know about any of them.
///
/// Both binary paths are resolved ONCE upstream — by the wrapper
/// script's `command -v podman` / `command -v rm` at render time —
/// and stored here as absolute `PathBuf`s. Every invocation reuses
/// the stored absolute path verbatim; there is no exec-time PATH
/// lookup ever, because the absolute path travels straight through
/// to `execve(2)`. This design exists because the manager runs
/// under a systemd-user-service unit whose PATH does NOT inherit
/// the wrapper's shell PATH — on NixOS workers `podman` and `rm`
/// live under `/run/current-system/sw/bin/`, which is not on the
/// default user-systemd PATH; any path-lookup inside the manager
/// would ENOENT (asm-tokenizer 2026-05-18 at 17481c4 for rm;
/// earlier for podman).
///
/// `rm_path` is invoked THROUGH `podman unshare` (no
/// `--root`/`--runroot`) — the user-namespace gives kruppb uid-0
/// inside which is what lets `unlinkat(2)` clear the rootless-
/// podman overlay tree (nix-store-pattern `r-xr-xr-x` parent dirs
/// block plain host rm via EACCES; uid-0-via-`CAP_DAC_OVERRIDE`
/// bypasses the dir-write-bit). Dropping `--root`/`--runroot` is
/// what fixes the prior storage-driver-busy lock; see
/// [`PodmanBackend::remove_tmp_tree`] doc for the full evidence
/// trail.
#[derive(Debug, Clone)]
pub struct RealPodman {
    podman_path: PathBuf,
    rm_path: PathBuf,
    storage_root: PathBuf,
    runroot: PathBuf,
}

impl RealPodman {
    pub fn new(
        podman_path: PathBuf,
        rm_path: PathBuf,
        storage_root: PathBuf,
        runroot: PathBuf,
    ) -> Self {
        Self {
            podman_path,
            rm_path,
            storage_root,
            runroot,
        }
    }

    /// Build a `podman` invocation pre-loaded with the common prefix:
    /// `--root <storage_root> --runroot <runroot> --cgroup-manager=cgroupfs`.
    /// All subcommands flow through this helper to keep the prefix
    /// in exactly one place.
    fn cmd(&self) -> Command {
        let mut c = Command::new(&self.podman_path);
        c.arg("--root")
            .arg(&self.storage_root)
            .arg("--runroot")
            .arg(&self.runroot)
            .arg("--cgroup-manager=cgroupfs");
        c
    }

    /// Run a command swallowing all output, returning exit-0 as bool.
    fn run_silent(mut cmd: Command) -> bool {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        matches!(cmd.status(), Ok(s) if s.success())
    }

    /// Run a command capturing stderr (stdout/stdin still nulled).
    /// `Ok(())` on exit-0; `Err(diag)` otherwise, where `diag` packs
    /// argv (debug-formatted — may include shell-unsafe chars, fine
    /// for a log line but not for replay), exit status, and the
    /// captured stderr decoded best-effort as UTF-8 via
    /// `String::from_utf8_lossy`.
    fn run_capture_stderr(mut cmd: Command) -> Result<(), String> {
        cmd.stdin(Stdio::null()).stdout(Stdio::null());
        let argv = format!("{:?}", cmd);
        match cmd.output() {
            Ok(out) => match out.status.success() {
                true => Ok(()),
                false => Err(format!(
                    "argv: {}; exit={}; stderr: {}",
                    argv,
                    out.status,
                    String::from_utf8_lossy(&out.stderr).trim_end()
                )),
            },
            Err(e) => Err(format!("argv: {}; spawn-error: {}", argv, e)),
        }
    }
}

impl PodmanBackend for RealPodman {
    fn container_exists(&self, name: &str) -> bool {
        let mut c = self.cmd();
        c.arg("container").arg("exists").arg(name);
        Self::run_silent(c)
    }

    fn exec_signal(&self, name: &str, pid: u32, signal: &str) -> bool {
        let mut c = self.cmd();
        c.arg("exec")
            .arg(name)
            .arg("kill")
            .arg(format!("-{}", signal))
            .arg(pid.to_string());
        Self::run_silent(c)
    }

    fn exec_pgrep_first_child(&self, name: &str) -> Option<u32> {
        let mut c = self.cmd();
        c.arg("exec")
            .arg(name)
            .arg("pgrep")
            .arg("-P")
            .arg("1")
            .arg("-o")
            .stdin(Stdio::null())
            .stderr(Stdio::null());
        let out = c.output().ok()?;
        match out.status.success() {
            false => None,
            true => {
                let text = String::from_utf8(out.stdout).ok()?;
                text.trim().lines().next()?.trim().parse::<u32>().ok()
            }
        }
    }

    fn kill_pid1(&self, name: &str, signal: &str) -> bool {
        let mut c = self.cmd();
        c.arg("kill").arg("--signal").arg(signal).arg(name);
        Self::run_silent(c)
    }

    fn stop(&self, name: &str, grace_secs: u32) -> bool {
        let mut c = self.cmd();
        c.arg("stop")
            .arg("-t")
            .arg(grace_secs.to_string())
            .arg(name);
        Self::run_silent(c)
    }

    fn rm_all(&self) -> bool {
        let mut c = self.cmd();
        c.arg("rm").arg("-af");
        Self::run_silent(c)
    }

    fn remove_tmp_tree(&self, path: &Path) -> Result<(), String> {
        // Validation is BEFORE any exec — if the path is malformed,
        // off-prefix, or contains forbidden characters, no rm runs.
        // The validated canonical form is what we hand to rm: any
        // symlink chain in the input has been collapsed, so the
        // recursion target is the actual on-disk tree.
        let canonical = validate_safe_tmp_path(path)?;
        // `podman unshare <rm> <path> -rf` WITHOUT `--root`/
        // `--runroot`. Both binary paths are absolute (resolved by
        // the wrapper at startup; see RealPodman doc). The
        // `self.cmd()` helper deliberately is NOT used here: that
        // helper prepends `--root=<storage>`/`--runroot=<run>`/
        // `--cgroup-manager=cgroupfs`, and those are what caused
        // the storage-driver-busy lock on `<prefix>/storage/overlay`
        // in the earlier shape (a70d3bf/62f3ffb). The userns entry
        // we need comes from rootless-podman defaults, not from
        // those flags.
        //
        // Argv shape: `unshare <rm-path> <canonical> -rf`. Path
        // BEFORE flags per the trait-doc safety contract: a
        // dropped path slot leaves `rm -rf` with no operand (safe
        // failure) rather than letting `-rf` survive into a
        // position that could attach to a different operand if
        // argv composition ever changes.
        let mut c = Command::new(&self.podman_path);
        c.arg("unshare")
            .arg(&self.rm_path)
            .arg(&canonical)
            .arg("-rf");
        Self::run_capture_stderr(c)
    }
}

/// Pre-flight safety gate for [`PodmanBackend::remove_tmp_tree`]. On
/// success returns the canonical (symlink-resolved, `..`-collapsed)
/// path that the caller hands to `rm -rf`; on failure returns a
/// diagnostic suitable for the operator log.
///
/// Six checks, listed in order of execution; each one fails closed:
///
///   1. **canonicalize** — resolves all symlinks in the input path
///      and collapses `..` segments. Also requires the path to
///      exist (returns `ENOENT` otherwise) — there is no value in
///      `rm -rf`-ing a nonexistent path, so we let canonicalize be
///      the existence probe at the same time.
///   2. **absolute** — canonicalize is documented to return absolute
///      paths on Unix, but we re-assert as a defense against the
///      contract changing or unusual mount setups.
///   3. **UTF-8** — `/tmp/asm-*` is ASCII by construction (the
///      wrapper generates hex-suffix names). A non-UTF-8 canonical
///      path indicates the input was constructed wrongly; refuse.
///   4. **strictly under `/tmp/`** — canonical path starts with
///      `/tmp/` AND is longer than `/tmp/` itself. A symlink whose
///      target leaves `/tmp/` fails here because canonicalize
///      already followed it: the symlink might live under `/tmp/`
///      but the canonical resolved path is what we check.
///   5. **no `/home/` substring** — defense against a canonical
///      path that traverses a user home dir (e.g. a `/tmp/asm-*`
///      that ended up resolving to `/home/user/tmp/asm-*` via a
///      mount or symlink quirk we missed). Cheap; fail closed.
///   6. **character whitelist** — `[a-zA-Z0-9./_-]` is the full set
///      of characters that appear in a well-formed
///      `/tmp/asm-{8hex}/<podman-overlay-tree>` path. Anything else
///      (`*`, `'`, `"`, `` ` ``, `$`, `;`, `&`, `|`, `<`, `>`, `\n`,
///      `\0`, whitespace, etc.) is rejected. The check is on the
///      canonical path (post-resolution), so it catches a symlink
///      whose target contains shell metas as well.
fn validate_safe_tmp_path(input: &Path) -> Result<PathBuf, String> {
    // Canonicalize FIRST — every subsequent check operates on the
    // resolved on-disk identity, not the user-supplied string. This
    // is the load-bearing safety step: a symlink under /tmp/ whose
    // target leaves /tmp/ resolves here and fails the /tmp/ prefix
    // check below; a `..`-laced input collapses; a non-existent
    // input fails at this step (canonicalize errors on ENOENT,
    // which we treat as "nothing to remove anyway, refuse").
    let canonical = input.canonicalize().map_err(|e| {
        format!(
            "validate_safe_tmp_path: canonicalize {} failed: {}",
            input.display(),
            e
        )
    })?;

    // canonicalize returns absolute on Unix; re-assert as a defense
    // against the contract changing or an unusual mount setup.
    if !canonical.is_absolute() {
        return Err(format!(
            "validate_safe_tmp_path: canonical path not absolute: {}",
            canonical.display()
        ));
    }

    // /tmp/asm-* paths are ASCII by construction; non-UTF-8 here
    // means the input was malformed.
    let s = canonical.to_str().ok_or_else(|| {
        format!(
            "validate_safe_tmp_path: canonical path not UTF-8: {:?}",
            canonical
        )
    })?;

    // Strictly under /tmp/ — refuses bare `/tmp/` itself so we can
    // never rm -rf the whole tmpfs.
    if !s.starts_with("/tmp/") || s.len() <= "/tmp/".len() {
        return Err(format!(
            "validate_safe_tmp_path: path not strictly under /tmp/: {}",
            s
        ));
    }

    // /home/ substring check — defense against canonical paths that
    // somehow traverse a user home directory via a mount or symlink
    // quirk we missed.
    if s.contains("/home/") {
        return Err(format!(
            "validate_safe_tmp_path: path traverses /home/: {}",
            s
        ));
    }

    // Character whitelist on the canonical (post-resolution) path —
    // catches `*`, `'`, `"`, `` ` ``, `$`, `;`, `&`, `|`, `<`, `>`,
    // `\n`, `\0`, whitespace, etc. The `/tmp/asm-{8hex}/<podman
    // overlay tree>` shape only ever produces `[a-zA-Z0-9./_-]`.
    if let Some(bad) = s
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(*c, '/' | '.' | '-' | '_')))
    {
        return Err(format!(
            "validate_safe_tmp_path: path contains non-whitelisted char {:?}: {}",
            bad, s
        ));
    }

    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Smoke: builder records podman_path/rm_path/storage/runroot
    /// in the command vector (we can't easily inspect a `Command`
    /// post-hoc without spawning, so this is more a constructor
    /// sanity-check than a behaviour test — behaviour is exercised
    /// via the mock in `tests/common`).
    #[test]
    fn real_backend_constructs() {
        let b = RealPodman::new(
            PathBuf::from("/nix/store/x/bin/podman"),
            PathBuf::from("/nix/store/y/bin/rm"),
            PathBuf::from("/r"),
            PathBuf::from("/rr"),
        );
        assert_eq!(b.podman_path, Path::new("/nix/store/x/bin/podman"));
        assert_eq!(b.rm_path, Path::new("/nix/store/y/bin/rm"));
        assert_eq!(b.storage_root, Path::new("/r"));
        assert_eq!(b.runroot, Path::new("/rr"));
    }

    /// Positive path: a real tempdir created under `/tmp/`
    /// canonicalizes to a path that satisfies every check. Returns
    /// Ok(canonical) and the canonical path is what we'd hand to
    /// `rm -rf`.
    #[test]
    fn validate_safe_tmp_path_accepts_real_tmp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        assert!(
            path.starts_with("/tmp/"),
            "tempfile not under /tmp/ — test env unusual: {:?}",
            path
        );
        let canonical = validate_safe_tmp_path(path).expect("valid path must pass");
        assert!(canonical.is_absolute());
        assert!(canonical.starts_with("/tmp/"));
    }

    /// Missing path fails at canonicalize before any other check
    /// runs. Diagnostic names canonicalize so the operator knows
    /// the failure mode.
    #[test]
    fn validate_safe_tmp_path_rejects_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let err = validate_safe_tmp_path(&missing).expect_err("missing path must fail");
        assert!(
            err.contains("canonicalize"),
            "diagnostic should name canonicalize, got: {}",
            err
        );
    }

    /// An existing path that is NOT under /tmp/ — `/etc` is the
    /// portable choice (exists on every Linux host, canonicalizes
    /// cleanly, not under /tmp/).
    #[test]
    fn validate_safe_tmp_path_rejects_off_tmp_path() {
        let err = validate_safe_tmp_path(Path::new("/etc"))
            .expect_err("/etc must reject");
        assert!(
            err.contains("not strictly under /tmp/"),
            "diagnostic mismatch: {}",
            err
        );
    }

    /// Load-bearing safety: a symlink that lives under /tmp/ but
    /// whose target leaves /tmp/ is rejected, because canonicalize
    /// follows the link and the resolved /etc fails the /tmp/
    /// prefix check. A hostile or accidental symlink cannot
    /// redirect the rm -rf out of /tmp/.
    #[test]
    fn validate_safe_tmp_path_rejects_symlink_escaping_tmp() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("escape");
        symlink("/etc", &link).expect("symlink create must work in tempdir");
        let err = validate_safe_tmp_path(&link)
            .expect_err("symlink leaving /tmp/ must reject");
        assert!(
            err.contains("not strictly under /tmp/"),
            "diagnostic mismatch: {}",
            err
        );
    }

    /// A real path under /tmp/ that contains the literal segment
    /// `/home/` — e.g. `/tmp/asm-XXX/home/leak` — is rejected.
    /// Defense against canonical paths that traverse a home dir
    /// via any quirk; check is `contains`, not `starts_with`.
    #[test]
    fn validate_safe_tmp_path_rejects_home_substring() {
        let dir = tempfile::tempdir().unwrap();
        let home_sub = dir.path().join("home").join("leak");
        fs::create_dir_all(&home_sub).expect("nested mkdir must work");
        let err = validate_safe_tmp_path(&home_sub)
            .expect_err("/home/ substring path must reject");
        assert!(
            err.contains("traverses /home/"),
            "diagnostic mismatch: {}",
            err
        );
    }

    /// Character whitelist enforced on real filesystem entries.
    /// Linux filenames accept `*`, `'`, etc. — we create them, then
    /// confirm canonicalize succeeds (the entry exists) but the
    /// whitelist check rejects.
    ///
    /// One file per forbidden char, asserted independently so a
    /// future whitelist relaxation has to deliberately delete the
    /// specific case, not "fix" one catch-all assertion.
    #[test]
    fn validate_safe_tmp_path_rejects_forbidden_chars() {
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            ("star", "a*b", '*'),
            ("squote", "a'b", '\''),
            ("dquote", "a\"b", '"'),
            ("semi", "a;b", ';'),
            ("amp", "a&b", '&'),
            ("pipe", "a|b", '|'),
            ("lt", "a<b", '<'),
            ("gt", "a>b", '>'),
            ("space", "a b", ' '),
            ("dollar", "a$b", '$'),
            ("backtick", "a`b", '`'),
            ("newline", "a\nb", '\n'),
            ("backslash", "a\\b", '\\'),
        ];
        for (slug, name, ch) in cases {
            let bucket = dir.path().join(slug);
            fs::create_dir(&bucket).unwrap();
            let path = bucket.join(name);
            fs::write(&path, b"").unwrap_or_else(|e| {
                panic!("create file {:?}: {}", path, e);
            });
            let err = match validate_safe_tmp_path(&path) {
                Ok(_) => panic!("must reject {:?}", ch),
                Err(e) => e,
            };
            assert!(
                err.contains("non-whitelisted char"),
                "{:?}: diagnostic mismatch: {}",
                ch,
                err
            );
        }
    }
}
