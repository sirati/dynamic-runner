//! Atomic staged-publish: move a file from a staging directory into a
//! destination tree, with the strongest atomicity guarantee the
//! filesystem allows.
//!
//! Single concern: given `(src, dst, src_root)`, deliver `dst` from
//! `src`'s contents in a way that survives a power loss between any
//! two syscalls without ever exposing a half-written `dst`.
//!
//! Strategy:
//!
//! * Same filesystem → `rename(2)` is atomic by definition; one
//!   syscall, done.
//! * Different filesystems → copy `src` to a temp in `staging_dir` (a
//!   hidden dir on the SAME filesystem as `dst`, OUTSIDE the published
//!   content tree), `fsync` the data, then `rename(2)` the temp over
//!   `dst` (intra-FS, atomic). Finally `fsync` `dst.parent()` so the
//!   rename itself is durable, and unlink `src`. Staging out of the
//!   content tree means a crash-orphaned temp never pollutes the
//!   published tree and a content walker never enumerates it.
//!
//! `src_root` is the caller's allow-list root: the publish refuses
//! to move a file that is not under `src_root`. Frameworks set this
//! to the staging mount path (e.g. `/app/out-tmp`) so a worker that
//! accidentally points the API at a path outside the staging area
//! fails fast instead of moving arbitrary files.
//!
//! ## Batch transaction ([`publish_all`])
//!
//! A multi-file publish runs in two phases so an interruption during
//! the slow part never exposes a partial *final* tree:
//!
//! * **Phase 1 (stage):** `copy_with_fsync` every cross-FS src to a
//!   `.publish-tmp` temp in `staging_dir`. Same-FS items (by `st_dev`)
//!   need no staging and are collected for the rename phase as-is. Each
//!   staged temp is owned by a [`StagedTemp`] RAII guard that reaps the
//!   file on drop unless the commit rename consumed it — so any failure
//!   or early-return between stage and successful rename (a phase-1
//!   copy error, a mid-batch phase-2 rename error, an EXDEV-fallback
//!   failure, a panic) leaves no temp behind. No final path is touched
//!   on a phase-1 failure, so the destination tree is unchanged.
//! * **Phase 2 (commit):** `rename(2)` every item back-to-back (each
//!   an intra-FS metadata commit, sub-ms on one NFS dir), fsync the
//!   touched parents, then unlink the staged srcs. This phase runs
//!   under a [`SignalMaskGuard`] that holds {SIGTERM,SIGINT,SIGHUP}
//!   pending so a catchable signal cannot tear the process apart
//!   mid-rename-loop; the signals are delivered normally on drop.
//! * **EXDEV fallback:** `st_dev` equality is only a hint — two bind
//!   mounts of one filesystem share it yet `rename` across them returns
//!   `EXDEV`. A "same-FS" rename that hits `EXDEV` is therefore not a
//!   failure: the item is deferred, copied to a `.publish-tmp` temp in
//!   `staging_dir` *outside* the signal mask (the copy is slow; the
//!   mask must only cover sub-ms renames), then committed by an
//!   intra-dst rename. This restores the cross-FS fallback the original
//!   `publish_one` had.
//!
//! [`publish_one`] is the single-item case, re-expressed as a
//! one-element [`publish_all`] so there is exactly one transaction
//! implementation.
//!
//! ## Stale-temp sweep ([`sweep_stale_tmps`])
//!
//! Hard kills (SIGKILL/SIGSTOP, power loss) can leave `.publish-tmp`
//! temps behind in `staging_dir`. [`sweep_stale_tmps`] reaps them from
//! that single directory (the caller points it at the same
//! `<dst_root>/.publish-tmp` the publish stages into — one flat, known
//! place, so the sweep never has to walk the published content tree).
//! But `staging_dir` is **shared across hosts** (multiple secondaries
//! write the same NFS tree) and pid is not host-unique — a blind
//! glob-delete would race a live sibling's in-flight temp. The sweep is
//! therefore scoped to the current host (via a host token embedded in
//! the temp name) and skips temps whose pid is still alive locally. See
//! [`sweep_stale_tmps`] for the full safety argument.
//!
//! The crate is deliberately small. Higher-level concerns (queueing,
//! drain-before-Done coordination, deployment-specific mount paths)
//! live in the caller.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use nix::sys::signal::{SigSet, SigmaskHow, Signal, sigprocmask};
use nix::unistd::{Pid, gethostname};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("source path is not under src_root: src={src:?} src_root={src_root:?}")]
    SourceOutsideRoot { src: PathBuf, src_root: PathBuf },

    #[error("source path could not be canonicalized: {path:?}: {source}")]
    SourceMissing {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("destination parent could not be created: {path:?}: {source}")]
    DestinationParentCreate {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "destination parent path is a file, not a directory: {path:?} \
         (a file with this name already exists where a directory is needed; \
         common cause: source corpus and output tree share inodes via \
         `cp -al` and the source name collides with a directory the worker \
         needs to create — point --slurm-root-folder at a fresh path or \
         remove the colliding file)"
    )]
    DestinationParentIsFile { path: PathBuf },

    #[error("cross-FS copy failed: src={src:?} tmp={tmp:?}: {source}")]
    Copy {
        src: PathBuf,
        tmp: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("fsync failed for {path:?}: {source}")]
    Fsync {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("rename failed: from={from:?} to={to:?}: {source}")]
    Rename {
        from: PathBuf,
        to: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("post-publish source unlink failed: {path:?}: {source}")]
    Unlink {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("signal mask {op} failed: {source}")]
    SignalMask {
        op: &'static str,
        #[source]
        source: nix::errno::Errno,
    },
}

/// Move `src` to `dst` atomically. `src` must be under `src_root`.
///
/// On the same filesystem this collapses to a single `rename(2)`.
/// Across filesystems it copies to a temp in `staging_dir` (a hidden
/// dir on the SAME filesystem as `dst`, OUTSIDE the published content
/// tree, so the final commit step is itself an intra-FS rename),
/// `fsync`s the data, renames over `dst`, `fsync`s `dst.parent()` for
/// rename durability, and unlinks `src` last.
///
/// Always-overwrite: if `dst` exists, it is replaced. Callers gate
/// "should I publish at all?" upstream (handler-level skip-existing
/// logic); reaching this function means publish is intended.
pub fn publish_one(
    src: &Path,
    dst: &Path,
    src_root: &Path,
    staging_dir: &Path,
) -> Result<(), PublishError> {
    // One transaction implementation: the single-item case is a
    // one-element batch. Same final state as a standalone rename/copy
    // (a one-item batch has nothing to interleave), so no behaviour
    // change for existing callers.
    publish_all(
        std::slice::from_ref(&(src.to_path_buf(), dst.to_path_buf())),
        src_root,
        staging_dir,
    )
}

/// Atomically publish a batch of `(src, dst)` items as one staged
/// transaction. Every `src` must be under `src_root`.
///
/// The batch runs in two phases (see the module docs):
///
/// 1. **Stage** every cross-FS src to a temp in `staging_dir`. A
///    failure here unwinds the temps staged *this batch* (via each
///    [`StagedTemp`]'s drop) and returns the error; no final path is
///    touched.
/// 2. **Commit** every item with `rename(2)` back-to-back under a
///    [`SignalMaskGuard`], then fsync the touched parents and unlink
///    the staged srcs.
///
/// `staging_dir` is a hidden directory on the SAME filesystem as the
/// destinations, OUTSIDE the published content tree (the caller wires
/// it to `<dst_root>/.publish-tmp`). Same-FS keeps the phase-2 rename
/// atomic; out-of-content-tree means a crash-orphaned temp never
/// pollutes the published tree and a content walker never enumerates
/// it. [`sweep_stale_tmps`] reaps orphans from this same directory.
///
/// Always-overwrite: an existing `dst` is replaced. Callers gate
/// "should I publish at all?" upstream; reaching this function means
/// publish is intended.
///
/// Validation and parent-creation for all items happen in phase 1
/// before any rename — so a bad item (outside `src_root`, missing
/// src, file-vs-dir collision) fails the whole batch with no finals
/// touched.
pub fn publish_all(
    items: &[(PathBuf, PathBuf)],
    src_root: &Path,
    staging_dir: &Path,
) -> Result<(), PublishError> {
    // Production path: the real `rename(2)` is the cross-device oracle.
    // Wrapped in a closure (not passed as `fs::rename` directly) so the
    // higher-ranked `Fn(&Path, &Path)` bound unifies both arg lifetimes.
    publish_all_with(items, src_root, staging_dir, |from, to| {
        fs::rename(from, to)
    })
}

/// [`publish_all`] with the final-commit `rename` injected. The
/// production entry point passes [`fs::rename`]; tests pass a fake that
/// returns `EXDEV` for a chosen `(src, dst)` pair to exercise the
/// bind-mount fallback (two mounts of the same underlying FS share an
/// `st_dev` yet `rename` across them yields `EXDEV`) without the
/// privileges a real bind mount needs. The single concern stays inside
/// this crate; the seam is the one `Fn(&Path, &Path) -> io::Result<()>`
/// the commit step would otherwise call directly.
fn publish_all_with(
    items: &[(PathBuf, PathBuf)],
    src_root: &Path,
    staging_dir: &Path,
    rename: impl Fn(&Path, &Path) -> io::Result<()>,
) -> Result<(), PublishError> {
    let canon_root = fs::canonicalize(src_root).map_err(|e| PublishError::SourceMissing {
        path: src_root.to_path_buf(),
        source: e,
    })?;
    // Ensure the staging dir exists before any cross-FS copy targets it.
    // Created on the destination FS (same FS as every dst) so the
    // staged temp→final rename stays an intra-FS atomic commit. A
    // no-op when it already exists (the common steady-state).
    fs::create_dir_all(staging_dir).map_err(|e| PublishError::DestinationParentCreate {
        path: staging_dir.to_path_buf(),
        source: e,
    })?;

    // PHASE 1 — stage. For each item: validate the src is under root,
    // ensure the dst parent exists, then classify same-FS vs cross-FS.
    // Cross-FS items are copied to a `.publish-tmp` sibling now (the
    // slow, signal-unmasked part). Each staged temp is registered so a
    // later failure unwinds exactly the temps this batch created.
    //
    // `st_dev` here is a fast-path HINT, not the source of truth:
    // distinct mounts of one underlying FS (bind mounts) share an
    // `st_dev`, so a "SameFs" classification can still `EXDEV` at
    // rename time. Phase 2 treats that as the cross-FS case via the
    // EXDEV fallback rather than crashing — the kernel's `rename`
    // result, not `classify`, is the cross-device oracle (restoring
    // the original `publish_one` semantics that #254 dropped).
    let mut commits: Vec<Commit> = Vec::with_capacity(items.len());
    for (src, dst) in items {
        let canon_src = validate_under_root(src, &canon_root)?;
        ensure_dst_parent(dst)?;
        match classify(&canon_src, dst) {
            Classification::SameFs => commits.push(Commit {
                rename_from: canon_src,
                dst: dst.clone(),
                unlink: None,
                guard: None,
            }),
            Classification::CrossFs => {
                let tmp = staging_tmp_path(staging_dir, dst);
                // Arm the cleanup guard BEFORE the copy: the
                // `create_new` open already created the temp, so any
                // failure (copy, fsync, or a later phase) must reap it.
                // `commits` is dropped on the `?`-early-return below,
                // running every guard's `Drop` — no manual unwind.
                let guard = StagedTemp::new(tmp.clone());
                copy_with_fsync(&canon_src, &tmp)?;
                commits.push(Commit {
                    rename_from: tmp,
                    dst: dst.clone(),
                    unlink: Some(canon_src),
                    guard: Some(guard),
                });
            }
        }
    }

    // PHASE 2 — commit. Hold {SIGTERM,SIGINT,SIGHUP} pending for the
    // back-to-back rename loop only (each rename is an intra-FS,
    // sub-ms metadata commit). The guard restores the prior mask on
    // drop, including on the early-return from a failed rename. fsync
    // of parents and unlink of srcs happen after the renames are
    // committed; they are not part of the uninterruptible window
    // (a rename already made the final visible).
    //
    // A SameFs item whose rename returns `EXDEV` (the bind-mount
    // false-positive) is NOT a failure: it is deferred to the cross-FS
    // fallback below. Crucially the deferral collects the item and
    // leaves the masked loop, so its slow copy runs OUTSIDE the mask —
    // the mask must only ever cover sub-ms renames, never a multi-second
    // copy (that would blow the SIGKILL grace, which is the whole reason
    // phase 1 stages first).
    let mut committed: Vec<Commit> = Vec::with_capacity(commits.len());
    let mut exdev_fallback: Vec<Commit> = Vec::new();
    {
        let _mask = SignalMaskGuard::block()?;
        for mut c in commits.drain(..) {
            match rename(&c.rename_from, &c.dst) {
                Ok(()) => {
                    // The rename consumed the staged temp: disarm so
                    // its guard does not remove the now-committed final.
                    if let Some(g) = c.guard.as_mut() {
                        g.disarm();
                    }
                    committed.push(c);
                }
                Err(e) if c.unlink.is_none() && is_cross_device(&e) => {
                    // SameFs hint was wrong (bind mount): no temp staged
                    // for this item yet, so the final dst is untouched.
                    // Defer to the unmasked cross-FS fallback.
                    exdev_fallback.push(c);
                }
                Err(e) => {
                    // Real rename failure. The failed item's `c` and
                    // every still-staged item left in `commits` drop
                    // here, each running its `StagedTemp` guard to reap
                    // its temp — no manual unwind, no mid-batch leak.
                    return Err(PublishError::Rename {
                        from: c.rename_from.clone(),
                        to: c.dst.clone(),
                        source: e,
                    });
                }
            }
        }
    }

    // CROSS-FS FALLBACK — for SameFs items the kernel rejected with
    // `EXDEV`. The copy is the slow part, so it runs here, OUTSIDE the
    // signal mask. Each item is then committed by an intra-dst rename
    // (the temp is a sibling of dst, so this rename cannot `EXDEV`)
    // under a fresh short mask, exactly as a phase-1-staged cross-FS
    // item would have been.
    if !exdev_fallback.is_empty() {
        let mut staged: Vec<Commit> = Vec::with_capacity(exdev_fallback.len());
        for c in exdev_fallback {
            // c.rename_from is the canonical src (SameFs items never
            // had a temp); c.unlink is None.
            let canon_src = c.rename_from;
            let tmp = staging_tmp_path(staging_dir, &c.dst);
            // Arm the guard before the copy: on a copy failure the
            // early-return drops `staged` (every prior guard) and this
            // `guard`, reaping all temps this fallback pass created. The
            // finals already committed above are genuine successes and
            // are left in place (an EXDEV item whose copy fails is
            // uncommittable — same exposure as the original per-item
            // cross-FS path).
            let guard = StagedTemp::new(tmp.clone());
            copy_with_fsync(&canon_src, &tmp)?;
            staged.push(Commit {
                rename_from: tmp,
                dst: c.dst,
                unlink: Some(canon_src),
                guard: Some(guard),
            });
        }
        {
            let _mask = SignalMaskGuard::block()?;
            for mut c in staged.drain(..) {
                match rename(&c.rename_from, &c.dst) {
                    Ok(()) => {
                        if let Some(g) = c.guard.as_mut() {
                            g.disarm();
                        }
                        committed.push(c);
                    }
                    Err(e) => {
                        // `c` and the still-staged remainder of `staged`
                        // drop here, each guard reaping its temp.
                        return Err(PublishError::Rename {
                            from: c.rename_from.clone(),
                            to: c.dst.clone(),
                            source: e,
                        });
                    }
                }
            }
        }
    }

    // Durability + cleanup, outside the mask: fsync every touched
    // parent directory so the renames survive power loss, then unlink
    // the staged srcs (cross-FS only — same-FS items were moved by
    // the rename itself).
    for c in &committed {
        fsync_parent(&c.dst)?;
    }
    for c in &committed {
        if let Some(src) = &c.unlink {
            fs::remove_file(src).map_err(|e| PublishError::Unlink {
                path: src.clone(),
                source: e,
            })?;
        }
    }
    Ok(())
}

/// One phase-2 commit: rename `rename_from` over `dst`, and (cross-FS
/// only) unlink the original src afterwards. For same-FS items
/// `rename_from` is the canonical src itself and `unlink` is `None`.
///
/// For cross-FS items `rename_from` is a staged temp owned by `guard`:
/// the [`StagedTemp`] removes that file on drop unless it is disarmed
/// (the rename consumed it). This is the belt-and-suspenders that makes
/// every error/early-return path between stage and successful rename
/// leak-free — a `?`-propagated error, a mid-batch rename failure, or a
/// panic all run the guard's `Drop` and reap the orphan. Same-FS items
/// carry no guard (`None`): their `rename_from` is the caller's src,
/// not a temp we own.
struct Commit {
    rename_from: PathBuf,
    dst: PathBuf,
    unlink: Option<PathBuf>,
    guard: Option<StagedTemp>,
}

/// RAII owner of a staged `.publish-tmp` file. On drop it best-effort
/// removes the file unless [`disarm`](StagedTemp::disarm)ed, so a
/// staged temp is reaped on EVERY failure path (error propagation,
/// mid-batch rename error, EXDEV-fallback failure, panic) without each
/// path open-coding a `remove_file`. The successful phase-2 rename
/// consumes the temp, so it disarms the guard.
///
/// Best-effort by design: a removal error during drop is ignored — the
/// originating error is the one that matters, and any residue is reaped
/// by [`sweep_stale_tmps`] on the next run.
struct StagedTemp {
    path: PathBuf,
    armed: bool,
}

impl StagedTemp {
    fn new(path: PathBuf) -> Self {
        StagedTemp { path, armed: true }
    }

    /// The rename consumed the temp (it no longer exists under this
    /// name): stop the drop from attempting a redundant remove.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagedTemp {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

enum Classification {
    SameFs,
    CrossFs,
}

/// Classify an item by comparing the device of the (already-validated)
/// src against the device of the (already-created) dst parent. The
/// phase-2 rename always targets `dst.parent()`, so this is the exact
/// device the commit rename runs within.
///
/// `st_dev` is a fast-path HINT, not the cross-device oracle. A
/// matching `st_dev` usually means `rename(2)` will succeed in one
/// syscall, so we skip the copy. But it is NOT sufficient: two mounts
/// of the same underlying filesystem (bind mounts — common when a
/// container binds one host FS to both `/app/out-tmp` and
/// `/app/out-network`) share an `st_dev` yet `rename` across them
/// returns `EXDEV`. So `classify` may answer "SameFs" for a pair the
/// kernel will reject; phase 2 catches that `EXDEV` and routes the item
/// through the cross-FS copy fallback. The kernel's `rename` result,
/// not this `st_dev` test, is the authority — which is why the false
/// "same-FS" case is handled rather than asserted impossible. A false
/// "cross-FS" (different `st_dev` but rename would have worked) only
/// costs a needless copy, never a wrong result.
fn classify(canon_src: &Path, dst: &Path) -> Classification {
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    match (fs::metadata(canon_src), fs::metadata(parent)) {
        (Ok(s), Ok(p)) if s.dev() == p.dev() => Classification::SameFs,
        // Either a stat failed (let phase 2 surface the real I/O
        // error) or the devices differ → take the cross-FS staging
        // path, which is correct in both cases.
        _ => Classification::CrossFs,
    }
}

/// Whether a `rename(2)` error is the cross-device rejection (`EXDEV`).
/// This is the kernel's authoritative cross-FS signal — the original
/// `publish_one` keyed its copy fallback off exactly this, and phase 2
/// restores that behaviour for `st_dev`-false-positive (bind-mount)
/// items. `io::ErrorKind::CrossesDevices` is still unstable, so match
/// the raw errno (re-exported via `nix::libc`) for toolchain stability.
fn is_cross_device(e: &io::Error) -> bool {
    e.raw_os_error() == Some(nix::libc::EXDEV)
}

/// Resolve symlinks on `src` and verify it sits under the
/// already-canonicalized `src_root`, returning the canonical src.
fn validate_under_root(src: &Path, canon_root: &Path) -> Result<PathBuf, PublishError> {
    let canon_src = fs::canonicalize(src).map_err(|e| PublishError::SourceMissing {
        path: src.to_path_buf(),
        source: e,
    })?;
    if !canon_src.starts_with(canon_root) {
        return Err(PublishError::SourceOutsideRoot {
            src: canon_src,
            src_root: canon_root.to_path_buf(),
        });
    }
    Ok(canon_src)
}

/// Create `dst`'s parent directory tree, surfacing the file-vs-dir
/// collision case as a targeted [`PublishError::DestinationParentIsFile`].
fn ensure_dst_parent(dst: &Path) -> Result<(), PublishError> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            // `create_dir_all` returns EEXIST when any ancestor along
            // the path is already a regular file (the kernel won't
            // overwrite a file with a directory of the same name).
            // Surface that case with a targeted error so operators
            // immediately see the file-vs-directory collision rather
            // than chasing a "could not be created" generic.
            if e.kind() == std::io::ErrorKind::AlreadyExists
                && let Some(culprit) = first_existing_file_ancestor(parent)
            {
                return PublishError::DestinationParentIsFile { path: culprit };
            }
            PublishError::DestinationParentCreate {
                path: parent.to_path_buf(),
                source: e,
            }
        })?;
    }
    Ok(())
}

/// Walk `path` from the root down looking for the first ancestor that
/// exists as a regular file (not a directory). Returned path is the
/// culprit `create_dir_all` tripped on. None when every existing
/// ancestor is a directory (the EEXIST originated elsewhere — caller
/// falls back to the generic `DestinationParentCreate` error).
fn first_existing_file_ancestor(path: &Path) -> Option<PathBuf> {
    let mut acc = PathBuf::new();
    for component in path.components() {
        acc.push(component.as_os_str());
        match fs::symlink_metadata(&acc) {
            Ok(md) if md.file_type().is_file() => return Some(acc),
            Ok(_) => continue,
            Err(_) => return None,
        }
    }
    None
}

/// Infix that marks a sibling as a publish staging temp. The full
/// name is `.{name}.{INFIX}.{host}.{pid}.{nanos}` — see
/// [`sibling_tmp_path`] (builder) and [`parse_tmp_token`] (sweep
/// parser), which are the two sides of this one convention.
const TMP_INFIX: &str = "publish-tmp";

/// The current host as a dot-free token. The hostname is the sweep's
/// shared-NFS safety boundary (own-host scope), and dots are the
/// field separator in the temp name, so any `.` (and any other
/// non-`[A-Za-z0-9_-]`) is collapsed to `_` to keep the token a
/// single unambiguous field. Falls back to `unknown-host` if the
/// syscall fails — a stable token still scopes the sweep correctly
/// (every process on this host shares the same fallback).
fn host_token() -> String {
    let raw = gethostname()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());
    sanitize_token(&raw)
}

fn sanitize_token(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() {
        out.push_str("unknown-host");
    }
    out
}

/// Build the staging-temp path for `dst` inside `staging_dir`.
///
/// The temp lands in `staging_dir` (a hidden directory on the SAME
/// filesystem as `dst`, OUTSIDE the published content tree), NOT as a
/// sibling of `dst`. Same-FS keeps the phase-2 `rename(2)` atomic;
/// out-of-content-tree means a crash-orphaned temp never pollutes the
/// published tree and a content walker never enumerates it.
///
/// The base name still encodes `dst`'s file name plus the
/// `{host}.{pid}.{nanos}` token so [`parse_tmp_token`] can scope the
/// sweep and two concurrent publishes of the same `dst` never collide.
/// The encoded name flattens `dst`'s own path separators (the temp is
/// flat in `staging_dir`, no nested dirs to create), so the leading
/// `.{name}` is `dst.file_name()` only — sufficient for uniqueness
/// given the `{pid}.{nanos}` suffix.
fn staging_tmp_path(staging_dir: &Path, dst: &Path) -> PathBuf {
    let name = dst
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "publish".to_string());
    let host = host_token();
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    staging_dir.join(format!(".{name}.{TMP_INFIX}.{host}.{pid}.{nanos}"))
}

fn copy_with_fsync(src: &Path, tmp: &Path) -> Result<(), PublishError> {
    let mut src_file = File::open(src).map_err(|e| PublishError::Copy {
        src: src.to_path_buf(),
        tmp: tmp.to_path_buf(),
        source: e,
    })?;
    let mut tmp_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)
        .map_err(|e| PublishError::Copy {
            src: src.to_path_buf(),
            tmp: tmp.to_path_buf(),
            source: e,
        })?;
    io::copy(&mut src_file, &mut tmp_file).map_err(|e| PublishError::Copy {
        src: src.to_path_buf(),
        tmp: tmp.to_path_buf(),
        source: e,
    })?;
    tmp_file.sync_all().map_err(|e| PublishError::Fsync {
        path: tmp.to_path_buf(),
        source: e,
    })?;
    Ok(())
}

fn fsync_parent(dst: &Path) -> Result<(), PublishError> {
    // fsync(2) on a directory fd flushes directory metadata so the
    // rename(2) we just performed is durable across power loss.
    // Linux behaviour; safe on POSIX targets the framework supports.
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    let dir = File::open(parent).map_err(|e| PublishError::Fsync {
        path: parent.to_path_buf(),
        source: e,
    })?;
    dir.sync_all().map_err(|e| PublishError::Fsync {
        path: parent.to_path_buf(),
        source: e,
    })?;
    Ok(())
}

/// The catchable signals deferred across the rename phase. Mirrors the
/// terminating set the slurm-wrapper traps (SIGTERM/SIGINT/SIGHUP);
/// SIGQUIT is omitted because it is the operator's deliberate
/// "core-dump now" escape hatch and should not be deferred. SIGKILL
/// and SIGSTOP are unblockable by the kernel — see the
/// [`SignalMaskGuard`] limitation note.
const DEFERRED: &[Signal] = &[Signal::SIGTERM, Signal::SIGINT, Signal::SIGHUP];

/// RAII guard that blocks {SIGTERM,SIGINT,SIGHUP} for the duration of
/// the critical section (the back-to-back rename phase), restoring the
/// caller's prior mask on drop — including on an early return through
/// `?`. Blocked signals are held **pending** by the kernel (not
/// dropped); the moment the guard drops they are delivered and the
/// worker's normal handler (`KeyboardInterrupt` / `SystemExit`) runs.
///
/// Mirrors the production `sigprocmask` pattern in
/// `slurm-wrapper/wrapper/src/signals.rs` (`block_signals` uses
/// `SIG_BLOCK`; the mask is restored here via `SIG_SETMASK` to the
/// saved set rather than `SIG_UNBLOCK`, so a signal the caller had
/// already blocked stays blocked). The worker runs the publish on its
/// single main thread, so per-process `sigprocmask` is the consistent
/// choice (the wrapper uses the same call).
///
/// # Limitation
///
/// SIGKILL and SIGSTOP cannot be blocked or caught by any process —
/// this guard does nothing against them. The complementary mitigations
/// for a hard kill mid-publish are [`publish_all`]'s stage-first
/// ordering (an interruption before phase 2 leaves only `.publish-tmp`
/// temps in `staging_dir`, never a partial final and never a temp in
/// the published content tree) and [`sweep_stale_tmps`] reaping those
/// leftovers from `staging_dir` on the next run.
struct SignalMaskGuard {
    /// The mask in effect before `block`, restored on drop.
    saved: SigSet,
}

impl SignalMaskGuard {
    fn block() -> Result<Self, PublishError> {
        let mut to_block = SigSet::empty();
        for &sig in DEFERRED {
            to_block.add(sig);
        }
        let mut saved = SigSet::empty();
        sigprocmask(SigmaskHow::SIG_BLOCK, Some(&to_block), Some(&mut saved)).map_err(|e| {
            PublishError::SignalMask {
                op: "block",
                source: e,
            }
        })?;
        Ok(SignalMaskGuard { saved })
    }
}

impl Drop for SignalMaskGuard {
    fn drop(&mut self) {
        // Restore the exact prior mask (SIG_SETMASK, not SIG_UNBLOCK)
        // so signals the caller had already blocked are not
        // accidentally unblocked. Drop cannot return an error; a
        // failure here is unrecoverable and would only happen on a
        // malformed mask, which cannot occur for a saved set.
        let _ = sigprocmask(SigmaskHow::SIG_SETMASK, Some(&self.saved), None);
    }
}

/// Reap stale `.{name}.{TMP_INFIX}.{host}.{pid}.{nanos}` temps left in
/// `dir` by a hard kill (SIGKILL/power loss) that bypassed
/// [`SignalMaskGuard`] and the normal cleanup paths. Returns the
/// number of temps removed.
///
/// `dir` is the publish staging directory (`<dst_root>/.publish-tmp`):
/// the single flat place every cross-FS publish stages into, so the
/// sweep reaps from exactly one directory and never has to walk the
/// published content tree. The reap is non-recursive by design — temps
/// are flat in `staging_dir`, never nested under it.
///
/// # Shared-NFS safety (load-bearing)
///
/// `dir` is **shared** across hosts: multiple secondaries on different
/// hosts stage into the same NFS `staging_dir`, and a worker's pid is
/// unique only on its own host — pid 1234 on host A is a different
/// process than pid 1234 on host B. A naive glob-and-delete of
/// `*.publish-tmp.*` would therefore race a *live* sibling on another
/// host whose copy is still in flight, deleting valid staged data.
///
/// The sweep is safe because the temp name embeds a **host token** and
/// the pid:
///
/// 1. **Host scope.** Only temps whose embedded host equals *this*
///    host ([`host_token`]) are considered. Another host's in-flight
///    temps are never touched — their host token differs.
/// 2. **Local pid-liveness.** Among own-host temps, one whose embedded
///    pid is still alive (`kill(pid, 0)` ≠ ESRCH) belongs to a live
///    same-host process (a sibling worker, or this process itself) and
///    is **skipped**. Because step 1 already fixed the host to ours,
///    the pid is meaningful in *our* local process table, so this
///    check is sound (it would be meaningless against a foreign host's
///    pid namespace — which is exactly why host-scoping must come
///    first). The intended target is the asm-tokenizer strand: a
///    restarted worker (a *new* pid) reaping temps from its crashed
///    prior run (the *old* pid, now dead) without racing any live
///    worker. The fresh process holds no temps of its own yet — it
///    sweeps before it publishes.
///
/// Net effect: only genuinely-orphaned own-host temps are removed; no
/// live worker's staging is ever raced.
pub fn sweep_stale_tmps(dir: &Path) -> Result<usize, PublishError> {
    let host = host_token();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        // A missing destination dir means nothing to sweep yet.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(PublishError::Fsync {
                path: dir.to_path_buf(),
                source: e,
            });
        }
    };

    let mut removed = 0usize;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some((tmp_host, pid)) = parse_tmp_token(&name) else {
            continue;
        };
        if tmp_host != host {
            // Step 1: never touch another host's temps.
            continue;
        }
        if pid_alive(pid) {
            // Step 2: a live same-host sibling owns this temp.
            continue;
        }
        if fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    Ok(removed)
}

/// Parse the `{host}` and `{pid}` fields out of a publish-temp file
/// name, or `None` if the name is not a publish temp. Mirrors the
/// builder in [`sibling_tmp_path`] — the two are the only knowers of
/// the `.{name}.{TMP_INFIX}.{host}.{pid}.{nanos}` layout.
///
/// Parsing splits on `.` from the *right*: the last two fields are
/// `nanos` and `pid`, the field before them is the (sanitized,
/// dot-free) host. The leading `.{name}.{TMP_INFIX}` may itself
/// contain dots (the published file name can), so the infix is located
/// by scanning for a `{TMP_INFIX}` segment that is followed by exactly
/// `host.pid.nanos`.
fn parse_tmp_token(name: &str) -> Option<(String, i32)> {
    // A publish temp always starts with a dot.
    if !name.starts_with('.') {
        return None;
    }
    let infix = format!(".{TMP_INFIX}.");
    // The trailing `host.pid.nanos` follows the LAST occurrence of the
    // infix, so a published file literally named like the infix can't
    // confuse the parser.
    let idx = name.rfind(&infix)?;
    let tail = &name[idx + infix.len()..];
    // tail = "host.pid.nanos" — host is dot-free (sanitized), pid and
    // nanos are integers. Split off the last two integer fields.
    let mut parts = tail.rsplitn(3, '.');
    let _nanos = parts.next()?;
    let pid_str = parts.next()?;
    let host = parts.next()?;
    if host.is_empty() {
        return None;
    }
    let pid: i32 = pid_str.parse().ok()?;
    Some((host.to_string(), pid))
}

/// Whether `pid` names a live process in the LOCAL process table.
/// `kill(pid, None)` performs the permission/existence probe without
/// sending a signal: `ESRCH` ⇒ no such process (stale), any other
/// result (incl. `EPERM`, which means the pid exists but is owned by
/// someone else) ⇒ treat as alive and do not reap.
fn pid_alive(pid: i32) -> bool {
    !matches!(
        nix::sys::signal::kill(Pid::from_raw(pid), None),
        Err(nix::errno::Errno::ESRCH)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    fn read_file(path: &Path) -> Vec<u8> {
        fs::read(path).unwrap()
    }

    /// The hidden staging dir for a destination root, mirroring the
    /// Python glue's `<dst_root>/.publish-tmp` convention. Same FS as
    /// the destinations (under the same root) and out of the published
    /// content tree.
    fn staging_in(dst_root: &Path) -> PathBuf {
        dst_root.join(".publish-tmp")
    }

    fn same_device(a: &Path, b: &Path) -> bool {
        fs::metadata(a).unwrap().dev() == fs::metadata(b).unwrap().dev()
    }

    #[test]
    fn destination_parent_is_file_surfaces_targeted_error() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();
        // A file lives at the path the worker expects to be a directory.
        let collision = dst_root.join("dataset").join("hello.tar.zst");
        fs::create_dir_all(collision.parent().unwrap()).unwrap();
        write_file(&collision, b"this is a file, not a directory");
        // Source is set up in the staging tree.
        let src = src_root.join("payload");
        write_file(&src, b"payload bytes");
        // Worker tries to publish to a path under the collision file
        // (e.g. an archive's per-member output).
        let dst = collision.join("member-output.csv");
        let err = publish_one(&src, &dst, &src_root, &staging_in(&dst_root)).unwrap_err();
        match err {
            PublishError::DestinationParentIsFile { path } => {
                assert_eq!(path, collision);
            }
            other => panic!("expected DestinationParentIsFile, got {other:?}"),
        }
    }

    #[test]
    fn error_display_contains_paths() {
        let e = PublishError::SourceOutsideRoot {
            src: PathBuf::from("/tmp/x"),
            src_root: PathBuf::from("/app/out-tmp"),
        };
        let msg = format!("{e}");
        assert!(msg.contains("/tmp/x"));
        assert!(msg.contains("/app/out-tmp"));
    }

    #[test]
    fn intra_fs_uses_single_rename() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let src = src_root.join("payload.bin");
        let dst = dst_root.join("out/payload.bin");
        write_file(&src, b"hello");

        publish_one(&src, &dst, &src_root, &staging_in(&dst_root)).unwrap();

        assert!(dst.exists(), "dst missing after publish");
        assert_eq!(read_file(&dst), b"hello");
        assert!(!src.exists(), "src not removed");

        let leftover = fs::read_dir(dst_root.join("out"))
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains("publish-tmp"));
        assert!(!leftover, "tmp sibling left behind");
    }

    #[test]
    fn dst_parent_auto_created() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        fs::create_dir_all(&src_root).unwrap();
        let src = src_root.join("a.bin");
        write_file(&src, b"x");

        let dst = root.path().join("network/deeply/nested/a.bin");
        assert!(!dst.parent().unwrap().exists());

        publish_one(&src, &dst, &src_root, &staging_in(&root.path().join("network"))).unwrap();
        assert!(dst.exists());
    }

    #[test]
    fn dst_overwritten_when_present() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let dst = dst_root.join("doc.txt");
        write_file(&dst, b"old");

        let src = src_root.join("doc.txt");
        write_file(&src, b"new");

        publish_one(&src, &dst, &src_root, &staging_in(&dst_root)).unwrap();
        assert_eq!(read_file(&dst), b"new");
    }

    #[test]
    fn source_outside_root_rejected() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let outside = root.path().join("outside");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&outside).unwrap();

        let src = outside.join("escape.bin");
        write_file(&src, b"nope");
        let dst = root.path().join("network/escape.bin");

        let err = publish_one(&src, &dst, &src_root, &staging_in(&root.path().join("network")))
            .unwrap_err();
        match err {
            PublishError::SourceOutsideRoot { .. } => {}
            other => panic!("expected SourceOutsideRoot, got {other:?}"),
        }
        assert!(src.exists(), "src must not be moved when validation fails");
        assert!(!dst.exists());
    }

    #[test]
    fn source_missing_returns_error() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        fs::create_dir_all(&src_root).unwrap();

        let src = src_root.join("never-existed.bin");
        let dst = root.path().join("network/x.bin");

        let err = publish_one(&src, &dst, &src_root, &staging_in(&root.path().join("network")))
            .unwrap_err();
        match err {
            PublishError::SourceMissing { .. } => {}
            other => panic!("expected SourceMissing, got {other:?}"),
        }
    }

    /// Cross-FS path uses the copy + fsync + rename + unlink branch.
    /// Only runs when /tmp and a separate test directory live on
    /// distinct filesystems (often tmpfs vs. the user's $HOME). When
    /// they're on the same filesystem the test silently passes via
    /// the fallthrough — the intra-FS path is already covered by
    /// `intra_fs_uses_single_rename`.
    #[test]
    fn cross_fs_falls_back_to_copy_fsync_rename() {
        // Try to get two paths on different filesystems: src on
        // /tmp (commonly tmpfs), dst under $HOME (typically a disk
        // FS). If they're on the same device, skip the cross-FS
        // assertions but exercise the rename branch anyway.
        let src_root = tempfile::tempdir_in("/tmp").unwrap();
        let dst_root = match tempfile::tempdir_in(
            std::env::var_os("HOME").unwrap_or_else(|| "/var/tmp".into()),
        ) {
            Ok(d) => d,
            Err(_) => return,
        };

        let src = src_root.path().join("blob.bin");
        let dst = dst_root.path().join("out/blob.bin");
        write_file(&src, b"contents");

        let cross = !same_device(src_root.path(), dst_root.path());
        let staging = staging_in(dst_root.path());

        publish_one(&src, &dst, src_root.path(), &staging).unwrap();

        assert!(dst.exists(), "dst missing");
        assert_eq!(read_file(&dst), b"contents");
        assert!(!src.exists(), "src not removed");
        if cross {
            // No tmp left in dst's content dir (it never was a sibling
            // there) — and the staging dir is drained after a success.
            let leftover = fs::read_dir(dst.parent().unwrap())
                .unwrap()
                .filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().contains("publish-tmp"));
            assert!(!leftover, "cross-FS tmp left in content dir");
            assert!(!any_tmp_in(&staging), "cross-FS tmp left in staging dir");
        }
    }

    // ---- C.1: batch transaction ----

    fn any_tmp_in(dir: &Path) -> bool {
        fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .any(|e| e.file_name().to_string_lossy().contains(TMP_INFIX))
            })
            .unwrap_or(false)
    }

    #[test]
    fn batch_publishes_all_items_no_temps_left() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let mut items = Vec::new();
        for i in 0..3 {
            let src = src_root.join(format!("p{i}.bin"));
            write_file(&src, format!("payload-{i}").as_bytes());
            items.push((src, dst_root.join(format!("out/p{i}.bin"))));
        }

        publish_all(&items, &src_root, &staging_in(&dst_root)).unwrap();

        for (i, (src, dst)) in items.iter().enumerate() {
            assert!(dst.exists(), "dst {i} missing");
            assert_eq!(read_file(dst), format!("payload-{i}").as_bytes());
            assert!(!src.exists(), "src {i} not removed");
        }
        assert!(!any_tmp_in(&dst_root.join("out")), "temp left behind");
    }

    /// Phase ordering: the whole batch is validated/staged in phase 1
    /// before ANY rename in phase 2. We make a LATER item fail
    /// validation (outside src_root); because no commit runs until
    /// every item passes phase 1, the EARLIER valid item's final must
    /// NOT exist — proving renames don't interleave with staging.
    /// And the partial-fail contract: no partial finals, no leftover
    /// temps from this batch.
    #[test]
    fn batch_phase1_failure_touches_no_finals_and_leaves_no_temps() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        let outside = root.path().join("outside");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();
        fs::create_dir_all(&outside).unwrap();

        let good_src = src_root.join("good.bin");
        write_file(&good_src, b"good");
        let good_dst = dst_root.join("out/good.bin");

        let bad_src = outside.join("escape.bin");
        write_file(&bad_src, b"escape");
        let bad_dst = dst_root.join("out/escape.bin");

        let items = vec![
            (good_src.clone(), good_dst.clone()),
            (bad_src.clone(), bad_dst.clone()),
        ];
        let err = publish_all(&items, &src_root, &staging_in(&dst_root)).unwrap_err();
        match err {
            PublishError::SourceOutsideRoot { .. } => {}
            other => panic!("expected SourceOutsideRoot, got {other:?}"),
        }

        // No final committed for either item.
        assert!(
            !good_dst.exists(),
            "earlier item committed before batch validated"
        );
        assert!(!bad_dst.exists(), "rejected item committed");
        // Sources untouched.
        assert!(good_src.exists(), "good src wrongly removed");
        assert!(bad_src.exists(), "bad src wrongly removed");
        // No temp from this batch survives in the content dir or the
        // staging dir.
        assert!(
            !any_tmp_in(&dst_root.join("out")),
            "phase-1 temp left in content dir"
        );
        assert!(
            !any_tmp_in(&staging_in(&dst_root)),
            "phase-1 temp not unwound from staging dir"
        );
    }

    #[test]
    fn publish_one_is_a_one_item_batch() {
        // Behavioural parity: publish_one delegates to publish_all and
        // yields the identical end state for the single-item case.
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let src = src_root.join("one.bin");
        let dst = dst_root.join("out/one.bin");
        write_file(&src, b"single");

        publish_one(&src, &dst, &src_root, &staging_in(&dst_root)).unwrap();
        assert_eq!(read_file(&dst), b"single");
        assert!(!src.exists());
        assert!(!any_tmp_in(&dst_root.join("out")));
    }

    // ---- bind-mount EXDEV fallback (cross-mount, same `st_dev`) ----

    /// A `rename` seam that returns `EXDEV` for a chosen `(from, to)`
    /// pair and delegates to the real `fs::rename` for everything else.
    /// This simulates the bind-mount case the test can't create without
    /// privileges: two mounts of one filesystem share an `st_dev` (so
    /// `classify` answers "SameFs") yet the kernel rejects the
    /// cross-mount rename with `EXDEV`. The pair is matched on the
    /// staging→final direct move; the later intra-dst temp→final rename
    /// (a true sibling rename) is allowed through.
    fn exdev_for(from: PathBuf, to: PathBuf) -> impl Fn(&Path, &Path) -> io::Result<()> {
        move |f: &Path, t: &Path| {
            if f == from && t == to {
                Err(io::Error::from_raw_os_error(nix::libc::EXDEV))
            } else {
                fs::rename(f, t)
            }
        }
    }

    /// REGRESSION (#254 → cross-device crash): a publish whose `st_dev`
    /// says "SameFs" but whose direct `rename` returns `EXDEV` (the
    /// bind-mount false positive) MUST succeed via the copy fallback —
    /// file lands, src removed, no leftover temp, no error. Pre-fix
    /// (#254) there was no `publish_all_with` seam and no fallback: the
    /// direct rename's `EXDEV` propagated as a `PublishError::Rename`,
    /// crashing the publish. This test pins the restored fallback.
    #[test]
    fn same_dev_rename_exdev_falls_back_to_copy() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let src = src_root.join("ncat_function_ranges.txt");
        let dst = dst_root.join("out/x64-clang_ncat_function_ranges.txt");
        write_file(&src, b"function ranges payload");

        // `classify` will see one tempdir filesystem → SameFs, so the
        // commit attempts a DIRECT canon_src→dst rename. The seam makes
        // exactly that rename EXDEV, forcing the fallback.
        let canon_src = fs::canonicalize(&src).unwrap();
        let rename = exdev_for(canon_src.clone(), dst.clone());

        publish_all_with(
            std::slice::from_ref(&(src.clone(), dst.clone())),
            &src_root,
            &staging_in(&dst_root),
            rename,
        )
        .expect("EXDEV on a same-st_dev rename must fall back, not fail");

        assert!(dst.exists(), "dst missing after EXDEV fallback");
        assert_eq!(read_file(&dst), b"function ranges payload");
        assert!(!src.exists(), "src not removed after fallback");
        assert!(
            !any_tmp_in(&dst_root.join("out")),
            "fallback temp left in content dir"
        );
        assert!(
            !any_tmp_in(&staging_in(&dst_root)),
            "fallback temp left in staging dir"
        );
    }

    /// The fallback is per-item: in a mixed batch, the non-EXDEV item
    /// still commits via the single fast-path rename (no copy, no temp)
    /// while only the EXDEV item is copied. Asserts both land, both srcs
    /// gone, no temps left.
    #[test]
    fn mixed_batch_only_exdev_item_uses_fallback() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging");
        let dst_root = root.path().join("network");
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let fast_src = src_root.join("fast.bin");
        let fast_dst = dst_root.join("out/fast.bin");
        write_file(&fast_src, b"fast-path");

        let slow_src = src_root.join("slow.bin");
        let slow_dst = dst_root.join("out/slow.bin");
        write_file(&slow_src, b"fallback-path");

        // Only the slow item's direct rename is forced to EXDEV.
        let slow_canon = fs::canonicalize(&slow_src).unwrap();
        let rename = exdev_for(slow_canon, slow_dst.clone());

        let items = vec![
            (fast_src.clone(), fast_dst.clone()),
            (slow_src.clone(), slow_dst.clone()),
        ];
        publish_all_with(&items, &src_root, &staging_in(&dst_root), rename)
            .expect("mixed batch must succeed");

        assert_eq!(read_file(&fast_dst), b"fast-path");
        assert_eq!(read_file(&slow_dst), b"fallback-path");
        assert!(!fast_src.exists(), "fast src not removed");
        assert!(!slow_src.exists(), "slow src not removed");
        assert!(!any_tmp_in(&dst_root.join("out")), "temp left in content dir");
        assert!(
            !any_tmp_in(&staging_in(&dst_root)),
            "temp left in staging dir"
        );
    }

    // ---- C.2: signal-mask RAII guard ----

    fn current_mask() -> SigSet {
        let mut cur = SigSet::empty();
        // Query the current mask without changing it.
        sigprocmask(SigmaskHow::SIG_BLOCK, None, Some(&mut cur)).unwrap();
        cur
    }

    /// The guard blocks the deferred set for its lifetime and restores
    /// the EXACT prior mask on drop — including when the scope it
    /// guards returns early via `?`. This is single-threaded and
    /// leaves the process mask exactly as found, so it does not leak
    /// into sibling tests.
    #[test]
    fn sigmask_guard_restores_on_early_return() {
        // Baseline: SIGTERM must not be in our mask going in. (Cargo's
        // test harness does not block it.) If a sibling left it set we
        // still get a meaningful before/after comparison.
        let before = current_mask();
        let before_term = before.contains(Signal::SIGTERM);

        // A scope that takes the guard and then early-returns via `?`,
        // exactly as publish_all's phase-2 rename loop does on a
        // rename error.
        fn guarded_early_return() -> Result<(), PublishError> {
            let _mask = SignalMaskGuard::block()?;
            // While held, every deferred signal is blocked.
            assert!(current_mask().contains(Signal::SIGTERM));
            assert!(current_mask().contains(Signal::SIGINT));
            assert!(current_mask().contains(Signal::SIGHUP));
            // Force an early return through `?` — the guard must still
            // run its Drop and restore the prior mask.
            Err(PublishError::Unlink {
                path: PathBuf::from("/forced"),
                source: std::io::Error::other("forced early return"),
            })?;
            Ok(())
        }

        let _ = guarded_early_return();

        let after = current_mask();
        assert_eq!(
            after.contains(Signal::SIGTERM),
            before_term,
            "SIGTERM mask not restored after guarded early return"
        );
    }

    // ---- C.3: stale-temp sweep ----

    fn make_tmp(dir: &Path, name: &str, host: &str, pid: i32, nanos: u128) {
        let f = dir.join(format!(".{name}.{TMP_INFIX}.{host}.{pid}.{nanos}"));
        write_file(&f, b"staged");
    }

    #[test]
    fn parse_tmp_token_round_trips_builder() {
        let staging = Path::new("/dest/.publish-tmp");
        let dst = Path::new("/dest/out/data.tar.zst");
        let tmp = staging_tmp_path(staging, dst);
        // The temp lands flat in the staging dir, not as a sibling of dst.
        assert_eq!(tmp.parent().unwrap(), staging);
        let name = tmp.file_name().unwrap().to_string_lossy().into_owned();
        let (host, pid) = parse_tmp_token(&name).expect("builder output must parse");
        assert_eq!(host, host_token());
        assert_eq!(pid, std::process::id() as i32);
    }

    #[test]
    fn parse_tmp_token_rejects_non_temps() {
        assert!(parse_tmp_token("data.tar.zst").is_none());
        assert!(parse_tmp_token(".data.tar.zst").is_none());
        // Missing the trailing pid/nanos fields.
        assert!(parse_tmp_token(".data.publish-tmp.host").is_none());
        // Non-integer pid.
        assert!(parse_tmp_token(".data.publish-tmp.host.notapid.123").is_none());
    }

    /// Sweep reaps own-host + dead-pid temps; never touches a foreign
    /// host's temps, a live (own-pid) temp, or a non-temp file.
    #[test]
    fn sweep_scopes_to_own_host_and_dead_pids() {
        let dir = tempfile::tempdir().unwrap();
        let host = host_token();
        let dead_pid = i32::MAX; // reliably no such process
        let live_pid = std::process::id() as i32; // this test process

        // (1) own host + dead pid → REAPED.
        make_tmp(dir.path(), "data", &host, dead_pid, 1);
        // (2) own host + live pid → KEPT (live sibling / self).
        make_tmp(dir.path(), "data", &host, live_pid, 2);
        // (3) foreign host + dead pid → KEPT (could be live elsewhere).
        make_tmp(dir.path(), "data", "some-other-host", dead_pid, 3);
        // (4) a normal published file (not a temp) → KEPT.
        write_file(&dir.path().join("data.tar.zst"), b"real output");

        let removed = sweep_stale_tmps(dir.path()).unwrap();
        assert_eq!(removed, 1, "exactly the own-host dead-pid temp reaped");

        let surviving: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        // The reaped one is gone.
        assert!(
            !surviving
                .iter()
                .any(|n| n.ends_with(&format!(".{dead_pid}.1"))),
            "own-host dead-pid temp not reaped"
        );
        // Live-pid temp, foreign-host temp, and real file survive.
        assert!(
            surviving
                .iter()
                .any(|n| n.ends_with(&format!(".{live_pid}.2")))
        );
        assert!(surviving.iter().any(|n| n.contains("some-other-host")));
        assert!(surviving.iter().any(|n| n == "data.tar.zst"));
    }

    #[test]
    fn sweep_missing_dir_is_ok() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("does-not-exist");
        assert_eq!(sweep_stale_tmps(&missing).unwrap(), 0);
    }

    // ---- #676: staged temps live in the staging dir, out of the
    //            content tree, and every failure path reaps them ----

    /// (b) The cross-FS staging temp lands in the STAGING dir, never in
    /// the published content dir, and the final still commits
    /// atomically. Forced cross-FS via the EXDEV seam (the direct
    /// canon_src→dst rename is rejected, routing the item through the
    /// copy-to-staging fallback). On success the content dir holds only
    /// the final and the staging dir is drained.
    #[test]
    fn staged_temp_lives_in_staging_dir_not_content_dir() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging-src");
        let dst_root = root.path().join("network");
        let staging = staging_in(&dst_root);
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let src = src_root.join("payload.bin");
        let dst = dst_root.join("out/dataset/variant/payload.bin");
        write_file(&src, b"deep-nested-payload");

        let canon_src = fs::canonicalize(&src).unwrap();
        // A rename seam that records every staging→final move it sees
        // (so we can assert the temp's parent is the staging dir), then
        // forces EXDEV on the direct move to drive the copy fallback.
        let rename = exdev_for(canon_src, dst.clone());

        publish_all_with(
            std::slice::from_ref(&(src.clone(), dst.clone())),
            &src_root,
            &staging,
            rename,
        )
        .expect("forced cross-FS publish must land the final");

        assert!(dst.exists(), "final not committed");
        assert_eq!(read_file(&dst), b"deep-nested-payload");
        assert!(!src.exists(), "src not consumed");
        // The content dir (every level of it) never held a temp.
        assert!(
            !any_tmp_in(dst.parent().unwrap()),
            "temp polluted the content dir"
        );
        // The staging dir is drained on success.
        assert!(!any_tmp_in(&staging), "staging dir not drained on success");
    }

    /// A `rename` seam that returns a chosen *real* (non-EXDEV) error
    /// for a specific `(from, to)` pair and delegates everything else to
    /// `fs::rename`. Used to drive a genuine phase-2 rename failure so
    /// the staged-temp cleanup (the `StagedTemp` drop) is exercised.
    fn fail_rename_for(
        from: PathBuf,
        to: PathBuf,
        errno: i32,
    ) -> impl Fn(&Path, &Path) -> io::Result<()> {
        move |f: &Path, t: &Path| {
            if f == from && t == to {
                Err(io::Error::from_raw_os_error(errno))
            } else {
                fs::rename(f, t)
            }
        }
    }

    /// (a) A phase-2 rename error on a cross-FS (staged) item leaves NO
    /// temp behind — neither in the content dir nor the staging dir. The
    /// item is routed cross-FS by an EXDEV on its direct move, staged to
    /// the staging dir, and then its fallback temp→final rename is
    /// forced to fail with a real error (EIO). The `StagedTemp` drop on
    /// the error-return must reap the temp.
    ///
    /// REVERT-CHECK: with `StagedTemp::drop` removed (or never armed),
    /// this temp survives in the staging dir — the regression this fix
    /// prevents.
    #[test]
    fn fallback_rename_error_reaps_staged_temp() {
        let root = tempfile::tempdir().unwrap();
        let src_root = root.path().join("staging-src");
        let dst_root = root.path().join("network");
        let staging = staging_in(&dst_root);
        fs::create_dir_all(&src_root).unwrap();
        fs::create_dir_all(&dst_root).unwrap();

        let src = src_root.join("doomed.bin");
        let dst = dst_root.join("out/doomed.bin");
        write_file(&src, b"doomed payload");
        let canon_src = fs::canonicalize(&src).unwrap();

        // Seam: EXDEV the direct canon_src→dst move (drive the fallback),
        // and EIO the fallback's temp→dst move. The temp path is
        // unknown ahead of time (it embeds nanos), so match the EIO on
        // the destination only via a closure that fails ANY rename whose
        // target is `dst` but whose source is NOT canon_src (i.e. the
        // temp→dst commit).
        let canon_for_seam = canon_src.clone();
        let dst_for_seam = dst.clone();
        let rename = move |f: &Path, t: &Path| -> io::Result<()> {
            if t == dst_for_seam {
                if f == canon_for_seam {
                    // direct move → force the cross-FS fallback
                    Err(io::Error::from_raw_os_error(nix::libc::EXDEV))
                } else {
                    // the fallback temp→dst commit → real failure
                    Err(io::Error::from_raw_os_error(nix::libc::EIO))
                }
            } else {
                fs::rename(f, t)
            }
        };

        let err = publish_all_with(
            std::slice::from_ref(&(src.clone(), dst.clone())),
            &src_root,
            &staging,
            rename,
        )
        .expect_err("forced fallback rename error must surface");
        match err {
            PublishError::Rename { .. } => {}
            other => panic!("expected Rename error, got {other:?}"),
        }

        // No final (the commit failed), and NO temp survives anywhere.
        assert!(!dst.exists(), "no final on a failed commit");
        assert!(
            !any_tmp_in(&staging),
            "staged temp leaked into staging dir on rename error"
        );
        assert!(
            !any_tmp_in(&dst_root.join("out")),
            "staged temp leaked into content dir on rename error"
        );
        // Silence the unused-helper warning while keeping the named
        // seam available for future real-error tests.
        let _ = fail_rename_for;
    }

    /// (c) The sweep reaps a dead-pid own-host orphan from the staging
    /// dir EVEN WHEN the published final sibling already exists — the
    /// guard keys on host+pid, not on "a final exists nearby", so a
    /// successfully-published final never shields a dead-pid orphan from
    /// the sweep.
    #[test]
    fn sweep_reaps_dead_pid_orphan_even_with_existing_final() {
        let dst_root = tempfile::tempdir().unwrap();
        let staging = staging_in(dst_root.path());
        fs::create_dir_all(&staging).unwrap();
        let host = host_token();
        let dead_pid = i32::MAX;

        // The successfully-published final lives in the content tree.
        write_file(&dst_root.path().join("out/data.tar.zst"), b"published");
        // A dead-pid orphan for the SAME logical output sits in staging.
        make_tmp(&staging, "data.tar.zst", &host, dead_pid, 42);

        let removed = sweep_stale_tmps(&staging).unwrap();
        assert_eq!(removed, 1, "dead-pid orphan not reaped despite final");
        assert!(!any_tmp_in(&staging), "orphan survived in staging dir");
        // The published final is untouched (the sweep only scans staging).
        assert!(dst_root.path().join("out/data.tar.zst").exists());
    }
}
