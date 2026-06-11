//! Rust-driven directory traversal with a generic visitor.
//!
//! Reads the local filesystem directly via [`std::fs::read_dir`]; the
//! [`Visitor`] decides which subfolders to descend into and which files to
//! mark for processing. Marks accumulate in the driver, not in the visitor
//! — visitors stay stateless w.r.t. the result set and only carry whatever
//! per-subtree context they want via `Payload`.
//!
//! The walk is fully synchronous: the per-directory work is dominated by
//! blocking `read_dir`/`metadata` syscalls, and embedding an async runtime
//! on top of that only buys complexity (and a panic when the caller is
//! already inside one). Callers that need to keep an async executor
//! responsive should run [`walk`] under `spawn_blocking` / `block_in_place`.
//!
//! See [`walk`] for the entry point.

use std::path::{Path, PathBuf};

/// Internal directory entry — the per-listing intermediate before the
/// driver splits subfolders from files for the visitor.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DirEntry {
    File { name: String, size: u64 },
    Dir { name: String },
}

impl DirEntry {
    fn name(&self) -> &str {
        match self {
            DirEntry::File { name, .. } | DirEntry::Dir { name } => name.as_str(),
        }
    }
}

/// One directory's listing plus the diagnostic tallies gathered while
/// producing it. The tallies are per-directory; [`walk`] sums them across
/// the whole traversal into [`WalkStats`].
struct DirListing {
    entries: Vec<DirEntry>,
    /// `dirent`s iterated from this directory (before any classification),
    /// excluding non-UTF-8 names (which can't be acted on at all).
    seen: u64,
    /// Entries that were symlinks resolving to a regular file or directory
    /// (i.e. followed and kept). Distinguished cheaply via one extra
    /// `symlink_metadata` (`lstat`) per entry.
    symlinks_followed: u64,
    /// Symlinks whose target does not resolve (dangling) — skipped, not
    /// fatal.
    broken_symlinks_skipped: u64,
}

/// Read one directory's entries, resolving symlinks (a symlink to a regular
/// file appears as [`DirEntry::File`], a symlink to a directory as
/// [`DirEntry::Dir`]), skipping broken symlinks and non-UTF-8 names
/// silently, and returning entries sorted alphabetically by name. The
/// returned [`DirListing`] also carries the per-directory diagnostic
/// tallies (entries seen, symlinks followed, broken symlinks skipped).
///
/// Lifted from `dynrunner_gateway::local::LocalGateway::list_dir`: the
/// previous `Filesystem` trait had this same body behind one impl. Since
/// the SSH-side impl is also being deleted (discovery now always runs on
/// whichever process owns the data), the trait is gone too.
fn list_dir(path: &Path) -> std::io::Result<DirListing> {
    let read = std::fs::read_dir(path)?;

    let mut entries = Vec::new();
    let mut seen: u64 = 0;
    let mut symlinks_followed: u64 = 0;
    let mut broken_symlinks_skipped: u64 = 0;
    for child in read {
        let child = child?;
        let name = match child.file_name().into_string() {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!(
                    path = %path.display(),
                    "skipping non-UTF-8 entry in directory listing"
                );
                continue;
            }
        };
        seen += 1;

        // Classify the symlink-ness first via an `lstat` that does NOT
        // follow, so a dangling symlink can be counted as a *broken
        // symlink* rather than mistaken for a transient IO error on a
        // regular file. `false` for a vanished entry / unreadable lstat —
        // it then falls into the generic-skip path below.
        let is_symlink = std::fs::symlink_metadata(child.path())
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false);

        // Follow symlinks (matches the historical Python `Path.is_file()`
        // semantics). A dangling symlink makes this `stat` return Err; we
        // skip it (never fatal) and, when we know it was a symlink, count
        // it as broken so the per-walk summary surfaces the corpus issue.
        let meta = match std::fs::metadata(child.path()) {
            Ok(m) => m,
            Err(_) => {
                if is_symlink {
                    broken_symlinks_skipped += 1;
                }
                continue;
            }
        };

        if is_symlink {
            symlinks_followed += 1;
        }

        if meta.is_dir() {
            entries.push(DirEntry::Dir { name });
        } else if meta.is_file() {
            entries.push(DirEntry::File {
                name,
                size: meta.len(),
            });
        }
        // Other kinds (sockets, fifos, block/char devices) are ignored.
    }

    entries.sort_by(|a, b| a.name().cmp(b.name()));
    Ok(DirListing {
        entries,
        seen,
        symlinks_followed,
        broken_symlinks_skipped,
    })
}

/// Per-directory subfolder slot handed to the visitor.
#[derive(Debug, Clone)]
pub struct FolderInfo {
    pub name: String,
}

/// Per-directory file slot handed to the visitor.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
}

/// What the visitor decided for one directory.
///
/// `enter` and `mark` reference indices into the `subfolders` and `files`
/// slices that were passed to [`Visitor::visit`]. Out-of-range indices are
/// rejected by the driver with [`WalkError::IndexOutOfBounds`].
pub struct VisitOutcome<P> {
    pub enter: Vec<(usize, P)>,
    pub mark: Vec<(usize, P)>,
}

impl<P> Default for VisitOutcome<P> {
    fn default() -> Self {
        Self {
            enter: Vec::new(),
            mark: Vec::new(),
        }
    }
}

/// One marked file collected during traversal.
#[derive(Debug, Clone)]
pub struct Marked<P> {
    /// Path relative to the walk root, including the file name.
    pub relative_path: PathBuf,
    pub size: u64,
    pub payload: P,
}

/// `tracing` target for the once-per-walk diagnostic summary, so callers
/// (and tests) can scope a subscriber to exactly this module's output.
pub const LOG_TARGET: &str = "dynrunner_discovery::walk";

/// Aggregated diagnostic tallies for one [`walk`] traversal. Logged once
/// (at the [`LOG_TARGET`] target) when the walk finishes, and returned so
/// callers can assert / surface them without scraping log output.
///
/// Pure diagnostics — none of these feed the result set ([`walk`] returns
/// the marked files independently). The counters exist to make a corpus
/// shape problem (e.g. a tree full of dangling symlinks, or one that loops)
/// visible in one line instead of silent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalkStats {
    /// Directory `dirent`s iterated across the whole walk (pre-classification,
    /// excluding non-UTF-8 names). The root's own entries plus every
    /// descended subdirectory's.
    pub entries_seen: u64,
    /// Files slated for processing by the visitor (the size of the returned
    /// `Vec<Marked<_>>`).
    pub files_yielded: u64,
    /// Subdirectories the visitor chose to descend into and which were
    /// successfully listed.
    pub dirs_descended: u64,
    /// Entries that were symlinks resolving to a regular file or directory
    /// (followed and kept).
    pub symlinks_followed: u64,
    /// Symlinks whose target does not resolve (dangling) — skipped.
    pub broken_symlinks_skipped: u64,
    /// Directories skipped because following them would revisit an ancestor
    /// (a symlink cycle). Skipped, never fatal.
    pub symlink_cycles_skipped: u64,
}

/// Decides per directory which subfolders to descend into and which files
/// to slate for processing.
///
/// `parent_payload` is the payload that the parent's `enter` produced for
/// this subdirectory. At the root call it is `None`.
pub trait Visitor {
    type Payload;
    type Error;

    fn visit(
        &mut self,
        parent_payload: Option<&Self::Payload>,
        subfolders: &[FolderInfo],
        files: &[FileInfo],
    ) -> Result<VisitOutcome<Self::Payload>, Self::Error>;
}

/// What [`walk`] returns on success: the marked files in visit order paired
/// with the traversal's diagnostic [`WalkStats`]. Aliased to keep the
/// signature readable (and clippy's `type_complexity` quiet).
pub type WalkOutput<V> = (Vec<Marked<<V as Visitor>::Payload>>, WalkStats);

#[derive(Debug, thiserror::Error)]
pub enum WalkError<E> {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("visitor returned an out-of-bounds {kind} index {index} (len={len}) at {path}")]
    IndexOutOfBounds {
        kind: &'static str,
        index: usize,
        len: usize,
        path: String,
    },
    #[error("visitor: {0}")]
    Visitor(E),
}

/// Depth-first walk of `root` on the local filesystem, calling `visitor`
/// once per directory.
///
/// Returns the full list of marked files in visit order (parents before
/// children, alphabetical among siblings) plus the traversal's diagnostic
/// [`WalkStats`] (also logged once at [`LOG_TARGET`] when the walk
/// finishes).
///
/// # Symlink robustness
///
/// `list_dir` follows symlinks (a symlink to a regular file is yielded as a
/// file, to a directory as a directory). Two pathological cases are handled
/// defensively rather than aborting the walk:
///
/// * **Broken (dangling) symlinks** are skipped and tallied in
///   [`WalkStats::broken_symlinks_skipped`].
/// * **Directory symlink cycles** (a symlink pointing back at an ancestor)
///   are detected by canonical path: a directory whose real path was already
///   visited is skipped and tallied in [`WalkStats::symlink_cycles_skipped`],
///   so a self-referential tree can never loop forever. Canonicalisation
///   also dedups a directory reachable via two distinct symlinks (it is
///   walked once).
pub fn walk<V>(root: &Path, visitor: &mut V) -> Result<WalkOutput<V>, WalkError<V::Error>>
where
    V: Visitor,
{
    let mut marked: Vec<Marked<V::Payload>> = Vec::new();
    let mut stats = WalkStats::default();
    // Real (symlink-resolved) directory paths already listed, so a symlink
    // that points back at an ancestor — or sideways at an already-walked
    // subtree — is visited at most once. A directory whose path cannot be
    // canonicalised (vanished mid-walk) is treated as unseen and listed
    // normally; `list_dir` then surfaces any IO error.
    let mut visited: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    let mut stack: Vec<(PathBuf, PathBuf, Option<V::Payload>)> = Vec::new();
    stack.push((root.to_path_buf(), PathBuf::new(), None));

    while let Some((abs, rel, parent_payload)) = stack.pop() {
        if let Ok(real) = std::fs::canonicalize(&abs)
            && !visited.insert(real)
        {
            tracing::warn!(
                target: LOG_TARGET,
                path = %abs.display(),
                "skipping directory that resolves to an already-visited \
                 path (symlink cycle)"
            );
            stats.symlink_cycles_skipped += 1;
            continue;
        }

        let listing = list_dir(&abs)?;
        stats.entries_seen += listing.seen;
        stats.symlinks_followed += listing.symlinks_followed;
        stats.broken_symlinks_skipped += listing.broken_symlinks_skipped;
        // A non-root directory reaching this point was popped, passed the
        // cycle guard, and listed — i.e. actually descended into. The root
        // (empty relative path) is the walk's start, not a descent.
        if !rel.as_os_str().is_empty() {
            stats.dirs_descended += 1;
        }

        let mut subfolders: Vec<FolderInfo> = Vec::new();
        let mut files: Vec<FileInfo> = Vec::new();
        for e in listing.entries {
            match e {
                DirEntry::Dir { name } => subfolders.push(FolderInfo { name }),
                DirEntry::File { name, size } => files.push(FileInfo { name, size }),
            }
        }

        let outcome = visitor
            .visit(parent_payload.as_ref(), &subfolders, &files)
            .map_err(WalkError::Visitor)?;

        for (idx, payload) in outcome.mark {
            let f = files.get(idx).ok_or_else(|| WalkError::IndexOutOfBounds {
                kind: "file",
                index: idx,
                len: files.len(),
                path: abs.display().to_string(),
            })?;
            marked.push(Marked {
                relative_path: rel.join(&f.name),
                size: f.size,
                payload,
            });
            stats.files_yielded += 1;
        }

        // Push enters in reverse so the visit order matches alphabetical (the
        // listing is already sorted; reversing here means the first listed
        // subfolder is popped first).
        let mut enters: Vec<(usize, V::Payload)> = outcome.enter;
        enters.sort_by_key(|(idx, _)| std::cmp::Reverse(*idx));
        for (idx, payload) in enters {
            let d = subfolders
                .get(idx)
                .ok_or_else(|| WalkError::IndexOutOfBounds {
                    kind: "folder",
                    index: idx,
                    len: subfolders.len(),
                    path: abs.display().to_string(),
                })?;
            let child_abs = abs.join(&d.name);
            let child_rel = rel.join(&d.name);
            stack.push((child_abs, child_rel, Some(payload)));
        }
    }

    tracing::debug!(
        target: LOG_TARGET,
        entries_seen = stats.entries_seen,
        files_yielded = stats.files_yielded,
        dirs_descended = stats.dirs_descended,
        symlinks_followed = stats.symlinks_followed,
        broken_symlinks_skipped = stats.broken_symlinks_skipped,
        symlink_cycles_skipped = stats.symlink_cycles_skipped,
        "discovery walk of {} complete: {} entries seen, {} files yielded, \
         {} dirs descended ({} symlinks followed, {} broken symlinks skipped, \
         {} symlink cycles skipped)",
        root.display(),
        stats.entries_seen,
        stats.files_yielded,
        stats.dirs_descended,
        stats.symlinks_followed,
        stats.broken_symlinks_skipped,
        stats.symlink_cycles_skipped,
    );

    Ok((marked, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Visitor that descends into every subfolder, marks every file with its
    /// path, and threads the parent path through `Payload` so we can assert
    /// payload propagation.
    struct EveryFile {
        seen_payloads: Mutex<Vec<Option<String>>>,
    }

    impl Visitor for EveryFile {
        type Payload = String;
        type Error = std::convert::Infallible;

        fn visit(
            &mut self,
            parent_payload: Option<&Self::Payload>,
            subfolders: &[FolderInfo],
            files: &[FileInfo],
        ) -> Result<VisitOutcome<Self::Payload>, Self::Error> {
            self.seen_payloads
                .lock()
                .unwrap()
                .push(parent_payload.cloned());

            let prefix = parent_payload.cloned().unwrap_or_default();
            let mut out = VisitOutcome::default();
            for (i, d) in subfolders.iter().enumerate() {
                let p = if prefix.is_empty() {
                    d.name.clone()
                } else {
                    format!("{prefix}/{}", d.name)
                };
                out.enter.push((i, p));
            }
            for (i, f) in files.iter().enumerate() {
                let p = if prefix.is_empty() {
                    f.name.clone()
                } else {
                    format!("{prefix}/{}", f.name)
                };
                out.mark.push((i, p));
            }
            Ok(out)
        }
    }

    #[test]
    fn walks_one_level() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.bin"), vec![0u8; 10]).unwrap();
        std::fs::write(tmp.path().join("b.bin"), vec![0u8; 20]).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(tmp.path(), &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["a.bin", "b.bin"]);
        assert_eq!(marked[0].size, 10);
        assert_eq!(marked[1].size, 20);
        assert_eq!(
            stats,
            WalkStats {
                entries_seen: 2,
                files_yielded: 2,
                dirs_descended: 0,
                ..WalkStats::default()
            }
        );
    }

    #[test]
    fn descends_and_propagates_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir(root.join("x86")).unwrap();
        std::fs::create_dir(root.join("x64")).unwrap();
        std::fs::write(root.join("top.bin"), vec![0u8; 1]).unwrap();
        std::fs::write(root.join("x86/a.bin"), vec![0u8; 2]).unwrap();
        std::fs::write(root.join("x64/b.bin"), vec![0u8; 3]).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(root, &mut v).unwrap();
        let mut names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["top.bin", "x64/b.bin", "x86/a.bin"]);
        // root + 2 subdirs: 4 entries (top.bin, x86, x64, then a.bin + b.bin),
        // 3 files yielded, 2 dirs descended. No symlinks anywhere.
        assert_eq!(stats.files_yielded, 3);
        assert_eq!(stats.dirs_descended, 2);
        assert_eq!(stats.entries_seen, 5);
        assert_eq!(stats.symlinks_followed, 0);

        // root visit gets None; child visits get the parent dir name as payload.
        let seen = v.seen_payloads.lock().unwrap().clone();
        assert!(seen.contains(&None));
        assert!(seen.contains(&Some("x86".into())));
        assert!(seen.contains(&Some("x64".into())));
    }

    #[test]
    fn skips_unentered_subfolder() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Create a subfolder we expect the walker NOT to descend into.
        // Populate it with a file that would show up if descent leaked.
        std::fs::create_dir(root.join("skip")).unwrap();
        std::fs::write(root.join("skip/should-not-see"), b"x").unwrap();

        struct NoEnter;
        impl Visitor for NoEnter {
            type Payload = ();
            type Error = std::convert::Infallible;
            fn visit(
                &mut self,
                _: Option<&()>,
                _: &[FolderInfo],
                _: &[FileInfo],
            ) -> Result<VisitOutcome<()>, std::convert::Infallible> {
                Ok(VisitOutcome::default())
            }
        }
        let (marked, stats) = walk(root, &mut NoEnter).unwrap();
        assert!(marked.is_empty());
        // The unentered subfolder is SEEN at the root listing but never
        // descended into.
        assert_eq!(stats.entries_seen, 1);
        assert_eq!(stats.dirs_descended, 0);
        assert_eq!(stats.files_yielded, 0);
    }

    #[test]
    fn out_of_bounds_index_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        // Empty dir: visitor's enter=[(99, ...)] is unconditionally OOB.
        struct Bad;
        impl Visitor for Bad {
            type Payload = ();
            type Error = std::convert::Infallible;
            fn visit(
                &mut self,
                _: Option<&()>,
                _: &[FolderInfo],
                _: &[FileInfo],
            ) -> Result<VisitOutcome<()>, std::convert::Infallible> {
                Ok(VisitOutcome {
                    enter: vec![(99, ())],
                    mark: vec![],
                })
            }
        }
        let err = walk(tmp.path(), &mut Bad).unwrap_err();
        assert!(matches!(
            err,
            WalkError::IndexOutOfBounds { kind: "folder", .. }
        ));
    }

    #[test]
    fn missing_root_yields_io_error() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        struct AnyVisitor;
        impl Visitor for AnyVisitor {
            type Payload = ();
            type Error = std::convert::Infallible;
            fn visit(
                &mut self,
                _: Option<&()>,
                _: &[FolderInfo],
                _: &[FileInfo],
            ) -> Result<VisitOutcome<()>, std::convert::Infallible> {
                Ok(VisitOutcome::default())
            }
        }
        let err = walk(&missing, &mut AnyVisitor).unwrap_err();
        assert!(matches!(err, WalkError::Io(_)));
    }

    // ── Symlink robustness (the #413 hardening) ──────────────────────────
    //
    // The framework walk follows symlinks via `fs::metadata` (a symlink to a
    // regular file is a File, to a directory is a Dir). These tests pin the
    // defensive handling of the pathological cases and the diagnostic
    // counters.

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    /// The production corpus shape: a binary that is a VALID RELATIVE symlink
    /// to a regular file must be yielded exactly like a real file, with its
    /// resolved (target) size, and counted as one followed symlink.
    #[cfg(unix)]
    #[test]
    fn yields_relative_symlink_to_file() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Content-addressed store + a sidecar-style binary that is a relative
        // symlink into it (readlink-resolves). The target is relative to the
        // symlink's own location at the root, so it stays inside the tree.
        std::fs::create_dir(root.join("store")).unwrap();
        std::fs::write(root.join("store/real.bin"), vec![0u8; 42]).unwrap();
        symlink("store/real.bin", root.join("mybin")).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(root, &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        // The symlinked binary is yielded; the store subdir's real file is
        // also reachable, but the symlink path resolves to the SAME inode and
        // surfaces under its own name at the root.
        assert!(
            names.contains(&"mybin".to_string()),
            "the relative symlink to a regular file must be yielded; got {names:?}"
        );
        let mybin = marked.iter().find(|m| m.relative_path.ends_with("mybin")).unwrap();
        assert_eq!(mybin.size, 42, "size must be the resolved target's size");
        assert_eq!(stats.symlinks_followed, 1);
        assert_eq!(stats.broken_symlinks_skipped, 0);
        assert_eq!(stats.symlink_cycles_skipped, 0);
    }

    /// A broken (dangling) symlink must NOT crash the walk — it is skipped
    /// and tallied. The sibling regular file is still yielded.
    #[cfg(unix)]
    #[test]
    fn broken_symlink_skipped_and_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("good.bin"), vec![0u8; 5]).unwrap();
        symlink("does/not/exist", root.join("dangling")).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(root, &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["good.bin"], "dangling symlink must not appear");
        assert_eq!(stats.broken_symlinks_skipped, 1);
        assert_eq!(stats.symlinks_followed, 0);
        assert_eq!(stats.files_yielded, 1);
    }

    /// A directory symlink that points back at an ancestor (a cycle) must
    /// terminate — the cycle directory is skipped and counted, never walked
    /// forever. A test that hangs is the failure mode for the old code.
    #[cfg(unix)]
    #[test]
    fn directory_symlink_cycle_terminates() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir(root.join("a")).unwrap();
        std::fs::write(root.join("a/leaf.bin"), vec![0u8; 3]).unwrap();
        // a/loop -> ..  (points at the root, an ancestor of a/)
        symlink("..", root.join("a/loop")).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(root, &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        // leaf.bin is collected exactly once; the cycle does not re-walk it.
        assert_eq!(
            names.iter().filter(|n| n.ends_with("leaf.bin")).count(),
            1,
            "the cycle must not re-yield files; got {names:?}"
        );
        assert!(
            stats.symlink_cycles_skipped >= 1,
            "the ancestor-pointing dir symlink must be counted as a cycle skip; stats={stats:?}"
        );
    }

    /// A symlink to a (non-cyclic) directory is followed and descended into
    /// normally — its files are yielded with the symlink name as the path
    /// prefix, and it counts as one followed symlink.
    #[cfg(unix)]
    #[test]
    fn symlink_to_directory_is_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir(root.join("realdir")).unwrap();
        std::fs::write(root.join("realdir/inner.bin"), vec![0u8; 7]).unwrap();
        symlink("realdir", root.join("link")).unwrap();

        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let (marked, stats) = walk(root, &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        // `realdir` and `link` resolve to the same real directory; the cycle
        // guard dedups them so `inner.bin` is yielded once, under whichever
        // of the two was visited first (alphabetical: `link` before
        // `realdir`).
        assert_eq!(
            names.iter().filter(|n| n.ends_with("inner.bin")).count(),
            1,
            "the directory's file is yielded once; got {names:?}"
        );
        assert_eq!(stats.symlinks_followed, 1, "the dir symlink is one follow");
        assert!(stats.dirs_descended >= 1);
    }
}
