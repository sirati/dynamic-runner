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

/// Read one directory's entries, resolving symlinks (a symlink to a regular
/// file appears as [`DirEntry::File`], a symlink to a directory as
/// [`DirEntry::Dir`]), skipping broken symlinks and non-UTF-8 names
/// silently, and returning entries sorted alphabetically by name.
///
/// Lifted from `dynrunner_gateway::local::LocalGateway::list_dir`: the
/// previous `Filesystem` trait had this same body behind one impl. Since
/// the SSH-side impl is also being deleted (discovery now always runs on
/// whichever process owns the data), the trait is gone too.
fn list_dir(path: &Path) -> std::io::Result<Vec<DirEntry>> {
    let read = std::fs::read_dir(path)?;

    let mut entries = Vec::new();
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

        // Follow symlinks (matches the historical Python `Path.is_file()`
        // semantics). Broken symlinks bubble up as Err here; skip silently.
        let meta = match std::fs::metadata(child.path()) {
            Ok(m) => m,
            Err(_) => continue,
        };

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
    Ok(entries)
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
/// children, alphabetical among siblings).
pub fn walk<V>(root: &Path, visitor: &mut V) -> Result<Vec<Marked<V::Payload>>, WalkError<V::Error>>
where
    V: Visitor,
{
    let mut marked: Vec<Marked<V::Payload>> = Vec::new();
    let mut stack: Vec<(PathBuf, PathBuf, Option<V::Payload>)> = Vec::new();
    stack.push((root.to_path_buf(), PathBuf::new(), None));

    while let Some((abs, rel, parent_payload)) = stack.pop() {
        let listing = list_dir(&abs)?;

        let mut subfolders: Vec<FolderInfo> = Vec::new();
        let mut files: Vec<FileInfo> = Vec::new();
        for e in listing {
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

    Ok(marked)
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
        let marked = walk(tmp.path(), &mut v).unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["a.bin", "b.bin"]);
        assert_eq!(marked[0].size, 10);
        assert_eq!(marked[1].size, 20);
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
        let marked = walk(root, &mut v).unwrap();
        let mut names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["top.bin", "x64/b.bin", "x86/a.bin"]);

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
        let marked = walk(root, &mut NoEnter).unwrap();
        assert!(marked.is_empty());
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
}
