//! Rust-driven directory traversal with a generic visitor.
//!
//! The [`Filesystem`](dynrunner_gateway::Filesystem) backend supplies the
//! per-directory listing; the [`Visitor`] decides which subfolders to descend
//! into and which files to mark for processing. Marks accumulate in the
//! driver, not in the visitor — visitors stay stateless w.r.t. the result
//! set and only carry whatever per-subtree context they want via `Payload`.
//!
//! See [`walk`] for the entry point.

use std::future::Future;
use std::path::PathBuf;

use dynrunner_gateway::{DirEntry, Filesystem, FsError};

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
pub trait Visitor: Send {
    type Payload: Send;
    type Error: Send;

    fn visit(
        &mut self,
        parent_payload: Option<&Self::Payload>,
        subfolders: &[FolderInfo],
        files: &[FileInfo],
    ) -> impl Future<Output = Result<VisitOutcome<Self::Payload>, Self::Error>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum WalkError<E> {
    #[error("filesystem: {0}")]
    Fs(#[from] FsError),
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

/// Depth-first walk of `root` on `fs`, calling `visitor` once per directory.
///
/// Returns the full list of marked files in visit order (parents before
/// children, alphabetical among siblings).
pub async fn walk<F, V>(
    fs: &F,
    root: &str,
    visitor: &mut V,
) -> Result<Vec<Marked<V::Payload>>, WalkError<V::Error>>
where
    F: Filesystem,
    V: Visitor,
{
    let mut marked: Vec<Marked<V::Payload>> = Vec::new();
    let mut stack: Vec<(String, PathBuf, Option<V::Payload>)> = Vec::new();
    stack.push((root.to_owned(), PathBuf::new(), None));

    while let Some((abs, rel, parent_payload)) = stack.pop() {
        let listing = fs.list_dir(&abs).await?;

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
            .await
            .map_err(WalkError::Visitor)?;

        for (idx, payload) in outcome.mark {
            let f = files
                .get(idx)
                .ok_or_else(|| WalkError::IndexOutOfBounds {
                    kind: "file",
                    index: idx,
                    len: files.len(),
                    path: abs.clone(),
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
                    path: abs.clone(),
                })?;
            let child_abs = if abs.ends_with('/') {
                format!("{abs}{}", d.name)
            } else {
                format!("{abs}/{}", d.name)
            };
            let child_rel = rel.join(&d.name);
            stack.push((child_abs, child_rel, Some(payload)));
        }
    }

    Ok(marked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct StubFs {
        entries: HashMap<String, Vec<DirEntry>>,
    }

    impl Filesystem for StubFs {
        async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
            self.entries
                .get(path)
                .cloned()
                .ok_or_else(|| FsError::NotFound(path.to_owned()))
        }
    }

    /// Visitor that descends into every subfolder, marks every file with its
    /// path, and threads the parent path through `Payload` so we can assert
    /// payload propagation.
    struct EveryFile {
        seen_payloads: Mutex<Vec<Option<String>>>,
    }

    impl Visitor for EveryFile {
        type Payload = String;
        type Error = std::convert::Infallible;

        async fn visit(
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

    fn fs(entries: &[(&str, Vec<DirEntry>)]) -> StubFs {
        StubFs {
            entries: entries
                .iter()
                .map(|(p, v)| ((*p).to_owned(), v.clone()))
                .collect(),
        }
    }

    #[tokio::test]
    async fn walks_one_level() {
        let fs = fs(&[(
            "/root",
            vec![
                DirEntry::File {
                    name: "a.bin".into(),
                    size: 10,
                },
                DirEntry::File {
                    name: "b.bin".into(),
                    size: 20,
                },
            ],
        )]);
        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let marked = walk(&fs, "/root", &mut v).await.unwrap();
        let names: Vec<_> = marked
            .iter()
            .map(|m| m.relative_path.to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["a.bin", "b.bin"]);
        assert_eq!(marked[0].size, 10);
        assert_eq!(marked[1].size, 20);
    }

    #[tokio::test]
    async fn descends_and_propagates_payload() {
        let fs = fs(&[
            (
                "/root",
                vec![
                    DirEntry::Dir { name: "x86".into() },
                    DirEntry::Dir { name: "x64".into() },
                    DirEntry::File {
                        name: "top.bin".into(),
                        size: 1,
                    },
                ],
            ),
            (
                "/root/x86",
                vec![DirEntry::File {
                    name: "a.bin".into(),
                    size: 2,
                }],
            ),
            (
                "/root/x64",
                vec![DirEntry::File {
                    name: "b.bin".into(),
                    size: 3,
                }],
            ),
        ]);
        let mut v = EveryFile {
            seen_payloads: Mutex::new(Vec::new()),
        };
        let marked = walk(&fs, "/root", &mut v).await.unwrap();
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

    #[tokio::test]
    async fn skips_unentered_subfolder() {
        let fs = fs(&[
            (
                "/root",
                vec![DirEntry::Dir {
                    name: "skip".into(),
                }],
            ),
            // /root/skip not in the stub — would error if walker descended.
        ]);
        struct NoEnter;
        impl Visitor for NoEnter {
            type Payload = ();
            type Error = std::convert::Infallible;
            async fn visit(
                &mut self,
                _: Option<&()>,
                _: &[FolderInfo],
                _: &[FileInfo],
            ) -> Result<VisitOutcome<()>, std::convert::Infallible> {
                Ok(VisitOutcome::default())
            }
        }
        let marked = walk(&fs, "/root", &mut NoEnter).await.unwrap();
        assert!(marked.is_empty());
    }

    #[tokio::test]
    async fn out_of_bounds_index_is_an_error() {
        let fs = fs(&[("/root", vec![])]);
        struct Bad;
        impl Visitor for Bad {
            type Payload = ();
            type Error = std::convert::Infallible;
            async fn visit(
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
        let err = walk(&fs, "/root", &mut Bad).await.unwrap_err();
        assert!(matches!(
            err,
            WalkError::IndexOutOfBounds { kind: "folder", .. }
        ));
    }
}
