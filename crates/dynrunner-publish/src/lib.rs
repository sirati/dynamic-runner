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
//! * Different filesystems → copy `src` to a sibling of `dst` in
//!   `dst`'s parent, `fsync` the data, then `rename(2)` the sibling
//!   over `dst` (intra-FS, atomic). Finally `fsync` `dst.parent()`
//!   so the rename itself is durable, and unlink `src`.
//!
//! `src_root` is the caller's allow-list root: the publish refuses
//! to move a file that is not under `src_root`. Frameworks set this
//! to the staging mount path (e.g. `/app/out-tmp`) so a worker that
//! accidentally points the API at a path outside the staging area
//! fails fast instead of moving arbitrary files.
//!
//! The crate is deliberately small. Higher-level concerns (queueing,
//! batching, drain-before-Done coordination, deployment-specific
//! mount paths) live in the caller.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
}

/// Move `src` to `dst` atomically. `src` must be under `src_root`.
///
/// On the same filesystem this collapses to a single `rename(2)`.
/// Across filesystems it copies to a sibling of `dst` (so the final
/// commit step is itself an intra-FS rename), `fsync`s the data,
/// renames over `dst`, `fsync`s `dst.parent()` for rename durability,
/// and unlinks `src` last.
///
/// Always-overwrite: if `dst` exists, it is replaced. Callers gate
/// "should I publish at all?" upstream (handler-level skip-existing
/// logic); reaching this function means publish is intended.
pub fn publish_one(
    src: &Path,
    dst: &Path,
    src_root: &Path,
) -> Result<(), PublishError> {
    // Resolve symlinks on both sides so a worker can't escape
    // `src_root` by symlinking out of the staging mount.
    let canon_src = fs::canonicalize(src).map_err(|e| PublishError::SourceMissing {
        path: src.to_path_buf(),
        source: e,
    })?;
    let canon_root = fs::canonicalize(src_root).map_err(|e| PublishError::SourceMissing {
        path: src_root.to_path_buf(),
        source: e,
    })?;
    if !canon_src.starts_with(&canon_root) {
        return Err(PublishError::SourceOutsideRoot {
            src: canon_src,
            src_root: canon_root,
        });
    }

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
                return PublishError::DestinationParentIsFile {
                    path: culprit,
                };
            }
            PublishError::DestinationParentCreate {
                path: parent.to_path_buf(),
                source: e,
            }
        })?;
    }

    // Fast path: same filesystem → rename(2) is atomic by definition.
    match fs::rename(&canon_src, dst) {
        Ok(()) => {
            fsync_parent(dst)?;
            return Ok(());
        }
        Err(e) if is_cross_device(&e) => {
            // Fall through to the cross-FS branch below.
        }
        Err(e) => {
            return Err(PublishError::Rename {
                from: canon_src,
                to: dst.to_path_buf(),
                source: e,
            });
        }
    }

    // Cross-FS path: copy to a sibling of `dst` (so the commit step
    // is itself an intra-FS rename), fsync the data, rename, fsync
    // the destination directory, then unlink `src`.
    let tmp = sibling_tmp_path(dst);
    copy_with_fsync(&canon_src, &tmp)?;
    fs::rename(&tmp, dst).map_err(|e| {
        // Best-effort cleanup of the partial tmp before bubbling.
        let _ = fs::remove_file(&tmp);
        PublishError::Rename {
            from: tmp.clone(),
            to: dst.to_path_buf(),
            source: e,
        }
    })?;
    fsync_parent(dst)?;
    fs::remove_file(&canon_src).map_err(|e| PublishError::Unlink {
        path: canon_src.clone(),
        source: e,
    })?;
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

fn is_cross_device(e: &io::Error) -> bool {
    // `io::ErrorKind::CrossesDevices` is unstable as of Rust 1.95;
    // match by raw errno for stability across toolchains.
    e.raw_os_error() == Some(libc::EXDEV)
}

fn sibling_tmp_path(dst: &Path) -> PathBuf {
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    let name = dst
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "publish".to_string());
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    parent.join(format!(".{name}.publish-tmp.{pid}.{nanos}"))
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
        let err = publish_one(&src, &dst, &src_root).unwrap_err();
        match err {
            PublishError::DestinationParentIsFile { path } => {
                assert_eq!(path, collision);
            }
            other => panic!(
                "expected DestinationParentIsFile, got {other:?}"
            ),
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

        publish_one(&src, &dst, &src_root).unwrap();

        assert!(dst.exists(), "dst missing after publish");
        assert_eq!(read_file(&dst), b"hello");
        assert!(!src.exists(), "src not removed");

        let leftover = fs::read_dir(dst_root.join("out"))
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains("publish-tmp")
            });
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

        publish_one(&src, &dst, &src_root).unwrap();
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

        publish_one(&src, &dst, &src_root).unwrap();
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

        let err = publish_one(&src, &dst, &src_root).unwrap_err();
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

        let err = publish_one(&src, &dst, &src_root).unwrap_err();
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

        publish_one(&src, &dst, src_root.path()).unwrap();

        assert!(dst.exists(), "dst missing");
        assert_eq!(read_file(&dst), b"contents");
        assert!(!src.exists(), "src not removed");
        if cross {
            // No tmp sibling left in dst's directory.
            let leftover = fs::read_dir(dst.parent().unwrap())
                .unwrap()
                .filter_map(|e| e.ok())
                .any(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .contains("publish-tmp")
                });
            assert!(!leftover, "cross-FS tmp sibling not cleaned up");
        }
    }
}
