//! Python bindings for the `dynrunner-publish` atomic staged-publish
//! crate.
//!
//! Single concern: surface the Rust crate's staged stage→destination
//! publish (single-item [`publish_one`], batch [`publish_all`]) and the
//! stale-temp [`sweep_stale_tmps`] reaper to Python, mapping every
//! `PublishError` variant onto a single `PublishError` exception class.
//! The Python layer reads the string form of the error to decide what
//! (if anything) to log; it does not branch on variant identity.
//!
//! All logic lives in `dynrunner-publish`; this module is a thin bridge
//! — type marshalling and one shared error map, nothing more.

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    _native,
    PublishError,
    PyException,
    "Atomic stage→destination publish failed."
);

/// Map a `dynrunner_publish::PublishError` onto the Python `PublishError`
/// exception. Single source of truth for the error boundary so every
/// binding (`publish_one`, `publish_all`, `sweep_stale_tmps`) surfaces
/// the variant's `Display` string identically — no per-binding match.
fn map_err(e: dynrunner_publish::PublishError) -> PyErr {
    PublishError::new_err(e.to_string())
}

#[pyfunction]
pub(crate) fn publish_one(src: PathBuf, dst: PathBuf, src_root: PathBuf) -> PyResult<()> {
    dynrunner_publish::publish_one(&src, &dst, &src_root).map_err(map_err)
}

/// Atomically publish a batch of `(src, dst)` pairs as one staged
/// transaction. Every `src` must be under `src_root`. See
/// `dynrunner_publish::publish_all` for the two-phase contract.
#[pyfunction]
pub(crate) fn publish_all(items: Vec<(PathBuf, PathBuf)>, src_root: PathBuf) -> PyResult<()> {
    dynrunner_publish::publish_all(&items, &src_root).map_err(map_err)
}

/// Reap stale `.publish-tmp` siblings left in `dir` by a hard kill,
/// returning the number removed. See `dynrunner_publish::sweep_stale_tmps`
/// for the shared-NFS safety argument (own-host scope + local pid
/// liveness).
#[pyfunction]
pub(crate) fn sweep_stale_tmps(dir: PathBuf) -> PyResult<usize> {
    dynrunner_publish::sweep_stale_tmps(&dir).map_err(map_err)
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Bridge tests for the publish bindings. They exercise the Rust
    //! `#[pyfunction]` bodies directly (the marshalling + shared error
    //! map) under an embedded interpreter; the underlying transaction
    //! semantics are covered in `dynrunner-publish`'s own suite.
    //!
    //! Gated on `test-with-python` because the `PublishError` exception
    //! type is constructed against a live CPython interpreter. Invoke
    //! as: `cargo test -p dynrunner-pyo3 --lib --no-default-features
    //!        --features test-with-python publish`.
    use super::*;
    use std::fs;
    use std::io::Write;

    fn write_file(path: &std::path::Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }

    /// `publish_all` over a temp dir: stage several srcs, commit them to
    /// their destinations, and confirm every src is consumed and every
    /// dst lands with the right bytes.
    #[test]
    fn publish_all_round_trips_multiple_items() {
        Python::attach(|_py| {
            let root = tempfile::tempdir().unwrap();
            let src_root = root.path().join("staging");
            let dst_root = root.path().join("network");
            fs::create_dir_all(&src_root).unwrap();
            fs::create_dir_all(&dst_root).unwrap();

            let mut items: Vec<(PathBuf, PathBuf)> = Vec::new();
            for i in 0..3 {
                let src = src_root.join(format!("p{i}.bin"));
                write_file(&src, format!("payload-{i}").as_bytes());
                items.push((src, dst_root.join(format!("out/p{i}.bin"))));
            }

            publish_all(items.clone(), src_root.clone()).expect("publish_all succeeds");

            for (i, (src, dst)) in items.iter().enumerate() {
                assert!(dst.exists(), "dst {i} missing after publish_all");
                assert_eq!(fs::read(dst).unwrap(), format!("payload-{i}").as_bytes());
                assert!(!src.exists(), "src {i} not consumed");
            }
        });
    }

    /// A `src` outside `src_root` must surface as the Python
    /// `PublishError` exception (the shared error map at work).
    #[test]
    fn publish_all_maps_error_to_python_exception() {
        Python::attach(|py| {
            let root = tempfile::tempdir().unwrap();
            let src_root = root.path().join("staging");
            let outside = root.path().join("outside");
            fs::create_dir_all(&src_root).unwrap();
            fs::create_dir_all(&outside).unwrap();

            let bad_src = outside.join("escape.bin");
            write_file(&bad_src, b"nope");
            let bad_dst = root.path().join("network/escape.bin");

            let err = publish_all(vec![(bad_src, bad_dst)], src_root)
                .expect_err("src outside root must error");
            assert!(
                err.is_instance_of::<PublishError>(py),
                "error must be the PublishError exception class"
            );
        });
    }

    /// `sweep_stale_tmps` returns the crate's reaped-temp count and
    /// leaves non-temp files untouched. The own-host/dead-pid reaping
    /// rule (and its shared-NFS safety) is the crate's concern, covered
    /// in `dynrunner-publish` — the crate's temp-name layout is private,
    /// so here we exercise the binding's marshalling and count
    /// passthrough over a directory with no reapable temps.
    #[test]
    fn sweep_stale_tmps_passes_through_count_and_keeps_real_files() {
        Python::attach(|_py| {
            let dir = tempfile::tempdir().unwrap();
            // A normal published file alongside — must survive.
            write_file(&dir.path().join("data.tar.zst"), b"real output");
            let removed = sweep_stale_tmps(dir.path().to_path_buf())
                .expect("sweep over a clean dir succeeds");
            assert_eq!(removed, 0, "no stale own-host temps to reap");
            assert!(dir.path().join("data.tar.zst").exists(), "real file kept");
        });
    }

    /// `sweep_stale_tmps` on a missing directory is a no-op returning 0
    /// (the binding faithfully passes the crate's NotFound→Ok(0)).
    #[test]
    fn sweep_stale_tmps_missing_dir_is_ok() {
        Python::attach(|_py| {
            let root = tempfile::tempdir().unwrap();
            let missing = root.path().join("does-not-exist");
            assert_eq!(
                sweep_stale_tmps(missing).expect("missing dir is ok"),
                0
            );
        });
    }
}
