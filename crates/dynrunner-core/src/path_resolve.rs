//! Path-shape normalisation against a source root.
//!
//! Consumers may emit `TaskInfo.path` in any of three legitimate
//! shapes (Bug-B class):
//!
//! * **absolute under the root** — discovery emitted
//!   `root.join(rel)` directly (the legacy shape);
//! * **absolute out-of-tree** — the path doesn't sit under the root
//!   (out-of-band shape — the framework records it but doesn't
//!   transform it);
//! * **relative** — the path IS the wire-identifier verbatim
//!   (the post-Bug-B shape consumers should prefer).
//!
//! Naive `path.strip_prefix(root)` covers only the absolute-under-
//! root case and silently mishandles the relative shape (returns
//! `Err`). [`resolve_against_root`] joins relative paths against
//! the root before stripping, so the same `match strip_prefix`
//! branch covers all three shapes uniformly.
//!
//! Mirrors the Python equivalent in `packaging/job_manager.py`
//! (commit d5d0604).

use std::path::{Path, PathBuf};

/// Result of resolving a path against a source root.
///
/// `absolute` is the absolute on-disk location to read the file at;
/// `relative` is `Some(rel)` when the resolved path sits under
/// `root` (with `rel` being the wire-identifier the secondary /
/// worker expects), or `None` for the absolute-out-of-tree shape.
#[derive(Debug, Clone)]
pub struct ResolvedPath {
    pub absolute: PathBuf,
    pub relative: Option<PathBuf>,
}

/// Resolve `path` against `root` and derive its wire-relative tail.
///
/// * absolute under `root` → `absolute = path`, `relative = Some(<tail>)`.
/// * absolute out-of-tree → `absolute = path`, `relative = None`.
/// * relative → `absolute = root.join(path)`, `relative = Some(path)`
///   (the join+strip cycle yields the original relative path verbatim).
pub fn resolve_against_root(path: &Path, root: &Path) -> ResolvedPath {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let relative = absolute
        .strip_prefix(root)
        .ok()
        .map(|p| p.to_path_buf());
    ResolvedPath { absolute, relative }
}

#[cfg(test)]
mod tests {
    use super::resolve_against_root;
    use std::path::{Path, PathBuf};

    #[test]
    fn abs_under_root_strips_to_relative_tail() {
        let r = resolve_against_root(Path::new("/srv/data/bin_0"), Path::new("/srv/data"));
        assert_eq!(r.absolute, PathBuf::from("/srv/data/bin_0"));
        assert_eq!(r.relative, Some(PathBuf::from("bin_0")));
    }

    #[test]
    fn abs_under_root_nested_strips_to_nested_tail() {
        let r = resolve_against_root(
            Path::new("/srv/data/nested/bin_1"),
            Path::new("/srv/data"),
        );
        assert_eq!(r.absolute, PathBuf::from("/srv/data/nested/bin_1"));
        assert_eq!(r.relative, Some(PathBuf::from("nested/bin_1")));
    }

    #[test]
    fn abs_out_of_tree_yields_none_relative() {
        let r = resolve_against_root(Path::new("/other/abs/bin_0"), Path::new("/srv/data"));
        assert_eq!(r.absolute, PathBuf::from("/other/abs/bin_0"));
        assert_eq!(r.relative, None);
    }

    #[test]
    fn rel_under_root_round_trips_verbatim() {
        let r = resolve_against_root(Path::new("bin_0"), Path::new("/srv/data"));
        assert_eq!(r.absolute, PathBuf::from("/srv/data/bin_0"));
        assert_eq!(r.relative, Some(PathBuf::from("bin_0")));
    }

    #[test]
    fn rel_under_root_nested_round_trips_verbatim() {
        let r = resolve_against_root(Path::new("nested/bin_1"), Path::new("/srv/data"));
        assert_eq!(r.absolute, PathBuf::from("/srv/data/nested/bin_1"));
        assert_eq!(r.relative, Some(PathBuf::from("nested/bin_1")));
    }
}
