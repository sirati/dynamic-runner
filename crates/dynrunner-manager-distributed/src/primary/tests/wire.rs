//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


/// Pin the wire-strip behaviour directly: PrimaryConfig::wire_local_path
/// returns the absolute path verbatim outside pre-staged mode and the
/// relative-to-root form inside it. Paths that don't sit under the
/// root pass through unchanged (the secondary then surfaces the
/// mismatch as NonRecoverable).
#[test]
fn wire_local_path_strips_pre_staged_prefix() {
    let mut cfg = PrimaryConfig::default();

    let mut bin = make_binary("x", 0);
    bin.path = std::path::PathBuf::from("/srv/data/bin_0");

    // Off → verbatim.
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with matching prefix (abs-under-src) → relative tail.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    // On with mismatching prefix (abs-out-of-tree) → verbatim
    // (consumer misconfig is surfaced downstream by
    // `resolve_pre_staged` returning None, not silently re-routed).
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/other/prefix"));
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with a relative `binary.path` (rel-under-src — the post-
    // Bug-B wire-id shape consumers emit). Resolving the relative
    // path against the prestaged root and re-stripping yields the
    // original relative form verbatim, which is exactly what
    // `secondary.src_network.join(<wire>)` expects. Pre-fix the
    // relative path silently fell through the strip-prefix Err arm
    // and shipped as-is — the value happened to be correct, but
    // for the wrong reason; this test pins the explicit round-trip.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    bin.path = std::path::PathBuf::from("bin_0");
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    bin.path = std::path::PathBuf::from("nested/bin_1");
    assert_eq!(cfg.wire_local_path(&bin), "nested/bin_1");
}
