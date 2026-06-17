//! Single concern: provide the image tar to the runtime as a
//! node-local file, then load it into podman storage (the binary
//! analogue of generate.rs:695-711).
//!
//! Why a node-local content-addressed cache: every secondary on a
//! cluster is its own SLURM job and the framework hands them ALL the
//! same shared-FS staged tarball (`PodmanImageMetadata.remote_path`).
//! The naive "cp shared-FS -> per-job /tmp" the legacy bash does means
//! N concurrent secondaries on one node each pull the same ~GB file off
//! the shared FS at once (NFS/Lustre read contention → 50-90s/secondary
//! at higher --jobs), and sequential runs on a node re-pull it every
//! time. A node-local cache keyed by the tarball's content digest
//! collapses all of that to a single shared-FS read per (node, digest):
//! the first job on a node populates `/tmp/<prefix>-imgcache/<digest>.tar`,
//! every later secondary (same run or a later one) reuses it with NO
//! shared-FS read. The digest in the path makes invalidation automatic
//! — a rebuilt image gets a new digest → a new cache path → the stale
//! entry is simply never referenced again.
//!
//! Concurrency is lock-free: a cold-node populate copies to a uniquely
//! named temp file then `rename`s it onto the digest-named target.
//! `rename(2)` within one filesystem is atomic, so a concurrent reader
//! sees either no cache file or the fully-written one — never a torn
//! read. Two cold-start writers race harmlessly: both copy identical
//! bytes (same digest) and the last rename wins. No lock means no
//! stale-lock fragility on a crashed populate; an interrupted copy only
//! leaves an unreferenced temp file (node /tmp is ephemeral).
//!
//! Podman storage stays per-job (`$PODMAN_STORAGE` under the random
//! `$RNDTMP`) untouched: the `load` still runs once per job into fresh
//! storage. Only the *bytes-from-shared-FS* step is cached. Sharing
//! podman storage itself is deliberately NOT done here — the per-job
//! storage root is load-bearing for the orphan-container preflight
//! sweep (generate.rs preflight) and concurrent writers would race
//! podman's layer DB.

use crate::bin_resolve::ResolvedBins;
use crate::dirs::Layout;
use dynrunner_slurm_wrapper_config::WrapperConfig;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Operator-facing marker the bash prints to STDOUT on load failure
/// (generate.rs:707). Consumers scan the `.out` file for this first.
const LOAD_FAILED_STDOUT: &str =
    "ERROR: image load failed; secondary cannot start. See the .err file for the runtime's diagnostic.";
/// The terser STDERR companion (generate.rs:708).
const LOAD_FAILED_STDERR: &str = "ERROR: image load failed; secondary cannot start.";

/// Hard cap on the number of node-local cache entries (digest-named
/// `*.tar` files) kept after a fresh populate. Each entry is a full
/// ~1.6 GB image tar (one per build/rev), so an unbounded cache on a
/// long-lived node accumulates one tar per rev until the host hits
/// ENOSPC. Keeping the newest few covers the realistic working set — the
/// current rev plus the immediately prior one(s) a still-draining older
/// run on the same node may be mid-load on — while bounding total cache
/// footprint to `KEEP_LAST_N` tars per consumer prefix. The current image
/// always occupies one of these slots and is never evicted, so the cap
/// holds regardless of the current entry's mtime rank (see
/// [`evict_stale_cache_entries`]).
const KEEP_LAST_N: usize = 3;

/// Node-local content-addressed cache entry for the image tarball,
/// keyed by `cfg.image_digest`. `None` when the digest is empty
/// (back-compat / test callers) — those fall back to the per-job copy.
///
/// Path shape: `<layout.image_cache_root>/<digest>.tar`, i.e.
/// `/tmp/<name_prefix>-imgcache/<digest>.tar` in production. The cache
/// root lives on `Layout` (the node-path concern) outside the per-job
/// `$RNDTMP` so the entry outlives a single job and is reused by every
/// secondary on the node. The digest is the file stem, so any content
/// change is a new path (automatic invalidation).
fn cache_path(cfg: &WrapperConfig, layout: &Layout) -> Option<PathBuf> {
    if cfg.image_digest.is_empty() {
        return None;
    }
    Some(
        layout
            .image_cache_root
            .join(format!("{}.tar", cfg.image_digest)),
    )
}

/// Bound the node-local cache to a HARD cap of `KEEP_LAST_N` digest-named
/// `*.tar` entries, evicting the oldest non-current ones by explicit
/// per-file `remove_file`. The cache is content-addressed and only ever
/// grows on a fresh populate (a new digest = a new file that is never
/// overwritten), so without this it accumulates one ~1.6 GB tar per
/// build/rev forever until the node hits ENOSPC. Run after publishing a
/// new entry so the just-written file is on disk and counted.
///
/// `keep_digest` is the content hash of the image THIS job is loading. Its
/// `<digest>.tar` is NEVER an eviction candidate — it reserves one of the
/// `KEEP_LAST_N` slots, and the newest `KEEP_LAST_N - 1` non-current
/// entries fill the rest; everything older is evicted. So the total is a
/// hard cap of `KEEP_LAST_N` regardless of the current entry's mtime rank.
/// This protects the live image against eviction by a racing same-node
/// secondary that just populated newer digests (the cache root is shared
/// per `name_prefix` across all secondaries on the node — see
/// `dirs.rs::Layout::image_cache_root`).
///
/// Eviction is best-effort: a failure to stat or unlink any single entry
/// is logged and skipped, never propagated — a leaked tar is a disk-space
/// concern, not a reason to fail an otherwise-loadable image. Only files
/// directly under `cache_dir` ending in `.tar` are considered; the
/// dot-prefixed `.<digest>.<suffix>.<pid>.tmp` in-flight populate temps
/// are ignored (they are not `*.tar` and are owned by a live writer).
fn evict_stale_cache_entries(cache_dir: &Path, keep_digest: &str) {
    let keep_name = format!("{keep_digest}.tar");
    // Collect (mtime, path) for every committed cache entry.
    let read = match std::fs::read_dir(cache_dir) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "WARNING: cannot scan image cache {} for eviction ({e}); skipping",
                cache_dir.display()
            );
            return;
        }
    };
    // Collect only NON-current committed tars as eviction candidates; the
    // current image is unconditionally retained and reserves one slot of
    // the KEEP_LAST_N budget, so the total cache size is a HARD cap of
    // KEEP_LAST_N regardless of the current entry's mtime rank.
    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    for ent in read.flatten() {
        let path = ent.path();
        // Only committed digest tars; skip dirs, temps, anything non-`.tar`.
        let is_tar = path.extension().is_some_and(|e| e == "tar");
        if !is_tar {
            continue;
        }
        // Never an eviction candidate: the live image this job is loading.
        if path.file_name().is_some_and(|n| n == keep_name.as_str()) {
            continue;
        }
        match ent.metadata() {
            Ok(meta) if meta.is_file() => {
                let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                candidates.push((mtime, path));
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "WARNING: cannot stat cache entry {} for eviction ({e}); skipping",
                    path.display()
                );
            }
        }
    }
    // Reserve one slot for the current image; keep the newest (N-1) of the
    // remaining non-current entries, evict the rest.
    let keep_non_current = KEEP_LAST_N.saturating_sub(1);
    if candidates.len() <= keep_non_current {
        return; // Within bound; nothing to evict.
    }
    // Newest mtime first, so the first `keep_non_current` are retained.
    candidates.sort_by_key(|(mtime, _)| std::cmp::Reverse(*mtime));
    for (_, path) in candidates.into_iter().skip(keep_non_current) {
        match std::fs::remove_file(&path) {
            Ok(()) => println!("Evicted stale image cache entry: {}", path.display()),
            Err(e) => eprintln!(
                "WARNING: failed to evict stale cache entry {} ({e}); skipping",
                path.display()
            ),
        }
    }
}

/// Copy the shared-FS image straight to the per-job writable scratch
/// (`layout.local_image`, under the random `$RNDTMP` this uid owns) and
/// return that path. This is the cache-free path: used when there is no
/// content key to cache by, AND as the universal fallback whenever the
/// node-local cache dir cannot be written (foreign-owned root, ENOSPC,
/// EROFS, ...). The per-job scratch is always this uid's own writable
/// tree, so this never collides across uids — it trades the cache's
/// shared-FS-read saving for guaranteed startup.
fn copy_per_job(cfg: &WrapperConfig, layout: &Layout) -> Result<PathBuf, String> {
    println!("Copying image to local temp directory...");
    std::fs::copy(&cfg.image_path, &layout.local_image).map_err(|e| {
        format!(
            "failed to copy image {} to {}: {e}",
            cfg.image_path,
            layout.local_image.display()
        )
    })?;
    println!("Image copied to: {}", layout.local_image.display());
    Ok(layout.local_image.clone())
}

/// Ensure a node-local copy of the image tar exists and return the path
/// the `load` command should read as `$LOCAL_IMAGE`.
///
/// With a digest: reuse the cache file if present (no shared-FS read),
/// else populate it atomically (copy-to-temp + `rename`). Without a
/// digest: copy straight to the per-job `layout.local_image` (legacy
/// behaviour).
fn provide_local_image(cfg: &WrapperConfig, layout: &Layout) -> Result<PathBuf, String> {
    let Some(cache) = cache_path(cfg, layout) else {
        // No content key → cannot safely cache; copy per job.
        return copy_per_job(cfg, layout);
    };

    // Cache hit: the digest-named file already exists on this node. The
    // digest IS the content identity, so an existing entry is exactly the
    // image we want — reuse it with no shared-FS read.
    if cache.exists() {
        println!("Image cache hit: reusing {}", cache.display());
        return Ok(cache);
    }

    // Cache miss: populate atomically. Copy to a uniquely named temp in
    // the SAME directory (so `rename` stays within one filesystem and is
    // atomic), then rename onto the digest-named target.
    let cache_dir = cache
        .parent()
        .expect("cache_path always has a parent under /tmp");
    // A failure to provision the cache dir is NEVER fatal: the cache is an
    // optimisation, not a correctness requirement. An unwritable cache dir
    // (e.g. a foreign-uid-owned `/tmp/<prefix>-imgcache` collision, or any
    // other ENOSPC/EROFS/EACCES) falls back to the per-job writable copy so
    // the secondary still starts. (uid-namespacing the root in dirs.rs is
    // the primary fix for the collision; this is the belt-and-braces guard
    // against any unexpected unwritable cache.)
    if let Err(e) = std::fs::create_dir_all(cache_dir) {
        eprintln!(
            "WARNING: cannot create image cache dir {} ({e}); falling back to per-job copy",
            cache_dir.display()
        );
        return copy_per_job(cfg, layout);
    }

    // Temp name disambiguated by (rand_suffix, pid): two cold-start
    // secondaries racing to populate the same digest each write their
    // own temp, so neither clobbers a half-written file of the other.
    let tmp = cache_dir.join(format!(
        ".{}.{}.{}.tmp",
        cfg.image_digest,
        cfg.rand_suffix,
        std::process::id(),
    ));
    println!(
        "Image cache miss: copying {} -> {}",
        cfg.image_path,
        cache.display()
    );
    if let Err(e) = std::fs::copy(&cfg.image_path, &tmp) {
        // Best-effort temp cleanup so a failed copy doesn't litter.
        let _ = std::fs::remove_file(&tmp);
        // Copying INTO the cache dir failed (classically EACCES on a
        // foreign-owned cache root that `create_dir_all` "succeeded" on
        // because it already existed). The cache is an optimisation: fall
        // back to the per-job writable copy rather than stranding the
        // secondary.
        eprintln!(
            "WARNING: cannot populate image cache temp {} ({e}); falling back to per-job copy",
            tmp.display()
        );
        return copy_per_job(cfg, layout);
    }
    // Atomic publish. A concurrent populate may have published first;
    // rename overwrites with byte-identical content, so the winner is
    // irrelevant. If rename fails (e.g. the dir vanished), fall back to
    // the temp file itself — the load can still read it this job.
    if let Err(e) = std::fs::rename(&tmp, &cache) {
        eprintln!(
            "WARNING: could not publish image cache entry {} ({e}); using per-job temp {}",
            cache.display(),
            tmp.display()
        );
        return Ok(tmp);
    }
    println!("Image cached at: {}", cache.display());
    // A fresh digest was just published — bound the cache so an old rev's
    // tar doesn't linger forever. Protect the digest we just loaded.
    evict_stale_cache_entries(cache_dir, &cfg.image_digest);
    Ok(cache)
}

/// Provide the image as a node-local file (cache-aware), then run
/// `cfg.load_command` via `bash -c` with
/// `LOCAL_IMAGE`/`PODMAN_STORAGE`/`PODMAN_RUN` (and the
/// wrapper-shell-scope `PODMAN_BIN`/`RM_BIN`) exported. `Err` carries the
/// operator-facing failure marker text.
pub fn copy_and_load(
    cfg: &WrapperConfig,
    layout: &Layout,
    bins: &ResolvedBins,
) -> Result<(), String> {
    let local_image = provide_local_image(cfg, layout)?;
    run_load(cfg, layout, bins, &local_image)
}

/// Run the load command against an already-node-local `$LOCAL_IMAGE`.
fn run_load(
    cfg: &WrapperConfig,
    layout: &Layout,
    bins: &ResolvedBins,
    local_image: &Path,
) -> Result<(), String> {
    // if ! {load_command}; then ... (generate.rs:700-711). The runtime's
    // own stdout/stderr is inherited so its diagnostic lands in the job's
    // .out/.err exactly as the bash left it.
    //
    // XDG_RUNTIME_DIR is set per-child to `layout.podman_run` (the bash
    // exported it globally at generate.rs:347 for podman's rootless storage
    // cookie; here it is a per-`Command` env so it never clobbers the
    // wrapper's own value that `shutdown_spawn`'s bus probe reads).
    //
    // The child mask reset (signals::child_pre_exec) restores an empty
    // signal mask before exec so the load command (and anything it spawns)
    // sees normal signal disposition rather than the wrapper's blocked set.
    println!("Loading image into container runtime...");
    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&cfg.load_command)
        .env("LOCAL_IMAGE", local_image)
        .env("PODMAN_STORAGE", &layout.podman_storage)
        .env("PODMAN_RUN", &layout.podman_run)
        .env("XDG_RUNTIME_DIR", &layout.podman_run)
        .env("PODMAN_BIN", &bins.podman)
        .env("RM_BIN", &bins.rm);
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        cmd.pre_exec(crate::signals::child_pre_exec());
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn load command via bash: {e}"))?;

    if !status.success() {
        println!("{LOAD_FAILED_STDOUT}");
        eprintln!("{LOAD_FAILED_STDERR}");
        return Err(LOAD_FAILED_STDOUT.to_string());
    }

    println!("Image loaded successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm_wrapper_config::ConnectionMode;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// Build a `WrapperConfig` with every field populated; the test
    /// overrides only `image_path`/`load_command`/`image_digest` per case.
    /// An empty `image_digest` bypasses the cache (per-job copy path);
    /// a non-empty one routes through the node-local content cache.
    fn cfg_with(image_path: String, load_command: String, image_digest: String) -> WrapperConfig {
        WrapperConfig {
            name_prefix: "asm".to_string(),
            rand_suffix: "2f1d4e89".to_string(),
            secondary_id: "sec-0".to_string(),
            image_path,
            image_tar_basename: "asm-tokenizer.tar".to_string(),
            image_digest,
            image_name: "asm-tokenizer".to_string(),
            image_tag: "latest".to_string(),
            load_command,
            container_command: "dynamic_runner._secondary_bootstrap".to_string(),
            cores_spec: "-2".to_string(),
            max_memory_spec: "-2G".to_string(),
            mem_manager_reserved_bytes: Some(524_288_000),
            secondary_module: "asm_tokenizer.secondary".to_string(),
            extra_run_args: vec!["--ulimit".to_string(), "nofile=8192:8192".to_string()],
            srcbins_network: "/net/srcbins".to_string(),
            output_network: "/net/out".to_string(),
            log_network: "/net/log".to_string(),
            dynrunner_network_dir: Some("/net/dynrunner".to_string()),
            connection: ConnectionMode::Standard {
                gateway_host: "gw.cluster".to_string(),
                gateway_port: 4433,
            },
            is_observer: false,
            shutdown_manager_bin_path: Some(PathBuf::from("/opt/dynrunner-slurm-shutdown")),
        }
    }

    /// A `Layout` rooted in `root`, with a dummy source tar written and
    /// its bytes returned so callers can assert the copy is faithful.
    /// `image_digest` selects the path under test: empty → per-job copy
    /// to `local_image`; non-empty → node-local content cache under the
    /// tempdir-rooted `image_cache_root` (kept inside `root` so the test
    /// never writes to the real `/tmp`).
    fn fixture(
        root: &std::path::Path,
        load_command: &str,
        image_digest: &str,
    ) -> (WrapperConfig, Layout, ResolvedBins, Vec<u8>) {
        let rndtmp = root.join("rndtmp");
        std::fs::create_dir_all(&rndtmp).unwrap();
        let src = root.join("source.tar");
        let bytes = b"docker-archive-bytes".to_vec();
        std::fs::write(&src, &bytes).unwrap();

        let layout = Layout {
            rndtmp: rndtmp.clone(),
            container_name: "asm-2f1d4e89-sec-0".to_string(),
            src_tmp: rndtmp.join("src"),
            out_tmp: rndtmp.join("out"),
            log_tmp: rndtmp.join("log"),
            work_tmp: rndtmp.join("work"),
            podman_storage: rndtmp.join("storage"),
            podman_run: rndtmp.join("run"),
            socket_dir: rndtmp.join("sockets"),
            cmd_socket: rndtmp.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-2f1d4e89".to_string(),
            shutdown_log_dir: rndtmp.join("log-network/sec-0"),
            shutdown_log_path: rndtmp.join("log-network/sec-0/shutdown-manager.log"),
            wrapper_log_path: rndtmp.join("log-network/sec-0/wrapper.log"),
            shutdown_pid_file: rndtmp.join("shutdown-manager.pid"),
            local_image: rndtmp.join("asm-tokenizer.tar"),
            // Tempdir-rooted cache root so cache-path tests stay isolated
            // from the real /tmp.
            image_cache_root: root.join("imgcache"),
        };
        let cfg = cfg_with(
            src.to_string_lossy().into_owned(),
            load_command.to_string(),
            image_digest.to_string(),
        );
        let bins = ResolvedBins {
            podman: "podman".to_string(),
            rm: "rm".to_string(),
        };
        (cfg, layout, bins, bytes)
    }

    #[test]
    fn load_success_copies_image() {
        let dir = tempdir().unwrap();
        // Empty digest → legacy per-job copy to local_image.
        let (cfg, layout, bins, bytes) = fixture(dir.path(), "true", "");

        copy_and_load(&cfg, &layout, &bins).expect("load should succeed");

        assert!(layout.local_image.exists());
        assert_eq!(std::fs::read(&layout.local_image).unwrap(), bytes);
    }

    #[test]
    fn load_failure_returns_err() {
        let dir = tempdir().unwrap();
        let (cfg, layout, bins, _) = fixture(dir.path(), "false", "");

        let err = copy_and_load(&cfg, &layout, &bins).expect_err("non-zero load command must fail");
        assert_eq!(err, LOAD_FAILED_STDOUT);
        // The copy still happened before the load attempt.
        assert!(layout.local_image.exists());
    }

    #[test]
    fn load_command_sees_exported_env() {
        let dir = tempdir().unwrap();
        let probe = dir.path().join("probe.txt");
        // Echo every var the wrapper shell scope provides into one file.
        let cmd = format!(
            "printf '%s\\n%s\\n%s\\n%s\\n%s\\n' \
             \"$LOCAL_IMAGE\" \"$PODMAN_STORAGE\" \"$PODMAN_RUN\" \"$PODMAN_BIN\" \"$RM_BIN\" > {}",
            probe.display()
        );
        let (cfg, layout, bins, _) = fixture(dir.path(), &cmd, "");

        copy_and_load(&cfg, &layout, &bins).expect("probe load should succeed");

        let got = std::fs::read_to_string(&probe).unwrap();
        let lines: Vec<&str> = got.lines().collect();
        assert_eq!(lines[0], layout.local_image.to_string_lossy());
        assert_eq!(lines[1], layout.podman_storage.to_string_lossy());
        assert_eq!(lines[2], layout.podman_run.to_string_lossy());
        assert_eq!(lines[3], bins.podman);
        assert_eq!(lines[4], bins.rm);
    }

    // ---- node-local content-addressed cache (non-empty digest) ----

    /// Cache miss populates the digest-named entry, the load reads it,
    /// and the per-job `local_image` is NOT touched (the shared-FS copy
    /// landed in the cache, not in the per-job scratch).
    #[test]
    fn cache_miss_populates_and_loads_from_cache() {
        let dir = tempdir().unwrap();
        let digest = "deadbeefcafe";
        let (cfg, layout, bins, bytes) = fixture(dir.path(), "true", digest);

        copy_and_load(&cfg, &layout, &bins).expect("load should succeed");

        let entry = layout.image_cache_root.join(format!("{digest}.tar"));
        assert!(entry.exists(), "cache entry must be populated");
        assert_eq!(std::fs::read(&entry).unwrap(), bytes);
        assert!(
            !layout.local_image.exists(),
            "cache path must not write the per-job local_image"
        );
    }

    /// `$LOCAL_IMAGE` handed to the load command points at the cache
    /// entry, not the per-job scratch, when a digest is set.
    #[test]
    fn cache_load_command_sees_cache_path() {
        let dir = tempdir().unwrap();
        let digest = "0011223344ff";
        let probe = dir.path().join("probe.txt");
        let cmd = format!("printf '%s' \"$LOCAL_IMAGE\" > {}", probe.display());
        let (cfg, layout, bins, _) = fixture(dir.path(), &cmd, digest);

        copy_and_load(&cfg, &layout, &bins).expect("probe load should succeed");

        let entry = layout.image_cache_root.join(format!("{digest}.tar"));
        let seen = std::fs::read_to_string(&probe).unwrap();
        assert_eq!(seen, entry.to_string_lossy());
    }

    /// A pre-existing cache entry is reused verbatim WITHOUT re-reading
    /// the source tarball — the load reads the cached bytes even after
    /// the shared-FS source is deleted.
    #[test]
    fn cache_hit_skips_source_read() {
        let dir = tempdir().unwrap();
        let digest = "abc123def456";
        let probe = dir.path().join("probe.txt");
        let cmd = format!("cat \"$LOCAL_IMAGE\" > {}", probe.display());
        let (cfg, layout, bins, _) = fixture(dir.path(), &cmd, digest);

        // Pre-seed the cache with distinguishable content.
        let entry = layout.image_cache_root.join(format!("{digest}.tar"));
        std::fs::create_dir_all(&layout.image_cache_root).unwrap();
        let cached_bytes = b"already-cached-layers".to_vec();
        std::fs::write(&entry, &cached_bytes).unwrap();

        // Delete the shared-FS source: a cache hit must not touch it.
        std::fs::remove_file(&cfg.image_path).unwrap();

        copy_and_load(&cfg, &layout, &bins).expect("cache hit must not need the source");

        assert_eq!(std::fs::read(&probe).unwrap(), cached_bytes);
    }

    // ---- unwritable-cache fallback (the cross-uid collision fix) ----

    /// When the cache dir exists but is NOT writable by this process (the
    /// shape of a foreign-uid-owned `/tmp/<prefix>-imgcache` on a shared
    /// SLURM node), the populate copy INTO it fails with EACCES. The
    /// provide path must NOT return a fatal Err — it must fall back to the
    /// per-job writable copy so the secondary still starts.
    ///
    /// Drives `provide_local_image` DIRECTLY (the fn that owns the
    /// cache-vs-per-job decision) rather than `copy_and_load`: the fallback
    /// decision is made entirely before `run_load`, so this test needs no
    /// `bash -c` fork. Fork-free keeps it fully hermetic — only fs ops
    /// scoped to a unique tempdir, no subprocess — so it is robust under
    /// parallel execution and adds no fork pressure to the test pool.
    ///
    /// Revert-check: with the copy-failure `return copy_per_job(...)`
    /// reverted to `return Err(...)`, `provide_local_image` errors and this
    /// test fails — i.e. it pins the stranding bug.
    #[test]
    fn unwritable_cache_dir_falls_back_to_per_job_copy() {
        use std::os::unix::fs::PermissionsExt;

        // Running as root would bypass the permission check (root ignores
        // mode bits), so the EACCES we rely on never fires. Skip rather
        // than assert a false negative.
        if nix::unistd::geteuid().is_root() {
            eprintln!("skipping: euid 0 bypasses mode-bit permission checks");
            return;
        }

        let dir = tempdir().unwrap();
        let digest = "facefeed1234";
        let (cfg, layout, _bins, bytes) = fixture(dir.path(), "true", digest);

        // Pre-create the cache root mode-0555 (r-xr-xr-x): create_dir_all
        // sees it already exists and "succeeds", but a copy INTO it is
        // EACCES — exactly the foreign-owned-root collision shape.
        std::fs::create_dir_all(&layout.image_cache_root).unwrap();
        std::fs::set_permissions(
            &layout.image_cache_root,
            std::fs::Permissions::from_mode(0o555),
        )
        .unwrap();

        let provided = provide_local_image(&cfg, &layout);

        // Restore writability so tempdir cleanup can remove the tree.
        let _ = std::fs::set_permissions(
            &layout.image_cache_root,
            std::fs::Permissions::from_mode(0o755),
        );

        let provided =
            provided.expect("unwritable cache must fall back, not strand the secondary");

        // The fallback returned the per-job writable scratch path, faithfully
        // copied, and did NOT populate the unwritable cache.
        assert_eq!(
            provided, layout.local_image,
            "fallback must hand back the per-job local_image path"
        );
        assert!(
            layout.local_image.exists(),
            "fallback must write the per-job local_image"
        );
        assert_eq!(std::fs::read(&layout.local_image).unwrap(), bytes);
        assert!(
            !layout
                .image_cache_root
                .join(format!("{digest}.tar"))
                .exists(),
            "the unwritable cache must not have been populated"
        );
    }

    // ---- bounded eviction (the resource-leak fix) ----

    /// Helper: write a digest-named cache tar and stamp its mtime so the
    /// eviction sort order is deterministic (higher `age_secs` = older).
    fn seed_entry(cache_root: &std::path::Path, digest: &str, age_secs: u64) -> PathBuf {
        std::fs::create_dir_all(cache_root).unwrap();
        let path = cache_root.join(format!("{digest}.tar"));
        std::fs::write(&path, format!("layers-of-{digest}")).unwrap();
        let mtime = filetime_from_age(age_secs);
        set_mtime(&path, mtime);
        path
    }

    fn filetime_from_age(age_secs: u64) -> std::time::SystemTime {
        // A fixed recent base so all stamps are well-ordered and positive.
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000 - age_secs)
    }

    /// Set a file's mtime via libc `utimensat` (no extra crate; the
    /// wrapper already links libc). Atime is left at "now"/UTIME_OMIT.
    fn set_mtime(path: &std::path::Path, t: std::time::SystemTime) {
        use std::os::unix::ffi::OsStrExt;
        let secs = t
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as libc::time_t;
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: secs,
                tv_nsec: 0,
            },
        ];
        let c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(rc, 0, "utimensat failed for {}", path.display());
    }

    /// Populating an (N+1)-th distinct digest evicts the cache down to a
    /// hard cap of `KEEP_LAST_N`: the just-loaded current digest plus the
    /// `KEEP_LAST_N - 1` next-newest survive; everything older is unlinked.
    ///
    /// Revert-check: with the `evict_stale_cache_entries` call removed
    /// from `provide_local_image`, ALL pre-seeded entries persist and the
    /// `== KEEP_LAST_N` assertion fails — i.e. this test catches the leak.
    #[test]
    fn populate_evicts_down_to_keep_last_n() {
        let dir = tempdir().unwrap();
        let current = "ffffffffffff"; // the rev THIS job loads (cache miss)
        let (cfg, layout, bins, _) = fixture(dir.path(), "true", current);
        let root = &layout.image_cache_root;

        // Pre-seed KEEP_LAST_N + 2 OLDER distinct entries (all older than
        // the soon-to-be-published current one). seeded[0] is the newest
        // non-current; ages strictly increase with index.
        let mut seeded = Vec::new();
        for i in 0..(KEEP_LAST_N + 2) {
            let digest = format!("old{i:04x}");
            seeded.push(seed_entry(root, &digest, (i as u64 + 1) * 100));
        }

        // Cache miss on `current` → populate → eviction runs.
        copy_and_load(&cfg, &layout, &bins).expect("load should succeed");

        // Exactly KEEP_LAST_N committed entries remain (hard cap).
        let remaining: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "tar"))
            .collect();
        assert_eq!(
            remaining.len(),
            KEEP_LAST_N,
            "cache must be bounded to KEEP_LAST_N after populate; got {remaining:?}"
        );

        // The current (just-loaded) entry is always retained.
        let current_entry = root.join(format!("{current}.tar"));
        assert!(
            current_entry.exists(),
            "the current image must never be evicted"
        );

        // Current reserves one slot, so only the newest KEEP_LAST_N - 1
        // non-current entries survive; the rest are evicted.
        let keep_non_current = KEEP_LAST_N - 1;
        for kept in seeded.iter().take(keep_non_current) {
            assert!(kept.exists(), "newest non-current entries survive: {kept:?}");
        }
        for evicted in seeded.iter().skip(keep_non_current) {
            assert!(!evicted.exists(), "older non-current entries evicted: {evicted:?}");
        }
    }

    /// The live digest is protected even when its mtime is the OLDEST in
    /// the cache: it must survive while newer-but-not-current entries are
    /// evicted. This is the load-bearing concurrency guard — a racing
    /// same-node secondary that just touched newer digests must not evict
    /// the image this job is mid-load on.
    #[test]
    fn current_image_retained_even_when_oldest() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("imgcache");
        let current = "cccccccccccc";

        // Current entry is the OLDEST (largest age).
        seed_entry(&root, current, 9_000);
        // KEEP_LAST_N newer non-current entries; newer[0] is the newest,
        // newer[KEEP_LAST_N - 1] the oldest non-current.
        let mut newer = Vec::new();
        for i in 0..KEEP_LAST_N {
            newer.push(seed_entry(&root, &format!("new{i:04x}"), (i as u64 + 1) * 10));
        }

        evict_stale_cache_entries(&root, current);

        // Current survives despite being oldest.
        assert!(
            root.join(format!("{current}.tar")).exists(),
            "oldest-but-current entry must be retained"
        );
        // Hard cap holds: current + (KEEP_LAST_N - 1) newest non-current.
        let count = std::fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "tar"))
            .count();
        assert_eq!(
            count, KEEP_LAST_N,
            "protecting the current image must not inflate the bound"
        );
        // The oldest NON-current entry was the eviction victim, NOT the
        // older current one — the current-image guard reordered priority.
        assert!(
            !newer[KEEP_LAST_N - 1].exists(),
            "the oldest non-current entry must be evicted"
        );
    }

    /// In-flight populate temps (`.<digest>.<suffix>.<pid>.tmp`) and the
    /// current image are both untouched by eviction; only committed
    /// non-current `*.tar` over the bound are removed.
    #[test]
    fn eviction_ignores_temp_files() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("imgcache");
        std::fs::create_dir_all(&root).unwrap();
        let current = "aaaaaaaaaaaa";
        seed_entry(&root, current, 1);
        // A live populate temp for some other digest, freshly stamped.
        let temp = root.join(".bbbbbbbbbbbb.2f1d4e89.4242.tmp");
        std::fs::write(&temp, b"half-written").unwrap();

        evict_stale_cache_entries(&root, current);

        assert!(temp.exists(), "in-flight .tmp must never be evicted");
        assert!(root.join(format!("{current}.tar")).exists());
    }
}
