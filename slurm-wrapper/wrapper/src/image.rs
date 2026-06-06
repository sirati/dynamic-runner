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
        println!("Copying image to local temp directory...");
        std::fs::copy(&cfg.image_path, &layout.local_image).map_err(|e| {
            format!(
                "failed to copy image {} to {}: {e}",
                cfg.image_path,
                layout.local_image.display()
            )
        })?;
        println!("Image copied to: {}", layout.local_image.display());
        return Ok(layout.local_image.clone());
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
    std::fs::create_dir_all(cache_dir)
        .map_err(|e| format!("failed to create image cache dir {}: {e}", cache_dir.display()))?;

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
        return Err(format!(
            "failed to copy image {} to cache temp {}: {e}",
            cfg.image_path,
            tmp.display()
        ));
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
            podman_storage: rndtmp.join("storage"),
            podman_run: rndtmp.join("run"),
            socket_dir: rndtmp.join("sockets"),
            cmd_socket: rndtmp.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-2f1d4e89".to_string(),
            shutdown_log_dir: rndtmp.join("log-network/sec-0"),
            shutdown_log_path: rndtmp.join("log-network/sec-0/shutdown-manager.log"),
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
}
