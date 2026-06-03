//! Driver tests for the [`MemProfileSampler`] orchestrator.
//!
//! The sampler does its sysfs reads against an arbitrary directory
//! handed in at `on_task_assigned` time, so we can stand up a fake
//! cgroup tree in a tempdir (three files: `memory.current`,
//! `memory.swap.current`, `memory.stat`) and drive the loop without
//! touching real cgroup-v2. The output dir is also a tempdir so each
//! test starts from a clean slate.
//!
//! All tests use the current-thread tokio runtime so we keep a single
//! deterministic scheduler — the sampler's background task is the
//! only other concurrent actor.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tempfile::tempdir;

use super::{MemProfileConfig, MemProfileSampler};

/// Lay down the three cgroup-v2 pseudo-files the reader looks for.
/// `current` lands in `memory.current`, `swap` in
/// `memory.swap.current`, plus a small representative `memory.stat`.
fn make_fake_cgroup(dir: &Path, current: u64, swap: u64) {
    fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
    fs::write(dir.join("memory.swap.current"), format!("{swap}\n")).unwrap();
    fs::write(dir.join("memory.stat"), "anon 100\nfile 200\npgfault 5\n").unwrap();
}

/// Decode a memprofile file into its complete JSONL lines. The file
/// is a concatenation of zstd frames each containing one
/// `\n`-terminated JSON object; the round-trip decoder reads to EOF.
fn decode_samples(path: &Path) -> Vec<serde_json::Value> {
    let file = fs::File::open(path).expect("open profile file");
    let mut decoder = zstd::stream::read::Decoder::new(file).expect("decoder");
    let mut decoded = Vec::new();
    // A truncated tail (shouldn't happen in these tests, but harmless
    // if it does) returns Err; bytes written before the error stand.
    let _ = decoder.read_to_end(&mut decoded);
    let text = std::str::from_utf8(&decoded).expect("utf8");
    text.split_terminator('\n')
        .map(|line| serde_json::from_str(line).expect("each line is JSON"))
        .collect()
}

/// Build a config rooted at `output_dir` with a tight `sample_interval`
/// so tests don't wait the 1-second production cadence.
fn config(output_dir: PathBuf, interval: Duration) -> MemProfileConfig {
    MemProfileConfig {
        output_dir,
        sample_interval: interval,
    }
}

/// Full happy path: open file via assign, accumulate samples across
/// several ticks, close on complete, shutdown drains the join handle.
#[tokio::test(flavor = "current_thread")]
async fn assign_tick_complete_round_trip() {
    let out = tempdir().expect("out dir");
    let cg = tempdir().expect("cg dir");
    make_fake_cgroup(cg.path(), 1_024, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(30)));

    sampler.on_task_assigned(
        "task-1".to_string(),
        7,
        cg.path().to_path_buf(),
        Instant::now(),
    );

    // 200ms / 30ms ≈ 6 ticks; leave headroom for the first-tick race
    // (the first `interval.tick()` resolves immediately and may run
    // before the Assign command is processed in select! ordering).
    tokio::time::sleep(Duration::from_millis(200)).await;

    sampler.on_task_completed("task-1".to_string());
    sampler.shutdown().await;

    let path = out.path().join("task-1.worker-7.memprofile.jsonl.zst");
    assert!(path.exists(), "profile file should be written");

    let samples = decode_samples(&path);
    assert!(
        samples.len() >= 3,
        "expected >= 3 samples within 200ms at 30ms tick, got {}",
        samples.len()
    );
    for s in &samples {
        assert_eq!(s["memory_current"].as_u64().unwrap(), 1_024);
        assert_eq!(s["worker_id"].as_u64().unwrap(), 7);
    }
}

/// Worker EOF (no `TaskCompleted` arrives) must still close the
/// open profile. Documents the disconnect-flush invariant the
/// transport hookup relies on.
#[tokio::test(flavor = "current_thread")]
async fn disconnect_with_active_task_flushes() {
    let out = tempdir().expect("out dir");
    let cg = tempdir().expect("cg dir");
    make_fake_cgroup(cg.path(), 2_048, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(30)));

    sampler.on_task_assigned(
        "task-1".to_string(),
        7,
        cg.path().to_path_buf(),
        Instant::now(),
    );
    tokio::time::sleep(Duration::from_millis(120)).await;

    sampler.on_worker_disconnected(7);
    sampler.shutdown().await;

    let path = out.path().join("task-1.worker-7.memprofile.jsonl.zst");
    assert!(path.exists(), "profile file should exist after disconnect");

    let samples = decode_samples(&path);
    assert!(
        !samples.is_empty(),
        "expected >= 1 sample before disconnect, got {}",
        samples.len()
    );
    for s in &samples {
        assert_eq!(s["memory_current"].as_u64().unwrap(), 2_048);
    }
}

/// Send Assign and Shutdown back-to-back without any sleep between.
/// Both are FIFO on the mpsc; Assign is processed first and opens the
/// file, Shutdown then drains. No panic, no I/O loss beyond the
/// (possibly zero) samples that hadn't ticked yet.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_race_no_panic() {
    let out = tempdir().expect("out dir");
    let cg = tempdir().expect("cg dir");
    make_fake_cgroup(cg.path(), 4_096, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(50)));

    sampler.on_task_assigned(
        "task-1".to_string(),
        7,
        cg.path().to_path_buf(),
        Instant::now(),
    );
    sampler.shutdown().await;

    let path = out.path().join("task-1.worker-7.memprofile.jsonl.zst");
    assert!(
        path.exists(),
        "file opened at assign-time should exist even on rapid shutdown",
    );
    // Sample count may be 0 (no tick fired before shutdown drained
    // the queue) or 1+ (a tick raced in before shutdown). Both are
    // acceptable — the test only documents no-panic + file-exists.
}

/// Defensive: a malformed `task_id` containing `..` or an absolute
/// prefix must not let a task write outside the configured output
/// dir. The sampler warn-logs and skips. (The framework's boundary
/// contract makes `task_id` non-optional + non-empty; the unsafe-
/// segment guard is the only remaining defensive check on this path.)
#[tokio::test(flavor = "current_thread")]
async fn unsafe_task_id_skipped() {
    let out = tempdir().expect("out dir");
    let cg = tempdir().expect("cg dir");
    make_fake_cgroup(cg.path(), 1_024, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(30)));

    sampler.on_task_assigned(
        "../../etc/passwd".to_string(),
        7,
        cg.path().to_path_buf(),
        Instant::now(),
    );
    sampler.on_task_assigned(
        "/absolute/path".to_string(),
        8,
        cg.path().to_path_buf(),
        Instant::now(),
    );

    tokio::time::sleep(Duration::from_millis(80)).await;
    sampler.shutdown().await;

    let entries: Vec<_> = fs::read_dir(out.path())
        .expect("read output dir")
        .filter_map(Result::ok)
        .collect();
    assert!(
        entries.is_empty(),
        "no file should escape output dir, found {:?}",
        entries.iter().map(|e| e.path()).collect::<Vec<_>>(),
    );
}

/// asm-tokenizer's `task_id` shape includes slashes (e.g.
/// `nping/x86/clang/9/Os`). The writer is responsible for
/// `create_dir_all` on the parent; this test exercises the full path
/// through the sampler.
#[tokio::test(flavor = "current_thread")]
async fn slash_task_id_creates_nested_subdir() {
    let out = tempdir().expect("out dir");
    let cg = tempdir().expect("cg dir");
    make_fake_cgroup(cg.path(), 8_192, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(30)));

    sampler.on_task_assigned(
        "nping/x86/clang/9/Os".to_string(),
        3,
        cg.path().to_path_buf(),
        Instant::now(),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    sampler.on_task_completed("nping/x86/clang/9/Os".to_string());
    sampler.shutdown().await;

    let path = out
        .path()
        .join("nping")
        .join("x86")
        .join("clang")
        .join("9")
        .join("Os.worker-3.memprofile.jsonl.zst");
    assert!(
        path.exists(),
        "nested per-task file should exist at {path:?}"
    );
}

/// Two concurrent tasks on two different workers with different
/// cgroup snapshots write to two distinct files, each carrying its
/// own values.
#[tokio::test(flavor = "current_thread")]
async fn per_worker_isolation() {
    let out = tempdir().expect("out dir");
    let cg_a = tempdir().expect("cg a");
    let cg_b = tempdir().expect("cg b");
    make_fake_cgroup(cg_a.path(), 1_111, 0);
    make_fake_cgroup(cg_b.path(), 2_222, 0);

    let sampler =
        MemProfileSampler::spawn(config(out.path().to_path_buf(), Duration::from_millis(30)));

    sampler.on_task_assigned(
        "task-a".to_string(),
        1,
        cg_a.path().to_path_buf(),
        Instant::now(),
    );
    sampler.on_task_assigned(
        "task-b".to_string(),
        2,
        cg_b.path().to_path_buf(),
        Instant::now(),
    );

    tokio::time::sleep(Duration::from_millis(120)).await;

    sampler.on_task_completed("task-a".to_string());
    sampler.on_task_completed("task-b".to_string());
    sampler.shutdown().await;

    let path_a = out.path().join("task-a.worker-1.memprofile.jsonl.zst");
    let path_b = out.path().join("task-b.worker-2.memprofile.jsonl.zst");
    assert!(path_a.exists(), "task-a file should exist");
    assert!(path_b.exists(), "task-b file should exist");

    let samples_a = decode_samples(&path_a);
    let samples_b = decode_samples(&path_b);
    assert!(!samples_a.is_empty(), "task-a should have samples");
    assert!(!samples_b.is_empty(), "task-b should have samples");
    for s in &samples_a {
        assert_eq!(s["memory_current"].as_u64().unwrap(), 1_111);
        assert_eq!(s["worker_id"].as_u64().unwrap(), 1);
    }
    for s in &samples_b {
        assert_eq!(s["memory_current"].as_u64().unwrap(), 2_222);
        assert_eq!(s["worker_id"].as_u64().unwrap(), 2);
    }
}
