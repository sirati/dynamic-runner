//! Local-mode integration smoke for the per-task memory profiler.
//!
//! Gated on cgroup-v2 with the memory controller delegated — on hosts
//! without that (Docker default, CI without `--privileged`) the test
//! emits an `eprintln!` explaining the skip and returns success. The
//! gate predicate mirrors `crate::cgroup::setup_worker_cgroup`'s
//! controller check: presence of `memory` in
//! `/sys/fs/cgroup/cgroup.controllers`.
//!
//! What this test pins (the EXTERNAL contract a downstream consumer
//! sees):
//!
//!   * **Path composition.** The on-disk layout is
//!     `{output_dir}/memprofile/{task_id}.worker-{N}.memprofile.jsonl.zst`.
//!     The `memprofile/` subdir join is the plumbing layer's
//!     responsibility (see `dynrunner_pyo3::config::local_manager` for
//!     the production join site); the integration test composes the
//!     same `MEMPROFILE_SUBDIR` constant to keep the two pinned
//!     together.
//!   * **Round-trip.** Each `.jsonl.zst` file decodes as a sequence
//!     of self-contained zstd frames, each frame containing exactly
//!     one `\n`-terminated JSON object matching the `Sample` schema.
//!   * **`memory_current > 0`.** Confirms the sampler actually read
//!     the fake cgroup leaf (not just wrote a stub).
//!   * **File-per-task.** Two distinct `task_id`s assigned in the
//!     same run produce two distinct output files.
//!
//! Why this is NOT a "real LocalManager" boot: the in-crate
//! `manager::tests::memprofile_run_level_smoke` already pins the
//! sampler-construction-and-teardown wiring against a real
//! `LocalManager`, and `memprofile_hook_writes_profile_with_fake_subcgroup`
//! pins the hook-surface contract with an injected subcgroup. The
//! seams those tests use (`install_sampler_for_test`,
//! `install_worker_subcgroup_for_test`) are `pub(super)` — not
//! reachable from this integration target. Booting a real
//! `LocalManager` with a real `WorkerFactory` from outside the crate
//! requires the heavy Python-subprocess scaffolding seen in
//! `tests/subprocess_integration.rs`, which would duplicate
//! coverage. So this test exercises ONLY the `memprofile` module's
//! public API surface — the contract an external consumer of the
//! framework would actually depend on.

use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use dynrunner_manager_local::memprofile::config::MEMPROFILE_SUBDIR;
use dynrunner_manager_local::memprofile::{MemProfileConfig, MemProfileSampler};

/// Runtime probe — same shape as the plan's "5-line
/// `cgroup_v2_with_memory_available`" gate. Returns `true` only when
/// `/sys/fs/cgroup/cgroup.controllers` exists AND advertises the
/// `memory` token. On hosts without delegated v2 (Docker default,
/// some CI runners) the file may be missing OR exist but list only
/// a subset of controllers — both cases return `false` and the test
/// SKIPS rather than fails.
fn cgroup_v2_with_memory_available() -> bool {
    fs::read_to_string("/sys/fs/cgroup/cgroup.controllers")
        .map(|c| c.split_whitespace().any(|t| t == "memory"))
        .unwrap_or(false)
}

/// Lay down the three cgroup-v2 pseudo-files the sampler's
/// `cgroup_reader` looks for. Returning a stable triple keeps the
/// per-task assertions simple (`memory_current` is exactly the value
/// passed in here, regardless of how many ticks fire).
fn write_fake_cgroup_leaf(leaf: &Path, memory_current: u64, swap_current: u64) {
    fs::create_dir_all(leaf).expect("create leaf dir");
    fs::write(leaf.join("memory.current"), format!("{memory_current}\n"))
        .expect("write memory.current");
    fs::write(
        leaf.join("memory.swap.current"),
        format!("{swap_current}\n"),
    )
    .expect("write memory.swap.current");
    fs::write(leaf.join("memory.stat"), "anon 4096\nfile 0\npgfault 7\n")
        .expect("write memory.stat");
}

/// Decode all complete zstd frames in `path` into the JSONL lines
/// they carry. The file is a concatenation of one-frame-per-sample;
/// the decoder reads through every frame and stops cleanly at the
/// last complete one. A truncated tail returns `Err` from
/// `read_to_end`; the bytes written before the error stand — same
/// resilience contract real consumers (`zstd -dc`) get.
fn decode_samples(path: &Path) -> Vec<serde_json::Value> {
    let file = fs::File::open(path)
        .unwrap_or_else(|e| panic!("open profile file {}: {e}", path.display()));
    let mut decoder = zstd::stream::read::Decoder::new(file).expect("zstd decoder");
    let mut decoded = Vec::new();
    let _ = decoder.read_to_end(&mut decoded);
    let text = std::str::from_utf8(&decoded).expect("decoded bytes are utf-8");
    text.split_terminator('\n')
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("every JSONL line must parse as Sample JSON; got {line:?}: {e}")
            })
        })
        .collect()
}

/// Assert the schema invariants the plan's `Sample` struct documents.
/// Tries the top-level convenience keys first (the contract callers
/// rely on for `jq`-style access), then walks `memory_stat` to
/// confirm the verbatim kernel passthrough survived the round trip.
fn assert_sample_shape(sample: &serde_json::Value, expected_worker_id: u64) {
    assert!(
        sample["t_ns"].as_u64().is_some(),
        "t_ns must be a u64; got {sample}"
    );
    assert!(
        sample["t_rel_ns"].as_u64().is_some(),
        "t_rel_ns must be a u64; got {sample}"
    );
    assert_eq!(
        sample["worker_id"].as_u64(),
        Some(expected_worker_id),
        "worker_id mismatch in sample {sample}"
    );
    assert!(
        sample["memory_current"].as_u64().is_some(),
        "memory_current must be a u64; got {sample}"
    );
    assert!(
        sample["swap_current"].as_u64().is_some(),
        "swap_current must be a u64; got {sample}"
    );
    assert!(
        sample["memory_stat"].is_object(),
        "memory_stat must be a JSON object; got {sample}"
    );

    // memory.stat passthrough: the fake leaf wrote `anon 4096`, the
    // sampler reads it verbatim, the writer round-trips it as u64.
    let memory_stat = sample["memory_stat"].as_object().unwrap();
    let anon = memory_stat
        .get("anon")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("anon key missing from memory_stat: {sample}"));
    assert_eq!(
        anon, 4096,
        "memory.stat anon value should round-trip verbatim"
    );
}

/// End-to-end: drive two task assignments through the public
/// `MemProfileSampler` API and verify the two output files land at
/// the documented path shape, each containing at least one valid
/// `Sample` with `memory_current > 0`. Optionally cross-checks the
/// frame format against the `zstd` CLI when available — pinning that
/// real consumers reading via `zstd -dc <file>` recover the same
/// sample stream.
///
/// Uses the current-thread runtime to match the in-crate
/// memprofile driver tests (and because this crate's dev-deps
/// don't pull in tokio's `rt-multi-thread` feature). The sampler
/// itself spawns its background tick task on whatever runtime is
/// current, so single-threaded is sufficient.
#[tokio::test(flavor = "current_thread")]
async fn memprofile_smoke_local_two_tasks_produces_two_files() {
    if !cgroup_v2_with_memory_available() {
        eprintln!("skipping memprofile_smoke: cgroup-v2 with memory controller not available");
        return;
    }

    // Output dir composition mirrors what the PyO3 plumbing layer
    // (`config::local_manager::resolve_memprofile_output_dir`) does:
    // the operator hands the framework an `output_dir`, the framework
    // joins `MEMPROFILE_SUBDIR` before constructing the sampler.
    // Composing the constant here pins the two layers together — if
    // the plumbing layer ever changed the join, this test would
    // continue to assert the public contract.
    let run_output = tempfile::tempdir().expect("run output tempdir");
    let memprofile_dir = run_output.path().join(MEMPROFILE_SUBDIR);

    // Two distinct fake cgroup leaves, one per "worker". The leaf
    // values intentionally differ (1024 vs 2048) so a buggy sampler
    // that read the wrong leaf would surface as a wrong-value
    // assertion failure instead of a passing test.
    let cg_root = tempfile::tempdir().expect("cgroup tempdir");
    let leaf_w0 = cg_root.path().join("worker-0");
    let leaf_w1 = cg_root.path().join("worker-1");
    write_fake_cgroup_leaf(&leaf_w0, 1024, 0);
    write_fake_cgroup_leaf(&leaf_w1, 2048, 0);

    // Tight sample interval keeps the test responsive — production
    // cadence is 1 s; tests don't pay that. The 30 ms / 200 ms pair
    // mirrors the in-crate driver tests' cadence.
    let sampler = MemProfileSampler::spawn(MemProfileConfig {
        output_dir: memprofile_dir.clone(),
        sample_interval: Duration::from_millis(30),
    });

    let started = Instant::now();
    sampler.on_task_assigned("task-A".to_string(), 0, leaf_w0.clone(), started);
    sampler.on_task_assigned("task-B".to_string(), 1, leaf_w1.clone(), started);

    // Let the tick loop fire several times so each writer accumulates
    // at least one frame. 200 ms / 30 ms ≈ 6 ticks worst case, plenty
    // for the assertion below.
    tokio::time::sleep(Duration::from_millis(200)).await;

    sampler.on_task_completed("task-A".to_string());
    sampler.on_task_completed("task-B".to_string());

    // `shutdown` drains the command queue and joins the background
    // task, so the on-disk files are final by the time it returns.
    sampler.shutdown().await;

    // File-per-task: each task_id maps to a distinct
    // `{task_id}.worker-{N}.memprofile.jsonl.zst` under the
    // composed `memprofile/` subdir.
    let file_a = memprofile_dir.join("task-A.worker-0.memprofile.jsonl.zst");
    let file_b = memprofile_dir.join("task-B.worker-1.memprofile.jsonl.zst");

    assert!(
        file_a.exists(),
        "expected per-task file for task-A at {}",
        file_a.display()
    );
    assert!(
        file_b.exists(),
        "expected per-task file for task-B at {}",
        file_b.display()
    );
    assert_ne!(
        file_a, file_b,
        "file-per-task contract: two task_ids must land at distinct paths"
    );

    // Round-trip + schema invariants for each file. The expected
    // `memory_current` matches the value written into the fake
    // leaf — proves the sampler actually read the leaf rather than
    // emitting a stub.
    let samples_a = decode_samples(&file_a);
    let samples_b = decode_samples(&file_b);
    assert!(
        !samples_a.is_empty(),
        "expected >= 1 sample in task-A file, got 0"
    );
    assert!(
        !samples_b.is_empty(),
        "expected >= 1 sample in task-B file, got 0"
    );

    for s in &samples_a {
        assert_sample_shape(s, 0);
        assert_eq!(
            s["memory_current"].as_u64(),
            Some(1024),
            "memory_current should match the value written to leaf_w0"
        );
    }
    for s in &samples_b {
        assert_sample_shape(s, 1);
        assert_eq!(
            s["memory_current"].as_u64(),
            Some(2048),
            "memory_current should match the value written to leaf_w1"
        );
    }

    // Best-effort cross-check against the `zstd` CLI. If the binary
    // is not on PATH we skip the cross-check silently — the Rust
    // round-trip above is the load-bearing assertion. When the CLI
    // is present, this pins that consumers reading via
    // `zstd -dc <file>` recover the same sample stream the Rust
    // decoder produced.
    if Command::new("zstd").arg("-V").status().is_ok() {
        assert_cli_round_trip(&file_a, &samples_a);
        assert_cli_round_trip(&file_b, &samples_b);
    }
}

/// Decode `path` via `zstd -dc <path>` and compare the resulting
/// JSONL sample stream against `expected` (the Rust-decoder output).
/// Mismatches typically mean either (a) the Rust writer emitted
/// non-standard frames the CLI can't parse, or (b) the encoding
/// settings differ between the two. Both would break downstream
/// `jq`-style consumers — surfacing it here keeps the file format
/// honest.
fn assert_cli_round_trip(path: &Path, expected: &[serde_json::Value]) {
    let out = Command::new("zstd")
        .arg("-dc")
        .arg(path)
        .output()
        .expect("zstd -dc execution");
    assert!(
        out.status.success(),
        "zstd -dc failed for {}: {}",
        path.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = std::str::from_utf8(&out.stdout).expect("CLI stdout is utf-8");
    let cli_lines: Vec<serde_json::Value> = text
        .split_terminator('\n')
        .map(|line| serde_json::from_str(line).expect("each CLI-decoded line parses"))
        .collect();
    // Length parity is the minimum guarantee — full Value equality
    // would over-constrain (the kernel timestamps in `t_ns` /
    // `t_rel_ns` are read-time and can't be replayed identically),
    // so we compare on the contract-bearing fields per sample.
    assert_eq!(
        cli_lines.len(),
        expected.len(),
        "zstd CLI decoded a different number of samples ({}) than the Rust decoder ({}) for {}",
        cli_lines.len(),
        expected.len(),
        path.display()
    );
    for (cli, rust) in cli_lines.iter().zip(expected.iter()) {
        assert_eq!(
            cli["worker_id"], rust["worker_id"],
            "worker_id divergence between CLI and Rust decoders"
        );
        assert_eq!(
            cli["memory_current"], rust["memory_current"],
            "memory_current divergence between CLI and Rust decoders"
        );
        assert_eq!(
            cli["memory_stat"], rust["memory_stat"],
            "memory_stat divergence between CLI and Rust decoders"
        );
    }
}
