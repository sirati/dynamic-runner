//! Hook-level integration for the secondary's per-task memprofile
//! sampler. Mirrors the same-named test on the [`LocalManager`] side
//! (`crates/dynrunner-manager-local/src/manager/tests.rs ::
//! memprofile_hook_writes_profile_with_fake_subcgroup`):
//!
//!   1. Build a `SecondaryCoordinator` whose `WorkerPool` holds one
//!      worker without going through full setup (the in-process
//!      channel factories don't materialise real cgroup leaves; the
//!      hook-level test injects its OWN sampler + subcgroup handle so
//!      production paths stay bypassed deliberately).
//!   2. Hand-build a `SubcgroupHandle` pointing at a tempdir with
//!      cgroup-v2-shaped pseudo-files via the
//!      `SubcgroupHandle::from_cgroup_dir_for_test` seam.
//!   3. Spawn a `MemProfileSampler` with a tight sample interval and
//!      install it via `install_sampler_for_test`.
//!   4. Drive `notify_sampler_assigned` + `notify_sampler_completed`
//!      directly, then `shutdown_sampler_if_present` to drain.
//!   5. Assert the per-task profile file lands at the expected path
//!      and round-trips through `zstd::Decoder` + JSONL parse with the
//!      `memory_current` field reflecting the pseudo-file content.
//!
//! Single concern: pin the secondary-side hook wiring contract — when
//! the pool DOES surface a per-worker subcgroup AND the sampler is
//! constructed, the assign / complete hook fires and the file
//! materialises. No election state, no wire I/O, no `process_tasks`
//! loop.

#![cfg(test)]

use std::io::Read;
use std::time::{Duration, Instant};

use super::super::test_helpers::{election_config, make_secondary};

/// Construction-lifecycle pin: `install_sampler_for_test` makes
/// `sampler_is_some()` true; `shutdown_sampler_if_present` tears it
/// back down. The terminal-cleanup paths (Done, setup-deadline error,
/// panik) all funnel through `shutdown_sampler_if_present`, so this
/// pins the take+await contract those paths depend on without
/// driving the full operational loop.
#[tokio::test(flavor = "current_thread")]
async fn sampler_install_then_shutdown_is_idempotent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let out = tempfile::tempdir().expect("out tempdir");
            let mut config = election_config("sec-0");
            config.output_dir = Some(out.path().to_path_buf());
            let mut secondary = make_secondary(config);

            // Pre-install the sampler is None — the construction
            // site lives inside `run_until_setup_or_done_inner` which
            // we do NOT drive in this test.
            assert!(!secondary.sampler_is_some(), "sampler must be lazy");

            let sampler = dynrunner_manager_local::memprofile::MemProfileSampler::spawn(
                dynrunner_manager_local::memprofile::MemProfileConfig {
                    output_dir: out.path().to_path_buf(),
                    sample_interval: Duration::from_millis(20),
                },
            );
            secondary.install_sampler_for_test(sampler);
            assert!(
                secondary.sampler_is_some(),
                "install_sampler_for_test must populate the field"
            );

            // Drain returns the field to None; the second call is a
            // no-op so the terminal-cleanup paths can be invoked
            // unconditionally without needing their own
            // `Option::is_some` guard.
            secondary.shutdown_sampler_if_present().await;
            assert!(
                !secondary.sampler_is_some(),
                "shutdown_sampler_if_present must clear the field"
            );
            secondary.shutdown_sampler_if_present().await;
            assert!(
                !secondary.sampler_is_some(),
                "shutdown_sampler_if_present must be idempotent on None"
            );
        })
        .await;
}

/// End-to-end hook smoke: assign → tick → complete drops a real
/// `.jsonl.zst` file at the expected path with at least one sample
/// whose `memory_current` matches the fake pseudo-file content.
///
/// Mirrors `LocalManager::memprofile_hook_writes_profile_with_fake_subcgroup`
/// exactly — the only deltas are (a) `make_secondary` instead of
/// `LocalManager::new` + (b) the helper namespace
/// (`SecondaryCoordinator::install_*_for_test` instead of
/// `LocalManager::install_*_for_test`).
#[tokio::test(flavor = "current_thread")]
async fn memprofile_hook_writes_profile_with_fake_subcgroup() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Output dir for profile files.
            let out = tempfile::tempdir().expect("out tempdir");
            // Fake cgroup leaf with the three pseudo-files the
            // reader needs (memory.current, memory.swap.current,
            // memory.stat). Same shape as the kernel exposes under
            // `<workers>/worker-<id>/`.
            let cg = tempfile::tempdir().expect("cg tempdir");
            let leaf = cg.path().join("worker-0");
            std::fs::create_dir(&leaf).unwrap();
            std::fs::write(leaf.join("memory.current"), "4096\n").unwrap();
            std::fs::write(leaf.join("memory.swap.current"), "0\n").unwrap();
            std::fs::write(leaf.join("memory.stat"), "anon 4096\nfile 0\n").unwrap();

            // Build a coordinator with `output_dir` unset so the
            // production sampler construction in
            // `run_until_setup_or_done_inner` doesn't fire; we want
            // to drive the hooks directly against the installed
            // tempdir-rooted sampler below.
            let mut secondary = make_secondary(election_config("sec-0"));

            // Bring up one worker so the hook has a real
            // `WorkerHandle` slot to look up. `make_secondary`
            // configured `num_workers = 1`. `initialize_workers`
            // through the `FakeWorkerFactory` from `test_helpers`
            // skips real cgroup setup (the in-process channel
            // factory ignores its `_subcgroup` argument).
            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            // `initialize_workers` returns the spawned pool; the typed
            // lifecycle holds the pool only inside Configuring/Operational,
            // so land Operational and install the pool there (the same
            // place the production `enter_configuring → enter_operational`
            // flow moves it). `install_worker_subcgroup_for_test` /
            // `notify_sampler_*` reach the pool via `pool_mut()`, which is
            // only resolvable once the lifecycle carries a pool.
            let pool = secondary
                .initialize_workers(&mut factory)
                .await
                .expect("worker init");
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Inject the fake subcgroup onto worker 0 — production
            // would materialise this via `prepare_worker_subgroup`
            // at pool spawn time, gated on
            // `mem_manager_reserved_bytes` being `Some(_)`.
            // Injecting it directly lets us test the sampler wiring
            // end-to-end without driving the full cgroup-v2
            // initialisation (which is host-dependent and gated by
            // delegated subtree controls).
            let handle = dynrunner_manager_local::cgroup::SubcgroupHandle::from_cgroup_dir_for_test(
                leaf.clone(),
            );
            secondary.install_worker_subcgroup_for_test(0, handle);

            // Stand up the sampler with a tight sample interval so
            // the test doesn't pay the 1 s production cadence.
            // Direct construction (not via `run_until_setup_or_done`)
            // keeps the test focused on the hook surface.
            let sampler = dynrunner_manager_local::memprofile::MemProfileSampler::spawn(
                dynrunner_manager_local::memprofile::MemProfileConfig {
                    output_dir: out.path().to_path_buf(),
                    sample_interval: Duration::from_millis(20),
                },
            );
            secondary.install_sampler_for_test(sampler);

            // Drive the hooks. The hook only reads `task_id` so a
            // minimal hand-built TaskInfo is sufficient — the rest
            // of the assignment plumbing is the path under test on
            // the LocalManager side and irrelevant here.
            let binary: dynrunner_core::TaskInfo<super::super::test_helpers::TestId> =
                dynrunner_core::TaskInfo {
                    path: std::path::PathBuf::from("/tmp/task-A"),
                    size: 50,
                    identifier: super::super::test_helpers::TestId("task-A".into()),
                    phase_id: dynrunner_core::PhaseId::from("p"),
                    type_id: dynrunner_core::TypeId::from("default"),
                    affinity_id: None,
                    payload: serde_json::Value::Null,
                    task_id: "task-A".to_string(),
                    task_depends_on: vec![],
                    preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
                    preferred_version: Default::default(),
                    resolved_path: None,
                };
            secondary.notify_sampler_assigned(0, &binary);

            // Let several sample ticks fire so the writer
            // accumulates samples. The sample interval is 20 ms; 150
            // ms covers ~7 ticks with margin for scheduling jitter.
            // We don't pin a minimum count: even one sample is
            // enough to assert wiring works.
            let _ = Instant::now();
            tokio::time::sleep(Duration::from_millis(150)).await;

            secondary.notify_sampler_completed("task-A".to_string());

            // Drain through the same shutdown helper the production
            // teardown paths use — this both flushes the writer's
            // last frame and joins the background task so the on-disk
            // file is final by the time the call returns.
            secondary.shutdown_sampler_if_present().await;

            let expected = out.path().join("task-A.worker-0.memprofile.jsonl.zst");
            assert!(
                expected.exists(),
                "expected profile file at {}",
                expected.display()
            );
            // Round-trip: zstd-decode + JSONL parse. At least one
            // complete frame (sample) must have been written.
            let file = std::fs::File::open(&expected).expect("open profile");
            let mut decoder = zstd::stream::read::Decoder::new(file).expect("decoder");
            let mut decoded = Vec::new();
            let _ = decoder.read_to_end(&mut decoded);
            let text = std::str::from_utf8(&decoded).expect("utf8");
            let lines: Vec<&str> = text.split_terminator('\n').collect();
            assert!(
                !lines.is_empty(),
                "expected >= 1 sample in profile file, got 0 (raw: {decoded:?})"
            );
            let first: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
            assert_eq!(first["worker_id"].as_u64(), Some(0));
            assert_eq!(first["memory_current"].as_u64(), Some(4096));
        })
        .await;
}
