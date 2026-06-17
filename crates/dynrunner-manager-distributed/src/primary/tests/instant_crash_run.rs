//! THE run-level repro of asm-tokenizer run_20260612_095601: a task
//! whose worker dies INSTANTLY with a nonzero exit on every attempt
//! (consumer arg-validation raise at worker startup — before Ready,
//! before any wire error). Pre-fix, every death was reinjected
//! through the uncharged backpressure shape, so the task was
//! re-dispatched at memory speed forever — one hash re-executed
//! 24,323 times, fail_retry/fail_final flat at 0, no backoff, no
//! termination; this test (RED pre-fix: it times out) pins the fixed
//! end state:
//!
//!   1. the run TERMINATES (the instant-crash class is charged into
//!      the same failed_tasks → retry-bucket → permanence accounting
//!      the wire-failure and OOM classes use),
//!   2. the task is permanently failed after exactly its retry budget,
//!   3. attempts are PACED (the per-task re-dispatch backoff +
//!      startup-crash respawn backoff keep the typed-spawn count
//!      bounded and spaced — never a hot spin).

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn instantly_crashing_task_is_charged_paced_and_the_run_terminates() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    let outcome = tokio::time::timeout(
        Duration::from_secs(120),
        local.run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let typed_spawns: std::rc::Rc<std::cell::RefCell<Vec<std::time::Instant>>> =
                std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let factory = super::test_helpers::CrashingTypedWorkerFactory {
                typed_spawns: typed_spawns.clone(),
            };

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) = spawn_real_secondary_with_factory(
                "sec-0".into(),
                /* num_workers = */ 1,
                max_res,
                factory,
                /* retry_max_passes = */ 1,
            );

            // Wire the channel pair into the primary's transport.
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });
            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_millis(50),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // ONE task whose every execution attempt dies instantly:
            // the first-bind typed respawn produces a real subprocess
            // that exits 1 before Ready, every time.
            let binaries = vec![make_binary("doomed", 50)];

            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, binaries, deps);
            primary
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, ops, ope)
                .await
                .unwrap();

            let completed = primary.completed_count();
            let failed_residual = primary.failed_count();
            let passes_used = primary.retry_passes_used_for_test();
            drop(primary);
            let _ = sec_handle.await;

            (completed, failed_residual, passes_used, typed_spawns)
        }),
    )
    .await;

    // RED pre-fix: the uncharged backpressure requeue loop never
    // terminates and this timeout trips.
    let (completed, failed_residual, passes_used, typed_spawns) = outcome.expect(
        "the run must TERMINATE: an instantly-crashing task must reach \
         permanent failure within its retry budget, never re-dispatch \
         forever (the 24,323-redispatch production bug)",
    );

    assert_eq!(completed, 0, "the doomed task never completes");
    assert_eq!(
        failed_residual, 1,
        "the doomed task must be a permanent failure in the primary's ledger"
    );
    assert_eq!(
        passes_used, 1,
        "exactly the configured retry budget must be consumed"
    );

    // Pacing: attempts are BOUNDED (budget-shaped, not spin-shaped). The
    // permanent failure within the retry budget is the correctness pin
    // above (`failed_residual == 1`, `passes_used == 1`); this is the
    // anti-spin pin — a bounded, low count, never the production
    // hundreds-per-second spin. The exact physical subprocess-spawn count
    // is 1..=4.
    //
    // Why 1 is valid HERE (and not a retry-suppression regression): this
    // doomed task crashes its worker at SETUP — the type-shift respawn for
    // the initial-assignment dies before Ready, before the task binary
    // ever runs. With the decoupled (non-blocking) bring-up the secondary
    // re-sends a duplicate welcome (its setup gate never released), so the
    // primary re-serves the setup trio and the second startup-crash
    // terminal RACES that re-serve — landing while the task sits QUEUED
    // (re-dispatch backoff) — and is reclaimed by the idempotent
    // never-re-execute dedup ("terminal arrived for a task sitting
    // QUEUED"). Re-materializing a subprocess for a DETERMINISTIC startup
    // crash is pointless, so collapsing to one physical respawn is benign.
    // The retry BUDGET is still consumed (`passes_used == 1`) and the task
    // still reaches permanent failure.
    //
    // This does NOT suppress retry RE-EXECUTION for a genuine TRANSIENT
    // failure: see `recoverable_failure_succeeds_on_retry_pass`, which
    // pins `attempts["/tmp/flaky"] == 2` — a real fail-then-succeed task
    // PHYSICALLY re-executes in a second worker invocation POST the
    // decoupled-bring-up change (its original terminal settles into
    // `failed_tasks` and the retry pass re-dispatches LATER, so there is no
    // old-terminal-racing-the-queued-retry to dedup). The blocking-wait era
    // happened to separate this instant-crash's two attempts into two
    // spawns; that count was a timing artifact, not a correctness
    // requirement.
    let spawns = typed_spawns.borrow();
    assert!(
        (1..=4).contains(&spawns.len()),
        "typed-spawn attempts must be budget-shaped (bounded, not a spin), got {}",
        spawns.len()
    );
    // When the retry attempt DID materialize a second physical spawn, it
    // must be backoff-spaced (never a hot re-spawn). Skipped when the
    // retry was deduped to a single spawn (nothing to space).
    if spawns.len() >= 2 {
        let gap = spawns[1].duration_since(spawns[0]);
        assert!(
            gap >= Duration::from_millis(400),
            "attempts must be separated by a backoff window, got {gap:?}"
        );
    }
}
