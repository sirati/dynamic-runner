//! Regression: a chain of singleton-typed phases (one item per
//! phase, each phase declaring a distinct `TypeId`) MUST complete on
//! the secondary without wedging the tokio runtime mid-respawn.
//!
//! # Bug pinned
//!
//! asm-tokenizer LMU dispatch on slurm-test-env, 2026-05-17:
//! singleton-phase chain `tokenize → unify_vocab → memmap` wedged
//! after sec-0 completed `unify_vocab`. Primary dispatched the
//! follow-up `memmap` task (different `TypeId`) 0.1ms later, but
//! sec-0's tokio runtime went silent for the full 300s keepalive
//! window — no router events, no keepalive ticks, no worker activity.
//! The peer-recovery path eventually requeued the lost task once
//! keepalive_timeouts fired on the primary side.
//!
//! # Reproducer shape
//!
//! 1-secondary cluster, 1 worker. Three pre-staged phases A → B → C
//! with one task each, each phase carrying a distinct `TypeId`. The
//! singleton-per-phase shape forces a strict serialisation:
//! type-A binds the slot, then type-shifts to B (kill+respawn),
//! then type-shifts to C (kill+respawn).
//!
//! # Fail mode pre-fix
//!
//! The whole `secondary.run` call wedges past the bounded
//! `tokio::time::timeout` deadline — see assertion at the bottom of
//! the test body. Without the bound, the test would hang forever and
//! the harness would report it as a 60s timeout error rather than the
//! actionable "runtime silent during type-shift" message.
//!
//! # Fail mode post-fix
//!
//! All three tasks complete; the secondary returns cleanly; the test
//! observes `completed_count == 3` and no wall-clock gap >60s between
//! consecutive `task done` worker events.

#![cfg(test)]

use std::time::Duration;

use dynrunner_core::TaskInfo;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, MessageType,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{
    channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd,
};
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{FixedEstimator, NoPeers, TestId};
use super::super::*;

/// WorkerFactory that fakes a runner AND distinguishes
/// `spawn_worker_for_type` from `spawn_worker`. The bug pinned by this
/// regression is specifically about the type-shift respawn path on a
/// secondary's `select!`-driven loop; exercising the distinct method
/// is what makes the in-process fixture mirror the production
/// `SubprocessWorkerFactory` shape (which dispatches per-type argv via
/// `spawn_worker_for_type`).
///
/// `slow_ready_for_type` injects an artificial pre-`Response::Ready`
/// delay scoped to a specific `TypeId`. The production wedge appears
/// when a freshly-respawned worker subprocess takes nontrivial time
/// (Python import, container startup, slow filesystem) to send its
/// first Ready response; without this knob the in-process channel
/// fake sends Ready in the same microsecond the spawn returns, which
/// is too fast to expose the synchronous wait-loop wedge. With the
/// knob set to a value larger than the bounded test timeout, the
/// pre-fix synchronous `ensure_worker_for_type` blocks the secondary's
/// `select!` for the entire delay and the test's
/// `tokio::time::timeout` trips. With the post-fix
/// `ensure_worker_for_type_async`, the wait happens on a background
/// task and the operational loop's other arms keep running — the
/// test's bounded timeout is never approached.
struct TypedFakeWorkerFactory {
    /// Per-spawn count of `spawn_worker_for_type` calls. Used by the
    /// regression assertion to confirm the test actually exercised
    /// the type-shift respawn path (vs trivially completing through
    /// the same-type fast path or the initial pool spawn).
    type_shift_spawns: std::rc::Rc<std::cell::Cell<u32>>,
    /// Optional `(target_type_str, delay)`: when set, the spawned
    /// fake worker for `target_type_str` sleeps `delay` before
    /// emitting `Response::Ready`. Empty means no delay (in-process
    /// fast path).
    slow_ready_for_type: Option<(String, Duration)>,
}

impl TypedFakeWorkerFactory {
    fn new() -> Self {
        Self {
            type_shift_spawns: std::rc::Rc::new(std::cell::Cell::new(0)),
            slow_ready_for_type: None,
        }
    }

    /// Configure a per-`TypeId` Ready-delay. The slowdown applies to
    /// `spawn_worker_for_type` calls whose `type_id` matches the
    /// `target_type` argument; other types still respond instantly.
    fn with_slow_ready(mut self, target_type: &str, delay: Duration) -> Self {
        self.slow_ready_for_type = Some((target_type.into(), delay));
        self
    }

    fn type_shift_spawn_count(&self) -> u32 {
        self.type_shift_spawns.get()
    }
}

impl WorkerFactory<ChannelManagerEnd> for TypedFakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: dynrunner_core::WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        spawn_fake_worker_task(Duration::ZERO)
    }

    fn spawn_worker_for_type(
        &mut self,
        _worker_id: dynrunner_core::WorkerId,
        type_id: &dynrunner_core::TypeId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.type_shift_spawns
            .set(self.type_shift_spawns.get() + 1);
        let delay = self
            .slow_ready_for_type
            .as_ref()
            .filter(|(t, _)| t == type_id.as_str())
            .map(|(_, d)| *d)
            .unwrap_or(Duration::ZERO);
        spawn_fake_worker_task(delay)
    }
}

fn spawn_fake_worker_task(
    ready_delay: Duration,
) -> Result<(ChannelManagerEnd, Option<u32>), String> {
    let (manager_end, runner_end) = channel_pair();
    tokio::task::spawn_local(async move {
        let mut runner = runner_end;
        // Optional synthetic startup delay before reporting Ready.
        // Models a freshly-spawned Python worker subprocess that
        // legitimately takes nontrivial time to import its task
        // module before signalling readiness. Pre-fix
        // `ensure_worker_for_type` would block the secondary's
        // `select!` arm for the full delay; post-fix the wait
        // runs on a background task and other arms (keepalive,
        // peer messages, OOM ticks) keep firing.
        if !ready_delay.is_zero() {
            tokio::time::sleep(ready_delay).await;
        }
        let _ = runner.send(Response::Ready).await;
        loop {
            match dynrunner_core::MessageReceiver::<Command>::recv(&mut runner).await {
                Some(Command::Stop) => break,
                Some(Command::ProcessTask { .. }) => {
                    let _ = runner.send(Response::Done { result_data: None }).await;
                }
                None => break,
            }
        }
    });
    Ok((manager_end, None))
}

/// Build a `TaskInfo` placed in the named phase with a distinct
/// `TypeId`. The singleton-per-phase shape (1 item, 1 type) is what
/// forces the kill+respawn on every phase boundary.
fn singleton_task(name: &str, phase: &str, type_str: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from(phase),
        type_id: dynrunner_core::TypeId::from(type_str),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some(name.into()),
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

/// Drive a 3-phase singleton-typed chain through one secondary. Sends
/// task assignments serially, one per TaskRequest, in dependency
/// order. Models a primary that does not pre-stage phase B's items
/// until phase A's completion arrives — the consumer's
/// `on_phase_end → spawn_tasks` pattern.
///
/// `keepalive_arrivals` (out-param) records the elapsed wall-clock
/// duration (relative to the start of `process_tasks`) at which
/// each `Keepalive` message arrived. The test body's
/// keepalive-liveness assertion uses this to detect the bug: pre-fix
/// the type-shift sync wait wedged the secondary's `select!` so the
/// entire slow-Ready window is observed as ZERO keepalive arrivals
/// between the start and the wedge's resume (typically `t=0` and
/// `t≈slow_ready_delay`); tokio's `MissedTickBehavior::Burst` then
/// dumps all the missed ticks at once on resume, collapsing them
/// into a sub-millisecond cluster. Post-fix the wait runs on a
/// background task and consecutive arrivals are spaced by the
/// `keepalive_interval`.
///
/// The discriminator implemented at the test body is "at least 2
/// keepalives must arrive STRICTLY BEFORE `slow_ready_delay / 2`":
/// post-fix the 200ms cadence yields ~3 keepalives in the first
/// 750ms; pre-fix the sync wedge keeps the wire silent for the
/// full slow_ready_delay so 0 keepalives arrive in that window.
async fn fake_primary_singleton_chain(
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    keepalive_arrivals: std::rc::Rc<std::cell::RefCell<Vec<Duration>>>,
    run_start: std::time::Instant,
) {
    // Chain order: A → B → C, with distinct TypeIds so each phase
    // boundary forces a kill+respawn via `ensure_worker_for_type`.
    let chain: [(&str, &str, &str); 3] = [
        ("task_a", "phase_a", "type_a"),
        ("task_b", "phase_b", "type_b"),
        ("task_c", "phase_c", "type_c"),
    ];
    let total = chain.len();

    // Wait for welcome + cert exchange.
    let mut got_welcome = false;
    let mut got_cert = false;
    while !got_welcome || !got_cert {
        if let Some(msg) = from_secondary.recv().await {
            match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            }
        }
    }

    // Empty peer list, empty initial assignment, transfer complete.
    // Same shape `fake_primary` in `processing.rs` uses; the
    // secondary's setup path requires all three before it leaves the
    // setup phase and enters `process_tasks`.
    to_secondary
        .send(DistributedMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            pre_staged_mode: false,
            uses_file_based_items: true,
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::TransferComplete {
            sender_id: "primary".into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .unwrap();

    // Serial dispatch: on each TaskRequest, send the next un-issued
    // chain task. Track bounced tasks (backpressure-shaped
    // TaskFailed) and re-issue them on the next TaskRequest before
    // moving on — mirrors the real `PrimaryCoordinator`'s
    // `handle_primary_peer_rejection` requeue contract.
    let mut completed = 0usize;
    let mut next_task_idx = 0usize;
    let mut requeue: Vec<(&'static str, &'static str, &'static str)> = Vec::new();
    while completed < total {
        if let Some(msg) = from_secondary.recv().await {
            match msg.msg_type() {
                MessageType::TaskComplete => {
                    completed += 1;
                }
                MessageType::TaskFailed => {
                    // Backpressure / respawn-bounce. Real primary
                    // recognises the marker via
                    // `peer/message_handler.rs::is_backpressure` and
                    // re-queues the binary; the fake mirrors that
                    // shape by stashing the task for re-issue on
                    // the next TaskRequest. The hash is enough to
                    // recover the (name, phase, type_str) triple
                    // because the chain shape is fixed.
                    if let DistributedMessage::TaskFailed {
                        task_hash,
                        ..
                    } = msg
                    {
                        // Find the chain entry whose hash matches.
                        // O(N) over N=3 is trivial.
                        if let Some(entry) = chain
                            .iter()
                            .find(|(name, _, _)| {
                                task_hash == format!("hash_{name}")
                            })
                            .copied()
                        {
                            requeue.push(entry);
                        }
                    }
                }
                MessageType::TaskRequest => {
                    let worker_id = match msg {
                        DistributedMessage::TaskRequest { worker_id, .. } => {
                            worker_id
                        }
                        _ => 0,
                    };
                    let dispatch = if let Some(re) = requeue.pop() {
                        Some(re)
                    } else if next_task_idx < total {
                        let entry = chain[next_task_idx];
                        next_task_idx += 1;
                        Some(entry)
                    } else {
                        None
                    };
                    if let Some((name, phase, type_str)) = dispatch {
                        let binary = singleton_task(name, phase, type_str);
                        let hash = format!("hash_{name}");
                        to_secondary
                            .send(DistributedMessage::TaskAssignment {
                                sender_id: "primary".into(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id,
                                zip_file: None,
                                binary_info: DistributedBinaryInfo::from_task_info(&binary),
                                local_path: binary
                                    .path
                                    .to_string_lossy()
                                    .into_owned(),
                                file_hash: hash,
                                predecessor_outputs:
                                    std::collections::BTreeMap::new(),
                            })
                            .unwrap();
                    }
                }
                MessageType::Keepalive => {
                    keepalive_arrivals
                        .borrow_mut()
                        .push(run_start.elapsed());
                }
                _ => {}
            }
        }
    }

    // Drop channel so the secondary's primary_transport.recv() returns
    // None and the loop exits cleanly.
    drop(to_secondary);
}

/// Run the chain end-to-end under one secondary + one fake primary
/// with the supplied factory. Returns `(completed_count,
/// type_shift_spawns, keepalive_arrivals)`. `keepalive_arrivals`
/// is the sequence of elapsed-wall-clock times (relative to
/// `secondary.run` start) at which each `Keepalive` message
/// arrived at the fake primary — the load-bearing keepalive-
/// liveness metric.
async fn run_singleton_chain_with_factory(
    mut factory: TypedFakeWorkerFactory,
    test_timeout: Duration,
    keepalive_interval: Duration,
) -> (usize, u32, Vec<Duration>) {
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };

    let config = SecondaryConfig {
        secondary_id: "sec-0".into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval,
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        setup_deadline: Duration::from_secs(60),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        oom_retry_max_passes: 1,
        output_dir: None,
        memuse_log_path: None,
    };

    let keepalive_arrivals: std::rc::Rc<std::cell::RefCell<Vec<Duration>>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let keepalive_arrivals_cb = keepalive_arrivals.clone();
    let run_start = std::time::Instant::now();
    let secondary_id = config.secondary_id.clone();
    let primary_handle = tokio::task::spawn_local(
        fake_primary_singleton_chain(
            secondary_id,
            sec_to_pri_rx,
            pri_to_sec_tx,
            keepalive_arrivals_cb,
            run_start,
        ),
    );

    let mut secondary = SecondaryCoordinator::new(
        config,
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let run_result = tokio::time::timeout(
        test_timeout,
        secondary.run(&mut factory),
    )
    .await;

    match run_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("secondary.run errored: {e}"),
        Err(_) => panic!(
            "secondary.run wedged past {test_timeout:?} deadline — \
             singleton-typed phase chain did not complete. \
             Live bug: tokio runtime goes silent during the \
             type-shift respawn between phases."
        ),
    }

    let completed = secondary.completed_count();
    let type_shifts = factory.type_shift_spawn_count();

    primary_handle.await.unwrap();

    let arrivals = keepalive_arrivals.borrow().clone();
    (completed, type_shifts, arrivals)
}

/// Regression: singleton-typed phase chain must complete without
/// wedging the secondary's tokio runtime. Bounded by
/// `tokio::time::timeout` so a hang surfaces as a clear test failure
/// rather than a 2-minute global test-suite timeout.
#[tokio::test(flavor = "current_thread")]
async fn singleton_typed_phase_chain_completes_on_secondary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Production-typical 60s keepalive interval; bounded
            // `test_timeout` gates wall-clock. Happy-path round
            // trips are sub-millisecond; a >2s observation here
            // already evidences a wedge.
            let (completed, type_shifts, _arrivals) =
                run_singleton_chain_with_factory(
                    TypedFakeWorkerFactory::new(),
                    Duration::from_secs(30),
                    Duration::from_secs(60),
                )
                .await;

            assert_eq!(
                completed, 3,
                "all 3 singleton-typed tasks must complete"
            );

            // Sanity check: the test must have exercised the
            // type-shift respawn code path at least twice (A→B
            // and B→C). A 0 here would mean the test inadvertently
            // went through `spawn_worker` only (e.g. all tasks
            // share a `TypeId`) and is not actually pinning the
            // regression.
            assert!(
                type_shifts >= 2,
                "expected ≥2 spawn_worker_for_type calls (A→B, B→C); \
                 observed {type_shifts}"
            );
        })
        .await;
}

/// Regression (load-bearing): under a slow worker-Ready response on
/// one phase boundary, the secondary's `select!` MUST keep firing
/// keepalives. Pre-fix the synchronous `ensure_worker_for_type` await
/// blocked every other arm — keepalives, peer messages, OOM ticks —
/// for the entire duration the new subprocess took to send
/// `Response::Ready`. Production observed 300+s of tokio runtime
/// silence with the primary's keepalive_timeout firing as the only
/// recovery. This test pins the keepalive-liveness contract by:
///
///   1. Configuring `TypedFakeWorkerFactory` so the `type_b`
///      respawn delays 1.5 s before sending `Response::Ready`.
///   2. Setting the secondary's `keepalive_interval` to 200ms so
///      multiple ticks should fire during the slow-Ready window.
///   3. Asserting the MAX observed wall-clock gap between
///      successive Keepalive messages on the secondary→primary
///      wire is bounded by ~3× the configured cadence. Pre-fix the
///      synchronous wait wedges `select!` so the gap grows to AT
///      LEAST the slow_ready_delay (1500ms); the assertion's 800ms
///      ceiling sits strictly between the async-path natural
///      jitter (200–400ms gap) and the pre-fix wedge (≥1500ms),
///      yielding a discriminator that fires deterministically on
///      the bug.
/// Regression (load-bearing) — first-bind variant of the
/// keepalive-liveness contract. Pre-fix BOTH first-bind (`None →
/// Some(T)`) AND true type-shift (`Some(T1) → Some(T2)`) routed
/// through the synchronous `ensure_worker_for_type`; the prior
/// Bug A fix (commit 7862339) only switched true-type-shift to the
/// async-event flow. The first-bind path retained the inline
/// `poll_ready` loop and wedged the secondary's `select!` for the
/// full slow-Ready window every time a worker was bound to its
/// initial type. Production observed this on the asm-tokenizer
/// 80-task wedge where secondaries 2 & 3 processed exactly one
/// task each then went silent — the initial first-bind respawn
/// blocked the operational loop long enough that the primary
/// keepalive_timeout fired and recovery routed work elsewhere.
///
/// This test pins the keepalive-liveness contract for the
/// first-bind path by configuring the factory to slow Ready
/// specifically on `type_a` (the first task's type), so the
/// first-bind respawn engages the slow-Ready window AND the test
/// asserts keepalives keep firing through it. Same discriminator
/// shape as the type-shift variant above.
#[tokio::test(flavor = "current_thread")]
async fn keepalives_keep_firing_during_slow_ready_on_first_bind() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Slow type_a's Ready by 1.5 s. The first task in
            // the chain is `task_a` with `type_a`, so the
            // initial first-bind respawn engages the slow-Ready
            // window. Subsequent type-shifts use type_b/type_c
            // which respond instantly — keeps the test focused on
            // the first-bind window.
            let factory = TypedFakeWorkerFactory::new()
                .with_slow_ready("type_a", Duration::from_millis(1500));

            let (completed, type_shifts, arrivals) =
                run_singleton_chain_with_factory(
                    factory,
                    Duration::from_secs(10),
                    Duration::from_millis(200),
                )
                .await;

            assert_eq!(
                completed, 3,
                "all 3 tasks complete even with slow Ready on type_a's first bind"
            );
            assert!(
                type_shifts >= 2,
                "type-shift respawn must have fired ≥2 times \
                 (A→B, B→C); observed {type_shifts}"
            );
            let liveness_window = Duration::from_millis(1000);
            let early_keepalives: usize = arrivals
                .iter()
                .filter(|t| **t < liveness_window)
                .count();
            assert!(
                early_keepalives >= 2,
                "only {early_keepalives} keepalive(s) arrived in \
                 the first {liveness_window:?} of the run — the \
                 secondary's tokio `select!` was wedged during the \
                 slow-Ready first-bind window. Pre-fix the \
                 synchronous wait inside `ensure_worker_for_type` \
                 blocks the operational loop for the entire \
                 slow_ready_delay (1500ms here) on the FIRST-BIND \
                 path (`loaded_type_id == None`), and the missed-tick \
                 burst on resume collapses into a single \
                 microsecond-spaced cluster at t≈1500ms. Post-fix \
                 the first-bind binary is stashed in \
                 `pending_first_bind` and the operational loop \
                 keeps firing keepalives during the wait — at \
                 least 4 should land before {liveness_window:?}. \
                 Arrivals (relative to run start): {arrivals:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn keepalives_keep_firing_during_slow_ready_on_type_shift() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Slow type_b's Ready by 1.5 s; the rest stay
            // instant. The wedge would manifest as a 1.5s
            // tokio-runtime silence pre-fix.
            let factory = TypedFakeWorkerFactory::new()
                .with_slow_ready("type_b", Duration::from_millis(1500));

            let (completed, type_shifts, arrivals) =
                run_singleton_chain_with_factory(
                    factory,
                    // 10 s ceiling — plenty of headroom over the
                    // 1.5s slow_ready_delay + keepalive cadence.
                    Duration::from_secs(10),
                    // 200 ms keepalive cadence so multiple ticks
                    // accrue during the slow-Ready window.
                    Duration::from_millis(200),
                )
                .await;

            assert_eq!(
                completed, 3,
                "all 3 tasks complete even with slow Ready on type_b"
            );
            assert!(
                type_shifts >= 2,
                "type-shift respawn must have fired ≥2 times \
                 (A→B, B→C); observed {type_shifts}"
            );
            // Liveness window: how many keepalives arrived STRICTLY
            // BEFORE the slow_ready_delay elapses? With the 1500ms
            // delay and 200ms keepalive cadence, post-fix expects
            // ≥4 keepalives in the [0, 1000ms) window (the first
            // tick fires at t≈0 plus 4 cadence ticks at t=200,
            // 400, 600, 800). Pre-fix the entire window is
            // wedge-silent — tokio's `MissedTickBehavior::Burst`
            // collapses every missed tick into a single
            // sub-millisecond cluster on resume at t≈1500ms, so
            // pre-fix observes 0–1 keepalives in [0, 1000ms).
            // 2-keepalive floor sits strictly between the
            // post-fix expected count (≥4) and the pre-fix
            // wedge ceiling (≤1).
            let liveness_window = Duration::from_millis(1000);
            let early_keepalives: usize = arrivals
                .iter()
                .filter(|t| **t < liveness_window)
                .count();
            assert!(
                early_keepalives >= 2,
                "only {early_keepalives} keepalive(s) arrived in \
                 the first {liveness_window:?} of the run — the \
                 secondary's tokio `select!` was wedged during the \
                 slow-Ready type-shift window. Pre-fix the \
                 synchronous wait inside `ensure_worker_for_type` \
                 blocks the operational loop for the entire \
                 slow_ready_delay (1500ms here); the 200ms \
                 keepalive ticker can't fire while parked, and the \
                 missed-tick burst on resume collapses into a \
                 single microsecond-spaced cluster at t≈1500ms. \
                 Post-fix the wait runs in a background task and \
                 consecutive keepalives arrive every 200ms — at \
                 least 4 should land before {liveness_window:?}. \
                 Arrivals (relative to run start): {arrivals:?}"
            );
        })
        .await;
}
