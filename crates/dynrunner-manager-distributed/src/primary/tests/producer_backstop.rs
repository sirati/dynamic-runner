//! #310 — the producer-mode mesh-always e2e regression backstop.
//!
//! ONE scenario family exercising, COMBINED, the four legs that consumer
//! incidents kept hitting pairwise:
//!
//!   1. **Relocated primary** — the setup peer ColdStart-seeds phase-1 + the
//!      phase graph, then relocates the primary onto `sec-0` (the
//!      `PromotionSnapshot` path), so the operational primary is CO-LOCATED
//!      with its own secondary plus ≥2 remote secondaries.
//!   2. **Phase chaining** — phase-2 is declared (in `phase_deps`) but EMPTY
//!      at seed time; its real work is injected from inside
//!      `on_phase_end("phase1")` via `PrimaryCommand::SpawnTasks` (the
//!      asm-tokenizer `FullPipelineTask.on_phase_end →
//!      primary_handle.spawn_tasks` contract). The chaining hooks are
//!      installed IDENTICALLY on every promotion-capable peer's recipe —
//!      exactly like the production consumer callback — so the no-redo
//!      assertions are meaningful.
//!   3. **Worker churn** — phase-1 carries MORE distinct `type_id`s than the
//!      fleet has workers (1 worker per secondary), so type-shift respawns
//!      are forced mid-phase by pigeonhole, every first task is a
//!      post-Ready first-bind, and one task (`p1_poison`) drives the
//!      disconnect-with-error shape: the worker answers
//!      `Response::Error { NonRecoverable, .. }`, which the pool surfaces as
//!      `WorkerEvent::Disconnected { result, binary: Some(..) }` (the
//!      #341/#344/#348 class) — it must resolve as a reported terminal.
//!   4. **Failover** (headline test only) — the PROMOTED primary's whole
//!      node is KILLED mid-phase-2 (its thread-local runtime is dropped, the
//!      process-death analogue: pump + primary + co-located secondary +
//!      workers all vanish mid-poll). The survivors detect the death,
//!      run the adaptive-quorum election, the lex-lowest survivor (`sec-1`)
//!      promotes from its converged CRDT mirror, and the run completes with
//!      NO REDO: `on_phase_end("phase1")` never re-fires, ledger-terminal
//!      work is never re-dispatched, and the final accounting is exact.
//!
//! # Fidelity notes (honest gaps vs production)
//!
//! * The mesh is the in-process CHANNEL mesh (deterministic, fast), not
//!   QUIC. Two consequences:
//!   - membership-departure (election leg C) is lazy on this transport — a
//!     dead peer leaves a survivor's `connected_ids` only after a DIRECTED
//!     send to it fails (the Router purges the closed channel); broadcasts
//!     tolerate closed channels silently. The failover here therefore arms
//!     primarily via leg (B), the `primary_silence_backstop`
//!     receive-staleness backstop (tuned tight), which is a genuine
//!     production leg — the QUIC-teardown fast path (leg C) is covered by
//!     the dedicated election unit suites (`secondary/tests/failover_*`).
//!   - the first directed frame to the dead primary is dropped by the
//!     transport purge rather than erroring back synchronously; the
//!     coordinator-edge no-route absorb + buffered-terminal-replay then
//!     covers subsequent sends, exactly as on QUIC after teardown.
//! * The kill drops the WHOLE sec-0 node (its dedicated thread's runtime),
//!   which is the faithful SLURM node-death shape. A completion that was
//!   in flight on the dead node's worker — or whose report raced the death
//!   window — may legitimately RE-RUN (crash recovery of unrecorded work);
//!   the no-redo assertions therefore distinguish ledger-terminal work
//!   (must run EXACTLY once) from kill-window in-flight work (1..=2 runs).

use super::*;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use dynrunner_core::PhaseId;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_transport_channel::peer_mesh;

use crate::observer::ObserverCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::process::{LocalRole, Mesh, Node, NodeRunInputs, PrimaryRunArgs, RunTerminal};

/// The promoted-primary recipe type every promotion-capable compute peer
/// carries in these scenarios.
type PromoteRecipe =
    crate::process::PromotedPrimaryBuilder<ResourceStealingScheduler, FixedEstimator, TestId>;

/// The fully-typed node composition (one per peer): primary + secondary +
/// observer role slots over the channel transport.
type BackstopNode = Node<
    TestId,
    ChannelPeerTransport<TestId>,
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    SecondaryCoordinator<
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    ObserverCoordinator<TestId>,
>;

type BackstopInputs =
    NodeRunInputs<ScriptedWorkerFactory, ResourceStealingScheduler, FixedEstimator, TestId>;

/// One phase-lifecycle callback firing, tagged with WHICH promoted primary
/// fired it — the cross-primary event log is what the no-redo assertions
/// read (a re-fired `on_phase_end("phase1")` on the failover winner would
/// appear as a second `End` entry tagged `sec-1`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum PhaseEvent {
    Start {
        primary: String,
        phase: String,
    },
    End {
        primary: String,
        phase: String,
        completed: u32,
        failed: u32,
    },
}

/// Cross-thread event log (the failover scenario's first promoted primary
/// lives on its own thread).
type EventLog = Arc<Mutex<Vec<PhaseEvent>>>;

/// Election-tuned secondary config: 50 ms keepalives (the election tick
/// cadence), the standard 3-miss death deadline (150 ms — the per-peer
/// agreement threshold the Suspecting tally uses), and a TIGHT
/// `primary_silence_backstop` (1.2 s) so leg (B) arms the failover promptly
/// after the promoted primary's node is killed, while staying an order of
/// magnitude above the primary's 100 ms keepalive emission (no false arm on
/// a live primary).
fn backstop_sec_config(id: &str, can_be_primary: bool) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024u64,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_millis(1200),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// The promoted primary's config: 100 ms keepalive emission (feeds the
/// survivors' leg-(B) freshness on a LIVE primary) with a 1 s
/// secondary-death deadline (10 misses) so the failover-promoted primary
/// reaps the dead peer's inherited in-flight work promptly without
/// false-reaping a busy-but-live survivor.
fn promoted_primary_config(node_id: &str) -> PrimaryConfig {
    PrimaryConfig {
        node_id: node_id.into(),
        num_secondaries: 3,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        keepalive_interval: Duration::from_millis(100),
        keepalive_miss_threshold: 10,
        mesh_ready_timeout: Duration::from_secs(5),
        ..PrimaryConfig::default()
    }
}

/// The consumer-pattern phase hooks for a promoted primary's recipe: log
/// every `on_phase_start` / `on_phase_end` firing into the shared
/// cross-primary `events` log (tagged with this primary's id), and on the
/// FIRST `on_phase_end("phase1")` inject the phase-2 batch through the
/// promoted primary's own command channel — the exact
/// `FullPipelineTask.on_phase_end → primary_handle.spawn_tasks` consumer
/// contract. `phase2_started_tx` (when supplied) signals the test driver the
/// moment `on_phase_start("phase2")` fires — the failover scenario's
/// kill-timing trigger.
///
/// Installed IDENTICALLY on EVERY promotion-capable peer (the production
/// consumer callback exists on whichever node promotes): the no-redo
/// contract is that the framework never RE-FIRES `on_phase_end("phase1")`
/// on a failover-promoted primary, which the event log pins.
fn chaining_hooks(
    primary_id: &str,
    events: EventLog,
    phase2_items: Vec<TaskInfo<TestId>>,
    phase2_started_tx: Option<tokio_mpsc::UnboundedSender<()>>,
) -> PromoteHooksFactory {
    let primary_id = primary_id.to_string();
    Box::new(move |command_sender| {
        let start_log = events.clone();
        let start_primary = primary_id.clone();
        let on_start: OnPhaseStart = Box::new(move |p: &PhaseId| {
            start_log.lock().unwrap().push(PhaseEvent::Start {
                primary: start_primary.clone(),
                phase: p.to_string(),
            });
            if p.as_str() == "phase2"
                && let Some(tx) = &phase2_started_tx
            {
                let _ = tx.send(());
            }
        });
        let mut already_spawned = false;
        let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32, _outputs| {
            events.lock().unwrap().push(PhaseEvent::End {
                primary: primary_id.clone(),
                phase: p.to_string(),
                completed: c,
                failed: f,
            });
            if p.as_str() == "phase1" && !already_spawned {
                already_spawned = true;
                let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                // `try_send` — the callback runs synchronously inside the
                // cascade; the cascade's post-callback drain picks the
                // command up inline (the established consumer pattern, see
                // `phase_ordering.rs`).
                let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                    tasks: phase2_items.clone(),
                    reply: reply_tx,
                });
            }
        });
        (on_start, on_end)
    })
}

/// Compose one compute peer's node: a real `SecondaryCoordinator` (with the
/// scripted worker factory) plus, when `promote` is supplied, the
/// promotion-capable wiring (`can_be_primary` welcome + promotion signal +
/// the recipe). Pure composition — the caller decides WHERE to run it (the
/// main `LocalSet` or the failover scenario's kill thread).
fn compose_compute_node(
    id: &str,
    transport: ChannelPeerTransport<TestId>,
    factory: ScriptedWorkerFactory,
    promote: Option<PromoteRecipe>,
) -> (BackstopNode, BackstopInputs) {
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) = mesh.register_local_role(LocalRole::Secondary, PeerId::from(id));
    mesh.publish_membership();
    let mut secondary = SecondaryCoordinator::new(
        backstop_sec_config(id, promote.is_some()),
        client,
        inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.set_bootstrap_primary_id("setup".to_string());
    let (node, promo_tx) = Node::new(mesh);
    secondary.register_promotion_signal(promo_tx);
    let node = node.with_secondary(secondary, slot);
    let inputs = BackstopInputs {
        secondary_factory: Some(factory),
        promote,
        ..Default::default()
    };
    (node, inputs)
}

/// Compose the SETUP peer's node: a `ColdStart` primary that seeds
/// `binaries` + `phase_deps` into the CRDT, relocates onto the lowest-id
/// eligible compute peer, and swaps into the standalone observer (whose
/// converged terminal/counts the test reads as the operator-side ledger).
fn compose_setup_node(
    transport: ChannelPeerTransport<TestId>,
    binaries: Vec<TaskInfo<TestId>>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
) -> (BackstopNode, BackstopInputs) {
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) = mesh.register_local_role(LocalRole::Primary, PeerId::from("setup"));
    mesh.publish_membership();
    let config = PrimaryConfig {
        num_secondaries: 3,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        ..test_primary_config()
    };
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let primary = PrimaryCoordinator::new(
        config,
        client,
        inbox,
        demote_rx,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let (node, _promo_tx) = Node::new(mesh);
    let node = node.with_primary(primary, slot);
    let inputs = BackstopInputs {
        primary_run_args: Some(PrimaryRunArgs {
            seed: SeedSource::ColdStart {
                binaries: binaries.into_iter().map(|b| (b, false)).collect(),
                phase_deps,
            },
            on_phase_start: Box::new(|_| {}),
            on_phase_end: Box::new(|_, _, _, _| {}),
        }),
        primary_demote_tx: Some(demote_tx),
        ..Default::default()
    };
    (node, inputs)
}

/// The phase graph: `phase2` depends on `phase1`; `phase2` is declared but
/// owns NO seeded work (its items are injected from `on_phase_end("phase1")`).
fn backstop_phase_deps() -> HashMap<PhaseId, Vec<PhaseId>> {
    let mut deps = HashMap::new();
    deps.insert(PhaseId::from("phase2"), vec![PhaseId::from("phase1")]);
    deps
}

/// The phase-1 churn corpus: 6 tasks across FIVE distinct `type_id`s on a
/// 3-worker fleet (1 worker per secondary), so at least two workers MUST
/// type-shift mid-phase (pigeonhole), and `p1_poison` (its own type — its
/// dispatch is a first-bind, so the failure lands on a POST-READY-ASSIGNED
/// task) is scripted to fail the worker with the NonRecoverable
/// disconnect-with-error shape.
fn phase1_corpus() -> Vec<TaskInfo<TestId>> {
    vec![
        make_phased_typed_binary("p1_alpha_0", "phase1", "alpha", 50),
        make_phased_typed_binary("p1_beta_0", "phase1", "beta", 60),
        make_phased_typed_binary("p1_gamma_0", "phase1", "gamma", 70),
        make_phased_typed_binary("p1_delta_0", "phase1", "delta", 80),
        make_phased_typed_binary("p1_alpha_1", "phase1", "alpha", 90),
        make_phased_typed_binary("p1_poison", "phase1", "poison", 50),
    ]
}

/// The worker script shared by both scenarios: `p1_poison` answers the
/// production nix-build-failure error (NonRecoverable, with the binary in
/// hand → `WorkerEvent::Disconnected { binary: Some(..) }` at the pool).
fn poison_script() -> (String, WorkerScript) {
    (
        "p1_poison".to_string(),
        WorkerScript::Error {
            delay: Duration::ZERO,
            error_type: dynrunner_core::ErrorType::NonRecoverable,
            message: "nix build returned non-zero".to_string(),
        },
    )
}

/// Per-task run count from the fleet-wide scripted-factory ledger, keyed by
/// the fixture name (the wire `relative_path` is `/tmp/<name>`).
fn runs_of(factory: &ScriptedWorkerFactory, name: &str) -> u32 {
    factory
        .run_counts
        .lock()
        .unwrap()
        .get(&format!("/tmp/{name}"))
        .copied()
        .unwrap_or(0)
}

/// Index of the single event matching `pred`, asserting EXACTLY one match.
fn index_of_single(log: &[PhaseEvent], what: &str, pred: impl Fn(&PhaseEvent) -> bool) -> usize {
    let hits: Vec<usize> = log
        .iter()
        .enumerate()
        .filter(|(_, e)| pred(e))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "expected exactly one {what}; got {} — event log: {log:?}",
        hits.len()
    );
    hits[0]
}

// ─────────────────────────────────────────────────────────────────────────
// Stage test: relocate × churn × chaining (no failover). The 3 non-failover
// legs combined end-to-end on real `Node::run`s over the channel mesh.
// ─────────────────────────────────────────────────────────────────────────

/// Relocated primary (setup → sec-0, co-located with its own secondary +
/// 2 remote secondaries) runs phase-1 under type-shift + disconnect-error
/// churn, chains phase-2 via the `on_phase_end` injection, and completes
/// with an exact ledger: 9 completed + 1 NonRecoverable failure, every task
/// run exactly once, `on_phase_end` per phase exactly once and in order.
#[tokio::test(flavor = "current_thread")]
async fn producer_backstop_relocate_churn_chaining_completes() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let ids: Vec<String> = ["setup", "sec-0", "sec-1", "sec-2"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            let mut transports = peer_mesh::<TestId>(&ids);
            let sec2_t = transports.pop().unwrap();
            let sec1_t = transports.pop().unwrap();
            let sec0_t = transports.pop().unwrap();
            let setup_t = transports.pop().unwrap();

            let factory = ScriptedWorkerFactory::new(vec![poison_script()]);
            let events: EventLog = Arc::new(Mutex::new(Vec::new()));

            let phase2_items: Vec<TaskInfo<TestId>> = (0..4)
                .map(|i| make_phased_typed_binary(&format!("p2_task_{i}"), "phase2", "omega", 40))
                .collect();

            // sec-0: the relocate target, carrying the chaining recipe.
            let recipe0 = build_test_promote_recipe_with_config_and_hooks(
                promoted_primary_config("sec-0"),
                None,
                chaining_hooks("sec-0", events.clone(), phase2_items.clone(), None),
            );
            let (sec0_node, sec0_inputs) =
                compose_compute_node("sec-0", sec0_t, factory.clone(), Some(recipe0));
            let sec0_h = tokio::task::spawn_local(sec0_node.run(sec0_inputs));

            // sec-1 / sec-2: plain remote secondaries.
            let (sec1_node, sec1_inputs) =
                compose_compute_node("sec-1", sec1_t, factory.clone(), None);
            let sec1_h = tokio::task::spawn_local(sec1_node.run(sec1_inputs));
            let (sec2_node, sec2_inputs) =
                compose_compute_node("sec-2", sec2_t, factory.clone(), None);
            let sec2_h = tokio::task::spawn_local(sec2_node.run(sec2_inputs));

            // setup: cold-seeds phase-1 + the graph, relocates, observes.
            let (setup_node, setup_inputs) =
                compose_setup_node(setup_t, phase1_corpus(), backstop_phase_deps());
            let setup_h = tokio::task::spawn_local(setup_node.run(setup_inputs));

            // ── The whole run must resolve; a wedge FAILS via timeout. ──
            let sec0_out = tokio::time::timeout(Duration::from_secs(60), sec0_h)
                .await
                .expect(
                    "WEDGE: the relocated primary's node did not resolve within 60s \
                     (relocate × churn × chaining run hung)",
                )
                .expect("sec-0 node task join");
            assert!(
                matches!(sec0_out.terminal, RunTerminal::Done),
                "the relocated primary must finish Done; got {:?}",
                sec0_out.terminal
            );
            // Exact ledger: 5 phase-1 completions + 1 NonRecoverable failure
            // (the disconnect-with-error task) + 4 injected phase-2
            // completions. assigned == terminal: completed + failed == 10.
            assert_eq!(
                sec0_out.completed, 9,
                "9 completions (5 phase-1 + 4 phase-2)"
            );
            assert_eq!(
                sec0_out.failed, 1,
                "the poison disconnect-with-error must resolve as exactly one \
                 permanent failure (NOT an orphan that wedges the phase barrier)"
            );
            assert_eq!(sec0_out.stranded, 0, "no stranded work");

            let setup_out = tokio::time::timeout(Duration::from_secs(30), setup_h)
                .await
                .expect("the setup-peer observer must resolve within 30s of run end")
                .expect("setup node task join");
            assert!(
                matches!(setup_out.terminal, RunTerminal::Done),
                "setup observer terminal: {:?}",
                setup_out.terminal
            );
            assert_eq!(
                setup_out.completed, 9,
                "the observer's converged mirror must agree with the primary's ledger"
            );

            for (h, who) in [(sec1_h, "sec-1"), (sec2_h, "sec-2")] {
                let out = tokio::time::timeout(Duration::from_secs(30), h)
                    .await
                    .unwrap_or_else(|_| panic!("{who} must resolve within 30s of run end"))
                    .expect("secondary node task join");
                assert!(
                    matches!(out.terminal, RunTerminal::Done),
                    "{who} terminal: {:?}",
                    out.terminal
                );
            }

            // ── Phase-lifecycle contract (the chaining leg). ──
            let log = events.lock().unwrap().clone();
            let p1_end = index_of_single(
                &log,
                "on_phase_end(phase1)",
                |e| matches!(e, PhaseEvent::End { phase, .. } if phase == "phase1"),
            );
            assert_eq!(
                log[p1_end],
                PhaseEvent::End {
                    primary: "sec-0".into(),
                    phase: "phase1".into(),
                    completed: 5,
                    failed: 1,
                },
                "on_phase_end(phase1) must report the full terminal tally \
                 (5 completed + the 1 NonRecoverable churn failure); log: {log:?}"
            );
            let p2_start = index_of_single(
                &log,
                "on_phase_start(phase2)",
                |e| matches!(e, PhaseEvent::Start { phase, .. } if phase == "phase2"),
            );
            let p2_end = index_of_single(
                &log,
                "on_phase_end(phase2)",
                |e| matches!(e, PhaseEvent::End { phase, .. } if phase == "phase2"),
            );
            assert_eq!(
                log[p2_end],
                PhaseEvent::End {
                    primary: "sec-0".into(),
                    phase: "phase2".into(),
                    completed: 4,
                    failed: 0,
                },
                "on_phase_end(phase2) must report all 4 injected items; log: {log:?}"
            );
            assert!(
                p1_end < p2_start && p2_start < p2_end,
                "ordering must be End(phase1) < Start(phase2) < End(phase2); log: {log:?}"
            );

            // ── Churn accounting. Every task ran EXACTLY once (no dup
            // dispatch through the respawn churn), and the fleet actually
            // CHURNED: ≥ 3 initial spawns + ≥ 6 type first-binds (six
            // distinct type_ids each bind at least once somewhere). ──
            for b in phase1_corpus().iter().chain(phase2_items.iter()) {
                let name = b.task_id.as_str();
                assert_eq!(
                    runs_of(&factory, name),
                    1,
                    "task {name} must run exactly once; run_counts: {:?}",
                    factory.run_counts.lock().unwrap()
                );
            }
            let spawns = factory.spawn_count.load(Ordering::SeqCst);
            assert!(
                spawns >= 9,
                "worker churn must actually happen: ≥ 3 initial spawns + ≥ 6 \
                 type-(re)binds (alpha/beta/gamma/delta/poison/omega each bind \
                 at least once); got {spawns} spawn_worker calls"
            );
        })
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Headline test: the full 4-way combination — relocate × churn × chaining
// × FAILOVER (kill the promoted primary's node mid-phase-2; a survivor
// wins the adaptive-quorum election; the run completes with no redo).
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn producer_backstop_failover_mid_phase2_no_redo() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let ids: Vec<String> = ["setup", "sec-0", "sec-1", "sec-2"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            let mut transports = peer_mesh::<TestId>(&ids);
            let sec2_t = transports.pop().unwrap();
            let sec1_t = transports.pop().unwrap();
            let sec0_t = transports.pop().unwrap();
            let setup_t = transports.pop().unwrap();

            // p2_slow tasks outlast the kill window (in flight at the kill);
            // p2_fast tasks complete + replicate well before it.
            let factory = ScriptedWorkerFactory::new(vec![
                poison_script(),
                (
                    "p2_slow".to_string(),
                    WorkerScript::Done {
                        delay: Duration::from_millis(2500),
                    },
                ),
            ]);
            let events: EventLog = Arc::new(Mutex::new(Vec::new()));
            let (phase2_started_tx, mut phase2_started_rx) = tokio_mpsc::unbounded_channel::<()>();

            let phase2_items: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_phased_typed_binary(&format!("p2_fast_{i}"), "phase2", "omega", 40))
                .chain((0..3).map(|i| {
                    make_phased_typed_binary(&format!("p2_slow_{i}"), "phase2", "omega", 40)
                }))
                .collect();

            // ── Survivors (main LocalSet): BOTH promotion-capable, BOTH
            // carrying the SAME consumer chaining hooks (the production
            // consumer callback exists on whichever node promotes — the
            // no-redo contract is that the framework never re-fires it). ──
            let recipe1 = build_test_promote_recipe_with_config_and_hooks(
                promoted_primary_config("sec-1"),
                None,
                chaining_hooks("sec-1", events.clone(), phase2_items.clone(), None),
            );
            let (sec1_node, sec1_inputs) =
                compose_compute_node("sec-1", sec1_t, factory.clone(), Some(recipe1));
            let sec1_h = tokio::task::spawn_local(sec1_node.run(sec1_inputs));

            let recipe2 = build_test_promote_recipe_with_config_and_hooks(
                promoted_primary_config("sec-2"),
                None,
                chaining_hooks("sec-2", events.clone(), phase2_items.clone(), None),
            );
            let (sec2_node, sec2_inputs) =
                compose_compute_node("sec-2", sec2_t, factory.clone(), Some(recipe2));
            let sec2_h = tokio::task::spawn_local(sec2_node.run(sec2_inputs));

            // ── sec-0 (the kill target): its WHOLE node runs on a dedicated
            // thread with its own current_thread runtime + LocalSet. The
            // kill drops that runtime — pump, promoted primary, co-located
            // secondary, and workers all vanish mid-poll, the process-death
            // analogue (`Node::run` spawns its roles as separate tasks, so
            // aborting just the `run` future would NOT kill them). ──
            let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
            // `true` = sec-0's node finished BEFORE the kill (a premature
            // RunComplete — must not happen).
            let (sec0_done_tx, sec0_done_rx) = tokio::sync::oneshot::channel::<bool>();
            let thread_factory = factory.clone();
            let thread_events = events.clone();
            let thread_phase2_items = phase2_items.clone();
            let sec0_thread = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sec-0 thread runtime");
                let finished_before_kill = {
                    let thread_local = tokio::task::LocalSet::new();
                    rt.block_on(thread_local.run_until(async move {
                        let recipe0 = build_test_promote_recipe_with_config_and_hooks(
                            promoted_primary_config("sec-0"),
                            None,
                            chaining_hooks(
                                "sec-0",
                                thread_events,
                                thread_phase2_items,
                                Some(phase2_started_tx),
                            ),
                        );
                        let (sec0_node, sec0_inputs) =
                            compose_compute_node("sec-0", sec0_t, thread_factory, Some(recipe0));
                        let run = sec0_node.run(sec0_inputs);
                        tokio::pin!(run);
                        tokio::select! {
                            _out = &mut run => true,
                            _ = kill_rx => false,
                        }
                    }))
                    // `thread_local` (and with it every spawned sec-0 task)
                    // drops HERE, before the runtime.
                };
                drop(rt);
                let _ = sec0_done_tx.send(finished_before_kill);
            });

            // ── setup: cold-seeds + relocates onto sec-0 (lowest-id
            // eligible), then observes. ──
            let (setup_node, setup_inputs) =
                compose_setup_node(setup_t, phase1_corpus(), backstop_phase_deps());
            let setup_h = tokio::task::spawn_local(setup_node.run(setup_inputs));

            // ── Wait for phase-2 to start on the FIRST promoted primary,
            // let the fast tasks complete + replicate (the slow ones, 2.5 s,
            // stay in flight), then KILL. ──
            tokio::time::timeout(Duration::from_secs(60), phase2_started_rx.recv())
                .await
                .expect(
                    "WEDGE: phase-2 never started on the relocated primary within \
                     60s (relocate / churn / chaining leg hung before the kill)",
                )
                .expect("phase2-started channel closed without firing");
            tokio::time::sleep(Duration::from_millis(800)).await;
            kill_tx.send(()).expect("sec-0 kill signal");
            let finished_before_kill = tokio::time::timeout(Duration::from_secs(10), sec0_done_rx)
                .await
                .expect("sec-0's thread must acknowledge the kill within 10s")
                .expect("sec-0 done channel");
            assert!(
                !finished_before_kill,
                "sec-0's node finished BEFORE the kill — the run prematurely \
                 completed mid-phase-2 (slow tasks were still owed)"
            );

            // ── The survivors must elect (sec-1 is the lex-lowest live
            // candidate), promote, finish phase-2, and complete the run. ──
            let sec1_out = tokio::time::timeout(Duration::from_secs(90), sec1_h)
                .await
                .expect(
                    "WEDGE: the failover did not complete within 90s — the \
                     survivors never elected/promoted, or the promoted primary \
                     never finished the inherited run",
                )
                .expect("sec-1 node task join");
            assert!(
                matches!(sec1_out.terminal, RunTerminal::Done),
                "the failover-promoted primary must finish Done; got {:?}",
                sec1_out.terminal
            );
            // Exact accounting: 12 tasks total (6 phase-1 + 6 phase-2);
            // completed + failed == total, with exactly the one poison
            // failure. assigned == terminal: nothing stranded.
            assert_eq!(
                sec1_out.completed, 11,
                "11 completions (5 phase-1 + 6 phase-2) on the failover-promoted \
                 primary's converged ledger"
            );
            assert_eq!(sec1_out.failed, 1, "exactly the poison failure");
            assert_eq!(sec1_out.stranded, 0, "no stranded work after the failover");

            let sec2_out = tokio::time::timeout(Duration::from_secs(30), sec2_h)
                .await
                .expect("sec-2 must drain + exit within 30s of run completion")
                .expect("sec-2 node task join");
            assert!(
                matches!(sec2_out.terminal, RunTerminal::Done),
                "sec-2 terminal: {:?}",
                sec2_out.terminal
            );

            let setup_out = tokio::time::timeout(Duration::from_secs(30), setup_h)
                .await
                .expect("the setup observer must resolve within 30s of run completion")
                .expect("setup node task join");
            assert!(
                matches!(setup_out.terminal, RunTerminal::Done),
                "setup observer terminal: {:?}",
                setup_out.terminal
            );
            assert_eq!(
                setup_out.completed, 11,
                "the observer's converged mirror must agree with the final ledger"
            );

            tokio::task::spawn_blocking(move || sec0_thread.join())
                .await
                .expect("join-blocking task")
                .expect("sec-0 thread must not panic");

            // ── NO-REDO: the phase-lifecycle event log across BOTH promoted
            // primaries. ──
            let log = events.lock().unwrap().clone();
            // on_phase_end(phase1) fired EXACTLY once, on the FIRST promoted
            // primary — the failover winner must NOT re-fire it (its hydrate
            // seeds the fully-drained phase straight to Done), so the
            // consumer's injection hook cannot double-inject.
            let p1_end = index_of_single(
                &log,
                "on_phase_end(phase1)",
                |e| matches!(e, PhaseEvent::End { phase, .. } if phase == "phase1"),
            );
            assert_eq!(
                log[p1_end],
                PhaseEvent::End {
                    primary: "sec-0".into(),
                    phase: "phase1".into(),
                    completed: 5,
                    failed: 1,
                },
                "on_phase_end(phase1) must fire exactly once, on sec-0, with the \
                 full tally; log: {log:?}"
            );
            // on_phase_start fired exactly once per phase, both on sec-0 —
            // the failover winner re-fires NEITHER (started_phases is
            // derived from the inherited CRDT).
            let p1_start = index_of_single(
                &log,
                "on_phase_start(phase1)",
                |e| matches!(e, PhaseEvent::Start { phase, .. } if phase == "phase1"),
            );
            let p2_start = index_of_single(
                &log,
                "on_phase_start(phase2)",
                |e| matches!(e, PhaseEvent::Start { phase, .. } if phase == "phase2"),
            );
            for (i, what) in [(p1_start, "Start(phase1)"), (p2_start, "Start(phase2)")] {
                assert!(
                    matches!(&log[i], PhaseEvent::Start { primary, .. } if primary == "sec-0"),
                    "{what} must have fired on the FIRST promoted primary only \
                     (no re-fire on the failover winner); log: {log:?}"
                );
            }
            // on_phase_end(phase2) fired EXACTLY once, on the FAILOVER
            // winner, with the EXACT event tally (#358): the per-phase F4
            // tally is bumped by the `merge_task_state` join on every
            // winning `TaskCompleted` apply (`cluster_state/merge.rs`), so
            // the survivor's mirror advanced its tally in lockstep with the
            // per-completion broadcasts — the failover winner inherits an
            // exact count and reports the true 6 (3 pre-kill fast + 3
            // post-promotion slow completions), not the pre-#358
            // `inherited-lagged-tally + post-promotion observations` (0 +
            // 3 = 3) that the snapshot/anti-entropy-only replication of the
            // old `note_item_completed`-side bump produced.
            let p2_end = index_of_single(
                &log,
                "on_phase_end(phase2)",
                |e| matches!(e, PhaseEvent::End { phase, .. } if phase == "phase2"),
            );
            match &log[p2_end] {
                PhaseEvent::End {
                    primary,
                    completed,
                    failed,
                    ..
                } => {
                    assert_eq!(
                        primary, "sec-1",
                        "on_phase_end(phase2) must fire on the failover winner; \
                         log: {log:?}"
                    );
                    assert_eq!(
                        *completed, 6,
                        "on_phase_end(phase2).completed must be the EXACT event \
                         total — 3 pre-kill + 3 post-promotion completions — on \
                         the failover winner (#358 apply-side tally bump); \
                         log: {log:?}"
                    );
                    assert_eq!(*failed, 0, "no phase-2 failures; log: {log:?}");
                }
                other => panic!("index_of_single returned a non-End event: {other:?}"),
            }
            assert!(
                p1_end < p2_start && p2_start < p2_end,
                "ordering must be End(phase1) < Start(phase2) < End(phase2); \
                 log: {log:?}"
            );

            // ── NO-REDO: per-task run counts. ──
            // Phase-1 work was LEDGER-TERMINAL (completed/failed + replicated)
            // long before the kill: the failover winner must never have
            // re-dispatched ANY of it.
            for b in phase1_corpus() {
                let name = b.task_id.as_str();
                assert_eq!(
                    runs_of(&factory, name),
                    1,
                    "ledger-terminal phase-1 task {name} must have run exactly \
                     once across the failover (no redo of recorded work); \
                     run_counts: {:?}",
                    factory.run_counts.lock().unwrap()
                );
            }
            // The fast phase-2 tasks completed + replicated comfortably
            // before the kill (instant workers, 800 ms window): also exactly
            // once.
            for i in 0..3 {
                let name = format!("p2_fast_{i}");
                assert_eq!(
                    runs_of(&factory, &name),
                    1,
                    "pre-kill-completed task {name} must have run exactly once \
                     (no reassignment of completed tasks); run_counts: {:?}",
                    factory.run_counts.lock().unwrap()
                );
            }
            // The slow phase-2 tasks were IN FLIGHT at the kill: each ran at
            // least once; a copy lost with sec-0's own worker — or a
            // completion report lost in the primary-less window — may
            // legitimately re-run ONCE (crash recovery of unrecorded work,
            // NOT a redo of ledger-terminal work).
            for i in 0..3 {
                let name = format!("p2_slow_{i}");
                let runs = runs_of(&factory, &name);
                assert!(
                    (1..=2).contains(&runs),
                    "kill-window in-flight task {name} must run 1..=2 times \
                     (1 = survived/replayed; 2 = lost with the dead node and \
                     legitimately recovered); got {runs}; run_counts: {:?}",
                    factory.run_counts.lock().unwrap()
                );
            }
        })
        .await;
}
