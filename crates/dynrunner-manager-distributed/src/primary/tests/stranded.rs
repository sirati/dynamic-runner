//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Stranded-task accounting: a "happy path" run where every binary
/// reaches a terminal completion must report `stranded_count() == 0`.
///
/// Pin: the new counter must not leak a stale residue from a previous
/// run, must be reset at `run()` start, and must agree with
/// `total - completed - failed` on the success arm. Without this
/// guard a refactor that forgot to reset `stranded_count` between
/// runs would silently turn every clean run into a `RunError::ClusterCollapsed`.
#[tokio::test(flavor = "current_thread")]
async fn stranded_count_is_zero_on_clean_run() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never running the dispatch loop this test asserts).
            seed_operational_ledger(&mut primary, binaries, deps);
            primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await
                .unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);
            assert_eq!(
                primary.stranded_count(),
                0,
                "clean-run stranded must be zero (total - completed - failed)"
            );
        })
        .await;
}

/// Mid-handshake disconnect helper: the fake sends Welcome + Cert +
/// MeshReady, then immediately drops the channel that lets it talk
/// back to the primary the moment it sees its first inbound message
/// (which in practice will be `PeerInfo`, the next message after
/// the primary completes its half of the handshake). The drop closes
/// the primary's mpsc receiver, surfacing as `recv() -> None` inside
/// the operational loop — i.e. the cluster-collapse failure mode the
/// stranded-tracking patch is designed to detect.
///
/// Kept next to the test that uses it because the shape ("die
/// after handshake, before the run loop can dispatch a single task")
/// is specific to the cluster-collapse regression and not general
/// enough to merit promotion to `test_helpers.rs`.
async fn fake_secondary_dies_post_mesh_ready(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
            is_observer: false,
            can_be_primary: false,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
            liveness_port: None,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Wait for any one inbound message — at this point the
    // handshake on the primary side is past the wait_for_connections
    // phase and we're either in peer_setup or initial_assignment.
    // Drop the outbound channel by letting it go out of scope so the
    // primary's `recv()` returns `None` once every fake has dropped
    // its clone. Any further inbound messages are simply discarded
    // by closing the receiver.
    let _ = incoming_from_primary.recv().await;
    drop(outgoing_to_primary);
}

/// Transfer-complete-window disconnect helper: the fake completes the full
/// handshake (Welcome + Cert + MeshReady) and stays alive THROUGH the
/// primary's `perform_initial_assignment` — it drains inbound until it sees
/// its `InitialAssignment` (so that send SUCCEEDS, i.e. the assignment-time
/// collapse path does NOT fire) — then drops its outbound channel. Dropping
/// it tears the channel transport's peer down, so the production pump's
/// `recv_peer()` returns `None` and the pump exits, dropping the primary's
/// egress-queue receiver. The NEXT pre-loop send — `send_transfer_complete`,
/// which runs immediately after assignment — then observes the gone mesh-pump
/// (`client.send` `Err`), latching `mesh_pump_gone` and tripping
/// `run_pipeline`'s post-transfer collapse gate.
///
/// Kept next to the test that uses it for the same reason as
/// [`fake_secondary_dies_post_mesh_ready`]: the shape ("survive assignment,
/// die at the transfer-complete window") is specific to this collapse-window
/// regression, not general enough to promote to `test_helpers.rs`.
async fn fake_secondary_dies_at_transfer_complete(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
            is_observer: false,
            can_be_primary: false,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
            liveness_port: None,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            target: None,
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Drain inbound until the InitialAssignment lands. Everything earlier
    // (PeerInfo, the cold-seed ClusterMutation batch) is consumed silently so
    // its `send_to` succeeds — the assignment-time collapse path must NOT be
    // what fires here. The moment the InitialAssignment is in hand the
    // assignment send has already succeeded; dropping the outbound channel now
    // closes this peer so the pump exits BEFORE the primary's immediately-
    // following `send_transfer_complete` queues — surfacing the gone mesh-pump
    // on that send.
    while let Some(msg) = incoming_from_primary.recv().await {
        if matches!(msg, DistributedMessage::InitialAssignment { .. }) {
            break;
        }
    }
    drop(outgoing_to_primary);
}

/// Thread-local tracing buffer: captures every ERROR event emitted on the
/// current thread for the lifetime of the returned guard.
///
/// `current_thread` tokio flavour + `LocalSet` keep every spawned
/// fake-secondary on the same thread as the test future, so a
/// `set_default()` thread-local subscriber is reached by every
/// `tracing::error!` site that the `run()` flow hits.
///
/// PARALLEL-SAFETY: `tracing` caches per-callsite interest GLOBALLY. With no
/// process-global subscriber (the `--lib` test binary's default when run
/// alone or filtered), a sibling test running in parallel evaluates the
/// diagnostic's `error!` callsite against the no-op global dispatcher first
/// and CACHES it as `never` — after which a thread-local subscriber never
/// sees the event. To defeat that we (1) idempotently install an
/// ERROR-interested process-global subscriber so the callsite can never cache
/// as `never` (a no-op when a sibling's `fmt::try_init` already set one — both
/// are ERROR-interested), and (2) `rebuild_interest_cache()` after attaching
/// the thread-local recorder so any already-poisoned `never` is recomputed to
/// `always`. The thread-local recorder then takes precedence on this thread,
/// so the line lands in `buf`.
fn capture_logs_thread_local() -> (
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    tracing::dispatcher::DefaultGuard,
) {
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // (1) Ensure SOME ERROR-interested process-global subscriber exists so the
    // diagnostic's callsite is never globally cached as `never`. Idempotent:
    // a no-op once any sibling test's `fmt::try_init` (also ERROR-interested)
    // has set the global. The `Err` (already-set) is the success case here.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(buf.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    // (2) Attach the per-thread recorder, then recompute every callsite's
    // interest against the now-ERROR-interested global so a prior parallel
    // `never`-poisoning is cleared and the diagnostic is recorded on emit.
    let guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();
    (buf, guard)
}

/// T-stranded-on-cluster-collapse: when the secondaries die fatally after
/// handshake but BEFORE any task is dispatched, the OPERATIONAL primary's
/// `run` must return `RunError::ClusterCollapsed` carrying the per-category
/// counts; the post-call accounting must satisfy
/// `completed + failed + stranded == total`; and the diagnostic log line must
/// fire so consumers grepping for "tasks left unassigned because cluster
/// routing collapsed" see it on every collapse.
///
/// This pins the assignment-time collapse path specifically: the secondaries
/// die post-mesh-ready, so they are gone by the time the operational primary's
/// `perform_initial_assignment` fans out `InitialAssignment`. The `send_to` to
/// the first (now-dead) secondary fails because the mesh-pump's egress receiver
/// has been dropped — the egress-side twin of the operational loop's
/// `recv() -> None` collapse criterion. `perform_initial_assignment` surfaces
/// the typed `InitialAssignmentOutcome::ClusterCollapsed`, and `run_pipeline`'s
/// `PromotedDestination` arm routes it into `finalize_terminal_accounting` —
/// the SOLE strand-classification site, shared with the operational-loop
/// finalize tail — so the full un-dispatched pool surfaces as stranded with the
/// proper `ClusterCollapsed` counts (NOT a raw `RunError::Other`).
///
/// Regression guard: pre-fix the assignment-time `send_to` failure
/// `?`-escaped `run_pipeline` as `Err(RunError::Other("mesh-pump (local-dispatch
/// receiver) dropped"))`, bypassing the strand-classification (which ran only
/// in `run_operational_and_finalize`, AFTER assignment) — a latent gap the
/// uniform-relocate reorder exposed by moving `wait_for_mesh_ready` ahead of the
/// role branch (so a secondary dying at mesh-ready now dies BEFORE assignment,
/// where the OLD ordering had it still alive). Twin:
/// `strand_broadcasts_run_aborted_not_run_complete`.
#[tokio::test(flavor = "current_thread")]
async fn stranded_on_cluster_collapse_returns_err_with_counts() {
    let (log_buf, _log_guard) = capture_logs_thread_local();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                fleet_dead_timeout: std::time::Duration::from_secs(600),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let total = binaries.len();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_dies_post_mesh_ready(
                    id,
                    /* num_workers = */ 1,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (the operational-primary seed). The
            // secondaries die post-mesh-ready → the assignment-time collapse
            // gap fires (see the IGNORED-doc above).
            seed_operational_ledger(&mut primary, binaries, deps);
            let outcome = primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await;

            // CORRECT expectation (un-ignore once the root-cause fix lands):
            match outcome {
                Err(RunError::ClusterCollapsed { stranded, outcome }) => {
                    assert!(
                        stranded > 0,
                        "stranded must be positive on cluster collapse"
                    );
                    assert_eq!(
                        outcome.total_terminal() + stranded,
                        total,
                        "succeeded + fail_retry + fail_oom + fail_final + stranded must equal total"
                    );
                    assert_eq!(outcome.succeeded, primary.completed_count());
                    assert_eq!(
                        outcome.fail_retry + outcome.fail_oom + outcome.fail_final,
                        primary.failed_count(),
                        "per-class fail buckets must sum to total failed_tasks count"
                    );
                    assert_eq!(stranded, primary.stranded_count());
                }
                other => panic!(
                    "expected RunError::ClusterCollapsed, got {other:?} (counters: \
                 succeeded={} failed={} stranded={} total={})",
                    primary.completed_count(),
                    primary.failed_count(),
                    primary.stranded_count(),
                    total,
                ),
            }

            // The collapse-arm's `tracing::error!` in
            // `finalize_terminal_accounting` must fire so ops scripts grepping
            // the log can detect every routing collapse. Captured via the
            // parallel-safe thread-local recorder (see
            // `capture_logs_thread_local`).
            let captured = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
            assert!(
                captured.contains("tasks left unassigned because cluster routing collapsed"),
                "diagnostic 'tasks left unassigned because cluster routing collapsed' must \
             fire on the cluster-collapse arm so ops scripts can detect it; captured \
             error-level logs:\n{captured}"
            );
        })
        .await;
}

/// #235 primary half: on a routing collapse (`stranded > 0`) the shared
/// finalize tail must broadcast `ClusterMutation::RunAborted { reason }`,
/// NOT `RunComplete`. Pre-fix the tail broadcast `RunComplete`
/// unconditionally, so a still-connected observer's narrator projected a
/// false "run complete" over a collapsed cluster (the strand was invisible
/// on the important channel).
///
/// `apply_and_broadcast_cluster_mutations` applies the terminal mutation to
/// the primary's OWN `cluster_state` before fanning it over the mesh — the
/// same mutation every connected peer receives — so the post-run CRDT state
/// is the faithful observable for WHAT was broadcast: `run_aborted()` is
/// `Some` and `run_complete()` is false. The locally-returned
/// `RunError::ClusterCollapsed` is unchanged (asserted separately).
///
/// Exercises the SAME assignment-time collapse path as
/// [`stranded_on_cluster_collapse_returns_err_with_counts`] (see its doc): the
/// secondaries die post-mesh-ready, so `perform_initial_assignment` hits the
/// mesh-pump-gone send failure, surfaces `InitialAssignmentOutcome::Cluster
/// Collapsed`, and `run_pipeline` routes it into `finalize_terminal_accounting`
/// — the shared strand-classification site that broadcasts the honest
/// `RunAborted` terminal. This test pins the BROADCAST half: the peer-facing
/// terminal is `RunAborted` (carrying the `ClusterCollapsed` render), NOT
/// `RunComplete`. Regression guard: pre-fix the assignment-time collapse
/// escaped as `Err(Other("mesh-pump dropped"))` BEFORE any terminal broadcast,
/// so neither `RunAborted` nor `RunComplete` was ever applied.
#[tokio::test(flavor = "current_thread")]
async fn strand_broadcasts_run_aborted_not_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                fleet_dead_timeout: std::time::Duration::from_secs(600),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_dies_post_mesh_ready(
                    id,
                    /* num_workers = */ 1,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never reaching the strand/complete behaviour this test asserts).
            seed_operational_ledger(&mut primary, binaries, deps);
            let outcome = primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await;

            // Local return is the unchanged ClusterCollapsed.
            assert!(
                matches!(outcome, Err(RunError::ClusterCollapsed { .. })),
                "strand must still return ClusterCollapsed locally, got {outcome:?}"
            );

            // The peer-facing broadcast is the honest RunAborted, NOT
            // RunComplete — observed through the local apply.
            let state = primary.cluster_state_for_test();
            let reason = state.run_aborted().unwrap_or_else(|| {
                panic!(
                    "strand must broadcast RunAborted (run_aborted() = Some); \
                     run_complete()={}",
                    state.run_complete()
                )
            });
            assert!(
                !state.run_complete(),
                "strand must NOT latch RunComplete — pre-fix it did, narrating a \
                 false success on a collapsed cluster"
            );
            // Reason reuses the ClusterCollapsed render — carries the
            // per-class breakdown so the observer's narrator emits a
            // meaningful aborted line on the important channel.
            assert!(
                reason.contains("cluster routing collapsed"),
                "abort reason must carry the ClusterCollapsed render, got: {reason}"
            );
        })
        .await;
}

/// T-stranded-at-transfer-complete: the SIBLING-send collapse window.
///
/// A secondary survives the primary's `perform_initial_assignment` (its
/// `InitialAssignment` send succeeds) but dies immediately after, so the
/// next pre-loop send — `send_transfer_complete` — hits the now-gone local
/// mesh-pump. Pre-fix that `send_to` `?`-escaped `run_pipeline` as a raw
/// `Err(RunError::Other("…mesh-pump…dropped"))`, bypassing the
/// strand-classification entirely: the assigned-but-unconfirmed work was an
/// UNCLASSIFIED `Other` instead of a clean `ClusterCollapsed`. The fix makes
/// `send_to` latch the local-pump-gone condition on `mesh_pump_gone` (the SOLE
/// detection point, shared with every pre-loop send) and `run_pipeline`'s
/// post-transfer gate route it into `finalize_terminal_accounting` — the SAME
/// SOLE strand-classification site the assignment-time path and the
/// operational loop converge on. So a death in the transfer-complete window
/// is classified identically: `ClusterCollapsed` with per-category counts, the
/// diagnostic log line, and the honest `RunAborted` terminal broadcast.
///
/// This pins the newly-covered sibling-send site (the assignment-time path is
/// pinned by [`stranded_on_cluster_collapse_returns_err_with_counts`] /
/// [`strand_broadcasts_run_aborted_not_run_complete`]); together they cover the
/// whole pre-loop send chain uniformly.
#[tokio::test(flavor = "current_thread")]
async fn stranded_at_transfer_complete_window_returns_err_with_counts() {
    let (log_buf, _log_guard) = capture_logs_thread_local();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Single secondary: its drop (after it has the InitialAssignment in
            // hand) deterministically tears the pump down in the window between
            // the assignment send and `send_transfer_complete`.
            let (transport, secondary_ends) = setup_test(1);

            let config = PrimaryConfig {
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                fleet_dead_timeout: std::time::Duration::from_secs(600),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let total = binaries.len();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_dies_at_transfer_complete(
                    id,
                    /* num_workers = */ 2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot`. The secondary dies in the
            // transfer-complete window → the sibling-send collapse gate fires.
            seed_operational_ledger(&mut primary, binaries, deps);
            let outcome = primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await;

            match outcome {
                Err(RunError::ClusterCollapsed { stranded, outcome }) => {
                    assert!(
                        stranded > 0,
                        "stranded must be positive on transfer-complete-window collapse"
                    );
                    assert_eq!(
                        outcome.total_terminal() + stranded,
                        total,
                        "succeeded + fail_retry + fail_oom + fail_final + stranded must equal total"
                    );
                    assert_eq!(stranded, primary.stranded_count());
                }
                other => panic!(
                    "expected RunError::ClusterCollapsed, got {other:?} (counters: \
                     succeeded={} failed={} stranded={} total={})",
                    primary.completed_count(),
                    primary.failed_count(),
                    primary.stranded_count(),
                    total,
                ),
            }

            // The shared collapse-arm diagnostic must fire (same SOLE finalize
            // site as the assignment-time path), so ops scripts grepping the
            // log detect this window's collapse too.
            let captured = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
            assert!(
                captured.contains("tasks left unassigned because cluster routing collapsed"),
                "the collapse diagnostic must fire on the transfer-complete-window arm; \
                 captured error-level logs:\n{captured}"
            );

            // The peer-facing broadcast is the honest RunAborted, NOT
            // RunComplete (the sibling-send window reaches the SAME terminal
            // broadcast as the assignment-time path).
            let state = primary.cluster_state_for_test();
            assert!(
                state.run_aborted().is_some(),
                "transfer-complete-window collapse must broadcast RunAborted; \
                 run_complete()={}",
                state.run_complete()
            );
            assert!(
                !state.run_complete(),
                "transfer-complete-window collapse must NOT latch RunComplete"
            );
        })
        .await;
}

/// T-stranded-after-Owed-discovery: the mode-2 `RelocatedSeed`/`--source-
/// already-staged` collapse path — the false-green this whole strand-accounting
/// machinery exists to prevent, on the ONE seed shape the prior collapse tests
/// never exercised.
///
/// The operational primary inherits a `DiscoveryDebt::Owed` ledger with NO
/// corpus (the pre-staged / relocated-seed shape: the setup peer staged only
/// the phase graph + the `Owed` marker, never the tasks). At hydrate the ledger
/// is empty, so a denominator captured THEN would be `0`. `discover_on_promotion`
/// then runs the registered policy, seeds N tasks, and RE-hydrates
/// `self.total_tasks` to N. A cluster collapse AFTER discovery (secondaries die
/// post-mesh-ready, so they are gone by `perform_initial_assignment`) must
/// strand all N discovered-but-undispatched tasks: `RunError::ClusterCollapsed`
/// with `stranded == N`, the honest `RunAborted` terminal broadcast, and the
/// collapse diagnostic.
///
/// Regression guard: pre-fix `run_pipeline` captured `let total =
/// self.total_tasks` AFTER hydrate but BEFORE `discover_on_promotion`, so on
/// this Owed/pre-staged path the captured `total` was the stale pre-discovery
/// `0` and flowed into `finalize_terminal_accounting`, where `stranded =
/// 0.saturating_sub(0) = 0` ⇒ the gate broadcast `RunComplete` and returned
/// `Ok(())` — rc=0 on a collapsed cluster with N tasks never dispatched. The
/// fix makes the finalize tail read the LIVE `self.total_tasks` (refreshed by
/// discovery's re-hydrate), so the stale snapshot can never reach the stranded
/// denominator. The existing collapse tests seed via `seed_operational_ledger`
/// (corpus present, debt `Undeclared`/`Settled`) so they never hit this path —
/// this test's distinguishing setup is the `Owed` marker + discovery seed.
#[tokio::test(flavor = "current_thread")]
async fn stranded_after_owed_discovery_collapse_returns_err_not_run_complete() {
    let (log_buf, _log_guard) = capture_logs_thread_local();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                fleet_dead_timeout: std::time::Duration::from_secs(600),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Inherited Owed ledger with NO corpus: declare the discovery debt
            // (the relocated-seed / pre-staged shape) and DO NOT
            // `seed_operational_ledger` — at hydrate the ledger is empty, so a
            // denominator captured then would be the stale `0`.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::DiscoveryDebtDeclared);

            // The discovery policy that `discover_on_promotion` runs on the
            // inherited `Owed` marker — yields the N-task corpus the setup peer
            // never seeded, then re-hydrates `total_tasks` to N.
            const N: usize = 6;
            let discovered: Vec<TaskInfo<TestId>> = (0..N)
                .map(|i| make_binary(&format!("disc_{i}"), 50 + (i as u64) * 10))
                .collect();
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                discovered,
                HashMap::new(),
                fires.clone(),
            ));

            // Secondaries die post-mesh-ready → they are gone by the time the
            // operational primary's `perform_initial_assignment` fans out (the
            // assignment-time collapse), which fires AFTER `discover_on_promotion`
            // has already seeded + re-hydrated to N. So the full discovered pool
            // is stranded.
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary_dies_post_mesh_ready(
                    id,
                    /* num_workers = */ 1,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (_deps, ops, ope) = noop_phase_args();
            // Operational primary on the Owed/discovery seed path:
            // `PromotionSnapshot` ⇒ `BootstrapRole::PromotedDestination`, which
            // runs `discover_on_promotion` then the in-place tail.
            let outcome = primary.run(SeedSource::PromotionSnapshot, ops, ope).await;

            assert_eq!(
                fires.get(),
                1,
                "the discovery policy must run exactly once on the Owed path"
            );

            // Post-fix: the live `self.total_tasks` (N, set by discovery's
            // re-hydrate) is the denominator, so the full pool strands.
            match outcome {
                Err(RunError::ClusterCollapsed { stranded, outcome }) => {
                    assert_eq!(
                        stranded, N,
                        "every discovered-but-undispatched task must strand \
                         (N discovered, none dispatched before the collapse)"
                    );
                    assert_eq!(
                        outcome.total_terminal() + stranded,
                        N,
                        "succeeded + fail_retry + fail_oom + fail_final + stranded must equal N"
                    );
                    assert_eq!(stranded, primary.stranded_count());
                }
                other => panic!(
                    "pre-fix this returned Ok(()) (the false-green: stale total=0 ⇒ \
                     stranded=0 ⇒ RunComplete); expected RunError::ClusterCollapsed with \
                     stranded={N}, got {other:?} (counters: succeeded={} failed={} \
                     stranded={} total_tasks={})",
                    primary.completed_count(),
                    primary.failed_count(),
                    primary.stranded_count(),
                    primary.total_tasks,
                ),
            }

            // The peer-facing broadcast is the honest RunAborted, NOT the
            // false-success RunComplete the stale-total path latched pre-fix.
            let state = primary.cluster_state_for_test();
            assert!(
                state.run_aborted().is_some(),
                "Owed-discovery collapse must broadcast RunAborted; run_complete()={}",
                state.run_complete()
            );
            assert!(
                !state.run_complete(),
                "Owed-discovery collapse must NOT latch RunComplete — that is the \
                 exact false-green the stale-total bug produced (rc=0 on a collapsed \
                 cluster with {N} tasks never dispatched)"
            );

            // The shared collapse diagnostic must fire so ops scripts detect it.
            let captured = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
            assert!(
                captured.contains("tasks left unassigned because cluster routing collapsed"),
                "the collapse diagnostic must fire on the Owed-discovery collapse arm; \
                 captured error-level logs:\n{captured}"
            );
        })
        .await;
}

/// #235 primary half, clean twin: a happy-path run must still broadcast
/// `RunComplete` (not `RunAborted`) — the conditional only diverges on
/// `stranded > 0`. Same local-apply observable as the strand twin.
#[tokio::test(flavor = "current_thread")]
async fn clean_run_broadcasts_run_complete_not_aborted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..4)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(
                    id,
                    /* num_workers = */ 2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never reaching the strand/complete behaviour this test asserts).
            seed_operational_ledger(&mut primary, binaries, deps);
            primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await
                .expect("clean run must return Ok");

            let state = primary.cluster_state_for_test();
            assert!(state.run_complete(), "clean run must latch RunComplete");
            assert!(
                state.run_aborted().is_none(),
                "clean run must NOT broadcast RunAborted, got reason: {:?}",
                state.run_aborted()
            );
        })
        .await;
}

/// Pin Fix-#21 contract: when the operational loop's
/// `fleet_dead_timeout` arm fires with queued tasks in the pool, the
/// drained binaries must be classified as `stranded` — not `failed`.
/// They were never dispatched, no secondary attempted them, no worker
/// reported a failure, so the only honest category is "couldn't be
/// tried". Pre-fix the arm pushed each pending task's hash into
/// `failed_tasks`, conflating worker-reported failure with
/// never-dispatched and (worse) burning the retry budget on tasks
/// that hadn't actually failed.
///
/// We drive the operational loop directly (bypassing `run()`'s setup
/// phases) so the fleet-dead arm fires from a primed state: empty
/// secondaries map + non-empty pool + tight timeout. This isolates
/// the pre/post fix semantic delta to a single observable assertion
/// (`failed_tasks.is_empty() && pool drained`) — pre-fix the arm
/// would land every drained binary in `failed_tasks`; post-fix the
/// arm leaves `failed_tasks` empty and the binaries flow into the
/// run-level `stranded` category by way of the `total - completed -
/// failed` accounting in `run()`.
#[tokio::test(flavor = "current_thread")]
async fn fleet_dead_timeout_pending_become_stranded_not_failed() {
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let config = PrimaryConfig {
                num_secondaries: 0,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_secs(60),
                retry_max_passes: 0,
                // Zero timeout so the very first loop iteration's
                // `elapsed >= fleet_dead_timeout` predicate trips, no
                // wall-clock wait needed in the test.
                fleet_dead_timeout: std::time::Duration::ZERO,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Prime: pool with three queued binaries, empty secondaries
            // map (the fleet-dead predicate is `secondaries.is_empty() &&
            // !pool.is_empty()`), `total_tasks` set so the run-level
            // accounting can later compute `stranded = total -
            // completed - failed`.
            let phase = dynrunner_core::PhaseId::from("default");
            let mut pool =
                PendingPool::<TestId>::new([phase.clone()], std::collections::HashMap::new())
                    .expect("default-phase pool");
            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            pool.extend(binaries.clone()).expect("valid extend");
            primary.pending = Some(pool);
            primary.all_binaries = binaries.clone();
            primary.total_tasks = binaries.len();

            // No workers, no secondaries — fleet-dead arm fires
            // immediately on entry to the operational loop.
            primary
                .operational_loop()
                .await
                .expect("operational_loop must return Ok on the fleet-dead exit path");

            // Pool must be drained so the loop terminates (both pre and
            // post fix).
            assert!(
                primary.pool().is_empty(),
                "fleet-dead arm must drain the queued pool"
            );
            // Fix-#21 contract: pre-fix this set was populated with the
            // drained binaries' hashes; post-fix it stays empty so the
            // `total - completed - failed` accounting downstream
            // classifies them as stranded.
            assert!(
                primary.failed_tasks.is_empty(),
                "fleet-dead pending must NOT be classified as failed; pre-fix \
             arm pushed pending hashes into failed_tasks, conflating \
             never-dispatched with worker-reported failure (got {:?})",
                primary.failed_tasks
            );
            assert!(
                primary.completed_tasks.is_empty(),
                "fleet-dead with un-dispatched tasks must report no completions"
            );

            // Drive the run-level accounting that `run()` would do post-
            // operational-loop, end-to-end-equivalent. With failed and
            // completed both empty, every binary lands in the stranded
            // bucket — exactly the category Fix-#21 surfaces.
            let total = primary.total_tasks;
            let completed = primary.completed_tasks.len();
            let failed = primary.failed_tasks.len();
            let stranded = total.saturating_sub(completed + failed);
            assert_eq!(
                stranded, total,
                "every un-dispatched binary must surface as stranded \
             (completed={completed} failed={failed} total={total})"
            );
        })
        .await;
}

/// Pin: `drain_pending_messages` processes any `TaskComplete` /
/// `TaskFailed` messages still queued in the inbound transport when
/// it's invoked, updating `completed_tasks` / `failed_tasks` exactly
/// as the operational loop's `recv → dispatch_message` pipeline does.
/// This is the helper the post-loop drain step in `run()` calls before
/// computing the stranded count, closing the window where the
/// pre-fix accounting saw pre-drain counters and false-positived clean
/// runs into `RunError::ClusterCollapsed` (counted successful
/// completions as `stranded`).
///
/// Construction: we drive the helper directly (no `run()` lifecycle)
/// by pre-loading TaskComplete messages into the shared incoming
/// channel and asserting the helper drains them. The drain helper
/// reuses `dispatch_message`, which calls `handle_task_complete`,
/// which inserts into `completed_tasks` regardless of whether a
/// matching worker exists — so we don't have to plumb a fake worker
/// to exercise the counter update.
#[tokio::test(flavor = "current_thread")]
async fn drain_pending_messages_updates_completed_set() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Inject three TaskComplete messages from the fake secondary's
            // outbound clone (which is what `secondary_ends[i].2` is — the
            // shared inbound side from the primary's perspective). Closing
            // the sender at the end ensures `transport.recv()` will yield
            // `Some` for each queued message and then `None`, exercising
            // the drain helper's "process until empty" path through both
            // arms of the recv result.
            let (sec_id, _to_sec_rx, incoming_tx) = secondary_ends.into_iter().next().unwrap();
            for hash in ["hash-a", "hash-b", "hash-c"] {
                incoming_tx
                    .send(DistributedMessage::TaskComplete {
                        target: None,
                        sender_id: sec_id.clone(),
                        timestamp: 0.0,
                        secondary_id: sec_id.clone(),
                        worker_id: 0,
                        task_hash: hash.into(),
                        result_data: None,
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    })
                    .unwrap();
            }
            // Drop the sender so the recv channel will eventually yield
            // `None`. The drain helper should treat that as "transport
            // closed → drain complete" and break.
            drop(incoming_tx);

            primary
                .drain_pending_messages(Duration::from_millis(500))
                .await
                .expect("drain must succeed on healthy transport");

            // Post-drain the per-hash completed-set has every drained
            // TaskComplete's hash. We pin the HashSet contents directly
            // (rather than `completed_count()`) because the test injects
            // synthesized TaskComplete messages without prior TaskAdded
            // mutations — `cluster_state.apply(TaskCompleted)` NoOps on
            // a non-existent ledger entry (CRDT precondition). The drain's
            // load-bearing job here is to consume the messages through
            // `handle_task_complete` (line 58: `completed_tasks.insert`),
            // not to converge the CRDT mirror; the post-fix
            // `completed_count()` reads through the CRDT and would NOT
            // observe these synthetic completes.
            assert_eq!(
                primary.completed_tasks.len(),
                3,
                "drain must have processed all three queued TaskComplete messages \
             (per-hash set populated by handle_task_complete's direct insert)"
            );
            for hash in ["hash-a", "hash-b", "hash-c"] {
                assert!(
                    primary.completed_tasks.contains(hash),
                    "completed_tasks must contain {hash} after drain"
                );
            }
        })
        .await;
}

/// Pin Fix-#23 contract end-to-end: a happy-path run with multiple
/// secondaries where every dispatched task succeeds must not surface
/// any task as `stranded`. Pre-fix the accounting in `run()` ran
/// before any post-loop drain of in-flight TaskComplete messages,
/// flipping clean runs into `RunError::ClusterCollapsed`. Post-fix
/// the drain step processes whatever was still in transit before the
/// stranded computation, so completed runs report
/// `completed=N / failed=0 / stranded=0` and `run()` returns `Ok(())`.
///
/// Two secondaries here (vs the N=1 fixture in
/// `stranded_count_is_zero_on_clean_run`) widens the surface to the
/// multi-secondary code paths — completion-forwarding, per-secondary
/// worker bookkeeping — that the e2e scenario in #19 surfaced.
#[tokio::test(flavor = "current_thread")]
async fn clean_run_does_not_false_positive_stranded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..4)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let total = binaries.len();

            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(
                    id,
                    /* num_workers = */ 2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never reaching the strand/complete behaviour this test asserts).
            seed_operational_ledger(&mut primary, binaries, deps);
            primary
                .run(SeedSource::PromotionSnapshot, ops, ope)
                .await
                .expect("clean multi-secondary run must return Ok");

            assert_eq!(
                primary.completed_count(),
                total,
                "every binary must report completed on a clean run"
            );
            assert_eq!(
                primary.failed_count(),
                0,
                "no binary should land in failed on a clean run"
            );
            assert_eq!(
                primary.stranded_count(),
                0,
                "no binary should be stranded on a clean run — \
             pre-fix the accounting ran before pending TaskCompletes \
             drained, false-positiving successful tasks as stranded"
            );
        })
        .await;
}
