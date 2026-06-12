//! Regression tests for the relocate/promote STAGING-CONTEXT completeness
//! bug: a promoted/relocated compute-peer primary must carry the run's
//! `uses_file_based_items` / `source_pre_staged_root` / `source_dir` so its
//! own `InitialAssignment` re-stamps the SAME dispatch flags the original
//! submitter primary did — otherwise a REMOTE secondary that receives that
//! assignment re-requires a `StageFile` for a no-file / bind-mounted item and
//! fails NonRecoverable ("not pre-staged at <path>; expected StageFile
//! notification first").
//!
//! The pre-existing relocate e2e (`node_gates`) did NOT catch this: its lone
//! compute peer BECOMES the primary and dispatches to its OWN same-peer
//! worker pool, which never runs the secondary-side
//! `report_unresolvable_task` guard. These tests add a SECOND, plain real
//! secondary (the dispatch TARGET) so the promoted primary's stamped flags
//! are actually exercised by the remote-secondary dispatch resolver.
//!
//! Two coverage tiers:
//!   * `stamped_initial_assignment_flags_*` — assert, at the
//!     InitialAssignment-flag level, that a primary whose `PrimaryConfig`
//!     carries each facet stamps the matching wire flags (the chain a
//!     promoted primary's recipe-threaded config must reproduce).
//!   * `relocated_primary_*` — drive the FULL `Node::run` relocate with a
//!     promote recipe that mirrors the PRODUCTION pyo3 recipe's SOURCE: it
//!     stamps the flags from the node's own LOCAL PRODUCER while the
//!     relocate-target's `InitialAssignment`-fed cell stays at `Default` (the
//!     recipe asserts this) — exactly the lifecycle a relocate-target sees (no
//!     `InitialAssignment` before promotion). A SEPARATE real secondary is the
//!     dispatch target; assert it does NOT wrongly raise "not pre-staged".
//!     This catches the false-green that a pre-built config (which bypasses
//!     the cell-vs-producer source question) silently passed.

use super::*;

use crate::process::{LocalRole, Mesh, MeshHost, Node, NodeRunInputs, PrimaryRunArgs};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_transport_channel::peer_mesh;
use std::sync::{Arc, Mutex};

// ─────────────────────────────────────────────────────────────────────────
// Tier 1: InitialAssignment-flag-level stamping per facet.
//
// `perform_initial_assignment` is the single site that stamps
// `pre_staged_mode` / `uses_file_based_items` (from `self.config`) into every
// recipient's `InitialAssignment`. A promoted primary whose recipe threaded
// the staging context into its `PrimaryConfig` must produce exactly these
// stamps. We drive an operational `PromotionSnapshot` primary off a config
// carrying each facet (the SAME config shape the promote recipe builds) and
// CAPTURE the wire `InitialAssignment` the secondary receives — asserting on
// the bytes the recipient's dispatch resolver keys off, not on the primary's
// internal config.
// ─────────────────────────────────────────────────────────────────────────

/// Build a one-secondary operational primary off `config`, run it to
/// completion against a `fake_secondary`, and return the
/// `(pre_staged_mode, uses_file_based_items)` flags it stamped into the
/// `InitialAssignment` the secondary received. A wire tap between the primary's
/// egress and the fake's inbox snapshots the first `InitialAssignment` frame.
async fn stamped_flags_for_config(config: PrimaryConfig) -> (bool, bool) {
    let secondary_id = "sec-0".to_string();

    // primary → (tap) → fake secondary
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    let (tap_tx, tap_rx) = tokio_mpsc::unbounded_channel();
    // fake secondary → primary
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let captured: Arc<Mutex<Option<(bool, bool)>>> = Arc::new(Mutex::new(None));
    let captured_for_tap = captured.clone();
    // The tap forwards every frame to the fake AND snapshots the first
    // InitialAssignment's flags.
    tokio::task::spawn_local(async move {
        let mut rx = pri_to_sec_rx;
        while let Some(msg) = rx.recv().await {
            if let DistributedMessage::InitialAssignment {
                pre_staged_mode,
                uses_file_based_items,
                ..
            } = &msg
            {
                let mut slot = captured_for_tap.lock().unwrap();
                if slot.is_none() {
                    *slot = Some((*pre_staged_mode, *uses_file_based_items));
                }
            }
            if tap_tx.send(msg).is_err() {
                break;
            }
        }
    });

    tokio::task::spawn_local(fake_secondary(
        secondary_id.clone(),
        2,
        1024 * 1024 * 1024,
        tap_rx,
        sec_to_pri_tx,
    ));

    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
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
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let binaries = vec![make_binary("only", 10)];
    {
        let (deps, ops, ope) = noop_phase_args();
        seed_operational_ledger(&mut primary, binaries, deps);
        primary
            .run(SeedSource::PromotionSnapshot, ops, ope)
            .await
            .unwrap();
    }

    let result = captured.lock().unwrap().take();
    result.expect("secondary must receive exactly one InitialAssignment")
}

/// Facet (a): `uses_file_based_items=false` → the stamped wire flag is
/// `false`, so the secondary's dispatch resolver passes `local_path` through
/// as an opaque identifier and never requires a StageFile.
#[tokio::test(flavor = "current_thread")]
async fn stamped_initial_assignment_flags_uses_file_based_items_false() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = PrimaryConfig {
                uses_file_based_items: false,
                ..test_primary_config()
            };
            let (pre_staged, uses_files) = stamped_flags_for_config(config).await;
            assert!(!uses_files, "uses_file_based_items must stamp false");
            assert!(!pre_staged, "no pre-staged mode for this facet");
        })
        .await;
}

/// Facet (b): `source_pre_staged_root=Some` → the stamped `pre_staged_mode`
/// flag is `true`, so the secondary resolves bind-mounted files by existence
/// (no StageFile required).
#[tokio::test(flavor = "current_thread")]
async fn stamped_initial_assignment_flags_pre_staged_mode() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let root = tempfile::TempDir::new().expect("tmpdir");
            let config = PrimaryConfig {
                source_pre_staged_root: Some(root.path().to_path_buf()),
                ..test_primary_config()
            };
            let (pre_staged, uses_files) = stamped_flags_for_config(config).await;
            assert!(pre_staged, "source_pre_staged_root=Some must stamp pre_staged_mode=true");
            assert!(uses_files, "file-based stays true in pre-staged mode");
        })
        .await;
}

/// Facet (c) at the stamp level: mode-1 file-based, non-pre-staged → the
/// historical default flags (`pre_staged_mode=false`, `uses_file_based_items=
/// true`). The dispatch acceptance for this facet (the promoted primary
/// RE-STAGES via `maybe_auto_stage_initial`, so the secondary gets StageFile
/// records) is covered by `relocated_primary_mode1_file_based_restages`.
#[tokio::test(flavor = "current_thread")]
async fn stamped_initial_assignment_flags_mode1_defaults() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = test_primary_config();
            let (pre_staged, uses_files) = stamped_flags_for_config(config).await;
            assert!(!pre_staged, "mode-1 is not pre-staged");
            assert!(uses_files, "mode-1 items are file-based");
        })
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Tier 2: FULL Node::run relocate, dispatch-acceptance on a SEPARATE real
// secondary. The promoted primary is built via a promote recipe that mirrors
// the PRODUCTION pyo3 recipe's SOURCE: it stamps the staging flags from the
// node's own LOCAL PRODUCER, and ASSERTS the relocate-target's
// `InitialAssignment`-fed cell is still at `Default` at the promotion instant
// (the relocate-target receives no `InitialAssignment` before it promotes).
// The dispatch TARGET is a plain real secondary whose
// `report_unresolvable_task` guard FIRES if the promoted primary stamps the
// wrong flags — which it WOULD if the recipe read the (Default) cell.
// ─────────────────────────────────────────────────────────────────────────

/// What the promote recipe sources from the relocate-target's local producer
/// into the relocated primary's `PrimaryConfig`. One per facet.
struct RelocateStagingFacet {
    /// The relocated primary's `PrimaryConfig.uses_file_based_items`.
    uses_file_based_items: bool,
    /// The relocated primary's `PrimaryConfig.source_pre_staged_root`.
    source_pre_staged_root: Option<std::path::PathBuf>,
    /// The relocated primary's `PrimaryConfig.source_dir` (the re-stage root).
    source_dir: Option<std::path::PathBuf>,
    /// The task corpus the setup peer cold-seeds (the relocated primary
    /// inherits it via the snapshot). Paths are facet-specific.
    binaries: Vec<TaskInfo<TestId>>,
    /// The dispatch-target secondary's `src_network` (Some only for the
    /// bind-mount facet).
    target_src_network: Option<std::path::PathBuf>,
}

/// Drive a 3-node `Node::run` relocate — setup peer (cold-seeds + relocates),
/// `sec-0` (the relocate TARGET: promotion-eligible, becomes the primary via a
/// config-carrying recipe), and `sec-1` (a plain real secondary: the dispatch
/// target whose staging guard is what this test exercises). Returns
/// `(setup_outcome.completed, sec1_completed)`.
async fn run_relocate_with_dispatch_target(facet: RelocateStagingFacet) -> (usize, usize) {
    let RelocateStagingFacet {
        uses_file_based_items,
        source_pre_staged_root,
        source_dir,
        binaries,
        target_src_network,
    } = facet;
    let num_tasks = binaries.len();

    let ids = vec!["setup".to_string(), "sec-0".to_string(), "sec-1".to_string()];
    let mut transports = peer_mesh::<TestId>(&ids);
    let sec1_transport = transports.pop().unwrap(); // "sec-1"
    let sec0_transport = transports.pop().unwrap(); // "sec-0"
    let pri_transport = transports.pop().unwrap(); // "setup"

    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        1024 * 1024 * 1024u64,
    )]);

    // Shared config template for both real secondaries.
    let sec_config = |id: &str, can_be_primary: bool, src_network: Option<std::path::PathBuf>| {
        SecondaryConfig {
            secondary_id: id.into(),
            num_workers: 2,
            max_resources: max_res.clone(),
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
            src_network,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            primary_silence_backstop: Duration::from_secs(120),
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
    };

    // ── sec-0: the relocate TARGET (promotion-eligible). Its promote recipe
    //    sources the FACET'S staging flags from the LOCAL PRODUCER (mirroring
    //    the pyo3 recipe) while asserting the wire-fed cell stays at `Default`
    //    at promotion. ─────────────────────────────────────────────────────
    let mut sec0_mesh = Mesh::new(sec0_transport);
    let (sec0_slot, sec0_client, sec0_inbox) =
        sec0_mesh.register_local_role(LocalRole::Secondary, PeerId::from("sec-0"));
    sec0_mesh.publish_membership();
    // sec-0's co-located secondary (the promoted primary's same-peer
    // secondary) is a dispatch recipient too, so it needs the SAME
    // `src_network` as sec-1 — on a shared FS / common bind-mount every
    // secondary sees the staged corpus at the same path.
    let mut sec0 = SecondaryCoordinator::new(
        sec_config("sec-0", true, target_src_network.clone()),
        sec0_client,
        sec0_inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    sec0.set_bootstrap_primary_id("setup".to_string());
    let (sec0_node, sec0_promo_tx) = Node::new(MeshHost::on_local_set(sec0_mesh));
    sec0.register_promotion_signal(sec0_promo_tx);
    // Capture the relocate-target's LIVE staging-dispatch cell BEFORE
    // `with_secondary` consumes `sec0`. The recipe below threads it in to PROVE
    // it stays at `Default` at the promotion instant — a relocate-target gets
    // no `InitialAssignment` before it promotes — while the stamped flags come
    // from the LOCAL PRODUCER (`flags`). This drives the PRODUCTION recipe's
    // source discipline (the pyo3 recipe sources from the producer, not the
    // cell); a pre-built config would bypass it and re-create the false-green.
    let sec0_staging_cell = sec0.staging_dispatch_context_handle_for_test();
    let promote = build_test_promote_recipe_from_producer(
        "sec-0".to_string(),
        ProducerStagingFlags {
            uses_file_based_items,
            // The local producer's pre-staged discriminant: the recipe stamps
            // `source_pre_staged_root` IFF this is set (mirrors the submitter's
            // `source_pre_staged_root.is_some()` and the pyo3 recipe's
            // `pre_staged_mode` gate).
            pre_staged_mode: source_pre_staged_root.is_some(),
            source_pre_staged_root,
            source_dir,
        },
        sec0_staging_cell,
        None,
    );
    let sec0_node = sec0_node.with_secondary(sec0, sec0_slot);
    let sec0_inputs: NodeRunInputs<FakeWorkerFactory, _, _, TestId> = NodeRunInputs {
        secondary_factory: Some(FakeWorkerFactory),
        promote: Some(promote),
        ..Default::default()
    };
    let sec0_handle = tokio::task::spawn_local(sec0_node.run(sec0_inputs));

    // ── sec-1: a plain real secondary — the dispatch TARGET. NOT
    //    promotion-eligible (no promote recipe). Its `report_unresolvable_task`
    //    guard fires if the promoted primary stamps the wrong flags. ─────────
    let mut sec1_mesh = Mesh::new(sec1_transport);
    let (sec1_slot, sec1_client, sec1_inbox) =
        sec1_mesh.register_local_role(LocalRole::Secondary, PeerId::from("sec-1"));
    sec1_mesh.publish_membership();
    let mut sec1 = SecondaryCoordinator::new(
        sec_config("sec-1", false, target_src_network),
        sec1_client,
        sec1_inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    sec1.set_bootstrap_primary_id("setup".to_string());
    let (sec1_node, _sec1_promo_tx) = Node::new(MeshHost::on_local_set(sec1_mesh));
    let sec1_node = sec1_node.with_secondary(sec1, sec1_slot);
    let sec1_inputs: NodeRunInputs<FakeWorkerFactory, _, _, TestId> = NodeRunInputs {
        secondary_factory: Some(FakeWorkerFactory),
        // sec-1 never promotes — no recipe.
        promote: None,
        ..Default::default()
    };
    let sec1_handle = tokio::task::spawn_local(sec1_node.run(sec1_inputs));

    // ── Setup peer: cold-seeds the corpus + relocates onto sec-0. ───────────
    let mut pri_mesh = Mesh::new(pri_transport);
    let (pri_slot, pri_client, pri_inbox) =
        pri_mesh.register_local_role(LocalRole::Primary, PeerId::from("setup"));
    pri_mesh.publish_membership();
    let pri_config = PrimaryConfig {
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        // Two remote compute peers (sec-0, sec-1) must connect before the
        // setup peer relocates + assigns.
        num_secondaries: 2,
        ..test_primary_config()
    };
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let primary = PrimaryCoordinator::new(
        pri_config,
        pri_client,
        pri_inbox,
        demote_rx,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let (pri_node, _pri_promo_tx) = Node::new(MeshHost::on_local_set(pri_mesh));
    let pri_node = pri_node.with_primary(primary, pri_slot);
    let pri_inputs: NodeRunInputs<FakeWorkerFactory, _, _, TestId> = NodeRunInputs {
        primary_run_args: Some(PrimaryRunArgs {
            seed: SeedSource::ColdStart {
                // Unmarked cold-seed (no already-done items) — preserves
                // the pre-marker all-`Pending` behaviour this test asserts.
                binaries: binaries.into_iter().map(|b| (b, false)).collect(),
                phase_deps: HashMap::new(),
            },
            on_phase_start: Box::new(|_| {}),
            on_phase_end: Box::new(|_, _, _, _| {}),
        }),
        primary_demote_tx: Some(demote_tx),
        ..Default::default()
    };

    let setup_outcome = tokio::time::timeout(Duration::from_secs(30), pri_node.run(pri_inputs))
        .await
        .expect("setup-peer node must resolve (relocate → observer) within 30s");
    assert!(
        matches!(setup_outcome.terminal, crate::process::RunTerminal::Done),
        "setup-peer node (relocated observer) outcome: {:?}",
        setup_outcome.terminal
    );
    assert_eq!(
        setup_outcome.failed, 0,
        "no task may fail — a 'not pre-staged' rejection on the dispatch target \
         would surface as a NonRecoverable failure here"
    );

    let sec0_outcome = tokio::time::timeout(Duration::from_secs(30), sec0_handle)
        .await
        .expect("relocate-target node must finish within 30s")
        .expect("relocate-target node task join");
    assert!(
        matches!(sec0_outcome.terminal, crate::process::RunTerminal::Done),
        "relocate-target node (promoted primary) outcome: {:?}",
        sec0_outcome.terminal
    );

    let sec1_completed = tokio::time::timeout(Duration::from_secs(30), sec1_handle)
        .await
        .expect("dispatch-target secondary node must finish within 30s")
        .expect("dispatch-target secondary node task join");

    assert_eq!(
        setup_outcome.completed, num_tasks,
        "the converged ledger must reflect all {num_tasks} completions (no 'not \
         pre-staged' loss)"
    );

    (setup_outcome.completed, sec1_completed.completed)
}

/// Facet (a) — asm-dataset shape: `uses_file_based_items=false`. The corpus
/// items have RELATIVE/opaque paths with NO file backing and the dispatch
/// target has NO `src_network`. In file-based mode (the bug) the target's
/// `report_unresolvable_task` would fail every such item NonRecoverable; the
/// relocated primary must stamp `uses_file_based_items=false` so the target
/// passes `local_path` through opaquely and dispatch succeeds.
#[tokio::test(flavor = "current_thread")]
async fn relocated_primary_uses_file_based_items_false_no_restage() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Opaque (relative) identifiers — would trip the file-based guard.
            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| {
                    let mut b = make_binary(&format!("opaque_{i}"), 1);
                    b.path = std::path::PathBuf::from(format!("opaque://manifest-{i}"));
                    b
                })
                .collect();
            let (converged, sec1) = run_relocate_with_dispatch_target(RelocateStagingFacet {
                uses_file_based_items: false,
                source_pre_staged_root: None,
                source_dir: None,
                binaries,
                target_src_network: None,
            })
            .await;
            assert_eq!(converged, 3);
            // At least one task must have landed on the dispatch-target
            // secondary (round-robin across 2 secondaries) and been accepted
            // opaquely — the regression assertion.
            assert!(
                sec1 > 0,
                "the dispatch-target secondary must have accepted at least one \
                 opaque-identifier task (no 'not pre-staged' rejection)"
            );
        })
        .await;
}

/// Facet (b) — asm-tokenizer mode-2 shape: `--source-already-staged`. The
/// corpus files are bind-mounted into the dispatch target at its `src_network`
/// (we model the gateway/container split with two tmpdirs). The relocated
/// primary must stamp `pre_staged_mode=true` AND strip the gateway prefix in
/// `wire_local_path`, so the target resolves the bind-mounted file by
/// existence — no StageFile required.
#[tokio::test(flavor = "current_thread")]
async fn relocated_primary_pre_staged_mode_no_restage() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let gateway = tempfile::TempDir::new().expect("gateway tmpdir");
            let gateway_path = gateway.path().to_path_buf();
            let container = tempfile::TempDir::new().expect("container tmpdir");
            let container_path = container.path().to_path_buf();

            let names: Vec<String> = (0..4).map(|i| format!("bin_{i}")).collect();
            for name in &names {
                // Files exist only under the container (bind-mount) view.
                std::fs::write(container_path.join(name), b"x")
                    .expect("write fake binary in container view");
            }
            let binaries: Vec<TaskInfo<TestId>> = names
                .iter()
                .map(|n| {
                    let mut b = make_binary(n, 1);
                    b.path = gateway_path.join(n);
                    b
                })
                .collect();

            let (converged, sec1) = run_relocate_with_dispatch_target(RelocateStagingFacet {
                uses_file_based_items: true,
                source_pre_staged_root: Some(gateway_path.clone()),
                source_dir: None,
                binaries,
                target_src_network: Some(container_path.clone()),
            })
            .await;
            assert_eq!(converged, 4);
            assert!(
                sec1 > 0,
                "the dispatch-target secondary must resolve at least one \
                 bind-mounted task via src_network (no 'not pre-staged' rejection)"
            );
        })
        .await;
}

/// Facet (c) — mode-1 file-based, NON-pre-staged inputs that the submitter
/// would have StageFile-staged. The relocated primary RE-STAGES from scratch:
/// `maybe_auto_stage_initial` walks `source_dir` (which the recipe threaded)
/// and re-emits per-secondary StageFile records, so the dispatch target stages
/// the file and resolves it — no "not pre-staged" rejection. Without
/// `source_dir` on the promoted config, auto-stage skips and the target
/// re-requires a StageFile that never comes.
#[tokio::test(flavor = "current_thread")]
async fn relocated_primary_mode1_file_based_restages() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The shared source tree the relocated primary re-walks for the
            // content-hash + StageFile fan-out. RELATIVE binary paths so the
            // target (no src_network of its own beyond src_tmp) MUST receive a
            // StageFile to resolve them — the discriminating shape.
            let source = tempfile::TempDir::new().expect("source tmpdir");
            let source_path = source.path().to_path_buf();
            let names: Vec<String> = (0..3).map(|i| format!("file_{i}")).collect();
            for name in &names {
                std::fs::write(source_path.join(name), b"payload")
                    .expect("write source file");
            }
            let binaries: Vec<TaskInfo<TestId>> = names
                .iter()
                .map(|n| {
                    let mut b = make_binary(n, 7);
                    // Relative path: resolved against source_dir on the primary
                    // for the content-hash; the target needs the StageFile to
                    // place it.
                    b.path = std::path::PathBuf::from(n);
                    b
                })
                .collect();

            let (converged, sec1) = run_relocate_with_dispatch_target(RelocateStagingFacet {
                uses_file_based_items: true,
                source_pre_staged_root: None,
                source_dir: Some(source_path.clone()),
                binaries,
                // Mode-1: the shared drive IS visible to the target as its
                // `src_network` (the relocated primary's `source_dir` points at
                // the same tree on a shared FS). The relocated primary's
                // `maybe_auto_stage_initial` re-emits StageFile records with
                // relative `src_path`s; the target resolves them via
                // `src_network/<rel>`, copies into src_tmp, and registers the
                // path. WITHOUT the threaded `source_dir`, auto-stage skips, no
                // StageFile is emitted, and the target's `src_network.is_some()`
                // makes the unresolved relative path fail NonRecoverable — the
                // bug this test pins.
                target_src_network: Some(source_path.clone()),
            })
            .await;
            assert_eq!(converged, 3);
            assert!(
                sec1 > 0,
                "the dispatch-target secondary must stage + resolve at least one \
                 re-staged file (the relocated primary re-emitted StageFile \
                 records from its threaded source_dir)"
            );
        })
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Resume-detection: `maybe_auto_stage_initial` must NOT re-stage on a
// FAILOVER-PROMOTION resume (a populated CRDT — tasks already progressed
// past Pending). The corpus was staged once by the run's original primary;
// re-walking it on resume re-copies every binary needlessly. A genuinely
// FRESH promoted destination (all-Pending CRDT) still stages.
// ─────────────────────────────────────────────────────────────────────────

/// CRDT seed shape for [`build_staging_primary`] — which resume signal (if
/// any) the seeded ledger carries when `maybe_auto_stage_initial` runs.
enum StagingSeedShape {
    /// All tasks `Pending` — a first-ever promoted destination.
    Fresh,
    /// One task driven to `InFlight` — a genuine failover-resume.
    Resumed,
    /// All stageable tasks `Pending` PLUS one extra task that the SEED
    /// itself classified terminal `InvalidTask` (the #2 missing-dep
    /// ingest classification). A seed-time terminal is NOT dispatch
    /// progress — the corpus has never been staged anywhere — so the
    /// resume detector must keep the staging gate OPEN.
    FreshWithSeedTimeInvalid,
}

/// Seed `n` file-based binaries into a fresh primary's CRDT (all Pending),
/// hydrate, and return the primary + the source dir kept alive. `shape`
/// selects the resume signal the ledger carries (see [`StagingSeedShape`]).
/// The config has `source_dir` set + `uses_file_based_items` so the staging
/// gate is OTHERWISE open — isolating the resume gate as the sole
/// discriminator.
fn build_staging_primary(shape: StagingSeedShape) -> (TestPrimaryForStaging, tempfile::TempDir) {
    let source = tempfile::TempDir::new().expect("source tmpdir");
    let names: Vec<String> = (0..3).map(|i| format!("file_{i}")).collect();
    for name in &names {
        std::fs::write(source.path().join(name), b"payload").expect("write source file");
    }
    let config = PrimaryConfig {
        uses_file_based_items: true,
        source_dir: Some(source.path().to_path_buf()),
        ..PrimaryConfig::default()
    };
    let (transport, _ends) = setup_test(1);
    // The mesh keepalive is dropped at function exit: these tests call the
    // sync `maybe_auto_stage_initial` (no wire sends), so a torn-down mesh
    // is harmless.
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        // A capacity record so hydrate reconstructs `self.secondaries`
        // (`maybe_auto_stage_initial` reads `self.secondaries.keys()`).
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: 8 * 1024 * 1024 * 1024,
            }],
        });
        for (i, name) in names.iter().enumerate() {
            let mut b = make_binary(name, 7);
            b.path = std::path::PathBuf::from(name);
            let hash = crate::primary::wire::compute_task_hash(&b);
            cs.apply(ClusterMutation::TaskAdded { hash: hash.clone(), task: b });
            // Drive the first task to InFlight when modelling a RESUME (a
            // task has progressed past Pending — the populated-CRDT signal).
            if matches!(shape, StagingSeedShape::Resumed) && i == 0 {
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash,
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
        }
        // Seed-time terminal: one EXTRA task the ingest classification
        // itself marked `InvalidTask` (a missing dep — the #2 class).
        // Mirrors `originate_cold_seed`'s invalid_deps routing: seeded
        // `Pending` via `TaskAdded`, then transitioned terminal in the
        // same batch, BEFORE any dispatch (and before any staging)
        // happened anywhere.
        if matches!(shape, StagingSeedShape::FreshWithSeedTimeInvalid) {
            let invalid = make_binary("seed_time_invalid", 7);
            let hash = crate::primary::wire::compute_task_hash(&invalid);
            cs.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: invalid,
            });
            cs.apply(ClusterMutation::TaskFailed {
                hash,
                kind: dynrunner_core::ErrorType::InvalidTask {
                    reason: dynrunner_core::BoundedString::from(
                        "missing dep (phase=P, task_id=nope)".to_string(),
                    ),
                },
                error: "missing dep (phase=P, task_id=nope)".into(),
                version: Default::default(),
                attempt: 0,
            });
        }
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    (TestPrimaryForStaging(primary), source)
}

/// Newtype so the `mesh` keepalive guard stays bound for the primary's
/// lifetime even though the test only touches the coordinator.
struct TestPrimaryForStaging(
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
);

#[tokio::test(flavor = "current_thread")]
async fn auto_stage_skipped_on_populated_crdt_resume() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // RESUME: one task already InFlight ⇒ the CRDT is populated.
            let (mut p, _src) = build_staging_primary(StagingSeedShape::Resumed);
            p.0.maybe_auto_stage_initial()
                .expect("auto-stage call ok");
            assert!(
                p.0.pending_stage_files.is_empty(),
                "a populated-CRDT (failover-resume) primary must NOT re-stage \
                 — the corpus was staged by the original primary"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn auto_stage_runs_on_fresh_all_pending_promoted_destination() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // FRESH: every task Pending ⇒ a first-ever promoted destination,
            // NOT a resume. Staging must proceed (the gate stays open).
            let (mut p, _src) = build_staging_primary(StagingSeedShape::Fresh);
            p.0.maybe_auto_stage_initial()
                .expect("auto-stage call ok");
            assert!(
                !p.0.pending_stage_files.is_empty(),
                "a fresh all-Pending promoted destination MUST stage the corpus \
                 (resume detection does not over-suppress first-ever staging)"
            );
        })
        .await;
}

/// A SEED-TIME terminal is not dispatch progress. The cold seed classifies a
/// missing-dep task terminal `InvalidTask` BEFORE any task was ever
/// dispatched or staged anywhere; a promoted destination hydrating that
/// ledger is still a FIRST-EVER primary for the run and MUST stage the
/// corpus. Pre-fix, the resume detector summed `invalid_task` into its
/// "progressed" signal, mis-read the fresh relocate as a failover-resume,
/// skipped the staging walk entirely, and the very first dispatched task
/// failed NonRecoverable "not pre-staged at <path>" (the
/// distributed-local-subprocess e2e repro: the seed invalidated the consume
/// tasks, the promoted primary skipped staging, produce-0 died).
#[tokio::test(flavor = "current_thread")]
async fn auto_stage_runs_despite_seed_time_invalid_tasks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut p, _src) =
                build_staging_primary(StagingSeedShape::FreshWithSeedTimeInvalid);
            p.0.maybe_auto_stage_initial().expect("auto-stage call ok");
            assert!(
                !p.0.pending_stage_files.is_empty(),
                "seed-time InvalidTask entries must NOT suppress first-ever \
                 staging — only dispatch-derived progress (InFlight / Completed \
                 / Failed / Unfulfillable) marks a resume"
            );
        })
        .await;
}
