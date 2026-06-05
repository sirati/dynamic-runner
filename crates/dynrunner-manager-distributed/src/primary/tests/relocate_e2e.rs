//! Manager-layer END-TO-END proof of the full bootstrap hand-off.
//!
//! Unlike [`relocate_observe`] (which drives `relocate_primary_to` /
//! `run_as_observer` in isolation) and [`select_bootstrap`] (which unit-
//! tests the selection policy), these tests stand up a RUNNING channel
//! mesh — a submitter primary + ≥2 PRIMARY-CAPABLE secondaries — and let
//! the submitter's own `run()` bootstrap fork relocate authority onto the
//! lowest-id capable peer. That peer's on-demand-built `PrimaryCoordinator`
//! ACTUALLY dispatches the residual workload (the tasks the submitter's
//! one-per-worker initial assignment didn't place) over a
//! `ChannelPeerTransport` + its own-secondary loopback, broadcasts
//! `RunComplete`, and the submitter-observer exits on it.
//!
//! This is the first manager-layer exercise of the WHOLE hand-off path the
//! original 590s hang was hiding: a Transferred `PrimaryChanged` routed
//! through the chosen peer's setup FSM, the on-demand coordinator build,
//! and the submitter's observer tail — all without a real sleep beyond the
//! bounded settle windows the coordinators already use.

use super::*;

/// Per-peer inbound senders keyed by peer id — the shared fan-in table
/// every peer copies its `outgoing` from.
type SenderTable = HashMap<String, tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>>;
/// Per-peer inbound receivers keyed by peer id — each peer drains its own.
type ReceiverTable = HashMap<String, tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>>;

/// Allocate one inbound channel per peer id; returns a map id→sender (the
/// shared fan-in table every peer copies its `outgoing` from) plus each
/// peer's own receiver, keyed by id. The all-to-all `outgoing` for a peer
/// is simply the sender table MINUS that peer's own entry.
fn mesh_channels(ids: &[&str]) -> (SenderTable, ReceiverTable) {
    let mut senders = HashMap::new();
    let mut receivers = HashMap::new();
    for id in ids {
        let (tx, rx) = tokio_mpsc::unbounded_channel();
        senders.insert((*id).to_string(), tx);
        receivers.insert((*id).to_string(), rx);
    }
    (senders, receivers)
}

/// The `outgoing` table a peer holds: every OTHER peer's inbound sender.
fn outgoing_for(self_id: &str, senders: &SenderTable) -> SenderTable {
    senders
        .iter()
        .filter(|(id, _)| id.as_str() != self_id)
        .map(|(id, tx)| (id.clone(), tx.clone()))
        .collect()
}

fn big_ram() -> dynrunner_core::ResourceMap {
    dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        8 * 1024 * 1024 * 1024u64,
    )])
}

/// `PrimaryConfig` for the submitter in a relocation test: short timeouts
/// so the bootstrap reaches the hand-off fork fast.
fn submitter_config(num_secondaries: u32) -> PrimaryConfig {
    PrimaryConfig {
        num_secondaries,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        ..test_primary_config()
    }
}

/// HEADLINE: submitter + 2 primary-capable secondaries over a channel mesh.
/// The submitter bootstraps, then its `run()` fork relocates FULL authority
/// to the lowest-id capable peer (`sec-0`), and becomes an observer. The
/// chosen peer's on-demand `PrimaryCoordinator` dispatches the residual
/// workload and finalizes the run; the submitter-observer exits on the
/// `RunComplete` it broadcasts.
///
/// Asserts the proof of a REAL hand-off, not a totals reconcile:
///   (a) the CHOSEN peer's own co-located primary credited ALL tasks to its
///       OWN replicated ledger (`ActivatedPrimaryResult` == total) — per-
///       host primary attribution, captured the instant `run_activated`
///       returned;
///   (b) the submitter ran the OBSERVER tail: `run()` returned `Ok`, it
///       NEVER pinned itself as local primary (`primary_id == None` — the
///       relocate path never calls `activate_local_primary`), and its
///       replicated-ledger `completed_count()` == total (it observed every
///       terminal off the CRDT, not a pool it dispatched from);
///   (c) the chosen peer holds authority: `current_primary()` is the
///       lowest-id capable peer (`sec-0`), and the epoch advanced past the
///       bootstrap pin's 0 — the submitter's Transferred announce was epoch
///       1 (unit-pinned in `relocate_observe`), and the chosen peer's own
///       `activate_local_primary` then re-asserts authority via an Election
///       self-announce at the next epoch, so the OBSERVED final epoch is ≥ 1;
///   (d) all tasks completed — the per-secondary own-work counts partition
///       the full set across REAL workers (proving the chosen primary truly
///       dispatched to the loopback + the wire, not that totals merely
///       agree).
#[tokio::test(flavor = "current_thread")]
async fn e2e_relocation_chosen_peer_dispatches_and_submitter_observes() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const NUM_TASKS: usize = 10;
            // 2 secondaries × 2 workers = 4 initial-assignment slots, so the
            // submitter's one-per-worker initial assignment can place at
            // most 4 of the 10 tasks; the residual ≥6 MUST be dispatched by
            // the chosen peer's on-demand primary AFTER relocation. That is
            // what makes (a)/(d) a genuine dispatch proof.
            let (senders, mut receivers) = mesh_channels(&["primary", "sec-0", "sec-1"]);

            // Short keepalive so the two secondaries recognise each other as
            // alive fast (full mesh → MeshReady → the submitter releases its
            // relocation fork promptly), and a per-task latency so the
            // workload stays in-flight ACROSS the relocation window — the
            // residual tasks are then dispatched by the chosen peer's
            // on-demand primary, not raced-to-completion by the submitter.
            let keepalive = Duration::from_millis(10);
            // Every task path is `/tmp/bin_N`; the substring `"bin_"` matches
            // all, giving each a uniform bounded latency.
            let slow = || vec![("bin_".to_string(), Duration::from_millis(80))];

            // Primary-capable secondaries: each gets its own inbound rx + an
            // outgoing table to every other peer (incl. the submitter keyed
            // "primary"). They join can_be_primary=true and arm the channel-
            // mesh on-demand activator.
            let (sec0_handle, sec0_primary_result) = spawn_real_secondary_primary_capable(
                "sec-0".into(),
                2,
                big_ram(),
                keepalive,
                slow(),
                receivers.remove("sec-0").unwrap(),
                outgoing_for("sec-0", &senders),
            );
            let (sec1_handle, sec1_primary_result) = spawn_real_secondary_primary_capable(
                "sec-1".into(),
                2,
                big_ram(),
                keepalive,
                slow(),
                receivers.remove("sec-1").unwrap(),
                outgoing_for("sec-1", &senders),
            );

            // Submitter transport: outgoing to both secondaries, inbound is
            // its own mesh receiver.
            let submitter_inbound_rx = receivers.remove("primary").unwrap();
            let transport = ChannelPeerTransport::from_raw_channels(
                "primary".into(),
                outgoing_for("primary", &senders),
                submitter_inbound_rx,
            );
            // Drop the unused fan-in senders so no stray sender keeps a
            // receiver alive past its peer's exit.
            drop(senders);

            let mut submitter = PrimaryCoordinator::new(
                submitter_config(2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_TASKS)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                submitter
                    .run(binaries, deps, ops, ope)
                    .await
                    .expect("submitter run (bootstrap → relocate → observer) must return Ok");
            }

            // (b) The submitter ran the OBSERVER tail: the relocate path
            // took the `run_as_observer` branch (NOT the operational loop)
            // and never called `activate_local_primary`, so it never pinned
            // itself as the local primary — yet its replicated ledger
            // observed every terminal off the CRDT broadcasts.
            assert_eq!(
                submitter.primary_id, None,
                "the submitter must NOT have pinned itself as local primary — \
                 relocation runs the observer tail, never activate_local_primary"
            );
            assert_eq!(
                submitter.completed_count(),
                NUM_TASKS,
                "the submitter-observer's replicated ledger must observe every completion"
            );
            assert_eq!(submitter.failed_count(), 0, "no task may fail");

            // (c) The chosen peer holds authority. The bootstrap pin was not
            // an announce (epoch 0); the submitter's Transferred announce
            // took it to 1, then the chosen peer's own activate_local_primary
            // re-asserts via an Election self-announce at the next epoch — so
            // the observed final epoch advanced past 0 and `current_primary`
            // is the chosen lowest-id peer.
            assert_eq!(
                submitter.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the lowest-id capable peer must be the chosen primary"
            );
            assert!(
                submitter.cluster_state_for_test().primary_epoch() >= 1,
                "the epoch must advance past the bootstrap pin (0) once the chosen \
                 peer is named + asserts its own authority"
            );

            // Drop the submitter so its mesh senders close, letting the
            // secondaries' own `run()` loops wind down once the run is over.
            drop(submitter);

            let sec0_own = sec0_handle.await.unwrap();
            let sec1_own = sec1_handle.await.unwrap();

            // (a) Per-host primary attribution: the CHOSEN peer (sec-0) built
            // a co-located primary on demand and its OWN replicated ledger
            // credited ALL tasks. sec-1 was never named primary, so its
            // activator never fired.
            assert_eq!(
                sec0_primary_result.get(),
                Some(NUM_TASKS),
                "the CHOSEN peer's on-demand co-located primary must have credited \
                 every task to its OWN ledger (per-host dispatch proof, not a totals \
                 reconcile)"
            );
            assert_eq!(
                sec1_primary_result.get(),
                None,
                "a non-chosen primary-capable secondary must never build a primary"
            );

            // (d) Real dispatch to real workers: the per-secondary own-work
            // counts partition the full task set across the loopback (sec-0)
            // and the wire (sec-1). A non-zero sec-1 count proves the chosen
            // primary dispatched OVER THE MESH to a remote secondary, not
            // only to its own loopback.
            assert_eq!(
                sec0_own + sec1_own,
                NUM_TASKS,
                "every task must run on exactly one secondary's worker; own-work \
                 counts sec0={sec0_own}, sec1={sec1_own} must partition all tasks"
            );
            assert!(
                sec1_own > 0,
                "the chosen primary must have dispatched at least one task OVER THE \
                 MESH to the remote secondary (sec-1), not only to its own loopback"
            );
        })
        .await;
}

/// NO-CAPABLE-PEER: submitter + 2 secondaries that join `can_be_primary =
/// false` (the `disable_peer_overlay` shape) ⇒ `select_bootstrap_primary`
/// returns `None` ⇒ the submitter STAYS the full primary, dispatches, and
/// completes the run. No relocation, NO hang. (The `bare`
/// `spawn_real_secondary` joins `can_be_primary = false`.)
#[tokio::test(flavor = "current_thread")]
async fn e2e_no_capable_peer_submitter_stays_primary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const NUM_TASKS: usize = 10;
            let max_res = big_ram();
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();

            for i in 0..2u32 {
                let secondary_id = format!("sec-{i}");
                // Bare secondary: can_be_primary = false (no activator).
                let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                    spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());
                outgoing.insert(secondary_id, pri_to_sec_tx);
                sec_handles.push(handle);

                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(incoming_tx);

            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let mut submitter = PrimaryCoordinator::new(
                submitter_config(2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_TASKS)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                submitter
                    .run(binaries, deps, ops, ope)
                    .await
                    .expect("submitter run must complete as the local primary, no hang");
            }

            // The submitter STAYED primary: it took the `None` fork →
            // `activate_local_primary` (which pins `primary_id = self`) →
            // the operational loop, NOT the observer tail. It is its own
            // current_primary.
            assert_eq!(
                submitter.primary_id,
                Some("primary".to_string()),
                "with no capable peer the submitter must pin itself as local primary"
            );
            assert_eq!(
                submitter.cluster_state_for_test().current_primary(),
                Some("primary"),
                "with no hand-off target the submitter must stay current_primary"
            );
            assert_eq!(
                submitter.completed_count(),
                NUM_TASKS,
                "all tasks must complete"
            );
            assert_eq!(submitter.failed_count(), 0, "no task may fail");

            drop(submitter);

            let mut total_own = 0usize;
            for handle in sec_handles {
                total_own += handle.await.unwrap();
            }
            assert_eq!(
                total_own, NUM_TASKS,
                "the submitter-primary must have dispatched every task to the \
                 secondaries' workers"
            );
        })
        .await;
}

/// RELOCATION single-emit guard for the "initial setup done" milestone.
///
/// The submitter emits "initial setup done" once in `run_pipeline`, placed
/// BEFORE the bootstrap hand-off fork. When authority RELOCATES to a chosen
/// peer, that peer's on-demand primary enters via `run_activated` /
/// `run_activated_pipeline`, which bypasses `run_pipeline` (and therefore
/// the emit) entirely — it inherits the formed mesh and resumes from the
/// restored snapshot. So even though TWO `PrimaryCoordinator`s run during a
/// relocating run (the submitter's, then the chosen peer's on-demand one),
/// "initial setup done" must be captured EXACTLY ONCE.
///
/// Path this drives: the FULL bootstrap → relocate hand-off (the same
/// `Some(chosen)` → `relocate_primary_to` → `run_as_observer` fork the
/// headline e2e test exercises). The submitter relocates to the lowest-id
/// capable peer (`sec-0`), whose on-demand primary dispatches the residual
/// workload via `run_activated_pipeline` — the path that must NOT re-emit.
///
/// Why the assertion is REAL (not a tautology): every coordinator in this
/// test — the submitter, both secondaries, and the chosen peer's on-demand
/// activated primary — runs on the SAME `current_thread` `LocalSet`, under
/// a `set_default` thread-local `ImportantCapture` subscriber. So if the
/// relocated primary's `run_activated_pipeline` ever emitted "initial setup
/// done" (e.g. if the emit were moved into the shared
/// `run_operational_and_finalize` tail, or duplicated onto the activation
/// path), the capture WOULD record a second occurrence and `count == 1`
/// would fail. The assertion fires off the activated path actually running:
/// the headline-test invariants (relocation happened, the chosen peer's own
/// ledger credited every task) are re-checked here so a regression that
/// silently skips relocation can't make the count==1 pass vacuously.
#[tokio::test(flavor = "current_thread")]
async fn initial_setup_done_emitted_once_across_relocation() {
    use crate::test_capture::{ImportantCapture, important_only};
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Capture only importance-target events, scoped to this test's
            // thread for the lifetime of the run. `set_default` (not
            // `with_default`) so the subscriber is held across the `.await`;
            // `current_thread` + `LocalSet` keep every spawned coordinator
            // (submitter, secondaries, AND the chosen peer's on-demand
            // activated primary) on this thread, so an "initial setup done"
            // emit from ANY of them is reached. See `phase_ordering.rs` for
            // the same discipline.
            let capture = ImportantCapture::default();
            let subscriber =
                Registry::default().with(capture.clone().with_filter(important_only()));
            let _guard = tracing::subscriber::set_default(subscriber);

            const NUM_TASKS: usize = 10;
            // 2 secondaries × 2 workers = 4 initial-assignment slots, so the
            // submitter places at most 4 of 10 tasks; the residual ≥6 MUST
            // be dispatched by the chosen peer's on-demand primary AFTER
            // relocation — that on-demand primary is the `run_activated`
            // path that must NOT re-emit "initial setup done".
            let (senders, mut receivers) = mesh_channels(&["primary", "sec-0", "sec-1"]);

            let keepalive = Duration::from_millis(10);
            let slow = || vec![("bin_".to_string(), Duration::from_millis(80))];

            let (sec0_handle, sec0_primary_result) = spawn_real_secondary_primary_capable(
                "sec-0".into(),
                2,
                big_ram(),
                keepalive,
                slow(),
                receivers.remove("sec-0").unwrap(),
                outgoing_for("sec-0", &senders),
            );
            let (sec1_handle, sec1_primary_result) = spawn_real_secondary_primary_capable(
                "sec-1".into(),
                2,
                big_ram(),
                keepalive,
                slow(),
                receivers.remove("sec-1").unwrap(),
                outgoing_for("sec-1", &senders),
            );

            let submitter_inbound_rx = receivers.remove("primary").unwrap();
            let transport = ChannelPeerTransport::from_raw_channels(
                "primary".into(),
                outgoing_for("primary", &senders),
                submitter_inbound_rx,
            );
            drop(senders);

            let mut submitter = PrimaryCoordinator::new(
                submitter_config(2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_TASKS)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                submitter
                    .run(binaries, deps, ops, ope)
                    .await
                    .expect("submitter run (bootstrap → relocate → observer) must return Ok");
            }

            // Re-assert that a REAL relocation happened so the single-emit
            // count below can't pass vacuously: a regression that skipped
            // relocation (submitter stays primary) would emit exactly once
            // too, but would NOT exercise the activated-path bypass this
            // test guards. The submitter must have observed (never pinned
            // itself local) and the chosen peer must hold authority.
            assert_eq!(
                submitter.primary_id, None,
                "the submitter must have relocated (observer tail), not stayed local primary"
            );
            assert_eq!(
                submitter.completed_count(),
                NUM_TASKS,
                "the submitter-observer's replicated ledger must observe every completion"
            );
            assert_eq!(
                submitter.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the lowest-id capable peer must be the chosen primary"
            );

            drop(submitter);

            let _sec0_own = sec0_handle.await.unwrap();
            let _sec1_own = sec1_handle.await.unwrap();

            // The chosen peer's on-demand primary actually ran the activated
            // path (credited every task to its own ledger) — proving the
            // `run_activated_pipeline` bypass was truly exercised, so the
            // count==1 assertion below is a real negative control on it.
            assert_eq!(
                sec0_primary_result.get(),
                Some(NUM_TASKS),
                "the CHOSEN peer's on-demand primary must have run the activated path \
                 (credited every task to its OWN ledger)"
            );
            assert_eq!(
                sec1_primary_result.get(),
                None,
                "a non-chosen primary-capable secondary must never build a primary"
            );

            // The invariant: across the WHOLE relocating run — two
            // coordinators, submitter + chosen-peer's on-demand primary —
            // "initial setup done" was emitted EXACTLY ONCE. The submitter
            // emits it before relocating; the relocated primary
            // (`run_activated_pipeline`) must NOT emit a second.
            let setup_done_count = capture
                .messages()
                .iter()
                .filter(|m| m.contains("initial setup done"))
                .count();
            assert_eq!(
                setup_done_count,
                1,
                "'initial setup done' must be emitted EXACTLY ONCE across a relocating \
                 run — the submitter emits it before hand-off; the relocated primary's \
                 `run_activated_pipeline` must not re-emit; got {:?}",
                capture.messages()
            );
        })
        .await;
}
