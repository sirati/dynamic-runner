//! Manager-layer END-TO-END proof of the full bootstrap hand-off.
//!
//! Unlike [`relocate_observe`] (which drives `relocate_primary_to` in
//! isolation) and [`select_bootstrap`] (which unit-tests the selection
//! policy), these tests stand up a RUNNING channel
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

use crate::primary::coordinator::PrimaryRunOutcome;

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
            assert_eq!(submitter.completed_count(), NUM_TASKS, "all tasks must complete");
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

/// HEADLINE: submitter + 2 primary-capable secondaries over a channel mesh.
/// The submitter bootstraps, then its `run_consuming()` bootstrap fork
/// relocates FULL authority to the lowest-id capable peer (`sec-0`), DROPS
/// its own `PrimaryCoordinator` BY VALUE into the standalone observer's
/// handoff, and runs the observer tail. The chosen peer's on-demand
/// `PrimaryCoordinator` dispatches the residual workload and finalizes the
/// run; the submitter-observer exits on the `RunComplete` it broadcasts.
///
/// Asserts the proof of a REAL handoff, not a totals reconcile:
///   (a) the CHOSEN peer's own co-located primary credited ALL tasks to its
///       OWN replicated ledger (`ActivatedPrimaryResult` == total) — per-
///       host primary attribution, captured the instant `run_activated`
///       returned;
///   (b) the submitter's `PrimaryCoordinator` was CONSUMED by value into the
///       observer: `run_consuming` takes `submitter` by move (the binding is
///       gone after the call — a compile-checked move, not a `mem::take`
///       hollow shell), and the relocated outcome's accounting is re-sourced
///       from the OBSERVER's converged `cluster_state`
///       (`PrimaryRunOutcome::Relocated { completed, .. }` == total), proving
///       the observer took over the moved-in ledger;
///   (c) the run exited cleanly on the chosen peer's `RunComplete`:
///       `result == Ok(())` (the observer's `Done` terminal), `failed == 0`,
///       `stranded == 0` (an observer never dispatches);
///   (d) the transport + peer set rode across by value (no re-dial): the
///       observer drove anti-entropy / snapshot recovery over the SAME mesh
///       the submitter held, and real dispatch reached BOTH the chosen
///       peer's loopback AND the remote secondary over the wire — the
///       per-secondary own-work counts partition the full task set with a
///       non-zero remote share.
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

            let submitter = PrimaryCoordinator::new(
                submitter_config(2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_TASKS)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();

            // `run_consuming` takes `submitter` BY VALUE — after this call
            // the binding is gone (a compile-checked move into the observer
            // handoff, NOT a `mem::take` hollow shell). The post-run
            // accounting travels back through the returned outcome.
            let (deps, ops, ope) = noop_phase_args();
            let outcome = submitter
                .run_consuming(binaries, deps, ops, ope)
                .await
                .expect("submitter run (bootstrap → relocate → observer) must return Ok");

            // (b)/(c) The submitter RELOCATED: its `PrimaryCoordinator` was
            // consumed into the observer handoff, the observer drove the
            // moved-in ledger to terminal, and the accounting is re-sourced
            // from the observer's converged `cluster_state`.
            let PrimaryRunOutcome::Relocated {
                result,
                completed,
                failed,
                stranded,
            } = outcome
            else {
                panic!(
                    "the submitter must have RELOCATED (observer tail), not stayed local: {outcome:?}"
                );
            };
            result.expect("the observer must exit cleanly on the chosen peer's RunComplete (Done)");
            assert_eq!(
                completed, NUM_TASKS,
                "the relocated outcome's completed count must be re-sourced from the \
                 observer's converged ledger (every completion observed off the CRDT)"
            );
            assert_eq!(failed, 0, "no task may fail");
            assert_eq!(
                stranded, 0,
                "an observer never dispatches — it can strand nothing"
            );

            // Drop the secondary handles' join so the secondaries' own
            // `run()` loops wind down once the run is over (the submitter's
            // transport was already dropped inside `run_consuming` when the
            // observer's single-teardown ran, closing its mesh senders).
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

            // (d) Real dispatch to real workers over the moved-across mesh:
            // the per-secondary own-work counts partition the full task set
            // across the loopback (sec-0) and the wire (sec-1). A non-zero
            // sec-1 count proves the chosen primary dispatched OVER THE MESH
            // (the peer set the observer inherited by value, no re-dial) to a
            // remote secondary, not only to its own loopback.
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

/// RELOCATION single-emit guard for the "initial setup done" milestone,
/// across the by-value handoff.
///
/// The submitter emits "initial setup done" once in `run_pipeline`, placed
/// BEFORE the bootstrap hand-off fork. When authority RELOCATES to a chosen
/// peer, that peer's on-demand primary enters via `run_activated` /
/// `run_activated_pipeline`, which bypasses `run_pipeline` (and therefore
/// the emit) entirely. So even though TWO `PrimaryCoordinator`s run during a
/// relocating run (the submitter's, then the chosen peer's on-demand one) —
/// AND the submitter then becomes a standalone observer that narrates the
/// run — "initial setup done" must be captured EXACTLY ONCE.
///
/// Why the assertion is REAL (not a tautology): every coordinator in this
/// test — the submitter, both secondaries, the chosen peer's on-demand
/// activated primary, and the submitter's relocated observer — runs on the
/// SAME `current_thread` `LocalSet`, under a `set_default` thread-local
/// `ImportantCapture` subscriber. So a second emit from ANY of them (e.g. if
/// the emit were duplicated onto the activation path, or the observer
/// re-emitted it) WOULD be recorded and `count == 1` would fail. The headline
/// invariants (relocation happened, the chosen peer's own ledger credited
/// every task) are re-checked so a regression that silently skips relocation
/// can't make the count==1 pass vacuously.
#[tokio::test(flavor = "current_thread")]
async fn initial_setup_done_emitted_once_across_relocation() {
    use crate::test_capture::{ImportantCapture, important_only};
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let capture = ImportantCapture::default();
            let subscriber =
                Registry::default().with(capture.clone().with_filter(important_only()));
            let _guard = tracing::subscriber::set_default(subscriber);

            const NUM_TASKS: usize = 10;
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
            let (sec1_handle, _sec1_primary_result) = spawn_real_secondary_primary_capable(
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

            let submitter = PrimaryCoordinator::new(
                submitter_config(2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_TASKS)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();

            let (deps, ops, ope) = noop_phase_args();
            let outcome = submitter
                .run_consuming(binaries, deps, ops, ope)
                .await
                .expect("submitter run (bootstrap → relocate → observer) must return Ok");

            // Re-assert that a REAL relocation happened so the single-emit
            // count below can't pass vacuously: a regression that skipped
            // relocation (submitter stays primary) would emit exactly once
            // too, but would NOT exercise the activated-path bypass + the
            // observer tail this test guards.
            let PrimaryRunOutcome::Relocated {
                result, completed, ..
            } = outcome
            else {
                panic!("the submitter must have relocated (observer tail): {outcome:?}");
            };
            result.expect("the observer must exit cleanly (Done)");
            assert_eq!(
                completed, NUM_TASKS,
                "the relocated observer's converged ledger must observe every completion"
            );

            let _ = sec0_handle.await;
            let _ = sec1_handle.await;

            // The chosen peer's on-demand co-located primary credited every
            // task to its OWN ledger — the cell is set the instant
            // `run_activated` returns, which is only reached once the handle
            // above is joined.
            assert_eq!(
                sec0_primary_result.get(),
                Some(NUM_TASKS),
                "the chosen peer must hold authority + credit every task to its own ledger"
            );

            // The load-bearing assertion: "initial setup done" was captured
            // EXACTLY ONCE across the entire relocating run (the submitter's
            // single emit before the fork; never re-emitted by the activated
            // primary's `run_activated` path nor by the observer tail).
            let count = capture
                .messages()
                .iter()
                .filter(|m| m.contains("initial setup done"))
                .count();
            assert_eq!(
                count, 1,
                "\"initial setup done\" must be emitted EXACTLY ONCE across a relocating \
                 run (got {count}); the activated primary's run_activated path and the \
                 observer tail must NOT re-emit it"
            );
        })
        .await;
}
