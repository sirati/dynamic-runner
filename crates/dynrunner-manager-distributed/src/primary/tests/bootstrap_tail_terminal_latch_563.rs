//! #563 Seam 2 — `bootstrap_tail_dispatch` adopts a replicated run-terminal
//! verdict BEFORE entering the operational loop.
//!
//! Production trace (asm-tokenizer 2026-06-15): a peer secondary won a
//! failover election against a dying primary that had ALREADY authored
//! `RunAborted`. The new primary's `bootstrap_tail_dispatch` ran
//! `activate_local_primary` (broadcasting a fresh `PrimaryChanged`) then
//! entered `operational_loop`, which re-assigned every still-Pending task
//! in the inherited ledger — the observed "task dependency_graph assigned
//! to secondary-1-0" RIGHT after the failover. The existing
//! verdict-adoption gate inside `finalize_terminal_accounting`
//! (`coordinator.rs::run_aborted()` check, ~line 5364) fires only AFTER the
//! operational loop returns, so it cannot prevent the pre-finalize
//! re-dispatch.
//!
//! Seam 2 hoists the gate to the entry of `bootstrap_tail_dispatch`:
//!
//!   if Some(reason) = run_aborted()  -> Err(AbortedByClusterVerdict)
//!   if run_complete()                -> Ok(()) (no work, clean exit)
//!   otherwise                        -> activate_local_primary + run loop
//!
//! Tests:
//!   - `RunAborted` already latched → `Err(AbortedByClusterVerdict{reason})`
//!     WITHOUT activating + WITHOUT broadcasting a new PrimaryChanged
//!     (because `activate_local_primary` never runs);
//!   - `RunComplete` already latched → `Ok(())` clean short-circuit; and
//!   - NEGATIVE control with no latch → the existing happy-path
//!     `bootstrap_tail_activates_local_primary` shape still runs (the
//!     companion test in `setup_promote.rs` covers this; we cross-link).

use super::*;

const ABORT_REASON: &str = "runtime spawn_tasks rejected 46497 task(s): \
                            [duplicate task identity dependency_graph, ...]";

/// SEAM 2 — a promoted primary whose inherited ledger ALREADY carries the
/// `RunAborted` latch must adopt the verdict at the bootstrap-tail entry
/// and exit `Err(AbortedByClusterVerdict)` without running
/// `activate_local_primary` and without entering the operational loop.
/// Replays the asm-tokenizer 2026-06-15 cascade where the new primary
/// inherited the latch (via snapshot pull / anti-entropy convergence) and
/// the bootstrap-tail was the LAST gate able to prevent the re-dispatch.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_tail_adopts_run_aborted_without_activating_or_dispatching() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The promoted primary's CRDT mirror has the cluster's RunAborted
            // verdict latched — either delivered via the dying primary's
            // broadcast before promotion, or healed in via a snapshot/anti-
            // entropy pull immediately after promotion. The bootstrap-tail
            // gate consults the same first-writer-wins latch the
            // finalize-tail gate consults; the SAME `cluster_state.run_aborted()`
            // accessor.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::RunAborted {
                    reason: ABORT_REASON.into(),
                    counts: Default::default(),
                });

            let pre_primary_id = primary.primary_id.clone();
            let pre_current_primary = primary
                .cluster_state_for_test()
                .current_primary()
                .map(str::to_owned);
            let pre_primary_epoch = primary.cluster_state_for_test().primary_epoch();

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(),
            )
            .await;

            match exit {
                Ok(Err(crate::primary::RunError::AbortedByClusterVerdict { reason })) => {
                    assert_eq!(
                        reason, ABORT_REASON,
                        "the adopted verdict must carry the cluster's verbatim reason \
                         (first-writer-wins, the dying primary's authored text)",
                    );
                }
                other => panic!(
                    "bootstrap_tail_dispatch must adopt the latched RunAborted as \
                     Err(AbortedByClusterVerdict); got {other:?}"
                ),
            }

            // activate_local_primary was NOT called: primary_id is unchanged.
            assert_eq!(
                primary.primary_id, pre_primary_id,
                "the verdict-adoption gate must short-circuit BEFORE activate_local_primary; \
                 primary_id must be unchanged ({pre_primary_id:?} → {:?})",
                primary.primary_id,
            );
            // The CRDT current_primary / primary_epoch were NOT bumped: no fresh
            // PrimaryChanged was originated (the load-bearing observable — a
            // PrimaryChanged from a promoted-then-aborted node is the exact misleading
            // signal #563 set out to silence).
            assert_eq!(
                primary
                    .cluster_state_for_test()
                    .current_primary()
                    .map(str::to_owned),
                pre_current_primary,
                "no fresh PrimaryChanged may be originated under the latched verdict",
            );
            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                pre_primary_epoch,
                "primary_epoch must NOT advance under the latched verdict",
            );
            // The verdict latch carries through unchanged (sticky first-writer-wins).
            assert_eq!(
                primary.cluster_state_for_test().run_aborted(),
                Some(ABORT_REASON),
                "the gate is a READ — it must NOT clear / overwrite the latch",
            );
            // Self-id wasn't named anywhere either.
            assert_ne!(
                primary
                    .cluster_state_for_test()
                    .current_primary()
                    .map(str::to_owned),
                Some(own_id.to_string()),
                "the gate must NOT install self as the primary; it adopts the cluster verdict",
            );
        })
        .await;
}

/// SEAM 2 — the clean-finish twin: a promoted primary whose inherited
/// ledger ALREADY carries the `RunComplete` latch short-circuits to
/// `Ok(())`. The inherited ledger is fully terminal (no work left to
/// dispatch); the operational loop would immediately fall through to a
/// counter exit on a zero-Pending pool, so the short-circuit saves the
/// empty pass + the spurious fresh `PrimaryChanged` broadcast that would
/// otherwise narrate a phantom failover at the observer.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_tail_adopts_run_complete_short_circuits_clean_ok() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::RunComplete {
                    counts: Default::default(),
                });

            let pre_primary_id = primary.primary_id.clone();
            let pre_primary_epoch = primary.cluster_state_for_test().primary_epoch();

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(),
            )
            .await;
            assert!(
                matches!(exit, Ok(Ok(()))),
                "RunComplete latch must short-circuit to Ok(()); got {exit:?}"
            );
            assert_eq!(
                primary.primary_id, pre_primary_id,
                "RunComplete short-circuit must NOT call activate_local_primary",
            );
            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                pre_primary_epoch,
                "RunComplete short-circuit must NOT bump primary_epoch",
            );
        })
        .await;
}

/// SEAM 2 NEGATIVE REGRESSION — without ANY terminal latch the normal
/// activation runs (mirrors `setup_promote::bootstrap_tail_activates_local_primary`,
/// pinned here so a future refactor that misplaced the latch factor (e.g.
/// inverted, or applied unconditionally) is caught locally to this file).
/// A primary seeded pre-complete with zero tasks drives the tail to a
/// clean `Ok(())` AND asserts itself as the local primary.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_tail_no_latch_still_activates_local_primary_regression() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            assert!(primary.cluster_state_for_test().run_aborted().is_none());
            assert!(!primary.cluster_state_for_test().run_complete());

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(),
            )
            .await;
            assert!(
                matches!(exit, Ok(Ok(()))),
                "no terminal latch: the existing happy-path tail must still run (zero \
                 tasks → counter exit → Ok(())); got {exit:?}",
            );
            assert_eq!(
                primary.primary_id.as_deref(),
                Some(own_id.as_str()),
                "no-latch baseline must still activate THIS node as the local primary",
            );
            assert_eq!(
                primary
                    .cluster_state_for_test()
                    .current_primary()
                    .map(str::to_owned),
                Some(own_id.to_string()),
                "no-latch baseline must still install self in the CRDT primary register",
            );
        })
        .await;
}

// ── #563 Seam 0 — uniform broadcast on ANY fatal primary Err exit ──
//
// CONTRACT (run_pipeline chokepoint): any `Err(e)` returned from
// `run_pipeline_inner` broadcasts `RunAborted(err.to_string())` UNLESS a
// per-variant call site already authored the verdict OR the error is
// `RunError::PanikShutdown` (per-node kill, not a run-terminal). Closes
// #562: the in-process distributed runner's relocate-target returning
// `RunError::Other(StagingError)` (queue_initial_staging files-missing /
// etc.) previously left the standalone-observer awaiting a verdict that
// never came — an infinite hang the await_terminal_observer_delivery 60s
// hold cannot cover because that hold is the OBSERVER's re-broadcast,
// not the primary's first-emit.

/// SEAM 0 — a `RunError::BringUpFailed` (the structured 0/N-welcome
/// timeout) is a variant whose pre-existing return path DOES NOT call
/// `broadcast_terminal_verdict`: it `?`-escapes from `wait_for_connections`
/// straight up the stack. Under #562 / Seam 0 the chokepoint MUST catch
/// every such Err and broadcast `RunAborted` so the observer / surviving
/// fleet observe the run as terminated rather than waiting forever for a
/// verdict that never comes.
///
/// Replays `bringup_fatal::zero_welcome_timeout_is_structured_bring_up_fatal`
/// AND asserts the chokepoint authored the run-aborted latch with the
/// `BringUpFailed` `Display` text — the same SAME-process-mirror the wire
/// frame carries.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn seam0_chokepoint_broadcasts_run_aborted_on_bringup_failed() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut outgoing: HashMap<
                String,
                tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
            > = HashMap::new();
            let mut held_rx = Vec::new();
            for i in 0..4 {
                let (tx, rx) = tokio_mpsc::unbounded_channel();
                outgoing.insert(format!("sec-{i}"), tx);
                held_rx.push(rx);
            }
            let (_inbound_hold, inbound_rx) = tokio_mpsc::unbounded_channel();
            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, inbound_rx);

            let config = PrimaryConfig {
                num_secondaries: 4,
                connect_timeout: Duration::from_secs(60),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let (deps, ops, ope) = noop_phase_args();
            let binaries = vec![(make_binary("a", 50), false)];
            let err = primary
                .run(
                    SeedSource::ColdStart {
                        binaries,
                        phase_deps: deps,
                    },
                    ops,
                    ope,
                )
                .await
                .expect_err("0/4-welcome bring-up timeout must Err");

            // Sanity-anchor: still the same structured-BringUpFailed variant
            // the bringup_fatal test pins. We do NOT regress that contract;
            // Seam 0 adds the broadcast ON TOP of the existing local return.
            assert!(
                matches!(
                    err,
                    crate::primary::RunError::BringUpFailed { ref reason }
                        if reason.contains("0/4 sent SecondaryWelcome")
                ),
                "Seam 0 must not change the local RunError type: {err:?}"
            );

            // Seam 0 contract: the chokepoint authored the RunAborted latch
            // with the err's Display text (the SAME text the wire frame
            // carries to every replica — the observer's `evaluate_exit`
            // reads exactly this string).
            let latched = primary
                .cluster_state_for_test()
                .run_aborted()
                .map(str::to_owned);
            let expected = err.to_string();
            assert_eq!(
                latched.as_deref(),
                Some(expected.as_str()),
                "the run_pipeline chokepoint must broadcast RunAborted carrying \
                 the err's Display text on a BringUpFailed exit (#563 Seam 0); \
                 latched={latched:?} expected={expected:?}"
            );
        })
        .await;
}

/// SEAM 0 NEGATIVE REGRESSION (idempotency / first-writer-wins) — an Err
/// exit whose per-variant call site ALREADY broadcast a verdict (the
/// pre-Seam-0 paths: SpawnRejected / FatalPolicyExit / RunShouldFail /
/// ClusterCollapsed / NoRelocationTarget / InvalidComposedGraph / the
/// duplicate-task aborts) must NOT have its reason corrupted by a
/// later Seam 0 re-emit. The chokepoint's `!already_authored` gate is
/// the primary mechanism that skips the re-emit; first-writer-wins
/// (apply.rs:395 sticky-Some) is the wire safety net.
///
/// We can't easily synthesise a per-variant broadcast inside `run`
/// without driving the full pipeline to one of those exits, so this
/// regression pins the gate's READ side at the ClusterState level — the
/// same `cluster_state.run_aborted()` accessor the chokepoint consults.
///
/// PanikShutdown — the doc-forbidden variant (per-node kill, run
/// continues / re-elects) — is the OTHER skip case; the chokepoint's
/// explicit `is_panik` guard is a source-level pattern match on
/// `RunError::PanikShutdown { .. }`. Driving a real panik file from a
/// test duplicates `panik_integration`'s coverage without adding to the
/// Seam 0 contract, so we test the wire-safety side here and trust the
/// pattern-match read-by-inspection (cf. how `broadcast_terminal_verdict`'s
/// own per-variant classification doc says "panik authors no verdict").
#[test]
fn seam0_chokepoint_gate_recognises_pre_authored_verdict() {
    // The chokepoint's `!already_authored` predicate reads
    // `run_aborted().is_some() || run_complete()` — the SAME accessor
    // the per-variant call sites converge on, so pinning the accessor
    // behaviour pins the gate.
    let mut state =
        crate::cluster_state::ClusterState::<dynrunner_core::RunnerIdentifier>::new();

    // A per-variant call site fired first — its reason landed in the
    // sticky first-writer latch.
    const FIRST_REASON: &str = "the per-variant path's verbatim reason \
                                (SpawnRejected / FatalPolicyExit / etc.)";
    state.apply(ClusterMutation::RunAborted {
        reason: FIRST_REASON.into(),
        counts: Default::default(),
    });
    assert_eq!(state.run_aborted(), Some(FIRST_REASON));

    // The chokepoint's `!already_authored` gate would skip a re-emit;
    // even if a future regression removed the gate, the first-writer-
    // wins latch keeps the original reason — pinned here so a
    // regression that landed the chokepoint emit unconditionally still
    // cannot corrupt the wire fact.
    state.apply(ClusterMutation::RunAborted {
        reason: "would-be Seam 0 chokepoint reason (should NoOp)".into(),
        counts: Default::default(),
    });
    assert_eq!(
        state.run_aborted(),
        Some(FIRST_REASON),
        "first-writer-wins: the per-variant call site's reason MUST survive a \
         later Seam 0 re-emit (apply.rs:395 sticky-Some rule). The chokepoint's \
         own `!already_authored` gate is the primary mechanism that avoids the \
         spurious 'already latched' warn log; this latch behaviour is the \
         defence-in-depth.",
    );
}

/// SEAM 2 — the post-promotion snapshot-pull convergence scenario the
/// owner asked to gate in explicitly. Models the case where the broadcast
/// LOST its frame on this secondary's leg, the secondary won the
/// subsequent election, AND the snapshot-pull / anti-entropy backstop
/// delivers the converged `RunAborted` AFTER promotion but BEFORE
/// `bootstrap_tail_dispatch` runs. Driven here by applying the verdict
/// AFTER constructing the coordinator (modelling the snapshot-restore arm
/// landing the latch on this node's mirror) and BEFORE calling the
/// bootstrap tail — exactly the in-window ordering the gate must catch.
/// The CRDT delivery shape is identical to the broadcast path (apply on
/// the same `ClusterState` field, the snapshot arm and the live-apply arm
/// converge on the same `run_aborted` sticky-Some latch — verified in
/// `cluster_state/snapshot.rs:1052-1056`), so a single Seam 2 gate covers
/// BOTH the broadcast-arrived-pre-promotion path AND the
/// healed-by-snapshot-post-promotion path.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_tail_adopts_post_promotion_snapshot_pulled_verdict() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Model the post-promotion snapshot-pull / anti-entropy convergence:
            // BETWEEN the election win and the bootstrap-tail entry, an
            // anti-entropy digest exchange against a peer that DID receive the
            // dying primary's broadcast lands the verdict here via the snapshot
            // restore arm (apply.rs:395 = snapshot.rs:1055 in semantics). We
            // apply the live mutation directly because that arm and the snapshot
            // arm converge on the SAME sticky-Some latch.
            const SNAPSHOT_REASON: &str = "post-promotion snapshot-pull: runtime \
                                           spawn_tasks rejected 46497 task(s)";
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::RunAborted {
                    reason: SNAPSHOT_REASON.into(),
                    counts: Default::default(),
                });

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(),
            )
            .await;
            match exit {
                Ok(Err(crate::primary::RunError::AbortedByClusterVerdict { reason })) => {
                    assert_eq!(
                        reason, SNAPSHOT_REASON,
                        "a snapshot-pulled verdict must adopt with the SAME reason text \
                         the peer's snapshot carried"
                    );
                }
                other => panic!(
                    "the bootstrap-tail must adopt the snapshot-pulled verdict on the \
                     post-promotion convergence path; got {other:?}"
                ),
            }
        })
        .await;
}
