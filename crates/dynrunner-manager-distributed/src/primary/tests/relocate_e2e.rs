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
