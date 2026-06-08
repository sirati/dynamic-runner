//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Pre-seeded bootstrap exit semantics: the counter-based exit at the top
/// of `operational_loop` fires immediately when
/// `completed + failed >= total_tasks && active_workers == 0`. Pins the
/// cold path where `seed_cluster_state` ran locally and `total_tasks` was
/// non-zero at startup.
#[tokio::test(flavor = "current_thread")]
async fn pre_seeded_counter_exit_unchanged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, _to_sec_rx, _incoming_tx) = secondary_ends.into_iter().next().unwrap();

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                // Pre-seeded bootstrap: `seed_cluster_state` ran locally, so
                // `total_tasks` is set by `run()` from `binaries.len()`
                // and the counter-based exit must fire on the very first
                // iteration once completions cover the total.
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Pre-seeded mid-run state: 2 tasks total, both already in the
            // completed set (mirrors what would normally arrive via
            // TaskComplete handlers). No active workers. The counter
            // check on the first iteration is `2+0 >= 2 && 0 == 0` —
            // must trip immediately.
            let phase = dynrunner_core::PhaseId::from("default");
            let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
                [phase.clone()],
                std::collections::HashMap::new(),
            )
            .expect("default-phase pool");
            primary.pending = Some(pool);
            primary.total_tasks = 2;
            primary.completed_tasks.insert("h-legacy-1".into());
            primary.completed_tasks.insert("h-legacy-2".into());

            // Bounded wait. The counter-check exit should fire on
            // iteration 1 of the loop — well under 1s. A 5s ceiling is
            // overkill but stays consistent with the other operational-
            // loop tests.
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.operational_loop(),
            )
            .await;

            match exit {
                Ok(Ok(())) => {
                    // Exit path pinning: the pre-seeded counter-based exit
                    // fired, not the `cluster_state.run_complete()` branch.
                    assert!(
                        !primary.cluster_state_for_test().run_complete(),
                        "pre-seeded bootstrap exit must be via the counter check, \
                     not via the cluster_state.run_complete() branch"
                    );
                }
                Ok(Err(e)) => {
                    panic!("operational_loop returned Err in pre-seeded bootstrap scenario: {e}")
                }
                Err(_) => panic!(
                    "pre-seeded bootstrap operational_loop did not exit within 5s \
                 despite the counter check `2+0 >= 2 && active_workers == 0` \
                 being satisfied on the first iteration — regression on the \
                 historical exit semantics"
                ),
            }
        })
        .await;
}
