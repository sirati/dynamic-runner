//! A regular (non-observer) pre-staged secondary acts on a panik signal
//! delivered AFTER a `SetupPending` yield/re-entry.
//!
//! A regular secondary registers a panik-watcher signal receiver
//! (`register_panik_signal_rx`) AND can be the pre-staged discovery node, so
//! it both reaches the operational panik `select!` arm and takes the
//! `SetupPending` excursion (yield to the wrapper for `discover_items`, then
//! re-enter `process_tasks`). The panik receiver is the SOLE in-loop path by
//! which a SIGTERM (the SLURM time-limit / `scancel` → `kill -TERM` cascade)
//! or a sentinel file reaches the graceful-shutdown cascade — worker
//! teardown + the `Panik` terminal + exit(137) — letting the secondary
//! release its SLURM-allocated resources before the kernel `SIGKILL`s at the
//! grace deadline.
//!
//! The panik receiver must therefore survive the yield/re-entry: it is
//! resumable per-run state on `OperationalState`, taken into the loop-local
//! panik arm and restored there before the `SetupPending` return, NOT dropped
//! as a fire-once latch. If it were lost on re-entry the panik arm would park
//! on `pending()` forever, a post-discovery SIGTERM would NOT be acted on by
//! the secondary loop (the watcher's oneshot cannot reach the dead arm), and
//! graceful shutdown would fall back to the kernel `SIGKILL`.
//!
//! This drives the production path end-to-end: real setup handshake →
//! `Configuring → Operational` → first `process_tasks` yields `SetupPending`
//! → discovery ingest → panik signal fired → re-entry → the panik arm drains
//! the signal and the loop reaches terminal `Panik`. The mesh transport is
//! kept open throughout (the fake primary holds its sender and never closes
//! the channel) and no `RunComplete` is ever delivered, so the ONLY way the
//! re-entry can terminate is the panik arm — exactly the path a lost panik
//! receiver would never reach.

#![cfg(test)]

use std::collections::HashMap;

use dynrunner_core::PhaseId;
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{
    FakeWorkerFactory, FixedEstimator, TestId, channel_mesh_to_primary,
};
use super::super::{RunOutcome, SecondaryConfig, SecondaryCoordinator, SecondaryTerminal};
use super::processing::make_binary;

/// A fake primary that drives the secondary through setup in PRE-STAGED
/// mode (so it yields `SetupPending`), then holds the channel open
/// indefinitely. It never broadcasts `RunComplete` and never closes the
/// channel — so the secondary's mesh `recv_peer` stays pending and the
/// re-entry can only terminate via the panik arm, isolating it. Returns
/// only when its inbound channel closes (the secondary tore down).
async fn pre_staged_fake_primary(
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // Welcome + cert exchange.
    let mut got_welcome = false;
    let mut got_cert = false;
    while !got_welcome || !got_cert {
        match from_secondary.recv().await {
            Some(msg) => match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            },
            None => return,
        }
    }

    to_secondary
        .send(DistributedMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();

    // Pre-staged InitialAssignment: an EMPTY ledger with `pre_staged_mode:
    // true` is the discovery-yield carrier — the secondary will yield
    // `SetupPending` so the wrapper can run `discover_items`.
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            pre_staged_mode: true,
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

    // Drain whatever the secondary emits (task requests, keepalives) and
    // keep the channel alive. We never close `to_secondary`, so the
    // secondary's mesh `recv_peer` stays pending — the panik arm is the
    // only reachable exit on re-entry.
    while from_secondary.recv().await.is_some() {}
}

/// PRE-FIX: hangs (the panik receiver is `None` on re-entry → the panik arm
/// parks on `pending()` → the loop never observes the signal and never
/// terminates, because the mesh never closes and no `RunComplete` arrives).
/// POST-FIX: the second `run_until_setup_or_done` returns
/// `RunOutcome::Terminal` and the lifecycle records `Panik`.
#[tokio::test(flavor = "current_thread")]
async fn panik_signal_acted_on_after_setup_pending_reentry() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                hostname: "test-host".into(),
                // Long keepalive so no liveness/election tick perturbs the
                // bounded run.
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                can_be_primary: true,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(pre_staged_fake_primary(
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            // Channel-backed mesh with the fake primary folded in as the
            // `"primary"` member; `recv_peer` stays pending while the
            // primary holds its sender, so the mesh never closes.
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            secondary.set_bootstrap_primary_id("primary".to_string());

            // Register the panik signal receiver BEFORE entering
            // `run_until_setup_or_done`, exactly as the regular-secondary
            // PyO3 wrapper does. The receiver is the SOLE in-loop path for
            // an operator-initiated graceful shutdown to reach the panik
            // cascade; it must survive the `SetupPending` excursion below.
            let (panik_tx, panik_rx) = tokio::sync::oneshot::channel();
            secondary.register_panik_signal_rx(panik_rx);

            // Synthetic matched-path payload — the panik watcher itself is
            // not running here; we drive the signal channel directly so the
            // test pins the coordinator's reaction across the re-entry in
            // isolation. (The watcher's polling is covered by
            // `panik_watcher::tests::detects_file_creation_and_carries_path`.)
            let expected_path = std::path::PathBuf::from("/tmp/synthetic-panik-test");

            let mut factory = FakeWorkerFactory;

            // First entry: real setup → `Configuring → Operational` → the
            // empty pre-staged ledger makes `process_tasks` yield
            // `SetupPending`. The panik receiver is seeded into the loop's
            // panik arm from the coordinator slot and restored to
            // `OperationalState` before this yield.
            let first = secondary
                .run_until_setup_or_done(&mut factory)
                .await
                .expect("first run_until_setup_or_done must not error");
            assert!(
                matches!(first, RunOutcome::SetupPending),
                "pre-staged empty ledger must yield SetupPending, got {first:?}",
            );

            // Ingest discovery with ONE real item. This seeds the ledger
            // (clearing `setup_discovery_pending` on the count axis) and
            // latches the fire-once guard, WITHOUT broadcasting/applying a
            // local `RunComplete` — so on re-entry the loop's
            // `cluster_state.run_complete()` is still false and the ONLY way
            // it can terminate is the panik signal. (`active_tasks` stays
            // empty: ingest only seeds the CRDT; no worker assignment is
            // dispatched.)
            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(PhaseId::from("default"), vec![]);
            secondary
                .ingest_setup_discovery(vec![make_binary("item-0", 1)], deps)
                .await
                .expect("ingest must succeed");

            // Fire the panik signal AFTER the first yield but BEFORE re-entry.
            // The oneshot value buffers in the channel; the re-entry's panik
            // arm `rx.await` resolves it immediately. This stands in for a
            // SIGTERM / sentinel file delivered to the python AFTER the
            // discovery yield.
            //
            // The send result is NOT asserted: if the panik receiver was
            // dropped across the `SetupPending` yield (the very defect under
            // test), this send returns `Err` because there is no receiver
            // left. Either way the load-bearing signal is the re-entry below
            // — it can only reach `Terminal` if the receiver survived.
            let _ = panik_tx.send(crate::panik_watcher::PanikSignal {
                matched_path: expected_path.clone(),
                sender_pid: None,
            });

            // Re-entry: with the panik receiver re-attached from
            // `OperationalState`, the panik arm drains the signal, the
            // handler records the `Panik` terminal, and the loop returns
            // `Terminal`. PRE-FIX this hangs (panik `None` on re-entry → arm
            // parks on `pending()`, mesh never closes, no `RunComplete`), so
            // the bounded timeout below is the hang detector.
            let second = tokio::time::timeout(
                Duration::from_secs(10),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect(
                "re-entry timed out: the secondary never acted on the panik signal \
                 — the panik receiver was lost across the SetupPending re-entry",
            )
            .expect("second run_until_setup_or_done must not error");

            assert!(
                matches!(second, RunOutcome::Terminal),
                "re-entry must reach Terminal once the panik signal is acted on, got {second:?}",
            );
            match secondary.terminal() {
                Some(SecondaryTerminal::Panik {
                    matched_path,
                    reason,
                }) => {
                    assert_eq!(
                        matched_path, expected_path,
                        "the panik terminal must carry the matched path"
                    );
                    assert!(
                        reason.contains("synthetic-panik-test"),
                        "panik reason should contain the matched-path substring; got: {reason}"
                    );
                }
                other => panic!(
                    "re-entry must record the Panik terminal once the panik signal \
                     is acted on across the SetupPending re-entry, got {other:?}"
                ),
            }

            // Drop the secondary so the fake primary's inbound closes and
            // its task returns.
            drop(secondary);
            let _ = primary_handle.await;
        })
        .await;
}
