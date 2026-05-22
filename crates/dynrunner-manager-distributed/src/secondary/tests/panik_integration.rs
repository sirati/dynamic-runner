//! Integration tests for the panik (operator-initiated emergency
//! stop) end-to-end wire path on the secondary.
//!
//! Single concern of this file: pin the pipeline
//! "watcher signal arrives → coordinator reacts (broadcast or not
//! per source) → coordinator returns `RunOutcome::PanikShutdown`".
//!
//! Two source-paths are covered, mirroring the watcher's two trigger
//! sources (`panik_watcher::PanikWatcherConfig::paths` and
//! `::listen_for_sigterm`):
//!   - **File source** — `panik_file_source_broadcasts_and_returns_panik_shutdown`
//!     pins "matched filesystem path → `ClusterMutation::PanikRequested`
//!     reaches the primary wire, `RunOutcome::PanikShutdown` returned".
//!   - **SIGTERM source** — `panik_sigterm_source_does_not_broadcast_and_returns_panik_shutdown`
//!     pins "SIGTERM sentinel path → NO `PanikRequested` on the wire,
//!     local-only teardown, `RunOutcome::PanikShutdown` still returned
//!     with the SIGTERM sentinel path and per-host reason". This is the
//!     load-bearing assertion against the cascade-shutdown bug: a single
//!     host's SIGTERM must not force every peer to exit via the sticky-
//!     monotonic CRDT.
//!
//! The CRDT apply rule's tests live in
//! `cluster_state/tests/panik.rs`; the watcher's standalone tests
//! live in `panik_watcher.rs`. This file is the seam between them:
//! a real `SecondaryCoordinator` with its real `select!` loop,
//! consuming a real `oneshot::Receiver<PanikSignal>` via the
//! `register_panik_signal_rx` builder, asserting both the wire
//! emission (or absence) AND the `RunOutcome` returned to the caller.

#![cfg(test)]

use std::time::Duration;

use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, MessageType,
};
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{FakeWorkerFactory, TestId};
use super::super::*;

/// File source: run a secondary against a fake primary, register a
/// panik oneshot receiver, fire the panik signal (carrying a real
/// filesystem-style path, not the SIGTERM sentinel) once the loop is
/// in `process_tasks`, and assert:
///   1. the secondary emits a `ClusterMutation` carrying
///      `PanikRequested` on the primary transport,
///   2. `run_until_setup_or_done` returns `RunOutcome::PanikShutdown`
///      with the matched path the watcher carried.
///
/// SIGTERM source has the inverted assertion shape (NO broadcast) and
/// is covered by the sibling test below.
#[tokio::test(flavor = "current_thread")]
async fn panik_file_source_broadcasts_and_returns_panik_shutdown() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-panik".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
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
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_secs(2),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
            };

            let mut secondary: SecondaryCoordinator<_, _, _, _, _, TestId> =
                SecondaryCoordinator::new(
                    config,
                    transport,
                    super::super::test_helpers::NoPeers,
                    dynrunner_scheduler::ResourceStealingScheduler::memory(),
                    super::super::test_helpers::FixedEstimator(100),
                );

            // Register the panik signal receiver BEFORE entering
            // run_until_setup_or_done — the field is taken into the
            // operational loop's local state on first entry and
            // never re-read off `self` from the select arm. This
            // matches the production wiring in the PyO3 wrapper.
            let (panik_tx, panik_rx) = tokio::sync::oneshot::channel();
            secondary.register_panik_signal_rx(panik_rx);

            // The expected `matched_path` payload. We use a synthetic
            // path that does NOT need to exist on disk — the panik
            // watcher itself isn't running in this test, we drive
            // the signal channel directly so the test pins the
            // coordinator's reaction surface in isolation from the
            // watcher's polling behaviour. (The watcher's polling +
            // first-match-wins logic is covered separately by
            // `panik_watcher::tests::detects_file_creation_and_carries_path`.)
            let expected_path =
                std::path::PathBuf::from("/tmp/synthetic-panik-test");

            // Fire the panik signal AFTER spawning the secondary's
            // run loop and AFTER a small settle window so the
            // secondary is past wait_for_setup and inside
            // process_tasks. Without the settle the signal arrives
            // before the panik arm is wired up (the loop's
            // `take()` of `panik_signal_rx` happens at the top of
            // `process_tasks`, AFTER the setup-handshake phase);
            // firing too early would race the arm-installation and
            // the secondary would never observe the signal.
            let path_for_signal = expected_path.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = panik_tx.send(crate::panik_watcher::PanikSignal {
                    matched_path: path_for_signal,
                });
            });

            // Fake primary task. Drives the setup handshake but
            // never sends any TaskAssignment — the secondary sits
            // in process_tasks until the panik signal lands. The
            // fake primary also watches for the
            // `ClusterMutation::PanikRequested` message and signals
            // its arrival through a channel so the test can assert
            // the wire fan-out happened.
            let sec_id = "sec-panik".to_string();
            let (saw_panik_tx, mut saw_panik_rx) =
                tokio_mpsc::unbounded_channel::<ClusterMutation<TestId>>();
            let to_sec_clone = pri_to_sec_tx.clone();
            let primary_task = tokio::task::spawn_local(async move {
                let mut from_secondary = sec_to_pri_rx;
                let to_secondary = to_sec_clone;
                // Drive welcome / cert exchange / setup just enough
                // to get the secondary into process_tasks. We
                // reuse the production setup wire sequence from
                // `fake_primary` by calling its helper indirectly —
                // we can't call `fake_primary` directly because it
                // tries to drain `TaskComplete`s and that never
                // happens in this test. Instead, do the minimal
                // handshake inline.
                let mut got_welcome = false;
                let mut got_cert = false;
                while !got_welcome || !got_cert {
                    if let Some(msg) = from_secondary.recv().await {
                        match msg.msg_type() {
                            MessageType::SecondaryWelcome => got_welcome = true,
                            MessageType::CertExchange => got_cert = true,
                            _ => {}
                        }
                    } else {
                        return;
                    }
                }
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
                        secondary_id: sec_id.clone(),
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

                // From here on, watch for the panik broadcast. The
                // secondary may emit Keepalives and TaskRequests in
                // the interim; we just drain those silently.
                while let Some(msg) = from_secondary.recv().await {
                    if let DistributedMessage::ClusterMutation {
                        mutations, ..
                    } = &msg
                    {
                        for mutation in mutations {
                            if matches!(
                                mutation,
                                ClusterMutation::PanikRequested { .. }
                            ) {
                                let _ = saw_panik_tx.send(mutation.clone());
                            }
                        }
                    }
                }
            });

            let mut factory = FakeWorkerFactory;
            // Drive the secondary. The panik handler returns
            // `Ok(RunOutcome::PanikShutdown)` from
            // `run_until_setup_or_done`; the test's bounded sleep
            // above guarantees that fires within a few hundred
            // milliseconds.
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect("secondary did not return within budget")
            .expect("secondary returned Err on the panik path");

            match &outcome {
                RunOutcome::PanikShutdown {
                    matched_path,
                    reason,
                } => {
                    assert_eq!(matched_path, &expected_path);
                    assert!(
                        reason.contains("synthetic-panik-test"),
                        "panik reason should contain the matched-path \
                         substring; got: {reason}"
                    );
                }
                other => panic!(
                    "expected RunOutcome::PanikShutdown, got: {other:?}"
                ),
            }

            // Confirm the broadcast reached the primary wire. The
            // primary task above filtered for `PanikRequested`
            // mutations; we should see at least one with a `reason`
            // that matches the matched-path. Drop the primary task
            // first so its `from_secondary.recv()` returns None
            // and the loop exits cleanly.
            drop(pri_to_sec_tx);
            primary_task.abort();
            let _ = primary_task.await;
            // The mutation may or may not have reached us depending
            // on race with the abort — but if it did, the reason
            // must match. The wire emission is the load-bearing
            // assertion that drove this test in the first place;
            // sealing it with `try_recv` matches the production
            // contract (apply-and-broadcast logs a warning on send
            // failure but never blocks the panik-react path).
            let mut saw_panik_broadcast = false;
            while let Ok(mutation) = saw_panik_rx.try_recv() {
                if let ClusterMutation::PanikRequested { reason, .. } = mutation
                {
                    assert!(
                        reason.contains("synthetic-panik-test"),
                        "broadcast mutation's reason must reference the \
                         matched path; got: {reason}"
                    );
                    saw_panik_broadcast = true;
                }
            }
            assert!(
                saw_panik_broadcast,
                "primary wire never observed ClusterMutation::PanikRequested \
                 within the test's bounded run window — the secondary's \
                 panik arm did not broadcast"
            );
        })
        .await;
}

/// SIGTERM source: same coordinator harness as the file-source
/// sibling, but the panik signal carries
/// [`crate::panik_watcher::SIGTERM_SENTINEL_PATH`] (the documented
/// sentinel the watcher's SIGTERM arm emits). Assertions are
/// inverted relative to the file-source test:
///   1. `run_until_setup_or_done` STILL returns
///      `RunOutcome::PanikShutdown` (the local-teardown side of the
///      panik contract is unchanged), with `matched_path == <SIGTERM>`
///      and the per-host reason string `"panik SIGTERM (per-host)"`.
///   2. NO `ClusterMutation::PanikRequested` appears on the primary
///      wire — `handle_panik_signal` skips
///      `apply_and_broadcast_mutations` on the SIGTERM branch. This
///      is the load-bearing assertion against the cascade-shutdown
///      bug: a single host's SIGTERM (e.g. SLURM time-limit hitting
///      one node early) must not force every peer to exit via the
///      sticky-monotonic `PanikRequested` CRDT.
///
/// The wire-emission seam asserted here is the same one the sibling
/// file-source test pins: `apply_and_broadcast_mutations` sends the
/// mutation on BOTH the primary transport AND the peer transport,
/// so the primary-wire absence is sufficient evidence that the call
/// did not fire (rather than fired with a different transport).
#[tokio::test(flavor = "current_thread")]
async fn panik_sigterm_source_does_not_broadcast_and_returns_panik_shutdown() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-panik-sigterm".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
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
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_secs(2),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
            };

            let mut secondary: SecondaryCoordinator<_, _, _, _, _, TestId> =
                SecondaryCoordinator::new(
                    config,
                    transport,
                    super::super::test_helpers::NoPeers,
                    dynrunner_scheduler::ResourceStealingScheduler::memory(),
                    super::super::test_helpers::FixedEstimator(100),
                );

            let (panik_tx, panik_rx) = tokio::sync::oneshot::channel();
            secondary.register_panik_signal_rx(panik_rx);

            // SIGTERM source — the sentinel path the watcher's
            // SIGTERM arm carries on its `PanikSignal`. Constructed
            // via the watcher's documented constant (NOT a string
            // literal duplicated here) so any future change to the
            // sentinel propagates to this test through the type
            // system rather than silently desynchronising.
            let expected_path = std::path::PathBuf::from(
                crate::panik_watcher::SIGTERM_SENTINEL_PATH,
            );

            // Same settle window + fire pattern as the file-source
            // test — see that test's comment block for rationale on
            // why the signal can't fire before the loop's panik arm
            // is installed via `take()` inside process_tasks.
            let path_for_signal = expected_path.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = panik_tx.send(crate::panik_watcher::PanikSignal {
                    matched_path: path_for_signal,
                });
            });

            // Fake primary task: drive the setup handshake, then
            // record every `ClusterMutation` observed from the
            // secondary. The SIGTERM branch must not emit any
            // `PanikRequested` — we assert on the recorded log
            // after the coordinator returns, when any broadcast
            // would necessarily already have been issued
            // (`handle_panik_signal` returns synchronously after
            // the apply+broadcast call on the file-source branch).
            let sec_id = "sec-panik-sigterm".to_string();
            let (recorded_mutations_tx, mut recorded_mutations_rx) =
                tokio_mpsc::unbounded_channel::<ClusterMutation<TestId>>();
            let to_sec_clone = pri_to_sec_tx.clone();
            let primary_task = tokio::task::spawn_local(async move {
                let mut from_secondary = sec_to_pri_rx;
                let to_secondary = to_sec_clone;
                let mut got_welcome = false;
                let mut got_cert = false;
                while !got_welcome || !got_cert {
                    if let Some(msg) = from_secondary.recv().await {
                        match msg.msg_type() {
                            MessageType::SecondaryWelcome => got_welcome = true,
                            MessageType::CertExchange => got_cert = true,
                            _ => {}
                        }
                    } else {
                        return;
                    }
                }
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
                        secondary_id: sec_id.clone(),
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

                while let Some(msg) = from_secondary.recv().await {
                    if let DistributedMessage::ClusterMutation {
                        mutations, ..
                    } = &msg
                    {
                        for mutation in mutations {
                            // Record EVERY mutation (not just
                            // PanikRequested) so the test's
                            // assertion can distinguish "no
                            // ClusterMutation observed at all"
                            // (expected) from "some other
                            // ClusterMutation observed but no
                            // PanikRequested" (still acceptable —
                            // the bug-relevant invariant is the
                            // absence of PanikRequested) without
                            // changing the log shape between tests.
                            let _ = recorded_mutations_tx.send(mutation.clone());
                        }
                    }
                }
            });

            let mut factory = FakeWorkerFactory;
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect("secondary did not return within budget")
            .expect("secondary returned Err on the SIGTERM panik path");

            match &outcome {
                RunOutcome::PanikShutdown {
                    matched_path,
                    reason,
                } => {
                    assert_eq!(
                        matched_path, &expected_path,
                        "SIGTERM panik should surface the sentinel \
                         matched_path verbatim to the caller"
                    );
                    assert_eq!(
                        reason, "panik SIGTERM (per-host)",
                        "SIGTERM panik reason should use the per-host \
                         phrasing (NOT \"panik file: <SIGTERM>\" which \
                         conflated source type with file path)"
                    );
                }
                other => panic!(
                    "expected RunOutcome::PanikShutdown, got: {other:?}"
                ),
            }

            // Drain the primary-side recorder and assert NO
            // `PanikRequested` mutation appeared. This is the
            // load-bearing invariant: the SIGTERM branch of
            // `handle_panik_signal` must skip
            // `apply_and_broadcast_mutations` entirely, so the
            // sticky-monotonic CRDT never cascades the per-host
            // SIGTERM into a forced cluster-wide exit.
            drop(pri_to_sec_tx);
            primary_task.abort();
            let _ = primary_task.await;
            while let Ok(mutation) = recorded_mutations_rx.try_recv() {
                if let ClusterMutation::PanikRequested { reason, .. } = &mutation
                {
                    panic!(
                        "SIGTERM panik leaked a PanikRequested mutation \
                         onto the primary wire (reason: {reason}); this \
                         is the cascade-shutdown bug — SIGTERM-source \
                         signals must be local-only"
                    );
                }
            }
        })
        .await;
}
