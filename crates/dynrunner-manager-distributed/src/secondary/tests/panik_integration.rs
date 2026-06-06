//! Integration tests for the panik → self-departure end-to-end wire
//! path on the secondary.
//!
//! Single concern of this file: pin the pipeline
//! "watcher signal arrives → coordinator reacts (announce departure or
//! not, per source) → coordinator returns `RunOutcome::Terminal`
//! projecting to `SecondaryTerminal::Panik`".
//!
//! A node observing its OWN panik signal announces its departure from
//! the mesh via a self-authored
//! `ClusterMutation::PeerRemoved { id: <self>, cause:
//! SelfDeparture(reason) }`. That announcement is observability-only —
//! peers LOG it and mark the node Dead; it does NOT cancel cluster work
//! or terminate the run on peers. The departing node tears down its own
//! workers and exits locally with `RunOutcome::Terminal` (projecting to
//! `SecondaryTerminal::Panik`).
//!
//! Two source-paths are covered, mirroring the watcher's two trigger
//! sources (`panik_watcher::PanikWatcherConfig::paths` and
//! `::listen_for_sigterm`):
//!   - **File source** — `panik_file_source_broadcasts_and_returns_terminal_panik`
//!     pins "matched filesystem path → self-authored
//!     `ClusterMutation::PeerRemoved { SelfDeparture }` reaches the
//!     primary wire, `RunOutcome::Terminal` (projecting to
//!     `SecondaryTerminal::Panik`) returned".
//!   - **SIGTERM source** — `panik_sigterm_source_does_not_broadcast_and_returns_terminal_panik`
//!     pins "SIGTERM sentinel path → NO departure announcement on the
//!     wire, local-only teardown, `RunOutcome::Terminal` (projecting to
//!     `SecondaryTerminal::Panik`) still returned with the SIGTERM
//!     sentinel path and per-host reason".
//!     A single host's SIGTERM is a purely local event and the mesh
//!     stays free to continue / re-elect.
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
    ClusterMutation, DistributedMessage, MessageType, RemovalCause,
};
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, make_secondary_channel, start_secondary_pump,
};
use super::super::*;

/// File source: run a secondary against a fake primary, register a
/// panik oneshot receiver, fire the panik signal (carrying a real
/// filesystem-style path, not the SIGTERM sentinel) once the loop is
/// in `process_tasks`, and assert:
///   1. the secondary emits a `ClusterMutation` carrying a
///      self-authored `PeerRemoved { SelfDeparture }` on the primary
///      transport,
///   2. `run_until_setup_or_done` returns `RunOutcome::Terminal`
///      (projecting to `SecondaryTerminal::Panik`) with the matched path
///      the watcher carried.
///
/// SIGTERM source has the inverted assertion shape (NO announcement)
/// and is covered by the sibling test below.
///
/// Driven over the PRODUCTION concurrent mesh-pump (`start_secondary_pump`):
/// the pump drains the secondary's welcome/cert egress so the fake primary
/// completes the setup handshake, and fans the self-departure broadcast out
/// to the observed peer — the concurrency the sequential stub lacked.
#[tokio::test(flavor = "current_thread")]
async fn panik_file_source_broadcasts_and_returns_terminal_panik() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let (pri_to_sec_tx, pri_to_sec_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();

            // Channel-backed mesh: the primary is folded in (carries the
            // setup frames the fake-primary task injects), and one observed
            // peer receives the secondary's mesh `broadcast`s. The
            // self-departure announcement is a `Destination::All` broadcast
            // (`apply_and_broadcast_mutations`), which the mesh fans out to
            // the observed peer (the primary is excluded), so drain
            // `mesh_observer_rx` to assert it.
            let (unified, mut mesh_observer_rx) =
                super::super::test_helpers::channel_mesh_with_observed_peer(
                    "sec-panik",
                    sec_to_pri_tx,
                    pri_to_sec_rx,
                );

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
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_secs(2),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let mut secondary = make_secondary_channel(config, unified);
            secondary.set_bootstrap_primary_id("primary".to_string());

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
            let expected_path = std::path::PathBuf::from("/tmp/synthetic-panik-test");

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
                    // File source carries no signal, hence no sender PID.
                    sender_pid: None,
                });
            });

            // Fake primary task. Drives the setup handshake but
            // never sends any TaskAssignment — the secondary sits
            // in process_tasks until the panik signal lands. The
            // The self-departure now rides the MESH (recorded on
            // `mesh_log`), not the uplink — so the primary task only
            // drives the setup handshake and then drains the uplink
            // silently.
            let sec_id = "sec-panik".to_string();
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
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        peers: vec![],
                    })
                    .unwrap();
                to_secondary
                    .send(DistributedMessage::InitialAssignment {
                        target: None,
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
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        total_files: 0,
                        total_bytes: 0,
                    })
                    .unwrap();

                // Drain the uplink silently (keepalives / task
                // requests). The self-departure does NOT arrive here —
                // it's a mesh broadcast recorded on `mesh_log`.
                while from_secondary.recv().await.is_some() {}
            });

            // Spawn the production pump so the secondary's setup egress drains
            // (the fake primary's handshake completes) and the self-departure
            // broadcast fans out to the observed peer. `_guard` keeps the slot
            // + pump alive for the whole drive.
            let (mut secondary, _guard) = start_secondary_pump(secondary);

            let mut factory = FakeWorkerFactory;
            // Drive the secondary. The panik handler records the `Panik`
            // lifecycle terminal and returns `Ok(RunOutcome::Terminal)`
            // from `run_until_setup_or_done`; the test's bounded sleep
            // above guarantees that fires within a few hundred
            // milliseconds.
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect("secondary did not return within budget")
            .expect("secondary returned Err on the panik path");

            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "expected RunOutcome::Terminal on the panik path, got: {outcome:?}"
            );
            match secondary.terminal() {
                Some(SecondaryTerminal::Panik {
                    matched_path,
                    reason,
                }) => {
                    assert_eq!(matched_path, expected_path);
                    assert!(
                        reason.contains("synthetic-panik-test"),
                        "panik reason should contain the matched-path \
                         substring; got: {reason}"
                    );
                }
                other => panic!("expected SecondaryTerminal::Panik, got: {other:?}"),
            }

            // Confirm the departure announcement reached the MESH (the
            // unified transport fans the self-departure broadcast out to
            // the observed peer). Drop the primary task first so its
            // setup-handshake loop terminates cleanly.
            drop(pri_to_sec_tx);
            primary_task.abort();
            let _ = primary_task.await;
            // Drain the observed peer's inbound for the self-authored
            // PeerRemoved { SelfDeparture }. The wire emission is the
            // load-bearing assertion that drove this test.
            let mut saw_departure = false;
            while let Ok(msg) = mesh_observer_rx.try_recv() {
                if let DistributedMessage::ClusterMutation {
    target: _, mutations, .. } = msg {
                    for mutation in mutations {
                        if let ClusterMutation::PeerRemoved {
                            id,
                            cause: RemovalCause::SelfDeparture(reason),
                        } = mutation
                        {
                            assert_eq!(
                                id, "sec-panik",
                                "self-departure must carry the departing node's own id",
                            );
                            assert!(
                                reason.as_str().contains("synthetic-panik-test"),
                                "departure reason must reference the matched path; got: {}",
                                reason.as_str()
                            );
                            saw_departure = true;
                        }
                    }
                }
            }
            assert!(
                saw_departure,
                "mesh never observed a self-authored \
                 ClusterMutation::PeerRemoved {{ SelfDeparture }} — the \
                 secondary's panik arm did not announce its departure"
            );
        })
        .await;
}

/// SIGTERM source: same coordinator harness as the file-source
/// sibling, but the panik signal carries
/// [`crate::panik_watcher::SIGTERM_SENTINEL_PATH`] (the documented
/// sentinel the watcher's SIGTERM arm emits). Assertions are
/// inverted relative to the file-source test:
///   1. `run_until_setup_or_done` STILL returns `RunOutcome::Terminal`
///      (projecting to `SecondaryTerminal::Panik`; the local-teardown side
///      of the panik contract is unchanged), with `matched_path == <SIGTERM>`
///      and the per-host reason string `"panik SIGTERM (per-host)"`.
///   2. NO self-authored `ClusterMutation::PeerRemoved { SelfDeparture }`
///      appears on the primary wire — `handle_panik_signal` skips
///      `apply_and_broadcast_mutations` on the SIGTERM branch. A
///      single host's SIGTERM (e.g. SLURM time-limit hitting one
///      node early) is a purely local event; the mesh stays free to
///      continue / re-elect.
///
/// The wire-emission seam asserted here is the same one the sibling
/// file-source test pins: `apply_and_broadcast_mutations` sends the
/// mutation on BOTH the primary transport AND the peer transport,
/// so the primary-wire absence is sufficient evidence that the call
/// did not fire (rather than fired with a different transport).
///
/// Driven over the PRODUCTION concurrent mesh-pump (`start_secondary_pump`):
/// the pump drains the setup egress so the handshake completes, and would
/// fan any departure broadcast to the observed peer — so the empty
/// `mesh_observer_rx` is genuine evidence the SIGTERM branch broadcast
/// nothing, not a stalled pump.
#[tokio::test(flavor = "current_thread")]
async fn panik_sigterm_source_does_not_broadcast_and_returns_terminal_panik() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let (pri_to_sec_tx, pri_to_sec_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();

            // Channel-backed mesh with the primary folded in + one observed
            // peer, so the test can assert the SIGTERM branch broadcasts
            // NOTHING onto the mesh (nothing lands in `mesh_observer_rx`).
            let (unified, mut mesh_observer_rx) =
                super::super::test_helpers::channel_mesh_with_observed_peer(
                    "sec-panik-sigterm",
                    sec_to_pri_tx,
                    pri_to_sec_rx,
                );

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
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_secs(2),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let mut secondary = make_secondary_channel(config, unified);
            secondary.set_bootstrap_primary_id("primary".to_string());

            let (panik_tx, panik_rx) = tokio::sync::oneshot::channel();
            secondary.register_panik_signal_rx(panik_rx);

            // SIGTERM source — the sentinel path the watcher's
            // SIGTERM arm carries on its `PanikSignal`. Constructed
            // via the watcher's documented constant (NOT a string
            // literal duplicated here) so any future change to the
            // sentinel propagates to this test through the type
            // system rather than silently desynchronising.
            let expected_path =
                std::path::PathBuf::from(crate::panik_watcher::SIGTERM_SENTINEL_PATH);

            // Same settle window + fire pattern as the file-source
            // test — see that test's comment block for rationale on
            // why the signal can't fire before the loop's panik arm
            // is installed via `take()` inside process_tasks.
            let path_for_signal = expected_path.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = panik_tx.send(crate::panik_watcher::PanikSignal {
                    matched_path: path_for_signal,
                    // SIGTERM source carries the sender PID; a synthetic
                    // non-zero PID exercises the sender-carrying branch.
                    sender_pid: Some(12345),
                });
            });

            // Fake primary task: drive the setup handshake, then drain
            // the uplink silently. The SIGTERM branch must not emit any
            // self-departure announcement — we assert that on the mesh
            // recorder after the coordinator returns (the mesh broadcast
            // would necessarily already have been issued, since
            // `handle_panik_signal` returns synchronously after the
            // apply+broadcast call on the file-source branch).
            let sec_id = "sec-panik-sigterm".to_string();
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
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        peers: vec![],
                    })
                    .unwrap();
                to_secondary
                    .send(DistributedMessage::InitialAssignment {
                        target: None,
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
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        total_files: 0,
                        total_bytes: 0,
                    })
                    .unwrap();

                // Drain the uplink silently. The self-departure (if any)
                // would ride the mesh, not the uplink.
                while from_secondary.recv().await.is_some() {}
            });

            // Spawn the production pump so the setup egress drains (handshake
            // completes) and any departure broadcast would reach the observed
            // peer. `_guard` keeps the slot + pump alive for the drive.
            let (mut secondary, _guard) = start_secondary_pump(secondary);

            let mut factory = FakeWorkerFactory;
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect("secondary did not return within budget")
            .expect("secondary returned Err on the SIGTERM panik path");

            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "expected RunOutcome::Terminal on the SIGTERM panik path, got: {outcome:?}"
            );
            match secondary.terminal() {
                Some(SecondaryTerminal::Panik {
                    matched_path,
                    reason,
                }) => {
                    assert_eq!(
                        matched_path, expected_path,
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
                other => panic!("expected SecondaryTerminal::Panik, got: {other:?}"),
            }

            // Assert NO self-authored `PeerRemoved { SelfDeparture }`
            // mutation appeared on the mesh. This is the load-bearing
            // invariant: the SIGTERM branch of `handle_panik_signal`
            // must skip `apply_and_broadcast_mutations` entirely, so a
            // per-host SIGTERM never announces a mesh departure.
            drop(pri_to_sec_tx);
            primary_task.abort();
            let _ = primary_task.await;
            while let Ok(msg) = mesh_observer_rx.try_recv() {
                if let DistributedMessage::ClusterMutation {
    target: _, mutations, .. } = msg {
                    for mutation in mutations {
                        if let ClusterMutation::PeerRemoved {
                            cause: RemovalCause::SelfDeparture(reason),
                            ..
                        } = mutation
                        {
                            panic!(
                                "SIGTERM panik leaked a self-departure PeerRemoved \
                                 onto the mesh (reason: {}); SIGTERM-source \
                                 signals must be local-only",
                                reason.as_str()
                            );
                        }
                    }
                }
            }
        })
        .await;
}
