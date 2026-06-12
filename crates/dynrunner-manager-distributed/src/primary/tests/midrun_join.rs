//! Mid-run joiner setup serve — the respawn-replacement zombie replay.
//!
//! Production shape (run_20260612_045106): replacement secondary-4,
//! respawned MID-RUN for a dead member, joined membership (welcome
//! routed + relayed to the primary), but the setup trio's run-start
//! halves (`InitialAssignment` / `TransferComplete`) flow only in the
//! run-start batch — which had already fired. The replacement parked in
//! `wait_for_setup` forever (got_assignment/got_transfer never true),
//! re-sent its handshake on the retry cadence, never emitted
//! `MeshReady`, and the primary held it "unassignable until its mesh
//! leg confirms" — a permanently half-joined zombie holding a SLURM
//! slot.
//!
//! Pinned here:
//! 1. End-to-end at the manager level: with the run started, a FRESH
//!    member welcoming through the real dispatch path is served the
//!    full trio, exits `wait_for_setup`, emits `MeshReady` (observed in
//!    the primary's confirmation set — the #449 dispatch gate's input),
//!    becomes assignable, and completes work.
//! 2. The per-member serve: a post-run-start cert-exchange edge sends
//!    the trio (roster + EMPTY `InitialAssignment` + `TransferComplete`)
//!    and walks the member's typestate to `Operational` (keepalive
//!    seeded at the same instant the batch path seeds).
//! 3. Once-per-incarnation: a duplicate welcome/cert-exchange (the
//!    load-bearing handshake retry) does NOT double-serve.
//! 4. Bring-up unchanged: while the run has NOT started, the cert edge
//!    serves the roster only (negative control; the connect-wait
//!    governance tests live in `incremental_setup`).
//!
//! REVERT-CHECK: drop the `run_start_batch_fired` branch from
//! `serve_setup_on_cert_exchange` and tests 1–2 go RED exactly the
//! production way — the joiner receives only the roster, never exits
//! `wait_for_setup`, and the run completes without it ever running a
//! task.

use super::*;

use dynrunner_protocol_primary_secondary::{Destination, MessageType};

fn welcome_frame(id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::SecondaryWelcome {
        target: Some(Destination::Primary),
        sender_id: id.into(),
        timestamp: 0.0,
        secondary_id: id.into(),
        resources: vec![dynrunner_core::ResourceAmount {
            kind: dynrunner_core::ResourceKind::memory(),
            amount: 1024 * 1024 * 1024,
        }],
        worker_count: 1,
        hostname: "test-host".into(),
        is_observer: false,
        can_be_primary: true,
    }
}

fn cert_frame(id: &str, port: u16) -> DistributedMessage<TestId> {
    DistributedMessage::CertExchange {
        target: Some(Destination::Primary),
        sender_id: id.into(),
        timestamp: 0.0,
        secondary_id: id.into(),
        public_cert_pem: format!("CERT-{id}"),
        ipv4_address: Some("10.0.0.1".into()),
        ipv6_address: None,
        quic_port: port,
        liveness_port: None,
    }
}

/// Drain everything currently queued on a secondary's inbound channel.
fn drain(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<DistributedMessage<TestId>> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        out.push(msg);
    }
    out
}

/// Let inbound frames travel wire → pump → inbox → handlers → queued
/// egress → pump → outboxes (same shape as `incremental_setup`).
async fn settle() {
    settle_pump().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    settle_pump().await;
}

/// Count the run-start halves in `frames`, asserting the
/// `InitialAssignment`s are EMPTY (a mid-run joiner holds no
/// pre-assigned work).
fn run_start_halves(frames: &[DistributedMessage<TestId>]) -> (usize, usize) {
    let mut assignments = 0;
    let mut transfers = 0;
    for m in frames {
        match m {
            DistributedMessage::InitialAssignment {
                zip_files,
                workers_ready,
                staged_files,
                ..
            } => {
                assert!(
                    zip_files.is_empty() && workers_ready.is_empty() && staged_files.is_empty(),
                    "a mid-run joiner's InitialAssignment must be EMPTY \
                     (it pulls via TaskRequest; nothing to clobber)"
                );
                assignments += 1;
            }
            m if matches!(m.msg_type(), MessageType::TransferComplete) => transfers += 1,
            _ => {}
        }
    }
    (assignments, transfers)
}

/// Tests 2–4: the per-member serve at the cert-exchange edge, driven
/// through the direct handlers (the same edge `dispatch_message` drives
/// in the operational loop's inbound arm).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn midrun_cert_edge_serves_trio_once_per_incarnation_and_bringup_stays_roster_only() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (id0, mut rx0, _tx0) = ends.remove(0);

            let config = PrimaryConfig {
                num_secondaries: 1,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // ── 4: bring-up negative control — run NOT started, the cert
            //      edge serves the roster only. ──
            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            let mut cert = cert_frame(&id0, 5000);
            cert.clear_target();
            primary.handle_cert_exchange(cert).await;
            settle().await;
            let bringup = drain(&mut rx0);
            assert_eq!(
                run_start_halves(&bringup),
                (0, 0),
                "before the run starts the cert edge must serve the roster \
                 ONLY (the quorum-proceed policy governs the run start)"
            );
            assert!(
                matches!(
                    primary.secondaries.get(&id0),
                    Some(crate::state::SecondaryConnectionState::PeerDiscovery(_))
                ),
                "a bring-up member stays on the batch walk (PeerDiscovery)"
            );

            // ── 2: run started — a fresh incarnation's cert edge serves
            //      the full trio and walks it Operational. Model the
            //      production shape: the member died (purged) and its
            //      replacement welcomes after the run-start batch fired. ──
            primary.secondaries.remove(&id0);
            primary.run_start_batch_fired = true;

            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            let mut cert = cert_frame(&id0, 5001);
            cert.clear_target();
            primary.handle_cert_exchange(cert).await;
            settle().await;
            let served = drain(&mut rx0);
            assert!(
                served
                    .iter()
                    .any(|m| matches!(m.msg_type(), MessageType::PeerInfo)),
                "the mid-run joiner must receive its peer roster"
            );
            assert_eq!(
                run_start_halves(&served),
                (1, 1),
                "the mid-run joiner must be served the run-start halves \
                 (empty InitialAssignment + TransferComplete) at its cert \
                 edge — got frames {:?}",
                served.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert!(
                matches!(
                    primary.secondaries.get(&id0),
                    Some(crate::state::SecondaryConnectionState::Operational(_))
                ),
                "the mid-run serve must walk the member to Operational on \
                 the primary side (the mark_member_operational edge)"
            );

            // ── 3: duplicate handshake (the load-bearing welcome retry)
            //      must NOT double-serve the run-start halves. ──
            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            let mut cert = cert_frame(&id0, 5001);
            cert.clear_target();
            primary.handle_cert_exchange(cert).await;
            settle().await;
            let retried = drain(&mut rx0);
            assert_eq!(
                run_start_halves(&retried),
                (0, 0),
                "a duplicate welcome/cert-exchange from the same incarnation \
                 must not re-serve the run-start halves (once-per-incarnation) \
                 — got frames {:?}",
                retried.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert!(
                matches!(
                    primary.secondaries.get(&id0),
                    Some(crate::state::SecondaryConnectionState::Operational(_))
                ),
                "the duplicate handshake must not regress the walked member"
            );
        })
        .await;
}

/// Test 1: the production zombie, replayed end-to-end at the manager
/// level — the run_20260612_045106 shape verbatim: a member DIES
/// mid-run, and its respawned REPLACEMENT (a real `SecondaryCoordinator`
/// running the full `wait_for_setup`) welcomes through the real dispatch
/// path only after the run-start batch has long fired.
///
/// The replacement's wire exists at the transport level (the #454
/// relay/redial already heals the path itself); what is under test is
/// the MANAGER-level serve: the replacement must receive the trio, exit
/// `wait_for_setup`, emit `MeshReady` (observed in the primary's
/// confirmation set — the #449 gate input), become assignable, and
/// complete the dead member's remaining work. Pre-fix it parks in
/// `wait_for_setup` forever and the run never finishes (the work died
/// with sec-0).
#[tokio::test(flavor = "current_thread")]
async fn midrun_joiner_exits_setup_goes_meshready_and_completes_work() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                2 * 1024 * 1024 * 1024u64,
            )]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();

            // The doomed bring-up member sec-0: 1 worker, SLOW (250ms per
            // task) so most of the corpus is still pending when it dies.
            let (to_sec0_tx, from_sec0_rx, sec0_handle) = spawn_real_secondary_slow(
                "sec-0".into(),
                1,
                max_res.clone(),
                vec![("bin_".into(), Duration::from_millis(250))],
            );
            outgoing.insert("sec-0".to_string(), to_sec0_tx);
            {
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = from_sec0_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }

            // The replacement's wire (transport-level link) exists from
            // the start; the replacement PROCESS spawns mid-run in the
            // driver below.
            let (joiner_wire_tx, joiner_wire_rx) = tokio_mpsc::unbounded_channel();
            outgoing.insert("sec-1".to_string(), joiner_wire_tx);

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                // Only sec-0 is expected at bring-up; the replacement is a
                // mid-run arrival the connect wait knows nothing about.
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                // Fast death detection so the dead member is judged,
                // requeued, and purged well before the replacement
                // welcomes (hard window: 8 × 100ms = 800ms of silence).
                keepalive_interval: Duration::from_millis(100),
                silence_warn_multiples: vec![2, 4, 6],
                silence_hard_multiple: 8,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..8)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            // Driver: kill sec-0 mid-corpus (its WHOLE node task — pump,
            // coordinator, workers — aborts, the production process-death
            // shape), wait out the silence judgment, then bring up the
            // REAL replacement. Deliberately yields the replacement's
            // JoinHandle: it must outlive the primary's run (awaited only
            // after the primary drops and the wire closes).
            #[allow(clippy::async_yields_async)]
            let driver = async {
                // ~2 completions in (run start is immediate over
                // channels; 250ms per task).
                tokio::time::sleep(Duration::from_millis(600)).await;
                sec0_handle.abort();
                // Past the 800ms hard silence window + sweep cadence:
                // sec-0 is declared dead, requeued, and purged.
                tokio::time::sleep(Duration::from_millis(2000)).await;
                // A fresh process has no history: discard anything that
                // was broadcast onto the wire before the replacement
                // existed.
                let mut joiner_wire_rx = joiner_wire_rx;
                while joiner_wire_rx.try_recv().is_ok() {}
                let (to_joiner_tx, from_joiner_rx, joiner_handle) =
                    spawn_real_secondary("sec-1".into(), 2, max_res.clone());
                tokio::task::spawn_local(async move {
                    while let Some(msg) = joiner_wire_rx.recv().await {
                        if to_joiner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = from_joiner_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
                joiner_handle
            };

            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, binaries, deps);
            let (run_res, joiner_handle) =
                tokio::join!(primary.run(SeedSource::PromotionSnapshot, ops, ope), driver);
            run_res.unwrap();

            assert_eq!(
                primary.completed_count(),
                8,
                "all tasks must complete — the replacement must pick up the \
                 dead member's remaining corpus"
            );
            assert_eq!(primary.failed_count(), 0);

            // #449 composition: the replacement's MeshReady (emitted on
            // entering its operational loop) reached the primary, so the
            // dispatch gate's confirmation set names it — it was
            // assignable, not vetoed as half-joined.
            assert!(
                primary.mesh_ready_secondaries.contains("sec-1"),
                "the replacement must emit MeshReady once served the trio \
                 (it never exits wait_for_setup without the run-start halves) \
                 — confirmed set: {:?}",
                primary.mesh_ready_secondaries
            );
            // #443 composition: the replacement's operational keepalives
            // flowed (the emitter spins up only post-`wait_for_setup`),
            // so it is silence-judgeable like any member.
            assert!(
                primary.keepalive_proven.contains("sec-1"),
                "the replacement's operational keepalive must be proven \
                 (it keepalives only once operational)"
            );

            // Drop the primary to close the wire so the replacement exits.
            drop(primary);
            let joiner_ran = joiner_handle.await.unwrap();
            assert!(
                joiner_ran >= 1,
                "the replacement must become assignable and complete work \
                 (ran {joiner_ran}) — a zombie replacement strands the dead \
                 member's corpus"
            );
        })
        .await;
}
