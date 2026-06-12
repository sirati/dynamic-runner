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
//! 3. Re-serve on duplicate welcome: a duplicate welcome (the
//!    load-bearing handshake retry — it persists until the WHOLE trio
//!    has landed) from a member WITHOUT operational proof re-serves the
//!    trio (the retransmission path for a served-but-lost frame); a
//!    PROVEN-operational member's straggler duplicate does not, and a
//!    duplicate cert-exchange alone never re-runs the edge serve
//!    (once-per-incarnation there).
//! 4. Bring-up: while the run has NOT started, the cert edge (and any
//!    duplicate-welcome re-serve) serves the roster only (negative
//!    control; the connect-wait governance tests live in
//!    `incremental_setup`).
//! 5. The run_20260612_105712 delivery race: a mid-run joiner whose
//!    transport leg NEVER registers on the primary (so every
//!    `Destination::All` broadcast misses it) still receives its
//!    `PeerInfo` — the roster is served DIRECTED, on the same
//!    relay-capable class as the trio's other two frames — and reaches
//!    keepalives/MeshReady/assignability.
//!
//! REVERT-CHECK: drop the `run_start_batch_fired` branch from
//! `serve_setup_on_cert_exchange` and tests 1–2 go RED exactly the
//! production way — the joiner receives only the roster, never exits
//! `wait_for_setup`, and the run completes without it ever running a
//! task. Drop the directed `send_peer_roster_to` from the serve and
//! test 5 goes RED the run_20260612_105712 way — the joiner holds
//! `got_assignment`/`got_transfer` but never `got_peer_info`.

use super::*;

use dynrunner_protocol_primary_secondary::{Destination, KeepaliveRole, MessageType};

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

/// Count the `PeerInfo` frames in `frames`.
fn peer_infos(frames: &[DistributedMessage<TestId>]) -> usize {
    frames
        .iter()
        .filter(|m| matches!(m.msg_type(), MessageType::PeerInfo))
        .count()
}

/// Tests 2–4: the per-member serve at the cert-exchange edge and the
/// duplicate-welcome re-serve, driven through the direct handlers (the
/// same edges `dispatch_message` drives in the operational loop's
/// inbound arm).
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

            // ── 4b: bring-up duplicate welcome (unproven member) — the
            //      re-serve is ROSTER-ONLY (directed): the quorum-proceed
            //      policy still governs the run start. ──
            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            settle().await;
            let bringup_dup = drain(&mut rx0);
            assert!(
                peer_infos(&bringup_dup) >= 1,
                "a pre-run-start duplicate welcome from an unproven member \
                 must re-send its roster DIRECTED (the lost-broadcast \
                 retransmission), got frames {:?}",
                bringup_dup.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert_eq!(
                run_start_halves(&bringup_dup),
                (0, 0),
                "the run-start halves must NOT flow before the run starts, \
                 re-serve or not"
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

            // ── 3a: duplicate welcome from the served-but-UNPROVEN member
            //      (the production wedge: its gate never released, so its
            //      handshake retry keeps re-welcoming) — the trio must be
            //      RE-SERVED: directed roster + the run-start halves. ──
            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            settle().await;
            let retried = drain(&mut rx0);
            assert!(
                peer_infos(&retried) >= 1,
                "a duplicate welcome from an unproven member must re-send \
                 its roster (the lost-PeerInfo retransmission — \
                 run_20260612_105712), got frames {:?}",
                retried.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert_eq!(
                run_start_halves(&retried),
                (1, 1),
                "a duplicate welcome from an unproven member must re-serve \
                 the run-start halves — got frames {:?}",
                retried.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert!(
                matches!(
                    primary.secondaries.get(&id0),
                    Some(crate::state::SecondaryConnectionState::Operational(_))
                ),
                "the duplicate handshake must not regress the walked member"
            );

            // ── 3b: duplicate CERT-EXCHANGE alone never re-runs the edge
            //      serve (once-per-incarnation at that edge; the welcome
            //      is the retransmit-request frame). ──
            let mut cert = cert_frame(&id0, 5001);
            cert.clear_target();
            primary.handle_cert_exchange(cert).await;
            settle().await;
            let cert_dup = drain(&mut rx0);
            assert_eq!(
                (peer_infos(&cert_dup), run_start_halves(&cert_dup)),
                (0, (0, 0)),
                "a duplicate cert-exchange must not re-serve — got frames {:?}",
                cert_dup.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );

            // ── 3c: once the member's operational loop PROVABLY runs (a
            //      secondary-role keepalive landed), a straggler duplicate
            //      welcome is NOT re-served — re-serving would clear the
            //      proof (`mark_member_operational`'s keepalive re-seed)
            //      and regress the silence sweep's judgment bound. ──
            let keepalive = DistributedMessage::Keepalive {
                target: None,
                sender_id: id0.clone(),
                timestamp: 0.0,
                secondary_id: id0.clone(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            primary.note_secondary_keepalive_frame(&keepalive);
            let mut welcome = welcome_frame(&id0);
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            settle().await;
            let proven_dup = drain(&mut rx0);
            assert_eq!(
                (peer_infos(&proven_dup), run_start_halves(&proven_dup)),
                (0, (0, 0)),
                "a duplicate welcome from a PROVEN-operational member must \
                 not be re-served — got frames {:?}",
                proven_dup.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
            );
            assert!(
                primary.keepalive_proven.contains(&id0),
                "the suppressed re-serve must leave the operational proof \
                 intact"
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

/// Test 5: the run_20260612_105712 DELIVERY race, replayed end-to-end —
/// a mid-run joiner whose transport leg NEVER registers on the primary.
///
/// Topology (the production shape): the primary's transport holds a leg
/// to the live sibling only, so every `Destination::All` broadcast
/// misses the joiner for its whole life (the broadcast fans over the
/// legs registered at that instant, and nothing retransmits a missed
/// broadcast). The joiner reaches the primary directly (its welcome DID
/// land in production), and the sibling holds a direct leg to the
/// joiner — the relay path the Router forwards directed frames over,
/// exactly the "QUIC links from secondaries 2+3 established" evidence.
///
/// Production outcome (pre-fix): InitialAssignment + TransferComplete
/// arrived (directed, relayed), the PeerInfo broadcast vanished, and
/// the joiner sat at `got_peer_info=false` for 300s — zero keepalives,
/// silence-judged dead at ~124s. With the roster served DIRECTED (same
/// delivery class as the other two trio frames) the joiner must
/// complete its gate, emit MeshReady, prove its keepalives, and run
/// work.
///
/// REVERT-CHECK: drop `send_peer_roster_to` from
/// `serve_setup_on_cert_exchange` and this goes RED exactly the
/// production way (MeshReady/keepalive-proof assertions fail; the
/// joiner never runs a task).
#[tokio::test(flavor = "current_thread")]
async fn midrun_joiner_unregistered_leg_still_receives_peerinfo_directed() {
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

            // The joiner's channel ends + transport exist up-front
            // (channels are inert until pumped); its coordinator spawns
            // mid-run in the driver. Its inbox sender is handed ONLY to
            // the sibling below — never to the primary's transport — so
            // the broadcast set never contains the joiner.
            let (joiner_inbox_tx, joiner_to_pri_rx, joiner_transport) =
                channel_mesh_secondary_ends("sec-joiner");
            {
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = joiner_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }

            // The live sibling: SLOW (250ms per task, 1 worker) so the
            // corpus outlasts the join, with a direct leg to the joiner —
            // the relay forwarder for the primary's directed frames AND
            // the peer keepalive source the joiner's mesh-formation
            // report keys on (production: the joiner↔sibling QUIC legs
            // that were up while the primary leg was not). Tight
            // keepalives so the mesh-formed signal lands on the test's
            // clock, not the production 60s tempo.
            let mut sibling_config =
                real_secondary_config("sec-sibling".into(), 1, max_res.clone());
            sibling_config.keepalive_interval = Duration::from_millis(100);
            let (to_sibling_tx, from_sibling_rx, _sibling_handle) = spawn_real_secondary_node(
                sibling_config,
                SlowFakeWorkerFactory::with_markers(vec![(
                    "bin_".into(),
                    Duration::from_millis(250),
                )]),
                vec![("sec-joiner".into(), joiner_inbox_tx)],
            );
            outgoing.insert("sec-sibling".to_string(), to_sibling_tx);
            {
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = from_sibling_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                // Only the sibling is expected at bring-up; the joiner is
                // a mid-run arrival the connect wait knows nothing about.
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
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

            // Driver: with the run underway, spawn the joiner. Its leg on
            // the primary stays unregistered FOREVER — strictly harder
            // than the production "delayed past the broadcast".
            #[allow(clippy::async_yields_async)]
            let driver = async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                let mut config = real_secondary_config("sec-joiner".into(), 2, max_res.clone());
                // Tight keepalive: the joiner's mesh-formation check runs
                // on its keepalive tick, and its own keepalives are what
                // the primary's `keepalive_proven` records.
                config.keepalive_interval = Duration::from_millis(100);
                tokio::task::spawn_local(run_secondary_node(
                    config,
                    joiner_transport,
                    FakeWorkerFactory,
                ))
            };

            let (deps, ops, ope) = noop_phase_args();
            seed_operational_ledger(&mut primary, binaries, deps);
            let (run_res, joiner_handle) =
                tokio::join!(primary.run(SeedSource::PromotionSnapshot, ops, ope), driver);
            run_res.unwrap();

            assert_eq!(primary.completed_count(), 8, "all tasks must complete");
            assert_eq!(primary.failed_count(), 0);

            // The joiner exited `wait_for_setup` (⇒ its PeerInfo arrived
            // despite missing every broadcast) and confirmed its mesh leg.
            assert!(
                primary.mesh_ready_secondaries.contains("sec-joiner"),
                "the joiner must receive its roster DIRECTED and reach \
                 MeshReady — a broadcast-only roster never reaches a member \
                 whose leg registration lost the race (confirmed set: {:?})",
                primary.mesh_ready_secondaries
            );
            // Its operational keepalives flowed — the exact signal whose
            // absence got the production replacement silence-judged dead.
            assert!(
                primary.keepalive_proven.contains("sec-joiner"),
                "the joiner's operational keepalive must be proven \
                 (production: zero keepalives, declared dead at 124.45s)"
            );

            // Drop the primary to close the wires so the joiner exits.
            drop(primary);
            let joiner_ran = joiner_handle.await.unwrap();
            assert!(
                joiner_ran >= 1,
                "the joiner must become assignable and complete work \
                 (ran {joiner_ran})"
            );
        })
        .await;
}
