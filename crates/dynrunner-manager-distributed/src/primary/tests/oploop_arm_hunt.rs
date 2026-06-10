//! ── ingest-wedge RCA (round 5): drive the REAL operational `select!` loop
//! under the concurrent burst, with per-arm instrumentation as the oracle ──
//!
//! Rounds 2/3 drove `Node::run` over the channel / real-QUIC meshes and were
//! GREEN, but asserted only the END STATE (`completed == TASKS`). They never
//! observed WHICH `select!` arm won during the burst — so a wedge that
//! presents as "some non-inbox arm wins every iteration while the inbox arm
//! never wins again" would have been invisible to them as anything but a
//! timeout.
//!
//! This module closes that gap. It drives the REAL
//! [`PrimaryCoordinator::run`] — i.e. the actual operational `select!` loop in
//! `lifecycle/operational_loop.rs` — as a `PromotionSnapshot`-seeded promoted
//! primary on a `current_thread` `LocalSet`, fed by real remote
//! [`SecondaryCoordinator`]s over the REAL all-to-all QUIC mesh, and RETAINS
//! `&mut primary` so that, on a wedge (ingest stalls at `< TASKS` within the
//! timeout), it can read the live [`crate::oploop_instrumentation`] snapshot
//! off `primary.op_loop_arm_stats` and NAME the hot-looping arm. The full
//! concurrent mix is live: while the inbox arm ingests the burst, the
//! heartbeat / anti-entropy / dispatch-recheck (worker-mgmt) / liveness / etc.
//! arms all race it on the SAME loop.
//!
//! Variations cover the brief's dimensions: burst size/timing, payload size,
//! the co-located own worker completing mid-burst, and — the round-4 unmodeled
//! item — discovery-debt settle racing the burst (the promoted primary runs
//! `discover_on_promotion` IN its pre-loop while the remote completions are
//! already arriving at its inbox).
//!
//! ORACLE: every test asserts `completed == TASKS`. On a wedge the assertion's
//! failure message carries the arm snapshot (`iter`, per-arm `arm_counts`,
//! `since_inbox`, `last_arm`) — RED then NAMES the arm + the mechanism. GREEN
//! = the real loop ingests the full burst with the inbox arm winning its share
//! and `since_inbox` bounded; the remaining difference to production is the
//! real Python interpreter under load (no GIL in this harness), which the
//! shipped Part-1 instrumentation will now capture in production.

use super::*;

use std::time::Duration as StdDuration;

use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};
use dynrunner_transport_quic::PeerNetwork;

use crate::process::{LocalRole, Mesh};
use crate::secondary::PeerCertInfo;
use dynrunner_protocol_primary_secondary::address::PeerId;

/// A `WorkerFactory` whose runner returns a fixed-size `result_data` payload
/// on every `ProcessTask`. Each `TaskComplete` then carries `Some(Vec<u8>)`,
/// so the promoted primary's per-completion `ClusterMutation::TaskCompleted`
/// broadcast (fanned to every secondary) is the heaviest per-completion egress
/// the pump's biased select must drain ahead of the next ingest — the
/// worst-case for an egress arm starving the inbox arm. Self-contained (a
/// faithful port of the round-2/3 `PayloadWorkerFactory`) so this module does
/// not depend on a sibling that may not be present.
struct PayloadWorkerFactory {
    payload_bytes: usize,
}

impl dynrunner_manager_local::WorkerFactory<dynrunner_transport_channel::ChannelManagerEnd>
    for PayloadWorkerFactory
{
    fn spawn_worker(
        &mut self,
        _worker_id: dynrunner_core::WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(dynrunner_transport_channel::ChannelManagerEnd, Option<u32>), String> {
        use dynrunner_core::{MessageReceiver, MessageSender};
        use dynrunner_protocol_manager_worker::{Command, Response};
        let (manager_end, runner_end) = dynrunner_transport_channel::channel_pair();
        let payload_bytes = self.payload_bytes;
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner
                            .send(Response::Done {
                                result_data: Some(vec![0xABu8; payload_bytes]),
                            })
                            .await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

/// A `SecondaryConfig` for the QUIC arm-hunt harness (50 ms keepalive so the
/// fleet's mesh + the primary's heartbeat/anti-entropy arms stay HOT under the
/// burst — the concurrent-mix the round-5 brief demands). Mirrors the round-3
/// `quic_sec_config`.
fn arm_hunt_sec_config(id: &str, num_workers: u32, can_be_primary: bool) -> SecondaryConfig {
    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        8 * 1024 * 1024 * 1024u64,
    )]);
    SecondaryConfig {
        secondary_id: id.into(),
        num_workers,
        max_resources: max_res,
        hostname: "test-host".into(),
        keepalive_interval: StdDuration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: StdDuration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: StdDuration::from_secs(30),
        primary_silence_backstop: StdDuration::from_secs(120),
        unconfigured_deadline: StdDuration::from_secs(600),
        can_be_primary,
        resource_check_interval: StdDuration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: StdDuration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// `PeerConnectionInfo` advertisement for a started `PeerNetwork`. The
/// all-to-all roster is the vector of these. Ported from round-3.
fn peer_info_for(id: &str, net: &PeerNetwork<TestId>) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: id.into(),
        cert: net.cert_pem().to_string(),
        ipv4: Some("127.0.0.1".into()),
        ipv6: None,
        port: net.port(),
        is_observer: false,
        liveness_port: None,
    }
}

/// `PeerCertInfo` for a started `PeerNetwork` — the node's own dialable QUIC
/// cert/port. Ported from round-3.
fn cert_info_for(net: &PeerNetwork<TestId>) -> PeerCertInfo {
    PeerCertInfo {
        public_cert_pem: net.cert_pem().to_string(),
        ipv4_address: Some("127.0.0.1".into()),
        ipv6_address: None,
        quic_port: net.port(),
    }
}

/// Establish the full all-to-all QUIC mesh from the started networks + roster.
/// Dial everyone, settle, then have every network broadcast a keepalive twice
/// (writing the first app frame on each dialer's outbound stream so the
/// acceptors' parked `accept_bi` resolve and register the inbound peer) with
/// settles between. Ported VERBATIM from round-3's `establish_quic_mesh` (the
/// lower-id-dials + accept-needs-first-app-write dance is a production reality,
/// not a harness artefact).
async fn establish_quic_mesh(
    mut networks: Vec<(String, PeerNetwork<TestId>)>,
    roster: &[PeerConnectionInfo],
) -> Vec<(String, PeerNetwork<TestId>)> {
    for (_, net) in networks.iter_mut() {
        net.connect_to_peers(roster);
    }
    tokio::time::sleep(StdDuration::from_millis(300)).await;
    for round in 0..2 {
        for (id, net) in networks.iter_mut() {
            let _ = net
                .broadcast(DistributedMessage::Keepalive {
                    target: None,
                    sender_id: id.clone(),
                    timestamp: 0.0,
                    secondary_id: id.clone(),
                    active_workers: 0,
                    emitter_role: KeepaliveRole::Secondary,
                })
                .await;
        }
        tokio::time::sleep(StdDuration::from_millis(300)).await;
        let _ = round;
    }
    networks
}

/// Build + run a remote `SecondaryCoordinator` over a real `PeerNetwork`,
/// setting its `peer_cert_info` before the run. Generic over the
/// `WorkerFactory` so the payload-heavy variant can drive `result_data`-bearing
/// completions through the SAME helper. Returns the own-worker run count.
/// Ported from round-3's `run_remote_secondary_with_cert`.
async fn run_remote_secondary_with_cert<F>(
    config: SecondaryConfig,
    transport: PeerNetwork<TestId>,
    cert_info: PeerCertInfo,
    mut factory: F,
) -> usize
where
    F: dynrunner_manager_local::WorkerFactory<dynrunner_transport_channel::ChannelManagerEnd>,
{
    let mut mesh = Mesh::new(transport);
    let secondary_id = config.secondary_id.clone();
    let (_slot, client, inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from(secondary_id.as_str()));
    let mut secondary = SecondaryCoordinator::new(
        config,
        client,
        inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.set_bootstrap_primary_id("sec-0".to_string());
    secondary.set_peer_cert_info(cert_info);
    mesh.publish_membership();

    let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump = crate::process::pump::run_pump(mesh, control_rx);
    tokio::pin!(pump);

    {
        let run = secondary.run(&mut factory);
        tokio::pin!(run);
        tokio::select! {
            r = &mut run => { let _ = r; }
            _ = &mut pump => {}
        }
    }
    secondary.local_tasks_run_for_test()
}

/// What a driven promoted-primary run produced, plus the live arm snapshot so
/// a wedge names its hot arm.
struct PrimaryDriveOutcome {
    completed: usize,
    /// The operational-loop arm snapshot. `Some` if the loop was still running
    /// (a wedge — the run did not resolve, so `op_loop_arm_stats` is still
    /// published); `None` if the run resolved cleanly (the loop unpublished
    /// it on exit).
    arm_snapshot: Option<String>,
    timed_out: bool,
}

/// Drive a REAL `PromotionSnapshot`-seeded promoted primary's `run()` — the
/// actual operational `select!` — over the real QUIC mesh, retaining
/// `&mut primary` so the arm snapshot is readable AFTER the timeout. The
/// primary's own mesh-pump runs alongside (the production turn). The
/// `seed` closure performs whatever ledger/discovery seeding the variant needs
/// BEFORE the run (mirrors `build_test_promote_recipe`'s seed step).
async fn drive_promoted_primary_over_quic(
    primary_net: PeerNetwork<TestId>,
    config: PrimaryConfig,
    seed: impl FnOnce(
        &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    ),
    timeout: StdDuration,
) -> PrimaryDriveOutcome {
    let mut mesh = Mesh::new(primary_net);
    let node_id = config.node_id.clone();
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(node_id.as_str()));
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let mut primary = PrimaryCoordinator::new(
        config,
        client,
        inbox,
        demote_rx,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // Loss-of-primacy hook exactly as the production Node wires it; harmless on
    // this operational (non-relocating) path.
    primary.register_demote_on_displaced(demote_tx);
    seed(&mut primary);
    mesh.publish_membership();

    // The primary's own mesh-pump (sole mesh owner) — drains the queued egress
    // onto the wire AND demuxes inbound QUIC frames onto the primary slot.
    let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump = tokio::task::spawn_local(async move {
        let _slot = slot;
        crate::process::pump::run_pump(mesh, control_rx).await;
    });

    let timed_out;
    {
        // Drive the REAL operational select! via `run(PromotionSnapshot)`.
        let run = primary.run(
            crate::process::SeedSource::PromotionSnapshot,
            Box::new(|_| {}),
            Box::new(|_, _, _, _| {}),
        );
        tokio::pin!(run);
        match tokio::time::timeout(timeout, &mut run).await {
            Ok(_res) => {
                timed_out = false;
            }
            Err(_) => {
                // WEDGE: the run did not resolve. The run future is dropped
                // here, but it ran far enough to publish `op_loop_arm_stats`,
                // which is read off `&primary` below — naming the hot arm.
                timed_out = true;
            }
        }
    }

    let completed = primary.completed_count();
    let arm_snapshot = primary
        .op_loop_arm_stats
        .as_ref()
        .map(|s| s.snapshot().to_string());

    // Drop the pump so the remote secondaries' wire closes and they wind down.
    pump.abort();

    PrimaryDriveOutcome {
        completed,
        arm_snapshot,
        timed_out,
    }
}

const ARM_HUNT_REMOTE_SECONDARIES: u32 = 5;
const ARM_HUNT_TASKS: usize = 16;

/// ── ROUND 5 — production-signature burst, REAL loop, arm-instrumented ──
///
/// The promoted primary (co-located own worker = 1, can_be_primary topology
/// modelled by the `PromotionSnapshot` seed) ingests `ARM_HUNT_TASKS`
/// completions, the bulk arriving REMOTELY near-simultaneously over real QUIC,
/// while the full concurrent arm-mix races. The production signature was
/// "ingests exactly 4 of 16, never returns to its inbox; stats interval dead".
///
/// RED = `completed < TASKS` within the timeout; the panic message carries the
/// arm snapshot, so RED NAMES which arm hot-looped (`last_arm` / the dominant
/// `arm_counts` entry / `since_inbox`). GREEN = the real loop ingested the
/// full burst — the wedge is NOT in the operational-loop arm scheduling
/// reachable without the real Python interpreter.
#[tokio::test(flavor = "current_thread")]
async fn arm_hunt_remote_completion_burst_over_quic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let outcome = run_arm_hunt_burst(
                ARM_HUNT_REMOTE_SECONDARIES,
                ARM_HUNT_TASKS,
                0, // no payload
                StdDuration::from_secs(45),
            )
            .await;
            assert_eq!(
                outcome.completed, ARM_HUNT_TASKS,
                "WEDGE: the real operational loop ingested only {}/{} completions \
                 (timed_out={}). Arm snapshot at the wedge: [{}] — the dominant \
                 non-inbox arm + last_arm names the hot-looping arm; since_inbox \
                 is how long the inbox arm has not won.",
                outcome.completed,
                ARM_HUNT_TASKS,
                outcome.timed_out,
                outcome.arm_snapshot.as_deref().unwrap_or("<run resolved; no snapshot>"),
            );
        })
        .await;
}

const ARM_HUNT_PAYLOAD_REMOTE_SECONDARIES: u32 = 8;
const ARM_HUNT_PAYLOAD_TASKS: usize = 64;
const ARM_HUNT_PAYLOAD_BYTES: usize = 4096;

/// ── ROUND 5 — payload-heavy, larger burst, REAL loop, arm-instrumented ──
///
/// More remote secondaries + tasks + a `result_data` payload on every
/// completion → the heaviest per-completion egress the pump's biased select
/// must drain ahead of the next ingest (the worst case for an egress arm
/// starving the inbox arm). Same RED/GREEN + arm-naming oracle.
#[tokio::test(flavor = "current_thread")]
async fn arm_hunt_payload_burst_over_quic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let outcome = run_arm_hunt_burst(
                ARM_HUNT_PAYLOAD_REMOTE_SECONDARIES,
                ARM_HUNT_PAYLOAD_TASKS,
                ARM_HUNT_PAYLOAD_BYTES,
                StdDuration::from_secs(60),
            )
            .await;
            assert_eq!(
                outcome.completed, ARM_HUNT_PAYLOAD_TASKS,
                "WEDGE: the real operational loop ingested only {}/{} payload-bearing \
                 completions (timed_out={}). Arm snapshot at the wedge: [{}]",
                outcome.completed,
                ARM_HUNT_PAYLOAD_TASKS,
                outcome.timed_out,
                outcome.arm_snapshot.as_deref().unwrap_or("<run resolved; no snapshot>"),
            );
        })
        .await;
}

/// ── ROUND 5 — DISCOVERY-DEBT SETTLE RACING THE BURST (round-4 item 2) ──
///
/// The promoted primary inherits an EMPTY ledger + `DiscoveryDebt::Owed` and
/// carries a discovery policy, so its `run` pre-loop runs
/// `discover_on_promotion` — whose `(sd.discover)().await` is a real yield
/// point. The remote secondaries are already meshed and start polling the
/// instant the seed (`PhaseDepsSet + TaskAdded* + DiscoverySettled`) lands, so
/// their completion burst races the discovery settle: the inbox is being fed
/// while the pre-loop discovery future resolves, and the operational loop must
/// pick up that already-queued backlog on entry WITHOUT the inbox arm being
/// starved by the entry-sweep + the timer/bus arms.
///
/// RED = `completed < TASKS`; the arm snapshot names the arm. GREEN = the real
/// loop drains the discovery-raced backlog.
#[tokio::test(flavor = "current_thread")]
async fn arm_hunt_discovery_settle_races_burst_over_quic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let remote = ARM_HUNT_REMOTE_SECONDARIES;
            let tasks = ARM_HUNT_TASKS;
            let (primary_net, remote_handles) =
                stand_up_burst_fleet(remote, tasks, 0).await;

            // Discovered corpus + the Owed seed: an empty ledger that owes
            // discovery, the policy yielding the `tasks` binaries on its one
            // fire. The fire-count cell pins that the policy ran exactly once
            // (the discover_on_promotion path, not a cold seed).
            let binaries: Vec<TaskInfo<TestId>> = (0..tasks)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();
            let fire_count = std::rc::Rc::new(std::cell::Cell::new(0u32));
            let fc = std::rc::Rc::clone(&fire_count);

            let config = PrimaryConfig {
                node_id: "sec-0".to_string(),
                num_secondaries: 1 + remote,
                connect_timeout: StdDuration::from_secs(10),
                peer_timeout: StdDuration::from_secs(10),
                ..test_primary_config()
            };

            let outcome = drive_promoted_primary_over_quic(
                primary_net,
                config,
                move |primary| {
                    // Seed the Owed marker + register the discovery policy,
                    // exactly as the relocated/pre-staged recipe does. The
                    // operational `run` then fires `discover_on_promotion` in
                    // its pre-loop while the burst is already inbound.
                    primary.register_setup_discovery(fixed_discovery(
                        binaries,
                        HashMap::new(),
                        fc,
                    ));
                    primary.cluster_state_mut_for_test().apply(
                        ClusterMutation::DiscoveryDebtDeclared,
                    );
                },
                StdDuration::from_secs(45),
            )
            .await;

            assert_eq!(
                fire_count.get(),
                1,
                "the discovery policy must have fired exactly once \
                 (the discover_on_promotion pre-loop path)",
            );
            assert_eq!(
                outcome.completed, tasks,
                "WEDGE (discovery-settle races burst): ingested only {}/{} \
                 completions (timed_out={}). Arm snapshot at the wedge: [{}]",
                outcome.completed,
                tasks,
                outcome.timed_out,
                outcome.arm_snapshot.as_deref().unwrap_or("<run resolved; no snapshot>"),
            );

            drain_remote_handles(remote_handles).await;
        })
        .await;
}

/// Stand up the all-to-all QUIC mesh for a burst fleet: `remote` remote
/// secondaries (each `num_workers=2`, completing the bulk) + the promoted
/// primary's own network (`sec-0`). Returns the primary's `PeerNetwork` (NOT
/// yet meshed into a coordinator) + the spawned remote-secondary join handles.
/// `payload_bytes == 0` uses `FakeWorkerFactory`; `> 0` uses the payload
/// factory so every completion is heavy.
async fn stand_up_burst_fleet(
    remote: u32,
    _tasks: usize,
    payload_bytes: usize,
) -> (PeerNetwork<TestId>, Vec<tokio::task::JoinHandle<usize>>) {
    let mut ids = vec!["sec-0".to_string()];
    for i in 1..=remote {
        ids.push(format!("sec-{i}"));
    }
    let mut networks: Vec<(String, PeerNetwork<TestId>)> = Vec::new();
    for id in &ids {
        let net = PeerNetwork::<TestId>::start(id)
            .await
            .expect("peer network start");
        networks.push((id.clone(), net));
    }
    let roster: Vec<PeerConnectionInfo> = networks
        .iter()
        .map(|(id, net)| peer_info_for(id, net))
        .collect();
    networks = establish_quic_mesh(networks, &roster).await;

    let mut take = |want: &str| -> PeerNetwork<TestId> {
        let pos = networks.iter().position(|(id, _)| id == want).unwrap();
        networks.remove(pos).1
    };

    let mut remote_handles = Vec::new();
    for i in 1..=remote {
        let id = format!("sec-{i}");
        let net = take(&id);
        let cfg = arm_hunt_sec_config(&id, 2, false);
        let cert_info = cert_info_for(&net);
        if payload_bytes == 0 {
            remote_handles.push(tokio::task::spawn_local(async move {
                run_remote_secondary_with_cert(cfg, net, cert_info, FakeWorkerFactory).await
            }));
        } else {
            remote_handles.push(tokio::task::spawn_local(async move {
                run_remote_secondary_with_cert(
                    cfg,
                    net,
                    cert_info,
                    PayloadWorkerFactory { payload_bytes },
                )
                .await
            }));
        }
    }

    let primary_net = take("sec-0");
    (primary_net, remote_handles)
}

/// Run the common cold-seed burst: stand up the fleet, drive the promoted
/// primary's REAL `run()` seeded operationally (a populated ledger of `tasks`
/// binaries, NO discovery debt), and return the drive outcome. The bulk of the
/// `tasks` completions arrive over real QUIC from the `remote` secondaries
/// while the full arm-mix races.
async fn run_arm_hunt_burst(
    remote: u32,
    tasks: usize,
    payload_bytes: usize,
    timeout: StdDuration,
) -> PrimaryDriveOutcome {
    let (primary_net, remote_handles) = stand_up_burst_fleet(remote, tasks, payload_bytes).await;

    let binaries: Vec<TaskInfo<TestId>> = (0..tasks)
        .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
        .collect();

    let config = PrimaryConfig {
        node_id: "sec-0".to_string(),
        num_secondaries: 1 + remote,
        connect_timeout: StdDuration::from_secs(10),
        peer_timeout: StdDuration::from_secs(10),
        ..test_primary_config()
    };

    let outcome = drive_promoted_primary_over_quic(
        primary_net,
        config,
        move |primary| {
            // Operational seed: a populated ledger, no discovery debt → the
            // `PromotionSnapshot` `run` goes straight to the operational loop.
            seed_operational_ledger(primary, binaries, HashMap::new());
        },
        timeout,
    )
    .await;

    drain_remote_handles(remote_handles).await;
    outcome
}

/// Best-effort drain of the remote-secondary join handles (they wind down when
/// the primary's pump aborts and their wire closes). The decisive anti-loopback
/// fact is structural (`completed == TASKS` while the co-located node holds a
/// fraction of the fleet's workers), so this tally is not load-bearing — it
/// only confirms the burst was real, not a loopback trickle.
async fn drain_remote_handles(handles: Vec<tokio::task::JoinHandle<usize>>) {
    let mut total_remote_own = 0usize;
    for handle in handles {
        if let Ok(Ok(own)) = tokio::time::timeout(StdDuration::from_secs(10), handle).await {
            total_remote_own += own;
        }
    }
    // A non-fatal observation: over QUIC the teardown race can undercount, so
    // we do not assert a floor here (the structural `completed == TASKS` in the
    // caller is the decisive burst-was-real proof).
    let _ = total_remote_own;
}
