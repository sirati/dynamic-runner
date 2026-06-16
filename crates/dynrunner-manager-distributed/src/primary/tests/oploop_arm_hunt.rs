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
                    // Test fixtures ignore consumer custom messages.
                    Some(Command::Custom { .. }) => {}
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
        phase_status_log_intervals: vec![Duration::from_secs(60)],
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
        slurm_job_id: None,
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
    seed: impl FnOnce(&mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>),
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
            crate::process::SeedSource::PromotionSnapshot {
                kind: crate::process::BootstrapKind::Failover,
            },
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
                outcome.completed,
                ARM_HUNT_TASKS,
                "WEDGE: the real operational loop ingested only {}/{} completions \
                 (timed_out={}). Arm snapshot at the wedge: [{}] — the dominant \
                 non-inbox arm + last_arm names the hot-looping arm; since_inbox \
                 is how long the inbox arm has not won.",
                outcome.completed,
                ARM_HUNT_TASKS,
                outcome.timed_out,
                outcome
                    .arm_snapshot
                    .as_deref()
                    .unwrap_or("<run resolved; no snapshot>"),
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
                outcome.completed,
                ARM_HUNT_PAYLOAD_TASKS,
                "WEDGE: the real operational loop ingested only {}/{} payload-bearing \
                 completions (timed_out={}). Arm snapshot at the wedge: [{}]",
                outcome.completed,
                ARM_HUNT_PAYLOAD_TASKS,
                outcome.timed_out,
                outcome
                    .arm_snapshot
                    .as_deref()
                    .unwrap_or("<run resolved; no snapshot>"),
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
            let (primary_net, remote_handles) = stand_up_burst_fleet(remote, tasks, 0).await;

            // Discovered corpus + the Owed seed: an empty ledger that owes
            // discovery, the policy yielding the `tasks` binaries on its one
            // fire. The fire-count cell pins that the policy ran exactly once
            // (the discover_on_promotion path, not a cold seed).
            let binaries: Vec<TaskInfo<TestId>> = (0..tasks)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();
            let fire_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let fc = std::sync::Arc::clone(&fire_count);

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
                    primary.register_setup_discovery(fixed_discovery(binaries, HashMap::new(), fc));
                    primary
                        .cluster_state_mut_for_test()
                        .apply(ClusterMutation::DiscoveryDebtDeclared);
                },
                StdDuration::from_secs(45),
            )
            .await;

            assert_eq!(
                fire_count.load(std::sync::atomic::Ordering::Relaxed),
                1,
                "the discovery policy must have fired exactly once \
                 (the discover_on_promotion pre-loop path)",
            );
            assert_eq!(
                outcome.completed,
                tasks,
                "WEDGE (discovery-settle races burst): ingested only {}/{} \
                 completions (timed_out={}). Arm snapshot at the wedge: [{}]",
                outcome.completed,
                tasks,
                outcome.timed_out,
                outcome
                    .arm_snapshot
                    .as_deref()
                    .unwrap_or("<run resolved; no snapshot>"),
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
        let net = PeerNetwork::<TestId>::start(id, None)
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

// ─── ROUND 6 — the INBOX-ARM hot-spin (run_20260610_121427), captured by the
// round-5 arm instrumentation and replayed here ───
//
// Production signature on the wedged relocated primary: the INBOX arm wins
// essentially EVERY iteration (~600K/s, `since_inbox=0`, `last_arm=inbox`)
// while NO completions ingest (succeeded frozen) and the timer arms tick
// normally. Mechanism: `handle_task_request`'s "demoted-primary relay" —
// every TaskRequest the primary cannot assign was re-sent to
// `Destination::Primary`, which on the current primary is a mesh LOOPBACK
// into its OWN inbox (`Mesh::dispatch`'s `Primary` arm is loopback-only and
// a directed delivery never excludes the origin). `RoleSlot::deliver` clears
// the routing stamp, so the relayed frame re-matches `target: None` and is
// relayed again: a self-sustaining memory-speed cycle, one inbox win per
// re-relay. With ≥2 frames circulating, the mesh-pump's `biased` select
// (egress before ingress) never finds the egress queue empty, so WIRE
// ingress — the remote completions — starves: ingest freezes exactly as
// captured. The onset trigger is any unassignable TaskRequest (an unknown/
// not-yet-rostered worker, an idle worker with an empty dispatch view, a
// declined scheduler decision); the burst of freed-worker re-polls after a
// 14-task completion burst supplied several at once.

/// Ghost-frame factory: a `TaskRequest` from a secondary the primary has
/// never welcomed (no roster entry, no capacity record), stamped the way the
/// wire ingress demux expects. `worker_idx_for` returns `None` for it, so the
/// primary can never assign against it — the relay arm (pre-fix) re-relayed
/// it forever.
fn ghost_task_request(worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: Some(dynrunner_protocol_primary_secondary::Destination::Primary),
        sender_id: "ghost-sec".into(),
        timestamp: 1.0,
        secondary_id: "ghost-sec".into(),
        worker_id,
        available_resources: vec![],
    }
}

/// The round-6 channel-mesh fixture: ONE real channel secondary (so the
/// pre-loop connect/mesh chain is satisfied, as in the e2e tests) plus a RAW
/// injection handle into the primary transport's inbound — the test's stand-in
/// for "a frame arrives off the wire" (the pump's ingress demux routes it to
/// the primary slot by its stamped target).
struct GhostFixture {
    primary: PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    mesh_keepalive: PrimaryMeshKeepalive,
    /// Raw wire-inbound injection handle (frames enter the pump's ingress).
    wire_tx: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    _sec_handle: tokio::task::JoinHandle<usize>,
}

/// Build the fixture: a primary over `from_raw_channels` wired to one real
/// secondary (`sec-0`, 2 workers), with the production pump (via
/// [`build_test_primary`]). The ledger is seeded with `binaries`; when
/// `ghost_in_flight` is `Some(task)`, that task is additionally marked
/// `InFlight` on the never-connected `ghost-sec` — an inherited assignment
/// whose terminal never arrives, so the operational loop provably stays alive
/// for the test's whole observation window (`run_complete_check` cannot trip).
fn ghost_fixture(
    binaries: Vec<TaskInfo<TestId>>,
    ghost_in_flight: Option<TaskInfo<TestId>>,
) -> GhostFixture {
    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        1024 * 1024 * 1024u64,
    )]);
    let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
        spawn_real_secondary("sec-0".to_string(), 2, max_res);

    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
    // Forward secondary→primary frames into the same wire inbound the test
    // injects into (one inbound, exactly like the e2e harness).
    {
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

    let transport = ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
    let config = PrimaryConfig {
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        ..test_primary_config()
    };
    let (mut primary, mesh_keepalive) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    seed_operational_ledger(&mut primary, binaries, HashMap::new());
    if let Some(ghost) = ghost_in_flight {
        let hash = crate::primary::wire::compute_task_hash(&ghost);
        primary
            .cluster_state_mut_for_test()
            .apply(ClusterMutation::TaskAssigned {
                hash,
                secondary: "ghost-sec".into(),
                worker: 0,
                version: Default::default(),
                attempt: 0,
            });
    }

    GhostFixture {
        primary,
        mesh_keepalive,
        wire_tx: incoming_tx,
        _sec_handle: sec_handle,
    }
}

// ─── #504: the range-digest probe-herd wedge (oploop-no-stall) ───
//
// Production signature (run before this fix): at a large (66k) build-phase
// START, every behind secondary detected divergence off the primary's first
// post-TasksSpawned digest and broadcast a PullProbe to Destination::All in
// the same instant. The primary's inbox arm then ran ~14× O(66k) range folds
// back-to-back inside ONE 256-frame inbox batch-drain (one un-memoized
// `tasks_range_digest()` per inbound probe), a synchronous CPU burst that
// froze the single-threaded oploop → the GIL thread's `block_on` wedged. The
// fix memoizes the range digest (O(1) read), so the probe burst no longer
// folds the ledger and the loop keeps servicing its sibling timer arms.

/// Build a probe-burst fixture: a primary seeded with a LARGE ledger via ONE
/// `TasksSpawned` apply (the production wedge trigger) + an inherited ghost
/// in-flight task (so the run stays open for the observation window), wired to
/// one real secondary so the pre-loop mesh chain is satisfied, with a FAST
/// keepalive so the heartbeat arm ticks many times inside the window (the
/// sibling-arm liveness oracle). Returns the retained coordinator + the wire
/// inbound the test injects probes into.
fn probe_burst_fixture(ledger_size: usize) -> GhostFixture {
    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        1024 * 1024 * 1024u64,
    )]);
    let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
        spawn_real_secondary("sec-0".to_string(), 2, max_res);

    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
    {
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

    let transport = ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
    let config = PrimaryConfig {
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        // Fast keepalive ⇒ the heartbeat arm ticks ~every 50ms, so the
        // sibling-arm liveness oracle has many chances to win inside the
        // window (the default 5s would not fire within a snappy test).
        keepalive_interval: StdDuration::from_millis(50),
        ..test_primary_config()
    };
    let (mut primary, mesh_keepalive) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Seed the LARGE ledger in ONE TasksSpawned apply — the production wedge
    // trigger (the first post-spawn digest is what the herd probes against).
    let binaries: Vec<TaskInfo<TestId>> = (0..ledger_size)
        .map(|i| make_binary(&format!("bin_{i:06}"), 50 + (i as u64)))
        .collect();
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::TasksSpawned { tasks: binaries });

    // An inherited ghost in-flight task keeps the run open for the whole
    // observation window (so the arm stats are still live to read).
    let ghost = make_binary("bin_ghost_keepalive", 7);
    let ghost_hash = crate::primary::wire::compute_task_hash(&ghost);
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::TaskAdded {
            hash: ghost_hash.clone(),
            task: ghost,
        });
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::TaskAssigned {
            hash: ghost_hash,
            secondary: "ghost-sec".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        });

    GhostFixture {
        primary,
        mesh_keepalive,
        wire_tx: incoming_tx,
        _sec_handle: sec_handle,
    }
}

/// A `PullProbe` frame from a behind peer (a default/empty digest ⇒ the peer
/// is far behind ⇒ the primary is `ahead`, the realistic phase-start herd
/// case). The primary's `handle_pull_probe` folds its `tasks_range_digest`
/// for the reply REGARDLESS of the ahead bit, so every probe exercises the
/// (now O(1)) range-digest read.
fn pull_probe_frame(requester: &str) -> DistributedMessage<TestId> {
    DistributedMessage::PullProbe {
        target: None,
        sender_id: requester.into(),
        timestamp: 1.0,
        digest: dynrunner_protocol_primary_secondary::StateDigest::default(),
    }
}

/// ── #504 repro: a 14-probe herd over a 60k ledger must NOT stall the loop ──
///
/// Seed a 60k-task ledger in one TasksSpawned, let the loop settle, then inject
/// 14 simultaneous PullProbe frames (the phase-start herd). Each probe makes
/// the inbox arm build a `PullProbeReply` whose range digest — pre-fix — was an
/// O(60k) fold, so 14 of them in one inbox batch were a synchronous CPU burst
/// that froze the single-threaded oploop.
///
/// ORACLE (the loop never stalls): the SIBLING heartbeat arm must keep winning
/// THROUGH and AFTER the probe burst — its win count must advance across the
/// post-burst window. With the memo the 14 probe replies are O(1) each, so the
/// loop services its timers normally; a regression to the per-probe fold would
/// let the inbox arm monopolize the run-queue while folding 14×60k entries and
/// the heartbeat arm would visibly stall. The inbox arm must also have
/// PROCESSED the burst (its count covers the 14 probes).
#[tokio::test(flavor = "current_thread")]
async fn probe_herd_over_large_ledger_does_not_stall_oploop() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fixture = probe_burst_fixture(60_000);
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx,
                _sec_handle,
            } = fixture;

            // Fire the 14-probe HERD mid-window. The `run` future holds
            // `&mut primary` for its whole scope, so the arm stats are read
            // AFTER the run scope ends (the reference fixture's pattern); the
            // ghost in-flight keeps the run unresolved so the stats are still
            // live to read post-timeout.
            let probe_count = 14usize;
            tokio::task::spawn_local(async move {
                // Let the loop settle into operational steady state + the
                // heartbeat arm warm up (so a post-burst stall would be
                // distinguishable from a slow bring-up).
                tokio::time::sleep(StdDuration::from_millis(1500)).await;
                // THE HERD: 14 probes injected back-to-back so they land in
                // (at most) a couple of inbox batches — the production shape.
                for i in 0..probe_count {
                    let _ = wire_tx.send(pull_probe_frame(&format!("behind-sec-{i}")));
                }
            });

            // Observation window: settle (1.5s) + burst + ~2s of post-burst
            // loop activity. At a 50ms heartbeat cadence a HEALTHY loop ticks
            // the heartbeat arm ~70× across this window.
            let window = StdDuration::from_millis(3500);
            {
                let (_deps, ops, ope) = noop_phase_args();
                let run = primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                );
                tokio::pin!(run);
                let _ = tokio::time::timeout(window, &mut run).await;
            }

            // ORACLE 1 (the loop never stalled): the sibling heartbeat arm
            // ticked a healthy number of times across the window that CONTAINS
            // the 14-probe burst. A per-probe O(60k) fold would freeze the
            // single-threaded loop during the burst; here the memoized O(1)
            // read lets the timers keep firing. The floor (20) is far below the
            // ~70 a 50ms cadence yields over ~3.5s, but far above the handful a
            // stalled loop would manage — a wide, non-flaky margin.
            let heartbeat = arm_count_of(&primary, "heartbeat");
            assert!(
                heartbeat >= 20,
                "OPLOOP STALL (#504): the heartbeat arm ticked only {heartbeat}× \
                 across a ~3.5s window containing the 14-probe burst over a 60k \
                 ledger. A per-probe O(ledger) range fold would freeze the \
                 single-threaded loop; the memoized O(1) read must let the \
                 sibling timer arms keep winning.",
            );
            // ORACLE 2 (the burst was actually serviced, not dropped): the
            // inbox arm processed at least the 14 probes (plus mesh traffic).
            let inbox = arm_count_of(&primary, "inbox");
            assert!(
                inbox >= probe_count as u64,
                "the inbox arm must have PROCESSED the 14-probe burst \
                 (inbox wins={inbox}); a stalled loop would not have drained it"
            );
        })
        .await;
}

/// ── ROUND 6 repro #1 (the capture's spin, deterministic) ──
///
/// One TaskRequest from a never-welcomed secondary lands on the live
/// operational loop while one inherited in-flight task keeps the run open.
/// The request can never assign (`worker_idx_for` → `None`).
///
/// RED (pre-fix): the relay arm re-sends it to `Destination::Primary` →
/// loopback into the loop's own inbox → re-relay, forever. The inbox arm
/// wins millions of iterations in the observation window — the EXACT
/// arm-stats signature of run_20260610_121427 (`inbox≈iter`,
/// `since_inbox=0`, `last_arm=inbox`).
///
/// GREEN (post-fix): the unassignable request is dropped (R1: a TaskRequest
/// is a pure capacity hint; the requester re-polls on its own backoff), so
/// the inbox arm wins only for genuine traffic — orders of magnitude below
/// the bound.
#[tokio::test(flavor = "current_thread")]
async fn ghost_task_request_must_not_self_relay_spin() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let real = make_binary("bin_real", 50);
            let ghost = make_binary("bin_ghost", 60);
            let fixture = ghost_fixture(vec![real, ghost.clone()], Some(ghost));
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx,
                _sec_handle,
            } = fixture;

            // Inject the unassignable request once the run has settled into
            // the operational loop (connect + initial assignment done well
            // inside 1.5s on the channel mesh).
            tokio::task::spawn_local(async move {
                tokio::time::sleep(StdDuration::from_millis(1500)).await;
                let _ = wire_tx.send(ghost_task_request(0));
            });

            {
                let (_deps, ops, ope) = noop_phase_args();
                let run = primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                );
                tokio::pin!(run);
                // The ghost in-flight task never completes, so the run cannot
                // resolve: the timeout is the observation window, after which
                // the live arm stats are read off the retained coordinator.
                let _ = tokio::time::timeout(StdDuration::from_secs(5), &mut run).await;
            }

            let snap = primary
                .op_loop_arm_stats
                .as_ref()
                .map(|s| s.snapshot())
                .expect(
                    "the operational loop must still be live (the inherited \
                     in-flight task keeps the run open)",
                );
            let inbox_wins = snap
                .counts
                .iter()
                .find(|(name, _)| *name == "inbox")
                .map(|(_, n)| *n)
                .expect("inbox arm is instrumented");
            assert!(
                inbox_wins < 10_000,
                "INBOX-ARM HOT-SPIN: one unassignable TaskRequest drove {inbox_wins} \
                 inbox-arm wins in a ~3.5s window — the self-relay cycle \
                 (handle_task_request re-sending to Destination::Primary, which \
                 loopbacks into the primary's own inbox). Arm snapshot: [{snap}]",
            );
        })
        .await;
}

/// ── #491 parity: the primary inbox ARM batch-drains a backlog ──
///
/// The secondary's inbox arm (process_tasks.rs) was given a bounded follow-on
/// `drain_ready(INBOX_BATCH_DRAIN_CAP)` sweep in #491 so one `select!`
/// iteration consumes a BOUNDED BATCH, not a single frame, against the
/// O(tasks) pull-model ingress rate. The primary oploop's inbox arm was never
/// given the same sweep — at affine scale its single-frame-per-iteration drain
/// lost to that ingress rate, pinning the inbox arm ~95% and starving the
/// sibling timer arms into a runtime-starvation freeze. This pins the restored
/// parity at the LOOP level (the shared `RoleInbox::drain_ready` primitive is
/// unit-tested in `process::mesh_client`; here we prove the primary ARM
/// invokes it).
///
/// MECHANISM OF THE ORACLE: `arm_stats.record(ARM_INBOX)` fires exactly ONCE
/// per arm body — i.e. once per awaited `recv()` win — REGARDLESS of how many
/// frames the follow-on `drain_ready` sweep then absorbs (it counts select!
/// wins, not frames). So when a backlog of K frames is already queued:
///   • one-per-iteration (pre-fix): the arm must win K separate times to drain
///     K frames → `inbox` count ≈ K.
///   • batch-drain (post-fix): each win absorbs the whole ready backlog in its
///     `drain_ready` sweep → `inbox` count ≪ K (a handful of recv-led batches).
/// We inject K=64 unassignable ghost `TaskRequest`s back-to-back (so they pile
/// into the inbox as one ready backlog) and assert the inbox arm serviced them
/// in FEWER than K/2 selections — a margin one-per-iteration provably cannot
/// meet (it would need ≥K). Ghost requests are dropped on arrival (R1, see
/// `ghost_task_request_must_not_self_relay_spin`) so they neither self-relay
/// nor assign — each is purely a frame the arm must pull from the inbox, the
/// clean drain-rate probe.
#[tokio::test(flavor = "current_thread")]
async fn primary_inbox_arm_batch_drains_backlog_in_one_selection() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let real = make_binary("bin_real", 50);
            let ghost = make_binary("bin_ghost", 60);
            // The ghost in-flight keeps the run open for the whole window so
            // the live arm stats survive to be read post-timeout.
            let fixture = ghost_fixture(vec![real, ghost.clone()], Some(ghost));
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx,
                _sec_handle,
            } = fixture;

            // After the loop settles into operational steady state, fire a
            // BACKLOG of K ghost requests back-to-back (no awaits between
            // sends) so on the current-thread runtime they all queue into the
            // primary's inbox as one ready batch before the inbox arm next
            // polls — the production "freed-worker re-poll burst" shape.
            let backlog = 64usize;
            tokio::task::spawn_local(async move {
                tokio::time::sleep(StdDuration::from_millis(1500)).await;
                for w in 0..backlog as u32 {
                    let _ = wire_tx.send(ghost_task_request(w));
                }
            });

            {
                let (_deps, ops, ope) = noop_phase_args();
                let run = primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                );
                tokio::pin!(run);
                let _ = tokio::time::timeout(StdDuration::from_secs(5), &mut run).await;
            }

            let inbox = arm_count_of(&primary, "inbox");
            // ORACLE 1 (the backlog WAS serviced): the inbox arm fired at least
            // once — the loop pulled the burst, it was not left to rot.
            assert!(
                inbox >= 1,
                "the inbox arm never won — the {backlog}-frame backlog was \
                 never serviced",
            );
            // ORACLE 2 (batch-drain, not one-per-iteration): the K queued
            // frames were absorbed in FEWER than K/2 inbox-arm selections. A
            // one-per-iteration drain would need at LEAST K selections (one
            // recv per frame); the batch-drain folds the ready backlog into a
            // handful of recv-led batches. The K/2 ceiling is a wide,
            // non-flaky margin — even if mesh keepalives and the backlog split
            // across a few batches, it stays far below the K floor a
            // single-frame drain is pinned to.
            assert!(
                (inbox as usize) < backlog / 2,
                "PRIMARY INBOX ARM NOT BATCH-DRAINING (#491 parity): a backlog \
                 of {backlog} queued frames drove {inbox} inbox-arm selections — \
                 a one-per-iteration drain needs ≥{backlog} (one recv per \
                 frame). The bounded `drain_ready` follow-on must absorb the \
                 ready backlog within a recv-led batch so the count stays ≪ the \
                 frame count.",
            );
        })
        .await;
}

/// ── ROUND 6 repro #2 (the capture's ingest freeze, end-to-end) ──
///
/// The production sequence replayed: unassignable TaskRequests are ALREADY
/// queued on the primary's wire inbound when the run starts (the freed-worker
/// re-poll burst), and the real secondary's welcome/completions must flow
/// through the SAME pump behind them.
///
/// RED (pre-fix): the two ghost requests start a 2-frame self-relay cycle, so
/// the pump's `biased` select (egress before ingress) never finds the egress
/// queue empty and wire ingress starves — the secondary's frames never ingest
/// and the run cannot finish (the capture's "succeeded frozen; everything
/// after the onset vanished").
///
/// GREEN (post-fix): the ghosts are dropped on arrival; all tasks dispatch,
/// complete, and the run resolves.
#[tokio::test(flavor = "current_thread")]
async fn completion_burst_survives_unassignable_request_storm() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let tasks = 4usize;
            let binaries: Vec<TaskInfo<TestId>> = (0..tasks)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + (i as u64) * 10))
                .collect();
            let fixture = ghost_fixture(binaries, None);
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx,
                _sec_handle,
            } = fixture;

            // The unassignable burst is queued BEFORE the run starts — the
            // pump routes it into the primary slot ahead of the run's first
            // recv, exactly the "requests with nothing dispatchable" shape
            // the production onset had.
            let _ = wire_tx.send(ghost_task_request(0));
            let _ = wire_tx.send(ghost_task_request(1));

            let (_deps, ops, ope) = noop_phase_args();
            let result = tokio::time::timeout(
                StdDuration::from_secs(20),
                primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                ),
            )
            .await;

            let completed = primary.completed_count();
            match result {
                Err(_) => panic!(
                    "INGEST FREEZE: the run did not resolve within the window \
                     (completed {completed}/{tasks}) — unassignable TaskRequests \
                     wedged the loop/pump (the production run_20260610_121427 \
                     signature)",
                ),
                Ok(run_result) => {
                    assert!(
                        run_result.is_ok(),
                        "run must resolve cleanly despite the unassignable \
                         request burst; got {run_result:?}"
                    );
                    assert_eq!(
                        completed, tasks,
                        "every real completion must ingest despite the \
                         unassignable request burst"
                    );
                }
            }
        })
        .await;
}

/// ── ROUND 6 backstop: the primary's egress rejects `Destination::Primary` ──
///
/// The structural invariant behind the spin fix: a primary addressing "the
/// primary" is a self-send (the mesh's `Primary` dispatch arm is
/// loopback-only), which is at best a wasted hop and at worst the
/// self-sustaining inbox cycle. `PrimaryCoordinator::send_to` must reject it
/// loudly so no future caller can reintroduce the cycle; ordinary
/// destinations still queue.
#[tokio::test(flavor = "current_thread")]
async fn primary_send_to_rejects_destination_primary_self_send() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let transport = ChannelPeerTransport::from_raw_channels(
                "setup".into(),
                HashMap::new(),
                incoming_rx,
            );
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let frame = |sender: &str| DistributedMessage::<TestId>::Keepalive {
                target: None,
                sender_id: sender.to_string(),
                timestamp: 1.0,
                secondary_id: sender.to_string(),
                active_workers: 0,
                emitter_role: dynrunner_protocol_primary_secondary::KeepaliveRole::Primary,
            };
            let self_send = primary
                .send_to(
                    dynrunner_protocol_primary_secondary::Destination::Primary,
                    frame("setup"),
                )
                .await;
            assert!(
                self_send.is_err(),
                "the primary's egress must reject Destination::Primary \
                 (self-send loopback — the inbox-cycle hazard)"
            );
            let broadcast = primary
                .send_to(
                    dynrunner_protocol_primary_secondary::Destination::All,
                    frame("setup"),
                )
                .await;
            assert!(
                broadcast.is_ok(),
                "ordinary destinations must still queue: {broadcast:?}"
            );
        })
        .await;
}

/// ── ROUND 6 pin #3 (requirement 1: a dead inbox is loud + terminal) ──
///
/// Drop the mesh-pump (which owns the primary slot's `Arc`) mid-run: every
/// sender of the operational inbox is gone, `inbox.recv()` yields `None`.
/// The loop must take the transport-closed terminal path — gate the arm,
/// break, run final accounting — and the run future must RESOLVE. A spin
/// (the closed-mpsc hazard) or a silently-disabled arm that zombies the run
/// would both fail this as a timeout.
#[tokio::test(flavor = "current_thread")]
async fn inbox_closed_mid_run_breaks_loop_no_zombie_no_spin() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let real = make_binary("bin_real", 50);
            let ghost = make_binary("bin_ghost", 60);
            // The ghost in-flight keeps the loop from completing on its own,
            // so the ONLY way the run resolves inside the window is the
            // inbox-closed terminal path.
            let fixture = ghost_fixture(vec![real, ghost.clone()], Some(ghost));
            let GhostFixture {
                mut primary,
                mesh_keepalive,
                wire_tx: _wire_tx,
                _sec_handle,
            } = fixture;

            // Kill the mesh mid-run: aborting the pump drops the primary
            // slot `Arc` it owns → the inbox's only sender drops → recv None.
            tokio::task::spawn_local(async move {
                tokio::time::sleep(StdDuration::from_secs(2)).await;
                drop(mesh_keepalive);
            });

            let (_deps, ops, ope) = noop_phase_args();
            let result = tokio::time::timeout(
                StdDuration::from_secs(10),
                primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                ),
            )
            .await;

            assert!(
                result.is_ok(),
                "a closed operational inbox must BREAK the loop into final \
                 accounting (loud terminal), never zombie the run: the run \
                 future did not resolve within the window"
            );
        })
        .await;
}

// ─── the RESPAWN-ARM membership-join busy-wake (run_20260612_035105) ───
//
// Historical production signature on a promoted (remote-respawn) primary:
// the `respawn_request` arm-stat climbed ~50/min (7398→7797 in 8.4 min) with
// ZERO spawns, ZERO dead members, the fleet fully healthy. Mechanism: the
// respawn lifecycle listener forwarded EVERY `PeerLifecycleEvent::Added`
// (membership joins / re-admission echoes the busy mesh emits continuously)
// onto the respawn channel, where each Added woke the respawn arm to a
// guaranteed-no-op reconcile path.
//
// Fix (#543/#544/#546): the respawn pipeline no longer cares about `Added`
// events at all — the dispatcher (`dispatch_respawn_lifecycle`) routes only
// `Removed`, and the listener filters accordingly. A replacement that joins
// is just a normal member; there is nothing to reconcile (the prior
// pending-replacement / revoke pathway was deleted along with the respawn-
// flow scancel reachability). `Removed` events still fire the arm; `Added`
// never reaches it.

/// Read `arm_name`'s win count off a retained promoted primary's live
/// arm-stats snapshot. Panics if the loop already unpublished (resolved).
fn arm_count_of(
    primary: &PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    arm_name: &str,
) -> u64 {
    let snap = primary
        .op_loop_arm_stats
        .as_ref()
        .map(|s| s.snapshot())
        .expect("the operational loop must still be live (the inherited ghost task keeps it open)");
    snap.counts
        .iter()
        .find(|(name, _)| *name == arm_name)
        .map(|(_, n)| *n)
        .unwrap_or_else(|| panic!("{arm_name} arm is instrumented; snap=[{snap}]"))
}

/// Stream a window of generation-advancing `PeerJoined` frames for an
/// already-live peer into the running primary's wire inbound. Each is a
/// state-changing apply → an `Added` lifecycle event — the membership-join
/// churn the production busy-arm woke on. Returns once every frame is sent.
fn spawn_added_churn(wire_tx: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>) {
    tokio::task::spawn_local(async move {
        tokio::time::sleep(StdDuration::from_millis(1200)).await;
        for g in 100u64..140 {
            let _ = wire_tx.send(DistributedMessage::ClusterMutation {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::PeerJoined {
                    peer_id: "sec-0".into(),
                    is_observer: false,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: g,
                }],
            });
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
    });
}

/// ── repro: membership-join `Added` churn must NOT wake the respawn arm ──
///
/// A promoted (remote-respawn) primary with NO replacement pending receives
/// a window of 40 generation-advancing `Added` events.
///
/// RED (pre-fix): every `Added` reaches the respawn arm, each one a no-op
/// reconcile that still increments `respawn_request` — the count tracks the
/// join rate (≈40 here), the production busy-arm signature.
///
/// GREEN (post-fix): the listener drops `Added` while idle, so the arm
/// parks — `respawn_request` stays at zero across the whole churn window.
#[tokio::test(flavor = "current_thread")]
async fn respawn_arm_parks_through_membership_join_churn() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let real = make_binary("bin_real", 50);
            let ghost = make_binary("bin_ghost", 60);
            // The inherited ghost in-flight task keeps the run open for the
            // whole observation window (`run_complete_check` cannot trip).
            let fixture = ghost_fixture(vec![real, ghost.clone()], Some(ghost));
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx,
                _sec_handle,
            } = fixture;
            primary.enable_respawn_remote(crate::primary::respawn::RespawnBudget {
                max_per_secondary: 100,
                max_total: 100,
                cooldown: StdDuration::ZERO,
            });

            spawn_added_churn(wire_tx);

            {
                let (_deps, ops, ope) = noop_phase_args();
                let run = primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                );
                tokio::pin!(run);
                let _ = tokio::time::timeout(StdDuration::from_secs(5), &mut run).await;
            }

            let respawn_wins = arm_count_of(&primary, "respawn_request");
            assert_eq!(
                respawn_wins, 0,
                "RESPAWN-ARM JOIN BUSY-WAKE: 40 membership-join `Added` events \
                 woke the respawn arm {respawn_wins} times with no replacement \
                 pending (each a no-op reconcile). The arm must park on \
                 join churn while idle (the run_20260612_035105 signature).",
            );
        })
        .await;
}

// ─── #582: sustained SpawnTasks streaming keeps the loop healthy ───
//
// SMOKE (not a deterministic wedge repro). The production wedge
// (run_20260615_192743) required real CPU cost per batch on a real-sized
// cluster ledger — 46,141 descriptors over 621 batches (~74 tasks/batch),
// each apply walking a populated CRDT. The in-process fixture's empty
// ledger and tiny tasks make each apply microseconds, so the COMMAND arm
// returns to `select!` naturally fast and ARM_HEARTBEAT is never starved
// the way production was. This test therefore exercises the integration —
// `command_sender()` + real `select!` loop + arm_stats — under a sustained
// burst, and ASSERTS the loop stays healthy. The deterministic regression
// pin for the actual wedge mechanic lives in two focused unit tests:
//
//   * `command_channel::tests::small_spawn_tasks_rides_continuation_queue_no_fast_path`
//     pins (A): `apply_spawn_tasks` always rides the continuation queue
//     (no single-shot fast path).
//   * `command_channel::tests::fairness_gate_fires_when_heartbeat_overdue`
//     pins (C): `fire_heartbeat_if_overdue` runs the heartbeat body and
//     stamps ARM_HEARTBEAT when `last_heartbeat_fire` is stale by more
//     than `2 × keepalive_interval`.
//
// Inject 100 SpawnTasks bursts back-to-back into the COMMAND arm via
// `command_sender()` (each ≤ CHUNK tasks so pre-#582 it would take the
// fast path). Observe the arm-stats snapshot.
//
// ORACLE 1 (heartbeat keeps firing): `heartbeat` ≥ 20 over the ~3.5 s
// observation window — far above a stalled-loop count. A regression that
// silently dropped the heartbeat arm under burst (e.g. an arm body that
// re-borrowed `&mut self` and deadlocked, or a select! arm that became
// always-ready and starved its peers) would show 0-2 here.
//
// ORACLE 2 (positive control — the burst was serviced): `command` wins ≥
// `BURSTS` — at least one COMMAND-arm fire per SpawnTasks. Under-servicing
// would mean the fixture failed to load the loop, invalidating ORACLE 1.

/// One ≤ CHUNK-sized spawn batch as a `PrimaryCommand::SpawnTasks` for the
/// streaming burst. The reply oneshot is intentionally dropped on the
/// caller side (fire-and-forget, matching the in-runtime PyPrimaryHandle
/// path).
fn streaming_spawn_burst(
    batch_idx: usize,
    tasks_per_batch: usize,
) -> PrimaryCommand<TestId> {
    let tasks: Vec<TaskInfo<TestId>> = (0..tasks_per_batch)
        .map(|i| {
            let mut t = make_binary(&format!("stream_b{batch_idx}_t{i}"), 50);
            t.task_id = format!("stream_b{batch_idx}_t{i}_id");
            t
        })
        .collect();
    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
    PrimaryCommand::SpawnTasks {
        tasks,
        reply: reply_tx,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn streaming_spawn_burst_does_not_starve_heartbeat() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Reuse `probe_burst_fixture` for its FAST keepalive (50 ms) — a
            // production-cadence (5 s) heartbeat would not fire enough times
            // inside a snappy test window for a healthy floor. We bypass its
            // ledger-seed by using `ledger_size = 0`; the ghost in-flight task
            // it pre-applies keeps the run open for the observation window.
            // The burst's spawned tasks land freshly into the live pool via
            // the SpawnTasks command path under test.
            let fixture = probe_burst_fixture(0);
            let GhostFixture {
                mut primary,
                mesh_keepalive: _mesh,
                wire_tx: _wire_tx,
                _sec_handle,
            } = fixture;

            // Grab the command sender BEFORE the run takes `&mut primary`.
            let cmd_tx = primary.command_sender();

            // THE BURST: 30 SpawnTasks bursts at 30 ms intervals → ~900 ms of
            // sustained ~33 batches/sec load. ≤CHUNK tasks each, so pre-#582
            // each would take the single-shot fast path and ARM_COMMAND would
            // be perpetually ready.
            const BURSTS: usize = 100;
            // Near CHUNK size so each batch is non-trivial work but still
            // takes the fast path pre-fix (`tasks.len() <= CHUNK`).
            const TASKS_PER_BURST: usize = 200;
            // 0 ms — submit as fast as the send-await chain allows. Pre-fix
            // the COMMAND arm body runs each apply inline (~hundreds of µs
            // for 200 tasks), so saturating submission keeps it perpetually
            // ready. Production (consumer's run_20260615_192743) was 100 ms
            // per batch at ~10/sec, which under #586's biased priority
            // starved heartbeat for 75 s — we replicate the saturation here
            // without simulating the per-batch CPU time.
            const INTER_BURST_MS: u64 = 0;
            tokio::task::spawn_local(async move {
                // Settle the loop into steady state first so the heartbeat arm
                // has warmed up (a slow bring-up would otherwise look like
                // starvation against the floor).
                tokio::time::sleep(StdDuration::from_millis(1500)).await;
                for b in 0..BURSTS {
                    let cmd = streaming_spawn_burst(b, TASKS_PER_BURST);
                    // `send().await` because the bounded command channel could
                    // backpressure under burst — the test's harness is sized
                    // generously, but a non-await send would mask a regression
                    // where the operational loop fails to drain the channel.
                    let _ = cmd_tx.send(cmd).await;
                    tokio::time::sleep(StdDuration::from_millis(INTER_BURST_MS)).await;
                }
            });

            // Observation window: 1.5 s settle + 0.9 s burst + ~1.1 s
            // post-burst → 3.5 s, matching the #504 test's pattern.
            let window = StdDuration::from_millis(3500);
            {
                let (_deps, ops, ope) = noop_phase_args();
                let run = primary.run(
                    SeedSource::PromotionSnapshot {
                        kind: crate::process::BootstrapKind::Failover,
                    },
                    ops,
                    ope,
                );
                tokio::pin!(run);
                let _ = tokio::time::timeout(window, &mut run).await;
            }

            // ORACLE 1 (heartbeat fires through the burst — the #582 contract):
            let heartbeat = arm_count_of(&primary, "heartbeat");
            assert!(
                heartbeat >= 20,
                "OPLOOP STALL (#582): the heartbeat arm ticked only {heartbeat}× \
                 across a ~3.5 s window containing a 30-batch SpawnTasks burst \
                 at ~33 batches/sec. Pre-#582 the single-shot fast path inside \
                 `apply_spawn_tasks` made ARM_COMMAND perpetually ready, so \
                 #586's `biased;` starved ARM_HEARTBEAT for the entire burst — \
                 the production keepalive-deafness signature. The fix routes \
                 every SpawnTasks through `spawn_continuation_queue` (yields to \
                 sibling arms between chunks) AND defends with \
                 `fire_heartbeat_if_overdue` (synchronous fire when overdue by \
                 2× keepalive_interval). Heartbeat={heartbeat} means BOTH the \
                 yield-between-chunks contract and the fairness-gate fallback \
                 failed."
            );

            // ORACLE 2 (positive control — the burst was serviced):
            let command = arm_count_of(&primary, "command");
            assert!(
                command >= BURSTS as u64,
                "the COMMAND arm must have serviced the {BURSTS}-batch burst \
                 (command wins={command}); under-servicing would mean the \
                 fixture failed to load the loop, invalidating ORACLE 1."
            );
        })
        .await;
}
