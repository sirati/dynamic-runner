//! #313 — the terminal RUN VERDICT on the primary's deliberate fail-loud
//! exits. The CRDT carries `run_complete` (the SUCCESS latch) and
//! `run_aborted { reason }` (the failure twin); every DELIBERATE terminal
//! exit of the primary must broadcast the HONEST variant before
//! returning, so the fleet tears down (secondaries exit non-zero,
//! observer reports the failure reason) instead of idling into timeouts.
//! See `PrimaryCoordinator::broadcast_terminal_verdict` for the full
//! verdict-vs-failover exit-path classification.
//!
//! Test families in this file:
//! - wholesale runtime spawn rejection (`RunError::SpawnRejected`) — the
//!   verdict must be `RunAborted` (pre-#313 this path broadcast a FALSE
//!   `RunComplete` while the primary exited non-zero, so the fleet and
//!   the observer narrated a clean success over a failed run);
//! - `RunError::NoRelocationTarget` — a connected fleet with NO
//!   promotion-eligible peer can never elect a primary, so the verdict
//!   (not failover) must tear it down.
//!
//! The sibling verdict origination tests live with their exit paths:
//! the worker-mgmt fatal latch in `phase_end_raise.rs`, the
//! routing-collapse strand in `stranded.rs`, the #3a pre-phase duplicate
//! in the ingest/e2e families. The failover-preservation NEGATIVE (a
//! KILLED primary broadcasts nothing and the survivors still elect +
//! promote) is `producer_backstop.rs`'s failover leg.

use dynrunner_core::{PhaseId, TaskDep};

use super::*;
use crate::primary::command_channel::PrimaryCommand;

/// #415 face (b1) — the terminal verdict must reach a transiently-down
/// OBSERVER leg before the authority tears down.
///
/// run_20260611_155305: the compute primary broadcast `RunComplete` while
/// the relocated submitter→observer's `-R` leg was DOWN (the fleet-wide
/// drop coincided with run-end). The pre-fix `broadcast_terminal_verdict`
/// fired ONE broadcast + slept a FIXED 500ms, then the fleet tore down —
/// so the verdict never reached the observer, and a zero-authority
/// observer (which exits ONLY on observing the CRDT terminal — BUG-B)
/// blacked out forever once no peer was left to anti-entropy-pull from.
///
/// The fix HOLDS the authority alive — RE-BROADCASTING — until every
/// observer the roster names has a reachable leg again (bounded by the
/// grace cap). This test seeds an observer in `RoleTable.observers`, starts
/// its leg DOWN, brings it up mid-wait, and asserts the verdict is
/// RE-SENT after the leg re-folds AND that `broadcast_terminal_verdict`
/// does NOT return before the observer is reachable.
///
/// REVERT-CHECK: with the fixed-500ms settle restored the call returns with
/// the leg still down and exactly ONE broadcast (no re-send) — the observer
/// never gets the verdict.
#[tokio::test(flavor = "current_thread")]
async fn terminal_verdict_holds_for_a_transiently_down_observer_leg() {
    use crate::primary::test_helpers::ControllableMembershipPeer;
    use crate::process::{LocalRole, Mesh};
    use dynrunner_protocol_primary_secondary::address::PeerId;
    use std::collections::HashSet;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Connected set the pump publishes from. Start with ONE compute
            // secondary present but the OBSERVER leg ("obs") ABSENT — the
            // verdict-time fleet-wide drop shape.
            let connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>> =
                std::rc::Rc::new(std::cell::RefCell::new(HashSet::from(["sec-0".to_string()])));
            let transport = ControllableMembershipPeer::<TestId>::new(connected.clone());
            let broadcasts = transport.broadcast_log();

            let mut mesh = Mesh::new(transport);
            let (slot, client, inbox) = mesh
                .register_local_role(LocalRole::Primary, PeerId::from("setup"));
            mesh.publish_membership();
            let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = tokio::task::spawn_local(async move {
                let _slot = slot;
                crate::process::pump::run_pump(mesh, control_rx).await;
            });

            let (_demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
            let mut primary = PrimaryCoordinator::new(
                test_primary_config(),
                client,
                inbox,
                demote_rx,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Seed the roster: a compute secondary + an OBSERVER, so the
            // terminal-delivery gate has an observer id to hold for.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".to_string(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "obs".to_string(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("obs"),
                "the observer must be in the roster for the gate to engage"
            );

            // Bring the observer leg UP after 1.5s — the secondary's
            // bootstrap-redial re-folding the wire mid-wait.
            let flip = connected.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                flip.borrow_mut().insert("obs".to_string());
            });

            // The call must HOLD until the observer leg is reachable. Bound
            // it generously below the grace cap so a regression (returns
            // early, leg still down) is caught by the post-call assertion,
            // not a hang.
            let started = std::time::Instant::now();
            tokio::time::timeout(
                Duration::from_secs(20),
                primary.broadcast_terminal_verdict(
                    crate::primary::lifecycle::TerminalVerdict::Complete,
                ),
            )
            .await
            .expect("terminal verdict delivery must complete within the grace cap");
            let held = started.elapsed();

            // It held until the observer leg came up (≈1.5s), not the fixed
            // 500ms settle.
            assert!(
                held >= Duration::from_millis(1200),
                "broadcast_terminal_verdict must HOLD until the observer leg \
                 re-folds (≈1.5s), not return after the fixed 500ms settle; \
                 held only {held:?}"
            );
            assert!(
                connected.borrow().contains("obs"),
                "the observer leg must be reachable by the time the call returns"
            );

            // The verdict was RE-BROADCAST (more than the single pre-fix
            // send) so the freshly-re-folded observer leg actually received
            // it. Every logged frame is a RunComplete ClusterMutation.
            let sends = broadcasts.borrow();
            let verdict_sends = sends
                .iter()
                .filter(|m| {
                    matches!(
                        m,
                        DistributedMessage::ClusterMutation { mutations, .. }
                            if mutations.iter().any(|cm| matches!(cm, ClusterMutation::RunComplete { .. }))
                    )
                })
                .count();
            assert!(
                verdict_sends >= 2,
                "the terminal verdict must be RE-BROADCAST while the observer \
                 leg is down + once after it re-folds (the pump only fans NEW \
                 sends to a freshly-registered leg); saw {verdict_sends} \
                 RunComplete broadcast(s)"
            );

            drop(control);
            pump.abort();
        })
        .await;
}

/// #526 — the terminal verdict must reach a node still RELOCATING INTO an
/// observer (it has not yet ANNOUNCED itself) before the relocated primary
/// tears down.
///
/// The relocation race (distinct from the #415 down-leg case above): a
/// submitter primary relocates its role onto a compute peer, which becomes
/// the operational primary and — on a fast 5-task run — decides + broadcasts
/// `RunComplete` BEFORE the relocating-away node has finished its
/// primary→observer swap and fired its bootstrap snapshot request (the one
/// that originates the `PeerJoined { is_observer: true }` populating
/// `RoleTable.observers`). So at broadcast time the role-table observer
/// projection is EMPTY: the #415 known-leg hold sees no observer and is a
/// no-op, leaving only the fixed 500ms settle. The relocating-away node's
/// mesh LEG survives the retag (so it is reachable), but it misses the single
/// broadcast and — being a zero-authority observer that exits ONLY on the
/// `RunComplete` latch (BUG-B) — strands on the long fleet-death cadences
/// once the compute peers exit (`peer_count → 0`).
///
/// The fix records the relocating-away node as the primary's
/// `pending_observer` (from `PromotionSignal::relocating_from`, set on a
/// `Transferred` build) and HOLDS the verdict — re-broadcasting on the
/// surviving leg — until it ANNOUNCES (joins `RoleTable.observers`), bounded
/// by the 5s announce grace so a becoming-observer that died mid-swap cannot
/// stall teardown. This test seeds the pending observer with its leg UP but
/// NOT in the roster (the empty-observers projection that makes the #415 hold
/// a no-op), broadcasts, and — with the observer NEVER announcing (the
/// worst-case slow/dead-swap leg) — asserts the call HOLDS past the fixed
/// 500ms settle and tears down at the BOUNDED 5s cap, never the long
/// fleet-death cadences, AND re-broadcasts (so an observer that DOES announce
/// mid-hold receives the verdict in its buffered inbox).
///
/// `broadcast_terminal_verdict` borrows `&mut self` for the whole wait, so a
/// mid-wait CRDT announce cannot be applied from this same task; the
/// deterministic RESOLUTION-on-real-announce coverage is the relocation e2e
/// (`cluster_state_converges_on_primary_and_secondary`), which fires the
/// observer's actual bootstrap announce mid-relocation.
///
/// REVERT-CHECK: without the pending-observer hold the role-table projection
/// is empty at broadcast time, so `await_terminal_observer_delivery`
/// early-returns and the call resolves after only the fixed 500ms settle —
/// the `held >= 5s cap` assertion FAILS — with exactly ONE broadcast, the
/// exact strand (the observer announces after teardown, no peer left to pull
/// the verdict from).
#[tokio::test(flavor = "current_thread")]
async fn terminal_verdict_holds_for_a_relocating_in_observer_not_yet_announced() {
    use crate::primary::test_helpers::ControllableMembershipPeer;
    use crate::process::{LocalRole, Mesh};
    use dynrunner_protocol_primary_secondary::address::PeerId;
    use std::collections::HashSet;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The relocating-away node's mesh leg SURVIVES its primary→observer
            // retag, so "obs" is reachable from the start — what is missing is
            // its ANNOUNCE (it is not yet in RoleTable.observers). A compute
            // secondary is also present.
            let connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>> =
                std::rc::Rc::new(std::cell::RefCell::new(HashSet::from([
                    "sec-0".to_string(),
                    "obs".to_string(),
                ])));
            let transport = ControllableMembershipPeer::<TestId>::new(connected.clone());
            let broadcasts = transport.broadcast_log();

            let mut mesh = Mesh::new(transport);
            let (slot, client, inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("sec-0"));
            mesh.publish_membership();
            let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = tokio::task::spawn_local(async move {
                let _slot = slot;
                crate::process::pump::run_pump(mesh, control_rx).await;
            });

            let (_demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
            let mut primary = PrimaryCoordinator::new(
                test_primary_config(),
                client,
                inbox,
                demote_rx,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Seed ONLY the compute secondary in the roster — the relocating-in
            // observer has NOT announced yet (its bootstrap RequestSnapshotStream
            // has not reached this just-promoted primary), so it is ABSENT from
            // RoleTable.observers, exactly as in the fast-relocation race.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".to_string(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .is_empty(),
                "the relocating-in observer must NOT yet be announced (empty roster \
                 observers) — this is what makes the #415 known-leg hold a no-op"
            );

            // Record the relocating-away node as the pending observer — the
            // PromotionSignal::relocating_from this primary was built with.
            primary.set_pending_observer("obs".to_string());

            // The relocating-in observer NEVER announces in this fixture (its
            // swap is modeled as not completing within the window — the
            // worst-case "becoming-observer is slow / died mid-swap" leg). The
            // call must therefore HOLD past the fixed 500ms settle and tear down
            // ONLY at the bounded 5s announce cap — never the multi-minute
            // fleet-death / cluster-gone cadences the unfixed observer stranded
            // on. Bounding the test wait at 10s (> the 5s cap, < the 20s e2e
            // bound) catches BOTH a regression (returns at ~500ms — the pre-fix
            // strand) via the lower-bound assertion AND a runaway (no cap) via
            // the timeout. The companion `..not_yet_announced` deterministic
            // RESOLUTION-on-announce coverage is the relocation e2e
            // (`cluster_state_converges_on_primary_and_secondary`), which fires
            // the real bootstrap announce; here `broadcast_terminal_verdict`
            // holds `&mut self` for the whole wait, so a mid-wait CRDT announce
            // cannot be applied from this same task — the cap path is the
            // single-call seam this unit test can drive deterministically.
            let started = std::time::Instant::now();
            tokio::time::timeout(
                Duration::from_secs(10),
                primary.broadcast_terminal_verdict(
                    crate::primary::lifecycle::TerminalVerdict::Complete,
                ),
            )
            .await
            .expect(
                "broadcast_terminal_verdict must tear down at the bounded 5s announce \
                 cap, NEVER hang on the long fleet-death cadences",
            );
            let held = started.elapsed();

            // It HELD (past the fixed 500ms settle) — pre-fix the empty-observers
            // early-out returned after ~500ms, stranding the observer.
            assert!(
                held >= crate::primary::PENDING_OBSERVER_ANNOUNCE_GRACE,
                "broadcast_terminal_verdict must HOLD for the pending relocating-in \
                 observer until the 5s announce cap, not return after the fixed 500ms \
                 settle (pre-fix the empty-observers early-out stranded the observer); \
                 held only {held:?}"
            );
            // It was BOUNDED by the cap (not the long cadences): a small margin
            // over the 5s cap covers the trailing 500ms re-broadcast settle.
            assert!(
                held < crate::primary::PENDING_OBSERVER_ANNOUNCE_GRACE + Duration::from_secs(2),
                "the hold must be BOUNDED by the 5s announce cap, not run to the \
                 fleet-death / cluster-gone cadences; held {held:?}"
            );

            // The verdict was RE-BROADCAST (more than the single pre-fix send) so
            // a relocating-in observer that announces mid-hold (the e2e path)
            // receives it in its buffered inbox.
            let sends = broadcasts.borrow();
            let verdict_sends = sends
                .iter()
                .filter(|m| {
                    matches!(
                        m,
                        DistributedMessage::ClusterMutation { mutations, .. }
                            if mutations.iter().any(|cm| matches!(cm, ClusterMutation::RunComplete { .. }))
                    )
                })
                .count();
            assert!(
                verdict_sends >= 2,
                "the terminal verdict must be RE-BROADCAST while the relocating-in \
                 observer is unannounced + once after it announces, so its buffered \
                 inbox receives the verdict; saw {verdict_sends} RunComplete broadcast(s)"
            );

            drop(sends);
            drop(control);
            pump.abort();
        })
        .await;
}

/// #526 happy-path latency: when the relocating-in observer has ALREADY
/// announced by the time the verdict is decided (the common case — the swap
/// completes before the run does), the pending-observer hold adds ~0 latency.
/// The pending id is already in `RoleTable.observers` with a reachable leg, so
/// `delivery_complete` is satisfied on the pre-loop check and
/// `broadcast_terminal_verdict` returns after only the fixed settle. Guards
/// refinement 3 (no routine cap-wait on the happy path).
#[tokio::test(flavor = "current_thread")]
async fn terminal_verdict_pending_observer_already_announced_adds_no_hold() {
    use crate::primary::test_helpers::ControllableMembershipPeer;
    use crate::process::{LocalRole, Mesh};
    use dynrunner_protocol_primary_secondary::address::PeerId;
    use std::collections::HashSet;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>> =
                std::rc::Rc::new(std::cell::RefCell::new(HashSet::from([
                    "sec-0".to_string(),
                    "obs".to_string(),
                ])));
            let transport = ControllableMembershipPeer::<TestId>::new(connected.clone());
            let mut mesh = Mesh::new(transport);
            let (slot, client, inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("sec-0"));
            mesh.publish_membership();
            let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = tokio::task::spawn_local(async move {
                let _slot = slot;
                crate::process::pump::run_pump(mesh, control_rx).await;
            });

            let (_demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
            let mut primary = PrimaryCoordinator::new(
                test_primary_config(),
                client,
                inbox,
                demote_rx,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The pending observer has ALREADY announced (it is in the roster
            // with a reachable leg) by verdict time.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "obs".to_string(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            primary.set_pending_observer("obs".to_string());

            // Give the pump a tick to publish the membership so `has_route(obs)`
            // is true, then the delivery must return after only the fixed settle.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let started = std::time::Instant::now();
            tokio::time::timeout(
                Duration::from_secs(3),
                primary.broadcast_terminal_verdict(
                    crate::primary::lifecycle::TerminalVerdict::Complete,
                ),
            )
            .await
            .expect("delivery must complete promptly when the observer is already announced");
            let held = started.elapsed();
            assert!(
                held < crate::primary::PENDING_OBSERVER_ANNOUNCE_GRACE,
                "an already-announced pending observer must add NO hold (return after \
                 the fixed settle, well under the 5s cap); held {held:?}"
            );

            drop(control);
            pump.abort();
        })
        .await;
}

/// 1 real primary + 1 real secondary, 5 single-phase tasks that all
/// complete cleanly; `on_phase_end` then spawns a runtime batch whose
/// EVERY task the validator rejects (`UnknownDependency`). The run must
/// surface `RunError::SpawnRejected` locally AND broadcast the honest
/// `RunAborted` verdict — never `RunComplete` — and the verdict must
/// tear the real secondary down on its own.
#[tokio::test(flavor = "current_thread")]
async fn wholesale_spawn_rejection_broadcasts_run_aborted_not_run_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
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

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let on_start: OnPhaseStart = Box::new(|_p: &dynrunner_core::PhaseId| {});
            // The consumer pattern (`FullPipelineTask.on_phase_end →
            // primary_handle.spawn_tasks`): on the first phase end, spawn
            // ONE follow-up task whose dependency names a `(phase, task_id)`
            // identity that is NOT in the ledger — the validator rejects
            // the whole batch (`UnknownDependency`), netting ZERO dispatch
            // for planned work. `try_send` — the callback runs
            // synchronously inside the cascade; the cascade's post-callback
            // drain picks the command up inline (the established consumer
            // pattern, see `phase_ordering.rs`).
            let command_sender = primary.command_sender();
            let mut already_spawned = false;
            let on_end: OnPhaseEnd = Box::new(move |_p, _c, _f, _outputs| {
                if already_spawned {
                    return;
                }
                already_spawned = true;
                let mut rejected = make_binary("rejected", 100);
                rejected.task_id = "rejected_id".into();
                rejected.task_depends_on = vec![TaskDep {
                    task_id: "no_such_task".into(),
                    phase_id: PhaseId::from("default"),
                    inherit_outputs: false,
                    def_id: None,
                }];
                let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                    tasks: vec![rejected],
                    reply: reply_tx,
                });
            });

            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never reaching the finalize behaviour this test asserts).
            seed_operational_ledger(&mut primary, binaries, HashMap::new());
            let result = primary
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end)
                .await;

            // Local return: the structured loud-fail backstop, naming the
            // dropped identity.
            match &result {
                Err(RunError::SpawnRejected { rejected_task_ids }) => {
                    assert_eq!(
                        rejected_task_ids.as_slice(),
                        ["rejected_id".to_string()],
                        "the backstop must name the wholesale-rejected identity"
                    );
                }
                other => panic!(
                    "a wholesale-rejected runtime spawn must surface \
                     RunError::SpawnRejected, got {other:?}"
                ),
            }

            // #313 — the peer-facing verdict is the honest RunAborted, NOT
            // the false-success RunComplete the pre-fix tail latched (the
            // finalize broadcast ran BEFORE the spawn-rejection check, so
            // the fleet/observer narrated a clean run while the primary
            // exited non-zero). The primary's own local apply is the
            // faithful observable for WHAT was broadcast.
            let state = primary.cluster_state_for_test();
            let abort_reason = state
                .run_aborted()
                .unwrap_or_else(|| {
                    panic!(
                        "a wholesale spawn rejection must broadcast RunAborted \
                         (run_aborted() = Some); run_complete()={}",
                        state.run_complete()
                    )
                })
                .to_string();
            assert!(
                !state.run_complete(),
                "a wholesale spawn rejection must NOT latch RunComplete — \
                 pre-#313 it did, narrating a false success"
            );
            assert!(
                abort_reason.contains("spawn_tasks rejected"),
                "the abort reason must carry the SpawnRejected render, \
                 got: {abort_reason}"
            );

            // Fleet-teardown half (#313): the verdict landed on the REAL
            // secondary's CRDT mirror, so its `process_tasks` loop exits on
            // its own (`SecondaryTerminal::Aborted` → non-zero at the PyO3
            // boundary) instead of idling into a timeout.
            let sec_exit = tokio::time::timeout(Duration::from_secs(10), sec_handle).await;
            assert!(
                sec_exit.is_ok(),
                "the RunAborted verdict must tear the secondary down on its \
                 own; it idled past the 10s budget"
            );
        })
        .await;
}

/// A SETUP PEER (`ColdStart` ⇒ `BootstrapRole::SetupPeer`) whose connected
/// fleet has NO promotion-eligible peer (`fake_secondary` advertises
/// `can_be_primary:false`) errors `RunError::NoRelocationTarget` — and
/// must broadcast the `RunAborted` verdict first (#313): an election can
/// NEVER produce a primary in this topology, so failover cannot salvage
/// the run and without the verdict the connected non-promotable fleet
/// idles into its timeouts holding SLURM slots. The `run_consuming`
/// exit-contract half (the `PrimaryRunOutcome::Local { result: Err(..) }`
/// wrap) is pinned by `setup_promote.rs`'s
/// `setup_peer_empty_candidate_set_is_no_relocation_target`; this test
/// drives the borrowing `run` entry so the coordinator's CRDT mirror —
/// the faithful observable for what was broadcast — stays readable.
#[tokio::test(flavor = "current_thread")]
async fn no_relocation_target_broadcasts_run_aborted() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One CONNECTED but non-eligible secondary, so mesh formation
            // completes and the pipeline reaches the SetupPeer relocate
            // branch — but the candidate set is empty.
            let (transport, secondary_ends) = setup_test(1);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (deps, ops, ope) = noop_phase_args();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                primary.run(
                    SeedSource::ColdStart {
                        binaries: vec![],
                        phase_deps: deps,
                    },
                    ops,
                    ope,
                ),
            )
            .await
            .expect("the SetupPeer run must return promptly on the empty-candidate path");

            assert!(
                matches!(result, Err(RunError::NoRelocationTarget)),
                "a setup peer with no eligible compute peer must surface \
                 RunError::NoRelocationTarget; got {result:?}"
            );

            // #313 — the verdict broadcast precedes the error return; the
            // reason is the SAME render as the local error so both sides of
            // the wire agree.
            let state = primary.cluster_state_for_test();
            let abort_reason = state
                .run_aborted()
                .unwrap_or_else(|| {
                    panic!(
                        "NoRelocationTarget must broadcast RunAborted \
                         (run_aborted() = Some); run_complete()={}",
                        state.run_complete()
                    )
                })
                .to_string();
            assert!(
                !state.run_complete(),
                "NoRelocationTarget must NOT latch RunComplete"
            );
            assert!(
                abort_reason.contains("could not relocate the primary role"),
                "the abort reason must carry the NoRelocationTarget render, \
                 got: {abort_reason}"
            );
        })
        .await;
}

/// #513 — THE INVARIANT: the counts STAMPED on the verdict equal the counts
/// the verdict DECISION was computed from. `broadcast_terminal_verdict` stamps
/// `self.outcome_summary()` at broadcast time; this proves that read equals
/// the primary's authoritative decision count (no divergence) and that it
/// lands in `terminal_outcome()` for the narrator.
///
/// Seed a KNOWN terminal partition (2 completed, 1 failed-final), capture the
/// decision count (`outcome_summary()`) BEFORE the broadcast, broadcast the
/// `Complete` verdict, then assert the CARRIED `terminal_outcome()` equals
/// that decision count exactly — `verdict-counts == verdict-decision-counts`.
/// No observer is seeded, so the delivery gate returns after the bounded
/// fixed settle (no hang).
#[tokio::test(flavor = "current_thread")]
async fn stamped_verdict_counts_equal_the_decision_counts() {
    use crate::primary::test_helpers::ControllableMembershipPeer;
    use crate::process::{LocalRole, Mesh};
    use dynrunner_protocol_primary_secondary::address::PeerId;
    use std::collections::HashSet;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>> =
                std::rc::Rc::new(std::cell::RefCell::new(HashSet::from(["sec-0".to_string()])));
            let transport = ControllableMembershipPeer::<TestId>::new(connected.clone());
            let mut mesh = Mesh::new(transport);
            let (slot, client, inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("setup"));
            mesh.publish_membership();
            let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = tokio::task::spawn_local(async move {
                let _slot = slot;
                crate::process::pump::run_pump(mesh, control_rx).await;
            });

            let (_demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
            let mut primary = PrimaryCoordinator::new(
                test_primary_config(),
                client,
                inbox,
                demote_rx,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed a KNOWN terminal partition into the ledger: 2 completed,
            // 1 failed-final. No observer in the roster ⇒ the delivery gate
            // returns after the bounded settle.
            {
                let cs = primary.cluster_state_mut_for_test();
                for i in 0..3 {
                    let t = make_binary(&format!("bin_{i}"), 50);
                    let hash = crate::primary::wire::compute_task_hash(&t);
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: hash.clone(),
                        task: t,
                        def_id: None,
                    });
                    if i < 2 {
                        cs.apply(ClusterMutation::TaskCompleted {
                            hash,
                            result_data: None,
                            attempt: 0,
                        });
                    } else {
                        cs.apply(ClusterMutation::TaskFailed {
                            hash,
                            kind: dynrunner_core::ErrorType::NonRecoverable,
                            error: "boom".into(),
                            version: Default::default(),
                            attempt: Default::default(),
                        });
                    }
                }
            }

            // The DECISION count — what finalize would compute and decide the
            // verdict from (the same `&self` read `broadcast_terminal_verdict`
            // re-takes at stamp time, with no await/mutation in between).
            let decision = primary.outcome_summary();
            assert_eq!(decision.succeeded, 2);
            assert_eq!(decision.fail_final, 1);

            primary
                .broadcast_terminal_verdict(
                    crate::primary::lifecycle::TerminalVerdict::Complete,
                )
                .await;

            // The CARRIED counts (latched by the local apply of the verdict)
            // EQUAL the decision counts — the non-negotiable invariant.
            let carried = primary
                .cluster_state_for_test()
                .terminal_outcome()
                .expect("the verdict latched its carried counts locally");
            assert_eq!(
                carried,
                dynrunner_core::TerminalOutcomeCounts::from(decision),
                "stamped verdict counts MUST equal the verdict-decision counts \
                 (succeeded={} fail_final={})",
                decision.succeeded,
                decision.fail_final
            );

            drop(control);
            pump.abort();
        })
        .await;
}
