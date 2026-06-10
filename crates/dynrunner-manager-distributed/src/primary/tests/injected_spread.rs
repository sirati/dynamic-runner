//! Round-2 dispatch-spread regression: the INJECTED-task dispatch path
//! composed with the mesh-readiness gate, on the PROMOTED-primary shape
//! (production: asm-tokenizer run_20260610_130116 — 28 memmap tasks
//! spawned mid-run via `on_phase_end → spawn_tasks` packed 12/9/4/2/1
//! onto 5 of 15 secondaries, ten at ZERO, despite the #349 interleave
//! fix).
//!
//! The composition defect: a promoted/relocated primary starts with an
//! EMPTY `mesh_ready_secondaries` set (node-local, nothing seeds it —
//! deliberately NOT inherited via CRDT, because the predecessor's ledger
//! proves legs to the OLD node: "mesh-leg confirmed" is PAIRWISE,
//! member ↔ CURRENT primary). Pre-fix a secondary's `MeshReady` was
//! one-shot PER PROCESS (`mesh.mesh_ready_sent` latched forever, and the
//! report went to whichever primary held the role at that moment), so on
//! the promoted primary the confirmations were structurally
//! unrecoverable for already-operational members. The gate
//! (`should_skip_worker_for_dispatch` → `member_mesh_confirmed`) then
//! withheld 10/15 members from every proactive dispatch (the verbatim
//! production WARN: "member remains unassignable until its mesh leg
//! confirms ... skipping proactive dispatch"), and the injected batch
//! packed onto the confirmed stragglers.
//!
//! THE FIX (pinned at the secondary in
//! `secondary/tests/mesh_ready_reannounce.rs`): a secondary observing a
//! genuinely-applied `PrimaryChanged` re-arms its one-shot reporter and
//! RE-ANNOUNCES `MeshReady` to the new primary. This file pins the
//! PRIMARY-side composition those re-announces ride:
//!   - the re-announced `MeshReady`s arrive through the real inbound
//!     path and (duplicate-tolerantly) seed the confirmed set, so a
//!     mid-run injected batch interleaves across the WHOLE live fleet
//!     (the #349 spread contract) instead of packing;
//!   - the gate's original strand-prevention purpose
//!     (run_20260610_105906) is preserved: a member whose re-announce
//!     never arrives — its leg is dead, which is exactly what the gate
//!     exists to detect — still gets NO proactive work, even while it
//!     keepalives (liveness frames are NOT leg confirmation; only
//!     `MeshReady` is).

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TypeId};
use dynrunner_protocol_primary_secondary::KeepaliveRole;

use crate::primary::command_channel::handle_primary_command;
use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, drain_worker_signal_batch};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A zero-dep task in phase "p", type "default" (the injected-batch
/// shape: `affinity_id = None` → free-pool bucket, like the consumer's
/// memmap items).
fn ptask(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 50);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t.task_id = name.into();
    t
}

/// A secondary-emitted `Keepalive` — the routine liveness frame every
/// operational member sends the primary on its keepalive cadence.
/// Under the pairwise design this is NOT mesh-leg confirmation.
fn keepalive_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// The `MeshReady` a secondary (re-)announces after observing
/// `PrimaryChanged` — the frame the secondary-side fix guarantees every
/// already-operational member sends the NEW primary.
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// Seed the replicated ledger with the PROMOTED-primary roster shape:
/// the phase graph plus one `SecondaryCapacity` record per member, then
/// rebuild pool + worker roster through the REAL promoted-primary
/// builders (`hydrate_from_cluster_state` +
/// `reconstruct_workers_from_cluster_state` — the exact pair
/// `seed_from_promotion_snapshot` runs). Deliberately NOT
/// `register_idle_worker_for_test`, which marks members mesh-confirmed:
/// the point of this shape is that a promoted primary's
/// mesh-confirmation set starts EMPTY.
fn seed_promoted_roster(primary: &mut TestPrimary, secondary_ids: &[String], workers_each: u32) {
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
        for id in secondary_ids {
            cs.apply(ClusterMutation::SecondaryCapacity {
                secondary: id.clone(),
                worker_count: workers_each,
                resources: vec![ResourceAmount {
                    kind: ResourceKind::memory(),
                    amount: 8 * 1024 * 1024 * 1024,
                }],
            });
        }
    }
    primary.hydrate_from_cluster_state();
    primary.reconstruct_workers_from_cluster_state();
}

/// Spawn `tasks` through the REAL runtime-injection path — the
/// `PrimaryCommand::SpawnTasks` handler the consumer's
/// `on_phase_end → primary_handle.spawn_tasks` lands on — then service
/// the `TasksAdded` it emits exactly as the operational loop's
/// worker-management arm does (drain the bus → `react_to_worker_signal_batch`
/// → `dispatch_to_idle_workers`).
async fn inject_and_recheck(
    primary: &mut TestPrimary,
    wm_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
    tasks: Vec<TaskInfo<TestId>>,
) {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    handle_primary_command(
        primary,
        PrimaryCommand::SpawnTasks {
            tasks,
            reply: reply_tx,
        },
        &mut None,
    )
    .await;
    let errors = reply_rx
        .await
        .expect("spawn reply oneshot closed")
        .expect("spawn_tasks failed");
    assert!(errors.is_empty(), "no spawn rejections expected: {errors:?}");

    let batch = drain_worker_signal_batch(wm_rx, Duration::from_millis(50))
        .await
        .expect("runtime spawn must emit a TasksAdded batch");
    primary.react_to_worker_signal_batch(batch).await;
    settle_pump().await;
}

/// Drain every `TaskAssignment` from a secondary's wire end, returning
/// `(task_id, local_worker_id)` pairs.
fn drain_assignments(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, u32)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            binary_info,
            worker_id,
            ..
        } = msg
        {
            out.push((binary_info.task_id, worker_id));
        }
    }
    out
}

/// THE round-2 production composition. Promoted-primary shape (empty
/// mesh-confirmation set, roster rebuilt from replicated capacity), six
/// live members × 3 workers. A prior small phase ran one task on one
/// member (so that member is in `members_dispatched_to`, exactly like
/// the production unify phase). Every member's RE-ANNOUNCED `MeshReady`
/// (the frame the secondary-side fix emits on observing
/// `PrimaryChanged`) arrives through the real inbound path — one of
/// them TWICE, pinning that `handle_mesh_ready` tolerates the
/// duplicates an idempotent re-announce can produce. Then a 12-task
/// batch — fewer tasks than free slots — is injected mid-run through
/// the real `spawn_tasks → TasksAdded → recheck` path.
///
/// Contract (the #349 spread contract, now on the injected path with
/// the gate active): the WHOLE batch dispatches in the recheck,
/// interleaved across members — exactly 2 per secondary, nothing left
/// in the pool. WITHOUT the re-announces this exact shape degrades to
/// the production pack: the prior-phase member is gate-blocked
/// outright, every other member gets exactly ONE task before
/// `originate_task_assigned` flips it into the gated class mid-tick,
/// and 7 of 12 tasks strand in the pool with 13 workers idle (the
/// withholding half is pinned by
/// `injected_batch_withholds_from_member_whose_reannounce_never_arrived`).
#[tokio::test(flavor = "current_thread")]
async fn injected_batch_on_promoted_primary_spreads_across_live_fleet() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(6);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let ids: Vec<String> = ends.iter().map(|(id, _, _)| id.clone()).collect();

            // Worker-management bus: one installed sender for the whole
            // test, drained per stage (mirrors the operational loop's
            // single bus).
            let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            seed_promoted_roster(&mut primary, &ids, 3);

            // ── Prior phase: one task, dispatched + completed on ONE member ──
            inject_and_recheck(&mut primary, &mut wm_rx, vec![ptask("u0")]).await;

            let mut unify_member: Option<(usize, u32)> = None;
            for (idx, (_, rx, _)) in ends.iter_mut().enumerate() {
                for (task_id, worker_id) in drain_assignments(rx) {
                    assert_eq!(task_id, "u0", "only u0 exists in the prior phase");
                    assert!(unify_member.is_none(), "u0 must dispatch exactly once");
                    unify_member = Some((idx, worker_id));
                }
            }
            let (unify_idx, unify_worker) =
                unify_member.expect("the prior-phase task must dispatch");

            // Its terminal arrives through the REAL inbound path
            // (`dispatch_message`), like every production frame.
            let unify_id = ids[unify_idx].clone();
            primary
                .dispatch_message(
                    DistributedMessage::TaskComplete {
                        target: None,
                        sender_id: unify_id.clone(),
                        timestamp: 0.0,
                        secondary_id: unify_id.clone(),
                        worker_id: unify_worker,
                        task_hash: compute_task_hash(&ptask("u0")),
                        result_data: None,
                        delivery_seq: None,
                    },
                    &mut None,
                )
                .await
                .expect("TaskComplete dispatch");

            // ── The fleet's re-announced MeshReady frames land (the
            // secondary-side fix: every member that observed
            // PrimaryChanged re-sends). The prior-phase member's frame
            // arrives TWICE — a duplicate the unconditional
            // `handle_mesh_ready` insert must tolerate. ──
            for id in &ids {
                primary
                    .dispatch_message(mesh_ready_from(id), &mut None)
                    .await
                    .expect("MeshReady dispatch");
            }
            primary
                .dispatch_message(mesh_ready_from(&unify_id), &mut None)
                .await
                .expect("duplicate MeshReady dispatch");
            // Discard any bus signals the pre-injection stages emitted —
            // the injection below must stand on its own TasksAdded.
            while wm_rx.try_recv().is_ok() {}

            // ── Mid-run injection: 12 tasks over 18 idle slots ──
            let batch: Vec<TaskInfo<TestId>> =
                (0..12).map(|i| ptask(&format!("m{i}"))).collect();
            inject_and_recheck(&mut primary, &mut wm_rx, batch).await;

            let per_secondary: Vec<usize> = ends
                .iter_mut()
                .map(|(_, rx, _)| drain_assignments(rx).len())
                .collect();
            let total: usize = per_secondary.iter().sum();

            assert_eq!(
                total, 12,
                "the WHOLE injected batch must dispatch in the recheck — idle \
                 capacity (18 slots) exceeds the batch (12); leaving tasks \
                 pooled while workers idle is the production starve/pack; \
                 per-secondary: {per_secondary:?}"
            );
            assert!(
                per_secondary.iter().all(|&n| n == 2),
                "the injected batch must interleave across the live fleet \
                 (12 tasks / 6 members = exactly 2 each, the #349 spread \
                 contract); got {per_secondary:?}"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "no injected task may strand QUEUED in the pool while \
                 workers idle (the 12 dispatched are in-flight, not queued)"
            );
        })
        .await;
}

/// Gate-purpose preservation (the eda0f216 contract under the pairwise
/// design): a member that already got work and whose `MeshReady`
/// (re-)announce NEVER arrived must still receive NO proactive work —
/// EVEN while it keepalives. Liveness frames are not leg confirmation:
/// the half-joined strand member (run_20260610_105906) keepalives
/// healthily while its mesh egress leg silently swallows terminals, so
/// only the `MeshReady` arrival may flip the member assignable. The
/// heard-from-and-confirmed member absorbs the batch.
#[tokio::test(flavor = "current_thread")]
async fn injected_batch_withholds_from_member_whose_reannounce_never_arrived() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(2);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let confirmed_id = ends[0].0.clone();
            let unconfirmed_id = ends[1].0.clone();

            let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            seed_promoted_roster(
                &mut primary,
                &[confirmed_id.clone(), unconfirmed_id.clone()],
                2,
            );

            // BOTH members already got work (no first-dispatch exemption
            // left). Only `confirmed`'s re-announced MeshReady arrives;
            // `unconfirmed` KEEPALIVES (it is alive!) but its
            // confirmation never lands — its mesh leg is dead, the exact
            // member the gate exists to withhold from.
            primary.mark_member_dispatched_for_test(&confirmed_id);
            primary.mark_member_dispatched_for_test(&unconfirmed_id);
            primary
                .dispatch_message(mesh_ready_from(&confirmed_id), &mut None)
                .await
                .expect("MeshReady dispatch");
            primary
                .dispatch_message(keepalive_from(&unconfirmed_id), &mut None)
                .await
                .expect("keepalive dispatch");
            while wm_rx.try_recv().is_ok() {}

            inject_and_recheck(&mut primary, &mut wm_rx, vec![ptask("t0"), ptask("t1")]).await;

            assert_eq!(
                drain_assignments(&mut ends[0].1).len(),
                2,
                "the confirmed member's two idle workers absorb the batch"
            );
            assert!(
                drain_assignments(&mut ends[1].1).is_empty(),
                "NO task may be pushed to the member whose MeshReady never \
                 arrived — keepalives are liveness, not leg confirmation; \
                 its terminals would strand (the run_20260610_105906 class)"
            );
            assert!(
                primary.slot_is_idle_for_test(&unconfirmed_id, 0),
                "the unconfirmed member's workers stay idle"
            );
        })
        .await;
}
