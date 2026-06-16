//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;
use dynrunner_protocol_primary_secondary::address::Destination;

fn make_remote_worker(worker_id: u32, secondary_id: &str, busy: bool) -> RemoteWorkerState<TestId> {
    let state = if busy {
        let task = make_binary("placeholder", 0);
        let task_hash = crate::primary::wire::compute_task_hash(&task);
        crate::primary::SlotState::Assigned {
            task_hash,
            task: std::sync::Arc::new(task),
            estimated: dynrunner_core::ResourceMap::new(),
            // A live-busy worker for the dispatch-ordering tests; provenance
            // is irrelevant to ordering, so the realistic live default.
            provenance: crate::primary::SlotProvenance::Dispatched,
        }
    } else {
        crate::primary::SlotState::Idle
    };
    RemoteWorkerState {
        worker_id,
        secondary_id: secondary_id.into(),
        resource_budgets: dynrunner_core::ResourceMap::new(),
        state,
    }
}

#[test]
fn dispatch_order_equal_load_interleaves_across_secondaries() {
    // Roster laid out GROUPED per secondary (A's workers, then B's).
    // The pre-fix `(static_load, worker_id)` key returned [0, 1, 2, 3]
    // here — both of A's workers ahead of both of B's, i.e. spread was
    // an accident of the roster's interleaved layout, not a property
    // of the ordering policy. The projected-load key interleaves
    // regardless of layout: wave 0 is each secondary's first free
    // worker (tie-broken by worker_id), wave 1 the second.
    let workers = vec![
        make_remote_worker(0, "A", false),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", false),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![0, 2, 1, 3]);
}

#[test]
fn dispatch_order_prefers_less_loaded_secondary() {
    // A has 2 busy + 2 idle (load 2). B has 0 busy + 2 idle (load 0).
    // B's idle workers must come before A's even though A's worker_ids
    // are lower — the pre-fix iteration order would have given A first
    // dibs on tail-of-phase items.
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", true),
        make_remote_worker(2, "A", false),
        make_remote_worker(3, "A", false),
        make_remote_worker(4, "B", false),
        make_remote_worker(5, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![4, 5, 2, 3]);
}

#[test]
fn dispatch_order_excludes_busy_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", true),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![1, 3]);
}

#[test]
fn dispatch_order_empty_workers() {
    let workers: Vec<RemoteWorkerState<TestId>> = vec![];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

#[test]
fn dispatch_order_no_idle_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "B", true),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

/// The fill-to-capacity-then-spill regression at the ordering level:
/// under UNEQUAL load the pre-fix `(static_load, worker_id)` key
/// grouped WHOLE secondaries — every free worker of the least-loaded
/// secondary sorted ahead of every free worker of the next, so a
/// batch of T grants drained one secondary to capacity before the
/// other saw a single task. RED against the old key: granting the
/// first 14 order slots gave A all 14 and B zero. The projected-load
/// key interleaves: a consumer granting down the order behaves like
/// always-pick-the-lowest-projected-load, so after 14 grants the
/// per-secondary PROJECTED loads (busy-at-tick-start + granted)
/// differ by at most 1.
#[test]
fn dispatch_order_unequal_load_interleaves_not_fills() {
    // A: 14 free, load 0. B: 1 busy + 13 free, load 1.
    let mut workers = Vec::new();
    for i in 0..14 {
        workers.push(make_remote_worker(i, "A", false));
    }
    workers.push(make_remote_worker(14, "B", true));
    for i in 15..28 {
        workers.push(make_remote_worker(i, "B", false));
    }
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order.len(), 27);

    // Simulate a 14-task batch granting down the order.
    let mut granted: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for &idx in order.iter().take(14) {
        *granted
            .entry(workers[idx].secondary_id.as_str())
            .or_default() += 1;
    }
    let projected_a = granted.get("A").copied().unwrap_or(0); // load 0 + grants
    let projected_b = 1 + granted.get("B").copied().unwrap_or(0); // load 1 + grants
    assert!(
        granted.get("B").copied().unwrap_or(0) > 0,
        "the loaded secondary must still receive grants (old key gave it zero); \
         granted: {granted:?}"
    );
    assert!(
        projected_a.abs_diff(projected_b) <= 1,
        "projected per-secondary loads must balance to within 1 \
         (A: {projected_a}, B: {projected_b}, granted: {granted:?})"
    );
}

/// The asm-tokenizer production shape (run_20260610_121427): 15
/// secondaries × 14 workers, 28 tasks. Captured distribution was one
/// secondary at its FULL worker set (14), three more at 7/5/2, and
/// ELEVEN at zero — fill-to-capacity-then-spill. With the shared
/// projected-load ordering, a 28-grant batch over the all-idle fleet
/// must give every secondary at least 1 and none more than
/// ceil(28/15) = 2.
///
/// Worker ids mirror `reconstruct_workers_from_cluster_state`'s
/// round-robin construction (round-major), but the assertion holds
/// for any layout — the policy no longer depends on it.
#[test]
fn dispatch_order_production_shape_28_tasks_15_secondaries() {
    let secondaries: Vec<String> = (0..15).map(|s| format!("secondary-{s}")).collect();
    let mut workers = Vec::new();
    let mut worker_id = 0u32;
    for _round in 0..14 {
        for sec in &secondaries {
            workers.push(make_remote_worker(worker_id, sec, false));
            worker_id += 1;
        }
    }
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order.len(), 15 * 14);

    let mut granted: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for &idx in order.iter().take(28) {
        *granted
            .entry(workers[idx].secondary_id.as_str())
            .or_default() += 1;
    }
    assert_eq!(
        granted.len(),
        15,
        "every secondary must appear in a 28-grant batch; granted: {granted:?}"
    );
    let max = granted.values().copied().max().unwrap();
    let min = granted.values().copied().min().unwrap();
    assert!(
        min >= 1 && max <= 2,
        "28 grants over 15 idle secondaries must spread to 1..=2 each \
         (production shape was 14/7/5/2 + eleven zeros); granted: {granted:?}"
    );
}

/// T-#33: initial assignment is round-robin across secondaries AND
/// secondary iteration order is deterministic (sorted by name).
///
/// Setup: 3 secondaries × 1 worker × 3 binaries. With contiguous-
/// per-secondary order (pre-fix) the assignment was still
/// one-per-secondary in this exact-fit case, but the SECONDARY-ID
/// ORDER of which-secondary-got-which-binary was HashMap-random.
/// Post-fix the binaries land in sec-0, sec-1, sec-2 order.
///
/// More important regression case: tasks ≪ total_workers. With
/// pre-fix (contiguous), 3 secondaries × 2 workers × 3 tasks would
/// have given the first secondary 2 tasks and one other secondary
/// 1 task — the third got nothing. Post-fix all three each receive
/// exactly 1. We exercise that exact case here to pin the actual
/// behaviour change, not just the determinism gain.
#[tokio::test(flavor = "current_thread")]
async fn initial_assignment_is_round_robin_and_name_sorted() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(3);

            let config = PrimaryConfig {
                num_secondaries: 3,
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

            // 3 tasks, 3 secondaries × 2 workers = 6 worker slots.
            // The pre-fix contiguous-per-secondary order would have
            // given two secondaries all 3 tasks and one secondary 0.
            // Post-fix every secondary gets exactly 1.
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 50),
                make_binary("c", 50),
            ];

            // Per-secondary initial-assignment count, captured by
            // intercepting each secondary's primary→secondary channel.
            // Forwarder counts InitialAssignment binaries before
            // re-forwarding every message to the real fake-secondary,
            // so the lifecycle still completes via TaskComplete +
            // TaskRequest cycles.
            let counts: Arc<Mutex<std::collections::BTreeMap<String, usize>>> =
                Arc::new(Mutex::new(std::collections::BTreeMap::new()));

            for (id, sec_inbound, sec_outbound) in secondary_ends {
                let (inner_tx, inner_rx) = tokio_mpsc::unbounded_channel();
                let counts_for_secondary = Arc::clone(&counts);
                let id_for_forwarder = id.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_inbound;
                    while let Some(msg) = rx.recv().await {
                        if let DistributedMessage::InitialAssignment {
                            target: _,
                            zip_files,
                            ..
                        } = &msg
                        {
                            let n: usize = zip_files.iter().map(|zf| zf.binaries.len()).sum();
                            counts_for_secondary
                                .lock()
                                .unwrap()
                                .insert(id_for_forwarder.clone(), n);
                        }
                        if inner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                });

                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    inner_rx,
                    sec_outbound,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away,
            // never running the dispatch loop this test asserts).
            seed_operational_ledger(&mut primary, binaries, deps);
            primary
                .run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, ops, ope)
                .await
                .unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);

            // Each of the 3 secondaries must have received exactly 1
            // binary in its InitialAssignment. Pre-fix the
            // contiguous-per-secondary layout produced something like
            // {sec-X: 2, sec-Y: 1, sec-Z: 0} where X/Y/Z were
            // HashMap-random; the secondary that got 0 then had to
            // wait for the operational TaskRequest cycle to receive
            // any work at all.
            let final_counts = counts.lock().unwrap().clone();
            assert_eq!(
                final_counts.len(),
                3,
                "every secondary must receive an InitialAssignment \
                 (even an empty one) so wait_for_setup unblocks; \
                 captured: {:?}",
                final_counts
            );
            for sid in &["sec-0", "sec-1", "sec-2"] {
                let n = final_counts
                    .get(*sid)
                    .copied()
                    .expect("expected secondary missing from captured InitialAssignment");
                assert_eq!(
                    n, 1,
                    "{sid} expected exactly 1 initial-assignment binary, \
                     got {n}. Pre-fix this would fail because contiguous-\
                     per-secondary ordering plus HashMap-random iteration \
                     order gave 2 tasks to one secondary and 0 to another. \
                     Captured: {:?}",
                    final_counts
                );
            }
        })
        .await;
}

/// Drain every frame currently queued on a primary→secondary outbox
/// receiver, after letting the production pump drain the primary's queued
/// egress onto the wire. Returns the collected frames so the test can
/// classify them (plain `TransferComplete` vs a `Relay` envelope wrapping
/// one for a not-directly-connected target).
fn drain_ready(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<DistributedMessage<TestId>> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        out.push(msg);
    }
    out
}

/// Does this frame carry a `TransferComplete` whose ULTIMATE delivery
/// target is `secondary_id`, by the frame's own routing stamp — NOT by
/// which outbox it happened to land on?
///
/// Two shapes count, and ONLY these two:
/// - a plain `TransferComplete` stamped `Destination::Secondary(id)` with
///   `id == secondary_id` (the directed send arriving at its own outbox);
/// - a `Relay` envelope whose `target_id == secondary_id` wrapping a
///   `TransferComplete` (the directed send forwarded to a sibling because
///   the target had no direct primary link — the late-peer path).
///
/// A `Destination::All` broadcast frame is DELIBERATELY excluded: it has
/// no per-secondary target, so it is never "for" a specific id — it is
/// merely delivered to whichever outboxes were connected at send time.
/// Treating it as "for sec-2" would false-green the revert-check (a
/// broadcast landing on sec-0's outbox is delivery to sec-0, not sec-2);
/// the whole point of the bug is that the broadcast NEVER produces a
/// frame whose target is the absent sec-2. The connected-outbox delivery
/// of a broadcast is asserted separately via [`broadcast_delivered_on`].
fn transfer_complete_targeting(frame: &DistributedMessage<TestId>, secondary_id: &str) -> bool {
    match frame {
        DistributedMessage::TransferComplete { target, .. } => {
            matches!(target, Some(Destination::Secondary(id)) if id.as_str() == secondary_id)
        }
        DistributedMessage::Relay {
            target_id, inner, ..
        } => {
            target_id == secondary_id
                && matches!(inner.as_ref(), DistributedMessage::TransferComplete { .. })
        }
        _ => false,
    }
}

/// Did a `Destination::All`-broadcast `TransferComplete` land on this
/// outbox? The broadcast stamps `Destination::All`; the outbox owner is
/// implied by WHICH receiver the frame arrived on (the caller drains the
/// specific connected secondary's outbox), so this is the
/// outbox-keyed delivery check for the revert path. Distinct from
/// [`transfer_complete_targeting`], which is the per-target routing
/// check.
fn broadcast_delivered_on(frame: &DistributedMessage<TestId>) -> bool {
    matches!(
        frame,
        DistributedMessage::TransferComplete {
            target: Some(Destination::All),
            ..
        }
    )
}

/// LMU-gating relocation-completeness regression (asm-dataset
/// run_20260609_065317): the setup-gating `TransferComplete` frame must
/// reach a secondary that is in the CRDT roster but whose peer-mesh link
/// to the primary has NOT yet registered when the primary sends — exactly
/// as the `InitialAssignment` frame already does over the directed
/// router/relay path. The pre-fix `Destination::All` broadcast was a
/// fire-once snapshot over the currently-connected outboxes (no relay, no
/// replay), so a late-registering secondary PERMANENTLY missed it and
/// wedged in `wait_for_setup` until its setup deadline killed it.
///
/// Setup: 3 CRDT-known secondaries (sec-0, sec-1, sec-2), but the
/// primary's transport holds a DIRECT outbox to sec-0 and sec-1 ONLY —
/// sec-2 models the late-registering peer reachable solely via relay
/// through a connected sibling. `send_transfer_complete` must therefore
/// emit a `Relay` envelope wrapping sec-2's `TransferComplete` to the
/// lowest-id forwarder (sec-0), so sec-2 still gets its gate-release.
///
/// REVERT-CHECK (the `connected_snapshot_broadcast_misses_late_peer`
/// sibling below): driving the SAME transport's `broadcast` (the pre-fix
/// `Destination::All` path) emits NO relay envelope for sec-2 and reaches
/// only the connected outboxes — sec-2 is never addressed, reproducing
/// the wedge.
#[tokio::test(flavor = "current_thread")]
async fn transfer_complete_relays_to_late_registering_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Primary transport: a DIRECT outbox to sec-0 and sec-1 only.
            // sec-2 is deliberately absent from `outgoing` — it is the
            // CRDT-known-but-not-yet-mesh-registered late peer.
            let (to_sec0_tx, mut to_sec0_rx) = tokio_mpsc::unbounded_channel();
            let (to_sec1_tx, mut to_sec1_rx) = tokio_mpsc::unbounded_channel();
            let (_inbound_tx, inbound_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert("sec-0".to_string(), to_sec0_tx);
            outgoing.insert("sec-1".to_string(), to_sec1_tx);
            let transport = ChannelPeerTransport::<TestId>::from_raw_channels(
                dynrunner_core::SETUP_NODE_ID.into(),
                outgoing,
                inbound_rx,
            );

            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed the replicated ledger so `known_secondaries()` carries
            // all THREE — the roster `send_transfer_complete` (like
            // `perform_initial_assignment`) fans over. sec-2 is in the
            // roster despite having no direct primary outbox.
            {
                let cs = primary.cluster_state_mut_for_test();
                for sid in ["sec-0", "sec-1", "sec-2"] {
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: sid.to_string(),
                        worker_count: 1,
                        resources: vec![],
                    });
                }
            }

            // The fix under test: fan the setup-gating TransferComplete
            // per CRDT-known secondary over the directed router path.
            primary.send_transfer_complete().await.unwrap();
            // Let the production pump drain the primary's queued egress onto
            // the wire (and the router resolve direct-vs-relay).
            settle_pump().await;

            let sec0_frames = drain_ready(&mut to_sec0_rx);
            let sec1_frames = drain_ready(&mut to_sec1_rx);

            // sec-0 and sec-1 each receive their OWN directed
            // `Destination::Secondary(id)` TransferComplete (the
            // directly-connected targets).
            assert!(
                sec0_frames
                    .iter()
                    .any(|f| transfer_complete_targeting(f, "sec-0")),
                "sec-0 (directly connected) must receive its directed TransferComplete; \
                 got {sec0_frames:?}"
            );
            assert!(
                sec1_frames
                    .iter()
                    .any(|f| transfer_complete_targeting(f, "sec-1")),
                "sec-1 (directly connected) must receive its directed TransferComplete; \
                 got {sec1_frames:?}"
            );

            // THE FIX: sec-2 — CRDT-known but NOT a direct primary outbox —
            // still gets its gate-release, via a Relay envelope targeting
            // sec-2 forwarded to the lowest-id connected sibling (sec-0).
            // Without this the late peer wedges in wait_for_setup. Under the
            // pre-fix `Destination::All` broadcast NO such relay frame is
            // ever produced (proven by `connected_snapshot_broadcast_misses_late_peer`).
            let sec2_relayed_via_sec0 = sec0_frames
                .iter()
                .any(|f| transfer_complete_targeting(f, "sec-2"));
            assert!(
                sec2_relayed_via_sec0,
                "sec-2 (late-registering, relay-only) must receive a relayed \
                 TransferComplete (Relay envelope target_id=sec-2) via the lowest-id \
                 forwarder (sec-0); got {sec0_frames:?}"
            );
            // And NO broadcast-shaped (Destination::All) TransferComplete is
            // emitted — the fix is purely directed sends, so the revert-check
            // matcher's broadcast form must be absent here.
            assert!(
                !sec0_frames.iter().any(broadcast_delivered_on)
                    && !sec1_frames.iter().any(broadcast_delivered_on),
                "the fixed path emits only directed sends, never a Destination::All \
                 broadcast; got sec0={sec0_frames:?} sec1={sec1_frames:?}"
            );
        })
        .await;
}

/// REVERT-CHECK companion to
/// [`transfer_complete_relays_to_late_registering_secondary`]: pins that
/// the pre-fix `Destination::All` broadcast — a fire-once snapshot over
/// the currently-connected outboxes — does NOT reach a late-registering
/// secondary. Drives the transport's `broadcast` directly (the exact
/// mechanism `send_to(Destination::All, ..)` resolves to inside the pump)
/// with a TransferComplete over the SAME {sec-0, sec-1} connection set
/// while sec-2 is absent, and asserts NO relay-for-sec-2 is emitted. This
/// is the behaviour that wedged sec-2 in the real run; the fix above
/// routes around it.
#[tokio::test(flavor = "current_thread")]
async fn connected_snapshot_broadcast_misses_late_peer() {
    use dynrunner_protocol_primary_secondary::PeerTransport;

    let (to_sec0_tx, mut to_sec0_rx) = tokio_mpsc::unbounded_channel();
    let (to_sec1_tx, mut to_sec1_rx) = tokio_mpsc::unbounded_channel();
    let (_inbound_tx, inbound_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("sec-0".to_string(), to_sec0_tx);
    outgoing.insert("sec-1".to_string(), to_sec1_tx);
    let mut transport = ChannelPeerTransport::<TestId>::from_raw_channels(
        dynrunner_core::SETUP_NODE_ID.into(),
        outgoing,
        inbound_rx,
    );

    transport
        .broadcast(DistributedMessage::TransferComplete {
            target: Some(Destination::All),
            sender_id: dynrunner_core::SETUP_NODE_ID.into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .await
        .unwrap();

    let sec0_frames = drain_ready(&mut to_sec0_rx);
    let sec1_frames = drain_ready(&mut to_sec1_rx);

    // The connected peers DO get the plain `Destination::All` broadcast
    // TransferComplete on their own outboxes...
    assert!(
        sec0_frames.iter().any(broadcast_delivered_on),
        "sec-0 (connected) gets the broadcast TransferComplete; got {sec0_frames:?}"
    );
    assert!(
        sec1_frames.iter().any(broadcast_delivered_on),
        "sec-1 (connected) gets the broadcast TransferComplete; got {sec1_frames:?}"
    );
    // ...but the broadcast produces NO frame whose ULTIMATE target is the
    // absent sec-2 — no relay envelope, no directed send — and there is no
    // sec-2 outbox to deliver to. THIS is the fire-once-snapshot miss that
    // wedged sec-2 in `wait_for_setup`; the fixed path (asserted in
    // `transfer_complete_relays_to_late_registering_secondary`) emits a
    // Relay-to-sec-2 here instead.
    let any_target_sec2 = sec0_frames
        .iter()
        .chain(sec1_frames.iter())
        .any(|f| transfer_complete_targeting(f, "sec-2"));
    assert!(
        !any_target_sec2,
        "pre-fix Destination::All broadcast must NOT produce any TransferComplete \
         targeting the late peer sec-2 — got sec0={sec0_frames:?} sec1={sec1_frames:?}"
    );
}
