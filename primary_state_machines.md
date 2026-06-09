# Primary Coordinator — State-Machine Map

Worktree: `dynamic_runner-om-backlog` @ `handoff/open-backlog de51731b`
Scope: `crates/dynrunner-manager-distributed/src/primary/**` + `cluster_state/**`
Read-only extraction. All citations are `file:line` against the tree above.

---

## 1. Preamble — the guiding principle and how to read this doc

**The principle (the lens for the whole doc).** The primary is a *pure
function of the CRDT* (`ClusterState<I>`, `cluster_state/state.rs`). A primary
handed a populated CRDT is **RESUMING**, not starting; every action it takes
must be *derived* from CRDT contents, never from an independent "startup
procedure." Cold-start vs resume is therefore **not a code fork** — it is the
same state machine driven by different CRDT *contents*:

* empty CRDT  ⇒ seed it (`originate_cold_seed`) ⇒ then proceed;
* populated CRDT ⇒ continue (`seed_from_promotion_snapshot` already restored it).

The IDEAL shape is: ONE init (`hydrate_from_cluster_state`, the sole pool /
roster / `total_tasks` builder), and ONE operational machine, with the CRDT as
the single source of truth for every transition. The code is MOSTLY this — but
not entirely; section 5 enumerates every place it diverges (acts on `self.*`
mutable state, or runs an independent step) so the redesign can close the gap.

**How to read.** The state machine is *implicit* — Rust has no `enum State`
for the top-level lifecycle; states exist as positions in the `run_pipeline`
call chain and as `select!` arms in `operational_loop`. This doc makes those
implicit states **explicit boxes**. A box that owns a nested machine points at
that machine's entry node, e.g. `→ see WORKER machine: [Idle]`. Three things
ARE real enums and drive nested machines: `TaskState<I>`
(`cluster_state/types.rs:30`), `SlotState<I>` (`coordinator.rs:45`), and the
phase rollup projection `PhaseRollup` (`types.rs:505`). Transitions are
annotated with their trigger (the CRDT mutation / wire event / timer) and a
`file:line`.

Convention in the ASCII graphs:
* `[Name]`  — a state (box).
* `==>`     — the happy-path spine.
* `-->`     — a branch / conditional edge.
* `(impl)`  — an IMPLIED state: it exists in the code flow but is NOT a named
  enum variant (no `State::X`); it is a position in a function chain or a latch.

---

## 2. TOP-LEVEL primary state machine

Owner: `process/run/mod.rs` (the `Node` lifecycle loop) drives entry/exit;
`primary/coordinator.rs::run_pipeline` (`coordinator.rs:2516`) is the body.
The single CRDT-derived init is the `SeedSource` match at `coordinator.rs:2597`
followed by the UNCONDITIONAL `hydrate_from_cluster_state` at
`coordinator.rs:2610`.

```
                      ╔════════════════════════════════════════════════╗
                      ║  ENTRY  (Node::run, process/run/mod.rs:113/201) ║
                      ╚════════════════════════════════════════════════╝
                                 |                         |
            bootstrap submitter  |                         |  promotion signal
          primary_run_args=Some  |                         |  (PromotionSignal)
        spawn_primary_with(...)  |                         |  self_build_promoted_primary
          run_inputs.rs:140      |                         |  run/promotion.rs:35
                                 v                         v
                  [Cold init seed input]        [Promotion init seed input]   (impl)
                  SeedSource::ColdStart          SeedSource::PromotionSnapshot
                  {binaries, phase_deps}         (carries nothing;
                  run_inputs.rs:51               coordinator ALREADY
                                                 seed_from_promotion_snapshot'd
                                                 by the builder BEFORE run —
                                                 coordinator.rs:1126,
                                                 run/promotion.rs:62)
                                 |                         |
                                 |                         |
                                 v                         v
        ┌───────────────────────────────────────────────────────────────────┐
        │  [run_pipeline entry / per-run reset]                       (impl)  │
        │  coordinator.rs:2516–2582                                           │
        │  · reset stranded/spawn_rejected/setup_deadline/wm_fail/abort       │
        │  · spawn_run_dispatchers() (lifecycle + task-completed)  :2582      │
        │  NOTE: setup_pending() is CRDT-derived (task_count()==0), NOT a     │
        │        latch field — coordinator.rs:2045                            │
        └───────────────────────────────────────────────────────────────────┘
                                 |
                                 v
        ┌───────────────────────────────────────────────────────────────────┐
        │  [CRDT-derived init]  — THE single init seam                        │
        │  coordinator.rs:2597–2610                                           │
        │   match seed {                                                      │
        │     ColdStart       => originate_cold_seed(..)  ingest.rs:71        │
        │                        (seeds LOCAL CRDT: PhaseDepsSet + TaskAdded  │
        │                         fan-out + #2 InvalidTask; STAGES broadcast) │
        │     PromotionSnapshot => {} (CRDT already restored)                 │
        │   }                                                                 │
        │   hydrate_from_cluster_state()  hydrate.rs:69   ◄── SOLE builder of │
        │     pool, in_flight ledger, completed_tasks, all_binaries,         │
        │     failed_tasks, total_tasks, AND (via reconstruct_*) the worker  │
        │     roster + secondary roster — ALL pure derived caches of the CRDT │
        │   total = self.total_tasks  :2612                                   │
        └───────────────────────────────────────────────────────────────────┘
                                 |
                                 v
        ┌───────────────────────────────────────────────────────────────────┐
        │  [Wait-for-mesh chain]  (a serial pipeline of implied wait states)  │
        │  coordinator.rs:2631–2857   (command_rx taken out :2631)            │
        │                                                                     │
        │   [Await connections] wait_for_connections  :2634                   │
        │       ◄ welcome ⇒ handle_welcome originates PeerJoined +            │
        │         SecondaryCapacity into CRDT (connect.rs:314)  ── grows the  │
        │         CRDT roster that hydrate/reconstruct read FROM              │
        │   [fire_initial_phase_starts]  :2669  → see PHASE machine:[Active]  │
        │   [empty-phase cascade] if !setup_pending  :2727                    │
        │       drain_empty_active_phases + process_phase_lifecycle           │
        │   [#3a abort gate] fire_pending_run_abort  :2741 ─err→ [Terminal]   │
        │   [auto-stage] maybe_auto_stage_initial  :2769                      │
        │   [send_peer_lists]  :2772                                          │
        │   [Await peer connections] wait_for_peer_connections  :2775         │
        │   [originate_primary_membership]  :2786 (self PeerJoined into CRDT)  │
        │   [rebroadcast_full_roster]  :2801                                  │
        │   ┌── if required_setup_on_promote ───────────────────────────┐    │
        │   │  emit_setup_defer_handshake :2826 (NO seed, NO assignment) │    │
        │   │  else                                                      │    │
        │   │  broadcast_cold_seed :2828  +  perform_initial_assignment  │    │
        │   │       :2829  → see WORKER machine:[Idle] (roster push!)    │    │
        │   └────────────────────────────────────────────────────────────┘   │
        │   [send_transfer_complete]  :2833                                   │
        │   [Await mesh ready] wait_for_mesh_ready  :2849                     │
        │       (blocks until every connected secondary reported MeshReady;   │
        │        bounded by mesh_ready_timeout)  promotion.rs:33              │
        │   command_rx put back  :2857                                        │
        │   emit "initial setup done" (IMPORTANT)  :2877                      │
        └───────────────────────────────────────────────────────────────────┘
                                 |
                                 v
        ┌───────────────────────────────────────────────────────────────────┐
        │  [Activate local primary]  activate_local_primary  promotion.rs:168 │
        │   · self.primary_id = own id                                        │
        │   · originate_primary_changed  (PrimaryChanged{epoch+1} → CRDT)     │
        │       — UNIFORM: bootstrap AND promotion both announce here         │
        │   · broadcast_primary_keepalive (assert liveness at authority)      │
        └───────────────────────────────────────────────────────────────────┘
                                 |
                                 v
        ┌───────────────────────────────────────────────────────────────────┐
        │  [OPERATIONAL]  run_operational_and_finalize  coordinator.rs:2905   │
        │   operational_loop()  operational_loop.rs:132  (the big select!)    │
        │      top-of-loop: run_complete_check()  :268  ─true→ break          │
        │      arms: inbox.recv → dispatch_message  :510                      │
        │            command_rx (PrimaryHandle)     :388                      │
        │            matcher batch                  :429                      │
        │            worker-mgmt batch              :468  → see WORKER machine │
        │            heartbeat tick                 :548  → see WORKER machine │
        │            anti-entropy tick              :562                      │
        │            respawn req / join             :576/:613                 │
        │            panik signal                   :648 ─→ panik_outcome     │
        │            setup-promote deadline         :691 ─→ deadline_outcome  │
        │            stuck-worker 5-min watchdog     :715                     │
        │   (dispatch_message routes every TaskComplete/TaskFailed/TaskRequest │
        │    /Keepalive/ClusterMutation → TASK + WORKER + PHASE machines)     │
        └───────────────────────────────────────────────────────────────────┘
                  |                      |                       |
   loop broke via |         panik /      |   demote_rx fired     |  clean / collapse
   run_complete   |  wm-fail / deadline  |   (run_consuming only) |
                  v                      v                       v
        ┌──────────────────┐   ┌──────────────────┐   ┌────────────────────────┐
        │ [Finalize tail]  │   │ [Structured-abort│   │ [DEMOTE → RELOCATE]     │
        │ coordinator.rs:  │   │  return] :2916–   │   │ (impl) run_consuming    │
        │ 2964–3104        │   │  2962             │   │ select! :2368           │
        │ · run_retry_     │   │ · PanikShutdown   │   │ Pipeline::Demoted       │
        │   passes (no-op) │   │ · Other(wm reason)│   │  ⇒ into_observer_handoff│
        │ · drain_pending  │   │ · SetupDeadline   │   │     :2458 (FULL         │
        │ · final          │   │   Expired         │   │     destructure → drop  │
        │   accounting     │   │   (skip retry/    │   │     scheduler/pool/     │
        │   (stranded =    │   │    drain/         │   │     secondaries; carry  │
        │   total − term)  │   │    RunComplete)   │   │     CRDT+mesh+started_  │
        │ · terminal       │   └────────┬─────────┘   │     phases)             │
        │   broadcast:     │            |             │ ⇒ PrimaryRunOutcome::    │
        │   RunComplete    │            |             │   Relocated{handoff}     │
        │   (stranded==0)  │            |             └───────────┬─────────────┘
        │   | RunAborted   │            |                         |
        │   (stranded>0)   │            |                         v
        │   :3051          │            |             ┌────────────────────────┐
        │ · settle window  │            |             │ Node swaps slot in place│
        └────────┬─────────┘            |             │ swap_primary_to_observer│
                 |                       |             │ run/swap.rs:31          │
                 v                       v             │ ObserverCoordinator::    │
        ╔════════════════════════════════════════╗   │ from_handoff → run()    │
        ║ [TERMINAL]                              ║   │ (RESUMES off inherited  │
        ║ run_consuming ⇒ PrimaryRunOutcome::Local║   │  CRDT, zero authority)  │
        ║ {result, completed, failed, stranded}   ║   └─────────────────────────┘
        ║ coordinator.rs:2396 ; cleanup_run_      ║          (separate machine,
        ║ dispatchers :2395 ; Node maps Ok⇒Done,  ║           out of scope here)
        ║ Err⇒Failed  run/mod.rs:219              ║
        ╚════════════════════════════════════════╝
```

**Implied (un-named-enum) top-level states made explicit above:**
* `[run_pipeline entry / per-run reset]` — coordinator.rs:2516
* `[CRDT-derived init]` — the `SeedSource` match + unconditional hydrate (:2597)
* every `[Await *]` wait state — positions in the serial pre-loop chain
* `[Activate local primary]` — promotion.rs:168
* `[Finalize tail]` / `[Structured-abort return]` — coordinator.rs:2905–3104
* `[DEMOTE → RELOCATE]` — the `Pipeline::Demoted` arm of `run_consuming` (:2403)
* `setup_pending` is a CRDT-derived *predicate*, not a state — but it GATES the
  empty-phase cascade (:2727), `run_complete_check` (:88), and arms the
  setup-promote deadline; treat it as a guard, not a box.

---

## 3a. NESTED MACHINE — PHASE state machine

Two representations coexist and MUST agree:
* the AUTHORITATIVE CRDT projection `PhaseRollup` (`types.rs:505`, derived by
  `phase_rollups` `accessors.rs:233`) — pure function of `tasks` × `phase_deps`;
  this is what a zero-authority observer / promoted primary reads.
* the primary-LOCAL `PendingPool` phase state machine
  (Active/Draining/Drained/Done) driven by `process_phase_lifecycle`
  (`coordinator.rs:3312`). The pool is a *derived cache* of the CRDT, rebuilt
  by `hydrate` on every (re)entry.

Entry node is the rollup view of a phase. Triggers are CRDT task transitions
(which move the rollup) and pool-cascade ticks.

```
   ENTRY: [phase not in ledger]  (vacuous: has_any=false, has_live=false,
            dispatchable iff all deps !has_live)   accessors.rs:233
                 |
   first TaskAdded for this phase (originate_cold_seed ingest.rs:201,
   or ingest_setup_discovery, or TasksSpawned)        apply.rs:80
                 v
        [has_any=true, has_live=true]   ── the phase OWNS live work
                 |                                  rollup: types.rs:505
       phase is dispatchable iff every dep phase is !has_live
       (phase_dispatchable, accessors.rs:268)
                 |
       pool side: phase becomes Active → fire_initial_phase_starts
       (coordinator.rs:3171) inserts into phase_started_emitted ── ONCE per phase
       emits "starting job phase" (IMPORTANT) + on_phase_start cb
       + WorkerMgmtSignal::PhaseStartedNeedsWorkers{min} (:3198)
                 v
        [draining]  every task reaching a terminal CRDT state drops has_live
                 |   note_item_completed/failed bump replicated EVENT tally
                 |   (PhaseTally::Completed/Failed, coordinator.rs:3521/3553)
                 |   pool: poll_drain_transitions yields Drained phases (:3335)
                 v
        [retry-bucket cascade]  try_run_phase_retry_bucket
                 |   Recoverable bucket then OOM bucket  (:3362/:3373)
                 |   if a bucket reinjected → phase flips Drained→Active,
                 |   loop `continue`s (re-enters [has_live] above)
                 v
        [drain decision]  phase_can_proceed(p, completed, failed)
                 |        coordinator.rs:3269  (reads EVENT tallies)
          +------+-------------------------------+
          | proceed (completed>0 OR failed>0     | FAIL (no terminal accounting
          |  OR phase_min_workers==0)            |  yet residual work remains)
          v                                      v
   [Done]  mark_phase_done(p)            emit WorkerMgmtSignal::RunShouldFail
   pool flips dependents Blocked→Active   (coordinator.rs:3474) → see WORKER
   :3471 ; fire_initial_phase_starts      machine:[RunShouldFail handling]
   for newly-Active (:3485)               (phase layer NEVER breaks the loop
          |                                itself — decoupling law)
          v
   [has_any=true, has_live=false]  (terminal phase; dependents now dispatchable)
```

Trigger table (CRDT-event → phase effect):
| Trigger (CRDT mutation / event)        | file:line               | Phase effect                          |
|----------------------------------------|-------------------------|---------------------------------------|
| `TaskAdded` (seed/discovery)           | apply.rs:80             | phase gains live work → Active          |
| `TasksSpawned` (runtime)               | apply_tasks.rs:224      | adds Pending/Blocked entries           |
| `TaskCompleted`                        | apply.rs:125            | drops has_live for that task; tally++  |
| `TaskFailed` (terminal classes)        | apply.rs:150            | drops has_live; tally++                |
| pool drain edge                        | coordinator.rs:3335     | Drained phases surface for cascade     |
| retry bucket reinject                  | coordinator.rs:3362     | Drained→Active (phase re-lives)        |
| `mark_phase_done`                      | coordinator.rs:3471     | dependents Blocked→Active              |

---

## 3b. NESTED MACHINE — WORKER (slot) state machine

`SlotState<I>` is a REAL two-state enum (`coordinator.rs:45`): `Idle |
Assigned{task_hash, task, estimated}`. Assignment is reachable ONLY from `Idle`
(`RemoteWorkerState::assign`, :117, `debug_assert!`s the pre-state). A slot
returns to `Idle` only through `free_slot_on_terminal` keyed by the held hash
(:1727). On top of the per-slot enum sits the ROSTER lifecycle (a slot exists /
is dropped on death) and the SECONDARY-liveness machine that owns death
detection.

```
   ENTRY: [Idle]  SlotState::Idle  (coordinator.rs:46)
      built by EITHER:
        · reconstruct_workers_from_cluster_state  hydrate.rs:406
          (derived from CRDT: known_secondaries × SecondaryCapacity × InFlight)
        · perform_initial_assignment push           assignment.rs:110  (‡ see §5)
                 |
      scheduler picks this slot (assign_initial / handle_task_request)
      commit_assignment(worker_idx, task, hash, est)  coordinator.rs:1602
        · reserve_type_slot(type_id)
        · slot.assign(...)  Idle→Assigned
        · in_flight.insert(hash → InFlightEntry{phase, secondary, local_id})
        · originate TaskAssigned (Pending→InFlight in CRDT)  assignment.rs:280
                 v
   [Assigned{task_hash}]  SlotState::Assigned (coordinator.rs:52)
      held task identity == the in_flight ledger key == the CRDT InFlight worker
                 |
        +--------+----------------------------------+---------------------------+
        | terminal (TaskComplete/TaskFailed)        | send-failure rollback     | secondary death
        | free_slot_on_terminal(sec, wid, hash)     | rollback_assignment       | requeue_dead_secondary
        | coordinator.rs:1727                        | coordinator.rs:1635       | heartbeat/mod.rs:242
        | · verify held hash matches (else no-op)   | · release_type_slot       | · recover_inflight_for_
        | · slot→Idle ; in_flight.remove ;          | · slot.vacate()           |   dead_secondary
        |   release_type_slot                       | · in_flight.remove        |   (InFlight→Pending in
        | · return entry → note_item_* (PHASE)      |                           |   CRDT via TaskRequeued)
        v                                            v                           | · workers.retain(drop)
   [Idle]  (re-dispatchable)                    [Idle] + binary requeued        v
                                                                          [slot removed]
                                                              (worker gone; PeerRemoved
                                                               into CRDT; TimeoutDetected
                                                               to survivors)
```

SECONDARY-liveness sub-machine (owns "is the holder still alive?"):

```
   [welcome] handle_welcome connect.rs:243 → seed_keepalive (heartbeat:140)
        → originate PeerJoined + SecondaryCapacity into CRDT (connect.rs:314)
                 |
   [Operational]  SecondaryConnectionState::Operational  (only state the
        death-clock applies to — collect_heartbeat_report gate heartbeat:171)
                 |
        every keepalive: record_keepalive (heartbeat:118) resets the clock
        + clears silence_warn_stage
                 |
        heartbeat tick → process_heartbeat_tick (heartbeat:387)
          → decide_dead_secondaries (heartbeat:416): silence age crosses
            staged WARNs, then HARD backstop → declare_silent_secondaries_dead
                 v
   [dead] requeue_dead_secondary (heartbeat:242): see WORKER:[slot removed]
        + emit WorkerMgmtSignal::TasksAdded (heartbeat:380)
```

WORKER-MANAGEMENT bus reaction (the decoupled recheck; `worker_mgmt.rs:44`):

```
   [parked]  worker-mgmt select! arm awaits a coalesced WorkerSignalBatch
                 |
        +--------+------------------------+--------------------------+
   TasksAdded                  PhaseStartedNeedsWorkers{phase,min}   RunShouldFail{reason}
        |                                |                                |
   dispatch_to_idle_workers(bypass      handle_phase_started_needs_      handle_run_should_fail
   backpressure)  worker_mgmt.rs:73     workers worker_mgmt.rs:128       worker_mgmt.rs:157
   + maybe_requeue_silent_held_work     · min==0 → noop                  · sets worker_mgmt_fail_
        |                               · alive_worker_count>0 → noop      outcome (first wins)
        v                               · recovery in-progress/possible   |
   [parked]                              → defer to respawn               v
                                        · else → handle_run_should_fail   operational_loop top
                                                                          reads .is_some() → break
                                                                          (coordinator.rs:283)
                                                                          → [Structured-abort return]
```

| Trigger                          | file:line          | Worker effect                         |
|----------------------------------|--------------------|---------------------------------------|
| scheduler `Assign`               | coordinator.rs:1602| Idle→Assigned + ledger + CRDT InFlight |
| `TaskComplete` / `TaskFailed`    | coordinator.rs:1727| Assigned→Idle + ledger remove          |
| assignment send error            | coordinator.rs:1635| Assigned→Idle + requeue               |
| keepalive-miss → dead            | heartbeat.rs:242   | slot dropped + InFlight→Pending (CRDT) |
| `WorkerMgmtSignal::TasksAdded`   | worker_mgmt.rs:73  | dispatch recheck over free slots       |
| `RunShouldFail`                  | worker_mgmt.rs:157 | latch outcome → loop breaks            |

---

## 3c. NESTED MACHINE — TASK state machine (the CRDT lattice)

`TaskState<I>` (`cluster_state/types.rs:30`) is THE authoritative per-task
machine; every replica converges to it. Convergence is governed by
`TaskJoinKey` (`types.rs:436`): `attempt` (F2 retry generation, the TOP) >
`JoinBand` (NonTerminal < Blocked < Terminal, `types.rs:371`) > within-band
arbiters (version / TerminalRank). The monotone transitions route through
`merge_task_state`; the authoritative rank-DROPS (`TaskRequeued`,
`TaskReinjected`, `TaskRetried`) keep explicit preconditions and bypass the
join. Entry is `Pending` (a brand-new task at the cold attempt generation).

```
   ENTRY: [Pending{version, attempt}]   apply.rs:80  (TaskAdded)
          apply_tasks_spawned no-dep classify → Pending  apply_tasks.rs:359
                 |
        TaskAssigned{secondary,worker,version,attempt}  apply.rs:93
        (join: stale lower-attempt/version assignment LOSES)
                 v
   [InFlight{secondary, worker, version, attempt}]  types.rs:45
                 |
        +--------+----------------------+------------------------+----------------+
   TaskCompleted               TaskFailed{Recoverable/         TaskFailed{           TaskRequeued
   apply.rs:125                 NonRecoverable/ResourceExh.}     Unfulfillable}        (dead-secondary)
        |                       apply.rs:150                     apply.rs:177          apply.rs:332
        v                            v                                v                     v
   [Completed{attempt}]        [Failed{kind,last_error,         [Unfulfillable{       back to
   types.rs:60                  version,attempt}]  types.rs:68    reason,version,       [Pending]
   · cache outputs              · retry-eligible per kind         attempt}]            (rank-DROP,
   · resume_blocked_on →        · TaskRetried{attempt:n+1}        types.rs:98          attempt
     dependents Blocked→Pending   resets → Pending (gen bump)     · ReinjectTask →     preserved)
     (apply_tasks.rs:128)         apply.rs:391                      Pending             apply.rs:361
   · emit TaskCompletedEvent    · attempt_if_failed gates the      (TaskReinjected,
     (to_completed_event,         reset (types.rs:228)             apply.rs:293,
      types.rs:264)                                                 Unfulfillable-only)
```

Cascade / blocking variants:

```
   TasksSpawned classify (apply_tasks.rs:224):
     dep in Failed{NonRecoverable} OR InvalidTask  → [Failed{NonRecoverable}]  (cascade-fail)
     dep in Unfulfillable                          → [Blocked{on=dep_hash}]    types.rs:127
     dep in Pending/InFlight/Blocked/Failed(other) → [Blocked{on=first-unresolved}]
     no deps OR all deps Completed                 → [Pending]  (surfaced to grow pool)

   [Blocked{on, attempt}]  (JoinBand::Blocked — between non-terminal & terminal)
        prereq's TaskCompleted fires resume_blocked_on(prereq_hash)  apply_tasks.rs:128
                 v
        [Pending{attempt preserved}]   (cascade-resume, NOT a new retry attempt)

   [InvalidTask{reason,version,attempt}]  types.rs:155  — the TERMINAL TOP (D-T)
        · originated for #2 missing-dep (ingest.rs:220) and #3b run-wide
          invalidation (ingest.rs:360)
        · NON-reinjectable; locks out even an incoming TaskCompleted (apply.rs:138)
```

Terminal-band ordering (`TerminalRank`, `types.rs:383`):
`{Failed, Unfulfillable} < Completed < InvalidTask`. A `Completed` supersedes a
`Failed` (retry-success), but an `InvalidTask` is the unique TOP and is never
overwritten.

| Trigger (ClusterMutation)        | file:line          | Transition                              |
|----------------------------------|--------------------|-----------------------------------------|
| `TaskAdded`                      | apply.rs:80        | (vacant) → Pending{attempt:0}            |
| `TaskAssigned`                   | apply.rs:93        | Pending → InFlight (join-gated)         |
| `TaskCompleted`                  | apply.rs:125       | → Completed (+ resume_blocked_on)        |
| `TaskFailed{kind}`               | apply.rs:150       | → Failed / Unfulfillable / InvalidTask   |
| `TaskRetried{attempt:n+1}`       | apply.rs:391       | Failed{n} → Pending{n+1} (F2 rank-DROP)  |
| `TaskRequeued`                   | apply.rs:332       | InFlight → Pending (dead-secondary drop) |
| `TaskReinjected`                 | apply.rs:293       | Unfulfillable → Pending (operator)       |
| `TaskBlocked`                    | apply.rs:410       | Pending → Blocked (cascade-pause)        |
| `TasksSpawned`                   | apply_tasks.rs:224 | batch classify (Pending/Blocked/Failed)  |
| resume on prereq complete        | apply_tasks.rs:128 | Blocked → Pending                        |

---

## 4. How the parent machine derives every action from the nested ones

The top-level `[OPERATIONAL]` box does NOT carry per-task / per-worker / per-phase
state of its own; it owns `select!` arms that translate wire events into nested
transitions and reads nested state for its exits:

* `run_complete_check` (operational_loop.rs:61) reads the WORKER roster
  (`active_workers`), the PHASE/pool drain (`pool().is_run_complete`), the TASK
  counters (`completed_tasks + failed_tasks >= total_tasks`), and the CRDT
  `run_complete()` flag — every exit is a function of nested state.
* `setup_pending()` (coordinator.rs:2045) — a CRDT-derived predicate, gates the
  counter / pool-drain exits so the run is not declared done before any task
  exists.

This is the principle working CORRECTLY: the loop is a thin driver over the CRDT
and its derived caches. Section 5 is where it does NOT hold.

---

## 5. CRDT-INDEPENDENCE VIOLATIONS

Each item: WHERE, WHAT it does independent of / on top of the CRDT, and the
CRDT-pure form. These are the bridge to the redesign. Ordered by severity to the
principle.

### V1 — The `SeedSource` cold-vs-resume code FORK (the headline violation)

* **Where:** `coordinator.rs:2597–2605` (the `match seed`), the enum
  `process/run_inputs.rs:49`, and its construction at `run/promotion.rs:62`
  (promotion calls `seed_from_promotion_snapshot` BEFORE `run`) vs. the
  bootstrap path passing `ColdStart{binaries,phase_deps}`.
* **What is non-CRDT:** the *caller* hands in a typed discriminator that says
  "I am cold" or "I am resuming." The init then BRANCHES on it. The doc-comments
  insist "the arm selects ONLY who originates the CRDT first" — and structurally
  the post-match `hydrate` IS unified — but the principle says there should be
  NO discriminator at all: the machine should look at the CRDT, see it empty,
  and seed; see it populated, and continue. Today the *emptiness of the CRDT is
  not the trigger* — an out-of-band enum is. Two construction paths
  (bootstrap `spawn_primary_with` at run/mod.rs:133 vs. `self_build_promoted_primary`
  at run/promotion.rs:35) and two seed inputs exist where one CRDT-driven path
  should.
* **CRDT-pure form:** delete `SeedSource`. `run` takes the bootstrap task batch
  + phase_deps ALWAYS as *candidate* seed. Init becomes: `if
  cluster_state.task_count()==0 && cluster_state.phase_deps().is_empty() {
  originate_cold_seed(candidates) }` then unconditional `hydrate`. A populated
  CRDT (restored by the Node before `run`) makes `originate_cold_seed` a natural
  no-op (the `TaskAdded`/`PhaseDepsSet` applies all NoOp on already-present
  entries — apply.rs:89, apply.rs:269). Cold and resume become the SAME call
  with different CRDT *contents*, exactly the principle.

### V2 — `perform_initial_assignment` builds the roster by PUSH, not by derivation (roster-doubling hazard)

* **Where:** `assignment.rs:106–119` — `self.workers.push(RemoteWorkerState{..})`
  in a round-robin over `self.secondaries` (the connection table), NOT over the
  CRDT's `known_secondaries()` × `SecondaryCapacity`.
* **What is non-CRDT:** there are now TWO writers of `self.workers`:
  (a) `reconstruct_workers_from_cluster_state` (hydrate.rs:465, `self.workers =
  workers`, a pure CRDT derivation), and (b) `perform_initial_assignment`'s
  push. On the cold path the push happens to be benign ONLY because the ledger
  has no `SecondaryCapacity` yet at the `hydrate` call (`hydrate` runs at
  coordinator.rs:2610, BEFORE `wait_for_connections` originates capacity at
  connect.rs:329), so `reconstruct` builds 0 workers and the push fills an empty
  Vec. This is a TIMING coincidence, not a design guarantee: if `hydrate` ever
  ran after capacity landed (it does on the promotion path — hydrate.rs:312 +
  the re-hydrate at coordinator.rs:2610 with an inherited CRDT), the push would
  DOUBLE the roster. The roster is built from `self.secondaries` (a non-CRDT
  connection cache) instead of the replicated capacity records.
* **CRDT-pure form:** `perform_initial_assignment` must NOT build the roster. It
  should assume `self.workers` already holds the CRDT-derived roster (from
  `reconstruct_workers_from_cluster_state`, fed by the `SecondaryCapacity`
  records the welcomes originated) and ONLY run the scheduler's assignment over
  it. Move the roster build entirely into `reconstruct_*`, re-invoked after
  `wait_for_connections` populates capacity — one writer, one source (the CRDT).
  Then the cold path and the failover path build the roster identically.

### V3 — Cold-vs-resume reset asymmetry on `phase_started_emitted`

* **Where:** the reset is INSIDE `originate_cold_seed` (`ingest.rs:87`,
  `self.phase_started_emitted.clear()`) and deliberately NOT in `run_pipeline`
  (coordinator.rs:2561 explains it must survive on the promotion path); the
  promotion path seeds it from the CRDT in `seed_from_promotion_snapshot`
  (coordinator.rs:1145, from `phase_rollups().has_any`).
* **What is non-CRDT:** `phase_started_emitted` is a node-local `HashSet`
  consulted to decide whether to re-fire `on_phase_start` / re-emit "starting
  job phase" (fire_initial_phase_starts, coordinator.rs:3174). Its correct value
  IS derivable from the CRDT (`phase_rollups().has_any`) — and the promotion
  path does exactly that — but the cold path resets it via a side effect buried
  in the seed function, making the "call site IS the discriminator" the load-
  bearing mechanism. This is the V1 fork leaking into phase narration: a runtime
  read of `phase_started_emitted` is fine, but its INITIALIZATION is path-forked.
* **CRDT-pure form:** initialize `phase_started_emitted` ALWAYS from
  `phase_rollups().has_any` inside `hydrate_from_cluster_state` (the one init).
  On a freshly-seeded cold CRDT every task is `Pending` so `has_any` is true for
  seeded phases — which is wrong for the FIRST fire. The fix is to derive
  "already started" from a CRDT fact that distinguishes "task exists" from
  "phase has been dispatched": e.g. `has_any && (in_flight or terminal present)`,
  or replicate a per-phase started marker. Either way the value comes from the
  CRDT on BOTH paths, removing the side-effect reset.

### V4 — `setup_defer` is a config-driven fork around the seed + assignment

* **Where:** `coordinator.rs:2825` — `if self.config.required_setup_on_promote {
  emit_setup_defer_handshake() } else { broadcast_cold_seed();
  perform_initial_assignment() }`.
* **What is non-CRDT:** the decision to seed + assign vs. emit an empty
  handshake is read from `config.required_setup_on_promote`, a static flag, not
  from the CRDT. In defer mode the submitter primary deliberately holds an empty
  CRDT (`all_binaries=[]`) and waits for a promoted secondary to seed it. This is
  a legitimate *deployment* mode, but it is expressed as a top-level branch
  rather than as "the CRDT is empty because this node has nothing to seed —
  proceed with an empty seed." The `setup_pending()` predicate (which IS
  CRDT-derived) already distinguishes the empty-CRDT state; the seed/assign
  branch duplicates that distinction off a config flag.
* **CRDT-pure form:** the seed step should be unconditional `broadcast_cold_seed`
  (which is already a no-op when `pending_cold_seed_broadcast` is empty —
  ingest.rs:255). `perform_initial_assignment` over an empty roster/pool is
  already a no-op. The defer handshake's only non-derivable part is the
  `pre_staged_mode:true` wire flag the secondary's latch needs — that belongs in
  the per-secondary `InitialAssignment` regardless of mode. Collapse the branch:
  always seed (no-op if empty), always assign (no-op if empty), always send the
  handshake triple. The mode then expresses itself purely as "empty CRDT ⇒
  nothing to seed/assign," not as a code fork.

### V5 — `self.*` mutable decision-state caches that shadow the CRDT

These are derived caches the principle tolerates ONLY if rebuilt from the CRDT
on every (re)entry. Most ARE (hydrate rebuilds them). The flags below are
consulted for decisions and are NOT all reconstructed:

* **`self.secondaries` (connection table)** — coordinator.rs:311. Written by
  connect.rs / peer_setup.rs on the bootstrap handshake, and by
  `reconstruct_secondaries_from_cluster_state` (hydrate.rs:551) on failover. BUT
  `perform_initial_assignment` (assignment.rs:56) and
  `wait_for_mesh_ready` (promotion.rs:42) read `self.secondaries.keys()` as the
  authoritative roster, NOT `cluster_state.known_secondaries()`. On the cold
  path these can diverge from the CRDT capacity records (the connection table is
  populated at welcome; capacity is originated in the same call but the two are
  separate stores). CRDT-pure form: the roster reads route through
  `known_secondaries()`; `self.secondaries` keeps ONLY the transport-handle
  metadata the CRDT cannot hold (and even that is reconstructed metadata-only on
  failover — hydrate.rs:610).
* **`backpressured_secondaries`** — coordinator.rs:433. A node-local
  `HashMap<id, Instant>` consulted in dispatch (`is_backpressured`,
  coordinator.rs:1377) and written on a "No idle worker" Recoverable
  (failed.rs:141). NOT in the CRDT and NOT reconstructed on promotion — a
  promoted primary loses every backpressure timer. CRDT-pure form: either accept
  it as legitimately ephemeral (a fresh primary re-learns saturation on the next
  dispatch) and DOCUMENT it as non-failover state, or replicate it. Today it is
  silently node-local decision-state.
* **`silence_warn_stage`** — coordinator.rs:420. Per-secondary WARN-stage
  counter, node-local, not reconstructed. Diagnostic only (re-warns from stage 0
  on a fresh primary), so low severity, but it IS decision-state (gates the
  once-per-stage WARN) that is not CRDT-derived.
* **`fleet_dead_since`** — coordinator.rs:443. The fleet-dead grace clock. The
  *arming quantity* IS CRDT-derived (`alive_remote_secondary_count()`,
  operational_loop.rs:319), but the `since` instant is node-local; a promotion
  resets the grace window. Acceptable (each loop entry measures its own window)
  but worth flagging as non-reconstructed timing state.

For V5 the redesign rule is explicit: every `self.*` field consulted for a
DECISION must either be (a) reconstructed from the CRDT in `hydrate` (like
`workers`, `secondaries`, `failed_tasks`, `all_binaries`, `total_tasks`,
`in_flight`, `next_secondary_id` already are — hydrate.rs:224–347), or
(b) explicitly classified as ephemeral-by-design and documented so. The four
above are in neither category cleanly.

### V6 — The secondary-side discovery latch feeds the primary's pool out-of-band

* **Where:** `ingest_setup_discovery` is the `--source-already-staged` discovery
  feed (referenced coordinator.rs:2043, 2557; the surface is
  `task/mutation.rs:293`'s `carries_discovery_task_added`). The promoted
  secondary's discovery completes, broadcasts `TaskAdded`, and the
  `handle_cluster_mutation` receive path grows the pool from
  `newly_pending`/`carries_discovery_task_added`.
* **What is non-CRDT (subtle):** here the flow is ACTUALLY mostly correct — the
  discovery result lands in the CRDT (`TaskAdded`) and the pool is grown from the
  CRDT apply surface, and `setup_pending()` flips off CRDT-derived. The residual
  violation is that the *primary's* setup-defer mode (V4) and the *secondary's*
  discovery latch are two halves of one out-of-band protocol layered ON TOP of
  the CRDT to bootstrap an initially-empty ledger, rather than the empty CRDT
  itself driving "wait for someone to seed me." The `setup_promote_deadline`
  backstop (operational_loop.rs:691) exists precisely because there is no
  CRDT-intrinsic signal for "the seed is coming." CRDT-pure form: model
  "awaiting seed" as a first-class empty-CRDT state with a replicated "seed
  expected from peer X" marker, so the deadline and the latch both read the CRDT
  instead of `config.required_setup_on_promote` + a node-local secondary latch.

### V7 — `seed_inflight` carries a `#[allow(dead_code)]` / `TODO(R4)` re-home debt

* **Where:** `coordinator.rs:1660` — `#[allow(dead_code)] // TODO(R4): reachable
  via hydrate_from_cluster_state (P4 composition)`.
* **What is non-CRDT:** not a violation of the *principle* per se, but a flagged
  loose end on the CRDT-resume path: the in-flight ledger seed from the inherited
  CRDT is annotated dead-code pending a composition re-home. It is on the
  critical resume chain (hydrate.rs:341 calls it). Flag for the redesign so the
  resume-path ledger seed is not accidentally treated as dead.

---

## 6. Summary

* **Top-level machine (one paragraph):** A primary enters from the `Node`
  lifecycle loop either as the bootstrap submitter (`spawn_primary_with`) or as a
  promotion (`self_build_promoted_primary`, snapshot-seeded before `run`). Both
  converge on `run_pipeline`, which resets per-run scratch, spawns the two
  dispatchers, runs the single CRDT-derived init (`SeedSource` match →
  unconditional `hydrate_from_cluster_state`, the sole builder of pool / rosters
  / `total_tasks`), then walks a serial wait-for-mesh chain (await connections →
  fire initial phase starts → empty-phase cascade → abort gate → auto-stage →
  peer lists → await peer connections → membership → roster rebroadcast →
  seed+assign OR setup-defer handshake → transfer-complete → await mesh-ready),
  activates this node as primary (`activate_local_primary`: uniform
  `PrimaryChanged{epoch+1}` + keepalive), and enters the operational loop. The
  loop is a thin `select!` driver whose exits (`run_complete_check`) are pure
  functions of the WORKER roster, the PHASE/pool drain, the TASK counters, and
  the CRDT `run_complete` flag; it dispatches every wire event into the three
  nested machines. It terminates by finalizing (retry passes → drain → stranded
  accounting → `RunComplete`/`RunAborted` broadcast → settle) into
  `PrimaryRunOutcome::Local`, by a structured abort (panik / worker-mgmt-fail /
  setup-deadline), or by DEMOTE→RELOCATE: the `run_consuming` `select!` races the
  pipeline against `demote_rx`; a demote destructures `self` into an
  `ObserverHandoff` (`PrimaryRunOutcome::Relocated`) which the `Node` swaps in
  place into a standalone observer that resumes off the inherited CRDT.
* **CRDT-independence violations found: 7** (V1 SeedSource fork; V2
  roster-doubling push in `perform_initial_assignment`; V3 `phase_started_emitted`
  cold-vs-resume reset asymmetry; V4 `setup_defer` config-driven seed/assign fork;
  V5 four node-local decision-state caches not reconstructed from the CRDT
  [`secondaries` roster-reads, `backpressured_secondaries`, `silence_warn_stage`,
  `fleet_dead_since`]; V6 setup-defer + secondary discovery latch as an
  out-of-band protocol over an empty CRDT; V7 `seed_inflight` flagged dead-code on
  the resume chain).
```
