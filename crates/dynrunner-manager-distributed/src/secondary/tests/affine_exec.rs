//! #497 P4 — the secondary-local RUN-ONCE-PER-SECONDARY import executor.
//!
//! These tests drive the executor at the EXECUTOR LEVEL (Phase 4) with a stub
//! [`ImportAction`]; the dispatch-router intercept that calls
//! `ensure_affine_import` is Phase 5. They pin the headline correctness
//! invariant (run-once under concurrency) plus the failure paths:
//!
//!   * `eight_concurrent_dependents_trigger_exactly_one_import` (HEADLINE):
//!     8 dependents on the SAME not-yet-done affine hash → exactly ONE import,
//!     all 8 queued (`QueuedAfterLocalDependency` reported), the single import
//!     releases all 8 (`LocalDependencyReleased`).
//!   * `second_assignment_after_done_releases_immediately`: a hash already in
//!     `affine_done` → `AlreadyDone`, no import, no queued report.
//!   * `import_failure_recoverable_fails_each_queued_dependent_rerouteable`:
//!     a `Recoverable` import → each queued dependent `TaskFailed{Recoverable}`,
//!     `affine_done` NOT set.
//!   * `import_failure_does_not_poison_done_set`: after a failed import a fresh
//!     `ensure_affine_import` starts a NEW single run.

#![cfg(test)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

use dynrunner_core::{ErrorType, ResourceMap, TaskInfo, TaskKind, WorkerId};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use tokio::sync::Notify;

use super::super::test_helpers::{FakeWorkerFactory, TestId, make_secondary_recording};
use super::super::{AffineGateOutcome, PendingAffineDependent, SecondaryConfig};
use super::firstbind_orphan::{one_worker_config, task_assignment, test_oom_watcher};
use super::processing::make_binary;
use crate::affine_action::{IMPORT_OUTER_RETRIES, ImportAction, ImportError};

/// The stub the headline + failure tests share. Records every `import` call
/// in a `Mutex<usize>` counter and either:
///   * BLOCKS on a `Notify` until the test releases it (the headline's
///     "import in flight while the other dependents queue"), then returns the
///     scripted result, OR
///   * returns its scripted result IMMEDIATELY (the failure-path tests, which
///     do not need to observe the in-flight window).
///
/// `Mutex` (not `RefCell`) because the trait bound is `Send + Sync` (a real
/// `Arc<dyn ImportAction>` survives the relocation handoff) — the stub honours
/// that bound, exactly like `setup_exec::tests::StubUploader`.
struct StubImporter {
    calls: Mutex<usize>,
    /// `Some` ⇒ each `import` parks on this until `notify_one`d (the headline
    /// blocking window); `None` ⇒ return immediately (the failure paths).
    gate: Option<Arc<Notify>>,
    /// The result returned after the (optional) gate releases.
    result: Mutex<Result<(), ImportError>>,
}

impl StubImporter {
    /// A blocking importer: every `import` parks on the returned `Notify`
    /// until the test releases it, then returns `result`.
    fn blocking(result: Result<(), ImportError>) -> (Arc<Self>, Arc<Notify>) {
        let gate = Arc::new(Notify::new());
        let stub = Arc::new(Self {
            calls: Mutex::new(0),
            gate: Some(gate.clone()),
            result: Mutex::new(result),
        });
        (stub, gate)
    }

    /// A non-blocking importer that returns `result` immediately.
    fn immediate(result: Result<(), ImportError>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(0),
            gate: None,
            result: Mutex::new(result),
        })
    }

    fn call_count(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

/// A scripted importer that returns a SEQUENCE of results (one per call),
/// front-to-back; a call past the end defaults to `Ok` — for exercising the
/// shared core's bounded OUTER retry on `Transient`.
struct ScriptedImporter {
    calls: Mutex<usize>,
    script: Mutex<std::collections::VecDeque<Result<(), ImportError>>>,
}

impl ScriptedImporter {
    fn new(script: Vec<Result<(), ImportError>>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(0),
            script: Mutex::new(script.into()),
        })
    }
    fn call_count(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

#[async_trait::async_trait(?Send)]
impl ImportAction<TestId> for ScriptedImporter {
    async fn import(&self, _task: &TaskInfo<TestId>) -> Result<(), ImportError> {
        *self.calls.lock().unwrap() += 1;
        self.script.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }
}

#[async_trait::async_trait(?Send)]
impl ImportAction<TestId> for StubImporter {
    async fn import(&self, _task: &TaskInfo<TestId>) -> Result<(), ImportError> {
        *self.calls.lock().unwrap() += 1;
        if let Some(gate) = self.gate.as_ref() {
            gate.notified().await;
        }
        self.result.lock().unwrap().clone()
    }
}

/// A SecondaryAffine task `I` seeded into the secondary's replicated ledger
/// under `hash` (the executor resolves its `TaskInfo` from there). Built off
/// `make_binary` and re-kinded — the kind is excluded from the content hash,
/// so this stays a clean fixture.
fn seed_affine_task(
    sec: &mut crate::secondary::test_helpers::SecondaryHarness<
        crate::secondary::test_helpers::RecordingPeer<TestId>,
    >,
    hash: &str,
) -> TaskInfo<TestId> {
    let mut task = make_binary(hash, 0);
    task.kind = TaskKind::SecondaryAffine;
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: hash.to_string(),
        task: task.clone(),
    });
    task
}

/// A `PendingAffineDependent` for work task `B` (`work_hash`) bound to
/// `worker_id` — everything the Phase-5 release needs.
fn make_dependent(work_hash: &str, worker_id: WorkerId) -> PendingAffineDependent<TestId> {
    PendingAffineDependent {
        work_hash: work_hash.to_string(),
        worker_id,
        binary: make_binary(work_hash, 50),
        estimated: ResourceMap::new(),
        predecessor_outputs: BTreeMap::new(),
    }
}

/// Count the `LocalDependencyReleased` frames in the recorded egress log.
fn released_hashes(log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>) -> Vec<String> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::LocalDependencyReleased { task_hash, .. } => Some(task_hash.clone()),
            _ => None,
        })
        .collect()
}

/// Count the `TaskQueuedAfterLocalDependency` frames for `affine_hash`.
fn queued_work_hashes(
    log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
    affine_hash: &str,
) -> Vec<String> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TaskQueuedAfterLocalDependency {
                task_hash,
                affine_hash: ah,
                ..
            } if ah == affine_hash => Some(task_hash.clone()),
            _ => None,
        })
        .collect()
}

/// HEADLINE: 8 dependents on the SAME not-yet-done affine hash run the import
/// EXACTLY ONCE; ALL 8 enter `QueuedAfterLocalDependency`; the single import
/// releases ALL 8.
///
/// The import is driven on a separate `spawn_local` task (off a `StartedRun`)
/// so the test can observe the IN-FLIGHT window: while the blocking stub parks
/// on its gate, the other 7 `ensure_affine_import` calls append to the
/// `affine_running` queue (the run-once latch is the key's presence — no
/// second import). Then the gate releases and the single run drains all 8.
#[tokio::test(flavor = "current_thread")]
async fn eight_concurrent_dependents_trigger_exactly_one_import() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "import-monolith";
            seed_affine_task(&mut sec, affine_hash);
            let (stub, gate) = StubImporter::blocking(Ok(()));
            sec.set_import_action(stub.clone());

            // First dependent: vacant key → StartedRun (this caller drives the
            // single import). It queues + reports BEFORE the import runs (the
            // synchronous gate).
            let first = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            assert_eq!(
                first,
                AffineGateOutcome::StartedRun,
                "the first dependent on a not-yet-done import drives the run"
            );

            // The other 7 dependents queue behind the (not-yet-started) run:
            // the key is present, so each returns QueuedBehindRun and NO second
            // import is ever spawned.
            for i in 1..8 {
                let outcome = sec
                    .ensure_affine_import(
                        affine_hash.to_string(),
                        make_dependent(&format!("B{i}"), i as WorkerId),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    outcome,
                    AffineGateOutcome::QueuedBehindRun,
                    "dependent B{i} must ride the existing run, not start a second"
                );
            }

            // All 8 are queued behind the ONE hash (the run-once latch holds
            // every waiter).
            assert_eq!(
                sec.op_mut().affine_running.get(affine_hash).map(Vec::len),
                Some(8),
                "affine_running[hash] must hold ALL 8 dependents behind one run"
            );

            // All 8 reported QueuedAfterLocalDependency (CRDT-visible queued
            // state — the owner-emphasized "ALL waiting tasks enter
            // QueuedAfterLocalDependency").
            sec.drain_egress().await;
            let mut queued = queued_work_hashes(&log, affine_hash);
            queued.sort();
            assert_eq!(
                queued,
                (0..8).map(|i| format!("B{i}")).collect::<Vec<_>>(),
                "every one of the 8 dependents must report QueuedAfterLocalDependency"
            );

            // Drive the SINGLE import on its own task so the in-flight window
            // is observable. We move the coordinator into the task and take it
            // back via a oneshot — the coordinator is `!Send`/single-thread,
            // and the LocalSet keeps everything on one thread.
            let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
            let import_task = tokio::task::spawn_local(async move {
                let mut factory = FakeWorkerFactory;
                let r = sec
                    .run_affine_import_once(affine_hash.to_string(), &mut factory)
                    .await;
                let _ = done_tx.send(());
                (sec, r)
            });

            // Let the import task reach the (blocked) stub: yield until the
            // call counter ticks to 1. The import is now IN FLIGHT, parked on
            // its gate — the single run.
            for _ in 0..100 {
                if stub.call_count() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                stub.call_count(),
                1,
                "EXACTLY ONE import must run for the 8 dependents on the same hash"
            );
            assert!(
                done_rx.try_recv().is_err(),
                "the import must still be IN FLIGHT (blocked on the gate)"
            );

            // Release the single import; it drains the queue and releases all 8.
            gate.notify_one();
            let (mut sec, run_result) = import_task.await.unwrap();
            run_result.unwrap();

            // EXACTLY ONE import ran (no second one slipped in on the gate
            // release).
            assert_eq!(
                stub.call_count(),
                1,
                "still exactly one import after the run completes"
            );

            // Done set holds the hash; the run-once queue is drained.
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "a successful import marks the hash locally-done"
            );
            assert!(
                !sec.op_mut().affine_running.contains_key(affine_hash),
                "the drained run clears the affine_running key"
            );

            // EXACTLY 8 LocalDependencyReleased — one per queued dependent.
            sec.drain_egress().await;
            let mut released = released_hashes(&log);
            released.sort();
            assert_eq!(
                released,
                (0..8).map(|i| format!("B{i}")).collect::<Vec<_>>(),
                "the single import must release EXACTLY the 8 queued dependents"
            );
        })
        .await;
}

/// A second assignment AFTER the import is locally done releases immediately:
/// `affine_hash ∈ affine_done` → `AlreadyDone`, NO import call, NO
/// `QueuedAfterLocalDependency` report.
#[tokio::test(flavor = "current_thread")]
async fn second_assignment_after_done_releases_immediately() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "already-imported";
            seed_affine_task(&mut sec, affine_hash);
            let stub = StubImporter::immediate(Ok(()));
            sec.set_import_action(stub.clone());

            // First dependent runs the (immediate-success) import inline-ish:
            // StartedRun then drive the run to mark the hash done.
            let first = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            assert_eq!(first, AffineGateOutcome::StartedRun);
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();
            assert!(sec.op_mut().affine_done.contains(affine_hash));
            assert_eq!(stub.call_count(), 1);

            sec.drain_egress().await;
            log.borrow_mut().clear();

            // A LATER dependent on the now-done hash: AlreadyDone, no second
            // import, no queued report.
            let later = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B1", 1))
                .await
                .unwrap();
            assert_eq!(
                later,
                AffineGateOutcome::AlreadyDone,
                "a dependent on a locally-done import releases straight to InFlight"
            );
            assert_eq!(
                stub.call_count(),
                1,
                "a done hash must NEVER trigger a second import"
            );
            sec.drain_egress().await;
            assert!(
                queued_work_hashes(&log, affine_hash).is_empty(),
                "an AlreadyDone dependent must NOT report QueuedAfterLocalDependency"
            );
            assert!(
                released_hashes(&log).is_empty(),
                "the caller (router) releases an AlreadyDone dependent itself — \
                 the executor emits no LocalDependencyReleased"
            );
        })
        .await;
}

/// A `Recoverable` import failure fails EACH queued dependent as
/// `TaskFailed{Recoverable}` (re-routable per #495) and does NOT set
/// `affine_done`.
#[tokio::test(flavor = "current_thread")]
async fn import_failure_recoverable_fails_each_queued_dependent_rerouteable() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "import-fails-recoverable";
            seed_affine_task(&mut sec, affine_hash);
            let stub =
                StubImporter::immediate(Err(ImportError::Recoverable("nfs read pinch".into())));
            sec.set_import_action(stub.clone());

            // Queue three dependents behind the one run.
            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B1", 1))
                .await
                .unwrap();
            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B2", 2))
                .await
                .unwrap();
            assert_eq!(
                sec.op_mut().affine_running.get(affine_hash).map(Vec::len),
                Some(3)
            );

            // The single import FAILS recoverably. Each queued dependent is
            // failed re-routeably; the done set is NOT poisoned.
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();

            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "a FAILED import must NOT mark the hash locally-done"
            );
            assert!(
                !sec.op_mut().affine_running.contains_key(affine_hash),
                "the failed run still clears the affine_running key (no leak)"
            );

            sec.drain_egress().await;
            for i in 0..3 {
                let want = format!("B{i}");
                assert!(
                    log.borrow().iter().any(|m| matches!(
                        m,
                        DistributedMessage::TaskFailed { task_hash, error_type, .. }
                        if *task_hash == want && *error_type == ErrorType::Recoverable
                    )),
                    "queued dependent {want} must be failed TaskFailed{{Recoverable}} \
                     (re-routable per #495); got {:?}",
                    log.borrow()
                );
            }
            assert!(
                released_hashes(&log).is_empty(),
                "a failed import releases NOTHING — every dependent fails instead"
            );
        })
        .await;
}

/// After a failed import, a FRESH `ensure_affine_import` starts a NEW single
/// run — the done set was never poisoned, so the import is retried (on a later
/// assignment / another worker).
#[tokio::test(flavor = "current_thread")]
async fn import_failure_does_not_poison_done_set() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "import-retried";
            seed_affine_task(&mut sec, affine_hash);

            // First run: NonRecoverable failure. (NonRecoverable still does not
            // poison the LOCAL done set — the dependents cascade at the primary,
            // but a NEW assignment on THIS node may still retry its own import,
            // and another secondary certainly does.)
            let failing = StubImporter::immediate(Err(ImportError::NonRecoverable(
                "structurally un-importable".into(),
            )));
            sec.set_import_action(failing.clone());

            let first = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            assert_eq!(first, AffineGateOutcome::StartedRun);
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();
            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "a failed import must not poison the done set"
            );
            assert_eq!(failing.call_count(), 1);

            // A FRESH dependent now: because the hash is neither done nor
            // running, it is treated as the FIRST again → StartedRun, and a
            // SECOND import run is driven (the retry). Swap in a succeeding
            // importer to prove the retry can complete.
            let succeeding = StubImporter::immediate(Ok(()));
            sec.set_import_action(succeeding.clone());

            let retry = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B1", 1))
                .await
                .unwrap();
            assert_eq!(
                retry,
                AffineGateOutcome::StartedRun,
                "after a failed import, a fresh dependent starts a NEW single run"
            );
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();
            assert_eq!(
                succeeding.call_count(),
                1,
                "the retry import runs exactly once on the fresh run"
            );
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "the retried import now completes and marks the hash done"
            );
        })
        .await;
}

/// #509 — a build whose SecondaryAffine gate is NOT YET in the local ledger
/// (its `TaskAdded` was outrun by the build's assignment frame — a transient
/// CRDT sync race) must be RE-ROUTED (`TaskFailed{Recoverable}`), NOT failed
/// `NonRecoverable`. The primary does not re-route a NonRecoverable, so the
/// pre-fix verdict permanently LOST the build to a transient race. Once the
/// gate's `TaskAdded` syncs, a fresh assignment runs the import + releases the
/// build.
///
/// Revert-confirm: with the absent-gate verdict reverted to NonRecoverable,
/// the first-phase `error_type == Recoverable` assertion FAILS (the build is
/// dropped) — the test pins exactly the #509 regression.
#[tokio::test(flavor = "current_thread")]
async fn unsynced_gate_reroutes_recoverable_then_runs_once_synced() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "import-not-yet-synced";

            // The gate's TaskAdded has NOT yet reached this node — the hash is
            // in NEITHER the fat map nor the settled index. A succeeding
            // importer is registered to PROVE the absent verdict is decided by
            // the missing body, not the action (the action is never invoked
            // while the gate is unsynced).
            let importer = StubImporter::immediate(Ok(()));
            sec.set_import_action(importer.clone());

            // The build is assigned + queued behind the (absent) gate, then the
            // single drive runs. Because the gate body resolves over NEITHER
            // half of the logical ledger, the drive emits the transient absent
            // verdict.
            let first = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            assert_eq!(first, AffineGateOutcome::StartedRun);
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();

            // The importer was NEVER called (there was no body to import yet),
            // and the done set is NOT poisoned (a synced retry runs its own
            // import).
            assert_eq!(
                importer.call_count(),
                0,
                "an unsynced gate runs NO import (there is no body yet)"
            );
            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "an unsynced gate must not mark the hash locally-done"
            );

            // The build was RE-ROUTED, not permanently lost: a single
            // TaskFailed{Recoverable} (#509 — NonRecoverable would have dropped
            // it), and NOTHING released.
            sec.drain_egress().await;
            assert!(
                log.borrow().iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, error_type, .. }
                    if task_hash == "B0" && *error_type == ErrorType::Recoverable
                )),
                "an unsynced gate must re-route its queued build \
                 TaskFailed{{Recoverable}} (#509), NOT NonRecoverable; got {:?}",
                log.borrow()
            );
            assert!(
                released_hashes(&log).is_empty(),
                "an unsynced gate releases NOTHING — the build re-routes instead"
            );

            // The gate's TaskAdded NOW syncs (the build's retry re-assignment
            // arrives once the gate is in the ledger). A fresh dependent finds
            // the hash neither done nor running → StartedRun, and this time the
            // body resolves, so the single import RUNS and the build releases.
            seed_affine_task(&mut sec, affine_hash);
            let retry = sec
                .ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            assert_eq!(
                retry,
                AffineGateOutcome::StartedRun,
                "once the gate syncs, the re-routed build starts a fresh single run"
            );
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();
            assert_eq!(
                importer.call_count(),
                1,
                "the synced retry runs the import exactly once"
            );
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "the synced import completes and marks the hash locally-done"
            );
            sec.drain_egress().await;
            assert!(
                released_hashes(&log).contains(&"B0".to_string()),
                "the synced import releases the build (LocalDependencyReleased)"
            );
        })
        .await;
}

/// The shared core's bounded OUTER retry: a `Transient` the provider could not
/// absorb is re-attempted, and an eventual success releases the queued
/// dependent (the import is locally done).
#[tokio::test(flavor = "current_thread")]
async fn transient_import_failures_retry_then_succeed_and_release() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "transient-then-ok";
            seed_affine_task(&mut sec, affine_hash);
            // Two transient faults then success = three attempts (within the
            // IMPORT_OUTER_RETRIES + 1 budget).
            let stub = ScriptedImporter::new(vec![
                Err(ImportError::Transient("blip 1".into())),
                Err(ImportError::Transient("blip 2".into())),
                Ok(()),
            ]);
            sec.set_import_action(stub.clone());

            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();

            assert_eq!(
                stub.call_count(),
                3,
                "two transient retries then success = three import attempts"
            );
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "an eventual success after transient retries marks the hash done"
            );
            sec.drain_egress().await;
            assert_eq!(
                released_hashes(&log),
                vec!["B0".to_string()],
                "the queued dependent releases once the retried import succeeds"
            );
        })
        .await;
}

/// The shared core's bounded OUTER retry EXHAUSTED: transient faults beyond
/// the budget fold into a `Recoverable` work-task failure (the import could
/// not complete on THIS node → re-route per #495), and the done set is NOT
/// set.
#[tokio::test(flavor = "current_thread")]
async fn transient_import_failures_exhaust_to_recoverable_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "transient-forever";
            seed_affine_task(&mut sec, affine_hash);
            let always_transient: Vec<_> = (0..(IMPORT_OUTER_RETRIES as usize + 5))
                .map(|i| Err(ImportError::Transient(format!("blip {i}"))))
                .collect();
            let stub = ScriptedImporter::new(always_transient);
            sec.set_import_action(stub.clone());

            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();

            assert_eq!(
                stub.call_count(),
                IMPORT_OUTER_RETRIES as usize + 1,
                "the bounded outer retry caps at IMPORT_OUTER_RETRIES + 1 attempts"
            );
            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "an exhausted-transient import must NOT mark the hash done"
            );
            sec.drain_egress().await;
            assert!(
                log.borrow().iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, error_type, .. }
                    if *task_hash == "B0" && *error_type == ErrorType::Recoverable
                )),
                "an exhausted transient folds into a Recoverable (re-routable) \
                 work-task failure; got {:?}",
                log.borrow()
            );
        })
        .await;
}

/// No import action registered but a work task gates on an import: the wiring
/// error is surfaced LOUDLY as a `NonRecoverable` per-dependent failure (NOT a
/// silent success), and the done set is NOT set.
#[tokio::test(flavor = "current_thread")]
async fn unregistered_import_action_is_a_nonrecoverable_wiring_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let affine_hash = "no-importer";
            seed_affine_task(&mut sec, affine_hash);
            // Deliberately do NOT register an import action.

            sec.ensure_affine_import(affine_hash.to_string(), make_dependent("B0", 0))
                .await
                .unwrap();
            sec.run_affine_import_once(affine_hash.to_string(), &mut FakeWorkerFactory)
                .await
                .unwrap();

            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "a wiring-error import must NOT mark the hash done"
            );
            sec.drain_egress().await;
            assert!(
                log.borrow().iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, error_type, .. }
                    if *task_hash == "B0" && *error_type == ErrorType::NonRecoverable
                )),
                "an unregistered importer fails the queued dependent \
                 NonRecoverably (loud wiring error); got {:?}",
                log.borrow()
            );
        })
        .await;
}

// ─────────────────── Phase 5: the dispatch-router intercept ───────────────────
//
// These tests drive the WHOLE secondary dispatch path (`handle_inbound` →
// `dispatch_message` → the TaskAssignment arm) for a work task `B`, exercising
// the `unmet_local_affine_dep` gate, the off-loop import drive, and the
// completion → release dispatch — the seams Phase 5 wires.

/// An `n`-worker variant of [`one_worker_config`] (the multi-slot fixture the
/// "all 8 workers gate on one import" test needs).
fn n_worker_config(secondary_id: &str, n: u32) -> SecondaryConfig {
    SecondaryConfig {
        num_workers: n,
        ..one_worker_config(secondary_id)
    }
}

/// Seed a SecondaryAffine gate `I` into the secondary's ledger and drive it to
/// `AffineReady` via the public apply path (`TaskAdded` → Pending → the
/// Phase-2 `AffineReady` mutation), then assert it is `AffineReady`. The gate's
/// `task_id` is what a dependent work task points its `task_depends_on` edge
/// at. Keyed by `gate_hash` in the ledger (the kind is excluded from the
/// content hash, so a literal hash is a clean fixture).
fn seed_affine_ready_gate(
    sec: &mut crate::secondary::test_helpers::SecondaryHarness<
        crate::secondary::test_helpers::RecordingPeer<TestId>,
    >,
    gate_hash: &str,
    gate_task_id: &str,
) {
    let mut gate = make_binary(gate_task_id, 0);
    gate.kind = TaskKind::SecondaryAffine;
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: gate_hash.to_string(),
        task: gate,
    });
    sec.cluster_state.apply(ClusterMutation::AffineReady {
        hash: gate_hash.to_string(),
    });
    assert!(
        matches!(
            sec.cluster_state.task_state(gate_hash),
            Some(crate::cluster_state::TaskState::AffineReady { .. })
        ),
        "fixture precondition: the gate is AffineReady"
    );
}

/// A work task `B` (`work_id`) with a single dependency on the gate task
/// `gate_task_id` (same `default` phase `make_binary` uses) — the dependent
/// the router gates.
fn make_work_with_affine_dep(work_id: &str, gate_task_id: &str) -> TaskInfo<TestId> {
    let mut b = make_binary(work_id, 50);
    b.task_depends_on.push(dynrunner_core::TaskDep {
        task_id: gate_task_id.into(),
        phase_id: dynrunner_core::PhaseId::from("default"),
        inherit_outputs: false,
    });
    b
}

/// Pump every pending pool event so each freshly-assigned worker's deferred
/// first-bind binds into `active_tasks`. A fresh `FakeWorkerFactory` slot takes
/// its FIRST task of a type via `EnsureWorkerOutcome::RespawnInProgress` — the
/// resolved binary is stashed in `pending_first_bind` and the `WorkerEvent::Ready`
/// arm binds it once the new subprocess reports Ready. This drives that Ready
/// fan-in for every worker that has a stashed binary (bounded by `max` polls).
async fn pump_ready_bindings<P: dynrunner_protocol_primary_secondary::PeerTransport<TestId>>(
    sec: &mut crate::secondary::test_helpers::SecondaryHarness<P>,
    oom: &dynrunner_manager_local::oom::OomWatcher,
    max: usize,
) {
    for _ in 0..max {
        if sec.op_mut().pending_first_bind.is_empty() {
            break;
        }
        let Some(event) = sec.op_mut().pool.recv_event().await else {
            break;
        };
        sec.handle_worker_event(event, oom).await.unwrap();
    }
}

/// A `B` whose SecondaryAffine dep is `AffineReady` but NOT locally imported →
/// `B` does NOT reach a worker; it is reported `QueuedAfterLocalDependency` and
/// the single per-secondary import is driven.
#[tokio::test(flavor = "current_thread")]
async fn assignment_with_unmet_affine_dep_queues_b_not_worker() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut sec, gate_hash, "import");
            // A BLOCKING importer so the driven import stays in flight (we
            // assert it was started, not that it completed).
            let (stub, _gate) = StubImporter::blocking(Ok(()));
            sec.set_import_action(stub.clone());

            let b = make_work_with_affine_dep("B", "import");
            let b_hash = "work-b-hash";
            sec.handle_inbound(
                task_assignment("setup", "sec-2", 0, &b, b_hash),
                &mut FakeWorkerFactory,
            )
            .await;
            sec.drain_egress().await;

            // B did NOT reach a worker: nothing in active_tasks, the slot is
            // still idle.
            assert!(
                !sec.op_mut().active_tasks.contains_key(b_hash),
                "a work task gated on an unmet affine import must NOT be \
                 assigned to a worker"
            );
            assert!(
                sec.op_mut().pool.workers[0].is_idle_state(),
                "the worker must stay idle behind the gate"
            );
            // B was reported QueuedAfterLocalDependency.
            assert_eq!(
                queued_work_hashes(&log, gate_hash),
                vec![b_hash.to_string()],
                "B must be reported QueuedAfterLocalDependency"
            );
            // The single import was driven (queued + in flight on this node).
            assert_eq!(
                sec.op_mut().affine_running.get(gate_hash).map(Vec::len),
                Some(1),
                "the single per-secondary import is driven (B queued behind it)"
            );
            // Let the spawned import task reach the (blocked) stub.
            for _ in 0..100 {
                if stub.call_count() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                stub.call_count(),
                1,
                "the import was driven OFF the loop (the spawned task reached \
                 the blocking stub)"
            );
        })
        .await;
}

/// A `B` whose SecondaryAffine dep is ALREADY in this node's `affine_done` →
/// the gate is a no-op: `B` is assigned straight to its worker, no
/// `QueuedAfterLocalDependency` report, no import.
#[tokio::test(flavor = "current_thread")]
async fn assignment_with_locally_done_affine_dep_goes_straight_to_worker() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut sec, gate_hash, "import");
            // The import already ran on this node.
            sec.op_mut().affine_done.insert(gate_hash.to_string());
            // A stub that would FAIL the test if called — the locally-done
            // path must never run an import.
            let stub = StubImporter::immediate(Err(ImportError::NonRecoverable(
                "must not be called".into(),
            )));
            sec.set_import_action(stub.clone());

            let b = make_work_with_affine_dep("B", "import");
            let b_hash = "work-b-hash";
            sec.handle_inbound(
                task_assignment("setup", "sec-2", 0, &b, b_hash),
                &mut FakeWorkerFactory,
            )
            .await;
            sec.drain_egress().await;

            // B was assigned straight to its worker (first-bind binds on Ready).
            let oom = test_oom_watcher();
            pump_ready_bindings(&mut sec, &oom, 4).await;
            assert_eq!(
                sec.op_mut().active_tasks.get(b_hash),
                Some(&0u32),
                "a locally-done affine dep must let B assign straight to its worker"
            );
            // No queued report, no import.
            assert!(
                queued_work_hashes(&log, gate_hash).is_empty(),
                "a locally-done affine dep must NOT report QueuedAfterLocalDependency"
            );
            assert_eq!(
                stub.call_count(),
                0,
                "a locally-done affine dep must NOT run a second import"
            );
            assert!(
                !sec.op_mut().affine_running.contains_key(gate_hash),
                "no import is driven for a locally-done affine dep"
            );
        })
        .await;
}

/// A plain `Work` task with NO SecondaryAffine dependency takes the EXISTING
/// dispatch path unchanged (the regression guard): assigned to its worker, no
/// queued report, no import driven.
#[tokio::test(flavor = "current_thread")]
async fn assignment_with_no_affine_dep_unchanged() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            // No import action registered, no affine gate seeded — a plain
            // work task must never touch the affine path.
            let b = make_binary("B", 50);
            let b_hash = "work-b-hash";
            sec.handle_inbound(
                task_assignment("setup", "sec-2", 0, &b, b_hash),
                &mut FakeWorkerFactory,
            )
            .await;
            sec.drain_egress().await;

            // Assigned to its worker via the unchanged path (first-bind binds
            // on Ready).
            let oom = test_oom_watcher();
            pump_ready_bindings(&mut sec, &oom, 4).await;
            assert_eq!(
                sec.op_mut().active_tasks.get(b_hash),
                Some(&0u32),
                "a plain work task takes the existing dispatch path (assigned)"
            );
            // Nothing affine happened.
            assert!(
                sec.op_mut().affine_running.is_empty(),
                "no import is driven for a non-affine task"
            );
            assert!(
                log.borrow().iter().all(|m| !matches!(
                    m,
                    DistributedMessage::TaskQueuedAfterLocalDependency { .. }
                )),
                "a non-affine task must NEVER report QueuedAfterLocalDependency"
            );
        })
        .await;
}

/// 8 assignments for 8 DIFFERENT workers, all depending on the SAME gate `I`:
/// exactly ONE import is driven, all 8 are queued
/// (`QueuedAfterLocalDependency`), and on the import's completion all 8 are
/// released (`LocalDependencyReleased`) AND dispatched onto their workers.
#[tokio::test(flavor = "current_thread")]
async fn all_workers_on_node_gate_on_one_import() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(n_worker_config("sec-2", 8), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut sec, gate_hash, "import");
            let (stub, gate) = StubImporter::blocking(Ok(()));
            sec.set_import_action(stub.clone());

            // 8 assignments, one per worker, each a distinct B depending on I.
            for w in 0..8u32 {
                let b = make_work_with_affine_dep(&format!("B{w}"), "import");
                let b_hash = format!("work-b{w}-hash");
                sec.handle_inbound(
                    task_assignment("setup", "sec-2", w, &b, &b_hash),
                    &mut FakeWorkerFactory,
                )
                .await;
            }
            sec.drain_egress().await;

            // EXACTLY ONE import driven; all 8 queued behind it.
            assert_eq!(
                sec.op_mut().affine_running.get(gate_hash).map(Vec::len),
                Some(8),
                "all 8 dependents queue behind the ONE import"
            );
            let mut queued = queued_work_hashes(&log, gate_hash);
            queued.sort();
            assert_eq!(
                queued,
                (0..8).map(|w| format!("work-b{w}-hash")).collect::<Vec<_>>(),
                "every one of the 8 dependents reports QueuedAfterLocalDependency"
            );
            // None of the 8 reached a worker yet (all gated).
            for w in 0..8u32 {
                assert!(
                    !sec.op_mut()
                        .active_tasks
                        .contains_key(&format!("work-b{w}-hash")),
                    "B{w} must be gated (not yet on a worker)"
                );
            }
            // Let the single spawned import reach the blocking stub, then
            // assert EXACTLY one import ran for the 8 dependents.
            for _ in 0..100 {
                if stub.call_count() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                stub.call_count(),
                1,
                "EXACTLY ONE import runs for the 8 dependents on the same hash"
            );

            // Release the import; receive the off-loop completion and run the
            // on-loop release exactly as the `process_tasks` select! arm does.
            gate.notify_one();
            let completion = {
                let rx = sec.affine_import_rx.as_mut().expect("rx present");
                let mut got = None;
                for _ in 0..100 {
                    if let Ok(c) = rx.try_recv() {
                        got = Some(c);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                got.expect("the spawned import must post AffineImportComplete")
            };
            sec.complete_affine_import(
                completion.affine_hash,
                completion.outcome,
                &mut FakeWorkerFactory,
            )
            .await
            .unwrap();
            sec.drain_egress().await;

            // The hash is locally done; the run-once queue is drained.
            assert!(
                sec.op_mut().affine_done.contains(gate_hash),
                "the successful import marks the hash locally-done"
            );
            assert!(
                !sec.op_mut().affine_running.contains_key(gate_hash),
                "the drained run clears the affine_running key"
            );
            // All 8 released.
            let mut released = released_hashes(&log);
            released.sort();
            assert_eq!(
                released,
                (0..8).map(|w| format!("work-b{w}-hash")).collect::<Vec<_>>(),
                "the single import releases EXACTLY the 8 queued dependents"
            );
            // All 8 dispatched onto their workers (first-bind binds on Ready).
            let oom = test_oom_watcher();
            pump_ready_bindings(&mut sec, &oom, 64).await;
            for w in 0..8u32 {
                assert_eq!(
                    sec.op_mut().active_tasks.get(&format!("work-b{w}-hash")),
                    Some(&w),
                    "released B{w} must be dispatched onto its worker {w}"
                );
            }
        })
        .await;
}

/// HEADLINE (#497 P5 spill hole): a gate that RESOLVED to `AffineReady` and
/// then SPILLED to disk stays a detected unmet local affine dep. The live
/// `task_state(hash)` returns `None` after the spill (the fat body is evicted),
/// so the prior live-only check went BLIND and a build dispatched onto a
/// not-yet-imported node AFTER the spill would SKIP the import and fail. The
/// settled index keeps the `SettledClass::AffineReady` fact, so
/// `unmet_local_affine_dep` STILL returns `Some(hash)` and the import is gated.
#[tokio::test(flavor = "current_thread")]
async fn affine_gate_detected_after_spill() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut sec, gate_hash, "import");

            // SPILL the gate: the AffineReady join fixed-point is settle-
            // eligible, so a full spill sweep evicts its fat body and leaves
            // only the slim settled index entry. `task_state` now goes blind.
            let dir = tempfile::tempdir().expect("tempdir");
            let evicted = sec.force_settled_spill_for_test(&dir.path().join("spill.cbor"));
            assert_eq!(evicted, 1, "the AffineReady gate spills + evicts its fat body");
            assert!(
                sec.cluster_state.task_state(gate_hash).is_none(),
                "post-spill the fat body is gone — the live-only check would go blind"
            );
            assert!(
                sec.cluster_state.settled_contains(gate_hash),
                "the settled index retains the gate (the durable detection source)"
            );

            // The detector STILL sees the gate via the settled index.
            let b = make_work_with_affine_dep("B", "import");
            assert_eq!(
                sec.unmet_local_affine_dep(&b).as_deref(),
                Some(gate_hash),
                "a spilled AffineReady gate is STILL detected as an unmet local \
                 affine dep (the #497 P5 spill hole is closed)"
            );

            // End-to-end: the build is GATED (queued behind the import), NOT
            // skipped to a worker — the import drives exactly as the non-spill
            // path.
            let (stub, _gate) = StubImporter::blocking(Ok(()));
            sec.set_import_action(stub.clone());
            let b_hash = "work-b-hash";
            sec.handle_inbound(
                task_assignment("setup", "sec-2", 0, &b, b_hash),
                &mut FakeWorkerFactory,
            )
            .await;
            sec.drain_egress().await;

            assert!(
                !sec.op_mut().active_tasks.contains_key(b_hash),
                "a build gated on a SPILLED affine import must NOT skip to a worker"
            );
            assert_eq!(
                queued_work_hashes(&log, gate_hash),
                vec![b_hash.to_string()],
                "the build is reported QueuedAfterLocalDependency behind the spilled gate"
            );
            assert_eq!(
                sec.op_mut().affine_running.get(gate_hash).map(Vec::len),
                Some(1),
                "the single per-secondary import is driven behind the spilled gate"
            );
            // The drive resolves the SPILLED gate's body from the settled
            // record (`affine_gate_task` — fat OR settled) and actually RUNS
            // the import. Before the #509 fix the fat-only `task_state` read
            // went blind here and the spilled-gate import silently fell into
            // the absent verdict (the stub was never called).
            for _ in 0..100 {
                if stub.call_count() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                stub.call_count(),
                1,
                "the spilled-gate import is resolved from the settled record \
                 and driven off the loop (the fat-only read no longer goes blind)"
            );
        })
        .await;
}

/// A LATE JOINER learns the gate via the snapshot STREAM, not a live
/// `AffineReady` mutation: a node that joins after the gate already spilled on
/// the responder receives it through the settled-restore path (the responder
/// reads the settled record back into a fat `AffineReady` in the stream
/// package). The joiner's `unmet_local_affine_dep` detects it — and continues
/// to detect it after the joiner ITSELF spills the gate (exercising the
/// settled arm on the joining node).
#[tokio::test(flavor = "current_thread")]
async fn affine_gate_detected_after_settled_restore() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // DONOR: seed the gate to AffineReady, then spill it so it lives in
            // the donor's settled index (the responder serves it from disk).
            let (mut donor, _dlog) = make_secondary_recording(one_worker_config("donor"), 1);
            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut donor, gate_hash, "import");
            let donor_dir = tempfile::tempdir().expect("donor tempdir");
            let evicted = donor.force_settled_spill_for_test(&donor_dir.path().join("spill.cbor"));
            assert_eq!(evicted, 1, "the donor spills its AffineReady gate");

            // Materialise the snapshot-stream frames NOW — the settled gate is
            // read from the donor's spill file at frame-build time and baked
            // into the encoded payloads, BEFORE a second coordinator's driver
            // truncates the role-shared spill file at its own construction.
            let frames =
                crate::snapshot_stream::stream_frames_for_test(&donor.cluster_state, "donor", "s1");

            // JOINER: bootstrap from the donor via the production snapshot-
            // stream package sequence (the settled gate rides as a fat
            // `AffineReady` decoded from the donor's spill file).
            let (mut joiner, jlog) = make_secondary_recording(one_worker_config("sec-2"), 1);
            joiner.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = joiner.initialize_workers(&mut factory).await.unwrap();
            joiner.enter_operational_for_test();
            *joiner.pool_mut() = pool;
            for frame in frames {
                if let DistributedMessage::SnapshotStreamPackage { payload, .. } = frame {
                    let snap =
                        crate::cluster_state::decode_stream_payload::<TestId>(&payload)
                            .expect("decode package");
                    joiner.cluster_state.restore(snap);
                }
            }
            assert!(
                joiner.cluster_state.contains_task(gate_hash),
                "the joiner learned the gate via the settled-restore stream path"
            );

            // The joiner detects the gate it learned ONLY from a settled
            // restore (never a live AffineReady mutation on this node).
            let b = make_work_with_affine_dep("B", "import");
            assert_eq!(
                joiner.unmet_local_affine_dep(&b).as_deref(),
                Some(gate_hash),
                "a gate learned via settled-restore is detected as an unmet \
                 local affine dep on the late joiner"
            );

            // It stays detected after the JOINER itself spills the gate (the
            // settled arm on the joining node).
            let joiner_dir = tempfile::tempdir().expect("joiner tempdir");
            let n = joiner.force_settled_spill_for_test(&joiner_dir.path().join("spill.cbor"));
            assert_eq!(n, 1, "the joiner re-spills the restored gate");
            assert!(
                joiner.cluster_state.task_state(gate_hash).is_none(),
                "post re-spill the joiner's fat body is gone"
            );
            assert_eq!(
                joiner.unmet_local_affine_dep(&b).as_deref(),
                Some(gate_hash),
                "the gate stays detected after the joiner re-spills it"
            );

            // End-to-end on the joiner: the build is gated, not skipped.
            let (stub, _gate) = StubImporter::blocking(Ok(()));
            joiner.set_import_action(stub.clone());
            let b_hash = "work-b-hash";
            joiner
                .handle_inbound(
                    task_assignment("setup", "sec-2", 0, &b, b_hash),
                    &mut FakeWorkerFactory,
                )
                .await;
            joiner.drain_egress().await;
            assert!(
                !joiner.op_mut().active_tasks.contains_key(b_hash),
                "a build gated on a settled-restored gate must NOT skip to a worker"
            );
            assert_eq!(
                queued_work_hashes(&jlog, gate_hash),
                vec![b_hash.to_string()],
                "the build is queued behind the settled-restored gate's import"
            );
        })
        .await;
}

/// HEADLINE (#516 drift-class invariant): the unified resolver
/// [`crate::cluster_state::ClusterState::resolve_affine_ready_gate`] answers
/// BOTH "is this a ready gate?" (detection) AND "what is its body?"
/// (import-drive) from ONE fat∪settled read. For a SPILLED `AffineReady` gate —
/// where `task_state` (fat) returns `None` — a SINGLE resolve call must return
/// `Some` with `is_ready_gate == true` AND a body. This is the exact property
/// the OLD two-read split (`is_affine_ready_gate` over fat∪settled, but
/// `affine_gate_task` once fat-only — fe840943/#515) could violate: detection
/// said "gate" while the body read went blind → phantom "absent gate". One read
/// makes "detected ⟹ body-resolvable" tautological. The truly-absent hash (the
/// #509 sync race) resolves to `None`, which is the precondition the drive maps
/// to the Recoverable re-route — that classification is asserted end-to-end by
/// `unsynced_gate_reroutes_recoverable_then_runs_once_synced`.
#[tokio::test(flavor = "current_thread")]
async fn unified_resolver_one_read_recognizes_and_resolves_spilled_gate() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let gate_hash = "import-monolith";
            seed_affine_ready_gate(&mut sec, gate_hash, "import");

            // SPILL the gate so its fat body is evicted and only the slim
            // settled `AffineReady` index entry remains — the case the old
            // fat-only body read went blind on.
            let dir = tempfile::tempdir().expect("tempdir");
            let evicted = sec.force_settled_spill_for_test(&dir.path().join("spill.cbor"));
            assert_eq!(evicted, 1, "the AffineReady gate spills + evicts its fat body");
            assert!(
                sec.cluster_state.task_state(gate_hash).is_none(),
                "post-spill the fat body is gone — the fat-only body read would go blind"
            );

            // THE INVARIANT: ONE resolve call yields BOTH the recognition fact
            // AND the body over the settled half — they cannot disagree because
            // there is one read.
            let resolved = sec
                .cluster_state
                .resolve_affine_ready_gate(gate_hash)
                .expect("the spilled gate resolves over fat∪settled in one read");
            assert!(
                resolved.is_ready_gate,
                "the SAME single read recognizes the spilled entry as a ready gate"
            );
            assert_eq!(
                resolved.task.task_id, "import",
                "the SAME single read returns the spilled gate's body (read back from disk)"
            );

            // The thin projections agree with the unified read by construction
            // (they ARE the resolver): detection sees the gate, body resolves.
            assert!(
                sec.cluster_state.is_affine_ready_gate(gate_hash),
                "detection projection: the spilled gate is a ready gate"
            );
            assert_eq!(
                sec.cluster_state
                    .affine_gate_task(gate_hash)
                    .expect("body projection resolves the spilled gate")
                    .task_id,
                "import",
                "body projection: the spilled gate's body resolves to the same task"
            );

            // A truly-absent hash (the #509 sync race) resolves to `None` — the
            // precondition the drive maps to the Recoverable re-route.
            assert!(
                sec.cluster_state
                    .resolve_affine_ready_gate("never-synced")
                    .is_none(),
                "a hash in NEITHER half resolves to None (→ Recoverable re-route, #509)"
            );
            assert!(
                !sec.cluster_state.is_affine_ready_gate("never-synced"),
                "an absent hash is not a ready gate"
            );
            assert!(
                sec.cluster_state.affine_gate_task("never-synced").is_none(),
                "an absent hash has no body"
            );
        })
        .await;
}
