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

use super::super::test_helpers::{TestId, make_secondary_recording};
use super::super::{AffineGateOutcome, PendingAffineDependent};
use super::firstbind_orphan::one_worker_config;
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
                let r = sec.run_affine_import_once(affine_hash.to_string()).await;
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
            sec.run_affine_import_once(affine_hash.to_string())
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
