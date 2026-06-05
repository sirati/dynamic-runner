//! `PyObserverLateJoiner::run` — reads the peer-info dir, dials into
//! the mesh, restores the cluster snapshot, and drives the standalone
//! observer. Also exposes the `completed` getter.

use std::time::Duration;

use pyo3::prelude::*;

use dynrunner_manager_distributed::cluster_state::{ClusterState, ClusterStateSnapshot};
use dynrunner_manager_distributed::observer::{
    ObserverConfig, ObserverTerminal, build_cold_join_observer,
};
use dynrunner_manager_distributed::primary::RunError;
use dynrunner_protocol_primary_secondary::{DEFAULT_JOIN_TIMEOUT, PeerTransport};
use dynrunner_slurm::read_peer_info_dir_v2;

use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;

use super::PyObserverLateJoiner;
use super::helpers::{map_read_dir_error, records_to_seed};

/// Fleet-dead strand grace for a cold-join observer. There is no
/// operator kwarg for this backstop; the single source of truth mirrors
/// the primary's `PrimaryConfig` default (30s). It is the window after
/// the LAST mesh peer leaves before a stranded observer exits, and
/// doubles as the loop's re-check cadence.
const OBSERVER_FLEET_DEAD_TIMEOUT: Duration = Duration::from_secs(30);

#[pymethods]
impl PyObserverLateJoiner {
    /// Read the peer-info dir, dial into the mesh, restore the
    /// cluster snapshot, and drive the standalone observer loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        // -- pre-detach: read peer-info dir (synchronous file I/O,
        // small enough that we don't bother offloading; surfacing
        // ReadDirError as a Python exception before we even spin up
        // tokio keeps the error path simple).
        let records = read_peer_info_dir_v2(&self.peer_info_dir).map_err(map_read_dir_error)?;
        let seed = records_to_seed(&records);
        if seed.is_empty() {
            // `read_peer_info_dir_v2` already errors on the empty /
            // all-v1 case; this guards against the (currently
            // unreachable) future shape where the filter drops every
            // record post-conversion. Fail loud rather than spin in
            // `join_running_cluster`'s connect window.
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "observer late-joiner: peer-info dir produced zero usable seed entries \
                 after v2 filtering — refusing to enter join_running_cluster with an \
                 empty seed (would hang on the connect-budget)",
            ));
        }

        let observer_id = self.observer_id.clone();
        // Strand-backstop + setup-deadline thresholds for the standalone
        // observer's `ObserverConfig`. `peer_timeout` is the primary-silence
        // backstop; `setup_promote_deadline` is plumbed for config-shape
        // completeness but is inert here — `required_setup_on_promote=false`
        // (a late-joiner joins an already-running cluster, it is never the
        // setup-promoted node), so the deadline arm never arms.
        let peer_timeout = self.distributed_config.peer_timeout();
        let setup_promote_deadline = self.distributed_config.setup_promote_deadline();
        // Move the holdings set out of `self` so it can be handed to the
        // cold-join factory's announcer attach. After this point
        // `self.holdings` is empty; the observer is single-shot per
        // `__init__` so a second `run()` would never make sense anyway.
        let holdings = std::mem::take(&mut self.holdings);
        let panik_watcher_paths = self.panik_watcher_paths.clone();
        let panik_watcher_poll_interval =
            Duration::from_secs_f64(self.panik_watcher_poll_interval_secs);

        // Terminal-outcome shapes for the observer late-joiner's run.
        // `Done` returns the observed-completion count; `Panik`
        // signals the outer scope to call `std::process::exit(137)`
        // after the GIL is re-acquired; `Aborted` signals `exit(1)`.
        // Same shape the regular secondary uses — keeps the two
        // pyclasses' panik response structurally aligned.
        enum ObserverRunOutcome {
            Done(u32),
            Panik(std::path::PathBuf),
            Aborted(String),
        }
        let result: Result<ObserverRunOutcome, PyErr> =
            py.detach(|| -> Result<ObserverRunOutcome, PyErr> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "failed to create tokio runtime: {e}"
                        ))
                    })?;
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    // 1. Stand up the real peer transport with our chosen
                    //    observer-id through the backend-opaque factory.
                    //    The CN baked into the cert MUST match
                    //    `observer_id` because every dialing peer
                    //    validates the SAN against the logical id. The
                    //    standalone observer holds this mesh transport
                    //    directly (no cert bundle — it ships no PeerCertInfo).
                    let mut peer_network = transport_factory::observer_mesh::<RunnerIdentifier>(
                        &observer_id,
                    )
                    .await
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "observer late-joiner: {e}"
                        ))
                    })?;

                    // 2. Bootstrap rendezvous: hand the seed list to the
                    //    trait default impl, which sequences the dial +
                    //    snapshot request + reply wait. Errors get
                    //    typed strings; we PyErr them with the snapshot
                    //    JSON context so the operator can correlate.
                    // `is_observer = true`: this late-joiner is a strict
                    // observer. The flag rides the snapshot RPC so the
                    // responder records the joiner's ACTUAL role in the
                    // replicated `PeerJoined`. `can_be_primary = false`:
                    // an observer can never host the primary role (no
                    // workers, no dispatch authority).
                    // `join_running_cluster` fans the snapshot request to
                    // every seed and returns ALL responders' payloads
                    // (multi-responder bootstrap). The first reachable seed
                    // may be a secondary holding an incomplete roster, so a
                    // single reply could bootstrap from a partial snapshot;
                    // collecting every responder's snapshot and `restore()`-
                    // ing each (idempotent lattice) heals — the union is
                    // complete iff ANY responder (the primary above all)
                    // was complete.
                    let snapshot_jsons = peer_network
                        .join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT, true, false)
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "observer late-joiner: join_running_cluster failed: {e}"
                            ))
                        })?;

                    // 3. Decode each snapshot. The wire frame is a String
                    //    (the protocol crate keeps `I` erased there); we
                    //    materialise each back into the typed snapshot here.
                    //    A bootstrap-decode failure is FATAL: the observer
                    //    requested these snapshots precisely to populate its
                    //    CRDT, so a malformed bootstrap reply must not be
                    //    swallowed (continuing on an empty CRDT would report
                    //    a lie). The cold-join factory `restore()`s each — the
                    //    lattice unions them.
                    let snaps: Vec<ClusterStateSnapshot<RunnerIdentifier>> = snapshot_jsons
                        .iter()
                        .map(|snapshot_json| {
                            serde_json::from_str(snapshot_json).map_err(|e| {
                                pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "observer late-joiner: failed to decode \
                                 ClusterStateSnapshot from join_running_cluster reply: {e}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // 4. Build the standalone observer's config. No
                    //    scheduler / worker / dispatch fields — an observer
                    //    has none of those concerns; the config carries only
                    //    the node identity, the strand-backstop thresholds,
                    //    the (inert here) setup-promote deadline, and the
                    //    panik trigger inputs.
                    let config = ObserverConfig {
                        node_id: observer_id.clone(),
                        fleet_dead_timeout: OBSERVER_FLEET_DEAD_TIMEOUT,
                        peer_timeout,
                        setup_promote_deadline,
                        // A late-joiner joins an already-running cluster; it
                        // is never the setup-promoted node, so the deadline
                        // arm is structurally inert.
                        required_setup_on_promote: false,
                        panik_watcher_paths,
                        panik_watcher_poll_interval,
                    };

                    // 5. Cold-join: build the standalone ObserverCoordinator
                    //    over the live mesh transport + the bootstrap
                    //    snapshot(s). The factory installs the task-completed
                    //    sender, attaches the resource-holdings announcer's
                    //    role-change hook, then restores each snapshot (so the
                    //    restore's role-change fire pushes the initial holdings
                    //    announce into the registered channel), and spawns the
                    //    panik watcher. The whole observation runtime —
                    //    reporter, failure policies, announcer task, panik arm,
                    //    teardown — lives inside `ObserverCoordinator::run`.
                    let mut observer = build_cold_join_observer(
                        peer_network,
                        ClusterState::<RunnerIdentifier>::new(),
                        config,
                        snaps,
                        holdings,
                    );

                    // 6. Drive the single observer run loop. It returns the
                    //    run terminal (Done / Aborted / Panik); the strand
                    //    backstops (fleet-dead, primary-silence) surface as
                    //    `Err(RunError)`, which we route to a non-zero exit.
                    match observer.run().await {
                        Ok(ObserverTerminal::Done) => {
                            Ok(ObserverRunOutcome::Done(observer.completed_count() as u32))
                        }
                        Ok(ObserverTerminal::Aborted { reason }) => {
                            // The primary broadcast `RunAborted` (#3a
                            // pre-phase duplicate). Propagate to the PyO3
                            // boundary for exit(1) — an observer exits
                            // non-zero on a cluster-wide abort.
                            tracing::error!(
                                reason = %reason,
                                "observer run aborted by primary; propagating \
                                 to PyO3 boundary for exit(1)"
                            );
                            Ok(ObserverRunOutcome::Aborted(reason))
                        }
                        Ok(ObserverTerminal::Panik { matched_path }) => {
                            tracing::error!(
                                matched_path = %matched_path.display(),
                                "observer panik shutdown; propagating \
                                 to PyO3 boundary for exit(137)"
                            );
                            Ok(ObserverRunOutcome::Panik(matched_path))
                        }
                        Err(e) => {
                            // Strand backstop (fleet-dead / primary-silence)
                            // or a fatal-exit policy. Surface as a typed
                            // Python exception (non-zero exit) — the run was
                            // stranded, not cleanly complete.
                            Err(map_run_error(&e))
                        }
                    }
                }))
            });

        match result? {
            ObserverRunOutcome::Done(completed) => {
                self.completed = completed;
                Ok(())
            }
            ObserverRunOutcome::Panik(matched_path) => {
                // GIL re-acquired (the `py.detach` block returned).
                // Surface the cause to the dispatcher log one last
                // time then exit(137) — same exit-on-panik shape as
                // `PySecondaryCoordinator::run`.
                tracing::error!(
                    matched_path = %matched_path.display(),
                    "panik shutdown: observer exiting with code 137"
                );
                std::process::exit(137);
            }
            ObserverRunOutcome::Aborted(reason) => {
                // GIL re-acquired. The primary aborted the run
                // cluster-wide (#3a pre-phase duplicate). Log then
                // exit(1) — same exit-on-terminal shape as the
                // secondary's `RunOutcome::Terminal` /
                // `SecondaryTerminal::Aborted` arm.
                tracing::error!(
                    reason = %reason,
                    "run aborted by primary: observer exiting with code 1"
                );
                std::process::exit(1);
            }
        }
    }

    /// Observed completion count read off the snapshot + any live
    /// broadcasts the observer ingested during its run window.
    /// Equivalent to a regular secondary's `completed_count` —
    /// surfaces the union of completed tasks visible in
    /// `cluster_state.tasks`.
    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

/// Map an observer-run `RunError` (strand backstop or fatal-exit policy)
/// to a typed Python exception. A stranded observer exits non-zero — the
/// run never reached a clean terminal, so swallowing the error to exit 0
/// would report a lie.
fn map_run_error(e: &RunError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!(
        "observer late-joiner: run did not reach a clean terminal: {e}"
    ))
}
