//! `PyObserverLateJoiner::run` — reads the peer-info dir, dials into
//! the mesh, restores the cluster snapshot, and drives the standalone
//! observer. Also exposes the `completed` getter.

use std::time::Duration;

use pyo3::prelude::*;

use dynrunner_manager_distributed::GracefulAbortTrigger;
use dynrunner_manager_distributed::cluster_state::{ClusterState, ClusterStateSnapshot};
use dynrunner_manager_distributed::observer::{ObserverConfig, build_cold_join_observer};
use dynrunner_manager_distributed::primary::RunError;
use dynrunner_manager_distributed::process::{LocalRole, Mesh, Node, NodeRunInputs, RunTerminal};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{DEFAULT_JOIN_TIMEOUT, PeerTransport};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_slurm::read_peer_info_dir_v2;

use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;

use super::PyObserverLateJoiner;
use super::gateway_mode::acquire_gateway_seed;
use super::helpers::{decode_bootstrap_snapshots, map_read_dir_error, records_to_seed};

/// Fleet-dead strand grace for a cold-join observer. There is no
/// operator kwarg for this backstop; the single source of truth mirrors
/// the primary's `PrimaryConfig` default (30s). It is the window after
/// the LAST mesh peer leaves before a stranded observer exits, and
/// doubles as the loop's re-check cadence.
const OBSERVER_FLEET_DEAD_TIMEOUT: Duration = Duration::from_secs(30);

#[pymethods]
impl PyObserverLateJoiner {
    /// Acquire the seed (locally, or fetched + tunneled through the
    /// gateway), dial into the mesh, restore the cluster snapshot, and
    /// drive the standalone observer loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        // -- pre-detach, LOCAL mode only: read the peer-info dir
        // (synchronous file I/O, small enough that we don't bother
        // offloading; surfacing ReadDirError as a Python exception
        // before we even spin up tokio keeps the error path simple).
        // GATEWAY mode defers its acquisition (connect → fetch →
        // tunnel → rewrite) into the runtime below — every one of
        // those steps is async. The branch is resolved ONCE here; the
        // runtime body consumes a uniform `(seed, gateway_runtime)`.
        let gateway_cfg = self.gateway.clone();
        let peer_info_dir = self.peer_info_dir.clone();
        let local_seed = if gateway_cfg.is_none() {
            let records = read_peer_info_dir_v2(&self.peer_info_dir).map_err(map_read_dir_error)?;
            let mut seed = records_to_seed(&records);
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
            // Overlay the submitter-persisted cert pins (explicit path,
            // or the run's conventional local cert dir) so the peer
            // dials authenticate over QUIC. Absent credentials keep the
            // cert-less WSS-fallback seed byte-identical.
            super::helpers::apply_local_peer_credentials(
                &mut seed,
                self.mesh_credentials_path.as_deref(),
                &self.peer_info_dir,
            )?;
            Some(seed)
        } else {
            None
        };

        let observer_id = self.observer_id.clone();
        // Strand-backstop thresholds for the standalone observer's
        // `ObserverConfig`. `peer_timeout` is the primary-silence backstop.
        let peer_timeout = self.distributed_config.peer_timeout();
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
                    // Arm the operator's SIGUSR2 graceful-abort trigger
                    // FIRST — before the gateway/seed acquisition and the
                    // join rendezvous — so a signal sent during the
                    // bootstrap window is latched instead of killing the
                    // process via the kernel's default disposition (the
                    // CLI documents SIGUSR2-to-this-late-joiner, and even
                    // advises re-sending during a primary failover —
                    // exactly the window that used to be lethal). The SAME
                    // armed trigger is handed to the observer coordinator
                    // below (step 6b), so a buffered pre-seat delivery is
                    // consumed by its run loop exactly like a post-seat
                    // one. Delivery still requires the seat: the request
                    // routes via `Destination::Primary`, which only
                    // resolves once the bootstrap snapshot has warmed the
                    // role cache — there is nothing to send on before then.
                    let mut abort_trigger = Some(GracefulAbortTrigger::arm());

                    // 0. Acquire the seed. LOCAL mode resolved it
                    //    pre-detach; GATEWAY mode connects the gateway,
                    //    mirrors the remote `.info` dir, establishes the
                    //    per-peer `ssh -L` tunnels, and rewrites the
                    //    seed's dial targets to the local endpoints
                    //    (`gateway_mode.rs`). The returned runtime holds
                    //    the connected gateway + the tunnel registry for
                    //    the reconnector wiring and the teardown below.
                    let (seed, gateway_runtime) = match local_seed {
                        Some(seed) => (seed, None),
                        None => {
                            let cfg = gateway_cfg
                                .expect("gateway mode iff the local pre-read was skipped");
                            match acquire_gateway_seed(cfg, &peer_info_dir).await {
                                Ok((seed, runtime)) => (seed, Some(runtime)),
                                Err(e) => {
                                    // A bootstrap-failure exit like the
                                    // join body's below: narrate a latched
                                    // operator abort before surfacing.
                                    narrate_undelivered_abort(&mut abort_trigger).await;
                                    return Err(e);
                                }
                            }
                        }
                    };

                    // Run the join + observation body, then ALWAYS tear
                    // the gateway runtime down (tunnel children + the
                    // gateway master) on both the success and error
                    // exits before surfacing the outcome.
                    let run_result: Result<ObserverRunOutcome, PyErr> = async {
                        // 1. Stand up the real peer transport with our chosen
                        //    observer-id through the backend-opaque factory.
                        //    The CN baked into the cert MUST match
                        //    `observer_id` because every dialing peer
                        //    validates the SAN against the logical id. The
                        //    standalone observer holds this mesh transport
                        //    directly (no cert bundle — it ships no PeerCertInfo).
                        let mut peer_network =
                            transport_factory::observer_mesh::<RunnerIdentifier>(&observer_id)
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
                        // Milestone 3a (LLM-wake): the bootstrap window is
                        // opening — narrate the wait with its deadline budget
                        // so an operator on `--important-stdio-only` knows how
                        // long the join may block (the failure path already
                        // errors loudly). Emitted in BOTH modes; the join
                        // rendezvous runs identically regardless of how the
                        // seed was acquired.
                        super::bootstrap_narration::waiting_for_crdt(DEFAULT_JOIN_TIMEOUT);
                        let bootstrap = peer_network
                            .join_running_cluster(&seed, DEFAULT_JOIN_TIMEOUT, true, false)
                            .await
                            .map_err(|e| {
                                pyo3::exceptions::PyRuntimeError::new_err(format!(
                                    "observer late-joiner: join_running_cluster failed: {e}"
                                ))
                            })?;

                        // 3. Decode each stream-package payload. The wire
                        //    payload is an opaque String (the protocol crate
                        //    keeps `I` erased there); we materialise each back
                        //    into a typed PARTIAL snapshot here. A
                        //    bootstrap-decode failure is FATAL (the `?`
                        //    propagates) — the observer requested the stream
                        //    precisely to populate its CRDT, so a malformed
                        //    bootstrap payload must not be swallowed (continuing
                        //    on an empty CRDT would report a lie). This is the
                        //    BOOTSTRAP discriminator arm — the steady-state decode
                        //    is WARN-and-keep (the AE-3 cadence re-pulls). The
                        //    cold-join factory `restore()`s each — the lattice
                        //    unions the partials — then applies the live gossip
                        //    buffered during the window.
                        let snaps: Vec<ClusterStateSnapshot<RunnerIdentifier>> =
                            decode_bootstrap_snapshots(&bootstrap.payloads)?;

                        // Milestone 3b (LLM-wake): narrate the snapshot the
                        // join landed. `join_running_cluster` fans to every
                        // responder and the factory `restore()`s each into
                        // one unioned CRDT, so report the UNION of distinct
                        // task ids + capability-roster ids across responders
                        // (not a per-snapshot sum, which would double-count
                        // the overlap) — the same union the restore converges.
                        let task_ids: std::collections::HashSet<&str> = snaps
                            .iter()
                            .flat_map(|s| s.tasks.keys().map(String::as_str))
                            .collect();
                        let fleet_ids: std::collections::HashSet<&str> = snaps
                            .iter()
                            .flat_map(|s| s.capabilities.keys().map(String::as_str))
                            .collect();
                        super::bootstrap_narration::crdt_snapshot_received(
                            task_ids.len(),
                            fleet_ids.len(),
                        );

                        // 4. Build the standalone observer's config. No
                        //    scheduler / worker / dispatch fields — an observer
                        //    has none of those concerns; the config carries only
                        //    the node identity, the strand-backstop thresholds,
                        //    and the panik trigger inputs.
                        let config = ObserverConfig {
                            node_id: observer_id.clone(),
                            fleet_dead_timeout: OBSERVER_FLEET_DEAD_TIMEOUT,
                            peer_timeout,
                            panik_watcher_paths,
                            panik_watcher_poll_interval,
                            fleet_death_presumption:
                                ObserverConfig::DEFAULT_FLEET_DEATH_PRESUMPTION,
                        };

                        // 4b. Gateway mode: arm the per-LEG forward-recovery
                        //     trigger (#419). The transport's 5s reconnect
                        //     ticker re-dials each rewritten
                        //     `127.0.0.1:<local_port>` endpoint, but a single
                        //     dead `ssh -L` child leaves that endpoint
                        //     permanently un-connectable — the ticker redials a
                        //     hole forever (the run-level lost-visibility
                        //     trigger never fires while the OTHER legs keep the
                        //     observer Visible). Subscribe the transport's
                        //     persistent-dial-failure signal and drive the
                        //     SAME gated forward rebuild the lost-visibility
                        //     path uses, but for the ONE undialable peer. The
                        //     transport reports a peer id; the registry decides
                        //     "id → forward → rebuild" (unknown ids — peers that
                        //     joined post-bootstrap — are named-and-skipped by
                        //     the reconnector). Set on the bare `peer_network`
                        //     BEFORE it is consumed into the mesh below.
                        if let Some(runtime) = &gateway_runtime {
                            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                            peer_network.notify_persistent_dial_failures(tx);
                            let reconnector = dynrunner_slurm::LocalForwardTunnelReconnector::new(
                                runtime.tunnels.clone(),
                            );
                            // Per-leg recovery pump: drain undialable-peer ids
                            // and rebuild each one's forward through the SHARED
                            // registry (same liveness gate + escalation state as
                            // the observer's all-legs reconnector — both hold the
                            // same `Arc<LocalForwardTunnels>`, so a still-alive
                            // child is never rebuilt twice). Exits when the
                            // channel closes (the transport — owned by the Node
                            // below — drops at run end), so it is bound to the
                            // run lifetime without a separate teardown lever.
                            tokio::task::spawn_local(async move {
                                use dynrunner_manager_distributed::observer::TunnelReconnector;
                                while let Some(peer_id) = rx.recv().await {
                                    reconnector.reconnect(std::slice::from_ref(&peer_id)).await;
                                }
                            });
                        }

                        // 5. Wrap the live mesh transport (the bootstrap
                        //    rendezvous above already ran on the bare
                        //    `peer_network`, before it is consumed here) in the
                        //    role-demux `Mesh` and register the Observer slot,
                        //    minting the coordinator's `(client, inbox)` ends + the
                        //    `Arc<RoleSlot>` the `Node` holds as the teardown lever.
                        let mut mesh = Mesh::new(peer_network);
                        let (obs_slot, obs_client, obs_inbox) = mesh.register_local_role(
                            LocalRole::Observer,
                            PeerId::from(observer_id.as_str()),
                        );

                        // 6. Cold-join: build the standalone ObserverCoordinator
                        //    over the mesh ends + the bootstrap snapshot(s). The
                        //    factory installs the task-completed sender, attaches
                        //    the resource-holdings announcer's role-change hook,
                        //    then restores each snapshot (so the restore's
                        //    role-change fire pushes the initial holdings announce
                        //    into the registered channel), and spawns the panik
                        //    watcher. The whole observation runtime — reporter,
                        //    failure policies, announcer task, panik arm, teardown
                        //    — lives inside `ObserverCoordinator::run`.
                        let mut observer = build_cold_join_observer(
                            obs_client,
                            obs_inbox,
                            ClusterState::<RunnerIdentifier>::new(),
                            config,
                            snaps,
                            bootstrap.live_frames,
                            holdings,
                        );
                        // 6b. Hand the entry-armed SIGUSR2 trigger to the
                        //     coordinator (taken exactly once; a bootstrap
                        //     failure before this point leaves it in the
                        //     outer slot for the undelivered-abort
                        //     narration below). The run loop's
                        //     graceful-abort arm consumes it, so a signal
                        //     latched during the join is delivered to the
                        //     primary on the first poll.
                        observer.set_graceful_abort_trigger(
                            abort_trigger
                                .take()
                                .expect("armed once at entry; taken once here"),
                        );
                        // Gateway mode: the transport's own QUIC/WSS
                        // reconnect ticker re-dials the rewritten
                        // `127.0.0.1:<local_port>` endpoints, which heals
                        // ONLY if the underlying `ssh -L` child is rebuilt —
                        // wire the registry's reconnector so the observer's
                        // lost-visibility trigger drives exactly that
                        // (same-port rebuild, liveness-gated; see
                        // `dynrunner_slurm::local_forward`). Local mode keeps
                        // the factory's `None` (direct addresses heal through
                        // the ticker alone).
                        if let Some(runtime) = &gateway_runtime {
                            observer.set_tunnel_reconnector(std::sync::Arc::new(
                                dynrunner_slurm::LocalForwardTunnelReconnector::new(
                                    runtime.tunnels.clone(),
                                ),
                            ));
                        }

                        // 7. Compose a pure-observer `Node` (observer = Some;
                        //    primary/secondary = None) and drive `Node::run`. A
                        //    standalone observer IS a `Node` — one OS process
                        //    owning its role composition — so it goes through the
                        //    same `Node::run` as the submitter/compute peers; the
                        //    promotion/demote/swap arms are simply inert (no
                        //    secondary fires promotion, no primary to demote). The
                        //    `Node` owns the mesh-pump (ingress demux + PeerInfo
                        //    dialing) that the observer's ingress depends on.
                        let (node, _node_promo_tx) = Node::new(mesh);
                        let node = node.with_observer(observer, obs_slot);
                        let inputs: NodeRunInputs<
                            crate::subprocess_factory::SubprocessWorkerFactory,
                            ResourceStealingScheduler,
                            crate::estimator::PyMemoryEstimatorBridge,
                            RunnerIdentifier,
                        > = NodeRunInputs::default();
                        let node_outcome = node.run(inputs).await;

                        // 8. The node ends as an observer; `Node::run` resolves to
                        //    ONE role-agnostic `RunTerminal` (+ the observer's
                        //    converged `completed` count). Map the terminal to the
                        //    boundary's exit-code outcome (Done⇒0 / Aborted⇒1 /
                        //    Panik⇒137 / Failed⇒non-zero PyErr).
                        let completed = node_outcome.completed as u32;
                        match node_outcome.terminal {
                            RunTerminal::Done => Ok(ObserverRunOutcome::Done(completed)),
                            RunTerminal::GracefulAbort { reason } => {
                                // The composed graceful-abort verdict the
                                // observer derived (`run_complete ∧
                                // graceful_abort`). A deliberate clean
                                // wind-down: reported loudly (the narrator
                                // already emitted the counts summary on the
                                // important channel), exits 0 — distinct from
                                // the silent `Done` and from the hard-abort
                                // exit(1) below.
                                tracing::warn!(verdict = %reason, "run gracefully aborted");
                                Ok(ObserverRunOutcome::Done(completed))
                            }
                            RunTerminal::Aborted { reason } => {
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
                            RunTerminal::Panik { matched_path } => {
                                tracing::error!(
                                    matched_path = %matched_path.display(),
                                    "observer panik shutdown; propagating \
                                     to PyO3 boundary for exit(137)"
                                );
                                Ok(ObserverRunOutcome::Panik(matched_path))
                            }
                            RunTerminal::Failed { error } => {
                                // Strand backstop (fleet-dead / primary-silence)
                                // or a fatal-exit policy. Surface as a typed
                                // Python exception (non-zero exit) — the run was
                                // stranded, not cleanly complete.
                                Err(map_run_error(&error))
                            }
                        }
                    }
                    .await;
                    if let Some(runtime) = gateway_runtime {
                        runtime.teardown().await;
                    }
                    // Bootstrap failed BEFORE the trigger was handed to the
                    // observer (a seated run consumed it at step 6b): if the
                    // operator's graceful abort was latched during the
                    // failed bootstrap, narrate it on the wake stream so the
                    // intent is never silently lost — the process exits via
                    // the propagated error either way.
                    if run_result.is_err() {
                        narrate_undelivered_abort(&mut abort_trigger).await;
                    }
                    run_result
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

/// Failed-bootstrap tail: if the entry-armed SIGUSR2 trigger is still
/// unconsumed (the observer never seated — every seated run took it at
/// step 6b), narrate a latched operator graceful-abort on the wake stream
/// so the intent is never silently lost. The ONE undelivered-abort exit
/// shared by every bootstrap-failure path in `run`.
async fn narrate_undelivered_abort(trigger_slot: &mut Option<GracefulAbortTrigger>) {
    if let Some(trigger) = trigger_slot.take() {
        trigger.report_undelivered().await;
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
