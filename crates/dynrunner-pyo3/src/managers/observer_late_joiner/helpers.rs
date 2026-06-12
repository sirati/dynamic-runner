//! Peer-info-dir helpers: error mapping, seed-record construction, and
//! the local peer-credentials overlay (cert pins for QUIC dials).
//!
//! Called from the run loop; isolated here so the dispatcher body
//! stays focused on orchestration. `apply_local_peer_credentials` is
//! the one function with filesystem access (the credentials probe +
//! load); the rest are pure.

use pyo3::prelude::*;

use dynrunner_manager_distributed::cluster_state::ClusterStateSnapshot;
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use dynrunner_slurm::{PeerInfoReadDirError, PeerInfoRecord};

use crate::identifier::RunnerIdentifier;

/// Translate a [`PeerInfoReadDirError`] into the right PyError shape
/// for the operator. Single concern: error-mapping at the FFI
/// boundary; keeps the `run()` body focused on orchestration.
pub(super) fn map_read_dir_error(e: PeerInfoReadDirError) -> PyErr {
    match e {
        PeerInfoReadDirError::Io { ref dir, .. } => {
            // io::ErrorKind::NotFound is the most operator-actionable
            // shape (typo'd path, dir not yet created); other I/O
            // errors get the generic OSError shape. The full chain
            // is preserved via the Display impl.
            pyo3::exceptions::PyOSError::new_err(format!(
                "observer late-joiner: peer-info directory unreadable ({dir}): {e}"
            ))
        }
        PeerInfoReadDirError::Parse { ref path, .. } => pyo3::exceptions::PyValueError::new_err(
            format!("observer late-joiner: malformed peer-info file ({path}): {e}"),
        ),
        PeerInfoReadDirError::NoV2Records { ref dir } => {
            // The dir is structurally OK but produced no v2 records
            // — fail loud per the late-joiner design constraint.
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "observer late-joiner: peer-info dir {dir} contains no v2 records — \
                 either the dir is empty, has no `*.info` files, or every file is \
                 legacy v1 (pre-Step-7 wrapper). The late-joiner snapshot RPC \
                 requires the v2 envelope (secondary_id + quic_port + at least \
                 one of ipv4/ipv6; cert_pem_b64 is optional — the production \
                 wrapper omits it, and a cert-less record is dialed over WSS \
                 rather than QUIC). Re-run the cluster with a Step-7-or-newer \
                 wrapper, or point this flag at a different directory."
            ))
        }
    }
}

/// Convert SLURM-wrapper [`PeerInfoRecord`] entries into the wire-shape
/// [`PeerConnectionInfo`] entries `join_running_cluster` consumes.
///
/// # Filter logic
///
/// `read_peer_info_dir_v2` has already dropped v1 records; we just
/// need to construct the wire struct. Records missing `secondary_id`
/// or `quic_port` (a malformed v2 envelope that nonetheless parsed
/// — e.g. the wrapper crashed between writing the URI line and the
/// envelope tail) are dropped silently here rather than erroring:
/// the joiner's `peer_count() > 0` gate already handles the
/// "no usable seeds survived" case via [`JoinError::NoReachablePeer`].
///
/// [`JoinError::NoReachablePeer`]:
///   dynrunner_protocol_primary_secondary::JoinError::NoReachablePeer
pub(super) fn records_to_seed(records: &[PeerInfoRecord]) -> Vec<PeerConnectionInfo> {
    records
        .iter()
        .filter_map(|r| {
            let secondary_id = r.secondary_id.clone()?;
            let quic_port = r.quic_port?;
            // `cert` is `String` (not `Option<String>`) on the wire
            // frame. A cert-less record is the NORMAL production shape:
            // the SLURM wrapper intentionally omits `cert_pem_b64` from
            // the on-disk record (`slurm-wrapper/wrapper/src/network.rs`
            // — the peer's cert is minted in-container AFTER the record
            // is written, so the wrapper cannot know it). The dialer
            // (`peer/dial.rs::dial_peer`) skips the QUIC race when no
            // valid cert parses and goes straight to WSS on the same
            // port, which needs no pinned cert — so cert-less late-join
            // works end-to-end over WSS. Only the QUIC leg of the dial
            // requires a record that actually carries the cert.
            let cert = r.cert_pem.clone().unwrap_or_default();
            Some(PeerConnectionInfo {
                secondary_id,
                cert,
                ipv4: r.ipv4.clone(),
                ipv6: r.ipv6.clone(),
                port: quic_port,
                is_observer: r.is_observer.unwrap_or(false),
                // These reconstructed records seed the late-joiner's QUIC
                // DIAL set, not a liveness beacon: the late joiner is an
                // observer (no workers → never reaped → emits no beacon).
                // The on-disk peer-info-dir record carries no liveness port,
                // and the observer needs none, so `None`.
                liveness_port: None,
            })
        })
        .collect()
}

/// Overlay the submitter-persisted peer credentials (the roster's cert
/// pins — [`dynrunner_manager_distributed::peer_credentials`]) onto a
/// LOCAL-mode seed, so the joiner's peer dials authenticate over QUIC
/// instead of degrading to WSS.
///
/// Path resolution:
/// - `explicit_path` (`--observer-mesh-credentials`): the file MUST
///   load — the operator asked for it, so a failure is a hard error.
/// - `None`: derive the conventional location from the peer-info dir's
///   run id (`crate::slurm::local_run_state`) and PROBE it. Absent →
///   nothing happens (old run dirs / non-submitter hosts keep today's
///   WSS-fallback behaviour exactly); present-but-unloadable → WARN
///   and continue (a stale or torn file must not brick the join).
///
/// Gateway mode never calls this: tunneled legs are TCP-only (no
/// QUIC), and the constructor rejects the explicit flag with
/// `--gateway` up front.
pub(super) fn apply_local_peer_credentials(
    seed: &mut [PeerConnectionInfo],
    explicit_path: Option<&std::path::Path>,
    peer_info_dir: &std::path::Path,
) -> PyResult<()> {
    use crate::slurm::local_run_state;
    use dynrunner_manager_distributed::peer_credentials;

    let (path, explicit) = match explicit_path {
        Some(p) => (p.to_path_buf(), true),
        None => {
            let Some(run_id) = local_run_state::derive_run_id_from_info_dir(peer_info_dir) else {
                tracing::debug!(
                    peer_info_dir = %peer_info_dir.display(),
                    "peer-info dir does not follow the <base>/<run_id>/connection_info \
                     convention; skipping the local peer-credentials probe"
                );
                return Ok(());
            };
            let p = local_run_state::peer_credentials_path(&local_run_state::cert_dir_for_run(
                &run_id,
            ));
            if !p.exists() {
                tracing::debug!(
                    path = %p.display(),
                    "no local peer-credentials file for this run; peer dials \
                     keep the WSS fallback"
                );
                return Ok(());
            }
            (p, false)
        }
    };
    match peer_credentials::load_peer_credentials(&path) {
        Ok(creds) => {
            let filled = peer_credentials::overlay_seed_certs(seed, &creds);
            tracing::info!(
                path = %path.display(),
                credential_entries = creds.len(),
                seed_certs_overlaid = filled,
                "loaded local peer credentials; cert-pinned seed entries will \
                 dial peers over QUIC with valid certs"
            );
            Ok(())
        }
        Err(e) if explicit => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "observer late-joiner: failed to load the explicit mesh-credentials file: {e}"
        ))),
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "derived local peer-credentials file failed to load; \
                 continuing with the WSS fallback"
            );
            Ok(())
        }
    }
}

/// Decode every bootstrap stream-package payload (wire-erased
/// base64-CBOR strings) into the typed partial [`ClusterStateSnapshot`]s
/// the cold-join factory restores — one `restore` per package; the
/// idempotent lattice unions the partials in any order. The decode
/// itself is the ONE shared codec
/// (`dynrunner_manager_distributed::cluster_state::decode_stream_payload`),
/// never re-implemented here.
///
/// # Single concern + the BOOTSTRAP-decode fatal discriminator (D-C / D3)
///
/// This is the COLD-JOIN bootstrap decode — the observer requested the
/// stream precisely to populate its empty CRDT, so a malformed payload
/// must HARD-FAIL here (an `Err` the constructor propagates with `?`),
/// never be swallowed: continuing on an un-restored empty CRDT would make
/// the observer report a lie (premature run-complete / wrong counts). This
/// is the deliberate counterpart to the STEADY-STATE anti-entropy decode
/// arm (`secondary/dispatch/router.rs` + the observer's
/// `on_snapshot_stream_package`), which is WARN-and-keep because the AE-3
/// recovery cadence re-pulls (resuming from the last good cursor). The
/// discriminator is WHICH FUNCTION the decode lives in: this bootstrap
/// function = fatal; the steady-state loop arm = WARN.
pub(super) fn decode_bootstrap_snapshots(
    payloads: &[String],
) -> PyResult<Vec<ClusterStateSnapshot<RunnerIdentifier>>> {
    payloads
        .iter()
        .map(|payload| {
            dynrunner_manager_distributed::cluster_state::decode_stream_payload(payload).map_err(
                |e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "observer late-joiner: failed to decode snapshot-stream \
                         package from join_running_cluster reply: {e}"
                    ))
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm::{PeerInfoBuilder, parse_peer_info};

    /// Construct a `PeerInfoRecord` end-to-end via the SLURM-wrapper
    /// public surface (`Builder::format` then `parse`). Goes through
    /// the same bytes the on-disk file carries so the test exercises
    /// the same conversion path the late-joiner runs in production.
    fn record_from_builder(b: PeerInfoBuilder) -> PeerInfoRecord {
        parse_peer_info(&b.format()).expect("builder output round-trips through parse")
    }

    /// `records_to_seed` drops records missing the snapshot-RPC-required
    /// fields (`secondary_id` / `quic_port`) rather than producing a
    /// half-filled `PeerConnectionInfo` that would CN-fail on dial.
    /// The "happy" record passes through with every field carried over.
    #[test]
    fn records_to_seed_drops_records_missing_required_fields() {
        let happy = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT-PEM-HERE")
                .ipv4("10.0.0.1")
                .quic_port(51200)
                .is_observer(false),
        );
        // Missing quic_port: the dial would have nowhere to go.
        let no_port = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("sec-2")
                .cert_pem("CERT-PEM-HERE"),
        );
        // Missing secondary_id: the snapshot RPC envelope keys on
        // the responder's secondary_id; without it, the joiner
        // can't construct the unicast Address::Peer target.
        let no_id = record_from_builder(PeerInfoBuilder::new("compute3", 40003).quic_port(51202));

        let seed = records_to_seed(&[happy, no_port, no_id]);
        assert_eq!(seed.len(), 1);
        assert_eq!(seed[0].secondary_id, "sec-1");
        assert_eq!(seed[0].port, 51200);
        assert_eq!(seed[0].cert, "CERT-PEM-HERE");
        assert_eq!(seed[0].ipv4.as_deref(), Some("10.0.0.1"));
        assert!(!seed[0].is_observer);
    }

    /// `records_to_seed` carries `is_observer` through verbatim — the
    /// joiner's election filter (Step 7) needs the flag on every
    /// seed entry so it doesn't pick an observer as the responder
    /// preference.
    #[test]
    fn records_to_seed_preserves_is_observer_flag() {
        let observer = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("obs-1")
                .cert_pem("CERT-OBS")
                .quic_port(51200)
                .is_observer(true),
        );
        let regular = record_from_builder(
            PeerInfoBuilder::new("compute2", 40002)
                .secondary_id("reg-1")
                .cert_pem("CERT-REG")
                .quic_port(51201)
                .is_observer(false),
        );

        let mut seed = records_to_seed(&[observer, regular]);
        seed.sort_by(|a, b| a.secondary_id.cmp(&b.secondary_id));
        assert_eq!(seed.len(), 2);
        assert_eq!(seed[0].secondary_id, "obs-1");
        assert!(seed[0].is_observer);
        assert_eq!(seed[1].secondary_id, "reg-1");
        assert!(!seed[1].is_observer);
    }

    /// Records missing the optional `is_observer` envelope key
    /// default to `false` — matches the pre-Step-7 v2 senders
    /// (cf. PeerConnectionInfo's `#[serde(default)]`).
    #[test]
    fn records_to_seed_defaults_is_observer_to_false() {
        let r = record_from_builder(
            PeerInfoBuilder::new("compute1", 40001)
                .secondary_id("sec-1")
                .cert_pem("CERT")
                .quic_port(51200),
            // no is_observer set
        );
        let seed = records_to_seed(&[r]);
        assert_eq!(seed.len(), 1);
        assert!(!seed[0].is_observer);
    }

    /// BOOTSTRAP-decode fatal (D-C / D3): a malformed bootstrap snapshot
    /// reply HARD-FAILS (the constructor's `?` propagates the `Err`). This
    /// is the counterpart to the steady-state WARN arm — a cold-join MUST
    /// hard-fail on a corrupt INITIAL snapshot rather than observe an empty
    /// CRDT and report a lie.
    #[test]
    fn decode_bootstrap_snapshots_hard_fails_on_malformed() {
        let result = decode_bootstrap_snapshots(&["{not valid json".to_string()]);
        let err = result.expect_err("a malformed bootstrap snapshot must hard-fail");
        assert!(
            err.to_string()
                .contains("failed to decode ClusterStateSnapshot"),
            "the bootstrap-fatal error must name the decode failure: {err}"
        );
    }

    /// The happy path: well-formed bootstrap snapshot JSON decodes into the
    /// typed snapshot the cold-join factory restores.
    #[test]
    fn decode_bootstrap_snapshots_round_trips_valid_payload() {
        use dynrunner_manager_distributed::cluster_state::ClusterState;
        let json = serde_json::to_string(&ClusterState::<RunnerIdentifier>::new().snapshot())
            .expect("snapshot serializes");
        let snaps = decode_bootstrap_snapshots(&[json]).expect("valid snapshot decodes");
        assert_eq!(snaps.len(), 1);
    }

    fn certless_seed_entry(id: &str) -> PeerConnectionInfo {
        PeerConnectionInfo {
            secondary_id: id.into(),
            cert: String::new(),
            ipv4: Some("10.0.0.1".into()),
            ipv6: None,
            port: 5000,
            is_observer: false,
            liveness_port: None,
        }
    }

    /// EXPLICIT credentials path (`--observer-mesh-credentials`): a
    /// load failure is a HARD error — the operator asked for the file,
    /// silently proceeding cert-less would lie about the dial security.
    #[test]
    fn explicit_credentials_path_load_failure_is_fatal() {
        let mut seed = vec![certless_seed_entry("sec-0")];
        let err = apply_local_peer_credentials(
            &mut seed,
            Some(std::path::Path::new("/nonexistent/peer_credentials.json")),
            std::path::Path::new("/anywhere/connection_info"),
        )
        .expect_err("an explicit-but-unloadable credentials file must hard-fail");
        assert!(
            err.to_string().contains("mesh-credentials"),
            "the error must name the credentials load failure: {err}"
        );
    }

    /// EXPLICIT path that loads: certs are overlaid onto the cert-less
    /// seed entries by id.
    #[test]
    fn explicit_credentials_path_overlays_certs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_credentials.json");
        let mut cred = certless_seed_entry("sec-0");
        cred.cert = "PINNED-CERT".into();
        dynrunner_manager_distributed::peer_credentials::store_peer_credentials(&path, &[cred])
            .unwrap();

        let mut seed = vec![certless_seed_entry("sec-0"), certless_seed_entry("sec-1")];
        apply_local_peer_credentials(
            &mut seed,
            Some(&path),
            std::path::Path::new("/anywhere/connection_info"),
        )
        .expect("a loadable explicit credentials file applies");
        assert_eq!(seed[0].cert, "PINNED-CERT");
        assert_eq!(seed[1].cert, "", "an uncovered peer stays cert-less (WSS fallback)");
    }

    /// NO explicit path, conventional run dir, credentials PRESENT at
    /// the derived `/tmp/db-runner-cert-<run_id>` location: the full
    /// derive→probe→load→overlay path fills the certs — the exact
    /// pickup a late-joiner spawned on the submitter host runs. Uses a
    /// unique 1999-dated run id so it can never collide with a real
    /// run's local state; cleans the /tmp dir up afterwards.
    #[test]
    fn derived_credentials_present_overlay_end_to_end() {
        let run_id = format!("run_19990101_{:06}", std::process::id() % 1_000_000);
        let cert_dir = crate::slurm::local_run_state::cert_dir_for_run(&run_id);
        let cred_path = crate::slurm::local_run_state::peer_credentials_path(&cert_dir);
        let mut cred = certless_seed_entry("sec-0");
        cred.cert = "DERIVED-CERT".into();
        dynrunner_manager_distributed::peer_credentials::store_peer_credentials(
            &cred_path,
            &[cred],
        )
        .unwrap();

        let info_dir = std::path::PathBuf::from(format!("/cluster/logs/{run_id}/connection_info"));
        let mut seed = vec![certless_seed_entry("sec-0")];
        let result = apply_local_peer_credentials(&mut seed, None, &info_dir);
        // Clean up BEFORE asserting so a failure doesn't leak /tmp state.
        std::fs::remove_dir_all(&cert_dir).ok();
        result.expect("derived credentials must apply");
        assert_eq!(seed[0].cert, "DERIVED-CERT");
    }

    /// NO explicit path + a peer-info dir that doesn't follow the
    /// run-id convention: nothing is probed, the seed is untouched, and
    /// the call succeeds — the old-run-dirs backward-compat contract.
    #[test]
    fn derived_credentials_absent_keeps_seed_unchanged() {
        let mut seed = vec![certless_seed_entry("sec-0")];
        apply_local_peer_credentials(
            &mut seed,
            None,
            std::path::Path::new("/data/not-a-run-dir/connection_info"),
        )
        .expect("absence of credentials must never fail the join");
        assert_eq!(seed[0].cert, "");
    }
}
