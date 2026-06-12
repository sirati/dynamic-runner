//! Tests for the INCREMENTAL cohort entry
//! (`run_tunnel_cohort_inner`) — the bring-up shape the SLURM
//! pipeline consumes through the pyo3 background driver.
//!
//! Production trace these pin (run_20260612_035452): one SLURM job sat
//! PENDING ~11 min while three live secondaries' jobs were running;
//! the pipeline's blocking cohort gate held the primary's bind (and
//! ALL welcome service) hostage to the pending member for the full
//! setup deadline, and the live members expired unconfigured. The
//! incremental entry must:
//!   1. commit each ready member's tunnel as ITS job materializes,
//!      never waiting on a pending sibling, and
//!   2. keep servicing a member whose job starts LATE — even after the
//!      summary deadline fired (the resubmit / finally-scheduled case:
//!      the incident's 4th job DID start at ~11 min).
//!
//! Establishment is driven through the same DI seams the establish
//! tests use (stub spawner/verifier, no real ssh); info files come
//! from the filesystem via `LocalDirReader`.

use std::time::Duration;

use tempfile::TempDir;

use crate::preparation::SlurmPreparation;

use super::establish::{alive_child, listening_inner_verifier};
use super::opts_for;
use super::pipeline::LocalDirReader;

/// Write `<run_log_dir>/connection_info/<id>.info` — the moment a
/// member's SLURM job "starts on a node" from the watcher's POV.
fn write_info(tmp: &TempDir, id: &str, port: u16) {
    let dir = tmp.path().join("connection_info");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{id}.info")), format!("tcp://127.0.0.1:{port}\n")).unwrap();
}

/// Stub per-member spawner factory: every establish attempt succeeds
/// with a long-lived `/bin/sh sleep` child (reaped by `cleanup()` /
/// `kill_on_drop`).
fn stub_spawner_factory() -> impl FnMut(&str) -> crate::preparation::pipeline::BoxedSpawner {
    |_id| Box::new(|_host, _port| Box::pin(async { Ok(alive_child()) }))
}

/// The incident replay: 3 members, two ready (info present), one
/// PENDING (info never appears). The ready members' tunnels must
/// commit on their own schedule while the cohort future keeps
/// running for the pending member — including PAST the summary
/// deadline — and the late member must still be served when its job
/// finally materializes.
#[test]
fn pending_member_never_blockades_and_late_member_is_served_on_arrival() {
    let tmp = tempfile::tempdir().unwrap();
    // opts_for: setup_timeout (now the SUMMARY deadline) 1500ms,
    // poll_interval 20ms.
    let opts = opts_for(&tmp);

    // Jobs 0 and 1 are RUNNING (info present before the cohort starts);
    // job 2 is PENDING in the queue.
    write_info(&tmp, "secondary-0", 40000);
    write_info(&tmp, "secondary-1", 40001);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let prep = SlurmPreparation::new(opts);
        let cohort = prep.run_tunnel_cohort_inner(
            LocalDirReader,
            3,
            9999,
            stub_spawner_factory(),
            |_id| listening_inner_verifier(),
        );
        tokio::pin!(cohort);

        // (1) Ready members are served on their own schedule. If the
        // cohort future completes here, the pending member was dropped
        // rather than kept serviceable — also a failure.
        let ready_members_served = async {
            loop {
                if prep.secondary_port_map().len() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            _ = &mut cohort => panic!("cohort completed while a member is still pending"),
            _ = ready_members_served => {}
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                panic!("ready members were not served while a sibling was pending")
            }
        }
        let map = prep.secondary_port_map();
        assert_eq!(map.get("secondary-0"), Some(&40000));
        assert_eq!(map.get("secondary-1"), Some(&40001));
        assert!(!map.contains_key("secondary-2"));

        // (2) Let the SUMMARY deadline (1500ms) pass with the member
        // still pending: the cohort must stay alive (watchers are not
        // aborted at the deadline any more).
        tokio::select! {
            _ = &mut cohort => panic!("cohort gave up on the pending member at the summary deadline"),
            _ = tokio::time::sleep(Duration::from_millis(1700)) => {}
        }

        // (3) The pending job finally starts (the incident's ~11-min
        // PENDING job / the resubmit case) — its tunnel must establish
        // on arrival and the cohort then runs to completion.
        write_info(&tmp, "secondary-2", 40002);
        tokio::time::timeout(Duration::from_secs(10), &mut cohort)
            .await
            .expect("cohort must finish once the late member establishes");
        assert_eq!(
            prep.secondary_port_map().get("secondary-2"),
            Some(&40002),
            "late-starting member must get its tunnel on arrival"
        );

        prep.cleanup().await;
    }));
}

/// Cancellation contract the pyo3 background driver relies on:
/// dropping the cohort future mid-flight (a member still pending)
/// aborts the watchers, and `cleanup()` afterwards drains the
/// already-committed children without hanging.
#[test]
fn cancelled_cohort_cleans_up_committed_tunnels() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = opts_for(&tmp);
    write_info(&tmp, "secondary-0", 41000);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let prep = SlurmPreparation::new(opts);
        {
            let cohort = prep.run_tunnel_cohort_inner(
                LocalDirReader,
                2,
                9999,
                stub_spawner_factory(),
                |_id| listening_inner_verifier(),
            );
            tokio::pin!(cohort);
            // Run until member 0 committed (member 1 stays pending),
            // then CANCEL by dropping the pinned future.
            let first_served = async {
                loop {
                    if !prep.secondary_port_map().is_empty() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            tokio::select! {
                _ = &mut cohort => panic!("cohort completed with a pending member"),
                _ = first_served => {}
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    panic!("ready member was not served")
                }
            }
        } // cohort dropped here — JoinSet abort, kill_on_drop reap.

        // The committed tunnel is drained by cleanup; bounded so a
        // regression hangs the assertion, not the suite.
        tokio::time::timeout(Duration::from_secs(10), prep.cleanup())
            .await
            .expect("cleanup after cancellation must not hang");
    }));
}
