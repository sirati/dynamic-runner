//! Tests for the `TunnelStore` shapes — chiefly the observer-reconnect
//! [`PerSecondaryTunnelRegistry`] whose per-id liveness gate + replacement
//! is the defect-(a) fix. Driven with real `/bin/sh` children (a
//! long-running `sleep` for "alive", an immediate `exit` for "dead") so
//! the `try_wait()`-based liveness probe is exercised exactly as
//! production sees it — no mock process state.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::preparation::store::{PerSecondaryTunnelRegistry, SharedTunnelVec, TunnelStore};

/// A child that stays alive well past any test (reaped via
/// `kill_on_drop` when the registry/test drops it). Models a HEALTHY
/// `-R` forward.
fn long_running_child() -> Child {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg("sleep 60");
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    cmd.spawn().expect("spawn /bin/sh sleep")
}

/// A child that exits immediately. Models a DEAD `-R` forward whose ssh
/// subprocess already terminated.
fn immediately_exiting_child() -> Child {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg("exit 0");
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    cmd.spawn().expect("spawn /bin/sh exit")
}

fn registry() -> (
    PerSecondaryTunnelRegistry,
    Arc<Mutex<HashMap<String, Child>>>,
) {
    let inner = Arc::new(Mutex::new(HashMap::new()));
    (PerSecondaryTunnelRegistry::new(Arc::clone(&inner)), inner)
}

/// THE defect-(a) gate signal: a committed child that is still running
/// reads back as alive, so the reconnect path NO-OPs the rebuild. The
/// blindness that re-fired release+rebind every ~60s tick against the
/// observer's OWN healthy listener is killed at this layer — the cadence
/// now gets its success signal from the Child handle owner.
#[tokio::test(flavor = "current_thread")]
async fn is_alive_true_for_running_child_gates_the_rebuild() {
    let (reg, _inner) = registry();
    reg.commit("secondary-0", long_running_child()).await;
    assert!(
        reg.is_alive("secondary-0").await,
        "a running tunnel child must read alive so the rebuild is a no-op",
    );
}

/// An EXITED child reads back dead (so the reconnect path proceeds to
/// release+rebind), and an UNKNOWN id reads dead (first rebuild for a
/// never-registered secondary). Both are the "warrant a rebuild" verdict.
#[tokio::test(flavor = "current_thread")]
async fn is_alive_false_for_exited_and_unknown() {
    let (reg, _inner) = registry();

    // Unknown id: no entry ⇒ dead ⇒ rebuild warranted.
    assert!(
        !reg.is_alive("never-seen").await,
        "an id with no registry entry must read dead",
    );

    // Exited child: try_wait yields Ok(Some(status)) ⇒ dead.
    let mut dead = immediately_exiting_child();
    // Ensure the child has actually exited before we probe (race-free).
    let _ = dead.wait().await;
    reg.commit("secondary-1", dead).await;
    assert!(
        !reg.is_alive("secondary-1").await,
        "an exited tunnel child must read dead so release+rebind proceeds",
    );
}

/// Commit REPLACES the per-id entry and reaps the displaced child: after
/// a second commit for the same id, the registry holds exactly one entry
/// and the new (alive) child is the one that survives. This is the child-
/// accumulation fix — the anonymous Vec would have grown a dead lingerer
/// per rebuild.
#[tokio::test(flavor = "current_thread")]
async fn commit_replaces_entry_and_reaps_displaced() {
    let (reg, inner) = registry();

    // First commit: an exited child (the dead forward we are rebuilding).
    let mut old = immediately_exiting_child();
    let _ = old.wait().await;
    reg.commit("secondary-0", old).await;

    // Second commit for the SAME id: a fresh long-running child replaces
    // the dead one. The displaced child is terminated inside commit.
    reg.commit("secondary-0", long_running_child()).await;

    // Exactly one entry for the id — no accumulation.
    {
        let guard = inner.lock().await;
        assert_eq!(
            guard.len(),
            1,
            "registry must hold exactly one child per id"
        );
        assert!(guard.contains_key("secondary-0"));
    }
    // And it is the live replacement.
    assert!(
        reg.is_alive("secondary-0").await,
        "the surviving entry must be the fresh live child",
    );
}

/// `drain_and_terminate` empties the registry (and SIGTERMs the held
/// children); a second call is a harmless no-op — the same idempotent
/// teardown contract `cleanup()` relies on.
#[tokio::test(flavor = "current_thread")]
async fn registry_drain_is_idempotent() {
    let (reg, inner) = registry();
    reg.commit("secondary-0", long_running_child()).await;
    reg.commit("secondary-1", long_running_child()).await;
    assert_eq!(inner.lock().await.len(), 2);

    reg.drain_and_terminate().await;
    assert!(
        inner.lock().await.is_empty(),
        "drain must empty the registry"
    );

    // Second drain: no-op, no panic.
    reg.drain_and_terminate().await;
    assert!(inner.lock().await.is_empty());
}

/// The append-only `SharedTunnelVec` ignores the id (fresh node, no prior
/// child) and accumulates each commit — and drains them all. Pins that
/// the cohort/respawn path is unchanged by the store seam.
#[tokio::test(flavor = "current_thread")]
async fn shared_vec_appends_and_drains() {
    let vec: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));
    let store = SharedTunnelVec::new(Arc::clone(&vec));

    store.commit("secondary-0", long_running_child()).await;
    store.commit("secondary-1", long_running_child()).await;
    assert_eq!(
        vec.lock().await.len(),
        2,
        "Vec appends each committed child"
    );

    store.drain_and_terminate().await;
    assert!(vec.lock().await.is_empty(), "drain empties the Vec");
}
