//! Tests for the low-level ssh wire-up: `build_ssh_argv` shape (with
//! and without auth-options), and `verify_tunnel_alive` correctness
//! under concurrent invocation. Driven through the `super::ssh`
//! sub-module's `pub(super)` exposure.

use tokio::process::{Child, Command};
use tokio::task::JoinSet;

use crate::preparation::options::{PrepError, PreparationOptions};
use crate::preparation::ssh::{build_ssh_argv, verify_tunnel_alive};

/// Ssh spawn argv shape (no auth-options): -J jump_target form,
/// extra_port_forwards fan out, ExitOnForwardFailure present.
/// We test by rebuilding the argv in a sibling pure-function
/// `build_ssh_argv` — extracted so the spawn path is testable
/// without launching a real subprocess.
#[test]
fn argv_no_auth_uses_proxyjump_dash_j() {
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        22,
        vec![],
        vec![(2222, 9090)],
    );
    let argv = build_ssh_argv("compute01", 40000, 51000, &o);
    // -J alice@gw.example
    let j_idx = argv.iter().position(|s| s == "-J").expect("has -J");
    assert_eq!(argv[j_idx + 1], "alice@gw.example");
    // -R 40000:localhost:51000 + extra -R 9090:localhost:2222
    let rs: Vec<&str> = argv
        .iter()
        .enumerate()
        .filter(|(_, s)| s.as_str() == "-R")
        .map(|(i, _)| argv[i + 1].as_str())
        .collect();
    assert_eq!(rs, vec!["40000:localhost:51000", "9090:localhost:2222"]);
    // ExitOnForwardFailure=yes is present
    assert!(argv.iter().any(|s| s == "ExitOnForwardFailure=yes"));
    // remote user@host targets compute01 with the gateway user
    // (preparation defaults remote_user to gateway_user).
    assert!(argv.iter().any(|s| s == "alice@compute01"));
}

/// With auth_options non-empty we MUST NOT use -J (OpenSSH 7.3+
/// regression — -o flags don't propagate). Instead a
/// ProxyCommand= with the auth flags inline.
#[test]
fn argv_with_auth_uses_proxycommand() {
    let auth = vec![
        "-i".to_string(),
        "/tmp/key".into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        "IdentityAgent=none".into(),
    ];
    let o = PreparationOptions::new(
        "/logs".into(),
        "gw.example".into(),
        Some("alice".into()),
        2222,
        auth,
        vec![],
    );
    let argv = build_ssh_argv("compute01", 40000, 51000, &o);
    // No -J
    assert!(!argv.iter().any(|s| s == "-J"));
    // ProxyCommand= present, contains -i /tmp/key, IdentitiesOnly=yes,
    // -p 2222, -W %h:%p, alice@gw.example
    let proxy_cmd = argv
        .iter()
        .find(|s| s.starts_with("ProxyCommand="))
        .expect("has ProxyCommand=");
    assert!(proxy_cmd.contains("'-i' '/tmp/key'"));
    assert!(proxy_cmd.contains("'IdentitiesOnly=yes'"));
    assert!(proxy_cmd.contains("'-p' '2222'"));
    assert!(proxy_cmd.contains("'-W' '%h:%p'"));
    assert!(proxy_cmd.contains("'alice@gw.example'"));
}

/// Multi-watcher race regression: with ≥2 watchers calling
/// `verify_tunnel_alive` concurrently, each must observe the
/// outcome of *its own* spawned child — never a sibling's. The
/// pre-fix shape (`tunnels.lock().last_mut()`) was structurally
/// vulnerable to this: watcher A could verify watcher B's child
/// as soon as their `push` order interleaved.
///
/// We exercise the failure branch (each child exits immediately
/// with a unique stderr message) so the test is fast and the
/// stderr-attribution is directly observable in the assertion.
/// Pre-fix, the `last_mut()` lookup made misattribution possible
/// whenever a sibling's `push` interleaved between this
/// watcher's push and verify; the test asserts the post-fix
/// invariant that no such interleaving can ever occur because
/// each watcher operates on its own owned `Child`.
#[test]
fn verify_tunnel_alive_attributes_per_child_under_concurrency() {
    const N: usize = 4;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let outcomes: Vec<(String, PrepError)> = rt.block_on(local.run_until(async move {
        // Spawn N short-lived shells; each emits a marker
        // unique to its index on stderr and exits with rc=1.
        let mut children: Vec<(String, Child)> = Vec::with_capacity(N);
        for i in 0..N {
            let marker = format!("MARK-{i}");
            let mut cmd = Command::new("/bin/sh");
            cmd.arg("-c")
                .arg(format!("printf '%s' '{marker}' >&2; exit 1"));
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());
            cmd.kill_on_drop(true);
            let child = cmd.spawn().expect("spawn /bin/sh");
            children.push((format!("secondary-{i}"), child));
        }

        // Verify all in parallel from a JoinSet.
        let mut set: JoinSet<(String, PrepError)> = JoinSet::new();
        for (id, mut child) in children.into_iter() {
            set.spawn_local(async move {
                let err = verify_tunnel_alive(&id, &mut child)
                    .await
                    .expect_err("dying child must surface TunnelFailed");
                (id, err)
            });
        }

        let mut out = Vec::with_capacity(N);
        while let Some(joined) = set.join_next().await {
            out.push(joined.expect("watcher panicked"));
        }
        out
    }));

    assert_eq!(outcomes.len(), N);
    for (id, err) in outcomes {
        match err {
            PrepError::TunnelFailed {
                secondary_id,
                stderr,
                ..
            } => {
                assert_eq!(secondary_id, id);
                // Each child's stderr MUST contain its own
                // marker — pre-fix `last_mut()` could pull
                // sibling stderr instead.
                let idx: usize = id
                    .strip_prefix("secondary-")
                    .and_then(|s| s.parse().ok())
                    .expect("id parses");
                let expected = format!("MARK-{idx}");
                assert_eq!(
                    stderr, expected,
                    "watcher {id} got cross-attributed stderr {stderr:?}"
                );
            }
            other => panic!("expected TunnelFailed, got {other}"),
        }
    }
}
