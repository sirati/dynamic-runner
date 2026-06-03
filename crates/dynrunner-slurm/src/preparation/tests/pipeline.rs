//! Lifecycle tests on the public `SlurmPreparation` surface: the
//! outer-timeout state machine when no secondary's info file appears,
//! `cleanup()` idempotence, and a smoke check that the
//! filesystem-backed `LocalDirReader` honours the
//! `InfoFileReader::read` contract.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::preparation::SlurmPreparation;
use crate::preparation::options::{InfoFileReader, PrepError, PreparationOptions};

use super::opts_for;

/// Reads info files from a real local directory by polling the
/// filesystem — exercises the same control flow the real
/// gateway-backed reader will use, without needing a live SSH
/// gateway in the unit-test ring.
#[derive(Clone)]
struct LocalDirReader;

impl InfoFileReader for LocalDirReader {
    // The trait pins `+ 'static` on the returned future; an
    // `async fn` impl would infer `use<'_>` capturing `&self`
    // and fail to satisfy the bound. Keep the manual shape.
    #[allow(clippy::manual_async_fn)]
    fn read(
        &self,
        path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
        async move {
            match tokio::fs::read_to_string(&path).await {
                Ok(s) => Ok(Some(s)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(PrepError::Io(e)),
            }
        }
    }
}

#[derive(Clone)]
struct StuckReader {
    polls: Arc<AtomicUsize>,
}

impl InfoFileReader for StuckReader {
    fn read(
        &self,
        _path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
        let polls = self.polls.clone();
        async move {
            polls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
}

/// State-machine timeout: 0-of-N secondaries reach ready inside
/// the deadline. Reader returns `None` forever (info file never
/// shows up). Outer timeout fires before any watcher graduates
/// to spawning ssh — clean assertion path with no real subprocess
/// involvement.
///
/// Real ssh -R coverage (the spawn → verify path) lives in
/// the e2e suite, which has a real gateway.
#[test]
fn timeout_when_no_secondary_ready() {
    let tmp = tempfile::tempdir().unwrap();

    let opts = opts_for(&tmp);
    let polls = Arc::new(AtomicUsize::new(0));
    let reader = StuckReader {
        polls: polls.clone(),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let result: Result<HashMap<String, u16>, PrepError> =
        rt.block_on(local.run_until(async move {
            let prep = SlurmPreparation::new(opts);
            let r = prep.setup_ssh_tunnels(reader, 2, 9999).await;
            prep.cleanup().await;
            r
        }));

    match result {
        Ok(m) => panic!("expected setup to time out, got map={m:?}"),
        Err(PrepError::Timeout { ready, total }) => {
            assert_eq!(ready, 0);
            assert_eq!(total, 2);
        }
        Err(other) => panic!("unexpected error class: {other}"),
    }
    // Each of the 2 watchers must have polled multiple times
    // within the 1500ms deadline at 20ms cadence — minimum a
    // few polls per watcher.
    assert!(
        polls.load(Ordering::SeqCst) >= 4,
        "expected >=4 polls, got {}",
        polls.load(Ordering::SeqCst)
    );
}

/// Cleanup is idempotent: calling twice doesn't panic, second
/// call is a no-op. This exercises the `drain(..)` pattern.
#[test]
fn cleanup_is_idempotent() {
    let opts = PreparationOptions::new("/tmp".into(), "h".into(), None, 22, vec![], vec![]);
    let prep = SlurmPreparation::new(opts);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        prep.cleanup().await;
        prep.cleanup().await;
    });
}

/// Ssh spawn argv shape (no auth-options): -J jump_target form,
/// extra_port_forwards fan out, ExitOnForwardFailure present.
/// LocalDirReader smoke test: when the file exists, the reader
/// returns Some(content); when it doesn't, it returns None.
/// Sanity-check on the IO bridge the timeout test relies on.
#[test]
fn local_dir_reader_resolves_existing_and_missing_paths() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let present = tmp.path().join("present.info");
    std::fs::write(&present, "tcp://h:1234\n").unwrap();
    let absent = tmp.path().join("absent.info");
    let reader = LocalDirReader;
    let got = rt
        .block_on(reader.read(present.display().to_string()))
        .unwrap();
    assert_eq!(got.as_deref(), Some("tcp://h:1234\n"));
    let none = rt
        .block_on(reader.read(absent.display().to_string()))
        .unwrap();
    assert!(none.is_none());
}
