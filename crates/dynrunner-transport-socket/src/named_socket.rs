use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response, codec};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Process-global monotonic counter handing each [`NamedSocketManagerEnd::bind`]
/// a value no other live bind in this process shares. Combined with the
/// process pid it makes every bound socket file name unique per bind — the
/// load-bearing invariant behind the respawn-unlink fix (see `bind`).
static BIND_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Manager-side transport over a named Unix domain socket.
///
/// The manager binds a socket file, waits for the worker to connect,
/// and then communicates over the accepted connection.
pub struct NamedSocketManagerEnd {
    socket_path: PathBuf,
    listener: UnixListener,
    connection: Option<AcceptedConnection>,
}

struct AcceptedConnection {
    reader: BufReader<tokio::io::ReadHalf<UnixStream>>,
    writer: tokio::io::WriteHalf<UnixStream>,
}

impl NamedSocketManagerEnd {
    /// Bind a new named socket derived from `requested_path`.
    ///
    /// The endpoint owns its on-disk filename: this binds a
    /// **per-bind-unique sibling** of `requested_path` (the requested
    /// stem with a `.<pid>.<generation>` suffix interposed before the
    /// extension), not `requested_path` itself. That uniqueness is the
    /// fix for the worker-respawn unlink race: a respawn that rebinds
    /// the same logical `worker_<id>` slot used to land on the SAME
    /// path, so the dropped prior endpoint's [`Drop`] (which unlinks
    /// `self.socket_path`) deleted the freshly-bound socket out from
    /// under the new worker. Under a `current_thread` runtime there is
    /// no `.await` yield window between the new bind and the old drop,
    /// so the deletion was deterministic. Giving each bind its own path
    /// means a dropped endpoint's unlink can only ever target the path
    /// IT bound — never a newer endpoint's.
    ///
    /// Callers must read the actual bound path back via
    /// [`Self::socket_path`] (e.g. to hand it to the worker's argv); it
    /// is NOT equal to `requested_path`.
    pub fn bind(requested_path: &Path) -> std::io::Result<Self> {
        let socket_path = Self::unique_sibling(requested_path);

        // Create parent directories. (Derived from `requested_path` so
        // the per-bind suffix never affects which directory we target.)
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Defensive: a per-bind-unique path effectively never pre-exists,
        // but a crashed prior run that reused this pid+generation could
        // leave a stale file. Clear it so `bind` does not fail with
        // EADDRINUSE on the leftover.
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let listener = UnixListener::bind(&socket_path)?;

        Ok(Self {
            socket_path,
            listener,
            connection: None,
        })
    }

    /// Derive a per-bind-unique sibling path from `requested_path` by
    /// interposing a `.<pid>.<generation>` token before the file
    /// extension (or appending it when there is no extension). The
    /// `<generation>` comes from a process-global monotonic counter, so
    /// two binds in the same process can never collide; the `<pid>`
    /// disambiguates across processes that share a socket directory.
    ///
    /// Example: `…/worker_3.sock` → `…/worker_3.<pid>.<gen>.sock`.
    fn unique_sibling(requested_path: &Path) -> PathBuf {
        let generation = BIND_GENERATION.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let token = format!("{pid}.{generation}");

        let file_name = requested_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let unique_name = match file_name.rsplit_once('.') {
            // `stem.ext` → `stem.<token>.ext`. `rsplit_once` keeps the
            // extension on the last dot, matching the example above and
            // preserving any `.sock` suffix consumers may match on.
            Some((stem, ext)) if !stem.is_empty() => format!("{stem}.{token}.{ext}"),
            // No usable extension boundary (no dot, or a leading-dot
            // dotfile like `.sock`): append the token outright.
            _ => format!("{file_name}.{token}"),
        };

        match requested_path.parent() {
            Some(parent) => parent.join(unique_name),
            None => PathBuf::from(unique_name),
        }
    }

    /// Wait for a worker to connect. Must be called before sending/receiving.
    pub async fn accept(&mut self) -> std::io::Result<()> {
        let (stream, _addr) = self.listener.accept().await?;
        let (read_half, write_half) = tokio::io::split(stream);
        self.connection = Some(AcceptedConnection {
            reader: BufReader::new(read_half),
            writer: write_half,
        });
        Ok(())
    }

    /// Get the socket path.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for NamedSocketManagerEnd {
    fn drop(&mut self) {
        // Clean up socket file
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl MessageSender<Command> for NamedSocketManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        let conn = self
            .connection
            .as_mut()
            .ok_or_else(|| "No connection established".to_owned())?;
        let bytes = codec::serialize_command(&msg);
        conn.writer
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        conn.writer.flush().await.map_err(|e| e.to_string())
    }
}

impl MessageReceiver<Response> for NamedSocketManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        let conn = self.connection.as_mut()?;
        let mut line = String::new();
        match conn.reader.read_line(&mut line).await {
            Ok(0) => None,
            Ok(_) => codec::parse_response(&line),
            Err(_) => None,
        }
    }
}

/// Runner-side transport that connects to a named Unix domain socket.
pub struct NamedSocketRunnerEnd {
    reader: BufReader<tokio::io::ReadHalf<UnixStream>>,
    writer: tokio::io::WriteHalf<UnixStream>,
}

/// Maximum time the runner waits for the manager to bind its socket file
/// before giving up. Mirrors Python `NamedSocketInterface._setup_client`.
const CONNECT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Polling interval used while waiting for the socket file to appear.
const CONNECT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

impl NamedSocketRunnerEnd {
    /// Connect to a named socket at the given path.
    ///
    /// Polls for the socket file to appear (manager binds asynchronously) for
    /// up to [`CONNECT_WAIT_TIMEOUT`], then performs the connect. Returns an
    /// `ErrorKind::TimedOut` error if the file never appears.
    pub async fn connect(socket_path: &Path) -> std::io::Result<Self> {
        Self::wait_for_socket(socket_path, CONNECT_WAIT_TIMEOUT).await?;
        let stream = UnixStream::connect(socket_path).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        Ok(Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        })
    }

    /// Block until the socket file at `socket_path` exists, or `timeout`
    /// elapses. Cancel-safe: only awaits on `tokio::time::sleep`, which
    /// is itself cancel-safe. Exposed at crate-private visibility so
    /// tests can drive it with a short deadline.
    async fn wait_for_socket(
        socket_path: &Path,
        timeout: std::time::Duration,
    ) -> std::io::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if socket_path.exists() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Socket file {} did not appear within {}s",
                        socket_path.display(),
                        timeout.as_secs()
                    ),
                ));
            }
            tokio::time::sleep(CONNECT_POLL_INTERVAL).await;
        }
    }
}

impl MessageReceiver<Command> for NamedSocketRunnerEnd {
    async fn recv(&mut self) -> Option<Command> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(0) => None,
            Ok(_) => codec::parse_command(&line),
            Err(_) => None,
        }
    }
}

impl MessageSender<Response> for NamedSocketRunnerEnd {
    async fn send(&mut self, msg: Response) -> Result<(), String> {
        let bytes = codec::serialize_response(&msg);
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        self.writer.flush().await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn named_socket_roundtrip() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let dir = std::env::temp_dir().join(format!("db_test_{}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                let sock_path = dir.join("test.sock");

                let mut manager = NamedSocketManagerEnd::bind(&sock_path).unwrap();

                // Spawn a runner that connects. `bind` owns the on-disk
                // filename and returns a per-bind-unique sibling of the
                // requested path, so the runner must connect to the path
                // the manager actually bound — not `sock_path`.
                let sock_path_clone = manager.socket_path().to_owned();
                let runner_handle = tokio::task::spawn_local(async move {
                    let mut runner = NamedSocketRunnerEnd::connect(&sock_path_clone)
                        .await
                        .unwrap();

                    // Send Ready
                    runner.send(Response::Ready).await.unwrap();

                    // Receive command
                    let cmd = runner.recv().await.unwrap();
                    assert!(matches!(cmd, Command::ProcessTask { .. }));

                    // Send Done with an opaque byte payload. The framework
                    // treats `result_data` as fully opaque (see codec.rs):
                    // the bytes here are arbitrary and chosen to NOT look
                    // like any plausible int-pair shape, so this test
                    // verifies opaque-bytes round-trip and not accidental
                    // legacy-int parsing.
                    runner
                        .send(Response::Done {
                            result_data: Some(b"opaque-payload-bytes".to_vec()),
                        })
                        .await
                        .unwrap();
                });

                // Accept connection
                manager.accept().await.unwrap();

                // Receive Ready
                let resp = manager.recv().await.unwrap();
                assert!(matches!(resp, Response::Ready));

                // Send command
                manager
                    .send(Command::ProcessTask {
                        relative_path: "x/y".into(),
                        payload: None,
                        resolved_path: None,
                        predecessor_outputs: std::collections::BTreeMap::new(),
                    })
                    .await
                    .unwrap();

                // Receive Done
                let resp = manager.recv().await.unwrap();
                match resp {
                    Response::Done { result_data } => {
                        assert_eq!(result_data.unwrap(), b"opaque-payload-bytes");
                    }
                    _ => panic!("expected Done"),
                }

                runner_handle.await.unwrap();

                // Cleanup
                let _ = std::fs::remove_dir_all(&dir);
            })
            .await;
    }

    /// End-to-end: the runner-side `connect` must reach a manager that
    /// binds late. The manager binds 1s after the task starts, then
    /// hands its actual bound path to the runner (mirroring production,
    /// where the worker learns the path from its argv only after the
    /// manager has bound). `connect` then completes the round-trip.
    ///
    /// Note: `bind` owns its on-disk filename and returns a per-bind-
    /// unique sibling of the requested path, so the runner connects to
    /// `manager.socket_path()`, delivered over a `oneshot`. The
    /// wait-loop's appear-after-delay branch is covered in isolation by
    /// `wait_for_socket_returns_when_socket_appears_late`.
    #[tokio::test(flavor = "current_thread")]
    async fn runner_connects_to_late_bound_manager() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let dir = std::env::temp_dir().join(format!("db_test_late_{}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                let requested = dir.join("late.sock");

                let (path_tx, path_rx) = tokio::sync::oneshot::channel::<PathBuf>();

                // Manager binds 1s after the task starts, then publishes
                // the path it actually bound so the runner can connect.
                let manager_path = requested.clone();
                let manager_handle = tokio::task::spawn_local(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let mut manager = NamedSocketManagerEnd::bind(&manager_path).unwrap();
                    path_tx.send(manager.socket_path().to_owned()).unwrap();
                    manager.accept().await.unwrap();
                    // Hold the connection open until the runner closes.
                    let _ = manager.recv().await;
                });

                let start = std::time::Instant::now();
                let bound_path = path_rx.await.unwrap();
                let runner = NamedSocketRunnerEnd::connect(&bound_path).await;
                let elapsed = start.elapsed();
                assert!(runner.is_ok(), "runner connect failed: {:?}", runner.err());
                // Sanity check: the bind genuinely happened late (>= ~1s)
                // but the round-trip didn't burn an absurd amount of time.
                assert!(
                    elapsed >= std::time::Duration::from_millis(900),
                    "connect returned too fast ({:?}); late-bind path skipped",
                    elapsed
                );
                assert!(
                    elapsed < std::time::Duration::from_secs(10),
                    "connect took unexpectedly long: {:?}",
                    elapsed
                );

                // Drop the runner so the manager's recv unblocks.
                drop(runner);
                manager_handle.await.unwrap();

                let _ = std::fs::remove_dir_all(&dir);
            })
            .await;
    }

    /// The `wait_for_socket` helper must return `Ok` once the socket
    /// file appears, even when it appears after a delay (mirrors Python
    /// `_setup_client`'s poll). A spawned task creates the file ~300ms
    /// in; the helper started polling against a not-yet-existing path,
    /// so a successful return proves the appear-branch of the loop fired
    /// rather than an immediate ENOENT bail-out.
    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_socket_returns_when_socket_appears_late() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let dir =
                    std::env::temp_dir().join(format!("db_test_appear_{}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                let sock_path = dir.join("appears.sock");
                let _ = std::fs::remove_file(&sock_path);

                let create_path = sock_path.clone();
                let creator = tokio::task::spawn_local(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    std::fs::write(&create_path, b"").unwrap();
                });

                let start = std::time::Instant::now();
                let result = NamedSocketRunnerEnd::wait_for_socket(
                    &sock_path,
                    std::time::Duration::from_secs(5),
                )
                .await;
                let elapsed = start.elapsed();

                assert!(
                    result.is_ok(),
                    "wait_for_socket errored: {:?}",
                    result.err()
                );
                assert!(
                    elapsed >= std::time::Duration::from_millis(250),
                    "returned before the file could have appeared: {:?}",
                    elapsed
                );
                assert!(
                    elapsed < std::time::Duration::from_secs(2),
                    "wait_for_socket overshot the late-create window: {:?}",
                    elapsed
                );

                creator.await.unwrap();
                let _ = std::fs::remove_dir_all(&dir);
            })
            .await;
    }

    /// If the socket file never appears, the wait helper must surface
    /// a TimedOut error rather than hang forever. We invoke the helper
    /// directly with a short timeout so the test runs quickly.
    #[tokio::test(flavor = "current_thread")]
    async fn runner_times_out_when_socket_never_appears() {
        let sock_path =
            std::env::temp_dir().join(format!("db_test_missing_{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock_path);

        let start = std::time::Instant::now();
        let result = NamedSocketRunnerEnd::wait_for_socket(
            &sock_path,
            std::time::Duration::from_millis(300),
        )
        .await;
        let elapsed = start.elapsed();

        let err = result.expect_err("expected TimedOut error");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        // Should have waited roughly the timeout, not bailed instantly
        // and not hung past it.
        assert!(
            elapsed >= std::time::Duration::from_millis(250),
            "timeout fired too early: {:?}",
            elapsed
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "timeout exceeded budget: {:?}",
            elapsed
        );
    }

    /// `bind` must NOT bind the requested path verbatim: it interposes a
    /// per-bind token before the extension, keeping the parent directory
    /// and the `.sock` extension while making the filename unique.
    /// (`bind` constructs a tokio `UnixListener`, so a reactor is
    /// required.)
    #[tokio::test(flavor = "current_thread")]
    async fn bind_uses_unique_sibling_of_requested_path() {
        let dir = std::env::temp_dir().join(format!("db_test_unique_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let requested = dir.join("worker_7.sock");

        let manager = NamedSocketManagerEnd::bind(&requested).unwrap();
        let bound = manager.socket_path().to_owned();

        assert_ne!(
            bound, requested,
            "bind must not use the requested path verbatim"
        );
        assert_eq!(
            bound.parent(),
            requested.parent(),
            "must stay in the requested dir"
        );
        assert_eq!(
            bound.extension().and_then(|e| e.to_str()),
            Some("sock"),
            "the .sock extension must be preserved"
        );
        let name = bound.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with("worker_7.") && name.ends_with(".sock"),
            "bound filename should be a sibling of the requested stem: {name}"
        );
        assert!(bound.exists(), "the bound socket file must exist on disk");

        drop(manager);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Respawn-unlink regression pin (the bug this fix targets), at the
    /// transport boundary and without any Python worker.
    ///
    /// Two endpoints bound from the SAME requested path get DISTINCT
    /// on-disk files. Dropping the FIRST (the "prior" endpoint a
    /// worker-respawn replaces) runs its unlink, which must NOT remove
    /// the SECOND ("new") endpoint's file — exactly the deletion that
    /// previously vanished the freshly-bound socket out from under the
    /// respawned worker under the no-yield `current_thread` runtime.
    /// (`bind` constructs a tokio `UnixListener`, so a reactor is
    /// required.)
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_prior_endpoint_does_not_unlink_a_later_bind() {
        let dir = std::env::temp_dir().join(format!("db_test_respawn_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let requested = dir.join("worker_0.sock");

        // "Prior" endpoint (the one a respawn drops).
        let prior = NamedSocketManagerEnd::bind(&requested).unwrap();
        let prior_path = prior.socket_path().to_owned();

        // "New" endpoint binds the same logical slot.
        let fresh = NamedSocketManagerEnd::bind(&requested).unwrap();
        let fresh_path = fresh.socket_path().to_owned();

        assert_ne!(
            prior_path, fresh_path,
            "two binds of the same requested path must not collide"
        );
        assert!(prior_path.exists());
        assert!(fresh_path.exists());

        // Drop the prior endpoint — its Drop unlinks ONLY its own path.
        drop(prior);

        assert!(
            !prior_path.exists(),
            "prior endpoint's Drop should have unlinked its own file"
        );
        assert!(
            fresh_path.exists(),
            "the freshly-bound socket must survive the prior endpoint's Drop \
             (this is the respawn-unlink race the fix closes)"
        );

        drop(fresh);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
