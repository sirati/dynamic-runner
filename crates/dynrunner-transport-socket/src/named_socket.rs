use std::path::{Path, PathBuf};

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response, codec};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

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
    /// Bind a new named socket at the given path.
    pub fn bind(socket_path: &Path) -> std::io::Result<Self> {
        // Remove existing socket file
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }

        // Create parent directories
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(socket_path)?;

        Ok(Self {
            socket_path: socket_path.to_owned(),
            listener,
            connection: None,
        })
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
        local.run_until(async {
        let dir = std::env::temp_dir().join(format!("db_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock_path = dir.join("test.sock");

        let mut manager = NamedSocketManagerEnd::bind(&sock_path).unwrap();

        // Spawn a runner that connects
        let sock_path_clone = sock_path.clone();
        let runner_handle = tokio::task::spawn_local(async move {
            let mut runner = NamedSocketRunnerEnd::connect(&sock_path_clone).await.unwrap();

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
        }).await;
    }

    /// The runner-side `connect` must wait for the manager to bind its
    /// socket file (mirrors Python `_setup_client`'s 30s poll). Here we
    /// delay the manager's bind by 1s and assert that `connect` still
    /// completes successfully — i.e. the wait-loop kicked in instead of
    /// returning ECONNREFUSED/ENOENT immediately.
    #[tokio::test(flavor = "current_thread")]
    async fn runner_waits_for_late_socket() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let dir = std::env::temp_dir()
                    .join(format!("db_test_late_{}", std::process::id()));
                std::fs::create_dir_all(&dir).unwrap();
                let sock_path = dir.join("late.sock");
                // Make sure no stale file exists.
                let _ = std::fs::remove_file(&sock_path);

                // Manager binds 1s after the runner starts polling.
                let manager_path = sock_path.clone();
                let manager_handle = tokio::task::spawn_local(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let mut manager =
                        NamedSocketManagerEnd::bind(&manager_path).unwrap();
                    manager.accept().await.unwrap();
                    // Hold the connection open until the runner closes.
                    let _ = manager.recv().await;
                });

                let start = std::time::Instant::now();
                let runner = NamedSocketRunnerEnd::connect(&sock_path).await;
                let elapsed = start.elapsed();
                assert!(runner.is_ok(), "runner connect failed: {:?}", runner.err());
                // Sanity check: the wait actually happened (>= ~1s) but
                // didn't burn the full 30s timeout.
                assert!(
                    elapsed >= std::time::Duration::from_millis(900),
                    "connect returned too fast ({:?}); wait loop likely skipped",
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

    /// If the socket file never appears, the wait helper must surface
    /// a TimedOut error rather than hang forever. We invoke the helper
    /// directly with a short timeout so the test runs quickly.
    #[tokio::test(flavor = "current_thread")]
    async fn runner_times_out_when_socket_never_appears() {
        let sock_path = std::env::temp_dir().join(format!(
            "db_test_missing_{}.sock",
            std::process::id()
        ));
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
}
