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

impl NamedSocketRunnerEnd {
    /// Connect to a named socket at the given path.
    pub async fn connect(socket_path: &Path) -> std::io::Result<Self> {
        let stream = UnixStream::connect(socket_path).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        Ok(Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        })
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

            // Send Done
            runner
                .send(Response::Done {
                    result_data: Some(b"5:3".to_vec()),
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
            })
            .await
            .unwrap();

        // Receive Done
        let resp = manager.recv().await.unwrap();
        match resp {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"5:3");
            }
            _ => panic!("expected Done"),
        }

        runner_handle.await.unwrap();

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
        }).await;
    }
}
