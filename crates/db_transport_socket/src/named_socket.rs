use std::path::{Path, PathBuf};

use db_comm_api_base::{
    Command, CommandReceiver, CommandSender, Response, ResponseReceiver, ResponseSender,
};
use db_manager_runner_comm::codec;
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

impl CommandSender for NamedSocketManagerEnd {
    async fn send_command(&mut self, command: Command) -> Result<(), String> {
        let conn = self
            .connection
            .as_mut()
            .ok_or_else(|| "No connection established".to_owned())?;
        let bytes = codec::serialize_command(&command);
        conn.writer
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        conn.writer.flush().await.map_err(|e| e.to_string())
    }
}

impl ResponseReceiver for NamedSocketManagerEnd {
    async fn recv_responses(&mut self) -> Vec<Response> {
        let conn = match self.connection.as_mut() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let mut responses = Vec::new();
        let mut line = String::new();

        match conn.reader.read_line(&mut line).await {
            Ok(0) => return responses,
            Ok(_) => {
                if let Some(resp) = codec::parse_response(&line) {
                    responses.push(resp);
                }
            }
            Err(_) => return responses,
        }

        // Drain buffered
        loop {
            line.clear();
            let buf = conn.reader.buffer();
            if buf.is_empty() || !buf.contains(&b'\n') {
                break;
            }
            match conn.reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(resp) = codec::parse_response(&line) {
                        responses.push(resp);
                    }
                }
                Err(_) => break,
            }
        }

        responses
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

impl CommandReceiver for NamedSocketRunnerEnd {
    async fn recv_command(&mut self) -> Option<Command> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(0) => None,
            Ok(_) => codec::parse_command(&line),
            Err(_) => None,
        }
    }
}

impl ResponseSender for NamedSocketRunnerEnd {
    async fn send_response(&mut self, response: Response) -> Result<(), String> {
        let bytes = codec::serialize_response(&response);
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

    #[tokio::test]
    async fn named_socket_roundtrip() {
        let dir = std::env::temp_dir().join(format!("db_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock_path = dir.join("test.sock");

        let mut manager = NamedSocketManagerEnd::bind(&sock_path).unwrap();

        // Spawn a runner that connects
        let sock_path_clone = sock_path.clone();
        let runner_handle = tokio::spawn(async move {
            let mut runner = NamedSocketRunnerEnd::connect(&sock_path_clone).await.unwrap();

            // Send Ready
            runner.send_response(Response::Ready).await.unwrap();

            // Receive command
            let cmd = runner.recv_command().await.unwrap();
            assert!(matches!(cmd, Command::ProcessBinary { .. }));

            // Send Done
            runner
                .send_response(Response::Done {
                    warnings: 5,
                    filtered: 3,
                })
                .await
                .unwrap();
        });

        // Accept connection
        manager.accept().await.unwrap();

        // Receive Ready
        let responses = manager.recv_responses().await;
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], Response::Ready));

        // Send command
        manager
            .send_command(Command::ProcessBinary {
                relative_path: "x/y".into(),
            })
            .await
            .unwrap();

        // Receive Done
        let responses = manager.recv_responses().await;
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::Done { warnings, filtered } => {
                assert_eq!(*warnings, 5);
                assert_eq!(*filtered, 3);
            }
            _ => panic!("expected Done"),
        }

        runner_handle.await.unwrap();

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }
}
