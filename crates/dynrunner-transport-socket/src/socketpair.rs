use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response, codec};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Create a Unix socketpair. Returns the manager-side transport and the raw FD
/// for the child process. The child FD should be passed to the subprocess via
/// `--dynamic_queue <fd>` and kept open via `pass_fds`.
///
/// # Safety
/// The returned `child_fd` is a valid open file descriptor that must be passed
/// to the child process. The caller is responsible for closing it if the child
/// is not spawned.
pub fn create_socketpair() -> std::io::Result<(SocketpairManagerEnd, RawFd)> {
    let (parent_fd, child_fd) =
        nix::sys::socket::socketpair(
            nix::sys::socket::AddressFamily::Unix,
            nix::sys::socket::SockType::Stream,
            None,
            nix::sys::socket::SockFlag::empty(),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Convert OwnedFd to RawFd, transferring ownership
    let parent_raw: RawFd = parent_fd.into_raw_fd();
    let child_raw: RawFd = child_fd.into_raw_fd();

    // Convert the parent FD to an async UnixStream
    let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(parent_raw) };
    std_stream.set_nonblocking(true)?;
    let stream = UnixStream::from_std(std_stream)?;

    let (read_half, write_half) = tokio::io::split(stream);
    let reader = BufReader::new(read_half);

    Ok((
        SocketpairManagerEnd {
            reader,
            writer: write_half,
        },
        child_raw,
    ))
}

/// Manager-side transport over a Unix socketpair.
pub struct SocketpairManagerEnd {
    reader: BufReader<tokio::io::ReadHalf<UnixStream>>,
    writer: tokio::io::WriteHalf<UnixStream>,
}

impl MessageSender<Command> for SocketpairManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        let bytes = codec::serialize_command(&msg);
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        self.writer.flush().await.map_err(|e| e.to_string())
    }
}

impl MessageReceiver<Response> for SocketpairManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(0) => None, // EOF
            Ok(_) => codec::parse_response(&line),
            Err(_) => None,
        }
    }

}

/// Runner-side transport over a Unix socketpair.
///
/// Constructed from a raw FD inherited from the parent process.
pub struct SocketpairRunnerEnd {
    reader: BufReader<tokio::io::ReadHalf<UnixStream>>,
    writer: tokio::io::WriteHalf<UnixStream>,
}

impl SocketpairRunnerEnd {
    /// Create from an inherited raw file descriptor.
    ///
    /// # Safety
    /// The `fd` must be a valid, open file descriptor for a Unix socket
    /// inherited from the parent process.
    pub unsafe fn from_raw_fd(fd: RawFd) -> std::io::Result<Self> {
        let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
        std_stream.set_nonblocking(true)?;
        let stream = UnixStream::from_std(std_stream)?;

        let (read_half, write_half) = tokio::io::split(stream);
        let reader = BufReader::new(read_half);

        Ok(Self {
            reader,
            writer: write_half,
        })
    }
}

impl MessageReceiver<Command> for SocketpairRunnerEnd {
    async fn recv(&mut self) -> Option<Command> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(0) => None, // EOF
            Ok(_) => codec::parse_command(&line),
            Err(_) => None,
        }
    }
}

impl MessageSender<Response> for SocketpairRunnerEnd {
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

    #[tokio::test]
    async fn socketpair_command_roundtrip() {
        let (mut manager, child_fd) = create_socketpair().unwrap();
        let mut runner = unsafe { SocketpairRunnerEnd::from_raw_fd(child_fd).unwrap() };

        manager
            .send(Command::ProcessTask { relative_path: "test/bin".into(), payload: None })
            .await
            .unwrap();

        let cmd = runner.recv().await.unwrap();
        match cmd {
            Command::ProcessTask { relative_path, .. } => {
                assert_eq!(relative_path, "test/bin");
            }
            _ => panic!("expected ProcessTask"),
        }
    }

    #[tokio::test]
    async fn socketpair_response_roundtrip() {
        let (mut manager, child_fd) = create_socketpair().unwrap();
        let mut runner = unsafe { SocketpairRunnerEnd::from_raw_fd(child_fd).unwrap() };

        runner
            .send(Response::Done {
                result_data: Some(b"1:2".to_vec()),
            })
            .await
            .unwrap();

        let resp = manager.recv().await.unwrap();
        match resp {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"1:2");
            }
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn socketpair_stop_command() {
        let (mut manager, child_fd) = create_socketpair().unwrap();
        let mut runner = unsafe { SocketpairRunnerEnd::from_raw_fd(child_fd).unwrap() };

        manager.send(Command::Stop).await.unwrap();

        let cmd = runner.recv().await.unwrap();
        assert!(matches!(cmd, Command::Stop));
    }

    #[tokio::test]
    async fn socketpair_ready_then_done() {
        let (mut manager, child_fd) = create_socketpair().unwrap();
        let mut runner = unsafe { SocketpairRunnerEnd::from_raw_fd(child_fd).unwrap() };

        // Runner sends Ready
        runner.send(Response::Ready).await.unwrap();
        let resp = manager.recv().await.unwrap();
        assert!(matches!(resp, Response::Ready));

        // Manager sends task
        manager
            .send(Command::ProcessTask { relative_path: "a/b".into(), payload: None })
            .await
            .unwrap();

        let cmd = runner.recv().await.unwrap();
        assert!(matches!(cmd, Command::ProcessTask { .. }));

        // Runner sends Done
        runner
            .send(Response::Done {
                result_data: None,
            })
            .await
            .unwrap();
        let resp = manager.recv().await.unwrap();
        assert!(matches!(resp, Response::Done { .. }));
    }
}
