use db_comm_api_base::{MessageReceiver, MessageSender};
use db_manager_runner_comm::{Command, Response};
use db_transport_socket::named_socket::NamedSocketManagerEnd;
use db_transport_socket::socketpair::SocketpairManagerEnd;

// ── EitherManagerEnd: unified transport for socketpair + named socket ──

/// A manager-side transport endpoint that works with either socketpair or named
/// socket connections. Named sockets require an async `accept()` before
/// communication, which is performed lazily on the first `recv_responses` call.
pub(crate) enum EitherManagerEnd {
    Socketpair(SocketpairManagerEnd),
    /// Named socket — `Option` holds it until accept is called; after accept
    /// it stays `Some` (the accept mutates the inner state to have a connection).
    Named {
        inner: NamedSocketManagerEnd,
        accepted: bool,
    },
}

impl MessageSender<Command> for EitherManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.send(msg).await,
            EitherManagerEnd::Named { inner, accepted } => {
                if !*accepted {
                    return Err("Named socket: connection not yet accepted".into());
                }
                inner.send(msg).await
            }
        }
    }
}

impl MessageReceiver<Response> for EitherManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.recv().await,
            EitherManagerEnd::Named { inner, accepted } => {
                // Lazy accept: on first recv, wait for the worker to connect
                if !*accepted {
                    match inner.accept().await {
                        Ok(()) => {
                            *accepted = true;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "named socket accept failed");
                            return None;
                        }
                    }
                }
                inner.recv().await
            }
        }
    }
}
