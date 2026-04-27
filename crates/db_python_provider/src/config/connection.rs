use std::path::PathBuf;

/// Connection mode for worker communication.
#[derive(Clone, Debug)]
pub(crate) enum ConnectionMode {
    /// Anonymous Unix socketpair — FD is passed to child process.
    Socketpair,
    /// Named Unix domain socket — socket path is passed to child process.
    Named {
        socket_dir: PathBuf,
    },
}
