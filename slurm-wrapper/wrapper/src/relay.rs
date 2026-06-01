//! Single concern: the FIFO command-relay service (generate.rs:645-693).
//! HARD external contract — the response-line format
//! `output_N.sock,exit_N.sock,signal_N.sock,<pid>` and the per-command
//! socket naming are consumed by an out-of-repo client; reproduced
//! byte-for-byte here.
//!
//! Faithful port of the bash relay subshell:
//!   * `rm -f` + `mkfifo` cmd.sock and cmd.sock.response (generate.rs:651-653)
//!   * a `while true` loop reading one command line per iteration (:654-691)
//!   * per command: mkfifo output/exit/signal sockets, run the command with
//!     stdout+stderr to the output FIFO, report exit code via the exit FIFO,
//!     watch the signal FIFO to forward a kill, and write the response line
//!     (:656-678)
//!   * an ERROR/exit-1 branch when the cmd FIFO unexpectedly vanishes (:680-689)
//!
//! `shutdown` mirrors the wrapper trap's `kill -TERM $CMD_RELAY_PID; wait`
//! (generate.rs:404-407): it stops the loop and awaits its drain.

use crate::dirs::Layout;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nix::fcntl::{open, OFlag};
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::Mode;
use nix::unistd::{mkfifo, Pid};

/// Handle to the running relay task. Owns the join handle for the relay
/// loop plus the FIFO paths and a shutdown flag used to unblock a pending
/// FIFO read on teardown.
pub struct RelayHandle {
    task: tokio::task::JoinHandle<()>,
    cmd_socket: PathBuf,
    response_socket: PathBuf,
    shutdown: Arc<AtomicBool>,
}

/// `mkfifo` the command socket + response FIFO and spawn the relay loop
/// as a background task (generate.rs:651-693).
pub fn spawn(layout: &Layout) -> std::io::Result<RelayHandle> {
    let cmd_socket = layout.cmd_socket.clone();
    let response_socket = response_path(&cmd_socket);
    let socket_dir = layout.socket_dir.clone();

    // rm -f "<cmd_socket>" "<cmd_socket>.response"  (generate.rs:651)
    let _ = std::fs::remove_file(&cmd_socket);
    let _ = std::fs::remove_file(&response_socket);

    // mkfifo both, mode 0o666 (generate.rs:652-653).
    make_fifo(&cmd_socket)?;
    make_fifo(&response_socket)?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let loop_shutdown = Arc::clone(&shutdown);
    let loop_cmd_socket = cmd_socket.clone();
    let loop_response = response_socket.clone();

    let task = tokio::spawn(async move {
        relay_loop(loop_cmd_socket, loop_response, socket_dir, loop_shutdown).await;
    });

    Ok(RelayHandle {
        task,
        cmd_socket,
        response_socket,
        shutdown,
    })
}

impl RelayHandle {
    /// Terminate the relay and wait for it to drain (generate.rs:404-407).
    /// Best-effort: never panics.
    pub async fn shutdown(self) {
        // Set the cooperative stop flag. The loop waits on the cmd FIFO with a
        // bounded `poll` timeout and rechecks the flag each wakeup, so it
        // observes the flag and returns within one poll interval without any
        // writer rendezvous (the bash trap's `kill -TERM` interrupts `read`;
        // the bounded poll is the deadlock-free equivalent here).
        self.shutdown.store(true, Ordering::SeqCst);

        // Await the loop draining (mirror `wait`). Aborting is the fallback so
        // a stuck loop can never wedge teardown; the abort only affects the
        // async task, not any in-flight per-command child (those are detached,
        // matching the bash backgrounded `{ ... } &` subshells).
        let aborter = self.task.abort_handle();
        if tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .is_err()
        {
            aborter.abort();
        }

        // The FIFOs are torn down by the out-of-cgroup shutdown manager /
        // scratch cleanup, matching the bash lifecycle where the trap only
        // stops the relay and the socket FIFOs vanish with the scratch tree.
        let _ = &self.cmd_socket;
        let _ = &self.response_socket;
    }
}

/// The `while true` relay loop (generate.rs:654-691). `counter` mirrors the
/// bash `SOCKET_COUNTER` starting at 0 and incrementing before each accepted
/// command.
async fn relay_loop(
    cmd_socket: PathBuf,
    response_socket: PathBuf,
    socket_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
) {
    let mut counter: u64 = 0;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }

        // read -r CMD < "$cmd_socket"  (generate.rs:655). The bash `read`
        // blocks until a writer connects; here a non-blocking open + bounded
        // `poll` does the equivalent while still observing the shutdown flag
        // each poll interval (so teardown never wedges on a writer-less FIFO).
        let read_path = cmd_socket.clone();
        let stop = Arc::clone(&shutdown);
        let outcome = match tokio::task::spawn_blocking(move || read_command(&read_path, &stop))
            .await
        {
            Ok(o) => o,
            // JoinError (e.g. abort during shutdown): stop the loop.
            Err(_) => return,
        };

        if shutdown.load(Ordering::SeqCst) {
            return;
        }

        match outcome {
            ReadOutcome::ShutdownRequested => return,
            // if [ -n "$CMD" ]  (generate.rs:656)
            ReadOutcome::Line(ref c) if !c.is_empty() => {
                counter += 1;
                if let Err(e) = dispatch_command(
                    c,
                    counter,
                    &socket_dir,
                    &response_socket,
                    Arc::clone(&shutdown),
                )
                .await
                {
                    tracing::error!(error = %e, "command relay: failed to dispatch command");
                }
            }
            // Empty line: bash's `[ -n "$CMD" ]` is false and the loop simply
            // iterates again — no else branch fires.
            ReadOutcome::Line(_) => {}
            // read returned EOF/failure: elif [ ! -p "$cmd_socket" ]  (:680)
            ReadOutcome::Eof => {
                if !is_fifo(&cmd_socket) {
                    if shutdown.load(Ordering::SeqCst) {
                        return;
                    }
                    tracing::error!(
                        cmd_socket = %cmd_socket.display(),
                        "ERROR: command relay FIFO disappeared unexpectedly; secondary cannot continue."
                    );
                    return; // mirror `exit 1`
                }
                // FIFO still present but reader saw EOF (writer closed); loop.
            }
        }
    }
}

/// Outcome of one `read_command` attempt.
enum ReadOutcome {
    /// A newline-terminated command line (trailing newline stripped).
    Line(String),
    /// The FIFO reached EOF before any data — maps to the bash `read`
    /// returning non-zero, which routes to the `[ ! -p ... ]` check.
    Eof,
    /// The shutdown flag was observed while waiting; the loop must stop.
    ShutdownRequested,
}

/// How long `poll` waits between shutdown-flag rechecks.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// A cooperative stop condition for the poll-bounded FIFO read. Lets the same
/// read primitive serve both the command loop (stop = relay shutdown) and the
/// per-command signal watcher (stop = relay shutdown OR command finished).
trait Stop: Send + Sync {
    fn should_stop(&self) -> bool;
}

impl Stop for Arc<AtomicBool> {
    fn should_stop(&self) -> bool {
        self.load(Ordering::SeqCst)
    }
}

/// Stop when EITHER flag is set.
struct CombinedStop {
    a: Arc<AtomicBool>,
    b: Arc<AtomicBool>,
}

impl Stop for CombinedStop {
    fn should_stop(&self) -> bool {
        self.a.load(Ordering::SeqCst) || self.b.load(Ordering::SeqCst)
    }
}

/// Per-command dispatch (generate.rs:657-678): mkfifo the three sockets, run
/// the command, wire up exit-code reporting + signal forwarding, and write
/// the response line.
async fn dispatch_command(
    cmd: &str,
    counter: u64,
    socket_dir: &Path,
    response_socket: &Path,
    relay_shutdown: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let output_sock = socket_dir.join(format!("output_{counter}.sock"));
    let exit_sock = socket_dir.join(format!("exit_{counter}.sock"));
    let signal_sock = socket_dir.join(format!("signal_{counter}.sock"));

    // mkfifo "$OUTPUT_SOCK" "$EXIT_SOCK" "$SIGNAL_SOCK"  (generate.rs:661)
    make_fifo(&output_sock)?;
    make_fifo(&exit_sock)?;
    make_fifo(&signal_sock)?;

    // Command child: bash -c with stdout+stderr -> output FIFO, mirroring the
    // bash subshell `{ eval "$CMD" > "$OUTPUT_SOCK" 2>&1; ... } &`. The crucial
    // fidelity point: the FIFO open for write blocks until the CLIENT connects
    // as reader, and that block must happen in the CHILD — not in this dispatch
    // path — so the response line below can be written immediately (the client
    // learns the socket name from the response, then connects). We therefore
    // let bash itself perform the redirect via `exec` inside the child, exactly
    // like the bash subshell does, rather than opening the FIFO in the parent.
    // The reported pid is this bash child's pid: signalling it stops CMD
    // (CMD_PID fidelity note).
    let wrapped = format!(
        "exec > \"$DYNRUNNER_OUTPUT_SOCK\" 2>&1; {cmd}"
    );
    let mut command = tokio::process::Command::new("bash");
    command
        .arg("-c")
        .arg(wrapped)
        .env("DYNRUNNER_OUTPUT_SOCK", &output_sock)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Reset the inherited blocked signal mask before exec so relay user
    // commands get normal signal disposition (the relay's own kill -SIGNAL
    // forwarding targets this child's pid, but the command itself must not
    // start with the wrapper's monitored set blocked).
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        command.pre_exec(crate::signals::child_pre_exec());
    }
    let mut child = command.spawn()?;

    let cmd_pid = child.id().expect("child has a pid before wait") as i32;

    // Per-command "command finished" flag. Set by the completion task once the
    // child exits; the signal-forwarding watcher uses it to stop waiting (a
    // dead pid cannot be signalled). This is also what lets the signal watcher
    // be reclaimed instead of parking a thread forever on the signal FIFO — the
    // bash signal subshell can block indefinitely, but a leaked blocking thread
    // would wedge runtime teardown here.
    let cmd_done = Arc::new(AtomicBool::new(false));

    // Command-completion task (generate.rs:662-668): on exit, rm output FIFO,
    // write "<exit_code>\n" to exit FIFO, rm exit FIFO.
    let output_sock_c = output_sock.clone();
    let exit_sock_c = exit_sock.clone();
    let cmd_done_c = Arc::clone(&cmd_done);
    tokio::spawn(async move {
        let status = child.wait().await;
        cmd_done_c.store(true, Ordering::SeqCst);
        let code = match status {
            Ok(s) => s.code().unwrap_or_else(|| {
                // Killed by signal: bash's $? is 128 + signo.
                use std::os::unix::process::ExitStatusExt;
                s.signal().map(|sig| 128 + sig).unwrap_or(1)
            }),
            Err(_) => 1,
        };
        let _ = std::fs::remove_file(&output_sock_c);
        // Open for write blocks until the client reads the exit code, matching
        // bash `echo "$EXIT_CODE" > "$EXIT_SOCK"`.
        let exit_path = exit_sock_c.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open(&exit_path) {
                let _ = write!(f, "{code}\n");
            }
        })
        .await;
        let _ = std::fs::remove_file(&exit_sock_c);
    });

    // Signal-forwarding task (generate.rs:670-677): read one line from the
    // signal FIFO; if non-empty, kill -<SIGNAL> the command child (errors
    // tolerated); then rm the signal FIFO. The read is poll-bounded against
    // both the relay's global shutdown and this command's completion so the
    // watcher is always reclaimable.
    let signal_sock_c = signal_sock.clone();
    let signal_stop = CombinedStop {
        a: relay_shutdown,
        b: cmd_done,
    };
    tokio::spawn(async move {
        let read_path = signal_sock_c.clone();
        let outcome =
            tokio::task::spawn_blocking(move || read_command(&read_path, &signal_stop)).await;
        if let Ok(ReadOutcome::Line(line)) = outcome {
            if !line.is_empty() {
                if let Some(sig) = parse_signal(&line) {
                    let _ = kill(Pid::from_raw(cmd_pid), sig);
                }
            }
        }
        let _ = std::fs::remove_file(&signal_sock_c);
    });

    // echo "...response..." > "$cmd_socket.response"  (generate.rs:678). Open
    // for write blocks until the client reads the response, matching bash.
    let response_line = format_response_line(counter, cmd_pid);
    let response_path = response_socket.to_path_buf();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&response_path)?;
        f.write_all(response_line.as_bytes())?;
        Ok(())
    })
    .await
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))??;

    Ok(())
}

/// PURE helper: the HARD-contract response line (generate.rs:678). Uses
/// BASENAMES, comma-separated, with a trailing newline.
fn format_response_line(counter: u64, pid: i32) -> String {
    format!("output_{counter}.sock,exit_{counter}.sock,signal_{counter}.sock,{pid}\n")
}

/// `<cmd_socket>.response` path derivation (generate.rs:651-653 use the
/// literal suffix `.response` on the cmd socket path).
fn response_path(cmd_socket: &Path) -> PathBuf {
    let mut s = cmd_socket.as_os_str().to_os_string();
    s.push(".response");
    PathBuf::from(s)
}

/// mkfifo with mode 0o666 (generate.rs implicitly uses the process umask;
/// the brief mandates an explicit 0o666 mode).
fn make_fifo(path: &Path) -> std::io::Result<()> {
    mkfifo(path, Mode::from_bits_truncate(0o666))
        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))
}

/// `is_fifo` check mirroring `[ -p "$path" ]` (generate.rs:680).
fn is_fifo(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.file_type().is_fifo())
        .unwrap_or(false)
}

/// Wait for and read one command line from the cmd FIFO, observing `stop`.
///
/// Opens the FIFO read-end non-blocking so the open never parks (unlike a
/// plain blocking open, which would wedge teardown when no writer ever
/// connects). Then `poll`s for readability with a bounded timeout, rechecking
/// `stop` each wakeup. The bounded poll is the deadlock-free analogue of the
/// bash blocking `read -r CMD < fifo` plus the trap's `kill -TERM` interrupt.
///
/// Returns [`ReadOutcome::Line`] on a complete line, [`ReadOutcome::Eof`] only
/// when the FIFO node has vanished (the corrupt-state branch), and
/// [`ReadOutcome::ShutdownRequested`] when `stop` is observed while waiting.
fn read_command<S: Stop>(path: &Path, stop: &S) -> ReadOutcome {
    // O_RDONLY | O_NONBLOCK: returns immediately even with no writer present.
    let raw = match open(
        path,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(_) => {
            // Open failed: if the node is gone, that's the corrupt-state EOF
            // branch; otherwise treat as a transient miss and let the caller
            // re-enter (reported as Eof, which the loop re-validates).
            return ReadOutcome::Eof;
        }
    };
    // SAFETY: `raw` is a fresh fd we exclusively own.
    let owned = unsafe { OwnedFd::from_raw_fd(raw) };

    let timeout_ms = POLL_INTERVAL.as_millis() as libc::c_int;
    loop {
        if stop.should_stop() {
            return ReadOutcome::ShutdownRequested;
        }

        let mut pfd = libc::pollfd {
            fd: owned.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: single valid pollfd over an owned fd; libc::poll writes
        // revents and returns the readiness count or -1/errno.
        let n = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, timeout_ms) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return ReadOutcome::Eof;
        }

        if n == 0 {
            // Timed out with no event: recheck `stop` (top of loop) and the
            // FIFO's continued existence (the corrupt-state guard).
            if !is_fifo(path) {
                return ReadOutcome::Eof;
            }
            continue;
        }

        if pfd.revents & libc::POLLIN != 0 {
            // Data (or a zero-byte EOF from a writer that just closed) is
            // ready. Read one line via a buffered reader over the owned fd.
            return match read_line_from_fd(&owned) {
                // A writer connected and closed without sending a newline:
                // no command — keep waiting for the next writer (a healthy
                // FIFO is not the corrupt-state branch).
                Ok(None) => continue,
                Ok(Some(line)) => ReadOutcome::Line(line),
                Err(_) => ReadOutcome::Eof,
            };
        }

        // POLLHUP without POLLIN means no writer is currently connected (none
        // yet, or the last one closed with nothing buffered). `poll` reports
        // POLLHUP immediately and unconditionally, so re-polling at once would
        // busy-spin; sleep one interval first (this is also the stop-flag
        // recheck cadence). Keep waiting for the next writer, matching bash's
        // blocking open. Only a vanished node is the corrupt-state EOF.
        if !is_fifo(path) {
            return ReadOutcome::Eof;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Read a single line from an already-open FIFO fd. Returns `Ok(None)` on EOF
/// before any byte. Strips a single trailing newline (and CR) like `read -r`.
///
/// `poll` reported POLLIN, so a writer is connected; read the whole line
/// blockingly (the line may arrive across multiple writes). The fd is cleared
/// of `O_NONBLOCK` on a private dup so `read_line` blocks to the newline like
/// bash `read -r`, without disturbing the long-lived poll fd's flags.
fn read_line_from_fd(fd: &OwnedFd) -> std::io::Result<Option<String>> {
    // Dup into a File so the BufReader's drop closes only this clone, not the
    // long-lived `owned` fd.
    let dup = fd.try_clone()?;
    nix::fcntl::fcntl(dup.as_raw_fd(), nix::fcntl::FcntlArg::F_SETFL(OFlag::empty()))
        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
    let file = std::fs::File::from(dup);
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    Ok(Some(line))
}

/// Parse a signal token as bash `kill -<SIGNAL>` accepts it: a bare number
/// ("9", "15"), a name with or without the `SIG` prefix ("TERM", "SIGTERM").
fn parse_signal(token: &str) -> Option<Signal> {
    let t = token.trim();
    if let Ok(n) = t.parse::<i32>() {
        return Signal::try_from(n).ok();
    }
    let name = t.strip_prefix("SIG").unwrap_or(t);
    let with_prefix = format!("SIG{name}");
    with_prefix.parse::<Signal>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_response_line() {
        // HARD-contract golden (generate.rs:678).
        assert_eq!(
            format_response_line(1, 12345),
            "output_1.sock,exit_1.sock,signal_1.sock,12345\n"
        );
        assert_eq!(
            format_response_line(2, 999),
            "output_2.sock,exit_2.sock,signal_2.sock,999\n"
        );
    }

    fn layout_in(dir: &Path) -> Layout {
        let socket_dir = dir.join("sockets");
        std::fs::create_dir_all(&socket_dir).unwrap();
        Layout {
            rndtmp: dir.to_path_buf(),
            container_name: "asm-test-0".into(),
            src_tmp: dir.join("src"),
            out_tmp: dir.join("out"),
            log_tmp: dir.join("log"),
            podman_storage: dir.join("storage"),
            podman_run: dir.join("run"),
            socket_dir: socket_dir.clone(),
            cmd_socket: socket_dir.join("cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-test".into(),
            shutdown_log_path: dir.join("shutdown-manager.log"),
            shutdown_pid_file: dir.join("shutdown-manager.pid"),
            local_image: dir.join("image.tar"),
        }
    }

    #[tokio::test]
    async fn spawn_creates_fifos_and_shutdown_drains() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = layout_in(tmp.path());

        let handle = spawn(&layout).expect("spawn");

        // Both FIFOs created.
        let cmd_meta = std::fs::metadata(&layout.cmd_socket).expect("cmd fifo exists");
        assert!(cmd_meta.file_type().is_fifo(), "cmd.sock is a FIFO");
        let resp = response_path(&layout.cmd_socket);
        let resp_meta = std::fs::metadata(&resp).expect("response fifo exists");
        assert!(resp_meta.file_type().is_fifo(), "cmd.sock.response is a FIFO");

        // shutdown must return without hanging or panicking.
        tokio::time::timeout(std::time::Duration::from_secs(10), handle.shutdown())
            .await
            .expect("shutdown drained within timeout");
    }

    #[test]
    fn parse_signal_forms() {
        assert_eq!(parse_signal("9"), Some(Signal::SIGKILL));
        assert_eq!(parse_signal("TERM"), Some(Signal::SIGTERM));
        assert_eq!(parse_signal("SIGTERM"), Some(Signal::SIGTERM));
        assert_eq!(parse_signal("nonsense"), None);
    }

    /// Blockingly read the whole contents of a FIFO (open O_RDONLY blocks
    /// until the writer connects), then return it as a String. Test client
    /// side, run on a blocking thread.
    fn drain_fifo(path: PathBuf) -> String {
        let mut f = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let mut s = String::new();
        std::io::Read::read_to_string(&mut f, &mut s).unwrap();
        s
    }

    /// End-to-end client round-trip: this is the HARD-contract proof that the
    /// response line is written BEFORE the client connects to the output FIFO
    /// (i.e. no dispatch-vs-output-open deadlock). Mirrors the out-of-repo
    /// client: write CMD to cmd.sock, read the response line, then connect to
    /// the named output/exit FIFOs.
    #[tokio::test]
    async fn command_roundtrip_response_then_output_then_exit() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = layout_in(tmp.path());
        let handle = spawn(&layout).expect("spawn");

        let cmd_socket = layout.cmd_socket.clone();
        let resp_socket = response_path(&cmd_socket);
        let socket_dir = layout.socket_dir.clone();

        let body = async move {
            // Client writes the command line (open O_WRONLY blocks until the
            // relay's reader connects — the relay poll loop does).
            let w = cmd_socket.clone();
            tokio::task::spawn_blocking(move || {
                let mut f = std::fs::OpenOptions::new().write(true).open(&w).unwrap();
                f.write_all(b"printf hello\n").unwrap();
            })
            .await
            .unwrap();

            // Client reads the response line FIRST (must arrive without the
            // client having opened the output FIFO yet — the deadlock guard).
            let response = tokio::task::spawn_blocking(move || drain_fifo(resp_socket))
                .await
                .unwrap();
            assert!(
                response.starts_with("output_1.sock,exit_1.sock,signal_1.sock,"),
                "response line shape: {response:?}"
            );
            assert!(response.ends_with('\n'), "response newline: {response:?}");

            // Now connect to the named output + exit FIFOs.
            let out_path = socket_dir.join("output_1.sock");
            let exit_path = socket_dir.join("exit_1.sock");
            eprintln!("DBG client: draining output");
            let output = tokio::task::spawn_blocking(move || drain_fifo(out_path))
                .await
                .unwrap();
            eprintln!("DBG client: output={output:?}");
            assert_eq!(output, "hello", "command stdout");
            eprintln!("DBG client: draining exit");
            let exit = tokio::task::spawn_blocking(move || drain_fifo(exit_path))
                .await
                .unwrap();
            eprintln!("DBG client: exit={exit:?}");
            assert_eq!(exit.trim_end(), "0", "exit code");
        };

        tokio::time::timeout(std::time::Duration::from_secs(20), body)
            .await
            .expect("round-trip completed without deadlock");

        tokio::time::timeout(std::time::Duration::from_secs(10), handle.shutdown())
            .await
            .expect("shutdown drained within timeout");
    }
}
