//! Single concern: argv → typed `Config`.
//!
//! Parses the CLI contract documented in the project README. No I/O,
//! no signals, no podman. Failing to parse returns an error string;
//! the caller (main) prints it and exits non-zero.
//!
//! Argv parsing is hand-rolled. There are only nine flags and no
//! sub-commands, so pulling in a parsing crate would dwarf the actual
//! logic in compiled-binary size.

use std::path::PathBuf;
use std::time::Duration;

/// Fully resolved configuration. All defaults applied.
#[derive(Debug, Clone)]
pub struct Config {
    pub container_name: String,
    pub storage_root: PathBuf,
    pub runroot: PathBuf,
    pub tmp_prefix: PathBuf,
    pub pid_file: PathBuf,
    pub poll_interval: Duration,
    pub idle_shutdown: Duration,
    pub secondary_grace: Duration,
    pub container_stop_grace: Duration,
    /// Optional PID of the wrapper script that spawned this manager.
    /// When set, the poll loop probes the PID each tick and triggers
    /// SIGNAL_SHUTDOWN unconditionally on its disappearance — closing
    /// the SLURM-TIMEOUT race where proctrack reaps the wrapper
    /// before its signal trap can forward `systemctl --user kill`.
    /// `None` (the default) preserves pre-monitor behaviour.
    pub wrapper_pid: Option<u32>,
    /// Optional path where the manager appends its own log lines.
    /// When set, every `log()` line written to stderr is also
    /// appended to this file (best-effort; failures are non-fatal
    /// and surface on stderr only). When `None`, the manager logs
    /// to stderr alone — the pre-2026-05-18 behaviour.
    ///
    /// Owning the log destination at the binary level (rather than
    /// relying on the caller's stdio redirection — shell `>>` or
    /// systemd `StandardOutput=append:`) was added after the
    /// systemd-side append-properties were observed to silently
    /// drop the manager's stdio under service mode on the deployed
    /// systemd/MAC stack (asm-tokenizer 2026-05-18).
    pub log_file: Option<PathBuf>,
}

/// Default per the CLI contract.
const DEFAULT_POLL_INTERVAL_SECS: u64 = 2;
const DEFAULT_IDLE_SHUTDOWN_SECS: u64 = 5;
const DEFAULT_SECONDARY_GRACE_SECS: u64 = 5;
const DEFAULT_CONTAINER_STOP_GRACE_SECS: u64 = 10;

/// Mutable accumulator for in-progress parsing. Each required field is
/// `Option`; missing ones surface as named errors at finalization.
#[derive(Default)]
struct Builder {
    container_name: Option<String>,
    storage_root: Option<PathBuf>,
    runroot: Option<PathBuf>,
    tmp_prefix: Option<PathBuf>,
    pid_file: Option<PathBuf>,
    poll_interval_secs: Option<u64>,
    idle_shutdown_secs: Option<u64>,
    secondary_grace_secs: Option<u64>,
    container_stop_grace_secs: Option<u64>,
    wrapper_pid: Option<u32>,
    log_file: Option<PathBuf>,
}

impl Builder {
    fn finish(self) -> Result<Config, String> {
        Ok(Config {
            container_name: self
                .container_name
                .ok_or_else(|| "missing --container-name".to_string())?,
            storage_root: self
                .storage_root
                .ok_or_else(|| "missing --storage-root".to_string())?,
            runroot: self.runroot.ok_or_else(|| "missing --runroot".to_string())?,
            tmp_prefix: self
                .tmp_prefix
                .ok_or_else(|| "missing --tmp-prefix".to_string())?,
            pid_file: self
                .pid_file
                .ok_or_else(|| "missing --pid-file".to_string())?,
            poll_interval: Duration::from_secs(
                self.poll_interval_secs.unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
            ),
            idle_shutdown: Duration::from_secs(
                self.idle_shutdown_secs.unwrap_or(DEFAULT_IDLE_SHUTDOWN_SECS),
            ),
            secondary_grace: Duration::from_secs(
                self.secondary_grace_secs
                    .unwrap_or(DEFAULT_SECONDARY_GRACE_SECS),
            ),
            container_stop_grace: Duration::from_secs(
                self.container_stop_grace_secs
                    .unwrap_or(DEFAULT_CONTAINER_STOP_GRACE_SECS),
            ),
            wrapper_pid: self.wrapper_pid,
            log_file: self.log_file,
        })
    }
}

/// Parse from an iterator of argv values *excluding* `argv[0]`.
///
/// Accepts `--flag value` and `--flag=value`. Unknown flags are errors.
/// Empty/zero values for numeric flags are rejected — a zero
/// `--poll-interval-secs` would spin the loop.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Config, String> {
    let mut b = Builder::default();
    let mut iter = args.into_iter();
    while let Some(raw) = iter.next() {
        let (key, inline_value) = split_flag(&raw);
        let take_str = |iter: &mut dyn Iterator<Item = String>| -> Result<String, String> {
            match inline_value.clone() {
                Some(v) => Ok(v),
                None => iter
                    .next()
                    .ok_or_else(|| format!("{} requires a value", key)),
            }
        };
        let take_u64 = |iter: &mut dyn Iterator<Item = String>| -> Result<u64, String> {
            let v = take_str(iter)?;
            let n: u64 = v
                .parse()
                .map_err(|_| format!("{} expects a positive integer, got {:?}", key, v))?;
            match n {
                0 => Err(format!("{} must be > 0", key)),
                _ => Ok(n),
            }
        };
        match key.as_str() {
            "--container-name" => b.container_name = Some(take_str(&mut iter)?),
            "--storage-root" => b.storage_root = Some(PathBuf::from(take_str(&mut iter)?)),
            "--runroot" => b.runroot = Some(PathBuf::from(take_str(&mut iter)?)),
            "--tmp-prefix" => b.tmp_prefix = Some(PathBuf::from(take_str(&mut iter)?)),
            "--pid-file" => b.pid_file = Some(PathBuf::from(take_str(&mut iter)?)),
            "--poll-interval-secs" => b.poll_interval_secs = Some(take_u64(&mut iter)?),
            "--idle-shutdown-secs" => b.idle_shutdown_secs = Some(take_u64(&mut iter)?),
            "--secondary-grace-secs" => b.secondary_grace_secs = Some(take_u64(&mut iter)?),
            "--container-stop-grace-secs" => {
                b.container_stop_grace_secs = Some(take_u64(&mut iter)?)
            }
            "--wrapper-pid" => {
                // PIDs are 1..pid_max on Linux; reuse take_u64 (which
                // already rejects 0) and narrow to u32. Cast bounds:
                // pid_max never exceeds 2^22; explicit overflow check
                // keeps the contract honest if a caller passes garbage.
                let n = take_u64(&mut iter)?;
                let pid: u32 = n
                    .try_into()
                    .map_err(|_| format!("--wrapper-pid out of range: {}", n))?;
                b.wrapper_pid = Some(pid);
            }
            "--log-file" => b.log_file = Some(PathBuf::from(take_str(&mut iter)?)),
            other => return Err(format!("unknown flag: {}", other)),
        }
    }
    b.finish()
}

/// Split `--flag=value` into (`--flag`, `Some(value)`); otherwise (`--flag`, None).
fn split_flag(raw: &str) -> (String, Option<String>) {
    match raw.split_once('=') {
        Some((k, v)) => (k.to_string(), Some(v.to_string())),
        None => (raw.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    fn minimal_required() -> Vec<String> {
        argv(&[
            "--container-name",
            "asm-abc-secondary-0",
            "--storage-root",
            "/var/tmp/podman-root",
            "--runroot",
            "/var/tmp/podman-run",
            "--tmp-prefix",
            "/tmp/asm-XXX",
            "--pid-file",
            "/tmp/asm-XXX/shutdown.pid",
        ])
    }

    #[test]
    fn parses_required_with_defaults() {
        let cfg = parse(minimal_required()).expect("must parse");
        assert_eq!(cfg.container_name, "asm-abc-secondary-0");
        assert_eq!(cfg.poll_interval.as_secs(), DEFAULT_POLL_INTERVAL_SECS);
        assert_eq!(cfg.idle_shutdown.as_secs(), DEFAULT_IDLE_SHUTDOWN_SECS);
        assert_eq!(cfg.secondary_grace.as_secs(), DEFAULT_SECONDARY_GRACE_SECS);
        assert_eq!(
            cfg.container_stop_grace.as_secs(),
            DEFAULT_CONTAINER_STOP_GRACE_SECS
        );
    }

    #[test]
    fn parses_optional_overrides() {
        let mut args = minimal_required();
        args.extend(argv(&[
            "--poll-interval-secs",
            "1",
            "--idle-shutdown-secs",
            "30",
            "--secondary-grace-secs",
            "7",
            "--container-stop-grace-secs",
            "15",
        ]));
        let cfg = parse(args).expect("must parse");
        assert_eq!(cfg.poll_interval.as_secs(), 1);
        assert_eq!(cfg.idle_shutdown.as_secs(), 30);
        assert_eq!(cfg.secondary_grace.as_secs(), 7);
        assert_eq!(cfg.container_stop_grace.as_secs(), 15);
    }

    #[test]
    fn accepts_equals_form() {
        let cfg = parse(argv(&[
            "--container-name=foo",
            "--storage-root=/r",
            "--runroot=/rr",
            "--tmp-prefix=/t",
            "--pid-file=/p",
            "--poll-interval-secs=3",
        ]))
        .expect("must parse");
        assert_eq!(cfg.container_name, "foo");
        assert_eq!(cfg.poll_interval.as_secs(), 3);
    }

    #[test]
    fn missing_required_is_error() {
        let err = parse(argv(&["--container-name", "x"])).unwrap_err();
        assert!(err.contains("--storage-root"), "got: {}", err);
    }

    #[test]
    fn unknown_flag_is_error() {
        let mut args = minimal_required();
        args.extend(argv(&["--bogus", "value"]));
        let err = parse(args).unwrap_err();
        assert!(err.contains("--bogus"), "got: {}", err);
    }

    #[test]
    fn zero_numeric_is_rejected() {
        let mut args = minimal_required();
        args.extend(argv(&["--poll-interval-secs", "0"]));
        let err = parse(args).unwrap_err();
        assert!(err.contains("> 0"), "got: {}", err);
    }

    #[test]
    fn non_numeric_is_rejected() {
        let mut args = minimal_required();
        args.extend(argv(&["--idle-shutdown-secs", "five"]));
        let err = parse(args).unwrap_err();
        assert!(err.contains("positive integer"), "got: {}", err);
    }

    #[test]
    fn flag_without_value_is_error() {
        let err = parse(argv(&["--container-name"])).unwrap_err();
        assert!(err.contains("requires a value"), "got: {}", err);
    }

    #[test]
    fn wrapper_pid_defaults_to_none() {
        let cfg = parse(minimal_required()).expect("must parse");
        assert!(
            cfg.wrapper_pid.is_none(),
            "--wrapper-pid omitted ⇒ None (preserves pre-monitor behaviour)"
        );
    }

    #[test]
    fn wrapper_pid_parses_when_set() {
        let mut args = minimal_required();
        args.extend(argv(&["--wrapper-pid", "12345"]));
        let cfg = parse(args).expect("must parse");
        assert_eq!(cfg.wrapper_pid, Some(12345));
    }

    #[test]
    fn wrapper_pid_accepts_equals_form() {
        let mut args = minimal_required();
        args.extend(argv(&["--wrapper-pid=99"]));
        let cfg = parse(args).expect("must parse");
        assert_eq!(cfg.wrapper_pid, Some(99));
    }

    #[test]
    fn wrapper_pid_zero_rejected() {
        let mut args = minimal_required();
        args.extend(argv(&["--wrapper-pid", "0"]));
        let err = parse(args).unwrap_err();
        assert!(err.contains("> 0"), "got: {}", err);
    }

    #[test]
    fn wrapper_pid_overflow_rejected() {
        // 2^32 = 4_294_967_296 is one past u32::MAX.
        let mut args = minimal_required();
        args.extend(argv(&["--wrapper-pid", "4294967296"]));
        let err = parse(args).unwrap_err();
        assert!(err.contains("out of range"), "got: {}", err);
    }

    #[test]
    fn log_file_defaults_to_none() {
        let cfg = parse(minimal_required()).expect("must parse");
        assert!(
            cfg.log_file.is_none(),
            "--log-file omitted ⇒ None (stderr-only logging, pre-2026-05-18 default)"
        );
    }

    #[test]
    fn parses_log_file_optional() {
        let mut args = minimal_required();
        args.extend(argv(&["--log-file", "/tmp/shutdown.log"]));
        let cfg = parse(args).expect("must parse");
        assert_eq!(cfg.log_file, Some(PathBuf::from("/tmp/shutdown.log")));
    }

    #[test]
    fn log_file_equals_form() {
        let mut args = minimal_required();
        args.extend(argv(&["--log-file=/tmp/shutdown.log"]));
        let cfg = parse(args).expect("must parse");
        assert_eq!(cfg.log_file, Some(PathBuf::from("/tmp/shutdown.log")));
    }
}
