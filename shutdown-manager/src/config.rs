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
}
