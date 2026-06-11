//! Gateway-side peer-info fetch: [`fetch_dir_v2`] mirrors a remote
//! `connection_info/` directory's `*.info` files to a local directory
//! through a [`Gateway`], so the late-joiner bootstrap can run the
//! UNCHANGED local reader ([`super::read_dir::read_dir_v2`]) against
//! the mirrored copy.
//!
//! # Concern
//!
//! ONE concern: move the SLURM wrapper's `<secondary_id>.info` files
//! from a gateway-side directory onto the local filesystem. Parsing,
//! v2 filtering, and seed construction stay with their existing
//! owners (`read_dir_v2` / the dispatcher's seed builder) — this
//! module never opens an `.info` file's contents.
//!
//! # Loud failures
//!
//! Every failure names the exact failing step ([`PeerInfoFetchError`]):
//! an unlistable remote dir (typo'd path, gateway-side permission),
//! a directory with zero `*.info` files, a per-file download failure,
//! and a local mirror-dir creation failure each carry the path and
//! the underlying cause — the silent-branch rule for an operator
//! whose run hinges on this bootstrap step.

use std::path::Path;

use dynrunner_gateway::shell::shell_quote;
use dynrunner_gateway::traits::Gateway;

/// Error from [`fetch_dir_v2`] — one variant per failing step, each
/// naming the path it failed on.
#[derive(Debug, thiserror::Error)]
pub enum PeerInfoFetchError {
    /// Listing the remote directory failed: the gateway command
    /// errored, or `ls` returned non-zero (directory missing /
    /// unreadable on the gateway).
    #[error("failed to list gateway-side peer-info directory `{dir}`: {detail}")]
    List { dir: String, detail: String },
    /// The remote directory listed fine but contains no `*.info`
    /// files — the late-joiner has nothing to seed from. Either the
    /// path is wrong (the wrapper writes
    /// `<run_log_dir>/connection_info/<secondary_id>.info`) or the
    /// cluster has not reached the point where secondaries record
    /// their connection info.
    #[error(
        "gateway-side peer-info directory `{dir}` contains no `*.info` files — \
         point --observer-join-from-peer-info-dir at the run's \
         `<run_log_dir>/connection_info` directory on the gateway, or wait \
         for the cluster's secondaries to write their records"
    )]
    NoInfoFiles { dir: String },
    /// One file's download failed (scp through the gateway).
    #[error("failed to download gateway-side peer-info file `{path}`: {detail}")]
    Download { path: String, detail: String },
    /// Creating the local mirror directory failed.
    #[error("failed to create local mirror directory `{dir}`: {source}")]
    LocalDir {
        dir: String,
        #[source]
        source: std::io::Error,
    },
}

/// Mirror every `*.info` file under `remote_dir` (a gateway-side path)
/// into `local_dir`, returning how many files were fetched (≥ 1 on
/// `Ok`). The caller then reads the mirror with the unchanged local
/// [`read_dir_v2`](super::read_dir::read_dir_v2).
///
/// The filename filter mirrors `read_dir_v2`'s `.info`-extension
/// filter so the two stages agree on which files participate;
/// neighbours in the shared `connection_info` namespace (logs, temp
/// files) are skipped here exactly as the local reader skips them.
pub async fn fetch_dir_v2<G: Gateway>(
    gateway: &G,
    remote_dir: &str,
    local_dir: &Path,
) -> Result<usize, PeerInfoFetchError> {
    // Resolve `~` ONCE against the gateway-side home so the quoted
    // `ls` (which the remote shell would otherwise see verbatim) and
    // the per-file downloads agree on the same absolute path.
    let remote_dir = &gateway.expand_remote_path(remote_dir);

    // Step 1: list. `ls -1 --` keeps one name per line and guards
    // against option-looking dir names; a missing/unreadable dir is
    // a non-zero rc with the cause on stderr.
    let cmd = format!("ls -1 -- {}", shell_quote(remote_dir));
    let listing =
        gateway
            .execute_command(&cmd, None)
            .await
            .map_err(|e| PeerInfoFetchError::List {
                dir: remote_dir.to_owned(),
                detail: e.to_string(),
            })?;
    if !listing.success() {
        return Err(PeerInfoFetchError::List {
            dir: remote_dir.to_owned(),
            detail: format!(
                "`ls` exited {}: {}",
                listing.return_code,
                listing.stderr.trim()
            ),
        });
    }

    // Step 2: filter to the wrapper's `*.info` naming. Plain basenames
    // only — a name with a path separator would have to come from a
    // nested entry `ls -1` does not produce.
    let info_names: Vec<&str> = listing
        .stdout
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty() && name.ends_with(".info"))
        .collect();
    if info_names.is_empty() {
        return Err(PeerInfoFetchError::NoInfoFiles {
            dir: remote_dir.to_owned(),
        });
    }

    // Step 3: mirror locally.
    std::fs::create_dir_all(local_dir).map_err(|source| PeerInfoFetchError::LocalDir {
        dir: local_dir.display().to_string(),
        source,
    })?;
    for name in &info_names {
        let remote_path = format!("{}/{}", remote_dir.trim_end_matches('/'), name);
        let local_path = local_dir.join(name);
        gateway
            .download_file(&remote_path, &local_path)
            .await
            .map_err(|e| PeerInfoFetchError::Download {
                path: remote_path.clone(),
                detail: e.to_string(),
            })?;
    }
    tracing::info!(
        remote_dir = %remote_dir,
        local_dir = %local_dir.display(),
        files = info_names.len(),
        "fetched gateway-side peer-info files"
    );
    Ok(info_names.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_gateway::traits::{CommandResult, GatewayError};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    /// In-memory gateway double: `execute_command` answers the `ls`
    /// with a canned listing; `download_file` writes the canned file
    /// body to the requested local path. Mirrors the
    /// `RecordingGateway` shape in `tests/upload.rs`.
    #[derive(Default)]
    struct FakeGateway {
        /// remote path → file body. Listing is derived from the keys
        /// under the requested dir.
        files: HashMap<String, String>,
        /// When set, `ls` fails with this (rc, stderr).
        ls_failure: Option<(i32, String)>,
        /// Remote paths whose download must fail.
        download_failures: Vec<String>,
        downloads: Mutex<Vec<(String, PathBuf)>>,
    }

    impl Gateway for FakeGateway {
        async fn connect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn execute_command(
            &self,
            cmd: &str,
            _cwd: Option<&str>,
        ) -> Result<CommandResult, GatewayError> {
            assert!(cmd.starts_with("ls -1 -- "), "unexpected command: {cmd}");
            if let Some((rc, stderr)) = &self.ls_failure {
                return Ok(CommandResult {
                    return_code: *rc,
                    stdout: String::new(),
                    stderr: stderr.clone(),
                });
            }
            let dir = cmd
                .trim_start_matches("ls -1 -- ")
                .trim_matches('\'')
                .trim_end_matches('/');
            let mut names: Vec<String> = self
                .files
                .keys()
                .filter_map(|p| p.strip_prefix(&format!("{dir}/")).map(str::to_owned))
                .collect();
            names.sort();
            Ok(CommandResult {
                return_code: 0,
                stdout: names.join("\n"),
                stderr: String::new(),
            })
        }
        async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn download_file(&self, remote: &str, local: &Path) -> Result<(), GatewayError> {
            if self.download_failures.iter().any(|p| p == remote) {
                return Err(GatewayError::CopyFailed(format!("scp failed for {remote}")));
            }
            let body = self
                .files
                .get(remote)
                .unwrap_or_else(|| panic!("download of unlisted file {remote}"));
            std::fs::write(local, body).map_err(GatewayError::Io)?;
            self.downloads
                .lock()
                .unwrap()
                .push((remote.to_owned(), local.to_path_buf()));
            Ok(())
        }
        async fn create_directory(&self, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
            Ok(false)
        }
        fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
            Ok(())
        }
    }

    /// Happy path: every `*.info` file is mirrored byte-identically,
    /// non-`.info` neighbours are skipped, and the mirrored dir is
    /// readable by the UNCHANGED local v2 reader.
    #[tokio::test]
    async fn fetches_info_files_and_local_reader_consumes_them() {
        let record = crate::peer_info::Builder::new("compute1", 40001)
            .secondary_id("secondary-0")
            .ipv4("10.0.0.1")
            .quic_port(51200)
            .format();
        let mut gw = FakeGateway::default();
        gw.files
            .insert("/gw/run/connection_info/secondary-0.info".into(), record);
        gw.files.insert(
            "/gw/run/connection_info/wrapper.log".into(),
            "not an info file".into(),
        );
        let tmp = tempfile::tempdir().unwrap();

        let n = fetch_dir_v2(&gw, "/gw/run/connection_info", tmp.path())
            .await
            .expect("fetch succeeds");
        assert_eq!(n, 1);
        let records = crate::peer_info::read_dir_v2(tmp.path()).expect("local reader consumes");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].secondary_id.as_deref(), Some("secondary-0"));
        assert_eq!(records[0].quic_port, Some(51200));
    }

    /// A missing / unreadable remote dir fails LOUDLY, naming the dir
    /// and the `ls` failure detail (the exact failing step).
    #[tokio::test]
    async fn unlistable_dir_names_dir_and_cause() {
        let gw = FakeGateway {
            ls_failure: Some((
                2,
                "ls: cannot access '/gw/nope': No such file or directory".into(),
            )),
            ..Default::default()
        };
        let tmp = tempfile::tempdir().unwrap();
        let err = fetch_dir_v2(&gw, "/gw/nope", tmp.path())
            .await
            .expect_err("must fail loud");
        let msg = err.to_string();
        assert!(matches!(err, PeerInfoFetchError::List { .. }), "{err:?}");
        assert!(msg.contains("/gw/nope"), "{msg}");
        assert!(msg.contains("No such file or directory"), "{msg}");
    }

    /// A listable dir with zero `*.info` files fails LOUDLY with the
    /// remediation-bearing NoInfoFiles error.
    #[tokio::test]
    async fn zero_info_files_fails_loud() {
        let mut gw = FakeGateway::default();
        gw.files
            .insert("/gw/ci/other.log".into(), "neighbour".into());
        let tmp = tempfile::tempdir().unwrap();
        let err = fetch_dir_v2(&gw, "/gw/ci", tmp.path())
            .await
            .expect_err("must fail loud");
        assert!(
            matches!(err, PeerInfoFetchError::NoInfoFiles { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("/gw/ci"), "{err}");
    }

    /// A per-file download failure names the exact remote path.
    #[tokio::test]
    async fn download_failure_names_file() {
        let mut gw = FakeGateway::default();
        gw.files
            .insert("/gw/ci/secondary-0.info".into(), "tcp://h:1\n".into());
        gw.download_failures.push("/gw/ci/secondary-0.info".into());
        let tmp = tempfile::tempdir().unwrap();
        let err = fetch_dir_v2(&gw, "/gw/ci", tmp.path())
            .await
            .expect_err("must fail loud");
        assert!(
            matches!(err, PeerInfoFetchError::Download { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("secondary-0.info"), "{err}");
    }
}
