//! Tests for the batched `squeue -j <list>` status probe
//! ([`SlurmJobManager::get_job_status_batch`], #675).
//!
//! The authority probe used to issue one `squeue -j <id>` per secondary.
//! #675 batches the whole set into a SINGLE `squeue -j <id1>,<id2>,…`
//! call. The contract these tests pin: the batched parse produces the
//! SAME per-job `JobStatusInfo` map the per-job path would, INCLUDING a
//! job ABSENT from the output — SLURM omits finished/purged/unknown jobs
//! from a comma-list query rather than erroring, and that job must map to
//! the SAME "no row" snapshot (`state_kind == None`) a per-job
//! `squeue -j <id>` returns for a job no longer in the queue, so the
//! caller's "missing → consult sacct/ledger" interpretation is unchanged.

use std::path::Path;
use std::sync::Mutex;

use super::super::types::{JobStatus, SlurmJobManager};
use crate::config::SlurmConfig;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

/// Gateway whose `squeue -j <list>` returns a fixed multi-row body. The
/// body deliberately OMITS one of the queried ids to model SLURM dropping
/// a finished/purged job from a comma-list query.
struct BatchSqueueGateway {
    /// Canned stdout for the batched `%i|%T|%N|%r` query.
    stdout: String,
    /// Exit code for the batched query (0 = success).
    return_code: i32,
    /// Every command the probe issued, in order — lets the test assert
    /// that exactly ONE squeue call was made for the whole set.
    commands: Mutex<Vec<String>>,
}

impl BatchSqueueGateway {
    fn new(stdout: &str, return_code: i32) -> Self {
        Self {
            stdout: stdout.to_string(),
            return_code,
            commands: Mutex::new(Vec::new()),
        }
    }

    fn squeue_call_count(&self) -> usize {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with("squeue "))
            .count()
    }
}

impl Gateway for BatchSqueueGateway {
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
        self.commands.lock().unwrap().push(cmd.to_string());
        Ok(CommandResult {
            return_code: self.return_code,
            stdout: self.stdout.clone(),
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
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

fn manager(gw: BatchSqueueGateway) -> SlurmJobManager<BatchSqueueGateway> {
    SlurmJobManager::new(SlurmConfig::default(), gw)
}

/// The batched parse maps each PRESENT row to its state/node/reason and
/// maps an ABSENT id (omitted from the comma-list output) to the "no row"
/// snapshot — the same shape a per-job `squeue -j <id>` returns for a job
/// no longer in the queue. Exactly one squeue call is issued for the set.
#[tokio::test]
async fn batched_parse_matches_per_job_including_absent() {
    // 155627 is OMITTED from the body — it has left the queue.
    let body = "155626|RUNNING|krater07|None\n\
                155628|PENDING|None assigned|Resources\n";
    let gw = BatchSqueueGateway::new(body, 0);
    let mgr = manager(gw);

    let ids = vec![
        "155626".to_string(),
        "155627".to_string(),
        "155628".to_string(),
    ];
    let map = mgr.get_job_status_batch(&ids).await.expect("batch ok");

    // One squeue invocation for the whole set (N→1).
    assert_eq!(mgr.gateway().squeue_call_count(), 1);

    // Present rows parse identically to the per-job path.
    let running = map.get("155626").expect("155626 present");
    assert!(matches!(running.state_kind, Some(JobStatus::Running)));
    assert_eq!(running.node, "krater07");
    assert_eq!(running.reason, "None");

    let pending = map.get("155628").expect("155628 present");
    assert!(matches!(pending.state_kind, Some(JobStatus::Pending)));
    assert_eq!(pending.reason, "Resources");

    // Absent id → "no row" snapshot: state_kind == None, empty
    // node/reason. This is the per-job empty/non-zero result, which the
    // authority probe interprets as "not in queue → consult sacct/ledger".
    let absent = map.get("155627").expect("155627 keyed even when absent");
    assert!(absent.state_kind.is_none());
    assert!(absent.state.is_none());
    assert!(absent.node.is_empty());
    assert!(absent.reason.is_empty());
}

/// An empty squeue body (every queried job has left the queue) maps EVERY
/// id to the "no row" snapshot — the per-job path's empty-result shape for
/// all of them at once.
#[tokio::test]
async fn batched_parse_empty_body_maps_all_to_missing() {
    let gw = BatchSqueueGateway::new("", 0);
    let mgr = manager(gw);

    let ids = vec!["100".to_string(), "200".to_string()];
    let map = mgr.get_job_status_batch(&ids).await.expect("batch ok");

    for id in &ids {
        let info = map.get(id).expect("id keyed");
        assert!(info.state_kind.is_none(), "{id} should be missing");
    }
}

/// A non-zero exit on the whole batched query (e.g. SLURM rejected the
/// list) is treated as "no rows" — every id maps to the missing snapshot,
/// the same fail-direction the per-job path takes on a non-zero single
/// query. No id is dropped from the map.
#[tokio::test]
async fn batched_parse_nonzero_exit_maps_all_to_missing() {
    let gw = BatchSqueueGateway::new("", 1);
    let mgr = manager(gw);

    let ids = vec!["300".to_string()];
    let map = mgr.get_job_status_batch(&ids).await.expect("batch ok");

    let info = map.get("300").expect("id keyed");
    assert!(info.state_kind.is_none());
}

/// An empty id set issues NO squeue call and returns an empty map.
#[tokio::test]
async fn batched_parse_empty_set_issues_no_call() {
    let gw = BatchSqueueGateway::new("", 0);
    let mgr = manager(gw);

    let map = mgr.get_job_status_batch(&[]).await.expect("batch ok");
    assert!(map.is_empty());
    assert_eq!(mgr.gateway().squeue_call_count(), 0);
}
