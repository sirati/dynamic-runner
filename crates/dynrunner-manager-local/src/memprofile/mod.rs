//! 1-Hz per-task memory profiler.
//!
//! Reads each active worker's cgroup memory stats once per second and
//! appends one zstd-framed JSONL sample per task to a per-task file
//! under `{output_dir}/memprofile/`. Frame-per-sample lets a hard
//! manager death lose at most one sample (consumers truncate at the
//! last complete frame).
//!
//! The orchestrator here ([`MemProfileSampler`]) owns one background
//! tokio task and an unbounded mpsc command channel. Manager-side hot
//! paths fire fire-and-forget commands (`on_task_assigned`,
//! `on_task_completed`, `on_worker_disconnected`); all kernel reads
//! and JSONL writes happen on the sampler's own tick so the manager's
//! critical path never blocks on disk or sysfs.

pub mod cgroup_reader;
pub mod config;
pub mod error;
pub mod sample;
pub mod writer;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

pub use self::config::MemProfileConfig;
pub use self::error::MemProfileError;
pub use self::sample::Sample;

use self::writer::JsonlZstdWriter;

/// Command sent on the sampler's mpsc channel. The tick loop owns the
/// active-profiles map and is the only writer; all mutations come
/// through this channel.
enum Cmd {
    Assign {
        task_id: String,
        worker_id: u32,
        subcgroup_dir: PathBuf,
        started_at: Instant,
    },
    Complete {
        task_id: String,
    },
    WorkerDisconnect {
        worker_id: u32,
    },
    Shutdown,
}

/// Centralised 1-Hz per-task memory profiler. Spawned once per
/// `LocalManager` (when `output_dir` is configured); owns one
/// background tokio task. All public methods are non-blocking
/// command-channel sends — the tick loop does the I/O on its own
/// schedule so the manager's critical path doesn't block on disk or
/// kernel reads.
///
/// Lifecycle:
///   * `spawn(config)`  — start the background task; returns immediately.
///   * `on_task_assigned`, `on_task_completed`, `on_worker_disconnected`
///     — fire-and-forget hooks invoked from the manager event loop.
///   * `shutdown().await` — flushes every open writer and joins the
///     background task before returning. Consumes `self`.
pub struct MemProfileSampler {
    tx: UnboundedSender<Cmd>,
    join: Option<JoinHandle<()>>,
}

impl MemProfileSampler {
    /// Spawn the background tokio task. Returns immediately; the task
    /// lives on the current tokio runtime. Pre-condition: caller is
    /// inside a running tokio runtime (panics otherwise via
    /// `tokio::spawn`).
    pub fn spawn(config: MemProfileConfig) -> Self {
        let (tx, rx) = unbounded_channel();
        let join = tokio::spawn(run_tick_loop(config, rx));
        Self {
            tx,
            join: Some(join),
        }
    }

    /// Open a per-task profile file and start sampling. `task_id ==
    /// None` is the documented "legacy task without ID" path: log at
    /// debug level and skip silently. `task_id` containing `..` or an
    /// absolute-path prefix is rejected with a warn — defensive
    /// against malformed identifiers that would otherwise let a task
    /// write outside `output_dir`.
    pub fn on_task_assigned(
        &self,
        task_id: Option<String>,
        worker_id: u32,
        subcgroup_dir: PathBuf,
        started_at: Instant,
    ) {
        let Some(task_id) = task_id else {
            tracing::debug!(worker_id, "memprofile skipped: task_id absent");
            return;
        };
        if path_is_unsafe(&task_id) {
            tracing::warn!(
                task_id,
                worker_id,
                "memprofile skipped: task_id contains unsafe path segments"
            );
            return;
        }
        let _ = self.tx.send(Cmd::Assign {
            task_id,
            worker_id,
            subcgroup_dir,
            started_at,
        });
    }

    /// Finalise the per-task file. Idempotent if the task was never
    /// assigned (no-op).
    pub fn on_task_completed(&self, task_id: Option<String>) {
        if let Some(task_id) = task_id {
            let _ = self.tx.send(Cmd::Complete { task_id });
        }
    }

    /// Finalise every open profile for this worker. Used when the
    /// worker pipe-EOFs (crash, kernel-OOM kill, transport hiccup) and
    /// no `TaskCompleted` will arrive.
    pub fn on_worker_disconnected(&self, worker_id: u32) {
        let _ = self.tx.send(Cmd::WorkerDisconnect { worker_id });
    }

    /// Drain the command queue, flush every open writer, and join the
    /// background task. Consumes `self`. After this returns the
    /// sampler is fully gone.
    pub async fn shutdown(mut self) {
        let _ = self.tx.send(Cmd::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

/// Reject `task_id`s containing `..` segments or absolute-path
/// prefixes. The writer does `create_dir_all(parent)` so a malformed
/// `task_id` would otherwise let a task write outside the configured
/// `output_dir`. Defensive — not expected with real consumers.
fn path_is_unsafe(task_id: &str) -> bool {
    task_id.starts_with('/')
        || task_id.starts_with('\\')
        || task_id.split(['/', '\\']).any(|seg| seg == "..")
}

/// Per-task state owned by the tick loop. One entry per active
/// profile; created on `Assign`, dropped on `Complete` /
/// `WorkerDisconnect` / `Shutdown`.
struct ActiveProfile {
    worker_id: u32,
    subcgroup_dir: PathBuf,
    started_at: Instant,
    writer: JsonlZstdWriter,
}

/// Background loop: on each tick, read every active profile's cgroup
/// and write one frame; otherwise handle commands as they arrive.
/// Exits cleanly on `Shutdown` (or when all senders drop) after
/// flushing every open writer.
async fn run_tick_loop(config: MemProfileConfig, mut commands: UnboundedReceiver<Cmd>) {
    let mut active: HashMap<String, ActiveProfile> = HashMap::new();
    let mut interval = tokio::time::interval(config.sample_interval);
    // Skip behaviour matches the OomWatcher's cadence policy: if a
    // tick is missed (e.g. the loop was busy handling a burst of
    // commands), we skip the backlog rather than firing multiple
    // ticks back-to-back. The sampler is "best-effort 1 Hz", not
    // "guaranteed N samples per second".
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                sample_all(&mut active);
            }
            cmd = commands.recv() => {
                match cmd {
                    Some(Cmd::Assign { task_id, worker_id, subcgroup_dir, started_at }) => {
                        handle_assign(&config, &mut active, task_id, worker_id, subcgroup_dir, started_at);
                    }
                    Some(Cmd::Complete { task_id }) => {
                        handle_complete(&mut active, &task_id);
                    }
                    Some(Cmd::WorkerDisconnect { worker_id }) => {
                        handle_worker_disconnect(&mut active, worker_id);
                    }
                    Some(Cmd::Shutdown) | None => {
                        flush_all(&mut active);
                        break;
                    }
                }
            }
        }
    }
}

/// Sample every active profile once. Read failures and write failures
/// are logged at warn level and the sample is dropped — the profile
/// stays active so the next tick gets another chance.
fn sample_all(active: &mut HashMap<String, ActiveProfile>) {
    let now_wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    for (task_id, ap) in active.iter_mut() {
        match cgroup_reader::read(&ap.subcgroup_dir) {
            Ok(stat) => {
                let sample = Sample {
                    t_ns: now_wall,
                    t_rel_ns: ap.started_at.elapsed().as_nanos() as u64,
                    worker_id: ap.worker_id,
                    memory_current: stat.memory_current,
                    swap_current: stat.swap_current,
                    memory_stat: stat.memory_stat,
                };
                if let Err(e) = ap.writer.write_sample_as_frame(&sample) {
                    tracing::warn!(
                        task_id,
                        worker_id = ap.worker_id,
                        error = %e,
                        "memprofile write failed; dropping sample",
                    );
                }
            }
            Err(e) => tracing::warn!(
                task_id,
                worker_id = ap.worker_id,
                error = %e,
                "memprofile cgroup read failed; dropping sample",
            ),
        }
    }
}

/// Open the per-task file and insert the active-profile entry. File
/// path is `{output_dir}/{task_id}.worker-{N}.memprofile.jsonl.zst`;
/// `task_id` may contain slashes (asm-tokenizer convention), in which
/// case the writer materialises the nested parents via
/// `create_dir_all`.
fn handle_assign(
    config: &MemProfileConfig,
    active: &mut HashMap<String, ActiveProfile>,
    task_id: String,
    worker_id: u32,
    subcgroup_dir: PathBuf,
    started_at: Instant,
) {
    let path = config
        .output_dir
        .join(format!("{task_id}.worker-{worker_id}.memprofile.jsonl.zst"));
    match JsonlZstdWriter::open(&path) {
        Ok(writer) => {
            active.insert(
                task_id,
                ActiveProfile {
                    worker_id,
                    subcgroup_dir,
                    started_at,
                    writer,
                },
            );
        }
        Err(e) => tracing::warn!(
            task_id,
            worker_id,
            error = %e,
            "memprofile: failed to open output file; not tracking",
        ),
    }
}

/// Finalise one task's writer. `task_id` not in the active map is a
/// no-op (legacy task or out-of-order events).
fn handle_complete(active: &mut HashMap<String, ActiveProfile>, task_id: &str) {
    if let Some(ap) = active.remove(task_id)
        && let Err(e) = ap.writer.close()
    {
        tracing::warn!(task_id, error = %e, "memprofile close failed");
    }
}

/// Finalise every writer attached to `worker_id`. Used on worker
/// pipe-EOF (crash, kernel-OOM, transport flake) when no
/// `TaskCompleted` will arrive.
fn handle_worker_disconnect(active: &mut HashMap<String, ActiveProfile>, worker_id: u32) {
    let to_drop: Vec<String> = active
        .iter()
        .filter(|(_, ap)| ap.worker_id == worker_id)
        .map(|(k, _)| k.clone())
        .collect();
    for tid in to_drop {
        if let Some(ap) = active.remove(&tid)
            && let Err(e) = ap.writer.close()
        {
            tracing::warn!(
                task_id = tid,
                worker_id,
                error = %e,
                "memprofile close failed on disconnect",
            );
        }
    }
}

/// Final flush: close every writer in the active map. Called once on
/// shutdown so the on-disk files end on a complete frame.
fn flush_all(active: &mut HashMap<String, ActiveProfile>) {
    for (task_id, ap) in active.drain() {
        if let Err(e) = ap.writer.close() {
            tracing::warn!(task_id, error = %e, "memprofile close failed on shutdown");
        }
    }
}
