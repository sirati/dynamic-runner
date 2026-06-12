//! Reads `memory.current`, `memory.swap.current`, `memory.stat` from a
//! cgroup-v2 leaf directory.
//!
//! These are three small pseudo-files (kernel-served, no I/O latency
//! beyond a syscall). Each sampler tick reads all three; if any read
//! fails the sampler drops the sample for that tick and continues.
//!
//! Single concern: turn three sysfs files into one [`CgroupSample`].
//! The reader knows nothing about timing, file paths beyond the leaf
//! it was handed, or what the sampler does with the result.
//!
//! Per the plan, the kernel may emit any number of `memory.stat` keys
//! and the reader preserves them verbatim in a [`BTreeMap`] — no
//! allowlist, no filtering — so future-kernel additions land in the
//! profile without a code change here.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use super::error::MemProfileError;

/// One snapshot read from a worker's cgroup-v2 leaf.
///
/// `memory_stat` is ordered (`BTreeMap`) so downstream JSON
/// serialisation is deterministic; `Sample` (see `memprofile::sample`)
/// embeds this same map shape into the on-disk schema.
#[derive(Debug, Clone)]
pub struct CgroupSample {
    /// Bytes of resident memory charged to this cgroup
    /// (`memory.current`).
    pub memory_current: u64,
    /// Bytes of swap charged to this cgroup (`memory.swap.current`).
    /// Reported as `0` when the swap controller is not enabled on the
    /// host — see [`read`] for the fallback contract.
    pub swap_current: u64,
    /// Full verbatim parse of `memory.stat`. Keys are whatever the
    /// kernel emits; no allowlist applied.
    pub memory_stat: BTreeMap<String, u64>,
}

/// Read all three sysfs files from `cgroup_dir`. Returns a
/// [`MemProfileError`] if any read or parse fails — the caller drops
/// the sample for that tick.
///
/// `memory.swap.current` may legitimately be absent on hosts without
/// swap accounting enabled (the kernel does not create the file when
/// the `memory.swap` controller is not enabled in the parent's
/// `subtree_control`). [`io::ErrorKind::NotFound`] on that ONE file is
/// treated as "0 bytes" and is not an error. Any other I/O error from
/// any of the three files is surfaced as [`MemProfileError::Io`].
pub fn read(cgroup_dir: &Path) -> Result<CgroupSample, MemProfileError> {
    let (memory_current, swap_current) = read_charge(cgroup_dir)?;
    let memory_stat = read_stat(cgroup_dir)?;

    Ok(CgroupSample {
        memory_current,
        swap_current,
        memory_stat,
    })
}

/// Read just the `(memory.current, memory.swap.current)` pair from a
/// cgroup-v2 leaf. Shared primitive between the memprofile sampler
/// (via [`read`]) and the worker-charge accounting in
/// [`crate::monitor`] — one owner for the "what does this cgroup
/// currently hold in RAM + swap" read so the two consumers cannot
/// drift. Missing `memory.swap.current` (host without swap
/// accounting) reads as `0`, same contract as [`read`].
pub(crate) fn read_charge(cgroup_dir: &Path) -> Result<(u64, u64), MemProfileError> {
    let memory_current = read_scalar(cgroup_dir, "memory.current")?;
    let swap_current = read_swap_current(cgroup_dir)?;
    Ok((memory_current, swap_current))
}

/// Read a whole-file unsigned-integer pseudo-file (`memory.current`,
/// `memory.swap.current`). Trims trailing whitespace before parsing.
fn read_scalar(cgroup_dir: &Path, file_name: &str) -> Result<u64, MemProfileError> {
    let path = cgroup_dir.join(file_name);
    let raw = fs::read_to_string(&path).map_err(|e| MemProfileError::io(&path, e))?;
    parse_scalar(&path, &raw)
}

/// Parse a trimmed scalar pseudo-file into a `u64`. Lifted out of
/// [`read_scalar`] so the parse failure path has one home — the
/// fallback used by [`read_swap_current`] reuses it on the
/// `NotFound` branch via the same parse-error shape.
fn parse_scalar(path: &Path, raw: &str) -> Result<u64, MemProfileError> {
    let trimmed = raw.trim();
    trimmed.parse::<u64>().map_err(|e| {
        MemProfileError::parse(path, None, format!("expected u64, got {trimmed:?}: {e}"))
    })
}

/// Read `memory.swap.current`, treating absence (host without swap
/// accounting enabled) as `0` per the documented fallback. Any other
/// I/O error is propagated.
fn read_swap_current(cgroup_dir: &Path) -> Result<u64, MemProfileError> {
    let path = cgroup_dir.join("memory.swap.current");
    match fs::read_to_string(&path) {
        Ok(raw) => parse_scalar(&path, &raw),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(MemProfileError::io(&path, e)),
    }
}

/// Read `memory.stat` into a [`BTreeMap`]. Each non-empty line is a
/// space-delimited `"<key> <value>"`; unknown keys are preserved.
fn read_stat(cgroup_dir: &Path) -> Result<BTreeMap<String, u64>, MemProfileError> {
    let path = cgroup_dir.join("memory.stat");
    let raw = fs::read_to_string(&path).map_err(|e| MemProfileError::io(&path, e))?;
    parse_stat(&path, &raw)
}

/// Parse the body of a `memory.stat` file. Empty lines are skipped;
/// any malformed line returns a [`MemProfileError::Parse`] tagged
/// with the 1-indexed line number for operator debugging.
fn parse_stat(path: &Path, raw: &str) -> Result<BTreeMap<String, u64>, MemProfileError> {
    let mut out = BTreeMap::new();
    for (idx, line) in raw.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let key = parts
            .next()
            .ok_or_else(|| MemProfileError::parse(path, Some(line_no), "missing key"))?;
        let value_str = parts.next().ok_or_else(|| {
            MemProfileError::parse(
                path,
                Some(line_no),
                format!("missing value for key {key:?}"),
            )
        })?;
        let value: u64 = value_str.parse().map_err(|e| {
            MemProfileError::parse(
                path,
                Some(line_no),
                format!("expected u64 for key {key:?}, got {value_str:?}: {e}"),
            )
        })?;
        out.insert(key.to_string(), value);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Write a representative cgroup-v2 memory.stat block; the keys
    /// are real ones from a recent kernel, the values are arbitrary.
    const TYPICAL_MEMORY_STAT: &str = "\
anon 4500000000
file 100000000
kernel 16777216
kernel_stack 1048576
pagetables 8388608
percpu 524288
sock 0
shmem 0
file_mapped 95000000
file_dirty 0
file_writeback 0
swapcached 0
anon_thp 0
file_thp 0
shmem_thp 0
inactive_anon 0
active_anon 4500000000
inactive_file 50000000
active_file 200000000
unevictable 0
slab_reclaimable 8000000
slab_unreclaimable 8777216
slab 16777216
workingset_refault_anon 0
workingset_refault_file 12
workingset_activate_anon 0
workingset_activate_file 4
workingset_restore_anon 0
workingset_restore_file 0
workingset_nodereclaim 0
pgfault 12345
pgmajfault 12
pgrefill 0
pgscan 0
pgsteal 0
pgactivate 0
pgdeactivate 0
pglazyfree 0
pglazyfreed 0
thp_fault_alloc 0
thp_collapse_alloc 0
";

    /// Write all three files into `dir` with the supplied contents.
    /// Helper keeps each test focused on the parse / error case
    /// instead of repeating sysfs setup.
    fn write_cgroup(
        dir: &Path,
        memory_current: Option<&str>,
        swap_current: Option<&str>,
        memory_stat: Option<&str>,
    ) {
        if let Some(c) = memory_current {
            fs::write(dir.join("memory.current"), c).unwrap();
        }
        if let Some(s) = swap_current {
            fs::write(dir.join("memory.swap.current"), s).unwrap();
        }
        if let Some(s) = memory_stat {
            fs::write(dir.join("memory.stat"), s).unwrap();
        }
    }

    #[test]
    fn reads_concrete_values() {
        let dir = tempdir().unwrap();
        write_cgroup(
            dir.path(),
            Some("5368709120\n"),
            Some("0\n"),
            Some(TYPICAL_MEMORY_STAT),
        );

        let sample = read(dir.path()).expect("read should succeed");

        assert_eq!(sample.memory_current, 5_368_709_120);
        assert_eq!(sample.swap_current, 0);
        assert_eq!(sample.memory_stat.get("anon"), Some(&4_500_000_000));
        assert_eq!(sample.memory_stat.get("file"), Some(&100_000_000));
        assert_eq!(sample.memory_stat.get("pgfault"), Some(&12_345));
        assert_eq!(sample.memory_stat.get("pgmajfault"), Some(&12));
        assert_eq!(sample.memory_stat.get("kernel_stack"), Some(&1_048_576));
    }

    #[test]
    fn preserves_unknown_stat_keys() {
        let dir = tempdir().unwrap();
        // Mix kernel-known keys with a synthetic future-key the
        // reader has no business filtering.
        let stat = "\
anon 100
synthetic_future_key 42
file 200
";
        write_cgroup(dir.path(), Some("123\n"), Some("0\n"), Some(stat));

        let sample = read(dir.path()).expect("read should succeed");

        assert_eq!(sample.memory_stat.get("synthetic_future_key"), Some(&42));
        assert_eq!(sample.memory_stat.get("anon"), Some(&100));
        assert_eq!(sample.memory_stat.get("file"), Some(&200));
    }

    #[test]
    fn alphabetic_stat_order() {
        let dir = tempdir().unwrap();
        // Lines deliberately in non-alphabetic order to prove the
        // BTreeMap re-sorts on iteration.
        let stat = "\
zebra 1
apple 2
mango 3
banana 4
";
        write_cgroup(dir.path(), Some("0\n"), Some("0\n"), Some(stat));

        let sample = read(dir.path()).expect("read should succeed");

        let keys: Vec<&str> = sample.memory_stat.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["apple", "banana", "mango", "zebra"]);
    }

    #[test]
    fn missing_swap_current_treats_as_zero() {
        let dir = tempdir().unwrap();
        // Deliberately do NOT write memory.swap.current.
        write_cgroup(dir.path(), Some("777\n"), None, Some("anon 1\nfile 2\n"));

        let sample = read(dir.path()).expect("read should succeed without swap file");

        assert_eq!(sample.memory_current, 777);
        assert_eq!(sample.swap_current, 0);
        assert_eq!(sample.memory_stat.get("anon"), Some(&1));
    }

    #[test]
    fn io_error_propagates() {
        // Path does not exist; the first file the reader touches is
        // memory.current, so we should see that path in the error.
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");

        let err = read(&missing).expect_err("missing dir must error");

        match err {
            MemProfileError::Io { path, .. } => {
                assert!(
                    path.ends_with("memory.current"),
                    "expected path to end with memory.current, got {path:?}",
                );
            }
            other => panic!("expected Io variant, got {other:?}"),
        }
    }

    #[test]
    fn malformed_stat_line_propagates() {
        let dir = tempdir().unwrap();
        // Third line is malformed (non-numeric value); reader should
        // tag that 1-indexed line number.
        let stat = "\
anon 100
file 200
foo notanumber
shmem 0
";
        write_cgroup(dir.path(), Some("1\n"), Some("0\n"), Some(stat));

        let err = read(dir.path()).expect_err("malformed stat must error");

        match err {
            MemProfileError::Parse { line, path, .. } => {
                assert_eq!(line, Some(3));
                assert!(
                    path.ends_with("memory.stat"),
                    "expected path to end with memory.stat, got {path:?}",
                );
            }
            other => panic!("expected Parse variant, got {other:?}"),
        }
    }
}
