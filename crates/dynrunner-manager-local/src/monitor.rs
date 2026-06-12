//! Resource-usage monitor abstraction + the per-worker memory-charge
//! reader the manager's accounting consumes.
//!
//! Single concern: measure how much memory ONE worker is charged
//! with, where "charged" = resident + swapped-out bytes. Swap is
//! PRESSURE, not relief: as a dying worker's pages migrate to swap
//! its RSS shrinks, and an RSS-only reading tells the kill decision
//! the worker freed memory at exactly the moment it is thrashing.
//! [`MemoryCharge::charged_bytes`] is the single owner of that
//! accounting rule; everything downstream (scheduler pressure check,
//! OOM-watcher log sums, memuse rows) consumes the one charged
//! number.
//!
//! Source preference:
//!   1. The worker's cgroup-v2 leaf (`memory.current` +
//!      `memory.swap.current`) when the pool materialised one — this
//!      covers the worker's whole subtree (JVM/Ghidra children
//!      included), read via the shared
//!      [`crate::memprofile::cgroup_reader`] primitive.
//!   2. Fallback (flat-cgroup environments): `/proc/<pid>/statm`
//!      resident + `/proc/<pid>/status` `VmSwap` — per-process only,
//!      but still swap-aware.

use std::path::Path;

use dynrunner_core::{ResourceKind, ResourceMap};

/// One worker's memory charge: the resident and swapped-out
/// components, separately. Constructed by [`measure_worker_charge`];
/// consumed by `WorkerHandle::update_resource_usage`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryCharge {
    /// Bytes resident in RAM (cgroup `memory.current`, or statm RSS
    /// in the per-process fallback).
    pub resident_bytes: u64,
    /// Bytes swapped out (cgroup `memory.swap.current`, or
    /// `/proc/<pid>/status` `VmSwap` in the fallback). `0` on hosts
    /// without swap accounting.
    pub swap_bytes: u64,
}

impl MemoryCharge {
    /// The accounting rule, owned here: charged = resident + swap.
    /// Pages migrating RAM→swap keep the charge constant; swap
    /// growth on top of resident growth reads as growth.
    pub fn charged_bytes(&self) -> u64 {
        self.resident_bytes.saturating_add(self.swap_bytes)
    }

    /// Scheduler-facing shape: the memory kind carries the CHARGED
    /// bytes (the decision input), preserving the legacy "empty map
    /// when nothing measured" contract.
    pub fn to_resource_map(&self) -> ResourceMap {
        let charged = self.charged_bytes();
        if charged > 0 {
            ResourceMap::from([(ResourceKind::memory(), charged)])
        } else {
            ResourceMap::new()
        }
    }
}

/// Measure one worker's [`MemoryCharge`]. Prefers the worker's
/// cgroup-v2 leaf (whole-subtree accounting); falls back to the
/// per-process `/proc` files when no leaf exists or the leaf read
/// fails (e.g. torn down mid-restart). Returns a zero charge when
/// nothing is measurable — same disposition the legacy RSS-only
/// reader had.
pub fn measure_worker_charge(pid: Option<u32>, cgroup_dir: Option<&Path>) -> MemoryCharge {
    if let Some(dir) = cgroup_dir
        && let Ok((resident, swap)) = crate::memprofile::cgroup_reader::read_charge(dir)
    {
        return MemoryCharge {
            resident_bytes: resident,
            swap_bytes: swap,
        };
    }
    MemoryCharge {
        resident_bytes: ProcStatmMonitor::read_rss(pid),
        swap_bytes: read_proc_status_vm_swap(pid),
    }
}

/// Read `VmSwap` (bytes) from `/proc/<pid>/status`. `0` on any
/// failure (non-Linux, dead pid, kernel without swap accounting —
/// the field is simply absent then).
fn read_proc_status_vm_swap(pid: Option<u32>) -> u64 {
    #[cfg(target_os = "linux")]
    {
        let Some(pid) = pid else { return 0 };
        let Ok(contents) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
            return 0;
        };
        contents
            .lines()
            .find_map(|line| crate::oom::probe::parse_meminfo_line(line, "VmSwap:"))
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        0
    }
}

/// Trait for measuring resource usage of a worker process.
pub trait ResourceMonitor {
    fn measure(&self, pid: Option<u32>) -> ResourceMap;
}

/// Default implementation that reads RSS from `/proc/[pid]/statm`.
pub struct ProcStatmMonitor;

impl ResourceMonitor for ProcStatmMonitor {
    fn measure(&self, pid: Option<u32>) -> ResourceMap {
        let mem = Self::read_rss(pid);
        if mem > 0 {
            ResourceMap::from([(ResourceKind::memory(), mem)])
        } else {
            ResourceMap::new()
        }
    }
}

impl ProcStatmMonitor {
    fn read_rss(pid: Option<u32>) -> u64 {
        #[cfg(target_os = "linux")]
        {
            let pid = match pid {
                Some(p) => p,
                None => return 0,
            };
            let path = format!("/proc/{pid}/statm");
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    // statm format: size resident shared text lib data dt
                    // We want the second field (resident) in pages
                    if let Some(rss_pages_str) = contents.split_whitespace().nth(1)
                        && let Ok(rss_pages) = rss_pages_str.parse::<u64>()
                    {
                        let page_size = 4096u64; // standard Linux page size
                        return rss_pages * page_size;
                    }
                    0
                }
                Err(_) => 0,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write a fake cgroup-v2 leaf with the given `memory.current` /
    /// `memory.swap.current` bodies.
    fn write_leaf(dir: &Path, current: &str, swap: &str) {
        std::fs::write(dir.join("memory.current"), current).unwrap();
        std::fs::write(dir.join("memory.swap.current"), swap).unwrap();
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    /// THE swap-blindness pin: pages migrating from RAM to swap must
    /// read as (at least) constant charge, and swap growing past the
    /// RSS shrink must read as GROWTH — the kill decision consumes
    /// `charged_bytes` and must see pressure, not relief.
    #[test]
    fn rss_shrinks_while_swap_grows_reads_as_pressure() {
        let dir = tempfile::tempdir().unwrap();

        // Healthy: 4 GiB resident, nothing swapped.
        write_leaf(dir.path(), "4294967296\n", "0\n");
        let before = measure_worker_charge(None, Some(dir.path()));
        assert_eq!(before.resident_bytes, 4 * GIB);
        assert_eq!(before.swap_bytes, 0);
        assert_eq!(before.charged_bytes(), 4 * GIB);

        // Dying: RSS collapsed to 1 GiB while 5 GiB migrated to swap.
        write_leaf(dir.path(), "1073741824\n", "5368709120\n");
        let after = measure_worker_charge(None, Some(dir.path()));
        assert_eq!(after.resident_bytes, GIB);
        assert_eq!(after.swap_bytes, 5 * GIB);
        assert_eq!(after.charged_bytes(), 6 * GIB);
        assert!(
            after.charged_bytes() > before.charged_bytes(),
            "swap growth past the RSS shrink must read as growing charge"
        );

        // The scheduler-facing map carries the charged number.
        let mem = ResourceKind::memory();
        assert_eq!(after.to_resource_map().get(&mem), 6 * GIB);
    }

    /// Missing `memory.swap.current` (host without swap accounting)
    /// reads as swap=0 — same contract the memprofile reader pins.
    #[test]
    fn missing_swap_file_reads_zero_swap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("memory.current"), "777\n").unwrap();
        let charge = measure_worker_charge(None, Some(dir.path()));
        assert_eq!(charge.resident_bytes, 777);
        assert_eq!(charge.swap_bytes, 0);
    }

    /// An unreadable leaf (torn down mid-restart) falls back to the
    /// per-process `/proc` path rather than reporting a phantom zero
    /// while the process is alive.
    #[test]
    #[cfg(target_os = "linux")]
    fn unreadable_leaf_falls_back_to_proc() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("removed-leaf");
        let self_pid = std::process::id();
        let charge = measure_worker_charge(Some(self_pid), Some(&gone));
        // Our own test process is certainly resident.
        assert!(
            charge.resident_bytes > 0,
            "fallback must read the live process's statm RSS"
        );
    }

    /// No pid + no leaf → zero charge → empty resource map (the
    /// legacy "nothing measured" contract).
    #[test]
    fn nothing_measurable_yields_empty_map() {
        let charge = measure_worker_charge(None, None);
        assert_eq!(charge, MemoryCharge::default());
        assert!(charge.to_resource_map().is_empty());
    }

    /// `charged_bytes` saturates instead of overflowing on absurd
    /// kernel readings.
    #[test]
    fn charged_saturates() {
        let charge = MemoryCharge {
            resident_bytes: u64::MAX,
            swap_bytes: GIB,
        };
        assert_eq!(charge.charged_bytes(), u64::MAX);
    }
}
