//! Resource-usage monitor abstraction + the default
//! `/proc/[pid]/statm` implementation.

use dynrunner_core::{ResourceKind, ResourceMap};

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
                    if let Some(rss_pages_str) = contents.split_whitespace().nth(1) {
                        if let Ok(rss_pages) = rss_pages_str.parse::<u64>() {
                            let page_size = 4096u64; // standard Linux page size
                            return rss_pages * page_size;
                        }
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
