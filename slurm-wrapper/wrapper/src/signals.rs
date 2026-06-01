//! Single concern: signal provenance via signalfd(2) — the headline new
//! capability. Block the catchable signal set, read each delivery, and
//! log {monotonic+wall ts, signo, ssi_pid, ssi_uid, ssi_code
//! (SI_USER/SI_KERNEL/SI_TKILL/...), comm(ssi_pid), cmdline(ssi_pid)}.
//! Phase 1 (1J) implements.

/// Owns the signalfd monitor task. Phase 1 (1J) defines internals and
/// the install/spawn API shape.
pub struct SignalMonitor {
    // Phase 1 (1J) defines internals.
}

/// Block the signal set and start the provenance-logging monitor. MUST
/// run BEFORE any child is spawned so the blocked-signal mask and the
/// process-group leadership are inherited correctly.
pub fn install() -> std::io::Result<SignalMonitor> {
    todo!("1J: block signals + spawn signalfd provenance monitor")
}
