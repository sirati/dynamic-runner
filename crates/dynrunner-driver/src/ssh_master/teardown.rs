//! `SshMaster::disconnect` and `add_forward`. Disconnect branches
//! on `is_spawned` per locked design point (b): spawn-master runs
//! the kill ladder (terminates the daemon); adopt-master only
//! cancels per-forward `ssh -O cancel -R` — partial cleanup, not
//! termination. `add_forward` registers a runtime reverse forward
//! on an adopt-master.

use std::process::Command;
use std::sync::atomic::Ordering;

use crate::error::SshMasterError;

use super::SshMaster;

impl SshMaster {
    /// Register a runtime-added reverse forward on an adopt-master.
    /// Issues `ssh -O forward -R 0.0.0.0:<remote>:localhost:<local>`
    /// against the control socket and, on success, records the pair
    /// in `forwarded_ports` so the `disconnect()`-time `ssh -O
    /// cancel` finds it.
    ///
    /// Spawn-master forwards are baked into the spawn argv; calling
    /// `add_forward` on a spawn-master would issue a duplicate
    /// registration and is rejected with [`SshMasterError::Other`].
    pub fn add_forward(&mut self, local_port: u16, remote_port: u16) -> Result<(), SshMasterError> {
        if self.is_spawned {
            return Err(SshMasterError::Other(
                "add_forward is only valid on adopt-master; \
                 spawn-master forwards are baked into the spawn argv via SshConfig"
                    .into(),
            ));
        }
        if self.is_invalidated() {
            return Err(self.master_died_err());
        }
        let cp_str = self.control_path.to_string_lossy().into_owned();
        let mut cmd = Command::new("ssh");
        for arg in &self.auth_flags {
            cmd.arg(arg);
        }
        cmd.args([
            "-O",
            "forward",
            "-o",
            &format!("ControlPath={cp_str}"),
            "-R",
            &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
        ]);
        cmd.arg(self.target.as_str());
        let out = cmd
            .output()
            .map_err(|e| SshMasterError::Other(format!("ssh -O forward spawn: {e}")))?;
        if !out.status.success() {
            return Err(SshMasterError::Other(format!(
                "ssh -O forward {remote_port}:localhost:{local_port} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        self.forwarded_ports.push((local_port, remote_port));
        Ok(())
    }

    /// Tear the master down (spawn-master) or release per-forward
    /// state (adopt-master).
    ///
    /// **Spawn-master**: cancel the watcher, run `ssh -O exit`
    /// (polite), then SIGTERM/SIGKILL ladder via `nix::kill`.
    /// Surfaces [`SshMasterError::UnkillableMaster`] if even SIGKILL
    /// doesn't take.
    ///
    /// **Adopt-master**: per [`Self::forwarded_ports`] entry, run
    /// `ssh -O cancel -R 0.0.0.0:<remote>:localhost:<local>`. The
    /// master itself is *not* killed — it belongs to the upstream
    /// driver. Per locked design point (b): partial cleanup, not
    /// termination.
    ///
    /// Idempotent. Calling after a successful disconnect (or after
    /// watcher-observed invalidation) is a no-op (locked point
    /// (h.2)).
    pub fn disconnect(&mut self) -> Result<(), SshMasterError> {
        // Post-invalidation: no-op (h.2). Cancel the watcher first
        // in case Drop runs after this — keeps the cancel/teardown
        // ordering invariant from getting violated.
        if self.is_invalidated() {
            self.watcher_cancel.store(true, Ordering::SeqCst);
            if let Some(t) = self.watcher_thread.take() {
                let _ = t.join();
            }
            return Ok(());
        }

        if self.is_spawned {
            self.disconnect_spawn_master()
        } else {
            self.disconnect_adopt_master()
        }
    }

    fn disconnect_spawn_master(&mut self) -> Result<(), SshMasterError> {
        // Signal the watcher to exit silently. Anything from this
        // point forward is an *expected* teardown; the observer must
        // not log it as unexpected. The watcher thread observes the
        // flag on its next 1s poll and returns. We join the thread
        // *after* the daemon is down so it has its expected exit
        // condition (either flag-set or daemon-gone, whichever wins).
        self.watcher_cancel.store(true, Ordering::SeqCst);

        // Politely ask the master to exit via `ssh -O exit` first,
        // which cleans up the control socket and reverse forwards in
        // an orderly fashion. The control-socket request lands at
        // the daemon (the launcher is long gone by now).
        if let Some(cp_str) = self.control_path.to_str() {
            let mut cmd = Command::new("ssh");
            for arg in &self.auth_flags {
                cmd.arg(arg);
            }
            if self.port != 22 {
                cmd.args(["-p", &self.port.to_string()]);
            }
            cmd.args(["-O", "exit", "-o", &format!("ControlPath={cp_str}")]);
            cmd.arg(self.target.as_str());
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
            let _ = cmd.status();
        }

        // Wait for the daemon to actually exit, with SIGTERM/SIGKILL
        // fallback. Bounded at ~250ms total.
        let result = if let Some(pid) = self.daemon_pid.take() {
            self.run_kill_ladder(pid)
        } else {
            Ok(())
        };

        // Join the watcher thread (sync — it observes the cancel
        // flag on its next 1s poll). This is bounded at ~1s.
        if let Some(t) = self.watcher_thread.take() {
            let _ = t.join();
        }

        // Mark invalidated regardless of ladder outcome: post-
        // disconnect, the master is gone (or so unkillable that
        // further attempts won't help). Either way operations after
        // this should not target the daemon.
        self.invalidated.store(true, Ordering::SeqCst);
        // On clean teardown, last_known_pid is no longer interesting
        // — clear it so `master_pid()` returns None for the "we
        // explicitly tore it down" case (distinguishing from the
        // watcher-observed-death case where last_known_pid is still
        // Some).
        if result.is_ok() {
            self.last_known_pid = None;
        }

        result
    }

    fn disconnect_adopt_master(&mut self) -> Result<(), SshMasterError> {
        // Per locked point (b): adopt-master `disconnect()` runs
        // `ssh -O cancel -R …` per forwarded_ports entry. We do NOT
        // kill the daemon. We do NOT issue `ssh -O exit`.
        let cp_str = self.control_path.to_string_lossy().into_owned();
        let mut last_err: Option<SshMasterError> = None;
        for &(local_port, remote_port) in &self.forwarded_ports {
            let mut cmd = Command::new("ssh");
            for arg in &self.auth_flags {
                cmd.arg(arg);
            }
            cmd.args([
                "-O",
                "cancel",
                "-o",
                &format!("ControlPath={cp_str}"),
                "-R",
                &format!("0.0.0.0:{remote_port}:localhost:{local_port}"),
            ]);
            cmd.arg(self.target.as_str());
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
            match cmd.output() {
                Ok(o) if o.status.success() => continue,
                Ok(o) => {
                    last_err = Some(SshMasterError::Other(format!(
                        "ssh -O cancel -R {remote_port}:localhost:{local_port} \
                         exited {}: {}",
                        o.status.code().unwrap_or(-1),
                        String::from_utf8_lossy(&o.stderr).trim()
                    )));
                }
                Err(e) => {
                    last_err = Some(SshMasterError::Other(format!("ssh -O cancel spawn: {e}")));
                }
            }
        }
        // Whether or not the cancels succeeded, mark this handle as
        // invalidated so further calls are no-ops.
        self.invalidated.store(true, Ordering::SeqCst);
        match last_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
