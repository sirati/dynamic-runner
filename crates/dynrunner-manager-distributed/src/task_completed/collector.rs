//! Windowed failure-collector primitive.
//!
//! # Single concern
//!
//! Subscribe to terminal-failure events on the existing
//! [`TaskCompletedListener`] fabric and collect the failures observed
//! inside a TIME-BOUNDED window, deduped by their carried `last_error`
//! message, then hand the collected set to a terminal action when the
//! window elapses.
//!
//! This is ONE primitive with two thin policies layered on top (each in
//! its owning module, `crate::observer::failure_response`): the invalid_task monitor
//! (1-minute window → fatal-exit signal, one-shot) and the error
//! aggregator (rolling-10-minute window, collect `min(1min, remainder)`,
//! dedup-print, no exit, reset per rolling window). NOTHING here knows
//! about invalid_task, exit codes, or the importance channel — those are
//! the policies' concern, injected through [`CollectorPolicy`].
//!
//! # Why a listener + a driver task (not a pure listener)
//!
//! The [`TaskCompletedListener`] surface is synchronous and event-driven
//! — `on_event` fires only when a task terminates, and it carries no
//! clock. A windowed collector needs a TIMER: the window must elapse on
//! its own, even if no further failure arrives. So the primitive splits
//! into two halves over one shared [`CollectorState`]:
//!
//! * the **listener** ([`CollectorListener`]) runs on the
//!   task-completed dispatcher task; it filters + dedups each event and,
//!   when a fresh window arms, pokes the driver with the new deadline;
//! * the **driver** ([`run_collector`]) owns the timer; it sleeps until
//!   the armed deadline, drains the collected set, resets the window,
//!   and runs the policy's action.
//!
//! Both halves read the virtual `tokio::time` clock, so a `start_paused`
//! test advances the window deterministically with zero wall-clock race.
//!
//! # Dedup key (owner-decision C-5)
//!
//! Failures dedup on the carried `last_error` MESSAGE string. Two
//! failures with the same `error_kind` but different messages are
//! distinct events; two with the same message collapse into one entry
//! with a repeat count (`xN other tasks`). The message rides
//! [`TaskCompletedEvent::last_error`] (added for exactly this) so the
//! collector never re-reads the CRDT out of band. A failure with NO
//! message (`last_error == None`) keys on the empty string — all such
//! failures collapse together, which is the correct "indistinguishable
//! cause" behaviour.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;

use super::event::TaskCompletedEvent;
use super::listener::TaskCompletedListener;

/// One deduped failure collected inside a window: the representative
/// event (the FIRST one observed with this message) plus how many later
/// tasks failed with the SAME message. `repeat_count` is the number of
/// ADDITIONAL tasks (so a singleton failure has `repeat_count == 0`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectedFailure {
    /// The first event observed carrying this `last_error` message. Its
    /// `task_id` / `error_kind` / `last_error` are the representative the
    /// action renders in detail.
    pub representative: TaskCompletedEvent,
    /// Count of ADDITIONAL tasks that failed with the identical
    /// `last_error` message during the window. `0` for a singleton; the
    /// aggregator renders `xN other tasks` when `> 0`.
    pub repeat_count: usize,
}

impl CollectedFailure {
    /// Render one detail line for this failure:
    /// `  - task <id> [<kind>]: <message>( (xN other tasks))?`.
    ///
    /// `default_kind` is the label used when the representative carries no
    /// `error_kind` — the ONE knob that varies between consuming policies
    /// (the aggregator uses `"<unknown>"`; the invalid-task monitor uses
    /// `"invalid_task:"`). Everything else — message fallback, repeat
    /// suffix, layout — is identical across consumers, so it lives here on
    /// the owning type rather than being copied into each policy.
    pub fn render_detail_line(&self, default_kind: &str) -> String {
        let msg = self
            .representative
            .last_error
            .as_deref()
            .unwrap_or("<no message>");
        let kind = self
            .representative
            .error_kind
            .as_deref()
            .unwrap_or(default_kind);
        let repeat = if self.repeat_count > 0 {
            format!(" (x{} other tasks)", self.repeat_count)
        } else {
            String::new()
        };
        format!(
            "  - task {} [{}]: {}{}",
            self.representative.task_id, kind, msg, repeat
        )
    }
}

/// The window/filter/action policy a [`WindowedFailureCollector`] is
/// parameterised by. The primitive owns the window mechanics; the policy
/// owns WHICH failures count, HOW LONG the window is, and WHAT happens
/// when it elapses — the three knobs that distinguish the invalid_task
/// monitor from the error aggregator.
pub trait CollectorPolicy: Send {
    /// Filter: does this terminal-failure event participate in the
    /// window? The invalid_task monitor matches only `invalid_task:*`
    /// failures; the aggregator matches every failure. Successful
    /// completions never reach here (the collector drops `success` events
    /// before consulting the policy), so implementors only decide among
    /// failures.
    fn matches(&self, event: &TaskCompletedEvent) -> bool;

    /// The collection-window duration to use when a fresh window arms at
    /// `now`. A constant for the invalid_task monitor (1 min); a
    /// `min(1min, rolling-window remainder)` computation for the
    /// aggregator. Called once per arm, at the instant the first matching
    /// failure of a window is recorded.
    ///
    /// `&mut self` so a rolling-window policy can fold its rollover
    /// bookkeeping (detect a new 10-min window, reset its per-window
    /// reported-message memory) into the same arm-time call — the natural
    /// single point at which the policy learns a fresh sub-window is
    /// starting.
    fn window_for(&mut self, now: Instant) -> std::time::Duration;

    /// Run the terminal action over the failures collected in the just-
    /// elapsed window (first-seen order, deduped, with repeat counts).
    /// The invalid_task monitor fires its fatal-exit signal here; the
    /// aggregator emits the deduped report on the importance channel.
    /// Never called with an empty `collected` (the driver only fires when
    /// at least one matching failure armed the window).
    fn on_window_elapsed(&mut self, collected: Vec<CollectedFailure>, now: Instant);

    /// Whether the collector should keep accepting failures after a
    /// window fires. The invalid_task monitor is ONE-SHOT (`false`: after
    /// the fatal-exit signal there is nothing more to do); the aggregator
    /// rolls forever (`true`). When `false`, post-fire events are dropped
    /// and no further window ever arms.
    fn rearm_after_fire(&self) -> bool {
        true
    }
}

/// Mutable window state shared between the listener half and the driver
/// half. Guarded by a `Mutex` because the two halves run on different
/// tasks; every critical section is O(distinct-messages) and lock-free of
/// any await, so it never blocks the dispatcher.
struct CollectorState {
    /// `last_error message → CollectedFailure`, preserving first-seen
    /// order via `insertion_order`. A `HashMap` for O(1) dedup; order is
    /// tracked separately so the action renders failures in arrival
    /// order (the first distinct message reads first).
    deduped: HashMap<String, CollectedFailure>,
    /// Distinct messages in first-seen order. Indexes into `deduped` on
    /// drain so the rendered report is stable + arrival-ordered.
    insertion_order: Vec<String>,
    /// Deadline of the currently-armed window, or `None` when idle (no
    /// window armed; the next matching failure arms a fresh one).
    armed_until: Option<Instant>,
    /// Latched once a one-shot collector has fired. Drops every later
    /// event so no second window can arm. Always `false` for a rolling
    /// (re-arming) policy.
    closed: bool,
}

impl CollectorState {
    fn new() -> Self {
        Self {
            deduped: HashMap::new(),
            insertion_order: Vec::new(),
            armed_until: None,
            closed: false,
        }
    }

    /// Fold one matching failure into the dedup map. Returns `true` iff
    /// this call ARMED a fresh window (was idle, now collecting) so the
    /// caller knows to poke the driver with the new deadline.
    fn record(&mut self, event: &TaskCompletedEvent, deadline: Instant) -> bool {
        let key = event.last_error.clone().unwrap_or_default();
        match self.deduped.get_mut(&key) {
            Some(existing) => {
                existing.repeat_count += 1;
            }
            None => {
                self.insertion_order.push(key.clone());
                self.deduped.insert(
                    key,
                    CollectedFailure {
                        representative: event.clone(),
                        repeat_count: 0,
                    },
                );
            }
        }
        if self.armed_until.is_none() {
            self.armed_until = Some(deadline);
            true
        } else {
            false
        }
    }

    /// Drain the collected failures in first-seen order and reset the
    /// window to idle. `rearm` controls whether a later failure may arm a
    /// new window: a one-shot policy passes `false`, latching `closed`.
    fn drain(&mut self, rearm: bool) -> Vec<CollectedFailure> {
        let mut out = Vec::with_capacity(self.insertion_order.len());
        for key in self.insertion_order.drain(..) {
            if let Some(failure) = self.deduped.remove(&key) {
                out.push(failure);
            }
        }
        self.deduped.clear();
        self.armed_until = None;
        if !rearm {
            self.closed = true;
        }
        out
    }
}

/// The listener half: filters + dedups each terminal-failure event into
/// the shared [`CollectorState`] and pokes the driver when a fresh window
/// arms. Holds the policy ONLY to consult its `matches` filter +
/// `window_for` duration; the action lives on the driver side so the
/// dispatcher task (which runs `on_event`) never executes policy effects.
struct CollectorListener<P: CollectorPolicy> {
    state: Arc<Mutex<CollectorState>>,
    policy: Arc<Mutex<P>>,
    /// Pokes the driver with the deadline of a freshly-armed window. The
    /// driver re-reads the authoritative deadline off the shared state;
    /// this is purely the wakeup edge.
    wake: UnboundedSender<()>,
}

impl<P: CollectorPolicy> TaskCompletedListener for CollectorListener<P> {
    fn on_event(&self, event: &TaskCompletedEvent) {
        // Successful completions never participate.
        if event.success {
            return;
        }
        // Consult the filter (e.g. invalid_task-only) before touching the
        // window state.
        if !self
            .policy
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .matches(event)
        {
            return;
        }
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed {
            return;
        }
        let armed_fresh = if state.armed_until.is_none() {
            // Compute the window duration at arm-time (policy may make it
            // depend on `now`, e.g. the rolling-window remainder).
            let window = self
                .policy
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .window_for(now);
            state.record(event, now + window)
        } else {
            state.record(event, now /* unused: window already armed */)
        };
        drop(state);
        if armed_fresh {
            // Edge-trigger the driver to (re)schedule its sleep against
            // the new deadline. Unbounded + best-effort: a dropped driver
            // (post-cancel) makes this a no-op.
            let _ = self.wake.send(());
        }
    }
}

/// Build a windowed failure-collector over `policy`. Returns:
/// * the [`TaskCompletedListener`] to register via
///   `register_task_completed_listener`, and
/// * the driver future to spawn on the runtime ([`run_collector`]).
///
/// The two share one [`CollectorState`] + the wake channel. Splitting
/// construction this way keeps the registration site (which only needs
/// the `Box<dyn TaskCompletedListener>`) and the spawn site (which needs
/// the driver future) decoupled.
pub fn windowed_failure_collector<P: CollectorPolicy + 'static>(
    policy: P,
) -> (Box<dyn TaskCompletedListener>, CollectorDriver<P>) {
    let state = Arc::new(Mutex::new(CollectorState::new()));
    let policy = Arc::new(Mutex::new(policy));
    let (wake_tx, wake_rx) = unbounded_channel();
    let listener = CollectorListener {
        state: Arc::clone(&state),
        policy: Arc::clone(&policy),
        wake: wake_tx,
    };
    let driver = CollectorDriver {
        state,
        policy,
        wake: wake_rx,
    };
    (Box::new(listener), driver)
}

/// The driver half: owns the window timer. Spawn [`Self::run`] on the
/// runtime concurrently with the dispatcher.
pub struct CollectorDriver<P: CollectorPolicy> {
    state: Arc<Mutex<CollectorState>>,
    policy: Arc<Mutex<P>>,
    wake: UnboundedReceiver<()>,
}

impl<P: CollectorPolicy> CollectorDriver<P> {
    /// Drive the window timer until `cancel` resolves.
    ///
    /// Loop shape: when no window is armed, park on the wake channel
    /// (the listener pokes it when it arms one). When a window is armed,
    /// `sleep_until` its deadline; on elapse, drain + reset the window and
    /// run the policy action. Re-poked arms after a `min`-shrunk
    /// remainder simply reschedule against the authoritative `armed_until`
    /// read off the shared state each iteration.
    ///
    /// Cancel-safe: every awaited future (mpsc recv, `sleep_until`, the
    /// cancel future) is cancel-safe; dropping the driver mid-sleep
    /// abandons the timer cleanly.
    pub async fn run<F>(mut self, cancel: F)
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::pin!(cancel);
        loop {
            // Read the authoritative deadline each iteration so a re-arm
            // (or a one-shot close) is always reflected.
            let armed_until = {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.closed && state.armed_until.is_none() {
                    // One-shot already fired and will never re-arm — the
                    // collector is inert; just wait for cancel so the task
                    // shuts down cleanly with the rest of the run.
                    None
                } else {
                    state.armed_until
                }
            };

            match armed_until {
                None => {
                    // Idle (or permanently closed): wait for the listener
                    // to arm a window, or for cancel.
                    tokio::select! {
                        _ = &mut cancel => break,
                        poke = self.wake.recv() => {
                            if poke.is_none() {
                                // All listeners dropped → no more events
                                // can arrive; nothing left to time.
                                break;
                            }
                            // Loop re-reads `armed_until` and schedules.
                        }
                    }
                }
                Some(deadline) => {
                    tokio::select! {
                        _ = &mut cancel => break,
                        // Drain any redundant pokes that arrived while a
                        // window was already armed (record() only pokes on
                        // a fresh arm, but keep the channel from backing
                        // up if a future policy ever re-pokes). A `None`
                        // here means listeners are gone; keep the armed
                        // window honest by letting the sleep arm fire.
                        poke = self.wake.recv() => {
                            if poke.is_none() {
                                // Listeners gone; fall through to let the
                                // armed window elapse on the next loop.
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            let now = Instant::now();
                            let rearm = {
                                self.policy
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .rearm_after_fire()
                            };
                            let collected = {
                                let mut state =
                                    self.state.lock().unwrap_or_else(|p| p.into_inner());
                                state.drain(rearm)
                            };
                            if !collected.is_empty() {
                                self.policy
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .on_window_elapsed(collected, now);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Spawn-friendly free function mirroring `run_task_completed_dispatcher`:
/// drive `driver` until `cancel` resolves. Callers `tokio::spawn` /
/// `spawn_local` this per the surrounding runtime's shape.
pub async fn run_collector<P, F>(driver: CollectorDriver<P>, cancel: F)
where
    P: CollectorPolicy,
    F: std::future::Future<Output = ()>,
{
    driver.run(cancel).await;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    fn fail(task_id: &str, kind: &str, message: &str) -> TaskCompletedEvent {
        TaskCompletedEvent {
            task_id: task_id.into(),
            task_hash: format!("hash-{task_id}"),
            success: false,
            error_kind: Some(kind.into()),
            last_error: Some(message.into()),
        }
    }

    /// A test policy: matches every failure, fixed window, captures the
    /// collected sets each fire, and (optionally) one-shots.
    struct TestPolicy {
        window: Duration,
        rearm: bool,
        fires: Arc<Mutex<Vec<Vec<CollectedFailure>>>>,
    }
    impl CollectorPolicy for TestPolicy {
        fn matches(&self, _event: &TaskCompletedEvent) -> bool {
            true
        }
        fn window_for(&mut self, _now: Instant) -> Duration {
            self.window
        }
        fn on_window_elapsed(&mut self, collected: Vec<CollectedFailure>, _now: Instant) {
            self.fires.lock().unwrap().push(collected);
        }
        fn rearm_after_fire(&self) -> bool {
            self.rearm
        }
    }

    /// The window arms on the first failure, accumulates over its
    /// duration, dedups by `last_error`, and fires ONCE with the deduped
    /// set carrying `xN` repeat counts.
    #[tokio::test(start_paused = true)]
    async fn collects_and_dedups_by_last_error_with_repeat_counts() {
        let fires = Arc::new(Mutex::new(Vec::new()));
        let policy = TestPolicy {
            window: Duration::from_secs(60),
            rearm: true,
            fires: Arc::clone(&fires),
        };
        let (listener, driver) = windowed_failure_collector(policy);
        let cancel = tokio::sync::oneshot::channel::<()>();
        let driver_task = tokio::spawn(run_collector(driver, async move {
            let _ = cancel.1.await;
        }));

        // t=0: two distinct messages + a repeat of the first.
        listener.on_event(&fail("a", "non_recoverable", "boom"));
        listener.on_event(&fail("b", "non_recoverable", "splat"));
        listener.on_event(&fail("c", "non_recoverable", "boom")); // dup of "boom"

        // Advance to just before the window elapses: still collecting.
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(fires.lock().unwrap().is_empty(), "window not yet elapsed");
        // A late failure inside the window joins the same collection.
        listener.on_event(&fail("d", "oom", "oom-killed"));

        // Cross the deadline.
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        let got = fires.lock().unwrap().clone();
        assert_eq!(got.len(), 1, "fired exactly once");
        let window = &got[0];
        // Three distinct messages, first-seen order: boom, splat, oom-killed.
        assert_eq!(window.len(), 3);
        assert_eq!(window[0].representative.last_error.as_deref(), Some("boom"));
        assert_eq!(window[0].representative.task_id, "a", "first-seen rep");
        assert_eq!(window[0].repeat_count, 1, "boom repeated once (task c)");
        assert_eq!(
            window[1].representative.last_error.as_deref(),
            Some("splat")
        );
        assert_eq!(window[1].repeat_count, 0);
        assert_eq!(
            window[2].representative.last_error.as_deref(),
            Some("oom-killed")
        );
        assert_eq!(window[2].repeat_count, 0);

        drop(cancel.0);
        let _ = driver_task.await;
    }

    /// A successful completion never participates, and a non-matching
    /// failure (filtered out by the policy) never arms a window.
    #[tokio::test(start_paused = true)]
    async fn ignores_success_and_filtered_failures() {
        let fires = Arc::new(Mutex::new(Vec::new()));
        // Policy that matches ONLY invalid_task:* failures.
        struct InvalidOnly {
            fires: Arc<Mutex<Vec<Vec<CollectedFailure>>>>,
        }
        impl CollectorPolicy for InvalidOnly {
            fn matches(&self, event: &TaskCompletedEvent) -> bool {
                event
                    .error_kind
                    .as_deref()
                    .is_some_and(|k| k.starts_with("invalid_task:"))
            }
            fn window_for(&mut self, _now: Instant) -> Duration {
                Duration::from_secs(60)
            }
            fn on_window_elapsed(&mut self, collected: Vec<CollectedFailure>, _now: Instant) {
                self.fires.lock().unwrap().push(collected);
            }
        }
        let (listener, driver) = windowed_failure_collector(InvalidOnly {
            fires: Arc::clone(&fires),
        });
        let cancel = tokio::sync::oneshot::channel::<()>();
        let driver_task = tokio::spawn(run_collector(driver, async move {
            let _ = cancel.1.await;
        }));

        // A success and an ordinary failure: neither arms a window.
        listener.on_event(&TaskCompletedEvent {
            task_id: "ok".into(),
            task_hash: "h".into(),
            success: true,
            error_kind: None,
            last_error: None,
        });
        listener.on_event(&fail("nf", "non_recoverable", "boom"));
        tokio::time::advance(Duration::from_secs(120)).await;
        tokio::task::yield_now().await;
        assert!(
            fires.lock().unwrap().is_empty(),
            "no matching failure → no window ever fires"
        );

        drop(cancel.0);
        let _ = driver_task.await;
    }

    /// A one-shot policy fires exactly once: a failure arriving AFTER the
    /// first window elapses is dropped (no second window).
    #[tokio::test(start_paused = true)]
    async fn one_shot_policy_does_not_rearm() {
        let fires = Arc::new(Mutex::new(Vec::new()));
        let policy = TestPolicy {
            window: Duration::from_secs(60),
            rearm: false,
            fires: Arc::clone(&fires),
        };
        let (listener, driver) = windowed_failure_collector(policy);
        let cancel = tokio::sync::oneshot::channel::<()>();
        let driver_task = tokio::spawn(run_collector(driver, async move {
            let _ = cancel.1.await;
        }));

        listener.on_event(&fail("a", "non_recoverable", "boom"));
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert_eq!(fires.lock().unwrap().len(), 1, "fired once");

        // A new failure after the one-shot fired is dropped — no re-arm.
        listener.on_event(&fail("b", "non_recoverable", "splat"));
        tokio::time::advance(Duration::from_secs(120)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            fires.lock().unwrap().len(),
            1,
            "one-shot must not fire a second window"
        );

        drop(cancel.0);
        let _ = driver_task.await;
    }

    /// A rolling policy re-arms: a fresh failure after the first window
    /// fires arms (and fires) a new window.
    #[tokio::test(start_paused = true)]
    async fn rolling_policy_rearms_for_a_new_window() {
        let fires = Arc::new(Mutex::new(Vec::new()));
        let policy = TestPolicy {
            window: Duration::from_secs(60),
            rearm: true,
            fires: Arc::clone(&fires),
        };
        let (listener, driver) = windowed_failure_collector(policy);
        let cancel = tokio::sync::oneshot::channel::<()>();
        let driver_task = tokio::spawn(run_collector(driver, async move {
            let _ = cancel.1.await;
        }));

        listener.on_event(&fail("a", "non_recoverable", "boom"));
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert_eq!(fires.lock().unwrap().len(), 1);

        // Second window.
        listener.on_event(&fail("b", "non_recoverable", "splat"));
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        let got = fires.lock().unwrap().clone();
        assert_eq!(got.len(), 2, "rolling policy armed a second window");
        assert_eq!(got[1].len(), 1);
        assert_eq!(
            got[1][0].representative.last_error.as_deref(),
            Some("splat")
        );

        drop(cancel.0);
        let _ = driver_task.await;
    }
}
