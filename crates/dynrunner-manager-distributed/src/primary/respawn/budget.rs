//! `RespawnBudget::should_respawn` decision logic.

use super::types::{RespawnBudget, RespawnDecision, RespawnEvent};

impl RespawnBudget {
    /// Decide whether a respawn for `original_id` is admissible against
    /// the live `events` ring and the budget's three caps.
    ///
    /// Family-chain count: a "family" is the transitive closure of
    /// respawn events whose `new_id` was once another event's
    /// `original_id`. Starting from `original_id`, we walk the
    /// ring backwards to find the head of the chain (the first peer
    /// the operator originally provisioned), then count every event
    /// in the chain that already landed. If that count already meets
    /// `max_per_secondary`, the request is rejected with
    /// [`RespawnDecision::RejectFamilyBudget`].
    ///
    /// Total budget: any event in the ring counts toward
    /// `max_total`. Ring eviction (at [`RESPAWN_EVENTS_CAP`]) intentionally
    /// does NOT reset this budget; once 10 respawns have happened in
    /// the lifetime of the coordinator, the 11th is refused even if
    /// the ring has overflowed past the oldest entries — operators
    /// who want unlimited respawns disable the policy entirely.
    ///
    /// Cooldown: the most recent event whose `new_id` or `original_id`
    /// belongs to the same family must be at least `cooldown` older
    /// than `now`. The cooldown is per-family (not global) so a
    /// well-behaved cluster losing one peer per minute never trips
    /// it. Tested with deterministic timestamps (no wall-clock
    /// dependency).
    ///
    /// The walk is O(ring.len()) — bounded by [`RESPAWN_EVENTS_CAP`]
    /// (1024 today). Acceptable on the operational `select!` arm
    /// because it fires at the rate of peer deaths, not per-task.
    pub fn should_respawn(
        &self,
        original_id: &str,
        events: &std::collections::VecDeque<RespawnEvent>,
        now: std::time::SystemTime,
    ) -> RespawnDecision {
        // Total budget first — cheapest check, prunes the common
        // exhausted-cluster failure mode before the family walk.
        if (events.len() as u32) >= self.max_total {
            return RespawnDecision::RejectTotalBudget;
        }

        // Walk the chain rooted at `original_id` and tally the count.
        // The chain head is the original peer id; every subsequent
        // entry in the family was minted as a replacement for the
        // previous death. `family_ids` accumulates every id (old +
        // new) we've seen so the cooldown check can match on either
        // side.
        let mut family_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
        family_ids.insert(original_id);
        // Iterative expansion: each pass adds events whose
        // `original_id` or `new_id` is in `family_ids`. Cap the
        // passes at the ring length to bound the worst case;
        // realistic chains are very short (≤ max_per_secondary).
        let mut grew = true;
        while grew {
            grew = false;
            for ev in events {
                if family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
                {
                    if family_ids.insert(ev.original_id.as_str()) {
                        grew = true;
                    }
                    if family_ids.insert(ev.new_id.as_str()) {
                        grew = true;
                    }
                }
            }
        }

        let family_count = events
            .iter()
            .filter(|ev| {
                family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
            })
            .count() as u32;
        if family_count >= self.max_per_secondary {
            return RespawnDecision::RejectFamilyBudget;
        }

        // Cooldown is family-scoped: find the most recent event in
        // this family and require `now - at >= cooldown`. Walks the
        // ring once; `max_by_key` returns `None` when the family has
        // no prior events (first respawn → cooldown trivially
        // satisfied).
        if let Some(latest) = events
            .iter()
            .filter(|ev| {
                family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
            })
            .max_by_key(|ev| ev.at)
        {
            // Saturating: a future-dated `latest.at` (clock skew /
            // test fixture) returns `Duration::ZERO`, which compares
            // to `cooldown` correctly (`ZERO < cooldown` ⇒ reject).
            let elapsed = now
                .duration_since(latest.at)
                .unwrap_or(std::time::Duration::ZERO);
            if elapsed < self.cooldown {
                return RespawnDecision::RejectCooldown;
            }
        }

        RespawnDecision::Accept
    }
}
