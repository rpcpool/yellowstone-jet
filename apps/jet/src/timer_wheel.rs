//!
//! A fixed-size, lock-free ring of time-bucketed counters -- a small timer wheel.
//!
//! [`TimerWheel`] divides a trailing time window into `NUM_SLOTS` fixed-size buckets (a
//! `[Slot; NUM_SLOTS]` array, sized at compile time). Each bucket counts events that landed
//! during one `slot_duration`-wide tick of wall-clock time; the bucket for "now" rotates as
//! time passes, wrapping back to bucket 0 once a full window has elapsed. This gives an
//! approximate, low-overhead answer to "how much activity landed in the last `N * slot_duration`
//! of time" (or, as a special case, "has *any* activity landed in that window at all") without
//! ever taking a lock.
//!
//! The motivating case (see [`ActivityWindow`]) is 300 slots of 100ms each -- a 30 second
//! trailing window -- used to detect "no activity for 30 seconds" from many concurrently
//! incrementing threads.
//!
//! # Concurrency
//!
//! [`TimerWheel::increment`]/[`TimerWheel::increment_by`] never block: no `Mutex`, no spinlock,
//! nothing that can hold up a caller behind another thread doing I/O or waiting on a scheduler.
//! Under contention on the *same slot* (multiple threads incrementing within the same tick) it's
//! a bounded compare-and-swap retry loop, same as any other lock-free counter built on
//! `fetch_add`.
//!
//! Each slot packs its tick number and count into a single [`AtomicU64`] (`(tick << 32) |
//! count`) and updates both together in one `compare_exchange` -- tick and count are never
//! visible to another thread as two separate writes. That's deliberate: splitting them (e.g. a
//! separate `AtomicU64` for tick and one for count, "reset count, then claim the tick" or "claim
//! the tick, then reset count") has a real race either way. For example, claim-then-reset:
//! thread A CAS's the tick from the old generation to the new one, then (separately) zeroes the
//! count; if thread B observes the new tick in between and adds to the *old* count before A's
//! reset lands, A's reset silently discards B's increment. Packing both fields into one word
//! closes that window -- there is no "in between" state to observe.
//!
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// One bucket: `(tick << 32) | count`, updated as a single unit. See the module docs for why
/// this can't be two separate atomics.
///
/// `#[repr(align(64))]` pads it out to one full cache line (64 bytes on essentially every
/// current x86_64/ARM server part): `[Slot; NUM_SLOTS]` packs slots contiguously, and without
/// this, adjacent slots would share a cache line -- two threads concurrently incrementing
/// *different* ticks (the common case: different threads calling in at slightly different
/// times, or the same thread across a rotation) would still ping-pong that line between cores'
/// caches on every write, even though they're touching logically unrelated counters. Costs
/// 56 bytes of padding per slot to buy that isolation.
#[repr(align(64))]
struct Slot(AtomicU64);

const _: () = assert!(
    std::mem::size_of::<Slot>() == 64,
    "Slot should be exactly one cache line"
);

const COUNT_BITS: u32 = 32;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;

const fn pack(tick: u32, count: u32) -> u64 {
    ((tick as u64) << COUNT_BITS) | (count as u64)
}

const fn unpack(word: u64) -> (u32, u32) {
    ((word >> COUNT_BITS) as u32, (word & COUNT_MASK) as u32)
}

/// `duration.as_nanos()` as a plain `u64` instead of `Duration`'s own `u128`.
///
/// `Duration::as_nanos()` returns `u128` so it can represent durations up to ~584 billion years
/// without overflow -- not a real concern here (nothing in this module runs anywhere near
/// `u64::MAX` nanoseconds of continuous uptime, ~584 years), and dividing by a `u128` has no
/// hardware support on x86_64, so it lowers to a software routine far slower than the native
/// 64-bit `div` this produces instead.
const fn duration_as_nanos_u64(duration: Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64
}

impl Slot {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
}

///
/// A fixed-size (`NUM_SLOTS`, chosen at compile time), lock-free ring of per-tick counters
/// covering a trailing `NUM_SLOTS * slot_duration` window of wall-clock time.
///
/// See the [module docs](self) for the concurrency design. [`ActivityWindow`] is a ready-made
/// alias for the motivating 30-second/100ms-resolution case.
///
pub struct TimerWheel<const NUM_SLOTS: usize> {
    start: Instant,
    slot_duration_ns: u64,
    slots: [Slot; NUM_SLOTS],
}

impl<const NUM_SLOTS: usize> TimerWheel<NUM_SLOTS> {
    ///
    /// Builds a new wheel, with `now` as the start of tick 0. `slot_duration` is how much
    /// wall-clock time one bucket represents; the wheel as a whole covers a trailing
    /// `NUM_SLOTS * slot_duration` window.
    ///
    /// # Panics
    ///
    /// Panics if `slot_duration` is zero.
    ///
    pub fn new(slot_duration: Duration) -> Self {
        assert!(
            !slot_duration.is_zero(),
            "TimerWheel slot_duration must be non-zero"
        );
        Self {
            start: Instant::now(),
            slot_duration_ns: duration_as_nanos_u64(slot_duration).max(1),
            slots: std::array::from_fn(|_| Slot::new()),
        }
    }

    /// The tick `now` falls in, truncated to `u32`. Every comparison this type does is an
    /// equality or a wrapping subtraction, never an ordering comparison, so wraparound (after
    /// `2^32 * slot_duration` of continuous runtime) is harmless. `now` earlier than this
    /// wheel's start saturates to tick 0 rather than underflowing (see
    /// [`Instant::duration_since`]).
    fn tick_at(&self, now: Instant) -> u32 {
        let elapsed_ns = duration_as_nanos_u64(now.duration_since(self.start));
        (elapsed_ns / self.slot_duration_ns) as u32
    }

    /// Records one event in the current tick's slot. `now` drives the elapsed-time computation
    /// instead of an internal `Instant::now()` call, so callers who already have a timestamp
    /// handy (or need deterministic control over it, e.g. in tests) can supply it directly.
    pub fn increment(&self, now: Instant) {
        self.increment_by(now, 1);
    }

    /// Records `amount` events in the tick `now` falls in. See [`TimerWheel::increment`] for why
    /// `now` is a parameter rather than an internal `Instant::now()` call.
    ///
    /// A slot's count saturates at `u32::MAX` rather than wrapping -- overflowing a single
    /// 100ms-scale bucket would require billions of increments within one tick, at which point
    /// a saturated (obviously-maxed-out) count is a more honest answer than a wrapped one.
    pub fn increment_by(&self, now: Instant, amount: u32) {
        let now_tick = self.tick_at(now);
        let idx = (now_tick as usize) % NUM_SLOTS;
        let word = &self.slots[idx].0;

        let mut current = word.load(Ordering::Relaxed);
        loop {
            let (tick, count) = unpack(current);
            let new_word = if tick == now_tick {
                pack(tick, count.saturating_add(amount))
            } else {
                // This slot last belonged to an earlier tick (or has never been written) --
                // rotate it into the current one, discarding its stale count, in the same CAS
                // that publishes the new tick. No other thread can observe a state where the
                // tick has advanced but the count hasn't (or vice versa).
                pack(now_tick, amount)
            };
            match word.compare_exchange_weak(current, new_word, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Sums every slot whose last write falls within the trailing `NUM_SLOTS`-tick window
    /// ending at `now`, treating any slot last written further back than that (i.e. left over
    /// from a previous lap around the ring) as zero. This is the total recorded activity across
    /// the whole window, e.g. "how many events in the last 30 seconds".
    pub fn count_in_window(&self, now: Instant) -> u64 {
        let now_tick = self.tick_at(now);
        self.slots
            .iter()
            .map(|slot| {
                let (tick, count) = unpack(slot.0.load(Ordering::Relaxed));
                if now_tick.wrapping_sub(tick) < NUM_SLOTS as u32 {
                    count as u64
                } else {
                    0
                }
            })
            .sum()
    }

    /// `true` if no event has been recorded anywhere in the trailing `NUM_SLOTS`-tick window
    /// ending at `now` -- i.e. no activity for the whole `NUM_SLOTS * slot_duration` covered by
    /// this wheel.
    ///
    /// Equivalent to (but cheaper than, since it can return early) `count_in_window(now) == 0`.
    pub fn is_idle(&self, now: Instant) -> bool {
        let now_tick = self.tick_at(now);
        self.slots.iter().all(|slot| {
            let (tick, count) = unpack(slot.0.load(Ordering::Relaxed));
            count == 0 || now_tick.wrapping_sub(tick) >= NUM_SLOTS as u32
        })
    }

    /// How many *distinct* slots in the trailing `NUM_SLOTS`-tick window (ending at `now`) have
    /// recorded at least one event. Use this alongside [`TimerWheel::count_in_window`] to tell
    /// sustained activity apart from a single instantaneous burst: e.g. ten events that all
    /// landed in the same 100ms tick pass a raw `count_in_window() >= min_activity` check just
    /// as easily as ten events spread evenly across ten different ticks, but only the second
    /// case is actually sustained activity over time -- `active_slots_in_window(now) >=
    /// min_active_slots` is what tells them apart.
    pub fn active_slots_in_window(&self, now: Instant) -> usize {
        let now_tick = self.tick_at(now);
        self.slots
            .iter()
            .filter(|slot| {
                let (tick, count) = unpack(slot.0.load(Ordering::Relaxed));
                count > 0 && now_tick.wrapping_sub(tick) < NUM_SLOTS as u32
            })
            .count()
    }

    /// `true` if at least a `numerator / denominator` fraction of this wheel's `NUM_SLOTS`
    /// slots have recorded activity within the trailing window ending at `now` -- e.g.
    /// `active_slot_fraction_at_least(now, 1, 3)` asks "did activity show up in at least a
    /// third of this window's buckets," a bar that scales with `NUM_SLOTS` (and so, with
    /// `slot_duration`, with how long the window actually covers) rather than a fixed slot
    /// count like [`TimerWheel::active_slots_in_window`] alone gives you.
    ///
    /// Compares `active_slots * denominator` against `numerator * NUM_SLOTS` (cross-multiplied)
    /// rather than dividing, so this never loses precision to integer truncation the way
    /// `active_slots_in_window(now) >= NUM_SLOTS / denominator` would for a `denominator` that
    /// doesn't evenly divide `NUM_SLOTS` (e.g. `NUM_SLOTS = 300, denominator = 3` is fine, but
    /// `NUM_SLOTS = 100, denominator = 3` would silently round `100/3` down to `33`).
    ///
    /// # Panics
    ///
    /// Panics (via the multiplication overflowing) if `numerator` is large enough that
    /// `numerator * NUM_SLOTS` overflows `usize` -- not a real concern for the small fractions
    /// (halves, thirds, tenths) this is meant for.
    pub fn active_slot_fraction_at_least(
        &self,
        now: Instant,
        numerator: usize,
        denominator: usize,
    ) -> bool {
        debug_assert!(denominator > 0, "denominator must be non-zero");
        self.active_slots_in_window(now) * denominator >= numerator * NUM_SLOTS
    }
}

/// 300 slots of 100ms each: a trailing 30-second activity window, built with
/// [`ActivityWindow::new_30s`] (or [`Default::default`]). `TimerWheel<N>` itself has no blanket
/// `Default` impl -- `slot_duration` is a runtime `Duration` a generic impl couldn't know a
/// sensible value for -- but this one alias has an unambiguous "obvious" configuration, so it
/// gets a concrete [`Default`] impl of its own below.
pub type ActivityWindow = TimerWheel<300>;

impl ActivityWindow {
    /// The specific wheel this module exists for: 300 buckets of 100ms, a trailing 30-second
    /// activity window.
    pub fn new_30s() -> Self {
        Self::new(Duration::from_millis(100))
    }
}

impl Default for ActivityWindow {
    fn default() -> Self {
        Self::new_30s()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::thread};

    #[test]
    fn fresh_wheel_is_idle() {
        let wheel = TimerWheel::<5>::new(Duration::from_millis(10));
        assert!(wheel.is_idle(Instant::now()));
        assert_eq!(wheel.count_in_window(Instant::now()), 0);
    }

    #[test]
    fn increment_is_observed_immediately() {
        let wheel = TimerWheel::<5>::new(Duration::from_millis(10));
        wheel.increment(Instant::now());
        assert!(!wheel.is_idle(Instant::now()));
        assert_eq!(wheel.count_in_window(Instant::now()), 1);
    }

    #[test]
    fn increment_by_accumulates_within_the_same_tick() {
        let wheel = TimerWheel::<5>::new(Duration::from_secs(10)); // one huge tick, no rotation
        for _ in 0..10 {
            wheel.increment(Instant::now());
        }
        wheel.increment_by(Instant::now(), 5);
        assert_eq!(wheel.count_in_window(Instant::now()), 15);
    }

    #[test]
    fn active_slots_counts_a_single_burst_as_one_slot() {
        let wheel = TimerWheel::<5>::new(Duration::from_secs(10)); // one huge tick, no rotation
        for _ in 0..10 {
            wheel.increment(Instant::now());
        }
        assert_eq!(wheel.count_in_window(Instant::now()), 10);
        assert_eq!(
            wheel.active_slots_in_window(Instant::now()),
            1,
            "ten events in the same tick is still only one active slot"
        );
    }

    #[test]
    fn active_slots_counts_activity_spread_across_ticks() {
        // 100 slots * 1ms = a 100ms window -- comfortably wider than the ~10-15ms this test
        // actually takes, so nothing ages out before the final assertion. 5ms between
        // increments is comfortably wider than the 1ms slot duration, so each one reliably
        // lands in a fresh tick rather than racing the previous one for the same slot.
        let wheel = TimerWheel::<100>::new(Duration::from_millis(1));
        for _ in 0..3 {
            wheel.increment(Instant::now());
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(wheel.count_in_window(Instant::now()), 3);
        assert_eq!(wheel.active_slots_in_window(Instant::now()), 3);
    }

    #[test]
    fn active_slot_fraction_at_least_checks_the_cross_multiplied_ratio() {
        // Deterministic ticks via direct field access (same module tree, so `start` is visible
        // here) instead of real sleeps -- exact slot counts matter for a ratio check, and this
        // avoids any scheduler-jitter flakiness around a boundary case.
        let wheel = TimerWheel::<6>::new(Duration::from_millis(10));
        let tick = |n: u64| wheel.start + Duration::from_millis(10 * n);

        // Hit ticks 0 and 1 -- 2 of the 6 slots.
        wheel.increment(tick(0));
        wheel.increment(tick(1));

        let now = tick(5); // ticks 0..=5 are all still within the trailing 6-slot window
        assert_eq!(wheel.active_slots_in_window(now), 2);

        assert!(
            wheel.active_slot_fraction_at_least(now, 1, 3),
            "2/6 exactly equals 1/3"
        );
        assert!(
            !wheel.active_slot_fraction_at_least(now, 1, 2),
            "2/6 falls short of 1/2"
        );
    }

    #[test]
    fn activity_ages_out_of_the_window() {
        // 3 slots * 5ms = a 15ms window.
        let wheel = TimerWheel::<3>::new(Duration::from_millis(5));
        wheel.increment(Instant::now());
        assert!(!wheel.is_idle(Instant::now()));

        thread::sleep(Duration::from_millis(40)); // well past the 15ms window

        assert!(
            wheel.is_idle(Instant::now()),
            "activity recorded well outside the window should no longer count"
        );
        assert_eq!(wheel.count_in_window(Instant::now()), 0);
    }

    #[test]
    fn concurrent_increments_are_not_lost() {
        const THREADS: usize = 16;
        const PER_THREAD: usize = 5_000;

        // A long slot duration keeps every increment in tick 0 -- this isolates the test to
        // "concurrent increments within one slot never clobber each other", the same-slot half
        // of the race the module docs describe (the rotation half is covered by
        // `activity_ages_out_of_the_window` above, which only ever has one writer).
        let wheel = std::sync::Arc::new(TimerWheel::<4>::new(Duration::from_secs(60)));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let wheel = std::sync::Arc::clone(&wheel);
                thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        wheel.increment(Instant::now());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            wheel.count_in_window(Instant::now()),
            (THREADS * PER_THREAD) as u64
        );
    }
}
