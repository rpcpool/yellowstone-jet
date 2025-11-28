use std::sync::atomic::{AtomicBool, AtomicU64};

///
/// An atomic slot tracker that can be shared across tasks.
///
/// # Safety
///
/// This struct is thread-safe. Shared it using an atomic reference-counter.
///
/// # Poisoning
///
/// The slot tracker can be poisoned if the background task updating it panics or is dropped.
///
///
pub struct AtomicSlotTracker {
    pub(crate) slot: AtomicU64,
    pub(crate) closed: AtomicBool,
}

#[derive(Debug, thiserror::Error)]
#[error("AtomicSlotTracker poisoned, driver task may have panicked at slot {0}")]
pub struct PoisonError(u64);

impl AtomicSlotTracker {
    #[allow(dead_code)]
    pub(crate) fn new(initial_slot: u64) -> Self {
        Self {
            slot: AtomicU64::new(initial_slot),
            closed: AtomicBool::new(false),
        }
    }

    ///
    /// Load the current slot.
    ///
    /// Returns an error if the slot tracker is poisoned.
    ///
    pub fn load(&self) -> Result<u64, PoisonError> {
        let is_closed = self.closed.load(std::sync::atomic::Ordering::Acquire);
        let slot = self.slot.load(std::sync::atomic::Ordering::Relaxed);
        if is_closed {
            Err(PoisonError(slot))
        } else {
            Ok(slot)
        }
    }
}
