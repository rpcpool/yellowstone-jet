use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
};

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
#[derive(Debug, Clone)]
pub struct SlotTracker {
    pub inner: Arc<SharedSlotTracker>,
}

#[derive(Debug)]
pub struct SharedSlotTracker {
    pub slot: AtomicU64,
    pub closed: AtomicBool,
}

#[derive(Debug, thiserror::Error)]
#[error("AtomicSlotTracker disconnected, driver task may have panicked at slot {0}")]
pub struct Disconnected(u64);

impl SlotTracker {
    pub(crate) fn new(initial_slot: u64) -> Self {
        Self {
            inner: Arc::new(SharedSlotTracker {
                slot: AtomicU64::new(initial_slot),
                closed: AtomicBool::new(false),
            }),
        }
    }

    ///
    /// Builds an [`AtomicSlotTracker`] with a fixed initial slot for integration tests.
    ///
    #[cfg(any(test, feature = "intg-testing"))]
    pub fn new_for_test(initial_slot: u64) -> Self {
        Self::new(initial_slot)
    }

    ///
    /// Load the current slot.
    ///
    /// Returns an error if the slot tracker is poisoned.
    ///
    pub fn load(&self) -> Result<u64, Disconnected> {
        let is_closed = self.inner.closed.load(std::sync::atomic::Ordering::Acquire);
        let slot = self.inner.slot.load(std::sync::atomic::Ordering::Relaxed);
        if is_closed {
            Err(Disconnected(slot))
        } else {
            Ok(slot)
        }
    }
}
