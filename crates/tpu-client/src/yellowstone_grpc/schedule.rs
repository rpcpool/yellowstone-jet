//! A Yellowstone-specific UpcomingLeaderPredictor implementation
//!
//! This module provides an implementation of the UpcomingLeaderPredictor trait
//! tailored for Yellowstone, utilizing gRPC and RPC services to track the current slot
//! and predict upcoming leaders.
//!
//! # Safety
//!
//! This module is designed to be thread-safe and can be shared across multiple tasks.
//!
//! # Poisoning
//!
//! The slot tracker/managed schedule used in this implementation can be poisoned if the background task
//! updating it panics or is dropped.
//!
use {
    crate::{
        core::UpcomingLeaderPredictor, rpc::schedule::ManagedLeaderSchedule, slot::SlotTracker,
    },
    solana_pubkey::Pubkey,
    std::mem::MaybeUninit,
};

///
/// A Yellowstone-specific implementation of UpcomingLeaderPredictor
///
/// # Safety
///
/// This struct is cheaply-cloneable and can be shared between threads.
///
#[derive(Clone)]
pub struct YellowstoneUpcomingLeader {
    pub slot_tracker: SlotTracker,
    pub managed_schedule: ManagedLeaderSchedule,
}

impl UpcomingLeaderPredictor for YellowstoneUpcomingLeader {
    fn try_predict_next_n_leader_inclusive(&self, out: &mut [MaybeUninit<Pubkey>]) -> usize {
        let slot = self.slot_tracker.load().expect("load");

        // Every slot of a 4-slot leader window has the same leader, so `slot + 4 * i` lands in
        // the i-th window from now, with i = 0 being the current one.
        let leaders = (0..out.len())
            .map(|i| slot + (i * 4) as u64)
            .flat_map(|s| self.managed_schedule.get_leader(s).expect("get_leader"));
        let mut written = 0;
        for (dst, leader) in out.iter_mut().zip(leaders) {
            dst.write(leader);
            written += 1;
        }
        written
    }
}
