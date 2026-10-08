//! Pre-connecting to predicted upcoming leaders.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{SpawnSource, TpuSenderDriver},
    crate::core::{
        constants::NUM_CONSECUTIVE_LEADER_SLOTS, leader_fast_path::MAX_FAST_PATH_LEADERS,
        response::TpuSenderResponseCallback,
    },
    solana_pubkey::Pubkey,
    std::{
        collections::HashSet,
        time::{Duration, Instant},
    },
};

impl<CB> TpuSenderDriver<CB>
where
    CB: TpuSenderResponseCallback + Send + Sync + 'static,
{
    pub(super) fn try_predict_upcoming_leaders_if_necessary(&mut self, now: Instant) {
        // THIS BRANCH IS HIGHLY LIKELY TO BE TRUE
        if now.duration_since(self.next_leader_prediction_deadline) == Duration::ZERO {
            return;
        }

        const MINIMAL_SLOT_DURATION_SINCE_AGAVE_4_2: Duration = Duration::from_millis(200);
        // Predict every 3 slot.
        const MAX_WAIT_DUR_MS: u128 = MINIMAL_SLOT_DURATION_SINCE_AGAVE_4_2.as_millis()
            * (NUM_CONSECUTIVE_LEADER_SLOTS - 1) as u128;

        // Moved out for the loop below, since `spawn_connecting` needs `&mut self`.
        let mut upcoming_leaders_buf = std::mem::take(&mut self.upcoming_leaders_buf);
        let num_predicted = self
            .leader_predictor
            .try_predict_next_n_leader_inclusive(&mut upcoming_leaders_buf)
            .min(upcoming_leaders_buf.len());
        let mut visited = HashSet::<Pubkey>::with_capacity(num_predicted);
        self.fast_path_leaders.clear();
        for upcoming_leader in &upcoming_leaders_buf[..num_predicted] {
            // SAFETY: the `UpcomingLeaderPredictor` contract requires `out[..n]` to be
            // initialized. The buffer is also filled when the driver is built, so a predictor
            // that overreports `n` yields stale keys rather than uninitialized memory.
            let upcoming_leader = unsafe { upcoming_leader.assume_init() };
            let is_already_connectish = self.tx_worker_handle_map.contains_key(&upcoming_leader)
                || self.connecting_remote_peers.contains_key(&upcoming_leader);

            if !visited.insert(upcoming_leader) {
                // We have already processed this upcoming leader in this iteration.
                continue;
            }
            if self.fast_path_leaders.len() < MAX_FAST_PATH_LEADERS {
                self.fast_path_leaders.push(upcoming_leader);
            }

            if !is_already_connectish {
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_leader_prediction_hit();
                }
                tracing::trace!(
                    "Spawning connection for predicted upcoming leader: {}",
                    upcoming_leader
                );
                self.spawn_connecting(upcoming_leader, 1, SpawnSource::Prediction);
            } else {
                #[cfg(feature = "prometheus")]
                {
                    prom::incr_quic_gw_leader_prediction_miss();
                }
            }
        }

        self.refresh_fast_path();

        let wait_duration = if num_predicted == 0 {
            MINIMAL_SLOT_DURATION_SINCE_AGAVE_4_2
        } else {
            Duration::from_millis(MAX_WAIT_DUR_MS as u64)
        };

        self.next_leader_prediction_deadline = now + wait_duration;
        self.upcoming_leaders_buf = upcoming_leaders_buf;
    }
}
