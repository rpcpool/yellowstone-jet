//! Pre-connecting to predicted upcoming leaders.

#[cfg(feature = "prometheus")]
use crate::prom;
use {
    super::{SpawnSource, TpuSenderDriver},
    crate::core::{
        constants::{FOREVER, NUM_CONSECUTIVE_LEADER_SLOTS},
        response::TpuSenderResponseCallback,
    },
    solana_clock::DEFAULT_MS_PER_SLOT,
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
    pub(super) fn try_predict_upcoming_leaders_if_necessary(&mut self) {
        // THIS BRANCH IS HIGHLY LIKELY TO BE TRUE
        if self.next_leader_prediction_deadline.elapsed() == Duration::ZERO {
            return;
        }

        if let Some(lh) = self
            .config
            .leader_prediction_lookahead
            .map(|nz| nz.get() as u64)
        {
            const MINIMAL_SLOT_DURATION_SINCE_AGAVE_4_2: Duration = Duration::from_millis(200);
            // Predict every 3 slot.
            let wait_dur_ms = MINIMAL_SLOT_DURATION_SINCE_AGAVE_4_2.as_millis()
                * (NUM_CONSECUTIVE_LEADER_SLOTS - 1) as u128;
            let wait_dur = Duration::from_millis(wait_dur_ms as u64)
                .max(Duration::from_millis(DEFAULT_MS_PER_SLOT));
            self.next_leader_prediction_deadline = Instant::now() + wait_dur;

            let upcoming_leaders = self
                .leader_predictor
                .try_predict_next_n_leaders(lh as usize);
            let mut visited = HashSet::<Pubkey>::with_capacity(upcoming_leaders.len());
            for upcoming_leader in upcoming_leaders {
                let is_already_connectish =
                    self.tx_worker_handle_map.contains_key(&upcoming_leader)
                        || self.connecting_remote_peers.contains_key(&upcoming_leader);

                if !visited.insert(upcoming_leader) {
                    // We have already processed this upcoming leader in this iteration.
                    continue;
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
        } else {
            // If we don't have leader prediction lookahead configured, we don't predict upcoming leaders.
            // Set the next prediction deadline to a long time in the future.
            // So this function exit early next time it is called.
            self.next_leader_prediction_deadline = Instant::now() + FOREVER;
        }
    }
}
