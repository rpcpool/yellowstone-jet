use {
    futures::future::join,
    serde::Deserialize,
    solana_client::{client_error, nonblocking::rpc_client::RpcClient},
    solana_clock::DEFAULT_SLOTS_PER_EPOCH,
    solana_pubkey::Pubkey,
    std::{
        str::FromStr,
        sync::{Arc, RwLock, atomic::AtomicBool},
    },
    tokio::task::JoinHandle,
};

pub const DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(300);

///
/// A compact representation of the leader schedule for an epoch,
/// where each leader pubkey appears once for every 4 consecutive slots they lead.
///
#[derive(Clone, Debug)]
pub struct CompactSortedSchedule {
    pub first_slot: u64,
    schedule: Vec<Pubkey>,
}

impl CompactSortedSchedule {
    #[inline]
    pub fn last_slot(&self) -> u64 {
        self.first_slot + DEFAULT_SLOTS_PER_EPOCH - 1
    }

    ///
    /// Get the leader pubkey for a given slot in the epoch
    ///
    pub fn get(&self, slot: &u64) -> Option<&Pubkey> {
        let first_slot = self.first_slot;
        let max_slot = first_slot + DEFAULT_SLOTS_PER_EPOCH;
        if slot < &first_slot || slot >= &max_slot {
            return None;
        }
        let slot_idx = slot.saturating_sub(first_slot);
        let slot_idx = slot_idx / 4;
        // get the nearest leader boundary (every 4 slots)
        self.schedule.get(slot_idx as usize)
    }

    ///
    /// Iterator over (slot, leader_pubkey) pairs for the entire epoch
    ///
    pub fn iter_unnested_schedule(&self) -> impl Iterator<Item = (u64, &Pubkey)> {
        let first_slot = self.first_slot;
        self.schedule
            .iter()
            .enumerate()
            .flat_map(move |(slot_idx, pubk)| {
                (0..4).map(move |i| {
                    let slot = first_slot + (slot_idx as u64 * 4) + i;
                    (slot, pubk)
                })
            })
    }
}

pub fn unnest_rpc_get_leader_schedule_resp<'a, 'iter>(
    slot: u64,
    resp: impl IntoIterator<Item = (&'iter String, &'iter Vec<usize>)>,
) -> CompactSortedSchedule
where
    'a: 'iter,
{
    let mut ret = Vec::with_capacity(DEFAULT_SLOTS_PER_EPOCH as usize / 4);

    ret.resize(ret.capacity(), Pubkey::default());

    for (pubkey_str, sorted_slot_idx) in resp {
        let pubkey = Pubkey::from_str(pubkey_str.as_str()).expect("Pubkey::from_str");
        sorted_slot_idx
            .iter()
            .filter(|s| *s % 4 == 0)
            .map(|s| s / 4)
            .for_each(|slot_idx| {
                ret[slot_idx] = pubkey;
            });
    }
    let epoch = slot / DEFAULT_SLOTS_PER_EPOCH;

    CompactSortedSchedule {
        first_slot: epoch * DEFAULT_SLOTS_PER_EPOCH,
        schedule: ret,
    }
}

#[async_trait::async_trait]
pub trait ScheduleExt {
    ///
    /// Get the leader schedule for the epoch containing `slot_ctx`,
    /// unnesting the RPC response into a [`CompactSortedSchedule`].
    ///
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError>;
}

#[async_trait::async_trait]
impl ScheduleExt for RpcClient {
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError> {
        let referenced_slot = match slot_ctx {
            Some(slot) => slot,
            None => {
                let info = self.get_epoch_info().await?;
                info.absolute_slot
            }
        };

        Ok(self
            .get_leader_schedule(Some(referenced_slot))
            .await?
            .map(|nested| unnest_rpc_get_leader_schedule_resp(referenced_slot, nested.iter())))
    }
}

#[derive(Debug)]
struct InnerManagedLeaderSchedule {
    double_buffer: [CompactSortedSchedule; 2],
    fail: AtomicBool,
}

///
/// A managed leader schedule that automatically updates as epochs progress.
///
/// See [`spawn_managed_leader_schedule`] for spawning the background update task.
///
/// # Safety
///
/// This struct uses internal synchronization to allow concurrent access from multiple tasks.
///
/// # Clone
///
/// You can clone (cheaply) the `ManagedLeaderSchedule` to share it across multiple tasks.
///
#[derive(Clone)]
pub struct ManagedLeaderSchedule {
    inner: Arc<RwLock<InnerManagedLeaderSchedule>>,
}

///
/// Error indicating that the AutoLeaderSchedule background update task has failed.
///
#[derive(Debug, thiserror::Error)]
#[error("auto leader schedule poisoned")]
pub struct PoisonError;

impl ManagedLeaderSchedule {
    ///
    /// Get the leader for a given slot.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Pubkey))` if the leader for the slot is found.
    /// - `Ok(None)` if the slot is out of range of the current schedules.
    /// - `Err(PoisonError)` if the background update task has failed.
    ///
    /// # Errors
    ///
    /// Returns `PoisonError` if the background update task has failed.
    ///
    pub fn get_leader(&self, slot: u64) -> Result<Option<Pubkey>, PoisonError> {
        let schedules = self.inner.read().unwrap();
        // Relaxed ordering is sufficient here since fail does not protect any data.
        // We already use RwLock to protect the double_buffer data.
        if schedules.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PoisonError);
        }
        let schedule = if slot >= schedules.double_buffer[1].first_slot {
            &schedules.double_buffer[1]
        } else {
            &schedules.double_buffer[0]
        };
        Ok(schedule.get(&slot).cloned())
    }
}

async fn auto_leader_schedule_loop(
    config: ManagedLeaderScheduleConfig,
    shared: Arc<RwLock<InnerManagedLeaderSchedule>>,
    rpc_client: Arc<RpcClient>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    pub struct OnDrop {
        shared: Option<Arc<RwLock<InnerManagedLeaderSchedule>>>,
    }

    impl Drop for OnDrop {
        fn drop(&mut self) {
            if let Some(shared) = self.shared.take() {
                let Ok(schedules) = shared.read() else {
                    return;
                };
                // Since we are already synchronously dropping (RwLock), it's safe to use Relaxed ordering here.
                // This trick have been taking from std::sync::poison implementation!
                schedules
                    .fail
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    let initial = shared.read().expect("read").double_buffer.clone();

    let mut current_epoch = initial[0].first_slot / DEFAULT_SLOTS_PER_EPOCH;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(config.check_interval) => { }
            _ = cancellation_token.cancelled() => {
                tracing::info!("AutoLeaderSchedule: cancellation requested, exiting loop");
                break;
            }
        }

        let epoch = rpc_client
            .get_epoch_info()
            .await
            .expect("rpc_client.get_epoch_info")
            .epoch;

        if epoch == current_epoch {
            tracing::debug!("AutoLeaderSchedule: still in epoch {}", current_epoch);

            // Only fetch the next schedule if we're still in the same epoch
            // Making sure we have the freshest schedule ready when we transition
            let next_epoch_first_slot = (current_epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;
            let next_schedule = rpc_client
                .get_unnested_leader_schedule(Some(next_epoch_first_slot))
                .await
                .expect("rpc_client.get_unnested_leader_schedule next")
                .expect("None next schedule");
            {
                let mut schedules = shared.write().expect("write");
                schedules.double_buffer[1] = next_schedule;
            }

            continue;
        } else {
            tracing::info!(
                "AutoLeaderSchedule: detected epoch change {} -> {}",
                current_epoch,
                epoch
            );
            current_epoch = epoch;
            let first_slot_current_epoch = current_epoch * DEFAULT_SLOTS_PER_EPOCH;
            let next_epoch_first_slot = (current_epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;

            let current_schedule_fut =
                rpc_client.get_unnested_leader_schedule(Some(first_slot_current_epoch));

            let next_schedule_fut =
                rpc_client.get_unnested_leader_schedule(Some(next_epoch_first_slot));

            let (current_schedule, next_schedule) =
                join(current_schedule_fut, next_schedule_fut).await;
            let current_schedule = current_schedule
                .expect("rpc_client.get_unnested_leader_schedule current")
                .expect("None current schedule");
            let next_schedule = next_schedule
                .expect("rpc_client.get_unnested_leader_schedule next")
                .expect("None next schedule");

            {
                let mut schedules = shared.write().expect("write");
                schedules.double_buffer = [current_schedule, next_schedule];
            }
        }
    }
}

///
/// Configuration for spawning a managed leader schedule.
///
/// See [`spawn_managed_leader_schedule`].
#[derive(Debug, Clone, Deserialize)]
pub struct ManagedLeaderScheduleConfig {
    /// How long to wait before checking for a new epoch schedule
    #[serde(
        with = "humantime_serde",
        default = "ManagedLeaderScheduleConfig::default_check_interval"
    )]
    pub check_interval: std::time::Duration,
}

impl ManagedLeaderScheduleConfig {
    ///
    /// Default check interval duration.
    ///
    pub fn default_check_interval() -> std::time::Duration {
        DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL
    }
}

impl Default for ManagedLeaderScheduleConfig {
    fn default() -> Self {
        Self {
            check_interval: Self::default_check_interval(),
        }
    }
}

///
/// Spawn a managed leader schedule that automatically updates as epochs progress.
///
pub async fn spawn_managed_leader_schedule(
    rpc_client: Arc<RpcClient>,
    config: ManagedLeaderScheduleConfig,
) -> Result<(ManagedLeaderSchedule, JoinHandle<()>), client_error::ClientError> {
    let initial_schedule = rpc_client
        .get_unnested_leader_schedule(None)
        .await?
        .expect("Failed to fetch initial leader schedule");

    let current_epoch = initial_schedule.first_slot / DEFAULT_SLOTS_PER_EPOCH;
    let next_epoch_first_slot = (current_epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;

    let next_schedule = rpc_client
        .get_unnested_leader_schedule(Some(next_epoch_first_slot))
        .await?
        .expect("Failed to fetch next leader schedule");

    let shared = Arc::new(RwLock::new(InnerManagedLeaderSchedule {
        double_buffer: [initial_schedule, next_schedule],
        fail: AtomicBool::new(false),
    }));

    let shared_clone = shared.clone();
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let loop_ct = cancellation_token.clone();
    let jh = tokio::spawn(async move {
        auto_leader_schedule_loop(config, shared_clone, rpc_client, loop_ct).await;
    });

    Ok((ManagedLeaderSchedule { inner: shared }, jh))
}

#[cfg(test)]
mod tests {
    use {
        rand::distr::{Distribution, weighted::WeightedIndex},
        solana_clock::DEFAULT_SLOTS_PER_EPOCH,
        solana_pubkey::Pubkey,
        std::collections::BTreeMap,
    };

    fn exponential_distribution(n: usize, base: f64, r: f64) -> Vec<u64> {
        // returns a vector of stake weights
        (0..n).map(|i| (base * r.powi(i as i32)) as u64).collect()
    }

    #[test]
    fn test_unnest_rpc_get_leader_schedule_resp() {
        let validator_rounds = DEFAULT_SLOTS_PER_EPOCH as usize / 4;
        const N: usize = 100;
        let stakes = exponential_distribution(N, 1_000_000.0, 0.97);
        let dist = WeightedIndex::new(&stakes).unwrap();

        let validator_keys = (0..N)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<Pubkey>>();

        let mut rng = rand::rng();
        let mut nested_schedule: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for round in 0..validator_rounds {
            let chosen_idx = dist.sample(&mut rng);
            let chosen_key = &validator_keys[chosen_idx];
            let entries = nested_schedule.entry(chosen_key.to_string()).or_default();

            for i in 0..4 {
                let slot_idx = round * 4 + i;
                entries.push(slot_idx);
            }
        }

        let compact_schedule =
            super::unnest_rpc_get_leader_schedule_resp(0, nested_schedule.iter());

        assert!(compact_schedule.first_slot == 0);

        let mut actual: BTreeMap<String, Vec<usize>> = Default::default();

        for (slot, pubkey) in compact_schedule.iter_unnested_schedule() {
            let pubkey_str = pubkey.to_string();
            assert!(
                nested_schedule.contains_key(&pubkey_str),
                "Unknown pubkey {pubkey_str}",
            );
            // println!("Slot {} assigned to leader {}", slot, pubkey_str);
            actual.entry(pubkey_str).or_default().push(slot as usize);
        }

        // // assert keys perflectly match
        assert_eq!(nested_schedule.len(), actual.len());
        let diff = nested_schedule
            .keys()
            .filter(|k| !actual.contains_key(*k))
            .collect::<Vec<_>>();

        assert!(diff.is_empty(), "Mismatched keys: {diff:?}");
        assert_eq!(nested_schedule, actual);
    }
}
