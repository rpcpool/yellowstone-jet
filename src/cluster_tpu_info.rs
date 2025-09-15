use {
    crate::{
        grpc_geyser::SlotUpdateWithStatus,
        metrics::jet as metrics,
        util::{IncrementalBackoff, SlotStatus},
    },
    futures::future::FutureExt,
    solana_client::{
        client_error::Result as ClientResult,
        nonblocking::rpc_client::RpcClient,
        rpc_response::{RpcContactInfo, RpcLeaderSchedule},
    },
    solana_clock::{NUM_CONSECUTIVE_LEADER_SLOTS, Slot},
    solana_epoch_schedule::EpochSchedule,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        future::Future,
        net::SocketAddr,
        sync::{Arc, RwLock as StdRwLock},
    },
    tokio::{
        sync::broadcast,
        time::{Duration, Instant, sleep},
    },
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

#[async_trait::async_trait]
pub trait ClusterTpuInfoProvider: Send + Sync {
    fn latest_seen_slot(&self) -> Slot;
    fn get_cluster_nodes(&self) -> HashMap<Pubkey, RpcContactInfo>;
    fn get_leader_schedule(&self) -> HashMap<Slot, Pubkey>;
    fn get_leader_tpus(&self, leader_forward_count: usize) -> Vec<TpuInfo>;
}

#[async_trait::async_trait]
impl ClusterTpuInfoProvider for ClusterTpuInfo {
    fn latest_seen_slot(&self) -> Slot {
        self.latest_seen_slot()
    }

    fn get_cluster_nodes(&self) -> HashMap<Pubkey, RpcContactInfo> {
        self.get_cluster_nodes()
    }

    fn get_leader_schedule(&self) -> HashMap<Slot, Pubkey> {
        self.get_leader_schedule()
    }

    fn get_leader_tpus(&self, leader_forward_count: usize) -> Vec<TpuInfo> {
        self.get_leader_tpus(leader_forward_count)
    }
}

// Number of extra leader slots to keep in the schedule after the current slot
// This provides a buffer to avoid constantly fetching new schedules
const LEADER_SCHEDULE_RETENTION_SLOTS: u64 = 42;

#[derive(Debug, Clone, Copy)]
pub struct TpuInfo {
    pub leader: Pubkey,
    pub slots: [Slot; NUM_CONSECUTIVE_LEADER_SLOTS as usize],
    pub quic: Option<SocketAddr>,
    pub quic_forwards: Option<SocketAddr>,
}

#[async_trait::async_trait]
pub trait ClusterTpuRpcClient {
    async fn get_leader_schedule(
        &self,
        slot: Option<Slot>,
    ) -> ClientResult<Option<RpcLeaderSchedule>>;

    async fn get_cluster_nodes(&self) -> ClientResult<Vec<RpcContactInfo>>;
    async fn get_epoch_schedule(&self) -> ClientResult<EpochSchedule>;
}

#[async_trait::async_trait]
impl ClusterTpuRpcClient for RpcClient {
    async fn get_leader_schedule(
        &self,
        slot: Option<Slot>,
    ) -> ClientResult<Option<RpcLeaderSchedule>> {
        self.get_leader_schedule(slot).await
    }

    async fn get_cluster_nodes(&self) -> ClientResult<Vec<RpcContactInfo>> {
        self.get_cluster_nodes().await
    }

    async fn get_epoch_schedule(&self) -> ClientResult<EpochSchedule> {
        self.get_epoch_schedule().await
    }
}

#[derive(Debug, Default)]
struct ClusterTpuInfoInner {
    // Latest slot we've seen from FirstShredReceived
    latest_seen_slot: Slot,
    epoch_schedule: EpochSchedule,
    leader_schedule: HashMap<Slot, Pubkey>,
    cluster_nodes: HashMap<Pubkey, RpcContactInfo>,
}

impl ClusterTpuInfoInner {
    async fn get_epoch_schedule(
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
    ) -> EpochSchedule {
        let mut backoff = IncrementalBackoff::default();
        loop {
            backoff.maybe_tick().await;
            match rpc.get_epoch_schedule().await {
                Ok(epoch_schedule) => break epoch_schedule,
                Err(error) => {
                    backoff.init();
                    warn!("failed to get epoch schedule: {error:?}");
                }
            }
        }
    }

    fn get_tpu_info(&self, leader_slot: Slot) -> Option<TpuInfo> {
        if let Some(leader) = self.leader_schedule.get(&leader_slot) {
            if let Some(tpu_info) = self.cluster_nodes.get(leader) {
                let (epoch, index) = self.epoch_schedule.get_epoch_and_slot_index(leader_slot);
                let slot = self.epoch_schedule.get_first_slot_in_epoch(epoch) + index
                    - index % NUM_CONSECUTIVE_LEADER_SLOTS;
                return Some(TpuInfo {
                    leader: *leader,
                    slots: [slot, slot + 1, slot + 2, slot + 3],
                    quic: tpu_info.tpu_quic,
                    quic_forwards: tpu_info.tpu_forwards_quic,
                });
            }
        }

        None
    }
}

#[derive(Clone)]
pub struct ClusterTpuInfo {
    inner: Arc<StdRwLock<ClusterTpuInfoInner>>,
}

impl ClusterTpuInfo {
    pub async fn new(
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
        cluster_nodes_update_interval: Duration,
        cancellation_token: CancellationToken,
    ) -> (Self, impl Future<Output = ()>) {
        assert_eq!(NUM_CONSECUTIVE_LEADER_SLOTS, 4);

        let inner = Arc::new(StdRwLock::new(ClusterTpuInfoInner {
            epoch_schedule: ClusterTpuInfoInner::get_epoch_schedule(Arc::clone(&rpc)).await,
            ..Default::default()
        }));

        (
            Self {
                inner: Arc::clone(&inner),
            },
            async move {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("shutdown signal received in ClusterTpuInfo");
                    }
                    _ = ClusterTpuInfo::update_latest_slot_and_leader_schedule(
                        Arc::clone(&inner),
                        Arc::clone(&rpc),
                        slots_rx,
                    ) => {
                        info!("Update leader schedule suddenly finished");
                    }
                    _ = ClusterTpuInfo::update_cluster_nodes(inner, rpc, cluster_nodes_update_interval) => {
                        info!("Update cluster nodes suddenly finished");
                    }
                }
            }
            .boxed(),
        )
    }

    pub fn latest_seen_slot(&self) -> Slot {
        let start = Instant::now();
        let result = self
            .inner
            .read()
            .expect("rwlock schedule poisoned")
            .latest_seen_slot;
        metrics::observe_cluster_tpu_lock_time("latest_seen_slot", "read", start.elapsed());
        result
    }

    pub fn get_cluster_nodes(&self) -> HashMap<Pubkey, RpcContactInfo> {
        self.inner
            .read()
            .expect("rwlock schedule poisoned")
            .cluster_nodes
            .clone()
    }

    pub fn get_leader_schedule(&self) -> HashMap<Slot, Pubkey> {
        self.inner
            .read()
            .expect("rwlock schedule poisoned")
            .leader_schedule
            .clone()
    }

    async fn update_cluster_nodes(
        inner: Arc<StdRwLock<ClusterTpuInfoInner>>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let mut old_cluster = {
            inner
                .read()
                .expect("rwlock schedule poisoned")
                .cluster_nodes
                .clone()
        };
        loop {
            tokio::select! {
                _ = backoff.maybe_tick() => {}
            }

            let ts = Instant::now();
            let nodes = match rpc.get_cluster_nodes().await {
                Ok(nodes) => {
                    backoff.reset();
                    nodes
                        .into_iter()
                        .filter_map(|info| match info.pubkey.parse() {
                            Ok(pubkey) => Some((pubkey, info)),
                            Err(error) => {
                                warn!(
                                    "failed to parse cluster node identity {}: {error:?}",
                                    info.pubkey
                                );
                                None
                            }
                        })
                        .collect::<HashMap<Pubkey, RpcContactInfo>>()
                }
                Err(error) => {
                    metrics::cluster_nodes_set_size(0);
                    warn!("failed to get cluster nodes: {error:?}");
                    backoff.init();
                    continue;
                }
            };

            metrics::cluster_nodes_set_size(nodes.len());
            if old_cluster != nodes {
                if old_cluster.len() != nodes.len() {
                    info!(
                        size = nodes.len(),
                        elapsed_ms = ts.elapsed().as_millis(),
                        "update total number of cluster nodes",
                    );
                }
                let mut inner = inner.write().expect("rwlock schedule poisoned");
                inner.cluster_nodes = nodes.clone();
                old_cluster = nodes;
                drop(inner);
            }

            tokio::select! {
                _ = sleep(cluster_nodes_update_interval) => {}
            };
        }
    }
    async fn update_latest_slot_and_leader_schedule(
        inner: Arc<StdRwLock<ClusterTpuInfoInner>>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        mut slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let epoch_schedule = {
            inner
                .read()
                .expect("rwlock schedule poisoned")
                .epoch_schedule
                .clone()
        };
        let mut max_slot = {
            inner
                .read()
                .expect("rwlock schedule poisoned")
                .latest_seen_slot
        };
        let mut last_slot_instant = Instant::now();

        loop {
            let iteration_start = Instant::now();

            let mut new_latest_slot = tokio::select! {
                message = slots_rx.recv() => match message {
                    Ok(slot_update) => {
                        metrics::incr_slot_status_received_by_type(slot_update.slot_status.as_str());

                        if [SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized].contains(&slot_update.slot_status) {
                            continue;
                        }

                        debug!("Received {} for slot {}", slot_update.slot_status.as_str(), slot_update.slot);
                        slot_update.slot
                    },
                    Err(error) => {
                        anyhow::bail!("failed to receive slot: {error:?}");
                    }
                }
            };

            // Consume all pending updates to get the highest slot
            let mut updates_drained = 1;
            while let Ok(slot_update_next) = slots_rx.try_recv() {
                updates_drained += 1;
                metrics::incr_slot_status_received_by_type(slot_update_next.slot_status.as_str());

                if ![SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized]
                    .contains(&slot_update_next.slot_status)
                {
                    new_latest_slot = new_latest_slot.max(slot_update_next.slot);
                }
            }

            metrics::observe_slot_updates_drained_count(updates_drained);

            if max_slot >= new_latest_slot {
                continue;
            }
            metrics::observe_new_slot_arrival_interval(last_slot_instant.elapsed());
            last_slot_instant = Instant::now();

            max_slot = max_slot.max(new_latest_slot);

            let need_schedule_update = {
                let check_start = Instant::now();
                let lock_start = Instant::now();
                let mut locked = inner.write().expect("rwlock schedule poisoned");
                metrics::observe_cluster_tpu_lock_time(
                    "update_and_check_schedule",
                    "write",
                    lock_start.elapsed(),
                );

                locked.latest_seen_slot = max_slot;
                let need_update = !locked.leader_schedule.contains_key(&max_slot);

                metrics::observe_leader_schedule_exists_check_time(check_start.elapsed());
                metrics::set_leader_schedule_size(locked.leader_schedule.len());

                need_update
            };

            if need_schedule_update {
                // Get the first slot of the epoch that contains our current slot
                let epoch = epoch_schedule.get_epoch(max_slot);
                let epoch_start_slot = epoch_schedule.get_first_slot_in_epoch(epoch);

                info!(
                    "Need to fetch leader schedule for epoch {} (slot {} is in this epoch)",
                    epoch, max_slot
                );

                // Fetch the leader schedule with retries
                backoff.reset();
                loop {
                    backoff.maybe_tick().await;
                    metrics::incr_leader_schedule_rpc_attempts();
                    let rpc_start = Instant::now();
                    match rpc.get_leader_schedule(Some(max_slot)).await {
                        Ok(Some(leader_schedule)) => {
                            metrics::observe_leader_schedule_rpc_fetch_time(rpc_start.elapsed());

                            let parse_start = Instant::now();
                            let lock_start = Instant::now();
                            let mut locked =
                                inner.write().expect("rwlock epoch schedule is poisoned");
                            metrics::observe_cluster_tpu_lock_time(
                                "update_leader_schedule",
                                "write",
                                lock_start.elapsed(),
                            );

                            // Track entries before cleanup
                            let entries_before = locked.leader_schedule.len();

                            // Clean up old leader schedule entries
                            locked
                                .leader_schedule
                                .retain(|leader_schedule_slot, _pubkey| {
                                    *leader_schedule_slot + LEADER_SCHEDULE_RETENTION_SLOTS
                                        > max_slot
                                });

                            let entries_cleaned = entries_before - locked.leader_schedule.len();
                            metrics::set_leader_schedule_entries_cleaned(entries_cleaned);

                            // Add new leader schedule entries
                            let mut added = 0;
                            for (pubkey_str, slot_indices) in leader_schedule {
                                match pubkey_str.parse::<Pubkey>() {
                                    Ok(pubkey) => {
                                        for slot_index in slot_indices {
                                            let absolute_slot =
                                                epoch_start_slot + slot_index as u64;
                                            if locked
                                                .leader_schedule
                                                .insert(absolute_slot, pubkey)
                                                .is_none()
                                            {
                                                added += 1;
                                            }
                                        }
                                    }
                                    Err(error) => warn!(
                                        "failed to parse leader schedule identity {}: {error:?}",
                                        pubkey_str
                                    ),
                                }
                            }

                            metrics::set_leader_schedule_entries_added(added);
                            metrics::cluster_leaders_schedule_set_size(
                                locked.leader_schedule.len(),
                            );
                            metrics::set_leader_schedule_size(locked.leader_schedule.len());

                            info!(
                                added,
                                total = locked.leader_schedule.len(),
                                elapsed_ms = rpc_start.elapsed().as_millis(),
                                "updated leader schedule for epoch {}",
                                epoch
                            );

                            drop(locked);
                            metrics::observe_leader_schedule_parse_insert_time(
                                parse_start.elapsed(),
                            );

                            break;
                        }
                        Ok(None) => {
                            metrics::cluster_leaders_schedule_set_size(0);
                            backoff.init();
                            warn!("RPC returned no leader schedule for slot: {}", max_slot);
                        }
                        Err(error) => {
                            metrics::cluster_leaders_schedule_set_size(0);
                            backoff.init();
                            warn!("failed to get leader schedule: {error:?}");
                        }
                    }
                }
            }

            metrics::observe_slot_update_loop_iteration_time(iteration_start.elapsed());
        }
    }

    pub fn get_leader_tpus(&self, leader_forward_count: usize) -> Vec<TpuInfo> {
        let start = Instant::now();
        let inner = self
            .inner
            .read()
            .expect("rwlock epoch schedule is poisoned");

        let result = (0..=leader_forward_count as u64)
            .filter_map(|i| {
                let leader_slot = inner.latest_seen_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
                inner.get_tpu_info(leader_slot)
            })
            .collect::<Vec<_>>();

        metrics::observe_cluster_tpu_lock_time("get_leader_tpus", "read", start.elapsed());
        result
    }
}
