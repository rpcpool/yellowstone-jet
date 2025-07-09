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
    solana_clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_epoch_schedule::EpochSchedule,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc},
    tokio::{
        sync::{broadcast, RwLock},
        time::{sleep, Duration, Instant},
    },
    tracing::{debug, info, warn},
};

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
    inner: Arc<RwLock<ClusterTpuInfoInner>>,
    shutdown_update: broadcast::Sender<()>,
}

impl ClusterTpuInfo {
    pub async fn new(
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
        cluster_nodes_update_interval: Duration,
    ) -> (Self, impl Future<Output = ()>) {
        assert_eq!(NUM_CONSECUTIVE_LEADER_SLOTS, 4);

        let (tx, mut rx) = broadcast::channel(1);
        let inner = Arc::new(RwLock::new(ClusterTpuInfoInner {
            epoch_schedule: ClusterTpuInfoInner::get_epoch_schedule(Arc::clone(&rpc)).await,
            ..Default::default()
        }));

        (
            Self {
                inner: Arc::clone(&inner),
                shutdown_update: tx
            },
            async move {
                tokio::select! {
                    _ = rx.recv() => {
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

    pub async fn latest_seen_slot(&self) -> Slot {
        self.inner.read().await.latest_seen_slot
    }

    pub async fn get_cluster_nodes(&self) -> HashMap<Pubkey, RpcContactInfo> {
        self.inner.read().await.cluster_nodes.clone()
    }

    pub async fn get_leader_schedule(&self) -> HashMap<Slot, Pubkey> {
        self.inner.read().await.leader_schedule.clone()
    }

    async fn update_cluster_nodes(
        inner: Arc<RwLock<ClusterTpuInfoInner>>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let mut old_cluster = { inner.read().await.cluster_nodes.clone() };
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
                let mut inner = inner.write().await;
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
        inner: Arc<RwLock<ClusterTpuInfoInner>>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        mut slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let epoch_schedule = { inner.read().await.epoch_schedule.clone() };

        loop {
            let mut new_latest_slot = None;

            tokio::select! {
                message = slots_rx.recv() => match message {
                    Ok(slot_update) => {
                        if slot_update.slot_status == SlotStatus::SlotFirstShredReceived {
                            new_latest_slot = Some(slot_update.slot);
                            debug!("Received FirstShredReceived for slot {}", slot_update.slot);
                        }
                    },
                    Err(error) => {
                        anyhow::bail!("failed to receive slot: {error:?}");
                    }
                }
            }

            // Consume all pending updates to get the latest slot
            while let Ok(slot_update_next) = slots_rx.try_recv() {
                if slot_update_next.slot_status == SlotStatus::SlotFirstShredReceived {
                    new_latest_slot = Some(
                        new_latest_slot
                            .unwrap_or(slot_update_next.slot)
                            .max(slot_update_next.slot),
                    );
                }
            }

            if let Some(slot) = new_latest_slot {
                let mut locked = inner.write().await;
                let previous_slot = locked.latest_seen_slot;
                locked.latest_seen_slot = slot;
                drop(locked);

                debug!(
                    "Updated latest seen slot from {} to {}",
                    previous_slot, slot
                );

                // Check if we need to update the leader schedule
                // We fetch the schedule for the entire epoch when we enter a new epoch
                // or when we don't have schedule data for the current slot
                let locked = inner.read().await;
                let need_schedule_update = !locked.leader_schedule.contains_key(&slot);
                drop(locked);

                if need_schedule_update {
                    // Get the first slot of the epoch that contains our current slot
                    let epoch = epoch_schedule.get_epoch(slot);
                    let epoch_start_slot = epoch_schedule.get_first_slot_in_epoch(epoch);

                    info!(
                        "Need to fetch leader schedule for epoch {} (slot {} is in this epoch)",
                        epoch, slot
                    );

                    // Fetch the leader schedule with retries
                    backoff.reset();
                    loop {
                        tokio::select! {
                            _ = backoff.maybe_tick() => {}
                        }

                        let ts = Instant::now();
                        match rpc.get_leader_schedule(Some(slot)).await {
                            Ok(Some(leader_schedule)) => {
                                let mut locked = inner.write().await;

                                // Clean up old leader schedule entries
                                // Keep LEADER_SCHEDULE_RETENTION_SLOTS slots before current slot
                                locked
                                    .leader_schedule
                                    .retain(|leader_schedule_slot, _pubkey| {
                                        *leader_schedule_slot + LEADER_SCHEDULE_RETENTION_SLOTS
                                            > slot
                                    });

                                // Add new leader schedule entries
                                // The RPC returns a map of validator pubkey -> array of slot indices within the epoch
                                let mut added = 0;
                                for (pubkey_str, slot_indices) in leader_schedule {
                                    match pubkey_str.parse::<Pubkey>() {
                                        Ok(pubkey) => {
                                            for slot_index in slot_indices {
                                                let absolute_slot = epoch_start_slot + slot_index as u64;
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

                                metrics::cluster_leaders_schedule_set_size(
                                    locked.leader_schedule.len(),
                                );
                                info!(
                                    added,
                                    total = locked.leader_schedule.len(),
                                    elapsed_ms = ts.elapsed().as_millis(),
                                    "updated leader schedule for epoch {}",
                                    epoch
                                );

                                break;
                            }
                            Ok(None) => {
                                metrics::cluster_leaders_schedule_set_size(0);
                                backoff.init();
                                warn!("RPC returned no leader schedule for slot: {}", slot);
                            }
                            Err(error) => {
                                metrics::cluster_leaders_schedule_set_size(0);
                                backoff.init();
                                warn!("failed to get leader schedule: {error:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    // I don't really know if this is necessary. I could just add an underscore before shutdown_update and it would work
    // In any case, those futures will be shutdown if all tx references are dropped or task_group cancels them
    pub async fn shutdown(&self) {
        let _ = self.shutdown_update.send(());
    }

    pub async fn get_leader_tpus(&self, leader_forward_count: usize) -> Vec<TpuInfo> {
        let inner = self.inner.read().await;

        // TODO: Improve leader selection logic
        // Current behavior: Returns (leader_forward_count + 1) leaders total
        // - With leader_forward_count = 4, returns 5 leaders: current + 4 future
        // - Range 0..=leader_forward_count means indices 0,1,2,3,4
        //
        // Future improvements:
        // 1. Make leader_forward_count return exactly that many leaders (not +1)
        // 2. Add granular control to skip current leader if we're at their last slot
        // 3. Consider slot position within leader's 4-slot allocation
        // 4. Add option to exclude current leader entirely and only forward
        //
        // Example: If current slot is 103 (last slot for leader A who has 100-103),
        // we might want to skip leader A and get the next N leaders instead

        // Get TPU info for current and future leaders based on latest_seen_slot
        (0..=leader_forward_count as u64)
            .filter_map(|i| {
                let leader_slot = inner.latest_seen_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
                inner.get_tpu_info(leader_slot)
            })
            .collect::<Vec<_>>()
    }

    pub async fn get_latest_seen_slot(&self) -> Slot {
        self.inner.read().await.latest_seen_slot
    }
}
