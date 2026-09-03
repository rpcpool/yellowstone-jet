use {
    crate::{
        grpc_geyser::SlotUpdateWithStatus,
        metrics::jet as metrics,
        recent_leader_slot::{RecentLeaderSlots, SlotEvent},
        util::{IncrementalBackoff, SlotStatus},
    },
    arc_swap::ArcSwap,
    futures::future::FutureExt,
    solana_client::{
        client_error::Result as ClientResult,
        nonblocking::rpc_client::RpcClient,
        rpc_response::{RpcContactInfo, RpcLeaderSchedule},
    },
    solana_clock::Slot,
    solana_epoch_schedule::EpochSchedule,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc},
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
}

#[async_trait::async_trait]
impl ClusterTpuInfoProvider for ClusterTpuInfo {
    fn latest_seen_slot(&self) -> Slot {
        self.latest_seen_slot()
    }
}

// Number of extra leader slots to keep in the schedule after the current slot
// This provides a buffer to avoid constantly fetching new schedules
const LEADER_SCHEDULE_RETENTION_SLOTS: u64 = 42;

const NUM_CONSECUTIVE_LEADER_SLOTS: u64 = 4;

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
    latest_seen_slot: ArcSwap<Slot>,
    epoch_schedule: ArcSwap<EpochSchedule>,
    leader_schedule: ArcSwap<HashMap<Slot, Pubkey>>,
    cluster_nodes: ArcSwap<HashMap<Pubkey, RpcContactInfo>>,
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
        let leader_schedule = self.leader_schedule.load();
        let cluster_nodes = self.cluster_nodes.load();
        let epoch_schedule = self.epoch_schedule.load();

        if let Some(leader) = leader_schedule.get(&leader_slot) {
            if let Some(tpu_info) = cluster_nodes.get(leader) {
                let (epoch, index) = epoch_schedule.get_epoch_and_slot_index(leader_slot);
                let slot = epoch_schedule.get_first_slot_in_epoch(epoch) + index
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
    inner: Arc<ClusterTpuInfoInner>,
}

impl ClusterTpuInfo {
    pub async fn new(
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
        cluster_nodes_update_interval: Duration,
        cancellation_token: CancellationToken,
    ) -> (Self, impl Future<Output = ()>) {
        let inner = Arc::new(ClusterTpuInfoInner {
            epoch_schedule: ArcSwap::from_pointee(
                ClusterTpuInfoInner::get_epoch_schedule(Arc::clone(&rpc)).await,
            ),
            ..Default::default()
        });

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
        *self.inner.latest_seen_slot.load().as_ref()
    }

    pub fn get_cluster_nodes(&self) -> HashMap<Pubkey, RpcContactInfo> {
        self.inner.cluster_nodes.load().as_ref().clone()
    }

    pub fn get_rpc_contact_info(&self, pubkey: &Pubkey) -> Option<RpcContactInfo> {
        self.inner.cluster_nodes.load().get(pubkey).cloned()
    }

    pub fn get_solana_client_for_peer(&self, peer_pubkey: &Pubkey) -> Option<String> {
        self.inner
            .cluster_nodes
            .load()
            .get(peer_pubkey)
            .and_then(|info| info.client_id.clone())
    }

    pub fn get_leader_schedule(&self) -> HashMap<Slot, Pubkey> {
        self.inner.leader_schedule.load().as_ref().clone()
    }

    async fn update_cluster_nodes(
        inner: Arc<ClusterTpuInfoInner>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let mut old_cluster = inner.cluster_nodes.load().as_ref().clone();
        loop {
            backoff.maybe_tick().await;

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
                inner.cluster_nodes.store(Arc::new(nodes.clone()));
                old_cluster = nodes;
            }

            tokio::select! {
                _ = sleep(cluster_nodes_update_interval) => {}
            };
        }
    }
    async fn update_latest_slot_and_leader_schedule(
        inner: Arc<ClusterTpuInfoInner>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        mut slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let epoch_schedule = inner.epoch_schedule.load_full();
        let mut max_slot = *inner.latest_seen_slot.load().as_ref();
        let mut last_slot_instant = Instant::now();

        let mut current_slot_estimator = RecentLeaderSlots::new();
        loop {
            let iteration_start = Instant::now();

            tokio::select! {
                message = slots_rx.recv() => match message {
                    Ok(slot_update) => {
                        match slot_update.slot_status {
                            SlotStatus::SlotFirstShredReceived => {
                                current_slot_estimator.record(SlotEvent::Start(slot_update.slot));
                            }
                            SlotStatus::SlotCompleted => {
                                current_slot_estimator.record(SlotEvent::End(slot_update.slot));
                            }
                            _ => {
                                continue;
                            }
                        }
                        metrics::incr_slot_status_received_by_type(slot_update.slot_status.as_str());
                        debug!("Received {} for slot {}", slot_update.slot_status.as_str(), slot_update.slot);
                    },
                    Err(error) => {
                        anyhow::bail!("failed to receive slot: {error:?}");
                    }
                }
            };

            // Consume all pending updates to get the highest slot
            while let Ok(slot_update_next) = slots_rx.try_recv() {
                metrics::incr_slot_status_received_by_type(slot_update_next.slot_status.as_str());

                match slot_update_next.slot_status {
                    SlotStatus::SlotFirstShredReceived => {
                        current_slot_estimator.record(SlotEvent::Start(slot_update_next.slot));
                    }
                    SlotStatus::SlotCompleted => {
                        current_slot_estimator.record(SlotEvent::End(slot_update_next.slot));
                    }
                    _ => {
                        continue;
                    }
                }
            }

            metrics::observe_new_slot_arrival_interval(last_slot_instant.elapsed());

            let estimated_current_slot = current_slot_estimator.estimate_current_slot();
            if max_slot >= estimated_current_slot {
                continue;
            }
            max_slot = estimated_current_slot;
            last_slot_instant = Instant::now();

            inner.latest_seen_slot.store(Arc::new(max_slot));
            let leader_schedule = inner.leader_schedule.load();
            let need_schedule_update = !leader_schedule.contains_key(&max_slot);
            metrics::set_leader_schedule_size(leader_schedule.len());

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

                            let mut updated_leader_schedule =
                                inner.leader_schedule.load().as_ref().clone();
                            // Track entries before cleanup
                            let entries_before = updated_leader_schedule.len();

                            // Clean up old leader schedule entries
                            updated_leader_schedule.retain(|leader_schedule_slot, _pubkey| {
                                *leader_schedule_slot + LEADER_SCHEDULE_RETENTION_SLOTS > max_slot
                            });

                            let entries_cleaned = entries_before - updated_leader_schedule.len();
                            metrics::set_leader_schedule_entries_cleaned(entries_cleaned);

                            // Add new leader schedule entries
                            let mut added = 0;
                            for (pubkey_str, slot_indices) in leader_schedule {
                                match pubkey_str.parse::<Pubkey>() {
                                    Ok(pubkey) => {
                                        for slot_index in slot_indices {
                                            let absolute_slot =
                                                epoch_start_slot + slot_index as u64;
                                            if updated_leader_schedule
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

                            inner
                                .leader_schedule
                                .store(Arc::new(updated_leader_schedule));
                            let leader_schedule_size = inner.leader_schedule.load().len();

                            metrics::set_leader_schedule_entries_added(added);
                            metrics::cluster_leaders_schedule_set_size(leader_schedule_size);
                            metrics::set_leader_schedule_size(leader_schedule_size);

                            info!(
                                added,
                                total = leader_schedule_size,
                                elapsed_ms = rpc_start.elapsed().as_millis(),
                                "updated leader schedule for epoch {}",
                                epoch
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

    pub fn get_leader_tpus(
        &self,
        leader_forward_count: usize,
    ) -> impl IntoIterator<Item = TpuInfo> {
        let inner = &self.inner;
        let latest_seen_slot = *inner.latest_seen_slot.load().as_ref();

        (0..=leader_forward_count as u64).filter_map(move |i| {
            let leader_slot = latest_seen_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
            inner.get_tpu_info(leader_slot)
        })
    }
}
