use {
    crate::{
        config::{ConfigBlocklist, YellowstoneBlocklist},
        grpc_geyser::{GeyserStreams, SlotUpdateInfoWithCommitment},
        metrics::jet as metrics,
        util::{fork_oneshot, CommitmentLevel, IncrementalBackoff},
    },
    futures::future::{try_join_all, FutureExt},
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_response::RpcContactInfo},
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        epoch_schedule::EpochSchedule,
        pubkey::Pubkey,
    },
    std::{collections::HashMap, future::Future, net::SocketAddr, ops::DerefMut, sync::Arc},
    tokio::{
        sync::{broadcast, oneshot, RwLock},
        time::{sleep, Duration, Instant},
    },
    tracing::{info, warn},
};

#[derive(Debug, Clone, Copy)]
pub struct TpuInfo {
    pub leader: Pubkey,
    pub slots: [Slot; NUM_CONSECUTIVE_LEADER_SLOTS as usize],
    pub quic: Option<SocketAddr>,
    pub quic_forwards: Option<SocketAddr>,
}

#[derive(Debug, Default)]
pub struct ClusterTpuInfoInner {
    processed_slot: Slot,
    epoch_schedule: EpochSchedule,
    leader_schedule: HashMap<Slot, Pubkey>,
    cluster_nodes: HashMap<Pubkey, RpcContactInfo>,
}

impl ClusterTpuInfoInner {
    pub async fn test_inner_cluster(
        leader_schedule: HashMap<Slot, Pubkey>,
        cluster_nodes: HashMap<Pubkey, RpcContactInfo>,
    ) -> Self {
        Self {
            leader_schedule,
            cluster_nodes,
            ..Default::default()
        }
    }

    pub async fn new(rpc: String) -> Self {
        Self {
            epoch_schedule: Self::get_epoch_schedule(rpc).await,
            ..Default::default()
        }
    }

    async fn get_epoch_schedule(rpc: String) -> EpochSchedule {
        let rpc = RpcClient::new(rpc);
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

#[async_trait::async_trait]
pub trait UpdateClusterScheduleInfo {
    async fn update_cluster_nodes(
        &self,
        shutdown: oneshot::Receiver<()>,
        rpc: String,
        cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()>;

    async fn update_leader_schedule(
        &self,
        shutdown: oneshot::Receiver<()>,
        rpc: String,
        slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()>;

    async fn get_inner_data(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterTpuInfoInner>;
}

type ClusterTpuInfoInnerRwLock = RwLock<ClusterTpuInfoInner>;

pub type ClusterTpuInfoInnerShared = Arc<dyn UpdateClusterScheduleInfo + Send + Sync + 'static>;

#[async_trait::async_trait]

impl UpdateClusterScheduleInfo for ClusterTpuInfoInnerRwLock {
    async fn get_inner_data(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterTpuInfoInner> {
        self.read().await
    }

    async fn update_cluster_nodes(
        &self,
        mut shutdown: oneshot::Receiver<()>,
        rpc: String,
        cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let rpc = RpcClient::new(rpc);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
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
            let mut inner = self.write().await;
            if inner.cluster_nodes.len() != nodes.len() {
                info!(
                    size = nodes.len(),
                    elapsed_ms = ts.elapsed().as_millis(),
                    "update total number of cluster nodes",
                );
            }
            inner.cluster_nodes = nodes;
            drop(inner);

            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = sleep(cluster_nodes_update_interval) => {}
            };
        }
    }

    async fn update_leader_schedule(
        &self,
        mut shutdown: oneshot::Receiver<()>,
        rpc: String,
        mut slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let rpc = RpcClient::new(rpc);

        let epoch_schedule = { self.read().await.epoch_schedule.clone() };

        loop {
            let mut slot_processed = None;
            let mut slot_schedule = None;

            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                message = slots_rx.recv() => match message {
                    Ok(slot_update) => match slot_update.commitment {
                        CommitmentLevel::Processed => {
                            slot_processed = Some(slot_update.slot)
                        }
                        CommitmentLevel::Confirmed | CommitmentLevel::Finalized => {
                            slot_schedule = Some(slot_update.slot);
                        }
                    },
                    Err(error) => {
                        anyhow::bail!("failed to receive slot: {error:?}");
                    }
                }
            }

            // forward to latest update
            while let Ok(slot_update_next) = slots_rx.try_recv() {
                match slot_update_next.commitment {
                    CommitmentLevel::Processed => {
                        slot_processed = Some(
                            slot_processed
                                .unwrap_or(slot_update_next.slot)
                                .max(slot_update_next.slot),
                        );
                    }
                    CommitmentLevel::Confirmed | CommitmentLevel::Finalized => {
                        slot_schedule = Some(
                            slot_schedule
                                .unwrap_or(slot_update_next.slot)
                                .max(slot_update_next.slot),
                        );
                    }
                }
            }

            if let Some(slot) = slot_processed {
                let mut locked = self.write().await;
                locked.processed_slot = slot;
            }

            let Some(slot) = slot_schedule else {
                continue;
            };

            // check that leader schedule exists for received slot
            let locked = self.read().await;
            if locked.leader_schedule.contains_key(&slot) {
                continue;
            }
            drop(locked);

            // update leader schedule
            backoff.reset();
            let slot_offset =
                epoch_schedule.get_first_slot_in_epoch(epoch_schedule.get_epoch(slot));
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = backoff.maybe_tick() => {}
                }

                let ts = Instant::now();
                match rpc.get_leader_schedule(Some(slot)).await {
                    Ok(Some(leader_schedule)) => {
                        let mut locked = self.write().await;

                        // remove outdated
                        locked
                            .leader_schedule
                            .retain(|leader_schedule_slot, _pubkey| {
                                // no magic, 42 is a silly number to left some leaders
                                leader_schedule_slot + 42 > slot
                            });

                        // update
                        let mut added = 0;
                        for (pubkey, slots) in leader_schedule {
                            match pubkey.parse() {
                                Ok(pubkey) => {
                                    for slot_index in slots {
                                        if locked
                                            .leader_schedule
                                            .insert(slot_offset + slot_index as u64, pubkey)
                                            .is_none()
                                        {
                                            added += 1;
                                        }
                                    }
                                }
                                Err(error) => warn!(
                                    "failed to parse leader schedule identity {pubkey}: {error:?}"
                                ),
                            }
                        }
                        metrics::cluster_leaders_schedule_set_size(locked.leader_schedule.len());
                        info!(
                            added,
                            total = locked.leader_schedule.len(),
                            elapsed_ms = ts.elapsed().as_millis(),
                            "update leader schedule"
                        );

                        break;
                    }
                    Ok(None) => {
                        metrics::cluster_leaders_schedule_set_size(0);
                        backoff.init();
                        warn!("failed to find schedule for leader schedule slot: {slot}");
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

#[derive(Clone)]
pub struct ClusterTpuInfo {
    inner: ClusterTpuInfoInnerShared,
    shutdown_update: Arc<RwLock<Option<oneshot::Sender<()>>>>,
    blocklist: ConfigBlocklist,
    yellowstone_blocklist: Arc<YellowstoneBlocklist>,
}

impl ClusterTpuInfo {
    pub async fn new<G>(
        rpc: String,
        grpc: &G,
        inner: ClusterTpuInfoInnerShared,
        cluster_nodes_update_interval: Duration,
        blocklist: ConfigBlocklist,
        yellowstone_blocklist: Arc<YellowstoneBlocklist>,
    ) -> (Self, impl Future<Output = Result<Vec<()>, anyhow::Error>>)
    where
        G: GeyserStreams + Send + Sync + 'static,
    {
        assert_eq!(NUM_CONSECUTIVE_LEADER_SLOTS, 4);

        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(RwLock::new(Some(tx)));
        let (rx1, rx2) = fork_oneshot(rx);

        let slots_rx = grpc.subscribe_slots();

        (
            Self {
                inner: Arc::clone(&inner),
                shutdown_update: tx,
                blocklist,
                yellowstone_blocklist,
            },
            async move {
                try_join_all([
                    inner
                        .update_leader_schedule(rx1, rpc.clone(), slots_rx)
                        .boxed(),
                    inner
                        .update_cluster_nodes(rx2, rpc, cluster_nodes_update_interval)
                        .boxed(),
                ])
                .await
            }
            .boxed(),
        )
    }

    pub async fn shutdown(&self) {
        let mut tx = self.shutdown_update.write().await;
        match tx.deref_mut().take() {
            Some(tx) => {
                let _ = tx.send(());
            }
            None => {
                warn!("shutdown already in progress");
            }
        }
    }

    pub async fn get_leader_tpus(
        &self,
        leader_forward_count: usize,
        extra_tpu_forwards: impl IntoIterator<Item = TpuInfo>,
        list_pda_keys: &[String],
    ) -> Vec<TpuInfo> {
        let inner = self.inner.get_inner_data().await;

        let mut blocklisted = 0;
        let allow_lists = self.yellowstone_blocklist.allow_lists.read().await;
        let deny_lists = self.yellowstone_blocklist.deny_lists.read().await;

        let mut tpus = (0..=leader_forward_count as u64)
            .filter_map(|i| {
                let leader_slot = inner.processed_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
                inner.get_tpu_info(leader_slot).and_then(|info| {
                    let mut is_allow = false;

                    for key_list in list_pda_keys.iter() {
                        if let Some(deny_hash) = deny_lists.get(key_list) {
                            if deny_hash.contains(&info.leader) {
                                blocklisted += 1;
                                return None;
                            }
                        }

                        if let Some(allow_hash) = allow_lists.get(key_list) {
                            if !allow_hash.contains(&info.leader) {
                                blocklisted += 1;
                                return None;
                            } else {
                                is_allow = true;
                            }
                        }
                    }

                    if !is_allow && self.blocklist.leaders.contains(&info.leader) {
                        blocklisted += 1;
                        return None;
                    }

                    Some(info)
                })
            })
            .collect::<Vec<_>>();

        'outside: for extra_tpu in extra_tpu_forwards.into_iter() {
            let mut is_allow = false;
            for key_list in list_pda_keys.iter() {
                if let Some(deny_hash) = deny_lists.get(key_list) {
                    if deny_hash.contains(&extra_tpu.leader) {
                        blocklisted += 1;
                        continue 'outside;
                    }
                }

                if let Some(allow_hash) = allow_lists.get(key_list) {
                    if !allow_hash.contains(&extra_tpu.leader) {
                        blocklisted += 1;
                        continue 'outside;
                    } else {
                        is_allow = true;
                    }
                }
            }

            if !is_allow && self.blocklist.leaders.contains(&extra_tpu.leader) {
                blocklisted += 1;
            } else if tpus.iter().all(|tpu| tpu.leader != extra_tpu.leader) {
                let contact_info = inner.cluster_nodes.get(&extra_tpu.leader);
                tpus.push(TpuInfo {
                    leader: extra_tpu.leader,
                    slots: extra_tpu.slots,
                    quic: extra_tpu
                        .quic
                        .or(contact_info.and_then(|info| info.tpu_quic)),
                    quic_forwards: extra_tpu
                        .quic_forwards
                        .or(contact_info.and_then(|info| info.tpu_forwards_quic)),
                });
            }
        }

        metrics::sts_tpu_blocklisted_inc(blocklisted);

        tpus
    }
}
