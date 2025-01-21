use {
    crate::{
        config::ConfigBlocklist,
        grpc_geyser::{GeyserSubscriber, SlotUpdateInfoWithCommitment},
        metrics::jet as metrics,
        util::{
            CommitmentLevel, IncrementalBackoff, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    futures::future::{try_join_all, FutureExt},
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_response::RpcContactInfo},
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        epoch_schedule::EpochSchedule,
        pubkey::Pubkey,
    },
    std::{collections::HashMap, net::SocketAddr, ops::DerefMut, sync::Arc},
    tokio::{
        sync::{broadcast, Notify, RwLock},
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
struct ClusterTpuInfoInner {
    processed_slot: Slot,
    epoch_schedule: EpochSchedule,
    leader_schedule: HashMap<Slot, Pubkey>,
    cluster_nodes: HashMap<Pubkey, RpcContactInfo>,
}

impl ClusterTpuInfoInner {
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

type ClusterTpuInfoInnerShared = Arc<RwLock<ClusterTpuInfoInner>>;

#[derive(Debug, Clone)]
pub struct ClusterTpuInfo {
    inner: ClusterTpuInfoInnerShared,
    shutdown_schedule: Arc<Notify>,
    shutdown_nodes: Arc<Notify>,
    join_handle: WaitShutdownSharedJoinHandle,
    blocklist: ConfigBlocklist,
}

impl WaitShutdown for ClusterTpuInfo {
    fn shutdown(&self) {
        self.shutdown_schedule.notify_one();
        self.shutdown_nodes.notify_one();
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        let mut locked = self.join_handle.lock().await;
        locked.deref_mut().await
    }
}

impl ClusterTpuInfo {
    pub async fn new(
        rpc: String,
        grpc: &GeyserSubscriber,
        cluster_nodes_update_interval: Duration,
        blocklist: ConfigBlocklist,
    ) -> Self {
        assert_eq!(NUM_CONSECUTIVE_LEADER_SLOTS, 4);

        let shutdown_schedule = Arc::new(Notify::new());
        let shutdown_nodes = Arc::new(Notify::new());

        let inner = Arc::new(RwLock::new(ClusterTpuInfoInner {
            epoch_schedule: Self::get_epoch_schedule(rpc.clone()).await,
            ..Default::default()
        }));
        let slots_rx = grpc.subscribe_slots();

        Self {
            inner: Arc::clone(&inner),
            shutdown_schedule: Arc::clone(&shutdown_schedule),
            shutdown_nodes: Arc::clone(&shutdown_nodes),
            join_handle: Self::spawn(async move {
                try_join_all(&mut [
                    Self::update_leader_schedule(
                        shutdown_schedule,
                        Arc::clone(&inner),
                        rpc.clone(),
                        slots_rx,
                    )
                    .boxed(),
                    Self::update_cluster_nodes(
                        inner,
                        rpc,
                        cluster_nodes_update_interval,
                        shutdown_nodes,
                    )
                    .boxed(),
                ])
                .await
                .map(|_| ())
            }),
            blocklist,
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

    async fn update_leader_schedule(
        shutdown: Arc<Notify>,
        inner: ClusterTpuInfoInnerShared,
        rpc: String,
        mut slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let rpc = RpcClient::new(rpc);

        let epoch_schedule = { inner.read().await.epoch_schedule.clone() };

        loop {
            let mut slot_processed = None;
            let mut slot_schedule = None;

            tokio::select! {
                _ = shutdown.notified() => return Ok(()),
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
                let mut locked = inner.write().await;
                locked.processed_slot = slot;
            }

            let Some(slot) = slot_schedule else {
                continue;
            };

            // check that leader schedule exists for received slot
            let locked = inner.read().await;
            if locked.leader_schedule.contains_key(&slot) {
                continue;
            }
            drop(locked);

            // update leader schedule
            backoff.reset();
            let slot_offset =
                epoch_schedule.get_first_slot_in_epoch(epoch_schedule.get_epoch(slot));
            loop {
                backoff.maybe_tick().await;

                let ts = Instant::now();
                match rpc.get_leader_schedule(Some(slot)).await {
                    Ok(Some(leader_schedule)) => {
                        let mut locked = inner.write().await;

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

    async fn update_cluster_nodes(
        inner: ClusterTpuInfoInnerShared,
        rpc: String,
        cluster_nodes_update_interval: Duration,
        shutdown: Arc<Notify>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        let rpc = RpcClient::new(rpc);
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
            let mut inner = inner.write().await;
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
                _ = shutdown.notified() => return Ok(()),
                _ = sleep(cluster_nodes_update_interval) => {}
            };
        }
    }

    pub async fn get_leader_tpus(
        &self,
        leader_forward_count: usize,
        extra_tpu_forwards: impl IntoIterator<Item = TpuInfo>,
    ) -> Vec<TpuInfo> {
        let inner = self.inner.read().await;

        let mut blocklisted = 0;

        let mut tpus = (0..=leader_forward_count as u64)
            .filter_map(|i| {
                let leader_slot = inner.processed_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
                inner.get_tpu_info(leader_slot).and_then(|info| {
                    if self.blocklist.leaders.contains(&info.leader) {
                        blocklisted += 1;
                        None
                    } else {
                        Some(info)
                    }
                })
            })
            .collect::<Vec<_>>();

        for extra_tpu in extra_tpu_forwards.into_iter() {
            if self.blocklist.leaders.contains(&extra_tpu.leader) {
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
