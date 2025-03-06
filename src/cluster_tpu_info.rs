use {
    crate::{
        grpc_geyser::SlotUpdateInfoWithCommitment,
        metrics::jet as metrics,
        util::{CommitmentLevel, IncrementalBackoff},
    },
    futures::future::FutureExt,
    solana_client::{
        client_error::Result as ClientResult,
        nonblocking::rpc_client::RpcClient,
        rpc_response::{RpcContactInfo, RpcLeaderSchedule},
    },
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        epoch_schedule::EpochSchedule,
        pubkey::Pubkey,
    },
    solana_yellowstone_blocklist::state::{AclType, ListState},
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        net::SocketAddr,
        sync::Arc,
    },
    tokio::{
        sync::{broadcast, RwLock},
        time::{sleep, Duration, Instant},
    },
    tracing::{debug, info, warn},
};

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
    processed_slot: Slot,
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
        slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
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
                    _ = ClusterTpuInfo::update_leader_schedule(Arc::clone(&inner),Arc::clone(&rpc), slots_rx) => {
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

    pub async fn processed_slot(&self) -> Slot {
        self.inner.read().await.processed_slot
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

    async fn update_leader_schedule(
        inner: Arc<RwLock<ClusterTpuInfoInner>>,
        rpc: Arc<dyn ClusterTpuRpcClient + Send + Sync + 'static>,
        mut slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();

        let epoch_schedule = { inner.read().await.epoch_schedule.clone() };

        loop {
            let mut slot_processed = None;
            let mut slot_schedule = None;

            tokio::select! {
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
                tokio::select! {
                    _ = backoff.maybe_tick() => {}
                }

                let ts = Instant::now();
                match rpc.get_leader_schedule(Some(slot)).await {
                    Ok(Some(leader_schedule)) => {
                        let mut locked = inner.write().await;

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

    // I don't really know if this is necessary. I could just add an underscore before shutdown_update and it would work
    // In any case, those futures will be shutdown if all tx references are dropped or task_group cancels them
    pub async fn shutdown(&self) {
        let _ = self.shutdown_update.send(());
    }

    pub async fn get_leader_tpus(&self, leader_forward_count: usize) -> Vec<TpuInfo> {
        let inner = self.inner.read().await;

        (0..=leader_forward_count as u64)
            .filter_map(|i| {
                let leader_slot = inner.processed_slot + i * NUM_CONSECUTIVE_LEADER_SLOTS;
                inner.get_tpu_info(leader_slot)
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Default, Clone)]
pub struct LeadersSelector {
    blocklist: HashSet<Pubkey>,
    deny_lists: Arc<RwLock<HashMap<Pubkey, HashSet<Pubkey>>>>,
    allow_lists: Arc<RwLock<HashMap<Pubkey, HashSet<Pubkey>>>>,
}

impl LeadersSelector {
    pub async fn new_from_blockchain(
        url: String,
        blocklist: HashSet<Pubkey>,
        contract_pubkey: &Option<Pubkey>,
    ) -> Result<Self, anyhow::Error> {
        if let Some(contract_pubkey) = contract_pubkey {
            let rpc = RpcClient::new(url);
            let accounts = rpc.get_program_accounts(contract_pubkey).await?;
            let accounts_parsed: Vec<(Pubkey, HashSet<Pubkey>, AclType)>= accounts
                .iter()
                .filter_map(|account| {
                    match ListState::deserialize(account.1.data.as_slice()) {
                        Ok(state) => Some((
                            account.0,
                            Self::get_hashset(state.list),
                            state.meta.acl_type,
                        )),
                        Err(_) => {
                            tracing::error!("Error parsing account data from {:?}. \n Maybe you should check program pubkey", account.0);
                            None
                        }
                    }
                })
                .collect();

            let mut deny_list = HashMap::new();
            let mut allow_list = HashMap::new();

            accounts_parsed
                .into_iter()
                .for_each(|(key, hash_keys, acl)| match acl {
                    AclType::Allow => {
                        allow_list.insert(key, hash_keys);
                    }
                    AclType::Deny => {
                        deny_list.insert(key, hash_keys);
                    }
                });
            Ok(Self {
                blocklist,
                deny_lists: Arc::new(RwLock::new(deny_list)),
                allow_lists: Arc::new(RwLock::new(allow_list)),
            })
        } else {
            Ok(Self {
                blocklist,
                ..Default::default()
            })
        }
    }

    pub fn new(
        blocklist: HashSet<Pubkey>,
        deny_list: HashMap<Pubkey, HashSet<Pubkey>>,
        allow_list: HashMap<Pubkey, HashSet<Pubkey>>,
    ) -> Self {
        Self {
            blocklist,
            deny_lists: Arc::new(RwLock::new(deny_list)),
            allow_lists: Arc::new(RwLock::new(allow_list)),
        }
    }

    pub fn get_hashset(list: &[Pubkey]) -> HashSet<Pubkey> {
        list.iter().cloned().collect()
    }
}

#[async_trait::async_trait]
pub trait BlocklistUpdater {
    /// Used to update lists from contract Yellowstone-blocklist
    /// It parses the bytes in acc_data into an specified struct
    /// and then insert its data into a collection using key as index
    async fn update_list(&self, acc_data: &[u8], key: Pubkey);
}

#[async_trait::async_trait]
impl BlocklistUpdater for LeadersSelector {
    async fn update_list(&self, acc_data: &[u8], key: Pubkey) {
        match ListState::deserialize(acc_data) {
            Ok(state) => {
                let mut deny_lists = self.deny_lists.write().await;
                let mut allow_lists = self.allow_lists.write().await;

                match state.meta.acl_type {
                    AclType::Allow => {
                        deny_lists.remove(&key);
                        allow_lists.insert(key, Self::get_hashset(state.list));
                    }
                    AclType::Deny => {
                        allow_lists.remove(&key);
                        deny_lists.insert(key, Self::get_hashset(state.list));
                    }
                }
            }
            Err(err) => {
                tracing::error!("Unable to parse account {key:?} data. Maybe you should check program pubkey. Error: {err:?}");
            }
        }
    }
}

#[async_trait::async_trait]
pub trait BlockLeaders {
    /*
    Takes a mutable reference of a vector containing TpuInfo and retain only those allowed by the
    lists indicated by blocklist_keys
    # Example
    ```

    struct BlocklistYellowstone {
        blocklist: HashMap<Pubkey, HashSet<Pubkey>>
    }


    #[async_trait::async_trait]
    impl BlockLeaders for Blocklists {
        async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]) {
            let blocklist = self.blocklist;

            tpus.retain(|info| {
                for key_list in blocklist_keys.iter() {
                   if let Some(blocklist_hash) = blocklist.get(key_list) {
                       if blocklist_hash.contains(&info.leader) {
                            return false;
                       }
                   }
                   true
                 }
               }
            )
        }
    }


    async fn main() {
        let key1 = Pubkey::new_unique();
        let mut tpus = vec![TpuInfo {
            leader: key1,
            ...
        }]
        let blocklist_yellowstone = {
            blocklist: hashmap!{key1 => hashset!{key1}}
        }
        blocklist_yellowstone.block_leaders(&mut tpus, &[key1]).await;
       // Now tpus should have a length of zero
        assert!(tpus.is_empty());
    }
    ```
    */
    async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]);
}

#[async_trait::async_trait]

impl BlockLeaders for LeadersSelector {
    async fn block_leaders(&self, tpus: &mut Vec<TpuInfo>, blocklist_keys: &[Pubkey]) {
        let mut blocklisted = 0;
        let allow_lists = self.allow_lists.read().await;
        let deny_lists = self.deny_lists.read().await;

        tpus.retain(|info| {
            let mut is_allow = false;
            let leader = info.leader;

            // Check deny lists first
            for key_list in blocklist_keys {
                if let Some(deny_hash) = deny_lists.get(key_list) {
                    if deny_hash.contains(&leader) {
                        blocklisted += 1;
                        debug!(
                            "Leader {} BLOCKED - found in deny list {}",
                            leader, key_list
                        );
                        return false;
                    }
                }
            }

            // Then check allow lists
            for key_list in blocklist_keys {
                if let Some(allow_hash) = allow_lists.get(key_list) {
                    if !allow_hash.contains(&leader) {
                        blocklisted += 1;
                        debug!(
                            "Leader {} BLOCKED - not found in allow list {}",
                            leader, key_list
                        );
                        return false;
                    } else {
                        is_allow = true;
                    }
                }
            }

            if !is_allow && self.blocklist.contains(&leader) {
                blocklisted += 1;
                debug!("Leader {} BLOCKED - found in global blocklist", leader);
                return false;
            }
            true
        });

        metrics::sts_tpu_blocklisted_inc(blocklisted);
    }
}
