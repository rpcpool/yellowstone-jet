use std::{collections::HashMap, sync::Arc, time::Duration};

use maplit::hashset;
use solana_client::rpc_response::RpcContactInfo;
use solana_sdk::{clock::NUM_CONSECUTIVE_LEADER_SLOTS, pubkey::Pubkey};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};
use yellowstone_jet::{
    cluster_tpu_info::{ClusterTpuInfo, ClusterTpuInfoInner, UpdateClusterScheduleInfo},
    config::{ConfigBlocklist, YellowstoneBlocklist},
    grpc_geyser::{GeyserStreams, GrpcUpdateMessage, SlotUpdateInfoWithCommitment},
};

fn default_rpc_contact_info(key: &Pubkey) -> RpcContactInfo {
    RpcContactInfo {
        pubkey: key.to_string(),
        gossip: None,
        tvu: None,
        tpu: None,
        tpu_quic: None,
        tpu_forwards: None,
        tpu_forwards_quic: None,
        tpu_vote: None,
        serve_repair: None,
        rpc: None,
        pubsub: None,
        version: None,
        feature_set: None,
        shred_version: None,
    }
}

#[tokio::test]
async fn test_blocklist_onchain_blocks_every_tpu_leaders() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .deny_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {leader1, leader2});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string()])
        .await;

    assert!(res.is_empty());
}

#[tokio::test]
async fn test_blocklist_onchain_blocks_only_one_tpu_leader() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .deny_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {leader1});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string()])
        .await;

    assert!(res.len() == 1);
    assert!(res[0].leader == leader2);
}

#[tokio::test]
async fn test_blocklist_onchain_tpu_in_every_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .allow_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {leader1, leader2});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string()])
        .await;

    assert!(res.len() == 2);
    assert!(res[0].leader == leader1);
    assert!(res[1].leader == leader2);
}

#[tokio::test]
async fn test_blocklist_onchain_tpu_in_one_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .allow_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {leader1});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string()])
        .await;

    assert!(res.len() == 1);
    assert!(res[0].leader == leader1);
}

#[tokio::test]
async fn test_blocklist_onchain_no_tpu_in_any_allowlist() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .allow_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {});

    yellowstone_blocklist
        .allow_lists
        .write()
        .await
        .insert(leader2.to_string(), hashset! {});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string(), leader2.to_string()])
        .await;

    assert!(res.is_empty());
}

#[tokio::test]
async fn test_blocklist_onchain_allowlist_denylist_block_one_tpu() {
    let leader1 = Pubkey::new_unique();
    let leader2 = Pubkey::new_unique();
    let mut leader_schedule = HashMap::new();
    leader_schedule.insert(0, leader1);
    leader_schedule.insert(NUM_CONSECUTIVE_LEADER_SLOTS, leader2);
    let mut cluster_nodes = HashMap::new();
    cluster_nodes.insert(leader1, default_rpc_contact_info(&leader1));
    cluster_nodes.insert(leader2, default_rpc_contact_info(&leader2));

    let config_blocklist = ConfigBlocklist::default();

    let yellowstone_blocklist = Arc::new(YellowstoneBlocklist::default());
    yellowstone_blocklist
        .deny_lists
        .write()
        .await
        .insert(leader1.to_string(), hashset! {leader1});

    yellowstone_blocklist
        .allow_lists
        .write()
        .await
        .insert(leader2.to_string(), hashset! {leader1, leader2});

    let cluster_inner =
        ClusterTpuInfoInner::test_inner_cluster(leader_schedule, cluster_nodes).await;
    let mock_cluster_inner = Arc::new(MockClusterInner::new(cluster_inner));

    let (cluster_tpu, _) = ClusterTpuInfo::new(
        "".to_string(),
        &MockGrpc::new(),
        mock_cluster_inner,
        Duration::from_secs(1),
        config_blocklist,
        yellowstone_blocklist,
    )
    .await;

    let res = cluster_tpu
        .get_leader_tpus(2, [], &[leader1.to_string(), leader2.to_string()])
        .await;

    assert!(res.len() == 1);
    assert!(res[0].leader == leader2);
}

struct MockClusterInner {
    inner: RwLock<ClusterTpuInfoInner>,
}

impl MockClusterInner {
    fn new(inner: ClusterTpuInfoInner) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }
}

#[async_trait::async_trait]
impl UpdateClusterScheduleInfo for MockClusterInner {
    async fn update_cluster_nodes(
        &self,
        shutdown: oneshot::Receiver<()>,
        _rpc: String,
        _cluster_nodes_update_interval: Duration,
    ) -> anyhow::Result<()> {
        shutdown.await?;
        Ok(())
    }

    async fn update_leader_schedule(
        &self,
        shutdown: oneshot::Receiver<()>,
        _rpc: String,
        _slots_rx: broadcast::Receiver<SlotUpdateInfoWithCommitment>,
    ) -> anyhow::Result<()> {
        shutdown.await?;
        Ok(())
    }

    async fn get_inner_data(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterTpuInfoInner> {
        self.inner.read().await
    }
}

struct MockGrpc {
    slots_tx: broadcast::Sender<SlotUpdateInfoWithCommitment>,
    transactions_rx: Arc<Mutex<Option<mpsc::Receiver<GrpcUpdateMessage>>>>,
    _transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
}

impl MockGrpc {
    fn new() -> Self {
        let (slots_tx, _) = broadcast::channel(1);
        let (transactions_tx, transactions_rx) = mpsc::channel(10);

        Self {
            slots_tx,
            transactions_rx: Arc::new(Mutex::new(Some(transactions_rx))),
            _transactions_tx: transactions_tx,
        }
    }
}

#[async_trait::async_trait]
impl GeyserStreams for MockGrpc {
    fn subscribe_slots(&self) -> broadcast::Receiver<SlotUpdateInfoWithCommitment> {
        self.slots_tx.subscribe()
    }

    async fn subscribe_transactions(&self) -> Option<mpsc::Receiver<GrpcUpdateMessage>> {
        self.transactions_rx.lock().await.take()
    }
}
