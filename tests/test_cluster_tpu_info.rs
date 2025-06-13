use {
    maplit::hashmap,
    solana_client::{
        client_error::Result as ClientResult,
        rpc_response::{RpcContactInfo, RpcLeaderSchedule},
    },
    solana_sdk::{clock::Slot, epoch_schedule::EpochSchedule, hash::Hash, pubkey::Pubkey},
    std::{collections::HashMap, sync::Arc, time::Duration},
    tokio::sync::{Mutex, RwLock, broadcast, mpsc},
    yellowstone_jet::{
        cluster_tpu_info::{ClusterTpuInfo, ClusterTpuRpcClient},
        grpc_geyser::{GeyserStreams, GrpcUpdateMessage, SlotUpdateInfoWithCommitment},
        util::CommitmentLevel,
    },
};

const fn create_contact_info(pubkey: String) -> RpcContactInfo {
    RpcContactInfo {
        pubkey,
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
async fn test_processed_slot_update() {
    let mock_grpc = MockGrpc::new();
    let rpc = MockRpc::default();
    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::new(rpc),
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);
    let slot_proccessed = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot: 1,
        block_height: 1,
        commitment: CommitmentLevel::Processed,
    };

    let slot_proccessed2 = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot: 2,
        block_height: 1,
        commitment: CommitmentLevel::Processed,
    };

    mock_grpc
        .slots_tx
        .send(slot_proccessed)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_slot_processed = cluster_tpu.processed_slot();

    assert_eq!(slot_proccessed.slot, cluster_slot_processed);

    mock_grpc
        .slots_tx
        .send(slot_proccessed2)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_slot_processed = cluster_tpu.processed_slot();

    assert_eq!(slot_proccessed2.slot, cluster_slot_processed);

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[tokio::test]
async fn test_update_cluster_nodes() {
    let key1 = Pubkey::new_unique();
    let key2 = Pubkey::new_unique();
    let mock_grpc = MockGrpc::new();
    let node1 = create_contact_info(key1.to_string());
    let node2 = create_contact_info(key2.to_string());

    let mut cluster_compare = hashmap! {key1 => node1.clone()};

    let rpc = Arc::new(MockRpc::default());
    rpc.insert_cluster_nodes(node1).await;

    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::clone(&rpc) as Arc<dyn ClusterTpuRpcClient + Send + Sync>,
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_nodes = cluster_tpu.get_cluster_nodes();

    assert_eq!(cluster_nodes, cluster_compare);

    cluster_compare.insert(key2, node2.clone());
    rpc.insert_cluster_nodes(node2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_nodes = cluster_tpu.get_cluster_nodes();
    assert_eq!(cluster_nodes, cluster_compare);

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[tokio::test]
async fn test_leader_schedule_doesnot_update_before_slot_confirmed() {
    let key1 = Pubkey::new_unique();
    let mock_grpc = MockGrpc::new();

    let mut schedule = HashMap::new();

    // Numbers in vec are the slots
    // Which means slots 1 and 2 belongs to key1
    schedule.insert(key1.to_string(), vec![1, 2]);

    let rpc = Arc::new(MockRpc::default());
    rpc.set_schedule(schedule).await;

    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::clone(&rpc) as Arc<dyn ClusterTpuRpcClient + Send + Sync>,
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_schedule = cluster_tpu.get_leader_schedule();
    // Because we haven't sent a confirmed slot, then cluster_schedule has to be empty
    assert!(cluster_schedule == HashMap::new());

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[tokio::test]
async fn test_leader_schedule_update_after_slot_confirmed() {
    let key1 = Pubkey::new_unique();
    let mock_grpc = MockGrpc::new();

    let mut schedule = HashMap::new();

    // Numbers in vec are the slots
    // Which means slots 1 and 2 belongs to key1
    schedule.insert(key1.to_string(), vec![1, 2]);
    let schedule_compare = hashmap! {1 => key1, 2 => key1};

    let rpc = Arc::new(MockRpc::default());
    rpc.set_schedule(schedule).await;

    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::clone(&rpc) as Arc<dyn ClusterTpuRpcClient + Send + Sync>,
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let slot_confirmed = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot: 0,
        block_height: 1,
        commitment: CommitmentLevel::Confirmed,
    };

    mock_grpc
        .slots_tx
        .send(slot_confirmed)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_schedule = cluster_tpu.get_leader_schedule();
    // Cluster schedule has to be updated by now
    assert!(cluster_schedule == schedule_compare);

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[tokio::test]
async fn deletes_old_slots() {
    let key1 = Pubkey::new_unique();
    let key2 = Pubkey::new_unique();
    let mock_grpc = MockGrpc::new();

    let mut schedule = HashMap::new();

    // Numbers in vec are the slots
    // Which means slots 1 and 2 belongs to key1
    schedule.insert(key1.to_string(), vec![1, 2]);
    let schedule_compare1 = hashmap! {1 => key1, 2 => key1};

    let rpc = Arc::new(MockRpc::default());
    rpc.set_schedule(schedule).await;

    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::clone(&rpc) as Arc<dyn ClusterTpuRpcClient + Send + Sync>,
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let slot_confirmed = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot: 0,
        block_height: 1,
        commitment: CommitmentLevel::Confirmed,
    };

    mock_grpc
        .slots_tx
        .send(slot_confirmed)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_schedule = cluster_tpu.get_leader_schedule();
    // Cluster schedule has to be updated by now
    assert!(cluster_schedule == schedule_compare1);

    let mut schedule = HashMap::new();

    // All slots plus 42 less than the processed slot will be deleted
    let slot = 45;
    schedule.insert(key2.to_string(), vec![1_usize, 2_usize]);

    let epoch_schedule = rpc
        .get_epoch_schedule()
        .await
        .expect("Expected default epoch schedule");

    let slot_offset = epoch_schedule.get_first_slot_in_epoch(epoch_schedule.get_epoch(slot));

    let schedule_compare2 = hashmap! {slot_offset + 1 => key2, slot_offset + 2 => key2};
    rpc.set_schedule(schedule).await;

    let slot_confirmed = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot,
        block_height: 1,
        commitment: CommitmentLevel::Confirmed,
    };

    mock_grpc
        .slots_tx
        .send(slot_confirmed)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let cluster_schedule = cluster_tpu.get_leader_schedule();
    println!("{cluster_schedule:?}");
    println!("{schedule_compare2:?}");
    // Cluster schedule has to be updated by now
    assert!(cluster_schedule == schedule_compare2);

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[tokio::test]
async fn test_get_tpus_in_cluster() {
    let key1 = Pubkey::new_unique();
    let key2 = Pubkey::new_unique();
    let mock_grpc = MockGrpc::new();
    let node1 = create_contact_info(key1.to_string());
    let node2 = create_contact_info(key2.to_string());

    let rpc = Arc::new(MockRpc::default());
    rpc.insert_cluster_nodes(node1).await;
    rpc.insert_cluster_nodes(node2).await;

    let mut schedule = HashMap::new();

    schedule.insert(key1.to_string(), vec![0, 1, 2, 3]);
    schedule.insert(key2.to_string(), vec![4, 5, 6, 7]);
    rpc.set_schedule(schedule).await;

    let (cluster_tpu, cluster_futs) = ClusterTpuInfo::new(
        Arc::clone(&rpc) as Arc<dyn ClusterTpuRpcClient + Send + Sync>,
        mock_grpc.subscribe_slots(),
        Duration::from_secs(1),
    )
    .await;
    let h = tokio::spawn(cluster_futs);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let slot_confirmed = SlotUpdateInfoWithCommitment {
        block_hash: Hash::new_unique(),
        slot: 1,
        block_height: 1,
        commitment: CommitmentLevel::Confirmed,
    };

    mock_grpc
        .slots_tx
        .send(slot_confirmed)
        .expect("Error sending update");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let tpus = cluster_tpu.get_leader_tpus(2);

    assert_eq!(tpus.len(), 2);
    assert_eq!(tpus[0].leader, key1);
    assert_eq!(tpus[1].leader, key2);

    cluster_tpu.shutdown().await;
    let _ = h.await;
}

#[derive(Default)]
struct MockRpc {
    cluster_nodes: Arc<RwLock<Vec<RpcContactInfo>>>,
    schedule: Arc<RwLock<HashMap<String, Vec<usize>>>>,
}

impl MockRpc {
    async fn insert_cluster_nodes(&self, node: RpcContactInfo) {
        self.cluster_nodes.write().await.push(node);
    }

    async fn set_schedule(&self, schedule: HashMap<String, Vec<usize>>) {
        *self.schedule.write().await = schedule;
    }
}

#[async_trait::async_trait]
impl ClusterTpuRpcClient for MockRpc {
    async fn get_leader_schedule(
        &self,
        _slot: Option<Slot>,
    ) -> ClientResult<Option<RpcLeaderSchedule>> {
        Ok(Some(self.schedule.read().await.clone()))
    }

    async fn get_cluster_nodes(&self) -> ClientResult<Vec<RpcContactInfo>> {
        Ok(self.cluster_nodes.read().await.clone())
    }
    async fn get_epoch_schedule(&self) -> ClientResult<EpochSchedule> {
        Ok(EpochSchedule::default())
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
        let (_transactions_tx, transactions_rx) = mpsc::channel(10);

        Self {
            slots_tx,
            transactions_rx: Arc::new(Mutex::new(Some(transactions_rx))),
            _transactions_tx,
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
