mod testkit;

use {
    jsonrpsee::http_client::HttpClientBuilder,
    solana_keypair::{Keypair, write_keypair_file},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        path::PathBuf,
        sync::{Arc, RwLock},
        time::Duration,
    },
    testkit::generate_random_local_addr,
    tokio::sync::Mutex,
    yellowstone_jet::rpc::{
        RpcServer, RpcServerType,
        rpc_admin::{JetIdentityUpdater, RpcClient},
    },
    yellowstone_jet_tpu_client::identity::HardenedKeypair,
};
#[cfg(test)]
use {solana_clock::Slot, yellowstone_jet::cluster_tpu_info::ClusterTpuInfoProvider};

#[cfg(test)]
#[derive(Default)]
pub struct MockClusterTpuInfo {
    latest_slot: Slot,
}

#[cfg(test)]
#[async_trait::async_trait]
impl ClusterTpuInfoProvider for MockClusterTpuInfo {
    fn latest_seen_slot(&self) -> Slot {
        self.latest_slot
    }
}

fn clean_file(path: &PathBuf) {
    if path.exists() {
        std::fs::remove_file(path).expect("Failed to remove stale socket file");
    }
}

pub struct NullJetIdentityUpdater {
    current: Arc<RwLock<Pubkey>>,
}

#[async_trait::async_trait]
impl JetIdentityUpdater for NullJetIdentityUpdater {
    async fn update_identity(&mut self, new_identity: HardenedKeypair) {
        *self.current.write().unwrap() = new_identity.pubkey();
    }

    fn get_identity(&self) -> Pubkey {
        *self.current.read().unwrap()
    }
}

#[tokio::test]
pub async fn set_identity_if_expected() {
    let rpc_addr = generate_random_local_addr();
    let expected_identity = Keypair::new();
    let expected_identity_pubkey = expected_identity.pubkey();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));
    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };
    let mock_cluster_info = Arc::new(MockClusterTpuInfo::default());
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: Some(expected_identity.pubkey()),
            cluster_tpu_info: mock_cluster_info,
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .set_identity_from_bytes(Vec::from(expected_identity.to_bytes()), false)
            .await
            .expect("Error setting identity");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    let _ = h.await;
    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity_pubkey.to_string());
    let new_identity = shared.read().unwrap();
    assert_eq!(*new_identity, expected_identity_pubkey);

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn set_identity_wrong_keypair() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));
    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };
    let mock_cluster_info = Arc::new(MockClusterTpuInfo::default());
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: Some(expected_identity.pubkey()),
            cluster_tpu_info: mock_cluster_info,
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    let invalid_kp = Keypair::new();
    let _ = client
        .set_identity_from_bytes(Vec::from(invalid_kp.to_bytes()), false)
        .await
        .expect_err("Should return err");

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn set_identity_from_file() {
    let base_path = std::env::temp_dir();
    let keypair_json = base_path.join("keypair.json");
    let expected_identity = Keypair::new();
    clean_file(&keypair_json);

    write_keypair_file(&expected_identity, keypair_json.clone()).expect("Error while writing file");

    let rpc_addr = generate_random_local_addr();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));
    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };
    let mock_cluster_info = Arc::new(MockClusterTpuInfo::default());
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: Some(expected_identity.pubkey()),
            cluster_tpu_info: mock_cluster_info,
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .set_identity(keypair_json.display().to_string(), false)
            .await
            .expect("Error setting identity");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    let _ = h.await;

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity.pubkey().to_string());
    let new_identity = shared.read().unwrap();
    assert_eq!(*new_identity, expected_identity.pubkey());
    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn reset_identity_to_random() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));
    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };
    let mock_cluster_info = Arc::new(MockClusterTpuInfo::default());
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: Some(expected_identity.pubkey()),
            cluster_tpu_info: mock_cluster_info,
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .reset_identity()
            .await
            .expect("Error setting identity");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    let _ = h.await;

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_ne!(identity, expected_identity.pubkey().to_string());
    let new_identity = shared.read().unwrap();
    assert_ne!(*new_identity, expected_identity.pubkey());

    // Ensure the new identity is different from the initial one, since reset_identity generates a new random keypair
    assert_ne!(*new_identity, initial_kp.pubkey());
    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn test_get_latest_slot() {
    let rpc_addr = generate_random_local_addr();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));

    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };

    let expected_slot = 12345u64;

    let mock_cluster_info = MockClusterTpuInfo {
        latest_slot: expected_slot,
    };

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: None,
            cluster_tpu_info: Arc::new(mock_cluster_info),
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    let latest_slot = client
        .get_latest_slot()
        .await
        .expect("Error getting latest slot");
    assert_eq!(latest_slot, expected_slot);

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn test_get_latest_slot_updates() {
    let rpc_addr = generate_random_local_addr();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.pubkey()));

    let jet_identity_updater = NullJetIdentityUpdater {
        current: Arc::clone(&shared),
    };

    let initial_slot = 1000u64;
    let mock_cluster_info = Arc::new(RwLock::new(MockClusterTpuInfo {
        latest_slot: initial_slot,
    }));

    struct UpdatableMockClusterTpuInfo {
        inner: Arc<RwLock<MockClusterTpuInfo>>,
    }

    #[async_trait::async_trait]
    impl ClusterTpuInfoProvider for UpdatableMockClusterTpuInfo {
        fn latest_seen_slot(&self) -> Slot {
            self.inner.read().unwrap().latest_slot
        }
    }

    let updatable_mock = Arc::new(UpdatableMockClusterTpuInfo {
        inner: Arc::clone(&mock_cluster_info),
    });

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_updater))),
            allowed_identity: None,
            cluster_tpu_info: updatable_mock,
        },
    )
    .await;

    let client = HttpClientBuilder::default()
        .build(format!("http://{rpc_addr}"))
        .expect("Error build rpc client");

    // Verify initial slot
    let latest_slot = client
        .get_latest_slot()
        .await
        .expect("Error getting latest slot");
    assert_eq!(latest_slot, initial_slot);

    // Update the slot value
    let updated_slot = 2000u64;
    mock_cluster_info.write().unwrap().latest_slot = updated_slot;

    // Verify updated slot
    let latest_slot = client
        .get_latest_slot()
        .await
        .expect("Error getting latest slot");
    assert_eq!(latest_slot, updated_slot);

    rpc_admin.shutdown();
}
