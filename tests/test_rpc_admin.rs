mod testkit;

use {
    jsonrpsee::http_client::HttpClientBuilder,
    solana_sdk::{
        signature::{Keypair, write_keypair_file},
        signer::Signer,
    },
    std::{
        path::PathBuf,
        sync::{Arc, RwLock},
        time::Duration,
    },
    testkit::generate_random_local_addr,
    tokio::sync::Mutex,
    yellowstone_jet::{
        identity::{JetIdentitySyncGroup, JetIdentitySyncMember},
        rpc::{RpcServer, RpcServerType, rpc_admin::RpcClient},
    },
};

fn clean_file(path: &PathBuf) {
    if path.exists() {
        std::fs::remove_file(path).expect("Failed to remove stale socket file");
    }
}

pub struct NullJetIdentitySyncMember {
    new_identity: Arc<RwLock<Keypair>>,
}

#[async_trait::async_trait]
impl JetIdentitySyncMember for NullJetIdentitySyncMember {
    async fn pause_for_identity_update(
        &self,
        new_identity: Keypair,
        barrier: Arc<tokio::sync::Barrier>,
    ) {
        let shared = Arc::clone(&self.new_identity);
        tokio::spawn(async move {
            {
                let mut guard = shared.write().unwrap();
                *guard = new_identity;
                drop(guard);
            }
            barrier.wait().await;
        });
    }
}

#[tokio::test]
pub async fn set_identity_if_expected() {
    let rpc_addr = generate_random_local_addr();
    let expected_identity = Keypair::new();
    let expected_identity_pubkey = expected_identity.pubkey();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.insecure_clone()));
    let jet_identity_updater = NullJetIdentitySyncMember {
        new_identity: Arc::clone(&shared),
    };
    let jet_identity_group = JetIdentitySyncGroup::new(
        initial_kp.insecure_clone(),
        vec![Box::new(jet_identity_updater)],
    );
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group))),
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr))
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
    assert_eq!(new_identity.pubkey(), expected_identity_pubkey);

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn set_identity_wrong_keypair() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.insecure_clone()));
    let jet_identity_updater = NullJetIdentitySyncMember {
        new_identity: Arc::clone(&shared),
    };
    let jet_identity_group = JetIdentitySyncGroup::new(
        initial_kp.insecure_clone(),
        vec![Box::new(jet_identity_updater)],
    );
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group))),
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr))
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
    let shared = Arc::new(RwLock::new(initial_kp.insecure_clone()));
    let jet_identity_updater = NullJetIdentitySyncMember {
        new_identity: Arc::clone(&shared),
    };
    let jet_identity_group = JetIdentitySyncGroup::new(
        initial_kp.insecure_clone(),
        vec![Box::new(jet_identity_updater)],
    );
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group))),
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr))
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
    assert_eq!(new_identity.pubkey(), expected_identity.pubkey());
    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn reset_identity_to_random() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let initial_kp = Keypair::new();
    let shared = Arc::new(RwLock::new(initial_kp.insecure_clone()));
    let jet_identity_updater = NullJetIdentitySyncMember {
        new_identity: Arc::clone(&shared),
    };
    let jet_identity_group = JetIdentitySyncGroup::new(
        initial_kp.insecure_clone(),
        vec![Box::new(jet_identity_updater)],
    );
    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group))),
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr))
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
    assert_ne!(new_identity.pubkey(), expected_identity.pubkey());

    // Ensure the new identity is different from the initial one, since reset_identity generates a new random keypair
    assert_ne!(new_identity.pubkey(), initial_kp.pubkey());
    rpc_admin.shutdown();
}
