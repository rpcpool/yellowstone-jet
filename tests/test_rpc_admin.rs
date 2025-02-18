use {
    jsonrpsee::http_client::HttpClientBuilder,
    solana_sdk::{
        signature::{write_keypair_file, Keypair},
        signer::Signer,
    },
    std::path::PathBuf,
    yellowstone_jet::{
        quic_solana::ConnectionCache,
        rpc::{rpc_admin::RpcClient, RpcServer, RpcServerType},
        utils_test::{default_config_quic, generate_random_local_addr},
    },
};

fn clean_file(path: &PathBuf) {
    if path.exists() {
        std::fs::remove_file(path).expect("Failed to remove stale socket file");
    }
}

#[tokio::test]
pub async fn set_identity_if_expected() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let expected_identity_pubkey = expected_identity.pubkey();
    let connection_cache_kp = Keypair::new();
    let config = default_config_quic();
    let (_quic_session, quic_identity_man) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());
    let mut flushed = quic_identity_man.flush_transactions_receiver();

    let mut value_observer = quic_identity_man.observe_identity_change();

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .set_identity_from_bytes(Vec::from(expected_identity.to_bytes()), false)
            .await
            .expect("Error setting identity");
    });
    flushed.changed().await.expect("Error watching channel");
    flushed.borrow().notify_waiters();
    let _ = h.await;
    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity_pubkey.to_string());
    assert_eq!(identity, value_observer.observe().await.to_string());

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn set_identity_wrong_keypair() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let connection_cache_kp = Keypair::new();
    let config = default_config_quic();
    let (_quic_session, quic_identity_man) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    let _ = client
        .set_identity_from_bytes(Vec::from(connection_cache_kp.to_bytes()), false)
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
    let connection_cache_kp = Keypair::new();
    let config = default_config_quic();
    let (_quic_session, quic_identity_man) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());
    let mut flushed = quic_identity_man.flush_transactions_receiver();
    let mut value_observer = quic_identity_man.observe_identity_change();

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .set_identity(keypair_json.display().to_string(), false)
            .await
            .expect("Error setting identity");
    });
    flushed.changed().await.expect("Error watching channel");
    flushed.borrow().notify_waiters();
    let _ = h.await;

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity.pubkey().to_string());
    assert_eq!(identity, value_observer.observe().await.to_string());

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn get_identity() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let config = default_config_quic();
    let (_quic_session, quic_identity_man) =
        ConnectionCache::new(config, expected_identity.insecure_clone());

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity.pubkey().to_string());

    rpc_admin.shutdown();
}

#[tokio::test]
pub async fn reset_identity_to_random() {
    let rpc_addr = generate_random_local_addr();

    let expected_identity = Keypair::new();
    let config = default_config_quic();
    let (_quic_session, quic_identity_man) =
        ConnectionCache::new(config, expected_identity.insecure_clone());
    let mut flushed = quic_identity_man.flush_transactions_receiver();

    let rpc_admin = RpcServer::new(
        rpc_addr,
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: Some(expected_identity.pubkey()),
        },
    )
    .await
    .expect("Error creating rpc server");

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    let client2 = client.clone();

    let h = tokio::spawn(async move {
        client2
            .reset_identity()
            .await
            .expect("Error setting identity");
    });
    flushed.changed().await.expect("Error watching channel");
    flushed.borrow().notify_waiters();
    let _ = h.await;

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_ne!(identity, expected_identity.pubkey().to_string());

    rpc_admin.shutdown();
}
