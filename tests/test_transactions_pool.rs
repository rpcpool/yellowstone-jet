use {
    bytes::Bytes,
    jsonrpsee::http_client::HttpClientBuilder,
    solana_sdk::{
        hash::Hash,
        message::{v0, VersionedMessage},
        signature::Keypair,
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    std::{
        array,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    yellowstone_jet::{
        blockhash_queue::testkit::MockBlockhashQueue,
        cluster_tpu_info::TpuInfo,
        quic::{teskit::MockClusterTpuInfo, QuicClient},
        quic_solana::ConnectionCache,
        rpc::{rpc_admin::RpcClient, RpcServer, RpcServerType},
        testkit::{
            build_random_endpoint, default_config_quic, default_config_quic_client,
            default_config_transaction, generate_random_local_addr,
        },
        transactions::{
            testkit::MockRootedTransactions, SendTransactionRequest, SendTransactionsPool,
        },
        util::{flush_control, WaitShutdown},
    },
};

pub fn create_send_transaction_request(hash: Hash, max_resent: usize) -> SendTransactionRequest {
    let fake_wallet_keypair1 = Keypair::new();
    let fake_wallet_keypair2 = Keypair::new();
    let instructions = vec![system_instruction::transfer(
        &fake_wallet_keypair1.pubkey(),
        &fake_wallet_keypair2.pubkey(),
        10,
    )];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&fake_wallet_keypair1.pubkey(), &instructions, &[], hash)
                .expect("try compile"),
        ),
        &[&fake_wallet_keypair1],
    )
    .expect("try new");

    let wire_transaction = bincode::serialize(&tx).expect("Error getting wire_transaction");

    SendTransactionRequest {
        max_retries: Some(max_resent),
        signature: tx.signatures[0],
        wire_transaction,
        transaction: tx,
    }
}

#[tokio::test]
async fn test_transaction() {
    let endpoint_addr = generate_random_local_addr();
    let (endpoint, endpoint_keypair) = build_random_endpoint(endpoint_addr);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let h1 = tokio::spawn(async move {
        let connecting = endpoint.accept().await.expect("Accepting");
        let conn = connecting.await.expect("Connecting");
        let mut recv = conn.accept_uni().await.expect("Error in connection");
        let mut chunks: [Bytes; 4] = array::from_fn(|_| Bytes::new());
        let mut total_chunks_read = 0;
        while let Some(n_chunk) = recv.read_chunks(&mut chunks).await.expect("read") {
            total_chunks_read += n_chunk;
            if total_chunks_read > 4 {
                panic!("total_chunks_read > 4");
            }
        }
        let combined = chunks.iter().fold(vec![], |mut acc, chunk| {
            acc.extend_from_slice(chunk);
            acc
        });

        let transaction = bincode::deserialize::<VersionedTransaction>(&combined)
            .expect("Error deserializing from bincode");
        tx.send(transaction).await.expect("Channel was closed");
    });

    let tpu_endpoint = TpuInfo {
        leader: endpoint_keypair.pubkey(),
        quic: Some(endpoint_addr),
        quic_forwards: None,
        slots: [0, 1, 2, 3],
    };

    let expected_identity = Keypair::new();
    let config = default_config_quic();
    let (_flush_guard, flush_identity) = flush_control();
    let (quic_session, _quic_identity_man) = ConnectionCache::new(
        config,
        expected_identity.insecure_clone(),
        flush_identity.clone(),
    );

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();
    let cluster_tpu_info_mock = MockClusterTpuInfo::new(vec![tpu_endpoint]);

    let tx_hash = Hash::new_unique();
    let transaction_request = create_send_transaction_request(tx_hash, 1);

    block_height_service.increase_block_height(tx_hash).await;

    let quic_client = QuicClient::new(
        Arc::new(cluster_tpu_info_mock),
        default_config_quic_client(),
        Arc::new(quic_session),
    );

    let send_transactions_pool = SendTransactionsPool::new(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        quic_client,
        flush_identity.clone(),
    )
    .await
    .expect("Error during creation of transactions pool");

    let transaction_compare = transaction_request.transaction.clone();

    send_transactions_pool
        .send_transaction(transaction_request)
        .expect("Error sending transaction to pool");

    let rx_transaction = rx.recv().await;

    assert_eq!(
        rx_transaction,
        Some(transaction_compare),
        "Error receiving transaction"
    );

    let _ = h1.await;
    send_transactions_pool.shutdown();
}

#[tokio::test]
async fn reset_identity_after_flush() {
    let rpc_addr = generate_random_local_addr();
    let endpoint_addr = generate_random_local_addr();
    let (endpoint, endpoint_keypair) = build_random_endpoint(endpoint_addr);
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let count_transactions = Arc::new(AtomicUsize::new(0));
    let count_transactions2 = Arc::clone(&count_transactions);

    let h1 = tokio::spawn(async move {
        let connecting = endpoint.accept().await.expect("Accepting");

        let conn = connecting.await.expect("Connecting");
        loop {
            tokio::select! {
                res = conn.accept_uni() => {

                    match res {
                        Ok(_)=>{
                            count_transactions2.fetch_add(1,Ordering::Release);
                        }
                        Err(_) =>break }

                },
                _ = &mut rx => {
                    break
                }
            }
        }
    });

    let tpu_endpoint = TpuInfo {
        leader: endpoint_keypair.pubkey(),
        quic: Some(endpoint_addr),
        quic_forwards: None,
        slots: [0, 1, 2, 3],
    };

    let expected_identity = Keypair::new();
    let config = default_config_quic();
    let (_flush_guard, flush_identity) = flush_control();
    let (quic_session, quic_identity_man) = ConnectionCache::new(
        config,
        expected_identity.insecure_clone(),
        flush_identity.clone(),
    );

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();
    let cluster_tpu_info_mock = MockClusterTpuInfo::new(vec![tpu_endpoint]);

    let tx_hash = Hash::new_unique();
    let max_resent = 2;
    let transaction_request = create_send_transaction_request(tx_hash, max_resent);

    block_height_service.increase_block_height(tx_hash).await;

    let quic_client = QuicClient::new(
        Arc::new(cluster_tpu_info_mock),
        default_config_quic_client(),
        Arc::new(quic_session),
    );

    let send_transactions_pool = SendTransactionsPool::new(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        quic_client,
        flush_identity.clone(),
    )
    .await
    .expect("Error during creation of transactions pool");

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
        .build(format!("http://{}", rpc_addr))
        .expect("Error build rpc client");

    send_transactions_pool
        .send_transaction(transaction_request)
        .expect("Error sending transaction to pool");

    tokio::time::sleep(Duration::from_millis(2500)).await;

    let client2 = client.clone();
    let h_reset = tokio::spawn(async move {
        client2
            .reset_identity()
            .await
            .expect("Error resetting identity");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    flush_identity
        .wait_for_change(|val| !val)
        .await
        .expect("Expect to flush transactions");

    let _ = h_reset.await;
    let _ = tx.send(());
    // I add one because, system retries 3 times after sending one transaction
    assert!(
        (max_resent + 1) == count_transactions.load(Ordering::Relaxed),
        "{} == {}",
        (max_resent + 1),
        count_transactions.load(Ordering::Relaxed)
    );

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_ne!(identity, expected_identity.pubkey().to_string());
    let _ = h1.await;
    rpc_admin.shutdown();
    send_transactions_pool.shutdown();
}

#[tokio::test]
async fn set_identity_after_flush() {
    let rpc_addr = generate_random_local_addr();
    let endpoint_addr = generate_random_local_addr();
    let (endpoint, endpoint_keypair) = build_random_endpoint(endpoint_addr);
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let count_transactions = Arc::new(AtomicUsize::new(0));
    let count_transactions2 = Arc::clone(&count_transactions);

    let h1 = tokio::spawn(async move {
        let connecting = endpoint.accept().await.expect("Accepting");

        let conn = connecting.await.expect("Connecting");
        loop {
            tokio::select! {
                res = conn.accept_uni() => {
                    match res {
                        Ok(_)=>{
                            count_transactions2.fetch_add(1,Ordering::Release);
                        }
                        Err(_) => break}

                },
                _ = &mut rx => {
                    break
                }
            }
        }
    });

    let tpu_endpoint = TpuInfo {
        leader: endpoint_keypair.pubkey(),
        quic: Some(endpoint_addr),
        quic_forwards: None,
        slots: [0, 1, 2, 3],
    };

    let expected_identity = Keypair::new();
    let config = default_config_quic();
    let (_flush_guard, flush_identity) = flush_control();
    let (quic_session, quic_identity_man) =
        ConnectionCache::new(config, Keypair::new(), flush_identity.clone());

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();
    let cluster_tpu_info_mock = MockClusterTpuInfo::new(vec![tpu_endpoint]);

    let tx_hash = Hash::new_unique();
    let max_resent = 2;
    let transaction_request = create_send_transaction_request(tx_hash, max_resent);

    block_height_service.increase_block_height(tx_hash).await;

    let quic_client = QuicClient::new(
        Arc::new(cluster_tpu_info_mock),
        default_config_quic_client(),
        Arc::new(quic_session),
    );

    let send_transactions_pool = SendTransactionsPool::new(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        quic_client,
        flush_identity.clone(),
    )
    .await
    .expect("Error during creation of transactions pool");

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
        .build(format!("http://{}", rpc_addr))
        .expect("Error build rpc client");

    send_transactions_pool
        .send_transaction(transaction_request)
        .expect("Error sending transaction to pool");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    let expected_identity2 = expected_identity.insecure_clone();
    let client2 = client.clone();
    let h_reset = tokio::spawn(async move {
        client2
            .set_identity_from_bytes(Vec::from(expected_identity2.to_bytes()), false)
            .await
            .expect("Error resetting identity");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    flush_identity
        .wait_for_change(|val| !val)
        .await
        .expect("Expect to flush transactions");

    let _ = h_reset.await;
    let _ = tx.send(());
    assert!(
        (max_resent + 1) == count_transactions.load(Ordering::Relaxed),
        "{} == {}",
        (max_resent + 1),
        count_transactions.load(Ordering::Relaxed)
    );

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_eq!(identity, expected_identity.pubkey().to_string());
    let _ = h1.await;
    rpc_admin.shutdown();
    send_transactions_pool.shutdown();
}
