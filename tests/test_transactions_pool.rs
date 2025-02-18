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
            atomic::{AtomicU16, Ordering},
            Arc,
        },
        time::Duration,
    },
    yellowstone_jet::{
        blockhash_queue::MockBlockhashQueue,
        cluster_tpu_info::TpuInfo,
        quic::{MockClusterTpuInfo, QuicClient},
        quic_solana::ConnectionCache,
        rpc::{rpc_admin::RpcClient, RpcServer, RpcServerType},
        transactions::{MockRootedTransactions, SendTransactionRequest, SendTransactionsPool},
        util::WaitShutdown,
        utils_test::{
            build_random_endpoint, default_config_quic, default_config_quic_client,
            default_config_transaction, generate_random_local_addr,
        },
    },
};

pub fn create_send_transaction_request(hash: Hash) -> SendTransactionRequest {
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
        max_retries: Some(10),
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
        println!("Connection stablished");

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
    let (quic_session, quic_identity_man) =
        ConnectionCache::new(config, expected_identity.insecure_clone());

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = Arc::new(MockBlockhashQueue::new());
    let cluster_tpu_info_mock = MockClusterTpuInfo::new(vec![tpu_endpoint]);

    let tx_hash = Hash::new_unique();
    let transaction_request = create_send_transaction_request(tx_hash);

    block_height_service
        .increase_block_height(tx_hash.clone())
        .await;

    let quic_client = QuicClient::new(
        Arc::new(cluster_tpu_info_mock),
        default_config_quic_client(),
        Arc::new(quic_session),
    );

    let send_transactions_pool = SendTransactionsPool::new(
        default_config_transaction(),
        block_height_service.clone(),
        Arc::new(rooted_transactions),
        quic_client,
        quic_identity_man.flush_transactions_receiver(),
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

    let received_transactions = Arc::new(AtomicU16::new(0));
    let received_transactions2 = Arc::clone(&received_transactions);

    let h1 = tokio::spawn(async move {
        let connecting = endpoint.accept().await.expect("Accepting");
        let conn = connecting.await.expect("Connecting");
        println!("Connection stablished");
        loop {
            tokio::select! {
                res = conn.accept_uni() => {

                    res.expect("Error in connecction");
                    received_transactions2.fetch_add(1, Ordering::Release);
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
    let (quic_session, quic_identity_man) =
        ConnectionCache::new(config, expected_identity.insecure_clone());

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = Arc::new(MockBlockhashQueue::new());
    let cluster_tpu_info_mock = MockClusterTpuInfo::new(vec![tpu_endpoint]);

    let tx_hash = Hash::new_unique();
    let transaction_request = create_send_transaction_request(tx_hash);

    block_height_service
        .increase_block_height(tx_hash.clone())
        .await;

    let quic_client = QuicClient::new(
        Arc::new(cluster_tpu_info_mock),
        default_config_quic_client(),
        Arc::new(quic_session),
    );

    let send_transactions_pool = SendTransactionsPool::new(
        default_config_transaction(),
        block_height_service.clone(),
        Arc::new(rooted_transactions),
        quic_client,
        quic_identity_man.flush_transactions_receiver(),
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
        .build(format!("http://{}", rpc_addr.to_string()))
        .expect("Error build rpc client");

    send_transactions_pool
        .send_transaction(transaction_request)
        .expect("Error sending transaction to pool");

    tokio::time::sleep(Duration::from_millis(2500)).await;

    let client2 = client.clone();
    let h_client = tokio::spawn(async move {
        client2
            .reset_identity()
            .await
            .expect("Error resetting identity");
    });

    // let identity_during_flush = client.get_identity().await.expect("Error getting identity");
    // assert_eq!(
    //     identity_during_flush,
    //     expected_identity.pubkey().to_string()
    // );
    println!(
        "Received transactions {}",
        received_transactions.load(Ordering::Relaxed)
    );

    // let notify = watch_flush.borrow().clone();
    // notify.notified().await;
    let _ = h_client.await;

    tx.send(()).expect("Error in oneshot channel");

    let identity = client.get_identity().await.expect("Error getting identity");
    assert_ne!(identity, expected_identity.pubkey().to_string());
    let _ = h1.await;
    rpc_admin.shutdown();
    send_transactions_pool.shutdown();
}
