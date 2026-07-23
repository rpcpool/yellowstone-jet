mod testkit;

use {
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::{advance_nonce_account, transfer},
    solana_transaction::versioned::VersionedTransaction,
    std::{
        sync::{Arc, RwLock as StdRwLock},
        time::Duration,
        vec,
    },
    tokio::sync::mpsc,
    yellowstone_jet::{
        blockhash_queue::testkit::MockBlockhashQueue,
        transactions::{
            SendTransactionRequest, TransactionRetryScheduler, TransactionRetrySchedulerConfig,
            TransactionRetrySchedulerDlqEvent, UpcomingLeaderSchedule,
            testkit::mock_rooted_tx_channel,
        },
        util::CommitmentLevel,
    },
};

pub fn create_send_transaction_request(hash: Hash, max_resent: usize) -> SendTransactionRequest {
    let fake_wallet_keypair1 = Keypair::new();
    let fake_wallet_keypair2 = Keypair::new();
    let instructions = vec![transfer(
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
        wire_transaction: wire_transaction.into(),
        transaction: tx,
        policies: vec![],
        x_request_id: None,
    }
}

pub fn create_durable_nonce_txn_request(
    stored_nonce: Hash,
    max_resent: usize,
    nonce_address: Pubkey,
) -> SendTransactionRequest {
    let fake_wallet_keypair1 = Keypair::new();
    let fake_wallet_keypair2 = Keypair::new();
    let instructions = vec![
        advance_nonce_account(&nonce_address, &fake_wallet_keypair1.pubkey()),
        transfer(
            &fake_wallet_keypair1.pubkey(),
            &fake_wallet_keypair2.pubkey(),
            10,
        ),
    ];

    let tx = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(
                &fake_wallet_keypair1.pubkey(),
                &instructions,
                &[],
                stored_nonce,
            )
            .expect("try compile"),
        ),
        &[&fake_wallet_keypair1],
    )
    .expect("try new");

    let wire_transaction = bincode::serialize(&tx).expect("Error getting wire_transaction");

    SendTransactionRequest {
        max_retries: Some(max_resent),
        signature: tx.signatures[0],
        wire_transaction: wire_transaction.into(),
        transaction: tx,
        policies: vec![],
        x_request_id: None,
    }
}

#[derive(Default, Clone)]
pub struct FakeLeaderSchedule {
    share: Arc<StdRwLock<Vec<Pubkey>>>,
}

impl FakeLeaderSchedule {
    pub fn set_schedule(&self, schedule: Vec<Pubkey>) {
        let mut curr = self.share.write().unwrap();
        *curr = schedule;
    }
}

impl UpcomingLeaderSchedule for FakeLeaderSchedule {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey> {
        let schedule = self.share.read().unwrap();
        schedule[..leader_forward_lookahead].to_vec()
    }
    fn get_current_slot(&self) -> solana_clock::Slot {
        // For testing purposes, we can return a dummy slot.
        // In a real implementation, this would return the current slot.
        0
    }
}

#[tokio::test]
async fn it_should_retry_transaction_three_time() {
    let blockheight_service = MockBlockhashQueue::new();
    let config = TransactionRetrySchedulerConfig {
        max_retry: 10,
        retry_rate: Duration::from_millis(10),
        ..Default::default()
    };

    let (_rooted_tx, rooted_rx) = mock_rooted_tx_channel();
    let (dlq_tx, mut dlq_rx) = mpsc::unbounded_channel();

    let TransactionRetryScheduler { sink, mut source } = TransactionRetryScheduler::new(
        config,
        Arc::new(blockheight_service.clone()),
        Box::new(rooted_rx),
        Some(dlq_tx),
    );

    let blockhash = Hash::new_unique();
    blockheight_service.increase_block_height(blockhash);
    let tx = create_send_transaction_request(blockhash, 3);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();

    for _ in 0..3 {
        let request = source.recv().await.expect("Failed to receive request");
        assert_eq!(request.signature, tx.signature);
        assert_eq!(
            request.transaction.signatures[0],
            tx.transaction.signatures[0]
        );
        assert_eq!(request.wire_transaction, tx.wire_transaction);
    }

    source.try_recv().expect_err("Expected no more requests");
    let dlq_ev = dlq_rx
        .recv()
        .await
        .expect("Expected valid transaction in dead letter queue");
    assert!(matches!(
        dlq_ev,
        TransactionRetrySchedulerDlqEvent::ReachedMaxRetries(_)
    ));
}

#[tokio::test]
async fn it_should_not_attempt_invalid_transaction() {
    // setup_tracing_test(yellowstone_jet::transactions::module_path_for_test());
    let blockheight_service = MockBlockhashQueue::new();
    let config = TransactionRetrySchedulerConfig {
        max_retry: 10,
        retry_rate: Duration::from_millis(10),
        transaction_max_processing_age: 0,
        ..Default::default()
    };

    let (_rooted_tx, rooted_rx) = mock_rooted_tx_channel();
    let (dlq_tx, mut dlq_rx) = mpsc::unbounded_channel();
    let TransactionRetryScheduler { sink, mut source } = TransactionRetryScheduler::new(
        config,
        Arc::new(blockheight_service.clone()),
        Box::new(rooted_rx),
        Some(dlq_tx),
    );

    let blockhash1 = Hash::new_unique();
    let blockhash2 = Hash::new_unique();
    blockheight_service.increase_block_height(blockhash1);
    blockheight_service.increase_block_height(blockhash2);
    let tx = create_send_transaction_request(blockhash1, 3);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();
    source.try_recv().expect_err("Expected no more requests");
    let dlq_ev = dlq_rx
        .recv()
        .await
        .expect("Expected valid transaction in dead letter queue");

    println!("dlq_ev: {dlq_ev:?}");
    assert!(matches!(
        dlq_ev,
        TransactionRetrySchedulerDlqEvent::ReachedMaxProcessingAge(_)
    ));
}

#[tokio::test]
async fn it_should_retry_durable_nonce_transaction_without_blockhash_validation() {
    let blockheight_service = MockBlockhashQueue::new();
    let config = TransactionRetrySchedulerConfig {
        max_retry: 10,
        retry_rate: Duration::from_millis(10),
        transaction_max_processing_age: 0,
        ..Default::default()
    };

    let (_rooted_tx, rooted_rx) = mock_rooted_tx_channel();
    let (dlq_tx, mut dlq_rx) = mpsc::unbounded_channel();
    let TransactionRetryScheduler { sink, mut source } = TransactionRetryScheduler::new(
        config,
        Arc::new(blockheight_service.clone()),
        Box::new(rooted_rx),
        Some(dlq_tx),
    );

    let blockhash1 = Hash::new_unique();
    let blockhash2 = Hash::new_unique();
    blockheight_service.increase_block_height(blockhash1);
    blockheight_service.increase_block_height(blockhash2);

    let durable_nonce_account = Pubkey::new_unique();
    let tx = create_durable_nonce_txn_request(blockhash1, 3, durable_nonce_account);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();

    for _ in 0..3 {
        let request = source.recv().await.expect("Failed to receive request");
        assert_eq!(request.signature, tx.signature);
        assert_eq!(request.wire_transaction, tx.wire_transaction);
    }

    source.try_recv().expect_err("Expected no more requests");
    let dlq_ev = dlq_rx
        .recv()
        .await
        .expect("Expected transaction in dead letter queue");
    assert!(matches!(
        dlq_ev,
        TransactionRetrySchedulerDlqEvent::ReachedMaxRetries(_)
    ));
}

#[tokio::test]
async fn it_should_skip_landed_durable_nonce_transaction() {
    let blockheight_service = MockBlockhashQueue::new();
    let config = TransactionRetrySchedulerConfig {
        max_retry: 10,
        retry_rate: Duration::from_millis(10),
        transaction_max_processing_age: 0,
        ..Default::default()
    };

    let (rooted_tx, rooted_rx) = mock_rooted_tx_channel();
    let (dlq_tx, mut dlq_rx) = mpsc::unbounded_channel();
    let TransactionRetryScheduler { sink, mut source } = TransactionRetryScheduler::new(
        config,
        Arc::new(blockheight_service),
        Box::new(rooted_rx),
        Some(dlq_tx),
    );

    let durable_nonce_account = Pubkey::new_unique();
    let tx = create_durable_nonce_txn_request(Hash::new_unique(), 3, durable_nonce_account);
    rooted_tx
        .send(tx.signature, CommitmentLevel::Confirmed)
        .await;
    sink.send(Arc::new(tx)).unwrap();

    let dlq_ev = tokio::time::timeout(Duration::from_secs(1), dlq_rx.recv())
        .await
        .expect("timed out waiting for already-landed event")
        .expect("dead letter queue closed");
    assert!(matches!(
        dlq_ev,
        TransactionRetrySchedulerDlqEvent::AlreadyLanded(_)
    ));
    source
        .try_recv()
        .expect_err("Expected no forwarded request");
}

#[tokio::test]
async fn no_retry_scheduler_should_forward_durable_nonce_without_blockhash_validation() {
    let blockheight_service = MockBlockhashQueue::new();
    for _ in 0..200 {
        blockheight_service.increase_block_height(Hash::new_unique());
    }

    let yellowstone_jet::transactions::TransactionNoRetryScheduler { sink, mut source } =
        yellowstone_jet::transactions::TransactionNoRetryScheduler::new(Arc::new(
            blockheight_service,
        ));

    let durable_nonce_account = Pubkey::new_unique();
    let tx = create_durable_nonce_txn_request(Hash::new_unique(), 0, durable_nonce_account);
    let tx = Arc::new(tx);
    sink.send(Arc::clone(&tx)).unwrap();

    let request = tokio::time::timeout(Duration::from_secs(1), source.recv())
        .await
        .expect("timed out waiting for durable nonce transaction")
        .expect("scheduler source closed");
    assert_eq!(request.signature, tx.signature);
    assert_eq!(request.wire_transaction, tx.wire_transaction);
}
