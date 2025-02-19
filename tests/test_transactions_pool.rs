mod testkit;

use {
    solana_sdk::{
        hash::Hash,
        message::{v0, VersionedMessage},
        signature::{Keypair, Signature},
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    std::{
        sync::{Arc, RwLock as StdRwLock},
        time::Duration,
    },
    testkit::default_config_transaction,
    tokio::sync::{oneshot, RwLock},
    yellowstone_jet::{
        blockhash_queue::testkit::MockBlockhashQueue,
        transactions::{
            testkit::MockRootedTransactions, SendTransactionInfoId, SendTransactionRequest,
            SendTransactionsPool, TxSender,
        },
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

#[derive(Clone, Default)]
struct SpyTxSender {
    #[allow(clippy::type_complexity)]
    calls: Arc<StdRwLock<Vec<(SendTransactionInfoId, Signature, Arc<Vec<u8>>, usize)>>>,
}

impl SpyTxSender {
    fn call_count(&self) -> usize {
        self.calls.read().unwrap().len()
    }
}

#[async_trait::async_trait]
impl TxSender for SpyTxSender {
    async fn send_transaction(
        &self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
        leader_forward_count: usize,
    ) {
        self.calls
            .write()
            .unwrap()
            .push((id, signature, wire_transaction, leader_forward_count));
    }
}

#[tokio::test]
async fn test_transaction_send_successful_lifecycle() {
    let (tx, mut tx_recv) = tokio::sync::mpsc::channel(100);

    pub struct MockTxSender {
        tx: tokio::sync::mpsc::Sender<(SendTransactionInfoId, Signature, Arc<Vec<u8>>, usize)>,
    }

    #[async_trait::async_trait]
    impl TxSender for MockTxSender {
        async fn send_transaction(
            &self,
            id: SendTransactionInfoId,
            signature: Signature,
            wire_transaction: Arc<Vec<u8>>,
            leader_forward_count: usize,
        ) {
            self.tx
                .send((id, signature, wire_transaction, leader_forward_count))
                .await
                .expect("Error sending transaction");
        }
    }

    let mock_tx_sender = MockTxSender { tx: tx.clone() };

    let (spy_tx, spy_rx) = oneshot::channel();
    let h1 = tokio::spawn(async move {
        let (_, _sig, wire_tx, _) = tx_recv.recv().await.expect("Channel was closed");
        let transaction = bincode::deserialize::<VersionedTransaction>(&wire_tx)
            .expect("Error deserializing from bincode");
        spy_tx.send(transaction).expect("Error sending transaction");
    });

    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();

    let tx_hash = Hash::new_unique();
    let transaction_request = create_send_transaction_request(tx_hash, 1);

    block_height_service.increase_block_height(tx_hash).await;

    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        Arc::new(mock_tx_sender),
    )
    .await;

    let send_tx_pool_handle = tokio::spawn(send_tx_pool_fut);

    let transaction_compare = transaction_request.transaction.clone();

    send_transactions_pool
        .send_transaction(transaction_request)
        .expect("Error sending transaction to pool");

    let rx_transaction = spy_rx.await.expect("Error receiving transaction");

    assert_eq!(
        rx_transaction, transaction_compare,
        "Error receiving transaction"
    );

    drop(send_transactions_pool);
    let _ = h1.await;
    let _ = send_tx_pool_handle.await;
}

#[tokio::test]
async fn flushing_without_any_tx_in_queue_should_be_noop() {
    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();

    block_height_service.increase_block_height(tx_hash).await;

    let spy_tx_sender = SpyTxSender::default();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        Arc::new(spy_tx_sender.clone()),
    )
    .await;

    let _handle = tokio::spawn(send_tx_pool_fut);

    // Should not fail
    send_transactions_pool.flush().await;
    assert_eq!(spy_tx_sender.call_count(), 0);
}

#[tokio::test]
async fn it_should_flush_pending_tx() {
    let rooted_transactions = MockRootedTransactions::new();
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();

    block_height_service.increase_block_height(tx_hash).await;

    #[derive(Clone)]
    struct BlockingMockTxSender {
        wait: Arc<tokio::sync::Notify>,
        calls: Arc<RwLock<Vec<Signature>>>,
    }

    #[async_trait::async_trait]
    impl TxSender for BlockingMockTxSender {
        async fn send_transaction(
            &self,
            _id: SendTransactionInfoId,
            _signature: Signature,
            _wire_transaction: Arc<Vec<u8>>,
            _leader_forward_count: usize,
        ) {
            self.wait.notified().await;
            {
                let mut calls = self.calls.write().await;
                calls.push(_signature);
            }
        }
    }

    impl BlockingMockTxSender {
        async fn calls(&self) -> Vec<Signature> {
            self.calls.read().await.clone()
        }
    }

    let unblock_signal = Arc::new(tokio::sync::Notify::new());
    let mock_tx_sender = BlockingMockTxSender {
        wait: Arc::clone(&unblock_signal),
        calls: Arc::new(RwLock::new(Vec::new())),
    };

    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Arc::new(rooted_transactions),
        Arc::new(mock_tx_sender.clone()),
    )
    .await;

    let _handle = tokio::spawn(send_tx_pool_fut);

    let tx1 = create_send_transaction_request(tx_hash, 1);
    let tx1_sig = tx1.transaction.signatures[0];
    let tx2 = create_send_transaction_request(tx_hash, 1);
    let tx2_sig = tx2.transaction.signatures[0];
    let tx3 = create_send_transaction_request(tx_hash, 1);
    let tx3_sig = tx3.transaction.signatures[0];

    send_transactions_pool
        .send_transaction(tx1)
        .expect("Error sending transaction to pool");

    send_transactions_pool
        .send_transaction(tx2)
        .expect("Error sending transaction to pool");

    send_transactions_pool
        .send_transaction(tx3)
        .expect("Error sending transaction to pool");

    // Flush will be blocking
    let send_tx_pool2 = send_transactions_pool.clone();
    let flush_handle = tokio::spawn(async move {
        send_tx_pool2.flush().await;
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    unblock_signal.notify_waiters();

    flush_handle.await.expect("Error flushing");
    let actual_calls = mock_tx_sender.calls().await;

    assert_eq!(actual_calls.len(), 3);
    let actual_sig_set = actual_calls
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let expected_sig_set = vec![tx1_sig, tx2_sig, tx3_sig]
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(actual_sig_set, expected_sig_set);
}
