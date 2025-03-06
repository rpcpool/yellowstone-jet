mod testkit;

use {
    core::panic,
    solana_sdk::{
        hash::Hash,
        message::{v0, VersionedMessage},
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        system_instruction,
        transaction::VersionedTransaction,
    },
    std::{
        sync::{Arc, Mutex, RwLock as StdRwLock},
        time::Duration,
    },
    testkit::default_config_transaction,
    tokio::sync::{broadcast, oneshot, Barrier, Notify, RwLock},
    yellowstone_jet::{
        blockhash_queue::testkit::MockBlockhashQueue,
        transactions::{
            testkit::mock_rooted_tx_channel, BoxedTxChannelPermit, SendTransactionInfoId,
            SendTransactionRequest, SendTransactionsPool, TxChannel, TxChannelPermit,
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
        blocklist_pdas: vec![],
    }
}

#[derive(Clone, Default)]
enum SpyTxChannelMode {
    FailSend,
    FailPermit,
    #[default]
    Succeed,
}

type SendCalls = Arc<StdRwLock<Vec<(SendTransactionInfoId, Signature, Arc<Vec<u8>>)>>>;

#[derive(Clone)]
struct SpyTxChannel {
    #[allow(clippy::type_complexity)]
    send_calls: SendCalls,
    permit_calls: Arc<StdRwLock<Vec<()>>>,
    mode: Arc<Mutex<SpyTxChannelMode>>,
    tx_call_notify: broadcast::Sender<Signature>,
}

impl Default for SpyTxChannel {
    fn default() -> Self {
        let (tx_call_notify, _) = broadcast::channel(10);
        Self {
            send_calls: Arc::new(StdRwLock::new(Vec::new())),
            permit_calls: Arc::new(StdRwLock::new(Vec::new())),
            mode: Arc::new(Mutex::new(SpyTxChannelMode::Succeed)),
            tx_call_notify,
        }
    }
}

impl SpyTxChannel {
    fn send_calls_count(&self) -> usize {
        self.send_calls.read().unwrap().len()
    }

    fn set_mode(&self, mode: SpyTxChannelMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn subscribe_calls(&self) -> broadcast::Receiver<Signature> {
        self.tx_call_notify.subscribe()
    }

    fn permit_calls_count(&self) -> usize {
        self.permit_calls.read().unwrap().len()
    }
}

struct SpyTxChannelPermit {
    send_calls: SendCalls,
    mode: Arc<Mutex<SpyTxChannelMode>>,
    tx_call_notify: broadcast::Sender<Signature>,
}

#[async_trait::async_trait]
impl TxChannelPermit for SpyTxChannelPermit {
    async fn send_transaction(
        self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        self.send_calls
            .write()
            .unwrap()
            .push((id, signature, wire_transaction));

        let _ = self.tx_call_notify.send(signature);
        let mode = { self.mode.lock().unwrap().clone() };
        match mode {
            SpyTxChannelMode::FailSend => {
                panic!("Error sending transaction");
            }
            SpyTxChannelMode::Succeed => {}
            _ => unreachable!("Invalid mode"),
        }
    }
}

#[async_trait::async_trait]
impl TxChannel for SpyTxChannel {
    async fn reserve(
        &self,
        _leader_forward_count: usize,
        _blocklist_keys: Vec<Pubkey>,
    ) -> Option<BoxedTxChannelPermit> {
        let mode = Arc::clone(&self.mode);
        let curr_mode = { mode.lock().unwrap().clone() };
        self.permit_calls.write().unwrap().push(());
        if let SpyTxChannelMode::FailPermit = curr_mode {
            panic!("Error reserving permit");
        }
        let send_calls = Arc::clone(&self.send_calls);
        let tx_call_notify = self.tx_call_notify.clone();
        Some(BoxedTxChannelPermit::new(SpyTxChannelPermit {
            send_calls,
            mode,
            tx_call_notify,
        }))
    }
}

#[tokio::test]
async fn test_transaction_send_successful_lifecycle() {
    let (tx, mut tx_recv) = tokio::sync::mpsc::channel(100);

    pub struct MockTxSender {
        tx: tokio::sync::mpsc::Sender<(SendTransactionInfoId, Signature, Arc<Vec<u8>>)>,
    }

    pub struct MockTxChannelPermit {
        tx: tokio::sync::mpsc::Sender<(SendTransactionInfoId, Signature, Arc<Vec<u8>>)>,
    }

    #[async_trait::async_trait]
    impl TxChannelPermit for MockTxChannelPermit {
        async fn send_transaction(
            self,
            id: SendTransactionInfoId,
            signature: Signature,
            wire_transaction: Arc<Vec<u8>>,
        ) {
            self.tx
                .send((id, signature, wire_transaction))
                .await
                .expect("Error sending transaction");
        }
    }

    #[async_trait::async_trait]
    impl TxChannel for MockTxSender {
        async fn reserve(
            &self,
            _leader_forward_count: usize,
            _blocklist_keys: Vec<Pubkey>,
        ) -> Option<BoxedTxChannelPermit> {
            let permit = MockTxChannelPermit {
                tx: self.tx.clone(),
            };
            Some(BoxedTxChannelPermit::new(permit))
        }
    }

    let mock_tx_sender = MockTxSender { tx: tx.clone() };

    let (spy_tx, spy_rx) = oneshot::channel();
    let h1 = tokio::spawn(async move {
        let (_, _sig, wire_tx) = tx_recv.recv().await.expect("Channel was closed");
        let transaction = bincode::deserialize::<VersionedTransaction>(&wire_tx)
            .expect("Error deserializing from bincode");
        spy_tx.send(transaction).expect("Error sending transaction");
    });

    let (_tx, rooted_transactions) = mock_rooted_tx_channel();
    let block_height_service = MockBlockhashQueue::new();

    let tx_hash = Hash::new_unique();
    let transaction_request = create_send_transaction_request(tx_hash, 1);

    block_height_service.increase_block_height(tx_hash).await;

    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transactions),
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
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();

    block_height_service.increase_block_height(tx_hash).await;

    let spy_tx_sender = SpyTxChannel::default();
    let (_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
        Arc::new(spy_tx_sender.clone()),
    )
    .await;

    let _handle = tokio::spawn(send_tx_pool_fut);
    // Should not fail
    send_transactions_pool.flush().await;
    assert_eq!(spy_tx_sender.send_calls_count(), 0);
}

#[tokio::test]
async fn it_should_flush_pending_tx() {
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();

    block_height_service.increase_block_height(tx_hash).await;
    struct BlockingMockTxSendPermit {
        calls: Arc<RwLock<Vec<Signature>>>,
        wait: Arc<tokio::sync::Notify>,
        barrier: Arc<Barrier>,
    }

    #[async_trait::async_trait]
    impl TxChannelPermit for BlockingMockTxSendPermit {
        async fn send_transaction(
            self,
            _id: SendTransactionInfoId,
            signature: Signature,
            _wire_transaction: Arc<Vec<u8>>,
        ) {
            self.barrier.wait().await;
            self.wait.notified().await;
            self.calls.write().await.push(signature);
        }
    }

    #[derive(Clone)]
    struct BlockingMockTxChannel {
        wait: Arc<tokio::sync::Notify>,
        calls: Arc<RwLock<Vec<Signature>>>,
        barrier: Arc<Barrier>,
    }

    #[async_trait::async_trait]
    impl TxChannel for BlockingMockTxChannel {
        async fn reserve(
            &self,
            _leader_forward_count: usize,
            _blocklist_keys: Vec<Pubkey>,
        ) -> Option<BoxedTxChannelPermit> {
            let calls = Arc::clone(&self.calls);
            let wait = Arc::clone(&self.wait);
            let barrier = Arc::clone(&self.barrier);
            let permit = BlockingMockTxSendPermit {
                calls,
                wait,
                barrier,
            };
            Some(BoxedTxChannelPermit::new(permit))
        }
    }

    impl BlockingMockTxChannel {
        async fn calls(&self) -> Vec<Signature> {
            self.calls.read().await.clone()
        }
    }

    let unblock_signal = Arc::new(tokio::sync::Notify::new());
    let barrier = Arc::new(Barrier::new(3 + 1)); // 3 = number of tasks, 1 = main test thread
    let mock_tx_sender = BlockingMockTxChannel {
        wait: Arc::clone(&unblock_signal),
        calls: Arc::new(RwLock::new(Vec::new())),
        barrier: Arc::clone(&barrier),
    };

    let (_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
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
    barrier.wait().await;
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

#[tokio::test]
async fn it_should_retry_failed_send_transactions() {
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();
    let retry_count = 3;
    let tx1 = create_send_transaction_request(tx_hash, retry_count);
    block_height_service.increase_block_height(tx_hash).await;

    let spy_tx_sender = SpyTxChannel::default();
    spy_tx_sender.set_mode(SpyTxChannelMode::FailSend);

    let (_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
        Arc::new(spy_tx_sender.clone()),
    )
    .await;

    let _handle = tokio::spawn(send_tx_pool_fut);

    let mut calls_rx = spy_tx_sender.subscribe_calls();
    send_transactions_pool
        .send_transaction(tx1.clone())
        .expect("Error sending transaction to pool");

    let mut actual_retry_cnt = 0;
    while let Ok(sig) = calls_rx.recv().await {
        tracing::info!("Received signature: {:?}", sig);
        assert!(sig == tx1.signature);
        actual_retry_cnt += 1;
        if actual_retry_cnt == retry_count {
            break;
        }
    }

    assert_eq!(actual_retry_cnt, retry_count);
    assert_eq!(spy_tx_sender.send_calls_count(), 3);
}

#[tokio::test]
async fn it_should_retry_on_failed_permit_tx() {
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();
    let retry_count = 3;
    let tx1 = create_send_transaction_request(tx_hash, retry_count);
    block_height_service.increase_block_height(tx_hash).await;

    let spy_tx_sender = SpyTxChannel::default();
    spy_tx_sender.set_mode(SpyTxChannelMode::FailPermit);

    let (_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
        Arc::new(spy_tx_sender.clone()),
    )
    .await;

    let _handle = tokio::spawn(send_tx_pool_fut);

    let mut deadletter = send_transactions_pool.subscribe_dead_letter();
    send_transactions_pool
        .send_transaction(tx1.clone())
        .expect("Error sending transaction to pool");

    let sig = deadletter
        .recv()
        .await
        .expect("Error receiving dead letter");

    assert_eq!(spy_tx_sender.send_calls_count(), 0);
    assert_eq!(spy_tx_sender.permit_calls_count(), 3);
    assert_eq!(sig, tx1.signature);
}

#[tokio::test]
async fn it_should_not_attempt_already_finalized_tx() {
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();
    let retry_count = 3;
    let tx1 = create_send_transaction_request(tx_hash, retry_count);
    block_height_service.increase_block_height(tx_hash).await;

    let spy_tx_sender = SpyTxChannel::default();
    spy_tx_sender.set_mode(SpyTxChannelMode::FailSend);

    let (mocked_rooted_transaction_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
        Arc::new(spy_tx_sender.clone()),
    )
    .await;

    // Make this tx finalized already
    mocked_rooted_transaction_tx
        .send(
            tx1.signature,
            yellowstone_jet::util::CommitmentLevel::Finalized,
        )
        .await;

    let _handle = tokio::spawn(send_tx_pool_fut);

    let mut finalized_tx_rx = send_transactions_pool.subscribe_to_finalized_tx();
    send_transactions_pool
        .send_transaction(tx1.clone())
        .expect("Error sending transaction to pool");

    let sig = finalized_tx_rx
        .recv()
        .await
        .expect("Error receiving dead letter");

    assert_eq!(spy_tx_sender.send_calls_count(), 0);
    assert_eq!(spy_tx_sender.permit_calls_count(), 0);
    assert_eq!(sig, tx1.signature);
}

#[tokio::test]
async fn it_should_not_retry_tx_that_become_finalized() {
    // This test makes sure the transaction is not retry when it becomes finalized mid-flight in the first
    // send attempt.
    let block_height_service = MockBlockhashQueue::new();
    let tx_hash = Hash::new_unique();
    let retry_count = 3;
    let tx1 = create_send_transaction_request(tx_hash, retry_count);
    block_height_service.increase_block_height(tx_hash).await;

    #[derive(Clone)]
    pub struct MockTxSender {
        send_calls: Arc<RwLock<Vec<Signature>>>,
        tx: tokio::sync::mpsc::Sender<(SendTransactionInfoId, Signature, Arc<Vec<u8>>)>,
        blocker: Arc<Notify>,
        barrier: Arc<Barrier>,
    }

    pub struct MockTxChannelPermit {
        send_calls: Arc<RwLock<Vec<Signature>>>,
        #[allow(dead_code)]
        tx: tokio::sync::mpsc::Sender<(SendTransactionInfoId, Signature, Arc<Vec<u8>>)>,
        blocker: Arc<Notify>,
        barrier: Arc<Barrier>,
    }

    #[async_trait::async_trait]
    impl TxChannelPermit for MockTxChannelPermit {
        async fn send_transaction(
            self,
            _id: SendTransactionInfoId,
            _signature: Signature,
            _wire_transaction: Arc<Vec<u8>>,
        ) {
            {
                self.send_calls.write().await.push(_signature);
            }
            self.barrier.wait().await;
            self.blocker.notified().await;
            panic!("Error sending transaction");
        }
    }

    #[async_trait::async_trait]
    impl TxChannel for MockTxSender {
        async fn reserve(
            &self,
            _leader_forward_count: usize,
            _blocklist_keys: Vec<Pubkey>,
        ) -> Option<BoxedTxChannelPermit> {
            let permit = MockTxChannelPermit {
                send_calls: Arc::clone(&self.send_calls),
                tx: self.tx.clone(),
                blocker: Arc::clone(&self.blocker),
                barrier: Arc::clone(&self.barrier),
            };
            Some(BoxedTxChannelPermit::new(permit))
        }
    }

    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let blocker = Arc::new(Notify::new());
    let barrier = Arc::new(Barrier::new(2));
    let mock_tx_sender = MockTxSender {
        tx,
        send_calls: Default::default(),
        blocker: Arc::clone(&blocker),
        barrier: Arc::clone(&barrier),
    };

    let (mocked_rooted_transaction_tx, rooted_transaction_rx) = mock_rooted_tx_channel();
    let (send_transactions_pool, send_tx_pool_fut) = SendTransactionsPool::spawn(
        default_config_transaction(),
        Arc::new(block_height_service),
        Box::new(rooted_transaction_rx),
        Arc::new(mock_tx_sender.clone()),
    )
    .await;

    // Make this tx finalized already

    let _handle = tokio::spawn(send_tx_pool_fut);

    let mut finalized_tx_rx = send_transactions_pool.subscribe_to_finalized_tx();
    send_transactions_pool
        .send_transaction(tx1.clone())
        .expect("Error sending transaction to pool");

    // Waits for the send to be inflight...
    barrier.wait().await;

    // Send the Finalized notification mid-flight
    mocked_rooted_transaction_tx
        .send(
            tx1.signature,
            yellowstone_jet::util::CommitmentLevel::Finalized,
        )
        .await;

    // Unblock the inflight send.
    blocker.notify_one();

    // Wait for the finalized tx to be received.
    let sig = finalized_tx_rx
        .recv()
        .await
        .expect("Error receiving dead letter");

    let total_send_calls = mock_tx_sender.send_calls.read().await.len();
    assert_eq!(sig, tx1.signature);
    assert_eq!(total_send_calls, 1);
}
