use {
    crate::{
        blockhash_queue::BlockHeightService,
        cluster_tpu_info::ClusterTpuInfo,
        grpc_geyser::{BlockMetaWithCommitment, GrpcUpdateMessage, TransactionReceived},
        metrics::jet as metrics,
        quic_gateway::{GatewayResponse, GatewayTransaction},
        util::CommitmentLevel,
    },
    bytes::Bytes,
    solana_clock::{MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES, Slot},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        future::Future,
        ops::DerefMut,
        sync::{Arc, RwLock as StdRwLock},
    },
    tokio::{
        sync::mpsc::{self},
        task::{self, JoinSet},
        time::Instant,
    },
    tracing::error,
    yellowstone_shield_store::{CheckError, PolicyStoreTrait},
};

#[derive(Debug, Default)]
pub struct RootedTransactionsSlotInfo {
    transactions: HashSet<Signature>,
    blockmeta: Option<BlockMetaWithCommitment>,
}

pub type RootedTransactionsUpdateSignature = (Signature, CommitmentLevel);

///
/// Trait for getting the upcoming leader schedule
///
pub trait UpcomingLeaderSchedule {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey>;
}

impl UpcomingLeaderSchedule for ClusterTpuInfo {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey> {
        self.get_leader_tpus(leader_forward_lookahead)
            .into_iter()
            .map(|tpu| tpu.leader)
            .collect()
    }
}

///
/// Base trait for Rooted transaction update notifier
///
#[async_trait::async_trait]
pub trait RootedTxReceiver {
    fn subscribe_signature(&mut self, signature: Signature);
    fn unsubscribe_signature(&mut self, signature: Signature);
    // async fn unsubscribe_signatures(&mut self, signatures: Vec<Signature>);
    fn get_transaction_commitment(&mut self, signature: Signature) -> Option<CommitmentLevel>;
    async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)>;
}

#[derive(Debug, Default)]
pub struct RootedTransactionsInner {
    slots: HashMap<Slot, RootedTransactionsSlotInfo>,
    transactions: HashMap<Signature, Slot>,
    watch_signatures: HashSet<Signature>,
}

///
/// Rooted transaction receiver that uses gRPC dragonsmouth API to receive transaction updates
///
#[derive(Debug)]
pub struct GrpcRootedTxReceiver {
    inner: Arc<StdRwLock<RootedTransactionsInner>>,
    rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
}

#[async_trait::async_trait]
impl RootedTxReceiver for GrpcRootedTxReceiver {
    fn get_transaction_commitment(&mut self, signature: Signature) -> Option<CommitmentLevel> {
        let locked = self.inner.read().expect("RwLock poisoned");
        locked
            .transactions
            .get(&signature)
            .and_then(|slot| locked.slots.get(slot))
            .and_then(|info| info.blockmeta)
            .map(|info| info.commitment)
    }

    fn subscribe_signature(&mut self, signature: Signature) {
        let mut locked = self.inner.write().expect("RwLock poisoned");
        locked.watch_signatures.insert(signature);
    }

    fn unsubscribe_signature(&mut self, signature: Signature) {
        let mut locked = self.inner.write().expect("RwLock poisoned");
        locked.watch_signatures.remove(&signature);
    }

    async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)> {
        self.rx.recv().await
    }
}

impl GrpcRootedTxReceiver {
    ///
    /// Creates a new rooted transactions service
    ///
    /// Arguments:
    ///
    /// * `grpc_rx` - gRPC sending transaction updates
    ///
    pub fn new(grpc_rx: mpsc::Receiver<GrpcUpdateMessage>) -> (Self, impl Future) {
        let shared = Default::default();
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_fut = Self::start_loop(grpc_rx, Arc::clone(&shared), tx);
        let this = Self { inner: shared, rx };
        (this, loop_fut)
    }

    async fn start_loop(
        mut grpc_rx: mpsc::Receiver<GrpcUpdateMessage>,
        inner: Arc<StdRwLock<RootedTransactionsInner>>,
        signature_updates_tx: mpsc::UnboundedSender<RootedTransactionsUpdateSignature>,
    ) {
        let mut transactions_bulk = vec![];
        loop {
            let blockmeta_update = tokio::select! {
                message = grpc_rx.recv() => match message {
                    Some(GrpcUpdateMessage::Transaction(transaction)) => {
                        transactions_bulk.push(transaction);
                        continue;
                    }
                    Some(GrpcUpdateMessage::Slot(_)) => {
                        // We don't need slot updates here
                        continue;
                    },
                    Some(GrpcUpdateMessage::BlockMeta(blockmeta)) => blockmeta,
                    None => panic!("RootedTransactions: gRPC streamed closed"),
                }
            };

            // Acquire write lock
            // IMPORTANT: Don't hold lock across await points
            let mut locked = inner.write().expect("RwLock poisoned");
            let RootedTransactionsInner {
                slots,
                transactions,
                watch_signatures,
                ..
            } = locked.deref_mut();

            // Update slot commitment
            let entry = slots.entry(blockmeta_update.slot).or_default();
            entry.blockmeta = Some(blockmeta_update);
            for signature in entry.transactions.iter() {
                if watch_signatures.contains(signature)
                    && signature_updates_tx
                        .send((*signature, blockmeta_update.commitment))
                        .is_err()
                {
                    // The loop shutdown when the receiver is closed
                    return;
                }
            }

            // Removed outdated slots
            if blockmeta_update.commitment == CommitmentLevel::Finalized {
                let mut removed = Vec::new();
                slots.retain(|_slot, info| {
                    let retain = if let Some(blockmeta) = info.blockmeta {
                        if blockmeta.commitment == CommitmentLevel::Finalized {
                            // Keep more blocks than MAX_PROCESSING_AGE
                            blockmeta.block_height + MAX_RECENT_BLOCKHASHES as u64
                                > blockmeta.block_height
                        } else {
                            blockmeta.slot > blockmeta_update.slot
                        }
                    } else {
                        true
                    };

                    if !retain {
                        removed.push(std::mem::take(&mut info.transactions));
                    }

                    retain
                });
                for signature in removed.into_iter().flatten() {
                    transactions.remove(&signature);
                }
            }

            // Add transactions
            for TransactionReceived { slot, signature } in transactions_bulk.drain(..) {
                let entry = slots.entry(slot).or_default();
                if entry.transactions.insert(signature) {
                    if let Some(blockmeta) = &entry.blockmeta {
                        if watch_signatures.contains(&signature)
                            && signature_updates_tx
                                .send((signature, blockmeta.commitment))
                                .is_err()
                        {
                            // The loop shutdown when the receiver is closed
                            return;
                        }
                    }
                    transactions.insert(signature, slot);
                }
            }

            metrics::rooted_transactions_pool_set_size(transactions.len());
        }
    }
}

#[derive(Debug, Clone)]
pub struct SendTransactionRequest {
    pub signature: Signature,
    pub transaction: VersionedTransaction,
    pub wire_transaction: Bytes,
    pub max_retries: Option<usize>,
    pub policies: Vec<Pubkey>,
}

struct RetryableTx {
    tx: Arc<SendTransactionRequest>,
    leftover_attempt: usize,
}

///
/// Transaction scheduler runtime that tracks which transaction landed and resends transactions that are not landed yet.
///
pub struct TransactionRetrySchedulerRuntime {
    block_height_service: Arc<dyn BlockHeightService + Send + Sync + 'static>,
    rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
    last_known_block_height: u64,
    tx_pool: HashMap<Signature, RetryableTx>,
    block_height_deadline_map: BTreeMap<u64, Vec<Signature>>,
    insertion_order: VecDeque<(Instant, Signature)>,
    transaction_source: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
    response_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    retry_rate: tokio::time::Duration,
    stop_send_on_commitment: CommitmentLevel,
    max_retry: usize,
    max_processing_age: u64,
    dlq: Option<mpsc::UnboundedSender<TransactionRetrySchedulerDlqEvent>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionRetrySchedulerDlqEvent {
    ReachedMaxProcessingAge(Signature),
    ReachedMaxRetries(Signature),
    AlreadyLanded(Signature),
}

impl TransactionRetrySchedulerRuntime {
    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(self.retry_rate);

        loop {
            tokio::select! {
                now = interval.tick() => {
                    self.gc_deadline_map();
                    self.resend_non_landed_tx(now);
                }
                source = self.transaction_source.recv() => {
                    match source {
                        Some(newtx) => self.add_new_transaction(newtx, Instant::now()),
                        None => {
                            break;
                        }
                    }
                }
                maybe_signature_update = self.rooted_transactions.recv() => {
                    match maybe_signature_update {
                        Some((signature, commitment)) => self.handle_tx_commitment_update(signature, commitment),
                        None => {
                            error!("signature updates channel is closed");
                            break;
                        }
                    }
                },
            }
            metrics::sts_pool_set_size(self.tx_pool.len());
        }
    }

    fn resend_non_landed_tx(&mut self, now: Instant) {
        while let Some(inserted_at) = self.insertion_order.front().map(|(t, _)| *t) {
            if now.duration_since(inserted_at) < self.retry_rate {
                break;
            }
            let (_, signature) = self.insertion_order.pop_front().unwrap();
            if let Some(rtx) = self.tx_pool.get_mut(&signature) {
                if rtx.leftover_attempt > 0 {
                    rtx.leftover_attempt = rtx.leftover_attempt.saturating_sub(1);
                    tracing::trace!(
                        "resending tx {signature}, attempts left: {}",
                        rtx.leftover_attempt
                    );
                    let _ = self.response_sink.send(Arc::clone(&rtx.tx));
                    self.insertion_order.push_back((now, signature));
                } else {
                    self.tx_pool.remove(&signature);
                    if let Some(dlq) = &self.dlq {
                        let _ = dlq.send(TransactionRetrySchedulerDlqEvent::ReachedMaxRetries(
                            signature,
                        ));
                    }
                    tracing::trace!("tx {signature} reached max attempts, dropping it");
                }
            }
        }
    }

    fn gc_deadline_map(&mut self) {
        let outdated_block_height = self
            .last_known_block_height
            .saturating_sub(self.max_processing_age);
        let right = self
            .block_height_deadline_map
            .split_off(&outdated_block_height);

        let deprecrated_tx = std::mem::replace(&mut self.block_height_deadline_map, right);
        let deprecated_cnt = deprecrated_tx.values().map(|v| v.len()).sum::<usize>();
        if deprecated_cnt > 0 {
            tracing::debug!(
                "cleaning up {} outdated transactions from the deadline map",
                deprecated_cnt
            );
        }
        deprecrated_tx
            .into_iter()
            .flat_map(|(_, signatures)| signatures)
            .for_each(|signature| {
                self.tx_pool.remove(&signature);
                self.rooted_transactions.unsubscribe_signature(signature);
                if let Some(dlq) = &self.dlq {
                    let _ = dlq.send(TransactionRetrySchedulerDlqEvent::ReachedMaxProcessingAge(
                        signature,
                    ));
                }
            });
    }

    fn handle_tx_commitment_update(&mut self, signature: Signature, commitment: CommitmentLevel) {
        if commitment == self.stop_send_on_commitment {
            tracing::trace!(
                "transaction {signature} landed on commitment {commitment:?}, removing from the pool"
            );
            if self.tx_pool.remove(&signature).is_some() {
                metrics::sts_landed_inc();
            }
        }
    }

    fn add_new_transaction(&mut self, tx: Arc<SendTransactionRequest>, now: Instant) {
        // Make sure to not double count this metric elsewhere.
        metrics::sts_received_inc();
        let max_retries = tx.max_retries.unwrap_or(0).min(self.max_retry);

        let current_block_height = self
            .block_height_service
            .get_block_height_for_commitment(CommitmentLevel::Confirmed)
            .unwrap_or(0);
        self.last_known_block_height = current_block_height;
        let last_valid_block_height = self
            .block_height_service
            .get_block_height(tx.transaction.message.recent_blockhash())
            .map(|block_height| block_height + self.max_processing_age)
            .unwrap_or(current_block_height + self.max_processing_age);

        let signature = tx.signature;
        tracing::trace!(
            "received new transaction {signature}, max_retries: {max_retries}, last_valid_block_height: {last_valid_block_height}, current_block_height: {current_block_height}"
        );

        if last_valid_block_height < current_block_height {
            if let Some(dlq) = &self.dlq {
                let _ = dlq.send(TransactionRetrySchedulerDlqEvent::ReachedMaxProcessingAge(
                    signature,
                ));
            }
            tracing::warn!(
                %signature,
                last_valid_block_height,
                current_block_height,
                "transaction last valid block height is less than current block height, dropping transaction"
            );
            return;
        }

        let is_landed = self
            .rooted_transactions
            .get_transaction_commitment(signature)
            .filter(|cl| cl >= &self.stop_send_on_commitment)
            .is_some();

        if is_landed {
            tracing::debug!("skipping {signature}, transaction already landed");
            metrics::sts_landed_inc();
            if let Some(dlq) = &self.dlq {
                let _ = dlq.send(TransactionRetrySchedulerDlqEvent::AlreadyLanded(signature));
            }
            return;
        }

        self.rooted_transactions.subscribe_signature(signature);
        // -1 because we are going to send the transaction immediately
        let leftover_attempt = max_retries.saturating_sub(1);

        self.tx_pool.insert(
            signature,
            RetryableTx {
                tx: Arc::clone(&tx),
                leftover_attempt,
            },
        );
        tracing::trace!(
            "added transaction {signature} to the pool, attempts left: {}",
            leftover_attempt
        );
        self.block_height_deadline_map
            .entry(last_valid_block_height)
            .or_default()
            .push(signature);
        let _ = self.response_sink.send(tx);
        self.insertion_order.push_back((now, signature));
    }
}

pub struct TransactionRetryScheduler {
    pub sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    pub source: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
}

pub struct TransactionRetrySchedulerConfig {
    pub retry_rate: tokio::time::Duration,
    pub stop_send_on_commitment: CommitmentLevel,
    pub max_retry: usize,
    /// The maximum age of a transaction in blocks a transaction can stay in the scheduler pool.
    pub transaction_max_processing_age: u64,
}

impl Default for TransactionRetrySchedulerConfig {
    fn default() -> Self {
        Self {
            retry_rate: tokio::time::Duration::from_secs(1),
            stop_send_on_commitment: CommitmentLevel::Confirmed,
            max_retry: 3,
            transaction_max_processing_age: MAX_PROCESSING_AGE as u64,
        }
    }
}

///
/// Transaction scheduler that does not retry transactions.
/// It forwards transactions to the next leader if the transaction's last valid block height is less than the current block height.
///
pub struct TransactionNoRetryScheduler {
    pub sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    pub source: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
}

impl TransactionNoRetryScheduler {
    pub fn new(blockheight_service: Arc<dyn BlockHeightService + Send + Sync + 'static>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (scheduler_resp_tx, scheduler_resp_rx) =
            mpsc::unbounded_channel::<Arc<SendTransactionRequest>>();

        tokio::spawn(
            async move { Self::fwd_loop(blockheight_service, rx, scheduler_resp_tx).await },
        );
        Self {
            sink: tx,
            source: scheduler_resp_rx,
        }
    }

    async fn fwd_loop(
        blockheight_service: Arc<dyn BlockHeightService + Send + Sync + 'static>,
        mut incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
        response_sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    ) {
        loop {
            let Some(tx) = incoming_transaction_rx.recv().await else {
                tracing::trace!("incoming transaction channel is closed");
                break;
            };
            // Make sure to not double count this metric elsewhere.
            metrics::sts_received_inc();
            let current_block_height = blockheight_service
                .get_block_height_for_commitment(CommitmentLevel::Confirmed)
                .unwrap_or(0);
            let last_valid_block_height = blockheight_service
                .get_block_height(tx.transaction.message.recent_blockhash())
                .unwrap_or(0)
                + MAX_PROCESSING_AGE as u64;

            if last_valid_block_height < current_block_height {
                tracing::trace!(
                    "transaction {} last valid block height {} is less than current block height {}, dropping transaction",
                    tx.signature,
                    last_valid_block_height,
                    current_block_height
                );
                continue;
            }

            let signature = tx.signature;
            if response_sink.send(tx).is_err() {
                tracing::trace!("response sink is closed, stopping transaction forwarding");
                break;
            }

            tracing::trace!("forwarding transaction {signature}");
        }
    }
}

impl TransactionRetryScheduler {
    pub fn new(
        config: TransactionRetrySchedulerConfig,
        block_height_service: Arc<dyn BlockHeightService + Send + Sync + 'static>,
        rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
        dlq: Option<mpsc::UnboundedSender<TransactionRetrySchedulerDlqEvent>>,
    ) -> Self {
        Self::new_on(
            config,
            block_height_service,
            rooted_transactions,
            dlq,
            tokio::runtime::Handle::current(),
        )
    }

    pub fn new_on(
        config: TransactionRetrySchedulerConfig,
        block_height_service: Arc<dyn BlockHeightService + Send + Sync + 'static>,
        rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
        dlq: Option<mpsc::UnboundedSender<TransactionRetrySchedulerDlqEvent>>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (scheduler_resp_tx, scheduler_resp_rx) =
            mpsc::unbounded_channel::<Arc<SendTransactionRequest>>();
        let mut scheduler_runtime = TransactionRetrySchedulerRuntime {
            block_height_service,
            rooted_transactions,
            last_known_block_height: 0,
            tx_pool: Default::default(),
            block_height_deadline_map: Default::default(),
            insertion_order: Default::default(),
            transaction_source: rx,
            retry_rate: config.retry_rate,
            stop_send_on_commitment: config.stop_send_on_commitment,
            max_retry: config.max_retry,
            response_sink: scheduler_resp_tx,
            max_processing_age: config.transaction_max_processing_age,
            dlq,
        };
        // Automatically close the runtime when all reference to `tx` are dropped.
        runtime.spawn(async move {
            scheduler_runtime.run().await;
        });
        Self {
            sink: tx,
            source: scheduler_resp_rx,
        }
    }
}

///
/// Foward transactions to N validators.
///
/// Applies transaction's shield policies configuration.
///
/// Prevent duplicate transaction being inflight at the same time.
///
pub struct TransactionFanout {
    leader_schedule_service: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
    policy_store_service: Arc<dyn TransactionPolicyStore + Send + Sync + 'static>,
    tx_gateway_sender: mpsc::Sender<GatewayTransaction>,
    gateway_response_rx: mpsc::UnboundedReceiver<GatewayResponse>,
    incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
    leader_fwd_count: usize,
    transaction_send_set: JoinSet<Result<Signature, SendTransactionError>>,
    transaction_send_set_meta: HashMap<task::Id, Signature>,
    inflight_transactions: HashSet<Signature>,
}

#[derive(Debug, thiserror::Error)]
enum SendTransactionError {
    #[error("transaction gateway sink is closed")]
    GatewayClosed,
    #[error(transparent)]
    ShieldPoliciesNotFound(#[from] CheckError),
}

pub trait TransactionPolicyStore {
    fn is_allowed(&self, policies: &[Pubkey], leader: &Pubkey) -> Result<bool, CheckError>;
}

impl<T: PolicyStoreTrait> TransactionPolicyStore for T {
    fn is_allowed(&self, policies: &[Pubkey], leader: &Pubkey) -> Result<bool, CheckError> {
        self.snapshot().is_allowed(policies, leader)
    }
}

pub struct AlwaysAllowTransactionPolicyStore;

impl TransactionPolicyStore for AlwaysAllowTransactionPolicyStore {
    fn is_allowed(&self, _policies: &[Pubkey], _leader: &Pubkey) -> Result<bool, CheckError> {
        Ok(true)
    }
}

pub struct TransactionSchedulerBidi {
    pub scheduler_tx: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    pub scheduler_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
}

pub struct QuicGatewayBidi {
    pub sink: mpsc::Sender<GatewayTransaction>,
    pub source: mpsc::UnboundedReceiver<GatewayResponse>,
}

// The following example illustrates the transaction fanout architecture up to 3 remote validators.
//
//  ┌─────────────┐         ┌─────────────┐      ┌─────────────┐         ┌────────────┐
//  │  Transaction│         │  Transaction│      │ Transaction ┼───3.1──►│    QUIC    │
//  │   Source    ┼───1────►│  Scheduler  ├──2───►   Fanout    ├───3.2──►│  Gateway   │
//  │             │         │             │      │             ├───3.3──►│            │
//  └─────────────┘         └─────────────┘      └──────▲──────┘         └─────┬──────┘
//                                                      │                      │
//                                                      │                      4
//                                                      └────────(feedback)────┘
//  1. Transaction Source sends a transaction to the Scheduler.
//  2. Transaction Scheduler select a transaction and sends it to the fanout.
//  3. Transaction Fanout forwards the transaction to the next (N) validators:
//  4. Quic Gateway sends back transaction status
//
// Tranasction fanout should stay relatively "dumb":
//  1. No scheduling decisions.
//  2. No transaction retry logic.
//  3. No transaction validation.
//
// We do however apply transaction's shield policies configuration + prevent duplicate transaction being inflight at the same time.
//
// It should just forward transactions to the next (N) validators and wait for the response from the QUIC gateway.
//
// Custom retry logic is implemented in the "transaction scheduler" which is hidden behind a tokio channel giving us free polymorphism.
//
impl TransactionFanout {
    pub fn new(
        leader_schedule_service: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
        policy_store_service: Arc<dyn TransactionPolicyStore + Send + Sync + 'static>,
        incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
        quic_gateway_bidi: QuicGatewayBidi,
        leader_fwd_count: usize,
    ) -> Self {
        Self {
            leader_schedule_service,
            policy_store_service,
            tx_gateway_sender: quic_gateway_bidi.sink,
            gateway_response_rx: quic_gateway_bidi.source,
            incoming_transaction_rx,
            leader_fwd_count,
            transaction_send_set: JoinSet::new(),
            transaction_send_set_meta: HashMap::new(),
            inflight_transactions: HashSet::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                maybe = self.incoming_transaction_rx.recv() => {
                    match maybe {
                        Some(newtx) => self.fwd_tx(newtx),
                        None => {
                            error!("new transactions channel is closed");
                            break;
                        }
                    }
                }
                maybe = self.gateway_response_rx.recv() => {
                    match maybe {
                        Some(response) => {
                            self.handle_gateway_response(response);
                        }
                        None => {
                            error!("gateway response channel is closed");
                            break;
                        }
                    }
                }
                Some(result) = self.transaction_send_set.join_next_with_id() => {
                    let (task_id, result) = result.expect("task join failed");
                    self.handle_transaction_sent_result(task_id, result);
                }
            }
        }
    }

    fn handle_gateway_response(&mut self, response: GatewayResponse) {
        match response {
            GatewayResponse::TxSent(gateway_tx_sent) => {
                let tx_sig = gateway_tx_sent.tx_sig;
                // BECAREFUL: THE SAME TRANSACTION CAN BE SENT TO MULTIPLE LEADERS,
                // SO REMOVE MAY RETURN FALSE.
                self.inflight_transactions.remove(&tx_sig);
                tracing::trace!(
                    "transaction {tx_sig} forwarded to {} validator",
                    gateway_tx_sent.remote_peer_identity
                );
            }
            GatewayResponse::TxFailed(gateway_tx_failed) => {
                let tx_sig = gateway_tx_failed.tx_sig;
                tracing::trace!("transaction {tx_sig} failed");
                self.inflight_transactions.remove(&tx_sig);
            }
            GatewayResponse::TxDrop(tx_drop) => {
                let tx_sig = tx_drop.tx_sig;
                tracing::trace!("transaction {tx_sig} dropped by QUIC gateway");
                self.inflight_transactions.remove(&tx_sig);
            }
        }
        metrics::sts_inflight_set_size(self.inflight_transactions.len());
    }

    fn handle_transaction_sent_result(
        &mut self,
        task_id: task::Id,
        result: Result<Signature, SendTransactionError>,
    ) {
        let signature = self
            .transaction_send_set_meta
            .remove(&task_id)
            .expect("unknown task id");
        match result {
            Ok(signature2) => {
                assert!(signature == signature2, "task id mismatch");
                tracing::trace!("transaction {signature} sent to QUIC gateway");
            }
            Err(SendTransactionError::GatewayClosed) => {
                tracing::error!("gateway sender is closed");
            }
            Err(SendTransactionError::ShieldPoliciesNotFound(_)) => {
                metrics::shield_policies_not_found_inc();
            }
        }
    }

    fn fwd_tx(&mut self, tx: Arc<SendTransactionRequest>) {
        if self.inflight_transactions.contains(&tx.signature) {
            tracing::trace!(
                "transaction {} is already in flight, skipping",
                tx.signature
            );
            return;
        }
        self.inflight_transactions.insert(tx.signature);
        let leader_schedule_service = Arc::clone(&self.leader_schedule_service);
        let policy_store_service = Arc::clone(&self.policy_store_service);
        let leader_fwd = self.leader_fwd_count;
        let gateway_sink = self.tx_gateway_sender.clone();
        let signature = tx.signature;
        let send_fut = async move {
            let next_leaders = leader_schedule_service.leader_lookahead(leader_fwd);

            for dest in next_leaders {
                if !policy_store_service.is_allowed(&tx.policies, &dest)? {
                    metrics::sts_tpu_denied_inc_by(1);
                    tracing::trace!("transaction {signature} is not allowed to be sent to {dest}");
                    continue;
                }
                let gateway_tx = GatewayTransaction {
                    tx_sig: tx.signature,
                    wire: tx.wire_transaction.clone(),
                    remote_peer: dest,
                };
                gateway_sink
                    .send(gateway_tx)
                    .await
                    .map_err(|_| SendTransactionError::GatewayClosed)?;
            }
            Ok(tx.signature)
        };

        let ah = self.transaction_send_set.spawn(send_fut);
        self.transaction_send_set_meta.insert(ah.id(), signature);
    }
}

pub const fn module_path_for_test() -> &'static str {
    module_path!()
}

pub mod testkit {
    use {
        super::RootedTxReceiver,
        crate::util::CommitmentLevel,
        solana_signature::Signature,
        std::{
            collections::HashMap,
            sync::{Arc, RwLock as StdRwLock},
        },
        tokio::sync::mpsc::{self, UnboundedReceiver},
    };

    #[derive(Default)]
    struct MockRootedTxChannelShared {
        transactions: HashMap<Signature, CommitmentLevel>,
    }

    pub struct MockRootedTransactionsTx {
        shared: Arc<StdRwLock<MockRootedTxChannelShared>>,
        tx: mpsc::UnboundedSender<(Signature, CommitmentLevel)>,
    }

    impl MockRootedTransactionsTx {
        pub async fn send(&self, signature: Signature, commitment: CommitmentLevel) {
            self.shared
                .write()
                .expect("shared lock poisoned")
                .transactions
                .insert(signature, Default::default());
            self.tx.send((signature, commitment)).unwrap();
        }
    }

    pub struct MockRootedTransactionsRx {
        shared: Arc<StdRwLock<MockRootedTxChannelShared>>,
        rx: UnboundedReceiver<(Signature, CommitmentLevel)>,
    }

    pub fn mock_rooted_tx_channel() -> (MockRootedTransactionsTx, MockRootedTransactionsRx) {
        let shared = Default::default();
        let (tx, rx) = mpsc::unbounded_channel();
        let rooted_rx = MockRootedTransactionsRx {
            shared: Arc::clone(&shared),
            rx,
        };
        let rooted_tx = MockRootedTransactionsTx { shared, tx };
        (rooted_tx, rooted_rx)
    }

    #[async_trait::async_trait]
    impl RootedTxReceiver for MockRootedTransactionsRx {
        fn subscribe_signature(&mut self, _signature: Signature) {}

        fn unsubscribe_signature(&mut self, _signature: Signature) {}

        fn get_transaction_commitment(&mut self, signature: Signature) -> Option<CommitmentLevel> {
            let locked = self.shared.read().expect("shared lock poisoned");
            locked.transactions.get(&signature).copied()
        }

        async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)> {
            self.rx.recv().await
        }
    }
}
