use {
    crate::{
        blockhash_queue::BlockHeightService,
        cluster_tpu_info::ClusterTpuInfo,
        grpc_geyser::GrpcUpdateMessage,
        grpc_lewis::LewisEventHandler,
        metrics::jet as metrics,
        rooted_transaction_state::{RootedTxEffect, RootedTxEvent, RootedTxStateMachine},
        util::CommitmentLevel,
    },
    bytes::Bytes,
    solana_clock::{MAX_PROCESSING_AGE, Slot},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        future::Future,
        sync::{Arc, RwLock as StdRwLock},
    },
    tokio::{
        sync::mpsc::{self},
        task::{self, JoinSet},
        time::Instant,
    },
    tracing::error,
    yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxn},
    yellowstone_shield_store::{CheckError, PolicyStoreTrait},
};

pub type RootedTransactionsUpdateSignature = (Signature, CommitmentLevel);

///
/// Trait for getting the upcoming leader schedule
///
pub trait UpcomingLeaderSchedule {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey>;
    fn get_current_slot(&self) -> Slot;
}

impl UpcomingLeaderSchedule for ClusterTpuInfo {
    fn leader_lookahead(&self, leader_forward_lookahead: usize) -> Vec<Pubkey> {
        self.get_leader_tpus(leader_forward_lookahead)
            .into_iter()
            .map(|tpu| tpu.leader)
            .collect()
    }

    fn get_current_slot(&self) -> Slot {
        self.latest_seen_slot()
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

///
/// Processes transaction commitment updates from gRPC stream.
///
/// Uses a state machine to track transaction locations and commitment levels,
/// notifying subscribers when transactions reach new commitment levels.
/// The state machine is thread-safe (Arc<RwLock>) as trait methods can be
/// called from different threads than the processing loop.
///
#[derive(Debug)]
pub struct GrpcRootedTxReceiver {
    state_machine: Arc<StdRwLock<RootedTxStateMachine>>,
    rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
}

#[async_trait::async_trait]
impl RootedTxReceiver for GrpcRootedTxReceiver {
    fn get_transaction_commitment(&mut self, signature: Signature) -> Option<CommitmentLevel> {
        let state_machine = self.state_machine.read().expect("RwLock poisoned");
        state_machine
            .state
            .transactions
            .get(&signature)
            .and_then(|slot| state_machine.state.slots.get(slot))
            .and_then(|info| info.blockmeta)
            .map(|meta| meta.commitment)
    }

    fn subscribe_signature(&mut self, signature: Signature) {
        let mut state_machine = self.state_machine.write().expect("RwLock poisoned");
        state_machine.state.watched_signatures.insert(signature);
    }

    fn unsubscribe_signature(&mut self, signature: Signature) {
        let mut state_machine = self.state_machine.write().expect("RwLock poisoned");
        state_machine.state.watched_signatures.remove(&signature);
    }

    async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)> {
        self.rx.recv().await
    }
}

impl GrpcRootedTxReceiver {
    pub fn new(grpc_rx: mpsc::Receiver<GrpcUpdateMessage>) -> (Self, impl Future) {
        let state_machine = Arc::new(StdRwLock::new(RootedTxStateMachine::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        let loop_fut = Self::start_loop(grpc_rx, Arc::clone(&state_machine), tx);
        let this = Self { state_machine, rx };
        (this, loop_fut)
    }

    async fn start_loop(
        mut grpc_rx: mpsc::Receiver<GrpcUpdateMessage>,
        state_machine: Arc<StdRwLock<RootedTxStateMachine>>,
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
                    None => break,
                }
            };

            // Process events with the state machine locked
            let effects = {
                let mut state_machine = state_machine.write().expect("RwLock poisoned");

                // Process buffered transactions first
                let mut all_effects = Vec::new();
                for tx in transactions_bulk.drain(..) {
                    all_effects.extend(
                        state_machine.process_event(RootedTxEvent::TransactionReceived(tx)),
                    );
                }

                // Process block meta update
                all_effects.extend(
                    state_machine.process_event(RootedTxEvent::BlockMetaUpdate(blockmeta_update)),
                );

                all_effects
            }; // Lock is released here

            // Send notifications (outside the lock)
            for effect in effects {
                let RootedTxEffect::NotifyWatcher {
                    signature,
                    commitment,
                } = effect;
                if signature_updates_tx.send((signature, commitment)).is_err() {
                    return;
                }
            }

            // Update metrics
            let transaction_count = state_machine
                .read()
                .expect("RwLock poisoned")
                .state
                .transactions
                .len();
            metrics::rooted_transactions_pool_set_size(transaction_count);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SendTransactionRequest {
    pub signature: Signature,
    pub transaction: VersionedTransaction,
    pub wire_transaction: Vec<u8>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FanoutConfig {
    #[deprecated(note = "use SmartFanout instead")]
    Custom(usize),
    #[default]
    SmartFanout,
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
    tpu_sender: mpsc::Sender<TpuSenderTxn>,
    gateway_response_rx: mpsc::UnboundedReceiver<TpuSenderResponse>,
    incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
    transaction_send_set: JoinSet<Result<Signature, SendTransactionError>>,
    transaction_send_set_meta: HashMap<task::Id, Signature>,
    inflight_transactions: HashSet<Signature>,
    fanout_config: FanoutConfig,
    lewis_handler: Option<Arc<LewisEventHandler>>,
    extra_fwd: Arc<[Pubkey]>,
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
    pub sink: mpsc::Sender<TpuSenderTxn>,
    pub source: mpsc::UnboundedReceiver<TpuSenderResponse>,
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
        // Extra remote peer to forward too
        fanout_config: FanoutConfig,
        extra_fwd: Vec<Pubkey>,
        lewis_handler: Option<Arc<LewisEventHandler>>,
    ) -> Self {
        Self {
            leader_schedule_service,
            policy_store_service,
            tpu_sender: quic_gateway_bidi.sink,
            gateway_response_rx: quic_gateway_bidi.source,
            incoming_transaction_rx,
            transaction_send_set: JoinSet::new(),
            transaction_send_set_meta: HashMap::new(),
            inflight_transactions: HashSet::new(),
            lewis_handler,
            extra_fwd: extra_fwd.into(),
            fanout_config,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                maybe = self.incoming_transaction_rx.recv() => {
                    match maybe {
                        Some(newtx) => self.fwd_tx(newtx),
                        None => {
                            tracing::warn!("transactions channel is closed");
                            break;
                        }
                    }
                }
                maybe = self.gateway_response_rx.recv() => {
                    match maybe {
                        Some(response) => {
                            self.handle_gateway_response(&response);
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

    fn handle_gateway_response(&mut self, response: &TpuSenderResponse) {
        // Forward to Lewis if handler is configured
        if let Some(handler) = &self.lewis_handler {
            let current_slot = self.leader_schedule_service.get_current_slot();
            handler.handle_gateway_response(response, current_slot);
        }
        match response {
            TpuSenderResponse::TxSent(gateway_tx_sent) => {
                let tx_sig = gateway_tx_sent.tx_sig;
                // BECAREFUL: THE SAME TRANSACTION CAN BE SENT TO MULTIPLE LEADERS,
                // SO REMOVE MAY RETURN FALSE.
                self.inflight_transactions.remove(&tx_sig);
                tracing::trace!(
                    "transaction {tx_sig} forwarded to {} validator",
                    gateway_tx_sent.remote_peer_identity
                );
            }
            TpuSenderResponse::TxFailed(gateway_tx_failed) => {
                let tx_sig = gateway_tx_failed.tx_sig;
                tracing::trace!("transaction {tx_sig} failed");
                self.inflight_transactions.remove(&tx_sig);
            }
            TpuSenderResponse::TxDrop(tx_drop) => {
                for (gw_tx, _curr_attempt) in &tx_drop.dropped_tx_vec {
                    let tx_sig = gw_tx.tx_sig;
                    tracing::trace!("transaction {tx_sig} dropped by QUIC gateway");
                    self.inflight_transactions.remove(&tx_sig);
                }
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
        let tx = Arc::unwrap_or_clone(tx);
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
        let tpu_sink = self.tpu_sender.clone();
        let signature = tx.signature;
        let lewis_handler = self.lewis_handler.clone();
        let extra_fwd = Arc::clone(&self.extra_fwd);
        let fanout_config = self.fanout_config;
        let send_fut = async move {
            let current_slot = leader_schedule_service.get_current_slot();
            #[allow(deprecated)]
            let fanout_count = match fanout_config {
                FanoutConfig::Custom(count) => count.max(1),
                FanoutConfig::SmartFanout => {
                    // We only fanout when we reached half of the current leader window.
                    let reminder = current_slot % 4;
                    if reminder < 2 { 1 } else { 2 }
                }
            };
            let next_leaders = leader_schedule_service.leader_lookahead(fanout_count);
            let mut sent_mask = Vec::with_capacity(next_leaders.capacity());
            sent_mask.resize(next_leaders.len(), false);
            let txn_wire = Bytes::from_owner(tx.wire_transaction);
            for (i, dest) in next_leaders.iter().enumerate() {
                if !policy_store_service.is_allowed(&tx.policies, dest)? {
                    // Report skip to Lewis
                    if let Some(handler) = &lewis_handler {
                        handler.handle_skip(tx.signature, *dest, current_slot, &tx.policies);
                    }
                    metrics::sts_tpu_denied_inc_by(1);
                    tracing::trace!("transaction {signature} is not allowed to be sent to {dest}");
                    continue;
                }
                sent_mask[i] = true;
                let tpu_txn = TpuSenderTxn::from_bytes(tx.signature, *dest, txn_wire.clone());
                tpu_sink
                    .send(tpu_txn)
                    .await
                    .map_err(|_| SendTransactionError::GatewayClosed)?;
            }

            for extra in extra_fwd.iter() {
                let already_sent = next_leaders
                    .iter()
                    .zip(sent_mask.iter())
                    .any(|(leader, &sent)| sent && (leader == extra));

                if already_sent {
                    // We don't need to send again to this extra peer
                    continue;
                }

                let tpu_txn = TpuSenderTxn::from_bytes(tx.signature, *extra, txn_wire.clone());

                tpu_sink
                    .send(tpu_txn)
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
