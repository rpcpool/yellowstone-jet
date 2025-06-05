use {
    crate::{
        blockhash_queue::BlockHeighService,
        cluster_tpu_info::{ClusterTpuInfo, TpuInfo},
        config::ConfigSendTransactionService,
        grpc_geyser::{GrpcUpdateMessage, SlotUpdateInfoWithCommitment, TransactionReceived},
        metrics::jet as metrics,
        quic_gateway::{GatewayResponse, GatewayTransaction},
        util::CommitmentLevel,
    },
    bytes::Bytes,
    solana_sdk::{
        clock::{MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES, Slot},
        pubkey::Pubkey,
        signature::Signature,
        transaction::VersionedTransaction,
    },
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
    tracing::{debug, error},
    yellowstone_shield_store::{CheckError, PolicyStoreTrait},
};

#[derive(Debug, Default)]
pub struct RootedTransactionsSlotInfo {
    transactions: HashSet<Signature>,
    slot: Option<SlotUpdateInfoWithCommitment>,
}

pub type RootedTransactionsUpdateSignature = (Signature, CommitmentLevel);

///
/// Trait for getting the upcoming leader schedule
///
pub trait UpcomingLeaderSchedule {
    fn get_leader_tpus(&self, leader_forward_lookahead: usize) -> Vec<TpuInfo>;
}

impl UpcomingLeaderSchedule for ClusterTpuInfo {
    fn get_leader_tpus(&self, leader_forward_lookahead: usize) -> Vec<TpuInfo> {
        self.get_leader_tpus(leader_forward_lookahead)
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
            .and_then(|info| info.slot)
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
            let slot_update = tokio::select! {
                message = grpc_rx.recv() => match message {
                    Some(GrpcUpdateMessage::Transaction(transaction)) => {
                        transactions_bulk.push(transaction);
                        continue;
                    }
                    Some(GrpcUpdateMessage::Slot(slot)) => slot,
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
            let ts = Instant::now();

            // Update slot commitment
            let entry = slots.entry(slot_update.slot).or_default();
            entry.slot = Some(slot_update);
            for signature in entry.transactions.iter() {
                if watch_signatures.contains(signature)
                    && signature_updates_tx
                        .send((*signature, slot_update.commitment))
                        .is_err()
                {
                    // The loop shutdown when the receiver is closed
                    return;
                }
            }

            // Removed outdated slots
            if slot_update.commitment == CommitmentLevel::Finalized {
                let mut removed = Vec::new();
                slots.retain(|_slot, info| {
                    let retain = if let Some(slot) = info.slot {
                        if slot.commitment == CommitmentLevel::Finalized {
                            // Keep more blocks than MAX_PROCESSING_AGE
                            slot.block_height + MAX_RECENT_BLOCKHASHES as u64
                                > slot_update.block_height
                        } else {
                            slot.slot > slot_update.slot
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
            let bulk_size = transactions_bulk.len();
            for TransactionReceived { slot, signature } in transactions_bulk.drain(..) {
                let entry = slots.entry(slot).or_default();
                if entry.transactions.insert(signature) {
                    if let Some(slot) = &entry.slot {
                        if watch_signatures.contains(&signature)
                            && signature_updates_tx
                                .send((signature, slot.commitment))
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
            debug!(
                bulk_size,
                elapsed_ms = ts.elapsed().as_millis(),
                "rooted transactions updated"
            )
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

pub struct TransactionRetrier {
    block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
    rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
    incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
    sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
    config: ConfigSendTransactionService,
    last_known_block_height: u64,
    tx_buffer_map: HashMap<Signature, RetryableTx>,
    block_height_deadline_map: BTreeMap<u64, Vec<Signature>>,
    insertion_order: VecDeque<(Instant, Signature)>,
}

impl TransactionRetrier {
    pub fn new(
        block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
        rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
        incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
        sink: mpsc::UnboundedSender<Arc<SendTransactionRequest>>,
        config: ConfigSendTransactionService,
    ) -> Self {
        Self {
            block_height_service,
            rooted_transactions,
            incoming_transaction_rx,
            sink,
            config,
            last_known_block_height: 0,
            tx_buffer_map: HashMap::new(),
            block_height_deadline_map: BTreeMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(self.config.retry_rate);

        loop {
            if self.incoming_transaction_rx.is_closed() {
                break;
            }
            if self.sink.is_closed() {
                break;
            }

            tokio::select! {
                maybe = self.incoming_transaction_rx.recv() => {
                    match maybe {
                        Some(newtx) => self.add_new_transaction(newtx).await,
                        None => {
                            error!("new transactions channel is closed");
                            break;
                        }
                    }
                }
                now = interval.tick() => {
                    self.gc_deadline_map();
                    self.resend_non_landed_tx(now);
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
        }
    }

    fn resend_non_landed_tx(&mut self, now: Instant) {
        while let Some(inserted_at) = self.insertion_order.front().map(|(t, _)| t.clone()) {
            if now.duration_since(inserted_at) < self.config.retry_rate {
                break;
            }
            let (_, signature) = self.insertion_order.pop_front().unwrap();
            if let Some(rtx) = self.tx_buffer_map.get_mut(&signature) {
                if rtx.leftover_attempt > 0 {
                    rtx.leftover_attempt = rtx.leftover_attempt.saturating_sub(1);
                    let _ = self.sink.send(Arc::clone(&rtx.tx));
                    self.insertion_order.push_back((now, signature));
                } else {
                    self.tx_buffer_map.remove(&signature);
                    tracing::trace!("tx {signature} reached max attempts, dropping it");
                }
            }
        }
    }

    fn gc_deadline_map(&mut self) {
        let outdated_block_height = self
            .last_known_block_height
            .saturating_sub(MAX_PROCESSING_AGE as u64);
        let right = self
            .block_height_deadline_map
            .split_off(&outdated_block_height);

        let deprecrated_tx = std::mem::replace(&mut self.block_height_deadline_map, right);

        deprecrated_tx
            .into_iter()
            .flat_map(|(_, signatures)| signatures)
            .for_each(|signature| {
                self.tx_buffer_map.remove(&signature);
                self.rooted_transactions.unsubscribe_signature(signature);
            });
    }

    fn handle_tx_commitment_update(&mut self, signature: Signature, commitment: CommitmentLevel) {
        if commitment == self.config.stop_send_on_commitment {
            self.tx_buffer_map.remove(&signature);
        }
    }

    async fn add_new_transaction(&mut self, tx: Arc<SendTransactionRequest>) {
        let max_retries = tx
            .max_retries
            .or(self.config.default_max_retries)
            .unwrap_or(self.config.service_max_retries)
            .min(self.config.service_max_retries);

        let current_block_height = self
            .block_height_service
            .get_block_height_for_commitment(CommitmentLevel::Confirmed)
            .unwrap_or(0);
        self.last_known_block_height = current_block_height;
        let last_valid_block_height = self
            .block_height_service
            .get_block_height(tx.transaction.message.recent_blockhash())
            .map(|block_height| block_height + MAX_PROCESSING_AGE as u64)
            .unwrap_or(current_block_height + MAX_PROCESSING_AGE as u64);

        let signature = tx.signature;
        if last_valid_block_height < current_block_height {
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
            .filter(|cl| cl >= &self.config.stop_send_on_commitment)
            .is_some();

        if is_landed {
            tracing::debug!("skipping {signature}, transaction already landed");
            return;
        }

        self.rooted_transactions.subscribe_signature(signature);
        // -1 because we are going to send the transaction immediately
        let leftover_attempt = max_retries.saturating_sub(1);

        self.tx_buffer_map.insert(
            signature,
            RetryableTx {
                tx: Arc::clone(&tx),
                leftover_attempt,
            },
        );
        self.block_height_deadline_map
            .entry(last_valid_block_height)
            .or_default()
            .push(signature);

        self.sink
            .send(tx)
            .expect("transaction retrier sink is closed");
    }
}

///
/// Foward transactions to N valitx_sigs.
/// Applies transaction's shield policies configuration.
///
/// Prevent duplicate transaction being inflight at the same time.
///
pub struct TransactionFowarder {
    leader_schedule_service: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
    policy_store_service: Arc<dyn TransactionPolicyStore + Send + Sync + 'static>,
    block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
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

impl TransactionFowarder {
    pub fn new(
        leader_schedule_service: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
        policy_store_service: Arc<dyn TransactionPolicyStore + Send + Sync + 'static>,
        block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
        tx_gateway_sender: mpsc::Sender<GatewayTransaction>,
        gateway_response_rx: mpsc::UnboundedReceiver<GatewayResponse>,
        incoming_transaction_rx: mpsc::UnboundedReceiver<Arc<SendTransactionRequest>>,
        leader_fwd_count: usize,
    ) -> Self {
        Self {
            leader_schedule_service,
            policy_store_service,
            block_height_service,
            tx_gateway_sender,
            gateway_response_rx,
            incoming_transaction_rx,
            leader_fwd_count,
            transaction_send_set: JoinSet::new(),
            transaction_send_set_meta: HashMap::new(),
            inflight_transactions: HashSet::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            if self.tx_gateway_sender.is_closed() {
                error!("gateway sender is closed, stopping transaction pool");
                break;
            }
            tokio::select! {
                maybe = self.incoming_transaction_rx.recv() => {
                    match maybe {
                        Some(newtx) => self.add_new_transaction(newtx),
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
                assert!(self.inflight_transactions.remove(&tx_sig));
                tracing::trace!(
                    "transaction {tx_sig} forwarded to {} validator",
                    gateway_tx_sent.remote_peer_identity
                );
            }
            GatewayResponse::TxFailed(gateway_tx_failed) => {
                let tx_sig = gateway_tx_failed.tx_sig;
                assert!(self.inflight_transactions.remove(&tx_sig));
            }
            GatewayResponse::TxDrop(tx_drop) => {
                let tx_sig = tx_drop.tx_sig;
                assert!(self.inflight_transactions.remove(&tx_sig));
            }
        }
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

    fn add_new_transaction(&mut self, tx: Arc<SendTransactionRequest>) {
        let last_valid_block_height = self
            .block_height_service
            .get_block_height(tx.transaction.message.recent_blockhash())
            .map(|block_height| block_height + MAX_PROCESSING_AGE as u64)
            .unwrap_or(0);

        let current_block_height = self
            .block_height_service
            .get_block_height_for_commitment(CommitmentLevel::Confirmed)
            .unwrap_or(0);

        if last_valid_block_height < current_block_height {
            tracing::warn!(
                %tx.signature,
                last_valid_block_height,
                current_block_height,
                "transaction last valid block height is less than current block height, dropping transaction"
            );
            return;
        }

        let leader_schedule_service = Arc::clone(&self.leader_schedule_service);
        let policy_store_service = Arc::clone(&self.policy_store_service);
        let leader_fwd = self.leader_fwd_count;
        let gateway_sink = self.tx_gateway_sender.clone();
        let signature = tx.signature;
        let send_fut = async move {
            let next_leaders = leader_schedule_service.get_leader_tpus(leader_fwd);

            for dest in next_leaders {
                if !policy_store_service.is_allowed(&tx.policies, &dest.leader)? {
                    continue;
                }
                let gateway_tx = GatewayTransaction {
                    tx_sig: tx.signature,
                    wire: tx.wire_transaction.clone(),
                    remote_peer: dest.leader,
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

pub mod testkit {
    use {
        super::RootedTxReceiver,
        crate::util::CommitmentLevel,
        solana_sdk::signature::Signature,
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
