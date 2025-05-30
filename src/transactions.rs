use {
    crate::{
        blockhash_queue::BlockHeighService,
        config::ConfigSendTransactionService,
        grpc_geyser::{GrpcUpdateMessage, SlotUpdateInfoWithCommitment, TransactionReceived},
        metrics::jet as metrics,
        quic::{QuicClient, QuicSendTxPermit},
        quic_solana::IdentityFlusher,
        solana::get_durable_nonce,
        util::{BlockHeight, CommitmentLevel},
    },
    futures::future::BoxFuture,
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
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    },
    tokio::{
        sync::{
            RwLock, broadcast,
            mpsc::{self},
            oneshot,
        },
        task::{self, JoinSet},
        time::Instant,
    },
    tracing::{debug, error, info, instrument},
};

#[derive(Debug, Default)]
pub struct RootedTransactionsSlotInfo {
    transactions: HashSet<Signature>,
    slot: Option<SlotUpdateInfoWithCommitment>,
}

pub type RootedTransactionsUpdateSignature = (Signature, CommitmentLevel);

///
/// Base trait for Rooted transaction update notifier
///
#[async_trait::async_trait]
pub trait RootedTxReceiver {
    async fn subscribe_signature(&mut self, signature: Signature);
    async fn unsubscribe_signature(&mut self, signature: Signature);
    async fn get_transaction_commitment(&mut self, signature: Signature)
    -> Option<CommitmentLevel>;
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
    inner: Arc<RwLock<RootedTransactionsInner>>,
    rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
}

#[async_trait::async_trait]
impl RootedTxReceiver for GrpcRootedTxReceiver {
    async fn get_transaction_commitment(
        &mut self,
        signature: Signature,
    ) -> Option<CommitmentLevel> {
        let locked = self.inner.read().await;
        locked
            .transactions
            .get(&signature)
            .and_then(|slot| locked.slots.get(slot))
            .and_then(|info| info.slot)
            .map(|info| info.commitment)
    }

    async fn subscribe_signature(&mut self, signature: Signature) {
        let mut locked = self.inner.write().await;
        locked.watch_signatures.insert(signature);
    }

    async fn unsubscribe_signature(&mut self, signature: Signature) {
        let mut locked = self.inner.write().await;
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
        inner: Arc<RwLock<RootedTransactionsInner>>,
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
            let mut locked = inner.write().await;
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
    pub wire_transaction: Vec<u8>,
    pub max_retries: Option<usize>,
    pub policies: Vec<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct SendTransactionsPool {
    new_transactions_tx: mpsc::UnboundedSender<SendTransactionRequest>,
    cnc_tx: mpsc::Sender<SendTransactionPoolCommand>,
    dead_letter_queue: broadcast::Sender<Signature>,
    finalized_tx_notifier: broadcast::Sender<Signature>,
}

impl SendTransactionsPool {
    pub async fn spawn(
        config: ConfigSendTransactionService,
        block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
        rooted_transactions_rx: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
        tx_sender: Arc<dyn TxChannel + Send + Sync + 'static>,
    ) -> (Self, impl Future<Output = ()>) {
        let (new_transactions_tx, new_transactions_rx) = mpsc::unbounded_channel();

        let (deadletter_tx, _) = broadcast::channel(1000);
        let (finalized_tx_notifier, _) = broadcast::channel(1000);
        let task = SendTransactionsPoolTask {
            config,
            block_height_service,
            rooted_transactions: rooted_transactions_rx,
            tx_channel: tx_sender,
            new_transactions_rx,
            transactions: HashMap::new(),
            retry_schedule: BTreeMap::new(),
            connecting_map: Default::default(),
            connecting_tasks: JoinSet::new(),
            send_map: Default::default(),
            send_tasks: JoinSet::new(),
            dead_letter_queue: deadletter_tx.clone(),
            finalized_tx_notifier: finalized_tx_notifier.clone(),
        };

        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let frontend = Self {
            new_transactions_tx,
            cnc_tx,
            dead_letter_queue: deadletter_tx,
            finalized_tx_notifier,
        };

        // The backend task will end after all reference to the frontend task are dropped
        let backend = task.run(cnc_rx);
        (frontend, backend)
    }

    pub fn send_transaction(&self, request: SendTransactionRequest) -> anyhow::Result<()> {
        debug!(
            "Sending transaction with signature {} and policies: {:?}",
            request.signature, request.policies
        );
        anyhow::ensure!(
            self.new_transactions_tx.send(request).is_ok(),
            "send service task finished"
        );
        Ok(())
    }

    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.cnc_tx
            .send(SendTransactionPoolCommand::Flush { callback: tx })
            .await
            .expect("failed to send flush command");
        rx.await.expect("failed to receive flush command result");
    }

    pub fn subscribe_dead_letter(&self) -> broadcast::Receiver<Signature> {
        self.dead_letter_queue.subscribe()
    }

    pub fn subscribe_to_finalized_tx(&self) -> broadcast::Receiver<Signature> {
        self.finalized_tx_notifier.subscribe()
    }
}

#[async_trait::async_trait]
impl IdentityFlusher for SendTransactionsPool {
    async fn flush(&self) {
        SendTransactionsPool::flush(self).await;
    }
}

pub type SendTransactionInfoId = usize;

#[derive(Debug)]
struct SendTransactionInfo {
    id: SendTransactionInfoId,
    signature: Signature,
    wire_transaction: Arc<Vec<u8>>,
    total_retries: usize,
    max_retries: usize,
    last_valid_block_height: BlockHeight,
    landed: bool,
    policies: Vec<Pubkey>,
}

impl SendTransactionInfo {
    fn next_id() -> SendTransactionInfoId {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct SendTransactionTaskResult {
    id: SendTransactionInfoId,
    signature: Signature,
    retry_timestamp: Instant,
}

pub enum SendTransactionPoolCommand {
    Flush { callback: oneshot::Sender<()> },
}

#[async_trait::async_trait]
pub trait TxChannelPermit {
    async fn send_transaction(
        self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) -> ();
}

type BoxedInnerTxChannelPermit = Box<
    dyn FnOnce(SendTransactionInfoId, Signature, Arc<Vec<u8>>) -> BoxFuture<'static, ()> + Send,
>;

pub struct BoxedTxChannelPermit {
    inner: BoxedInnerTxChannelPermit,
}

impl BoxedTxChannelPermit {
    pub fn new<T>(pointee: T) -> Self
    where
        T: TxChannelPermit + Send + 'static,
    {
        Self {
            inner: Box::new(move |id, sig, wire_tx| pointee.send_transaction(id, sig, wire_tx)),
        }
    }
}

#[async_trait::async_trait]
impl TxChannelPermit for BoxedTxChannelPermit {
    async fn send_transaction(
        self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        (self.inner)(id, signature, wire_transaction).await;
    }
}

///
/// Back-pressured transaction channel
///
#[async_trait::async_trait]
pub trait TxChannel {
    ///
    /// Reserve a permit to send a transaction
    ///
    async fn reserve(
        &self,
        leader_foward_count: usize,
        policies: Vec<Pubkey>,
    ) -> Option<BoxedTxChannelPermit>;
}

#[async_trait::async_trait]
impl TxChannelPermit for QuicSendTxPermit {
    async fn send_transaction(
        self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        QuicSendTxPermit::send_transaction(self, id, signature, wire_transaction).await;
    }
}

#[async_trait::async_trait]
impl TxChannel for QuicClient {
    async fn reserve(
        &self,
        leader_foward_count: usize,
        policies: Vec<Pubkey>,
    ) -> Option<BoxedTxChannelPermit> {
        debug!(
            "QuicClient::reserve called with forwarding policies: {:?}",
            policies
        );
        QuicClient::reserve_send_permit(self, leader_foward_count, policies)
            .await
            .map(BoxedTxChannelPermit::new)
    }
}

pub struct TransactionInfo {
    pub id: SendTransactionInfoId,
    pub signature: Signature,
    pub wire_transaction: Arc<Vec<u8>>,
    pub policies: Vec<Pubkey>,
}

pub struct SendTransactionsPoolTask {
    config: ConfigSendTransactionService,
    block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
    rooted_transactions: Box<dyn RootedTxReceiver + Send + Sync + 'static>,
    tx_channel: Arc<dyn TxChannel + Send + Sync + 'static>,

    /// Connecting map holds pending connection request with the underlying tx sender.
    /// We split connection handling from transaction sending logic for flushing purposes.
    connecting_map: HashMap<task::Id, TransactionInfo>,
    /// Connecting task are stored in its seperated joinset.
    connecting_tasks: JoinSet<Option<BoxedTxChannelPermit>>,

    /// Send map holds pending send request with the underlying tx sender.
    /// Pending sends are transaction that acquired a TxSenderPermit prior to be send.
    send_map: HashMap<task::Id, (SendTransactionInfoId, Signature)>,
    /// Send task are stored in its seperated joinset.
    send_tasks: JoinSet<SendTransactionTaskResult>,

    new_transactions_rx: mpsc::UnboundedReceiver<SendTransactionRequest>,

    transactions: HashMap<Signature, SendTransactionInfo>,
    retry_schedule: BTreeMap<Instant, VecDeque<Signature>>,
    dead_letter_queue: broadcast::Sender<Signature>,

    ///
    /// Notifies when a managed transaction in the pool has been detected finalized.
    ///
    finalized_tx_notifier: broadcast::Sender<Signature>,
}

impl SendTransactionsPoolTask {
    pub async fn run(
        mut self,
        mut cnc_rx: mpsc::Receiver<SendTransactionPoolCommand>, // command-and-control channel
    ) {
        // let mut retry_interval = interval(Duration::from_millis(10));
        loop {
            metrics::sts_pool_set_size(self.transactions.len());
            metrics::sts_inflight_set_size(self.send_tasks.len());

            let next_retry_deadline = self
                .next_retry_deadline()
                .unwrap_or(Instant::now() + self.config.retry_rate);
            tokio::select! {
                maybe = cnc_rx.recv() => {
                    match maybe {
                        Some(SendTransactionPoolCommand::Flush { callback }) => {
                            self.flush_transactions().await;
                            let _ = callback.send(());
                        },
                        None => break,
                    }
                },
                _ = tokio::time::sleep_until(next_retry_deadline) => {
                    self.retry_next_in_schedule().await;
                }

                maybe = self.new_transactions_rx.recv(), if next_retry_deadline.elapsed() == Duration::ZERO => {
                    match maybe {
                        Some(newtx) => self.add_new_transaction(newtx).await,
                        None => {
                            error!("new transactions channel is closed");
                            break;
                        }
                    }
                }
                Some(result) = self.connecting_tasks.join_next_with_id() => {
                    self.handle_connecting_result(result).await;
                }
                Some(result) = self.send_tasks.join_next_with_id() => {
                    self.handle_send_result(result).await;
                }
                maybe_signature_update = self.rooted_transactions.recv() => {
                    match maybe_signature_update {
                        Some((signature, commitment)) => self.update_signature(signature, commitment).await,
                        None => {
                            error!("signature updates channel is closed");
                            break;
                        }
                    }
                },
            }
        }
    }

    async fn handle_connecting_result(
        &mut self,
        result: Result<(task::Id, Option<BoxedTxChannelPermit>), task::JoinError>,
    ) {
        match result {
            Ok((task_id, maybe_permit)) => {
                let tx_info = self
                    .connecting_map
                    .remove(&task_id)
                    .expect("unknown task id");
                match maybe_permit {
                    Some(permit) => self.spawn_send(permit, tx_info),
                    None => {
                        error!("upcoming leaders unavailable");
                    }
                }
            }
            Err(e) => {
                let task_id = e.id();
                error!("connecting task panic with {:?}", e);
                let tx_info = self
                    .connecting_map
                    .remove(&task_id)
                    .expect("unknown task id");

                // We must use the same code when we handle a failed send result, see [`SendTransactionsPoolTask::handle_send_result`]
                if let Some(info) = self.transactions.get_mut(&tx_info.signature) {
                    if info.id == tx_info.id {
                        info.total_retries += 1;
                        self.schedule_transaction_retry(
                            tx_info.signature,
                            Instant::now() + self.config.retry_rate,
                        )
                        .await;
                    }
                }
            }
        }
    }

    fn spawn_send(&mut self, permit: BoxedTxChannelPermit, tx_info: TransactionInfo) {
        if !self.transactions.contains_key(&tx_info.signature) {
            // The transaction has been removed from the pool
            return;
        }
        let retry_rate = self.config.retry_rate;
        let abort_handle = self.send_tasks.spawn(async move {
            permit
                .send_transaction(tx_info.id, tx_info.signature, tx_info.wire_transaction)
                .await;
            SendTransactionTaskResult {
                id: tx_info.id,
                signature: tx_info.signature,
                retry_timestamp: Instant::now() + retry_rate,
            }
        });
        self.send_map
            .insert(abort_handle.id(), (tx_info.id, tx_info.signature));
    }

    async fn handle_send_result(
        &mut self,
        result: Result<(task::Id, SendTransactionTaskResult), task::JoinError>,
    ) {
        let send_tx_result = match result {
            Ok((task_id, result)) => {
                let _ = self.send_map.remove(&task_id).expect("unknown task id");
                result
            }
            Err(e) => {
                let task_id = e.id();
                let (tx_id, tx_sig) = self.send_map.remove(&task_id).expect("unknown task id");
                error!("send task for tx {tx_sig} panic with {e:?}");
                SendTransactionTaskResult {
                    retry_timestamp: Instant::now() + self.config.retry_rate,
                    id: tx_id,
                    signature: tx_sig,
                }
            }
        };

        if let Some(info) = self.transactions.get_mut(&send_tx_result.signature) {
            if info.id == send_tx_result.id {
                info.total_retries += 1;
                self.schedule_transaction_retry(
                    send_tx_result.signature,
                    send_tx_result.retry_timestamp,
                )
                .await;
            }
        }
    }

    ///
    /// Flush will :
    ///
    /// 1. interrupt any connection attempt prior to calling this method.
    /// 2. Wait for all pending send tasks to complete.
    /// 3. Reschedule the managed retry attempt of each send transaction.
    /// 4. Resechdule the connection attempt interrupt at step (1).
    ///
    async fn flush_transactions(&mut self) {
        self.connecting_tasks.abort_all();
        let connectiong_map = std::mem::take(&mut self.connecting_map);

        for tx_info in connectiong_map.into_values() {
            // This is not good practice, but this code work because how ConnectionCacheIdentity flusher is implemented.
            // When the connection cache identity manager starts the flushing process, it clears out all connections certificates
            // AND it prevents concurrent connection from happening while the flush is in process.
            // Doing spawn_connect will not block the current flush process and they will internally wait for
            // new connection to be available.
            self.spawn_connect(
                tx_info.id,
                tx_info.signature,
                tx_info.wire_transaction,
                tx_info.policies,
            );
        }

        while let Some(result) = self.send_tasks.join_next_with_id().await {
            self.handle_send_result(result).await;
        }
    }

    #[instrument(skip_all, fields(signature = %signature))]
    async fn add_new_transaction(
        &mut self,
        SendTransactionRequest {
            signature,
            transaction,
            wire_transaction,
            max_retries,
            policies,
        }: SendTransactionRequest,
    ) {
        if self.transactions.contains_key(&signature) {
            info!(%signature, "transaction already in the pool");
            return;
        }
        // Resolve the proper max retries
        let max_retries = max_retries
            .or(self.config.default_max_retries)
            .unwrap_or(self.config.service_max_retries)
            .min(self.config.service_max_retries);

        let mut last_valid_block_height = self
            .block_height_service
            .get_block_height(transaction.message.recent_blockhash())
            .await
            .map(|block_height| block_height + MAX_PROCESSING_AGE as u64)
            .unwrap_or(0);

        // check durable nonce
        // we do not have access to Bank and can not make proper nonce checks
        let durable_nonce_info = get_durable_nonce(&transaction);
        if durable_nonce_info.is_some() {
            last_valid_block_height = self
                .block_height_service
                .get_block_height_for_commitment(CommitmentLevel::Confirmed)
                .await
                .map(|block_height| block_height + MAX_PROCESSING_AGE as u64)
                .unwrap_or(0);
        }

        let tx_commitment = self
            .rooted_transactions
            .get_transaction_commitment(signature)
            .await;

        // If the transaction is finalized we
        if tx_commitment == Some(CommitmentLevel::Finalized) {
            info!(%signature, "new transaction already finalized");

            let _ = self.finalized_tx_notifier.send(signature);
            return;
        }

        metrics::sts_received_inc();
        let mut landed = false;
        if let Some(tx_commitment) = tx_commitment {
            landed = tx_commitment >= self.config.stop_send_on_commitment;
            metrics::sts_landed_inc();
        }

        let id = SendTransactionInfo::next_id();
        let wire_transaction = Arc::new(wire_transaction);
        let info = SendTransactionInfo {
            id,
            signature,
            wire_transaction: Arc::clone(&wire_transaction),
            total_retries: 0,
            max_retries,
            last_valid_block_height,
            landed,
            policies: policies.clone(),
        };
        let update_existed = self.transactions.insert(signature, info).is_some();
        if !update_existed {
            self.rooted_transactions
                .subscribe_signature(signature)
                .await;
            metrics::sts_pool_set_size(self.transactions.len());
        }
        info!(
            id,
            %signature,
            max_retries,
            last_valid_block_height,
            landed,
            durable_nonce = durable_nonce_info.is_some(),
            update_existed,
            "add transaction to the pool"
        );

        if landed {
            let retry_timestamp = Instant::now() + self.config.retry_rate;
            self.schedule_transaction_retry(signature, retry_timestamp)
                .await;
        } else {
            self.spawn_connect(id, signature, wire_transaction, policies);
        }
    }

    async fn remove_transaction(&mut self, signature: Signature, reason: &'static str) {
        let removed_tx = self.transactions.remove(&signature);
        self.rooted_transactions
            .unsubscribe_signature(signature)
            .await;
        if let Some(info) = removed_tx {
            info!(id = info.id, %signature, reason, "remove transaction");
            if !info.landed {
                let _ = self.dead_letter_queue.send(info.signature);
            }
        }
    }

    fn spawn_connect(
        &mut self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
        policies: Vec<Pubkey>,
    ) {
        debug!(
            "Spawning connect for tx {}, id {}, with policies: {:?}",
            signature, id, &policies
        );
        let leader_forward_count = self.config.leader_forward_count;
        let tx_channel = Arc::clone(&self.tx_channel);
        let policies_connecting = policies.clone();
        let abort_handle = self.connecting_tasks.spawn(async move {
            debug!(
                "Reserving tx {} with policies: {:?}",
                signature, &policies_connecting
            );
            tx_channel
                .reserve(leader_forward_count, policies_connecting)
                .await
        });
        self.connecting_map.insert(
            abort_handle.id(),
            TransactionInfo {
                id,
                signature,
                wire_transaction,
                policies,
            },
        );
    }

    async fn schedule_transaction_retry(&mut self, signature: Signature, retry_timestamp: Instant) {
        if !self.transactions.contains_key(&signature) {
            return;
        }
        let retry_required = if self.config.relay_only_mode {
            self.transactions
                .get(&signature)
                .map(|info| info.total_retries <= info.max_retries)
                .unwrap_or(true)
        } else {
            true
        };

        if retry_required {
            self.retry_schedule
                .entry(retry_timestamp)
                .or_default()
                .push_back(signature);
        } else {
            self.remove_transaction(signature, "max_retries reached")
                .await;
        }
    }

    ///
    /// Returns the next retry deadline if any.
    ///
    fn next_retry_deadline(&self) -> Option<Instant> {
        self.retry_schedule.keys().next().cloned()
    }

    ///
    /// Tries to retry the next transaction in the schedule
    ///
    /// If the transaction has expired due to block height, it will be removed and the permit will be returned.
    async fn retry_next_in_schedule(&mut self) {
        if let Some(mut entry) = self.retry_schedule.first_entry() {
            if let Some(signature) = entry.get_mut().pop_front() {
                if let Some(info) = self.transactions.get(&signature) {
                    let latest_block_height = self
                        .block_height_service
                        .get_block_height_for_commitment(CommitmentLevel::Confirmed)
                        .await;

                    if let Some(block_height) = latest_block_height {
                        if info.last_valid_block_height < block_height {
                            self.remove_transaction(signature, "block_height expired")
                                .await;
                            return;
                        }
                    }

                    if info.total_retries <= info.max_retries {
                        self.spawn_connect(
                            info.id,
                            info.signature,
                            Arc::clone(&info.wire_transaction),
                            info.policies.clone(),
                        );
                    } else {
                        let retry_timestamp = Instant::now() + self.config.retry_rate;
                        self.schedule_transaction_retry(signature, retry_timestamp)
                            .await;
                    }
                }
            } else {
                // If the queue is empty, remove the key
                self.retry_schedule.pop_first();
            }
        }
    }

    #[instrument(skip_all, fields(signature = %signature, commitment = ?commitment))]
    async fn update_signature(&mut self, signature: Signature, commitment: CommitmentLevel) {
        if commitment == CommitmentLevel::Finalized {
            if let Some(info) = self.transactions.remove(&signature) {
                self.rooted_transactions
                    .unsubscribe_signature(signature)
                    .await;
                let _ = self.finalized_tx_notifier.send(info.signature);
                info!(id = info.id, %signature, "remove transaction: reached finalized commitment");
                metrics::sts_pool_set_size(self.transactions.len());
            }
        } else if let Some(info) = self.transactions.get_mut(&signature) {
            if !info.landed && commitment >= self.config.stop_send_on_commitment {
                info.landed = true;
                info!(id = info.id, %signature, "transaction landed");
                metrics::sts_landed_inc();
            }
        }
    }
}

pub mod testkit {
    use {
        super::RootedTxReceiver,
        crate::util::CommitmentLevel,
        solana_sdk::signature::Signature,
        std::{collections::HashMap, sync::Arc},
        tokio::sync::{
            RwLock,
            mpsc::{self, UnboundedReceiver},
        },
    };

    #[derive(Default)]
    struct MockRootedTxChannelShared {
        transactions: HashMap<Signature, CommitmentLevel>,
    }

    pub struct MockRootedTransactionsTx {
        shared: Arc<RwLock<MockRootedTxChannelShared>>,
        tx: mpsc::UnboundedSender<(Signature, CommitmentLevel)>,
    }

    impl MockRootedTransactionsTx {
        pub async fn send(&self, signature: Signature, commitment: CommitmentLevel) {
            self.shared
                .write()
                .await
                .transactions
                .insert(signature, Default::default());
            self.tx.send((signature, commitment)).unwrap();
        }
    }

    pub struct MockRootedTransactionsRx {
        shared: Arc<RwLock<MockRootedTxChannelShared>>,
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
        async fn subscribe_signature(&mut self, _signature: Signature) {}

        async fn unsubscribe_signature(&mut self, _signature: Signature) {}

        async fn get_transaction_commitment(
            &mut self,
            signature: Signature,
        ) -> Option<CommitmentLevel> {
            let locked = self.shared.read().await;
            locked.transactions.get(&signature).copied()
        }

        async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)> {
            self.rx.recv().await
        }
    }
}
