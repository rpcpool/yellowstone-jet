use {
    crate::{
        blockhash_queue::BlockHeighService,
        config::ConfigSendTransactionService,
        grpc_geyser::{
            GeyserSubscriber, GrpcUpdateMessage, SlotUpdateInfoWithCommitment, TransactionReceived,
        },
        metrics::jet as metrics,
        quic::{QuicClient, QuicSendTxPermit},
        quic_solana::IdentityFlusher,
        solana::get_durable_nonce,
        util::{
            BlockHeight, CommitmentLevel, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    futures::future::{BoxFuture, Join},
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES},
        signature::{self, Signature},
        transaction::VersionedTransaction,
    },
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        future::Future,
        ops::DerefMut,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::{
        sync::{
            mpsc::{self},
            oneshot, Notify, RwLock,
        },
        task::{self, AbortHandle, JoinSet},
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

#[async_trait::async_trait]
pub trait RootedTransactionSignatures {
    async fn subscribe_signature(&self, signature: Signature);
    async fn unsubscribe_signature(&self, signature: &Signature);
    async fn get_transaction_commitment(&self, signature: Signature) -> Option<CommitmentLevel>;
}

pub struct WatchSignatureCommand {
    signature: Signature,
    callback: oneshot::Sender<OwnedTxSubscription>,
}

pub struct UnwatchSignatureCommand {
    signature: Signature,
}

pub enum TxStatusSubscriptionCommand {
    WatchSignature(WatchSignatureCommand),
    UnwatchSignature(UnwatchSignatureCommand),
}

pub struct TransactionStatusReceiver {
    ///
    /// Watched signatures and their commitment level
    ///
    rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
    ///
    /// Command-and-Control channel to send commands to the subscription
    ///
    cnc_tx: mpsc::UnboundedSender<TxStatusSubscriptionCommand>,
}

///
/// Owned transaction subscription proof, that can be used to unsubscribe.
///
/// Once all references to this object are dropped, the subscription will be automatically unsubscribed.
///
pub struct OwnedTxSubscription {
    signature: Signature,
    cnc_tx: mpsc::UnboundedSender<TxStatusSubscriptionCommand>,
}

impl Drop for OwnedTxSubscription {
    fn drop(&mut self) {
        let unwatch_signature_cmd = UnwatchSignatureCommand {
            signature: self.signature,
        };
        let _ = self
            .cnc_tx
            .send(TxStatusSubscriptionCommand::UnwatchSignature(
                unwatch_signature_cmd,
            ));
    }
}

impl TransactionStatusReceiver {
    async fn recv(&mut self) -> Option<(Signature, CommitmentLevel)> {
        self.rx.recv().await
    }

    async fn watch_signature(&mut self, signature: Signature) -> OwnedTxSubscription {
        let (tx, rx) = oneshot::channel();
        let watch_cmd = WatchSignatureCommand {
            signature,
            callback: tx,
        };
        self.cnc_tx
            .send(TxStatusSubscriptionCommand::WatchSignature(watch_cmd))
            .expect("failed to send watch command");
        rx.await.expect("failed to receive watch command result")
    }
}

#[derive(Debug, Default)]
pub struct RootedTransactionsInner {
    slots: HashMap<Slot, RootedTransactionsSlotInfo>,
    transactions: HashMap<Signature, Slot>,
    watch_signatures: HashSet<Signature>,
    // signature_updates_rx: Option<mpsc::UnboundedReceiver<RootedTransactionsUpdateSignature>>,
}

#[derive(Debug, Clone)]
pub struct RootedTransactions {
    inner: Arc<RwLock<RootedTransactionsInner>>,
    shutdown: Arc<Notify>,
    join_handle: WaitShutdownSharedJoinHandle,
}

impl WaitShutdown for RootedTransactions {
    fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        let mut locked = self.join_handle.lock().await;
        locked.deref_mut().await
    }
}

#[async_trait::async_trait]
impl RootedTransactionSignatures for RootedTransactions {
    async fn get_transaction_commitment(&self, signature: Signature) -> Option<CommitmentLevel> {
        let locked = self.inner.read().await;
        locked
            .transactions
            .get(&signature)
            .and_then(|slot| locked.slots.get(slot))
            .and_then(|info| info.slot)
            .map(|info| info.commitment)
    }

    async fn subscribe_signature(&self, signature: Signature) {
        let mut locked = self.inner.write().await;
        locked.watch_signatures.insert(signature);
    }

    async fn unsubscribe_signature(&self, signature: &Signature) {
        let mut locked = self.inner.write().await;
        locked.watch_signatures.remove(signature);
    }
}

#[deprecated(note = "uses GrpcTxSubscribeAdapter instead")]
impl RootedTransactions {
    ///
    /// Creates a new rooted transactions service
    ///
    /// Arguments:
    ///
    /// * `grpc_rx` - gRPC sending transaction updates
    /// * `tx_status_update_tx` - the channel sink where we send filtered transaction updates to.
    ///
    pub async fn new(
        grpc_rx: mpsc::Receiver<GrpcUpdateMessage>,
        tx_status_update_tx: mpsc::UnboundedSender<RootedTransactionsUpdateSignature>,
    ) -> anyhow::Result<Self> {
        let shutdown = Arc::new(Notify::new());

        let shared = Default::default();

        Ok(Self {
            inner: Arc::clone(&shared),
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::start_loop(
                shutdown,
                grpc_rx,
                shared,
                tx_status_update_tx,
            )),
        })
    }

    async fn start_loop(
        shutdown: Arc<Notify>,
        mut grpc_rx: mpsc::Receiver<GrpcUpdateMessage>,
        inner: Arc<RwLock<RootedTransactionsInner>>,
        signature_updates_tx: mpsc::UnboundedSender<RootedTransactionsUpdateSignature>,
    ) -> anyhow::Result<()> {
        let mut transactions_bulk = vec![];
        loop {
            let slot_update = tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                message = grpc_rx.recv() => match message {
                    Some(GrpcUpdateMessage::Transaction(transaction)) => {
                        transactions_bulk.push(transaction);
                        continue;
                    }
                    Some(GrpcUpdateMessage::Slot(slot)) => slot,
                    None => anyhow::bail!("RootedTransactions: gRPC streamed closed"),
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

            macro_rules! send_update {
                ($signature:expr, $commitment:expr) => {
                    if watch_signatures.contains($signature) {
                        if signature_updates_tx.send((*$signature, $commitment)).is_err() {
                            error!(signature=%$signature, "failed to send an update");
                        }
                    }
                };
            }

            // Update slot commitment
            let entry = slots.entry(slot_update.slot).or_default();
            entry.slot = Some(slot_update);
            for signature in entry.transactions.iter() {
                send_update!(signature, slot_update.commitment);
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
                        send_update!(&signature, slot.commitment);
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
}

#[derive(Debug, Clone)]
pub struct SendTransactionsPool {
    new_transactions_tx: mpsc::UnboundedSender<SendTransactionRequest>,
    cnc_tx: mpsc::Sender<SendTransactionPoolCommand>,
}

impl SendTransactionsPool {
    pub async fn spawn(
        config: ConfigSendTransactionService,
        block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
        rooted_transactions: Arc<dyn RootedTransactionSignatures + Send + Sync + 'static>,
        tx_sender: Arc<dyn TxChannel + Send + Sync + 'static>,
        tx_status_update_rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
    ) -> (Self, impl Future<Output = ()>) {
        let (new_transactions_tx, new_transactions_rx) = mpsc::unbounded_channel();
        let task = SendTransactionsPoolTask {
            config,
            block_height_service,
            rooted_transactions,
            tx_channel: tx_sender,
            new_transactions_rx,
            tx_status_update_rx,
            transactions: HashMap::new(),
            retry_schedule: BTreeMap::new(),
            connecting_map: Default::default(),
            connecting_tasks: JoinSet::new(),
            send_map: Default::default(),
            send_tasks: JoinSet::new(),
        };

        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let frontend = Self {
            new_transactions_tx,
            cnc_tx,
        };

        // The backend task will end after all reference to the frontend task are dropped
        let backend = task.run(cnc_rx);
        (frontend, backend)
    }

    pub fn send_transaction(&self, request: SendTransactionRequest) -> anyhow::Result<()> {
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

pub struct BoxedTxChannelPermit {
    inner: Box<
        dyn FnOnce(SendTransactionInfoId, Signature, Arc<Vec<u8>>) -> BoxFuture<'static, ()> + Send,
    >,
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
    async fn reserve(&self, leader_foward_count: usize) -> Option<BoxedTxChannelPermit>;
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
    async fn reserve(&self, leader_foward_count: usize) -> Option<BoxedTxChannelPermit> {
        QuicClient::reserve_send_permit(self, leader_foward_count)
            .await
            .map(BoxedTxChannelPermit::new)
    }
}

pub struct TransactionInfo {
    pub id: SendTransactionInfoId,
    pub signature: Signature,
    pub wire_transaction: Arc<Vec<u8>>,
}

pub struct SendTransactionsPoolTask {
    config: ConfigSendTransactionService,
    block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
    rooted_transactions: Arc<dyn RootedTransactionSignatures + Send + Sync + 'static>,
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
    tx_status_update_rx: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,

    transactions: HashMap<Signature, SendTransactionInfo>,
    retry_schedule: BTreeMap<Instant, VecDeque<Signature>>,
}

impl SendTransactionsPoolTask {
    pub async fn run(
        mut self,
        mut cnc_rx: mpsc::Receiver<SendTransactionPoolCommand>, // command-and-control channel
    ) {
        // let mut retry_interval = interval(Duration::from_millis(10));
        loop {
            metrics::sts_pool_set_size(self.transactions.len());

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
                _ = tokio::time::sleep_until(next_retry_deadline) => {
                    self.retry_next_in_schedule().await;
                }
                maybe_signature_update = self.tx_status_update_rx.recv() => {
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
                self.schedule_transaction_retry(
                    tx_info.signature,
                    Instant::now() + self.config.retry_rate,
                )
                .await;
            }
        }
    }

    fn spawn_send(&mut self, permit: BoxedTxChannelPermit, tx_info: TransactionInfo) {
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
            self.spawn_connect(tx_info.id, tx_info.signature, tx_info.wire_transaction);
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
        }: SendTransactionRequest,
    ) {
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
            self.spawn_connect(id, signature, wire_transaction);
        }
    }

    async fn remove_transaction(&mut self, signature: Signature, reason: &'static str) {
        let removed_tx = self.transactions.remove(&signature);
        self.rooted_transactions
            .unsubscribe_signature(&signature)
            .await;
        if let Some(info) = removed_tx {
            info!(id = info.id, %signature, reason, "remove transaction");
        }
    }

    fn spawn_connect(
        &mut self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        let leader_forward_count = self.config.leader_forward_count;
        let tx_channel = Arc::clone(&self.tx_channel);
        let abort_handle = self
            .connecting_tasks
            .spawn(async move { tx_channel.reserve(leader_forward_count).await });
        self.connecting_map.insert(
            abort_handle.id(),
            TransactionInfo {
                id,
                signature,
                wire_transaction,
            },
        );
    }

    async fn schedule_transaction_retry(&mut self, signature: Signature, retry_timestamp: Instant) {
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
                    .unsubscribe_signature(&signature)
                    .await;
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
        super::{RootedTransactionSignatures, RootedTransactionsInner},
        crate::util::CommitmentLevel,
        solana_sdk::signature::Signature,
        std::sync::Arc,
        tokio::sync::{
            mpsc::{self, UnboundedSender},
            RwLock,
        },
    };

    #[derive(Debug, Clone)]
    pub struct MockRootedTransactions {
        inner: Arc<RwLock<RootedTransactionsInner>>,
        signature_updates_tx: UnboundedSender<(Signature, CommitmentLevel)>,
    }

    impl MockRootedTransactions {
        pub fn new() -> Self {
            let (signature_updates_tx, signature_updates_rx) = mpsc::unbounded_channel();

            let rooted_transactions_inner = RootedTransactionsInner {
                ..Default::default()
            };

            Self {
                inner: Arc::new(RwLock::new(rooted_transactions_inner)),
                signature_updates_tx,
            }
        }
    }

    impl Default for MockRootedTransactions {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockRootedTransactions {
        pub async fn update_signature(&self, signature: Signature, commitment: CommitmentLevel) {
            self.signature_updates_tx
                .send((signature, commitment))
                .expect("Error sending signature update");
        }
    }

    #[async_trait::async_trait]
    impl RootedTransactionSignatures for MockRootedTransactions {
        async fn subscribe_signature(&self, signature: Signature) {
            let mut locked = self.inner.write().await;
            locked.watch_signatures.insert(signature);
        }
        async fn unsubscribe_signature(&self, signature: &Signature) {
            let mut locked = self.inner.write().await;
            locked.watch_signatures.remove(signature);
        }
        async fn get_transaction_commitment(
            &self,
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
    }
}
