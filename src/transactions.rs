use {
    crate::{
        blockhash_queue::BlockHeighService,
        config::ConfigSendTransactionService,
        grpc_geyser::{
            GeyserSubscriber, GrpcUpdateMessage, SlotUpdateInfoWithCommitment, TransactionReceived,
        },
        metrics::jet as metrics,
        quic::QuicClient,
        solana::get_durable_nonce,
        util::{
            BlockHeight, CommitmentLevel, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    futures::future::{pending, FutureExt},
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        ops::DerefMut,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::SystemTime,
    },
    tokio::{
        sync::{mpsc, watch, Notify, RwLock},
        task::JoinSet,
        time::{interval, Duration, Instant},
    },
    tracing::{debug, error, info, instrument},
};

#[derive(Debug, Default)]
struct RootedTransactionsSlotInfo {
    transactions: HashSet<Signature>,
    slot: Option<SlotUpdateInfoWithCommitment>,
}

type RootedTransactionsUpdateSignature = (Signature, CommitmentLevel);

#[async_trait::async_trait]
pub trait RootedTransactionsTraits {
    async fn subscribe_signature(&self, signature: Signature);
    async fn unsubscribe_signature(&self, signature: &Signature);
    async fn subscribe_signature_updates(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<RootedTransactionsUpdateSignature>>;
    async fn get_transaction_commitment(&self, signature: Signature) -> Option<CommitmentLevel>;
}

#[derive(Debug, Default)]
struct RootedTransactionsInner {
    slots: HashMap<Slot, RootedTransactionsSlotInfo>,
    transactions: HashMap<Signature, Slot>,
    watch_signatures: HashSet<Signature>,
    signature_updates_rx: Option<mpsc::UnboundedReceiver<RootedTransactionsUpdateSignature>>,
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
impl RootedTransactionsTraits for RootedTransactions {
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

    async fn subscribe_signature_updates(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<RootedTransactionsUpdateSignature>> {
        let mut locked = self.inner.write().await;
        locked.signature_updates_rx.take()
    }
}

impl RootedTransactions {
    pub async fn new(grpc: &GeyserSubscriber) -> anyhow::Result<Self> {
        let shutdown = Arc::new(Notify::new());

        let (signature_updates_tx, signature_updates_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(RwLock::new(RootedTransactionsInner {
            signature_updates_rx: Some(signature_updates_rx),
            ..Default::default()
        }));

        Ok(Self {
            inner: Arc::clone(&inner),
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::start_loop(
                shutdown,
                grpc.subscribe_transactions().await.ok_or(anyhow::anyhow!(
                    "RootedTransactions: failed to subscribe on updates from gRPC"
                ))?,
                inner,
                signature_updates_tx,
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

#[derive(Debug)]
pub struct SendTransactionRequest {
    pub signature: Signature,
    pub transaction: VersionedTransaction,
    pub wire_transaction: Vec<u8>,
    pub max_retries: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SendTransactionsPool {
    new_transactions_tx: mpsc::UnboundedSender<SendTransactionRequest>,
    shutdown: Arc<Notify>,
    join_handle: WaitShutdownSharedJoinHandle,
}

impl WaitShutdown for SendTransactionsPool {
    fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        let mut locked = self.join_handle.lock().await;
        locked.deref_mut().await
    }
}

impl SendTransactionsPool {
    pub async fn new(
        config: ConfigSendTransactionService,
        block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
        rooted_transactions: Arc<dyn RootedTransactionsTraits + Send + Sync + 'static>,
        quic_client: QuicClient,
        flush_transactions: watch::Receiver<Arc<Notify>>,
    ) -> anyhow::Result<Self> {
        let shutdown = Arc::new(Notify::new());

        let (new_transactions_tx, new_transactions_rx) = mpsc::unbounded_channel();
        let signature_updates = rooted_transactions
            .subscribe_signature_updates()
            .await
            .ok_or(anyhow::anyhow!(
                "SendTransactionsPool: failed to subscribe on signature updates"
            ))?;
        let task = SendTransactionsPoolTask {
            shutdown: Arc::clone(&shutdown),
            config,
            block_height_service,
            rooted_transactions,
            quic_client,
            new_transactions_rx,
            signature_updates,
            tasks: JoinSet::new(),
            transactions: HashMap::new(),
            retry_schedule: BTreeMap::new(),
            flush_transactions,
        };

        Ok(Self {
            new_transactions_tx,
            shutdown,
            join_handle: Self::spawn(task.run()),
        })
    }

    pub fn send_transaction(&self, request: SendTransactionRequest) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.new_transactions_tx.send(request).is_ok(),
            "send service task finished"
        );
        Ok(())
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
    retry_timestamp: TransactionRetryTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionRetryTimestamp {
    timestamp: u128,
}

impl From<SystemTime> for TransactionRetryTimestamp {
    fn from(st: SystemTime) -> Self {
        let elapsed = st
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to calculate timestamp from UNIX_EPOCH");
        Self {
            timestamp: elapsed.as_millis() / 10,
        }
    }
}

impl TransactionRetryTimestamp {
    fn now() -> Self {
        SystemTime::now().into()
    }

    fn next(delay: Duration) -> Self {
        SystemTime::now()
            .checked_add(delay)
            .expect("failed to calculate SystemTime")
            .into()
    }
}

pub struct SendTransactionsPoolTask {
    shutdown: Arc<Notify>,
    config: ConfigSendTransactionService,
    block_height_service: Arc<dyn BlockHeighService + Send + Sync + 'static>,
    rooted_transactions: Arc<dyn RootedTransactionsTraits + Send + Sync + 'static>,
    quic_client: QuicClient,
    new_transactions_rx: mpsc::UnboundedReceiver<SendTransactionRequest>,
    signature_updates: mpsc::UnboundedReceiver<(Signature, CommitmentLevel)>,
    tasks: JoinSet<SendTransactionTaskResult>,
    transactions: HashMap<Signature, SendTransactionInfo>,
    retry_schedule: BTreeMap<TransactionRetryTimestamp, Vec<Signature>>,
    flush_transactions: watch::Receiver<Arc<Notify>>,
}

impl SendTransactionsPoolTask {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut start_flush_transactions = false;
        let mut notification: Arc<Notify> = Arc::new(Notify::new());

        let mut retry_interval = interval(Duration::from_millis(10));

        loop {
            if self.tasks.is_empty()
                && self.transactions.is_empty()
                && self.retry_schedule.is_empty()
                && start_flush_transactions
            {
                notification.notify_waiters();

                info!("All transactions sent. Resetting identity");
                start_flush_transactions = false;
            }

            let tasks_join_next = if self.tasks.is_empty() {
                pending().boxed()
            } else {
                self.tasks.join_next().boxed()
            };

            tokio::select! {
                res = self.flush_transactions.changed() => {
                    match res {
                        Ok(_) => {
                            eprintln!("Flushing all transactions in queue");

                            info!("Flushing all transactions in queue");
                            start_flush_transactions = true;
                            notification = self.flush_transactions.borrow().clone();
                        }
                        Err(_)  => {
                            error!("Reset identity channel was dropped");

                            return Ok(())}
                    }
                }
                _ = self.shutdown.notified() => return Ok(()),
                maybe_newtx = self.new_transactions_rx.recv() => match maybe_newtx {
                    Some(newtx) =>{
                        eprintln!("Received transaction before flush");
                        self.add_new_transaction(newtx).await
                    },
                    None => anyhow::bail!("incoming transactions channel is closed"),
                },
                maybe_result = tasks_join_next => {
                    eprintln!("Error joining tasks");
                    match maybe_result {
                        Some(Ok(result)) => self.handle_joined_task(result).await,
                        Some(Err(error)) => error!("failed to join send task: {error:?}"),
                        None => unreachable!("joined tasks can't be None")
                    };
                    metrics::sts_inflight_set_size(self.tasks.len());
                },
                _ = retry_interval.tick() => {
                    eprintln!("Error joining tasks");

                    self.retry_scheduled_transactions().await},
                maybe_signature_update = self.signature_updates.recv() => match maybe_signature_update {
                    Some((signature, commitment)) => self.update_signature(signature, commitment).await,
                    None => {
                        error!("signature updates channel is closed");
                        return Ok(());
                    }
                },
            }
        }
    }

    pub fn transactions_count(&mut self) -> usize {
        self.transactions.len()
    }

    pub fn retry_schedule_count(&mut self) -> usize {
        self.retry_schedule.len()
    }

    pub fn tasks_count(&mut self) -> usize {
        self.tasks.len()
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
        if tx_commitment == Some(CommitmentLevel::Finalized) {
            eprintln!("Transaction finalized for commitment");
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
            let retry_timestamp = TransactionRetryTimestamp::next(self.config.retry_rate);
            self.schedule_transaction_retry(signature, retry_timestamp)
                .await;
        } else {
            eprintln!("Transaction::spawn_send_transaction");

            self.spawn_send_transaction(id, signature, wire_transaction);
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

    fn spawn_send_transaction(
        &mut self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        let quic_client = self.quic_client.clone();
        let leader_forward_count = self.config.leader_forward_count;
        let retry_timestamp = TransactionRetryTimestamp::next(self.config.retry_rate);
        eprintln!("Transaction::send_transaction.await");

        self.tasks.spawn(async move {
            info!(id, %signature, "trying to send transaction");

            eprintln!("Transaction::send_transaction.await2");
            quic_client
                .send_transaction(id, signature, wire_transaction, leader_forward_count)
                .await;
            eprintln!("Transaction::send_transaction");

            SendTransactionTaskResult {
                id,
                signature,
                retry_timestamp,
            }
        });
        metrics::sts_inflight_set_size(self.tasks.len());
    }

    async fn handle_joined_task(
        &mut self,
        SendTransactionTaskResult {
            id,
            signature,
            retry_timestamp,
        }: SendTransactionTaskResult,
    ) {
        eprintln!("Transaction::handle_joined_task");

        if let Some(info) = self.transactions.get_mut(&signature) {
            eprintln!("Transaction::handle_joined_task info {:?}", info);

            if info.id == id {
                info.total_retries += 1;
                self.schedule_transaction_retry(signature, retry_timestamp)
                    .await;
            }
        }
    }

    async fn schedule_transaction_retry(
        &mut self,
        signature: Signature,
        retry_timestamp: TransactionRetryTimestamp,
    ) {
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
                .push(signature);
        } else {
            self.remove_transaction(signature, "max_retries reached")
                .await;
        }
    }

    #[instrument(skip_all)]
    async fn retry_scheduled_transactions(&mut self) {
        let st_now = TransactionRetryTimestamp::now();
        let mut scheduled = vec![];
        loop {
            match self.retry_schedule.keys().next().copied() {
                Some(st) if st <= st_now => {
                    scheduled.push(self.retry_schedule.remove(&st));
                }
                _ => break,
            }
        }

        let maybe_block_height = self
            .block_height_service
            .get_block_height_for_commitment(CommitmentLevel::Confirmed)
            .await;

        let retry_timestamp = TransactionRetryTimestamp::next(self.config.retry_rate);
        for signature in scheduled.into_iter().flatten().flatten() {
            if let Some(info) = self.transactions.get(&signature) {
                if let Some(block_height) = maybe_block_height {
                    if info.last_valid_block_height < block_height {
                        self.remove_transaction(signature, "block_height expired")
                            .await;
                        continue;
                    }
                }

                if info.total_retries <= info.max_retries {
                    self.spawn_send_transaction(
                        info.id,
                        info.signature,
                        Arc::clone(&info.wire_transaction),
                    );
                } else {
                    self.schedule_transaction_retry(signature, retry_timestamp)
                        .await;
                }
            }
        }
        metrics::sts_pool_set_size(self.transactions.len());
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

#[derive(Debug, Clone)]
pub struct MockRootedTransactions {
    inner: Arc<RwLock<RootedTransactionsInner>>,
}

impl MockRootedTransactions {
    pub fn new() -> Self {
        let (_, signature_updates_rx) = mpsc::unbounded_channel();

        let rooted_transactions_inner = RootedTransactionsInner {
            signature_updates_rx: Some(signature_updates_rx),
            ..Default::default()
        };

        Self {
            inner: Arc::new(RwLock::new(rooted_transactions_inner)),
        }
    }
}

#[async_trait::async_trait]
impl RootedTransactionsTraits for MockRootedTransactions {
    async fn subscribe_signature(&self, signature: Signature) {
        let mut locked = self.inner.write().await;
        locked.watch_signatures.insert(signature);
    }
    async fn unsubscribe_signature(&self, signature: &Signature) {
        let mut locked = self.inner.write().await;
        locked.watch_signatures.remove(signature);
    }
    async fn subscribe_signature_updates(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<RootedTransactionsUpdateSignature>> {
        let mut locked = self.inner.write().await;
        locked.signature_updates_rx.take()
    }
    async fn get_transaction_commitment(&self, signature: Signature) -> Option<CommitmentLevel> {
        let locked = self.inner.read().await;
        locked
            .transactions
            .get(&signature)
            .and_then(|slot| locked.slots.get(slot))
            .and_then(|info| info.slot)
            .map(|info| info.commitment)
    }
}
