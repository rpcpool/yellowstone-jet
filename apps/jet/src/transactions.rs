use {
    crate::{
        blockhash_queue::BlockHeightService, cluster_tpu_info::ClusterTpuInfo,
        grpc_lewis::LewisEventHandler, metrics::jet as metrics, solana::get_durable_nonce,
        util::CommitmentLevel,
    },
    bytes::Bytes,
    solana_clock::{MAX_PROCESSING_AGE, Slot},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    tokio::{
        sync::mpsc::{self},
        task::{self, JoinSet},
    },
    tracing::error,
    yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxn, TpuSenderTxnInfo},
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

#[derive(Debug, Clone)]
pub struct SendTransactionRequest {
    pub signature: Signature,
    pub transaction: VersionedTransaction,
    pub wire_transaction: Bytes,
    pub max_retries: Option<usize>,
    pub policies: Vec<Pubkey>,
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
            let durable_nonce = get_durable_nonce(&tx.transaction);
            if let Some(durable_nonce) = durable_nonce {
                let signature = tx.signature;
                tracing::trace!(
                    %signature,
                    %durable_nonce,
                    "forwarding durable nonce transaction without blockhash validation"
                );
                if response_sink.send(tx).is_err() {
                    tracing::trace!("response sink is closed, stopping transaction forwarding");
                    break;
                }
                continue;
            }

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
    fn signature_from_info(info: &Option<TpuSenderTxnInfo>) -> Option<Signature> {
        info.as_ref()
            .and_then(|txn_info| txn_info.downcast_ref::<Signature>())
            .copied()
    }

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
                if let Some(tx_sig) = Self::signature_from_info(&gateway_tx_sent.info) {
                    // BECAREFUL: THE SAME TRANSACTION CAN BE SENT TO MULTIPLE LEADERS,
                    // SO REMOVE MAY RETURN FALSE.
                    self.inflight_transactions.remove(&tx_sig);
                    tracing::trace!(
                        "transaction {tx_sig} forwarded to {} validator",
                        gateway_tx_sent.remote_peer_identity
                    );
                } else {
                    tracing::trace!(
                        "received TxSent without signature metadata for {}",
                        gateway_tx_sent.remote_peer_identity
                    );
                }
            }
            TpuSenderResponse::TxFailed(gateway_tx_failed) => {
                if let Some(tx_sig) = Self::signature_from_info(&gateway_tx_failed.info) {
                    tracing::trace!("transaction {tx_sig} failed");
                    self.inflight_transactions.remove(&tx_sig);
                } else {
                    tracing::trace!(
                        "received TxFailed without signature metadata for {}",
                        gateway_tx_failed.remote_peer_identity
                    );
                }
            }
            TpuSenderResponse::TxDrop(tx_drop) => {
                for (gw_tx, _curr_attempt) in &tx_drop.dropped_tx_vec {
                    if let Some(tx_sig) = Self::signature_from_info(&gw_tx.info) {
                        tracing::trace!("transaction {tx_sig} dropped by QUIC gateway");
                        self.inflight_transactions.remove(&tx_sig);
                    } else {
                        tracing::trace!(
                            "received dropped transaction without signature metadata for {}",
                            tx_drop.remote_peer_identity
                        );
                    }
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
            let txn_wire = tx.wire_transaction.clone();
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
                let txn_info = TpuSenderTxnInfo::new(tx.signature);
                let tpu_txn = TpuSenderTxn::from_bytes(*dest, txn_wire.clone(), Some(txn_info));
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

                let txn_info = TpuSenderTxnInfo::new(tx.signature);
                let tpu_txn = TpuSenderTxn::from_bytes(*extra, txn_wire.clone(), Some(txn_info));

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
