use {
    crate::{
        config::ConfigUpstreamGrpc,
        metrics::jet as metrics,
        util::{BlockHeight, CommitmentLevel, IncrementalBackoff, SlotStatus},
    },
    futures::{
        FutureExt,
        stream::{Stream, StreamExt},
    },
    maplit::hashmap,
    semver::{Version, VersionReq},
    serde::Deserialize,
    solana_clock::Slot,
    solana_hash::{Hash, ParseHashError},
    solana_signature::Signature,
    std::{
        collections::BTreeMap,
        future::Future,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{Mutex, broadcast, mpsc},
        task::{JoinError, JoinHandle},
        time,
    },
    tokio_util::sync::CancellationToken,
    tonic::transport::channel::ClientTlsConfig,
    tracing::{debug, error, info, warn},
    yellowstone_grpc_client::{GeyserGrpcBuilder, GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        prelude::{
            BlockHeight as GrpcBlockHeight, CommitmentLevel as GrpcCommitmentLevel,
            SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateSlot, SubscribeUpdateTransactionStatus, subscribe_update::UpdateOneof,
        },
        tonic::Status,
    },
};

const QUEUE_SIZE_SLOT_UPDATE: usize = 10_000;
const QUEUE_SIZE_BLOCKMETA_UPDATE: usize = 1_000;
const QUEUE_SIZE_TRANSACTIONS: usize = 1_000_000;

#[derive(Debug, thiserror::Error)]
pub enum GeyserError {
    #[error("gRPC connection failed: {0}")]
    ConnectionFailed(String),

    #[error("gRPC stream error: {0}")]
    StreamError(#[from] Status),

    #[error("gRPC stream ended unexpectedly")]
    StreamEnded,

    #[error("Channel send failed: {channel}")]
    ChannelSendFailed { channel: &'static str },

    #[error("Invalid blockhash: {0}")]
    InvalidBlockhash(#[from] ParseHashError),

    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    #[error("Block metadata missing block height")]
    MissingBlockHeight,

    #[error("Unexpected gRPC message: {0}")]
    UnexpectedMessage(String),

    #[error("Version validation failed: {0}")]
    VersionValidation(String),

    #[error("Version parse error: {0}")]
    VersionParse(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Semver parse error: {0}")]
    SemverParse(#[from] semver::Error),

    #[error("gRPC client error: {0}")]
    GrpcClient(String),
}

pub type Result<T> = std::result::Result<T, GeyserError>;

/*
 * SlotTrackingInfo coordinates between slot status updates and block metadata.
 * These can arrive in any order, so we track what we've seen and emit
 * block metadata only when we have both the metadata AND a commitment status.
 */
#[derive(Debug, Default, Clone, Copy)]
struct SlotTrackingInfo {
    // Bitmask tracking which slot statuses we've seen
    statuses_seen: u8,
    block_height: BlockHeight,
    block_hash: Hash,
    has_block_meta: bool,
}

impl SlotTrackingInfo {
    const fn mark_status_seen(&mut self, status: SlotStatus) {
        self.statuses_seen |= 1 << (status as i32 as u8);
    }

    const fn has_seen_status(&self, status: SlotStatus) -> bool {
        self.statuses_seen & (1 << (status as i32 as u8)) != 0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BlockMetaWithCommitment {
    pub slot: Slot,
    pub block_height: BlockHeight,
    pub block_hash: Hash,
    pub commitment: CommitmentLevel,
}

#[derive(Debug, Clone, Copy)]
pub struct SlotUpdateWithStatus {
    pub slot: Slot,
    pub slot_status: SlotStatus,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionReceived {
    pub slot: Slot,
    pub signature: Signature,
}

#[derive(Debug, Clone, Copy)]
pub enum GrpcUpdateMessage {
    BlockMeta(BlockMetaWithCommitment),
    Slot(SlotUpdateWithStatus),
    Transaction(TransactionReceived),
}

pub struct GeyserHandle {
    inner: JoinHandle<Result<()>>,
}

impl Future for GeyserHandle {
    type Output = std::result::Result<Result<()>, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

#[derive(Debug)]
pub struct GeyserSubscriber {
    block_meta_rx: broadcast::Receiver<BlockMetaWithCommitment>,
    slots_rx: broadcast::Receiver<SlotUpdateWithStatus>,
    transactions_rx: Arc<Mutex<Option<mpsc::Receiver<GrpcUpdateMessage>>>>,
}

impl GeyserSubscriber {
    pub fn new(
        primary_grpc: ConfigUpstreamGrpc,
        include_transactions: bool,
        cancellation_token: CancellationToken,
    ) -> (Self, GeyserHandle) {
        let (slots_tx, slots_rx) = broadcast::channel(QUEUE_SIZE_SLOT_UPDATE);
        let (block_meta_tx, block_meta_rx) = broadcast::channel(QUEUE_SIZE_BLOCKMETA_UPDATE);

        let (transactions_tx, transactions_rx) = mpsc::channel(QUEUE_SIZE_TRANSACTIONS);
        if !include_transactions {
            info!("Transactions disabled, skipping transaction subscription in gRPC Geyser");
        }

        let geyser_handle = tokio::spawn(Self::grpc_subscribe(
            primary_grpc,
            slots_tx.clone(),
            block_meta_tx.clone(),
            transactions_tx,
            include_transactions,
            cancellation_token,
        ));
        let geyser_handle = GeyserHandle {
            inner: geyser_handle,
        };

        let geyser = Self {
            slots_rx,
            block_meta_rx,
            transactions_rx: Arc::new(Mutex::new(Some(transactions_rx))),
        };

        (geyser, geyser_handle)
    }

    async fn grpc_subscribe(
        primary_grpc: ConfigUpstreamGrpc,
        slots_tx: broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
        include_transactions: bool,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let endpoint = primary_grpc.endpoint;
        let x_token = primary_grpc.x_token;

        loop {
            Self::reset_slot_metrics();

            // Check for shutdown before attempting connection
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            let stream = tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("gRPC subscriber: cancellation token triggered, shutting down...");
                    return Ok(());
                },
                result = Self::grpc_open(&endpoint, x_token.as_deref(), true, include_transactions) => {
                    result
                }
                _ = time::sleep(Duration::from_secs(30)) => {
                    warn!("Timeout opening gRPC connection ({endpoint})");
                    continue;
                }
            };

            let stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Failed to open gRPC connection ({endpoint}): {e:?}");
                    // TODO: we probably need to backoff + maximum retries here
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            // Process stream until it ends or errors
            if let Err(e) = Self::process_grpc_stream(
                stream,
                &slots_tx,
                &block_meta_tx,
                &transactions_tx,
                include_transactions,
                cancellation_token.clone(),
            )
            .await
            {
                error!("gRPC stream processing error ({endpoint}): {e:?}");
            }

            // Small delay before reconnecting
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    /*
     * Core stream processing logic - processes until stream ends.
     * Shutdown is handled in the outer loop between reconnections.
     */
    pub async fn process_grpc_stream<S>(
        mut stream: S,
        slots_tx: &broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: &broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: &mpsc::Sender<GrpcUpdateMessage>,
        include_transactions: bool,
        cancellation_token: CancellationToken,
    ) -> Result<()>
    where
        S: Stream<Item = std::result::Result<SubscribeUpdate, Status>> + Unpin,
    {
        let mut slot_tracking = BTreeMap::<Slot, SlotTrackingInfo>::new();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("gRPC stream processing: cancellation token triggered, shutting down...");
                    return Ok(());
                }

                // Prioritize stream processing
                message = stream.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            Self::handle_grpc_message(
                                msg,
                                &mut slot_tracking,
                                slots_tx,
                                block_meta_tx,
                                transactions_tx,
                                include_transactions,
                            )
                            .await?;

                            metrics::set_slot_tracking_btreemap_size(slot_tracking.len());
                        }
                        Some(Err(error)) => {
                            return Err(GeyserError::StreamError(error));
                        }
                        None => {
                            return Err(GeyserError::StreamEnded);
                        }
                    }
                }
            }
        }
    }

    async fn handle_grpc_message(
        msg: SubscribeUpdate,
        slot_tracking: &mut BTreeMap<Slot, SlotTrackingInfo>,
        slots_tx: &broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: &broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: &mpsc::Sender<GrpcUpdateMessage>,
        include_transactions: bool,
    ) -> Result<()> {
        let msg_start = Instant::now();
        let msg_type = match &msg.update_oneof {
            Some(UpdateOneof::Slot(_)) => "slot",
            Some(UpdateOneof::TransactionStatus(_)) => "transaction",
            Some(UpdateOneof::BlockMeta(_)) => "block_meta",
            Some(UpdateOneof::Ping(_)) => "ping",
            _ => "unknown",
        };

        let result = match msg.update_oneof {
            Some(UpdateOneof::Slot(slot_update)) => {
                Self::handle_slot_update(
                    slot_update,
                    slot_tracking,
                    slots_tx,
                    block_meta_tx,
                    transactions_tx,
                    include_transactions,
                )
                .await
            }
            Some(UpdateOneof::TransactionStatus(tx_status)) => {
                if include_transactions {
                    Self::handle_transaction_status(tx_status, transactions_tx).await
                } else {
                    // Skip transaction processing when transactions are disabled
                    Ok(())
                }
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                Self::handle_block_meta(
                    block_meta,
                    slot_tracking,
                    block_meta_tx,
                    transactions_tx,
                    include_transactions,
                )
                .await
            }
            Some(UpdateOneof::Ping(_)) => {
                debug!("ping received");
                Ok(())
            }
            _ => {
                return Err(GeyserError::UnexpectedMessage(format!("{msg:?}")));
            }
        };

        metrics::observe_grpc_message_handle_time(msg_type, msg_start.elapsed());
        metrics::incr_grpc_messages_processed(msg_type);

        result
    }

    async fn handle_slot_update(
        slot_update: SubscribeUpdateSlot,
        slot_tracking: &mut BTreeMap<Slot, SlotTrackingInfo>,
        slots_tx: &broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: &broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: &mpsc::Sender<GrpcUpdateMessage>,
        include_transactions: bool,
    ) -> Result<()> {
        let handle_start = Instant::now();
        let SubscribeUpdateSlot { slot, status, .. } = slot_update;
        let slot_status = SlotStatus::from(status);

        let entry = slot_tracking.entry(slot).or_default();
        entry.mark_status_seen(slot_status);

        // Send slot update immediately
        let slot_update = SlotUpdateWithStatus { slot, slot_status };
        let send_start = Instant::now();
        match slots_tx.send(slot_update) {
            Ok(_) => {
                metrics::observe_grpc_channel_send_time("slots", send_start.elapsed());
            }
            Err(_) => {
                metrics::incr_grpc_channel_send_failures("slots");
            }
        }

        // Check if we should emit block meta
        let mut emissions = 0;
        if entry.has_block_meta {
            if let Some(commitment) = slot_status_to_commitment(slot_status) {
                emissions += 1;
                let block_meta = BlockMetaWithCommitment {
                    slot,
                    block_height: entry.block_height,
                    block_hash: entry.block_hash,
                    commitment,
                };

                let send_start = Instant::now();
                match block_meta_tx.send(block_meta) {
                    Ok(_) => {
                        metrics::observe_grpc_channel_send_time("block_meta", send_start.elapsed());
                    }
                    Err(_) => {
                        metrics::incr_grpc_channel_send_failures("block_meta");
                    }
                }

                // Only send to transactions channel if transactions are included
                if include_transactions {
                    let send_start = Instant::now();
                    match transactions_tx
                        .send(GrpcUpdateMessage::BlockMeta(block_meta))
                        .await
                    {
                        Ok(_) => {
                            metrics::observe_grpc_channel_send_time(
                                "transactions",
                                send_start.elapsed(),
                            );
                        }
                        Err(_) => {
                            metrics::incr_grpc_channel_send_failures("transactions");
                            return Err(GeyserError::ChannelSendFailed {
                                channel: "transactions",
                            });
                        }
                    }
                }
            }
        }

        // Track block meta emissions
        metrics::observe_block_meta_emissions_count(emissions);

        metrics::grpc_slot_set(slot_status, slot);

        // Cleanup on finalized
        if slot_status == SlotStatus::SlotFinalized {
            let before_size = slot_tracking.len();
            *slot_tracking = slot_tracking.split_off(&slot);
            let after_size = slot_tracking.len();

            debug!(
                "Cleaned up {} slots on finalized slot {}",
                before_size - after_size,
                slot
            );

            metrics::set_slot_tracking_btreemap_size(slot_tracking.len());
        }

        metrics::observe_grpc_slot_update_handle_time(handle_start.elapsed());
        Ok(())
    }

    async fn handle_transaction_status(
        tx_status: SubscribeUpdateTransactionStatus,
        transactions_tx: &mpsc::Sender<GrpcUpdateMessage>,
    ) -> Result<()> {
        let SubscribeUpdateTransactionStatus {
            slot, signature, ..
        } = tx_status;
        let signature = Signature::try_from(signature).expect("Invalid signature format");

        let send_start = Instant::now();
        match transactions_tx
            .send(GrpcUpdateMessage::Transaction(TransactionReceived {
                slot,
                signature,
            }))
            .await
        {
            Ok(_) => {
                metrics::observe_grpc_channel_send_time("transactions", send_start.elapsed());
                Ok(())
            }
            Err(_) => {
                metrics::incr_grpc_channel_send_failures("transactions");
                Err(GeyserError::ChannelSendFailed {
                    channel: "transactions",
                })
            }
        }
    }

    async fn handle_block_meta(
        block_meta_update: SubscribeUpdateBlockMeta,
        slot_tracking: &mut BTreeMap<Slot, SlotTrackingInfo>,
        block_meta_tx: &broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: &mpsc::Sender<GrpcUpdateMessage>,
        include_transactions: bool,
    ) -> Result<()> {
        let SubscribeUpdateBlockMeta {
            slot,
            blockhash,
            block_height: Some(GrpcBlockHeight { block_height }),
            ..
        } = block_meta_update
        else {
            return Err(GeyserError::MissingBlockHeight);
        };

        let block_hash = blockhash.parse()?;

        let entry = slot_tracking.entry(slot).or_default();
        entry.block_height = block_height;
        entry.block_hash = block_hash;
        entry.has_block_meta = true;

        // Emit block meta for any commitment statuses we've already seen
        let mut emissions = 0;
        for status in [
            SlotStatus::SlotProcessed,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
        ] {
            if entry.has_seen_status(status) {
                if let Some(commitment) = slot_status_to_commitment(status) {
                    emissions += 1;
                    let block_meta = BlockMetaWithCommitment {
                        slot,
                        block_height,
                        block_hash,
                        commitment,
                    };

                    let send_start = Instant::now();
                    match block_meta_tx.send(block_meta) {
                        Ok(_) => {
                            metrics::observe_grpc_channel_send_time(
                                "block_meta",
                                send_start.elapsed(),
                            );
                        }
                        Err(_) => {
                            metrics::incr_grpc_channel_send_failures("block_meta");
                        }
                    }

                    // Only send to transactions channel if transactions are included
                    if include_transactions {
                        let send_start = Instant::now();
                        match transactions_tx
                            .send(GrpcUpdateMessage::BlockMeta(block_meta))
                            .await
                        {
                            Ok(_) => {
                                metrics::observe_grpc_channel_send_time(
                                    "transactions",
                                    send_start.elapsed(),
                                );
                            }
                            Err(_) => {
                                metrics::incr_grpc_channel_send_failures("transactions");
                                return Err(GeyserError::ChannelSendFailed {
                                    channel: "transactions",
                                });
                            }
                        }
                    }
                }
            }
        }

        // Track block meta emissions
        metrics::observe_block_meta_emissions_count(emissions);

        Ok(())
    }

    fn reset_slot_metrics() {
        metrics::grpc_slot_set(SlotStatus::SlotProcessed, 0);
        metrics::grpc_slot_set(SlotStatus::SlotConfirmed, 0);
        metrics::grpc_slot_set(SlotStatus::SlotFinalized, 0);
        metrics::grpc_slot_set(SlotStatus::SlotFirstShredReceived, 0);
        metrics::grpc_slot_set(SlotStatus::SlotCompleted, 0);
        metrics::grpc_slot_set(SlotStatus::SlotCreatedBank, 0);
        metrics::grpc_slot_set(SlotStatus::SlotDead, 0);
    }

    async fn grpc_open(
        endpoint: &str,
        x_token: Option<&str>,
        full: bool,
        include_transactions: bool,
    ) -> Result<impl Stream<Item = std::result::Result<SubscribeUpdate, Status>> + use<>> {
        let mut backoff = IncrementalBackoff::default();
        loop {
            backoff.maybe_tick().await;

            let builder = GeyserGrpcBuilder::from_shared(endpoint.to_string())
                .expect("endpoint") // if endpoint is invalid, fail fast
                .x_token(x_token)
                .expect("x_token"); // if x_token is invalid, fail fast

            let builder = builder
                .max_decoding_message_size(128 * 1024 * 1024) // 128MiB
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(3))
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .expect("tls_config"); // if tls_config is invalid, fail fast

            let result = builder.connect().await;
            let mut client = match result {
                Ok(client) => {
                    backoff.reset();
                    client
                }
                Err(error) => {
                    // TODO: we should probably limit the number of retries here + dig into the error to see if it's retryable
                    warn!("failed to connect ({endpoint}): {error:?}");
                    backoff.init();
                    continue;
                }
            };

            Self::validate_version(&mut client).await?;

            let (slots, blocks_meta) = if full {
                (
                    hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                        filter_by_commitment: Some(false),
                        interslot_updates: Some(true), // Get all slot updates
                    } },
                    hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
                )
            } else {
                (hashmap! {}, hashmap! {})
            };

            let mut request = SubscribeRequest {
                slots,
                blocks_meta,
                commitment: Some(GrpcCommitmentLevel::Processed as i32),
                ..SubscribeRequest::default()
            };

            // Only add transaction subscription if transactions are included
            if include_transactions {
                request.transactions_status =
                    hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() };
            }

            match client.subscribe_once(request).await {
                Ok(stream) => {
                    let mode_suffix = if !include_transactions {
                        " (no transactions)"
                    } else {
                        ""
                    };
                    if full {
                        info!(
                            "subscribed on slot (all statuses){} and blocks meta ({endpoint}){mode_suffix}",
                            if include_transactions {
                                ", transactions statuses"
                            } else {
                                ""
                            }
                        );
                    } else {
                        info!(
                            "subscribed{} ({endpoint}){mode_suffix}",
                            if include_transactions {
                                " on transactions statuses"
                            } else {
                                ""
                            }
                        );
                    }
                    return Ok(stream);
                }
                Err(error) => warn!("failed to subscribe ({endpoint}): {error:?}"),
            }
        }
    }

    async fn validate_version(geyser: &mut GeyserGrpcClient<impl Interceptor>) -> Result<()> {
        #[derive(Debug, Deserialize)]
        struct GrpcVersionOld {
            version: String,
        }

        #[derive(Debug, Deserialize)]
        struct GrpcVersionNew {
            version: GrpcVersionOld,
        }

        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum GrpcVersion {
            Old(GrpcVersionOld),
            New(GrpcVersionNew),
        }

        let response = geyser
            .get_version()
            .await
            .map_err(|e| GeyserError::GrpcClient(format!("failed to get version: {e}")))?;

        let version = match serde_json::from_str::<GrpcVersion>(&response.version)? {
            GrpcVersion::Old(s) => s.version,
            GrpcVersion::New(s) => s.version.version,
        };

        let version = Version::parse(&version)?;
        let required = VersionReq::parse(">=1.14.1").map_err(|e| {
            GeyserError::VersionParse(format!("failed to parse required version: {e}"))
        })?;

        if !required.matches(&version) {
            return Err(GeyserError::VersionValidation(format!(
                "gRPC version {version} doesn't match required {required}"
            )));
        }

        Ok(())
    }
}

const fn slot_status_to_commitment(status: SlotStatus) -> Option<CommitmentLevel> {
    match status {
        SlotStatus::SlotProcessed => Some(CommitmentLevel::Processed),
        SlotStatus::SlotConfirmed => Some(CommitmentLevel::Confirmed),
        SlotStatus::SlotFinalized => Some(CommitmentLevel::Finalized),
        _ => None,
    }
}

#[async_trait::async_trait]
pub trait GeyserStreams {
    fn subscribe_slots(&self) -> broadcast::Receiver<SlotUpdateWithStatus>;
    fn subscribe_block_meta(&self) -> broadcast::Receiver<BlockMetaWithCommitment>;
    async fn subscribe_transactions(&self) -> Option<mpsc::Receiver<GrpcUpdateMessage>>;
}

#[async_trait::async_trait]
impl GeyserStreams for GeyserSubscriber {
    fn subscribe_slots(&self) -> broadcast::Receiver<SlotUpdateWithStatus> {
        self.slots_rx.resubscribe()
    }

    fn subscribe_block_meta(&self) -> broadcast::Receiver<BlockMetaWithCommitment> {
        self.block_meta_rx.resubscribe()
    }

    async fn subscribe_transactions(&self) -> Option<mpsc::Receiver<GrpcUpdateMessage>> {
        self.transactions_rx.lock().await.take()
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{grpc_geyser::SlotTrackingInfo, util::SlotStatus},
        solana_hash::Hash,
    };

    #[test]
    fn test_slot_seen_status() {
        let mut info = SlotTrackingInfo {
            statuses_seen: 0,
            block_height: 0,
            block_hash: Hash::default(),
            has_block_meta: false,
        };

        for status in [
            SlotStatus::SlotProcessed,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCompleted,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotDead,
        ] {
            assert!(!info.has_seen_status(status));
        }

        for status in [
            SlotStatus::SlotProcessed,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCompleted,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotDead,
        ] {
            info.mark_status_seen(status);
            assert!(info.has_seen_status(status));
        }

        info.statuses_seen = 0; // Reset for next checks

        // Mark in reverse order
        for status in [
            SlotStatus::SlotDead,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotCompleted,
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotFinalized,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotProcessed,
        ] {
            assert!(!info.has_seen_status(status));
            info.mark_status_seen(status);
            assert!(info.has_seen_status(status));
        }

        info.statuses_seen = 0; // Reset again

        // Check that marking a status doesn't affect others
        info.mark_status_seen(SlotStatus::SlotConfirmed);
        assert!(!info.has_seen_status(SlotStatus::SlotProcessed));

        info.statuses_seen = 0; // Reset again
        info.mark_status_seen(SlotStatus::SlotConfirmed);
        assert!(!info.has_seen_status(SlotStatus::SlotFinalized));
    }
}
