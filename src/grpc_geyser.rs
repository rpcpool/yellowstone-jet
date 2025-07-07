use {
    crate::{
        config::ConfigUpstreamGrpc,
        metrics::jet as metrics,
        util::{fork_oneshot, BlockHeight, CommitmentLevel, IncrementalBackoff, SlotStatus},
    },
    anyhow::Context,
    futures::{
        future::{try_join, TryFutureExt},
        stream::{Stream, StreamExt},
        FutureExt,
    },
    maplit::hashmap,
    semver::{Version, VersionReq},
    serde::Deserialize,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signature::Signature,
    std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration},
    tokio::{
        sync::{
            broadcast, mpsc,
            oneshot::{self, error::TryRecvError},
            Mutex,
        },
        task::{JoinError, JoinHandle},
    },
    tonic::transport::channel::ClientTlsConfig,
    tracing::{debug, error, info, warn},
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        prelude::{
            subscribe_update::UpdateOneof, BlockHeight as GrpcBlockHeight,
            CommitmentLevel as GrpcCommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateSlot, SubscribeUpdateTransactionStatus,
        }, tonic::Status
    },
};

const QUEUE_SIZE_SLOT_UPDATE: usize = 10_000;
const QUEUE_SIZE_BLOCKMETA_UPDATE: usize = 1_000;
const QUEUE_SIZE_TRANSACTIONS: usize = 1_000_000;

// Internal tracking of slot information
// This is used to coordinate between slot updates and block metadata
// since they arrive at different times
#[derive(Debug, Default, Clone, Copy)]
struct SlotTrackingInfo {
    // Track which statuses we've seen using a bitmask
    // This allows us to know when to send block metadata updates
    statuses_seen: u8, // Bitmask for seen statuses
    // Block metadata (may arrive before or after slot status updates)
    block_height: BlockHeight,
    block_hash: Hash,
    has_block_meta: bool,
}

impl SlotTrackingInfo {
    fn mark_status_seen(&mut self, status: SlotStatus) {
        self.statuses_seen |= 1 << (status as i32 as u8);
    }

    fn has_seen_status(&self, status: SlotStatus) -> bool {
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
    inner: JoinHandle<anyhow::Result<()>>,
}

impl Future for GeyserHandle {
    type Output = Result<anyhow::Result<()>, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct GeyserSubscriber {
    block_meta_tx: broadcast::Sender<BlockMetaWithCommitment>,
    slots_tx: broadcast::Sender<SlotUpdateWithStatus>,
    transactions_rx: Arc<Mutex<Option<mpsc::Receiver<GrpcUpdateMessage>>>>,
}

impl GeyserSubscriber {
    pub fn new(
        shutdown_rx: oneshot::Receiver<()>,
        primary_grpc: ConfigUpstreamGrpc,
        secondary_grpc: ConfigUpstreamGrpc,
    ) -> (Self, GeyserHandle) {
        let (slots_tx, _) = broadcast::channel(QUEUE_SIZE_SLOT_UPDATE);
        let (block_meta_tx, _) = broadcast::channel(QUEUE_SIZE_BLOCKMETA_UPDATE);
        let (transactions_tx, transactions_rx) = mpsc::channel(QUEUE_SIZE_TRANSACTIONS);

        let geyser_handle = tokio::spawn(Self::grpc_subscribe(
            shutdown_rx,
            primary_grpc,
            secondary_grpc,
            slots_tx.clone(),
            block_meta_tx.clone(),
            transactions_tx,
        ));
        let geyser_handle = GeyserHandle {
            inner: geyser_handle,
        };

        let geyser = Self {
            slots_tx,
            block_meta_tx,
            transactions_rx: Arc::new(Mutex::new(Some(transactions_rx))),
        };

        (geyser, geyser_handle)
    }

    async fn grpc_subscribe(
        shutdown_rx: oneshot::Receiver<()>,
        primary_grpc: ConfigUpstreamGrpc,
        secondary_grpc: ConfigUpstreamGrpc,
        slots_tx: broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
    ) -> anyhow::Result<()> {
        let (shutdown1, shutdown2) = fork_oneshot(shutdown_rx);
        try_join(
            Self::grpc_subscribe_primary(
                shutdown1,
                primary_grpc.endpoint,
                primary_grpc.x_token,
                slots_tx,
                block_meta_tx,
                transactions_tx.clone(),
            ),
            Self::grpc_subscribe_secondary(
                shutdown2,
                secondary_grpc.endpoint,
                secondary_grpc.x_token,
                transactions_tx,
            ),
        )
        .await?;
        Ok(())
    }

    async fn grpc_subscribe_primary(
        mut shutdown_rx: oneshot::Receiver<()>,
        endpoint: String,
        x_token: Option<String>,
        slots_tx: broadcast::Sender<SlotUpdateWithStatus>,
        block_meta_tx: broadcast::Sender<BlockMetaWithCommitment>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
    ) -> anyhow::Result<()> {
        // This function manages two separate streams of data:
        // 1. Slot status updates (FirstShredReceived, Processed, Confirmed, etc.) - sent immediately
        // 2. Block metadata (height, hash) - sent only for commitment statuses when available
        //
        // The challenge is that these can arrive in any order:
        // - Slot status might arrive before block metadata
        // - Block metadata might arrive before commitment status
        //
        // We use SlotTrackingInfo to coordinate between these two streams
        loop {
            // Reset metrics
            metrics::grpc_slot_set(SlotStatus::SlotProcessed, 0);
            metrics::grpc_slot_set(SlotStatus::SlotConfirmed, 0);
            metrics::grpc_slot_set(SlotStatus::SlotFinalized, 0);
            metrics::grpc_slot_set(SlotStatus::SlotFirstShredReceived, 0);
            metrics::grpc_slot_set(SlotStatus::SlotCompleted, 0);
            metrics::grpc_slot_set(SlotStatus::SlotCreatedBank, 0);
            metrics::grpc_slot_set(SlotStatus::SlotDead, 0);

            let mut slot_tracking = BTreeMap::<Slot, SlotTrackingInfo>::new();
            let mut stream = tokio::select! {
                result = Self::grpc_open(&endpoint, x_token.as_deref(), true) => {
                    result?
                }
                _ = &mut shutdown_rx => return Ok(()),
            };

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    message = stream.next() => match message {
                        Some(Ok(msg)) => match msg.update_oneof {
                            Some(UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. })) => {
                                // Convert gRPC status to our SlotStatus
                                let slot_status = SlotStatus::from(status);

                                // Track that we've seen this status
                                let entry = slot_tracking.entry(slot).or_default();
                                entry.mark_status_seen(slot_status);

                                // Send slot update immediately
                                // This ensures consumers get slot status notifications as fast as possible
                                let slot_update = SlotUpdateWithStatus {
                                    slot,
                                    slot_status,
                                };
                                let _ = slots_tx.send(slot_update);
                                if transactions_tx
                                    .send(GrpcUpdateMessage::Slot(slot_update))
                                    .await
                                    .is_err()
                                    && matches!(shutdown_rx.try_recv(), Err(TryRecvError::Empty))
                                {
                                    anyhow::bail!("gRPC: failed to send slot update to transactions channel")
                                }

                                // If we have block meta for this slot and this is a commitment status,
                                // send block meta update. This handles the case where block meta
                                // arrived before the commitment status
                                if entry.has_block_meta {
                                    if let Some(commitment) = slot_status_to_commitment(slot_status) {
                                        let block_meta = BlockMetaWithCommitment {
                                            slot,
                                            block_height: entry.block_height,
                                            block_hash: entry.block_hash,
                                            commitment,
                                        };
                                        let _ = block_meta_tx.send(block_meta);
                                        if transactions_tx
                                            .send(GrpcUpdateMessage::BlockMeta(block_meta))
                                            .await
                                            .is_err()
                                            && matches!(shutdown_rx.try_recv(), Err(TryRecvError::Empty))
                                        {
                                            anyhow::bail!("gRPC: failed to send block meta to transactions channel")
                                        }
                                    }
                                }

                                metrics::grpc_slot_set(slot_status, slot);

                                // Cleanup on finalized
                                if slot_status == SlotStatus::SlotFinalized {
                                    slot_tracking = slot_tracking.split_off(&slot);
                                }
                            }
                            Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                                slot,
                                signature,
                                ..
                            })) => {
                                if transactions_tx.send(GrpcUpdateMessage::Transaction(TransactionReceived {
                                    slot,
                                    signature: Signature::try_from(signature).map_err(|error| {
                                        anyhow::anyhow!("gRPC: failed to parse signature ({endpoint}): {error:?}")
                                    })?,
                                }))
                                .await
                                .is_err() && matches!(shutdown_rx.try_recv(), Err(TryRecvError::Empty)) {
                                    anyhow::bail!("gRPC: failed to send transaction status")
                                }
                            }
                            Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                                slot,
                                blockhash,
                                block_height: Some(GrpcBlockHeight { block_height }),
                                ..
                            })) => {
                                let block_hash = match blockhash.parse() {
                                    Ok(hash) => hash,
                                    Err(error) => {
                                        anyhow::bail!("gRPC: failed to parse blockhash ({endpoint}): {error:?}");
                                    }
                                };

                                // Update tracking info
                                let entry = slot_tracking.entry(slot).or_default();
                                entry.block_height = block_height;
                                entry.block_hash = block_hash;
                                entry.has_block_meta = true;

                                // Send block meta updates for any commitment statuses we've already seen
                                // This handles the case where slot status updates arrived before block meta
                                for status in [SlotStatus::SlotProcessed, SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized] {
                                    if entry.has_seen_status(status) {
                                        if let Some(commitment) = slot_status_to_commitment(status) {
                                            let block_meta = BlockMetaWithCommitment {
                                                slot,
                                                block_height,
                                                block_hash,
                                                commitment,
                                            };
                                            let _ = block_meta_tx.send(block_meta);
                                            if transactions_tx
                                                .send(GrpcUpdateMessage::BlockMeta(block_meta))
                                                .await
                                                .is_err()
                                                && matches!(shutdown_rx.try_recv(), Err(TryRecvError::Empty))
                                            {
                                                anyhow::bail!("gRPC: failed to send block meta to transactions channel")
                                            }
                                        }
                                    }
                                }
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                debug!("ping received {endpoint}");
                            }
                            _ => {
                                anyhow::bail!("gRPC: received unexpected message ({endpoint}): {msg:?}");
                            }
                        },
                        Some(Err(error)) => {
                            error!("gRPC: receive message error ({endpoint}): {error:?}");
                            break;
                        },
                        None => {
                            error!("gRPC: unexpected ending of the stream ({endpoint})");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn grpc_subscribe_secondary(
        mut shutdown_rx: oneshot::Receiver<()>,
        endpoint: String,
        x_token: Option<String>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
    ) -> anyhow::Result<()> {
        loop {
            let mut stream = tokio::select! {
                result = Self::grpc_open(&endpoint, x_token.as_deref(), false) => {
                    result?
                }
                _ = &mut shutdown_rx => return Ok(()),
            };
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    message = stream.next() => match message {
                        Some(Ok(msg)) => match msg.update_oneof {
                            Some(UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                                slot,
                                signature,
                                ..
                            })) => {
                                if transactions_tx.send(GrpcUpdateMessage::Transaction(TransactionReceived {
                                    slot,
                                    signature: Signature::try_from(signature).map_err(|error| {
                                        anyhow::anyhow!("gRPC: failed to parse signature ({endpoint}): {error:?}")
                                    })?,
                                }))
                                .await
                                .is_err() && matches!(shutdown_rx.try_recv(), Err(TryRecvError::Empty)) {
                                    anyhow::bail!("gRPC: failed to send transaction status")
                                }
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                debug!("ping received {endpoint}");
                                continue;
                            }
                            _ => {
                                anyhow::bail!("gRPC: received unexpected message ({endpoint}): {msg:?}");
                            }
                        },
                        Some(Err(error)) => {
                            error!("gRPC: receive message error ({endpoint}): {error:?}");
                            break;
                        },
                        None => {
                            error!("gRPC: unexpected ending of the stream ({endpoint})");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn grpc_open(
        endpoint: &str,
        x_token: Option<&str>,
        full: bool,
    ) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        let mut backoff = IncrementalBackoff::default();
        loop {
            backoff.maybe_tick().await;

            let mut client = match async {
                GeyserGrpcClient::build_from_shared(endpoint.to_string())
                    .and_then(|builder| builder.x_token(x_token))
            }
            .and_then(|builder| async {
                builder
                    .max_decoding_message_size(128 * 1024 * 1024) // 128MiB
                    .connect_timeout(Duration::from_secs(3))
                    .timeout(Duration::from_secs(3))
                    .tls_config(ClientTlsConfig::new().with_native_roots())?
                    .connect()
                    .await
            })
            .await
            {
                Ok(client) => {
                    backoff.reset();
                    client
                }
                Err(error) => {
                    warn!("failed to connect ({endpoint}): {error:?}");
                    backoff.init();
                    continue;
                }
            };
            Self::validate_version(&mut client)
                .await
                .with_context(|| format!("invalid version for endpoint: {endpoint}"))?;

            let (slots, blocks_meta) = if full {
                (
                    hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                        filter_by_commitment: Some(false),
                        interslot_updates: Some(true), // Get all slot statuses including FirstShredReceived
                    } },
                    hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
                )
            } else {
                (hashmap! {}, hashmap! {})
            };

            match client.subscribe_once(SubscribeRequest {
                slots,
                transactions_status: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
                blocks_meta,
                commitment: Some(GrpcCommitmentLevel::Processed as i32),
                ..SubscribeRequest::default()
            }).await {
                Ok(stream) => {
                    if full {
                        info!("subscribed on slot (all statuses), transactions statuses and blocks meta ({endpoint})");
                    } else {
                        info!("subscribed on transactions statuses ({endpoint})");
                    }
                    return Ok(stream);
                }
                Err(error) => warn!("failed to subscribe ({endpoint}): {error:?}"),
            }
        }
    }

    async fn validate_version(
        geyser: &mut GeyserGrpcClient<impl Interceptor>,
    ) -> anyhow::Result<()> {
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
            .context("failed to get gRPC version")?;
        let version = match serde_json::from_str::<GrpcVersion>(&response.version)
            .context("failed to parse gRPC version")?
        {
            GrpcVersion::Old(s) => s.version,
            GrpcVersion::New(s) => s.version.version,
        };

        let version = Version::parse(&version)?;
        let required =
            VersionReq::parse(">=1.14.1").context("failed to parse required gRPC version")?;
        anyhow::ensure!(
            required.matches(&version),
            "connected gRPC ({version}) doesn't match to required version `{required}`"
        );

        Ok(())
    }
}

// Helper function to convert SlotStatus to CommitmentLevel
// Only Processed, Confirmed, and Finalized map to commitment levels
// Other statuses (FirstShredReceived, etc.) are slot-only events
fn slot_status_to_commitment(status: SlotStatus) -> Option<CommitmentLevel> {
    match status {
        SlotStatus::SlotProcessed => Some(CommitmentLevel::Processed),
        SlotStatus::SlotConfirmed => Some(CommitmentLevel::Confirmed),
        SlotStatus::SlotFinalized => Some(CommitmentLevel::Finalized),
        _ => None, // FirstShredReceived, Completed, CreatedBank, Dead don't map to commitment levels
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
        self.slots_tx.subscribe()
    }

    fn subscribe_block_meta(&self) -> broadcast::Receiver<BlockMetaWithCommitment> {
        self.block_meta_tx.subscribe()
    }

    async fn subscribe_transactions(&self) -> Option<mpsc::Receiver<GrpcUpdateMessage>> {
        self.transactions_rx.lock().await.take()
    }
}
