use {
    crate::{
        config::ConfigUpstreamGrpc,
        metrics::jet as metrics,
        util::{fork_oneshot, BlockHeight, CommitmentLevel, IncrementalBackoff},
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
    solana_sdk::{clock::Slot, hash::Hash, signature::Signature},
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
        },
        tonic::Status,
    },
};

const QUEUE_SIZE_SLOT_UPDATE: usize = 10_000;
const QUEUE_SIZE_TRANSACTIONS: usize = 1_000_000;

#[derive(Debug, Default, Clone, Copy)]
struct SlotUpdateInfo {
    slot: bool,
    block_height: BlockHeight,
    block_hash: Hash,
}

#[derive(Debug, Clone, Copy)]
pub struct SlotUpdateInfoWithCommitment {
    pub slot: Slot,
    pub block_height: BlockHeight,
    pub block_hash: Hash,
    pub commitment: CommitmentLevel,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionReceived {
    pub slot: Slot,
    pub signature: Signature,
}

#[derive(Debug, Clone, Copy)]
pub enum GrpcUpdateMessage {
    Slot(SlotUpdateInfoWithCommitment),
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
    slots_tx: broadcast::Sender<SlotUpdateInfoWithCommitment>,
    transactions_rx: Arc<Mutex<Option<mpsc::Receiver<GrpcUpdateMessage>>>>,
    // shutdown_tx: broadcast::Sender<()>,
    // join_handle: WaitShutdownSharedJoinHandle,
}

// impl WaitShutdown for GeyserSubscriber {
//     fn shutdown(&self) {
//         let _ = self.shutdown_tx.send(());
//     }

//     async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
//         let mut locked = self.join_handle.lock().await;
//         locked.deref_mut().await
//     }
// }

impl GeyserSubscriber {
    pub fn new(
        shutdown_rx: oneshot::Receiver<()>,
        primary_grpc: ConfigUpstreamGrpc,
        secondary_grpc: ConfigUpstreamGrpc,
    ) -> (Self, GeyserHandle) {
        // let (shutdown_tx, _) = broadcast::channel(1);

        let (slots_tx, _) = broadcast::channel(QUEUE_SIZE_SLOT_UPDATE);
        let (transactions_tx, transactions_rx) = mpsc::channel(QUEUE_SIZE_TRANSACTIONS);

        let geyser_handle = tokio::spawn(Self::grpc_subscribe(
            // Arc::new(AtomicBool::new(false)),
            // shutdown_tx.clone(),
            shutdown_rx,
            primary_grpc,
            secondary_grpc,
            slots_tx.clone(),
            transactions_tx,
        ));
        let geyser_handle = GeyserHandle {
            inner: geyser_handle,
        };

        let geyser = Self {
            slots_tx: slots_tx.clone(),
            transactions_rx: Arc::new(Mutex::new(Some(transactions_rx))),
        };

        (geyser, geyser_handle)
    }

    pub fn subscribe_slots(&self) -> broadcast::Receiver<SlotUpdateInfoWithCommitment> {
        self.slots_tx.subscribe()
    }

    pub async fn subscribe_transactions(&self) -> Option<mpsc::Receiver<GrpcUpdateMessage>> {
        self.transactions_rx.lock().await.take()
    }

    async fn grpc_subscribe(
        shutdown_rx: oneshot::Receiver<()>,
        primary_grpc: ConfigUpstreamGrpc,
        secondary_grpc: ConfigUpstreamGrpc,
        slots_tx: broadcast::Sender<SlotUpdateInfoWithCommitment>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
    ) -> anyhow::Result<()> {
        let (shutdown1, shutdown2) = fork_oneshot(shutdown_rx);
        try_join(
            Self::grpc_subscribe_primary(
                shutdown1,
                primary_grpc.endpoint,
                primary_grpc.x_token,
                slots_tx,
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
        slots_tx: broadcast::Sender<SlotUpdateInfoWithCommitment>,
        transactions_tx: mpsc::Sender<GrpcUpdateMessage>,
    ) -> anyhow::Result<()> {
        loop {
            metrics::grpc_slot_set(CommitmentLevel::Processed, 0);
            metrics::grpc_slot_set(CommitmentLevel::Confirmed, 0);
            metrics::grpc_slot_set(CommitmentLevel::Finalized, 0);

            let mut slot_updates = BTreeMap::<Slot, SlotUpdateInfo>::new();
            let mut stream = tokio::select! {
                result = Self::grpc_open(&endpoint, x_token.as_deref(), true) => {
                    result?
                }
                _ = &mut shutdown_rx => return Ok(()),
            };
            loop {
                let (slot, slot_info, commitment) = tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    message = stream.next() => match message {
                        Some(Ok(msg)) => match msg.update_oneof {
                            Some(UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. })) => {
                                let entry = slot_updates.entry(slot).or_default();
                                entry.slot = true;

                                let commitment = match GrpcCommitmentLevel::try_from(status) {
                                    Ok(GrpcCommitmentLevel::Processed) => CommitmentLevel::Processed,
                                    Ok(GrpcCommitmentLevel::Confirmed) => CommitmentLevel::Confirmed,
                                    Ok(GrpcCommitmentLevel::Finalized) => CommitmentLevel::Finalized,
                                    Ok(_) => continue,
                                    Err(error) => {
                                        anyhow::bail!("gRPC: failed to parse commitment level ({endpoint}): {error:?}")
                                    }
                                };
                                (slot, *entry, commitment)
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
                                continue;
                            }
                            Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                                slot,
                                blockhash,
                                block_height: Some(GrpcBlockHeight { block_height }),
                                ..
                            })) => {
                                let entry = slot_updates.entry(slot).or_default();
                                entry.block_height = block_height;
                                entry.block_hash = match blockhash.parse() {
                                    Ok(hash) => hash,
                                    Err(error) => {
                                        anyhow::bail!("gRPC: failed to parse blockhash ({endpoint}): {error:?}");
                                    }
                                };
                                (slot, *entry, CommitmentLevel::Processed)
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
                };

                if slot_info.slot && slot_info.block_height != 0 {
                    let slot_update = SlotUpdateInfoWithCommitment {
                        slot,
                        block_height: slot_info.block_height,
                        block_hash: slot_info.block_hash,
                        commitment,
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
                }

                metrics::grpc_slot_set(commitment, slot);
                if commitment == CommitmentLevel::Finalized {
                    slot_updates = slot_updates.split_off(&slot);
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
                    .max_decoding_message_size(128 * 1024 * 1024) // 128MiB, BlockMeta with rewards can be bigger than 60MiB
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
                    hashmap! { "".to_owned() => SubscribeRequestFilterSlots::default() },
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
                        info!("subscribed on slot, transactions statuses and blocks meta ({endpoint})");
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
