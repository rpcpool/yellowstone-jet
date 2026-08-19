use {
    crate::{
        config::{TpuPortKind, TpuSenderConfig},
        core::{
            Nothing, StakeBasedEvictionStrategy, TpuSenderResponseCallback, TpuSenderTxn,
            TpuSenderTxnInfo, UpdateIdentity,
        },
        rpc::{
            schedule::{
                ManagedLeaderSchedule, ManagedLeaderScheduleConfig, spawn_managed_leader_schedule,
            },
            solana_rpc_utils::RetryRpcSender,
            stake::{RpcValidatorStakeInfoServiceConfig, rpc_validator_stake_info_service},
            tpu_info::{RpcClusterTpuQuicInfoServiceConfig, rpc_cluster_tpu_info_service},
        },
        sender::{PollTpuSender, create_base_tpu_client},
        slot::SlotTracker,
        yellowstone_grpc::{
            schedule::YellowstoneUpcomingLeader,
            slot_tracker::{self},
        },
    },
    bytes::Bytes,
    derive_more::Display,
    futures::future,
    serde::Deserialize,
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client, rpc_client::RpcClientConfig,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        collections::{BTreeSet, HashSet},
        fmt,
        net::SocketAddr,
        sync::Arc,
        task::{Context, Poll},
    },
    tokio_util::sync::CancellationToken,
    url::Url,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcClient},
};

pub const DEFAULT_TPU_SENDER_CHANNEL_CAPACITY: usize = 1000;

///
/// Configuration object for [`YellowstoneTpuSender`].
///
#[derive(Debug, Clone, Deserialize)]
pub struct YellowstoneTpuSenderConfig {
    ///
    /// TPU-Quic event-loop configuration options.
    ///
    pub tpu: TpuSenderConfig,
    ///
    /// Configuration for internal [`crate::rpc::tpu_info::RpcClusterTpuQuicInfoService`]
    ///
    pub tpu_info: RpcClusterTpuQuicInfoServiceConfig,
    ///
    /// Configuration for internal [`crate::rpc::schedule::ManagedLeaderSchedule`]
    ///
    pub schedule: ManagedLeaderScheduleConfig,
    ///
    /// Configuration for internal [`crate::rpc::stake::RpcValidatorStakeInfoService`]
    ///
    pub stake: RpcValidatorStakeInfoServiceConfig,
    ///
    /// Capacity of the internal channel used to send transactions to the TPU sender task.
    ///
    pub channel_capacity: usize,
    ///
    /// Endpoints for RPC and gRPC services.
    pub endpoints: Endpoints,
}

impl Default for YellowstoneTpuSenderConfig {
    fn default() -> Self {
        Self {
            tpu: Default::default(),
            tpu_info: Default::default(),
            schedule: Default::default(),
            stake: Default::default(),
            channel_capacity: DEFAULT_TPU_SENDER_CHANNEL_CAPACITY,
            endpoints: Endpoints::default(),
        }
    }
}

///
/// Error cases of [`create_yellowstone_tpu_sender`] and [`create_yellowstone_tpu_sender_with_clients`].
///
#[derive(thiserror::Error, Debug)]
pub enum CreateTpuSenderError {
    ///
    /// Error caused by [`rpc_client::RpcClient`] API call.
    ///
    #[error(transparent)]
    RpcClientError(#[from] ClientError),
    ///
    /// Error caused by [`yellowstone_grpc_client::GeyserGrpcClient`] API call.
    ///
    #[error(transparent)]
    YellowstoneGrpcError(#[from] yellowstone_grpc_client::GeyserGrpcClientError),
    ///
    /// Raised when subscribing to a remote Yellowstone gRPC Subscription ended.
    ///
    #[error("geyser client returned empty slot tracker stream")]
    GeyserSubscriptionEnded,
}

///
/// A fully-featured _smart_ TPU sender using Yellowstone services.
///
/// This tpu-sender is aware of the leader schedule and the current ledger tip.
///
/// This allow this object to route transaction directly to the current/upcoming leader(s)
///
/// See [`create_yellowstone_tpu_sender`] for creation.
///
/// # Example
///
/// ```ignore
///
/// let my_identity = solana_keypair::read_keypair_file("/path/to/my/id.json").expect("read_keypair_file");
///
/// let NewYellowstoneTpuSender {
///     sender,
///     related_objects_jh: _,
/// } = create_yellowstone_tpu_sender(
///     Default::default(),
///     my_identity,
///     Endpoints {
///         rpc: "https://my.rpc.endpoint".to_string(),
///         grpc: "https://my.grpc.endpoint".to_string(),
///         grpc_x_token: Some("my-secret".to_string()),
///     }
/// ).await.expect("yellowstone-tpu-sender");
///
/// let rpc_client = rpc_client::RpcClient::new(
///     "https://api.mainnet-beta.solana.com",
///     CommitmentConfig::confirmed(),
/// );
///
/// let latest_blockhash = rpc_client
///     .get_latest_blockhash()
///     .await
///     .expect("get_latest_blockhash");
///
/// let instructions = vec![transfer(&identity.pubkey(), &recipient, lamports)];
/// let transaction = VersionedTransaction::try_new(
///     VersionedMessage::V0(
///         v0::Message::try_compile(&identity.pubkey(), &instructions, &[], latest_blockhash)
///             .expect("try_compile"),
///     ),
///     &[&identity],
/// )
/// .expect("try_new");
/// let signature = transaction.signatures[0];
/// tracing::info!("generate transaction {signature} with send lamports {lamports}");
/// let bincoded_txn = bincode::serialize(&transaction).expect("bincode::serialize");
/// let txn_info = TpuSenderTxnInfo::new(signature);
/// sender
///     .send_txn(bincoded_txn, Some(txn_info))
///     .await
///     .expect("send_transaction");
/// ```
///
/// # Send with blocklist
///
/// You can provide a blocklist to prevent sending to specific leaders.
///
/// ```ignore
///
/// let leader_to_block = vec![Pubkey::from_str("HEL1UZMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU").expect("from_str")];
/// sender
///     .send_txn_with_blocklist(
///         bincoded_txn,
///         Some(leader_to_block),
///         Some(TpuSenderTxnInfo::new(signature)),
///     )
///     .await;
/// ```
///
/// If you are using [Yellowstone Shield crate](https://crates.io/crates/yellowstone-shield-store),
/// you can enable `shield` feature flag and use Shield policies as blocklist:
///
/// ```ignore
///
/// let policy_store: yellowstone_shield_store::PolicyStore = <...>;
/// let policies = vec![
///     Pubkey::from_str("PolicyPubkey1...").expect("from_str"),
///     Pubkey::from_str("PolicyPubkey2...").expect("from_str"),
/// ];
///
/// let shield_blocklist = ShieldBlockList {
///     policy_store: &policy_store,
///     shield_policy_addresses: &policies,
///     default_return_value: true, // allow sending when in doubt
/// };
///
/// sender
///     .send_txn_with_shield_policies(
///         bincoded_txn,
///         shield_blocklist,
///         Some(TpuSenderTxnInfo::new(signature)),
///     )
///     .await;
///
/// ```
///
/// # Broadcast sending
///
/// ```ignore
/// let dests = vec![
///     Pubkey::from_str("2nhGaJvR17TeytzJVajPfABHQcAwinKoCG8F69gRdQot").expect("from_str"),
///     Pubkey::from_str("EdGevanA2MZsDpxDXK6b36FH7RCcTuDZZRcc6MEyE9hy").expect("from_str"),
/// ];
///
/// sender
///     .send_txn_many_dest(
///         bincoded_txn,
///         &dests,
///         Some(TpuSenderTxnInfo::new(signature)),
///     )
///     .await;
///
/// ```
///
///
/// # Callbacks on TPU responses
///
/// You can provide an implementation of [`TpuSenderResponseCallback`] when creating the TPU sender.
/// This callback will be invoked for each response received from the TPU, including failed and dropped transactions.
///
/// This module provides a default implementation that sends the responses to a provided [`tokio::sync::mpsc::UnboundedSender`].
///
/// ```ignore
/// let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel::<TpuSenderResponse>();
///
/// let NewYellowstoneTpuSender {
///     sender,
///     related_objects_jh: _,
/// } = create_yellowstone_tpu_sender_with_callback(
///     Default::default(),
///     my_identity,
///     Endpoints {
///         rpc: "https://my.rpc.endpoint".to_string(),
///         grpc: "https://my.grpc.endpoint".to_string(),
///         grpc_x_token: Some("my-secret".to_string()),
///     },
///     callback_tx,
/// ).await.expect("yellowstone-tpu-sender");
///
/// // In another task, receive the responses
/// callback_task = tokio::spawn(async move {
///     while let Some(response) = callback_rx.recv().await {
///         tracing::info!("Received TPU sender response: {:?}", response);
///     }
/// });
/// ```
///
/// ## Custom callback implementation
///
/// You can also implement your own callback by implementing the [`TpuSenderResponseCallback`] trait.
///
/// ```rust
/// #[derive(Clone)]
/// struct LoggingCallback;
///
/// impl LoggingCallback {
///     fn signature_from_info(
///         info: &Option<TpuSenderTxnInfo>,
///     ) -> Option<Signature> {
///         info.as_ref()
///             .and_then(|txn_info| txn_info.downcast_ref::<Signature>())
///             .copied()
///     }
/// }
///
/// impl TpuSenderResponseCallback for LoggingCallback {
///     fn call(&self, response: TpuSenderResponse) {
///         use std::io::Write;
///         let mut stdout = std::io::stdout();
///         match response {
///             TpuSenderResponse::TxSent(info) => {
///                 if let Some(sig) = Self::signature_from_info(&info.info) {
///                     writeln!(
///                         &mut stdout,
///                         "Transaction {} send to {}",
///                         sig,
///                         info.remote_peer_identity
///                     )
///                     .expect("writeln");
///                 }
///             }
///             TpuSenderResponse::TxFailed(info) => {
///                 if let Some(sig) = Self::signature_from_info(&info.info) {
///                     writeln!(&mut stdout, "Transaction failed: {}", sig).expect("writeln");
///                 }
///             }
///             TpuSenderResponse::TxDrop(info) => {
///                 for (txn, _) in info.dropped_tx_vec {
///                     if let Some(sig) = Self::signature_from_info(&txn.info) {
///                         writeln!(&mut stdout, "Transaction dropped: {}", sig)
///                             .expect("writeln");
///                     }
///                 }
///             }
///         }
///     }
/// }
/// ```
///
/// # `TpuSenderTxnInfo`
///
/// `TpuSenderTxnInfo` is the typed metadata container attached to each send request.
/// It is propagated to all response variants so callers can correlate responses with
/// their own request context.
///
/// ```ignore
/// #[derive(Clone, Copy, Debug)]
/// struct TxMeta {
///     signature: Signature,
///     shard: u8,
/// }
///
/// let meta = TxMeta {
///     signature,
///     shard: 2,
/// };
///
/// sender
///     .send_txn(
///         bincoded_txn,
///         Some(TpuSenderTxnInfo::new(meta)),
///     )
///     .await
///     .expect("send_txn");
///
/// // Later, decode the same metadata from a callback/receiver response.
/// match response {
///     TpuSenderResponse::TxSent(ok) => {
///         if let Some(meta) = ok.info.as_ref().and_then(|i| i.downcast_ref::<TxMeta>()) {
///             tracing::info!("sent {} shard {}", meta.signature, meta.shard);
///         }
///     }
///     TpuSenderResponse::TxFailed(err) => {
///         if let Some(meta) = err.info.as_ref().and_then(|i| i.downcast_ref::<TxMeta>()) {
///             tracing::warn!("failed {} shard {}", meta.signature, meta.shard);
///         }
///     }
///     TpuSenderResponse::TxDrop(drop) => {
///         for (txn, _) in drop.dropped_tx_vec {
///             if let Some(meta) = txn.info.as_ref().and_then(|i| i.downcast_ref::<TxMeta>()) {
///                 tracing::warn!("dropped {} shard {}", meta.signature, meta.shard);
///             }
///         }
///     }
/// }
/// ```
///
/// # `&mut self`
///
/// All methods of this struct take `&mut self` because the internal state of the sender may change due to [update_identity](`crate::yellowstone_grpc::sender::YellowstoneTpuSender::update_identity`) calls.
/// Updating identity typically requires carefully synchronizing with custom application logic, making `&mut self` appropriate to prevent concurrent usage.
///
/// If you need concurrent access to the sender, consider cloning the sender as it is cheaply-cloneable.
///
/// # Clone
///
/// This struct is cheaply-cloneable and can be shared between threads.
#[derive(Clone)]
pub struct YellowstoneTpuSender {
    base_tpu_sender: PollTpuSender,
    ///
    /// If true, coalesce multiple sends to the same remote tpu socket address into a single send.
    ///
    /// Default is true.
    ///
    /// # Multplexing Note
    ///
    /// Some validators in the network may share the same TPU address because they may have TPU proxy in front of them.
    /// In this case, sending multiple transactions to different validators sharing the same address may be redundant.
    /// By enabling this option, the sender will coalesce multiple sends to the same address into a single send, reducing network overhead.
    ///
    coalesce_send_many_tpu_port_collision: bool,
    atomic_slot_tracker: SlotTracker,
    leader_schedule: ManagedLeaderSchedule,
    leader_tpu_info: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync>,
    tpu_port_kind: TpuPortKind,
    _on_drop: Option<Arc<YellowstoneTpuSenderLifecycle>>,
}

#[derive(Default)]
struct YellowstoneTpuSenderLifecycle {
    shutdown: CancellationToken,
}

impl Drop for YellowstoneTpuSenderLifecycle {
    fn drop(&mut self) {
        // This runs once, when the last sender lifecycle Arc is dropped.
        self.shutdown.cancel();
    }
}

///
/// Error case when the leader for a transaction is unknown.
///
#[derive(thiserror::Error)]
#[error("unknown leader {unknown_leader} for transaction")]
pub struct UnknownLeaderError {
    txn: Bytes,
    unknown_leader: Pubkey,
}

impl fmt::Debug for UnknownLeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown leader: {}", self.unknown_leader)
    }
}

///
/// Error case for [`YellowstoneTpuSender`]'s transaction sending API.
///
/// See [`YellowstoneTpuSender::send_txn`] for more details.
///
#[derive(Debug, Display)]
pub enum SendErrorKind {
    ///
    /// The channel between [`YellowstoneTpuSender`] and the actual tpu event-loop is closed.
    #[display("tpu sender disconnected")]
    Closed,
    ///
    /// The internal slot tracked closed, await [`NewYellowstoneTpuSender::related_objects_jh`] to get more information about the error.
    ///
    #[display("slot tracker disconnected")]
    SlotTrackerDisconnected,
    ///
    /// The internal managed leader schedule got poisoned, await [`NewYellowstoneTpuSender::related_objects_jh`] to get more information about the error.
    ///
    #[display("managed leader schedule disconnected")]
    ManagedLeaderScheduleDisconnected,
    ///
    /// No remote peers currently matched the user-provided `Blocklist`.
    #[display("destination(s) blocked")]
    RemotePeerBlocked,
}

///
/// Error returned when sending a transaction with [`YellowstoneTpuSender`]'s transaction sending API.
///
#[derive(Debug, thiserror::Error)]
#[error("{kind} for transaction")]
pub struct SendError {
    ///
    /// Kind of send error.
    ///
    pub kind: SendErrorKind,
    ///
    /// The transaction that failed to be sent.
    ///
    pub txn: Bytes,
}

///
/// Base trait to implements custom Blocklist
///
pub trait Blocklist {
    ///
    /// Returns true if `peer_address` should be blocked.
    ///
    fn is_blocked(&self, peer_address: &Pubkey) -> bool;
}

impl Blocklist for HashSet<Pubkey> {
    fn is_blocked(&self, pubkey: &Pubkey) -> bool {
        self.contains(pubkey)
    }
}

impl Blocklist for BTreeSet<Pubkey> {
    fn is_blocked(&self, peer_address: &Pubkey) -> bool {
        self.contains(peer_address)
    }
}

impl<V> Blocklist for std::collections::HashMap<Pubkey, V> {
    fn is_blocked(&self, peer_address: &Pubkey) -> bool {
        self.contains_key(peer_address)
    }
}

impl Blocklist for Vec<Pubkey> {
    fn is_blocked(&self, peer_address: &Pubkey) -> bool {
        self.contains(peer_address)
    }
}

impl Blocklist for &[Pubkey] {
    fn is_blocked(&self, peer_address: &Pubkey) -> bool {
        self.contains(peer_address)
    }
}

///
/// A blocklist that is empty, equivalent of a pass-through filter.
///
pub struct NoBlocklist;

impl Blocklist for NoBlocklist {
    ///
    /// Always returns false, indicating no pubkey is blocked.
    ///
    fn is_blocked(&self, _pubkey: &Pubkey) -> bool {
        false
    }
}

#[cfg_attr(
    docsrs,
    doc(cfg(feature = "shield", doc = "only if `shield` feature-flag is enabled"))
)]
#[cfg(feature = "shield")]
///
/// Yellowstone Shield blocklist implementation, enabled with `shield` feature-flag.
///
pub struct ShieldBlockList<'a> {
    ///
    /// Reference to the [`yellowstone_shield_store::PolicyStore`].
    ///
    pub policy_store: &'a yellowstone_shield_store::PolicyStore,
    ///
    /// List of shield policies to check against.
    ///
    pub shield_policy_addresses: &'a [Pubkey],
    ///
    /// Default return value when [`yellowstone_shield_store::PolicyStore`] lookup fails.
    /// recommended to be `true` to allow sending when in doubt.
    ///
    pub default_return_value: bool,
}

#[cfg_attr(docsrs, doc(cfg(feature = "shield")))]
#[cfg(feature = "shield")]
impl Blocklist for ShieldBlockList<'_> {
    fn is_blocked(&self, peer_address: &Pubkey) -> bool {
        use yellowstone_shield_store::PolicyStoreTrait;

        !self
            .policy_store
            .snapshot()
            .is_allowed(self.shield_policy_addresses, peer_address)
            .unwrap_or(self.default_return_value)
    }
}

impl YellowstoneTpuSender {
    ///
    /// Creates a new [`YellowstoneTpuSender`] with the provided configuration and initial identity.
    ///
    /// # Arguments
    ///
    /// - `config`: Configuration for the TPU sender [`YellowstoneTpuSenderConfig`], including TPU event-loop, RPC, gRPC, and other settings.
    /// - `initial_identity`: The initial [`Keypair`] identity to use for sending transactions.
    ///
    /// # Notes
    ///
    /// They initial [`Keypair`] could be a temporary identity, and you can update it later using [`YellowstoneTpuSender::update_identity`].
    /// You can generate one easily via [`Keypair::new()`].
    ///
    pub async fn connect(
        config: YellowstoneTpuSenderConfig,
        initial_identity: Keypair,
    ) -> Result<YellowstoneTpuSender, CreateTpuSenderError> {
        let NewYellowstoneTpuSender {
            sender,
            related_objects_jh: _,
        } = create_yellowstone_tpu_sender(config, initial_identity).await?;
        Ok(sender)
    }

    ///
    /// Creates a new [`YellowstoneTpuSender`] with the provided configuration, initial identity, and a callback for TPU responses.
    ///
    /// # Arguments
    ///
    /// - `config`: Configuration for the TPU sender [`YellowstoneTpuSenderConfig`], including TPU event-loop, RPC, gRPC, and other settings.
    /// - `initial_identity`: The initial [`Keypair`] identity to use for sending transactions.
    /// - `callback`: An implementation of [`TpuSenderResponseCallback`] that will be invoked for each response received from the TPU, including successful sends, failures, and dropped transactions.
    ///
    pub async fn connect_with_callback(
        config: YellowstoneTpuSenderConfig,
        initial_identity: Keypair,
        callback: impl TpuSenderResponseCallback + 'static,
    ) -> Result<YellowstoneTpuSender, CreateTpuSenderError> {
        let NewYellowstoneTpuSender {
            sender,
            related_objects_jh: _,
        } = create_yellowstone_tpu_sender_with_callback(config, initial_identity, callback).await?;
        Ok(sender)
    }

    pub fn from_parts(
        tpu_sender: impl Into<PollTpuSender>,
        leader_tpu_info: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync>,
        managed_leader_schedule: ManagedLeaderSchedule,
        atomic_slot_tracker: SlotTracker,
        tpu_port_kind: TpuPortKind,
    ) -> Self {
        YellowstoneTpuSender {
            base_tpu_sender: tpu_sender.into(),
            leader_tpu_info,
            atomic_slot_tracker,
            coalesce_send_many_tpu_port_collision: true,
            leader_schedule: managed_leader_schedule,
            tpu_port_kind,
            _on_drop: None,
        }
    }

    ///
    /// Sends a transaction to the specified destinations.
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    /// * `dests` - The list of destination pubkeys to send the transaction to.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully to all destinations, or a `SendError` if there was an error.
    ///
    /// # Note
    ///
    /// If `dests` is empty, the function returns `Ok(())` immediately
    ///
    pub async fn send_txn_many_dest<T>(
        &mut self,
        txn: T,
        dests: &[Pubkey],
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        if dests.is_empty() {
            return Ok(());
        }

        let wire_txn = Bytes::from_owner(txn);
        let mut dest_addr_vec = Vec::with_capacity(dests.len());

        for dest in dests {
            if let Some(addr) = self
                .leader_tpu_info
                .get_quic_dest_addr(dest, self.tpu_port_kind)
            {
                if self.coalesce_send_many_tpu_port_collision && dest_addr_vec.contains(&addr) {
                    // Skip duplicate address when coalescing is enabled
                    continue;
                }
                dest_addr_vec.push(addr);
            }
            let tpu_txn = TpuSenderTxn {
                remote_peer: *dest,
                wire: wire_txn.clone(),
                info: txn_info,
            };
            if future::poll_fn(|cx| self.base_tpu_sender.poll_reserve(cx))
                .await
                .is_err()
            {
                return Err(SendError {
                    kind: SendErrorKind::Closed,
                    txn: wire_txn,
                });
            } else {
                self.base_tpu_sender
                    .send_item(tpu_txn)
                    .map_err(|e| SendError {
                        kind: SendErrorKind::Closed,
                        txn: e
                            .into_inner()
                            .expect("send_item should return back txn")
                            .wire,
                    })?;
            }
        }
        Ok(())
    }

    ///
    /// Sends a transaction to the TPUs of the current leader and to the next leader iff near the slot boundary (2/4 slots).
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    /// * `blocklist` - (Optional) [`Blocklist`], if provided, prevent a transaction from being sent to a disallow remote peer.
    ///
    /// # Note
    ///
    /// The fanout succeed if the sender can schedule at least one send to a leader.
    ///
    pub async fn send_txn_fanout_with_blocklist<T, B>(
        &mut self,
        txn: T,
        blocklist: Option<B>,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
        B: Blocklist,
    {
        let wire_txn = Bytes::from_owner(txn);
        let current_slot = match self.atomic_slot_tracker.load() {
            Ok(slot) => slot,
            Err(_) => {
                return Err(SendError {
                    kind: SendErrorKind::SlotTrackerDisconnected,
                    txn: wire_txn,
                });
            }
        };
        let reminder = current_slot % 4;
        let floor_leader_boundary = current_slot.saturating_sub(reminder);

        // Each leader gets 4 slots
        // If we are near the boundary (2/4), we need to send to the next leader as well
        let n = if reminder >= 2 { 2 } else { 1 };

        let mut blocked_cnt = 0;
        let result = (0..n)
            .map(|i| floor_leader_boundary + (i * 4) as u64)
            .map(|leader_slot_boundary| self.leader_schedule.get_leader(leader_slot_boundary))
            .filter_map(|res| match res {
                Ok(None) => {
                    panic!("unknown leader for slot boundary {floor_leader_boundary}");
                }
                Ok(Some(leader)) => {
                    if let Some(blocklist) = &blocklist {
                        if blocklist.is_blocked(&leader) {
                            blocked_cnt += 1;
                            None
                        } else {
                            Some(Ok(leader))
                        }
                    } else {
                        Some(Ok(leader))
                    }
                }
                Err(_) => Some(Err(SendErrorKind::ManagedLeaderScheduleDisconnected)),
            })
            .collect::<Result<Vec<_>, SendErrorKind>>();

        match result {
            Ok(leaders) => {
                if leaders.is_empty() && blocked_cnt > 0 {
                    Err(SendError {
                        kind: SendErrorKind::RemotePeerBlocked,
                        txn: wire_txn,
                    })
                } else {
                    self.send_txn_many_dest(wire_txn, &leaders, txn_info).await
                }
            }
            Err(err_kind) => Err(SendError {
                kind: err_kind,
                txn: wire_txn,
            }),
        }
    }

    ///
    /// Sets whether to coalesce multiple sends to the same remote tpu socket address into a single send.
    /// It is set to true by default as it prevents fragmentation edge cases.
    ///
    /// # Arguments
    ///
    /// * `coalesce` - If true, coalesce multiple sends to the same remote tpu socket address.
    ///
    ///
    /// # Multplexing Note
    ///
    /// Some validators in the network may share the same TPU address because they may have TPU proxy in front of them.
    /// In this case, sending multiple transactions to different validators sharing the same address may be redundant.
    /// By enabling this option, the sender will coalesce multiple sends to the same address into
    ///
    pub const fn set_coalesce_many_dest_collision(&mut self, coalesce: bool) {
        self.coalesce_send_many_tpu_port_collision = coalesce;
    }

    ///
    /// Sends a transaction to the TPU of the current leader.
    ///
    /// Same as calling [`YellowstoneTpuSender::send_txn_with_blocklist`] with `Some(NoBlocklist)`.
    ///
    /// # Arguments
    ///
    /// * `txn` - The bincoded transaction slice to send.
    /// * `txn_info` - Optional [`TpuSenderTxnInfo`].
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    ///
    ///
    pub async fn send_txn<T>(
        &mut self,
        txn: T,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.send_txn_with_blocklist(txn, Some(NoBlocklist), txn_info)
            .await
    }

    ///
    /// Sends a transaction to the TPU of the current leader, while preventing sending to blocked peers.
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    /// * `blocklist` - The [`Blocklist`] to use.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    ///
    ///
    pub async fn send_txn_with_blocklist<T, B>(
        &mut self,
        txn: T,
        blocklist: Option<B>,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
        B: Blocklist,
    {
        self.send_txn_fanout_with_blocklist(txn, blocklist, txn_info)
            .await
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(feature = "shield", doc = "only if `shield` feature-flag is enabled"))
    )]
    #[cfg(feature = "shield")]
    ///
    /// Sends a transaction to the TPU of the current leader, while applying Yellowstone Shield blocklist policies.
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    /// * `shield` - The shield blocklist policies to apply, see [`ShieldBlockList`].
    ///
    ///  # Returns
    ///  `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    pub async fn send_txn_with_shield_policies<T>(
        &mut self,
        txn: T,
        shield: ShieldBlockList<'_>,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.send_txn_fanout_with_blocklist(txn, Some(shield), txn_info)
            .await
    }

    ///
    /// Updates the identity keypair used by the TPU sender.
    ///
    /// # Arguments
    ///
    /// * `new_identity` - The new identity [`Keypair`] to use.
    ///
    pub fn update_identity(&mut self, new_identity: Keypair) -> UpdateIdentity {
        self.base_tpu_sender.update_identity(new_identity)
    }
}

///
/// Owned, in-progress multi-destination send state for [`PollYellowstoneTpuSender`].
///
/// Tracks the same fanout state that [`YellowstoneTpuSender::send_txn_many_dest`] keeps on its
/// stack (or, for `.await`-based callers, inside its generated future), except here the fields
/// live inline in [`PollYellowstoneTpuSender`] instead of being borrowed from it, so it never
/// needs a lifetime parameter.
///
struct PollPendingSend {
    wire_txn: Bytes,
    dests: Vec<Pubkey>,
    txn_info: Option<TpuSenderTxnInfo>,
    next_dest: usize,
    pending_txn: Option<TpuSenderTxn>,
}

///
/// A poll-based equivalent of [`YellowstoneTpuSender`].
///
/// [`YellowstoneTpuSender`]'s send methods are `async fn`s, so the futures they return borrow
/// `&mut self` (and, for [`YellowstoneTpuSender::send_txn_with_shield_policies`], the borrowed
/// [`ShieldBlockList`] as well) for their entire lifetime. That makes them impossible to store in
/// a struct alongside the sender they borrow from without either boxing them or resorting to
/// unsafe self-referential tricks.
///
/// [`PollYellowstoneTpuSender`] avoids that entirely: every `start_send_*` method resolves the
/// destination list *synchronously* (exactly like
/// [`YellowstoneTpuSender::send_txn_fanout_with_blocklist`] does internally) and stores the
/// resulting fanout state as owned data inline ([`PollPendingSend`]). Any borrow (like
/// [`ShieldBlockList`]) only needs to live for the duration of the `start_send_*` call itself.
///
/// # Usage
///
/// This mirrors the `start_send`/`poll_send` contract used by `futures::Sink`, except a
/// single [`PollYellowstoneTpuSender`] only ever buffers one in-flight send at a time (there is
/// no separate readiness check ahead of `start_send_*` -- draining via `poll_send` and being
/// ready for the next send are the same condition here):
///
/// 1. Call one of the `start_send_*` methods to begin a send.
/// 2. Call [`PollYellowstoneTpuSender::poll_send`] and wait for it to return `Poll::Ready`. This drains
///    the started send to all of its destinations.
/// 3. Once flushed, go back to step 1 to start the next send.
///
/// Calling a `start_send_*` method before a previously started send has been fully drained via
/// [`PollYellowstoneTpuSender::poll_send`] is a logic error and will panic.
///
pub struct PollYellowstoneTpuSender {
    sender: YellowstoneTpuSender,
    pending: Option<PollPendingSend>,
}

#[derive(thiserror::Error, Debug)]
#[error("disconnected")]
pub struct PollSendError {
    wire_txn: Bytes,
    dests: Vec<Pubkey>,
}

impl PollYellowstoneTpuSender {
    ///
    /// Wraps an existing [`YellowstoneTpuSender`] in poll-based mode.
    ///
    pub const fn new(sender: YellowstoneTpuSender) -> Self {
        Self {
            sender,
            pending: None,
        }
    }

    ///
    /// Drives any in-progress send -- across all of its destinations -- to completion.
    ///
    /// Returns `Poll::Ready(Ok(()))` once there is no in-progress send, at which point a
    /// `start_send_*` method may be called. Must be called (and must return `Poll::Ready`)
    /// before every `start_send_*` call.
    ///
    pub fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), PollSendError>> {
        loop {
            let Some(pending) = self.pending.as_mut() else {
                return Poll::Ready(Ok(()));
            };

            if pending.pending_txn.is_some() {
                match self.sender.base_tpu_sender.poll_reserve(cx) {
                    Poll::Ready(Ok(())) => {
                        let txn = pending
                            .pending_txn
                            .take()
                            .expect("checked by pending_txn.is_some() above");
                        if self.sender.base_tpu_sender.send_item(txn).is_err() {
                            let err = PollSendError {
                                wire_txn: pending.wire_txn.clone(),
                                dests: std::mem::take(&mut pending.dests),
                            };
                            self.pending = None;
                            return Poll::Ready(Err(err));
                        }
                        continue;
                    }
                    Poll::Ready(Err(_)) => {
                        let err = PollSendError {
                            wire_txn: pending.wire_txn.clone(),
                            dests: std::mem::take(&mut pending.dests),
                        };
                        self.pending = None;
                        return Poll::Ready(Err(err));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if pending.next_dest >= pending.dests.len() {
                self.pending = None;
                continue;
            }

            let remote_peer = pending.dests[pending.next_dest];
            pending.next_dest += 1;
            pending.pending_txn = Some(TpuSenderTxn {
                remote_peer,
                wire: pending.wire_txn.clone(),
                info: pending.txn_info,
            });
        }
    }

    fn assert_ready_for_start_send(&self) {
        assert!(
            self.pending.is_none(),
            "PollYellowstoneTpuSender::start_send_* called before poll_send returned Poll::Ready"
        );
    }

    ///
    /// Poll-based equivalent of [`YellowstoneTpuSender::send_txn_many_dest`].
    ///
    /// # Panics
    ///
    /// Panics if called before a previously started send was fully drained via
    /// [`PollYellowstoneTpuSender::poll_send`].
    ///
    pub fn start_send_txn_many_dest<T>(
        &mut self,
        txn: T,
        dests: &[Pubkey],
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.assert_ready_for_start_send();

        let wire_txn = Bytes::from_owner(txn);
        if dests.is_empty() {
            return Ok(());
        }

        let mut selected_dests = Vec::with_capacity(dests.len());
        let mut dest_addr_vec = Vec::<SocketAddr>::with_capacity(dests.len());
        for dest in dests {
            if let Some(addr) = self
                .sender
                .leader_tpu_info
                .get_quic_dest_addr(dest, self.sender.tpu_port_kind)
            {
                if self.sender.coalesce_send_many_tpu_port_collision
                    && dest_addr_vec.contains(&addr)
                {
                    continue;
                }
                dest_addr_vec.push(addr);
            }
            selected_dests.push(*dest);
        }

        self.pending = Some(PollPendingSend {
            wire_txn,
            dests: selected_dests,
            txn_info,
            next_dest: 0,
            pending_txn: None,
        });
        Ok(())
    }

    ///
    /// Poll-based equivalent of [`YellowstoneTpuSender::send_txn_with_blocklist`].
    ///
    /// # Panics
    ///
    /// Panics if called before a previously started send was fully drained via
    /// [`PollYellowstoneTpuSender::poll_send`].
    ///
    pub fn start_send_txn_with_blocklist<T, B>(
        &mut self,
        txn: T,
        blocklist: Option<B>,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
        B: Blocklist,
    {
        self.assert_ready_for_start_send();

        let wire_txn = Bytes::from_owner(txn);
        let current_slot = match self.sender.atomic_slot_tracker.load() {
            Ok(slot) => slot,
            Err(_) => {
                return Err(SendError {
                    kind: SendErrorKind::SlotTrackerDisconnected,
                    txn: wire_txn,
                });
            }
        };
        let reminder = current_slot % 4;
        let floor_leader_boundary = current_slot.saturating_sub(reminder);
        let n = if reminder >= 2 { 2 } else { 1 };

        let mut blocked_cnt = 0;
        let result = (0..n)
            .map(|i| floor_leader_boundary + (i * 4) as u64)
            .map(|leader_slot_boundary| {
                self.sender.leader_schedule.get_leader(leader_slot_boundary)
            })
            .filter_map(|res| match res {
                Ok(None) => {
                    panic!("unknown leader for slot boundary {floor_leader_boundary}");
                }
                Ok(Some(leader)) => {
                    if let Some(blocklist) = &blocklist {
                        if blocklist.is_blocked(&leader) {
                            blocked_cnt += 1;
                            None
                        } else {
                            Some(Ok(leader))
                        }
                    } else {
                        Some(Ok(leader))
                    }
                }
                Err(_) => Some(Err(SendErrorKind::ManagedLeaderScheduleDisconnected)),
            })
            .collect::<Result<Vec<_>, SendErrorKind>>();

        match result {
            Ok(leaders) => {
                if leaders.is_empty() && blocked_cnt > 0 {
                    Err(SendError {
                        kind: SendErrorKind::RemotePeerBlocked,
                        txn: wire_txn,
                    })
                } else {
                    self.start_send_txn_many_dest(wire_txn, &leaders, txn_info)
                }
            }
            Err(err_kind) => Err(SendError {
                kind: err_kind,
                txn: wire_txn,
            }),
        }
    }

    ///
    /// Poll-based equivalent of [`YellowstoneTpuSender::send_txn`].
    ///
    /// # Panics
    ///
    /// Panics if called before a previously started send was fully drained via
    /// [`PollYellowstoneTpuSender::poll_send`].
    ///
    pub fn start_send_txn<T>(
        &mut self,
        txn: T,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.start_send_txn_with_blocklist(txn, Some(NoBlocklist), txn_info)
    }

    #[cfg_attr(
        docsrs,
        doc(cfg(feature = "shield", doc = "only if `shield` feature-flag is enabled"))
    )]
    #[cfg(feature = "shield")]
    ///
    /// Poll-based equivalent of [`YellowstoneTpuSender::send_txn_with_shield_policies`].
    ///
    /// The [`ShieldBlockList`] borrow only needs to be valid for the duration of this call: it is
    /// used synchronously to resolve the upcoming leaders into a destination list, which is then
    /// owned by this [`PollYellowstoneTpuSender`] for the rest of the send.
    ///
    /// # Panics
    ///
    /// Panics if called before a previously started send was fully drained via
    /// [`PollYellowstoneTpuSender::poll_send`].
    ///
    pub fn start_send_txn_with_shield_policies<T>(
        &mut self,
        txn: T,
        shield: ShieldBlockList<'_>,
        txn_info: Option<TpuSenderTxnInfo>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.start_send_txn_with_blocklist(txn, Some(shield), txn_info)
    }

    ///
    /// Updates the identity keypair used by the underlying [`YellowstoneTpuSender`].
    ///
    pub fn update_identity(&mut self, new_identity: Keypair) -> UpdateIdentity {
        self.sender.update_identity(new_identity)
    }

    ///
    /// Unwraps the underlying [`YellowstoneTpuSender`].
    ///
    /// # Panics
    ///
    /// Panics if there is a send in-progress that hasn't been drained via
    /// [`PollYellowstoneTpuSender::poll_send`].
    ///
    pub fn into_inner(self) -> YellowstoneTpuSender {
        assert!(
            self.pending.is_none(),
            "PollYellowstoneTpuSender::into_inner called with a send in-progress"
        );
        self.sender
    }
}

impl From<YellowstoneTpuSender> for PollYellowstoneTpuSender {
    fn from(sender: YellowstoneTpuSender) -> Self {
        Self::new(sender)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::core::LeaderTpuInfoService, std::collections::HashMap};

    struct FakeLeaderTpuInfo(HashMap<Pubkey, SocketAddr>);

    impl LeaderTpuInfoService for FakeLeaderTpuInfo {
        fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.0.get(leader_pubkey).copied()
        }

        fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.0.get(leader_pubkey).copied()
        }
    }

    /// Builds a [`PollYellowstoneTpuSender`] with a fake leader-tpu-info map and a fixed,
    /// single-epoch leader schedule, for tests that don't need real RPC/QUIC services.
    fn test_yellowstone_sender(
        addrs: HashMap<Pubkey, SocketAddr>,
        coalesce: bool,
        current_slot: u64,
        schedule: Vec<Pubkey>,
    ) -> (
        PollYellowstoneTpuSender,
        tokio::sync::mpsc::Receiver<TpuSenderTxn>,
    ) {
        let (tpu_sender, rx) = crate::sender::TpuSender::new_test(8);
        let sender = YellowstoneTpuSender {
            base_tpu_sender: PollTpuSender::new(tpu_sender),
            coalesce_send_many_tpu_port_collision: coalesce,
            atomic_slot_tracker: SlotTracker::new(current_slot),
            leader_schedule: ManagedLeaderSchedule::new_for_test(0, schedule),
            leader_tpu_info: Arc::new(FakeLeaderTpuInfo(addrs)),
            tpu_port_kind: TpuPortKind::Forwards,
            _on_drop: Some(Arc::new(YellowstoneTpuSenderLifecycle::default())),
        };
        (PollYellowstoneTpuSender::new(sender), rx)
    }

    #[tokio::test]
    async fn start_send_txn_many_dest_fans_out_to_all_destinations() {
        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        let addrs = HashMap::from([
            (peer1, "127.0.0.1:8001".parse().unwrap()),
            (peer2, "127.0.0.1:8002".parse().unwrap()),
        ]);
        let (mut sender, mut rx) = test_yellowstone_sender(addrs, true, 0, vec![]);

        sender
            .start_send_txn_many_dest(Bytes::from_static(b"wire"), &[peer1, peer2], None)
            .expect("start_send_txn_many_dest");

        futures::future::poll_fn(|cx| sender.poll_send(cx))
            .await
            .expect("poll_send");

        let mut received = vec![
            rx.recv().await.expect("recv 1").remote_peer,
            rx.recv().await.expect("recv 2").remote_peer,
        ];
        received.sort();
        let mut expected = vec![peer1, peer2];
        expected.sort();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn start_send_txn_many_dest_with_empty_dests_is_immediately_ready() {
        let (mut sender, _rx) = test_yellowstone_sender(HashMap::new(), true, 0, vec![]);

        sender
            .start_send_txn_many_dest(Bytes::from_static(b"wire"), &[], None)
            .expect("start_send_txn_many_dest");

        // No destinations were resolved, so there's nothing to drain: poll_send should
        // resolve on its very first poll.
        futures::future::poll_fn(|cx| sender.poll_send(cx))
            .await
            .expect("poll_send");
    }

    #[tokio::test]
    async fn start_send_txn_many_dest_coalesces_same_address_destinations() {
        let peer1 = Pubkey::new_unique();
        let peer2 = Pubkey::new_unique();
        // Both peers share the same TPU socket address (e.g. behind a proxy).
        let shared_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let addrs = HashMap::from([(peer1, shared_addr), (peer2, shared_addr)]);
        let (mut sender, mut rx) = test_yellowstone_sender(addrs, true, 0, vec![]);

        sender
            .start_send_txn_many_dest(Bytes::from_static(b"wire"), &[peer1, peer2], None)
            .expect("start_send_txn_many_dest");

        futures::future::poll_fn(|cx| sender.poll_send(cx))
            .await
            .expect("poll_send");

        let received = rx
            .recv()
            .await
            .expect("expected exactly one coalesced send");
        assert_eq!(received.remote_peer, peer1);
        assert!(
            rx.try_recv().is_err(),
            "the second destination should have been coalesced away"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "start_send_* called before poll_send returned Poll::Ready")]
    async fn start_send_panics_if_previous_send_not_drained() {
        let peer = Pubkey::new_unique();
        let addrs = HashMap::from([(peer, "127.0.0.1:9100".parse().unwrap())]);
        let (mut sender, _rx) = test_yellowstone_sender(addrs, true, 0, vec![]);

        sender
            .start_send_txn_many_dest(Bytes::from_static(b"wire"), &[peer], None)
            .expect("first start_send_txn_many_dest");
        // Second call before draining the first via `poll_send` must panic.
        let _ = sender.start_send_txn_many_dest(Bytes::from_static(b"wire"), &[peer], None);
    }

    #[tokio::test]
    async fn start_send_txn_with_blocklist_returns_remote_peer_blocked_when_all_blocked() {
        let leader = Pubkey::new_unique();
        let addrs = HashMap::from([(leader, "127.0.0.1:9300".parse().unwrap())]);
        // `current_slot = 0` resolves to a single leader boundary (slot 0), which the
        // fixed schedule maps to `leader`.
        let (mut sender, _rx) = test_yellowstone_sender(addrs, true, 0, vec![leader]);

        let err = sender
            .start_send_txn_with_blocklist(Bytes::from_static(b"wire"), Some(vec![leader]), None)
            .expect_err("expected the only resolved leader to be blocked");

        assert!(matches!(err.kind, SendErrorKind::RemotePeerBlocked));
    }

    #[tokio::test]
    async fn start_send_txn_with_blocklist_sends_to_unblocked_leader() {
        let leader = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let addrs = HashMap::from([(leader, "127.0.0.1:9301".parse().unwrap())]);
        let (mut sender, mut rx) = test_yellowstone_sender(addrs, true, 0, vec![leader]);

        sender
            .start_send_txn_with_blocklist(
                Bytes::from_static(b"wire"),
                Some(vec![other]), // blocklist doesn't include `leader`
                None,
            )
            .expect("start_send_txn_with_blocklist");

        futures::future::poll_fn(|cx| sender.poll_send(cx))
            .await
            .expect("poll_send");

        let received = rx.recv().await.expect("recv");
        assert_eq!(received.remote_peer, leader);
    }

    #[test]
    fn drop_of_last_sender_cancels_lifecycle_shutdown() {
        let (sender, _rx) = test_yellowstone_sender(HashMap::new(), true, 0, vec![]);
        let sender = sender.into_inner();
        let shutdown = sender
            ._on_drop
            .as_ref()
            .expect("lifecycle")
            .shutdown
            .clone();
        let sender2 = sender.clone();

        drop(sender);
        assert!(
            !shutdown.is_cancelled(),
            "dropping one clone must not cancel while another clone exists"
        );

        drop(sender2);
        assert!(
            shutdown.is_cancelled(),
            "dropping the last sender must cancel lifecycle shutdown"
        );
    }
}

///
/// Object returned when creating a new [`YellowstoneTpuSender`].
///
/// See [`create_yellowstone_tpu_sender_with_clients`] for creation.
///
pub struct NewYellowstoneTpuSender {
    ///
    /// The created Yellowstone TPU sender.
    ///
    pub sender: YellowstoneTpuSender,
    ///
    /// Join handle for related background tasks.
    ///
    /// # Note
    /// Dropping this handle will not stop the TPU sender itself, but it still recommended to await it to ensure proper cleanup.
    ///
    pub related_objects_jh: tokio::task::JoinHandle<()>,
}

/// Creates a Yellowstone TPU sender with the specified configuration.
///
/// # Arguments
///
/// * `config` - [`YellowstoneTpuSenderConfig`] for the Yellowstone TPU sender.
/// * `initial_identity` - The initial identity [`Keypair`] for the TPU sender.
/// * `rpc_client` - An RPC client [`rpc_client::RpcClient`] to interact with the Solana network.
/// * `grpc_client` - A gRPC client [`GeyserGrpcClient`] to interact with the Yellowstone Geyser service.
///
/// # Returns
///
/// A tuple containing the created [`YellowstoneTpuSender`] and a receiver for [`TpuSenderResponse`].
/// You can drop the receiver if you don't need to handle responses.
///
pub async fn create_yellowstone_tpu_sender_with_clients<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    rpc_client: Arc<rpc_client::RpcClient>,
    grpc_client: GeyserGrpcClient,
    callback: Option<CB>,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    let (tpu_info_service, tpu_info_service_jh) =
        rpc_cluster_tpu_info_service(Arc::clone(&rpc_client), config.tpu_info).await?;

    tracing::debug!("spawned tpu info service");

    let (managed_leader_schedule, managed_leader_schedule_jh) =
        spawn_managed_leader_schedule(Arc::clone(&rpc_client), config.schedule)
            .await
            .expect("spawn_managed_leader_schedule");

    tracing::debug!("spawned managed leader schedule");

    let (stake_service, stake_info_jh) =
        rpc_validator_stake_info_service(Arc::clone(&rpc_client), config.stake).await;

    tracing::debug!("spawned stake info service");

    let atomic_slot_tracker = slot_tracker::atomic_slot_tracker(grpc_client)
        .await?
        .ok_or(CreateTpuSenderError::GeyserSubscriptionEnded)?;

    tracing::debug!("spawned slot tracker service");

    // TODO: make it configurable in another release
    let connection_eviction_strategy = StakeBasedEvictionStrategy {
        ..Default::default()
    };

    let leader_predictor = YellowstoneUpcomingLeader {
        slot_tracker: atomic_slot_tracker.clone(),
        managed_schedule: managed_leader_schedule.clone(),
    };
    let tpu_port_kind = config.tpu.tpu_port;
    let tpu_info_service: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync> =
        Arc::new(tpu_info_service);
    let base_tpu_sender = create_base_tpu_client(
        config.tpu,
        initial_identity,
        Arc::clone(&tpu_info_service),
        Arc::new(stake_service.clone()),
        Arc::new(connection_eviction_strategy),
        Arc::new(leader_predictor),
        callback,
        config.channel_capacity,
    )
    .await;

    tracing::debug!("created base tpu sender");

    let lifecycle = Arc::new(YellowstoneTpuSenderLifecycle::default());

    let sender = YellowstoneTpuSender {
        base_tpu_sender: PollTpuSender::new(base_tpu_sender),
        atomic_slot_tracker: atomic_slot_tracker.clone(),
        coalesce_send_many_tpu_port_collision: true,
        leader_schedule: managed_leader_schedule,
        leader_tpu_info: Arc::clone(&tpu_info_service),
        tpu_port_kind,
        _on_drop: Some(Arc::clone(&lifecycle)),
    };

    let handles = vec![
        tpu_info_service_jh,
        managed_leader_schedule_jh,
        stake_info_jh,
    ];
    let handle_name_vec = vec![
        "tpu-info-service",
        "managed-leader-schedule",
        "stake-info-service",
    ];

    Ok(NewYellowstoneTpuSender {
        sender,
        related_objects_jh: tokio::spawn(yellowstone_tpu_deps_overseer(
            handle_name_vec,
            handles,
            lifecycle.shutdown.clone(),
        )),
    })
}

///
/// Endpoints required to connect to Yellowstone services.
///
/// This struct is embedded in [`YellowstoneTpuSenderConfig`] (see for more details).
///
#[derive(Deserialize, Clone, Debug)]
pub struct Endpoints {
    /// RPC endpoint URL.
    #[serde(default = "Endpoints::default_rpc_url")]
    pub rpc: Url,
    /// gRPC endpoint URL.
    #[serde(default = "Endpoints::default_grpc_url")]
    pub grpc: Url,
    /// Optional X-Token for authentication.
    #[serde(default)]
    pub grpc_x_token: Option<String>,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            rpc: Self::default_rpc_url(),
            grpc: Self::default_grpc_url(),
            grpc_x_token: None,
        }
    }
}

impl Endpoints {
    fn default_rpc_url() -> Url {
        Url::parse("http://localhost:8899").unwrap()
    }

    fn default_grpc_url() -> Url {
        Url::parse("http://localhost:10000").unwrap()
    }
}

///
/// Connects to the specified RPC and gRPC endpoints to create a Yellowstone TPU sender.
///
/// See [`create_yellowstone_tpu_sender_with_clients`] for more details.
///
pub async fn create_yellowstone_tpu_sender_with_callback<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    callback: CB,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    let Endpoints {
        rpc,
        grpc,
        grpc_x_token,
    } = config.endpoints.clone();

    let http_sender = HttpSender::new(rpc);
    let rpc_sender = RetryRpcSender::new(http_sender, Default::default());

    let rpc_client = Arc::new(rpc_client::RpcClient::new_sender(
        rpc_sender,
        RpcClientConfig {
            commitment_config: CommitmentConfig::confirmed(),
            ..Default::default()
        },
    ));

    let grpc_client = GeyserGrpcBuilder::from_shared(grpc.as_str().to_owned())
        .expect("from_shared")
        .x_token(grpc_x_token)
        .expect("x-token")
        .tls_config(ClientTlsConfig::default().with_enabled_roots())
        .expect("tls_config")
        .connect()
        .await
        .expect("connect");

    tracing::debug!("connected to rpc/grpc endpoints");

    create_yellowstone_tpu_sender_with_clients(
        config,
        initial_identity,
        rpc_client,
        grpc_client,
        Some(callback),
    )
    .await
}

pub async fn create_yellowstone_tpu_sender(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError> {
    create_yellowstone_tpu_sender_with_callback(config, initial_identity, Nothing).await
}

async fn yellowstone_tpu_deps_overseer(
    handle_name_vec: Vec<&'static str>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    shutdown: CancellationToken,
) {
    if handles.is_empty() {
        return;
    }

    let abort_handles = handles
        .iter()
        .map(|jh| jh.abort_handle())
        .collect::<Vec<_>>();

    // Wait for the first task to finish
    tokio::select! {
        _ = shutdown.cancelled() => {
            tracing::info!(
                "Yellowstone TPU sender handles all dropped, aborting dependency tasks"
            );
            abort_handles.iter().for_each(|h| h.abort());
        }
        result = futures::future::select_all(handles) => {
            let (finished_handle, i, rest) = result;
            if finished_handle.is_err() {
                tracing::error!(
                    "Yellowstone TPU sender dependency task '{}' has failed with {finished_handle:?}",
                    handle_name_vec.get(i).unwrap_or(&"unknown")
                );
            } else {
                tracing::warn!(
                    "Yellowstone TPU sender dependency task '{}' has finished",
                    handle_name_vec.get(i).unwrap_or(&"unknown")
                );
            }

            // Abort the rest
            rest.into_iter().for_each(|jh| jh.abort());
        }
    }
}
