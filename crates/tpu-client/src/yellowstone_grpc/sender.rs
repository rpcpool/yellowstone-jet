use {
    crate::{
        config::{TpuPortKind, TpuSenderConfig},
        core::{
            Nothing, StakeBasedEvictionStrategy, TpuSenderResponse, TpuSenderResponseCallback,
            TpuSenderTxn,
        },
        rpc::{
            schedule::{
                ManagedLeaderSchedule, ManagedLeaderScheduleConfig, spawn_managed_leader_schedule,
            },
            solana_rpc_utils::RetryRpcSender,
            stake::{RpcValidatorStakeInfoServiceConfig, rpc_validator_stake_info_service},
            tpu_info::{RpcClusterTpuQuicInfoServiceConfig, rpc_cluster_tpu_info_service},
        },
        sender::{TpuSender, create_base_tpu_client},
        slot::AtomicSlotTracker,
        yellowstone_grpc::{
            schedule::YellowstoneUpcomingLeader,
            slot_tracker::{self, YellowstoneSlotTrackerOk},
        },
    },
    bytes::Bytes,
    derive_more::Display,
    serde::Deserialize,
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client, rpc_client::RpcClientConfig,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::{Keypair, Signature},
    solana_pubkey::Pubkey,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        collections::{BTreeSet, HashSet},
        fmt,
        sync::Arc,
    },
    tokio::sync::mpsc::UnboundedSender,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcClient},
};

pub const DEFAULT_TPU_SENDER_CHANNEL_CAPACITY: usize = 100_000;

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
}

impl Default for YellowstoneTpuSenderConfig {
    fn default() -> Self {
        Self {
            tpu: Default::default(),
            tpu_info: Default::default(),
            schedule: Default::default(),
            stake: Default::default(),
            channel_capacity: DEFAULT_TPU_SENDER_CHANNEL_CAPACITY,
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
/// sender
///     .send_txn(signature, bincoded_txn)
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
/// sender.send_txn_with_blocklist(signature, bincoded_txn, Some(leader_to_block)).await;
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
/// sender.send_txn_with_shield_policies(signature, bincoded_txn, shield_blocklist).await;
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
/// sender.send_txn_many_dest(signature, bincoded_txn, dests).await;
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
/// impl TpuSenderResponseCallback for LoggingCallback {
///     fn call(&self, response: TpuSenderResponse) {
///         use std::io::Write;
///         let mut stdout = std::io::stdout();
///         match response {
///             TpuSenderResponse::TxSent(info) => {
///                 writeln!(
///                     &mut stdout,
///                     "Transaction {} send to {}",
///                     info.tx_sig, info.remote_peer_identity
///                 )
///                 .expect("writeln");
///             }
///             TpuSenderResponse::TxFailed(info) => {
///                 writeln!(&mut stdout, "Transaction failed: {}", info.tx_sig).expect("writeln");
///             }
///             TpuSenderResponse::TxDrop(info) => {
///                 for (txn, _) in info.dropped_tx_vec {
///                     writeln!(&mut stdout, "Transaction dropped: {}", txn.tx_sig)
///                         .expect("writeln");
///                 }
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
    base_tpu_sender: TpuSender,
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
    atomic_slot_tracker: Arc<AtomicSlotTracker>,
    leader_schedule: ManagedLeaderSchedule,
    leader_tpu_info: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync>,
    tpu_port_kind: TpuPortKind,
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
        sig: Signature,
        txn: T,
        dests: &[Pubkey],
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
                tx_sig: sig,
                remote_peer: *dest,
                wire: wire_txn.clone(),
            };
            if self.base_tpu_sender.send_txn(tpu_txn).await.is_err() {
                return Err(SendError {
                    kind: SendErrorKind::Closed,
                    txn: wire_txn,
                });
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
    async fn send_txn_fanout_with_blocklist<T, B>(
        &mut self,
        sig: Signature,
        txn: T,
        blocklist: Option<B>,
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
                    self.send_txn_many_dest(sig, wire_txn, &leaders).await
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
    pub fn set_coalesce_many_dest_collision(&mut self, coalesce: bool) {
        self.coalesce_send_many_tpu_port_collision = coalesce;
    }

    ///
    /// Sends a transaction to the TPU of the current leader.
    ///
    /// Same as calling [`YellowstoneTpuSender::send_txn_with_blocklist`] with `Some(NoBlocklist)`
    ///
    /// # Arguments
    ///
    /// * `sig` - The signature identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    ///
    ///
    pub async fn send_txn<T>(&mut self, sig: Signature, txn: T) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.send_txn_with_blocklist(sig, txn, Some(NoBlocklist))
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
        sig: Signature,
        txn: T,
        blocklist: Option<B>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
        B: Blocklist,
    {
        self.send_txn_fanout_with_blocklist(sig, txn, blocklist)
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
        sig: Signature,
        txn: T,
        shield: ShieldBlockList<'_>,
    ) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.send_txn_fanout_with_blocklist(sig, txn, Some(shield))
            .await
    }

    ///
    /// Updates the identity keypair used by the TPU sender.
    ///
    /// # Arguments
    ///
    /// * `new_identity` - The new identity [`Keypair`] to use.
    ///
    pub async fn update_identity(&mut self, new_identity: Keypair) {
        self.base_tpu_sender.update_identity(new_identity).await;
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
    grpc_client: GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor + Clone + 'static>,
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

    let YellowstoneSlotTrackerOk {
        atomic_slot_tracker,
        join_handle: slot_tracker_jh,
    } = slot_tracker::atomic_slot_tracker(grpc_client)
        .await?
        .ok_or(CreateTpuSenderError::GeyserSubscriptionEnded)?;

    tracing::debug!("spawned slot tracker service");

    // TODO: make it configurable in another release
    let connection_eviction_strategy = StakeBasedEvictionStrategy {
        ..Default::default()
    };

    let leader_predictor = YellowstoneUpcomingLeader {
        slot_tracker: Arc::clone(&atomic_slot_tracker),
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

    let sender = YellowstoneTpuSender {
        base_tpu_sender,
        atomic_slot_tracker,
        coalesce_send_many_tpu_port_collision: true,
        leader_schedule: managed_leader_schedule,
        leader_tpu_info: Arc::clone(&tpu_info_service),
        tpu_port_kind,
    };

    let handles = vec![
        tpu_info_service_jh,
        managed_leader_schedule_jh,
        stake_info_jh,
        slot_tracker_jh,
    ];
    let handle_name_vec = vec![
        "tpu-info-service",
        "managed-leader-schedule",
        "stake-info-service",
        "slot-tracker",
    ];

    Ok(NewYellowstoneTpuSender {
        sender,
        related_objects_jh: tokio::spawn(yellowstone_tpu_deps_overseer(handle_name_vec, handles)),
    })
}

///
/// Endpoints required to connect to Yellowstone services.
///
pub struct Endpoints {
    /// RPC endpoint URL.
    pub rpc: String,
    /// gRPC endpoint URL.
    pub grpc: String,
    /// Optional X-Token for authentication.
    pub grpc_x_token: Option<String>,
}

///
/// Connects to the specified RPC and gRPC endpoints to create a Yellowstone TPU sender.
///
/// See [`create_yellowstone_tpu_sender_with_clients`] for more details.
///
pub async fn create_yellowstone_tpu_sender_with_callback<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    endpoints: Endpoints,
    callback: CB,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    let Endpoints {
        rpc,
        grpc,
        grpc_x_token,
    } = endpoints;

    let http_sender = HttpSender::new(rpc);
    let rpc_sender = RetryRpcSender::new(http_sender, Default::default());

    let rpc_client = Arc::new(rpc_client::RpcClient::new_sender(
        rpc_sender,
        RpcClientConfig {
            commitment_config: CommitmentConfig::confirmed(),
            ..Default::default()
        },
    ));

    let grpc_client = GeyserGrpcBuilder::from_shared(grpc)
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
    endpoints: Endpoints,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError> {
    create_yellowstone_tpu_sender_with_callback(config, initial_identity, endpoints, Nothing).await
}

async fn yellowstone_tpu_deps_overseer(
    handle_name_vec: Vec<&'static str>,
    handles: Vec<tokio::task::JoinHandle<()>>,
) {
    // Wait for the first task to finish

    let (finished_handle, i, rest) = futures::future::select_all(handles).await;
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

impl TpuSenderResponseCallback for UnboundedSender<TpuSenderResponse> {
    fn call(&self, response: TpuSenderResponse) {
        let _ = self.send(response);
    }
}
