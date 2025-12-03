#[cfg(feature = "bytes")]
use bytes::Bytes;
use {
    crate::{
        config::TpuSenderConfig,
        core::{StakeBasedEvictionStrategy, TpuSenderResponse, TpuSenderTxn, WireTxnFlavor},
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
    tokio::sync::mpsc::UnboundedReceiver,
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
/// A fully-featured TPU sender using Yellowstone services.
///
/// This is a "smart" tpu-sender which knows who is aware of the leader schedule and the current ledger tip.
/// This allow this object to route transaction directly to the current/upcoming leader(s)
///
/// See [`create_yellowstone_tpu_sender`] for creation.
///
/// # Example
///
/// ```ignore
///
/// let txn_signature: Signature = <...>;
/// // `YellowstoneTpuSender` supports multiple bincoded container.
/// let bincoded_txn1_vec: Vec<u8> = <...>;
/// let bincoded_txn2_bytes: Bytes = <...>;
/// let bincoded_txn3_shared_vec: Arc<Vec<u8>> = <...>;
/// let bincoded_txn4_shared_slice: Arc<[u8]> = <...>;
///
/// let my_identity = Keypair::new();
///
/// let NewYellowstoneTpuSender {
///     sender,
///     related_objects_jh: _,
///     response: _,
/// } = create_yellowstone_tpu_sender(
///     Default::default(),
///     my_identity,
///     config.upstream.grpc.endpoint.clone(),
///     config.upstream.grpc.x_token.clone(),
/// ).await.expect("yellowstone-tpu-sender");
///
///
/// sender.send_txn(txn_signature, bincoded_txn).await.expect("send txn 1");
/// sender.send_txn(txn_signature, bincoded_txn1_vec).await.expect("send txn 1");
/// sender.send_txn(txn_signature, bincoded_txn2_bytes).await.expect("send txn 2");
/// sender.send_txn(txn_signature, bincoded_txn3_shared_vec).await.expect("send txn 3");
/// sender.send_txn(txn_signature, bincoded_txn4_shared_slice).await.expect("send txn 4");
///
///
/// ```
///
/// # Send with blocklist
///
/// ```ignore
///
/// let txn_signature: Signature = <...>;
/// let my_txn: Vec<u8> = <...>;
/// let leader_to_block = Pubkey::from_str("HEL1UZMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU").expect("from_str");
/// let my_block_list = vec![leader_to_block];
///
/// sender.send_txn_with_blocklist(txn_signature, my_txn, Some(my_block_list)).await;
/// ```
///
/// If you are using [Yellowstone Shield crate](https://crates.io/crates/yellowstone-shield-store),
/// you can enable `shield` feature flag and use Shield policies as blocklist:
///
/// ```ignore
///
/// let txn_signature: Signature = <...>;
/// let my_txn: Vec<u8> = <...>;
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
/// sender.send_txn_with_shield_policies(txn_signature, my_txn, shield_blocklist).await;
///
/// ```
///
/// # Broadcast sending
///
/// ```ignore
///
/// let txn_signature: Signature = <...>;
/// let my_txn: Vec<u8> = <...>;
/// let dests = vec![
///     Pubkey::from_str("2nhGaJvR17TeytzJVajPfABHQcAwinKoCG8F69gRdQot").expect("from_str"),
///     Pubkey::from_str("EdGevanA2MZsDpxDXK6b36FH7RCcTuDZZRcc6MEyE9hy").expect("from_str"),
/// ];
///
/// sender.send_txn_many_dest(txn_signature, my_txn, dests).await;
///     
/// ```
///
/// # Sends to the current leader and the next N-1 leaders in the schedule
///
/// ```ignore
///
/// let txn_signature: Signature = <...>;
/// let my_txn: Vec<u8> = <...>;
/// let n = 3; // send to current leader + next 2 leaders
///
/// sender.send_txn_fanout(txn_signature, my_txn, n).await;
///
/// ```
///
/// # Safety
///
/// This struct is cheaply-cloneable and can be shared between threads.
#[derive(Clone)]
pub struct YellowstoneTpuSender {
    base_tpu_sender: TpuSender,
    atomic_slot_tracker: Arc<AtomicSlotTracker>,
    leader_schedule: ManagedLeaderSchedule,
}

///
/// Represents encoded transaction ready to be written to the Wire.
///
/// There are multiples way of represents binary slice in Rust, these enum allow
/// to support the most common one and allow some optimization when using zero-copy container such `Arc` or `Bytes`.
///
#[derive(Debug, Clone)]
pub enum WireTransaction {
    ///
    /// [`Vec`]-backed wire formatted transaction.
    ///
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    /// [`Bytes`]-backed wire formatted transaction, useful for zero-copy transaction fanout.
    Bytes(Bytes),
    /// `Arc<Vec<u8>>`-backed wire formatted transaction.
    SharedVec(Arc<Vec<u8>>),
    /// `Arc<[u8]>`-backed wire formatted transaction.
    SharedSlice(Arc<[u8]>),
}

impl From<Vec<u8>> for WireTransaction {
    fn from(v: Vec<u8>) -> Self {
        WireTransaction::Vec(v)
    }
}

#[cfg(feature = "bytes")]
impl From<Bytes> for WireTransaction {
    fn from(b: Bytes) -> Self {
        WireTransaction::Bytes(b)
    }
}

impl From<Arc<Vec<u8>>> for WireTransaction {
    fn from(v: Arc<Vec<u8>>) -> Self {
        WireTransaction::SharedVec(v)
    }
}

impl From<Arc<[u8]>> for WireTransaction {
    fn from(s: Arc<[u8]>) -> Self {
        WireTransaction::SharedSlice(s)
    }
}

#[derive(thiserror::Error)]
#[error("unknown leader {unknown_leader} for transaction")]
pub struct UnknownLeaderError {
    txn: WireTransaction,
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

#[derive(Debug, thiserror::Error)]
#[error("{kind} for transaction")]
pub struct SendError {
    pub kind: SendErrorKind,
    pub txn: WireTransaction,
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

#[cfg(feature = "shield")]
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
    /// * `txn` - The [`WireTransaction`] to send.
    /// * `dests` - The list of destination pubkeys to send the transaction to.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully to all destinations, or a `SendError` if there was an error.
    ///
    /// # Note
    ///
    /// If `dests` is empty, the function returns `Ok(())` immediately
    /// If `dests` contains multiple entries, the transaction may be cloned multiple times.
    ///
    /// To avoid copies when sending to multiple destinations, consider using `Bytes` (feature-flag `bytes`) or `Arc<Vec<u8>>`/`Arc<[u8]>`
    pub async fn send_txn_many_dest<T>(
        &mut self,
        sig: Signature,
        txn: T,
        dests: &[Pubkey],
    ) -> Result<(), SendError>
    where
        T: Into<WireTransaction>,
    {
        if dests.is_empty() {
            return Ok(());
        }

        let wire_txn = txn.into();

        let wire_txn = if dests.len() > 1 {
            match wire_txn {
                WireTransaction::Vec(v) => {
                    // Promote to SharedVec to avoid copy for each destination
                    let arc = Arc::new(v.clone());
                    WireTransaction::SharedVec(arc)
                }
                whatever => whatever,
            }
        } else {
            wire_txn
        };

        for dest in dests {
            let wire_flavor = match &wire_txn {
                WireTransaction::Vec(v) => WireTxnFlavor::Vec(v.clone()),
                #[cfg(feature = "bytes")]
                WireTransaction::Bytes(b) => WireTxnFlavor::Bytes(b.clone()),
                WireTransaction::SharedVec(arc_v) => WireTxnFlavor::SharedVec(Arc::clone(arc_v)),
                WireTransaction::SharedSlice(arc_s) => WireTxnFlavor::Shared(Arc::clone(arc_s)),
            };
            let tpu_txn = TpuSenderTxn {
                tx_sig: sig,
                remote_peer: *dest,
                wire: wire_flavor,
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
    /// Sends a transaction to the TPUs of the next `n` leaders (include the current leader).
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The [`WireTransaction`] to send.
    /// * `n` - The number of upcoming leaders to send the transaction to. Examples include:
    ///     - `n = 1`: send to the current leader only.
    ///     - `n = 2`: send to the current leader and the next leader in the schedule
    ///     - `n = 3`: send to the current leader and the next two leaders in the schedule
    /// * `blocklist` - (Optional) [`Blocklist`], if provided, prevent a transaction from being sent to a disallow remote peer.
    ///
    /// # Note
    ///
    /// Setting `n` &gt; 1 may result in multiple copies of the transaction being created.
    /// To avoid copies when sending to multiple leaders, consider using `Bytes` (feature-flag `bytes`) or `Arc<Vec<u8>>`/`Arc<[u8]>` as the transaction type.
    ///
    /// The fanout is "best-effort" and succeed if the sender can schedule at least one send to a leader.
    ///
    pub async fn send_txn_fanout_with_blocklist<T, B>(
        &mut self,
        sig: Signature,
        txn: T,
        n: usize,
        blocklist: Option<B>,
    ) -> Result<(), SendError>
    where
        T: Into<WireTransaction>,
        B: Blocklist,
    {
        let wire_txn = txn.into();
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

        let mut blocked_cnt = 0;
        let result = (0..n)
            .map(|i| floor_leader_boundary + (i * 4) as u64)
            .map(|leader_slot_boundary| self.leader_schedule.get_leader(leader_slot_boundary))
            .filter_map(|res| match res {
                Ok(None) => {
                    panic!("unknown leader for slot boundary {}", floor_leader_boundary);
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

    pub async fn send_txn_fanout<T>(
        &mut self,
        sig: Signature,
        txn: T,
        n: usize,
    ) -> Result<(), SendError>
    where
        T: Into<WireTransaction>,
    {
        self.send_txn_fanout_with_blocklist::<T, NoBlocklist>(sig, txn, n, None)
            .await
    }

    ///
    /// Sends a transaction to the TPU of the current leader.
    ///
    /// Same as calling [`YellowstoneTpuSender::send_txn_with_blocklist`] with `Some(NoBlocklist)`
    ///
    /// # Arguments
    ///
    /// * `sig` - The signature identifying the transaction.
    /// * `txn` - The transaction to send.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    ///
    ///
    pub async fn send_txn<T>(&mut self, sig: Signature, txn: T) -> Result<(), SendError>
    where
        T: Into<WireTransaction>,
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
    /// * `txn` - The [`WireTransaction`] to send.
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
        T: Into<WireTransaction>,
        B: Blocklist,
    {
        self.send_txn_fanout_with_blocklist(sig, txn, 1, blocklist)
            .await
    }

    ///
    /// Sends a transaction to the TPU of the current leader, while applying Yellowstone Shield blocklist policies.
    ///
    /// # Arguments
    ///
    /// * `sig` - The [`Signature`] identifying the transaction.
    /// * `txn` - The [`WireTransaction`] to send.
    /// * `shield` - The shield blocklist policies to apply, see [`ShieldBlockList`].
    ///
    ///  # Returns
    ///  `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    #[cfg(feature = "shield")]
    pub async fn send_txn_with_shield_policies<T>(
        &mut self,
        sig: Signature,
        txn: T,
        shield: ShieldBlockList<'_>,
    ) -> Result<(), SendError>
    where
        T: Into<WireTransaction>,
    {
        self.send_txn_fanout_with_blocklist(sig, txn, 1, Some(shield))
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
/// Object returned when creating a new Yellowstone TPU sender.
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
    ///
    /// Receiver for TPU sender responses.
    ///
    /// # Note
    /// You can drop this receiver if you don't need to handle responses.
    ///
    pub response: UnboundedReceiver<TpuSenderResponse>,
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
pub async fn create_yellowstone_tpu_sender_with_clients(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    rpc_client: Arc<rpc_client::RpcClient>,
    grpc_client: GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor + Clone + 'static>,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError> {
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

    let (base_tpu_sender, resp) = create_base_tpu_client(
        config.tpu,
        initial_identity,
        Arc::new(tpu_info_service.clone()),
        Arc::new(stake_service.clone()),
        Arc::new(connection_eviction_strategy),
        Arc::new(leader_predictor),
        config.channel_capacity,
    )
    .await;

    tracing::debug!("created base tpu sender");

    let sender = YellowstoneTpuSender {
        base_tpu_sender,
        atomic_slot_tracker,
        leader_schedule: managed_leader_schedule,
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
        response: resp,
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
pub async fn create_yellowstone_tpu_sender(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    endpoints: Endpoints,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError> {
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

    create_yellowstone_tpu_sender_with_clients(config, initial_identity, rpc_client, grpc_client)
        .await
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
