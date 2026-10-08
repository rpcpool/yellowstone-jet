//! Spawning a driver, and the handles callers use to talk to it.

use {
    super::TpuSenderDriver,
    crate::{
        config::TpuSenderConfig,
        core::{
            eviction::{
                stake_based::StakeBasedEvictionStrategy, strategy::ConnectionEvictionStrategy,
            },
            identity_update::TpuSenderIdentityUpdater,
            inlet::TpuSenderDriverInlet,
            leader_fast_path::{LeaderFastPath, MAX_FAST_PATH_LEADERS},
            peer_addr_watcher::RemotePeerAddrWatcher,
            response::{Nothing, TpuSenderResponseCallback},
            services::{
                IgnorantLeaderPredictor, LeaderTpuInfoService, UpcomingLeaderPredictor,
                ValidatorStakeInfoService,
            },
        },
        identity::TpuIdentity,
    },
    arc_swap::ArcSwap,
    quinn::Endpoint,
    solana_pubkey::Pubkey,
    std::{mem::MaybeUninit, sync::Arc, time::Instant},
    tokio::{
        runtime::Handle,
        sync::mpsc::{self},
        task::{JoinHandle, JoinSet},
    },
    tokio_util::sync::PollSender,
};

///
/// Context struct holding handles to interact with a spawned TPU sender driver.
///
pub struct TpuSenderSessionContext {
    ///
    /// The [`TpuSenderIdentityUpdater`] use to change the driver configured [`Keypair`].
    ///
    pub identity_updater: TpuSenderIdentityUpdater,

    ///
    /// Sink to send transactions to.
    /// If every clone of the inlet is dropped or closed, the underlying driver runtime stops too.
    ///
    pub driver_tx_sink: TpuSenderDriverInlet,

    ///
    /// Handle to tokio-based QUIC driver runtime.
    /// Dropping this handle does not interrupt the driver runtime.
    ///
    pub driver_join_handle: JoinHandle<()>,
}

///
/// Factory struct to spawn tokio-based QUIC driver
///
pub struct TpuSenderDriverSpawner {
    /// Service to get validator stake info.
    pub stake_info_map: Arc<dyn ValidatorStakeInfoService + Send + Sync + 'static>,
    /// Service to get peers TPU gossip info
    pub leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static>,
    /// Capacity of the channel used to send transaction to the driver.
    pub driver_tx_channel_capacity: usize,
}

impl TpuSenderDriverSpawner {
    pub fn spawn_default_with_callback<CB>(
        &self,
        identity: TpuIdentity,
        callback_sink: CB,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        self.spawn::<CB>(
            identity,
            Default::default(),
            Arc::new(StakeBasedEvictionStrategy::default()),
            Arc::new(IgnorantLeaderPredictor),
            Some(callback_sink),
        )
    }

    pub fn spawn_with_default(&self, identity: TpuIdentity) -> TpuSenderSessionContext {
        self.spawn::<Nothing>(
            identity,
            Default::default(),
            Arc::new(StakeBasedEvictionStrategy::default()),
            Arc::new(IgnorantLeaderPredictor),
            None,
        )
    }

    pub fn spawn<CB>(
        &self,
        identity: TpuIdentity,
        config: TpuSenderConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_schedule: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
        callback_sink: Option<CB>,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        self.spawn_on(
            identity,
            config,
            eviction_strategy,
            leader_schedule,
            callback_sink,
            tokio::runtime::Handle::current(),
        )
    }

    pub fn spawn_on<CB>(
        &self,
        identity: TpuIdentity,
        config: TpuSenderConfig,
        eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync + 'static>,
        leader_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync + 'static>,
        response_callback: Option<CB>,
        driver_rt: Handle,
    ) -> TpuSenderSessionContext
    where
        CB: TpuSenderResponseCallback,
    {
        if config.unsafe_allow_arbitrary_txn_size {
            #[cfg(feature = "intg-testing")]
            {
                tracing::info!(
                    "TpuSenderConfig::allow_arbitrary_txn_size is set to true. This is allowed in integration testing builds."
                );
            }
            #[cfg(not(feature = "intg-testing"))]
            {
                panic!(
                    "TpuSenderConfig::allow_arbitrary_txn_size can only be set to true in integration testing builds."
                );
            }
        }

        let (tx_inlet, tx_outlet) = mpsc::channel(self.driver_tx_channel_capacity);
        let (driver_cnc_tx, driver_cnc_rx) = mpsc::channel(10);

        let bind_addr = config.endpoint_bind_addr;
        let mut endpoints = vec![];
        for _ in 0..config.num_endpoints.get() {
            let endpoint = (0..config.max_local_port_binding_attempts)
                .find_map(|_| {
                    let (_, client_socket) =
                        solana_net_utils::bind_in_range(bind_addr, config.endpoint_port_range)
                            .ok()?;
                    Endpoint::new(
                        quinn::EndpointConfig::default(),
                        None,
                        client_socket,
                        Arc::new(quinn::TokioRuntime),
                    )
                    .ok()
                })
                .unwrap_or_else(|| {
                    // A non-wildcard bind address that is not configured on the host fails here
                    // with EADDRNOTAVAIL. Abort rather than fall back to the wildcard, which
                    // would silently source traffic from the host's primary address.
                    panic!(
                        "Failed to create QUIC endpoint bound to {bind_addr} in port range {:?} after {} attempts",
                        config.endpoint_port_range, config.max_local_port_binding_attempts,
                    )
                });

            endpoints.push(endpoint);
        }

        let remote_peer_addr_watcher = RemotePeerAddrWatcher::new(
            config.remote_peer_addr_watch_interval,
            config.tpu_port,
            Arc::clone(&self.leader_tpu_info_service),
        );
        let current_identity_pubkey = Arc::new(ArcSwap::new(Arc::new(identity.pubkey())));
        // One extra slot because the prediction includes the current leader.
        let upcoming_leaders_buf_len = config
            .leader_prediction_lookahead
            .map_or(0, |lookahead| lookahead.get() + 1);
        let upcoming_leaders_buf =
            vec![MaybeUninit::new(Pubkey::default()); upcoming_leaders_buf_len].into_boxed_slice();
        let fast_path = LeaderFastPath::new();
        let driver = TpuSenderDriver {
            stake_info_map: Arc::clone(&self.stake_info_map),
            tx_worker_handle_map: Default::default(),
            tx_worker_task_meta_map: Default::default(),
            tx_worker_set: Default::default(),
            active_staked_sorted_remote_peer: Default::default(),
            tx_queues: Default::default(),
            identity,
            current_identity_pubkey: Arc::clone(&current_identity_pubkey),
            connecting_tasks: JoinSet::new(),
            connecting_meta: Default::default(),
            connecting_remote_peers: Default::default(),
            leader_tpu_info_service: Arc::clone(&self.leader_tpu_info_service),
            config,
            tx_inlet: tx_outlet,
            response_outlet: response_callback.clone(),
            cnc_rx: driver_cnc_rx,
            tasklet: Default::default(),
            tasklet_meta: Default::default(),
            last_peer_activity: Default::default(),
            being_evicted_peers: Default::default(),
            eviction_strategy,
            connecting_blocked_by_eviction_list: Default::default(),
            endpoints,
            remote_peer_addr_watcher,
            leader_predictor,
            upcoming_leaders_buf,
            fast_path: fast_path.clone(),
            fast_path_leaders: Vec::with_capacity(MAX_FAST_PATH_LEADERS),
            next_leader_prediction_deadline: Instant::now(),
            connecting_remote_peers_addr: Default::default(),
            connection_map: Default::default(),
            connection_version: 0,
            endpoint_sequence: 0,
            pending_connection_eviction_set: Default::default(),
            active_staked_sorted_remote_peer_addr: Default::default(),
            orphan_connection_set: Default::default(),
        };

        let jh = driver_rt.spawn(driver.run());

        TpuSenderSessionContext {
            driver_tx_sink: TpuSenderDriverInlet::new(tx_inlet, fast_path),
            identity_updater: TpuSenderIdentityUpdater {
                cnc_tx: PollSender::new(driver_cnc_tx),
                current_identity_pubkey,
            },
            driver_join_handle: jh,
        }
    }
}
