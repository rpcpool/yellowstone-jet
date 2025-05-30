///
/// THIS FILE AS BEEN IMPORTED FROM JET-GATEWAY
/// TODO: CREATE A COMMON LIB
use {
    crate::solana_rpc_utils::SolanaRpcErrorKindExt,
    futures::{StreamExt, stream},
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_response::RpcVoteAccountStatus},
    solana_sdk::{pubkey::Pubkey, quic::QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS},
    solana_streamer::nonblocking::quic::{ConnectionPeerType, compute_max_allowed_uni_streams},
    std::{
        collections::HashMap, future::Future, str::FromStr, sync::Arc, sync::RwLock as StdRwLock,
    },
    tokio::{
        sync::{mpsc, oneshot},
        time::Instant,
    },
    tokio_stream::wrappers::ReceiverStream,
};

pub fn stake_to_per100ms_limit(stake: u64, total_stake: u64) -> u64 {
    if stake == 0 || total_stake == 0 || stake > total_stake {
        return 10;
    }

    // Comes from jet repository in `stake.rs`
    (80_000f64 * stake as f64 / total_stake as f64).floor() as u64
}

pub fn stake_to_max_stream(stake: u64, total_stake: u64) -> u64 {
    if stake == 0 {
        QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS as u64
    } else {
        compute_max_allowed_uni_streams(ConnectionPeerType::Staked(stake), total_stake) as u64
    }
}

///
/// Extract the mapping of validator identity to stake amount from the RPC response.
///
fn extract_node_stake_mapping(
    rpc_vote_account_status: RpcVoteAccountStatus,
) -> HashMap<Pubkey, u64> {
    rpc_vote_account_status
        .current
        .iter()
        .chain(&rpc_vote_account_status.delinquent)
        .map(|vote_account_info| {
            Pubkey::from_str(vote_account_info.node_pubkey.as_str())
                .map(|pubkey| (pubkey, vote_account_info.activated_stake))
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .expect("Failed to parse validator accounts")
}

///
/// A thread-safe map of validator identity to stake amount.
///
/// It is used to cache the stake information of all validators in the cluster.
///
/// Stake information is refreshed periodically by a background task spawn by [`auto_refresh_stake_info_map`].
///
#[derive(Clone)]
pub struct StakeInfoMap {
    // Make sure to never lend the lock.
    // Always lock it before accessing the inner data.
    // and always unlock it before returning.
    inner: Arc<StdRwLock<StakeInfoMapInner>>,
}

///
/// Holds various stake limits for a validator.
///
pub struct ValidatorStakeLimits {
    pub max_streams: u64,
    pub per100ms_limit: u64,
}

impl ValidatorStakeLimits {
    const fn unstaked() -> Self {
        Self {
            max_streams: QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS as u64,
            per100ms_limit: 10,
        }
    }
}

impl StakeInfoMap {
    pub fn get_stake_info(&self, validator_identity: Pubkey) -> Option<u64> {
        let stake_mappings = self.inner.read().expect("lock poisoned");
        stake_mappings.mapping.get(&validator_identity).copied()
    }

    pub fn get_stake_info_with_total_stake(
        &self,
        validator_identity: Pubkey,
    ) -> Option<(u64, u64)> {
        let stake_mappings = self.inner.read().expect("lock poisoned");
        stake_mappings
            .mapping
            .get(&validator_identity)
            .copied()
            .map(|stake| (stake, stake_mappings.total_stake))
    }

    ///
    /// Get the stake limits for a validator.
    ///
    /// If the validator is not found in the cache, it returns `ValidatorStakeLimits::unstaked()`.
    ///
    pub fn get_stake_limits(&self, validator_identity: Pubkey) -> ValidatorStakeLimits {
        let stake_mappings = self.inner.read().expect("lock poisoned");
        stake_mappings
            .mapping
            .get(&validator_identity)
            .copied()
            .map(|stake| ValidatorStakeLimits {
                max_streams: stake_to_max_stream(stake, stake_mappings.total_stake),
                per100ms_limit: stake_to_per100ms_limit(stake, stake_mappings.total_stake),
            })
            .unwrap_or_else(ValidatorStakeLimits::unstaked)
    }

    pub fn get_total_stake(&self) -> u64 {
        let stake_mappings = self.inner.read().expect("lock poisoned");
        stake_mappings.total_stake
    }

    pub fn constant<IT>(entries: IT) -> Self
    where
        IT: IntoIterator<Item = (Pubkey, u64)>,
    {
        let mapping: HashMap<_, _> = entries.into_iter().collect();
        let total_stake = mapping.values().sum();
        let inner = StakeInfoMapInner {
            mapping,
            total_stake,
        };
        let shared = Arc::new(StdRwLock::new(inner));
        Self { inner: shared }
    }

    pub fn empty() -> Self {
        Self::constant(vec![])
    }
}

struct StakeInfoMapInner {
    mapping: HashMap<Pubkey, u64>,
    total_stake: u64,
}

///
/// Command to control the background task spawned by [`spawn_cache_stake_info_map`].
///
/// The task can be stopped by sending [`CacheStakeInfoMapCommand::Stop`] through the command-and-control channel.
///
/// Read more about the command-and-control pattern in the [`SpawnMode`] enum.
pub enum CacheStakeInfoMapCommand {
    ManualRefresh { callback: oneshot::Sender<()> },
    Stop,
}

///
/// Spawn mode for [`spawn_cache_stake_info_map`].
///
#[derive(Default)]
pub enum SpawnMode {
    // Launch the task in the background and return a handle to it.
    // If the handle is dropped, the task will not be cancelled automatically.
    #[default]
    Detached,
    // Launch the task in the background as long as the command-and-control channel is open.
    Attached {
        // command-and-control channel
        cnc_rx: mpsc::Receiver<CacheStakeInfoMapCommand>,
    },
}

struct RefreshStakeInfoMapTask {
    rpc: RpcClient,
    last_refresh: Instant,
    interval: std::time::Duration,
    cnc_rx: Option<mpsc::Receiver<CacheStakeInfoMapCommand>>,
    shared: Arc<StdRwLock<StakeInfoMapInner>>,
    current_epoch: u64,
}

impl RefreshStakeInfoMapTask {
    async fn refresh(&mut self, force: bool) {
        self.last_refresh = Instant::now();
        let t = Instant::now();

        let current_epoch = match self.rpc.get_epoch_info().await {
            Ok(epoch_info) => epoch_info.epoch,
            Err(err) => {
                if err.is_transient() {
                    tracing::error!("Failed to get epoch info: {:?}", err);
                    return;
                } else {
                    panic!("Failed to get epoch info: {:?}", err);
                }
            }
        };

        if self.current_epoch == current_epoch || !force {
            tracing::info!("Epoch unchanged, skipping stake info refresh");
            return;
        }

        self.current_epoch = current_epoch;

        let resp = self.rpc.get_vote_accounts().await;
        match resp {
            Ok(resp) => {
                let stake_mappings = extract_node_stake_mapping(resp);
                let total_stake = stake_mappings.values().sum();
                // BECAREFUL : WE SHOULD NEVER HOLD THE LOCK ACROSS AN AWAIT POINT.
                let mut shared = self.shared.write().expect("lock poisoned");
                shared.mapping = stake_mappings;
                shared.total_stake = total_stake;
            }
            Err(err) => {
                if err.is_transient() {
                    tracing::error!("Failed to get vote accounts: {:?}", err);
                } else {
                    panic!("Failed to get vote accounts: {:?}", err);
                }
            }
        }
        let e = t.elapsed();
        tracing::info!("Refreshed stake info map in {:?}", e);
        self.last_refresh = Instant::now();
    }

    async fn run(mut self) {
        let mut command_stream = match self.cnc_rx.take() {
            Some(cnc_rx) => ReceiverStream::new(cnc_rx).boxed(),
            None => stream::pending().boxed(),
        };
        loop {
            let next_refresh = self.last_refresh + self.interval;
            tokio::select! {
                _ = tokio::time::sleep_until(next_refresh) => {
                    self.refresh(false).await;
                },
                maybe = command_stream.next() => {
                    match maybe {
                        Some(command) => {
                            match command {
                                CacheStakeInfoMapCommand::ManualRefresh { callback } => {
                                    self.refresh(true).await;
                                    let _ = callback.send(());
                                },
                                CacheStakeInfoMapCommand::Stop => {
                                    break;
                                }
                            }
                        },
                        None => {
                            tracing::trace!("Command stream closed");
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("RefreshStakeInfoMapTask exiting");
    }
}

///
/// Spawn a task that periodically refreshes [`CacheStakeInfoMap`] with the latest stake information.
///
pub async fn spawn_cache_stake_info_map(
    rpc: RpcClient,
    interval: std::time::Duration,
    mode: SpawnMode,
) -> (StakeInfoMap, impl Future<Output = ()>) {
    let initial_value = rpc
        .get_vote_accounts()
        .await
        .expect("Failed to get vote accounts");

    let current_epoch = rpc
        .get_epoch_info()
        .await
        .expect("Failed to get epoch info")
        .epoch;
    let initial_stake_mappings = extract_node_stake_mapping(initial_value);
    let total_stakes = initial_stake_mappings.values().sum();

    let inner = StakeInfoMapInner {
        mapping: initial_stake_mappings,
        total_stake: total_stakes,
    };
    let shared = Arc::new(StdRwLock::new(inner));
    let ret = StakeInfoMap {
        inner: Arc::clone(&shared),
    };

    let task = RefreshStakeInfoMapTask {
        rpc,
        last_refresh: Instant::now(),
        interval,
        cnc_rx: match mode {
            SpawnMode::Attached { cnc_rx } => Some(cnc_rx),
            _ => None,
        },
        shared,
        current_epoch,
    };

    (ret, task.run())
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            solana_rpc_utils::testkit::{
                MockRpcSender, return_fatal_error, return_sucess, return_transient_error,
            },
            stake::{self, CacheStakeInfoMapCommand, SpawnMode},
        },
        solana_client::{
            nonblocking::rpc_client::RpcClient,
            rpc_client::RpcClientConfig,
            rpc_request::RpcRequest,
            rpc_response::{RpcVoteAccountInfo, RpcVoteAccountStatus},
        },
        solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer},
        tokio::sync::{mpsc, oneshot},
    };

    pub fn mock_rpc_vote_account_info(
        validator_pubkey: Pubkey,
        activated_stake: u64,
    ) -> RpcVoteAccountInfo {
        RpcVoteAccountInfo {
            vote_pubkey: Keypair::new().pubkey().to_string(),
            node_pubkey: validator_pubkey.to_string(),
            activated_stake,
            commission: 0,
            epoch_vote_account: true,
            epoch_credits: vec![],
            last_vote: 1,
            root_slot: 1,
        }
    }

    #[tokio::test]
    pub async fn it_should_return_stake_info() {
        let node_kp1 = Keypair::new();
        let node_kp2 = Keypair::new();
        let info1 = mock_rpc_vote_account_info(node_kp1.pubkey(), 100);
        let info2 = mock_rpc_vote_account_info(node_kp2.pubkey(), 1000);

        let mock_rpc_sender = MockRpcSender::default();

        let mock_get_vote_account = RpcVoteAccountStatus {
            current: vec![info1.clone()],
            delinquent: vec![info2.clone()],
        };

        mock_rpc_sender.set_method_return(
            RpcRequest::GetVoteAccounts,
            return_sucess(mock_get_vote_account),
        );

        let mock = RpcClient::new_sender(mock_rpc_sender.clone(), RpcClientConfig::default());

        let (stake_info_map, _handle) = stake::spawn_cache_stake_info_map(
            mock,
            std::time::Duration::from_secs(1),
            Default::default(),
        )
        .await;

        let actual = stake_info_map
            .get_stake_info(node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 100);
        let actual = stake_info_map
            .get_stake_info(node_kp2.pubkey())
            .expect("none");
        assert_eq!(actual, 1000);

        let total_stake = stake_info_map.get_total_stake();
        assert_eq!(total_stake, 1100);
    }

    #[tokio::test]
    pub async fn it_should_panic_during_spawn_if_rpc_error() {
        let mock_rpc_sender = MockRpcSender::all_fatal_errors();
        let mock = RpcClient::new_sender(mock_rpc_sender, RpcClientConfig::default());
        tokio::spawn(async move {
            stake::spawn_cache_stake_info_map(
                mock,
                std::time::Duration::from_secs(1),
                Default::default(),
            )
            .await
            .1
            .await;
        })
        .await
        .expect_err("should panic");
    }

    #[tokio::test]
    pub async fn it_should_refresh_stake_info_map() {
        let node_kp1 = Keypair::new();
        let node_kp2 = Keypair::new();
        let info1 = mock_rpc_vote_account_info(node_kp1.pubkey(), 100);
        let info2 = mock_rpc_vote_account_info(node_kp2.pubkey(), 1000);

        let mock_rpc_sender = MockRpcSender::default();

        let mock_get_vote_account = RpcVoteAccountStatus {
            current: vec![info1.clone()],
            delinquent: vec![info2.clone()],
        };
        mock_rpc_sender.set_method_return(
            RpcRequest::GetVoteAccounts,
            return_sucess(mock_get_vote_account),
        );
        let mock = RpcClient::new_sender(mock_rpc_sender.clone(), RpcClientConfig::default());
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let (stake_info_map, cache_fresh_fut) = stake::spawn_cache_stake_info_map(
            mock,
            std::time::Duration::from_secs(1),
            SpawnMode::Attached { cnc_rx },
        )
        .await;

        let handle = tokio::spawn(cache_fresh_fut);

        let actual = stake_info_map
            .get_stake_info(node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 100);
        let actual = stake_info_map
            .get_stake_info(node_kp2.pubkey())
            .expect("none");
        assert_eq!(actual, 1000);

        let total_stake = stake_info_map.get_total_stake();
        assert_eq!(total_stake, 1100);

        // Update existing node stake
        let info1 = mock_rpc_vote_account_info(node_kp1.pubkey(), 200);
        // Add new node stake
        let node_kp3 = Keypair::new();
        let info3 = mock_rpc_vote_account_info(node_kp3.pubkey(), 2000);
        let mock_get_vote_account = RpcVoteAccountStatus {
            current: vec![info1.clone(), info3.clone()],
            delinquent: vec![],
        };
        mock_rpc_sender.set_method_return(
            RpcRequest::GetVoteAccounts,
            return_sucess(mock_get_vote_account),
        );
        // Increment epoch since stake only change per epoch.
        mock_rpc_sender.incr_epoch();

        let (cb_tx, cb_rx) = oneshot::channel();
        cnc_tx
            .send(CacheStakeInfoMapCommand::ManualRefresh { callback: cb_tx })
            .await
            .expect("send failed");

        cb_rx.await.expect("callback failed");

        let actual = stake_info_map
            .get_stake_info(node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 200);
        let actual = stake_info_map.get_stake_info(node_kp2.pubkey());
        assert!(actual.is_none());
        let actual = stake_info_map
            .get_stake_info(node_kp3.pubkey())
            .expect("none");
        assert_eq!(actual, 2000);

        let total_stake = stake_info_map.get_total_stake();
        assert_eq!(total_stake, 2200);

        cnc_tx
            .send(CacheStakeInfoMapCommand::Stop)
            .await
            .expect("send failed");
        handle.await.expect("task failed");
    }

    #[tokio::test]
    pub async fn it_should_not_panic_on_transient_error() {
        let node_kp1 = Keypair::new();
        let node_kp2 = Keypair::new();
        let info1 = mock_rpc_vote_account_info(node_kp1.pubkey(), 100);
        let info2 = mock_rpc_vote_account_info(node_kp2.pubkey(), 1000);

        let mock_rpc_sender = MockRpcSender::default();

        let mock_get_vote_account = RpcVoteAccountStatus {
            current: vec![info1.clone()],
            delinquent: vec![info2.clone()],
        };
        mock_rpc_sender.set_method_return(
            RpcRequest::GetVoteAccounts,
            return_sucess(mock_get_vote_account),
        );
        let mock = RpcClient::new_sender(mock_rpc_sender.clone(), RpcClientConfig::default());
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let (stake_info_map, cache_fresh_fut) = stake::spawn_cache_stake_info_map(
            mock,
            std::time::Duration::from_secs(1),
            SpawnMode::Attached { cnc_rx },
        )
        .await;

        let handle = tokio::spawn(cache_fresh_fut);

        let actual = stake_info_map.get_total_stake();
        assert_eq!(actual, 1100);

        // Update existing node stake
        mock_rpc_sender.incr_epoch();

        mock_rpc_sender.set_method_return(RpcRequest::GetVoteAccounts, return_transient_error());

        let (cb_tx, cb_rx) = oneshot::channel();
        cnc_tx
            .send(CacheStakeInfoMapCommand::ManualRefresh { callback: cb_tx })
            .await
            .expect("send failed");

        // If callback return an error it means it pannic
        cb_rx.await.expect("callback failed");

        let actual = stake_info_map.get_total_stake();
        // Should return the previous value
        assert_eq!(actual, 1100);

        cnc_tx
            .send(CacheStakeInfoMapCommand::Stop)
            .await
            .expect("send failed");

        handle.await.expect("task failed");
    }

    #[tokio::test]
    pub async fn it_should_panic_on_non_transient_error() {
        let node_kp1 = Keypair::new();
        let node_kp2 = Keypair::new();
        let info1 = mock_rpc_vote_account_info(node_kp1.pubkey(), 100);
        let info2 = mock_rpc_vote_account_info(node_kp2.pubkey(), 1000);

        let mock_rpc_sender = MockRpcSender::default();

        let mock_get_vote_account = RpcVoteAccountStatus {
            current: vec![info1.clone()],
            delinquent: vec![info2.clone()],
        };
        mock_rpc_sender.set_method_return(
            RpcRequest::GetVoteAccounts,
            return_sucess(mock_get_vote_account),
        );
        let mock = RpcClient::new_sender(mock_rpc_sender.clone(), RpcClientConfig::default());
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let (stake_info_map, cache_fresh_fut) = stake::spawn_cache_stake_info_map(
            mock,
            std::time::Duration::from_secs(1),
            SpawnMode::Attached { cnc_rx },
        )
        .await;

        let handle = tokio::spawn(cache_fresh_fut);

        let actual = stake_info_map.get_total_stake();
        assert_eq!(actual, 1100);

        // Update existing node stake
        mock_rpc_sender.incr_epoch();

        mock_rpc_sender.set_method_return(RpcRequest::GetVoteAccounts, return_fatal_error());

        let (cb_tx, cb_rx) = oneshot::channel();
        cnc_tx
            .send(CacheStakeInfoMapCommand::ManualRefresh { callback: cb_tx })
            .await
            .expect("send failed");

        // If callback return an error it means it pannic
        cb_rx.await.expect_err("callback");
        handle.await.expect_err("handle");
    }
}
