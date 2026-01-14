///
/// THIS FILE AS BEEN IMPORTED FROM JET
/// TODO: CREATE A COMMON LIB
use {
    crate::{core::ValidatorStakeInfoService, rpc::solana_rpc_utils::SolanaRpcErrorKindExt},
    futures::{StreamExt, stream},
    serde::Deserialize,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_response::RpcVoteAccountStatus},
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        str::FromStr,
        sync::{Arc, RwLock as StdRwLock},
    },
    tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
        time::Instant,
    },
    tokio_stream::wrappers::ReceiverStream,
};

pub const DEFAULT_STAKE_INFO_REFRESH_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

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

struct StakeInfoMapInner {
    mapping: HashMap<Pubkey, u64>,
    total_stake: u64,
}

// use for tests
pub(crate) enum CacheStakeInfoMapCommand {
    #[allow(dead_code)]
    ManualRefresh { callback: oneshot::Sender<()> },
    #[allow(dead_code)]
    Stop,
}

struct RefreshStakeInfoMapTask {
    rpc: Arc<RpcClient>,
    last_refresh: Instant,
    interval: std::time::Duration,
    cnc_rx: Option<mpsc::Receiver<CacheStakeInfoMapCommand>>,
    shared: Arc<StdRwLock<StakeInfoMapInner>>,
    current_epoch: u64,
    drop_rx: oneshot::Receiver<()>,
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
                    panic!("Failed to get epoch info: {err:?}");
                }
            }
        };

        if self.current_epoch == current_epoch && !force {
            tracing::debug!("Epoch unchanged, skipping stake info refresh");
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
                    tracing::error!("Failed to get vote accounts: {err:?}");
                } else {
                    panic!("Failed to get vote accounts: {err:?}");
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
                _ = &mut self.drop_rx => {
                    tracing::info!("Received drop signal, exiting RefreshStakeInfoMapTask");
                    break;
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

struct DropRefreshStakeInfoMapTask {
    drop_sig: Option<oneshot::Sender<()>>,
}

impl Drop for DropRefreshStakeInfoMapTask {
    fn drop(&mut self) {
        if let Some(tx) = self.drop_sig.take() {
            let _ = tx.send(());
        }
    }
}

///
/// RPC-based implementation of [`ValidatorStakeInfoService`].
///
/// See [`rpc_validator_stake_info_service`] for creating an instance.
///
/// # Clone
///
/// Cloning this service is cheap and shares the same underlying stake info index.
///
/// # Safety
///
/// This service is thread-safe.
///
#[derive(Clone)]
pub struct RpcValidatorStakeInfoService {
    inner: Arc<StdRwLock<StakeInfoMapInner>>,
    _on_drop: Arc<DropRefreshStakeInfoMapTask>,
}

impl ValidatorStakeInfoService for RpcValidatorStakeInfoService {
    fn get_stake_info(&self, validator_identity: &Pubkey) -> Option<u64> {
        let shared = self.inner.read().expect("read");
        shared.mapping.get(validator_identity).copied()
    }
}

impl RpcValidatorStakeInfoService {
    ///
    /// Get the total stake across all validators.
    ///
    pub fn get_total_stake(&self) -> u64 {
        let shared = self.inner.read().expect("read");
        shared.total_stake
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcValidatorStakeInfoServiceConfig {
    ///
    /// Interval between stake info refreshes.
    ///
    #[serde(
        with = "humantime_serde",
        default = "RpcValidatorStakeInfoServiceConfig::default_refresh_interval"
    )]
    pub refresh_interval: std::time::Duration,
}

impl RpcValidatorStakeInfoServiceConfig {
    fn default_refresh_interval() -> std::time::Duration {
        DEFAULT_STAKE_INFO_REFRESH_INTERVAL
    }
}

impl Default for RpcValidatorStakeInfoServiceConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Self::default_refresh_interval(),
        }
    }
}

pub(crate) async fn build_validator_stake_info_service_inner(
    rpc: Arc<RpcClient>,
    config: RpcValidatorStakeInfoServiceConfig,
    cnc_rx: Option<mpsc::Receiver<CacheStakeInfoMapCommand>>,
) -> (RpcValidatorStakeInfoService, JoinHandle<()>) {
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
    let (drop_tx, drop_rx) = oneshot::channel();
    let drop = DropRefreshStakeInfoMapTask {
        drop_sig: Some(drop_tx),
    };
    let ret = RpcValidatorStakeInfoService {
        inner: Arc::clone(&shared),
        _on_drop: Arc::new(drop),
    };
    let task = RefreshStakeInfoMapTask {
        rpc,
        last_refresh: Instant::now(),
        interval: config.refresh_interval,
        cnc_rx,
        shared,
        current_epoch,
        drop_rx,
    };

    let jh = tokio::spawn(task.run());

    (ret, jh)
}

///
/// Creates an RPC-based validator stake info service.
///
/// # Arguments
///
/// * `rpc` - The RPC client to use for fetching stake info.
/// * `config` - Configuration for the stake info service.
///
/// # Returns
///
/// An instance of `RpcValidatorStakeInfoService`.
///
/// Note: This function spawns a background task to periodically refresh the stake info map.
/// When all references to the returned service are dropped, the background task will be stopped automatically.
///
pub async fn rpc_validator_stake_info_service(
    rpc: Arc<RpcClient>,
    config: RpcValidatorStakeInfoServiceConfig,
) -> (RpcValidatorStakeInfoService, JoinHandle<()>) {
    build_validator_stake_info_service_inner(rpc, config, None).await
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{
            core::ValidatorStakeInfoService,
            rpc::{
                solana_rpc_utils::testkit::{
                    MockRpcSender, return_fatal_error, return_sucess, return_transient_error,
                },
                stake::{self, CacheStakeInfoMapCommand, RpcValidatorStakeInfoServiceConfig},
            },
        },
        solana_client::{
            nonblocking::rpc_client::RpcClient,
            rpc_client::RpcClientConfig,
            rpc_request::RpcRequest,
            rpc_response::{RpcVoteAccountInfo, RpcVoteAccountStatus},
        },
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        std::sync::Arc,
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
        let config = RpcValidatorStakeInfoServiceConfig {
            refresh_interval: std::time::Duration::from_secs(1),
        };
        let mock = Arc::new(RpcClient::new_sender(
            mock_rpc_sender.clone(),
            RpcClientConfig::default(),
        ));
        let (stake_info_map, _handle) = stake::rpc_validator_stake_info_service(mock, config).await;

        let actual = stake_info_map
            .get_stake_info(&node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 100);
        let actual = stake_info_map
            .get_stake_info(&node_kp2.pubkey())
            .expect("none");
        assert_eq!(actual, 1000);

        let total_stake = stake_info_map.get_total_stake();
        assert_eq!(total_stake, 1100);
    }

    #[tokio::test]
    pub async fn it_should_panic_during_spawn_if_rpc_error() {
        let mock_rpc_sender = MockRpcSender::all_fatal_errors();
        let mock = Arc::new(RpcClient::new_sender(
            mock_rpc_sender,
            RpcClientConfig::default(),
        ));
        tokio::spawn(async move {
            stake::build_validator_stake_info_service_inner(mock, Default::default(), None)
                .await
                .1
                .await
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
        let mock = Arc::new(RpcClient::new_sender(
            mock_rpc_sender.clone(),
            RpcClientConfig::default(),
        ));
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let config = RpcValidatorStakeInfoServiceConfig {
            refresh_interval: std::time::Duration::from_secs(1),
        };
        let (stake_info_map, cache_fresh_fut) =
            stake::build_validator_stake_info_service_inner(mock, config, Some(cnc_rx)).await;

        let handle = tokio::spawn(cache_fresh_fut);

        let actual = stake_info_map
            .get_stake_info(&node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 100);
        let actual = stake_info_map
            .get_stake_info(&node_kp2.pubkey())
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
            .get_stake_info(&node_kp1.pubkey())
            .expect("none");
        assert_eq!(actual, 200);
        let actual = stake_info_map.get_stake_info(&node_kp2.pubkey());
        assert!(actual.is_none());
        let actual = stake_info_map
            .get_stake_info(&node_kp3.pubkey())
            .expect("none");
        assert_eq!(actual, 2000);

        let total_stake = stake_info_map.get_total_stake();
        assert_eq!(total_stake, 2200);

        cnc_tx
            .send(CacheStakeInfoMapCommand::Stop)
            .await
            .expect("send failed");
        let _ = handle.await.expect("task failed");
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
        let mock = Arc::new(RpcClient::new_sender(
            mock_rpc_sender.clone(),
            RpcClientConfig::default(),
        ));
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let config = RpcValidatorStakeInfoServiceConfig {
            refresh_interval: std::time::Duration::from_secs(1),
        };
        let (stake_info_map, handle) =
            stake::build_validator_stake_info_service_inner(mock, config, Some(cnc_rx)).await;

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
        drop(stake_info_map);
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
        let mock = Arc::new(RpcClient::new_sender(
            mock_rpc_sender.clone(),
            RpcClientConfig::default(),
        ));
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let config = RpcValidatorStakeInfoServiceConfig {
            refresh_interval: std::time::Duration::from_secs(1),
        };

        let (stake_info_map, handle) =
            stake::build_validator_stake_info_service_inner(mock, config, Some(cnc_rx)).await;

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

        // If callback return an error it means it panic
        cb_rx.await.expect_err("callback");
        drop(stake_info_map);
        handle.await.expect_err("handle");
    }
}
