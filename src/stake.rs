use {
    crate::{
        metrics::jet as metrics,
        util::{
            IncrementalBackoff, ValueObserver, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::pubkey::Pubkey,
    std::{ops::DerefMut, sync::Arc},
    tokio::{
        sync::Notify,
        time::{sleep, Duration, Instant},
    },
    tracing::{info, warn},
};

#[derive(Debug, Clone)]
pub struct StakeInfo {
    shutdown: Arc<Notify>,
    join_handle: WaitShutdownSharedJoinHandle,
}

impl WaitShutdown for StakeInfo {
    fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        let mut locked = self.join_handle.lock().await;
        locked.deref_mut().await
    }
}

impl StakeInfo {
    pub fn new(
        rpc: RpcClient,
        update_interval: Duration,
        reactive_identity: ValueObserver<Pubkey>,
    ) -> Self {
        let shutdown = Arc::new(Notify::new());
        Self {
            shutdown: Arc::clone(&shutdown),
            join_handle: Self::spawn(Self::update_stake(
                shutdown,
                rpc,
                update_interval,
                reactive_identity,
            )),
        }
    }

    async fn update_stake(
        shutdown: Arc<Notify>,
        rpc: RpcClient,
        update_interval: Duration,
        mut reactive_identity: ValueObserver<Pubkey>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        loop {
            tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                _ = backoff.maybe_tick() => {},
            }
            let ts = Instant::now();
            let vote_accounts = match rpc.get_vote_accounts().await {
                Ok(vote_accounts) => {
                    backoff.reset();
                    vote_accounts
                }
                Err(error) => {
                    metrics::cluster_nodes_set_size(0);
                    warn!("failed to get cluster nodes: {error:?}");
                    backoff.init();
                    continue;
                }
            };
            let total_vote_accounts = vote_accounts.current.len() + vote_accounts.delinquent.len();
            tracing::trace!("total vote accounts: {total_vote_accounts}");
            let identity = reactive_identity.get_current();
            let identity_str = identity.to_string();
            let vote_account_info = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .find(|info| info.node_pubkey == identity_str);

            let stake = if let Some(stake) = vote_account_info {
                stake.activated_stake
            } else {
                warn!("Did not find identity in vote accounts list");
                0
            };
            // .unwrap_or_default();
            let total_stake: u64 = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .map(|vote_account| vote_account.activated_stake)
                .sum();

            // https://github.com/rpcpool/solana-public/blob/v2.1.11-triton-public/streamer/src/nonblocking/quic.rs#L780-L790
            let max_streams = if stake == 0 || stake > total_stake {
                // https://github.com/rpcpool/solana-public/blob/v2.1.11-triton-public/streamer/src/nonblocking/stream_throttle.rs#L36-L76
                // `max_unstaked_connections` = 500, `max_streams_per_ms` = 250
                // `max_unstaked_load_in_throttling_window` = 0.2 * 250 * 100 / 500 = 10
                10
            } else {
                // https://github.com/rpcpool/solana-public/blob/v2.1.11-triton-public/streamer/src/nonblocking/stream_throttle.rs#L151-L195
                // `max_unstaked_connections` = 500, `max_streams_per_ms` = 250
                // `current_load` = 2_500 (min value, give us mix streams capacity)
                // `capacity_in_ema_window` = 40_000 * stake (%)
                // `calculated_capacity` = 40_000 * stake (%) * 100 / 50 = 80_000 * stake (%)
                (80_000f64 * stake as f64 / total_stake as f64).floor() as u64
            };

            metrics::cluster_identity_stake_set(metrics::ClusterIdentityStakeKind::Jet, stake);
            metrics::cluster_identity_stake_set(
                metrics::ClusterIdentityStakeKind::Total,
                total_stake,
            );
            metrics::cluster_identity_stake_set(
                metrics::ClusterIdentityStakeKind::MaxStreams,
                max_streams,
            );

            info!(
                elapsed_ms = ts.elapsed().as_millis(),
                "update stake info: {stake} / {total_stake} = {max_streams}",
            );
            tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                _ = reactive_identity.observe() => {},
                _ = sleep(update_interval) => {}
            };
        }
    }
}
