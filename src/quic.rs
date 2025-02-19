use {
    crate::{
        cluster_tpu_info::{ClusterTpuInfo, TpuInfo},
        config::{ConfigQuic, ConfigQuicTpuPort},
        metrics::jet as metrics,
        quic_solana::ConnectionCache,
        transactions::SendTransactionInfoId,
    },
    futures::{
        future::{join_all, BoxFuture},
        FutureExt,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::{fmt, net::SocketAddr, sync::Arc},
    tokio::{sync::broadcast, time::timeout},
    tracing::{debug, instrument},
};

#[derive(Clone)]
pub struct QuicClient {
    upcoming_leader_schedule: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
    config: Arc<ConfigQuic>,
    connection_cache: Arc<ConnectionCache>,
    extra_tpu_forward: Vec<TpuInfo>,
    tx_broadcast_metrics: broadcast::Sender<QuicClientMetric>,
}

impl fmt::Debug for QuicClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicTxFowarder").finish()
    }
}

#[derive(Clone, Debug)]
pub enum QuicClientMetric {
    SendAttempts {
        sig: Signature,
        leader: Pubkey,
        leader_tpu_addr: SocketAddr,
        slots: Vec<Slot>,
        error: Option<SendError>,
    },
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SendError {
    Timeout,
    QuicError(String),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Timeout => write!(f, "timeout"),
            SendError::QuicError(err) => write!(f, "quic error: {}", err),
        }
    }
}

///
/// Trait for getting the upcoming leader schedule
///
pub trait UpcomingLeaderSchedule {
    fn get_leader_tpus(
        &self,
        leader_forward_lookahead: usize,
        extra_tpu_forward: Vec<TpuInfo>,
    ) -> BoxFuture<'_, Vec<TpuInfo>>;
}

impl UpcomingLeaderSchedule for ClusterTpuInfo {
    fn get_leader_tpus(
        &self,
        leader_forward_lookahead: usize,
        extra_tpu_forward: Vec<TpuInfo>,
    ) -> BoxFuture<'_, Vec<TpuInfo>> {
        let extra_tpu_forward = extra_tpu_forward.into_iter().collect::<Vec<_>>();
        async move {
            self.get_leader_tpus(leader_forward_lookahead, extra_tpu_forward)
                .await
        }
        .boxed()
    }
}

impl QuicClient {
    pub fn new(
        upcoming_leader_schedule: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
        config: ConfigQuic,
        connection_cache: Arc<ConnectionCache>,
    ) -> Self {
        let extra_tpu_forward = config
            .extra_tpu_forward
            .iter()
            .map(|tpu| TpuInfo {
                leader: tpu.leader,
                slots: [0, 0, 0, 0],
                quic: tpu.quic,
                quic_forwards: tpu.quic_forwards,
            })
            .collect();

        let (tx, _) = broadcast::channel(10000);
        Self {
            upcoming_leader_schedule,
            config: Arc::new(config),
            connection_cache,
            extra_tpu_forward,
            tx_broadcast_metrics: tx,
        }
    }

    pub fn subscribe_metrics(&self) -> broadcast::Receiver<QuicClientMetric> {
        self.tx_broadcast_metrics.subscribe()
    }

    #[instrument(skip_all, fields(id, signature, leader_forward_count))]
    pub async fn send_transaction(
        &self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
        leader_forward_count: usize,
    ) {
        let mut tpus_info = self
            .upcoming_leader_schedule
            .get_leader_tpus(leader_forward_count, self.extra_tpu_forward.clone())
            .await;
        tpus_info.extend(self.extra_tpu_forward.iter().cloned());

        let tpu_send_fut = tpus_info.into_iter().map(|tpu_info| {
            let send_retry_count = self.config.send_retry_count;
            let config_tpu_port = self.config.tpu_port;
            let session = Arc::clone(&self.connection_cache);
            let wire_transaction = Arc::clone(&wire_transaction);
            let send_timeout = self.config.send_timeout;
            let tx_broadcast_metrics = self.tx_broadcast_metrics.clone();
            async move {
                let Some(tpu_addr) = (match config_tpu_port {
                    ConfigQuicTpuPort::Normal => tpu_info.quic,
                    ConfigQuicTpuPort::Forwards => tpu_info.quic_forwards,
                }) else {
                    return Ok(());
                };

                debug!(
                    id,
                    %signature,
                    tpu.leader = %tpu_info.leader,
                    tpu.slots = ?tpu_info.slots,
                    tpu.quic = %tpu_addr,
                    "trying to send transaction",
                );

                for _ in 0..send_retry_count {
                    match timeout(
                        send_timeout,
                        session.send_buffer(tpu_addr, wire_transaction.as_ref(), &tpu_info),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            debug!(
                                id,
                                %signature,
                                tpu.leader = %tpu_info.leader,
                                tpu.slots = ?tpu_info.slots,
                                tpu.quic = %tpu_addr,
                                "successfully sent transaction",
                            );
                            metrics::quic_send_attempts_inc(
                                &tpu_info.leader,
                                &tpu_addr,
                                "success",
                                "",
                            );
                            let metric = QuicClientMetric::SendAttempts {
                                sig: signature,
                                leader: tpu_info.leader,
                                leader_tpu_addr: tpu_addr,
                                slots: tpu_info.slots.to_vec(),
                                error: None,
                            };
                            let _ = tx_broadcast_metrics.send(metric);
                            return Ok(());
                        }
                        Ok(Err(error)) => {
                            debug!(
                                id,
                                %signature,
                                tpu.leader = %tpu_info.leader,
                                tpu.slots = ?tpu_info.slots,
                                tpu.quic = %tpu_addr,
                                error = ?error,
                                "failed to send transaction",
                            );
                            metrics::quic_send_attempts_inc(
                                &tpu_info.leader,
                                &tpu_addr,
                                "error",
                                &error.get_categorie(),
                            );
                            let metric = QuicClientMetric::SendAttempts {
                                sig: signature,
                                leader: tpu_info.leader,
                                leader_tpu_addr: tpu_addr,
                                slots: tpu_info.slots.to_vec(),
                                error: Some(SendError::QuicError(error.to_string())),
                            };
                            let _ = tx_broadcast_metrics.send(metric);
                            if error.is_timedout() {
                                break;
                            }
                        }
                        Err(_timeout) => {
                            debug!(
                                id,
                                %signature,
                                tpu.leader = %tpu_info.leader,
                                tpu.slots = ?tpu_info.slots,
                                tpu.quic = %tpu_addr,
                                "failed to send transaction: timedout",
                            );
                            metrics::quic_send_attempts_inc(
                                &tpu_info.leader,
                                &tpu_addr,
                                "timedout",
                                "timedout",
                            );

                            let metric = QuicClientMetric::SendAttempts {
                                sig: signature,
                                leader: tpu_info.leader,
                                leader_tpu_addr: tpu_addr,
                                slots: tpu_info.slots.to_vec(),
                                error: Some(SendError::Timeout),
                            };
                            let _ = tx_broadcast_metrics.send(metric);
                            break;
                        }
                    }
                }
                Err(())
            }
        });

        let success = join_all(tpu_send_fut)
            .await
            .into_iter()
            .filter(|value| value.is_ok())
            .count();
        metrics::sts_tpu_send_inc(success);
    }
}

pub mod teskit {
    use {
        super::UpcomingLeaderSchedule,
        crate::cluster_tpu_info::TpuInfo,
        futures::{future::BoxFuture, FutureExt},
    };

    pub struct MockClusterTpuInfo {
        leaders: Vec<TpuInfo>,
    }

    impl MockClusterTpuInfo {
        pub const fn new(leaders: Vec<TpuInfo>) -> Self {
            Self { leaders }
        }
    }

    impl UpcomingLeaderSchedule for MockClusterTpuInfo {
        fn get_leader_tpus(
            &self,
            _leader_forward_lookahead: usize,
            _extra_tpu_forward: Vec<TpuInfo>,
        ) -> BoxFuture<'_, Vec<TpuInfo>> {
            let leaders = self.leaders.clone();
            async move { leaders }.boxed()
        }
    }
}
