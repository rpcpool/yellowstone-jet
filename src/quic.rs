use {
    crate::{
        cluster_tpu_info::{ClusterTpuInfo, TpuInfo}, config::{ConfigQuic, ConfigQuicTpuPort}, event_tracker::{SendAttempt, TransactionEventTracker}, metrics::jet::{self as metrics, shield_policies_not_found_inc, sts_tpu_denied_inc_by}, quic_solana::{ConnectionCache, ConnectionCacheSendPermit}, transactions::SendTransactionInfoId
    },
    futures::{
        future::{join_all, BoxFuture},
        FutureExt,
    },
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{fmt, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{sync::broadcast, time::timeout},
    tracing::{debug, instrument},
    yellowstone_shield_store::PolicyStoreTrait,
};

#[derive(Clone)]
pub struct QuicClient {
    upcoming_leader_schedule: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
    config: Arc<ConfigQuic>,
    connection_cache: Arc<ConnectionCache>,
    extra_tpu_forward: Vec<TpuInfo>,
    tx_broadcast_metrics: broadcast::Sender<QuicClientMetric>,
    shield_policy_store: Option<Arc<dyn PolicyStoreTrait + Send + Sync + 'static>>,
    event_tracker: Option<Arc<dyn TransactionEventTracker + Send + Sync + 'static>>,
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
    fn get_leader_tpus(&self, leader_forward_lookahead: usize) -> BoxFuture<'_, Vec<TpuInfo>>;

    fn get_current_slot(&self) -> BoxFuture<'_, Slot>;
}

impl UpcomingLeaderSchedule for ClusterTpuInfo {
    fn get_leader_tpus(&self, leader_forward_lookahead: usize) -> BoxFuture<'_, Vec<TpuInfo>> {
        async move { self.get_leader_tpus(leader_forward_lookahead).await }.boxed()
    }

    fn get_current_slot(&self) -> BoxFuture<'_, Slot> {
        async move { self.get_latest_seen_slot().await }.boxed()
    }
}

pub struct QuicSendTxPermit {
    connection_permits: Vec<ConnectionCacheSendPermit>,
    send_retry_count: usize,
    send_timeout: Duration,
    tx_broadcast_metrics: broadcast::Sender<QuicClientMetric>,
    event_tracker: Option<Arc<dyn TransactionEventTracker + Send + Sync + 'static>>,
    current_slot: Slot,
    filtered_out_validators: Vec<(Pubkey, SocketAddr, String)>,
}

impl QuicSendTxPermit {
    #[instrument(skip_all, fields(id, signature))]
    pub async fn send_transaction(
        self,
        id: SendTransactionInfoId,
        signature: Signature,
        wire_transaction: Arc<Vec<u8>>,
    ) {
        let mut all_send_attempts = Vec::new();

        for (validator, tpu_addr, reason) in self.filtered_out_validators {
            all_send_attempts.push(SendAttempt::skipped(validator, tpu_addr, reason));
        }

        let send_futs = self.connection_permits.into_iter().map(|permit| {
            let tx_broadcast_metrics = self.tx_broadcast_metrics.clone();
            let send_retry_count = self.send_retry_count;
            let send_timeout = self.send_timeout;
            let wire_tx = Arc::clone(&wire_transaction);

            async move {
                let tpu_info = permit.tpu_info;
                let tpu_addr = permit.addr;
                let mut final_attempt = SendAttempt::failed(
                    tpu_info.leader,
                    tpu_addr,
                    "No attempt made".to_string()
                );

                for _ in 0..send_retry_count {
                    match timeout(send_timeout, permit.send_buffer(&wire_tx)).await {
                        Ok(Ok(())) => {
                            metrics::quic_send_attempts_inc(
                                &tpu_info.leader,
                                &tpu_addr,
                                "success",
                                "",
                            );
                            final_attempt = SendAttempt::success(tpu_info.leader, tpu_addr);
                            let metric = QuicClientMetric::SendAttempts {
                                sig: signature,
                                leader: tpu_info.leader,
                                leader_tpu_addr: tpu_addr,
                                slots: tpu_info.slots.to_vec(),
                                error: None,
                            };
                            let _ = tx_broadcast_metrics.send(metric);
                            return final_attempt;
                        }
                        Ok(Err(error)) => {
                            metrics::quic_send_attempts_inc(
                                &tpu_info.leader,
                                &tpu_addr,
                                "error",
                                &error.get_categorie(),
                            );
                            final_attempt = SendAttempt::failed(
                                tpu_info.leader,
                                tpu_addr,
                                error.to_string()
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
                            final_attempt = SendAttempt::failed(
                                tpu_info.leader,
                                tpu_addr,
                                "Send timeout".to_string()
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
                final_attempt
            }
        });

        // Collect all send results
        let send_attempts: Vec<SendAttempt> = join_all(send_futs).await;

        // Count successes
        let success_count = send_attempts.iter()
            .filter(|attempt| attempt.result.is_success())
            .count();

        all_send_attempts.extend(send_attempts);

        metrics::sts_tpu_send_inc(success_count);

        // Emit the event with all attempts (skipped + actual sends)
        if let Some(event_tracker) = &self.event_tracker {
            event_tracker.track_transaction_send(
                &signature,
                self.current_slot,
                all_send_attempts,
            );
        }
    }
}

impl QuicClient {
    pub fn new(
        upcoming_leader_schedule: Arc<dyn UpcomingLeaderSchedule + Send + Sync + 'static>,
        config: ConfigQuic,
        connection_cache: Arc<ConnectionCache>,
        shield_policy_store: Option<Arc<dyn PolicyStoreTrait + Send + Sync + 'static>>,
        event_tracker: Option<Arc<dyn TransactionEventTracker + Send + Sync + 'static>>,
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
            shield_policy_store,
            event_tracker,
        }
    }

    pub fn subscribe_metrics(&self) -> broadcast::Receiver<QuicClientMetric> {
        self.tx_broadcast_metrics.subscribe()
    }

    ///
    /// Reserves a broadcast permit to upcoming Nth leaders.
    ///
    /// # Parameters
    ///
    /// * `leader_forward_count` - The number of leaders to broadcast/foward to
    ///
    pub async fn reserve_send_permit(
        &self,
        leader_forward_count: usize,
        policies: Vec<Pubkey>,
    ) -> Option<QuicSendTxPermit> {
        let mut tpus_info = self
            .upcoming_leader_schedule
            .get_leader_tpus(leader_forward_count)
            .await;

        let current_slot = self
            .upcoming_leader_schedule
            .get_current_slot()
            .await;

        debug!("Attempt to send to {} leaders at slot {}", tpus_info.len(), current_slot);

        tpus_info.extend(self.extra_tpu_forward.iter().cloned());

        // Collect all validators with their TPU addresses before filtering
        let mut all_validators: Vec<(Pubkey, SocketAddr)> = Vec::new();
        for tpu_info in &tpus_info {
            if let Some(addr) = match self.config.tpu_port {
                ConfigQuicTpuPort::Normal => tpu_info.quic,
                ConfigQuicTpuPort::Forwards => tpu_info.quic_forwards,
            } {
                all_validators.push((tpu_info.leader, addr));
            }
        }

        // Track which validators were filtered out
        let mut filtered_out: Vec<(Pubkey, SocketAddr, String)> = Vec::new();

        let tpus_info = if let Some(store) = self.shield_policy_store.as_ref() {
            let snapshot = store.snapshot();

            tpus_info
                .into_iter()
                .filter(|tpu_info| {
                    match snapshot.is_allowed(&policies, &tpu_info.leader) {
                        Ok(allowed) => {
                            if !allowed {
                                // Track filtered out validator
                                if let Some(addr) = match self.config.tpu_port {
                                    ConfigQuicTpuPort::Normal => tpu_info.quic,
                                    ConfigQuicTpuPort::Forwards => tpu_info.quic_forwards,
                                } {
                                    filtered_out.push((
                                        tpu_info.leader,
                                        addr,
                                        "Policy denied".to_string()
                                    ));
                                }
                            }
                            allowed
                        }
                        Err(_) => {
                            shield_policies_not_found_inc();
                            // Track filtered out validator
                            if let Some(addr) = match self.config.tpu_port {
                                ConfigQuicTpuPort::Normal => tpu_info.quic,
                                ConfigQuicTpuPort::Forwards => tpu_info.quic_forwards,
                            } {
                                filtered_out.push((
                                    tpu_info.leader,
                                    addr,
                                    "Policy not found".to_string()
                                ));
                            }
                            false
                        }
                    }
                })
                .collect()
        } else {
            tpus_info
        };

        let before_policy_check_tpu_infos_count = tpus_info.len();
        sts_tpu_denied_inc_by(before_policy_check_tpu_infos_count - tpus_info.len());

        debug!("After filtering, sending to {} leaders", tpus_info.len());

        let futs = tpus_info
            .into_iter()
            .filter_map(|tpu_info| {
                match self.config.tpu_port {
                    ConfigQuicTpuPort::Normal => tpu_info.quic,
                    ConfigQuicTpuPort::Forwards => tpu_info.quic_forwards,
                }
                .map(|addr| (tpu_info, addr))
            })
            .map(|(tpu_info, addr)| self.connection_cache.reserve_send_permit(tpu_info, addr));

        let connection_permits = join_all(futs).await;

        if connection_permits.is_empty() {
            None
        } else {
            Some(QuicSendTxPermit {
                connection_permits,
                send_retry_count: self.config.send_retry_count,
                tx_broadcast_metrics: self.tx_broadcast_metrics.clone(),
                send_timeout: self.config.send_timeout,
                event_tracker: self.event_tracker.clone(),
                current_slot,
                filtered_out_validators: filtered_out,
            })
        }
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
        fn get_leader_tpus(&self, _leader_forward_lookahead: usize) -> BoxFuture<'_, Vec<TpuInfo>> {
            let leaders = self.leaders.clone();
            async move { leaders }.boxed()
        }

        fn get_current_slot(&self) -> BoxFuture<'_, solana_clock::Slot> {
            async move { 0 }.boxed()
        }
    }
}
