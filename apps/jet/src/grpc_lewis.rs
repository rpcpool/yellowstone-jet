use {
    crate::{
        config::ConfigLewisEvents,
        proto::lewis::{
            Event, EventAck, EventJet, event, transaction_tracker_client::TransactionTrackerClient,
        },
        transactions::JetTxnInfo,
        util::{IncrementalBackoff, create_x_token_interceptor},
    },
    futures::{SinkExt, Stream, StreamExt},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::VecDeque,
        future::Future,
        net::SocketAddr,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::time::Duration,
    tonic::transport::{Channel, Endpoint},
    tracing::{debug, error, info, warn},
    yellowstone_jet_tpu_client::core::TpuSenderResponse,
};

#[derive(Debug, thiserror::Error)]
pub enum LewisClientError {
    #[error("Failed to create endpoint: {0}")]
    EndpointError(#[from] tonic::transport::Error),

    #[error("Failed to connect to Lewis: {0}")]
    ConnectionError(String),

    #[error("Failed to send event to gRPC stream: {0}")]
    StreamSendError(String),

    #[error("Failed to flush gRPC stream: {0}")]
    StreamFlushError(String),

    #[error("Lewis stream terminated unexpectedly")]
    StreamTerminated,

    #[error("Failed to receive acknowledgment from Lewis: {0}")]
    AckError(tonic::Status),

    #[error("Max reconnection attempts exceeded")]
    MaxReconnectAttemptsExceeded,
}

struct LewisTpuResponseStreamAdapter<St> {
    inner: St,
    jet_id: String,
    pending: VecDeque<Event>,
}

impl<St> LewisTpuResponseStreamAdapter<St> {
    fn txn_info(
        info: &Option<yellowstone_jet_tpu_client::core::TpuSenderTxnInfo>,
    ) -> Option<&JetTxnInfo> {
        info.as_ref()
            .and_then(|txn_info| txn_info.downcast_ref::<JetTxnInfo>())
        // .cloned()
    }

    pub fn handle_gateway_response(&mut self, response: &TpuSenderResponse) {
        match response {
            TpuSenderResponse::TxSent(sent) => {
                let Some(info) = Self::txn_info(&sent.info) else {
                    return;
                };
                let event = self.build_event(
                    &info.signature,
                    sent.remote_peer_identity,
                    Some(sent.remote_peer_addr),
                    info.send_at_slot,
                    None,
                    false,
                    vec![],
                );
                self.pending.push_back(event);
            }
            TpuSenderResponse::TxFailed(failed) => {
                let Some(info) = Self::txn_info(&failed.info) else {
                    return;
                };
                let event = self.build_event(
                    &info.signature,
                    failed.remote_peer_identity,
                    Some(failed.remote_peer_addr),
                    info.send_at_slot,
                    Some(failed.failure_reason.clone()),
                    false,
                    vec![],
                );
                self.pending.push_back(event);
            }
            TpuSenderResponse::TxDrop(dropped) => {
                let drop_reason_str = dropped.drop_reason.to_string();
                for (gateway_tx, _attempt_count) in &dropped.dropped_tx_vec {
                    let Some(info) = Self::txn_info(&gateway_tx.info) else {
                        warn!("Missing TpuSenderTxnInfo in TxDrop response");
                        continue;
                    };
                    let event = self.build_event(
                        &info.signature,
                        dropped.remote_peer_identity,
                        None, // No TPU addr for dropped
                        info.send_at_slot,
                        Some(drop_reason_str.clone()),
                        false,
                        vec![],
                    );
                    self.pending.push_back(event);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_event(
        &self,
        signature: &Signature,
        validator: Pubkey,
        tpu_addr: Option<SocketAddr>,
        slot: Slot,
        error: Option<String>,
        skipped: bool,
        shield_policies: Vec<String>,
    ) -> Event {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Event {
            event: Some(event::Event::Jet(EventJet {
                req_id: String::new(),
                cascade_id: String::new(),
                jet_gateway_id: String::new(),
                jet_id: self.jet_id.clone(),
                sig: signature.as_ref().to_vec(),
                slot,
                ts,
                validator: validator.to_string(),
                tpu_addr: tpu_addr.map(|a| a.to_string()).unwrap_or_default(),
                error: error.unwrap_or_default(),
                skipped,
                shield_policies,
            })),
        }
    }
}

impl<St> Stream for LewisTpuResponseStreamAdapter<St>
where
    St: Stream<Item = TpuSenderResponse> + Unpin,
{
    type Item = Event;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.pending.pop_front() {
                return std::task::Poll::Ready(Some(event));
            }

            match this.inner.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(response)) => {
                    this.handle_gateway_response(&response);
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    if this.pending.is_empty() {
                        return std::task::Poll::Pending;
                    }
                }
            }
        }
    }
}

pub fn create_lewis_pipeline<St>(
    config: ConfigLewisEvents,
    rx: St,
) -> impl Future<Output = Result<(), LewisClientError>> + Send
where
    St: Stream<Item = TpuSenderResponse> + Unpin + Send,
{
    let jet_id = config.jet_id.clone().unwrap_or_default();

    let adapter_rx = LewisTpuResponseStreamAdapter {
        inner: rx,
        jet_id,
        pending: Default::default(),
    };
    let fut = auto_reconnect_drain_loop(config, adapter_rx);

    info!("Lewis event pipeline created");
    fut
}

async fn auto_reconnect_drain_loop<St>(
    config: ConfigLewisEvents,
    mut rx: St,
) -> Result<(), LewisClientError>
where
    St: Stream<Item = Event> + Unpin + Send,
{
    let mut attempt = 0;

    let mut backoff = IncrementalBackoff::new(
        config.reconnect_initial_interval,
        config.reconnect_max_interval,
    );

    loop {
        if attempt == 0 {
            backoff.init();
        }
        match drain_loop(&config, &mut rx).await {
            Ok(()) => {
                info!("Lewis event stream completed normally");
                backoff.reset();
                return Ok(());
            }
            Err(e) => {
                attempt += 1;

                if attempt >= config.max_reconnect_attempts {
                    error!(
                        "Max reconnection attempts ({}) exceeded",
                        config.max_reconnect_attempts
                    );
                    // Drain remaining events to prevent blocking
                    while rx.next().await.is_some() {
                        prom::lewis_events_dropped_inc();
                    }
                    return Err(LewisClientError::MaxReconnectAttemptsExceeded);
                }

                warn!(
                    "Lewis connection failed (attempt {}/{}): {}. Retrying...",
                    attempt, config.max_reconnect_attempts, e
                );

                backoff.maybe_tick().await;
            }
        }
    }
}

async fn create_channel(config: &ConfigLewisEvents) -> Result<Channel, LewisClientError> {
    let endpoint = Endpoint::from_shared(config.endpoint.clone())?
        .connect_timeout(config.connect_timeout)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(config.keepalive_interval)
        .keep_alive_timeout(config.keepalive_timeout)
        .keep_alive_while_idle(config.keep_alive_while_idle);

    endpoint
        .connect()
        .await
        .map_err(|e| LewisClientError::ConnectionError(e.to_string()))
}

async fn drain_loop<St>(config: &ConfigLewisEvents, rx: &mut St) -> Result<(), LewisClientError>
where
    St: Stream<Item = Event> + Unpin,
{
    debug!("Connecting to Lewis at {}", config.endpoint);

    let channel = create_channel(config).await?;
    info!("Connected to Lewis");

    // Always use interceptor (it's a no-op if x_token is None)
    let interceptor = create_x_token_interceptor(config.x_token.clone());
    let mut client = TransactionTrackerClient::with_interceptor(channel, interceptor);

    let (mut tx, rx_stream) = futures::channel::mpsc::channel(config.queue_size_grpc);
    let response = client.track_events(rx_stream);

    // Local batch buffer
    let mut batch = Vec::with_capacity(config.batch_size_threshold as usize);
    let mut flush_interval = tokio::time::interval(config.batch_timeout);
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tokio::pin!(response);

    loop {
        // Check stream timeout
        tokio::select! {
            // Handle incoming events
            maybe = rx.next() => {
                let Some(event) = maybe else {
                    debug!("Event channel closed, finishing stream");
                    break;
                };
                batch.push(event);

                if batch.len() >= config.batch_size_threshold as usize {
                    send_batch(&mut tx, &mut batch).await?;
                    flush_interval.reset();
                }
            }

            // Timeout-based flush
            _ = flush_interval.tick() => {
                if !batch.is_empty() {
                    debug!("Flushing batch on timeout ({:?})", config.batch_timeout);
                    send_batch(&mut tx, &mut batch).await?;
                }
            }

            // Monitor gRPC stream health
            result = &mut response => {
                match result {
                    Ok(resp) => {
                        let _ack: EventAck = resp.into_inner();
                        info!("Lewis stream completed with acknowledgment");
                        // Send any remaining events before returning
                        if !batch.is_empty() {
                            send_batch(&mut tx, &mut batch).await?;
                        }
                        return Ok(());
                    }
                    Err(status) => {
                        warn!("Lewis stream failed: {}", status);
                        return Err(LewisClientError::AckError(status));
                    }
                }
            }
        }
    }
    if !batch.is_empty() {
        send_batch(&mut tx, &mut batch).await?;
    }

    drop(tx);

    // Wait for final acknowledgment with timeout
    let ack_timeout = tokio::time::timeout(Duration::from_secs(5), response);

    match ack_timeout.await {
        Ok(Ok(resp)) => {
            let _ack: EventAck = resp.into_inner();
            info!("Lewis acknowledged stream completion");
            Ok(())
        }
        Ok(Err(status)) => {
            warn!("Lewis acknowledgment failed: {}", status);
            Err(LewisClientError::AckError(status))
        }
        Err(_) => {
            warn!("Lewis acknowledgment timed out");
            Ok(())
        }
    }
}

async fn send_batch(
    tx: &mut futures::channel::mpsc::Sender<Event>,
    batch: &mut Vec<Event>,
) -> Result<(), LewisClientError> {
    if batch.is_empty() {
        return Ok(());
    }

    debug!("Sending batch of {} events", batch.len());
    let batch_size = batch.len();

    for event in batch.drain(..) {
        tx.send(event)
            .await
            .map_err(|e| LewisClientError::StreamSendError(e.to_string()))?;
    }

    tx.flush()
        .await
        .map_err(|e| LewisClientError::StreamFlushError(e.to_string()))?;

    for _ in 0..batch_size {
        prom::lewis_events_sent_inc();
    }

    debug!("Successfully sent batch of {} events", batch_size);
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::transactions::JetTxnInfo,
        solana_clock::Slot,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::{
            collections::VecDeque,
            net::{IpAddr, Ipv4Addr},
        },
        yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxnInfo, TxFailed, TxSent},
    };

    fn test_adapter(
        jet_id: &str,
    ) -> LewisTpuResponseStreamAdapter<impl Stream<Item = TpuSenderResponse>> {
        LewisTpuResponseStreamAdapter {
            inner: futures::stream::empty(),
            jet_id: jet_id.to_string(),
            pending: VecDeque::new(),
        }
    }

    #[test]
    fn test_gateway_response_sent() {
        let mut adapter = test_adapter("test-jet");
        let sig = Signature::new_unique();
        let validator = Pubkey::new_unique();
        let send_at_slot: Slot = 12345;

        let response = TpuSenderResponse::TxSent(TxSent {
            remote_peer_identity: validator,
            remote_peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            info: Some(TpuSenderTxnInfo::new(JetTxnInfo {
                signature: sig,
                send_at_slot,
                x_request_id: None,
                x_subscription_id: None,
            })),
        });

        adapter.handle_gateway_response(&response);

        let event = adapter.pending.pop_front().unwrap();
        match event.event {
            Some(event::Event::Jet(jet_event)) => {
                assert_eq!(jet_event.sig, sig.as_ref());
                assert_eq!(jet_event.validator, validator.to_string());
                assert_eq!(jet_event.slot, send_at_slot);
                assert!(!jet_event.skipped);
                assert!(jet_event.error.is_empty());
                assert_eq!(jet_event.jet_id, "test-jet");
                assert_eq!(jet_event.tpu_addr, "127.0.0.1:8000");
            }
            _ => panic!("Expected Jet event"),
        }
    }

    #[test]
    fn test_gateway_response_failed() {
        let mut adapter = test_adapter("test-jet");

        let tx_sig = Signature::new_unique();
        let validator = Pubkey::new_unique();
        let send_at_slot: Slot = 100;

        let response = TpuSenderResponse::TxFailed(TxFailed {
            remote_peer_identity: validator,
            remote_peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            failure_reason: "send-failed".to_string(),
            info: Some(TpuSenderTxnInfo::new(JetTxnInfo {
                signature: tx_sig,
                send_at_slot,
                x_request_id: None,
                x_subscription_id: None,
            })),
        });

        adapter.handle_gateway_response(&response);

        let event = adapter.pending.pop_front().unwrap();
        match event.event {
            Some(event::Event::Jet(jet_event)) => {
                assert!(!jet_event.skipped);
                assert_eq!(jet_event.error, "send-failed");
                assert!(!jet_event.tpu_addr.is_empty());
                assert_eq!(jet_event.sig, tx_sig.as_ref());
                assert_eq!(jet_event.validator, validator.to_string());
                assert_eq!(jet_event.slot, send_at_slot);
            }
            _ => panic!("Expected Jet event"),
        }
    }

    #[test]
    fn test_gateway_response_ignores_non_jet_tx_info() {
        let mut adapter = test_adapter("test-jet");

        let response = TpuSenderResponse::TxSent(TxSent {
            remote_peer_identity: Pubkey::new_unique(),
            remote_peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            // Wrong metadata type: adapter should ignore this response.
            info: Some(TpuSenderTxnInfo::new(Signature::new_unique())),
        });

        adapter.handle_gateway_response(&response);
        assert!(adapter.pending.is_empty());
    }
}

pub mod prom {
    use prometheus::IntCounter;

    lazy_static::lazy_static! {
        static ref LEWIS_EVENTS_DROPPED: IntCounter = IntCounter::new(
            "lewis_events_dropped_total",
            "Total number of events dropped due to channel closure"
        ).unwrap();

        static ref LEWIS_EVENTS_SENT: IntCounter = IntCounter::new(
            "lewis_events_sent_total",
            "Total number of events sent to Lewis gRPC stream"
        ).unwrap();
    }

    pub fn lewis_events_dropped_inc() {
        LEWIS_EVENTS_DROPPED.inc();
    }

    pub fn lewis_events_sent_inc() {
        LEWIS_EVENTS_SENT.inc();
    }

    pub fn register_metrics(reg: &prometheus::Registry) {
        reg.register(Box::new(LEWIS_EVENTS_DROPPED.clone()))
            .unwrap();
        reg.register(Box::new(LEWIS_EVENTS_SENT.clone())).unwrap();
    }
}
