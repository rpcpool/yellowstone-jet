use {
    crate::{
        config::ConfigLewisEvents,
        proto::lewis::{
            Event, EventAck, EventJet, event, transaction_tracker_client::TransactionTrackerClient,
        },
        util::{IncrementalBackoff, create_x_token_interceptor},
    },
    futures::SinkExt,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        future::Future,
        net::SocketAddr,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::{sync::mpsc, time::Duration},
    tokio_util::sync::CancellationToken,
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

#[derive(Clone)]
pub struct LewisEventHandler {
    tx: mpsc::Sender<Event>,
    jet_id: String,
}

impl LewisEventHandler {
    pub fn handle_skip(
        &self,
        signature: Signature,
        validator: Pubkey,
        slot: Slot,
        policies: &[Pubkey],
    ) {
        let event = self.build_event(
            signature,
            validator,
            None,
            slot,
            None,
            true,
            policies.iter().map(|p| p.to_string()).collect(),
        );
        self.emit(event);
    }

    pub fn handle_gateway_response(&self, response: &TpuSenderResponse, slot: Slot) {
        match response {
            TpuSenderResponse::TxSent(sent) => {
                let event = self.build_event(
                    sent.tx_sig,
                    sent.remote_peer_identity,
                    Some(sent.remote_peer_addr),
                    slot,
                    None,
                    false,
                    vec![],
                );
                self.emit(event);
            }
            TpuSenderResponse::TxFailed(failed) => {
                let event = self.build_event(
                    failed.tx_sig,
                    failed.remote_peer_identity,
                    Some(failed.remote_peer_addr),
                    slot,
                    Some(failed.failure_reason.clone()),
                    false,
                    vec![],
                );
                self.emit(event);
            }
            TpuSenderResponse::TxDrop(dropped) => {
                let drop_reason_str = dropped.drop_reason.to_string();
                for (gateway_tx, _attempt_count) in &dropped.dropped_tx_vec {
                    let event = self.build_event(
                        gateway_tx.tx_sig,
                        dropped.remote_peer_identity,
                        None, // No TPU addr for dropped
                        slot,
                        Some(drop_reason_str.clone()),
                        false,
                        vec![],
                    );
                    self.emit(event);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_event(
        &self,
        signature: Signature,
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

    fn emit(&self, event: Event) {
        // Drop on buffer full
        if let Err(e) = self.tx.try_send(event) {
            warn!("Lewis event channel full or closed, dropping event: {}", e);
            prom::lewis_events_dropped_inc();
        }
    }
}

pub fn create_lewis_pipeline(
    config: Option<ConfigLewisEvents>,
    cancellation_token: CancellationToken,
) -> (
    Option<Arc<LewisEventHandler>>,
    Option<impl Future<Output = Result<(), LewisClientError>> + Send>,
) {
    let Some(config) = config else {
        return (None, None);
    };

    let (tx, rx) = mpsc::channel(config.event_buffer_size);
    let jet_id = config.jet_id.clone().unwrap_or_default();

    let handler = Arc::new(LewisEventHandler { tx, jet_id });
    let fut = run_lewis_client(config, rx, cancellation_token);

    info!("Lewis event pipeline created");
    (Some(handler), Some(fut))
}

async fn run_lewis_client(
    config: ConfigLewisEvents,
    mut rx: mpsc::Receiver<Event>,
    cancellation_token: CancellationToken,
) -> Result<(), LewisClientError> {
    let mut attempt = 0;

    let mut backoff = IncrementalBackoff::new(
        config.reconnect_initial_interval,
        config.reconnect_max_interval,
    );

    loop {
        if attempt == 0 {
            backoff.init();
        }
        if cancellation_token.is_cancelled() {
            info!("Lewis client cancellation requested, exiting");
            return Ok(());
        }

        match connect_and_stream(&config, &mut rx, cancellation_token.clone()).await {
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
                    while rx.recv().await.is_some() {
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
        .http2_keep_alive_interval(config.keepalive_interval)
        .keep_alive_timeout(config.keepalive_timeout)
        .keep_alive_while_idle(config.keep_alive_while_idle);

    endpoint
        .connect()
        .await
        .map_err(|e| LewisClientError::ConnectionError(e.to_string()))
}

async fn connect_and_stream(
    config: &ConfigLewisEvents,
    rx: &mut mpsc::Receiver<Event>,
    cancellation_token: CancellationToken,
) -> Result<(), LewisClientError> {
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

    // Stream timeout
    let stream_deadline = if config.stream_timeout > Duration::ZERO {
        Some(tokio::time::Instant::now() + config.stream_timeout)
    } else {
        None
    };

    tokio::pin!(response);

    loop {
        // Check stream timeout
        if let Some(deadline) = stream_deadline {
            if tokio::time::Instant::now() >= deadline {
                warn!("Stream timeout reached after {:?}", config.stream_timeout);
                break;
            }
        }

        tokio::select! {
            // Check for cancellation
            _ = cancellation_token.cancelled() => {
                break;
            }
            // Handle incoming events
            maybe = rx.recv() => {
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
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::net::{IpAddr, Ipv4Addr},
        yellowstone_jet_tpu_client::core::{TpuSenderResponse, TxSent},
    };

    #[test]
    fn test_event_creation() {
        let (tx, mut rx) = mpsc::channel(100);
        let handler = LewisEventHandler {
            tx,
            jet_id: "test-jet".to_string(),
        };

        let sig = Signature::new_unique();
        let validator = Pubkey::new_unique();
        let policies = vec![Pubkey::new_unique(), Pubkey::new_unique()];

        handler.handle_skip(sig, validator, 12345, &policies);

        let event = rx.try_recv().unwrap();
        match event.event {
            Some(event::Event::Jet(jet_event)) => {
                assert_eq!(jet_event.sig, sig.as_ref());
                assert_eq!(jet_event.validator, validator.to_string());
                assert_eq!(jet_event.slot, 12345);
                assert!(jet_event.skipped);
                assert_eq!(jet_event.shield_policies.len(), 2);
            }
            _ => panic!("Expected Jet event"),
        }
    }

    #[test]
    fn test_gateway_response_sent() {
        let (tx, mut rx) = mpsc::channel(100);
        let handler = LewisEventHandler {
            tx,
            jet_id: "test-jet".to_string(),
        };

        let response = TpuSenderResponse::TxSent(TxSent {
            remote_peer_identity: Pubkey::new_unique(),
            tx_sig: Signature::new_unique(),
            remote_peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
        });

        handler.handle_gateway_response(&response, 100);

        let event = rx.try_recv().unwrap();
        match event.event {
            Some(event::Event::Jet(jet_event)) => {
                assert!(!jet_event.skipped);
                assert!(jet_event.error.is_empty());
                assert!(!jet_event.tpu_addr.is_empty());
            }
            _ => panic!("Expected Jet event"),
        }
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
