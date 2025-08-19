use {
    crate::{
        config::ConfigLewisEvents,
        metrics::jet as metrics,
        proto::lewis::{
            Event, EventAck, EventJet, event, transaction_tracker_client::TransactionTrackerClient,
        },
        quic_gateway::GatewayResponse,
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
    tokio::sync::mpsc,
    tonic::transport::Endpoint,
    tracing::{debug, info, warn},
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
}

#[derive(Clone)]
pub struct LewisEventHandler {
    tx: mpsc::UnboundedSender<Event>,
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

    pub fn handle_gateway_response(&self, response: &GatewayResponse, slot: Slot) {
        match response {
            GatewayResponse::TxSent(sent) => {
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
            GatewayResponse::TxFailed(failed) => {
                let event = self.build_event(
                    failed.tx_sig,
                    failed.remote_peer_identity,
                    Some(failed.remote_peer_addr),
                    slot,
                    Some(failed.failure_reason.to_string()),
                    false,
                    vec![],
                );
                self.emit(event);
            }
            GatewayResponse::TxDrop(dropped) => {
                // Iterate over all dropped transactions
                for (gateway_tx, _attempt_count) in &dropped.dropped_gateway_tx_vec {
                    let event = self.build_event(
                        gateway_tx.tx_sig,
                        dropped.remote_peer_identity,
                        None, // No TPU addr for dropped
                        slot,
                        Some(dropped.drop_reason.to_string()),
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
        if self.tx.send(event).is_err() {
            warn!("Lewis event channel closed, dropping event");
            metrics::lewis_events_dropped_inc();
        }
    }
}

pub fn create_lewis_pipeline(
    config: Option<ConfigLewisEvents>,
) -> (
    Option<Arc<LewisEventHandler>>,
    Option<impl Future<Output = Result<(), LewisClientError>> + Send>,
) {
    let Some(config) = config else {
        return (None, None);
    };

    let (tx, rx) = mpsc::unbounded_channel();
    let jet_id = config.jet_id.clone().unwrap_or_default();

    let handler = Arc::new(LewisEventHandler { tx, jet_id });
    let fut = run_lewis_client(config, rx);

    info!("Lewis event pipeline created");

    (Some(handler), Some(fut))
}

async fn run_lewis_client(
    config: ConfigLewisEvents,
    mut rx: mpsc::UnboundedReceiver<Event>,
) -> Result<(), LewisClientError> {
    match connect_and_stream(&config, &mut rx).await {
        Ok(()) => {
            info!("Lewis event stream completed normally");
            Ok(())
        }
        Err(e) => {
            warn!("Lewis connection failed: {}. Draining remaining events", e);
            // Drain to prevent blocking
            while rx.recv().await.is_some() {}
            Err(e)
        }
    }
}

async fn connect_and_stream(
    config: &ConfigLewisEvents,
    rx: &mut mpsc::UnboundedReceiver<Event>,
) -> Result<(), LewisClientError> {
    debug!("Connecting to Lewis at {}", config.endpoint);

    let channel = Endpoint::from_shared(config.endpoint.clone())?
        .connect_timeout(config.connect_timeout)
        .http2_keep_alive_interval(config.keepalive_interval)
        .keep_alive_timeout(config.keepalive_timeout)
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|e| LewisClientError::ConnectionError(e.to_string()))?;

    info!("Connected to Lewis");

    let mut client = TransactionTrackerClient::new(channel);
    let (tx, rx_stream) = futures::channel::mpsc::channel(config.queue_size_grpc);

    let response = client.track_events(rx_stream);
    let mut tx = Box::pin(tx);

    let mut pending = 0u64;
    let mut last_flush = tokio::time::Instant::now();

    tokio::pin!(response);

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        tx.send(event).await
                            .map_err(|e| LewisClientError::StreamSendError(e.to_string()))?;
                        pending += 1;
                        metrics::lewis_events_sent_inc();

                        // Batch events
                        if pending >= config.batch_size_threshold ||
                           last_flush.elapsed() > config.batch_timeout {
                            tx.flush().await
                                .map_err(|e| LewisClientError::StreamFlushError(e.to_string()))?;
                            debug!("Flushed {} events", pending);
                            pending = 0;
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    None => {
                        // Channel closed, flush and complete
                        if pending > 0 {
                            tx.flush().await
                                .map_err(|e| LewisClientError::StreamFlushError(e.to_string()))?;
                        }
                        drop(tx);

                        match response.await {
                            Ok(resp) => {
                                let _ack: EventAck = resp.into_inner();
                                info!("Lewis acknowledged stream completion");
                            }
                            Err(status) => {
                                return Err(LewisClientError::AckError(status));
                            }
                        }
                        return Ok(());
                    }
                }
            }

            _ = &mut response => {
                warn!("Lewis stream ended unexpectedly");
                return Err(LewisClientError::StreamTerminated);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::quic_gateway::GatewayTxSent,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::net::{IpAddr, Ipv4Addr},
    };

    #[test]
    fn test_event_creation() {
        let (tx, mut rx) = mpsc::unbounded_channel();
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
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = LewisEventHandler {
            tx,
            jet_id: "test-jet".to_string(),
        };

        let response = GatewayResponse::TxSent(GatewayTxSent {
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
