use {
    crate::{
        config::ConfigLewisEvents,
        metrics::jet as metrics,
        proto::lewis::{Event, EventAck, transaction_tracker_client::TransactionTrackerClient},
        transaction_events::{TransactionEvent, TransactionEventTracker},
    },
    anyhow::Context,
    futures::SinkExt,
    solana_clock::Slot,
    solana_signature::Signature,
    std::{future::Future, sync::Arc, time::Duration},
    tokio::sync::mpsc,
    tonic::transport::Endpoint,
    tracing::{debug, info, warn},
};

/// Internal trait for Lewis event client implementations
pub trait LewisEventClientImpl: Send + Sync {
    fn emit(&self, event: Event);
}

/// Real implementation that sends events to Lewis via gRPC
struct RealLewisClient {
    tx: Option<mpsc::Sender<Event>>,
}

impl LewisEventClientImpl for RealLewisClient {
    fn emit(&self, event: Event) {
        if let Some(tx) = &self.tx {
            match tx.try_send(event) {
                Ok(()) => {
                    debug!("Queued Lewis event");
                    metrics::lewis_events_push_inc(Ok(()));
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("Lewis event queue full, dropping event");
                    metrics::lewis_events_push_inc(Err(()));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("Lewis event channel closed");
                    metrics::lewis_events_push_inc(Err(()));
                }
            }
        }
    }
}

/// Client for sending transaction events to Lewis tracking service.
///
/// This client maintains a background task that streams events to Lewis
/// via gRPC. Events are buffered and sent in batches for efficiency.
#[derive(Clone)]
pub struct LewisEventClient {
    inner: Arc<dyn LewisEventClientImpl>,
    jet_id: Option<String>,
}

impl LewisEventClient {
    pub fn create_event_tracker(
        config: Option<ConfigLewisEvents>,
    ) -> (
        Option<Arc<LewisEventClient>>,
        Option<impl Future<Output = anyhow::Result<()>> + Send>,
    ) {
        let Some(config) = config else {
            return (None, None);
        };

        let (tx, rx) = mpsc::channel(config.queue_size_buffer);
        let inner = Arc::new(RealLewisClient { tx: Some(tx) });
        let client = Arc::new(Self {
            inner,
            jet_id: config.jet_id.clone(),
        });

        let tracker = client;
        info!(
            "Lewis event tracker created for endpoint: {}",
            config.endpoint
        );

        let fut = Self::run_event_loop(config, rx);
        (Some(tracker), Some(fut))
    }

    pub fn new_mock(mock_impl: Arc<dyn LewisEventClientImpl>, jet_id: Option<String>) -> Self {
        Self {
            inner: mock_impl,
            jet_id,
        }
    }

    async fn run_event_loop(
        config: ConfigLewisEvents,
        mut rx: mpsc::Receiver<Event>,
    ) -> anyhow::Result<()> {
        match Self::connect_and_stream(&config, &mut rx).await {
            Ok(()) => {
                info!("Lewis event stream completed normally");
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Lewis connection failed: {}. Continuing without event tracking.",
                    e
                );
                // Drain the channel to prevent blocking
                while rx.recv().await.is_some() {}
                Ok(())
            }
        }
    }

    async fn connect_and_stream(
        config: &ConfigLewisEvents,
        rx: &mut mpsc::Receiver<Event>,
    ) -> anyhow::Result<()> {
        debug!("Connecting to Lewis at {}", config.endpoint);

        let channel = Endpoint::from_shared(config.endpoint.clone())?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .context("Failed to connect to Lewis")?;

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
                            tx.send(event).await.context("Failed to send to gRPC stream")?;
                            pending += 1;
                            metrics::lewis_events_feed_inc();

                            // Batch events for efficiency
                            if pending >= 10 || last_flush.elapsed() > Duration::from_millis(100) {
                                tx.flush().await.context("Failed to flush stream")?;
                                debug!("Flushed {} events", pending);
                                pending = 0;
                                last_flush = tokio::time::Instant::now();
                            }
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            if pending > 0 {
                                tx.flush().await.context("Failed to flush final events")?;
                            }
                            drop(tx);

                            match response.await {
                                Ok(resp) => {
                                    let _ack: EventAck = resp.into_inner();
                                    info!("Lewis acknowledged stream completion");
                                }
                                Err(status) => {
                                    warn!("Lewis final ack error: {}", status);
                                }
                            }
                            return Ok(());
                        }
                    }
                }

                _ = &mut response => {
                    warn!("Lewis stream ended unexpectedly");
                    return Err(anyhow::anyhow!("Stream terminated by server"));
                }
            }
        }
    }

    fn emit(&self, event: Event) {
        self.inner.emit(event);
    }
}

#[async_trait::async_trait]
impl TransactionEventTracker for LewisEventClient {
    fn track_transaction_send(
        &self,
        signature: &Signature,
        slot: Slot,
        ts_received: i64,
        events: Vec<TransactionEvent>,
    ) {
        let mut builder = event_builders::JetEventBuilder::new(
            // TODO: We are not sending these IDs yet, because it will require changes
            // to the Jet proto.
            String::new(), // req_id
            String::new(), // cascade_id
            String::new(), // jet_gateway_id
            self.jet_id.clone().unwrap_or_default(),
            signature,
            slot,
            ts_received,
        );

        // Convert TransactionEvents to proto JetSend messages
        for event in events {
            builder = builder.add_event(event);
        }

        let event = builder.build();
        self.emit(event);
    }
}

/// Builders for constructing Lewis protocol buffer events from transaction events
pub mod event_builders {
    use {
        super::*,
        crate::{
            proto::lewis::{Event, EventJet, JetSend},
            transaction_events::TransactionEvent,
        },
    };

    pub struct JetEventBuilder {
        req_id: String,
        cascade_id: String,
        jet_gateway_id: String,
        jet_id: String,
        signature: Vec<u8>,
        slot: u64,
        ts_received: i64, // Timestamp when the transaction was received
        jet_sends: Vec<JetSend>,
    }

    impl JetEventBuilder {
        pub fn new(
            req_id: String,
            cascade_id: String,
            jet_gateway_id: String,
            jet_id: String,
            signature: &Signature,
            slot: u64,
            ts_received: i64,
        ) -> Self {
            Self {
                req_id,
                cascade_id,
                jet_gateway_id,
                jet_id,
                signature: signature.as_ref().to_vec(),
                slot,
                ts_received,
                jet_sends: Vec::new(),
            }
        }

        /// Convert a TransactionEvent into a proto JetSend and add to builder
        pub fn add_event(mut self, event: TransactionEvent) -> Self {
            match event {
                TransactionEvent::TransactionReceived { .. } => {
                    // Skip - this is metadata, not a send attempt
                }
                TransactionEvent::PolicySkipped {
                    validator,
                    timestamp,
                } => {
                    self.jet_sends.push(JetSend {
                        validator: validator.to_string(),
                        ts: timestamp,
                        skipped: true,
                        tpu_addr: String::new(), // No address for skipped
                        error: "Policy denied".to_string(),
                    });
                }
                TransactionEvent::SendAttempt {
                    validator,
                    tpu_addr,
                    result,
                    timestamp,
                    ..
                } => {
                    self.jet_sends.push(JetSend {
                        validator: validator.to_string(),
                        ts: timestamp,
                        skipped: false,
                        tpu_addr: tpu_addr.to_string(),
                        error: result.err().unwrap_or_default(),
                    });
                }
                TransactionEvent::ConnectionFailed {
                    validator,
                    tpu_addr,
                    error,
                    timestamp,
                } => {
                    self.jet_sends.push(JetSend {
                        validator: validator.to_string(),
                        ts: timestamp,
                        skipped: false,
                        tpu_addr: tpu_addr.to_string(),
                        error,
                    });
                }
            }
            self
        }

        pub fn build(self) -> Event {
            Event {
                event: Some(crate::proto::lewis::event::Event::Jet(EventJet {
                    req_id: self.req_id,
                    cascade_id: self.cascade_id,
                    jet_gateway_id: self.jet_gateway_id,
                    jet_id: self.jet_id,
                    sig: self.signature,
                    jet_sends: self.jet_sends,
                    slot: self.slot,
                    ts_received: self.ts_received,
                })),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{proto::lewis::event::Event as ProtoEvent, transaction_events::TransactionEvent},
        solana_pubkey::Pubkey,
        std::{
            net::{IpAddr, Ipv4Addr, SocketAddr},
            sync::Mutex,
        },
    };

    /// Mock implementation for testing
    struct MockLewisClientImpl {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl LewisEventClientImpl for MockLewisClientImpl {
        fn emit(&self, event: Event) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[test]
    fn test_lewis_event_builder() {
        let sig = Signature::new_unique();
        let validator = Pubkey::new_unique();
        let slot = 12345;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);

        let mut builder = event_builders::JetEventBuilder::new(
            "req-123".to_string(),
            "cascade-456".to_string(),
            "gateway-789".to_string(),
            "jet-abc".to_string(),
            &sig,
            slot,
            1000,
        );

        // Add successful send attempt
        builder = builder.add_event(TransactionEvent::SendAttempt {
            validator,
            tpu_addr: addr,
            attempt_num: 1,
            result: Ok(()),
            timestamp: 1000,
        });

        // Add policy skip
        builder = builder.add_event(TransactionEvent::PolicySkipped {
            validator,
            timestamp: 2000,
        });

        let event = builder.build();

        match event.event {
            Some(ProtoEvent::Jet(jet_event)) => {
                assert_eq!(jet_event.req_id, "req-123");
                assert_eq!(jet_event.cascade_id, "cascade-456");
                assert_eq!(jet_event.jet_gateway_id, "gateway-789");
                assert_eq!(jet_event.jet_id, "jet-abc");
                assert_eq!(jet_event.sig, sig.as_ref());
                assert_eq!(jet_event.slot, slot);
                assert_eq!(jet_event.jet_sends.len(), 2);
                assert_eq!(jet_event.ts_received, 1000);

                // Check first send (successful)
                assert_eq!(jet_event.jet_sends[0].validator, validator.to_string());
                assert_eq!(jet_event.jet_sends[0].tpu_addr, addr.to_string());
                assert!(!jet_event.jet_sends[0].skipped);
                assert!(jet_event.jet_sends[0].error.is_empty());

                // Check second send (policy skip)
                assert_eq!(jet_event.jet_sends[1].validator, validator.to_string());
                assert!(jet_event.jet_sends[1].skipped);
                assert_eq!(jet_event.jet_sends[1].error, "Policy denied");
            }
            _ => panic!("Expected Jet event"),
        }
    }

    #[test]
    fn test_mock_lewis_client() {
        let mock_impl = Arc::new(MockLewisClientImpl {
            events: Arc::new(Mutex::new(Vec::new())),
        });
        let client = LewisEventClient::new_mock(
            Arc::<MockLewisClientImpl>::clone(&mock_impl),
            Some("test-jet".to_string()),
        );

        let sig = Signature::new_unique();
        let validator = Pubkey::new_unique();
        let slot = 12345;
        let ts_received = 1000;

        let events = vec![
            TransactionEvent::TransactionReceived {
                leaders: vec![validator],
                slot,
                timestamp: 1000,
            },
            TransactionEvent::SendAttempt {
                validator,
                tpu_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
                attempt_num: 1,
                result: Ok(()),
                timestamp: 2000,
            },
        ];

        client.track_transaction_send(&sig, slot, ts_received, events);

        // Verify event was emitted
        let captured_events = mock_impl.events.lock().unwrap();
        assert_eq!(captured_events.len(), 1);

        match &captured_events[0].event {
            Some(ProtoEvent::Jet(jet_event)) => {
                assert_eq!(jet_event.sig, sig.as_ref());
                assert_eq!(jet_event.slot, slot);
                assert_eq!(jet_event.ts_received, ts_received);
                assert_eq!(jet_event.jet_sends.len(), 1); // TransactionReceived is skipped
            }
            _ => panic!("Expected Jet event"),
        }
    }
}
