use {
    crate::{
        config::ConfigLewisEvents,
        event_tracker::{SendAttempt, SendResult, TransactionEventTracker},
        metrics::jet as metrics,
        proto::lewis::{transaction_tracker_client::TransactionTrackerClient, Event, EventAck},
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

#[derive(Clone)]
pub struct LewisEventClient {
    tx: Option<mpsc::Sender<Event>>,
}

impl LewisEventClient {
    pub fn create_event_tracker(
        config: Option<ConfigLewisEvents>,
    ) -> (
        Option<Arc<dyn TransactionEventTracker + Send + Sync>>,
        Option<impl Future<Output = anyhow::Result<()>> + Send>,
    ) {
        let Some(config) = config else {
            return (None, None);
        };

        let (tx, rx) = mpsc::channel(config.queue_size_buffer);
        let client = Self { tx: Some(tx) };

        let tracker = Arc::new(client) as Arc<dyn TransactionEventTracker + Send + Sync>;
        info!(
            "Lewis event tracker created for endpoint: {}",
            config.lewis_endpoint
        );

        let fut = Self::run_event_loop(config, rx);
        (Some(tracker), Some(fut))
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
                while rx.recv().await.is_some() {}
                Ok(())
            }
        }
    }

    async fn connect_and_stream(
        config: &ConfigLewisEvents,
        rx: &mut mpsc::Receiver<Event>,
    ) -> anyhow::Result<()> {
        debug!("Connecting to Lewis at {}", config.lewis_endpoint);

        let channel = Endpoint::from_shared(config.lewis_endpoint.clone())?
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

                            if pending >= 10 || last_flush.elapsed() > Duration::from_millis(100) {
                                tx.flush().await.context("Failed to flush stream")?;
                                debug!("Flushed {} events", pending);
                                pending = 0;
                                last_flush = tokio::time::Instant::now();
                            }
                        }
                        None => {
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

#[async_trait::async_trait]
impl TransactionEventTracker for LewisEventClient {
    fn track_transaction_send(
        &self,
        signature: &Signature,
        slot: Slot,
        send_attempts: Vec<SendAttempt>,
    ) {
        let mut builder = event_builders::JetEventBuilder::new(
            // TODO: we need to define these.
            String::new(), // req_id - we'll leave these empty for now
            String::new(), // cascade_id
            String::new(), // jet_gateway_id
            String::new(), // jet_id
            signature,
            slot,
        );

        for attempt in send_attempts {
            builder = match attempt.result {
                SendResult::Success => builder.add_send(
                    attempt.validator.to_string(),
                    attempt.tpu_addr.to_string(),
                    false,
                    None,
                ),
                SendResult::Skipped { reason } => builder.add_send(
                    attempt.validator.to_string(),
                    attempt.tpu_addr.to_string(),
                    true,
                    Some(reason),
                ),
                SendResult::Failed { error } => builder.add_send(
                    attempt.validator.to_string(),
                    attempt.tpu_addr.to_string(),
                    false,
                    Some(error),
                ),
            };
        }

        let event = builder.build();
        self.emit(event);
    }
}

pub mod event_builders {
    use {
        super::*,
        crate::proto::lewis::{Event, EventJet, JetSend},
        std::time::SystemTime,
    };

    pub struct JetEventBuilder {
        req_id: String,
        cascade_id: String,
        jet_gateway_id: String,
        jet_id: String,
        signature: Vec<u8>,
        slot: u64,
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
        ) -> Self {
            Self {
                req_id,
                cascade_id,
                jet_gateway_id,
                jet_id,
                signature: signature.as_ref().to_vec(),
                slot,
                jet_sends: Vec::new(),
            }
        }

        pub fn add_send(
            mut self,
            validator: String,
            tpu_addr: String,
            skipped: bool,
            error: Option<String>,
        ) -> Self {
            self.jet_sends.push(JetSend {
                validator,
                ts: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                skipped,
                tpu_addr,
                error: error.unwrap_or_default(),
            });
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
                })),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_pubkey::Pubkey,
        std::net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let (tx, mut rx) = mpsc::channel(10);
        let client = Arc::new(LewisEventClient { tx: Some(tx) });

        let signature = Signature::from([1u8; 64]);
        let slot = 98765;

        let send_attempts = vec![
            SendAttempt::success(
                Pubkey::from([1u8; 32]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            ),
            SendAttempt::failed(
                Pubkey::from([2u8; 32]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8002),
                "Connection timeout".to_string(),
            ),
            SendAttempt::skipped(
                Pubkey::from([3u8; 32]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 8003),
                "Rate limited".to_string(),
            ),
        ];

        client.track_transaction_send(&signature, slot, send_attempts);

        let event = rx.recv().await.unwrap();
        let jet_event = match event.event {
            Some(crate::proto::lewis::event::Event::Jet(jet)) => jet,
            _ => panic!("Expected Jet event"),
        };

        assert_eq!(jet_event.sig, signature.as_ref());
        assert_eq!(jet_event.slot, slot);
        assert_eq!(jet_event.jet_sends.len(), 3);

        assert!(!jet_event.jet_sends[0].skipped);
        assert!(jet_event.jet_sends[0].error.is_empty());

        assert!(!jet_event.jet_sends[1].skipped);
        assert_eq!(jet_event.jet_sends[1].error, "Connection timeout");

        assert!(jet_event.jet_sends[2].skipped);
        assert_eq!(jet_event.jet_sends[2].error, "Rate limited");
    }

    #[tokio::test]
    async fn test_event_emission() {
        let (tx, mut rx) = mpsc::channel(10);
        let client = LewisEventClient { tx: Some(tx) };

        let event = event_builders::JetEventBuilder::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            &Signature::default(),
            12345,
        )
        .build();

        client.emit(event);

        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_full_queue_drops_events() {
        let (tx, mut rx) = mpsc::channel(1);
        let client = LewisEventClient { tx: Some(tx) };

        let event1 = event_builders::JetEventBuilder::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            &Signature::default(),
            1,
        )
        .build();
        client.emit(event1);

        let event2 = event_builders::JetEventBuilder::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            &Signature::default(),
            2,
        )
        .build();
        client.emit(event2);

        drop(client);
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 1);
    }
}
