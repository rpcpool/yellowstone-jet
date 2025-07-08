use {
    crate::{
        config::ConfigLewisEvents,
        metrics::jet as metrics,
        proto::lewis::{
            transaction_tracker_client::TransactionTrackerClient,
            Event, EventAck,
        },
        util::IncrementalBackoff,
    },
    anyhow::Context,
    futures::{
        future::{pending, Future, FutureExt},
        sink::SinkExt,
    },
    std::{
        sync::Arc,
        time::Duration,
    },
    tokio::sync::mpsc,
    tonic::transport::channel::{Channel, Endpoint},
    tracing::error,
};

pub trait EventSender: Send + Sync + 'static {
    fn try_send(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>>;
}

impl EventSender for mpsc::Sender<Event> {
    fn try_send(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        self.try_send(event)
    }
}

pub trait ConnectionFactory: Send + Sync + 'static {
    fn create_endpoint(&self, endpoint: &str) -> anyhow::Result<Endpoint>;
}

pub struct DefaultConnectionFactory;

impl ConnectionFactory for DefaultConnectionFactory {
    fn create_endpoint(&self, endpoint: &str) -> anyhow::Result<Endpoint> {
        Ok(Endpoint::from_shared(endpoint.to_string())?
            // TODO: make configurable
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(1))
            .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?)
    }
}

#[derive(Clone)]
pub struct LewisEventClient {
    tx: Option<Arc<dyn EventSender>>,
}

impl LewisEventClient {
    pub fn new(
        config: Option<ConfigLewisEvents>,
    ) -> (Self, Option<impl Future<Output = anyhow::Result<()>> + Send>) {
        match config {
            None => (Self { tx: None }, None),
            Some(config) => {
                let (tx, rx) = mpsc::channel(config.queue_size_buffer);
                let client = Self {
                    tx: Some(Arc::new(tx) as Arc<dyn EventSender>),
                };

                let fut = Self::create_event_loop(
                    config,
                    rx,
                    Box::new(DefaultConnectionFactory),
                );

                (client, Some(fut))
            }
        }
    }

    pub fn with_factory(
        config: Option<ConfigLewisEvents>,
        connection_factory: Box<dyn ConnectionFactory>,
    ) -> (Self, Option<impl Future<Output = anyhow::Result<()>> + Send>) {
        match config {
            None => (Self { tx: None }, None),
            Some(config) => {
                let (tx, rx) = mpsc::channel(config.queue_size_buffer);
                let client = Self {
                    tx: Some(Arc::new(tx) as Arc<dyn EventSender>),
                };

                let fut = Self::create_event_loop(config, rx, connection_factory);

                (client, Some(fut))
            }
        }
    }

    fn create_event_loop(
        config: ConfigLewisEvents,
        rx: mpsc::Receiver<Event>,
        connection_factory: Box<dyn ConnectionFactory>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async move {
            Self::send_loop(
                config.lewis_endpoint,
                config.queue_size_grpc,
                rx,
                connection_factory,
            ).await
        }
    }

    async fn send_loop(
        endpoint: String,
        buffer_size: usize,
        mut rx: mpsc::Receiver<Event>,
        connection_factory: Box<dyn ConnectionFactory>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();

        loop {
            backoff.maybe_tick().await;

            let channel = match Self::connect(&endpoint, &*connection_factory).await {
                Ok(channel) => {
                    backoff.reset();
                    channel
                }
                Err(error) => {
                    error!(?error, endpoint, "failed to connect to Lewis event service");
                    backoff.init();
                    continue;
                }
            };

            let mut client = TransactionTrackerClient::new(channel);

            if let Err(error) = Self::handle_stream_session(
                &mut client,
                buffer_size,
                &mut rx,
            ).await {
                error!(?error, "Lewis event stream session failed");
            }
        }
    }

    async fn connect(
        endpoint: &str,
        factory: &dyn ConnectionFactory,
    ) -> anyhow::Result<Channel> {
        factory
            .create_endpoint(endpoint)?
            .connect()
            .await
            .context("failed to establish connection")
    }

    async fn handle_stream_session(
        client: &mut TransactionTrackerClient<Channel>,
        buffer_size: usize,
        rx: &mut mpsc::Receiver<Event>,
    ) -> anyhow::Result<()> {
        let (mut stream_tx, stream_rx) = futures::channel::mpsc::channel(buffer_size);

        let response_fut = client.track_events(stream_rx);

        let feed_result = Self::event_feed_loop(&mut stream_tx, rx).await;

        stream_tx.close_channel();

        match response_fut.await {
            Ok(response) => {
                let _ack: EventAck = response.into_inner();
                feed_result
            }
            Err(status) => Err(anyhow::anyhow!("gRPC error: {}", status)),
        }
    }

    async fn event_feed_loop(
        stream_tx: &mut futures::channel::mpsc::Sender<Event>,
        rx: &mut mpsc::Receiver<Event>,
    ) -> anyhow::Result<()> {
        let mut flush_required = false;

        loop {
            let flush_fut = if flush_required {
                stream_tx.flush().boxed()
            } else {
                pending().boxed()
            };

            tokio::select! {
                result = flush_fut => {
                    result.context("failed to flush events")?;
                    flush_required = false;
                },
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            stream_tx.feed(event).await.context("failed to feed event")?;
                            flush_required = true;
                            metrics::lewis_events_feed_inc();
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    pub fn emit(&self, event: Event) {
        if let Some(tx) = &self.tx {
            metrics::lewis_events_push_inc(tx.try_send(event).map_err(|_| ()));
        }
    }
}

pub mod event_builders {
    use super::*;
    use crate::proto::lewis::{EventJet, JetSend};
    use solana_signature::Signature;
    use std::time::SystemTime;

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
            error: Option<String>,
        ) -> Self {
            self.jet_sends.push(JetSend {
                validator,
                ts: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                skipped: error.is_some(),
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
