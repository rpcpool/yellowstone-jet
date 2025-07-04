use {
    crate::{
        config::ConfigMetricsUpstream,
        metrics::jet as metrics,
        proto::metrics::{
            jet_metrics_upstream_client::JetMetricsUpstreamClient, transaction_event::Event,
            TransactionEvent, TransactionEventSendAttempt,
        },
        util::{
            IncrementalBackoff, WaitShutdown, WaitShutdownJoinHandleResult,
            WaitShutdownSharedJoinHandle,
        },
    },
    anyhow::Context,
    futures::{
        future::{pending, FutureExt},
        sink::SinkExt,
    },
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        fmt,
        net::SocketAddr,
        ops::DerefMut,
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::sync::{mpsc, Mutex, Notify},
    tonic::transport::channel::{ClientTlsConfig, Endpoint},
    tracing::error,
};

#[derive(Debug)]
pub struct GrpcClientInner {
    tx: mpsc::Sender<TransactionEvent>,
    shutdown: Arc<Notify>,
    join_handle: Mutex<Option<WaitShutdownSharedJoinHandle>>,
}

#[derive(Debug, Clone)]
pub struct GrpcClient {
    inner: Option<Arc<GrpcClientInner>>,
}

impl WaitShutdown for GrpcClient {
    fn shutdown(&self) {
        if let Some(inner) = &self.inner {
            inner.shutdown.notify_one();
        }
    }

    async fn wait_shutdown_future(self) -> WaitShutdownJoinHandleResult {
        if let Some(inner) = &self.inner {
            if let Some(jh) = inner.join_handle.lock().await.take() {
                let mut locked = jh.lock().await;
                return locked.deref_mut().await;
            }
        }
        Ok(Ok(()))
    }
}

impl GrpcClient {
    pub fn new(config: Option<ConfigMetricsUpstream>) -> Self {
        Self {
            inner: config.map(|config| {
                let (tx, rx) = mpsc::channel(config.queue_size_buffer);
                let shutdown = Arc::new(Notify::new());
                Arc::new(GrpcClientInner {
                    tx,
                    shutdown: Arc::clone(&shutdown),
                    join_handle: Mutex::new(Some(Self::spawn(async move {
                        tokio::select! {
                            () = shutdown.notified() => Ok(()),
                            result = Self::send_loop(config.endpoint, config.queue_size_grpc, Arc::new(Mutex::new(rx))) => result,
                        }
                    }))),
                })
            }),
        }
    }

    async fn send_loop(
        endpoint: String,
        buffer_size: usize,
        rx: Arc<Mutex<mpsc::Receiver<TransactionEvent>>>,
    ) -> anyhow::Result<()> {
        let mut backoff = IncrementalBackoff::default();
        loop {
            backoff.maybe_tick().await;

            let mut client = match Endpoint::from_shared(endpoint.clone())?
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(1))
                .tls_config(ClientTlsConfig::new().with_native_roots())?
                .connect()
                .await
            {
                Ok(channel) => {
                    backoff.reset();
                    JetMetricsUpstreamClient::new(channel)
                }
                Err(error) => {
                    error!(?error, "failed to connect to gRPC upstream metrics");
                    backoff.init();
                    continue;
                }
            };

            let (mut stream_tx, stream_rx) = futures::channel::mpsc::channel(buffer_size);

            let rx = Arc::clone(&rx);
            let jh = tokio::spawn(async move {
                let mut rx = rx.lock().await;
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
                                    metrics::metrics_upstream_feed_inc();
                                }
                                None => return Ok::<(), anyhow::Error>(()),
                            }
                        }
                    }
                }
            });

            if let Err(error) = client.stream_transaction_events(stream_rx).await {
                error!(%error, "metrics upstream gRPC stream finished");
            } else if let Err(error) = jh.await {
                error!(%error, "grpc upstream metrics stream failed");
            }
        }
    }

    pub fn emit_send_attempt(
        &self,
        signature: &Signature,
        leader: &Pubkey,
        slots: &[Slot],
        tpu_addr: SocketAddr,
        error: Option<impl fmt::Debug>,
    ) {
        if let Some(inner) = &self.inner {
            let event = TransactionEvent {
                timestamp: Some(SystemTime::now().into()),
                signature: signature.as_ref().to_vec(),
                event: Some(Event::SendAttempt(TransactionEventSendAttempt {
                    leader: leader.as_ref().to_vec(),
                    slots: slots.to_vec(),
                    tpu_addr: tpu_addr.to_string(),
                    error: error.map(|error| format!("{:?}", error)),
                })),
            };
            metrics::metrics_upstream_push_inc(inner.tx.try_send(event).map_err(|_error| ()))
        }
    }
}
