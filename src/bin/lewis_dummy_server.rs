use {
    clap::Parser,
    solana_signature::Signature,
    std::{
        net::SocketAddr,
        sync::atomic::{AtomicU64, Ordering},
    },
    tonic::{Request, Response, Status, Streaming, transport::server::Server},
    tracing::{error, info},
    yellowstone_jet::{
        proto::lewis::{
            Event, EventAck,
            event::Event as EventType,
            transaction_tracker_server::{TransactionTracker, TransactionTrackerServer},
        },
        setup_tracing,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, default_value = "127.0.0.1:8005")]
    pub listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    setup_tracing(false).expect("Failed to set up tracing");
    let args = Args::parse();

    let service = TransactionTrackerServer::new(DummyLewisService {
        client_counter: AtomicU64::new(0),
    });

    info!("Lewis dummy server listening on {}", args.listen);

    Server::builder()
        .add_service(service)
        .serve(args.listen)
        .await
        .expect("Failed to start server");
    Ok(())
}

#[derive(Debug)]
pub struct DummyLewisService {
    client_counter: AtomicU64,
}

impl DummyLewisService {
    async fn handle_stream(
        client_id: u64,
        mut request: Request<Streaming<Event>>,
    ) -> Result<(), Status> {
        info!(client_id, "new event stream started");

        let mut event_count = 0u64;

        while let Some(event) = request
            .get_mut()
            .message()
            .await
            .expect("Failed to read event")
        {
            event_count += 1;

            match event.event {
                Some(EventType::Cascade(_)) => {
                    continue; // Skip cascade events
                }
                Some(EventType::Jet(jet)) => {
                    let sig = Signature::try_from(jet.sig)
                        .map_err(|_| Status::invalid_argument("invalid signature"))
                        .expect("Failed to parse signature");
                    info!(
                        client_id,
                        event_count,
                        %sig,
                        req_id = jet.req_id,
                        jet_id = jet.jet_id,
                        slot = jet.slot,
                        tpu_addr = jet.tpu_addr,
                        skipped = jet.skipped,
                        error = ?jet.error,
                        "received jet send attempt"
                    );
                }
                None => {
                    error!(client_id, event_count, "received event without type");
                }
            }
        }

        info!(client_id, event_count, "event stream completed");
        Ok(())
    }
}

#[tonic::async_trait]
impl TransactionTracker for DummyLewisService {
    async fn track(
        &self,
        _request: Request<yellowstone_jet::proto::lewis::Transactions>,
    ) -> Result<Response<yellowstone_jet::proto::lewis::Empty>, Status> {
        Err(Status::unimplemented("track method not implemented"))
    }

    async fn track_events(
        &self,
        request: Request<Streaming<Event>>,
    ) -> Result<Response<EventAck>, Status> {
        let client_id = self.client_counter.fetch_add(1, Ordering::Relaxed);

        if let Err(error) = Self::handle_stream(client_id, request).await {
            error!(client_id, %error, "stream handler error");
            return Err(Status::internal("stream handler error"));
        }

        Ok(Response::new(EventAck {}))
    }
}
