use {
    clap::Parser,
    futures::SinkExt,
    rand::Rng,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    yellowstone_jet::{
        grpc_lewis::event_builders::JetEventBuilder,
        proto::lewis::{
            transaction_tracker_client::TransactionTrackerClient,
            Event,
        },
        setup_tracing,
    },
    std::time::Duration,
    tokio::time::interval,
    tonic::transport::channel::Endpoint,
    tracing::info,
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Lewis endpoint
    #[clap(long, default_value = "http://localhost:8005")]
    pub endpoint: String,

    /// Events per second
    #[clap(long, default_value = "1")]
    pub rate: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing(false)?;
    let args = Args::parse();

    // Connect to Lewis
    let channel = Endpoint::from_shared(args.endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(1))
        .connect()
        .await?;

    let mut client = TransactionTrackerClient::new(channel);
    let (mut stream_tx, stream_rx) = futures::channel::mpsc::channel(100);

    // Start the streaming call
    let response = client.track_events(stream_rx).await?;
    info!("Connected to Lewis, received ack: {:?}", response);
    info!("Sending only Jet events at {} events/second", args.rate);

    // Event generation loop
    let mut interval = interval(Duration::from_millis(1000 / args.rate));
    let mut event_counter = 0u64;

    loop {
        interval.tick().await;
        event_counter += 1;

        let event = create_jet_event(event_counter);

        info!(event_counter, "sending jet event");
        stream_tx.send(event).await?;

        // Occasionally flush to ensure events are sent
        if event_counter % 10 == 0 {
            stream_tx.flush().await?;
        }
    }
}

fn create_jet_event(counter: u64) -> Event {
    let mut rng = rand::thread_rng();
    let send_attempts = rng.gen_range(1..=5);

    let mut builder = JetEventBuilder::new(
        format!("req-{}", counter),
        format!("cascade-{}", counter % 3),
        format!("jet-gateway-{}", counter % 2),
        format!("jet-{}", counter % 4),
        &Signature::default(),
        250_000_000 + counter,
    );

    for i in 0..send_attempts {
        let has_error = rng.gen_bool(0.1);
        builder = builder.add_send(
            Pubkey::new_unique().to_string(),
            format!("192.168.1.{}:8003", i + 1),
            if has_error {
                Some("Connection timeout".to_string())
            } else {
                None
            },
        );
    }

    builder.build()
}
