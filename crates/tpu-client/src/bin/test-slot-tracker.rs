use {
    clap::Parser,
    std::{
        env,
        io::{self, IsTerminal as _, Write},
        path::PathBuf,
    },
    tracing::level_filters::LevelFilter,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt},
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder},
    yellowstone_jet_tpu_client::{self, yellowstone_grpc::slot_tracker::YellowstoneSlotTrackerOk},
};

pub fn setup_tracing() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let subscriber = tracing_subscriber::registry().with(env_filter);
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_ansi(is_atty);
    subscriber.with(io_layer).try_init().expect("try_init");
}

#[derive(clap::Parser, Debug)]
struct Args {
    /// Endpoint to Yellowstone gRPC service
    #[clap(long, short)]
    endpoint: Option<String>,

    /// X-Token for Yellowstone gRPC service
    #[clap(long, short)]
    x_token: Option<String>,

    /// Path to .env file to load
    dotenv: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    setup_tracing();
    let mut out = std::io::stdout();
    let args = Args::parse();
    if let Ok(env_path) = args.dotenv.unwrap_or("./.env".into()).canonicalize() {
        if dotenvy::from_path(env_path).is_err() {
            tracing::warn!("Failed to load .env file");
        }
    } else {
        tracing::warn!("Failed to canonicalize .env file path");
    }

    let grpc_endpoint = match args.endpoint {
        Some(endpoint) => endpoint,
        None => env::var("GRPC_ENDPOINT")
            .expect("GRPC_ENDPOINT must be set in dotenv file or environment"),
    };

    let grpc_x_token = match args.x_token {
        Some(x_token) => Some(x_token),
        None => match env::var("GRPC_X_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => {
                tracing::warn!("GRPC_X_TOKEN not set in dotenv file or environment");
                None
            }
        },
    };

    let geyser_client = GeyserGrpcBuilder::from_shared(grpc_endpoint)
        .expect("from_shared")
        .x_token(grpc_x_token)
        .expect("x-token")
        .tls_config(ClientTlsConfig::default().with_enabled_roots())
        .expect("tls_config")
        .connect()
        .await
        .expect("connect");

    let YellowstoneSlotTrackerOk {
        atomic_slot_tracker,
        mut join_handle,
    } = yellowstone_jet_tpu_client::yellowstone_grpc::slot_tracker::atomic_slot_tracker(
        geyser_client,
    )
    .await
    .expect("atomic_slot_tracker")
    .expect("Some");
    let mut ctrlc = tokio::spawn(tokio::signal::ctrl_c());

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(400));
    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = &mut ctrlc => break,
            _ = &mut join_handle => {
                tracing::error!("Yellowstone slot tracker task exited unexpectedly");
                break;
            }
        }
        let slot = atomic_slot_tracker.load().expect("load");
        writeln!(&mut out, "Current Yellowstone slot: {slot}").expect("write");
        tracing::info!("Current Yellowstone slot: {}", slot);
    }
}
