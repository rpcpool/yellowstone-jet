use {
    clap::Parser,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    std::{
        env,
        io::{self, IsTerminal as _},
        path::PathBuf,
        sync::Arc,
    },
    tracing::level_filters::LevelFilter,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt},
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcClient, Interceptor},
    yellowstone_jet_tpu_client::{
        core::UpcomingLeaderPredictor,
        rpc::schedule::spawn_managed_leader_schedule,
        yellowstone_grpc::{
            schedule::YellowstoneUpcomingLeader, slot_tracker::YellowstoneSlotTrackerOk,
        },
    },
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

async fn build_geyser_client(
    endpoint: String,
    x_token: Option<String>,
) -> GeyserGrpcClient<impl Interceptor + Clone + 'static> {
    GeyserGrpcBuilder::from_shared(endpoint)
        .expect("from_shared")
        .x_token(x_token)
        .expect("x-token")
        .tls_config(ClientTlsConfig::default().with_enabled_roots())
        .expect("tls_config")
        .connect()
        .await
        .expect("connect")
}

#[derive(clap::Parser, Debug)]
struct Args {
    #[clap(long, short)]
    /// Path to .env file to load
    dotenv: Option<PathBuf>,
    /// Endpoint to RPC service
    #[clap(long, short)]
    rpc: Option<String>,
    /// Endpoint to Yellowstone gRPC service
    #[clap(long, short)]
    grpc: Option<String>,
    /// X-Token for Yellowstone gRPC service
    x_token: Option<String>,
}

#[tokio::main]
async fn main() {
    use std::io::Write;
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
    let rpc_endpoint = match args.rpc {
        Some(endpoint) => endpoint,
        None => env::var("RPC_ENDPOINT")
            .expect("RPC_ENDPOINT must be set in dotenv file or environment"),
    };

    let grpc_endpoint = match args.grpc {
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

    let geyser_client = build_geyser_client(grpc_endpoint, grpc_x_token).await;

    let rpc_client1 = Arc::new(RpcClient::new_with_commitment(
        rpc_endpoint.clone(),
        CommitmentConfig::confirmed(),
    ));

    let (managed_leader_schedule, mut managed_leader_schedule_jh) =
        spawn_managed_leader_schedule(Arc::clone(&rpc_client1), Default::default())
            .await
            .expect("spawn_managed_leader_schedule");

    let YellowstoneSlotTrackerOk {
        atomic_slot_tracker,
        join_handle: mut slot_tracker_jh,
    } = yellowstone_jet_tpu_client::yellowstone_grpc::slot_tracker::atomic_slot_tracker(
        geyser_client,
    )
    .await
    .expect("atomic_slot_tracker")
    .expect("some");

    let mut ctrlc = tokio::spawn(tokio::signal::ctrl_c());

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(400));
    let leader_predictor = YellowstoneUpcomingLeader {
        managed_schedule: managed_leader_schedule.clone(),
        slot_tracker: Arc::clone(&atomic_slot_tracker),
    };
    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = &mut ctrlc => break,
            _ = &mut slot_tracker_jh => {
                tracing::error!("Yellowstone slot tracker task exited unexpectedly");
                break;
            }
            _ = &mut managed_leader_schedule_jh => {
                tracing::error!("managed leader schedule task exited unexpectedly");
                break;
            }
        }

        let current_slot = rpc_client1.get_slot().await.expect("get_slot");

        let leader = managed_leader_schedule
            .get_leader(current_slot)
            .expect("poisoned")
            .expect("unknow slot");

        let upcoming_leaders = leader_predictor.try_predict_next_n_leaders(3);

        writeln!(
            &mut out,
            "current_slot: {current_slot}, leader: {leader}, upcoming leaders: {upcoming_leaders:?}"
        )
        .expect("writeln");
    }
}
