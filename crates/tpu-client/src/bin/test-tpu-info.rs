use {
    clap::Parser,
    solana_client::nonblocking::rpc_client::RpcClient,
    std::{
        env,
        io::{self, IsTerminal as _},
        path::PathBuf,
        sync::Arc,
    },
    tracing::level_filters::LevelFilter,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt},
    yellowstone_jet_tpu_client::rpc::tpu_info::{
        RpcClusterTpuQuicInfoServiceConfig, rpc_cluster_tpu_info_service,
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

#[derive(clap::Parser, Debug)]
struct Args {
    /// Path to .env file to load
    dotenv: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    setup_tracing();

    let args = Args::parse();
    if let Ok(env_path) = args.dotenv.unwrap_or("./.env".into()).canonicalize() {
        if dotenvy::from_path(env_path).is_err() {
            tracing::warn!("Failed to load .env file");
        }
    } else {
        tracing::warn!("Failed to canonicalize .env file path");
    }
    let rpc_endpoint =
        env::var("RPC_ENDPOINT").expect("RPC_ENDPOINT must be set in dotenv file or environment");
    let rpc_client = Arc::new(RpcClient::new(rpc_endpoint));

    let config = RpcClusterTpuQuicInfoServiceConfig {
        refresh_interval: std::time::Duration::from_secs(10),
        ..Default::default()
    };

    let (tpu_info_service, mut jh) = rpc_cluster_tpu_info_service(rpc_client, config)
        .await
        .expect("rpc_cluster_tpu_info_service");

    let ctrlc = tokio::signal::ctrl_c();

    tokio::select! {
        _ = ctrlc => {
            tracing::info!("Received Ctrl-C, shutting down");
            drop(tpu_info_service);
            jh.await.expect("tpu_info_service join handle");
        }
        _ = &mut jh => {
            tracing::error!("tpu_info_service task exited unexpectedly");
        }
    }
}
