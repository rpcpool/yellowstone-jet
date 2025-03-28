use {
    std::io::{self, IsTerminal},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

pub mod blockhash_queue;
pub mod cluster_tpu_info;
pub mod config;
pub mod crypto_provider;
pub mod feature_flags;
pub mod grpc_geyser;
pub mod grpc_jet;
pub mod grpc_metrics;
pub mod metrics;
pub mod payload;
pub mod proto;
pub mod pubkey_challenger;
pub mod quic;
pub mod quic_gateway;
pub mod quic_solana;
pub mod rpc;
pub mod solana;
pub mod solana_rpc_utils;
pub mod stake;
pub mod task_group;
pub mod transaction_handler;
pub mod transactions;
pub mod util;
pub mod version;

pub fn setup_tracing(json: bool) -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let subscriber = tracing_subscriber::registry().with(env_filter);
    if json {
        let io_layer = tracing_subscriber::fmt::layer()
            .with_line_number(true)
            .json();
        subscriber.with(io_layer).try_init()?;
    } else {
        let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
        let io_layer = tracing_subscriber::fmt::layer()
            .with_line_number(true)
            .with_ansi(is_atty);
        subscriber.with(io_layer).try_init()?;
    }
    Ok(())
}
