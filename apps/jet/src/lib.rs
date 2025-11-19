#![recursion_limit = "256"]
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
pub mod grpc_lewis;
pub mod identity;
pub mod jet_gateway;
pub mod metrics;
pub mod payload;
pub mod proto;
pub mod pubkey_challenger;
pub mod quic_client;
pub mod rooted_transaction_state;
pub mod rpc;
pub mod solana;
pub mod solana_rpc_utils;
pub mod stake;
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

pub fn setup_tracing_test(module: &str) -> anyhow::Result<()> {
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_line_number(true);

    let level_layer = EnvFilter::builder()
        .with_default_directive(format!("{module}=trace").parse()?)
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()?;
    Ok(())
}

pub fn setup_tracing_test_many(
    modules: impl IntoIterator<Item = &'static str>,
) -> anyhow::Result<()> {
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_line_number(true);

    let directives = modules
        .into_iter()
        .fold(EnvFilter::default(), |filter, module| {
            filter.add_directive(format!("{module}=trace").parse().expect("invalid module"))
        });

    tracing_subscriber::registry()
        .with(io_layer)
        .with(directives)
        .try_init()?;
    Ok(())
}
