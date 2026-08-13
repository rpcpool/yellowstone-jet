use {
    std::io::{self, IsTerminal},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

pub mod http_ndjson_drain;

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
