use {anyhow::Context, clap::Parser, serde::Deserialize, std::path::PathBuf, url::Url};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    username: String,
    password: String,
}

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Waits for a ClickHouse refreshable view's cycle to complete, then runs a list of \
             purge SQL scripts against it -- e.g. mv_landed_transactions followed by \
             purge_chain_transaction_staging.sql / purge_sent_transaction_pending.sql. Loops \
             forever; each iteration blocks on `SYSTEM WAIT VIEW`, so it self-paces to the \
             view's own refresh cadence instead of guessing at a cron interval."
)]
struct Args {
    /// Path to config file (YAML)
    #[clap(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// ClickHouse HTTP interface endpoint, e.g. http://127.0.0.1:8123
    url: Url,
    credentials: Option<Credentials>,
    /// Name of the refreshable materialized view to wait on before each round of purges,
    /// e.g. `mv_landed_transactions`. Qualify with a database if it isn't in the default one.
    wait_for_view: String,
    /// Purge scripts to run, in order, once the wait above completes.
    purge_scripts: Vec<PathBuf>,
}

async fn execute(
    client: &reqwest::Client,
    url: &Url,
    credentials: &Option<Credentials>,
    query: &str,
) -> anyhow::Result<()> {
    let request_builder = client.post(url.as_str());
    let request_builder = match credentials {
        Some(Credentials { username, password }) => {
            request_builder.basic_auth(username, Some(password))
        }
        None => request_builder,
    };
    request_builder
        .body(query.to_owned())
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_str = std::fs::read_to_string(&args.config)?;
    let config: Config = serde_yaml::from_str(&config_str)?;

    let purge_scripts: Vec<(PathBuf, String)> = config
        .purge_scripts
        .into_iter()
        .map(|path| {
            let sql = std::fs::read_to_string(&path)
                .with_context(|| format!("reading purge script {path:?}"))?;
            Ok((path, sql))
        })
        .collect::<anyhow::Result<_>>()?;

    let wait_query = format!("SYSTEM WAIT VIEW {}", config.wait_for_view);
    let client = reqwest::Client::new();

    loop {
        tracing::info!(view = %config.wait_for_view, "waiting for refresh cycle to complete");
        execute(&client, &config.url, &config.credentials, &wait_query).await?;

        for (path, sql) in &purge_scripts {
            tracing::info!(?path, "running purge script");
            execute(&client, &config.url, &config.credentials, sql).await?;
        }
    }
}
