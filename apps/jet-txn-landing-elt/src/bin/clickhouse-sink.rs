use {
    clap::Parser,
    futures::{Sink, SinkExt, TryStreamExt, future},
    jet_transaction_landing::http_ndjson_drain::{
        Credentials, DEFAULT_MAX_INFLIGHT_SENDS, DEFAULT_MAX_NDJSON_LEN, HttpNdJsonSink,
        HttpTxnTraceDrainConfig, XHeaderEntry,
    },
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        num::NonZeroUsize,
        path::PathBuf,
        pin::Pin,
        task::{Context, Poll},
    },
    url::Url,
    yellowstone_fumarole_client::{
        FumaroleClient, FumaroleSubscribeConfig,
        config::FumaroleConfig,
        proto::{CreateConsumerGroupRequest, InitialOffsetPolicy},
        stream::{FumaroleBlockEvent, FumaroleBlockStreamEvent},
    },
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterEntry, SubscribeRequestFilterTransactions,
            SubscribeUpdateEntry, SubscribeUpdateTransactionStatus, subscribe_update::UpdateOneof,
        },
        tonic::Code::AlreadyExists,
    },
};

///
/// A transaction that landed on-chain, as observed from a Fumarole block stream.
///
#[derive(Debug, Clone, Serialize)]
pub struct LandedTransaction {
    pub signature: String,
    pub slot: u64,
    pub txn_index: u64,
    pub failed: bool,
}

///
/// A block entry -- a batch of transactions sharing a single PoH tick -- as observed from a
/// Fumarole block stream. Destined for `chain_entry_staging`.
///
#[derive(Debug, Clone, Serialize)]
pub struct LandedEntry {
    pub slot: u64,
    pub entry_index: u64,
    pub executed_transactions_count: u64,
}

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Jet tranasction landing Clickhouse Sink Ingests landed transactions from a Yellowstone Fumarole block stream and sends them to a Clickhouse HTTP NDJSON endpoint."
)]
struct Args {
    /// Path to config file (YAML)
    #[clap(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MyFumaroleConfig {
    #[serde(flatten)]
    fumarole: FumaroleConfig,
    subscriber_name: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigTracing {
    #[serde(default)]
    json: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    fumarole: MyFumaroleConfig,
    #[serde(default = "Config::default_clickhouse_url")]
    clickhouse_url: Url,
    #[serde(default = "Config::default_clickhouse_username")]
    clickhouse_username: String,
    #[serde(default = "Config::default_clickhouse_password")]
    clickhouse_password: String,
    #[serde(default = "Config::default_max_batch_size")]
    max_batch_size: usize,
    #[serde(default = "Config::default_max_inflight_batches")]
    max_inflight_batches: usize,
    #[serde(default)]
    tracing: ConfigTracing,
}

impl Config {
    fn default_max_batch_size() -> usize {
        DEFAULT_MAX_NDJSON_LEN
    }

    fn default_max_inflight_batches() -> usize {
        DEFAULT_MAX_INFLIGHT_SENDS
    }

    fn default_clickhouse_url() -> Url {
        Url::parse("http://localhost:8123").unwrap()
    }

    fn default_clickhouse_username() -> String {
        "default".to_string()
    }

    fn default_clickhouse_password() -> String {
        "".to_string()
    }
}

const INSERT_CHAIN_TRANASCTION_STAGING_HTTP_ROUTE: &str = "/?query=INSERT%20INTO%20jet.chain_transaction_staging%20FORMAT%20JSONEachRow&async_insert=1&wait_for_async_insert=0";

const INSERT_CHAIN_ENTRY_STAGING_HTTP_ROUTE: &str = "/?query=INSERT%20INTO%20jet.chain_entry_staging%20FORMAT%20JSONEachRow&async_insert=1&wait_for_async_insert=0";

///
/// Filters one block's updates down to its landed transactions. Takes `&FumaroleBlockEvent`
/// rather than consuming it -- the block is shared (via `Arc`) between this projection and
/// `project_entries`, so neither side can take ownership of it.
///
fn project_transactions(block: &FumaroleBlockEvent) -> Vec<LandedTransaction> {
    let slot = block.slot;
    block
        .iter()
        .filter_map(|update| match update.update_oneof.clone()? {
            UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                signature,
                err,
                index,
                ..
            }) => Some(LandedTransaction {
                signature: bs58::encode(&signature).into_string(),
                txn_index: index,
                slot,
                failed: err.is_some(),
            }),
            _ => None,
        })
        .collect()
}

///
/// Filters one block's updates down to its entries. See `project_transactions` for why this
/// borrows rather than consumes.
///
fn project_entries(block: &FumaroleBlockEvent) -> Vec<LandedEntry> {
    let slot = block.slot;
    block
        .iter()
        .filter_map(|update| match update.update_oneof.clone()? {
            UpdateOneof::Entry(SubscribeUpdateEntry {
                index,
                executed_transaction_count,
                ..
            }) => Some(LandedEntry {
                slot,
                entry_index: index,
                executed_transactions_count: executed_transaction_count,
            }),
            _ => None,
        })
        .collect()
}

struct LandedBlockSink {
    transaction_drain: HttpNdJsonSink,
    entry_drain: HttpNdJsonSink,
}

impl LandedBlockSink {
    fn new(transaction_drain: HttpNdJsonSink, entry_drain: HttpNdJsonSink) -> Self {
        Self {
            transaction_drain,
            entry_drain,
        }
    }
}

impl Sink<FumaroleBlockEvent> for LandedBlockSink {
    type Error = jet_transaction_landing::http_ndjson_drain::HttpTxnTraceDrainError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match <HttpNdJsonSink as Sink<Vec<LandedTransaction>>>::poll_ready(
            Pin::new(&mut this.transaction_drain),
            cx,
        ) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        <HttpNdJsonSink as Sink<Vec<LandedEntry>>>::poll_ready(Pin::new(&mut this.entry_drain), cx)
    }

    fn start_send(self: Pin<&mut Self>, item: FumaroleBlockEvent) -> Result<(), Self::Error> {
        let this = self.get_mut();
        <HttpNdJsonSink as Sink<Vec<LandedTransaction>>>::start_send(
            Pin::new(&mut this.transaction_drain),
            project_transactions(&item),
        )?;
        <HttpNdJsonSink as Sink<Vec<LandedEntry>>>::start_send(
            Pin::new(&mut this.entry_drain),
            project_entries(&item),
        )?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match <HttpNdJsonSink as Sink<Vec<LandedTransaction>>>::poll_flush(
            Pin::new(&mut this.transaction_drain),
            cx,
        ) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        <HttpNdJsonSink as Sink<Vec<LandedEntry>>>::poll_flush(Pin::new(&mut this.entry_drain), cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match <HttpNdJsonSink as Sink<Vec<LandedTransaction>>>::poll_close(
            Pin::new(&mut this.transaction_drain),
            cx,
        ) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        <HttpNdJsonSink as Sink<Vec<LandedEntry>>>::poll_close(Pin::new(&mut this.entry_drain), cx)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config_str = std::fs::read_to_string(&args.config)?;
    let config: Config = serde_yaml::from_str(&config_str)?;
    jet_transaction_landing::setup_tracing(config.tracing.json)?;

    let endpoint = config.fumarole.fumarole.endpoint.clone();
    let mut client = FumaroleClient::connect(config.fumarole.fumarole.clone()).await?;
    tracing::info!(endpoint, "connected to fumarole endpoint");

    let request = SubscribeRequest {
        transactions_status: HashMap::from([(
            "jet".to_owned(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                ..Default::default()
            },
        )]),
        // Entries are a separate subscription filter from transactions_status -- without this,
        // the server never sends SubscribeUpdateEntry updates at all, so project_entries always
        // sees an empty block and chain_entry_staging never gets a row, regardless of grants.
        entry: HashMap::from([("jet".to_owned(), SubscribeRequestFilterEntry::default())]),
        commitment: Some(1), // Confirmed
        ..Default::default()
    };

    let subscribe_config = FumaroleSubscribeConfig {
        data_channel_capacity: NonZeroUsize::new(100_000).unwrap(),
        ..Default::default()
    };

    let result = client
        .create_consumer_group(CreateConsumerGroupRequest {
            consumer_group_name: config.fumarole.subscriber_name.clone(),
            initial_offset_policy: InitialOffsetPolicy::Latest as i32,
            ..Default::default()
        })
        .await;

    if let Err(e) = result {
        if e.code() == AlreadyExists {
            tracing::info!("consumer group already exists, continuing");
        } else {
            return Err(e.into());
        }
    };

    let subscription = client
        .subscribe_with_config(
            config.fumarole.subscriber_name.clone(),
            request,
            subscribe_config,
        )
        .await?;
    let (_sink, fumarole_stream) = subscription.split();

    let transaction_drain_url = config
        .clickhouse_url
        .join(INSERT_CHAIN_TRANASCTION_STAGING_HTTP_ROUTE)?;

    tracing::info!(?transaction_drain_url, "clickhouse transaction drain url");
    let entry_drain_url = config
        .clickhouse_url
        .join(INSERT_CHAIN_ENTRY_STAGING_HTTP_ROUTE)?;

    tracing::info!(?entry_drain_url, "clickhouse entry drain url");
    let creds = Credentials::XHeaders(vec![
        XHeaderEntry {
            name: "X-ClickHouse-User".to_string(),
            value: config.clickhouse_username.clone(),
        },
        XHeaderEntry {
            name: "X-ClickHouse-Key".to_string(),
            value: config.clickhouse_password.clone(),
        },
    ]);

    let transaction_drain_config = HttpTxnTraceDrainConfig {
        url: transaction_drain_url,
        credentials: Some(creds.clone()),
        max_ndjson_len: config.max_batch_size,
        max_inflight_sends: config.max_inflight_batches,
    };

    let entry_drain_config = HttpTxnTraceDrainConfig {
        url: entry_drain_url,
        credentials: Some(creds.clone()),
        max_ndjson_len: config.max_batch_size,
        max_inflight_sends: config.max_inflight_batches,
    };

    let transaction_drain = HttpNdJsonSink::with_config(transaction_drain_config);
    let entry_drain = HttpNdJsonSink::with_config(entry_drain_config);
    let mut landed_sink =
        LandedBlockSink::new(transaction_drain, entry_drain).sink_map_err(anyhow::Error::from);

    let mut block_items = fumarole_stream
        .block_stream()
        .try_filter_map(|event| {
            future::ready(Ok(match event {
                FumaroleBlockStreamEvent::Block(block) => Some(block),
                FumaroleBlockStreamEvent::SlotStatus(_) => None,
            }))
        })
        .map_err(anyhow::Error::from);

    landed_sink.send_all(&mut block_items).await?;

    tracing::info!("fumarole stream ended; exiting");
    Ok(())
}
