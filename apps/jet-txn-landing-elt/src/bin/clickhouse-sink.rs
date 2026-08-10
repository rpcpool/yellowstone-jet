use {
    clap::Parser,
    futures::{Stream, StreamExt},
    jet_transaction_landing::http_ndjson_drain::{HttpTxnTraceDrain, HttpTxnTraceDrainConfig},
    serde::{Deserialize, Serialize},
    std::{
        collections::{HashMap, VecDeque},
        num::NonZeroUsize,
        path::PathBuf,
        pin::Pin,
        task::{Context, Poll},
        time::{Duration, Instant},
    },
    yellowstone_fumarole_client::{
        FumaroleClient, FumaroleSubscribeConfig,
        config::FumaroleConfig,
        proto::{CreateConsumerGroupRequest, InitialOffsetPolicy},
        stream::{BlockStream, FumaroleBlockIterator, FumaroleBlockStreamEvent},
    },
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdateTransactionStatus,
            subscribe_update::UpdateOneof,
        },
        tonic::{self, Code::AlreadyExists},
    },
};

///
/// A transaction that landed on-chain, as observed from a Fumarole block stream.
///
#[derive(Debug, Clone, Serialize)]
pub struct LandedTransaction {
    pub signature: String,
    pub slot: u64,
    pub failed: bool,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    fumarole: MyFumaroleConfig,
    /// Name of the persistent Fumarole subscriber to use.
    drain: HttpTxnTraceDrainConfig,
}

pub struct LandedTransactionBlockIterator {
    slot: u64,
    block: FumaroleBlockIterator,
    txn_count: usize,
}

impl Iterator for LandedTransactionBlockIterator {
    type Item = LandedTransaction;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(update) = self.block.next() {
            let slot = self.slot;
            let UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                signature,
                err,
                ..
            }) = update.update_oneof?
            else {
                continue;
            };
            self.txn_count += 1;
            let failed = err.is_some();
            return Some(LandedTransaction {
                signature: bs58::encode(&signature).into_string(),
                slot,
                failed,
            });
        }
        tracing::info!(
            slot = self.slot,
            txn_count = self.txn_count,
            "finished fumarole block landed transactions"
        );
        None
    }
}

///
/// Adapts a Fumarole `BlockStream` into a `Stream<Item = LandedTransaction>`, flattening each
/// completed block into its landed transactions.
///
/// Written as a plain `poll_next` rather than `.filter_map().flat_map()` with async closures:
/// every `async {}` block's generated `Future` is unconditionally `!Unpin`, which poisons the
/// whole `futures`-combinator chain built on top of it and forces `.boxed()` further up. This
/// type holds only plain, already-`Unpin` fields (`BlockStream`, `VecDeque`), so it's `Unpin`
/// for free and can be handed to `HttpTxnTraceDrain` directly.
///
struct LandedTransactionStream {
    inner: BlockStream,
    block_recv_since_last_tick: usize,
    pending: VecDeque<LandedTransactionBlockIterator>,
    last_block_received_at: Instant,
}

impl LandedTransactionStream {
    fn new(inner: BlockStream) -> Self {
        Self {
            inner,
            pending: VecDeque::new(),
            block_recv_since_last_tick: 0,
            last_block_received_at: Instant::now(),
        }
    }
}

impl Stream for LandedTransactionStream {
    type Item = LandedTransactionBlockIterator;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(item) = this.pending.pop_front() {
                return Poll::Ready(Some(item));
            }
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(FumaroleBlockStreamEvent::Block(block)))) => {
                    this.block_recv_since_last_tick += 1;
                    let now = Instant::now();
                    if now.duration_since(this.last_block_received_at) > Duration::from_secs(5) {
                        let block_per_second = this.block_recv_since_last_tick.div_euclid(5);
                        tracing::info!(
                            "block reception rate over last 5 seconds: {block_per_second}/s"
                        );
                        this.block_recv_since_last_tick = 0;
                        this.last_block_received_at = now;
                    }
                    this.pending.push_back(LandedTransactionBlockIterator {
                        slot: block.slot,
                        block: block.into_iter(),
                        txn_count: 0,
                    });
                }
                Poll::Ready(Some(Ok(FumaroleBlockStreamEvent::SlotStatus(_)))) => {}
                Poll::Ready(Some(Err(err))) => {
                    tracing::error!(?err, "fumarole subscription error");
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_str = std::fs::read_to_string(&args.config)?;
    let config: Config = serde_yaml::from_str(&config_str)?;

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
    let block_stream = fumarole_stream.block_stream();

    let source = LandedTransactionStream::new(block_stream);

    let drain = HttpTxnTraceDrain::with_config(source, config.drain);
    drain.await?;

    tracing::info!("fumarole stream ended; exiting");
    Ok(())
}
