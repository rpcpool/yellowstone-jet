use {
    anyhow::Context,
    clap::Parser,
    futures::{channel::mpsc, future::TryFutureExt, sink::SinkExt},
    serde::{de, Deserialize, Deserializer},
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcBlockConfig, RpcSendTransactionConfig},
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        message::{v0, VersionedMessage},
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::{read_keypair_file, Signature},
        signer::{keypair::Keypair, Signer},
        system_instruction,
        transaction::VersionedTransaction,
    },
    solana_transaction_status::{TransactionDetails, UiTransactionEncoding},
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
        str::FromStr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration as StdDuration, Instant},
    },
    tokio::{
        fs,
        sync::Mutex,
        time::{sleep, Duration},
    },
    tonic::{
        transport::{channel::ClientTlsConfig, Endpoint},
        Response, Streaming,
    },
    tracing::{error, info},
    yellowstone_jet::{
        payload::{RpcSendTransactionConfigWithBlockList, TransactionPayload},
        proto::jet::{
            jet_gateway_client::JetGatewayClient, publish_request::Message as PublishMessage,
            PublishRequest, PublishResponse, PublishTransaction,
        },
        setup_tracing,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Path to config
    #[clap(long)]
    pub config: PathBuf,

    /// Number of transactions to generate
    #[clap(long)]
    pub count: u64,

    /// Check the balance of the wallet and airdrop Solana (devnet and testnet only)
    #[clap(long)]
    pub airdrop: bool, // default is false

    /// Use legacy payload format
    #[clap(long)]
    pub legacy: bool, // default is the new tx payload

    /// Enable benchmarking mode
    #[clap(long)]
    pub benchmark: bool, // default is false

    /// Number of benchmark rounds
    #[clap(long, default_value = "1")]
    pub rounds: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Secret key for transactions
    #[serde(deserialize_with = "Config::deserialize_wallet_secret_key")]
    pub wallet_secret_key: Arc<Keypair>,

    /// Rpc endpoint to fetch latest blockhash
    #[serde(default = "Config::default_rpc")]
    pub rpc: String,

    /// Transactions output, Jet-like or Kafka
    #[serde(default)]
    pub output: ConfigOutput,

    /// Fetch recent blockhash for selected commitment level
    #[serde(default)]
    pub recent_blockhash_commitment: CommitmentLevel,

    /// Set a specific compute unit limit that the transaction is allowed to consume
    #[serde(default)]
    pub compute_budget_unit_limit: Option<u32>,

    /// Set a compute unit price in “micro-lamports” to pay a higher transaction fee for higher transaction prioritization
    #[serde(default)]
    pub compute_budget_unit_price: Option<u64>,

    /// Number of send retries in STS
    #[serde(default)]
    pub max_retries: Option<usize>,

    /// List of program derived addresses to blocklistS
    #[serde(default)]
    pub blocklist_pdas: Vec<String>,
}

impl Config {
    pub async fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = fs::read(path)
            .await
            .with_context(|| format!("failed to read config from {:?}", path))?;
        Ok(serde_yaml::from_slice(&contents)?)
    }

    fn default_rpc() -> String {
        "http://127.0.0.1:8899/".to_owned()
    }

    fn deserialize_wallet_secret_key<'de, D>(deserializer: D) -> Result<Arc<Keypair>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let keypair_file = PathBuf::deserialize(deserializer)?;
        let keypair = read_keypair_file(keypair_file).map_err(de::Error::custom)?;
        Ok(Arc::new(keypair))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ConfigOutput {
    /// Jet-like endpoint for sending generated transactions
    Jet { jet: String },
    /// jet-gateway details
    JetGateway { gateway: String },
}

impl Default for ConfigOutput {
    fn default() -> Self {
        Self::Jet {
            jet: "http://127.0.0.1:8000/".to_owned(),
        }
    }
}

enum TransactionSender {
    Jet {
        rpc: RpcClient,
    },
    JetGateway {
        tx: Mutex<mpsc::Sender<PublishRequest>>,
    },
}

impl TransactionSender {
    async fn from_config(config: ConfigOutput) -> anyhow::Result<Self> {
        Ok(match config {
            ConfigOutput::Jet { jet } => Self::Jet {
                rpc: RpcClient::new(jet),
            },
            ConfigOutput::JetGateway { gateway } => {
                let channel = Endpoint::from_shared(gateway)?
                    .connect_timeout(Duration::from_secs(3))
                    .timeout(Duration::from_secs(1))
                    .tls_config(ClientTlsConfig::new().with_native_roots())?
                    .connect()
                    .await
                    .context("failed to connect")?;
                let mut client = JetGatewayClient::new(channel);

                let (tx, rx) = mpsc::channel(1);
                let mut response: Response<Streaming<PublishResponse>> = client.publish(rx).await?;
                tokio::spawn(async move {
                    while let Ok(Some(message)) = response.get_mut().message().await {
                        info!(?message, "new message from gateway");
                    }
                    error!("gateway streaming finished");
                });

                Self::JetGateway { tx: Mutex::new(tx) }
            }
        })
    }
}

impl TransactionSender {
    async fn send(
        &self,
        transaction: VersionedTransaction,
        config: RpcSendTransactionConfigWithBlockList,
        should_use_legacy_txn: bool,
    ) -> anyhow::Result<Signature> {
        match self {
            Self::Jet { rpc } => rpc
                .send_transaction_with_config(&transaction, config.config.unwrap_or_default())
                .await
                .map_err(Into::into),
            Self::JetGateway { tx } => {
                let signature = transaction.signatures[0];
                let payload =
                    TransactionPayload::create(&transaction, config, should_use_legacy_txn)?;
                let proto_tx = payload.to_proto::<PublishTransaction>();
                tx.lock()
                    .await
                    .send(PublishRequest {
                        message: Some(PublishMessage::Transaction(proto_tx)),
                    })
                    .await?;
                Ok(signature)
            }
        }
    }
}
async fn verify_balance(pubkey: &Pubkey, rpc: &RpcClient) -> anyhow::Result<()> {
    let balance = rpc
        .get_balance(pubkey)
        .await
        .context("Failed to get account balance")?;

    info!(
        "Account balance: {} SOL",
        balance as f64 / LAMPORTS_PER_SOL as f64
    );

    if balance == 0 {
        info!("Requesting airdrop of 1 SOL for {}", pubkey);
        rpc.request_airdrop(pubkey, LAMPORTS_PER_SOL)
            .await
            .context("Failed to request airdrop")?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing(false)?;
    let args = Args::parse();

    let config = Config::load(&args.config)
        .await
        .with_context(|| format!("failed to load config from {:?}", args.config))?;
    let wallet_pubkey = config.wallet_secret_key.pubkey();
    info!("load key {wallet_pubkey}");

    let metrics = Arc::new(Mutex::new(BenchmarkMetrics::default()));
    let sender = Arc::new(TransactionSender::from_config(config.output.clone()).await?);

    for round in 0..args.rounds {
        info!("Starting benchmark round {} of {}", round + 1, args.rounds);

        let rpc_sol = RpcClient::new(config.rpc.clone());

        if args.airdrop {
            let _ = verify_balance(&wallet_pubkey, &rpc_sol).await;
        }

        let commitment = CommitmentConfig {
            commitment: config.recent_blockhash_commitment,
        };
        let latest_slot = rpc_sol
            .get_slot_with_commitment(commitment)
            .await
            .context("failed to fetch latest slot")?;
        let latest_block = rpc_sol
            .get_block_with_config(
                latest_slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(TransactionDetails::None),
                    rewards: Some(false),
                    commitment: Some(commitment),
                    max_supported_transaction_version: Some(u8::MAX),
                },
            )
            .await
            .context("failed to fetch latest block")?;
        let latest_blockhash = latest_block
            .blockhash
            .parse()
            .context("failed to parse Hash")?;
        info!(
            "latest blockhash {latest_blockhash} at slot {} height {}",
            latest_slot,
            latest_block.block_height.unwrap_or(0)
        );

        let landed = Arc::new(AtomicUsize::new(0));

        futures::future::try_join_all((0..args.count).map(|index| {
            let count = args.count;
            let config = config.clone();
            let landed = Arc::clone(&landed);
            let sender = Arc::clone(&sender);
            let metrics = Arc::clone(&metrics);
            let should_use_legacy_txn = args.legacy;
            let benchmark = args.benchmark;

            async move {
                let rpc_sol = RpcClient::new(config.rpc);

                let start_create = Instant::now();
                let transaction = create_transaction(
                    &config.wallet_secret_key,
                    index,
                    latest_blockhash,
                    Some((config.compute_budget_unit_limit, config.compute_budget_unit_price)),
                ).await?;
                if benchmark {
                    metrics.lock().await.creation_times.push(start_create.elapsed());
                }

                let signature = transaction.signatures[0];
                info!("generate transaction {signature} with send lamports {}", 5_000 + index);

                let config = RpcSendTransactionConfigWithBlockList {
                    config: Some(RpcSendTransactionConfig {
                        skip_preflight: true,
                        skip_sanitize: false,
                        preflight_commitment: Some(CommitmentLevel::Finalized),
                        encoding: Some(UiTransactionEncoding::Base64),
                        max_retries: config.max_retries,
                        min_context_slot: None,
                    }),
                    blocklist_pdas: config.blocklist_pdas
                        .iter()
                        .filter_map(|addr| Pubkey::from_str(addr).ok())
                        .collect(),
                };

                let start_send = Instant::now();
                match sender.send(transaction, config, should_use_legacy_txn).await {
                    Ok(send_signature) => {
                        if benchmark {
                            let mut metrics = metrics.lock().await;
                            metrics.send_times.push(start_send.elapsed());
                            metrics.total_transactions += 1;
                        }
                        anyhow::ensure!(signature == send_signature, "received invalid signature from sender");
                        info!("successfully send transaction {signature}");

                        sleep(Duration::from_millis(3_200)).await;
                        let mut attempt = 0;
                        let start_land = Instant::now();
                        loop {
                            match rpc_sol
                                .get_signature_status_with_commitment(
                                    &signature,
                                    CommitmentConfig::confirmed(),
                                )
                                .await
                            {
                                Ok(Some(status)) => {
                                    if benchmark {
                                        let mut metrics = metrics.lock().await;
                                        metrics.land_times.push(start_land.elapsed());
                                        metrics.retry_counts.push(attempt);
                                        metrics.successful_transactions += 1;
                                    }
                                    info!(
                                        "transaction {signature} landed with status {status:?} {} / {count}",
                                        landed.fetch_add(1, Ordering::Relaxed) + 1
                                    );
                                    break;
                                }
                                Ok(None) => {
                                    info!("transaction {signature} still not landed, attempt {attempt}");
                                    attempt += 1;
                                },
                                Err(error) => {
                                    if benchmark {
                                        let mut metrics = metrics.lock().await;
                                        metrics.errors.entry(error.to_string())
                                            .and_modify(|e| *e += 1)
                                            .or_insert(1);
                                    }
                                    error!("transaction {signature} get status network error: {error:?}");
                                }
                            }
                            sleep(Duration::from_millis(3_200)).await;
                        }
                    }
                    Err(error) => {
                        if benchmark {
                            let mut metrics = metrics.lock().await;
                            metrics.failed_transactions += 1;
                            metrics.errors.entry(error.to_string())
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }
                        error!("failed to send transaction {signature} {error:?}");
                    }
                }

                Ok(())
            }
        }))
        .map_ok(|_| ())
        .await?;

        if round < args.rounds - 1 {
            info!("Waiting 5 seconds before next round...");
            sleep(Duration::from_secs(5)).await;
        }
    }

    if args.benchmark {
        let metrics = metrics.lock().await;
        info!(
            "Benchmark Results for {} rounds using {} payload format:",
            args.rounds,
            if args.legacy { "legacy" } else { "new" }
        );
        metrics.print_statistics();
    }

    Ok(())
}

#[derive(Debug, Default)]
struct BenchmarkMetrics {
    creation_times: Vec<StdDuration>,
    send_times: Vec<StdDuration>,
    land_times: Vec<StdDuration>,
    total_transactions: usize,
    successful_transactions: usize,
    failed_transactions: usize,
    retry_counts: Vec<usize>,
    errors: HashMap<String, usize>,
}

impl BenchmarkMetrics {
    fn print_statistics(&self) {
        info!("Transaction Statistics:");
        info!("Total Transactions: {}", self.total_transactions);
        info!("Successful: {}", self.successful_transactions);
        info!("Failed: {}", self.failed_transactions);
        info!(
            "Success Rate: {:.2}%",
            (self.successful_transactions as f64 / self.total_transactions as f64) * 100.0
        );

        if !self.retry_counts.is_empty() {
            let avg_retries =
                self.retry_counts.iter().sum::<usize>() as f64 / self.retry_counts.len() as f64;
            let max_retries = self.retry_counts.iter().max().unwrap_or(&0);
            info!("Average Retries per Transaction: {:.2}", avg_retries);
            info!("Maximum Retries: {}", max_retries);
        }

        for (name, times) in [
            ("Creation", &self.creation_times),
            ("Sending", &self.send_times),
            ("Landing", &self.land_times),
        ] {
            if times.is_empty() {
                continue;
            }

            let mut sorted_times = times.clone();
            sorted_times.sort();

            let total = times.iter().sum::<StdDuration>();
            let avg = total / times.len() as u32;
            let median = sorted_times[times.len() / 2];
            let p95 = sorted_times[(times.len() as f64 * 0.95) as usize];
            let p99 = sorted_times[(times.len() as f64 * 0.99) as usize];
            let max = sorted_times.last().unwrap_or(&StdDuration::ZERO);
            let min = sorted_times.first().unwrap_or(&StdDuration::ZERO);

            info!("{name} Time Statistics:");
            info!("  Total: {:?}", total);
            info!("  Average: {:?}", avg);
            info!("  Median: {:?}", median);
            info!("  95th percentile: {:?}", p95);
            info!("  99th percentile: {:?}", p99);
            info!("  Max: {:?}", max);
            info!("  Min: {:?}", min);
            info!(
                "  Throughput: {:.2} tx/s",
                times.len() as f64 / total.as_secs_f64()
            );
        }

        if !self.errors.is_empty() {
            info!("Error Distribution:");
            for (error, count) in &self.errors {
                info!("  {}: {} occurrences", error, count);
            }
        }
    }
}

async fn create_transaction(
    wallet_keypair: &Keypair,
    index: u64,
    latest_blockhash: Hash,
    compute_budget: Option<(Option<u32>, Option<u64>)>,
) -> anyhow::Result<VersionedTransaction> {
    let mut instructions = vec![];
    if let Some((limit, price)) = compute_budget {
        if let Some(limit) = limit {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
        }
        if let Some(price) = price {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(price));
        }
    }
    let lamports = 5_000 + index;
    instructions.push(system_instruction::transfer(
        &wallet_keypair.pubkey(),
        &wallet_keypair.pubkey(),
        lamports,
    ));

    Ok(VersionedTransaction::try_new(
        VersionedMessage::V0(v0::Message::try_compile(
            &wallet_keypair.pubkey(),
            &instructions,
            &[],
            latest_blockhash,
        )?),
        &[wallet_keypair],
    )?)
}
