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
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
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
                let proto_tx = payload.to_proto::<PublishTransaction>()?;
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

    let rpc_sol = RpcClient::new(config.rpc.clone());

    // Verify balance before getting block.
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
    let should_use_legacy_txn = args.legacy;

    let landed = Arc::new(AtomicUsize::new(0));
    let sender = Arc::new(TransactionSender::from_config(config.output.clone()).await?);
    futures::future::try_join_all((0..args.count).map(|index| {
        let count = args.count;
        let config = config.clone();
        let landed = Arc::clone(&landed);
        let sender = Arc::clone(&sender);
        async move {
            let rpc_sol = RpcClient::new(config.rpc);

            let mut instructions = vec![];
            if let Some(limit) = config.compute_budget_unit_limit {
                instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
            }
            if let Some(price) = config.compute_budget_unit_price {
                instructions.push(ComputeBudgetInstruction::set_compute_unit_price(price));
            }
            let lamports = 5_000 + index;
            instructions.push(system_instruction::transfer(
                &wallet_pubkey,
                &wallet_pubkey,
                lamports,
            ));
            let transaction = VersionedTransaction::try_new(
                VersionedMessage::V0(v0::Message::try_compile(
                    &wallet_pubkey,
                    &instructions,
                    &[],
                    latest_blockhash,
                )?),
                &[&config.wallet_secret_key],
            )?;
            let signature = transaction.signatures[0];
            info!("generate transaction {signature} with send lamports {lamports}");

           let config = RpcSendTransactionConfigWithBlockList {
                config: Some(RpcSendTransactionConfig {
                    skip_preflight: true,
                    skip_sanitize: false,
                    preflight_commitment: Some(CommitmentLevel::Finalized),
                    encoding: Some(UiTransactionEncoding::Base64),
                    max_retries: config.max_retries,
                    min_context_slot: None,
                }),
                  blocklist_pdas: Some(config.blocklist_pdas),
            };
            match sender.send(transaction, config, should_use_legacy_txn).await {
                Ok(send_signature) => {
                    anyhow::ensure!(signature == send_signature, "received invalid signature from sender");
                    info!("successfully send transaction {signature}");

                    sleep(Duration::from_millis(3_200)).await;
                    let mut attempt = 0;
                    loop {
                        match rpc_sol
                            .get_signature_status_with_commitment(
                                &signature,
                                CommitmentConfig::confirmed(),
                            )
                            .await
                        {
                            Ok(Some(status)) => {
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
                                error!("transaction {signature} get status network error: {error:?}");
                            }
                        }
                        sleep(Duration::from_millis(3_200)).await;
                    }
                }
                Err(error) => error!("failed to send transaction {signature} {error:?}"),
            }

            Ok(())
        }
    }))
    .map_ok(|_| ())
    .await
}
