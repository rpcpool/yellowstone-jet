#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use {
    clap::{Parser, Subcommand},
    futures::future::FutureExt,
    solana_client::{
        nonblocking::rpc_client::RpcClient as SolanaRpcClient, rpc_client::RpcClientConfig,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::Keypair,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        collections::HashMap,
        net::SocketAddr,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tokio::{
        runtime::Builder,
        signal::unix::{SignalKind, signal},
        sync::mpsc::{self, UnboundedReceiver},
        task::{self, JoinSet},
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
    yellowstone_jet::{
        blockhash_queue::BlockhashQueue,
        config::{ConfigJet, RpcErrorStrategy, load_config},
        grpc_geyser::{GeyserStreams, GeyserSubscriber},
        identity::JetIdentitySyncMember,
        jet_gateway::spawn_jet_gw_listener,
        metrics::{REGISTRY, jet as metrics},
        rpc::{RpcServer, RpcServerType},
        setup_tracing,
        solana_rpc_utils::{RetryRpcSender, RetryRpcSenderStrategy},
        stake::spawn_cache_stake_info_map,
        transaction_handler::TransactionHandler,
        transactions::SendTransactionRequest,
        util::WaitShutdown,
    },
    yellowstone_jet_tpu_client::{
        core::{TpuSenderResponse, TpuSenderResponseCallback},
        yellowstone_grpc::sender::{
            Endpoints, NewYellowstoneTpuSender, ShieldBlockList, YellowstoneTpuSender,
            create_yellowstone_tpu_sender_with_callback,
        },
    },
    yellowstone_shield_store::PolicyStore,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Light-jet version that connects to jet-gateway"
)]
struct Args {
    /// Path to config
    #[clap(long)]
    pub config: PathBuf,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,

    /// Prometheus bind address for scraping metrics
    #[clap(long, help = "prometheus bind address for scraping metrics")]
    pub prometheus: Option<SocketAddr>,
}

#[derive(Debug, Subcommand)]
enum ArgsCommandAdmin {
    /// Print current identity
    GetIdentity,
    /// Set new identity from file
    SetIdentity {
        /// Path to file with Keypair
        #[clap(long)]
        identity: Option<PathBuf>,
    },
    /// Reset identity
    ResetIdentityKeypair,
}

fn main() -> anyhow::Result<()> {
    Builder::new_multi_thread()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
            format!("jet.tokio{id:02}")
        })
        .enable_all()
        .build()?
        .block_on(main2())
}

async fn main2() -> anyhow::Result<()> {
    let args = Args::parse();
    let config: ConfigJet = load_config(&args.config).await?;
    if args.check {
        return Ok(());
    }

    setup_tracing(config.tracing.json)?;
    run_jet(config, args.prometheus).await
}

async fn run_jet(
    config: ConfigJet,
    prometheus_bind_addr: Option<SocketAddr>,
) -> anyhow::Result<()> {
    let mut tg: JoinSet<()> = JoinSet::default();
    let mut tg_name_map = HashMap::<task::Id, String>::new();
    metrics::init();
    let jet_cancellation_token = CancellationToken::new();
    if let Some(identity) = config.identity.expected {
        metrics::quic_set_identity_expected(identity);
    }

    let retry_strategy = match config.upstream.rpc_on_error.clone() {
        RpcErrorStrategy::Fixed { interval, retries } => Some(RetryRpcSenderStrategy::FixedDelay {
            delay: interval,
            max_retries: retries.get(),
        }),
        RpcErrorStrategy::Exponential {
            base,
            factor,
            retries,
        } => Some(RetryRpcSenderStrategy::ExponentialBackoff {
            base,
            exp: factor,
            max_retries: retries.get(),
        }),
        RpcErrorStrategy::Fail => None,
    };

    let rpc_sender = HttpSender::new(config.upstream.rpc.clone());
    let rpc_client_config = RpcClientConfig::with_commitment(CommitmentConfig::finalized());
    let rpc_client = match retry_strategy {
        Some(strategy) => {
            let rpc_sender = RetryRpcSender::new(rpc_sender, strategy);
            solana_client::nonblocking::rpc_client::RpcClient::new_sender(
                rpc_sender,
                rpc_client_config,
            )
        }
        None => solana_client::nonblocking::rpc_client::RpcClient::new_sender(
            rpc_sender,
            rpc_client_config,
        ),
    };

    let (stake_info_map, stake_info_bg_fut) = spawn_cache_stake_info_map(
        rpc_client,
        config.upstream.stake_update_interval,
        None,
        jet_cancellation_token.child_token(),
    )
    .await;

    let shield_policy_store = if config
        .features
        .is_feature_enabled(yellowstone_jet::proto::jet::Feature::YellowstoneShield)
    {
        let policy_store_config = config.upstream.clone().into();
        let policy_store = PolicyStore::build()
            .config(policy_store_config)
            .run()
            .await?;

        Some(policy_store)
    } else {
        None
    };

    let (geyser, geyser_handle) = GeyserSubscriber::new(
        config.upstream.grpc.clone(),
        !config.send_transaction_service.relay_only_mode,
        jet_cancellation_token.child_token(),
    );
    let blockhash_queue = BlockhashQueue::new(geyser.subscribe_block_meta());

    let initial_identity = config.identity.keypair.unwrap_or(Keypair::new());

    let (scheduler_in, scheduler_out) = mpsc::unbounded_channel::<Arc<SendTransactionRequest>>();
    let tpu_sender_endpoints = Endpoints {
        rpc: config.upstream.rpc.clone(),
        grpc: config.upstream.grpc.endpoint.clone(),
        grpc_x_token: config.upstream.grpc.x_token.clone(),
    };
    #[derive(Clone)]
    struct LoggingCallback;

    impl TpuSenderResponseCallback for LoggingCallback {
        fn call(&self, response: TpuSenderResponse) {
            use std::io::Write;
            let mut stdout = std::io::stdout();
            match response {
                TpuSenderResponse::TxSent(info) => {
                    writeln!(
                        &mut stdout,
                        "Transaction {} send to {}",
                        info.tx_sig, info.remote_peer_identity
                    )
                    .expect("writeln");
                }
                TpuSenderResponse::TxFailed(info) => {
                    writeln!(&mut stdout, "Transaction failed: {}", info.tx_sig).expect("writeln");
                }
                TpuSenderResponse::TxDrop(info) => {
                    for (txn, _) in info.dropped_tx_vec {
                        writeln!(&mut stdout, "Transaction dropped: {}", txn.tx_sig)
                            .expect("writeln");
                    }
                }
            }
        }
    }
    let NewYellowstoneTpuSender {
        sender,
        related_objects_jh,
    } = create_yellowstone_tpu_sender_with_callback(
        Default::default(),
        initial_identity.insecure_clone(),
        tpu_sender_endpoints,
        LoggingCallback,
    )
    .await
    .expect("yellowstone-tpu-sender");

    let ah = tg.spawn(async move {
        related_objects_jh
            .await
            .expect("yellowstone_tpu_sender_related_objects join handle");
    });
    tg_name_map.insert(
        ah.id(),
        "yellowstone_tpu_sender_related_objects".to_string(),
    );

    let tx_handler_rpc = Arc::new(SolanaRpcClient::new(config.upstream.rpc.clone()));
    let tx_handler = TransactionHandler {
        transaction_sink: scheduler_in,
        rpc: tx_handler_rpc,
        proxy_sanitize_check: false,
        proxy_preflight_check: false,
    };

    let ah = tg.spawn(tpu_sender_loop(
        scheduler_out,
        sender,
        shield_policy_store,
        jet_cancellation_token.child_token(),
    ));

    tg_name_map.insert(ah.id(), "tpu_sender_loop".to_string());

    let rpc_solana_like = RpcServer::new(
        config.listen_solana_like.bind[0],
        RpcServerType::SolanaLike {
            tx_handler: tx_handler.clone(),
        },
    )
    .await;
    let mut jet_identity_sync_members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync + 'static>> =
        vec![];

    let jet_gw_listener = match config.jet_gateway {
        Some(config_jet_gateway) => {
            let jet_gw_cancellation_token = jet_cancellation_token.child_token();
            if config_jet_gateway.endpoints.is_empty() {
                warn!("no endpoints for jet-gateway with existed config");
                None
            } else {
                let jet_gw_config = config_jet_gateway.clone();
                let expected_identity = config.identity.expected;

                info!("starting jet-gateway listener");
                let stake_info = stake_info_map.clone();
                let jet_gw_identity = initial_identity.insecure_clone();
                let tx_sender = RpcServer::create_solana_like_rpc_server_impl(tx_handler);
                let (jet_gw_identity_updater, jet_gw_fut) = spawn_jet_gw_listener(
                    stake_info,
                    jet_gw_config,
                    tx_sender,
                    expected_identity,
                    config.features,
                    jet_gw_identity,
                    jet_gw_cancellation_token,
                );
                jet_identity_sync_members.push(Box::new(jet_gw_identity_updater));
                Some(jet_gw_fut.boxed())
            }
        }
        _ => {
            drop(tx_handler);
            warn!("Skipping jet-gateway listener, no config provided");
            None
        }
    };

    let mut sigint = signal(SignalKind::interrupt())?;

    let ah = tg.spawn(stake_info_bg_fut);
    tg_name_map.insert(ah.id(), "stake_refresh_task".to_string());

    let ah = tg.spawn(async move {
        geyser_handle
            .await
            .expect("geyser handle")
            .expect("geyser result");
    });
    tg_name_map.insert(ah.id(), "geyser".to_string());

    let ah = tg.spawn(async move {
        blockhash_queue
            .wait_shutdown()
            .await
            .expect("blockhash queue shutdown");
    });
    tg_name_map.insert(ah.id(), "blockhash_queue".to_string());

    if let Some(jet_gw_listener_fut) = jet_gw_listener {
        let ah = tg.spawn(jet_gw_listener_fut);
        tg_name_map.insert(ah.id(), "jet_gw_listener".to_string());
    }

    if let Some(prometheus_bind_addr) = prometheus_bind_addr {
        let my_ct = jet_cancellation_token.child_token();
        tracing::info!(
            "starting prometheus scrap server at {}",
            prometheus_bind_addr
        );
        let ah = tg.spawn(async move {
            yellowstone_jet::util::prom::serve_prometheus_metric(
                REGISTRY.clone(),
                prometheus_bind_addr,
                my_ct,
            )
            .await
        });
        tg_name_map.insert(ah.id(), "prometheus_scrape_http_server".to_string());
    }

    let ah = tg.spawn(async move {
        sigint.recv().await;
        info!("SIGINT received...");
    });

    tg_name_map.insert(ah.id(), "SIGINT".to_string());

    let Some(result) = tg.join_next_with_id().await else {
        panic!("no task in the task group can ever happen");
    };
    macro_rules! get_id {
        ($joinset_join_result_with_id:expr) => {
            match $joinset_join_result_with_id {
                Ok((id, _)) => *id,
                Err(e) => e.id().clone(),
            }
        };
    }
    jet_cancellation_token.cancel();
    let task_id = get_id!(&result);
    let first = tg_name_map
        .remove(&task_id)
        .unwrap_or_else(|| format!("unknown task {task_id:?}"));
    warn!("shutting down, task {first} finished first with: {result:?}");
    rpc_solana_like.shutdown();

    const SHUTDOWN_DURATION: std::time::Duration = std::time::Duration::from_secs(10);
    let shutdown_deadline = Instant::now() + SHUTDOWN_DURATION;
    loop {
        tokio::select! {
            Some(result) = tg.join_next_with_id() => {
                let task_id = get_id!(&result);
                let remaining_tasks = tg.len();
                let name = tg_name_map
                    .remove(&task_id)
                    .unwrap_or_else(|| format!("unknown task {task_id:?}"));
                if result.is_ok() {
                    info!("task -- {name} : finished cleanly, {remaining_tasks} remaining");
                } else {
                    warn!("task -- {name} : finished with error: {result:?}, {remaining_tasks} remaining");
                }
                if remaining_tasks == 0 {
                    break;
                }
            }
            _ = tokio::time::sleep_until(shutdown_deadline) => {
                warn!("some tasks did not shut down in time, aborting them");
                break;
            }
            else => {
                break;
            }
        }
    }
    drop(jet_cancellation_token);
    if !tg.is_empty() {
        for (_id, name) in tg_name_map.iter() {
            warn!("task -- {name} : did not finish in time, aborting");
        }
    }
    tg.abort_all();
    Ok(())
}

///
/// Example of using jet-tpu-client
pub async fn tpu_sender_loop(
    mut incoming: UnboundedReceiver<Arc<SendTransactionRequest>>,
    mut tpu_sender: YellowstoneTpuSender,
    shield: Option<PolicyStore>,
    cancellation_token: CancellationToken,
) {
    while let Some(request) = incoming.recv().await {
        if cancellation_token.is_cancelled() {
            break;
        }
        let request = Arc::unwrap_or_clone(request);
        let SendTransactionRequest {
            signature,
            transaction: _,
            wire_transaction,
            max_retries: _,
            policies,
        } = request;

        let blocklist = shield.as_ref().map(|shield| ShieldBlockList {
            policy_store: shield,
            shield_policy_addresses: &policies,
            default_return_value: false,
        });

        let result = tpu_sender
            .send_txn_with_blocklist(signature, wire_transaction, blocklist)
            .await;
        if let Err(e) = result {
            tracing::error!(
                "failed to send transaction {signature} via TPU: {:?}",
                e.kind
            );
        }
    }
}
