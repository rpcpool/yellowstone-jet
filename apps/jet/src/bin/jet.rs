#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::FutureExt,
    jsonrpsee::http_client::HttpClientBuilder,
    reqwest::{Client, Url},
    solana_client::rpc_client::RpcClientConfig,
    solana_commitment_config::CommitmentConfig,
    solana_keypair::{Keypair, read_keypair},
    solana_pubkey::Pubkey,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        collections::HashMap,
        fs::{self, OpenOptions},
        net::SocketAddr,
        os::unix::fs::OpenOptionsExt,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tokio::{
        runtime::Builder,
        signal::unix::{SignalKind, signal},
        sync::{Mutex, mpsc, watch},
        task::{self, JoinHandle, JoinSet},
        time::Instant,
    },
    tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream},
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
    yellowstone_jet::{
        blockhash_queue::BlockhashQueue,
        cluster_tpu_info::ClusterTpuInfo,
        config::{ConfigJet, PrometheusConfig, RpcErrorStrategy, load_config},
        grpc_geyser::{GeyserStreams, GeyserSubscriber},
        grpc_lewis::create_lewis_pipeline,
        identity::{JetIdentitySyncGroup, JetIdentitySyncMember},
        metrics::{REGISTRY, collect_to_text, jet as metrics},
        rpc::{RpcServer, RpcServerType, rpc_admin::RpcClient},
        setup_tracing,
        solana_rpc_utils::{RetryRpcSender, RetryRpcSenderStrategy},
        stake::{self, StakeInfoMap, spawn_cache_stake_info_map},
        transaction_handler::TransactionHandler,
        transactions::{
            AlwaysAllowTransactionPolicyStore, DropExpiredTransactions, FanoutConfig,
            SendTransactionRequest, TransactionFanout, TransactionPolicyStore,
        },
        txn_trace_drain::HttpTxnTraceDrain,
        util::{WaitShutdown, prom::inject_job_label},
    },
    yellowstone_jet_tpu_client::{
        core::{
            IgnorantLeaderPredictor, LeaderTpuInfoService, OverrideTpuInfoService,
            StakeBasedEvictionStrategy, UpcomingLeaderPredictor,
        },
        sender::{PollTpuSender, create_base_tpu_client},
    },
    yellowstone_shield_store::PolicyStore,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, Parser)]
#[clap(author, version, about)]
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

    #[command(subcommand)]
    pub command: Option<ArgsCommands>,
}

#[derive(Debug, Subcommand)]
enum ArgsCommands {
    /// Jet admin RPC interface
    Admin {
        #[command(subcommand)]
        cmd: ArgsCommandAdmin,
    },
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
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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

    match args.command {
        Some(ArgsCommands::Admin { cmd }) => run_cmd_admin(config, cmd).await,
        None => run_jet(config, args.prometheus).await,
    }
}

async fn run_cmd_admin(config: ConfigJet, admin_cmd: ArgsCommandAdmin) -> anyhow::Result<()> {
    let addr = format!("http://{}", config.listen_admin.bind[0]);
    let client = HttpClientBuilder::default().build(addr)?;

    match admin_cmd {
        ArgsCommandAdmin::GetIdentity => {
            let identity = client.get_identity().await?;
            println!("{identity}");
        }
        ArgsCommandAdmin::SetIdentity { identity } => {
            let identity_prev = client.get_identity().await?;

            let mut reader: Box<dyn std::io::Read> = if let Some(identity_path) = identity {
                // Canonicalize the path to avoid symlink attacks
                let canonical_path = fs::canonicalize(&identity_path)
                    .with_context(|| format!("Unable to canonicalize file: {identity_path:?}"))?;

                // Open with O_NOFOLLOW on Unix to prevent TOCTOU symlink attacks
                #[cfg(unix)]
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&canonical_path)
                    .with_context(|| format!("Unable to open file: {canonical_path:?}"))?;

                #[cfg(not(unix))]
                let file = fs::File::open(&canonical_path)
                    .with_context(|| format!("Unable to open file: {canonical_path:?}"))?;

                Box::new(file)
            } else {
                Box::new(std::io::stdin())
            };

            let keypair = read_keypair(&mut reader)
                .map_err(|error| anyhow::anyhow!(error.to_string()))
                .context("Unable to read JSON keypair")?;

            client
                .set_identity_from_bytes(Vec::from(keypair.to_bytes()), false)
                .await?;

            let identity = client.get_identity().await?;
            anyhow::ensure!(
                identity != identity_prev,
                format!("Failed to update identity: {identity} (new) != {identity_prev} (old)")
            );
            println!("Successfully updated identity to {identity}");
        }
        ArgsCommandAdmin::ResetIdentityKeypair => {
            client.reset_identity().await?;
        }
    }

    Ok(())
}

///
/// This task keeps the stake metrics up to date for the current identity.
///
async fn keep_stake_metrics_up_to_date_task(
    mut stake_info_identity_observer: watch::Receiver<Pubkey>,
    stake_info_map: StakeInfoMap,
    cancellation_token: CancellationToken,
) {
    loop {
        let current_identy = *stake_info_identity_observer.borrow_and_update();

        let (stake, total_stake) = stake_info_map
            .get_stake_info_with_total_stake(current_identy)
            .unwrap_or((0, 0));

        let max_pps = stake::stake_to_per100ms_limit(stake, total_stake);
        let max_streams = stake::stake_to_max_stream(stake, total_stake);

        metrics::cluster_identity_stake_set(metrics::ClusterIdentityStakeKind::Jet, stake);
        metrics::cluster_identity_stake_set(metrics::ClusterIdentityStakeKind::Total, total_stake);
        metrics::cluster_identity_stake_set(
            metrics::ClusterIdentityStakeKind::MaxPermitPer100ms,
            max_pps,
        );
        metrics::cluster_identity_stake_set(
            metrics::ClusterIdentityStakeKind::MaxStreams,
            max_streams,
        );

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            result = stake_info_identity_observer.changed() => {
                result.expect("stake_info_identity_observer changed failed");
            }
        }
    }
}

async fn run_jet(
    config: ConfigJet,
    prometheus_bind_addr: Option<SocketAddr>,
) -> anyhow::Result<()> {
    let mut tg = JoinSet::default();
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

    let shield_policy_store = if config.enable_yellowstone_shield {
        let policy_store_config = config.upstream.clone().into();
        let policy_store = PolicyStore::build()
            .config(policy_store_config)
            .run()
            .await?;

        Arc::new(policy_store) as Arc<dyn TransactionPolicyStore + Send + Sync>
    } else {
        Arc::new(AlwaysAllowTransactionPolicyStore)
    };

    let (geyser, geyser_handle) = GeyserSubscriber::new(
        config.upstream.grpc.clone(),
        false,
        jet_cancellation_token.child_token(),
    );
    let blockhash_queue = BlockhashQueue::new(geyser.subscribe_block_meta());

    let rpc_client = Arc::new(solana_client::nonblocking::rpc_client::RpcClient::new(
        config.upstream.rpc.as_str().to_string(),
    ));

    let (cluster_tpu_info, cluster_tpu_info_tasks) = ClusterTpuInfo::new(
        rpc_client,
        geyser.subscribe_slots(),
        config.upstream.cluster_nodes_update_interval,
        jet_cancellation_token.child_token(),
    )
    .await;

    let initial_identity = config.identity.keypair.unwrap_or(Keypair::new());

    let leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static> =
        Arc::new(OverrideTpuInfoService {
            override_vec: config.quic.tpu_sender.tpu_info_override.clone(),
            other: cluster_tpu_info.clone(),
        });

    let connection_predictor = if config.quic.tpu_sender.leader_prediction_lookahead.is_some() {
        Arc::new(cluster_tpu_info.clone()) as Arc<dyn UpcomingLeaderPredictor + Send + Sync>
    } else {
        Arc::new(IgnorantLeaderPredictor)
    };

    // Set up Lewis event tracking pipeline
    let maybe_callback_sink = match (config.http_txn_trace_drain, config.lewis_events) {
        (None, None) => None,
        (None, Some(lewis_config)) => {
            let (tpu_client_callback_tx, tpu_client_callback_rx) =
                tokio::sync::mpsc::unbounded_channel();
            let tpu_client_callback_rx = UnboundedReceiverStream::new(tpu_client_callback_rx);
            let lewis_fut = create_lewis_pipeline(lewis_config, tpu_client_callback_rx);
            let ah = tg.spawn(
                lewis_fut
                    .inspect(|result| {
                        if let Err(e) = result {
                            error!("Lewis client error: {e}");
                        }
                    })
                    .map(drop),
            );
            tg_name_map.insert(ah.id(), "lewis_client".to_string());
            Some(tpu_client_callback_tx)
        }
        (Some(http_txn_drain_config), None) => {
            let (tpu_client_callback_tx, tpu_client_callback_rx) =
                tokio::sync::mpsc::unbounded_channel();
            let drain = HttpTxnTraceDrain::with_config(
                UnboundedReceiverStream::new(tpu_client_callback_rx),
                cluster_tpu_info.clone(),
                http_txn_drain_config,
            );
            let ah = tg.spawn(async move {
                let _ = drain.await.inspect_err(|e| {
                    error!("HTTP txn trace drain error: {e}");
                });
            });
            tg_name_map.insert(ah.id(), "http_txn_trace_drain".to_string());
            Some(tpu_client_callback_tx)
        }
        (Some(_), Some(_)) => {
            panic!("http_txn_trace_drain and lewis_events cannot be used together")
        }
    };

    let tpu_sender = create_base_tpu_client(
        config.quic.tpu_sender.clone(),
        initial_identity.insecure_clone(),
        leader_tpu_info_service,
        Arc::new(stake_info_map.clone()),
        Arc::new(StakeBasedEvictionStrategy {
            peer_idle_eviction_grace_period: config.quic.connection_idle_eviction_grace,
        }),
        connection_predictor,
        maybe_callback_sink,
        1000, // This capacity should not be too deep, so transaction does not sits too long in the queue.
    )
    .await;
    let identity_updater = tpu_sender.get_owned_identity_updater();
    let tpu_sender = PollTpuSender::new(tpu_sender);

    // Root means the first stage of the transaction pipeline.
    // the root channel can have deeper queue, because the transaction will be processed by the fanout stage, and then sent to the tpu stage.
    let (root_txn_inlet, root_txn_outlet) = mpsc::channel::<SendTransactionRequest>(10_000);

    let root_txn_outlet = ReceiverStream::new(root_txn_outlet);
    let root_txn_outlet = DropExpiredTransactions::new(root_txn_outlet, blockhash_queue.clone());

    #[allow(deprecated)]
    let mut tx_forwader = TransactionFanout::new(
        Arc::new(cluster_tpu_info.clone()),
        shield_policy_store,
        root_txn_outlet,
        tpu_sender,
        config
            .send_transaction_service
            .leader_forward_count
            .map_or(FanoutConfig::SmartFanout, FanoutConfig::Custom),
        config.send_transaction_service.extra_fanout,
    );

    let ah = tg.spawn(async move { tx_forwader.run().await });
    tg_name_map.insert(ah.id(), "transaction_fanout".to_string());

    let jet_identity_sync_members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync + 'static>> =
        vec![Box::new(identity_updater)];

    let tx_handler =
        TransactionHandler::new(root_txn_inlet, config.listen_solana_like.fail_on_preflight);

    let rpc_solana_like = RpcServer::new(
        config.listen_solana_like.bind[0],
        RpcServerType::SolanaLike {
            tx_handler: tx_handler.clone(),
            log_invalid_txn: config.log_invalid_txn,
        },
    )
    .await;

    let mut sigint = signal(SignalKind::interrupt())?;

    let jet_identity_group_syncer =
        JetIdentitySyncGroup::new(initial_identity, jet_identity_sync_members);
    let identity_observer = jet_identity_group_syncer.get_identity_watcher();
    let rpc_admin = RpcServer::new(
        config.listen_admin.bind[0],
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group_syncer))),
            allowed_identity: config.identity.expected,
            cluster_tpu_info: Arc::new(cluster_tpu_info),
        },
    )
    .await;

    let ah = tg.spawn(stake_info_bg_fut);
    tg_name_map.insert(ah.id(), "stake_refresh_task".to_string());

    let ah = tg.spawn(keep_stake_metrics_up_to_date_task(
        identity_observer.clone(),
        stake_info_map.clone(),
        jet_cancellation_token.child_token(),
    ));
    tg_name_map.insert(ah.id(), "stake_info_metrics_update".to_string());

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

    let ah = tg.spawn(async move {
        cluster_tpu_info_tasks.await;
    });
    tg_name_map.insert(ah.id(), "cluster_tpu_info".to_string());

    if let Some(config_prometheus) = config.prometheus {
        let push_gw_task = spawn_push_prometheus_metrics(
            identity_observer.clone(),
            config_prometheus,
            jet_cancellation_token.child_token(),
        )
        .await;
        let ah = tg.spawn(async move {
            push_gw_task.await.expect("prometheus_push_gw");
        });
        tg_name_map.insert(ah.id(), "prometheus_push_gw".to_string());
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
    rpc_admin.shutdown();
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
    if !tg.is_empty() {
        for name in tg_name_map.values() {
            warn!("task -- {name} : did not finish in time, aborting");
        }
    }
    tg.abort_all();
    Ok(())
}

async fn spawn_push_prometheus_metrics(
    mut jet_identity: watch::Receiver<Pubkey>,
    config: PrometheusConfig,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    let prometheus_url = Url::parse(&config.url).expect("");
    let mut interval = tokio::time::interval(config.push_interval);
    let client = Client::new();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let current_identity = *jet_identity.borrow_and_update();
                    let labels_to_inject = [
                        ("job", "jet"),
                        ("instance", &current_identity.to_string() as &str),
                    ];
                    if let Err(error) = client
                        .post(prometheus_url.clone())
                        .header("Content-Type", "text/plain")
                        .body(inject_job_label(&collect_to_text(), labels_to_inject))
                        .send()
                        .await {
                            warn!(?error, "Error pushing metrics");
                        }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    })
}
