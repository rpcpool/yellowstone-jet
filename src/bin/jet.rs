// main.rs
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::future::FutureExt,
    jsonrpsee::http_client::HttpClientBuilder,
    reqwest::{Client, Url},
    solana_client::{
        nonblocking::rpc_client::RpcClient as SolanaRpcClient, rpc_client::RpcClientConfig,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::{Keypair, read_keypair},
    solana_pubkey::Pubkey,
    solana_quic_definitions::QUIC_MAX_TIMEOUT,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        fs,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tokio::{
        runtime::Builder,
        signal::unix::{SignalKind, signal},
        sync::{Mutex, oneshot, watch},
        task::JoinHandle,
    },
    tracing::{info, warn},
    yellowstone_jet::{
        blockhash_queue::BlockhashQueue,
        cluster_tpu_info::ClusterTpuInfo,
        config::{ConfigJet, PrometheusConfig, RpcErrorStrategy, load_config},
        grpc_geyser::{GeyserStreams, GeyserSubscriber},
        identity::{JetIdentitySyncGroup, JetIdentitySyncMember},
        jet_gateway::spawn_jet_gw_listener,
        metrics::{collect_to_text, jet as metrics},
        quic_gateway::{
            IgnorantLeaderPredictor, QuicGatewayConfig, StakeBasedEvictionStrategy,
            TokioQuicGatewaySession, TokioQuicGatewaySpawner, UpcomingLeaderPredictor,
        },
        rpc::{RpcServer, RpcServerType, rpc_admin::RpcClient},
        setup_tracing,
        solana::sanitize_transaction_support_check,
        solana_rpc_utils::{RetryRpcSender, RetryRpcSenderStrategy},
        stake::{self, StakeInfoMap, spawn_cache_stake_info_map},
        task_group::TaskGroup,
        transaction_handler::TransactionHandler,
        transactions::{
            AlwaysAllowTransactionPolicyStore, GrpcRootedTxReceiver, QuicGatewayBidi,
            TransactionFanout, TransactionNoRetryScheduler, TransactionPolicyStore,
            TransactionRetryScheduler, TransactionRetrySchedulerConfig,
        },
        util::WaitShutdown,
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
        None => run_jet(config).await,
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

            if let Some(identity) = identity {
                let identity = fs::canonicalize(&identity)
                    .with_context(|| format!("Unable to access path: {identity:?}"))?;
                client
                    .set_identity(identity.display().to_string(), false)
                    .await?;
            } else {
                let mut stdin = std::io::stdin();
                let identity = read_keypair(&mut stdin)
                    .map_err(|error| anyhow::anyhow!(error.to_string()))
                    .context("Unable to read JSON keypair from stdin")?;
                client
                    .set_identity_from_bytes(Vec::from(identity.to_bytes()), false)
                    .await?;
            }

            let identity = client.get_identity().await?;
            anyhow::ensure!(
                identity != identity_prev,
                format!("Failed to update identity: {identity} (new) != {identity_prev} (old)")
            );
            println!("Successfully update identity to {identity}");
        }
        ArgsCommandAdmin::ResetIdentityKeypair => {
            client.reset_identity().await?;
        }
    }

    Ok(())
}

// fn spawn_lewis_metric_subscriber(
//     config: Option<ConfigMetricsUpstream>,
//     mut rx: broadcast::Receiver<QuicClientMetric>,
// ) -> JoinHandle<()> {
//     let grpc_metrics = GrpcMetricsClient::new(config);
//     tokio::spawn(async move {
//         loop {
//             match rx.recv().await {
//                 Ok(metric) => match metric {
//                     QuicClientMetric::SendAttempts {
//                         sig,
//                         leader,
//                         leader_tpu_addr,
//                         slots,
//                         error,
//                     } => {
//                         grpc_metrics.emit_send_attempt(
//                             &sig,
//                             &leader,
//                             slots.as_slice(),
//                             leader_tpu_addr,
//                             error,
//                         );
//                     }
//                 },
//                 Err(broadcast::error::RecvError::Closed) => {
//                     break;
//                 }
//                 Err(broadcast::error::RecvError::Lagged(_)) => {
//                     warn!("lewis metrics subscriber lagged behind");
//                 }
//             }
//         }
//     })
// }

///
/// This task keeps the stake metrics up to date for the current identity.
///
async fn keep_stake_metrics_up_to_date_task(
    mut stake_info_identity_observer: watch::Receiver<Pubkey>,
    stake_info_map: StakeInfoMap,
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
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            result = stake_info_identity_observer.changed() => {
                result.expect("stake_info_identity_observer changed failed");
            }
        }
    }
}

async fn run_jet(config: ConfigJet) -> anyhow::Result<()> {
    let mut tg = TaskGroup::default();
    metrics::init();

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
        stake::SpawnMode::Detached,
    )
    .await;

    let local = tokio::task::LocalSet::new();

    let shield_policy_store = if config
        .features
        .is_feature_enabled(yellowstone_jet::proto::jet::Feature::YellowstoneShield)
    {
        let policy_store = PolicyStore::build()
            .config(config.upstream.clone().into())
            .run(&local)
            .await?;

        Arc::new(policy_store) as Arc<dyn TransactionPolicyStore + Send + Sync>
    } else {
        Arc::new(AlwaysAllowTransactionPolicyStore)
    };

    let (shutdown_geyser_tx, shutdown_geyser_rx) = oneshot::channel();
    let (geyser, mut geyser_handle) =
        GeyserSubscriber::new(shutdown_geyser_rx, config.upstream.grpc.clone());
    let blockhash_queue = BlockhashQueue::new(&geyser);

    let rpc_client = Arc::new(solana_client::nonblocking::rpc_client::RpcClient::new(
        config.upstream.rpc.clone(),
    ));
    let (cluster_tpu_info, cluster_tpu_info_tasks) = ClusterTpuInfo::new(
        rpc_client,
        geyser.subscribe_slots(),
        config.upstream.cluster_nodes_update_interval,
    )
    .await;

    let rooted_tx_geyser_rx = geyser
        .subscribe_transactions()
        .await
        .expect("failed to subscribe geyser transactions");
    let (rooted_transactions_rx, rooted_tx_loop_fut) =
        GrpcRootedTxReceiver::new(rooted_tx_geyser_rx);

    let initial_identity = config.identity.keypair.unwrap_or(Keypair::new());
    let quic_gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map: stake_info_map.clone(),
        gateway_tx_channel_capacity: 10000,
        leader_tpu_info_service: Arc::new(cluster_tpu_info.clone()),
    };

    let connection_predictor = if config.quic.connection_prediction_lookahead.is_some() {
        Arc::new(cluster_tpu_info.clone()) as Arc<dyn UpcomingLeaderPredictor + Send + Sync>
    } else {
        Arc::new(IgnorantLeaderPredictor)
    };

    let quic_gateway_config = QuicGatewayConfig {
        port_range: config.quic.endpoint_port_range,
        max_idle_timeout: config.quic.max_idle_timeout.min(QUIC_MAX_TIMEOUT),
        connecting_timeout: config.quic.connection_handshake_timeout,
        num_endpoints: config.quic.endpoint_count,
        max_send_attempt: config.quic.send_retry_count,
        leader_prediction_lookahead: config.quic.connection_prediction_lookahead,
        ..Default::default()
    };

    let TokioQuicGatewaySession {
        gateway_identity_updater,
        gateway_tx_sink,
        gateway_response_source,
        gateway_join_handle,
    } = quic_gateway_spawner.spawn(
        initial_identity.insecure_clone(),
        quic_gateway_config,
        Arc::new(StakeBasedEvictionStrategy {
            peer_idle_eviction_grace_period: config.quic.connection_idle_eviction_grace,
        }),
        connection_predictor,
    );

    tg.spawn_cancelable("gateway", async move {
        gateway_join_handle.await.expect("quic gateway join handle");
    });

    let quic_gateway_bidi = QuicGatewayBidi {
        sink: gateway_tx_sink,
        source: gateway_response_source,
    };

    let (scheduler_in, scheduler_out) = if !config.send_transaction_service.relay_only_mode {
        info!(
            "Disabled relay-only mode, transactions retry will be enabled -- this should be used only by unstaked jet instance"
        );
        let TransactionRetryScheduler { sink, source } = TransactionRetryScheduler::new(
            TransactionRetrySchedulerConfig {
                retry_rate: config.send_transaction_service.retry_rate,
                stop_send_on_commitment: config.send_transaction_service.stop_send_on_commitment,
                max_retry: config
                    .send_transaction_service
                    .default_max_retries
                    .unwrap_or(config.send_transaction_service.service_max_retries),
                ..Default::default()
            },
            Arc::new(blockhash_queue.clone()),
            Box::new(rooted_transactions_rx),
            None,
        );
        (sink, source)
    } else {
        tracing::info!("Running in relay-only mode, transactions retry will be disabled");
        let TransactionNoRetryScheduler { sink, source } =
            TransactionNoRetryScheduler::new(Arc::new(blockhash_queue.clone()));
        (sink, source)
    };

    let mut tx_forwader = TransactionFanout::new(
        Arc::new(cluster_tpu_info.clone()),
        shield_policy_store,
        scheduler_out,
        quic_gateway_bidi,
        config.send_transaction_service.leader_forward_count,
    );

    tg.spawn_cancelable(
        "transaction_forwarder",
        async move { tx_forwader.run().await },
    );

    // TODO check if really need lewis
    // let quic_tx_metrics_listener = quic_tx_sender.subscribe_metrics();
    // let lewis = spawn_lewis_metric_subscriber(config.metrics_upstream, quic_tx_metrics_listener);

    let mut jet_identity_sync_members: Vec<Box<dyn JetIdentitySyncMember + Send + Sync + 'static>> =
        vec![Box::new(gateway_identity_updater)];

    let tx_handler_rpc = Arc::new(SolanaRpcClient::new(config.upstream.rpc.clone()));
    let sanitize_supported = sanitize_transaction_support_check(&tx_handler_rpc)
        .await
        .expect("sanitize transaction support check");
    let tx_handler = TransactionHandler {
        transaction_sink: scheduler_in,
        rpc: tx_handler_rpc,
        proxy_sanitize_check: config.listen_solana_like.proxy_sanitize_check && sanitize_supported,
        proxy_preflight_check: config.listen_solana_like.proxy_preflight_check,
    };

    let rpc_solana_like = RpcServer::new(
        config.listen_solana_like.bind[0],
        RpcServerType::SolanaLike {
            tx_handler: tx_handler.clone(),
        },
    )
    .await?;

    let jet_gw_listener = match config.jet_gateway {
        Some(config_jet_gateway) => {
            if config_jet_gateway.endpoints.is_empty() {
                warn!("no endpoints for jet-gateway with existed config");
                None
            } else {
                let jet_gw_config = config_jet_gateway.clone();
                let expected_identity = config.identity.expected;

                info!("starting jet-gateway listener");
                let stake_info = stake_info_map.clone();
                let jet_gw_identity = initial_identity.insecure_clone();
                let tx_sender = RpcServer::create_solana_like_rpc_server_impl(tx_handler.clone());
                let (jet_gw_identity_updater, jet_gw_fut) = spawn_jet_gw_listener(
                    stake_info,
                    jet_gw_config,
                    tx_sender,
                    expected_identity,
                    config.features,
                    jet_gw_identity,
                );
                jet_identity_sync_members.push(Box::new(jet_gw_identity_updater));
                Some(jet_gw_fut.boxed())
            }
        }
        _ => {
            warn!("Skipping jet-gateway listener, no config provided");
            None
        }
    };

    let mut sigint = signal(SignalKind::interrupt())?;

    let jet_identity_group_syncer =
        JetIdentitySyncGroup::new(initial_identity, jet_identity_sync_members);
    let identity_observer = jet_identity_group_syncer.get_identity_watcher();
    let rpc_admin = RpcServer::new(
        config.listen_admin.bind[0],
        RpcServerType::Admin {
            jet_identity_updater: Arc::new(Mutex::new(Box::new(jet_identity_group_syncer))),
            allowed_identity: config.identity.expected,
        },
    )
    .await?;

    tg.spawn_cancelable("stake_cache_refresh_task", stake_info_bg_fut);

    tg.spawn_cancelable(
        "stake_info_metrics_update",
        keep_stake_metrics_up_to_date_task(identity_observer.clone(), stake_info_map.clone()),
    );

    // tg.spawn_cancelable("lewis", async move {
    //     lewis.await.expect("lewis");
    // });

    tg.spawn_with_shutdown("geyser", |mut stop| async move {
        tokio::select! {
            result = &mut geyser_handle => {
                result.expect("geyser handle").expect("geyser result");
            },
            _ = &mut stop => {
                let _ = shutdown_geyser_tx.send(());
                geyser_handle.await.expect("geyser handle").expect("geyser result");
            },
        }
    });

    tg.spawn_with_shutdown("blockhash_queue", |mut stop| async move {
        tokio::select! {
            result = blockhash_queue.clone().wait_shutdown() => {
                result.expect("blockhash_queue");
            },
            _ = &mut stop => {
                blockhash_queue.shutdown();
                blockhash_queue.wait_shutdown().await.expect("blockhash_queue shutdown");
            },
        }
    });

    tg.spawn_cancelable("cluster_tpu_info", async move {
        cluster_tpu_info_tasks.await;
    });

    tg.spawn_cancelable("rooted_transactions", async move {
        rooted_tx_loop_fut.await;
    });

    if let Some(jet_gw_listener_fut) = jet_gw_listener {
        tg.spawn_cancelable("jet_gw_listener", jet_gw_listener_fut);
    }

    if let Some(config_prometheus) = config.prometheus {
        let push_gw_task =
            spawn_push_prometheus_metrics(config_prometheus).await?;
        tg.spawn_cancelable("prometheus_push_gw", async move {
            push_gw_task.await.expect("prometheus_push_gw");
        })
    }

    tg.spawn_cancelable("SIGINT", async move {
        sigint.recv().await;
        info!("SIGINT received...");
    });

    local
        .run_until(async {
            let (first, result, rest) = tg.wait_one().await.expect("task group empty");
            rpc_admin.shutdown();
            rpc_solana_like.shutdown();

            warn!("first task group finished {first} with  {result:?}");

            for (name, result) in rest {
                if let Err(e) = result {
                    tracing::error!("task: {name} shutdown with: {e:?}");
                }
            }

            Ok(())
        })
        .await
}

async fn spawn_push_prometheus_metrics(
    config: PrometheusConfig,
) -> anyhow::Result<JoinHandle<()>> {
    let prometheus_url = Url::parse(&config.url).expect("");
    let mut interval = tokio::time::interval(config.push_interval);
    let client = Client::new();

    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
            _ = interval.tick() => {
                    if let Err(error) = client
                    .post(prometheus_url.clone())
                    .header("Content-Type", "text/plain")
                    .body(collect_to_text())
                    .send()
                    .await {
                        warn!(?error, "Error pushing metrics");
                    }
                }
            }
        }
    }))
}
