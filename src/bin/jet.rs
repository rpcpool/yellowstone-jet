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
        collections::HashMap,
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
        sync::{Mutex, watch},
        task::{self, JoinHandle, JoinSet},
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
    yellowstone_jet::{
        blockhash_queue::BlockhashQueue,
        cluster_tpu_info::ClusterTpuInfo,
        config::{ConfigJet, PrometheusConfig, RpcErrorStrategy, load_config},
        grpc_geyser::{GeyserStreams, GeyserSubscriber},
        grpc_lewis::create_lewis_pipeline,
        identity::{JetIdentitySyncGroup, JetIdentitySyncMember},
        jet_gateway::spawn_jet_gw_listener,
        metrics::{collect_to_text, inject_job_label, jet as metrics},
        quic_gateway::{
            IgnorantLeaderPredictor, LeaderTpuInfoService, OverrideTpuInfoService,
            QuicGatewayConfig, StakeBasedEvictionStrategy, TokioQuicGatewaySession,
            TokioQuicGatewaySpawner, UpcomingLeaderPredictor,
        },
        rpc::{RpcServer, RpcServerType, rpc_admin::RpcClient},
        setup_tracing,
        solana::sanitize_transaction_support_check,
        solana_rpc_utils::{RetryRpcSender, RetryRpcSenderStrategy},
        stake::{self, StakeInfoMap, spawn_cache_stake_info_map},
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

async fn run_jet(config: ConfigJet) -> anyhow::Result<()> {
    // let mut tg = TaskGroup::default();
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

    let (geyser, geyser_handle) = GeyserSubscriber::new(
        config.upstream.grpc.clone(),
        !config.send_transaction_service.relay_only_mode,
        jet_cancellation_token.child_token(),
    );
    let blockhash_queue = BlockhashQueue::new(geyser.subscribe_block_meta());

    let rpc_client = Arc::new(solana_client::nonblocking::rpc_client::RpcClient::new(
        config.upstream.rpc.clone(),
    ));

    let (cluster_tpu_info, cluster_tpu_info_tasks) = ClusterTpuInfo::new(
        rpc_client,
        geyser.subscribe_slots(),
        config.upstream.cluster_nodes_update_interval,
        jet_cancellation_token.child_token(),
    )
    .await;

    let rooted_tx_geyser_rx = geyser
        .subscribe_transactions()
        .await
        .expect("failed to subscribe geyser transactions");
    let (rooted_transactions_rx, rooted_tx_loop_fut) =
        GrpcRootedTxReceiver::new(rooted_tx_geyser_rx);

    let initial_identity = config.identity.keypair.unwrap_or(Keypair::new());

    let leader_tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync + 'static> =
        Arc::new(OverrideTpuInfoService {
            override_vec: config.quic.tpu_info_override.clone(),
            other: cluster_tpu_info.clone(),
        });

    let quic_gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map: stake_info_map.clone(),
        gateway_tx_channel_capacity: 10000,
        leader_tpu_info_service,
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

    let ah = tg.spawn(async move {
        gateway_join_handle.await.expect("quic gateway join handle");
    });
    tg_name_map.insert(ah.id(), "quic_gateway".to_string());

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

    // Set up Lewis event tracking pipeline
    let (lewis_handler, lewis_fut) = create_lewis_pipeline(
        config.lewis_events.clone(),
        jet_cancellation_token.child_token(),
    );

    let mut tx_forwader = TransactionFanout::new(
        Arc::new(cluster_tpu_info.clone()),
        shield_policy_store,
        scheduler_out,
        quic_gateway_bidi,
        config.send_transaction_service.leader_forward_count,
        config.send_transaction_service.extra_fanout,
        lewis_handler,
    );

    let ah = tg.spawn(async move { tx_forwader.run().await });
    tg_name_map.insert(ah.id(), "transaction_fanout".to_string());

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
    .await;

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

    // Spawn Lewis client task if configured
    if let Some(fut) = lewis_fut {
        let ah = tg.spawn(
            fut.inspect(|result| {
                if let Err(e) = result {
                    error!("Lewis client error: {e}");
                }
            })
            .map(drop),
        );
        tg_name_map.insert(ah.id(), "lewis_client".to_string());
    }

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

    let ah = tg.spawn(async move {
        rooted_tx_loop_fut.await;
    });
    tg_name_map.insert(ah.id(), "rooted_tx_receiver".to_string());

    if let Some(jet_gw_listener_fut) = jet_gw_listener {
        let ah = tg.spawn(jet_gw_listener_fut);
        tg_name_map.insert(ah.id(), "jet_gw_listener".to_string());
    }

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

    let ah = tg.spawn(async move {
        sigint.recv().await;
        info!("SIGINT received...");
    });

    tg_name_map.insert(ah.id(), "SIGINT".to_string());

    local
        .run_until(async {
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
            if tg.len() > 0 {
                for (_id, name) in tg_name_map.iter() {
                    warn!("task -- {name} : did not finish in time, aborting");
                }
            }
            tg.abort_all();
            Ok(())
        })
        .await
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

                    if let Err(error) = client
                        .post(prometheus_url.clone())
                        .header("Content-Type", "text/plain")
                        .body(inject_job_label(&collect_to_text(), "jet", &current_identity.to_string()))
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
