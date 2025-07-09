use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::future::{self, Either, FutureExt},
    jsonrpsee::http_client::HttpClientBuilder,
    reqwest::{Client, Url},
    solana_client::rpc_client::RpcClientConfig,
    solana_commitment_config::CommitmentConfig,
    solana_keypair::{read_keypair, Keypair},
    solana_pubkey::Pubkey,
    solana_rpc_client::http_sender::HttpSender,
    std::{
        convert::identity,
        fs,
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::{
        runtime::Builder,
        signal::unix::{signal, SignalKind},
        sync::oneshot,
        task::JoinHandle,
    },
    tracing::{info, warn, error},
    yellowstone_jet::{
        blockhash_queue::BlockhashQueue, cluster_tpu_info::ClusterTpuInfo, config::{
            load_config, ConfigJet, ConfigJetGatewayClient,
            PrometheusConfig, RpcErrorStrategy,
        }, feature_flags::FeatureSet, grpc_geyser::{GeyserStreams, GeyserSubscriber}, grpc_jet::GrpcServer, grpc_lewis::LewisEventClient, metrics::{collect_to_text, inject_job_label, jet as metrics}, quic::QuicClient, quic_solana::ConnectionCache, rpc::{rpc_admin::RpcClient, rpc_solana_like::RpcServerImpl, RpcServer, RpcServerType}, setup_tracing, solana_rpc_utils::{RetryRpcSender, RetryRpcSenderStrategy}, stake::{self, spawn_cache_stake_info_map, StakeInfoMap}, task_group::TaskGroup, transactions::{GrpcRootedTxReceiver, SendTransactionsPool}, util::{IdentityFlusherWaitGroup, PubkeySigner, ValueObserver, WaitShutdown}
    },
    yellowstone_shield_store::{PolicyStore, PolicyStoreTrait},
};

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

async fn spawn_jet_gw_listener(
    stake_info: StakeInfoMap,
    jet_gw_config: ConfigJetGatewayClient,
    mut identity_observer: ValueObserver<PubkeySigner>,
    tx_sender: RpcServerImpl,
    expected_identity: Option<Pubkey>,
    features: FeatureSet,
    mut stop_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    loop {
        let jet_gw_config2 = jet_gw_config.clone();
        let tx_sender2 = tx_sender.clone();
        let features = features.clone();
        let mut identity_observer2 = identity_observer.clone();
        let (stop_tx2, stop_rx2) = tokio::sync::oneshot::channel();
        let stake_info2 = stake_info.clone();
        let fut = identity_observer.until_value_change(move |current_identity| {
            if let Some(expected_identity) = expected_identity {
                if current_identity.pubkey() != expected_identity {
                    let actual_pubkey = current_identity.pubkey();
                    warn!("expected identity: {expected_identity}, actual identity: {actual_pubkey}");
                    warn!("will not connect to jet-gateway with identity: {actual_pubkey}, waiting for correct identity to be set...");
                    future::pending().boxed()
                } else {
                    GrpcServer::run_with(
                        Arc::new(current_identity),
                        stake_info2,
                        jet_gw_config2.clone(),
                        tx_sender2.clone(),
                        features,
                        stop_rx2,
                    ).boxed()
                }
            } else {
                GrpcServer::run_with(
                    Arc::new(current_identity),
                    stake_info2,
                    jet_gw_config2.clone(),
                    tx_sender2.clone(),
                    features,
                    stop_rx2,
                ).boxed()
            }
        });
        tokio::select! {
            result = fut => {
                match result {
                    Either::Left(_) => {}
                    Either::Right(_) => {
                        warn!("jet-gateway listener stopped");
                    }
                }
            },
            _ = &mut stop_rx => {
                drop(stop_tx2);
                return Ok(());
            },
            current_identity = identity_observer2.observe() => {
                if let Some(expected_identity) = expected_identity {
                    if current_identity.pubkey() != expected_identity {
                        drop(stop_tx2);
                    }
                }
            }
        }
    }
}

///
/// This task keeps the stake metrics up to date for the current identity.
///
async fn keep_stake_metrics_up_to_date_task(
    mut stake_info_identity_observer: ValueObserver<Pubkey>,
    stake_info_map: StakeInfoMap,
) {
    loop {
        let current_identy = stake_info_identity_observer.get_current();

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
            _ = stake_info_identity_observer.observe() => {}
        }
    }
}

async fn run_jet(config: ConfigJet) -> anyhow::Result<()> {
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

        Some(Arc::new(policy_store) as Arc<dyn PolicyStoreTrait + Send + Sync>)
    } else {
        None
    };

    let (shutdown_geyser_tx, shutdown_geyser_rx) = oneshot::channel();
    let (geyser, mut geyser_handle) = GeyserSubscriber::new(
        shutdown_geyser_rx,
        config.upstream.primary_grpc.clone(),
        config
            .upstream
            .secondary_grpc
            .unwrap_or(config.upstream.primary_grpc),
    );
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

    let identity_flusher_wg = IdentityFlusherWaitGroup::default();

    let initial_identity = config.identity.keypair.unwrap_or(Keypair::new());
    let (quic_session, quic_identity_man) = ConnectionCache::new(
        config.quic.clone(),
        initial_identity,
        stake_info_map.clone(),
        identity_flusher_wg.clone(),
    );

    let (event_tracker, lewis_fut) = LewisEventClient::create_event_tracker(config.lewis_events);

    let quic_tx_sender = QuicClient::new(
        Arc::new(cluster_tpu_info.clone()),
        config.quic.clone(),
        Arc::new(quic_session),
        shield_policy_store,
        event_tracker,
    );

    let (send_transactions, send_tx_pool_fut) = SendTransactionsPool::spawn(
        config.send_transaction_service,
        Arc::new(blockhash_queue.clone()),
        Box::new(rooted_transactions_rx),
        Arc::new(quic_tx_sender.clone()),
    )
    .await;

    identity_flusher_wg
        .add_flusher(Box::new(send_transactions.clone()))
        .await;

    let stake_info_identity_observer = quic_identity_man.observe_identity_change();
    let quic_identity_observer = quic_identity_man.observe_signer_change();

    let rpc_admin = RpcServer::new(
        config.listen_admin.bind[0],
        RpcServerType::Admin {
            quic_identity_man,
            allowed_identity: config.identity.expected,
        },
    )
    .await?;

    let rpc_solana_like = RpcServer::new(
        config.listen_solana_like.bind[0],
        RpcServerType::SolanaLike {
            stp: send_transactions.clone(),
            rpc: config.upstream.rpc.clone(),
            proxy_sanitize_check: config.listen_solana_like.proxy_sanitize_check,
            proxy_preflight_check: config.listen_solana_like.proxy_preflight_check,
        },
    )
    .await?;

    let (stop_jet_gw_listener_tx, stop_jet_gw_listener_rx) = oneshot::channel();
    let jet_gw_listener = if let Some(config_jet_gateway) = config.jet_gateway {
        if config_jet_gateway.endpoints.is_empty() {
            warn!("no endpoints for jet-gateway with existed config");
            None
        } else {
            let jet_gw_config = config_jet_gateway.clone();
            let quic_identity_observer = quic_identity_observer.clone();
            let expected_identity = config.identity.expected;

            let tx_sender = RpcServer::create_solana_like_rpc_server_impl(
                send_transactions.clone(),
                config.upstream.rpc.clone(),
                config.listen_solana_like.proxy_sanitize_check,
                config.listen_solana_like.proxy_preflight_check,
            )
            .await
            .expect("rpc server impl");

            info!("starting jet-gateway listener");
            let stake_info = stake_info_map.clone();
            let h = tokio::spawn(async move {
                spawn_jet_gw_listener(
                    stake_info,
                    jet_gw_config,
                    quic_identity_observer,
                    tx_sender,
                    expected_identity,
                    config.features,
                    stop_jet_gw_listener_rx,
                )
                .await
            })
            .map(|result| result.map_err(anyhow::Error::new).and_then(identity));

            Some(h.boxed())
        }
    } else {
        warn!("Skipping jet-gateway listener, no config provided");
        None
    };

    let mut sigint = signal(SignalKind::interrupt())?;

    let mut tg = TaskGroup::default();

    tg.spawn_cancelable("stake_cache_refresh_task", stake_info_bg_fut);

    tg.spawn_cancelable(
        "stake_info_metrics_update",
        keep_stake_metrics_up_to_date_task(stake_info_identity_observer, stake_info_map.clone()),
    );

     if let Some(fut) = lewis_fut {
        tg.spawn_with_shutdown("lewis_events", |mut stop| async move {
            tokio::select! {
                result = fut => {
                    if let Err(e) = result {
                        error!("Lewis event loop error: {}", e);
                    }
                }
                _ = &mut stop => {
                    info!("Shutting down Lewis event client");
                }
            }
        });
    }

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

    tg.spawn_cancelable("send_transactions_pool", send_tx_pool_fut);

    if let Some(mut jet_gw_listener) = jet_gw_listener {
        tg.spawn_with_shutdown("jet_gw_listener", |mut stop| async move {
            tokio::select! {
                result = &mut jet_gw_listener => {
                    result.expect("jet_gw_listener");
                },
                _ = &mut stop => {
                    let _ = stop_jet_gw_listener_tx.send(());
                    jet_gw_listener.await.expect("jet_gw_listener");
                },
            }
        });
    }

    if let Some(config_prometheus) = config.prometheus {
        let push_gw_task =
            spawn_push_prometheus_metrics(quic_identity_observer, config_prometheus).await?;
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
    identity_observer: ValueObserver<PubkeySigner>,
    config: PrometheusConfig,
) -> anyhow::Result<JoinHandle<()>> {
    let prometheus_url = Url::parse(&config.url).expect("");
    let mut interval = tokio::time::interval(config.push_interval);
    let client = Client::new();

    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
            _ = interval.tick() => {
                let current_identity = identity_observer.get_current().pubkey();

                if let Err(error) = client
                    .post(prometheus_url.clone())
                    .header("Content-Type", "text/plain")
                    .body(inject_job_label(&collect_to_text(), "jet", &current_identity.to_string()))
                    .send()
                    .await {
                        warn!(?error, "Error pushing metrics");
                    }
                }
            }
        }
    }))
}
