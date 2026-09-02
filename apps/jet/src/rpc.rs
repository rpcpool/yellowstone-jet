use jsonrpsee::types::error::{ErrorObject, ErrorObjectOwned, INVALID_PARAMS_CODE};

pub mod admin {
    use {
        super::invalid_params,
        crate::{cluster_tpu_info::ClusterTpuInfoProvider, timer_wheel::ActivityWindow},
        anyhow::Context as _,
        futures::future::{BoxFuture, FutureExt, TryFutureExt, ready},
        hyper::{Request, Response, StatusCode},
        jsonrpsee::{
            core::{RpcResult, async_trait, http_helpers::Body},
            proc_macros::rpc,
            server::{ServerBuilder, ServerHandle},
        },
        solana_pubkey::Pubkey,
        std::{
            error::Error,
            fmt,
            future::Future,
            net::SocketAddr,
            sync::Arc,
            task::{Context, Poll},
            time::{Duration, Instant},
        },
        tokio::sync::Mutex,
        tower::Service,
        tracing::{debug, info},
        yellowstone_jet_tpu_client::identity::HardenedKeypair,
    };

    pub const TPU_STALL_THRESHOLD: Duration = Duration::from_secs(30);

    #[derive(Default)]
    pub struct TpuActivityTracker {
        pub sent_activity: ActivityWindow,
        pub failed_activity: ActivityWindow,
    }

    #[rpc(server, client)]
    pub trait Rpc {
        #[method(name = "getLatestSlot")]
        async fn get_latest_slot(&self) -> RpcResult<u64>;

        #[method(name = "getIdentity")]
        async fn get_identity(&self) -> RpcResult<String>;

        #[method(name = "setIdentity")]
        async fn set_identity(&self, keypair_file: String, require_tower: bool) -> RpcResult<()>;

        #[method(name = "setIdentityFromBytes")]
        async fn set_identity_from_bytes(
            &self,
            identity_keypair: Vec<u8>,
            require_tower: bool,
        ) -> RpcResult<()>;

        #[method(name = "resetIdentity")]
        async fn reset_identity(&self) -> RpcResult<()>;
    }

    #[async_trait::async_trait]
    pub trait JetIdentityUpdater {
        async fn update_identity(&mut self, identity: HardenedKeypair);

        fn get_identity(&self) -> Pubkey;
    }

    pub struct RpcServerImpl<JetIdentityUpdateT> {
        // pub quic: QuicTxSender,
        pub allowed_identity: Option<Pubkey>,
        pub jet_identity_updater: Arc<Mutex<JetIdentityUpdateT>>,
        pub cluster_tpu_info: Arc<dyn ClusterTpuInfoProvider>,
    }

    #[async_trait]
    impl<JetIdentityUpdateT> RpcServer for RpcServerImpl<JetIdentityUpdateT>
    where
        JetIdentityUpdateT: JetIdentityUpdater + Send + 'static,
    {
        async fn get_latest_slot(&self) -> RpcResult<u64> {
            Ok(self.cluster_tpu_info.latest_seen_slot())
        }

        async fn get_identity(&self) -> RpcResult<String> {
            let identity = self.jet_identity_updater.lock().await.get_identity();
            Ok(identity.to_string())
        }

        async fn set_identity(&self, keypair_file: String, require_tower: bool) -> RpcResult<()> {
            if require_tower {
                return Err(invalid_params(
                    "set_identity with require_tower is not supported".to_owned(),
                ));
            }
            let keypair = HardenedKeypair::read_from_file(&keypair_file).map_err(|err| {
                invalid_params(format!(
                    "Failed to read identity keypair from {keypair_file}: {err}"
                ))
            })?;
            self.set_keypair(keypair).await
        }

        async fn set_identity_from_bytes(
            &self,
            identity_keypair: Vec<u8>,
            require_tower: bool,
        ) -> RpcResult<()> {
            if require_tower {
                return Err(invalid_params(
                    "set_identity_from_bytes with require_tower is not supported".to_owned(),
                ));
            }
            let keypair =
                HardenedKeypair::try_from(identity_keypair.as_slice()).map_err(|err| {
                    invalid_params(format!(
                        "Failed to read identity keypair from provided byte array: {err}"
                    ))
                })?;
            self.set_keypair(keypair).await
        }

        async fn reset_identity(&self) -> RpcResult<()> {
            let random_identity = HardenedKeypair::new();

            self.jet_identity_updater
                .lock()
                .await
                .update_identity(random_identity)
                .await;
            Ok(())
        }
    }

    impl<JetIdentityUpdateT> RpcServerImpl<JetIdentityUpdateT>
    where
        JetIdentityUpdateT: JetIdentityUpdater + Send + 'static,
    {
        async fn set_keypair(&self, identity: HardenedKeypair) -> RpcResult<()> {
            if let Some(allow_ident) = &self.allowed_identity
                && allow_ident != &identity.pubkey()
            {
                return Err(invalid_params("invalid identity".to_owned()));
            }
            let pubkey = identity.pubkey();
            self.jet_identity_updater
                .lock()
                .await
                .update_identity(identity)
                .await;
            info!("update identity: {pubkey}");

            Ok(())
        }
    }

    /// Answers the `/metrics` endpoint with the process's Prometheus text-format metrics.
    fn metrics_response() -> (StatusCode, String) {
        (StatusCode::OK, crate::metrics::collect_to_text())
    }

    #[derive(Clone)]
    pub struct UriRequestMiddleware<S, F> {
        service: S,
        uri: &'static str,
        get_response: F,
    }

    impl<S, F, Fut> Service<Request<Body>> for UriRequestMiddleware<S, F>
    where
        S: Service<Request<Body>, Response = Response<Body>>,
        S::Response: 'static,
        S::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
        S::Future: Send + 'static,
        F: Fn() -> Fut,
        Fut: Future<Output = (StatusCode, String)> + Send + 'static,
    {
        type Response = S::Response;
        type Error = Box<dyn Error + Send + Sync + 'static>;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, request: Request<Body>) -> Self::Future {
            if self.uri == request.uri() {
                let get_response_fut = (self.get_response)();
                let uri = self.uri.to_string();
                let ts = Instant::now();
                async move {
                    let (status, body) = get_response_fut.await;
                    let response = Response::builder()
                        .status(status)
                        .body(Body::new(body))
                        .expect("failed to create response");
                    debug!(
                        uri = uri,
                        elapsed_ms = ts.elapsed().as_millis(),
                        "created response for uri"
                    );
                    Ok(response)
                }
                .boxed()
            } else {
                self.service.call(request).map_err(Into::into).boxed()
            }
        }
    }

    /// A real (not closure-driven) `/health` middleware: unlike [`UriRequestMiddleware`], it
    /// owns its dependencies directly as fields rather than through a generic `get_response`
    /// closure, so it has somewhere to grow tracked state later (e.g. a stall tracker) without
    /// reshaping the closure signature every time.
    pub struct HealthService<S, JetIdentityUpdateT> {
        service: S,
        jet_identity_updater: Arc<Mutex<JetIdentityUpdateT>>,
        allowed_identity: Option<Pubkey>,
        tpu_activity_tracker: Arc<TpuActivityTracker>,
    }

    // Not `#[derive(Clone)]`: that would also require `JetIdentityUpdateT: Clone`, but it only
    // ever appears behind `Arc<Mutex<_>>` here, which is `Clone` unconditionally.
    impl<S: Clone, JetIdentityUpdateT> Clone for HealthService<S, JetIdentityUpdateT> {
        fn clone(&self) -> Self {
            Self {
                service: self.service.clone(),
                jet_identity_updater: Arc::clone(&self.jet_identity_updater),
                allowed_identity: self.allowed_identity,
                tpu_activity_tracker: Arc::clone(&self.tpu_activity_tracker),
            }
        }
    }

    impl<S, JetIdentityUpdateT> HealthService<S, JetIdentityUpdateT> {
        const URI: &'static str = "/health";

        const fn new(
            service: S,
            jet_identity_updater: Arc<Mutex<JetIdentityUpdateT>>,
            allowed_identity: Option<Pubkey>,
            tpu_activity_tracker: Arc<TpuActivityTracker>,
        ) -> Self {
            Self {
                service,
                jet_identity_updater,
                allowed_identity,
                tpu_activity_tracker,
            }
        }
    }

    impl<S, JetIdentityUpdateT> HealthService<S, JetIdentityUpdateT>
    where
        JetIdentityUpdateT: JetIdentityUpdater + Send + 'static,
    {
        /// Answers the `/health` endpoint: unhealthy if `allowed_identity` is set and the
        /// current identity doesn't match it, or if
        /// [`crate::metrics::jet::get_health_status`] reports an error; `ok` otherwise.
        async fn health_check(
            jet_identity_updater: &Arc<Mutex<JetIdentityUpdateT>>,
            allowed_identity: Option<Pubkey>,
            tpu_activity_tracker: Arc<TpuActivityTracker>,
        ) -> (StatusCode, String) {
            let now = Instant::now();
            let current_identity = jet_identity_updater.lock().await.get_identity();
            if let Some(expected) = allowed_identity {
                if expected != current_identity {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "identity mismatch".to_owned(),
                    );
                }
            }

            let sent_cnt = tpu_activity_tracker.sent_activity.count_in_window(now);
            let failed_cnt = tpu_activity_tracker.failed_activity.count_in_window(now);
            let total_cnt = sent_cnt + failed_cnt;
            if total_cnt > 0 {
                // Make sure at lesat 1/5 of the slot of the time wheel has been used in case of only failure.
                let failure_are_minimally_spread = tpu_activity_tracker
                    .failed_activity
                    .active_slot_fraction_at_least(now, 1, 5);
                if sent_cnt == 0 && failed_cnt > 0 && failure_are_minimally_spread {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "tpu sender has stalled".to_owned(),
                    );
                }
            }

            // TODO: need to check TPUs for processed?
            match crate::metrics::jet::get_health_status() {
                Ok(()) => (StatusCode::OK, "ok".to_owned()),
                Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
            }
        }
    }

    impl<S, JetIdentityUpdateT> Service<Request<Body>> for HealthService<S, JetIdentityUpdateT>
    where
        S: Service<Request<Body>, Response = Response<Body>>,
        S::Response: 'static,
        S::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
        S::Future: Send + 'static,
        JetIdentityUpdateT: JetIdentityUpdater + Send + 'static,
    {
        type Response = S::Response;
        type Error = Box<dyn Error + Send + Sync + 'static>;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, request: Request<Body>) -> Self::Future {
            if request.uri() == Self::URI {
                let jet_identity_updater = Arc::clone(&self.jet_identity_updater);
                let allowed_identity = self.allowed_identity;
                let ts = Instant::now();
                let tpu_activity_tracker = Arc::clone(&self.tpu_activity_tracker);
                async move {
                    let (status, body) = Self::health_check(
                        &jet_identity_updater,
                        allowed_identity,
                        tpu_activity_tracker,
                    )
                    .await;
                    let response = Response::builder()
                        .status(status)
                        .body(Body::new(body))
                        .expect("failed to create response");
                    debug!(
                        uri = Self::URI,
                        elapsed_ms = ts.elapsed().as_millis(),
                        "created response for uri"
                    );
                    Ok(response)
                }
                .boxed()
            } else {
                self.service.call(request).map_err(Into::into).boxed()
            }
        }
    }

    pub struct AdminServer {
        server_handle: Option<ServerHandle>,
    }

    impl fmt::Debug for AdminServer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("AdminServer").finish()
        }
    }

    impl AdminServer {
        pub async fn new<IU>(
            addr: SocketAddr,
            jet_identity_updater: IU,
            allowed_identity: Option<Pubkey>,
            cluster_tpu_info: Arc<dyn ClusterTpuInfoProvider>,
            tpu_activity_tracker: Arc<TpuActivityTracker>,
        ) -> Self
        where
            IU: JetIdentityUpdater + Send + 'static,
        {
            let jet_identity_updater = Arc::new(Mutex::new(jet_identity_updater));
            let health_jet_identity_updater = Arc::clone(&jet_identity_updater);
            let server_middleware = tower::ServiceBuilder::new()
                .layer_fn(move |service| {
                    let tpu_activity_tracker = Arc::clone(&tpu_activity_tracker);
                    HealthService::new(
                        service,
                        Arc::clone(&health_jet_identity_updater),
                        allowed_identity,
                        tpu_activity_tracker,
                    )
                })
                .layer_fn(|service| UriRequestMiddleware {
                    service,
                    uri: "/metrics",
                    get_response: || ready(metrics_response()),
                });

            let server_handle = ServerBuilder::new()
                .set_http_middleware(server_middleware)
                .build(addr)
                .await
                .map(|server| {
                    server.start(
                        RpcServerImpl {
                            allowed_identity,
                            jet_identity_updater,
                            cluster_tpu_info,
                        }
                        .into_rpc(),
                    )
                })
                .with_context(|| format!("Failed to start HTTP server at {addr}"))
                .expect("Failed to start HTTP server");
            info!("started RPC admin server on {addr}");

            Self {
                server_handle: Some(server_handle),
            }
        }

        pub fn shutdown(mut self) {
            if let Some(server_handle) = self.server_handle.take() {
                let _ = server_handle.stop();
            }
        }
    }
}

pub mod solana_like {
    use {
        crate::{
            http_tx_handler::{
                HttpTransactionHandler, HttpTxMiddleware, SimulationPerformed, XRequestId,
                XSubscriptionId,
            },
            metrics,
            payload::JetRpcSendTransactionConfig,
            rpc::invalid_params,
            solana::decode_and_deserialize,
            transaction_handler::TransactionHandler,
        },
        anyhow::Context as _,
        jsonrpsee::{
            Extensions,
            core::{RpcResult, async_trait},
            proc_macros::rpc,
            server::{ServerBuilder, ServerConfigBuilder, ServerHandle},
        },
        solana_rpc_client_api::response::RpcVersionInfo,
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction_status_client_types::UiTransactionEncoding,
        std::{borrow::Cow, fmt, net::SocketAddr},
        tracing::{debug, info, warn},
    };

    // should be more than enough for `sendTransaction` request
    const MAX_REQUEST_BODY_SIZE: u32 = 32 * (1 << 10); // 32kB

    #[rpc(server, client)]
    pub trait Rpc {
        #[method(name = "getVersion")]
        fn get_version(&self) -> RpcResult<RpcVersionInfo>;

        #[method(name = "sendTransaction", with_extensions)]
        async fn send_transaction(
            &self,
            data: String,
            config: Option<JetRpcSendTransactionConfig>,
        ) -> RpcResult<String>;
    }

    pub struct RpcServerImpl {
        pub log_invalid_txn: bool,
        pub tx_handler: TransactionHandler,
    }

    fn apply_simulation_performed(
        config_with_policies: &mut JetRpcSendTransactionConfig,
        extensions: &Extensions,
    ) {
        if extensions.get::<SimulationPerformed>().is_some() {
            config_with_policies.config.skip_preflight = true;
        }
    }

    #[async_trait]
    impl RpcServer for RpcServerImpl {
        fn get_version(&self) -> RpcResult<RpcVersionInfo> {
            debug!("get_version rpc request received");
            Ok(TransactionHandler::get_version())
        }

        async fn send_transaction(
            &self,
            extensions: &Extensions,
            data: String,
            config_with_forwarding_policies: Option<JetRpcSendTransactionConfig>,
        ) -> RpcResult<String /* Signature */> {
            debug!("send_transaction rpc request received");
            let mut config_with_policies = config_with_forwarding_policies.unwrap_or_default();
            apply_simulation_performed(&mut config_with_policies, extensions);
            let config = config_with_policies.config;

            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

            let (_, transaction) = decode_and_deserialize::<VersionedTransaction>(
                data,
                encoding
                    .into_binary_encoding()
                    .ok_or_else(|| invalid_params("unsupported encoding"))?,
            )?;
            let maybe_txn_sig = transaction.signatures.first().cloned();
            let maybe_request_id = extensions.get::<XRequestId>().map(|x| x.0);
            let maybe_subscription_id = extensions.get::<XSubscriptionId>().map(|x| x.0);

            self.tx_handler
                .handle_versioned_transaction(transaction, config_with_policies, maybe_request_id, maybe_subscription_id)
                .await
                .inspect_err(|e| {
                    let name = e.variant_name();
                    metrics::jet::incr_versioned_txn_handler_error(name);
                    let sig = if self.log_invalid_txn {
                        if let Some(sig) = maybe_txn_sig {
                            Cow::Owned(sig.to_string())
                        } else {
                            Cow::Borrowed("unknown")
                        }
                    } else if maybe_txn_sig.is_some() {
                        Cow::Borrowed("[REDACTED]")
                    } else {
                        Cow::Borrowed("unknown")
                    };
                    warn!(
                        signature = %sig,
                        x_request_id = %maybe_request_id.map(|x| x.to_string()).unwrap_or_else(|| "unknown".to_string()),
                        error = %e,
                        "send_transaction failed"
                    )
                })
                .map_err(Into::into)
        }
    }

    pub struct SolanaLikeServer {
        server_handle: Option<ServerHandle>,
    }

    impl fmt::Debug for SolanaLikeServer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("SolanaLikeServer").finish()
        }
    }

    impl SolanaLikeServer {
        pub async fn new(
            addr: SocketAddr,
            tx_handler: TransactionHandler,
            log_invalid_txn: bool,
        ) -> Self {
            let rpc_server_impl = RpcServerImpl {
                tx_handler: tx_handler.clone(),
                log_invalid_txn,
            };
            let http_tx_handler = HttpTransactionHandler::new(tx_handler, log_invalid_txn);
            let server_middleware = tower::ServiceBuilder::new()
                .layer_fn(move |service| HttpTxMiddleware::new(service, http_tx_handler.clone()));
            let server_config = ServerConfigBuilder::default()
                .max_request_body_size(MAX_REQUEST_BODY_SIZE)
                .build();
            let server_handle = ServerBuilder::new()
                .set_http_middleware(server_middleware)
                .set_config(server_config)
                .build(addr)
                .await
                .map(|server| server.start(rpc_server_impl.into_rpc()))
                .with_context(|| format!("Failed to start HTTP server at {addr}"))
                .expect("Failed to start HTTP server");
            info!("started RPC solana-like server on {addr}");

            Self {
                server_handle: Some(server_handle),
            }
        }

        pub fn shutdown(mut self) {
            if let Some(server_handle) = self.server_handle.take() {
                let _ = server_handle.stop();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use {super::*, solana_rpc_client_api::config::RpcSendTransactionConfig};

        #[test]
        fn simulation_performed_extension_allows_preflight_requested_config() {
            let mut extensions = Extensions::new();
            extensions.insert(SimulationPerformed);
            let mut config = JetRpcSendTransactionConfig {
                config: RpcSendTransactionConfig {
                    skip_preflight: false,
                    ..Default::default()
                },
                ..Default::default()
            };

            apply_simulation_performed(&mut config, &extensions);

            assert!(config.config.skip_preflight);
        }

        #[test]
        fn missing_simulation_performed_extension_preserves_config() {
            let extensions = Extensions::new();
            let mut config = JetRpcSendTransactionConfig {
                config: RpcSendTransactionConfig {
                    skip_preflight: false,
                    ..Default::default()
                },
                ..Default::default()
            };

            apply_simulation_performed(&mut config, &extensions);

            assert!(!config.config.skip_preflight);
        }
    }
}

pub fn invalid_params(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned::<()>(INVALID_PARAMS_CODE, message, None)
}
