use {
    crate::transaction_handler::TransactionHandler,
    anyhow::Context as _,
    dashmap::DashMap,
    futures::future::{BoxFuture, FutureExt, TryFutureExt, ready},
    governor::{
        Quota, RateLimiter,
        clock::DefaultClock,
        middleware::NoOpMiddleware,
        state::{InMemoryState, NotKeyed},
    },
    hyper::{Request, Response, StatusCode},
    jsonrpsee::{
        core::http_helpers::Body,
        server::{ServerBuilder, ServerHandle},
        types::error::{ErrorObject, ErrorObjectOwned, INVALID_PARAMS_CODE},
    },
    rpc_admin::JetIdentityUpdater,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        error::Error,
        fmt,
        future::Future,
        hash::Hash,
        net::SocketAddr,
        num::NonZeroU32,
        sync::{Arc, Mutex as StdMutex},
        task::{Context, Poll},
        time::Instant,
    },
    tokio::sync::Mutex,
    tower::Service,
    tracing::{debug, info},
};

// should be more than enough for `sendTransaction` request
const MAX_REQUEST_BODY_SIZE: u32 = 32 * (1 << 10); // 32kB

type RpcLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

#[derive(Clone)]
pub struct RpcRateLimiter<K: Eq + Hash + Clone> {
    limiters: Arc<DashMap<K, Arc<RpcLimiter>>>,
    limits_config: Arc<HashMap<K, u32>>,
    default_limit: u32,
}

impl<K: Eq + Hash + Clone> RpcRateLimiter<K> {
    pub fn new(limits_config: HashMap<K, u32>, default_limit: u32) -> Self {
        Self {
            limiters: Arc::new(DashMap::new()),
            limits_config: Arc::new(limits_config),
            default_limit,
        }
    }

    pub fn check(&self, key: &K) -> Result<(), ()> {
        let limit = self
            .limits_config
            .get(key)
            .copied()
            .unwrap_or(self.default_limit);

        if limit == 0 {
            return Ok(());
        }

        let limiter = self.limiters.entry(key.clone()).or_insert_with(|| {
            let quota = Quota::per_second(NonZeroU32::new(limit).unwrap());
            Arc::new(RateLimiter::direct(quota))
        });

        if limiter.check().is_err() {
            Err(())
        } else {
            Ok(())
        }
    }
}

pub enum RpcServerType {
    Admin {
        jet_identity_updater: Arc<Mutex<Box<dyn JetIdentityUpdater + Send + 'static>>>,
        allowed_identity: Option<Pubkey>,
    },

    SolanaLike {
        tx_handler: TransactionHandler,
        rate_limiter: RpcRateLimiter<Pubkey>,
    },
}

#[derive(Clone)]
pub struct RpcServer {
    server_handle: Arc<StdMutex<Option<ServerHandle>>>,
}

impl fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer").finish()
    }
}

impl RpcServer {
    pub async fn new(addr: SocketAddr, server_type: RpcServerType) -> anyhow::Result<Self> {
        let server_type_str = match server_type {
            RpcServerType::Admin { .. } => "admin",
            RpcServerType::SolanaLike { .. } => "solana-like",
        };
        let server_handle = match server_type {
            RpcServerType::Admin {
                jet_identity_updater: quic_identity_man,
                allowed_identity,
            } => {
                use rpc_admin::{RpcServer, RpcServerImpl};

                let server_middleware = tower::ServiceBuilder::new()
                    .layer_fn(|service| UriRequestMiddleware {
                        service,
                        uri: "/health",
                        get_response: || {
                            // TODO: need to check TPUs for processed?
                            match crate::metrics::jet::get_health_status() {
                                Ok(()) => ready((StatusCode::OK, "ok".to_owned())),
                                Err(error) => {
                                    ready((StatusCode::SERVICE_UNAVAILABLE, error.to_string()))
                                }
                            }
                        },
                    })
                    .layer_fn(|service| UriRequestMiddleware {
                        service,
                        uri: "/metrics",
                        get_response: || ready((StatusCode::OK, crate::metrics::collect_to_text())),
                    });

                ServerBuilder::new()
                    .set_http_middleware(server_middleware)
                    .build(addr)
                    .await
                    .map(|server| {
                        server.start(
                            RpcServerImpl {
                                allowed_identity,
                                jet_identity_updater: quic_identity_man,
                            }
                            .into_rpc(),
                        )
                    })
            }
            RpcServerType::SolanaLike {
                tx_handler,
                rate_limiter,
            } => {
                use rpc_solana_like::RpcServer;

                let rpc_server_impl =
                    Self::create_solana_like_rpc_server_impl(tx_handler, rate_limiter);

                ServerBuilder::new()
                    .max_request_body_size(MAX_REQUEST_BODY_SIZE)
                    .build(addr)
                    .await
                    .map(|server| server.start(rpc_server_impl.into_rpc()))
            }
        }
        .with_context(|| format!("Failed to start HTTP server at {addr}"))?;
        info!("started RPC {server_type_str} server on {addr}");

        Ok(Self {
            server_handle: Arc::new(StdMutex::new(Some(server_handle))),
        })
    }

    pub const fn create_solana_like_rpc_server_impl(
        tx_handler: TransactionHandler,
        rate_limiter: RpcRateLimiter<Pubkey>,
    ) -> rpc_solana_like::RpcServerImpl {
        rpc_solana_like::RpcServerImpl {
            tx_handler,
            rate_limiter,
        }
    }

    pub fn shutdown(self) {
        match self.server_handle.lock().unwrap().take() {
            Some(server_handle) => {
                server_handle.stop().expect("alive server");
            }
            None => panic!("RpcServer already shutdown"),
        }
    }
}

pub mod rpc_admin {
    use {
        super::invalid_params,
        crate::transaction_handler::ENABLE_TIP_CHECK,
        jsonrpsee::{
            core::{RpcResult, async_trait},
            proc_macros::rpc,
        },
        solana_keypair::{Keypair, read_keypair_file},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        std::sync::{Arc, atomic::Ordering},
        tokio::sync::Mutex,
        tracing::info,
    };

    #[rpc(server, client)]
    pub trait Rpc {
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

        #[method(name = "setTipCheck")]
        async fn set_tip_check(&self, enabled: bool) -> RpcResult<()>;
    }

    #[async_trait::async_trait]
    pub trait JetIdentityUpdater {
        async fn update_identity(&mut self, identity: Keypair);

        async fn get_identity(&self) -> Pubkey;
    }

    pub struct RpcServerImpl {
        // pub quic: QuicTxSender,
        pub allowed_identity: Option<Pubkey>,
        pub jet_identity_updater: Arc<Mutex<Box<dyn JetIdentityUpdater + Send + 'static>>>,
    }

    #[async_trait]
    impl RpcServer for RpcServerImpl {
        async fn get_identity(&self) -> RpcResult<String> {
            let identity = self.jet_identity_updater.lock().await.get_identity().await;
            Ok(identity.to_string())
        }

        async fn set_identity(&self, keypair_file: String, require_tower: bool) -> RpcResult<()> {
            if require_tower {
                return Err(invalid_params(
                    "set_identity with require_tower is not supported".to_owned(),
                ));
            }
            let keypair = read_keypair_file(&keypair_file).map_err(|err| {
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
            let keypair = Keypair::from_bytes(&identity_keypair).map_err(|err| {
                invalid_params(format!(
                    "Failed to read identity keypair from provided byte array: {err}"
                ))
            })?;
            self.set_keypair(keypair).await
        }

        async fn reset_identity(&self) -> RpcResult<()> {
            let random_identity = Keypair::new();
            self.jet_identity_updater
                .lock()
                .await
                .update_identity(random_identity)
                .await;
            Ok(())
        }

        async fn set_tip_check(&self, enabled: bool) -> RpcResult<()> {
            ENABLE_TIP_CHECK.store(enabled, Ordering::Relaxed);
            info!("Tip check enabled: {}", enabled);
            Ok(())
        }
    }

    impl RpcServerImpl {
        async fn set_keypair(&self, identity: Keypair) -> RpcResult<()> {
            if let Some(allow_ident) = &self.allowed_identity {
                if allow_ident != &identity.pubkey() {
                    return Err(invalid_params("invalid identity".to_owned()));
                }
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
}

pub mod rpc_solana_like {
    use {
        crate::{
            payload::JetRpcSendTransactionConfig, rpc::RpcRateLimiter, rpc::invalid_params,
            solana::decode_and_deserialize, transaction_handler::TransactionHandler,
        },
        jsonrpsee::{
            core::{RpcResult, async_trait},
            proc_macros::rpc,
            types::error::ErrorObject,
        },
        solana_pubkey::Pubkey,
        solana_rpc_client_api::response::RpcVersionInfo,
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction_status_client_types::UiTransactionEncoding,
        tracing::debug,
    };

    #[rpc(server, client)]
    pub trait Rpc {
        #[method(name = "getVersion")]
        fn get_version(&self) -> RpcResult<RpcVersionInfo>;

        #[method(name = "sendTransaction")]
        async fn send_transaction(
            &self,
            data: String,
            config: Option<JetRpcSendTransactionConfig>,
        ) -> RpcResult<String>;
    }

    #[derive(Clone)]
    pub struct RpcServerImpl {
        pub tx_handler: TransactionHandler,
        pub rate_limiter: RpcRateLimiter<Pubkey>,
    }

    impl RpcServerImpl {
        // Internal method specifically for handling VersionedTransaction directly
        pub async fn handle_internal_transaction(
            &self,
            transaction: VersionedTransaction,
            config: JetRpcSendTransactionConfig,
        ) -> RpcResult<String /* Signature */> {
            debug!("handling internal versioned transaction");

            self.tx_handler
                .handle_versioned_transaction(transaction, config)
                .await
                .map_err(Into::into)
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
            data: String,
            config_with_forwarding_policies: Option<JetRpcSendTransactionConfig>,
        ) -> RpcResult<String /* Signature */> {
            debug!("send_transaction rpc request received");
            let config_with_policies = config_with_forwarding_policies.unwrap_or_default();
            let config = config_with_policies.config;

            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

            let (_, transaction) = decode_and_deserialize::<VersionedTransaction>(
                data,
                encoding
                    .into_binary_encoding()
                    .ok_or_else(|| invalid_params("unsupported encoding"))?,
            )?;

            if let Some(signer_pubkey) = transaction.message.static_account_keys().first() {
                if self.rate_limiter.check(signer_pubkey).is_err() {
                    tracing::warn!("RPC rate limit exceeded for signer: {}", signer_pubkey);
                    return Err(ErrorObject::owned(
                        -32002,
                        format!(
                            "Transaction sent by {} rate limited. Please try again",
                            signer_pubkey
                        ),
                        None::<()>,
                    ));
                }
            }

            self.handle_internal_transaction(transaction, config_with_policies)
                .await
        }
    }
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

pub fn invalid_params(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned::<()>(INVALID_PARAMS_CODE, message, None)
}
