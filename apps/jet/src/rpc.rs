use {
    crate::{cluster_tpu_info::ClusterTpuInfoProvider, transaction_handler::TransactionHandler},
    anyhow::Context as _,
    futures::future::{BoxFuture, FutureExt, TryFutureExt, ready},
    hyper::{Request, Response, StatusCode},
    jsonrpsee::{
        core::http_helpers::Body,
        server::{ServerBuilder, ServerConfigBuilder, ServerHandle},
        types::error::{ErrorObject, ErrorObjectOwned, INVALID_PARAMS_CODE},
    },
    rpc_admin::JetIdentityUpdater,
    solana_pubkey::Pubkey,
    std::{
        error::Error,
        fmt,
        future::Future,
        net::SocketAddr,
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

pub enum RpcServerType {
    Admin {
        jet_identity_updater: Arc<Mutex<Box<dyn JetIdentityUpdater + Send + 'static>>>,
        allowed_identity: Option<Pubkey>,
        cluster_tpu_info: Arc<dyn ClusterTpuInfoProvider>,
    },

    SolanaLike {
        tx_handler: TransactionHandler,
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
    pub async fn new(addr: SocketAddr, server_type: RpcServerType) -> Self {
        let server_type_str = match server_type {
            RpcServerType::Admin { .. } => "admin",
            RpcServerType::SolanaLike { .. } => "solana-like",
        };
        let server_handle = match server_type {
            RpcServerType::Admin {
                jet_identity_updater,
                allowed_identity,
                cluster_tpu_info,
            } => {
                use rpc_admin::{RpcServer, RpcServerImpl};
                let health_jet_identity_updater = Arc::clone(&jet_identity_updater);
                let server_middleware = tower::ServiceBuilder::new()
                    .layer_fn(move |service| {
                        let jet_identity_updater = Arc::clone(&health_jet_identity_updater);
                        UriRequestMiddleware {
                            service,
                            uri: "/health",
                            get_response: move || {
                                if let Some(expected) = allowed_identity {
                                    let current =
                                        jet_identity_updater.blocking_lock().get_identity();
                                    if expected != current {
                                        return ready((
                                            StatusCode::SERVICE_UNAVAILABLE,
                                            "identity mismatch".to_owned(),
                                        ));
                                    }
                                }

                                // TODO: need to check TPUs for processed?
                                match crate::metrics::jet::get_health_status() {
                                    Ok(()) => ready((StatusCode::OK, "ok".to_owned())),
                                    Err(error) => {
                                        ready((StatusCode::SERVICE_UNAVAILABLE, error.to_string()))
                                    }
                                }
                            },
                        }
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
                                jet_identity_updater,
                                cluster_tpu_info,
                            }
                            .into_rpc(),
                        )
                    })
            }
            RpcServerType::SolanaLike { tx_handler } => {
                use rpc_solana_like::RpcServer;

                let rpc_server_impl = Self::create_solana_like_rpc_server_impl(tx_handler);
                let server_config = ServerConfigBuilder::default()
                    .max_request_body_size(MAX_REQUEST_BODY_SIZE)
                    .build();
                ServerBuilder::new()
                    .set_config(server_config)
                    .build(addr)
                    .await
                    .map(|server| server.start(rpc_server_impl.into_rpc()))
            }
        }
        .with_context(|| format!("Failed to start HTTP server at {addr}"))
        .expect("Failed to start HTTP server");
        info!("started RPC {server_type_str} server on {addr}");

        Self {
            server_handle: Arc::new(StdMutex::new(Some(server_handle))),
        }
    }

    pub const fn create_solana_like_rpc_server_impl(
        tx_handler: TransactionHandler,
    ) -> rpc_solana_like::RpcServerImpl {
        rpc_solana_like::RpcServerImpl { tx_handler }
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
        crate::cluster_tpu_info::ClusterTpuInfoProvider,
        jsonrpsee::{
            core::{RpcResult, async_trait},
            proc_macros::rpc,
        },
        solana_keypair::{Keypair, read_keypair_file},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        std::sync::Arc,
        tokio::sync::Mutex,
        tracing::info,
    };

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
        async fn update_identity(&mut self, identity: Keypair);

        fn get_identity(&self) -> Pubkey;
    }

    pub struct RpcServerImpl {
        // pub quic: QuicTxSender,
        pub allowed_identity: Option<Pubkey>,
        pub jet_identity_updater: Arc<Mutex<Box<dyn JetIdentityUpdater + Send + 'static>>>,
        pub cluster_tpu_info: Arc<dyn ClusterTpuInfoProvider>,
    }

    #[async_trait]
    impl RpcServer for RpcServerImpl {
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
            let keypair = Keypair::try_from(identity_keypair.as_slice()).map_err(|err| {
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
            payload::JetRpcSendTransactionConfig, rpc::invalid_params,
            solana::decode_and_deserialize, transaction_handler::TransactionHandler,
        },
        jsonrpsee::{
            core::{RpcResult, async_trait},
            proc_macros::rpc,
        },
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

            let (_, transaction) = decode_and_deserialize(
                data,
                encoding
                    .into_binary_encoding()
                    .ok_or_else(|| invalid_params("unsupported encoding"))?,
            )?;

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
