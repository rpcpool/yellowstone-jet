use {
    crate::{
        blocking_services::AllowTxSigner, quic_solana::ConnectionCacheIdentity,
        solana::sanitize_transaction_support_check, transactions::SendTransactionsPool,
    },
    anyhow::Context as _,
    futures::future::{ready, BoxFuture, FutureExt, TryFutureExt},
    hyper::{Request, Response, StatusCode},
    jsonrpsee::{
        core::http_helpers::Body,
        server::{ServerBuilder, ServerHandle},
        types::error::{ErrorObject, ErrorObjectOwned, INVALID_PARAMS_CODE},
    },
    solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient,
    solana_sdk::pubkey::Pubkey,
    std::{
        error::Error,
        fmt,
        future::Future,
        net::SocketAddr,
        sync::{Arc, Mutex as StdMutex},
        task::{Context, Poll},
        time::Instant,
    },
    tower::Service,
    tracing::{debug, info},
};

// should be more than enough for `sendTransaction` request
const MAX_REQUEST_BODY_SIZE: u32 = 32 * (1 << 10); // 32kB

pub enum RpcServerType {
    Admin {
        quic_identity_man: ConnectionCacheIdentity,
        allowed_identity: Option<Pubkey>,
    },

    SolanaLike {
        stp: SendTransactionsPool,
        rpc: String,
        proxy_sanitize_check: bool,
        proxy_preflight_check: bool,
        policies: Arc<dyn AllowTxSigner + Send + Sync + 'static>,
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
                quic_identity_man,
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
                                quic_identity_man,
                            }
                            .into_rpc(),
                        )
                    })
            }
            RpcServerType::SolanaLike {
                stp: sts,
                rpc,
                proxy_sanitize_check,
                proxy_preflight_check,
                policies,
            } => {
                use rpc_solana_like::RpcServer;

                let rpc_server_impl = Self::create_solana_like_rpc_server_impl(
                    sts,
                    rpc,
                    proxy_sanitize_check,
                    proxy_preflight_check,
                    policies,
                )
                .await?;

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

    pub async fn create_solana_like_rpc_server_impl(
        sts: SendTransactionsPool,
        rpc: String,
        proxy_sanitize_check: bool,
        proxy_preflight_check: bool,
        policies: Arc<dyn AllowTxSigner + Send + Sync + 'static>,
    ) -> anyhow::Result<rpc_solana_like::RpcServerImpl> {
        let rpc = Arc::new(SolanaRpcClient::new(rpc));
        let sanitize_supported = sanitize_transaction_support_check(&rpc).await?;

        Ok(rpc_solana_like::RpcServerImpl {
            sts,
            rpc,
            proxy_sanitize_check: proxy_sanitize_check && sanitize_supported,
            proxy_preflight_check,
            policies,
        })
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
        crate::quic_solana::ConnectionCacheIdentity,
        jsonrpsee::{
            core::{async_trait, RpcResult},
            proc_macros::rpc,
        },
        solana_sdk::{
            pubkey::Pubkey,
            signer::{
                keypair::{read_keypair_file, Keypair},
                Signer,
            },
        },
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
    }

    pub struct RpcServerImpl {
        // pub quic: QuicTxSender,
        pub allowed_identity: Option<Pubkey>,
        pub quic_identity_man: ConnectionCacheIdentity,
    }

    #[async_trait]
    impl RpcServer for RpcServerImpl {
        async fn get_identity(&self) -> RpcResult<String> {
            Ok(self.quic_identity_man.get_identity().await.to_string())
        }

        async fn set_identity(&self, keypair_file: String, require_tower: bool) -> RpcResult<()> {
            let keypair = read_keypair_file(&keypair_file).map_err(|err| {
                invalid_params(format!(
                    "Failed to read identity keypair from {keypair_file}: {err}"
                ))
            })?;
            self.set_keypair(keypair, require_tower).await
        }

        async fn set_identity_from_bytes(
            &self,
            identity_keypair: Vec<u8>,
            require_tower: bool,
        ) -> RpcResult<()> {
            let keypair = Keypair::from_bytes(&identity_keypair).map_err(|err| {
                invalid_params(format!(
                    "Failed to read identity keypair from provided byte array: {err}"
                ))
            })?;
            self.set_keypair(keypair, require_tower).await
        }

        async fn reset_identity(&self) -> RpcResult<()> {
            let random_identity = Keypair::new();
            self.quic_identity_man
                .update_keypair(&random_identity)
                .await;
            Ok(())
        }
    }

    impl RpcServerImpl {
        async fn set_keypair(&self, identity: Keypair, require_tower: bool) -> RpcResult<()> {
            if require_tower {
                return Err(invalid_params(
                    "`require_tower: true` is not supported at the moment".to_owned(),
                ));
            }

            if let Some(allow_ident) = &self.allowed_identity {
                if allow_ident != &identity.pubkey() {
                    return Err(invalid_params("invalid identity".to_owned()));
                }
            }

            self.quic_identity_man.update_keypair(&identity).await;
            info!("update identity: {}", identity.pubkey());

            Ok(())
        }
    }
}

pub mod rpc_solana_like {
    use {
        crate::{
            blocking_services::AllowTxSigner, metrics,
            payload::RpcSendTransactionConfigWithBlockList, rpc::invalid_params,
            solana::decode_and_deserialize, transaction_handler::TransactionHandler,
            transactions::SendTransactionsPool,
        },
        jsonrpsee::{
            core::{async_trait, RpcResult},
            proc_macros::rpc,
            types::{error::INVALID_REQUEST_CODE, ErrorObject},
        },
        solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient,
        solana_rpc_client_api::response::RpcVersionInfo,
        solana_sdk::transaction::VersionedTransaction,
        solana_transaction_status::UiTransactionEncoding,
        std::sync::Arc,
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
            config: Option<RpcSendTransactionConfigWithBlockList>,
        ) -> RpcResult<String>;
    }

    #[derive(Clone)]
    pub struct RpcServerImpl {
        pub sts: SendTransactionsPool,
        pub rpc: Arc<SolanaRpcClient>,
        pub proxy_sanitize_check: bool,
        pub proxy_preflight_check: bool,
        pub policies: Arc<dyn AllowTxSigner + Send + Sync + 'static>,
    }

    impl RpcServerImpl {
        // Internal method specifically for handling VersionedTransaction directly
        pub async fn handle_internal_transaction(
            &self,
            transaction: VersionedTransaction,
            config: RpcSendTransactionConfigWithBlockList,
        ) -> RpcResult<String /* Signature */> {
            debug!("handling internal versioned transaction");

            let handler = TransactionHandler::new(
                self.sts.clone(),
                &self.rpc,
                self.proxy_sanitize_check,
                self.proxy_preflight_check,
            );

            handler
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
            config_with_blocklist: Option<RpcSendTransactionConfigWithBlockList>,
        ) -> RpcResult<String /* Signature */> {
            debug!("send_transaction rpc request received");
            let config_with_blocklist = config_with_blocklist.unwrap_or_default();
            let config = config_with_blocklist.config.unwrap_or_default();

            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

            let (_, transaction) = decode_and_deserialize(
                data,
                encoding
                    .into_binary_encoding()
                    .ok_or_else(|| invalid_params("unsupported encoding"))?,
            )?;

            if self
                .policies
                .allow_tx_signer(&config_with_blocklist.blocklist_pdas, &transaction)
            {
                metrics::jet::increment_banned_transactions_total();

                return Err(ErrorObject::owned::<()>(
                    INVALID_REQUEST_CODE,
                    "Transaction signer is banned",
                    None,
                ));
            }

            self.handle_internal_transaction(transaction, config_with_blocklist)
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
