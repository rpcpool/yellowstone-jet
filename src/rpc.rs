use {
    crate::{
        quic_solana::ConnectionCacheIdentity, solana::sanitize_transaction_support_check,
        transactions::SendTransactionsPool,
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
            } => {
                use rpc_solana_like::RpcServer;

                let rpc_server_impl = Self::create_solana_like_rpc_server_impl(
                    sts,
                    rpc,
                    proxy_sanitize_check,
                    proxy_preflight_check,
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
    ) -> anyhow::Result<rpc_solana_like::RpcServerImpl> {
        let rpc = Arc::new(SolanaRpcClient::new(rpc));
        let sanitize_supported = sanitize_transaction_support_check(&rpc).await?;

        Ok(rpc_solana_like::RpcServerImpl {
            sts,
            rpc,
            proxy_sanitize_check: proxy_sanitize_check && sanitize_supported,
            proxy_preflight_check,
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
        super::invalid_params,
        crate::{
            solana::decode_and_deserialize,
            transactions::{SendTransactionRequest, SendTransactionsPool},
        },
        jsonrpsee::{
            core::{async_trait, RpcResult},
            proc_macros::rpc,
            types::error::{
                ErrorObject, ErrorObjectOwned, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG,
            },
        },
        solana_client::{
            client_error::{ClientError, ClientErrorKind},
            nonblocking::rpc_client::RpcClient as SolanaRpcClient,
            rpc_config::{RcpSanitizeTransactionConfig, RpcSimulateTransactionConfig},
            rpc_request::{RpcError, RpcResponseErrorData},
            rpc_response::{Response as RpcResponse, RpcSimulateTransactionResult},
        },
        solana_rpc_client_api::{
            config::RpcSendTransactionConfig,
            custom_error::{
                NodeUnhealthyErrorData, JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
            },
            response::RpcVersionInfo,
        },
        solana_sdk::{
            commitment_config::CommitmentConfig,
            transaction::{TransactionError, VersionedTransaction},
        },
        solana_transaction_status::UiTransactionEncoding,
        std::sync::Arc,
        tracing::{debug, warn},
    };

    #[rpc(server, client)]
    pub trait Rpc {
        #[method(name = "getVersion")]
        fn get_version(&self) -> RpcResult<RpcVersionInfo>;

        #[method(name = "sendTransaction")]
        async fn send_transaction(
            &self,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> RpcResult<String>;
    }

    #[derive(Clone)]
    pub struct RpcServerImpl {
        pub sts: SendTransactionsPool,
        pub rpc: Arc<SolanaRpcClient>,
        pub proxy_sanitize_check: bool,
        pub proxy_preflight_check: bool,
    }

    #[async_trait]
    impl RpcServer for RpcServerImpl {
        fn get_version(&self) -> RpcResult<RpcVersionInfo> {
            debug!("get_version rpc request received");
            let version = solana_version::Version::default();
            Ok(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            })
        }

        async fn send_transaction(
            &self,
            data: String,
            config: Option<RpcSendTransactionConfig>,
        ) -> RpcResult<String> {
            debug!("send_transaction rpc request received");
            let RpcSendTransactionConfig {
                skip_preflight,
                skip_sanitize,
                preflight_commitment,
                encoding,
                max_retries,
                min_context_slot,
            } = config.unwrap_or_default();
            let tx_encoding = encoding.unwrap_or(UiTransactionEncoding::Base58);
            let binary_encoding = tx_encoding.into_binary_encoding().ok_or_else(|| {
                invalid_params(format!(
                    "unsupported encoding: {tx_encoding}. Supported encodings: base58, base64"
                ))
            })?;
            let (wire_transaction, unsanitized_tx) =
                decode_and_deserialize::<VersionedTransaction>(data, binary_encoding)?;

            if !skip_preflight {
                if self.proxy_preflight_check {
                    match self
                        .rpc
                        .simulate_transaction_with_config(
                            &unsanitized_tx,
                            RpcSimulateTransactionConfig {
                                sig_verify: true,
                                commitment: preflight_commitment
                                    .map(|commitment| CommitmentConfig { commitment }),
                                min_context_slot,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(RpcResponse {
                            context: _,
                            value:
                                RpcSimulateTransactionResult {
                                    err,
                                    logs,
                                    units_consumed,
                                    return_data,
                                    ..
                                },
                        }) => {
                            if let Some(error) = err {
                                return Err(ErrorObject::owned(
                                    JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                                    format!("Transaction simulation failed: {error}"),
                                    Some(RpcSimulateTransactionResult {
                                        err: Some(error),
                                        logs,
                                        accounts: None,
                                        units_consumed,
                                        return_data,
                                        inner_instructions: None,
                                        replacement_blockhash: None,
                                    }),
                                ));
                            }
                        }
                        Err(error) => {
                            return Err(unwrap_client_error("simulate_transaction", error));
                        }
                    }
                } else {
                    warn!("received TX with preflight check");
                    return Err(invalid_params("proxy_preflight_check option is not active"));
                }
            } else if !skip_sanitize && self.proxy_sanitize_check {
                if let Err(error) = self
                    .rpc
                    .sanitize_transaction(
                        &unsanitized_tx,
                        RcpSanitizeTransactionConfig {
                            sig_verify: true,
                            commitment: preflight_commitment
                                .map(|commitment| CommitmentConfig { commitment }),
                            min_context_slot,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    return Err(unwrap_client_error("sanitize_transaction", error));
                }
            } else {
                // We can not sanitize transaction because we need access to Bank, so we use `VersionedTransaction::sanitize`
                // let preflight_commitment =
                //     preflight_commitment.map(|commitment| CommitmentConfig { commitment });
                // let preflight_bank = &*meta.get_bank_with_config(RpcContextConfig {
                //     commitment: preflight_commitment,
                //     min_context_slot,
                // })?;
                // let transaction = sanitize_transaction(unsanitized_tx, preflight_bank)?;
                // let signature = *transaction.signature();
                unsanitized_tx.sanitize().map_err(|error| {
                    let error: TransactionError = error.into();
                    invalid_params(format!("invalid transaction: {error}"))
                })?;
            }

            let transaction = unsanitized_tx;
            let signature = transaction.signatures[0];

            if let Err(error) = self.sts.send_transaction(SendTransactionRequest {
                signature,
                transaction,
                wire_transaction,
                max_retries,
            }) {
                return Err(ErrorObject::owned(
                    INTERNAL_ERROR_CODE,
                    INTERNAL_ERROR_MSG,
                    Some(format!("{error}")),
                ));
            }

            Ok(signature.to_string())
        }
    }

    fn unwrap_client_error(call: &str, error: ClientError) -> ErrorObjectOwned {
        if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
            code,
            message,
            data,
        }) = error.kind
        {
            ErrorObject::owned(
                code as i32,
                message,
                match data {
                    RpcResponseErrorData::Empty => None,
                    RpcResponseErrorData::SendTransactionPreflightFailure(_) => {
                        unreachable!("impossible response")
                    }
                    RpcResponseErrorData::NodeUnhealthy { num_slots_behind } => {
                        Some(NodeUnhealthyErrorData { num_slots_behind })
                    }
                },
            )
        } else {
            warn!("{call} failed: {error}");
            ErrorObject::owned::<()>(INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, None)
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
