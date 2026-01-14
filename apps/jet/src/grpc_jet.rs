//! Jet GRPC implementation that handles transaction routing between:
//! cascade-router -> jet-gateway -> jet
//!
//! The system supports two payload formats:
//! 1. Legacy format: Simple binary serialized transaction
//! 2. New format: Structured TransactionWrapper with config and metadata
//!
//! This dual-format support ensures backward compatibility while allowing
//! new features through the structured format.
use {
    crate::{
        config::ConfigJetGatewayClient,
        feature_flags::FeatureSet,
        metrics::{
            self,
            jet::{increment_send_transaction_error, increment_send_transaction_success},
        },
        payload::{TransactionDecoder, TransactionPayload},
        proto::jet::{
            AnswerChallengeRequest, AnswerChallengeResponse, AuthRequest, FeatureFlags,
            GetChallengeRequest, InitialSubscribeRequest, Ping, Pong, SubscribeRequest,
            SubscribeResponse, SubscribeTransaction, SubscribeUpdateLimit, auth_request,
            auth_response, jet_gateway_client::JetGatewayClient,
            subscribe_request::Message as SubscribeRequestMessage,
            subscribe_response::Message as SubscribeResponseMessage,
            subscribe_transaction::Payload,
        },
        pubkey_challenger::{OneTimeAuthToken, append_nonce_and_sign},
        rpc::rpc_solana_like::RpcServerImpl as RpcServerImplSolanaLike,
        stake::StakeInfoMap,
        util::{IncrementalBackoff, create_x_token_interceptor, ms_since_epoch},
        version::VERSION,
    },
    anyhow::Context,
    futures::{
        future::FutureExt,
        sink::{Sink, SinkExt},
        stream::{Stream, StreamExt},
    },
    solana_signer::Signer,
    std::sync::Arc,
    tokio::{
        task::JoinSet,
        time::{Duration, interval},
    },
    tonic::{
        Request, Status,
        transport::channel::{ClientTlsConfig, Endpoint},
    },
    tracing::{debug, error, info},
    uuid::Uuid,
};

pub const DEFAULT_LOCK_KEY: &str = "jet-gateway";

const X_ONE_TIME_AUTH_TOKEN: &str = "x-one-time-auth-token";
const X_JET_VERSION: &str = "x-jet-version";

#[derive(Debug, thiserror::Error)]
pub enum TransactionHandlerError {
    #[error("failed to parse transaction payload: {0}")]
    PayloadParseError(String),

    #[error("failed to decode transaction: {0}")]
    DecodeError(String),

    #[error("failed to send transaction: {0}")]
    SendError(String),
}

pub struct GrpcTransactionHandler {
    tx_sender: RpcServerImplSolanaLike,
}

impl GrpcTransactionHandler {
    pub const fn new(tx_sender: RpcServerImplSolanaLike) -> Self {
        Self { tx_sender }
    }

    /// Processes incoming transactions in either legacy or new format.
    /// - For legacy format: Directly deserializes and sends the transaction
    /// - For new format: Extracts config and metadata before sending
    pub async fn handle_transaction(
        &self,
        transaction: SubscribeTransaction,
    ) -> Result<(), TransactionHandlerError> {
        let payload = TransactionPayload::try_from(transaction)
            .map_err(|e| TransactionHandlerError::PayloadParseError(e.to_string()))?;

        let (transaction, config_option) = TransactionDecoder::decode(&payload)
            .map_err(|e| TransactionHandlerError::DecodeError(e.to_string()))?;

        let config = config_option.unwrap_or_default();

        self.tx_sender
            .handle_internal_transaction(transaction, config)
            .await
            .map_err(|e| TransactionHandlerError::SendError(e.to_string()))?;

        Ok(())
    }
}

pub struct GrpcServer {}

async fn get_jet_gw_subscribe_auth_token(
    signer: ArcSigner,
    endpoint: String,
    x_token: Option<String>,
) -> anyhow::Result<OneTimeAuthToken> {
    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(1))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let interceptor = create_x_token_interceptor(x_token);
    let mut client = JetGatewayClient::with_interceptor(channel, interceptor);

    let auth_req = AuthRequest {
        auth_step: Some(auth_request::AuthStep::BeginAuth(GetChallengeRequest {
            pubkey_to_verify: signer.pubkey().to_bytes().to_vec(),
        })),
    };
    let resp = client.auth(auth_req).await?.into_inner();

    let answer = match resp.auth_step {
        Some(auth_response::AuthStep::BeginAuth(challenge_resp)) => {
            let challenge = &challenge_resp.challenge;
            let nonce = Uuid::new_v4().as_bytes().to_vec();
            let signed_challenge = append_nonce_and_sign(&signer, challenge, &nonce);
            AuthRequest {
                auth_step: Some(auth_request::AuthStep::CompleteAuth(
                    AnswerChallengeRequest {
                        challenge: challenge_resp.challenge,
                        signature: bincode::serialize(&signed_challenge)
                            .expect("failed to serialize signed challenge"),
                        pubkey_to_verify: signer.pubkey().to_bytes().to_vec(),
                        nonce,
                    },
                )),
            }
        }
        _ => return Err(anyhow::anyhow!("unexpected response")),
    };

    let resp = client.auth(answer).await?.into_inner().auth_step;

    match resp {
        Some(auth_response::AuthStep::CompleteAuth(AnswerChallengeResponse {
            success,
            one_time_auth_token,
        })) => {
            if success {
                let otak = bs58::decode(one_time_auth_token)
                    .into_vec()
                    .expect("failed to decode one-time-auth-token");
                let otak = bincode::deserialize::<OneTimeAuthToken>(&otak)
                    .expect("unexpected one-time-auth-token format");
                Ok(otak)
            } else {
                Err(anyhow::anyhow!("failed to authenticate"))
            }
        }
        result => Err(anyhow::anyhow!("unexpected response: {result:?}")),
    }
}

/// Establish a bidirectional streaming connection to the jet-gateway
///
/// Protocol initialization sequence:
/// 1. For new servers (v2+):
///    - First message: Init with feature flags
///    - Second message: UpdateLimit
///      If Init fails, fallback to legacy protocol
///
/// 2. For legacy servers (v1):
///    - Only send UpdateLimit message
///
/// This approach ensures backward compatibility while enabling
/// new features when supported by the server.
pub async fn grpc_subscribe_jet_gw(
    signer: ArcSigner,
    endpoint: String,
    x_token: Option<String>,
    stream_buffer_size: usize,
    features: FeatureSet,
) -> anyhow::Result<(
    impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
    impl Stream<Item = Result<SubscribeResponse, Status>>,
)> {
    // First get the OTAK authentication token
    let otak =
        get_jet_gw_subscribe_auth_token(Arc::clone(&signer), endpoint.clone(), x_token.clone())
            .await?;

    // Set up communication channels
    let (subscribe_tx, subscribe_rx) = futures::channel::mpsc::channel(stream_buffer_size);
    let (init_tx, init_rx) = futures::channel::mpsc::channel(2);

    // Establish GRPC connection
    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(1))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let interceptor = create_x_token_interceptor(x_token);
    let mut client = JetGatewayClient::with_interceptor(channel, interceptor);

    // Set up authenticated connection
    let mut subscribe_req = Request::new(init_rx);
    let ser_otak = bincode::serialize(&otak).expect("failed to serialize one-time-auth-token");
    let bs58_otak = bs58::encode(ser_otak).into_string();
    subscribe_req.metadata_mut().insert(
        X_ONE_TIME_AUTH_TOKEN,
        bs58_otak
            .try_into()
            .expect("failed to convert to AsciiMetadataValue"),
    );

    let version = serde_json::to_string(&VERSION)?;

    subscribe_req.metadata_mut().insert(
        X_JET_VERSION,
        version
            .try_into()
            .expect("failed to convert to AsciiMetadataValue"),
    );

    let stream = match client.subscribe(subscribe_req).await {
        Ok(resp) => resp.into_inner(),
        Err(status) => {
            return Err(anyhow::anyhow!(
                "Failed to establish subscription: {status}"
            ));
        }
    };

    // Try to initialize as v2 client if features are enabled
    let mut is_legacy_server = false;
    if !features.is_empty() {
        debug!(
            "Attempting v2 protocol - sending feature flags: {:?}",
            features.enabled_features()
        );
        let init_request = SubscribeRequest {
            message: Some(SubscribeRequestMessage::Init(InitialSubscribeRequest {
                features: Some(FeatureFlags {
                    supported_features: features.enabled_features(),
                }),
            })),
        };

        if let Err(e) = init_tx.clone().send(init_request).await {
            debug!("Server rejected v2 protocol: {}. Falling back to v1", e);
            is_legacy_server = true;
        }
    } else {
        is_legacy_server = true;
    }

    // Always send rate limit update (works for both v1 and v2)
    let limit_message = SubscribeRequest {
        message: Some(SubscribeRequestMessage::UpdateLimit(SubscribeUpdateLimit {
            messages_per100ms: 100,
        })),
    };

    if let Err(e) = init_tx.clone().send(limit_message).await {
        return Err(anyhow::anyhow!("Failed to send rate limit message: {e}"));
    }

    // Forward remaining messages
    tokio::spawn(async move {
        let mut subscribe_rx: futures::channel::mpsc::Receiver<SubscribeRequest> = subscribe_rx;
        let mut init_tx = init_tx;

        while let Some(msg) = subscribe_rx.next().await {
            if msg.message.is_some() {
                if init_tx.send(msg).await.is_err() {
                    break;
                }
            } else {
                debug!("Dropping empty message - not sending to gateway");
            }
        }
    });

    if is_legacy_server {
        debug!("Connected using legacy v1 protocol");
    } else {
        debug!("Connected using v2 protocol with feature flags");
    }

    Ok((subscribe_tx, stream))
}

type ArcSigner = Arc<dyn Signer + Send + Sync + 'static>;

impl GrpcServer {
    pub async fn run_with(
        signer: ArcSigner,
        stake_info: StakeInfoMap,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
        features: FeatureSet,
    ) {
        Self::grpc_subscribe(signer, stake_info, config, tx_sender, features).await
    }

    async fn grpc_subscribe(
        signer: ArcSigner,
        stake_info: StakeInfoMap,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
        features: FeatureSet,
    ) {
        const STREAM_BUFFER_SIZE: usize = 10;
        const MAX_SEND_TRANSACTIONS: usize = 10;
        const LIMIT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
        const MAX_QUICK_DISCONNECTS: usize = 3;

        let mut backoff = IncrementalBackoff::default();
        let mut tasks = JoinSet::<anyhow::Result<()>>::new();
        let mut quick_disconnects = 0;
        let mut last_connect_time = std::time::Instant::now();
        let mut consecutive_failed_connects = 0;
        let max_resubscribe_attempts = config
            .maximum_subscribe_attempts
            .map(|v| v.get())
            .unwrap_or(usize::MAX - 1);
        loop {
            backoff.maybe_tick().await;

            // Reset quick disconnect counter if we've been connected for a while
            if last_connect_time.elapsed() > Duration::from_secs(5) {
                quick_disconnects = 0;
            }

            let (mut sink, mut stream) = match Self::grpc_connect(
                Arc::clone(&signer),
                &config.endpoints,
                config.x_token.as_ref(),
                STREAM_BUFFER_SIZE,
                features.clone(),
            )
            .await
            {
                Ok((sink, stream)) => {
                    backoff.reset();
                    last_connect_time = std::time::Instant::now();
                    consecutive_failed_connects = 0;
                    (sink, stream)
                }
                Err(error) => {
                    error!(?error, "failed to connect to gRPC jet-gateway");
                    consecutive_failed_connects += 1;
                    if consecutive_failed_connects >= max_resubscribe_attempts {
                        panic!("Too many consecutive failed connects. Exiting...");
                    }
                    // If error mentions feature flags, exit completely
                    if error.to_string().contains("features")
                        || error.to_string().contains("Feature")
                    {
                        error!(
                            "Fatal error - feature flags not supported by gateway. Please remove them from config.yml"
                        );
                        // Wait a bit before exiting to allow log to flush
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        panic!("Exiting due to feature flag incompatibility");
                    }

                    backoff.init();
                    continue;
                }
            };

            let mut limit_interval = interval(LIMIT_UPDATE_INTERVAL);

            let my_identity = signer.pubkey();

            let configured_max_messages_per100ms = config.max_streams;
            let loop_result = async {
                loop {
                    if let Err(error) = async {
                        tokio::select! {
                            _ = limit_interval.tick() => {
                                let limits = stake_info.get_stake_limits(my_identity);
                                let messages_per100ms = configured_max_messages_per100ms
                                    .map(|proposed| proposed.get())
                                    .filter(|proposed| proposed <= &limits.per100ms_limit)
                                    .unwrap_or(limits.per100ms_limit);
                                let message = SubscribeRequest {
                                    message: Some(SubscribeRequestMessage::UpdateLimit(SubscribeUpdateLimit { messages_per100ms }))
                                };
                                sink.send(message).await.context("failed to send limit value")
                            }
                            message = stream.next(), if tasks.len() < MAX_SEND_TRANSACTIONS => {
                                let message = message
                                    .ok_or(anyhow::anyhow!("stream finished"))?
                                    .context("failed to receive message")?
                                    .message
                                    .ok_or(anyhow::anyhow!("no message in response"))?;

                                match message {
                                    SubscribeResponseMessage::Ping(Ping { id }) => {
                                        let message = SubscribeRequest {
                                            message: Some(SubscribeRequestMessage::Pong(Pong { id })),
                                        };
                                        sink.send(message).await.context("failed to send pong response")
                                    },
                                    SubscribeResponseMessage::Pong(_) => Ok(()),
                                    SubscribeResponseMessage::Transaction(transaction) => {
                                        let timestamp = transaction.payload.as_ref().and_then(|p| {
                                            if let Payload::NewPayload(wrapper) = p {
                                                wrapper.timestamp
                                            } else {
                                                None
                                            }
                                        });

                                        let tx_sender = tx_sender.clone();
                                        tasks.spawn(async move {
                                            let handler = GrpcTransactionHandler::new(tx_sender);
                                            match handler.handle_transaction(transaction).await {
                                                Ok(_) => {
                                                    if let Some(gateway_timestamp) = timestamp {
                                                        let latency = ms_since_epoch().saturating_sub(gateway_timestamp);
                                                        metrics::jet::observe_forwarded_txn_latency(latency as f64);
                                                    }
                                                    increment_send_transaction_success();
                                                }
                                                Err(e) => {
                                                    increment_send_transaction_error();
                                                    error!(?e, "Failed to handle transaction");
                                                }
                                            }
                                            Ok(())
                                        });
                                        Ok(())
                                    }
                                }
                            }
                            Some(result) = tasks.join_next() => {
                                let result = result.expect("failed to join send_transaction task");
                                if result.is_err() {
                                    increment_send_transaction_error();
                                    error!(?result, "failed to send transaction");
                                } else {
                                    increment_send_transaction_success();
                                }
                                Ok(())
                            }
                        }
                    }.await {
                        error!(?error, "Error in gateway stream");
                        return error;
                    }
                }
            }.await;

            metrics::jet::gateway_set_disconnected(&config.endpoints);

            // If we get disconnected quickly too many times, there might be a protocol issue
            if last_connect_time.elapsed() < Duration::from_secs(2) {
                quick_disconnects += 1;
                if quick_disconnects >= MAX_QUICK_DISCONNECTS {
                    error!(
                        "Too many quick disconnections ({quick_disconnects}). Last error: {loop_result}"
                    );
                    error!(
                        "This may indicate a protocol mismatch or feature flag incompatibility."
                    );
                    error!("Waiting longer before reconnecting...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    quick_disconnects = 0;
                }
            }
        }
    }

    async fn grpc_connect(
        signer: ArcSigner,
        endpoints: &[String],
        x_token: Option<&String>,
        stream_buffer_size: usize,
        features: FeatureSet,
    ) -> anyhow::Result<(
        impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError> + use<>,
        impl Stream<Item = Result<SubscribeResponse, Status>> + use<>,
    )> {
        let x_token = x_token.cloned();

        let mut tasks = JoinSet::new();
        for endpoint in endpoints.iter().cloned() {
            let signer2 = Arc::clone(&signer);
            let features_clone = features.clone();
            tasks.spawn(
                grpc_subscribe_jet_gw(
                    signer2,
                    endpoint.clone(),
                    x_token.clone(),
                    stream_buffer_size,
                    features_clone,
                )
                .map(|result| (result, endpoint)),
            );
        }

        let mut last_err = None;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((Ok((sink, stream)), endpoint)) => {
                    info!(endpoint, "jet connected to gateway");
                    metrics::jet::gateway_set_connected(endpoints, endpoint);
                    return Ok((sink, stream));
                }
                Ok((Err(error), _endpoint)) => {
                    last_err = Some(error);
                    continue;
                }
                Err(error) => {
                    debug!(?error, "failed to join future with connecting to proxy");
                    last_err = Some(anyhow::anyhow!(error));
                    continue;
                }
            }
        }
        Err(last_err.expect("error should exist"))
    }
}
