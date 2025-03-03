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
            auth_request, auth_response, jet_gateway_client::JetGatewayClient,
            subscribe_request::Message as SubscribeRequestMessage,
            subscribe_response::Message as SubscribeResponseMessage,
            subscribe_transaction::Payload, AnswerChallengeRequest, AnswerChallengeResponse,
            AuthRequest, FeatureFlags, GetChallengeRequest, Ping, Pong, SubscribeRequest,
            SubscribeResponse, SubscribeTransaction, SubscribeUpdateLimit,
        },
        pubkey_challenger::{append_nonce_and_sign, OneTimeAuthToken},
        rpc::rpc_solana_like::RpcServerImpl as RpcServerImplSolanaLike,
        util::{ms_since_epoch, IncrementalBackoff},
    },
    anyhow::Context,
    futures::{
        future::FutureExt,
        sink::{Sink, SinkExt},
        stream::{Stream, StreamExt},
    },
    solana_sdk::signer::Signer,
    std::sync::Arc,
    tokio::{
        sync::oneshot,
        task::JoinSet,
        time::{interval, Duration},
    },
    tonic::{
        metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
        service::Interceptor,
        transport::channel::{ClientTlsConfig, Endpoint},
        Request, Response, Status, Streaming,
    },
    tracing::{debug, error, info},
    uuid::Uuid,
};

pub const DEFAULT_LOCK_KEY: &str = "jet-gateway";

const X_ONE_TIME_AUTH_TOKEN: &str = "x-one-time-auth-token";

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
    ) -> anyhow::Result<String> {
        let payload = TransactionPayload::try_from(transaction)
            .map_err(|e| anyhow::anyhow!("Failed to parse transaction payload: {}", e))?;

        let (transaction, config) = TransactionDecoder::decode(&payload)
            .map_err(|e| anyhow::anyhow!("Failed to decode transaction: {}", e))?;

        self.tx_sender
            .handle_internal_transaction(transaction, config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))
    }
}

pub struct GrpcServer {}

async fn get_jet_gw_subscribe_auth_token(
    signer: ArcSigner,
    endpoint: String,
    x_token: GrpcClientXToken,
) -> anyhow::Result<OneTimeAuthToken> {
    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(1))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    let mut client = JetGatewayClient::with_interceptor(channel, x_token);

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

pub async fn grpc_subscribe_jet_gw(
    signer: ArcSigner,
    endpoint: String,
    x_token: GrpcClientXToken,
    stream_buffer_size: usize,
    features: FeatureSet,
) -> anyhow::Result<(
    impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
    impl Stream<Item = Result<SubscribeResponse, Status>>,
)> {
    // First get the OTAK
    let otak =
        get_jet_gw_subscribe_auth_token(Arc::clone(&signer), endpoint.clone(), x_token.clone())
            .await?;

    let channel = Endpoint::from_shared(endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(1))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let mut client = JetGatewayClient::with_interceptor(channel, x_token);
    let (subscribe_tx, subscribe_rx) = futures::channel::mpsc::channel(stream_buffer_size);

    // Prepare OTAK authentication
    let mut subscribe_req = Request::new(subscribe_rx);
    let ser_otak = bincode::serialize(&otak).expect("failed to serialize one-time-auth-token");
    let bs58_otak = bs58::encode(ser_otak).into_string();
    subscribe_req.metadata_mut().insert(
        X_ONE_TIME_AUTH_TOKEN,
        bs58_otak
            .try_into()
            .expect("failed to convert to AsciiMetadataValue"),
    );

    // Establish authenticated connection
    let response: Response<Streaming<SubscribeResponse>> = client.subscribe(subscribe_req).await?;
    let stream = response.into_inner();

    // SAFE APPROACH: Send an update limit message first
    // This is guaranteed to work with older gateways and avoids the ping check
    let limit_msg = SubscribeRequest {
        message: Some(SubscribeRequestMessage::UpdateLimit(SubscribeUpdateLimit {
            messages_per100ms: 10, // Safe default value
        })),
    };

    // Send the limit update first to ensure connection is stable
    let mut subscribe_tx_clone = subscribe_tx.clone();
    if let Err(e) = subscribe_tx_clone.send(limit_msg).await {
        debug!("Failed to send initial limit update: {}", e);
        return Ok((subscribe_tx, stream));
    }

    // Wait a bit to ensure the connection is stable
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now try to send feature flags
    // We do this in a fire-and-forget way since errors shouldn't break the connection
    if !features.enabled_features().is_empty() {
        tokio::spawn({
            let endpoint = endpoint.clone();
            let mut subscribe_tx = subscribe_tx.clone();
            let features = features.clone();
            async move {
                // Send feature flags in a separate task to avoid blocking
                let feature_request = SubscribeRequest {
                    message: Some(SubscribeRequestMessage::Features(FeatureFlags {
                        supported_features: features.enabled_features(),
                    })),
                };

                // If this fails, it's likely because the gateway doesn't support feature flags
                if let Err(e) = subscribe_tx.send(feature_request).await {
                    debug!("Failed to send feature flags (likely older gateway): {}", e);
                    // Track metrics for unsupported features
                    for feature in features.enabled_features() {
                        metrics::jet::increment_gateway_version_mismatch(&endpoint, &feature);
                    }
                }
            }
        });
    }

    Ok((subscribe_tx, stream))
}

type ArcSigner = Arc<dyn Signer + Send + Sync + 'static>;

impl GrpcServer {
    pub async fn run_with(
        signer: ArcSigner,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
        features: FeatureSet,
        mut stop_rx: oneshot::Receiver<()>,
    ) {
        tokio::select! {
            () = Self::grpc_subscribe(signer, config, tx_sender, features) => {},
            _ = &mut stop_rx => {},
        }
    }

    async fn grpc_subscribe(
        signer: ArcSigner,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
        features: FeatureSet,
    ) {
        const STREAM_BUFFER_SIZE: usize = 10;
        const MAX_SEND_TRANSACTIONS: usize = 10;
        const LIMIT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

        let mut backoff = IncrementalBackoff::default();
        let mut tasks = JoinSet::<anyhow::Result<()>>::new();

        loop {
            backoff.maybe_tick().await;

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
                    (sink, stream)
                }
                Err(error) => {
                    error!(?error, "failed to connect to gRPC jet-gateway");
                    backoff.init();
                    continue;
                }
            };

            let mut limit_interval = interval(LIMIT_UPDATE_INTERVAL);

            loop {
                if let Err(error) = async {
                    tokio::select! {
                        _ = limit_interval.tick() => {
                            let messages_per100ms = config
                                .max_streams
                                .unwrap_or_else(metrics::jet::cluster_identity_stake_get_max_streams);
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
                    error!(?error);
                    break;
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
        impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
        impl Stream<Item = Result<SubscribeResponse, Status>>,
    )> {
        let x_token = GrpcClientXToken::new(x_token)?;

        let mut tasks = JoinSet::new();
        for endpoint in endpoints.iter().cloned() {
            let signer2 = Arc::clone(&signer);
            let features = features.clone();
            tasks.spawn(
                grpc_subscribe_jet_gw(
                    signer2,
                    endpoint.clone(),
                    x_token.clone(),
                    stream_buffer_size,
                    features,
                )
                .map(|result| (result, endpoint)),
            );
        }

        let mut last_err = None;
        loop {
            match tasks.join_next().await {
                Some(Ok((Ok((sink, stream)), endpoint))) => {
                    info!(endpoint, "jet connected to gateway");
                    metrics::jet::gateway_set_connected(endpoints, endpoint);
                    return Ok((sink, stream));
                }
                Some(Ok((Err(error), endpoint))) => {
                    debug!(endpoint, ?error, "failed to connect");
                    last_err = Some(error);
                    continue;
                }
                Some(Err(error)) => {
                    debug!(?error, "failed to join future with connecting to proxy");
                    last_err = Some(anyhow::anyhow!(error));
                    continue;
                }
                None => return Err(last_err.expect("error should exists")),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct GrpcClientXToken {
    x_token: Option<AsciiMetadataValue>,
}

impl Interceptor for GrpcClientXToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", x_token);
        }
        Ok(request)
    }
}

impl GrpcClientXToken {
    pub fn new<T>(x_token: Option<T>) -> anyhow::Result<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        Ok(Self {
            x_token: x_token.map(|x_token| x_token.try_into()).transpose()?,
        })
    }

    pub const fn none() -> Self {
        Self { x_token: None }
    }
}
