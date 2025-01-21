use {
    crate::{
        config::ConfigJetGatewayClient,
        metrics,
        proto::jet::{
            auth_request, auth_response, jet_gateway_client::JetGatewayClient,
            subscribe_request::Message as SubscribeRequestMessage,
            subscribe_response::Message as SubscribeResponseMessage, AnswerChallengeRequest,
            AnswerChallengeResponse, AuthRequest, GetChallengeRequest, Ping, Pong,
            SubscribeRequest, SubscribeResponse, SubscribeTransaction, SubscribeUpdateLimit,
        },
        pubkey_challenger::{append_nonce_and_sign, OneTimeAuthToken},
        rpc::rpc_solana_like::{RpcServer as _, RpcServerImpl as RpcServerImplSolanaLike},
        util::{now_ms, IncrementalBackoff},
    },
    anyhow::Context,
    base64::{prelude::BASE64_STANDARD, Engine},
    futures::{
        future::FutureExt,
        sink::{Sink, SinkExt},
        stream::{Stream, StreamExt},
    },
    serde::{Deserialize, Serialize},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::{signer::Signer, transaction::VersionedTransaction},
    solana_transaction_status::UiTransactionEncoding,
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

/// Represents a transaction payload for gRPC/Protobuf communication in the Jet ecosystem.
/// Used for transaction routing between cascade-router -> jet-gateway -> jet instances.
/// The payload is serialized to JSON when transmitted via Protobuf.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Payload {
    pub transaction: String, // encoded transaction as Json, JsonParsed, Base64 and Base58 (default)
    pub config: RpcSendTransactionConfig, // solana defaults configuration
    pub timestamp: Option<u64>,
}

impl Payload {
    /// Solana have the default encoding as Base58, but we prefer using Base64 when
    /// we create our own Payloads, because it's way faster.
    /// Supported encoding methods: (Base64 and Base58)
    pub fn new(
        transaction: &VersionedTransaction,
        mut config: RpcSendTransactionConfig,
    ) -> anyhow::Result<Self> {
        let encoding = match config.encoding {
            Some(UiTransactionEncoding::Base58) => UiTransactionEncoding::Base58,
            Some(UiTransactionEncoding::Base64) => UiTransactionEncoding::Base64,
            None => UiTransactionEncoding::Base64,
            _ => {
                return Err(anyhow::anyhow!(
                    "Only Base58 and Base64 encodings are supported"
                ))
            }
        };

        let serialized_transaction = bincode::serialize(transaction)?;
        let encoded_transaction = match encoding {
            UiTransactionEncoding::Base58 => bs58::encode(serialized_transaction).into_string(),
            _ => BASE64_STANDARD.encode(serialized_transaction),
        };

        config.encoding = Some(encoding);
        Ok(Self {
            transaction: encoded_transaction,
            config,
            timestamp: None,
        })
    }

    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(payload).map_err(Into::into)
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
) -> anyhow::Result<(
    impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
    impl Stream<Item = Result<SubscribeResponse, Status>>,
)> {
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

    let mut subscribe_req = Request::new(subscribe_rx);

    let ser_otak = bincode::serialize(&otak).expect("failed to serialize one-time-auth-token");
    let bs58_otak = bs58::encode(ser_otak).into_string();
    subscribe_req.metadata_mut().insert(
        X_ONE_TIME_AUTH_TOKEN,
        bs58_otak
            .try_into()
            .expect("failed to convert to AsciiMetadataValue"),
    );

    // Send the OTAK first to authenticate
    let response: Response<Streaming<SubscribeResponse>> = client.subscribe(subscribe_req).await?;

    Ok((subscribe_tx, response.into_inner()))
}

type ArcSigner = Arc<dyn Signer + Send + Sync + 'static>;

impl GrpcServer {
    pub async fn run_with(
        signer: ArcSigner,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
        mut stop_rx: oneshot::Receiver<()>,
    ) {
        tokio::select! {
            () = Self::grpc_subscribe(signer, config, tx_sender) => {},
            _ = &mut stop_rx => {},
        }
    }

    async fn grpc_subscribe(
        signer: ArcSigner,
        config: ConfigJetGatewayClient,
        tx_sender: RpcServerImplSolanaLike,
    ) {
        const STREAM_BUFFER_SIZE: usize = 10;
        const MAX_SEND_TRANSACTIONS: usize = 10;
        const LIMIT_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

        // let identity = identity.to_bytes().to_vec();
        let mut backoff = IncrementalBackoff::default();
        let mut tasks = JoinSet::new();
        loop {
            backoff.maybe_tick().await;

            let (mut sink, mut stream) = match Self::grpc_connect(
                Arc::clone(&signer),
                &config.endpoints,
                config.x_token.as_ref(),
                STREAM_BUFFER_SIZE,
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

            let mut interval = interval(LIMIT_UPDATE_INTERVAL);
            loop {
                if let Err(error) = async {
                    let next_message = if tasks.len() > MAX_SEND_TRANSACTIONS {
                        futures::future::pending().boxed()
                    } else {
                        stream.next().boxed()
                    };
                    let next_task = if tasks.is_empty() {
                        futures::future::pending().boxed()
                    } else {
                        tasks.join_next().boxed()
                    };

                    tokio::select! {
                        _ = interval.tick() => {
                            let messages_per100ms = config
                                .max_streams
                                .unwrap_or_else(metrics::jet::cluster_identity_stake_get_max_streams);
                            let message = SubscribeRequest {
                                message: Some(SubscribeRequestMessage::UpdateLimit(SubscribeUpdateLimit { messages_per100ms }))
                            };
                            sink.send(message).await.context("failed to send limit value")
                        }
                        message = next_message => {
                            match message
                                .ok_or(anyhow::anyhow!("stream finished"))?
                                .context("failed to receive message")?
                                .message
                                .ok_or(anyhow::anyhow!("no message in response"))? {
                                    SubscribeResponseMessage::Ping(Ping { id }) => {
                                        let message = SubscribeRequest {
                                            message: Some(SubscribeRequestMessage::Pong(Pong { id })),
                                        };
                                        sink.send(message).await.context("failed to send pong response")
                                    },
                                    SubscribeResponseMessage::Pong(Pong { id: _ }) => Ok(()),
                                    SubscribeResponseMessage::Transaction(SubscribeTransaction { payload }) => {
                                        let Payload {
                                            transaction,
                                            config,
                                            timestamp,
                                        } = Payload::decode(&payload).context("failed to decode message")?;
                                        // Calculate latency if timestamp exists
                                        if let Some(gateway_timestamp) = timestamp {
                                            let now = now_ms();
                                            // Calculate latency in milliseconds
                                            let latency = match now.checked_sub(gateway_timestamp) {
                                                Some(lat) => lat as f64,
                                                None => {
                                                    error!("Invalid timestamp calculation");
                                                    0.0
                                                }
                                            };

                                            // Record latency metric
                                            metrics::jet::observe_forwarded_txn_latency(latency);
                                        }
                                        let tx_sender = tx_sender.clone();
                                        tasks.spawn(async move {
                                            tx_sender.send_transaction(transaction, Some(config)).await
                                        });
                                        Ok(())
                                    },
                                }
                        }
                        joined_task = next_task => {
                            let send_tx_result = joined_task.unwrap().context("failed to join future with sending tx")?;
                            send_tx_result.context("failed to send transaction")?;
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
    ) -> anyhow::Result<(
        impl Sink<SubscribeRequest, Error = futures::channel::mpsc::SendError>,
        impl Stream<Item = Result<SubscribeResponse, Status>>,
    )> {
        let x_token = GrpcClientXToken::new(x_token)?;

        let mut tasks = JoinSet::new();
        for endpoint in endpoints.iter().cloned() {
            let signer2 = Arc::clone(&signer);
            tasks.spawn(
                grpc_subscribe_jet_gw(
                    signer2,
                    endpoint.clone(),
                    x_token.clone(),
                    stream_buffer_size,
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
