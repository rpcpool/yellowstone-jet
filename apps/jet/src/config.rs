use {
    crate::{feature_flags::FeatureSet, util::CommitmentLevel},
    anyhow::Context,
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    solana_keypair::{Keypair, read_keypair_file},
    solana_pubkey::Pubkey,
    std::{
        collections::HashSet,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        num::{NonZeroU64, NonZeroUsize},
        path::{Path, PathBuf},
        str::FromStr,
    },
    tokio::{fs, time::Duration},
    yellowstone_jet_tpu_client::{config::TpuSenderConfig, core::DEFAULT_LEADER_DURATION},
    yellowstone_shield_store::{
        PolicyStoreConfig, PolicyStoreGrpcConfig, PolicyStoreRpcConfig, ShieldStoreCommitmentLevel,
    },
};

pub const DEFAULT_TPU_CONNECTION_POOL_SIZE: usize = 1;

fn deser_pubkey_vec<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    let strings = Vec::<String>::deserialize(deserializer)?;
    strings
        .into_iter()
        .map(|s| s.parse().map_err(de::Error::custom))
        .collect()
}

pub async fn load_config<T>(path: impl AsRef<Path>) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let contents = fs::read(path)
        .await
        .with_context(|| "failed to read config")?;
    serde_yaml::from_slice(&contents).map_err(Into::into)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigJet {
    pub tracing: ConfigTracing,

    /// Identity options
    pub identity: ConfigIdentity,

    /// RPC & gRPC for upstream validator
    pub upstream: ConfigUpstream,

    /// jet-gateway endpoints
    pub jet_gateway: Option<ConfigJetGatewayClient>,

    /// Admin server listen options
    pub listen_admin: ConfigListenAdmin,

    /// Solana-like server listen options
    pub listen_solana_like: ConfigListenSolanaLike,

    /// Send retry options
    pub send_transaction_service: ConfigSendTransactionService,

    /// Quic config
    pub quic: ConfigQuic,

    /// Send events to Lewis
    pub lewis_events: Option<ConfigLewisEvents>,

    /// Features Flags
    #[serde(default)]
    pub features: FeatureSet,

    /// Prometheus Push Gateway
    pub prometheus: Option<PrometheusConfig>,

    /// Shield Program ID (Optional, default to yellowstone-shield-store default)
    #[serde(default, deserialize_with = "ConfigJet::deserialize_maybe_program_id")]
    pub program_id: Option<Pubkey>,
}

impl ConfigJet {
    fn deserialize_maybe_program_id<'de, D>(deserializer: D) -> Result<Option<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            Some(program_id_str) => Pubkey::from_str(&program_id_str)
                .map(Some)
                .map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigTracing {
    pub json: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigIdentity {
    ///
    /// Represents the expected validator identity.
    ///
    /// Do not send transactions if Quic identity doesn't match specified one
    #[serde(default, deserialize_with = "ConfigIdentity::deserialize_maybe_pubkey")]
    pub expected: Option<Pubkey>,
    /// Load specified keypair from file
    #[serde(
        default,
        deserialize_with = "ConfigIdentity::deserialize_maybe_keypair"
    )]
    pub keypair: Option<Keypair>,
}

impl ConfigIdentity {
    fn deserialize_maybe_pubkey<'de, D>(deserializer: D) -> Result<Option<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            Some(pubkey) => pubkey.parse().map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }

    fn deserialize_maybe_keypair<'de, D>(deserializer: D) -> Result<Option<Keypair>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<PathBuf>::deserialize(deserializer)? {
            Some(path) => read_keypair_file(path).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConfigUpstream {
    /// gRPC service
    /// The `primary_grpc` alias is used to maintain compatibility with previous versions.
    /// It is recommended to use `grpc` instead.
    #[serde(alias = "primary_grpc")]
    pub grpc: ConfigUpstreamGrpc,

    /// RPC endpoint
    #[serde(default = "ConfigUpstream::default_rpc")]
    pub rpc: String,

    ///
    /// RPC retry strategy
    /// This strategy will be used when `rpc` call failed due to transient error.
    #[serde(default = "ConfigUpstream::default_rpc_retry")]
    pub rpc_on_error: RpcErrorStrategy,

    /// Cluster nodes information update interval in milliseconds
    #[serde(
        default = "ConfigUpstream::default_cluster_nodes_update_interval",
        with = "humantime_serde"
    )]
    pub cluster_nodes_update_interval: Duration,

    /// Stake update interval
    #[serde(
        default = "ConfigUpstream::default_stake_update_interval",
        with = "humantime_serde"
    )]
    pub stake_update_interval: Duration,
}

impl ConfigUpstream {
    const fn default_rpc_retry() -> RpcErrorStrategy {
        RpcErrorStrategy::Fixed {
            interval: Duration::from_millis(100),
            retries: unsafe { NonZeroUsize::new_unchecked(3) },
        }
    }

    fn default_rpc() -> String {
        "http://127.0.0.1:8899".to_owned()
    }

    const fn default_cluster_nodes_update_interval() -> Duration {
        Duration::from_secs(30)
    }

    const fn default_stake_update_interval() -> Duration {
        Duration::from_secs(30)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigUpstreamGrpc {
    /// gRPC service endpoint
    #[serde(default = "ConfigUpstreamGrpc::default_endpoint")]
    pub endpoint: String,

    /// Optional token for access to gRPC
    pub x_token: Option<String>,
}

impl ConfigUpstreamGrpc {
    fn default_endpoint() -> String {
        "http://127.0.0.1:10000".to_owned()
    }
}

impl From<ConfigUpstream> for PolicyStoreConfig {
    fn from(config: ConfigUpstream) -> Self {
        let ConfigUpstream {
            rpc,
            grpc: ConfigUpstreamGrpc { endpoint, x_token },
            ..
        } = config;

        PolicyStoreConfig {
            rpc: PolicyStoreRpcConfig { endpoint: rpc },
            grpc: PolicyStoreGrpcConfig {
                endpoint,
                x_token,
                max_decoding_message_size: Some(100_000_000),
                commitment: Some(ShieldStoreCommitmentLevel::Confirmed),
                connect_timeout: Duration::from_secs(60),
                http2_adaptive_window: true,
                http2_keep_alive: true,
                timeout: Duration::from_secs(60),
                tcp_nodelay: true,
                http2_keep_alive_interval: None,
                http2_keep_alive_timeout: None,
                http2_keep_alive_while_idle: None,
                initial_connection_window_size: None,
                initial_stream_window_size: None,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConfigJetGatewayClient {
    /// gRPC service endpoints, only one connection would be used
    pub endpoints: Vec<String>,

    /// Access token
    pub x_token: Option<String>,

    /// Maximum number of permit that can be received from jet-gateway, overrides staked-based stream computation.
    /// If set to `None`, then stream size would be computed based on stake.
    /// It is clipped to the maximum staked-based stream size.
    #[serde(
        default,
        deserialize_with = "ConfigJetGatewayClient::deserialize_maybe_nonzero_u64"
    )]
    pub max_streams: Option<NonZeroU64>,

    ///
    /// Maximum number of subscribe attempts to the jet-gateway.
    /// If set to `None`, then it would be infinite.
    #[serde(default = "ConfigJetGatewayClient::default_maximum_subscribe_attempts")]
    pub maximum_subscribe_attempts: Option<NonZeroUsize>,
}

impl ConfigJetGatewayClient {
    fn deserialize_maybe_nonzero_u64<'de, D>(
        deserializer: D,
    ) -> Result<Option<NonZeroU64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // If 0 then fallback to None.
        Ok(Option::<u64>::deserialize(deserializer)?.and_then(NonZeroU64::new))
    }

    const fn default_maximum_subscribe_attempts() -> Option<NonZeroUsize> {
        None
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigListenAdmin {
    /// RPC listen address
    #[serde(deserialize_with = "deserialize_listen")]
    pub bind: Vec<SocketAddr>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigListenSolanaLike {
    /// RPC listen addresses
    #[serde(deserialize_with = "deserialize_listen")]
    pub bind: Vec<SocketAddr>,

    /// Allow to do sanitize check on RPC server (required for ALTs), supported only on patched nodes
    /// If option set to `true`` then Jet would check `sanitizeTransaction` method before start
    /// See https://github.com/rpcpool/solana-public/tree/v1.17.31-rpc-sanitize-tx
    #[serde(default)]
    pub proxy_sanitize_check: bool,

    /// Allow to do preflight check on RPC server (simulateTransaction)
    #[serde(default)]
    pub proxy_preflight_check: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigSendTransactionService {
    /// Default max retries of sending transaction
    pub default_max_retries: Option<usize>,

    /// Service max retries
    #[serde(default = "ConfigSendTransactionService::default_service_max_retries")]
    pub service_max_retries: usize,

    /// Stop send transaction when landed at specified commitment
    #[serde(default = "ConfigSendTransactionService::default_stop_send_on_commitment")]
    pub stop_send_on_commitment: CommitmentLevel,

    /// The number of upcoming leaders to which to forward transactions
    #[deprecated(
        note = "jet already implements smart fanout based on slot timing. Having too high fanout creates jitter."
    )]
    #[serde(default = "ConfigSendTransactionService::default_leader_forward_count")]
    pub leader_forward_count: Option<usize>,

    /// Try to send transaction every retry_rate duration
    #[serde(
        default = "ConfigSendTransactionService::default_retry_rate",
        with = "humantime_serde"
    )]
    pub retry_rate: Duration,

    /// Drop transactions from the pool once max retries limit is reached (landed statistic would be invalid)
    #[serde(default)]
    pub relay_only_mode: bool,

    /// Extra forward (transactions would be always sent to these nodes)
    /// regardless of the transaction yellowstone-shield policies.
    #[serde(default, deserialize_with = "deser_pubkey_vec")]
    pub extra_fanout: Vec<Pubkey>,
}

impl ConfigSendTransactionService {
    const fn default_service_max_retries() -> usize {
        usize::MAX
    }

    const fn default_stop_send_on_commitment() -> CommitmentLevel {
        CommitmentLevel::Confirmed
    }

    const fn default_leader_forward_count() -> Option<usize> {
        None
    }

    const fn default_retry_rate() -> Duration {
        Duration::from_millis(1_000)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigQuic {
    #[serde(flatten)]
    pub tpu_sender: TpuSenderConfig,

    ///
    /// Connection eviction is trigger when the total number of connections in the QUIC gateway exceeds configured
    /// `max_concurrent_connection`.
    ///
    /// Connection eviction will evict lower staked connection first that have not been used for in the last
    /// `connection_eviction_grace` duration.
    ///
    /// Default is `2s` (2 seconds) which means that if a connection has not been used for 2 seconds, it will
    /// be elligbile for eviction.
    ///
    #[serde(
        default = "ConfigQuic::default_connection_eviction_grace",
        with = "humantime_serde"
    )]
    pub connection_idle_eviction_grace: Duration,
}

impl ConfigQuic {
    const fn default_connection_eviction_grace() -> Duration {
        DEFAULT_LEADER_DURATION
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLewisEvents {
    /// gRPC endpoint for Lewis event service
    pub endpoint: String,

    /// Optional X-Token for authentication
    pub x_token: Option<String>,

    /// Events gRPC queue size
    #[serde(default = "ConfigLewisEvents::default_queue_size_grpc")]
    pub queue_size_grpc: usize,

    /// Jet ID to use for events
    #[serde(default)]
    pub jet_id: Option<String>,

    /// Batch size threshold - number of events before forcing a flush
    #[serde(default = "ConfigLewisEvents::default_batch_size_threshold")]
    pub batch_size_threshold: u64,

    /// Batch timeout - max time to wait before flushing events
    #[serde(
        default = "ConfigLewisEvents::default_batch_timeout",
        with = "humantime_serde"
    )]
    pub batch_timeout: Duration,

    /// Connection timeout for Lewis gRPC
    #[serde(
        default = "ConfigLewisEvents::default_connect_timeout",
        with = "humantime_serde"
    )]
    pub connect_timeout: Duration,

    /// HTTP2 keepalive interval
    #[serde(
        default = "ConfigLewisEvents::default_keepalive_interval",
        with = "humantime_serde"
    )]
    pub keepalive_interval: Duration,

    /// Keepalive timeout
    #[serde(
        default = "ConfigLewisEvents::default_keepalive_timeout",
        with = "humantime_serde"
    )]
    pub keepalive_timeout: Duration,

    /// Size of internal event buffer between handler and client
    #[serde(default = "ConfigLewisEvents::default_event_buffer_size")]
    pub event_buffer_size: usize,

    /// Keep HTTP2 connection alive even when idle
    #[serde(default = "ConfigLewisEvents::default_keep_alive_while_idle")]
    pub keep_alive_while_idle: bool,

    /// Maximum number of reconnection attempts
    #[serde(default = "ConfigLewisEvents::default_max_reconnect_attempts")]
    pub max_reconnect_attempts: usize,

    /// Initial interval for reconnection backoff
    #[serde(
        default = "ConfigLewisEvents::default_reconnect_initial_interval",
        with = "humantime_serde"
    )]
    pub reconnect_initial_interval: Duration,

    /// Maximum interval for reconnection backoff
    #[serde(
        default = "ConfigLewisEvents::default_reconnect_max_interval",
        with = "humantime_serde"
    )]
    pub reconnect_max_interval: Duration,

    /// Maximum time for the entire stream
    #[serde(
        default = "ConfigLewisEvents::default_stream_timeout",
        with = "humantime_serde"
    )]
    pub stream_timeout: Duration,
}

impl ConfigLewisEvents {
    const fn default_queue_size_grpc() -> usize {
        10_000
    }

    const fn default_batch_size_threshold() -> u64 {
        512
    }

    const fn default_batch_timeout() -> Duration {
        Duration::from_millis(1000)
    }

    const fn default_connect_timeout() -> Duration {
        Duration::from_secs(10)
    }

    const fn default_keepalive_interval() -> Duration {
        Duration::from_secs(30)
    }

    const fn default_keepalive_timeout() -> Duration {
        Duration::from_secs(10)
    }

    const fn default_event_buffer_size() -> usize {
        100_000
    }

    const fn default_keep_alive_while_idle() -> bool {
        true
    }

    const fn default_max_reconnect_attempts() -> usize {
        3
    }

    const fn default_reconnect_initial_interval() -> Duration {
        Duration::from_millis(1000)
    }

    const fn default_reconnect_max_interval() -> Duration {
        Duration::from_secs(30)
    }

    const fn default_stream_timeout() -> Duration {
        Duration::from_secs(300) // 0 means no timeout
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigListenGrpc {
    /// gRPC listen address
    #[serde(deserialize_with = "deserialize_listen")]
    pub bind: Vec<SocketAddr>,
}

fn deserialize_listen<'de, D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Debug, PartialEq, Eq, Hash, Deserialize)]
    #[serde(untagged)]
    enum Value {
        SocketAddr(SocketAddr),
        Port(u16),
        Env { env: String },
    }

    let addrs = HashSet::<Value>::deserialize(deserializer)?
        .into_iter()
        .map(|value| match value {
            Value::SocketAddr(addr) => Ok(addr),
            Value::Port(port) => Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(0, 0, 0, 0),
                port,
            ))),
            Value::Env { env } => std::env::var(env)
                .map_err(|error| format!("{error:}"))
                .and_then(|value| match value.parse() {
                    Ok(addr) => Ok(addr),
                    Err(error) => match value.parse() {
                        Ok(port) => Ok(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::new(0, 0, 0, 0),
                            port,
                        ))),
                        Err(_) => Err(format!("{error:?}")),
                    },
                })
                .map_err(de::Error::custom),
        })
        .collect::<Result<Vec<SocketAddr>, _>>()?;

    if addrs.len() == 1 {
        Ok(addrs)
    } else {
        Err(de::Error::custom(
            "only 1 listen address supported right now".to_owned(),
        ))
    }
}

///
/// THIS CODE HAS BEEN COPY-PASTED FROM THE `jet-gateway` repo
/// TODO: Refactor this code to be shared common lib.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum RpcErrorStrategy {
    #[serde(rename = "fixed")]
    Fixed {
        #[serde(with = "humantime_serde")]
        interval: Duration,
        #[serde(default = "RpcErrorStrategy::default_retries")]
        retries: NonZeroUsize,
    },
    #[serde(rename = "exponential")]
    Exponential {
        #[serde(with = "humantime_serde")]
        base: Duration,
        factor: f64,
        #[serde(default = "RpcErrorStrategy::default_retries")]
        retries: NonZeroUsize,
    },
    #[serde(rename = "fail")]
    Fail,
}

impl RpcErrorStrategy {
    const fn default_retries() -> NonZeroUsize {
        unsafe { NonZeroUsize::new_unchecked(3) }
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct PrometheusConfig {
    pub url: String,
    #[serde(
        default = "PrometheusConfig::default_push_interval",
        with = "humantime_serde"
    )]
    pub push_interval: Duration,
}

impl PrometheusConfig {
    const fn default_push_interval() -> Duration {
        Duration::from_secs(10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deser_jet_gateway_client() {
        let yaml = r#"
        max_streams: null
        endpoints:
            - http://127.0.0.1:8002
        # Access token
        x_token: null
        "#;

        let cfg: ConfigJetGatewayClient = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.endpoints, vec!["http://127.0.0.1:8002"]);
        assert_eq!(cfg.max_streams, None);
        assert_eq!(cfg.x_token, None);

        // Interpret 0 as None
        let yaml = r#"
        max_streams: 0
        endpoints:
            - http://127.0.0.1:8002
        # Access token
        x_token: null
        "#;

        let cfg: ConfigJetGatewayClient = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.endpoints, vec!["http://127.0.0.1:8002"]);
        assert_eq!(cfg.max_streams, None);
        assert_eq!(cfg.x_token, None);
    }
}
