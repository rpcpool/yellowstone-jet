use {
    crate::{feature_flags::FeatureSet, util::CommitmentLevel},
    anyhow::Context,
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    solana_net_utils::{PortRange, VALIDATOR_PORT_RANGE},
    solana_sdk::{
        pubkey::Pubkey,
        quic::{
            QUIC_CONNECTION_HANDSHAKE_TIMEOUT, QUIC_KEEP_ALIVE, QUIC_MAX_TIMEOUT,
            QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS,
        },
        signer::keypair::{read_keypair_file, Keypair},
    },
    solana_tpu_client::tpu_client::DEFAULT_TPU_CONNECTION_POOL_SIZE,
    std::{
        collections::HashSet,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        num::NonZeroUsize,
        ops::Range,
        path::{Path, PathBuf},
    },
    tokio::{fs, time::Duration},
    yellowstone_shield_store::{NullConfig, VixenConfig},
};

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

    /// Send metrics to lewis
    pub metrics_upstream: Option<ConfigMetricsUpstream>,

    /// Features Flags
    #[serde(default)]
    pub features: FeatureSet,

    /// Prometheus Push Gateway
    pub prometheus: Option<PrometheusConfig>,

    /// Config Shield
    pub shield: ConfigShield,
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

/// Yellowstone Shield policy store configuration
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigShield {
    /// Vixen configuration for syncing the shield store
    pub vixen: VixenConfig<NullConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigUpstream {
    /// Primary gRPC service
    pub primary_grpc: ConfigUpstreamGrpc,

    /// Secondary gRPC service, by default primary would be used
    /// Used only for additional transaction status subscribe
    pub secondary_grpc: Option<ConfigUpstreamGrpc>,

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
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, Deserialize)]
pub struct ConfigJetGatewayClient {
    /// gRPC service endpoints, only one connection would be used
    pub endpoints: Vec<String>,

    /// Access token
    pub x_token: Option<String>,
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

#[derive(Debug, Clone, Copy, Deserialize)]
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
    #[serde(default = "ConfigSendTransactionService::default_leader_forward_count")]
    pub leader_forward_count: usize,

    /// Try to send transaction every retry_rate duration
    #[serde(
        default = "ConfigSendTransactionService::default_retry_rate",
        with = "humantime_serde"
    )]
    pub retry_rate: Duration,

    /// Drop transactions from the pool once max retries limit is reached (landed statistic would be invalid)
    #[serde(default)]
    pub relay_only_mode: bool,
}

impl ConfigSendTransactionService {
    const fn default_service_max_retries() -> usize {
        usize::MAX
    }

    const fn default_stop_send_on_commitment() -> CommitmentLevel {
        CommitmentLevel::Confirmed
    }

    const fn default_leader_forward_count() -> usize {
        4
    }

    const fn default_retry_rate() -> Duration {
        Duration::from_millis(1_000)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigQuic {
    /// Total number of pools (one pool per remote address, i.e. one per leader)
    /// Solana value is 1024
    /// https://github.com/solana-labs/solana/blob/v1.17.31/connection-cache/src/connection_cache.rs#L22
    #[serde(default = "ConfigQuic::default_connection_max_pools")]
    pub connection_max_pools: NonZeroUsize,

    /// TPU connection pool size per remote address
    /// Default is `solana_tpu_client::tpu_client::DEFAULT_TPU_CONNECTION_POOL_SIZE` (1 from 1.17.33 / 1.18.12, previous value is 4)
    #[serde(
        default = "ConfigQuic::default_connection_pool_size",
        deserialize_with = "ConfigQuic::deserialize_connection_pool_size"
    )]
    pub connection_pool_size: usize,

    /// Number of immediate retries in case of failed send (not applied to timedout)
    /// Solana do not retry, atlas doing 4 retries, by default we keep same limit as Solana
    #[serde(default = "ConfigQuic::default_send_retry_count")]
    pub send_retry_count: usize,

    /// Kind of Quic port: `normal` or `forwards`
    pub tpu_port: ConfigQuicTpuPort,

    /// Quic handshake timeout.
    /// Default is `solana_sdk::quic::QUIC_CONNECTION_HANDSHAKE_TIMEOUT` -- 60s
    #[serde(
        default = "ConfigQuic::default_connection_handshake_timeout",
        with = "humantime_serde"
    )]
    pub connection_handshake_timeout: Duration,

    /// Maximum duration of inactivity to accept before timing out the connection.
    /// https://docs.rs/quinn/0.10.2/quinn/struct.TransportConfig.html#method.max_idle_timeout
    /// Default is `solana_sdk::quic::QUIC_KEEP_ALIVE` -- 2s
    #[serde(
        default = "ConfigQuic::default_max_idle_timeout",
        with = "humantime_serde"
    )]
    pub max_idle_timeout: Duration,

    /// Period of inactivity before sending a keep-alive packet
    /// https://docs.rs/quinn/0.10.2/quinn/struct.TransportConfig.html#method.keep_alive_interval
    /// Default is `solana_sdk::quic::QUIC_KEEP_ALIVE` -- 1s
    #[serde(
        default = "ConfigQuic::default_keep_alive_interval",
        with = "humantime_serde"
    )]
    pub keep_alive_interval: Duration,

    /// Send tx timeout, for batches value multipled by number of transactions in the batch
    /// Solana default value is 10 seconds
    #[serde(default = "ConfigQuic::default_send_timeout", with = "humantime_serde")]
    pub send_timeout: Duration,

    /// Ports used by QUIC endpoints
    /// https://docs.rs/solana-net-utils/1.18.11/solana_net_utils/constant.VALIDATOR_PORT_RANGE.html
    /// Default is `solana_net_utils::VALIDATOR_PORT_RANGE` -- `8000..10000`
    #[serde(
        default = "ConfigQuic::default_endpoint_port_range",
        deserialize_with = "ConfigQuic::deserialize_endpoint_port_range"
    )]
    pub endpoint_port_range: PortRange,

    /// See `solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams`
    /// https://github.com/anza-xyz/agave/blob/v1.17.31/streamer/src/nonblocking/quic.rs#L244-L279
    /// Minumum value is `QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS` -- for unstaked nodes, 128
    /// Value for staked calculated from total stake, but maximum is `QUIC_MAX_STAKED_CONCURRENT_STREAMS`
    #[serde(default = "ConfigQuic::default_send_max_concurrent_streams")]
    pub send_max_concurrent_streams: usize,

    /// Extra TPU forward (transactions would be always sent to these nodes)
    #[serde(default)]
    pub extra_tpu_forward: Vec<ConfigExtraTpuForward>,
}

impl ConfigQuic {
    pub fn default_connection_max_pools() -> NonZeroUsize {
        NonZeroUsize::new(1024).unwrap()
    }

    pub const fn default_connection_pool_size() -> usize {
        DEFAULT_TPU_CONNECTION_POOL_SIZE
    }

    pub const fn default_send_retry_count() -> usize {
        1
    }

    pub const fn default_connection_handshake_timeout() -> Duration {
        QUIC_CONNECTION_HANDSHAKE_TIMEOUT
    }

    pub const fn default_max_idle_timeout() -> Duration {
        QUIC_MAX_TIMEOUT
    }

    pub const fn default_keep_alive_interval() -> Duration {
        QUIC_KEEP_ALIVE
    }

    pub const fn default_send_timeout() -> Duration {
        Duration::from_secs(10)
    }

    pub const fn default_endpoint_port_range() -> PortRange {
        VALIDATOR_PORT_RANGE
    }

    pub const fn default_send_max_concurrent_streams() -> usize {
        QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS
    }

    fn deserialize_connection_pool_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        NonZeroUsize::deserialize(deserializer).map(|v| v.get())
    }

    fn deserialize_endpoint_port_range<'de, D>(deserializer: D) -> Result<PortRange, D::Error>
    where
        D: Deserializer<'de>,
    {
        Range::deserialize(deserializer).map(|range| (range.start, range.end))
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigQuicTpuPort {
    #[default]
    Normal,
    Forwards,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigExtraTpuForward {
    #[serde(deserialize_with = "ConfigExtraTpuForward::deserialize_pubkey")]
    pub leader: Pubkey,
    #[serde(default)]
    pub quic: Option<SocketAddr>,
    #[serde(default)]
    pub quic_forwards: Option<SocketAddr>,
}

impl ConfigExtraTpuForward {
    fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigMetricsUpstream {
    /// lewis gRPC metrics endpoint
    pub endpoint: String,
    /// Events gRPC queue size
    #[serde(default = "ConfigMetricsUpstream::default_queue_size_grpc")]
    pub queue_size_grpc: usize,
    /// Event buffer queue size
    #[serde(default = "ConfigMetricsUpstream::default_queue_size_buffer")]
    pub queue_size_buffer: usize,
}

impl ConfigMetricsUpstream {
    const fn default_queue_size_grpc() -> usize {
        1_000
    }

    const fn default_queue_size_buffer() -> usize {
        100_000
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigEtcd {
    pub endpoints: Vec<String>,
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
                .map_err(|error| format!("{:}", error))
                .and_then(|value| match value.parse() {
                    Ok(addr) => Ok(addr),
                    Err(error) => match value.parse() {
                        Ok(port) => Ok(SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::new(0, 0, 0, 0),
                            port,
                        ))),
                        Err(_) => Err(format!("{:?}", error)),
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
