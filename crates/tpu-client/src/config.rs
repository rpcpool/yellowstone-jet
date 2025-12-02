use {
    serde::{Deserialize, Deserializer, de},
    solana_net_utils::{PortRange, VALIDATOR_PORT_RANGE},
    solana_pubkey::Pubkey,
    solana_quic_definitions::QUIC_MAX_TIMEOUT,
    std::{net::SocketAddr, num::NonZeroUsize, ops::Range, time::Duration},
};

///
/// Specifies which TPU port to use for QUIC connections.
///
#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TpuPortKind {
    ///
    /// Use the normal TPU port for QUIC connections.
    ///
    Normal,
    ///
    /// (Preferred) Use the forwards TPU port for QUIC connections.
    ///
    #[default]
    Forwards,
}

///
/// Specifies how to rewrite TPU addresses for QUIC connections for a specific remote peer.
///
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TpuOverrideInfo {
    ///
    /// The remote peer's public key to overide the TPU address for.
    ///
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub remote_peer: Pubkey,
    ///
    /// The QUIC TPU address to use for the remote peer.
    ///
    pub quic_tpu: SocketAddr,
    ///
    /// The QUIC TPU forward address to use for the remote peer.
    ///
    pub quic_tpu_forward: SocketAddr,
}

fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}

fn deserialize_port_range<'de, D>(deserializer: D) -> Result<PortRange, D::Error>
where
    D: Deserializer<'de>,
{
    Range::deserialize(deserializer).map(|range| (range.start, range.end))
}

///
/// Configuration for the TPU sender.
///
/// This control various aspects of the TPU QUIC sender behavior, mostly QUIC connection parameters:
///     - number of endpoints
///     - connection timeouts
///     - send timeouts
///     - max concurrent connections
///     - port range to bind local endpoints to
///     - etc...
///
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TpuSenderConfig {
    ///
    /// Port range to bind local QUIC endpoints to.
    ///
    #[serde(
        deserialize_with = "deserialize_port_range",
        default = "TpuSenderConfig::default_port_range"
    )]
    pub endpoint_port_range: PortRange,

    ///
    /// Maximum idle timeout for QUIC connections.
    ///
    /// Connections idle for longer than this duration will be closed.
    #[serde(
        default = "TpuSenderConfig::default_max_idle_timeout",
        with = "humantime_serde"
    )]
    pub max_idle_timeout: Duration,

    ///
    /// Maximum number of consecutive connection attempts
    ///
    #[serde(default = "TpuSenderConfig::default_max_connection_attempts")]
    pub max_connection_attempts: usize,

    ///
    /// Capacity of the transaction sender worker channel per remote peer.
    ///
    #[serde(default = "TpuSenderConfig::default_transaction_sender_worker_channel_capacity")]
    pub transaction_sender_worker_channel_capacity: usize,

    ///
    /// Timeout for establishing a connection to a remote peer.
    ///
    #[serde(
        default = "TpuSenderConfig::default_connection_timeout",
        alias = "connection_handshake_timeout", /*LEGACY */
        with = "humantime_serde"
    )]
    pub connecting_timeout: Duration,

    ///
    /// Which TPU port to use for QUIC connections, default is "forwards".
    ///
    #[serde(default = "TpuSenderConfig::default_tpu_port_kind")]
    pub tpu_port: TpuPortKind,

    ///
    /// Maximum number of concurrent connections to a remote peer.
    ///
    #[serde(default = "TpuSenderConfig::default_max_concurrent_connections")]
    pub max_concurrent_connection: usize,

    ///
    /// Maximum number of attempts to bind a local port to a remote peer.
    ///
    #[serde(default = "TpuSenderConfig::default_max_local_port_binding_attempts")]
    pub max_local_port_binding_attempts: usize,

    ///
    /// How many endpoints to create for the QUIC gateway.
    /// Each endpoint will be bound to a different port in the port range.
    /// Each endpoint has its own "event loop" and can handle multiple connections concurrently.
    ///
    /// The number of endpoints should not be greater than the numbe of CPU cores dedicated to jet.
    ///
    /// The number of endpoints depends on the stake of the gateway as lower stake gateway should require less endpoints.
    ///
    /// Recommended try 1 endpoint per 8 CPU cores dedicated to jet.
    ///
    #[serde(
        default = "TpuSenderConfig::default_num_endpoints",
        alias = "endpoint_count" /*LEGACY */
    )]
    pub num_endpoints: NonZeroUsize,

    ///
    /// Maximum number of consecutive transaction sending attempts to a remote peer.
    /// Attempt may fail due to connection losts, stream limit exceeded, etc.
    /// It might be useful to retry sending a transaction at least 2-3 times before giving up.
    ///
    #[serde(
        default = "TpuSenderConfig::default_max_send_attempt",
        alias = "send_retry_count" /*LEGACY */
    )]
    pub max_send_attempt: NonZeroUsize,

    ///
    /// Interval to watch remote peer address changes.
    ///
    #[serde(
        default = "TpuSenderConfig::default_remote_peer_addr_watch_interval",
        with = "humantime_serde"
    )]
    pub remote_peer_addr_watch_interval: Duration,

    ///
    /// Timeout for sending a transaction to a remote peer.
    ///
    #[serde(
        default = "TpuSenderConfig::default_send_timeout",
        with = "humantime_serde"
    )]
    pub send_timeout: Duration,

    ///
    /// Maximum number of leaders to predict
    ///
    #[serde(
        default = "TpuSenderConfig::default_leader_prediction_lookahead",
        alias = "connection_prediction_lookahead" /*LEGACY */
    )]
    pub leader_prediction_lookahead: Option<NonZeroUsize>,

    ///
    /// The TPU address rewrite map for QUIC connections.
    ///
    #[serde(default)]
    pub tpu_info_override: Vec<TpuOverrideInfo>,
}

impl TpuSenderConfig {
    pub const fn default_connection_timeout() -> Duration {
        DEFAULT_CONNECTION_TIMEOUT
    }

    pub const fn default_max_idle_timeout() -> Duration {
        QUIC_MAX_TIMEOUT
    }

    pub const fn default_max_connection_attempts() -> usize {
        DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT
    }

    pub const fn default_transaction_sender_worker_channel_capacity() -> usize {
        DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE
    }

    pub const fn default_max_concurrent_connections() -> usize {
        DEFAULT_MAX_CONCURRENT_CONNECTIONS
    }

    pub const fn default_max_local_port_binding_attempts() -> usize {
        DEFAULT_MAX_LOCAL_BINDING_PORT_ATTEMPTS
    }

    pub const fn default_num_endpoints() -> NonZeroUsize {
        DEFAULT_QUIC_DRIVER_ENDPOINT_COUNT
    }

    pub const fn default_max_send_attempt() -> NonZeroUsize {
        DEFAULT_MAX_SEND_ATTEMPT
    }

    pub const fn default_remote_peer_addr_watch_interval() -> Duration {
        DEFAULT_REMOTE_PEER_ADDR_WATCH_INTERVAL
    }

    pub const fn default_send_timeout() -> Duration {
        DEFAULT_TX_SEND_TIMEOUT
    }

    pub const fn default_leader_prediction_lookahead() -> Option<NonZeroUsize> {
        Some(DEFAULT_LEADER_PREDICTION_LOOKAHEAD)
    }

    pub const fn default_tpu_port_kind() -> TpuPortKind {
        TpuPortKind::Forwards
    }

    pub const fn default_port_range() -> PortRange {
        VALIDATOR_PORT_RANGE
    }
}

///
/// Each [`quinn::Endpoint`] has its own event-loop.
/// Each endpoint can manage thousands of connections concurrently.
/// HOWEVER, each [`quinn::Endpoint`] has a state mutex lock.
/// Quickly looking at quinn's source code, it seems that each lock acquisition is quite short live.
/// If we have too many connections over a single endpoint, we might end up with a lot of contention on the endpoint mutex.
/// At the same time, if we have too many endpoints, we might end up with too many event loops running concurrently.
/// Talking with Anza, we should not open more than 5 endpoints to host QUIC connections.
pub const DEFAULT_QUIC_DRIVER_ENDPOINT_COUNT: NonZeroUsize =
    NonZeroUsize::new(5).expect("default endpoint count must be non-zero");
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT: usize = 3;
pub const DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE: usize = 10_000;
pub const DEFAULT_MAX_CONCURRENT_CONNECTIONS: usize = 1024;
pub const DEFAULT_MAX_LOCAL_BINDING_PORT_ATTEMPTS: usize = 3;
pub const DEFAULT_LEADER_DURATION: Duration = Duration::from_secs(2); // 400ms * 4 rounded to seconds
pub const DEFAULT_MAX_SEND_ATTEMPT: NonZeroUsize = NonZeroUsize::new(3).unwrap();
pub const DEFAULT_REMOTE_PEER_ADDR_WATCH_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_TX_SEND_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_LEADER_PREDICTION_LOOKAHEAD: NonZeroUsize = NonZeroUsize::new(4).unwrap();

impl Default for TpuSenderConfig {
    fn default() -> Self {
        Self {
            endpoint_port_range: VALIDATOR_PORT_RANGE,
            max_idle_timeout: QUIC_MAX_TIMEOUT,
            max_connection_attempts: DEFAULT_MAX_CONSECUTIVE_CONNECTION_ATTEMPT,
            transaction_sender_worker_channel_capacity: DEFAULT_PER_PEER_TRANSACTION_QUEUE_SIZE,
            connecting_timeout: DEFAULT_CONNECTION_TIMEOUT,
            max_concurrent_connection: DEFAULT_MAX_CONCURRENT_CONNECTIONS,
            max_local_port_binding_attempts: DEFAULT_MAX_LOCAL_BINDING_PORT_ATTEMPTS,
            tpu_port: TpuPortKind::default(),
            num_endpoints: DEFAULT_QUIC_DRIVER_ENDPOINT_COUNT,
            max_send_attempt: DEFAULT_MAX_SEND_ATTEMPT,
            remote_peer_addr_watch_interval: DEFAULT_REMOTE_PEER_ADDR_WATCH_INTERVAL,
            send_timeout: DEFAULT_TX_SEND_TIMEOUT,
            leader_prediction_lookahead: Some(DEFAULT_LEADER_PREDICTION_LOOKAHEAD),
            tpu_info_override: Vec::new(),
        }
    }
}

#[cfg(test)]
pub mod test {
    use {
        crate::config::{TpuPortKind, TpuSenderConfig},
        std::num::NonZeroUsize,
    };

    #[test]
    fn it_should_deser_tpu_sender_config_with_defaults() {
        let yaml = r#"
        endpoint_port_range:
            start: 8000
            end: 9000
        "#;
        let expected = TpuSenderConfig {
            endpoint_port_range: (8000, 9000),
            ..Default::default()
        };

        let config: super::TpuSenderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint_port_range, expected.endpoint_port_range);
    }

    #[test]
    fn it_should_supports_legacy_jet_field_name() {
        let yaml = r#"
        endpoint_count: 3
        send_retry_count: 5
        connection_prediction_lookahead: 7
        tpu_port: normal
        "#;
        let expected = TpuSenderConfig {
            num_endpoints: NonZeroUsize::new(3).unwrap(),
            max_send_attempt: NonZeroUsize::new(5).unwrap(),
            leader_prediction_lookahead: NonZeroUsize::new(7),
            tpu_port: TpuPortKind::Normal,
            ..TpuSenderConfig::default()
        };

        let config: super::TpuSenderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config, expected);
    }
}
