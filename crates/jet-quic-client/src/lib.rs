//! Reference client for jet's raw QUIC transaction ingress.
//!
//! Speaks the same wire protocol jet's raw QUIC server expects: an mTLS-authenticated
//! QUIC connection, with one transaction per unidirectional stream (raw bytes, no
//! length-prefix framing — the stream's FIN is the message boundary).
//!
//! Two building blocks: [`JetQuicEndpoint`] creates connections (holding the
//! TLS/identity config so it can be reused to create more than one), and
//! [`RawJetTransactionSender`] owns one connection and sends on it.
//!
//! ## Why connections are never shared
//!
//! `quinn::Endpoint` and `quinn::Connection` are internally implemented as thin handles
//! onto state that's shared (and mutex-guarded) across every clone of that handle and
//! every stream/`Incoming` derived from it — that's what lets you cheaply `.clone()` a
//! `Connection` and hand it to several tasks. We deliberately don't use that: sharing
//! one connection (or one endpoint) across concurrent tasks means every one of those
//! tasks contends on the same internal lock, which works against the lowest latency
//! possible for any individual send.
//!
//! So this crate cancels out quinn's built-in concurrency features on purpose:
//! [`JetQuicConnection`] is not `Clone` and isn't shared, and
//! [`RawJetTransactionSender::send_transaction`] takes `&mut self` specifically so the
//! *compiler* — not a runtime lock — enforces that a given connection is never in use
//! from two tasks at once (see the `quinn-client` skill's stream-concurrency rule).
//! Concurrency is instead the caller's problem to solve at a higher level: open several
//! independent connections (via [`JetQuicEndpoint::connect`], each its own
//! `quinn::Endpoint`/socket, no shared state between them) and spread work across them
//! yourself, rather than fanning many tasks out over one shared connection/endpoint and
//! paying quinn's internal lock contention for it.

mod dns;
mod tls;
pub mod topology;

pub use {
    dns::{DnsResolver, ResolveError, StdDnsResolver, set_resolver},
    tls::{RootCertStore, ServerVerification, default_client_config, load_cert_pem, load_key_pem},
};
use {
    rustls::Error as RustlsError,
    std::net::{IpAddr, Ipv4Addr, SocketAddr},
};

/// ALPN protocol id negotiated with jet's raw QUIC ingress. Must match the server side.
pub const ALPN_JET_RAW_TX_PROTOCOL_ID: &[u8] = b"jet-raw-tx";

/// Errors from creating a connection — [`JetQuicEndpoint::connect`]/[`default_client_config`].
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid TLS credentials/config: {0}")]
    Tls(#[from] RustlsError),
    #[error("failed to resolve server address: {0}")]
    Resolve(#[from] ResolveError),
    #[error("failed to initiate connection: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
}

/// Errors from sending on an already-established connection —
/// [`RawJetTransactionSender::send_transaction`]. Deliberately narrower than
/// [`ConnectError`]: TLS, binding, and handshake failures can't happen here, only
/// failures of a connection that was already up.
#[derive(Debug, thiserror::Error)]
pub enum SendTransactionError {
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("failed to write transaction bytes: {0}")]
    Write(#[from] quinn::WriteError),
}

impl SendTransactionError {
    /// Whether this failure means the connection itself is dead (as opposed to e.g.
    /// just this one stream being stopped by the peer, which says nothing about the
    /// connection's health) — i.e. whether [`JetTransactionSender`] should reconnect.
    const fn is_connection_fatal(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::Write(quinn::WriteError::ConnectionLost(_))
        )
    }
}

/// Errors from [`JetTransactionSender::send_transaction`]: either the send failed and no
/// reconnect was warranted/attempted, or it failed *and* the reconnect that followed
/// also failed.
#[derive(Debug, thiserror::Error)]
pub enum TransactionSendError {
    #[error("{0}")]
    Send(#[from] SendTransactionError),
    #[error("connection died ({send_error}) and reconnecting failed: {reconnect_error}")]
    Reconnect {
        send_error: SendTransactionError,
        reconnect_error: ConnectError,
    },
}

pub struct JetQuicEndpoint {
    quinn_endpoint: quinn::Endpoint,
}

impl JetQuicEndpoint {
    pub fn bind(local_addr: Option<SocketAddr>) -> Result<Self, std::io::Error> {
        let quinn_endpoint = quinn::Endpoint::client(
            local_addr.unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
        )?;
        Ok(Self { quinn_endpoint })
    }

    pub async fn connect(
        self,
        server_addr: &ServerAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
    ) -> Result<JetQuicConnection, ConnectError> {
        let remote_addr = match server_addr {
            ServerAddr::SocketAddr(addr) => *addr,
            ServerAddr::Named { host, port } => {
                dns::resolve(host, port.unwrap_or(DEFAULT_SERVER_PORT)).await?
            }
        };
        let mut quinn_endpoint = self.quinn_endpoint;
        quinn_endpoint.set_default_client_config(client_config.clone());

        let connection = quinn_endpoint.connect(remote_addr, &server_name)?.await?;
        Ok(JetQuicConnection {
            _quinn_endpoint: quinn_endpoint,
            client_config,
            quinn_conn: connection,
        })
    }
}

/// A QUIC connection and the endpoint that owns its socket, held together — dropping
/// the endpoint would close the connection, so this type exists specifically so the two
/// are never separated. Deliberately not `Clone`: holding a `JetQuicConnection` *is*
/// having exclusive use of that connection.
pub struct JetQuicConnection {
    // Kept alive: dropping it would close `quinn_conn`.
    _quinn_endpoint: quinn::Endpoint,
    client_config: quinn::ClientConfig,
    quinn_conn: quinn::Connection,
}

/// Owns one [`JetQuicConnection`] and sends transactions on it.
pub struct RawJetTransactionSender {
    connection: JetQuicConnection,
}

impl RawJetTransactionSender {
    pub const fn new(connection: JetQuicConnection) -> Self {
        Self { connection }
    }

    /// Sends one transaction: opens a fresh unidirectional stream and writes the raw
    /// bytes. Fire-and-forget — there is no application-level ack, and the stream
    /// finishes implicitly when it's dropped at the end of this call (or resets, if the
    /// peer already stopped it — see quinn's `Drop for SendStream`).
    ///
    /// Takes `&mut self` so the compiler enforces this is never called concurrently
    /// from two tasks on the same sender — see the `quinn-client` skill's
    /// stream-concurrency rule for why that matters.
    pub async fn send_transaction(
        &mut self,
        wire_transaction: &[u8],
    ) -> Result<(), SendTransactionError> {
        let mut send = self.connection.quinn_conn.open_uni().await?;
        send.write_all(wire_transaction).await?;
        Ok(())
    }
}

/// Where to reach the server: a fixed address, or a hostname + port for a
/// [`DnsResolver`] to resolve — fresh on *every* [`JetQuicEndpoint::connect`] call, not
/// once and cached, so reconnects naturally pick up any DNS change (IP rotation,
/// failover, load-balancer updates) instead of being stuck with whatever address was
/// first resolved. Purely data: resolving it is [`JetQuicEndpoint`]'s (and the
/// process-global [`DnsResolver`]'s) job, not this type's.
#[derive(Debug)]
pub enum ServerAddr {
    SocketAddr(SocketAddr),
    /// A hostname to resolve, and the port to pair the resolved address with. Kept as
    /// two separate fields rather than one combined `"host:port"` string specifically
    /// so there's no ambiguity about whether a port is embedded in the string — the
    /// port is never part of what gets resolved, just carried through to the result.
    /// `None` defaults to 443, the standard TLS port.
    Named {
        host: String,
        port: Option<u16>,
    },
}

impl From<SocketAddr> for ServerAddr {
    fn from(addr: SocketAddr) -> Self {
        Self::SocketAddr(addr)
    }
}

/// Splits at the last `:` to separate an optional port from the host — e.g.
/// `"jet.example.com:8443"` becomes `host = "jet.example.com"`, `port = Some(8443)`.
/// If there's no `:`, or what follows it isn't a valid `u16`, the whole string is taken
/// as the host with no port (defaults to 443 — see [`ServerAddr::Named`]). Splitting at
/// the *last* `:` means this only makes sense for a hostname, never a literal IPv6
/// address (which is all colons) — use [`ServerAddr::SocketAddr`]/`From<SocketAddr>` for
/// those instead.
impl From<String> for ServerAddr {
    fn from(addr: String) -> Self {
        match addr.rsplit_once(':') {
            Some((host, port)) if let Ok(port) = port.parse() => Self::Named {
                host: host.to_owned(),
                port: Some(port),
            },
            _ => Self::Named {
                host: addr,
                port: None,
            },
        }
    }
}

/// The port [`ServerAddr::Named`] resolves to when none is given — the standard TLS
/// port.
pub const DEFAULT_SERVER_PORT: u16 = 443;
