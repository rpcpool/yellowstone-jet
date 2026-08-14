//! Raw QUIC transaction ingress: an mTLS-authenticated QUIC listener that receives
//! transactions directly over QUIC streams, alongside jet's existing HTTP/JSON-RPC
//! ingress.
//!
//! [`RawQuicServer`] follows hyper's `Server` shape: a builder that only deals in
//! trait objects (so it's independently testable with in-memory certs/verifiers, no
//! filesystem or network involved), and `with_shutdown` to run the accept loop until an
//! arbitrary shutdown future resolves. Turning on-disk/HTTP config into those trait
//! objects — [`from_config`] — is separate glue code, not part of the builder itself.

pub mod cert_resolver;
pub mod client_verifier;
mod listener;

use {
    self::{
        cert_resolver::{CertResolver, CertResolverError},
        client_verifier::{
            AllowAnyClientVerifier, Allowlist, AllowlistSource, AllowlistSourceError,
            DirAllowlistSource, HttpAllowlistSource,
        },
    },
    crate::{
        config::{ConfigClientAllowlistSource, ConfigRawQuicServer},
        metrics::jet as metrics,
        transaction_handler::TransactionHandler,
    },
    quinn::crypto::rustls::{NoInitialCipherSuite, QuicServerConfig},
    rustls::server::{ResolvesServerCert, danger::ClientCertVerifier},
    std::{future::Future, net::SocketAddr, sync::Arc, time::Duration},
    tracing::warn,
};

/// ALPN protocol id negotiated with jet's raw QUIC ingress. Must match clients (see the
/// `jet-quic-client` crate).
pub const ALPN_JET_RAW_TX_PROTOCOL_ID: &[u8] = jet_quic_client::ALPN_JET_RAW_TX_PROTOCOL_ID;

#[derive(Debug, thiserror::Error)]
pub enum RawQuicServerError {
    #[error("missing required raw quic server builder field: {0}")]
    MissingField(&'static str),
    #[error("invalid TLS server config: {0}")]
    Tls(#[from] rustls::Error),
    #[error("invalid QUIC server config: {0}")]
    QuicConfig(#[from] NoInitialCipherSuite),
    #[error("failed to bind raw quic endpoint: {0}")]
    Bind(#[from] std::io::Error),
    #[error("failed to load server certificate directory: {0}")]
    CertResolver(#[from] CertResolverError),
    #[error("failed to load client allow-list: {0}")]
    Allowlist(#[from] AllowlistSourceError),
    #[error(
        "SO_REUSEPORT was requested but is not supported on this platform \
         (only unix targets support binding multiple sockets to the same address)"
    )]
    ReusePortUnsupported,
}

#[derive(Clone, Default)]
pub struct RawQuicServerBuilder {
    bind: Option<SocketAddr>,
    cert_resolver: Option<Arc<dyn ResolvesServerCert>>,
    client_verifier: Option<Arc<dyn ClientCertVerifier>>,
    tx_handler: Option<TransactionHandler>,
    reuse_port: bool,
}

impl RawQuicServerBuilder {
    pub const fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind = Some(addr);
        self
    }

    pub fn cert_resolver(mut self, resolver: Arc<dyn ResolvesServerCert>) -> Self {
        self.cert_resolver = Some(resolver);
        self
    }

    pub fn client_verifier(mut self, verifier: Arc<dyn ClientCertVerifier>) -> Self {
        self.client_verifier = Some(verifier);
        self
    }

    pub fn transaction_handler(mut self, handler: TransactionHandler) -> Self {
        self.tx_handler = Some(handler);
        self
    }

    /// Binds the endpoint's socket with `SO_REUSEPORT`, so multiple independent
    /// [`RawQuicServer`] instances (each with their own socket, endpoint state, and
    /// accept loop) can share the same listen address. The kernel load-balances
    /// incoming datagrams across them, consistently by 4-tuple, so a given
    /// connection's packets keep landing on the same instance.
    ///
    /// This exists because every `Connection`/stream/`Incoming` derived from a
    /// `quinn::Endpoint` shares that endpoint's internal state (its connection-ID
    /// table and its one socket) behind synchronization — under high connection or
    /// stream churn that shared state becomes a contention point. Sharding across N
    /// independent endpoints (one per thread/core) removes the cross-shard
    /// contention entirely, at the cost of connection state no longer being visible
    /// across shards. Unix-only (unsupported on Windows); default is `false`.
    pub const fn reuse_port(mut self, reuse_port: bool) -> Self {
        self.reuse_port = reuse_port;
        self
    }

    pub fn build(self) -> Result<RawQuicServer, RawQuicServerError> {
        let bind = self.bind.ok_or(RawQuicServerError::MissingField("bind"))?;
        let cert_resolver = self
            .cert_resolver
            .ok_or(RawQuicServerError::MissingField("cert_resolver"))?;
        let client_verifier = self
            .client_verifier
            .ok_or(RawQuicServerError::MissingField("client_verifier"))?;
        let tx_handler = self
            .tx_handler
            .ok_or(RawQuicServerError::MissingField("transaction_handler"))?;

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut tls_config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(client_verifier)
            .with_cert_resolver(cert_resolver);
        tls_config.alpn_protocols = vec![ALPN_JET_RAW_TX_PROTOCOL_ID.to_vec()];

        let quic_server_config = QuicServerConfig::try_from(tls_config)?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
        let socket = bind_socket(bind, self.reuse_port)?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            socket,
            quinn::default_runtime()
                .expect("no async runtime found (are we inside a Tokio runtime?)"),
        )?;

        Ok(RawQuicServer {
            endpoint,
            tx_handler,
        })
    }
}

/// Plain `UdpSocket::bind` by default; with `SO_REUSEPORT` set first when requested
/// (see [`RawQuicServerBuilder::reuse_port`]). Mirrors exactly what
/// `quinn::Endpoint::server` itself does internally, plus the reuse-port option quinn
/// doesn't expose.
fn bind_socket(
    addr: SocketAddr,
    reuse_port: bool,
) -> Result<std::net::UdpSocket, RawQuicServerError> {
    if !reuse_port {
        return std::net::UdpSocket::bind(addr).map_err(RawQuicServerError::Bind);
    }

    #[cfg(unix)]
    {
        use socket2::{Domain, Protocol, Socket, Type};

        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
            .map_err(RawQuicServerError::Bind)?;
        socket
            .set_reuse_port(true)
            .map_err(RawQuicServerError::Bind)?;
        if addr.is_ipv6() {
            let _ = socket.set_only_v6(false);
        }
        socket
            .bind(&addr.into())
            .map_err(RawQuicServerError::Bind)?;
        Ok(socket.into())
    }
    #[cfg(not(unix))]
    {
        Err(RawQuicServerError::ReusePortUnsupported)
    }
}

pub struct RawQuicServer {
    endpoint: quinn::Endpoint,
    tx_handler: TransactionHandler,
}

impl RawQuicServer {
    pub fn builder() -> RawQuicServerBuilder {
        RawQuicServerBuilder::default()
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// Runs the accept loop until `shutdown` resolves, then stops accepting new
    /// connections and lets in-flight ones drain.
    pub async fn with_shutdown(self, shutdown: impl Future<Output = ()>) {
        listener::accept_loop(self.endpoint, self.tx_handler, shutdown).await
    }

    /// Runs the accept loop forever — for callers who manage cancellation externally
    /// (e.g. by aborting the task this is spawned on).
    pub async fn serve(self) {
        self.with_shutdown(std::future::pending()).await
    }
}

impl std::future::IntoFuture for RawQuicServer {
    type Output = ();
    type IntoFuture = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.serve())
    }
}

/// A cheap, `Arc`-backed handle for forcing an out-of-band cert/allow-list rescan —
/// independent of the accept loop's own lifecycle, usable from both a periodic poll
/// task and the admin RPC's `reloadRawQuicCerts` method.
pub struct RawQuicReloadHandle {
    cert_resolver: Arc<CertResolver>,
    allowlist: Option<Arc<Allowlist>>,
}

pub struct ReloadReport {
    pub cert_reloaded: Result<(), CertResolverError>,
    /// `None` when running in `debug_accept_any_client` mode (no allow-list to reload).
    pub allowlist_result: Option<Result<usize, AllowlistSourceError>>,
}

impl RawQuicReloadHandle {
    pub async fn reload(&self) -> ReloadReport {
        let cert_reloaded = self.cert_resolver.reload();
        if let Err(ref error) = cert_reloaded {
            warn!(%error, "failed to reload raw quic server cert directory");
        }

        let allowlist_result = match &self.allowlist {
            Some(allowlist) => {
                let result = allowlist.reload().await;
                match &result {
                    Ok(len) => {
                        metrics::raw_quic_allowlist_reload_inc("success");
                        metrics::raw_quic_allowlist_size_set(*len);
                    }
                    Err(error) => {
                        warn!(%error, "failed to reload raw quic client allow-list");
                        metrics::raw_quic_allowlist_reload_inc("error");
                        metrics::raw_quic_allowlist_fetch_error_inc();
                    }
                }
                Some(result)
            }
            None => None,
        };

        ReloadReport {
            cert_reloaded,
            allowlist_result,
        }
    }
}

/// Runs `handle.reload()` on a fixed interval until `shutdown` resolves. The first tick
/// is skipped since [`from_config`] already loads everything once up front.
pub async fn poll_reload_loop(
    handle: Arc<RawQuicReloadHandle>,
    interval: Duration,
    shutdown: impl Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                handle.reload().await;
            }
        }
    }
}

/// Turns [`ConfigRawQuicServer`] into `config.workers` bound [`RawQuicServer`]
/// instances (sharing one address via `SO_REUSEPORT` when `workers > 1`) plus a single
/// [`RawQuicReloadHandle`] — all shards share the same cert resolver/allow-list `Arc`s,
/// so one `reload()` call updates every shard at once. This is the only place that
/// knows how to go from directories/URLs/config to the trait objects the builder
/// itself deals in.
pub async fn from_config(
    config: &ConfigRawQuicServer,
    tx_handler: TransactionHandler,
) -> Result<(Vec<RawQuicServer>, RawQuicReloadHandle), RawQuicServerError> {
    let cert_resolver = CertResolver::from_dir(&config.server_cert_dir)?;

    let (client_verifier, allowlist): (Arc<dyn ClientCertVerifier>, Option<Arc<Allowlist>>) =
        if config.debug_accept_any_client {
            warn!(
                "raw quic server starting with debug_accept_any_client=true -- ALL client \
                 certificates will be accepted, the allow-list is bypassed entirely. This must \
                 never be enabled in production."
            );
            (AllowAnyClientVerifier::new(), None)
        } else {
            let source: Box<dyn AllowlistSource> = match &config.client_allowlist {
                ConfigClientAllowlistSource::Dir { path } => {
                    Box::new(DirAllowlistSource::new(path.clone()))
                }
                ConfigClientAllowlistSource::Http { url, timeout } => {
                    Box::new(HttpAllowlistSource::new(url.clone(), *timeout))
                }
            };
            let allowlist = Allowlist::load(source).await?;
            metrics::raw_quic_allowlist_size_set(allowlist.len());
            (allowlist.verifier(), Some(allowlist))
        };

    let reload_handle = RawQuicReloadHandle {
        cert_resolver: Arc::clone(&cert_resolver),
        allowlist,
    };

    let worker_count = config.workers.get();
    let mut builder = RawQuicServer::builder()
        .bind(config.bind[0])
        .cert_resolver(cert_resolver as Arc<dyn ResolvesServerCert>)
        .client_verifier(client_verifier)
        .transaction_handler(tx_handler);
    if worker_count > 1 {
        builder = builder.reuse_port(true);
    }

    let servers = (0..worker_count)
        .map(|_| builder.clone().build())
        .collect::<Result<Vec<_>, _>>()?;

    Ok((servers, reload_handle))
}
