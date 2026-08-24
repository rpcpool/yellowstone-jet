//! Raw QUIC transaction ingress: an mTLS-authenticated QUIC listener that receives
//! transactions directly over QUIC streams, alongside jet's existing HTTP/JSON-RPC
//! ingress.
//!
//! [`Server`] follows hyper's `Server` shape: a builder that only deals in trait
//! objects (so it's independently testable with in-memory certs/verifiers, no
//! filesystem or network involved), and `server_with_shutdown` to run the accept loop
//! until an arbitrary shutdown future resolves. Turning on-disk/HTTP config into those
//! trait objects — [`from_config`] — is separate glue code, not part of the builder
//! itself.

pub mod cert_resolver;
pub mod client_identity;
pub mod client_verifier;
pub mod connection_limiter;
mod server;

use {
    self::{
        cert_resolver::{CertResolver, CertResolverError, SelfSignedCertResolver},
        client_verifier::{
            AllowAnyClientVerifier, Allowlist, AllowlistSource, AllowlistSourceError,
            DirAllowlistSource, HttpAllowlistSource, SkipClientVerifier,
        },
        connection_limiter::ConnectionLimiter,
    },
    crate::{
        config::{ConfigClientAllowlistSource, ConfigRawQuicServer},
        metrics::jet as metrics,
        transaction_handler::TransactionHandler,
    },
    quinn::crypto::rustls::{NoInitialCipherSuite, QuicServerConfig},
    rustls::server::{ResolvesServerCert, danger::ClientCertVerifier},
    std::{future::Future, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration},
    tracing::warn,
};

/// ALPN protocol id negotiated with jet's raw QUIC ingress. Must match clients (see the
/// `jet-quic-client` crate).
pub const ALPN_JET_RAW_TX_PROTOCOL_ID: &[u8] = jet_quic_client::ALPN_JET_RAW_TX_PROTOCOL_ID;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
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
pub struct ServerBuilder {
    bind: Option<SocketAddr>,
    cert_resolver: Option<Arc<dyn ResolvesServerCert>>,
    client_verifier: Option<Arc<dyn ClientCertVerifier>>,
    reuse_port: bool,
    connection_limiter: Option<ConnectionLimiter>,
}

impl ServerBuilder {
    /// The address the built [`Server`]'s `Endpoint` binds to. Optional: if never
    /// called, [`Self::build`] defaults to `0.0.0.0:0` (all interfaces, an
    /// OS-assigned ephemeral port).
    pub const fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind = Some(addr);
        self
    }

    /// Injects the strategy used to resolve which certificate the server presents per
    /// connection (e.g. by SNI) -- any [`ResolvesServerCert`] implementation, whether
    /// that's [`CertResolver`]'s hot-reloadable PEM directory or a custom one. Optional:
    /// if never called, [`Self::build`] defaults to [`SelfSignedCertResolver`], a
    /// freshly generated, random self-signed certificate.
    pub fn cert_resolver(mut self, resolver: Arc<dyn ResolvesServerCert>) -> Self {
        self.cert_resolver = Some(resolver);
        self
    }

    /// Injects the strategy used to verify a client's certificate -- e.g. [`Allowlist`]'s
    /// pinned-cert verifier, or [`AllowAnyClientVerifier`] for dev/debug. Optional: if
    /// never called, [`Self::build`] defaults to [`SkipClientVerifier`], which doesn't
    /// even request a client certificate.
    pub fn client_verifier(mut self, verifier: Arc<dyn ClientCertVerifier>) -> Self {
        self.client_verifier = Some(verifier);
        self
    }

    /// Binds the endpoint's socket with `SO_REUSEPORT`, so multiple independent
    /// [`Server`] instances (each with their own socket, endpoint state, and accept
    /// loop) can share the same listen address. The kernel load-balances incoming
    /// datagrams across them, consistently by 4-tuple, so a given connection's packets
    /// keep landing on the same instance.
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

    /// Caps how many concurrent connections a single client identity (account +
    /// subscription, per the client certificate's Subject Alternative Name -- see
    /// [`client_identity::ClientIdentity`]) may hold open at once, across every
    /// endpoint the built [`Server`] accepts on -- see [`ConnectionLimiter`]. Unset by
    /// default: no per-client cap.
    ///
    /// The limiter this constructs is shared by every `Server` built from a clone of
    /// `self` ([`ConnectionLimiter`] is cheap to [`Clone`] -- an `Arc`-backed count map
    /// inside -- so cloning the builder carries the same shared limiter along), so
    /// calling this once on a base builder *before* cloning it once per
    /// [`Self::reuse_port`] worker gives all those workers one shared cap; calling it
    /// again on an already-cloned builder instead creates an independent limiter for
    /// that clone alone.
    pub fn max_connections_per_client(mut self, max: NonZeroUsize) -> Self {
        self.connection_limiter = Some(ConnectionLimiter::new(max));
        self
    }

    /// Builds the [`Server`], binding its `Endpoint` to [`Self::bind`]'s address (or
    /// its default -- see that method). `tx_handler` is the one field every server
    /// needs with no sensible default, so unlike the others it's a required argument
    /// here rather than an optional builder method backed by a runtime check.
    pub fn build(self, tx_handler: TransactionHandler) -> Result<Server, ServerError> {
        let bind = self
            .bind
            .unwrap_or_else(|| SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0)));
        let cert_resolver = match self.cert_resolver {
            Some(resolver) => resolver,
            None => SelfSignedCertResolver::new()? as Arc<dyn ResolvesServerCert>,
        };
        let client_verifier = self
            .client_verifier
            .unwrap_or_else(|| SkipClientVerifier::new() as Arc<dyn ClientCertVerifier>);
        let connection_limiter = self.connection_limiter;

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

        Ok(Server {
            endpoint,
            tx_handler,
            connection_limiter,
        })
    }
}

/// Binds `addr`, optionally with `SO_REUSEPORT` -- mirroring exactly what
/// `quinn::Endpoint::server` itself does internally, plus the reuse-port option quinn
/// doesn't expose.
fn bind_socket(addr: SocketAddr, reuse_port: bool) -> Result<std::net::UdpSocket, ServerError> {
    if !reuse_port {
        return std::net::UdpSocket::bind(addr).map_err(ServerError::Bind);
    }

    #[cfg(unix)]
    {
        use socket2::{Domain, Protocol, Socket, Type};

        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
            .map_err(ServerError::Bind)?;
        socket.set_reuse_port(true).map_err(ServerError::Bind)?;
        if addr.is_ipv6() {
            let _ = socket.set_only_v6(false);
        }
        socket.bind(&addr.into()).map_err(ServerError::Bind)?;
        Ok(socket.into())
    }
    #[cfg(not(unix))]
    {
        Err(ServerError::ReusePortUnsupported)
    }
}

pub struct Server {
    endpoint: quinn::Endpoint,
    tx_handler: TransactionHandler,
    connection_limiter: Option<ConnectionLimiter>,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// Runs the accept loop until `shutdown` resolves, then stops accepting new
    /// connections and lets in-flight ones drain.
    pub async fn server_with_shutdown(self, shutdown: impl Future<Output = ()>) {
        server::accept_loop(
            self.endpoint,
            self.tx_handler,
            self.connection_limiter,
            shutdown,
        )
        .await
    }

    /// Runs the accept loop forever — for callers who manage cancellation externally
    /// (e.g. by aborting the task this is spawned on).
    pub async fn serve(self) {
        self.server_with_shutdown(std::future::pending()).await
    }
}

impl std::future::IntoFuture for Server {
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

/// Turns [`ConfigRawQuicServer`] into `config.workers` bound [`Server`] instances
/// (sharing one address via `SO_REUSEPORT` when `workers > 1`) plus a single
/// [`RawQuicReloadHandle`] — all shards share the same cert resolver/allow-list `Arc`s,
/// so one `reload()` call updates every shard at once. This is the only place that
/// knows how to go from directories/URLs/config to the trait objects the builder
/// itself deals in.
pub async fn from_config(
    config: &ConfigRawQuicServer,
    tx_handler: TransactionHandler,
) -> Result<(Vec<Server>, RawQuicReloadHandle), ServerError> {
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
    let mut builder = Server::builder()
        .bind(config.bind[0])
        .cert_resolver(cert_resolver as Arc<dyn ResolvesServerCert>)
        .client_verifier(client_verifier);
    if worker_count > 1 {
        builder = builder.reuse_port(true);
    }

    let servers = (0..worker_count)
        .map(|_| builder.clone().build(tx_handler.clone()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok((servers, reload_handle))
}

#[cfg(test)]
mod bind_tests {
    use super::*;

    #[tokio::test]
    async fn build_binds_to_the_requested_address() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));

        let server = ServerBuilder::default()
            .bind(addr)
            .cert_resolver(Arc::new(NoCertsResolver) as Arc<dyn ResolvesServerCert>)
            .client_verifier(AllowAnyClientVerifier::new())
            .build(TransactionHandler::new(tx, true))
            .expect("build server");

        assert_eq!(server.local_addr().expect("local addr").ip(), addr.ip());
    }

    #[tokio::test]
    async fn build_defaults_bind_cert_resolver_and_client_verifier() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let server = ServerBuilder::default()
            .build(TransactionHandler::new(tx, true))
            .expect("build server with every default applied");

        assert_eq!(
            server.local_addr().expect("local addr").ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            "bind should default to 0.0.0.0"
        );
    }

    /// A [`ResolvesServerCert`] that never actually needs to resolve anything -- this
    /// test only exercises binding, not a live TLS handshake, so this just satisfies
    /// the builder's required field.
    #[derive(Debug)]
    struct NoCertsResolver;

    impl ResolvesServerCert for NoCertsResolver {
        fn resolve(
            &self,
            _hello: rustls::server::ClientHello<'_>,
        ) -> Option<Arc<rustls::sign::CertifiedKey>> {
            None
        }
    }
}
