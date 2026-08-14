//! DNS resolution, decoupled from [`crate::JetQuicEndpoint`] so callers can override it
//! process-wide (caching, a specific DNS-over-HTTPS client, a real async resolver, etc.)
//! instead of being stuck with the bundled [`StdDnsResolver`] — see [`set_resolver`].

use std::{future::Future, net::SocketAddr, pin::Pin, sync::OnceLock};

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("failed to resolve {host:?}: {source}")]
    Io {
        host: String,
        #[source]
        source: std::io::Error,
    },
    #[error("DNS lookup for {0:?} returned no addresses")]
    NoAddressFound(String),
}

/// Resolves a bare hostname to a concrete address. Takes `port` separately rather than
/// a combined `"host:port"` string: the lookup itself only ever concerns the host (a
/// port is never something DNS resolves, just something to carry through to the
/// result), and splitting them removes any ambiguity at the
/// [`ServerAddr::Named`](crate::ServerAddr::Named) call site about whether a port is or
/// isn't embedded in the string.
///
/// A trait — rather than a fixed implementation — specifically so callers can supply
/// their own resolution strategy instead of being stuck with whatever
/// [`StdDnsResolver`] does.
///
/// Uses an associated `Future` type (a generic associated type, parameterized by the
/// borrow's lifetime) rather than returning `Box<dyn Future<...>>`, so a resolve call
/// costs nothing beyond whatever the implementation itself does — no heap allocation,
/// no dynamic dispatch through this trait for anyone implementing it directly. The one
/// place this crate itself needs `dyn` dispatch — the process-global slot behind
/// [`set_resolver`] — pays a `Box::pin` at that boundary instead (an internal, private
/// adapter type, not part of the public API), rather than making this trait
/// `dyn`-compatible at the cost of every implementor's zero-cost future.
pub trait DnsResolver: Send + Sync {
    type Future<'a>: Future<Output = Result<SocketAddr, ResolveError>> + Send + 'a
    where
        Self: 'a;

    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> Self::Future<'a>;
}

/// The default resolver: [`std::net::ToSocketAddrs`], i.e. whatever the OS's own
/// resolver does (`getaddrinfo` on Unix). No caching, and — since it depends on nothing
/// but `std` — no async I/O underneath either: this makes a **blocking** syscall on
/// whatever task polls it. Inject a different [`DnsResolver`] (backed by
/// `tokio::net::lookup_host`, a real async DNS client, etc.) if that blocking matters
/// for your workload.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdDnsResolver;

impl DnsResolver for StdDnsResolver {
    type Future<'a> = std::future::Ready<Result<SocketAddr, ResolveError>>;

    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> Self::Future<'a> {
        use std::net::ToSocketAddrs;
        let result = (host, port)
            .to_socket_addrs()
            .map_err(|source| ResolveError::Io {
                host: host.to_owned(),
                source,
            })
            .and_then(|mut addrs| {
                addrs
                    .next()
                    .ok_or_else(|| ResolveError::NoAddressFound(host.to_owned()))
            });
        std::future::ready(result)
    }
}

/// Object-safe adapter over [`DnsResolver`], used only to let one implementation be
/// stored behind the process-global [`RESOLVER`] slot — [`DnsResolver`] itself is
/// deliberately not `dyn`-compatible (see its docs), so this is the one place in the
/// crate that pays a `Box::pin` per call. That's the tolerable case, not a regression:
/// resolution happens once per [`crate::JetQuicEndpoint::connect`] call, not on any
/// per-message hot path (see the `async-rust-hot-path` skill).
trait ErasedDnsResolver: Send + Sync {
    fn resolve_erased<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, ResolveError>> + Send + 'a>>;
}

impl<T: DnsResolver> ErasedDnsResolver for T {
    fn resolve_erased<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, ResolveError>> + Send + 'a>> {
        Box::pin(DnsResolver::resolve(self, host, port))
    }
}

static RESOLVER: OnceLock<Box<dyn ErasedDnsResolver>> = OnceLock::new();

/// Overrides the process-global [`DnsResolver`] used to resolve every
/// [`ServerAddr::Named`](crate::ServerAddr::Named) address in this process. Defaults to
/// [`StdDnsResolver`] if never called.
///
/// Must be called once, before any resolution has happened yet (typically at startup) —
/// panics if the global resolver has already been installed, whether explicitly by an
/// earlier call to this function or implicitly by an earlier resolve falling back to
/// the default. There is exactly one resolver for the whole process, not one per
/// connector/endpoint — this is a deliberate global, not per-instance configuration.
pub fn set_resolver<R: DnsResolver + 'static>(resolver: R) {
    assert!(
        RESOLVER.set(Box::new(resolver)).is_ok(),
        "jet_quic_client::dns::set_resolver: a DNS resolver is already installed for this process"
    );
}

/// Resolves `host`/`port` through whichever [`DnsResolver`] is currently installed
/// (see [`set_resolver`]), installing the default ([`StdDnsResolver`]) on first use if
/// none was set explicitly.
pub(crate) async fn resolve(host: &str, port: u16) -> Result<SocketAddr, ResolveError> {
    RESOLVER
        .get_or_init(|| Box::new(StdDnsResolver))
        .resolve_erased(host, port)
        .await
}
