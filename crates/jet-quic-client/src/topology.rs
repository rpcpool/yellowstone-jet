//! Generated client stub for `JetQuicServiceDiscovery` (see
//! `proto/jet-topology.proto`) — the topology feed jet endpoints use to discover each
//! other. Server code is deliberately not generated (see `build.rs`): this crate is a
//! client only.

#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::missing_const_for_fn)]

use {
    futures::Stream,
    std::{
        future::Future,
        net::SocketAddr,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        service::{Interceptor, interceptor::InterceptedService},
        transport::{Channel, Endpoint},
    },
};

tonic::include_proto!("jet_topology");

/// How often a [`GrpcJetQuicServiceDiscoveryStream`] pings the server to keep the
/// subscription alive and detect a silently dead connection.
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// How long to wait before retrying after a connect/stream failure. This is a
/// low-traffic, low-bandwidth stream, so there's no need for anything fancier
/// (jittered backoff, etc.) — a fixed delay is enough to avoid hammering the server.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Outbound requests are queued through a small buffered channel rather than sent
/// directly, so a slow/backed-up connection can't block the ping timer from firing.
const OUTBOUND_CHANNEL_CAPACITY: usize = 4;

/// How many *consecutive* transient connect failures to tolerate before giving up and
/// treating the streak itself as fatal. Transient failures are individually worth
/// retrying, but an unbroken run of them means retrying isn't actually helping — at
/// `RECONNECT_DELAY` apart, this is a little over 3 minutes of silent retries before
/// surfacing anything to the caller.
const MAX_CONSECUTIVE_CONNECT_FAILURES: u32 = 10;

type TopologyClient = jet_quic_service_discovery_client::JetQuicServiceDiscoveryClient<
    InterceptedService<Channel, XTokenInterceptor>,
>;

/// Attaches an `x-token` metadata header to every request, if one was configured.
#[derive(Clone)]
struct XTokenInterceptor {
    x_token: Option<String>,
}

impl Interceptor for XTokenInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(x_token) = &self.x_token {
            let value = x_token.parse().map_err(|_| {
                tonic::Status::invalid_argument("x-token is not a valid header value")
            })?;
            request.metadata_mut().insert("x-token", value);
        }
        Ok(request)
    }
}

/// A live `SubscribeTopology` call: the inbound update stream, and the sender half of
/// the outbound request stream (used to queue pings on it).
struct Session {
    inbound: tonic::Streaming<TopologyUpdate>,
    outbound_tx: mpsc::Sender<TopologyRequest>,
    ping_interval: tokio::time::Interval,
    next_ping_id: u64,
}

/// Connects `client` and opens the `SubscribeTopology` bidi stream.
async fn connect(mut client: TopologyClient) -> Result<Session, tonic::Status> {
    let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);

    let response = client
        .subscribe_topology(ReceiverStream::new(outbound_rx))
        .await?;
    let inbound = response.into_inner();

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval()`'s first tick fires immediately; skip that so we don't ping right
    // after just having connected.
    ping_interval.reset();

    Ok(Session {
        inbound,
        outbound_tx,
        ping_interval,
        next_ping_id: 0,
    })
}

/// Waits [`RECONNECT_DELAY`] before attempting [`connect`] again — used for every
/// reconnect after the first (which connects immediately, no delay).
async fn reconnect(client: TopologyClient) -> Result<Session, tonic::Status> {
    tokio::time::sleep(RECONNECT_DELAY).await;
    connect(client).await
}

/// Whether `status` is a plausibly-transient failure — a transport-level hiccup or the
/// server temporarily unable to handle the request — worth silently retrying, as
/// opposed to one that will never succeed no matter how many times it's retried (bad
/// credentials, a request the server rejects as malformed, an unimplemented method,
/// etc.). `Unknown` is included on the transient side because tonic surfaces plain
/// transport/IO failures (a refused connection, a reset TCP stream) under that code
/// when there's no real gRPC status to report.
fn is_transient(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Aborted
            | tonic::Code::ResourceExhausted
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

type ConnectFuture = Pin<Box<dyn Future<Output = Result<Session, tonic::Status>> + Send>>;

enum State {
    Connecting(ConnectFuture),
    // Boxed: `Session` is far larger than `ConnectFuture`, and state transitions here
    // are rare (a reconnect at most every few seconds) — not a hot path, so the
    // allocation is a non-issue (see the `async-rust-hot-path` skill).
    Streaming(Box<Session>),
    /// A non-transient error was hit. Terminal: every poll from here on returns
    /// `None` — see the [`Stream`] impl's docs on why an `Err` is always this
    /// stream's last item.
    Done,
}

/// A self-reconnecting [`Stream`] of [`JetQuicTopology`] snapshots from jet's topology
/// service discovery feed. Handles its own liveness pings and, if the connection drops
/// with a transient failure (a transport hiccup, the server temporarily unavailable),
/// transparently reconnects and resumes with no item surfaced for that failure.
///
/// A *non*-transient failure (anything that won't be fixed by retrying: bad
/// credentials, a malformed request, etc.) is different: this stream follows the rule
/// that once a [`Stream<Item = Result<_, _>>`] yields an `Err`, that `Err` is always
/// its last item — so a non-transient failure is surfaced exactly once and the stream
/// is then permanently done (every subsequent poll returns `None`), rather than
/// silently retrying forever against something that will never succeed.
///
/// A run of transient failures is also bounded — see
/// [`MAX_CONSECUTIVE_CONNECT_FAILURES`] — so a persistent (if individually
/// "transient-shaped") outage still eventually surfaces an error and stops, rather than
/// retrying silently forever.
pub struct GrpcJetQuicServiceDiscoveryStream {
    client: TopologyClient,
    state: State,
    /// Consecutive transient connect failures since the last successful connect —
    /// reset to `0` on every successful (re)connect, checked against
    /// [`MAX_CONSECUTIVE_CONNECT_FAILURES`] on every failure.
    consecutive_connect_failures: u32,
}

impl GrpcJetQuicServiceDiscoveryStream {
    /// Lazily connects to `endpoint` (parsed eagerly here; the actual TCP/TLS
    /// connection is established on first use, and reused/re-established by `tonic`
    /// itself as needed) and opens the topology subscription. `x_token`, if given, is
    /// sent as an `x-token` metadata header on every request.
    pub fn new(endpoint: String, x_token: Option<String>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint)?.connect_lazy();
        let client =
            jet_quic_service_discovery_client::JetQuicServiceDiscoveryClient::with_interceptor(
                channel,
                XTokenInterceptor { x_token },
            );

        let state = State::Connecting(Box::pin(connect(client.clone())));
        Ok(Self {
            client,
            state,
            consecutive_connect_failures: 0,
        })
    }
}

impl Stream for GrpcJetQuicServiceDiscoveryStream {
    type Item = Result<JetQuicTopology, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Nothing here needs pin-projection: `ConnectFuture` is already pinned via
        // `Box`, and everything else (`tonic::Streaming`, `mpsc::Sender`,
        // `tokio::time::Interval`) is `Unpin`.
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Done => return Poll::Ready(None),
                State::Connecting(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(session)) => {
                        this.consecutive_connect_failures = 0;
                        this.state = State::Streaming(Box::new(session));
                    }
                    Poll::Ready(Err(status)) => {
                        if !is_transient(&status) {
                            this.state = State::Done;
                            return Poll::Ready(Some(Err(status)));
                        }

                        this.consecutive_connect_failures += 1;
                        if this.consecutive_connect_failures >= MAX_CONSECUTIVE_CONNECT_FAILURES {
                            this.state = State::Done;
                            return Poll::Ready(Some(Err(tonic::Status::unavailable(format!(
                                "giving up after {} consecutive connection failures; \
                                 last error: {status}",
                                this.consecutive_connect_failures
                            )))));
                        }

                        // Still under the limit — retried silently, nothing surfaced.
                        this.state = State::Connecting(Box::pin(reconnect(this.client.clone())));
                    }
                },
                State::Streaming(session) => {
                    // Best-effort: queue a ping on each tick. If the outbound channel
                    // is gone (stream already broken), the inbound poll below will
                    // observe that and reconnect anyway.
                    if session.ping_interval.poll_tick(cx).is_ready() {
                        let id = session.next_ping_id;
                        session.next_ping_id += 1;
                        let _ = session.outbound_tx.try_send(TopologyRequest {
                            request: Some(topology_request::Request::Ping(Ping { id })),
                        });
                    }

                    match Pin::new(&mut session.inbound).poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        // The server closed the stream cleanly (no error) — treat like
                        // any other disconnect and reconnect.
                        Poll::Ready(None) => {
                            this.state =
                                State::Connecting(Box::pin(reconnect(this.client.clone())));
                        }
                        Poll::Ready(Some(Err(status))) => {
                            if is_transient(&status) {
                                this.state =
                                    State::Connecting(Box::pin(reconnect(this.client.clone())));
                            } else {
                                this.state = State::Done;
                                return Poll::Ready(Some(Err(status)));
                            }
                        }
                        Poll::Ready(Some(Ok(TopologyUpdate { update }))) => match update {
                            Some(topology_update::Update::TopologySnapshot(snapshot)) => {
                                return match JetQuicTopology::try_from(snapshot) {
                                    Ok(topology) => Poll::Ready(Some(Ok(topology))),
                                    // A malformed snapshot isn't a connection problem —
                                    // retrying won't produce a different snapshot, so
                                    // this is non-transient: surface it and stop, same
                                    // as any other fatal error.
                                    Err(error) => {
                                        this.state = State::Done;
                                        Poll::Ready(Some(Err(tonic::Status::invalid_argument(
                                            error.to_string(),
                                        ))))
                                    }
                                };
                            }
                            // Liveness only — nothing to surface, keep polling.
                            Some(topology_update::Update::Pong(_)) | None => {}
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JetQuicNodeInfo {
    pub name: String,
    pub remote_addr: SocketAddr,
    pub stake: u64,
}

#[derive(Debug, Clone)]
pub struct JetQuicTopology {
    pub nodes: Vec<JetQuicNodeInfo>,
}

/// Why a [`JetEndpoint`]/[`TopologySnapshot`] from the wire couldn't be turned into
/// [`JetQuicNodeInfo`]/[`JetQuicTopology`].
#[derive(Debug, thiserror::Error)]
pub enum TopologyConversionError {
    #[error("endpoint {name:?} has an invalid ip address {ip_address:?}: {source}")]
    InvalidIpAddress {
        name: String,
        ip_address: String,
        source: std::net::AddrParseError,
    },
    #[error("endpoint {name:?} has a port ({port}) that doesn't fit in u16")]
    PortOutOfRange { name: String, port: u32 },
}

impl TryFrom<JetEndpoint> for JetQuicNodeInfo {
    type Error = TopologyConversionError;

    fn try_from(endpoint: JetEndpoint) -> Result<Self, Self::Error> {
        let ip = endpoint.ip_address.parse().map_err(|source| {
            TopologyConversionError::InvalidIpAddress {
                name: endpoint.name.clone(),
                ip_address: endpoint.ip_address.clone(),
                source,
            }
        })?;
        let port =
            u16::try_from(endpoint.port).map_err(|_| TopologyConversionError::PortOutOfRange {
                name: endpoint.name.clone(),
                port: endpoint.port,
            })?;

        Ok(Self {
            name: endpoint.name,
            remote_addr: SocketAddr::new(ip, port),
            stake: endpoint.stake,
        })
    }
}

impl TryFrom<TopologySnapshot> for JetQuicTopology {
    type Error = TopologyConversionError;

    fn try_from(snapshot: TopologySnapshot) -> Result<Self, Self::Error> {
        let nodes = snapshot
            .jet_endpoints
            .into_iter()
            .map(JetQuicNodeInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { nodes })
    }
}
