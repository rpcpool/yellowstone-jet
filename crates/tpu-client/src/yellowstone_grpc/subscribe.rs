use {
    futures::{Stream, future::BoxFuture},
    std::{
        error::Error as StdError,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    },
    yellowstone_grpc_client::{
        GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult, GeyserStream,
    },
    yellowstone_grpc_proto::{
        geyser::{SubscribeRequest, SubscribeUpdate},
        tonic::Status,
    },
};

/// Delay before the first reconnect attempt after the inner stream dies.
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(500);
/// Upper bound on the exponential reconnect delay.
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

/// Produces a fresh stream for [`AutoReconnectStream`] to fall back on whenever
/// the current one dies (error or graceful close).
pub trait Connector {
    type Item;
    type StreamError: StdError + Send + Sync + 'static;
    type ConnectError: StdError + Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::Item, Self::StreamError>> + Unpin + Send + 'static;
    type ConnectFut: Future<Output = Result<Self::Stream, Self::ConnectError>> + Send + 'static;

    fn connect(&mut self) -> Self::ConnectFut;
}

#[derive(Debug, thiserror::Error)]
pub enum AutoReconnectStreamError<SE, CE>
where
    SE: StdError + 'static,
    CE: StdError + 'static,
{
    #[error(transparent)]
    Stream(SE),
    #[error(transparent)]
    Reconnect(CE),
}

/// Stream wrapper that transparently reconnects via `C: Connector` whenever the
/// current stream errors or ends, with exponential backoff between attempts.
pub struct AutoReconnectStream<C: Connector> {
    connector: C,
    inner: Option<C::Stream>,
    pending_reconnect: Option<BoxFuture<'static, Result<C::Stream, C::ConnectError>>>,
    current_reconnect_attempt: usize,
}

impl<C: Connector> AutoReconnectStream<C> {
    pub fn new(connector: C, inner: C::Stream) -> Self {
        Self {
            connector,
            inner: Some(inner),
            pending_reconnect: None,
            current_reconnect_attempt: 0,
        }
    }

    /// Exponential backoff (capped) applied between reconnect attempts.
    fn reconnect_delay(attempt: usize) -> Duration {
        let millis = RECONNECT_INITIAL_DELAY
            .as_millis()
            .saturating_mul(1u128 << attempt.min(16))
            .min(RECONNECT_MAX_DELAY.as_millis());
        Duration::from_millis(millis as u64)
    }

    /// Schedules a fresh `connect()` call, replacing any prior pending reconnect.
    fn start_reconnect(&mut self) {
        let delay = Self::reconnect_delay(self.current_reconnect_attempt);
        self.current_reconnect_attempt += 1;
        let connect = self.connector.connect();

        self.pending_reconnect = Some(Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            connect.await
        }));
    }
}

impl<C> Stream for AutoReconnectStream<C>
where
    C: Connector + Unpin,
{
    type Item = Result<C::Item, AutoReconnectStreamError<C::StreamError, C::ConnectError>>;

    /// State machine with two states:
    /// - `inner` is `Some` -> poll it for items
    /// - `pending_reconnect` is `Some` -> poll the reconnect future
    ///
    /// Any error or graceful close observed on `inner` means it is dead, so a
    /// brand new stream is established from scratch via the connector.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        loop {
            if let Some(mut stream) = me.inner.take() {
                match Pin::new(&mut stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => {
                        me.inner = Some(stream);
                        return Poll::Ready(Some(Ok(item)));
                    }
                    Poll::Ready(Some(Err(err))) => {
                        me.start_reconnect();
                        return Poll::Ready(Some(Err(AutoReconnectStreamError::Stream(err))));
                    }
                    Poll::Ready(None) => {
                        me.start_reconnect();
                        continue;
                    }
                    Poll::Pending => {
                        me.inner = Some(stream);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(mut fut) = me.pending_reconnect.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        me.inner = Some(stream);
                        me.current_reconnect_attempt = 0;
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        me.start_reconnect();
                        return Poll::Ready(Some(Err(AutoReconnectStreamError::Reconnect(err))));
                    }
                    Poll::Pending => {
                        me.pending_reconnect = Some(fut);
                        return Poll::Pending;
                    }
                }
            }

            unreachable!(
                "AutoReconnectStream must have either an active stream or a pending reconnect"
            );
        }
    }
}

/// [`Connector`] that re-subscribes to a Yellowstone Geyser endpoint with a fixed request.
#[derive(Clone)]
pub struct GeyserConnector {
    pub(crate) client: GeyserGrpcClient,
    pub(crate) request: SubscribeRequest,
}

impl Connector for GeyserConnector {
    type Item = SubscribeUpdate;
    type StreamError = Status;
    type ConnectError = GeyserGrpcClientError;
    type Stream = GeyserStream;
    type ConnectFut = BoxFuture<'static, GeyserGrpcClientResult<GeyserStream>>;

    fn connect(&mut self) -> Self::ConnectFut {
        let mut client = self.client.clone();
        let request = self.request.clone();
        Box::pin(async move { client.subscribe_once(request).await })
    }
}

pub type AutoReconnectGeyserStream = AutoReconnectStream<GeyserConnector>;
pub type AutoReconnectGeyserStreamError = AutoReconnectStreamError<Status, GeyserGrpcClientError>;

pub struct AutoReconnectGeyserClient {
    inner: GeyserGrpcClient,
}

impl AutoReconnectGeyserClient {
    pub const fn new(inner: GeyserGrpcClient) -> Self {
        Self { inner }
    }

    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> GeyserGrpcClientResult<AutoReconnectGeyserStream> {
        let stream = self.inner.subscribe_once(request.clone()).await?;
        let connector = GeyserConnector {
            client: self.inner.clone(),
            request,
        };
        Ok(AutoReconnectStream::new(connector, stream))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::{StreamExt, stream},
        std::{
            collections::VecDeque,
            sync::{Arc, Mutex},
        },
    };

    #[derive(Debug, thiserror::Error)]
    #[error("mock stream error: {0}")]
    struct MockStreamError(&'static str);

    #[derive(Debug, thiserror::Error)]
    #[error("mock connect error: {0}")]
    struct MockConnectError(&'static str);

    type MockStream = stream::BoxStream<'static, Result<u32, MockStreamError>>;

    struct ConnectPlan {
        result: Result<Vec<Result<u32, MockStreamError>>, MockConnectError>,
    }

    #[derive(Clone)]
    struct MockConnector {
        plans: Arc<Mutex<VecDeque<ConnectPlan>>>,
        calls: Arc<Mutex<usize>>,
    }

    impl MockConnector {
        fn new(plans: Vec<ConnectPlan>) -> Self {
            Self {
                plans: Arc::new(Mutex::new(plans.into())),
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            *self.calls.lock().expect("calls mutex poisoned")
        }
    }

    impl Connector for MockConnector {
        type Item = u32;
        type StreamError = MockStreamError;
        type ConnectError = MockConnectError;
        type Stream = MockStream;
        type ConnectFut = BoxFuture<'static, Result<Self::Stream, Self::ConnectError>>;

        fn connect(&mut self) -> Self::ConnectFut {
            *self.calls.lock().expect("calls mutex poisoned") += 1;
            let plan = self
                .plans
                .lock()
                .expect("plans mutex poisoned")
                .pop_front()
                .expect("connect() called without a queued plan");
            Box::pin(async move { plan.result.map(|items| stream::iter(items).boxed()) })
        }
    }

    fn make_stream(items: Vec<Result<u32, MockStreamError>>) -> MockStream {
        stream::iter(items).boxed()
    }

    /// Polls `auto.next()` and `tokio::time::advance` concurrently: the first poll of
    /// `next()` registers the pending reconnect's sleep timer, then `advance` fires it,
    /// waking `next()` to resolve. Requires a paused clock (`start_paused = true`).
    async fn advance_and_next(
        auto: &mut AutoReconnectStream<MockConnector>,
        delay: Duration,
    ) -> Option<<AutoReconnectStream<MockConnector> as Stream>::Item> {
        let (item, ()) = tokio::join!(auto.next(), tokio::time::advance(delay));
        item
    }

    #[tokio::test]
    async fn forwards_items_without_reconnecting() {
        let connector = MockConnector::new(vec![]);
        // A stream that never ends on its own, so a well-behaved wrapper must
        // never need to reconnect while it keeps yielding items.
        let never_ending = stream::iter(vec![Ok(1), Ok(2)])
            .chain(stream::pending())
            .boxed();
        let mut auto = AutoReconnectStream::new(connector.clone(), never_ending);

        assert_eq!(auto.next().await.unwrap().unwrap(), 1);
        assert_eq!(auto.next().await.unwrap().unwrap(), 2);
        assert_eq!(connector.call_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_after_stream_error() {
        let connector = MockConnector::new(vec![ConnectPlan {
            result: Ok(vec![Ok(2)]),
        }]);
        let initial = make_stream(vec![Ok(1), Err(MockStreamError("boom"))]);
        let mut auto = AutoReconnectStream::new(connector.clone(), initial);

        assert_eq!(auto.next().await.unwrap().unwrap(), 1);

        let err = auto.next().await.expect("expected an error item");
        assert!(matches!(err, Err(AutoReconnectStreamError::Stream(_))));

        let item = advance_and_next(&mut auto, RECONNECT_MAX_DELAY).await;
        assert_eq!(item.unwrap().unwrap(), 2);
        assert_eq!(connector.call_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_after_graceful_close_without_surfacing_error() {
        let connector = MockConnector::new(vec![ConnectPlan {
            result: Ok(vec![Ok(42)]),
        }]);
        // No error here - the stream just ends.
        let mut auto = AutoReconnectStream::new(connector.clone(), make_stream(vec![Ok(1)]));

        assert_eq!(auto.next().await.unwrap().unwrap(), 1);

        let item = advance_and_next(&mut auto, RECONNECT_MAX_DELAY).await;
        assert_eq!(item.unwrap().unwrap(), 42);
        assert_eq!(connector.call_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn surfaces_reconnect_error_and_retries_with_backoff() {
        let connector = MockConnector::new(vec![
            ConnectPlan {
                result: Err(MockConnectError("still down")),
            },
            ConnectPlan {
                result: Ok(vec![Ok(7)]),
            },
        ]);
        let initial = make_stream(vec![Err(MockStreamError("boom"))]);
        let mut auto = AutoReconnectStream::new(connector.clone(), initial);

        let err = auto.next().await.expect("expected stream error");
        assert!(matches!(err, Err(AutoReconnectStreamError::Stream(_))));

        let err = advance_and_next(&mut auto, RECONNECT_MAX_DELAY)
            .await
            .expect("expected reconnect error");
        assert!(matches!(err, Err(AutoReconnectStreamError::Reconnect(_))));

        let item = advance_and_next(&mut auto, RECONNECT_MAX_DELAY).await;
        assert_eq!(item.unwrap().unwrap(), 7);
        assert_eq!(connector.call_count(), 2);
    }

    #[test]
    fn reconnect_delay_grows_and_caps() {
        assert_eq!(
            AutoReconnectStream::<MockConnector>::reconnect_delay(0),
            RECONNECT_INITIAL_DELAY
        );
        assert_eq!(
            AutoReconnectStream::<MockConnector>::reconnect_delay(1),
            RECONNECT_INITIAL_DELAY * 2
        );
        assert_eq!(
            AutoReconnectStream::<MockConnector>::reconnect_delay(100),
            RECONNECT_MAX_DELAY
        );
    }
}
