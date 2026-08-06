use {
    bytes::{BufMut, Bytes, BytesMut},
    futures::{Stream, StreamExt},
    hyper::{Method, header::CONTENT_TYPE},
    serde::{Deserialize, Serialize},
    std::collections::VecDeque,
    tokio::task::JoinSet,
    url::Url,
};

#[derive(Debug, Clone, Deserialize)]
pub struct XHeaderEntry {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum Credentials {
    XHeaders(Vec<XHeaderEntry>),
}

impl<'de> Deserialize<'de> for Credentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CredentialsVisitor;

        impl<'de> serde::de::Visitor<'de> for CredentialsVisitor {
            type Value = Credentials;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map with a single key `x-headers`")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| serde::de::Error::custom("expected a single key"))?;

                if key == "x-headers" {
                    Ok(Credentials::XHeaders(map.next_value()?))
                } else {
                    Err(serde::de::Error::unknown_variant(&key, &["x-headers"]))
                }
            }
        }

        deserializer.deserialize_map(CredentialsVisitor)
    }
}

struct NdjsonPayload {
    data: Bytes,
    len: usize,
}

impl From<NdjsonPayload> for reqwest::Body {
    fn from(payload: NdjsonPayload) -> Self {
        reqwest::Body::from(payload.data)
    }
}

struct SendOk {
    send_at_timestamp: std::time::Instant,
    ndjson_lines_sent: usize,
}

pub struct HttpTxnTraceDrain<St> {
    url: Url,
    credentials: Option<Credentials>,
    client: reqwest::Client,
    source: St,
    ndjson_buffer: BytesMut,
    ndjson_len: usize,
    max_ndjson_len: usize,
    pending_ndjson_payloads: VecDeque<NdjsonPayload>,
    send_joinset: JoinSet<Result<SendOk, reqwest::Error>>,
    max_inflight_sends: usize,
    stop: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpTxnTraceDrainError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    SendTaskFailed(#[from] tokio::task::JoinError),
}

struct BytesMutWriter<'buf> {
    bufmut: &'buf mut BytesMut,
}

impl std::io::Write for BytesMutWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bufmut.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum PollDrain {
    Pending,
    NeedFlush,
    ///
    /// A payload was already queued from a previous round; the source was not polled this call,
    /// so no new progress was made (unlike `NeedFlush`, which means the source just produced
    /// data that filled the buffer).
    ///
    AlreadyFlushable,
    Done,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpTxnTraceDrainConfig {
    ///
    /// The URL of the HTTP endpoint to which landed transactions will be sent. This should be an
    /// HTTP endpoint that accepts POST requests with an NDJSON body payload.
    pub url: Url,
    pub credentials: Option<Credentials>,
    ///
    /// The maximum number of NDJSON lines to buffer before sending a request.
    /// Once this limit is reached, the buffered data will be sent to the configured HTTP endpoint.
    ///
    /// # Note
    ///
    /// This is a soft limit. If the buffer is not full but the source stream is pending, the buffered data will be sent to avoid excessive latency.
    #[serde(default = "default_max_ndjson_len")]
    pub max_ndjson_len: usize,
    ///
    /// The maximum number of concurrent POST requests that can be in-flight at any given time. default to 10
    #[serde(default = "default_max_inflight_sends")]
    pub max_inflight_sends: usize,
}

pub const DEFAULT_MAX_NDJSON_LEN: usize = 1000;
pub const DEFAULT_MAX_INFLIGHT_SENDS: usize = 10;

const fn default_max_ndjson_len() -> usize {
    DEFAULT_MAX_NDJSON_LEN
}

const fn default_max_inflight_sends() -> usize {
    DEFAULT_MAX_INFLIGHT_SENDS
}

impl Default for HttpTxnTraceDrainConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("http://127.0.0.1:8123").unwrap(),
            credentials: None,
            max_ndjson_len: DEFAULT_MAX_NDJSON_LEN,
            max_inflight_sends: DEFAULT_MAX_INFLIGHT_SENDS,
        }
    }
}

impl<St> HttpTxnTraceDrain<St> {
    pub fn with_config(source: St, config: HttpTxnTraceDrainConfig) -> Self {
        Self {
            url: config.url,
            credentials: config.credentials,
            client: reqwest::Client::new(),
            source,
            ndjson_buffer: BytesMut::new(),
            ndjson_len: 0,
            max_ndjson_len: config.max_ndjson_len,
            pending_ndjson_payloads: VecDeque::new(),
            send_joinset: JoinSet::new(),
            max_inflight_sends: config.max_inflight_sends,
            stop: false,
        }
    }

    fn queue_for_sending_if_any(&mut self) {
        if self.ndjson_len > 0 {
            let payload = self.ndjson_buffer.split().freeze();
            self.pending_ndjson_payloads.push_back(NdjsonPayload {
                data: payload,
                len: self.ndjson_len,
            });
            self.ndjson_len = 0;
        }
    }

    ///
    /// Spawns a task to send the next payload in the queue, if any, and if the number of inflight sends is below the maximum.
    ///
    fn spawn_send_payload(&mut self) -> Result<(), HttpTxnTraceDrainError> {
        if self.send_joinset.len() >= self.max_inflight_sends {
            return Ok(());
        }

        if let Some(payload) = self.pending_ndjson_payloads.pop_front() {
            let request_builder = self.client.request(Method::POST, self.url.as_str());
            let request_builder = match &self.credentials {
                Some(Credentials::XHeaders(headers)) => {
                    headers.iter().fold(request_builder, |builder, header| {
                        builder.header(&header.name, &header.value)
                    })
                }
                None => request_builder,
            };
            let ndjson_lines_len = payload.len;
            let request = request_builder
                .body(payload)
                .header(CONTENT_TYPE, "application/x-ndjson")
                .build()?;
            let client = self.client.clone();
            let fut = client.execute(request);
            let now = std::time::Instant::now();
            let ok = SendOk {
                send_at_timestamp: now,
                ndjson_lines_sent: ndjson_lines_len,
            };
            self.send_joinset
                .spawn(async move { fut.await?.error_for_status().map(|_resp| ok) });
        }
        Ok(())
    }

    fn buffer_entry<T: Serialize>(&mut self, entry: T) {
        let mut writer = BytesMutWriter {
            bufmut: &mut self.ndjson_buffer,
        };
        match serde_json::to_writer(&mut writer, &entry) {
            Ok(_) => {
                self.ndjson_buffer.put_u8(b'\n');
                self.ndjson_len += 1;
                if self.ndjson_len >= self.max_ndjson_len {
                    self.queue_for_sending_if_any();
                }
            }
            Err(e) => {
                tracing::error!("Failed to serialize LandedTransaction: {}", e);
            }
        }
    }
}

impl<St> HttpTxnTraceDrain<St>
where
    St: Stream + Unpin,
    St::Item: IntoIterator,
    <St::Item as IntoIterator>::Item: Serialize,
{
    fn poll_drain_source(&mut self, cx: &mut std::task::Context<'_>) -> PollDrain {
        if !self.pending_ndjson_payloads.is_empty() {
            return PollDrain::AlreadyFlushable;
        }
        // Once the source has signaled completion, never poll it again: many `Stream`
        // implementations (e.g. `futures::stream::unfold`) panic if polled after returning
        // `None`, and there is nothing more to read regardless.
        if self.stop {
            return PollDrain::Done;
        }
        loop {
            match self.source.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(item)) => {
                    for entry in item {
                        self.buffer_entry(entry);
                    }
                    if !self.pending_ndjson_payloads.is_empty() {
                        return PollDrain::NeedFlush;
                    }
                }
                std::task::Poll::Ready(None) => {
                    self.stop = true;
                    return PollDrain::Done;
                }
                std::task::Poll::Pending => return PollDrain::Pending,
            }
        }
    }
}

impl<St> Future for HttpTxnTraceDrain<St>
where
    St: Stream + Unpin,
    St::Item: IntoIterator,
    <St::Item as IntoIterator>::Item: Serialize,
{
    type Output = Result<(), HttpTxnTraceDrainError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let mut is_drainable = false;
            match this.poll_drain_source(cx) {
                PollDrain::NeedFlush => {
                    // The source was just polled and produced data that filled the buffer:
                    // genuine new progress, so no waker was registered for it and it's worth
                    // looping to see if more is synchronously available.
                    is_drainable = true;
                }
                PollDrain::AlreadyFlushable => {
                    // A payload was already queued from a previous round and the source was not
                    // touched this call -- no new progress here. Whether to keep looping is
                    // entirely up to the JoinSet outcome below.
                }
                PollDrain::Done => {
                    this.queue_for_sending_if_any();
                    // Mark the drain as stopped, but continue to flush any remaining payloads.
                }
                PollDrain::Pending => {
                    // When pending is returned, the waker has been registered.
                    // Note: we don't actually wait to build the biggest payload possible,
                    // we just flush when the buffer is full or when the source is pending.
                    // This make data available to the consumer faster.
                    this.queue_for_sending_if_any();
                }
            }

            this.spawn_send_payload()?;

            match this.send_joinset.poll_join_next(cx) {
                std::task::Poll::Ready(Some(Ok(Ok(send_ok)))) => {
                    let SendOk {
                        send_at_timestamp,
                        ndjson_lines_sent: lines_sent,
                    } = send_ok;
                    // No waker registered.
                    // Successfully sent the payload
                    let send_latency = send_at_timestamp.elapsed();
                    tracing::info!(
                        "Successfully sent {} lines of landed txn payload in {:?}",
                        lines_sent,
                        send_latency
                    );
                }
                std::task::Poll::Ready(Some(Ok(Err(e)))) => {
                    if let Some(status) = e.status() {
                        tracing::error!(
                            "Failed to send landed txn payload: HTTP status {}",
                            status
                        );
                    } else {
                        return std::task::Poll::Ready(Err(HttpTxnTraceDrainError::ReqwestError(
                            e,
                        )));
                    }
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Err(HttpTxnTraceDrainError::SendTaskFailed(e)));
                }
                std::task::Poll::Ready(None) => {
                    // The JoinSet is empty, so `poll_join_next` did NOT register a waker
                    // (there is nothing in flight to wait on).
                    if this.stop && this.pending_ndjson_payloads.is_empty() {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    if !is_drainable {
                        // Nothing in flight and nothing new happened this round: we're
                        // purely waiting on the source, whose waker was already registered
                        // by `poll_drain_source` (we only get here via its `Done` or
                        // `Pending` case).
                        return std::task::Poll::Pending;
                    }
                }
                std::task::Poll::Pending => {
                    // The JoinSet is non-empty and registered a waker for the next task
                    // completion.
                    if !is_drainable {
                        return std::task::Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::stream,
        std::task::{Context, Waker},
    };

    #[derive(Debug, Clone, Serialize)]
    struct LandedTransaction {
        signature: String,
        slot: u64,
        failed: bool,
    }

    fn entry(signature: &str, slot: u64, failed: bool) -> LandedTransaction {
        LandedTransaction {
            signature: signature.to_string(),
            slot,
            failed,
        }
    }

    fn test_drain<St>(source: St, max_ndjson_buffer_size: usize) -> HttpTxnTraceDrain<St> {
        HttpTxnTraceDrain {
            url: Url::parse("http://127.0.0.1:1").unwrap(),
            credentials: None,
            client: reqwest::Client::new(),
            source,
            ndjson_buffer: BytesMut::new(),
            ndjson_len: 0,
            max_ndjson_len: max_ndjson_buffer_size,
            pending_ndjson_payloads: VecDeque::new(),
            send_joinset: JoinSet::new(),
            max_inflight_sends: 1,
            stop: false,
        }
    }

    #[tokio::test]
    async fn buffer_entry_appends_one_ndjson_line() {
        let mut drain = test_drain(stream::empty::<LandedTransaction>(), 100);

        drain.buffer_entry(entry("sig1", 42, false));

        assert_eq!(drain.ndjson_len, 1);
        assert!(drain.pending_ndjson_payloads.is_empty());
        let line = String::from_utf8(drain.ndjson_buffer.to_vec()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(
            parsed["signature"],
            serde_json::Value::String("sig1".into())
        );
        assert_eq!(parsed["slot"], 42);
        assert_eq!(parsed["failed"], false);
    }

    #[tokio::test]
    async fn buffer_entry_flushes_once_buffer_reaches_max_size() {
        let mut drain = test_drain(stream::empty::<LandedTransaction>(), 1);

        drain.buffer_entry(&entry("sig1", 1, false));

        assert_eq!(
            drain.ndjson_len, 0,
            "buffer should have been queued and reset"
        );
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
        assert_eq!(drain.pending_ndjson_payloads[0].len, 1);
    }

    #[tokio::test]
    async fn queue_for_sending_if_any_moves_buffered_lines_into_a_payload() {
        let mut drain = test_drain(stream::empty::<LandedTransaction>(), 100);
        drain.ndjson_buffer.extend_from_slice(b"{}\n");
        drain.ndjson_len = 1;

        drain.queue_for_sending_if_any();

        assert_eq!(drain.ndjson_len, 0);
        assert!(drain.ndjson_buffer.is_empty());
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
        assert_eq!(drain.pending_ndjson_payloads[0].len, 1);
    }

    #[tokio::test]
    async fn queue_for_sending_if_any_is_noop_on_empty_buffer() {
        let mut drain = test_drain(stream::empty::<LandedTransaction>(), 100);

        drain.queue_for_sending_if_any();

        assert!(drain.pending_ndjson_payloads.is_empty());
    }

    fn poll_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    #[tokio::test]
    async fn poll_drain_source_reports_already_flushable_without_polling_when_already_pending() {
        struct PanicsIfPolled;
        impl Stream for PanicsIfPolled {
            type Item = Vec<LandedTransaction>;
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                panic!("source should not be polled while a payload is already pending");
            }
        }

        let mut drain = test_drain(PanicsIfPolled, 100);
        drain.pending_ndjson_payloads.push_back(NdjsonPayload {
            data: Bytes::from_static(b"{}\n"),
            len: 1,
        });

        let mut cx = poll_cx();
        assert!(matches!(
            drain.poll_drain_source(&mut cx),
            PollDrain::AlreadyFlushable
        ));
    }

    #[tokio::test]
    async fn poll_drain_source_drains_the_stream_and_reports_done() {
        let mut drain = test_drain(stream::empty::<Vec<LandedTransaction>>(), 100);

        let mut cx = poll_cx();
        assert!(matches!(drain.poll_drain_source(&mut cx), PollDrain::Done));
        assert!(drain.stop);
    }

    #[tokio::test]
    async fn poll_drain_source_stops_early_once_a_response_fills_the_buffer() {
        let first = entry("sig1", 1, false);
        let second = entry("sig2", 2, false);
        // Each poll of the outer stream yields its own single-entry batch, so filling the
        // buffer on the first batch bails out before the second batch is ever polled.
        let mut drain = test_drain(stream::iter(vec![vec![first], vec![second]]), 1);

        let mut cx = poll_cx();
        assert!(matches!(
            drain.poll_drain_source(&mut cx),
            PollDrain::NeedFlush
        ));
        // Only the first item should have been consumed before bailing out to flush.
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
    }

    /// Runs `poll()` on a separate thread and waits (with a bound) for it to return, instead of
    /// `.await`-ing it directly: if `poll()` regresses into a busy-loop, it never yields, so an
    /// `.await` on the same thread would hang the test runner forever. A background thread lets
    /// the timeout actually fire.
    fn assert_poll_returns_pending_within(
        build: impl FnOnce() -> HttpTxnTraceDrain<futures::stream::Pending<Vec<LandedTransaction>>>
        + Send
        + 'static,
    ) {
        let handle = tokio::runtime::Handle::current();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _guard = handle.enter();
            let mut drain = build();
            let mut cx = poll_cx();
            let result = std::pin::Pin::new(&mut drain).poll(&mut cx);
            let _ = tx.send(matches!(result, std::task::Poll::Pending));
        });

        match rx.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(returned_pending) => assert!(
                returned_pending,
                "poll() should have returned Poll::Pending"
            ),
            Err(_) => panic!(
                "poll() did not return within 2s -- it is spinning in a tight loop instead of \
                 yielding, without having registered a waker"
            ),
        }
    }

    #[tokio::test]
    async fn poll_returns_pending_instead_of_spinning_when_idle() {
        assert_poll_returns_pending_within(|| {
            test_drain(stream::pending::<Vec<LandedTransaction>>(), 100)
        });
    }

    #[tokio::test]
    async fn poll_returns_pending_instead_of_spinning_while_a_send_is_in_flight() {
        assert_poll_returns_pending_within(|| {
            let mut drain = test_drain(stream::pending::<Vec<LandedTransaction>>(), 100);
            drain
                .send_joinset
                .spawn(std::future::pending::<Result<SendOk, reqwest::Error>>());
            drain
        });
    }

    #[tokio::test]
    async fn poll_returns_pending_instead_of_spinning_with_backlog_and_a_send_in_flight() {
        assert_poll_returns_pending_within(|| {
            let mut drain = test_drain(stream::pending::<Vec<LandedTransaction>>(), 100);
            drain.pending_ndjson_payloads.push_back(NdjsonPayload {
                data: Bytes::from_static(b"{}\n"),
                len: 1,
            });
            drain
                .send_joinset
                .spawn(std::future::pending::<Result<SendOk, reqwest::Error>>());
            drain
        });
    }

    /// Reads a minimal HTTP/1.1 request off `socket` and returns its body. Good enough for a
    /// single-purpose local test server -- not a general-purpose HTTP parser.
    async fn read_http_request_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt as _;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let n = socket.read(&mut chunk).await.expect("read request headers");
            assert!(n > 0, "connection closed before headers were fully read");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let headers = String::from_utf8_lossy(&buf[..header_end]);
        let content_length: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0);

        while buf.len() < header_end + content_length {
            let n = socket.read(&mut chunk).await.expect("read request body");
            assert!(n > 0, "connection closed before body was fully read");
            buf.extend_from_slice(&chunk[..n]);
        }

        buf[header_end..header_end + content_length].to_vec()
    }

    /// End-to-end test: a real `reqwest::Client` sending real HTTP requests over a real TCP
    /// socket to a small in-process server, which just records whatever body it receives.
    #[test]
    fn future_poll_sends_buffered_entries_to_a_real_http_server() {
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("build dedicated runtime");
            let outcome = runtime.block_on(async {
                use tokio::io::AsyncWriteExt as _;

                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind mock http server");
                let addr = listener.local_addr().expect("local_addr");
                let (body_tx, mut body_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

                let server = tokio::spawn(async move {
                    loop {
                        let Ok((mut socket, _)) = listener.accept().await else {
                            break;
                        };
                        let body_tx = body_tx.clone();
                        tokio::spawn(async move {
                            let body = read_http_request_body(&mut socket).await;
                            let _ = body_tx.send(body);
                            let _ = socket
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                                )
                                .await;
                            let _ = socket.shutdown().await;
                        });
                    }
                });

                let first = entry("sig1", 1, false);
                let second = entry("sig2", 2, true);
                // max_ndjson_buffer_size = 1 so each entry is flushed (and sent) as its own
                // request, and max_inflight_sends = 1 (from `test_drain`) forces them to be sent
                // one after another rather than concurrently.
                let mut drain = test_drain(stream::iter(vec![vec![first], vec![second]]), 1);
                drain.url = Url::parse(&format!("http://{addr}")).expect("mock server url");

                drain.await.expect("drain future should complete successfully");
                server.abort();

                let mut received = Vec::new();
                while let Ok(body) = body_rx.try_recv() {
                    for line in body.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
                        received.push(
                            serde_json::from_slice::<serde_json::Value>(line)
                                .expect("valid ndjson line")["signature"]
                                .clone(),
                        );
                    }
                }

                received
            });
            let _ = result_tx.send(outcome);
        });

        match result_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(received) => {
                assert_eq!(received.len(), 2, "expected one ndjson line per entry");
                assert!(received.contains(&serde_json::Value::String("sig1".into())));
                assert!(received.contains(&serde_json::Value::String("sig2".into())));
            }
            Err(_) => panic!(
                "drain future (or the mock http server) did not complete within 5s -- likely hung"
            ),
        }
    }

    #[test]
    fn deser_config() {
        let config_str = r#"
url: http://localhost:8123
credentials:
  x-headers:
    - name: X-Api-Key
      value: secret
"#;

        let config: HttpTxnTraceDrainConfig =
            serde_yaml::from_str(config_str).expect("deserialization should succeed");
        assert_eq!(config.url.as_str(), "http://localhost:8123/");
        assert!(matches!(config.credentials, Some(Credentials::XHeaders(_))));
        let Credentials::XHeaders(credentials) = config.credentials.as_ref().unwrap();
        assert_eq!(credentials[0].name, "X-Api-Key");
        assert_eq!(credentials[0].value, "secret");
    }
}
