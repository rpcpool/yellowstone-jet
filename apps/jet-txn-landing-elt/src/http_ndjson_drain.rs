use {
    bytes::{BufMut, Bytes, BytesMut},
    futures::Sink,
    hyper::{Method, header::CONTENT_TYPE},
    serde::{Deserialize, Serialize},
    std::{
        collections::VecDeque,
        pin::Pin,
        task::{Context, Poll},
    },
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

///
/// A `Sink` that batches whatever is fed into it as NDJSON lines and POSTs them to a
/// Clickhouse HTTP endpoint. Items are pushed in from the outside (e.g. via `SinkExt::send`,
/// `feed`, or `send_all`) rather than pulled from an owned source -- this lets one upstream be
/// split across several `HttpNdJsonDrain`s (one per destination table) without needing a
/// `Stream`-level fanout combinator.
///
pub struct HttpNdJsonSink {
    url: Url,
    credentials: Option<Credentials>,
    client: reqwest::Client,
    ndjson_buffer: BytesMut,
    ndjson_len: usize,
    max_ndjson_len: usize,
    pending_ndjson_payloads: VecDeque<NdjsonPayload>,
    send_joinset: JoinSet<Result<SendOk, reqwest::Error>>,
    max_inflight_sends: usize,
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

pub const DEFAULT_MAX_NDJSON_LEN: usize = 2000;
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

impl HttpNdJsonSink {
    pub fn with_config(config: HttpTxnTraceDrainConfig) -> Self {
        Self {
            url: config.url,
            credentials: config.credentials,
            client: reqwest::Client::new(),
            ndjson_buffer: BytesMut::new(),
            ndjson_len: 0,
            max_ndjson_len: config.max_ndjson_len,
            pending_ndjson_payloads: VecDeque::new(),
            send_joinset: JoinSet::new(),
            // A limit of 0 would mean `poll_ready` could never observe spare capacity and
            // `spawn_send_payload` could never spawn anything, i.e. the sink would accept
            // items forever without ever sending them. Clamp to 1 so that misconfiguration
            // degrades to "send one at a time" instead of a silent stall.
            max_inflight_sends: config.max_inflight_sends.max(1),
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

    ///
    /// Spawns a send for the next queued payload if there's room, then makes progress on
    /// exactly one in-flight send (logging success/failure). Returns `Ready(Ok(()))` if the
    /// caller should re-check its own condition and loop again, `Pending` if there is nothing
    /// further to do until the registered waker fires, or `Ready(Err(_))` on a fatal transport
    /// error.
    ///
    fn poll_advance(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), HttpTxnTraceDrainError>> {
        self.spawn_send_payload()?;
        match self.send_joinset.poll_join_next(cx) {
            Poll::Ready(Some(Ok(Ok(send_ok)))) => {
                let SendOk {
                    send_at_timestamp,
                    ndjson_lines_sent: lines_sent,
                } = send_ok;
                tracing::info!(
                    "Successfully sent {} lines of landed txn payload in {:?}",
                    lines_sent,
                    send_at_timestamp.elapsed()
                );
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(Err(e)))) => {
                if let Some(status) = e.status() {
                    tracing::error!("Failed to send landed txn payload: HTTP status {}", status);
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(HttpTxnTraceDrainError::ReqwestError(e)))
                }
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Err(HttpTxnTraceDrainError::SendTaskFailed(e)))
            }
            // Nothing in flight to wait on. Given `spawn_send_payload` just ran, this only
            // happens when `pending_ndjson_payloads` was already empty too (a non-zero
            // `max_inflight_sends` -- enforced by `with_config` -- would otherwise have spawned
            // it), so the caller's own condition is already satisfied; let it re-check rather
            // than block forever with no waker source.
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Item: Serialize> Sink<Item> for HttpNdJsonSink {
    type Error = HttpTxnTraceDrainError;

    ///
    /// Ready as long as (in-flight sends) + (payloads already queued waiting for a send slot)
    /// stays below `max_inflight_sends` -- treating a queued-but-unspawned payload as consuming
    /// capacity too, so the queue can't grow without bound while sends are slow.
    ///
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        loop {
            let capacity = this
                .max_inflight_sends
                .saturating_sub(this.pending_ndjson_payloads.len());
            if this.send_joinset.len() < capacity {
                return Poll::Ready(Ok(()));
            }
            match this.poll_advance(cx) {
                Poll::Ready(Ok(())) => continue,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        self.get_mut().buffer_entry(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.queue_for_sending_if_any();
        loop {
            if this.pending_ndjson_payloads.is_empty() && this.send_joinset.is_empty() {
                return Poll::Ready(Ok(()));
            }
            match this.poll_advance(cx) {
                Poll::Ready(Ok(())) => continue,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Item>>::poll_flush(self, cx)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, futures::SinkExt, std::task::Waker};

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

    fn test_drain(max_ndjson_buffer_size: usize, max_inflight_sends: usize) -> HttpNdJsonSink {
        HttpNdJsonSink {
            url: Url::parse("http://127.0.0.1:1").unwrap(),
            credentials: None,
            client: reqwest::Client::new(),
            ndjson_buffer: BytesMut::new(),
            ndjson_len: 0,
            max_ndjson_len: max_ndjson_buffer_size,
            pending_ndjson_payloads: VecDeque::new(),
            send_joinset: JoinSet::new(),
            max_inflight_sends: max_inflight_sends.max(1),
        }
    }

    #[tokio::test]
    async fn buffer_entry_appends_one_ndjson_line() {
        let mut drain = test_drain(100, 1);

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
        let mut drain = test_drain(1, 1);

        drain.buffer_entry(entry("sig1", 1, false));

        assert_eq!(
            drain.ndjson_len, 0,
            "buffer should have been queued and reset"
        );
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
        assert_eq!(drain.pending_ndjson_payloads[0].len, 1);
    }

    #[tokio::test]
    async fn queue_for_sending_if_any_moves_buffered_lines_into_a_payload() {
        let mut drain = test_drain(100, 1);
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
        let mut drain = test_drain(100, 1);

        drain.queue_for_sending_if_any();

        assert!(drain.pending_ndjson_payloads.is_empty());
    }

    fn poll_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    #[tokio::test]
    async fn with_config_clamps_zero_max_inflight_sends_to_one() {
        let drain = HttpNdJsonSink::with_config(HttpTxnTraceDrainConfig {
            max_inflight_sends: 0,
            ..Default::default()
        });
        assert_eq!(drain.max_inflight_sends, 1);
    }

    #[tokio::test]
    async fn poll_ready_is_immediately_ready_when_idle() {
        let mut drain = test_drain(100, 2);
        let mut cx = poll_cx();
        assert!(matches!(
            Sink::<LandedTransaction>::poll_ready(Pin::new(&mut drain), &mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[tokio::test]
    async fn poll_ready_blocks_once_inflight_plus_pending_reach_the_limit() {
        let mut drain = test_drain(100, 1);
        drain.pending_ndjson_payloads.push_back(NdjsonPayload {
            data: Bytes::from_static(b"{}\n"),
            len: 1,
        });
        drain
            .send_joinset
            .spawn(std::future::pending::<Result<SendOk, reqwest::Error>>());

        let mut cx = poll_cx();
        assert!(matches!(
            Sink::<LandedTransaction>::poll_ready(Pin::new(&mut drain), &mut cx),
            Poll::Pending
        ));
    }

    #[tokio::test]
    async fn start_send_buffers_without_touching_the_joinset() {
        let mut drain = test_drain(100, 1);

        Pin::new(&mut drain)
            .start_send(entry("sig1", 1, false))
            .unwrap();

        assert_eq!(drain.ndjson_len, 1);
        assert!(drain.send_joinset.is_empty());
    }

    /// Runs `poll_ready`/`poll_flush` on a separate thread and waits (with a bound) for it to
    /// return `Pending`, instead of `.await`-ing it directly: if it regresses into a busy-loop,
    /// it never yields, so an `.await` on the same thread would hang the test runner forever. A
    /// background thread lets the timeout actually fire.
    fn assert_poll_returns_pending_within(
        build: impl FnOnce() -> HttpNdJsonSink + Send + 'static,
        poll: impl FnOnce(
            Pin<&mut HttpNdJsonSink>,
            &mut Context<'_>,
        ) -> Poll<Result<(), HttpTxnTraceDrainError>>
        + Send
        + 'static,
    ) {
        let handle = tokio::runtime::Handle::current();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _guard = handle.enter();
            let mut drain = build();
            let mut cx = poll_cx();
            let result = poll(Pin::new(&mut drain), &mut cx);
            let _ = tx.send(matches!(result, Poll::Pending));
        });

        match rx.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(returned_pending) => assert!(returned_pending, "poll should have returned Pending"),
            Err(_) => panic!(
                "poll did not return within 2s -- it is spinning in a tight loop instead of \
                 yielding, without having registered a waker"
            ),
        }
    }

    #[tokio::test]
    async fn poll_ready_returns_pending_instead_of_spinning_while_a_send_is_in_flight() {
        assert_poll_returns_pending_within(
            || {
                let mut drain = test_drain(100, 1);
                drain.pending_ndjson_payloads.push_back(NdjsonPayload {
                    data: Bytes::from_static(b"{}\n"),
                    len: 1,
                });
                drain
                    .send_joinset
                    .spawn(std::future::pending::<Result<SendOk, reqwest::Error>>());
                drain
            },
            Sink::<LandedTransaction>::poll_ready,
        );
    }

    #[tokio::test]
    async fn poll_flush_returns_pending_instead_of_spinning_while_a_send_is_in_flight() {
        assert_poll_returns_pending_within(
            || {
                let mut drain = test_drain(100, 1);
                drain
                    .send_joinset
                    .spawn(std::future::pending::<Result<SendOk, reqwest::Error>>());
                drain
            },
            Sink::<LandedTransaction>::poll_flush,
        );
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
    fn sink_send_all_delivers_buffered_entries_to_a_real_http_server() {
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
                // request, and max_inflight_sends = 1 forces them to be sent one after another
                // rather than concurrently.
                let mut drain = test_drain(1, 1);
                drain.url = Url::parse(&format!("http://{addr}")).expect("mock server url");

                drain.send(first).await.expect("send sig1");
                drain.send(second).await.expect("send sig2");
                SinkExt::<LandedTransaction>::close(&mut drain)
                    .await
                    .expect("close drain");
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
            Err(_) => {
                panic!("drain (or the mock http server) did not complete within 5s -- likely hung")
            }
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
