use {
    bytes::{BufMut, Bytes, BytesMut},
    futures::{Stream, StreamExt},
    hyper::{Method, header::CONTENT_TYPE},
    serde::{Deserialize, Serialize},
    std::{
        collections::VecDeque,
        time::{Duration, Instant},
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

///
/// Accumulates send outcomes (lines sent, latency, failures) between periodic log lines instead
/// of logging one line per send -- a busy drain sends many small payloads per second, and a log
/// line per send drowns out everything else. [`SendMetricsSummary::maybe_report`] flushes (and
/// resets) the accumulated counters as one summary line once `REPORT_INTERVAL` has elapsed,
/// leaving genuine errors logged immediately (see the call site) since those are rare and worth
/// seeing as they happen, not batched.
///
struct SendMetricsSummary {
    lines_sent: u64,
    successful_sends: u64,
    failed_sends: u64,
    total_latency: Duration,
    last_report: Instant,
}

impl SendMetricsSummary {
    const REPORT_INTERVAL: Duration = Duration::from_secs(5);

    fn new() -> Self {
        Self {
            lines_sent: 0,
            successful_sends: 0,
            failed_sends: 0,
            total_latency: Duration::ZERO,
            last_report: Instant::now(),
        }
    }

    fn record_success(&mut self, latency: Duration, lines_sent: usize) {
        self.lines_sent += lines_sent as u64;
        self.successful_sends += 1;
        self.total_latency += latency;
    }

    const fn record_failure(&mut self) {
        self.failed_sends += 1;
    }

    /// `total_latency` divided by however many *successful* sends contributed to it (failures
    /// don't have a latency to average in), or `Duration::ZERO` if there haven't been any yet --
    /// avoids a division by zero rather than producing a nonsensical average.
    fn avg_latency(&self) -> Duration {
        self.successful_sends
            .try_into()
            .ok()
            .filter(|&n| n > 0)
            .map_or(Duration::ZERO, |n: u32| self.total_latency / n)
    }

    /// Logs and resets the accumulated counters once `REPORT_INTERVAL` has elapsed since the
    /// last report; a no-op (and does *not* reset anything) otherwise, so a burst of activity
    /// within one interval isn't split across multiple partial reports.
    fn maybe_report(&mut self) {
        if self.last_report.elapsed() < Self::REPORT_INTERVAL {
            return;
        }
        let avg_latency = self.avg_latency();
        tracing::info!(
            lines_sent = self.lines_sent,
            successful_sends = self.successful_sends,
            failed_sends = self.failed_sends,
            avg_latency_ms = avg_latency.as_secs_f64() * 1_000.0,
            "ndjson drain send summary (last {:?})",
            Self::REPORT_INTERVAL,
        );
        *self = Self::new();
    }
}

pub struct HttpNdjsonDrain<St> {
    url: Url,
    credentials: Option<Credentials>,
    client: reqwest::Client,
    source: St,
    ndjson_buffer: BytesMut,
    ndjson_len: usize,
    max_ndjson_len: usize,
    pending_ndjson_payloads: VecDeque<NdjsonPayload>,
    send_joinset: JoinSet<Result<(Duration, usize), reqwest::Error>>,
    max_inflight_sends: usize,
    stop: bool,
    send_metrics: SendMetricsSummary,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpNdjsonDrainError {
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
pub struct HttpNdjsonDrainConfig {
    ///
    /// The URL of the HTTP endpoint to which NDJSON batches will be posted.
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

impl Default for HttpNdjsonDrainConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("http://127.0.0.1:8123").unwrap(),
            credentials: None,
            max_ndjson_len: DEFAULT_MAX_NDJSON_LEN,
            max_inflight_sends: DEFAULT_MAX_INFLIGHT_SENDS,
        }
    }
}

impl<St> HttpNdjsonDrain<St> {
    pub fn with_config(source: St, config: HttpNdjsonDrainConfig) -> Self {
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
            send_metrics: SendMetricsSummary::new(),
        }
    }

    fn buffer_item(&mut self, item: &impl Serialize) {
        // Serialize the entry to JSON
        let mut writer = BytesMutWriter {
            bufmut: &mut self.ndjson_buffer,
        };
        match serde_json::to_writer(&mut writer, item) {
            Ok(_) => {
                // Write a newline to separate JSON objects
                self.ndjson_buffer.put_u8(b'\n');
                self.ndjson_len += 1;
                if self.ndjson_len >= self.max_ndjson_len {
                    self.queue_for_sending_if_any();
                }
            }
            Err(e) => {
                tracing::error!("Failed to serialize ndjson entry: {}", e);
            }
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
    fn spawn_send_payload(&mut self) -> Result<(), HttpNdjsonDrainError> {
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
            let started_at = Instant::now();
            self.send_joinset.spawn(async move {
                fut.await?
                    .error_for_status()
                    .map(|_resp| (started_at.elapsed(), ndjson_lines_len))
            });
            prom::inc_total_requests();
        }
        Ok(())
    }
}

impl<St> HttpNdjsonDrain<St>
where
    St: Stream + Unpin,
    St::Item: Unpin,
    for<'a> &'a St::Item: IntoIterator,
    for<'a> <&'a St::Item as IntoIterator>::Item: Serialize,
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
                std::task::Poll::Ready(Some(batch)) => {
                    for item in &batch {
                        self.buffer_item(&item);
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

impl<St> Future for HttpNdjsonDrain<St>
where
    St: Stream + Unpin,
    St::Item: Unpin,
    for<'a> &'a St::Item: IntoIterator,
    for<'a> <&'a St::Item as IntoIterator>::Item: Serialize,
{
    type Output = Result<(), HttpNdjsonDrainError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            this.send_metrics.maybe_report();
            prom::set_inflight_requests(this.send_joinset.len());
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
                std::task::Poll::Ready(Some(Ok(Ok((latency, lines_sent))))) => {
                    // No waker registered.
                    // Successfully sent the payload -- accumulated into the periodic summary
                    // (see `SendMetricsSummary`) rather than logged here, one line per send.
                    this.send_metrics.record_success(latency, lines_sent);
                }
                std::task::Poll::Ready(Some(Ok(Err(e)))) => {
                    if let Some(status) = e.status() {
                        this.send_metrics.record_failure();
                        tracing::error!("Failed to send ndjson payload: HTTP status {}", status);
                    } else {
                        return std::task::Poll::Ready(Err(HttpNdjsonDrainError::ReqwestError(e)));
                    }
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Err(HttpNdjsonDrainError::SendTaskFailed(e)));
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

pub mod prom {
    use prometheus::{IntCounter, IntGauge};

    lazy_static::lazy_static! {
        static ref INFLIGHT_REQUESTS: IntGauge = IntGauge::new(
            "yellowstone_jet_http_txn_trace_drain_inflight_requests",
            "Total number of inflight requests for the HTTP txn trace drain"
        ).unwrap();

        static ref TOTAL_REQUESTS: IntCounter = IntCounter::new(
            "yellowstone_jet_http_txn_trace_drain_total_requests",
            "Total number of requests sent by the HTTP txn trace drain"
        ).unwrap();
    }

    pub(crate) fn set_inflight_requests(len: usize) {
        INFLIGHT_REQUESTS.set(len as i64);
    }

    pub(crate) fn inc_total_requests() {
        TOTAL_REQUESTS.inc();
    }

    pub fn register_metrics(reg: &prometheus::Registry) {
        reg.register(Box::new(INFLIGHT_REQUESTS.clone())).unwrap();
        reg.register(Box::new(TOTAL_REQUESTS.clone())).unwrap();
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
    struct TestItem {
        signature: String,
        slot: u64,
    }

    fn item(signature: &str, slot: u64) -> TestItem {
        TestItem {
            signature: signature.to_string(),
            slot,
        }
    }

    fn test_drain<St>(source: St, max_ndjson_buffer_size: usize) -> HttpNdjsonDrain<St> {
        HttpNdjsonDrain {
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
            send_metrics: SendMetricsSummary::new(),
        }
    }

    #[tokio::test]
    async fn buffer_item_appends_one_ndjson_line() {
        let mut drain = test_drain(stream::empty::<Vec<TestItem>>(), 100);

        drain.buffer_item(&item("sig1", 1));

        assert_eq!(drain.ndjson_len, 1);
        assert!(drain.pending_ndjson_payloads.is_empty());
        let line = String::from_utf8(drain.ndjson_buffer.to_vec()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(
            parsed["signature"],
            serde_json::Value::String("sig1".to_string())
        );
        assert_eq!(parsed["slot"], 1);
    }

    #[tokio::test]
    async fn buffer_item_flushes_once_buffer_reaches_max_size() {
        let mut drain = test_drain(stream::empty::<Vec<TestItem>>(), 1);

        drain.buffer_item(&item("sig1", 1));

        assert_eq!(
            drain.ndjson_len, 0,
            "buffer should have been queued and reset"
        );
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
        assert_eq!(drain.pending_ndjson_payloads[0].len, 1);
    }

    #[tokio::test]
    async fn queue_for_sending_if_any_moves_buffered_lines_into_a_payload() {
        let mut drain = test_drain(stream::empty::<Vec<TestItem>>(), 100);
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
        let mut drain = test_drain(stream::empty::<Vec<TestItem>>(), 100);

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
            type Item = Vec<TestItem>;
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
        // An empty batch never queues a payload, so the loop must run the stream to
        // exhaustion instead of bailing out early to flush.
        let mut drain = test_drain(stream::iter(vec![Vec::<TestItem>::new()]), 100);

        let mut cx = poll_cx();
        assert!(matches!(drain.poll_drain_source(&mut cx), PollDrain::Done));
        assert!(drain.stop);
    }

    #[tokio::test]
    async fn poll_drain_source_stops_early_once_a_response_fills_the_buffer() {
        let first = vec![item("sig1", 1)];
        let second = vec![item("sig2", 2)];
        let mut drain = test_drain(stream::iter(vec![first, second]), 1);

        let mut cx = poll_cx();
        assert!(matches!(
            drain.poll_drain_source(&mut cx),
            PollDrain::NeedFlush
        ));
        // Only the first batch should have been consumed before bailing out to flush.
        assert_eq!(drain.pending_ndjson_payloads.len(), 1);
    }

    /// Runs `poll()` on a separate thread and waits (with a bound) for it to return, instead of
    /// `.await`-ing it directly: if `poll()` regresses into a busy-loop, it never yields, so an
    /// `.await` on the same thread would hang the test runner forever. A background thread lets
    /// the timeout actually fire.
    fn assert_poll_returns_pending_within(
        build: impl FnOnce() -> HttpNdjsonDrain<futures::stream::Pending<Vec<TestItem>>>
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
        // Nothing queued, nothing in flight, source not ready: the JoinSet is empty so polling
        // it registers no waker -- `poll()` must still return `Pending` rather than looping.
        assert_poll_returns_pending_within(|| test_drain(stream::pending::<Vec<TestItem>>(), 100));
    }

    #[tokio::test]
    async fn poll_returns_pending_instead_of_spinning_while_a_send_is_in_flight() {
        assert_poll_returns_pending_within(|| {
            let mut drain = test_drain(stream::pending::<Vec<TestItem>>(), 100);
            // A send is in flight but hasn't completed, and nothing else is queued: the JoinSet
            // poll genuinely registers a waker here, but `poll()` must still return `Pending`
            // instead of looping forever waiting for it to fire.
            drain.send_joinset.spawn(std::future::pending::<
                Result<(Duration, usize), reqwest::Error>,
            >());
            drain
        });
    }

    #[tokio::test]
    async fn poll_returns_pending_instead_of_spinning_with_backlog_and_a_send_in_flight() {
        assert_poll_returns_pending_within(|| {
            let mut drain = test_drain(stream::pending::<Vec<TestItem>>(), 100);
            // A payload is already queued (e.g. capacity was exhausted) *and* a send is in
            // flight: `poll_drain_source`'s fast path returns `AlreadyFlushable` (no new
            // progress), which must not be mistaken for a reason to keep looping.
            drain.pending_ndjson_payloads.push_back(NdjsonPayload {
                data: Bytes::from_static(b"{}\n"),
                len: 1,
            });
            drain.send_joinset.spawn(std::future::pending::<
                Result<(Duration, usize), reqwest::Error>,
            >());
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
    /// socket to a small in-process server, which just records whatever body it receives. This
    /// exercises the whole pipeline (buffering -> queueing -> spawning -> HTTP send -> the fixed
    /// `poll()` loop) rather than any single internal method in isolation.
    ///
    /// Runs on its own dedicated runtime on a background thread, bounded by `recv_timeout`: if
    /// the drain (or the server) ever hangs, this fails fast instead of hanging the test suite.
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

                let first = vec![item("sig1", 1)];
                let second = vec![item("sig2", 2)];
                // max_ndjson_buffer_size = 1 so each batch is flushed (and sent) as its own
                // request, and max_inflight_sends = 1 (from `test_drain`) forces them to be sent
                // one after another rather than concurrently.
                let mut drain = test_drain(stream::iter(vec![first, second]), 1);
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
                assert_eq!(received.len(), 2, "expected one ndjson line per item");
                assert!(received.contains(&serde_json::Value::String("sig1".to_string())));
                assert!(received.contains(&serde_json::Value::String("sig2".to_string())));
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
    - name: X-Other-Header
      value: other-secret
"#;

        let config: HttpNdjsonDrainConfig =
            serde_yaml::from_str(config_str).expect("deserialization should succeed");
        assert_eq!(config.url.as_str(), "http://localhost:8123/");
        assert!(matches!(config.credentials, Some(Credentials::XHeaders(_))));
        let Credentials::XHeaders(credentials) = config.credentials.as_ref().unwrap();
        assert_eq!(credentials[0].name, "X-Api-Key");
        assert_eq!(credentials[0].value, "secret");
        assert_eq!(credentials[1].name, "X-Other-Header");
        assert_eq!(credentials[1].value, "other-secret");
    }

    #[test]
    fn send_metrics_summary_does_not_report_before_the_interval_elapses() {
        let mut metrics = SendMetricsSummary::new();
        metrics.record_success(Duration::from_millis(10), 5);
        metrics.record_failure();

        // Nowhere near `REPORT_INTERVAL` (5s) yet.
        metrics.maybe_report();

        assert_eq!(
            metrics.lines_sent, 5,
            "a no-op report must not reset counters"
        );
        assert_eq!(metrics.successful_sends, 1);
        assert_eq!(metrics.failed_sends, 1);
    }

    #[test]
    fn send_metrics_summary_reports_and_resets_once_the_interval_elapses() {
        let mut metrics = SendMetricsSummary::new();
        metrics.record_success(Duration::from_millis(10), 5);
        metrics.record_success(Duration::from_millis(30), 7);
        metrics.record_failure();
        // Backdate the last report instead of sleeping -- same module, so `last_report` (a
        // private field) is directly visible here.
        metrics.last_report = Instant::now() - SendMetricsSummary::REPORT_INTERVAL;

        metrics.maybe_report();

        assert_eq!(
            metrics.lines_sent, 0,
            "reporting resets the accumulated counters"
        );
        assert_eq!(metrics.successful_sends, 0);
        assert_eq!(metrics.failed_sends, 0);
    }

    #[test]
    fn send_metrics_summary_average_latency_divides_by_successful_sends_only() {
        let mut metrics = SendMetricsSummary::new();
        metrics.record_success(Duration::from_millis(10), 1);
        metrics.record_success(Duration::from_millis(30), 1);
        // A failure contributes no latency and must not dilute the average.
        metrics.record_failure();

        assert_eq!(metrics.avg_latency(), Duration::from_millis(20));
    }

    #[test]
    fn send_metrics_summary_average_latency_is_zero_with_no_successes_yet() {
        let mut metrics = SendMetricsSummary::new();
        metrics.record_failure();

        assert_eq!(metrics.avg_latency(), Duration::ZERO);
    }
}
