//!
//! Generates random `TpuSenderResponse` values and feeds them through `HttpTxnTraceDrain`, so you
//! can visually verify what actually gets HTTP POSTed by the trace-drain pipeline.
//!
//! Requires an explicit `--url` endpoint that accepts `JSONEachRow` ndjson over HTTP.
//!
use {
    clap::Parser,
    futures::{StreamExt, stream},
    rand::Rng,
    solana_keypair::Signature,
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, time::Duration},
    url::Url,
    uuid::Uuid,
    yellowstone_jet::{
        setup_tracing,
        transactions::JetTxnInfo,
        txn_tracing_drain::{
            Credentials, HttpTxnTraceDrain, HttpTxnTraceDrainConfig, XHeaderEntry,
        },
    },
    yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxnInfo, TxFailed, TxSent},
};

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Generates random TpuSenderResponse values into HttpTxnTraceDrain so you can verify what gets HTTP POSTed."
)]
struct Args {
    /// URL to POST the ndjson trace payloads to.
    #[clap(
        long,
        default_value = "http://localhost:8123/?query=INSERT%20INTO%20jet.txn_trace%20FORMAT%20JSONEachRow"
    )]
    url: Url,

    /// Number of random responses to generate. Use 0 to generate forever (Ctrl-C to stop).
    #[clap(long, default_value_t = 0)]
    count: u64,

    /// Minimum number of responses generated per burst.
    #[clap(long, default_value_t = 1)]
    burst_min: u64,

    /// Maximum number of responses generated per burst.
    #[clap(long, default_value_t = 2000)]
    burst_max: u64,

    /// Delay between each generated batch, in milliseconds.
    #[clap(long, default_value_t = 200)]
    interval_ms: u64,

    /// Probability (0.0-1.0) that a generated response is a TxFailed rather than a TxSent.
    ///
    /// TxDrop responses are never generated: `into_txn_trace_entry`'s TxDrop branch is not
    /// implemented yet (`todo!()`) and would panic the drain.
    #[clap(long, default_value_t = 0.3)]
    fail_ratio: f64,

    /// Max ndjson lines buffered before a flush is queued for sending.
    ///
    /// If omitted, defaults to `burst_max` so one generated burst typically becomes one HTTP
    /// payload.
    #[clap(long)]
    max_ndjson_len: Option<usize>,

    /// Max number of HTTP sends in flight at once.
    #[clap(long, default_value_t = 4)]
    max_inflight_sends: usize,

    /// Extra `Name=Value` header to send with every request (repeatable).
    ///
    /// Parsed as a plain `(String, String)` tuple rather than `XHeaderEntry` directly: the
    /// latter doesn't derive `Clone`/`Debug`, which clap's `value_parser` requires.
    #[clap(long = "x-header", value_parser = parse_x_header)]
    x_headers: Vec<(String, String)>,
}

fn parse_x_header(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected `Name=Value`, got `{raw}`"))?;
    Ok((name.to_string(), value.to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing(false).expect("failed to set up tracing");
    let args = Args::parse();

    let credentials = (!args.x_headers.is_empty()).then(|| {
        Credentials::XHeaders(
            args.x_headers
                .into_iter()
                .map(|(name, value)| XHeaderEntry { name, value })
                .collect(),
        )
    });

    let burst_min = args.burst_min.max(1);
    let burst_max = args.burst_max.max(burst_min);
    let max_ndjson_len = args.max_ndjson_len.unwrap_or(burst_max as usize);

    let config = HttpTxnTraceDrainConfig {
        url: args.url,
        credentials,
        max_ndjson_len,
        max_inflight_sends: args.max_inflight_sends,
    };

    let count = args.count;
    let interval = Duration::from_millis(args.interval_ms);
    let fail_ratio = args.fail_ratio.clamp(0.0, 1.0);

    tracing::info!(
        count = if count == 0 { -1 } else { count as i64 },
        burst_min,
        burst_max,
        max_ndjson_len,
        interval_ms = args.interval_ms,
        fail_ratio,
        "generating responses (count = -1 means forever)"
    );

    let source = stream::unfold(0u64, move |generated| async move {
        if count > 0 && generated >= count {
            return None;
        }
        tokio::time::sleep(interval).await;
        let mut rng = rand::rng();
        let burst_len = rng.random_range(burst_min..=burst_max);
        let batch_len = if count == 0 {
            burst_len
        } else {
            (count - generated).min(burst_len)
        };

        let mut batch = Vec::with_capacity(batch_len as usize);
        for _ in 0..batch_len {
            let response = random_response(fail_ratio);
            log_generated(&response);
            batch.push(response);
        }

        Some((batch, generated + batch_len))
    })
    .flat_map(stream::iter)
    .boxed();

    let drain = HttpTxnTraceDrain::with_config(source, config);
    drain.await?;

    tracing::info!("all responses generated and flushed; exiting");
    Ok(())
}

fn random_response(fail_ratio: f64) -> TpuSenderResponse {
    let mut rng = rand::rng();

    let signature = Signature::new_unique();
    let x_request_id = rng.random_bool(0.8).then(Uuid::new_v4);
    let info = Some(TpuSenderTxnInfo::new(JetTxnInfo {
        signature,
        send_at_slot: rng.random_range(1..=u64::MAX),
        x_request_id,
    }));
    let remote_peer_identity = Pubkey::new_unique();
    let remote_peer_addr = SocketAddr::from(([127, 0, 0, 1], rng.random_range(9000..9999)));

    if rng.random_bool(fail_ratio) {
        const FAILURE_REASONS: &[&str] = &[
            "stream reset by peer",
            "connection timed out",
            "remote peer unreachable",
            "TLS handshake failed",
        ];
        let failure_reason =
            FAILURE_REASONS[rng.random_range(0..FAILURE_REASONS.len())].to_string();
        TpuSenderResponse::TxFailed(TxFailed {
            remote_peer_identity,
            remote_peer_addr,
            failure_reason,
            info,
        })
    } else {
        TpuSenderResponse::TxSent(TxSent {
            remote_peer_identity,
            remote_peer_addr,
            info,
        })
    }
}

fn log_generated(response: &TpuSenderResponse) {
    match response {
        TpuSenderResponse::TxSent(r) => {
            tracing::info!(
                remote_peer = %r.remote_peer_identity,
                addr = %r.remote_peer_addr,
                "generated TxSent"
            );
        }
        TpuSenderResponse::TxFailed(r) => {
            tracing::info!(
                remote_peer = %r.remote_peer_identity,
                addr = %r.remote_peer_addr,
                reason = %r.failure_reason,
                "generated TxFailed"
            );
        }
        TpuSenderResponse::TxDrop(_) => unreachable!("random_response never produces TxDrop"),
    }
}
