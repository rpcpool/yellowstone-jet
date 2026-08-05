//!
//! Generates random `TpuSenderResponse` values and feeds them through `HttpTxnTraceDrain`, so you
//! can visually verify what actually gets HTTP POSTed by the trace-drain pipeline.
//!
//! Requires a `--config` file pointing at an endpoint that accepts `JSONEachRow` ndjson over HTTP.
//!
use {
    clap::Parser,
    futures::{StreamExt, stream},
    rand::Rng,
    serde::Deserialize,
    solana_keypair::Signature,
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, path::PathBuf, time::Duration},
    uuid::Uuid,
    yellowstone_jet::{
        config::load_config,
        setup_tracing,
        transactions::JetTxnInfo,
        txn_trace_drain::{HttpTxnTraceDrain, HttpTxnTraceDrainConfig, SolanaClientResolver},
    },
    yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxnInfo, TxFailed, TxSent},
};

/// No-op resolver: the generated peer identities are random and don't correspond to any real
/// gossip-known client, so there's nothing to resolve.
struct NoopSolanaClientResolver;

impl SolanaClientResolver for NoopSolanaClientResolver {
    fn get_solana_client(&self, _peer_pubkey: &Pubkey) -> Option<String> {
        None
    }
}

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Generates random TpuSenderResponse values into HttpTxnTraceDrain so you can verify what gets HTTP POSTed."
)]
struct Args {
    /// Path to config file
    #[clap(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// URL to POST the ndjson trace payloads to, plus optional auth headers and buffering limits.
    #[serde(flatten)]
    drain: HttpTxnTraceDrainConfig,

    /// Number of random responses to generate. Use 0 to generate forever (Ctrl-C to stop).
    #[serde(default)]
    count: u64,

    /// Minimum number of responses generated per burst.
    #[serde(default = "Config::default_burst_min")]
    burst_min: u64,

    /// Maximum number of responses generated per burst.
    #[serde(default = "Config::default_burst_max")]
    burst_max: u64,

    /// Delay between each generated batch, in milliseconds.
    #[serde(default = "Config::default_interval_ms")]
    interval_ms: u64,

    /// Probability (0.0-1.0) that a generated response is a TxFailed rather than a TxSent.
    ///
    /// TxDrop responses are never generated: `into_txn_trace_entry`'s TxDrop branch is not
    /// implemented yet (`todo!()`) and would panic the drain.
    #[serde(default = "Config::default_fail_ratio")]
    fail_ratio: f64,
}

impl Config {
    const fn default_burst_min() -> u64 {
        1
    }

    const fn default_burst_max() -> u64 {
        2000
    }

    const fn default_interval_ms() -> u64 {
        200
    }

    const fn default_fail_ratio() -> f64 {
        0.3
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing(false).expect("failed to set up tracing");
    let args = Args::parse();
    let config: Config = load_config(&args.config).await?;

    let burst_min = config.burst_min.max(1);
    let burst_max = config.burst_max.max(burst_min);
    let max_ndjson_len = config.drain.max_ndjson_len;

    let count = config.count;
    let interval = Duration::from_millis(config.interval_ms);
    let fail_ratio = config.fail_ratio.clamp(0.0, 1.0);

    tracing::info!(
        count = if count == 0 { -1 } else { count as i64 },
        burst_min,
        burst_max,
        max_ndjson_len,
        interval_ms = config.interval_ms,
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

    let drain = HttpTxnTraceDrain::with_config(source, NoopSolanaClientResolver, config.drain);
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
