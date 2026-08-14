//! The accept loop itself, plus the (intentionally light) Phase B transaction
//! ingestion: one transaction per unidirectional QUIC stream, raw bytes, no
//! length-prefix framing — the stream's FIN is the message boundary, mirroring how
//! `crates/tpu-client` already writes transactions on the outbound side.

use {
    crate::{
        metrics::jet as metrics, payload::JetRpcSendTransactionConfig,
        transaction_handler::TransactionHandler,
    },
    bytes::Bytes,
    rustls::pki_types::CertificateDer,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    std::future::Future,
    tracing::{debug, info, warn},
    yellowstone_jet_tpu_client::core::PACKET_DATA_SIZE,
};

pub(super) async fn accept_loop(
    endpoint: quinn::Endpoint,
    tx_handler: TransactionHandler,
    shutdown: impl Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("raw quic server shutting down, no longer accepting connections");
                break;
            }
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                let tx_handler = tx_handler.clone();
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(connection) => {
                            metrics::raw_quic_connections_inc("success");
                            handle_connection(connection, tx_handler).await;
                        }
                        Err(error) => {
                            metrics::raw_quic_connections_inc("error");
                            metrics::raw_quic_tls_reject_inc();
                            debug!(%error, "raw quic handshake failed");
                        }
                    }
                });
            }
        }
    }
    endpoint.close(0u32.into(), b"shutting down");
    endpoint.wait_idle().await;
}

/// Streams on one connection are handled one at a time, sequentially — deliberately
/// *not* `tokio::spawn`ed per stream. Concurrent/multiplexed stream handling adds
/// scheduling overhead per stream and can increase tail latency; this repo's outbound
/// sender (`crates/tpu-client/src/core.rs`) makes the same choice for the same reason
/// (see its `ConnectingTask`/send-loop comments). A slow client only ever hurts its own
/// connection's throughput, never another connection's latency, since each connection
/// already runs on its own task.
async fn handle_connection(connection: quinn::Connection, tx_handler: TransactionHandler) {
    let peer = peer_fingerprint_hex(&connection);
    info!(
        remote = %connection.remote_address(),
        peer_fingerprint = peer.as_deref().unwrap_or("unknown"),
        "raw quic connection established"
    );

    loop {
        match connection.accept_uni().await {
            Ok(stream) => {
                handle_stream(stream, tx_handler.clone(), peer.clone()).await;
            }
            Err(error) => {
                debug!(%error, peer_fingerprint = peer.as_deref().unwrap_or("unknown"), "raw quic connection closed");
                break;
            }
        }
    }
}

async fn handle_stream(
    mut stream: quinn::RecvStream,
    tx_handler: TransactionHandler,
    peer: Option<String>,
) {
    let bytes = match stream.read_to_end(PACKET_DATA_SIZE).await {
        Ok(bytes) => bytes,
        Err(error) => {
            metrics::raw_quic_tx_received_inc("error");
            warn!(%error, peer_fingerprint = peer.as_deref().unwrap_or("unknown"), "failed to read raw quic transaction stream");
            return;
        }
    };

    let config = JetRpcSendTransactionConfig {
        config: RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        },
        forwarding_policies: vec![],
    };

    match tx_handler
        .handle_raw_transaction(Bytes::from(bytes), config, None)
        .await
    {
        Ok(signature) => {
            metrics::raw_quic_tx_received_inc("success");
            debug!(
                signature,
                peer_fingerprint = peer.as_deref().unwrap_or("unknown"),
                "raw quic transaction accepted"
            );
        }
        Err(error) => {
            metrics::raw_quic_tx_received_inc("error");
            warn!(%error, peer_fingerprint = peer.as_deref().unwrap_or("unknown"), "raw quic transaction rejected");
        }
    }
}

/// Best-effort customer attribution for logs/metrics: the hex-encoded leaf certificate,
/// independent of the (opaque, trait-object) verifier that authenticated it.
fn peer_fingerprint_hex(connection: &quinn::Connection) -> Option<String> {
    let identity = connection.peer_identity()?;
    let certs = identity.downcast_ref::<Vec<CertificateDer<'static>>>()?;
    let leaf = certs.first()?;
    Some(
        super::client_verifier::fingerprint(leaf)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}
