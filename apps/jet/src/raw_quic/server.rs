//! The accept loop itself, plus the (intentionally light) Phase B transaction
//! ingestion: one transaction per unidirectional QUIC stream, raw bytes, no
//! length-prefix framing — the stream's FIN is the message boundary, mirroring how
//! `crates/tpu-client` already writes transactions on the outbound side.

use {
    super::{
        client_identity::ClientIdentity,
        connection_limiter::{ConnectionLimiter, ConnectionPermit},
    },
    crate::{
        metrics::jet as metrics, payload::JetRpcSendTransactionConfig,
        transaction_handler::TransactionHandler,
    },
    bytes::Bytes,
    rustls::pki_types::CertificateDer,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    std::future::Future,
    tracing::{debug, info, warn},
    uuid::Uuid,
    yellowstone_jet_tpu_client::core::PACKET_DATA_SIZE,
};

pub(super) async fn accept_loop(
    endpoint: quinn::Endpoint,
    tx_handler: TransactionHandler,
    connection_limiter: Option<ConnectionLimiter>,
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
                let connection_limiter = connection_limiter.clone();
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(connection) => {
                            metrics::raw_quic_connections_inc("success");
                            match admit_connection(&connection, connection_limiter.as_ref()) {
                                Admission::Allowed { identity, _permit } => {
                                    handle_connection(connection, tx_handler, identity).await;
                                }
                                Admission::Rejected => {
                                    metrics::raw_quic_connections_inc("rejected_per_client_limit");
                                    debug!(
                                        remote = %connection.remote_address(),
                                        "raw quic connection rejected: client already at its \
                                         concurrent connection limit"
                                    );
                                    connection.close(1u32.into(), b"too many concurrent connections");
                                }
                            }
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

/// Whether a freshly-accepted connection may proceed. `identity` is extracted (at most)
/// once per connection here and handed back to the caller so [`handle_connection`] can
/// reuse it for logging instead of re-parsing the certificate.
enum Admission {
    Allowed {
        identity: Option<ClientIdentity>,
        _permit: Option<ConnectionPermit>,
    },
    Rejected,
}

/// No limiter configured, or no client identity to key one by (shouldn't happen since
/// client auth is mandatory, but fails open rather than blocking a connection the TLS
/// layer already authenticated) both allow the connection through unconditionally.
fn admit_connection(
    connection: &quinn::Connection,
    connection_limiter: Option<&ConnectionLimiter>,
) -> Admission {
    let identity = peer_identity(connection);
    let Some(limiter) = connection_limiter else {
        return Admission::Allowed {
            identity,
            _permit: None,
        };
    };
    let Some(identity) = identity else {
        warn!(
            remote = %connection.remote_address(),
            "raw quic connection has no client identity to rate-limit against; allowing it through"
        );
        return Admission::Allowed {
            identity: None,
            _permit: None,
        };
    };
    match limiter.try_acquire(identity.clone()) {
        Some(permit) => Admission::Allowed {
            identity: Some(identity),
            _permit: Some(permit),
        },
        None => Admission::Rejected,
    }
}

/// Streams on one connection are handled one at a time, sequentially — deliberately
/// *not* `tokio::spawn`ed per stream. Concurrent/multiplexed stream handling adds
/// scheduling overhead per stream and can increase tail latency; this repo's outbound
/// sender (`crates/tpu-client/src/core.rs`) makes the same choice for the same reason
/// (see its `ConnectingTask`/send-loop comments). A slow client only ever hurts its own
/// connection's throughput, never another connection's latency, since each connection
/// already runs on its own task.
async fn handle_connection(
    connection: quinn::Connection,
    tx_handler: TransactionHandler,
    identity: Option<ClientIdentity>,
) {
    let peer = identity.as_ref().map(ClientIdentity::to_string);
    info!(
        remote = %connection.remote_address(),
        client_identity = peer.as_deref().unwrap_or("unknown"),
        "raw quic connection established"
    );

    loop {
        match connection.accept_uni().await {
            Ok(stream) => {
                handle_stream(stream, tx_handler.clone(), identity.as_ref()).await;
            }
            Err(error) => {
                debug!(%error, client_identity = peer.as_deref().unwrap_or("unknown"), "raw quic connection closed");
                break;
            }
        }
    }
}

async fn handle_stream(
    mut stream: quinn::RecvStream,
    tx_handler: TransactionHandler,
    identity: Option<&ClientIdentity>,
) {
    let peer = identity.map(ClientIdentity::to_string);
    let bytes = match stream.read_to_end(PACKET_DATA_SIZE).await {
        Ok(bytes) => bytes,
        Err(error) => {
            metrics::raw_quic_tx_received_inc("error");
            warn!(%error, client_identity = peer.as_deref().unwrap_or("unknown"), "failed to read raw quic transaction stream");
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

    // TODO log this x_request_id somewhere so we can correlate it with the transaction signature in the logs, and/or return it to the client in a response
    let x_subscription_id = identity.map(|c| c.subscription_id);
    match tx_handler
        .handle_raw_transaction(Bytes::from(bytes), config, None)
        .await
    {
        Ok(signature) => {
            metrics::raw_quic_tx_received_inc("success");
        }
        Err(error) => {
            metrics::raw_quic_tx_received_inc("error");
            warn!(%error, client_identity = peer.as_deref().unwrap_or("unknown"), "raw quic transaction rejected");
        }
    }
}

/// The client's jet identity, extracted from the leaf certificate's Subject
/// Alternative Name (see [`ClientIdentity`]) -- used both for logging/metrics
/// attribution and as the [`ConnectionLimiter`] key.
fn peer_identity(connection: &quinn::Connection) -> Option<ClientIdentity> {
    let identity = connection.peer_identity()?;
    let certs = identity.downcast_ref::<Vec<CertificateDer<'static>>>()?;
    let leaf = certs.first()?;
    ClientIdentity::from_leaf_certificate(leaf)
}
