//! QUIC connection establishment to remote TPU peers.

use {
    crate::{
        core::constants::{QUIC_KEEP_ALIVE, QUIC_SEND_FAIRNESS},
        identity::TpuIdentity,
    },
    quinn::{ClientConfig, Connection, ConnectionError, Endpoint, IdleTimeout, TransportConfig},
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, sync::Arc, time::Duration},
    tokio::sync::Notify,
};

#[derive(thiserror::Error, Debug)]
pub(crate) enum ConnectingError {
    #[error(transparent)]
    ConnectError(#[from] quinn::ConnectError),
    #[error(transparent)]
    ConnectionError(#[from] quinn::ConnectionError),
}

///
/// A task to connect to a remote peer.
///
pub(crate) struct ConnectingTask {
    pub(crate) remote_peer_identity: Pubkey,
    pub(crate) identity: TpuIdentity,
    pub(crate) max_idle_timeout: Duration,
    pub(crate) connection_timeout: Duration,
    pub(crate) wait_for_eviction: Option<Arc<Notify>>,
    pub(crate) endpoint: Endpoint,
}

/// Translate a SocketAddr into a valid SNI for the purposes of QUIC connection
///
/// We do not actually check if the server holds a cert for this server_name
/// since Solana does not rely on DNS names, but we need to provide a unique
/// one to ensure that we present correct QUIC tokens to the correct server.
///
/// Code taken from <https://github.com/anza-xyz/agave/pull/7260>
pub fn socket_addr_to_quic_server_name(peer: SocketAddr) -> String {
    format!("{}.{}.sol", peer.ip(), peer.port())
}

impl ConnectingTask {
    pub(crate) async fn run(
        self,
        remote_peer_addr: SocketAddr,
    ) -> Result<Connection, ConnectingError> {
        if let Some(signal) = &self.wait_for_eviction {
            tracing::trace!(
                "Waiting for eviction to complete before connecting to remote peer: {}",
                self.remote_peer_identity
            );
            signal.notified().await;
            tracing::trace!(
                "Eviction completed, proceeding to connect to remote peer: {}",
                self.remote_peer_identity
            );
        }

        let transport_config = {
            let mut res = TransportConfig::default();

            let max_idle_timeout = IdleTimeout::try_from(self.max_idle_timeout)
                .expect("Failed to set QUIC max idle timeout");
            res.max_idle_timeout(Some(max_idle_timeout));
            res.keep_alive_interval(Some(QUIC_KEEP_ALIVE));
            // We don't want fairness : https://github.com/quinn-rs/quinn/pull/2002
            // Fairness use round-robin scheduling to write stream data into the next frame.
            // Disabling fairness makes that once a stream starts to write it won't be interrupted by round-robin.
            // This reduce the time the receive the (fin) "end" of a transaction, thus reducing latency.
            res.send_fairness(QUIC_SEND_FAIRNESS);
            res
        };

        let mut config = ClientConfig::new(Arc::new(self.identity.insecure_clone()));
        config.transport_config(Arc::new(transport_config));

        let server_name = socket_addr_to_quic_server_name(remote_peer_addr);
        let connecting = self
            .endpoint
            .connect_with(config, remote_peer_addr, server_name.as_str())
            .map_err(ConnectingError::ConnectError)?;

        tracing::trace!(
            "Connecting to remote peer: {} at address: {}",
            self.remote_peer_identity,
            remote_peer_addr,
        );
        let conn = tokio::time::timeout(self.connection_timeout, connecting)
            .await
            .map_err(|_| ConnectingError::ConnectionError(ConnectionError::TimedOut))??;

        Ok(conn)
    }
}
