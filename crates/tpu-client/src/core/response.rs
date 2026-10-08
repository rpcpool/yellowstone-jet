//! Send outcomes reported back to callers, and the callback trait that receives them.

use {
    crate::core::{
        constants::PACKET_DATA_SIZE,
        txn::{TpuSenderTxn, TpuSenderTxnInfo},
    },
    derive_more::Display,
    quinn::{ConnectionError, VarInt},
    solana_pubkey::Pubkey,
    std::{collections::VecDeque, net::SocketAddr},
};

///
/// Errors that can occur when sending a transaction.
///
#[derive(thiserror::Error, Debug)]
pub enum SendTxError {
    ///
    /// [`ConnectionError`] from quinn when attempting to write to the stream.
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    ///
    /// [`WriteError`](quinn::WriteError) from quinn when attempting to write to the stream.
    ///
    #[error("Failed to send transaction to remote peer {0:?}")]
    StreamStopped(VarInt),
    ///
    /// Stream is closed or reset by the remote peer (dropped or disallow).
    ///
    /// # Note
    ///
    /// Stream may be closed primarly for two reason:
    ///
    /// 1. "dropped" : The remote peer has evicted this connection to make room for other peers.
    /// 2. "disallow" : The remote peer is throttling our IP Address (too many connectino opened from same IP in the last throttling window).
    ///
    #[error("stream is closed or reset by remote peer")]
    StreamClosed,
    ///
    /// 0-RTT rejected by remote peer.
    ///
    /// # Note
    ///
    /// As of Agave v3.0.x, this error should not be raised since agave does not support 0-RTT yet.
    #[error("0-RTT rejected by remote peer")]
    ZeroRttRejected,
}

///
/// Information about a successful transaction send.
///
/// Note: The transaction may still fail to be processed by the remote peer.
/// This struct only indicates that the transaction was successfully sent over quinn's internal stream buffers.
///
#[derive(Debug)]
pub struct TxSent {
    ///
    /// The remote peer identity to which the transaction was sent.
    ///
    pub remote_peer_identity: Pubkey,
    ///
    /// The remote peer socket address to which the transaction was sent.
    ///
    pub remote_peer_addr: SocketAddr,
    ///
    /// Arbitrary information about the transaction send attempt. This can be used to store additional metadata or context about the transaction.
    pub info: Option<TpuSenderTxnInfo>,
}

///
/// Information about a failed transaction send attempt.
///
#[derive(Debug)]
pub struct TxFailed {
    ///
    /// The remote peer identity to which the transaction send failed.
    pub remote_peer_identity: Pubkey,
    ///
    /// The remote peer socket address to which the transaction send failed.
    ///
    pub remote_peer_addr: SocketAddr,
    ///
    /// Low-level reason for the failure.
    ///
    pub failure_reason: String,

    ///
    /// Arbitrary information about the transaction send attempt. This can be used to store additional metadata or context about the transaction.
    pub info: Option<TpuSenderTxnInfo>,
}

///
/// Reason why a transaction was dropped.
///
#[derive(Clone, Debug, Display)]
pub enum TxDropReason {
    ///
    /// The transaction queue for the remote peer reached its maximum capacity.
    ///
    #[display("reached downstream transaction worker transaction queue capacity")]
    RateLimited,
    ///
    /// The remote peer is unreachable via its gossup TPU QUIC contact info.
    ///
    #[display("remote peer is unreachable")]
    RemotePeerUnreachable,
    ///
    /// The internal event loop schedule dropped the transaction due to overload or out-dated information.
    ///
    #[display("tx got drop by driver")]
    DropByDriver,
    ///
    /// The remote peer is being evicted to make room for higher staked connections.
    ///
    #[display("remote peer is being evicted")]
    RemotePeerBeingEvicted,
    ///
    /// The transaction is invalid.
    ///
    #[display("transaction packet size exceeds PACKET_DATA_SIZE ({PACKET_DATA_SIZE} bytes)")]
    InvalidPacketSize,
    ///
    /// The remote peer identity changed.
    ///
    #[display("driver QUIC identity changed")]
    DriverIdentityChanged,
}

impl TxDropReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            TxDropReason::RateLimited => "rate-limited",
            TxDropReason::RemotePeerUnreachable => "remote-peer-unreachable",
            TxDropReason::DropByDriver => "drop-by-driver",
            TxDropReason::RemotePeerBeingEvicted => "remote-conn-peer-being-evicted",
            TxDropReason::InvalidPacketSize => "invalid-packet-size",
            TxDropReason::DriverIdentityChanged => "quic-driver-identity-changed",
        }
    }
}

///
/// Information about dropped transactions.
///
#[derive(Debug)]
pub struct TxDrop {
    ///
    /// The remote peer identity for which the transaction(s) were dropped.
    ///
    pub remote_peer_identity: Pubkey,
    ///
    /// Reason why the transaction(s) were dropped.
    ///
    pub drop_reason: TxDropReason,
    ///
    /// The list of dropped transactions with their attempt count.
    pub dropped_tx_vec: VecDeque<(TpuSenderTxn, usize)>,
}

///
/// Response from the internal TPU sender.
///
#[derive(Debug)]
pub enum TpuSenderResponse {
    /// Transaction sucessfully written to a QUIC STREAM Frame.
    TxSent(TxSent),
    /// Transaction failed to be sent after retries.
    TxFailed(TxFailed),
    /// Transaction(s) dropped before being sent.
    TxDrop(TxDrop),
}

///
/// Callback trait to handle TPU [`TpuSenderResponse`]s.
///
/// # Clone + Safety
///
/// The implementee must be cloneable since each remote peer connection will hold its own instance
/// and call it independently.
///
/// Lastly, the implementation is expected to be thread-safe since the callback can be called from multiple
/// threads.
///
/// # Note
/// A no-op implementation is provided via the [`Nothing`] struct.
///
pub trait TpuSenderResponseCallback: Clone + Send + Sync + 'static {
    fn call(&self, response: TpuSenderResponse);
}

impl<T> TpuSenderResponseCallback for Option<T>
where
    T: TpuSenderResponseCallback,
{
    fn call(&self, response: TpuSenderResponse) {
        if let Some(callback) = self {
            callback.call(response);
        }
    }
}

///
/// A no-op implementation of [`TpuSenderResponseCallback`].
///
#[derive(Debug, Clone)]
pub struct Nothing;

impl TpuSenderResponseCallback for Nothing {
    fn call(&self, _response: TpuSenderResponse) {
        // Do nothing
    }
}
