//! TLS settings shared by TPU QUIC clients and test servers.

use rustls::{NamedGroup, crypto::CryptoProvider};

/// This has been copy-pasted from `solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID`
pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";

pub fn crypto_provider() -> CryptoProvider {
    let mut provider = rustls::crypto::ring::default_provider();
    // Disable all key exchange algorithms except X25519
    provider
        .kx_groups
        .retain(|kx| kx.name() == NamedGroup::X25519);
    provider
}
