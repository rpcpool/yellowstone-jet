use {
    serde::{Deserialize, Deserializer, de},
    solana_pubkey::Pubkey,
    std::net::SocketAddr,
};

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigQuicTpuPort {
    #[default]
    Normal,
    Forwards,
}

///
/// Specifies how to rewrite TPU addresses for QUIC connections for a specific remote peer.
///
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TpuOverrideInfo {
    ///
    /// The remote peer's public key to overide the TPU address for.
    ///
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub remote_peer: Pubkey,
    ///
    /// The QUIC TPU address to use for the remote peer.
    ///
    pub quic_tpu: SocketAddr,
    ///
    /// The QUIC TPU forward address to use for the remote peer.
    ///
    pub quic_tpu_forward: SocketAddr,
}

fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}
