use {
    quinn::crypto::rustls::QuicServerConfig,
    solana_keypair::Keypair,
    solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID,
    solana_tls_utils::{SkipClientVerification, new_dummy_x509_certificate},
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::Arc,
    },
    yellowstone_jet_tpu_client::core::crypto_provider,
};

#[allow(dead_code)]
pub fn bind_ephemeral_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral udp port");
    socket.local_addr().expect("local addr").port()
}

#[allow(dead_code)]
pub fn generate_random_local_addr() -> SocketAddr {
    SocketAddr::new("127.0.0.1".parse().expect("ipv4"), bind_ephemeral_port())
}

#[allow(dead_code)]
pub fn generate_local_addr_for(ip: [u8; 4]) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), bind_ephemeral_port())
}

#[allow(dead_code)]
pub fn build_random_endpoint(addr: SocketAddr) -> (quinn::Endpoint, Keypair) {
    let kp = Keypair::new();

    let endpoint = build_validator_quic_tpu_endpoint(&kp, addr);

    (endpoint, kp)
}

#[allow(dead_code)]
pub fn build_validator_quic_tpu_endpoint(kp: &Keypair, addr: SocketAddr) -> quinn::Endpoint {
    let (cert, priv_key) = new_dummy_x509_certificate(kp);
    let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()
        .expect("server config build")
        .with_client_cert_verifier(SkipClientVerification::new())
        .with_single_cert(vec![cert], priv_key)
        .expect("quinn server config");
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let quic_server_config = QuicServerConfig::try_from(crypto).expect("quic server config");
    let config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
    quinn::Endpoint::server(config, addr).expect("quinn server endpoint")
}
