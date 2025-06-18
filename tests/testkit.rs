use {
    quinn::crypto::rustls::QuicServerConfig,
    rand::Rng,
    solana_sdk::signature::Keypair,
    solana_streamer::{
        nonblocking::quic::ALPN_TPU_PROTOCOL_ID, tls_certificates::new_dummy_x509_certificate,
    },
    std::{
        net::{SocketAddr, TcpListener},
        sync::Arc,
    },
    yellowstone_jet::{config::ConfigQuic, crypto_provider::crypto_provider},
};

#[allow(dead_code)]
pub fn find_available_port() -> Option<u16> {
    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        // Try up to 100 times to find an open port
        let (begin, end) = ConfigQuic::default_endpoint_port_range();
        let port = rng.gen_range(begin..=end);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        // Try to bind to the port; if successful, port is free
        if TcpListener::bind(addr).is_ok() {
            return Some(port);
        }
    }

    None // If no port found after 100 attempts, return None
}

#[allow(dead_code)]
pub fn generate_random_local_addr() -> SocketAddr {
    let port = find_available_port().expect("port");
    SocketAddr::new("127.0.0.1".parse().expect("ipv4"), port)
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
        .with_client_cert_verifier(solana_streamer::quic::SkipClientVerification::new())
        .with_single_cert(vec![cert], priv_key)
        .expect("quinn server config");
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let quic_server_config = QuicServerConfig::try_from(crypto).expect("quic server config");
    let config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
    quinn::Endpoint::server(config, addr).expect("quinn server endpoint")
}
