use {
    futures::{channel::oneshot, future::pending},
    quinn::ReadExactError,
    rand::Rng,
    solana_sdk::{signature::Keypair, signer::Signer},
    solana_streamer::{
        nonblocking::quic::ALPN_TPU_PROTOCOL_ID, tls_certificates::new_dummy_x509_certificate,
    },
    yellowstone_jet::{
        cluster_tpu_info::TpuInfo,
        config::{ConfigQuic, ConfigQuicTpuPort},
        quic_solana::ConnectionCache,
    },
    std::{
        net::{SocketAddr, TcpListener},
        sync::Arc,
    },
    tokio::sync::mpsc,
};

fn find_available_port() -> Option<u16> {
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

fn generate_random_local_addr() -> SocketAddr {
    let port = find_available_port().expect("port");
    SocketAddr::new("127.0.0.1".parse().expect("ipv4"), port)
}

pub fn build_random_endpoint(addr: SocketAddr) -> (quinn::Endpoint, Keypair) {
    let kp = Keypair::new();
    let (cert, priv_key) = new_dummy_x509_certificate(&kp);
    let mut crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(Arc::new(solana_streamer::quic::SkipClientVerification))
        .with_single_cert(vec![cert], priv_key)
        .expect("quinn server config");
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];
    let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = quinn::Endpoint::server(config, addr).expect("quinn server endpoint");

    (endpoint, kp)
}

#[tokio::test]
async fn send_buffer_should_land_properly() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let config = default_config_quic();
    let (quic_session, _identity_map) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let (start_tx, start_rx) = oneshot::channel::<()>();
    let h1 = tokio::spawn(async move {
        let _ = start_rx.await;
        let buf = "helloworld".as_bytes();
        let tpu_info = TpuInfo {
            leader: Keypair::new().pubkey(),
            slots: [0, 1, 2, 3],
            quic: None,
            quic_forwards: None,
        };
        quic_session
            .send_buffer(rx_server_addr, buf, &tpu_info)
            .await
            .expect("send buffer");
        pending::<()>().await;
    });

    let (client_tx, mut client_rx) = mpsc::channel(100);
    let h2 = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let conn = connecting.await.expect("quinn connection");
        let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
            .expect("get remote pubkey");
        let mut rx = conn.accept_uni().await.expect("accept uni");
        let mut buf = vec![0; 10];
        rx.read_exact(&mut buf).await.expect("read");
        client_tx.send((remote_key, buf)).await.expect("send");
    });

    start_tx.send(()).expect("send");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    assert_eq!(actual_remote_key, connection_cache_kp.pubkey());
    assert_eq!(msg, "helloworld");
    h2.await.expect("h2");
    h1.abort();
}

#[tokio::test]
async fn test_update_identity() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);
    let config = default_config_quic();
    let (quic_session, identity_map) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());

    let (start_tx, start_rx) = oneshot::channel::<()>();
    let mut identity_observer = identity_map.observe_identity_change();
    tokio::spawn(async move {
        let _ = start_rx.await;
        loop {
            let buf = "helloworld".as_bytes();
            let tpu_info = TpuInfo {
                leader: Keypair::new().pubkey(),
                slots: [0, 1, 2, 3],
                quic: None,
                quic_forwards: None,
            };
            quic_session
                .send_buffer(rx_server_addr, buf, &tpu_info)
                .await
                .expect("send buffer");
            let _ = identity_observer.observe().await;
        }
    });

    let (client_tx, mut client_rx) = mpsc::channel(100);
    tokio::spawn(async move {
        loop {
            let connecting = rx_server_endpoint.accept().await.expect("accept");
            let client_tx = client_tx.clone();
            tokio::spawn(async move {
                let conn = connecting.await.expect("quinn connection");
                let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
                    .expect("get remote pubkey");

                let mut rx = conn.accept_uni().await.expect("accept uni");

                let mut buf = vec![0; 10];
                rx.read_exact(&mut buf).await.expect("read");
                client_tx.send((remote_key, buf)).await.expect("send");
            });
        }
    });

    start_tx.send(()).expect("send");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    let current_identity1 = identity_map.get_identity().await;
    assert_eq!(actual_remote_key, current_identity1);
    assert_eq!(msg, "helloworld");
    let new_kp = Keypair::new();
    identity_map.update_keypair(&new_kp).await;
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    let current_identity2 = identity_map.get_identity().await;
    assert!(current_identity1 != current_identity2);
    assert_eq!(actual_remote_key, current_identity2);
    assert_eq!(msg, "helloworld");
}

#[tokio::test]
async fn update_identity_should_drop_all_connections() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);
    let config = default_config_quic();
    let (quic_session, identity_map) =
        ConnectionCache::new(config, connection_cache_kp.insecure_clone());

    let (start_tx, start_rx) = oneshot::channel::<()>();
    let mut identity_observer = identity_map.observe_identity_change();
    tokio::spawn(async move {
        let _ = start_rx.await;
        loop {
            let buf = "helloworld".as_bytes();
            let tpu_info = TpuInfo {
                leader: Keypair::new().pubkey(),
                slots: [0, 1, 2, 3],
                quic: None,
                quic_forwards: None,
            };
            quic_session
                .send_buffer(rx_server_addr, buf, &tpu_info)
                .await
                .expect("send buffer");
            let _ = identity_observer.observe().await;
        }
    });

    let (client_tx, mut client_rx) = mpsc::channel(100);
    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let client_tx = client_tx.clone();
        let conn = connecting.await.expect("quinn connection");
        let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
            .expect("get remote pubkey");

        let mut rx = conn.accept_uni().await.expect("accept uni");

        let mut buf = vec![0; 10];
        rx.read_exact(&mut buf).await.expect("read");
        client_tx.send((remote_key, buf)).await.expect("send");

        // Rereading the connection should fail after the identity is updated
        let mut buf2 = vec![0; 10];
        rx.read_exact(&mut buf2).await
    });

    start_tx.send(()).expect("send");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    let current_identity1 = identity_map.get_identity().await;
    assert_eq!(actual_remote_key, current_identity1);
    assert_eq!(msg, "helloworld");
    let new_kp = Keypair::new();
    identity_map.update_keypair(&new_kp).await;
    let maybe = client_rx.recv().await;
    assert!(maybe.is_none());
    let result = rx_server_handle.await.expect("rx_server_handle");
    assert!(matches!(result, Err(ReadExactError::FinishedEarly)));
}

pub fn default_config_quic() -> ConfigQuic {
    ConfigQuic {
        connection_max_pools: ConfigQuic::default_connection_max_pools(),
        connection_pool_size: ConfigQuic::default_connection_pool_size(),
        send_retry_count: ConfigQuic::default_send_retry_count(),
        tpu_port: ConfigQuicTpuPort::Normal,
        connection_handshake_timeout: ConfigQuic::default_connection_handshake_timeout(),
        max_idle_timeout: ConfigQuic::default_max_idle_timeout(),
        keep_alive_interval: ConfigQuic::default_keep_alive_interval(),
        send_timeout: ConfigQuic::default_send_timeout(),
        endpoint_port_range: ConfigQuic::default_endpoint_port_range(),
        send_max_concurrent_streams: ConfigQuic::default_send_max_concurrent_streams(),
        extra_tpu_forward: vec![],
    }
}