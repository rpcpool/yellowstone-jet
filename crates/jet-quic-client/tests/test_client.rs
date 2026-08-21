use {
    jet_quic_client::{
        ALPN_JET_RAW_TX_PROTOCOL_ID, ClientIdentity, JetQuicEndpoint, JetQuicSender, ServerAddr,
        ServerVerification, client_config_with_verification, load_cert_pem, load_key_pem,
    },
    quinn::crypto::rustls::QuicServerConfig,
    rcgen::{CertifiedKey, generate_simple_self_signed},
    rustls::{
        DigitallySignedStruct, DistinguishedName, Error as RustlsError, SignatureScheme,
        client::danger::HandshakeSignatureValid,
        crypto::CryptoProvider,
        pki_types::{CertificateDer, UnixTime},
        server::danger::{ClientCertVerified, ClientCertVerifier},
    },
    std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
    tokio::sync::mpsc,
};

/// Accepts any client certificate — this test only exercises the *client's* connecting
/// and sending behavior, not server-side auth (that's covered by `apps/jet`'s raw_quic
/// tests).
#[derive(Debug)]
struct AllowAnyClientVerifier {
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for AllowAnyClientVerifier {
    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        Ok(ClientCertVerified::assertion())
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

/// One received message, tagged with the remote (client-side) address the server saw
/// it arrive from — this is how tests prove distinct connections were actually used.
struct Received {
    from: SocketAddr,
    bytes: Vec<u8>,
}

/// Builds (but doesn't drive) a test server endpoint: accepts any client cert, ALPN
/// matching the real jet raw-QUIC protocol.
fn build_test_endpoint() -> (SocketAddr, quinn::Endpoint) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).expect("self-signed cert");
    let cert_der = load_cert_pem(cert.pem().as_bytes()).expect("cert der");
    let key_der = load_key_pem(key_pair.serialize_pem().as_bytes()).expect("key der");

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = Arc::new(AllowAnyClientVerifier {
        provider: Arc::clone(&provider),
    });
    let mut tls_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert_der], key_der)
        .expect("server cert");
    tls_config.alpn_protocols = vec![ALPN_JET_RAW_TX_PROTOCOL_ID.to_vec()];

    let quic_server_config = QuicServerConfig::try_from(tls_config).expect("quic server config");
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
    let endpoint = quinn::Endpoint::server(
        server_config,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    )
    .expect("bind test server");
    let addr = endpoint.local_addr().expect("local addr");
    (addr, endpoint)
}

/// Minimal test server: accepts connections (any client cert), reads every uni stream
/// on every connection to completion, and forwards each as a `Received` over `tx`.
fn spawn_test_server() -> (SocketAddr, mpsc::UnboundedReceiver<Received>) {
    let (addr, endpoint) = build_test_endpoint();

    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let Ok(connection) = incoming.await else {
                continue;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let remote = connection.remote_address();
                while let Ok(mut stream) = connection.accept_uni().await {
                    if let Ok(bytes) = stream.read_to_end(1024).await {
                        let _ = tx.send(Received {
                            from: remote,
                            bytes,
                        });
                    }
                }
            });
        }
    });

    (addr, rx)
}

/// Same as [`spawn_test_server`], except every accepted connection is closed
/// immediately (simulating a connection that's already dead by the time a client tries
/// to use it) instead of being read from. Sends a notification on the returned channel
/// once a close has happened, so tests can wait past the race instead of guessing.
fn spawn_test_server_closing_connections() -> (SocketAddr, mpsc::UnboundedReceiver<()>) {
    let (addr, endpoint) = build_test_endpoint();

    let (closed_tx, closed_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let Ok(connection) = incoming.await else {
                continue;
            };
            connection.close(0u32.into(), b"simulated dead connection");
            let _ = closed_tx.send(());
        }
    });

    (addr, closed_rx)
}

fn test_customer() -> (
    CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["customer.invalid".to_owned()]).expect("customer cert");
    let cert_der = load_cert_pem(cert.pem().as_bytes()).expect("cert der");
    let key_der = load_key_pem(key_pair.serialize_pem().as_bytes()).expect("key der");
    (cert_der, key_der)
}

async fn recv_n(rx: &mut mpsc::UnboundedReceiver<Received>, n: usize) -> Vec<Received> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("server did not receive expected message in time")
                .expect("server channel closed"),
        );
    }
    out
}

/// Connects one [`RawJetTransactionSender`] to `addr`, using an insecure (no cert
/// validation) client config — this is the crate's basic building block: bind an
/// endpoint, build a client config, connect, wrap the connection for sending.
async fn connect_sender(addr: SocketAddr) -> JetQuicSender {
    let (cert, key) = test_customer();
    let client_config =
        client_config_with_verification(ClientIdentity { cert, key }, ServerVerification::Insecure)
            .expect("client config");
    let endpoint = JetQuicEndpoint::bind(None).expect("bind endpoint");
    let connection = endpoint
        .connect(&ServerAddr::SocketAddr(addr), "localhost", client_config)
        .await
        .expect("connect");
    JetQuicSender::new(connection)
}

#[tokio::test]
async fn connects_and_sends_a_transaction() {
    let (addr, mut rx) = spawn_test_server();
    let mut sender = connect_sender(addr).await;

    sender
        .send_transaction(b"hello-transaction")
        .await
        .expect("send_transaction");

    let received = recv_n(&mut rx, 1).await;
    assert_eq!(received[0].bytes, b"hello-transaction");
}

/// A single connection is reused sequentially for many sends — the one-stream-at-a-time
/// pattern the `quinn-client` skill calls for, exercised end-to-end here.
#[tokio::test]
async fn sends_multiple_transactions_sequentially_on_one_connection() {
    let (addr, mut rx) = spawn_test_server();
    let mut sender = connect_sender(addr).await;

    const N: usize = 5;
    for i in 0..N {
        sender
            .send_transaction(format!("tx-{i}").as_bytes())
            .await
            .expect("send_transaction");
    }

    let mut got: Vec<Vec<u8>> = recv_n(&mut rx, N)
        .await
        .into_iter()
        .map(|r| r.bytes)
        .collect();
    got.sort();
    let mut want: Vec<Vec<u8>> = (0..N).map(|i| format!("tx-{i}").into_bytes()).collect();
    want.sort();
    assert_eq!(got, want);
}

/// Nothing in this crate pools or shares connections — spreading work across several
/// connections is entirely the caller's own doing: bind and connect several independent
/// [`JetQuicEndpoint`]s and hold a [`RawJetTransactionSender`] per connection.
#[tokio::test]
async fn independently_connected_senders_use_distinct_connections() {
    let (addr, mut rx) = spawn_test_server();

    const N: usize = 4;
    let mut senders = Vec::with_capacity(N);
    for _ in 0..N {
        senders.push(connect_sender(addr).await);
    }

    for (i, sender) in senders.iter_mut().enumerate() {
        sender
            .send_transaction(format!("conn-{i}").as_bytes())
            .await
            .expect("send_transaction");
    }

    let received = recv_n(&mut rx, N).await;
    let distinct: HashSet<SocketAddr> = received.iter().map(|r| r.from).collect();
    assert_eq!(
        distinct.len(),
        N,
        "expected {N} distinct connections, got {distinct:?}"
    );
}

/// With no reconnect logic in this crate (that's the caller's responsibility to layer
/// on top, if wanted), sending on a connection the peer already closed must surface as
/// an error rather than silently succeeding or retrying on its own.
#[tokio::test]
async fn send_on_a_dead_connection_errors() {
    let (addr, mut closed_rx) = spawn_test_server_closing_connections();
    let mut sender = connect_sender(addr).await;

    tokio::time::timeout(std::time::Duration::from_secs(5), closed_rx.recv())
        .await
        .expect("server did not close the connection in time");

    // The server already closed its side; give that a moment to actually propagate to
    // the client's connection state before the client tries to use it.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let result = sender.send_transaction(b"should-fail").await;
    assert!(
        result.is_err(),
        "expected send_transaction on a dead connection to fail, got {result:?}"
    );
}

#[test]
fn server_addr_from_string_splits_host_and_port() {
    match ServerAddr::from("jet.example.com:8443".to_owned()) {
        ServerAddr::Named { host, port } => {
            assert_eq!(host, "jet.example.com");
            assert_eq!(port, Some(8443));
        }
        other => panic!("expected Named, got {other:?}"),
    }
}

#[test]
fn server_addr_from_string_without_port_defaults_to_none() {
    match ServerAddr::from("jet.example.com".to_owned()) {
        ServerAddr::Named { host, port } => {
            assert_eq!(host, "jet.example.com");
            assert_eq!(port, None);
        }
        other => panic!("expected Named, got {other:?}"),
    }
}
