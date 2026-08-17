use {
    jet_quic_client::{JetQuicClientConfig, JetQuicSink, ServerVerification},
    rcgen::{CertifiedKey, generate_simple_self_signed},
    rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        path::Path,
        time::Duration,
    },
    tempfile::TempDir,
    tokio::{sync::mpsc, time::timeout},
    yellowstone_jet::{
        config::{ConfigClientAllowlistSource, ConfigRawQuicServer},
        raw_quic::{self, RawQuicServer},
        transaction_handler::TransactionHandler,
        transactions::SendTransactionRequest,
    },
};

fn write_server_default_bundle(dir: &Path) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).expect("self-signed cert");
    let mut contents = cert.pem();
    contents.push('\n');
    contents.push_str(&key_pair.serialize_pem());
    std::fs::write(dir.join("default.pem"), contents).expect("write server bundle");
}

struct Customer {
    cert_pem: String,
    cert_der: CertificateDer<'static>,
    key_der: PrivateKeyDer<'static>,
}

fn generate_customer() -> Customer {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["customer.invalid".to_owned()]).expect("customer cert");
    let cert_pem = cert.pem();
    let cert_der = CertificateDer::from_pem_slice(cert_pem.as_bytes()).expect("cert der");
    let key_der =
        PrivateKeyDer::from_pem_slice(key_pair.serialize_pem().as_bytes()).expect("key der");
    Customer {
        cert_pem,
        cert_der,
        key_der,
    }
}

fn write_customer(dir: &Path, name: &str, customer: &Customer) {
    std::fs::write(dir.join(format!("{name}.pem")), &customer.cert_pem).expect("write customer");
}

fn dummy_wire_transaction() -> Vec<u8> {
    let payer = Keypair::new();
    let to = Pubkey::new_unique();
    let instruction = transfer(&payer.pubkey(), &to, 1);
    let transaction = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&payer.pubkey(), &[instruction], &[], Hash::default())
                .expect("compile message"),
        ),
        &[&payer],
    )
    .expect("sign transaction");
    wincode::serialize(&transaction).expect("serialize transaction")
}

struct TestServer {
    // Held only for RAII: dropping either temp dir would delete the on-disk
    // certs/allow-list that the server (and `reload()`) may still read from.
    _server_cert_dir: TempDir,
    allowlist_dir: TempDir,
    rx: mpsc::Receiver<SendTransactionRequest>,
}

/// `initial_customers` are written to the allow-list directory *before* the server
/// starts, so they're part of the very first `from_config` load (no reload needed).
async fn build_server(
    debug_accept_any_client: bool,
    initial_customers: &[(&str, &Customer)],
) -> (RawQuicServer, raw_quic::RawQuicReloadHandle, TestServer) {
    let server_cert_dir = tempfile::tempdir().expect("tempdir");
    write_server_default_bundle(server_cert_dir.path());
    let allowlist_dir = tempfile::tempdir().expect("tempdir");
    for (name, customer) in initial_customers {
        write_customer(allowlist_dir.path(), name, customer);
    }

    let (tx, rx) = mpsc::channel(16);
    let tx_handler = TransactionHandler::new(tx, true);

    let config = ConfigRawQuicServer {
        bind: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
        server_cert_dir: server_cert_dir.path().to_path_buf(),
        client_allowlist: ConfigClientAllowlistSource::Dir {
            path: allowlist_dir.path().to_path_buf(),
        },
        reload_interval: Duration::from_secs(3600),
        debug_accept_any_client,
        workers: NonZeroUsize::new(1).unwrap(),
    };

    let (mut servers, reload_handle) = raw_quic::from_config(&config, tx_handler)
        .await
        .expect("start raw quic server");
    let server = servers.pop().expect("exactly one worker requested");

    (
        server,
        reload_handle,
        TestServer {
            _server_cert_dir: server_cert_dir,
            allowlist_dir,
            rx,
        },
    )
}

fn client_config(addr: SocketAddr, customer: &Customer) -> JetQuicClientConfig {
    JetQuicClientConfig {
        server_addr: addr,
        server_name: "localhost".to_owned(),
        client_cert: customer.cert_der.clone(),
        client_key: customer.key_der.clone_key(),
        server_verification: ServerVerification::Insecure,
        connections: NonZeroUsize::new(1).unwrap(),
        bind_ip: None,
        bind_port_range: None,
    }
}

/// Asserts that a client presenting `customer`'s cert cannot use a connection to
/// `addr`. TLS 1.3 mutual auth is asymmetric: the *client's* side of the handshake can
/// complete before it observes the server's asynchronous rejection of its certificate
/// (the server's `CONNECTION_CLOSE` is a separate message that arrives slightly later,
/// not something the initial connect call necessarily waits for). So a rejection can
/// surface either as `connect()` itself failing, or as the connection failing shortly
/// after on first use — both are checked here.
async fn assert_rejected(addr: SocketAddr, customer: &Customer) {
    match JetQuicSink::connect(client_config(addr, customer)).await {
        Err(_) => {}
        Ok(client) => {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let result = client.send_transaction(&dummy_wire_transaction()).await;
            assert!(
                result.is_err(),
                "connection should have been closed by the server after rejecting the client cert"
            );
        }
    }
}

#[tokio::test]
async fn allow_listed_client_connects_and_sends_transaction() {
    let customer = generate_customer();
    let (server, _reload, mut ctx) = build_server(false, &[("customer-a", &customer)]).await;

    let addr = server.local_addr().expect("local addr");
    let handle = tokio::spawn(server.serve());

    let client = JetQuicSink::connect(client_config(addr, &customer))
        .await
        .expect("allow-listed client should connect");

    let wire = dummy_wire_transaction();
    client
        .send_transaction(&wire)
        .await
        .expect("send transaction");

    let request = timeout(Duration::from_secs(5), ctx.rx.recv())
        .await
        .expect("transaction handler timed out")
        .expect("transaction handler channel closed");
    assert_eq!(request.wire_transaction.as_ref(), wire.as_slice());

    handle.abort();
}

#[tokio::test]
async fn non_allow_listed_client_is_rejected() {
    // Note: no customer cert is written to the allow-list directory.
    let (server, _reload, ctx) = build_server(false, &[]).await;
    let customer = generate_customer();

    let addr = server.local_addr().expect("local addr");
    let handle = tokio::spawn(server.serve());

    assert_rejected(addr, &customer).await;

    drop(ctx);
    handle.abort();
}

#[tokio::test]
async fn reload_picks_up_allowlist_changes() {
    let customer = generate_customer();
    let (server, reload, ctx) = build_server(false, &[("customer-a", &customer)]).await;

    let addr = server.local_addr().expect("local addr");
    let handle = tokio::spawn(server.serve());

    JetQuicSink::connect(client_config(addr, &customer))
        .await
        .expect("customer-a should be accepted from the initial load");

    // Remove the cert and reload: the same customer must now be rejected.
    std::fs::remove_file(ctx.allowlist_dir.path().join("customer-a.pem")).unwrap();
    reload.reload().await;

    assert_rejected(addr, &customer).await;

    handle.abort();
}

#[tokio::test]
async fn debug_accept_any_client_bypasses_allowlist() {
    // No customer cert is written to the allow-list directory at all.
    let (server, _reload, ctx) = build_server(true, &[]).await;
    let customer = generate_customer();

    let addr = server.local_addr().expect("local addr");
    let handle = tokio::spawn(server.serve());

    JetQuicSink::connect(client_config(addr, &customer))
        .await
        .expect("debug_accept_any_client should accept any cert");

    drop(ctx);
    handle.abort();
}

#[tokio::test]
async fn with_shutdown_stops_accepting_new_connections() {
    let customer = generate_customer();
    let (server, _reload, ctx) = build_server(false, &[("customer-a", &customer)]).await;

    let addr = server.local_addr().expect("local addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        server
            .with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    // Confirm the server accepts connections before shutdown.
    JetQuicSink::connect(client_config(addr, &customer))
        .await
        .expect("connect before shutdown");

    shutdown_tx.send(()).expect("send shutdown signal");
    timeout(Duration::from_secs(5), handle)
        .await
        .expect("server task timed out")
        .expect("server task panicked");

    // The endpoint is now closed; a new connection attempt must fail.
    let result = timeout(
        Duration::from_secs(2),
        JetQuicSink::connect(client_config(addr, &customer)),
    )
    .await;
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "no new connections should be accepted after shutdown"
    );

    drop(ctx);
}

/// Grabs a currently-free UDP port by binding to port 0 and immediately releasing it.
/// `SO_REUSEPORT` only lets multiple sockets share one *explicit* port — binding to
/// port 0 on each worker would instead hand every worker a different OS-assigned port,
/// so sharded workers need a fixed port picked up front.
fn pick_free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind probe socket")
        .local_addr()
        .expect("local addr")
        .port()
}

#[tokio::test]
async fn multiple_workers_share_the_same_address_via_reuse_port() {
    let customer = generate_customer();
    let server_cert_dir = tempfile::tempdir().expect("tempdir");
    write_server_default_bundle(server_cert_dir.path());
    let allowlist_dir = tempfile::tempdir().expect("tempdir");
    write_customer(allowlist_dir.path(), "customer-a", &customer);

    let (tx, _rx) = mpsc::channel(16);
    let tx_handler = TransactionHandler::new(tx, true);

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), pick_free_udp_port());
    let config = ConfigRawQuicServer {
        bind: vec![addr],
        server_cert_dir: server_cert_dir.path().to_path_buf(),
        client_allowlist: ConfigClientAllowlistSource::Dir {
            path: allowlist_dir.path().to_path_buf(),
        },
        reload_interval: Duration::from_secs(3600),
        debug_accept_any_client: false,
        workers: NonZeroUsize::new(4).unwrap(),
    };

    let (servers, _reload) = raw_quic::from_config(&config, tx_handler)
        .await
        .expect("start sharded raw quic servers");
    assert_eq!(servers.len(), 4);

    // Every worker bound the same address via SO_REUSEPORT, not four different ones.
    for server in &servers {
        assert_eq!(server.local_addr().expect("local addr"), addr);
    }

    let handles: Vec<_> = servers
        .into_iter()
        .map(|s| tokio::spawn(s.serve()))
        .collect();

    // A handful of independent connections should all succeed, regardless of which
    // shard's socket the kernel happens to route each one to.
    for _ in 0..4 {
        JetQuicSink::connect(client_config(addr, &customer))
            .await
            .expect("connect to a sharded raw quic server");
    }

    for handle in handles {
        handle.abort();
    }
}
