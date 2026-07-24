#![cfg(feature = "intg-testing")]

mod testkit;

use {
    crate::testkit::{build_validator_quic_tpu_endpoint, generate_random_local_addr},
    bytes::Bytes,
    quinn::ConnectionError,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        array,
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    },
    tokio::{
        sync::mpsc,
        task::{JoinHandle, JoinSet},
    },
    x509_parser::{asn1_rs::FromDer, certificate::X509Certificate, public_key::PublicKey},
    yellowstone_jet_tpu_client::{
        config::{TpuPortKind, TpuSenderConfig},
        core::{
            IgnorantLeaderPredictor, LeaderTpuInfoService, Nothing, StakeBasedEvictionStrategy,
            ValidatorStakeInfoService,
        },
        rpc::schedule::ManagedLeaderSchedule,
        sender::create_base_tpu_client,
        slot::SlotTracker,
        yellowstone_grpc::sender::YellowstoneTpuSender,
    },
};

#[derive(Clone)]
struct MockStakeInfoMap {
    stake_map: Arc<Mutex<HashMap<Pubkey, u64>>>,
}

impl MockStakeInfoMap {
    fn constant<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Pubkey, u64)>,
    {
        let stake_map = Arc::new(Mutex::new(HashMap::from_iter(iter)));
        Self { stake_map }
    }
}

impl ValidatorStakeInfoService for MockStakeInfoMap {
    fn get_stake_info(&self, validator_pubkey: &Pubkey) -> Option<u64> {
        self.stake_map
            .lock()
            .expect("stake map lock")
            .get(validator_pubkey)
            .cloned()
    }
}

#[derive(Clone)]
struct FakeLeaderTpuInfoService {
    shared: Arc<RwLock<HashMap<Pubkey, SocketAddr>>>,
}

impl FakeLeaderTpuInfoService {
    fn from_iter<IT>(it: IT) -> Self
    where
        IT: IntoIterator<Item = (Pubkey, SocketAddr)>,
    {
        let shared = Arc::new(RwLock::new(HashMap::from_iter(it)));
        Self { shared }
    }
}

impl LeaderTpuInfoService for FakeLeaderTpuInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.shared
            .read()
            .expect("read lock")
            .get(leader_pubkey)
            .cloned()
    }

    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        self.shared
            .read()
            .expect("read lock")
            .get(leader_pubkey)
            .cloned()
    }
}

struct MockedRemoteValidator;

struct InterceptedTxn {
    from: Pubkey,
    connection_id: usize,
    data: Vec<u8>,
}

fn get_remote_pubkey_from_quic_connection(conn: &quinn::Connection) -> Option<Pubkey> {
    conn.peer_identity()?
        .downcast::<Vec<rustls::pki_types::CertificateDer>>()
        .ok()
        .filter(|certs| certs.len() == 1)?
        .first()
        .and_then(get_pubkey_from_tls_certificate)
}

fn get_pubkey_from_tls_certificate(der_cert: &rustls::pki_types::CertificateDer) -> Option<Pubkey> {
    let (_, cert) = X509Certificate::from_der(der_cert.as_ref()).ok()?;
    match cert.public_key().parsed().ok()? {
        PublicKey::Unknown(key) => Pubkey::try_from(key).ok(),
        _ => None,
    }
}

impl MockedRemoteValidator {
    fn spawn(kp: Keypair, addr: SocketAddr) -> (mpsc::Receiver<InterceptedTxn>, JoinHandle<()>) {
        let endpoint = build_validator_quic_tpu_endpoint(&kp, addr);
        let (client_tx, client_rx) = mpsc::channel(100);
        let client_tx2 = client_tx.clone();

        let rx_server_handle = tokio::spawn(async move {
            let mut connection_set: JoinSet<Result<(), ConnectionError>> = JoinSet::new();
            let mut connection_id: usize = 0;

            loop {
                let connecting = tokio::select! {
                    result = endpoint.accept() => {
                        result.expect("accept connection")
                    }
                    Some(result) = connection_set.join_next() => {
                        let _ = result.expect("join next");
                        continue;
                    }
                };

                let new_connection_id = connection_id;
                let conn = connecting.await.expect("quinn connection");
                connection_id += 1;
                let remote_key =
                    get_remote_pubkey_from_quic_connection(&conn).expect("get remote pubkey");

                let client_tx = client_tx2.clone();
                connection_set.spawn(async move {
                    loop {
                        let mut rx = conn.accept_uni().await?;
                        let mut chunks: [Bytes; 4] = array::from_fn(|_| Bytes::new());
                        let mut total_chunks_read = 0;

                        while let Some(n_chunk) =
                            rx.read_chunks(&mut chunks).await.expect("read chunks")
                        {
                            total_chunks_read += n_chunk;
                            assert!(total_chunks_read <= 4, "total_chunks_read > 4");
                        }

                        let combined = chunks.iter().fold(vec![], |mut acc, chunk| {
                            acc.extend_from_slice(chunk);
                            acc
                        });

                        drop(rx);
                        let req = InterceptedTxn {
                            from: remote_key,
                            connection_id: new_connection_id,
                            data: combined,
                        };
                        client_tx.send(req).await.expect("send");
                    }
                });
            }
        });

        (client_rx, rx_server_handle)
    }
}

async fn build_sender_from_parts(
    gateway_kp: Keypair,
    leader_tpu_info: Arc<dyn LeaderTpuInfoService + Send + Sync>,
    schedule: Vec<Pubkey>,
    current_slot: u64,
) -> YellowstoneTpuSender {
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1_000)]);

    let base_tpu_sender = create_base_tpu_client(
        TpuSenderConfig {
            max_connection_attempts: 1,
            ..Default::default()
        },
        gateway_kp,
        Arc::clone(&leader_tpu_info),
        Arc::new(stake_info_map),
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Option::<Nothing>::None,
        128,
    )
    .await;

    YellowstoneTpuSender::from_parts(
        base_tpu_sender,
        leader_tpu_info,
        ManagedLeaderSchedule::new_for_test(0, schedule),
        SlotTracker::new_for_test(current_slot),
        TpuPortKind::Forwards,
    )
}

#[tokio::test]
async fn from_parts_send_txn_many_dest_should_land_properly() {
    let remote_addr = generate_random_local_addr();
    let remote_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let leader_tpu_info = Arc::new(FakeLeaderTpuInfoService::from_iter([(
        remote_identity.pubkey(),
        remote_addr,
    )])) as Arc<dyn LeaderTpuInfoService + Send + Sync>;

    let mut sender =
        build_sender_from_parts(gateway_kp.insecure_clone(), leader_tpu_info, vec![], 0).await;

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(remote_identity.insecure_clone(), remote_addr);

    sender
        .send_txn_many_dest(
            "helloworld".as_bytes().to_vec(),
            &[remote_identity.pubkey()],
            None,
        )
        .await
        .expect("send_txn_many_dest");

    let spy_req = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
        .await
        .expect("timeout waiting remote receive")
        .expect("recv");

    let msg = String::from_utf8(spy_req.data).expect("utf8");
    assert_eq!(msg, "helloworld");
    assert_eq!(spy_req.from, gateway_kp.pubkey());
}

#[tokio::test]
async fn from_parts_sending_multiple_tx_to_same_peer_should_reuse_connection() {
    let remote_addr = generate_random_local_addr();
    let remote_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let leader_tpu_info = Arc::new(FakeLeaderTpuInfoService::from_iter([(
        remote_identity.pubkey(),
        remote_addr,
    )])) as Arc<dyn LeaderTpuInfoService + Send + Sync>;

    let mut sender =
        build_sender_from_parts(gateway_kp.insecure_clone(), leader_tpu_info, vec![], 0).await;

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(remote_identity.insecure_clone(), remote_addr);

    const MAX_TX: usize = 5;
    for i in 0..MAX_TX {
        sender
            .send_txn_many_dest(
                format!("helloworld{i}").as_bytes().to_vec(),
                &[remote_identity.pubkey()],
                None,
            )
            .await
            .expect("send_txn_many_dest");
    }

    let mut connection_ids = Vec::with_capacity(MAX_TX);
    for i in 0..MAX_TX {
        let spy_req = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
            .await
            .expect("timeout waiting remote receive")
            .expect("recv");

        let msg = String::from_utf8(spy_req.data).expect("utf8");
        assert_eq!(msg, format!("helloworld{i}"));
        connection_ids.push(spy_req.connection_id);
    }

    let first = connection_ids
        .first()
        .copied()
        .expect("first connection id");
    assert!(
        connection_ids.iter().all(|id| *id == first),
        "expected all txs to reuse the same connection, got {connection_ids:?}"
    );
}

#[tokio::test]
async fn from_parts_send_txn_should_follow_managed_schedule() {
    let leader_addr = generate_random_local_addr();
    let non_leader_addr = generate_random_local_addr();

    let leader_identity = Keypair::new();
    let non_leader_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let leader_tpu_info = Arc::new(FakeLeaderTpuInfoService::from_iter([
        (leader_identity.pubkey(), leader_addr),
        (non_leader_identity.pubkey(), non_leader_addr),
    ])) as Arc<dyn LeaderTpuInfoService + Send + Sync>;

    let mut sender = build_sender_from_parts(
        gateway_kp.insecure_clone(),
        leader_tpu_info,
        vec![leader_identity.pubkey()],
        0,
    )
    .await;

    let (mut leader_rx, _leader_jh) =
        MockedRemoteValidator::spawn(leader_identity.insecure_clone(), leader_addr);
    let (mut non_leader_rx, _non_leader_jh) =
        MockedRemoteValidator::spawn(non_leader_identity.insecure_clone(), non_leader_addr);

    sender
        .send_txn("fanout".as_bytes().to_vec(), None)
        .await
        .expect("send_txn");

    let leader_req = tokio::time::timeout(Duration::from_secs(5), leader_rx.recv())
        .await
        .expect("timeout waiting leader receive")
        .expect("leader recv");

    assert_eq!(String::from_utf8(leader_req.data).expect("utf8"), "fanout");

    let non_leader_received =
        tokio::time::timeout(Duration::from_millis(400), non_leader_rx.recv()).await;
    assert!(
        non_leader_received.is_err(),
        "non-leader should not receive transaction"
    );
}

#[tokio::test]
async fn from_parts_should_support_identity_update() {
    let remote_addr = generate_random_local_addr();
    let remote_identity = Keypair::new();

    let gateway_identity_1 = Keypair::new();
    let gateway_identity_2 = Keypair::new();

    let leader_tpu_info = Arc::new(FakeLeaderTpuInfoService::from_iter([(
        remote_identity.pubkey(),
        remote_addr,
    )])) as Arc<dyn LeaderTpuInfoService + Send + Sync>;

    let mut sender = build_sender_from_parts(
        gateway_identity_1.insecure_clone(),
        leader_tpu_info,
        vec![],
        0,
    )
    .await;

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(remote_identity.insecure_clone(), remote_addr);

    sender
        .send_txn_many_dest(
            "before-update".as_bytes().to_vec(),
            &[remote_identity.pubkey()],
            None,
        )
        .await
        .expect("send before update");

    let first_req = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
        .await
        .expect("timeout waiting first tx")
        .expect("first recv");

    assert_eq!(first_req.from, gateway_identity_1.pubkey());

    sender
        .update_identity(gateway_identity_2.insecure_clone())
        .await
        .expect("update identity");

    sender
        .send_txn_many_dest(
            "after-update".as_bytes().to_vec(),
            &[remote_identity.pubkey()],
            None,
        )
        .await
        .expect("send after update");

    let second_req = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
        .await
        .expect("timeout waiting second tx")
        .expect("second recv");

    assert_eq!(second_req.from, gateway_identity_2.pubkey());
    assert_ne!(first_req.from, second_req.from);
}
