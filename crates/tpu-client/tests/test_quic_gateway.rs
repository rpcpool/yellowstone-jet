mod testkit;

use {
    crate::testkit::{build_validator_quic_tpu_endpoint, find_available_port},
    bytes::Bytes,
    quinn::{ConnectionError, VarInt},
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_signer::Signer,
    std::{
        array,
        collections::HashMap,
        net::SocketAddr,
        num::{NonZero, NonZeroUsize},
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    },
    testkit::{build_random_endpoint, generate_random_local_addr},
    tokio::{
        sync::mpsc,
        task::{self, JoinHandle, JoinSet},
    },
    tokio_stream::{StreamExt, StreamMap, wrappers::ReceiverStream},
    yellowstone_jet_tpu_client::{
        config::TpuSenderConfig,
        core::{
            IgnorantLeaderPredictor, LeaderTpuInfoService, Nothing, StakeBasedEvictionStrategy,
            TpuSenderDriverSpawner, TpuSenderResponse, TpuSenderSessionContext, TpuSenderTxn,
            TxDropReason, UpcomingLeaderPredictor, ValidatorStakeInfoService,
        },
    },
};

#[derive(Clone)]
pub struct MockStakeInfoMap {
    stake_map: Arc<Mutex<HashMap<Pubkey, u64>>>,
}

impl MockStakeInfoMap {
    pub fn constant<I>(iter: I) -> Self
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
            .unwrap()
            .get(validator_pubkey)
            .cloned()
    }
}

#[derive(Clone)]
pub struct FakeLeaderTpuInfoService {
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

    fn update_addr(&self, leader_pubkey: Pubkey, addr: SocketAddr) {
        let mut shared = self.shared.write().expect("write lock");
        shared.insert(leader_pubkey, addr);
    }
}

impl LeaderTpuInfoService for FakeLeaderTpuInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        let shared = self.shared.read().expect("read lock");
        shared.get(leader_pubkey).cloned()
    }

    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
        let shared = self.shared.read().expect("read lock");
        shared.get(leader_pubkey).cloned()
    }
}

struct MockedRemoteValidator;

struct MockReceipt {
    from: Pubkey,
    connection_id: usize,
    data: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum MockConnectionError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error(transparent)]
    ReadError(#[from] std::io::Error),
}

struct MockConnectionEstablished {
    remote_pubkey: Pubkey,
    connection_id: usize,
}

struct MockConnectionEnd {
    remote_pubkey: Pubkey,
    result: Result<(), ConnectionError>,
}

#[derive(Default)]
struct MockValidatorNotifiers {
    connection_end_notify: Option<mpsc::Sender<MockConnectionEnd>>,
    connection_established_notify: Option<mpsc::Sender<MockConnectionEstablished>>,
}

///
/// MockedRemoteValidator is a mock implementation of a remote validator that
///
/// spawns a QUIC endpoint that accepts connections and reads data from them.
/// Tis used to test the TokioQuicGatewaySpawner and its ability to handle
///
impl MockedRemoteValidator {
    fn spawn(
        kp: Keypair,
        addr: SocketAddr,
        notifiers: MockValidatorNotifiers,
    ) -> (mpsc::Receiver<MockReceipt>, JoinHandle<()>) {
        let endpoint = build_validator_quic_tpu_endpoint(&kp, addr);
        let (client_tx, client_rx) = mpsc::channel(100);
        let client_tx2 = client_tx.clone();
        struct ConnectionDetail {
            remote_pubkey: Pubkey,
        }
        let rx_server_handle = tokio::spawn(async move {
            let mut connection_set: JoinSet<Result<(), ConnectionError>> = JoinSet::new();
            let mut connection_set_meta: HashMap<task::Id, ConnectionDetail> = HashMap::new();
            let mut connection_id: usize = 0;
            loop {
                let connecting = tokio::select! {
                    result = endpoint.accept() => {
                        result.expect("accept connection")
                    }
                    Some(result) = connection_set.join_next_with_id() => {
                        let (id, result) = result.expect("join next");
                        let connection_detail = connection_set_meta.remove(&id)
                            .expect("connection detail");
                        if let Some(tx) = notifiers.connection_end_notify.as_ref() {
                            let _ = tx.send(MockConnectionEnd {
                                remote_pubkey: connection_detail.remote_pubkey,
                                result: result.clone(),
                            }).await;
                        }
                        continue;
                    }
                };
                let new_connection_id = connection_id;
                let conn = connecting.await.expect("quinn connection");
                connection_id += 1;
                let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
                    .expect("get remote pubkey");
                if let Some(tx) = notifiers.connection_established_notify.as_ref() {
                    let _ = tx
                        .send(MockConnectionEstablished {
                            remote_pubkey: remote_key,
                            connection_id: new_connection_id,
                        })
                        .await;
                }
                let client_tx = client_tx2.clone();
                let ah = connection_set.spawn(async move {
                    loop {
                        let mut rx = conn.accept_uni().await?;
                        // This code as been partially copied from agave source code:
                        let mut chunks: [Bytes; 4] = array::from_fn(|_| Bytes::new());
                        let mut total_chunks_read = 0;

                        while let Some(n_chunk) =
                            rx.read_chunks(&mut chunks).await.expect("read chunks")
                        {
                            total_chunks_read += n_chunk;
                            if total_chunks_read > 4 {
                                panic!("total_chunks_read > 4");
                            }
                        }
                        let combined = chunks.iter().fold(vec![], |mut acc, chunk| {
                            acc.extend_from_slice(chunk);
                            acc
                        });
                        drop(rx);
                        let req = MockReceipt {
                            from: remote_key,
                            connection_id: new_connection_id,
                            data: combined,
                        };
                        client_tx.send(req).await.expect("send");
                    }
                });
                connection_set_meta.insert(
                    ah.id(),
                    ConnectionDetail {
                        remote_pubkey: remote_key,
                    },
                );
            }
        });
        (client_rx, rx_server_handle)
    }
}

#[tokio::test]
async fn send_buffer_should_land_properly() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn_default_with_callback(gateway_kp.insecure_clone(), callback_tx);

    let (mut client_rx, _rx_server_handle) = MockedRemoteValidator::spawn(
        rx_server_identity.insecure_clone(),
        rx_server_addr,
        Default::default(),
    );
    let tx_sig = Signature::new_unique();
    let txn = TpuSenderTxn::from_owned(
        tx_sig,
        rx_server_identity.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    let spy_req = client_rx.recv().await.expect("recv");

    let TpuSenderResponse::TxSent(actual_resp) = callback_rx.recv().await.expect("recv response")
    else {
        panic!("Expected TpuSenderResponse::TxSent, got something else");
    };

    assert_eq!(actual_resp.tx_sig, tx_sig);
    assert_eq!(
        actual_resp.remote_peer_identity,
        rx_server_identity.pubkey()
    );

    let msg = String::from_utf8(spy_req.data).expect("utf8");
    assert_eq!(msg, "helloworld");
    assert_eq!(spy_req.from, gateway_kp.pubkey());
}

#[tokio::test]
async fn sending_multiple_tx_to_the_same_peer_should_reuse_the_same_connection() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();

    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn_default_with_callback(gateway_kp.insecure_clone(), callback_tx);
    const MAX_TX: u64 = 5;

    let (mut client_rx, _rx_server_handle) = MockedRemoteValidator::spawn(
        rx_server_identity.insecure_clone(),
        rx_server_addr,
        Default::default(),
    );

    let tx_sig_vec = (0..MAX_TX)
        .map(|_| Signature::new_unique())
        .collect::<Vec<_>>();
    for (i, tx_sig) in tx_sig_vec.iter().enumerate() {
        let txn = TpuSenderTxn::from_owned(
            *tx_sig,
            rx_server_identity.pubkey(),
            format!("helloworld{i}").as_bytes().to_vec(),
        );
        transaction_sink.send(txn).await.expect("send tx");
    }

    let mut connection_id_spy = vec![];
    for i in 0..MAX_TX {
        tracing::trace!("Waiting for tx response {i}");
        let TpuSenderResponse::TxSent(actual_resp) =
            callback_rx.recv().await.expect("recv response")
        else {
            panic!("Expected TpuSenderResponse::TxSent, got something else");
        };
        assert_eq!(actual_resp.tx_sig, tx_sig_vec[i as usize]);
        let spy_request = client_rx.recv().await.expect("recv");

        connection_id_spy.push(spy_request.connection_id);
        let msg = String::from_utf8(spy_request.data).expect("utf8");
        assert_eq!(msg, format!("helloworld{i}"));
        assert_eq!(
            actual_resp.remote_peer_identity,
            rx_server_identity.pubkey()
        );
        tracing::info!(
            "received tx: {} from remote peer: {} with connection id: {}",
            actual_resp.tx_sig,
            rx_server_identity.pubkey(),
            spy_request.connection_id
        );
    }
}

#[tokio::test]
async fn gateway_should_handle_connection_refused_by_peer() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };
    let gateway_config = TpuSenderConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(callback_tx),
    );

    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        drop(connecting);
    });

    let tx_sig = Signature::new_unique();
    let txn = TpuSenderTxn::from_owned(
        tx_sig,
        rx_server_identity.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    rx_server_handle.await.expect("h2");

    let resp = callback_rx.recv().await.expect("recv response");

    let TpuSenderResponse::TxDrop(mut actual_resp) = resp else {
        panic!("Expected TpuSenderResponse::TxSent, got something {resp:?}");
    };
    let (actual_tx_sig, _curr_attempt) = actual_resp.dropped_tx_vec.pop_front().unwrap();
    assert_eq!(actual_tx_sig.tx_sig, tx_sig);
    assert!(matches!(
        actual_resp.drop_reason,
        TxDropReason::RemotePeerUnreachable
    ));
    assert_eq!(
        actual_resp.remote_peer_identity,
        rx_server_identity.pubkey()
    );
}

#[tokio::test]
async fn it_should_update_gatway_identity() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();
    let gateway_config = TpuSenderConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let TpuSenderSessionContext {
        mut identity_updater,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(Nothing),
    );

    let (mut client_rx, _rx_server_handle) = MockedRemoteValidator::spawn(
        rx_server_identity.insecure_clone(),
        rx_server_addr,
        Default::default(),
    );

    let txn = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        rx_server_identity.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    let spy_request1 = client_rx.recv().await.expect("recv");
    let actual_remote_key1 = spy_request1.from;
    assert_eq!(actual_remote_key1, gateway_kp.pubkey());

    let gateway_identity2 = Keypair::new();

    identity_updater
        .update_identity(gateway_identity2.insecure_clone())
        .await;

    let txn = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        rx_server_identity.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    let spy_request2 = client_rx.recv().await.expect("recv");
    let actual_remote_key2 = spy_request2.from;
    assert_eq!(actual_remote_key2, gateway_identity2.pubkey());
    assert_ne!(actual_remote_key1, actual_remote_key2);
}

#[tokio::test]
async fn it_should_support_concurrent_remote_peer_connection() {
    let remote_validator_addr1 = generate_random_local_addr();
    let remote_validator_addr2 = generate_random_local_addr();
    let remote_validator_identity1 = Keypair::new();
    let remote_validator_identity2 = Keypair::new();
    let gateway_config = TpuSenderConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service = FakeLeaderTpuInfoService::from_iter([
        (remote_validator_identity1.pubkey(), remote_validator_addr1),
        (remote_validator_identity2.pubkey(), remote_validator_addr2),
    ]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(Nothing),
    );

    let (validator_rx1, _) = MockedRemoteValidator::spawn(
        remote_validator_identity1.insecure_clone(),
        remote_validator_addr1,
        Default::default(),
    );

    let (validator_rx2, _) = MockedRemoteValidator::spawn(
        remote_validator_identity2.insecure_clone(),
        remote_validator_addr2,
        Default::default(),
    );

    let mut stream_map = StreamMap::new();

    stream_map.insert(
        remote_validator_identity1.pubkey(),
        ReceiverStream::new(validator_rx1),
    );

    stream_map.insert(
        remote_validator_identity2.pubkey(),
        ReceiverStream::new(validator_rx2),
    );

    let txn = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        remote_validator_identity1.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    // Send it to the first remote peer
    transaction_sink.send(txn).await.expect("send tx");

    // Send it to the second remote peer
    let txn2 = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        remote_validator_identity2.pubkey(),
        "helloworld2".as_bytes().to_vec(),
    );
    transaction_sink.send(txn2).await.expect("send tx");

    let mut expected_remote_validators = vec![
        remote_validator_identity1.pubkey(),
        remote_validator_identity2.pubkey(),
    ];
    expected_remote_validators.sort_unstable();

    let actual_remote_validator1 = stream_map.next().await.expect("next").0;
    let actual_remote_validator2 = stream_map.next().await.expect("next").0;

    let mut actual_remote_validators = vec![actual_remote_validator1, actual_remote_validator2];
    actual_remote_validators.sort_unstable();

    assert_eq!(actual_remote_validators, expected_remote_validators,);
}

#[tokio::test]
async fn it_should_evict_connection() {
    let really_limited_port_range = find_available_port().expect("port");

    let remote_validator_addr1 = generate_random_local_addr();
    let remote_validator_addr2 = generate_random_local_addr();
    let remote_validator_identity1 = Keypair::new();
    let remote_validator_identity2 = Keypair::new();
    let gateway_config = TpuSenderConfig {
        max_connection_attempts: 1,
        max_concurrent_connection: 1, // LIMIT TO 1 CONCURRENT CONNECTION SHOULD TRIGGER CONNECTION EVICTION ON EACH NEW REMOTE DEST
        endpoint_port_range: (really_limited_port_range, really_limited_port_range + 3),
        num_endpoints: NonZero::new(1).unwrap(),
        max_local_port_binding_attempts: 1,
        ..Default::default()
    };
    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service = FakeLeaderTpuInfoService::from_iter([
        (remote_validator_identity1.pubkey(), remote_validator_addr1),
        (remote_validator_identity2.pubkey(), remote_validator_addr2),
    ]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(Nothing),
    );

    let (tx, mut validator_conn_spy1) = mpsc::channel(100);
    let notifier = MockValidatorNotifiers {
        connection_end_notify: Some(tx),
        ..Default::default()
    };
    let (validator_rx1, _) = MockedRemoteValidator::spawn(
        remote_validator_identity1.insecure_clone(),
        remote_validator_addr1,
        notifier,
    );

    let (tx, mut validator_conn_spy2) = mpsc::channel(100);
    let notifier = MockValidatorNotifiers {
        connection_end_notify: Some(tx),
        ..Default::default()
    };
    let (validator_rx2, _) = MockedRemoteValidator::spawn(
        remote_validator_identity2.insecure_clone(),
        remote_validator_addr2,
        notifier,
    );

    let mut stream_map = StreamMap::new();

    stream_map.insert(
        remote_validator_identity1.pubkey(),
        ReceiverStream::new(validator_rx1),
    );

    stream_map.insert(
        remote_validator_identity2.pubkey(),
        ReceiverStream::new(validator_rx2),
    );
    let txn = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        remote_validator_identity1.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    let actual_remote_validator1 = stream_map.next().await.expect("next").0;
    assert_eq!(
        actual_remote_validator1,
        remote_validator_identity1.pubkey()
    );

    // Now we send a tx to the second remote peer, this should evict the first connection
    let txn2 = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        remote_validator_identity2.pubkey(),
        "helloworld2".as_bytes().to_vec(),
    );
    transaction_sink.send(txn2).await.expect("send tx");

    let conn_end = validator_conn_spy1.recv().await.expect("recv");

    assert!(conn_end.result.is_err(), "connection should be evicted");
    assert_eq!(conn_end.remote_pubkey, gateway_kp.pubkey());

    let actual_remote_validator2 = stream_map.next().await.expect("next").0;
    assert_eq!(
        actual_remote_validator2,
        remote_validator_identity2.pubkey()
    );

    // Finally, send it back to the first remote peer, this should evict the second connection
    let txn3 = TpuSenderTxn::from_owned(
        Signature::new_unique(),
        remote_validator_identity1.pubkey(),
        "helloworld3".as_bytes().to_vec(),
    );
    transaction_sink.send(txn3).await.expect("send tx");
    let actual_remote_validator3 = stream_map.next().await.expect("next").0;
    assert_eq!(
        actual_remote_validator3,
        remote_validator_identity1.pubkey()
    );

    // The second connection should be evicted
    let conn_end = validator_conn_spy2.recv().await.expect("recv");

    assert!(conn_end.result.is_err(), "connection should be evicted");
    assert_eq!(conn_end.remote_pubkey, gateway_kp.pubkey());
}

#[tokio::test]
async fn it_should_retry_tx_failed_to_be_sent_due_to_connection_lost() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    // Here's the challenging when testing network error with quinn:
    // Writing to a uni-stream returns success if the the write has been flushed to the internal quinn buffer,
    // not the actual wire.
    // Also, even if you write it to the wire, it does not guarantee that the remote peer has received it, if for example,
    // the remote peer closed the uni stream right after it opened it.
    // If we send a too little payload, even if the remote peer closed the uni stream, it will not trigger a connection lost,
    // because quinn will be so fast that it will send the payload before the remote peer closes the stream.
    let huge_payload = Bytes::from(vec![0u8; 1024 * 1024 * 100]); // 100MB payload
    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };
    const MAX_CONN_ATTEMPT: NonZeroUsize = NonZeroUsize::new(1).unwrap();
    let gateway_config = TpuSenderConfig {
        max_connection_attempts: 1,
        max_send_attempt: MAX_CONN_ATTEMPT,
        ..Default::default()
    };
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(callback_tx),
    );

    let _rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let conn = connecting.await.expect("quinn connection");
        loop {
            // Simulate a connection lost by dropping the connection
            let mut uni = conn.accept_uni().await.expect("accept uni");
            let _ = uni.stop(VarInt::from_u32(0));
            drop(uni);
        }
    });

    let tx_sig = Signature::new_unique();
    let txn = TpuSenderTxn::from_owned(tx_sig, rx_server_identity.pubkey(), huge_payload.clone());
    transaction_sink.send(txn).await.expect("send tx");

    // This handle should return after MAX_CONN_ATTEMPT attempts
    tracing::trace!("Waiting for rx_server_handle to finish");
    // let _ = rx_server_handle.await;
    tracing::trace!("rx_server_handle finished");

    let resp = callback_rx.recv().await.expect("recv response");
    tracing::trace!("Received response: {:?}", resp);

    let TpuSenderResponse::TxFailed(actual_resp) = resp else {
        panic!("Expected TpuSenderResponse::TxSent, got something {resp:?}");
    };

    assert_eq!(actual_resp.tx_sig, tx_sig);
}

#[tokio::test]
async fn it_should_detect_remote_peer_address_change() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let gateway_config = TpuSenderConfig {
        // Keep it small so test runs fast.
        remote_peer_addr_watch_interval: Duration::from_millis(10),
        ..Default::default()
    };

    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(callback_tx),
    );

    let (tx, mut conn_spy_rx1) = mpsc::channel(100);
    let notifier = MockValidatorNotifiers {
        connection_end_notify: Some(tx),
        ..Default::default()
    };
    let (mut client_rx1, _rx_server_handle) = MockedRemoteValidator::spawn(
        rx_server_identity.insecure_clone(),
        rx_server_addr,
        notifier,
    );
    let tx_sig = Signature::new_unique();
    let txn = TpuSenderTxn::from_owned(
        tx_sig,
        rx_server_identity.pubkey(),
        "helloworld".as_bytes().to_vec(),
    );
    transaction_sink.send(txn).await.expect("send tx");

    let _ = client_rx1.recv().await.expect("recv");

    let TpuSenderResponse::TxSent(actual_resp) = callback_rx.recv().await.expect("recv response")
    else {
        panic!("Expected TpuSenderResponse::TxSent, got something else");
    };

    assert_eq!(actual_resp.tx_sig, tx_sig);

    // Now we change the remote peer address
    let new_rx_server_addr = generate_random_local_addr();
    // keep the same pubkey, but change the address
    let (mut client_rx2, _) = MockedRemoteValidator::spawn(
        rx_server_identity.insecure_clone(),
        new_rx_server_addr,
        Default::default(),
    );

    fake_tpu_info_service.update_addr(rx_server_identity.pubkey(), new_rx_server_addr);

    // rx_server_handle2.await.expect("rx server handle");
    // Wait for the connection to be evicted
    let conn_ended = conn_spy_rx1.recv().await.expect("recv");
    assert!(conn_ended.result.is_err(), "connection should be evicted");
    // Send a new transaction to the new address
    let tx_sig2 = Signature::new_unique();
    let txn2 = TpuSenderTxn::from_owned(
        tx_sig2,
        rx_server_identity.pubkey(),
        "helloworld2".as_bytes().to_vec(),
    );
    transaction_sink.send(txn2).await.expect("send tx");

    let _ = client_rx2.recv().await.expect("recv");
    let TpuSenderResponse::TxSent(actual_resp) = callback_rx.recv().await.expect("recv response")
    else {
        panic!("Expected TpuSenderResponse::TxSent, got something else");
    };
    assert_eq!(actual_resp.tx_sig, tx_sig2);
}

#[tokio::test]
async fn it_should_preemptively_connect_to_upcoming_leader_using_leader_predictions() {
    let gateway_kp = Keypair::new();
    let stake_info_map = MockStakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let mut validator_rx_vec = vec![];
    let mut validator_conn_ending_rx_vec = vec![];
    let mut validator_conn_establ_rx_vec = vec![];
    let mut validators_kp_vec = vec![];
    let mut validators_addr_vec = vec![];
    const NUM_VALIDATORS: usize = 3;

    // We spawn NUM_VALIDATORS remote validators, each with its own address and identity.
    for _ in 0..NUM_VALIDATORS {
        let remote_validator_addr = generate_random_local_addr();
        let remote_validator_identity = Keypair::new();
        validators_kp_vec.push(remote_validator_identity.insecure_clone());
        validators_addr_vec.push(remote_validator_addr);
        let (tx, rx) = mpsc::channel(100);
        let (tx_establish, rx_establish) = mpsc::channel(100);
        let notifier = MockValidatorNotifiers {
            connection_end_notify: Some(tx),
            connection_established_notify: Some(tx_establish),
        };
        let (validator_rx, _) = MockedRemoteValidator::spawn(
            remote_validator_identity.insecure_clone(),
            remote_validator_addr,
            notifier,
        );
        validator_rx_vec.push(validator_rx);
        validator_conn_ending_rx_vec.push(rx);
        validator_conn_establ_rx_vec.push(rx_establish);
    }

    let kp_to_addr_pairs = validators_kp_vec
        .iter()
        .zip(validators_addr_vec.iter())
        .map(|(kp, addr)| (kp.pubkey(), *addr))
        .collect::<Vec<_>>();

    let fake_tpu_info_service = FakeLeaderTpuInfoService::from_iter(kp_to_addr_pairs);

    let driver_spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(stake_info_map.clone()),
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        driver_tx_channel_capacity: 100,
    };

    let gateway_config = TpuSenderConfig {
        // Keep it small so test runs fast.
        remote_peer_addr_watch_interval: Duration::from_millis(10),
        // Set the lookahead to the number of validators we have
        leader_prediction_lookahead: Some(NonZeroUsize::new(NUM_VALIDATORS).unwrap()),
        ..Default::default()
    };

    struct FakeLeaderPredictor {
        validators: Vec<Pubkey>,
        calls: Arc<RwLock<usize>>,
    }

    impl UpcomingLeaderPredictor for FakeLeaderPredictor {
        fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
            {
                let mut calls = self.calls.write().expect("write lock");
                *calls += 1;
            }
            self.validators
                .iter()
                .cycle()
                .take(n)
                .cloned()
                .collect::<Vec<_>>()
        }
    }

    impl FakeLeaderPredictor {
        fn get_calls(&self) -> usize {
            let calls = self.calls.read().expect("read lock");
            *calls
        }
    }

    let fake_predictor = Arc::new(FakeLeaderPredictor {
        validators: validators_kp_vec.iter().map(|kp| kp.pubkey()).collect(),
        calls: Arc::new(RwLock::new(0)),
    });

    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink: transaction_sink,
        driver_join_handle: _,
    } = driver_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::clone(&fake_predictor) as Arc<dyn UpcomingLeaderPredictor + Send + Sync>,
        Some(Nothing),
    );

    // Since we provided a predictor strategy and a lookahead, the gateway should preemptively connect to the upcoming leaders.
    let mut validator_to_conn_id_map = HashMap::new();
    for (i, rx) in validator_conn_establ_rx_vec.iter_mut().enumerate() {
        let res = rx.recv().await.expect("recv connection end");
        let validator_pk = validators_kp_vec[i].pubkey();
        assert_eq!(res.remote_pubkey, gateway_kp.pubkey());
        validator_to_conn_id_map.insert(validator_pk, res.connection_id);
    }

    // Now send a transaction to each of the validators, this should reuse the connections established by the predictor.
    // Using each validator_rx receiver half, we will be notified on transaction reception with the connection id used.
    // We should see it reuses the same connection id for each validator, previously set in `validator_to_conn_id_map`.

    for (i, validator_rx) in validator_rx_vec.iter_mut().enumerate() {
        let tx_sig = Signature::new_unique();
        let txn = TpuSenderTxn::from_owned(
            tx_sig,
            validators_kp_vec[i].pubkey(),
            format!("helloworld{i}").as_bytes().to_vec(),
        );
        transaction_sink.send(txn).await.expect("send tx");

        let spy_request = validator_rx.recv().await.expect("recv");
        assert_eq!(spy_request.from, gateway_kp.pubkey());
        assert_eq!(
            spy_request.connection_id,
            validator_to_conn_id_map[&validators_kp_vec[i].pubkey()]
        );
    }

    assert!(fake_predictor.get_calls() >= 1);
}
