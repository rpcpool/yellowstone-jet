mod testkit;

use {
    bytes::Bytes,
    futures::stream,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        array,
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, RwLock as StdRwLock},
    },
    testkit::{build_random_endpoint, generate_random_local_addr},
    tokio::{
        sync::mpsc,
        task::{JoinHandle, JoinSet},
    },
    tokio_stream::{StreamExt, StreamMap, wrappers::ReceiverStream},
    yellowstone_jet::{
        quic_gateway::{
            GatewayResponse, GatewayTransaction, LeaderTpuInfoService, QuicGatewayConfig,
            StakedBaseEvictionStrategy, TokioQuicGatewaySession, TokioQuicGatewaySpawner,
            TxDropReason,
        },
        stake::StakeInfoMap,
    },
};

#[derive(Clone)]
pub struct FakeLeaderTpuInfoService {
    shared: Arc<StdRwLock<HashMap<Pubkey, SocketAddr>>>,
}

impl FakeLeaderTpuInfoService {
    fn from_iter<IT>(it: IT) -> Self
    where
        IT: IntoIterator<Item = (Pubkey, SocketAddr)>,
    {
        let shared = Arc::new(StdRwLock::new(HashMap::from_iter(it)));
        Self { shared }
    }
}

impl LeaderTpuInfoService for FakeLeaderTpuInfoService {
    fn get_quic_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        let shared = self.shared.read().expect("read lock");
        shared.get(&leader_pubkey).cloned()
    }

    fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        let shared = self.shared.read().expect("read lock");
        shared.get(&leader_pubkey).cloned()
    }
}

struct MockedRemoteValidator;

struct SpyRequest {
    from: Pubkey,
    connection_id: usize,
    data: Vec<u8>,
}

impl MockedRemoteValidator {
    fn spawn(pubkey: Pubkey, addr: SocketAddr) -> (mpsc::Receiver<SpyRequest>, JoinHandle<()>) {
        let endpoint = build_random_endpoint(addr).0;
        let (client_tx, client_rx) = mpsc::channel(100);

        let client_tx2 = client_tx.clone();
        let rx_server_handle = tokio::spawn(async move {
            let mut request_set = JoinSet::new();
            let mut connection_id: usize = 0;
            loop {
                let connecting = endpoint.accept().await.expect("accept");
                let new_connection_id = connection_id;
                connection_id += 1;
                let client_tx = client_tx2.clone();
                request_set.spawn(async move {
                    let conn = connecting.await.expect("quinn connection");
                    let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
                        .expect("get remote pubkey");
                    loop {
                        let mut rx = conn.accept_uni().await.expect("accept uni");
                        // This code as been partially copied from agave source code:
                        let mut chunks: [Bytes; 4] = array::from_fn(|_| Bytes::new());
                        let mut total_chunks_read = 0;
                        while let Some(n_chunk) = rx.read_chunks(&mut chunks).await.expect("read") {
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
                        let req = SpyRequest {
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

#[tokio::test]
async fn send_buffer_should_land_properly() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = StakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map,
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        gateway_tx_channel_capacity: 100,
    };

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        gateway_tx_sink: transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn_with_default(gateway_kp.insecure_clone());

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(rx_server_identity.pubkey(), rx_server_addr);
    let tx_sig = Signature::new_unique();
    transaction_sink
        .send(GatewayTransaction {
            tx_sig,
            wire: Bytes::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    let spy_req = client_rx.recv().await.expect("recv");

    let GatewayResponse::TxSent(actual_resp) =
        gateway_response_source.recv().await.expect("recv response")
    else {
        panic!("Expected GatewayResponse::TxSent, got something else");
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
    let stake_info_map = StakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map,
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        gateway_tx_channel_capacity: 100,
    };

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        gateway_tx_sink: transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn_with_default(gateway_kp.insecure_clone());
    const MAX_TX: u64 = 5;

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(rx_server_identity.pubkey(), rx_server_addr);

    let tx_sig_vec = (0..MAX_TX)
        .map(|_| Signature::new_unique())
        .collect::<Vec<_>>();
    for (i, tx_sig) in tx_sig_vec.iter().enumerate() {
        transaction_sink
            .send(GatewayTransaction {
                tx_sig: *tx_sig,
                wire: Bytes::from(format!("helloworld{i}").as_bytes().to_vec()),
                remote_peer: rx_server_identity.pubkey(),
            })
            .await
            .expect("send tx");
    }

    let mut connection_id_spy = vec![];
    for i in 0..MAX_TX {
        let GatewayResponse::TxSent(actual_resp) =
            gateway_response_source.recv().await.expect("recv response")
        else {
            panic!("Expected GatewayResponse::TxSent, got something else");
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
    }
}

#[tokio::test]
async fn gateway_should_handle_connection_refused_by_peer() {
    let rx_server_addr = generate_random_local_addr();
    let rx_server_identity = Keypair::new();

    let gateway_kp = Keypair::new();
    let stake_info_map = StakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map,
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        gateway_tx_channel_capacity: 100,
    };
    let gateway_config = QuicGatewayConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        gateway_tx_sink: transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakedBaseEvictionStrategy),
    );

    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        drop(connecting);
    });

    let tx_sig = Signature::new_unique();
    transaction_sink
        .send(GatewayTransaction {
            tx_sig,
            wire: Bytes::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    rx_server_handle.await.expect("h2");

    let resp = gateway_response_source.recv().await.expect("recv response");

    let GatewayResponse::TxDrop(actual_resp) = resp else {
        panic!("Expected GatewayResponse::TxSent, got something {resp:?}");
    };

    assert_eq!(actual_resp.tx_sig, tx_sig);
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
    let gateway_config = QuicGatewayConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let gateway_kp = Keypair::new();
    let stake_info_map = StakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service =
        FakeLeaderTpuInfoService::from_iter([(rx_server_identity.pubkey(), rx_server_addr)]);

    let gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map,
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        gateway_tx_channel_capacity: 100,
    };

    let TokioQuicGatewaySession {
        mut gateway_identity_updater,
        gateway_tx_sink: transaction_sink,
        gateway_response_source: _,
        gateway_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakedBaseEvictionStrategy),
    );

    let (mut client_rx, _rx_server_handle) =
        MockedRemoteValidator::spawn(rx_server_identity.pubkey(), rx_server_addr);

    transaction_sink
        .send(GatewayTransaction {
            tx_sig: Signature::new_unique(),
            wire: Bytes::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    let spy_request1 = client_rx.recv().await.expect("recv");
    let actual_remote_key1 = spy_request1.from;
    assert_eq!(actual_remote_key1, gateway_kp.pubkey());

    let gateway_identity2 = Keypair::new();

    gateway_identity_updater
        .update_identity(gateway_identity2.insecure_clone())
        .await;

    transaction_sink
        .send(GatewayTransaction {
            tx_sig: Signature::new_unique(),
            wire: Bytes::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

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
    let gateway_config = QuicGatewayConfig {
        max_connection_attempts: 1,
        ..Default::default()
    };
    let gateway_kp = Keypair::new();
    let stake_info_map = StakeInfoMap::constant([(gateway_kp.pubkey(), 1000)]);
    let fake_tpu_info_service = FakeLeaderTpuInfoService::from_iter([
        (remote_validator_identity1.pubkey(), remote_validator_addr1),
        (remote_validator_identity2.pubkey(), remote_validator_addr2),
    ]);

    let gateway_spawner = TokioQuicGatewaySpawner {
        stake_info_map,
        leader_tpu_info_service: Arc::new(fake_tpu_info_service.clone()),
        gateway_tx_channel_capacity: 100,
    };

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        gateway_tx_sink: transaction_sink,
        gateway_response_source: _,
        gateway_join_handle: _,
    } = gateway_spawner.spawn(
        gateway_kp.insecure_clone(),
        gateway_config,
        Arc::new(StakedBaseEvictionStrategy),
    );

    let (validator_rx1, _rx_server_handle) =
        MockedRemoteValidator::spawn(remote_validator_identity1.pubkey(), remote_validator_addr1);

    let (validator_rx2, _rx_server_handle) =
        MockedRemoteValidator::spawn(remote_validator_identity2.pubkey(), remote_validator_addr2);

    let mut stream_map = StreamMap::new();

    stream_map.insert(
        remote_validator_identity1.pubkey(),
        ReceiverStream::new(validator_rx1),
    );

    stream_map.insert(
        remote_validator_identity2.pubkey(),
        ReceiverStream::new(validator_rx2),
    );

    // Send it to the first remote peer
    transaction_sink
        .send(GatewayTransaction {
            tx_sig: Signature::new_unique(),
            wire: Bytes::from("helloworld".as_bytes()),
            remote_peer: remote_validator_identity1.pubkey(),
        })
        .await
        .expect("send tx");

    // Send it to the second remote peer

    transaction_sink
        .send(GatewayTransaction {
            tx_sig: Signature::new_unique(),
            wire: Bytes::from("helloworld2".as_bytes()),
            remote_peer: remote_validator_identity2.pubkey(),
        })
        .await
        .expect("send tx");

    let actual_remote_validator1 = stream_map.next().await.expect("next").0;

    let actual_remote_validator2 = stream_map.next().await.expect("next").0;

    assert_eq!(
        actual_remote_validator1,
        remote_validator_identity1.pubkey()
    );
    assert_eq!(
        actual_remote_validator2,
        remote_validator_identity2.pubkey()
    );
}
