mod testkit;

use {
    bytes::Bytes,
    solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer},
    std::{
        array,
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, RwLock},
    },
    testkit::{build_random_endpoint, generate_random_local_addr},
    tokio::sync::mpsc,
    yellowstone_jet::{
        quic_gateway::{
            GatewayResponse, GatewayTransaction, LeaderTpuInfoService, QuicGatewayConfig,
            TokioQuicGatewaySession, TokioQuicGatewaySpawner, TxDropReason,
        },
        stake::StakeInfoMap,
    },
};

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
}

#[async_trait::async_trait]
impl LeaderTpuInfoService for FakeLeaderTpuInfoService {
    async fn get_tpu_socket_addr(&self, leader_pubkey: Pubkey) -> Option<SocketAddr> {
        let shared = self.shared.read().expect("read lock");
        shared.get(&leader_pubkey).cloned()
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

    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn_with_default(gateway_kp.insecure_clone());

    let (client_tx, mut client_rx) = mpsc::channel(100);
    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let conn = connecting.await.expect("quinn connection");
        let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
            .expect("get remote pubkey");

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
        client_tx.send((remote_key, combined)).await.expect("send");
    });

    transaction_sink
        .send(GatewayTransaction {
            tx_id: 1,
            wire: Arc::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    rx_server_handle.await.expect("h2");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");

    let GatewayResponse::TxSent(actual_resp) =
        gateway_response_source.recv().await.expect("recv response")
    else {
        panic!("Expected GatewayResponse::TxSent, got something else");
    };

    assert_eq!(actual_resp.tx_id, 1);
    assert_eq!(
        actual_resp.remote_peer_identity,
        rx_server_identity.pubkey()
    );

    let msg = String::from_utf8(buf).expect("utf8");
    assert_eq!(msg, "helloworld");
    assert_eq!(actual_remote_key, gateway_kp.pubkey());
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

    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let TokioQuicGatewaySession {
        gateway_identity_updater: _,
        transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn_with_default(gateway_kp.insecure_clone());
    const MAX_TX: u64 = 5;
    let (client_tx, mut client_rx) = mpsc::channel(100);
    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let conn = connecting.await.expect("quinn connection");
        let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
            .expect("get remote pubkey");

        for _ in 0..MAX_TX {
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
            client_tx.send((remote_key, combined)).await.expect("send");
        }
    });

    for tx_id in 1..=MAX_TX {
        transaction_sink
            .send(GatewayTransaction {
                tx_id,
                wire: Arc::from(format!("helloworld{}", tx_id).as_bytes()),
                remote_peer: rx_server_identity.pubkey(),
            })
            .await
            .expect("send tx");
    }

    rx_server_handle.await.expect("h2");

    for i in 0..MAX_TX {
        let GatewayResponse::TxSent(actual_resp) =
            gateway_response_source.recv().await.expect("recv response")
        else {
            panic!("Expected GatewayResponse::TxSent, got something else");
        };
        assert_eq!(actual_resp.tx_id, i + 1);
        let (_, buf) = client_rx.recv().await.expect("recv");

        let msg = String::from_utf8(buf).expect("utf8");
        assert_eq!(msg, format!("helloworld{}", i + 1));
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
        transaction_sink,
        mut gateway_response_source,
        gateway_join_handle: _,
    } = gateway_spawner.spawn(gateway_kp.insecure_clone(), gateway_config);

    let rx_server_handle = tokio::spawn(async move {
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        drop(connecting);
    });

    transaction_sink
        .send(GatewayTransaction {
            tx_id: 1,
            wire: Arc::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    rx_server_handle.await.expect("h2");

    let resp = gateway_response_source.recv().await.expect("recv response");

    let GatewayResponse::TxDrop(actual_resp) = resp else {
        panic!("Expected GatewayResponse::TxSent, got something {resp:?}");
    };

    assert_eq!(actual_resp.tx_id, 1);
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

    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);

    let TokioQuicGatewaySession {
        mut gateway_identity_updater,
        transaction_sink,
        gateway_response_source: _,
        gateway_join_handle: _,
    } = gateway_spawner.spawn(gateway_kp.insecure_clone(), gateway_config);

    let (client_tx, mut client_rx) = mpsc::channel(100);
    let _rx_server_handle = tokio::spawn(async move {
        loop {
            let connecting = rx_server_endpoint.accept().await.expect("accept");
            let conn = connecting.await.expect("quinn connection");
            let remote_key = solana_streamer::nonblocking::quic::get_remote_pubkey(&conn)
                .expect("get remote pubkey");
            tracing::trace!("Remote connecting: {remote_key:?}");
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
            tracing::trace!("Received data from remote: {remote_key:?} - {combined:?}");
            client_tx.send((remote_key, combined)).await.expect("send");
        }
    });

    transaction_sink
        .send(GatewayTransaction {
            tx_id: 1,
            wire: Arc::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    let gateway_identity2 = Keypair::new();
    let (actual_remote_key1, _) = client_rx.recv().await.expect("recv");

    gateway_identity_updater
        .update_identity(gateway_identity2.insecure_clone())
        .await;

    transaction_sink
        .send(GatewayTransaction {
            tx_id: 2,
            wire: Arc::from("helloworld".as_bytes()),
            remote_peer: rx_server_identity.pubkey(),
        })
        .await
        .expect("send tx");

    let (actual_remote_key2, _) = client_rx.recv().await.expect("recv");

    assert_eq!(actual_remote_key1, gateway_kp.pubkey());
    assert_eq!(actual_remote_key2, gateway_identity2.pubkey());
}
