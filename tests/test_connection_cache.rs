mod testkit;

use {
    bytes::Bytes,
    futures::channel::oneshot,
    quinn::ReadExactError,
    solana_sdk::{signature::Keypair, signer::Signer},
    std::{array, sync::Arc, thread, time::Duration},
    testkit::{build_random_endpoint, default_config_quic, generate_random_local_addr},
    tokio::sync::mpsc,
    yellowstone_jet::{
        cluster_tpu_info::TpuInfo,
        quic_solana::{ConnectionCache, NullIdentityFlusher, QuicError},
        stake::StakeInfoMap,
    },
};

#[test]
fn send_buffer_should_timeout_on_idle_connection() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let mut config = default_config_quic();
    config.max_idle_timeout = std::time::Duration::from_secs(1);

    let single_threaded_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");

    let _server_handle = single_threaded_runtime.spawn(async move {
        let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);
        let connecting = rx_server_endpoint.accept().await.expect("accept");
        let _conn = connecting.await.expect("quinn connection");
        // Make a blocking operation here to stall tokio runtime causing connection keep-alive timeout in the send_buffer thread.
        thread::sleep(std::time::Duration::from_secs(10));
    });

    let send_buffer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");

    let send_buffer_result = send_buffer_rt
        .block_on(async move {
            let (quic_session, _identity_map) = ConnectionCache::new(
                config,
                connection_cache_kp.insecure_clone(),
                StakeInfoMap::empty(),
                NullIdentityFlusher,
            );
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
        })
        .expect_err("send buffer");

    assert!(matches!(
        send_buffer_result,
        QuicError::ConnectionError(quinn::ConnectionError::TimedOut)
    ));
}

#[tokio::test]
async fn send_buffer_should_land_properly() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let config = default_config_quic();

    let (quic_session, _identity_map) = ConnectionCache::new(
        config,
        connection_cache_kp.insecure_clone(),
        StakeInfoMap::empty(),
        NullIdentityFlusher,
    );
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
    });

    let (client_tx, mut client_rx) = mpsc::channel(100);
    let h2 = tokio::spawn(async move {
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

    start_tx.send(()).expect("send");
    h2.await.expect("h2");
    h1.await.expect("h1");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    assert_eq!(msg, "helloworld");
    assert_eq!(actual_remote_key, connection_cache_kp.pubkey());
}

#[tokio::test]
async fn test_update_identity() {
    let connection_cache_kp = Keypair::new();
    let rx_server_addr = generate_random_local_addr();
    let (rx_server_endpoint, _) = build_random_endpoint(rx_server_addr);
    let config = default_config_quic();
    let (quic_session, identity_map) = ConnectionCache::new(
        config,
        connection_cache_kp.insecure_clone(),
        StakeInfoMap::empty(),
        NullIdentityFlusher,
    );

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
                client_tx.send((remote_key, combined)).await.expect("send");
            });
        }
    });

    start_tx.send(()).expect("send");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    let current_identity1 = identity_map.get_identity().await;
    assert_eq!(actual_remote_key, current_identity1);
    assert_eq!(msg, "helloworld");
    let identity_map_arc = Arc::new(identity_map);
    let identity_map_arc2 = Arc::clone(&identity_map_arc);

    let h_update = tokio::spawn(async move {
        let new_kp = Keypair::new();
        identity_map_arc2.update_keypair(&new_kp).await
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    h_update.await.expect("Error updating identity");
    let (actual_remote_key, buf) = client_rx.recv().await.expect("recv");
    let msg = String::from_utf8(buf).expect("utf8");
    let current_identity2 = identity_map_arc.get_identity().await;
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
    let (quic_session, identity_map) = ConnectionCache::new(
        config,
        connection_cache_kp.insecure_clone(),
        StakeInfoMap::empty(),
        NullIdentityFlusher,
    );

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

    let h_update = tokio::spawn(async move {
        let new_kp = Keypair::new();
        identity_map.update_keypair(&new_kp).await
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    h_update.await.expect("Error updating identity");
    let maybe = client_rx.recv().await;
    assert!(maybe.is_none());
    let result = rx_server_handle.await.expect("rx_server_handle");
    assert!(matches!(result, Err(ReadExactError::FinishedEarly(_))));
}
