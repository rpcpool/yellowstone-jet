use {
    futures::stream,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signature::Signature,
    tokio::sync::{broadcast, mpsc},
    yellowstone_grpc_proto::{
        prelude::{
            BlockHeight, SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
            SubscribeUpdateTransactionStatus, subscribe_update::UpdateOneof,
        },
        tonic::Status,
    },
    yellowstone_jet::{
        grpc_geyser::{GeyserSubscriber, GrpcUpdateMessage, TransactionReceived},
        util::{CommitmentLevel, SlotStatus},
    },
};

/*
 * Test helpers to create gRPC messages
 */
const fn create_slot_update(slot: Slot, status: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            status,
            parent: None,
            dead_error: None,
        })),
        filters: vec![],
        created_at: None,
    }
}

fn create_block_meta(slot: Slot, block_height: Option<u64>) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: Hash::new_unique().to_string(),
            rewards: None,
            block_time: None,
            block_height: block_height.map(|h| BlockHeight { block_height: h }),
            parent_blockhash: Default::default(),
            parent_slot: 0,
            executed_transaction_count: 0,
            entries_count: 0,
        })),
        filters: vec![],
        created_at: None,
    }
}

fn create_transaction_status(slot: Slot, signature: &Signature) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::TransactionStatus(
            SubscribeUpdateTransactionStatus {
                slot,
                signature: signature.as_ref().to_vec(),
                is_vote: false,
                index: 0,
                err: None,
            },
        )),
        filters: vec![],
        created_at: None,
    }
}

#[tokio::test]
async fn test_block_meta_before_slot_update() {
    let (slots_tx, mut slots_rx) = broadcast::channel(100);
    let (block_meta_tx, mut block_meta_rx) = broadcast::channel(100);
    let (transactions_tx, _) = mpsc::channel(100);

    // Block meta arrives before slot update
    let messages = vec![
        Ok(create_block_meta(100, Some(1000))),
        Ok(create_slot_update(100, SlotStatus::SlotConfirmed as i32)),
    ];
    let stream = stream::iter(messages);

    let _ =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Verify slot update
    let slot_update = slots_rx.recv().await.unwrap();
    assert_eq!(slot_update.slot, 100);
    assert_eq!(slot_update.slot_status, SlotStatus::SlotConfirmed);

    // Verify block meta was emitted when slot update arrived
    let block_meta = block_meta_rx.recv().await.unwrap();
    assert_eq!(block_meta.slot, 100);
    assert_eq!(block_meta.block_height, 1000);
    assert_eq!(block_meta.commitment, CommitmentLevel::Confirmed);
}

#[tokio::test]
async fn test_non_commitment_status_no_block_meta() {
    let (slots_tx, mut slots_rx) = broadcast::channel(100);
    let (block_meta_tx, mut block_meta_rx) = broadcast::channel(100);
    let (transactions_tx, _) = mpsc::channel(100);

    // Non-commitment statuses should not emit block meta
    let messages = vec![
        Ok(create_slot_update(
            100,
            SlotStatus::SlotFirstShredReceived as i32,
        )),
        Ok(create_block_meta(100, Some(1000))),
        Ok(create_slot_update(100, SlotStatus::SlotCompleted as i32)),
        Ok(create_slot_update(100, SlotStatus::SlotCreatedBank as i32)),
        Ok(create_slot_update(100, SlotStatus::SlotDead as i32)),
    ];
    let stream = stream::iter(messages);

    let _ =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Verify we got all the slot updates
    assert_eq!(
        slots_rx.recv().await.unwrap().slot_status,
        SlotStatus::SlotFirstShredReceived
    );
    assert_eq!(
        slots_rx.recv().await.unwrap().slot_status,
        SlotStatus::SlotCompleted
    );
    assert_eq!(
        slots_rx.recv().await.unwrap().slot_status,
        SlotStatus::SlotCreatedBank
    );
    assert_eq!(
        slots_rx.recv().await.unwrap().slot_status,
        SlotStatus::SlotDead
    );

    // Verify NO block meta was sent
    assert!(block_meta_rx.try_recv().is_err());
}

#[tokio::test]
async fn test_multiple_commitment_statuses() {
    let (slots_tx, _) = broadcast::channel(100);
    let (block_meta_tx, mut block_meta_rx) = broadcast::channel(100);
    let (transactions_tx, _transaction_rx) = mpsc::channel(100);

    // All commitment statuses should emit block meta
    let messages = vec![
        Ok(create_block_meta(100, Some(1000))),
        Ok(create_slot_update(100, SlotStatus::SlotProcessed as i32)),
        Ok(create_slot_update(100, SlotStatus::SlotConfirmed as i32)),
        Ok(create_slot_update(100, SlotStatus::SlotFinalized as i32)),
    ];
    let stream = stream::iter(messages);

    let _ =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Verify we get block meta for each commitment level
    let meta1 = block_meta_rx.recv().await.unwrap();
    assert_eq!(meta1.commitment, CommitmentLevel::Processed);
    assert_eq!(meta1.slot, 100);
    assert_eq!(meta1.block_height, 1000);

    let meta2 = block_meta_rx.recv().await.unwrap();
    assert_eq!(meta2.commitment, CommitmentLevel::Confirmed);
    assert_eq!(meta2.slot, 100);

    let meta3 = block_meta_rx.recv().await.unwrap();
    assert_eq!(meta3.commitment, CommitmentLevel::Finalized);
    assert_eq!(meta3.slot, 100);
}

#[tokio::test]
async fn test_slot_tracking_cleanup_on_finalized() {
    let (slots_tx, _) = broadcast::channel(100);
    let (block_meta_tx, mut block_meta_rx) = broadcast::channel(100);
    let (transactions_tx, _transactions_rx) = mpsc::channel(100);

    /*
     * Test that slots before finalized are cleaned up from tracking
     */
    let messages = vec![
        // Add some slots with block meta
        Ok(create_block_meta(95, Some(995))),
        Ok(create_slot_update(95, SlotStatus::SlotProcessed as i32)),
        Ok(create_block_meta(96, Some(996))),
        Ok(create_slot_update(96, SlotStatus::SlotProcessed as i32)),
        Ok(create_block_meta(97, Some(997))),
        Ok(create_slot_update(97, SlotStatus::SlotProcessed as i32)),
        Ok(create_block_meta(98, Some(998))),
        Ok(create_slot_update(98, SlotStatus::SlotProcessed as i32)),
        Ok(create_block_meta(99, Some(999))),
        Ok(create_slot_update(99, SlotStatus::SlotProcessed as i32)),
        Ok(create_block_meta(100, Some(1000))),
        Ok(create_slot_update(100, SlotStatus::SlotProcessed as i32)),
        // Finalize slot 98 - should clean up slots < 98
        Ok(create_slot_update(98, SlotStatus::SlotFinalized as i32)),
        // Now slot 99 should still work (it's >= 98)
        Ok(create_slot_update(99, SlotStatus::SlotConfirmed as i32)),
    ];
    let stream = stream::iter(messages);

    let result =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Stream ends, so we expect an error
    assert!(result.is_err());

    // Count block metas - we should have received them
    let mut count = 0;
    while block_meta_rx.try_recv().is_ok() {
        count += 1;
    }

    // We should have:
    // - 6 for initial Processed statuses (slots 95-100)
    // - 1 for slot 98 Finalized
    // - 1 for slot 99 Confirmed
    assert_eq!(count, 8);
}

#[tokio::test]
async fn test_transaction_status_handling() {
    let (slots_tx, _) = broadcast::channel(100);
    let (block_meta_tx, _) = broadcast::channel(100);
    let (transactions_tx, mut transactions_rx) = mpsc::channel(100);

    let sig = Signature::new_unique();
    let messages = vec![
        Ok(create_transaction_status(100, &sig)),
        Ok(create_transaction_status(101, &sig)),
    ];
    let stream = stream::iter(messages);

    let _ =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Verify transactions were sent
    match transactions_rx.recv().await.unwrap() {
        GrpcUpdateMessage::Transaction(TransactionReceived { slot, signature }) => {
            assert_eq!(slot, 100);
            assert_eq!(signature, sig);
        }
        _ => panic!("Expected transaction message"),
    }

    match transactions_rx.recv().await.unwrap() {
        GrpcUpdateMessage::Transaction(TransactionReceived { slot, signature }) => {
            assert_eq!(slot, 101);
            assert_eq!(signature, sig);
        }
        _ => panic!("Expected transaction message"),
    }
}

#[tokio::test]
async fn test_stream_error_handling() {
    let (slots_tx, _) = broadcast::channel(100);
    let (block_meta_tx, _) = broadcast::channel(100);
    let (transactions_tx, _transactions_rx) = mpsc::channel(100);

    let messages = vec![
        Ok(create_slot_update(100, SlotStatus::SlotProcessed as i32)),
        Err(Status::unavailable("connection lost")),
    ];
    let stream = stream::iter(messages);

    let result =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Should return StreamError
    assert!(result.is_err());
    match result.unwrap_err() {
        yellowstone_jet::grpc_geyser::GeyserError::StreamError(_) => {}
        e => panic!("Expected StreamError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_invalid_block_meta() {
    let (slots_tx, _) = broadcast::channel(100);
    let (block_meta_tx, _) = broadcast::channel(100);
    let (transactions_tx, _transactions_rx) = mpsc::channel(100);

    let invalid_meta = create_block_meta(100, None);

    let messages = vec![Ok(invalid_meta)];
    let stream = stream::iter(messages);

    let result =
        GeyserSubscriber::process_grpc_stream(stream, &slots_tx, &block_meta_tx, &transactions_tx)
            .await;

    // Should return MissingBlockHeight error
    assert!(result.is_err());
    match result.unwrap_err() {
        yellowstone_jet::grpc_geyser::GeyserError::MissingBlockHeight => {}
        e => panic!("Expected MissingBlockHeight, got {:?}", e),
    }
}

#[cfg(test)]
mod memory_leak_tests {
    use {
        futures::stream,
        solana_clock::Slot,
        solana_hash::Hash,
        solana_signature::Signature,
        std::time::Instant,
        tokio::{
            sync::{broadcast, mpsc},
            time::{interval, Duration},
        },
        yellowstone_grpc_proto::{
            prelude::{
                BlockHeight, SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
                SubscribeUpdateTransactionStatus, subscribe_update::UpdateOneof,
            },
        },
        yellowstone_jet::{
            grpc_geyser::{GeyserSubscriber, GrpcUpdateMessage, BlockMetaWithCommitment, SlotUpdateWithStatus, TransactionReceived},
            util::{CommitmentLevel, SlotStatus},
        },
    };

    fn create_slot_update(slot: Slot, status: i32) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                status,
                parent: None,
                dead_error: None,
            })),
            filters: vec![],
            created_at: None,
        }
    }

    fn create_block_meta(slot: Slot, block_height: Option<u64>) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: Hash::new_unique().to_string(),
                rewards: None,
                block_time: None,
                block_height: block_height.map(|h| BlockHeight { block_height: h }),
                parent_blockhash: Default::default(),
                parent_slot: 0,
                executed_transaction_count: 0,
                entries_count: 0,
            })),
            filters: vec![],
            created_at: None,
        }
    }

    fn create_transaction_status(slot: Slot, signature: &Signature) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::TransactionStatus(
                SubscribeUpdateTransactionStatus {
                    slot,
                    signature: signature.as_ref().to_vec(),
                    is_vote: false,
                    index: 0,
                    err: None,
                },
            )),
            filters: vec![],
            created_at: None,
        }
    }

    #[cfg(target_os = "linux")]
    fn get_memory_usage() -> u64 {
        use std::fs;
        let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse::<u64>().unwrap_or(0) * 1024;
                }
            }
        }
        0
    }

    #[cfg(not(target_os = "linux"))]
    fn get_memory_usage() -> u64 {
        0
    }

    #[tokio::test]
    #[ignore = "Memory leak test - run manually with: cargo test -- --ignored"]
    async fn test_memory_leak_with_slow_consumer() {
        let (slots_tx, mut slots_rx) = broadcast::channel::<SlotUpdateWithStatus>(10_000);

        let start_memory = get_memory_usage();
        let start_time = Instant::now();

        let slow_consumer = tokio::spawn(async move {
            let mut received = 0;
            loop {
                // Simulate slow processing
                tokio::time::sleep(Duration::from_millis(100)).await;

                match slots_rx.recv().await {
                    Ok(_) => {
                        received += 1;
                        if received % 100 == 0 {
                            println!("Slow consumer received {} messages", received);
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        println!("WARNING: Consumer lagged by {} messages!", n);
                    },
                    Err(_) => break,
                }
            }
        });

        // Fast producer
        let producer = tokio::spawn(async move {
            for slot in 0..50_000 {
                let _ = slots_tx.send(SlotUpdateWithStatus {
                    slot,
                    slot_status: SlotStatus::SlotFirstShredReceived,
                });

                // Check if channel is backing up
                if slot % 1000 == 0 {
                    let lag = slots_tx.len();
                    let current_memory = get_memory_usage();
                    let memory_mb = current_memory / 1_048_576;
                    let memory_growth_mb = (current_memory as i64 - start_memory as i64) / 1_048_576;

                    println!(
                        "Slot {} - Channel lag: {}, Memory: {} MB (growth: {} MB)",
                        slot, lag, memory_mb, memory_growth_mb
                    );
                }

                // Small delay to prevent overwhelming
                if slot % 100 == 0 {
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }
            }
            println!("Producer finished sending");
        });

        // Wait for producer to finish
        let _ = producer.await;

        // Give consumer time to catch up
        tokio::time::sleep(Duration::from_secs(5)).await;

        let elapsed = start_time.elapsed().as_secs();
        let end_memory = get_memory_usage();
        let memory_growth = (end_memory as i64 - start_memory as i64) / 1_048_576;

        println!(
            "Test completed in {}s. Memory growth: {} MB",
            elapsed, memory_growth
        );

        drop(slow_consumer);
    }

    #[tokio::test]
    #[ignore = "Memory leak test - run manually with: cargo test -- --ignored"]
    async fn test_grpc_stream_with_realistic_pattern() {
        let start_memory = get_memory_usage();

        // Create a stream that mimics real gRPC patterns
        let messages: Vec<_> = (0..10_000)
            .flat_map(|slot| {
                let mut updates = vec![];

                // Typical mainnet pattern for a slot with interslot_updates
                updates.push(Ok(create_slot_update(slot, SlotStatus::SlotFirstShredReceived as i32)));
                updates.push(Ok(create_slot_update(slot, SlotStatus::SlotCreatedBank as i32)));
                updates.push(Ok(create_block_meta(slot, Some(slot * 2))));
                updates.push(Ok(create_slot_update(slot, SlotStatus::SlotProcessed as i32)));
                updates.push(Ok(create_slot_update(slot, SlotStatus::SlotCompleted as i32)));
                updates.push(Ok(create_slot_update(slot - 31, SlotStatus::SlotConfirmed as i32)));

                // Add some transactions
                for _ in 0..5 {
                    updates.push(Ok(create_transaction_status(slot, &Signature::new_unique())));
                }

                // Finalization happens around ~31 slots later
                if slot > 31 {
                    updates.push(Ok(create_slot_update(slot - 64, SlotStatus::SlotFinalized as i32)));
                }

                updates
            })
            .collect();

        println!("Created {} messages to process", messages.len());
        let stream = stream::iter(messages);

        let (slots_tx, mut slots_rx) = broadcast::channel(10_000);
        let (block_meta_tx, mut block_meta_rx) = broadcast::channel(1_000);
        let (transactions_tx, mut transactions_rx) = mpsc::channel(1_000_000);

        let slots_tx_monitor = slots_tx.clone();
        let block_meta_tx_monitor = block_meta_tx.clone();

        // Simulate slow slot consumer
        let slot_consumer = tokio::spawn(async move {
            let mut count = 0;
            while let Ok(_msg) = slots_rx.recv().await {
                count += 1;
                // Simulate processing delay every 1000 messages
                if count % 1000 == 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    println!("Slot consumer processed {} messages", count);
                }
            }
        });

        // Simulate slow block meta consumer
        let block_consumer = tokio::spawn(async move {
            let mut count = 0;
            while let Ok(_msg) = block_meta_rx.recv().await {
                count += 1;
                if count % 1000 == 0 {
                    println!("Block meta consumer processed {} messages", count);
                }
            }
        });

        // Drain transactions to prevent channel backup
        let tx_consumer = tokio::spawn(async move {
            let mut count = 0;
            while transactions_rx.recv().await.is_some() {
                count += 1;
                if count % 10000 == 0 {
                    println!("Transaction consumer processed {} messages", count);
                }
            }
        });

        let monitor = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));
            let start = Instant::now();
            loop {
                interval.tick().await;
                let current_memory = get_memory_usage();
                let memory_growth = (current_memory as i64 - start_memory as i64) / 1_048_576;
                println!(
                    "Processing... Time: {}s, Memory: {} MB (growth: {} MB), Slots lag: {}, BlockMeta lag: {}",
                    start.elapsed().as_secs(),
                    current_memory / 1_048_576,
                    memory_growth,
                    slots_tx_monitor.len(),
                    block_meta_tx_monitor.len()
                );

                if memory_growth > 100 {
                    eprintln!("WARNING: Memory growth exceeds 100MB!");
                }
            }
        });

        let process_handle = tokio::spawn(async move {
            let result = GeyserSubscriber::process_grpc_stream(
                stream,
                &slots_tx,
                &block_meta_tx,
                &transactions_tx
            ).await;

            drop(slots_tx);
            drop(block_meta_tx);
            drop(transactions_tx);

            result
        });

        let _ = process_handle.await;

        monitor.abort();

        let _ = slot_consumer.await;
        let _ = block_consumer.await;
        let _ = tx_consumer.await;

        let end_memory = get_memory_usage();
        let memory_growth = (end_memory as i64 - start_memory as i64) / 1_048_576;

        println!("Memory growth after processing: {} MB", memory_growth);

        if memory_growth > 50 {
            eprintln!("WARNING: Excessive memory growth detected: {} MB", memory_growth);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "Memory leak test - run manually with: cargo test -- --ignored"]
    async fn test_memory_leak_extended_run() {
        let start_memory = get_memory_usage();
        let start_time = Instant::now();

        let (slots_tx, _) = broadcast::channel::<SlotUpdateWithStatus>(10_000);
        let (block_meta_tx, _) = broadcast::channel::<BlockMetaWithCommitment>(1_000);
        let (transactions_tx, mut transactions_rx) = mpsc::channel::<GrpcUpdateMessage>(1_000_000);

        let tx_consumer = tokio::spawn(async move {
            while transactions_rx.recv().await.is_some() {}
        });

        let monitor_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let current_memory = get_memory_usage();
                let elapsed = start_time.elapsed().as_secs();
                let memory_growth = (current_memory as i64 - start_memory as i64) / 1_048_576;
                let growth_rate = if elapsed > 0 {
                    (memory_growth as f64 / elapsed as f64) * 3600.0 // MB per hour
                } else {
                    0.0
                };

                println!(
                    "Time: {}s, Memory: {} MB (growth: {} MB, rate: {:.1} MB/hour)",
                    elapsed,
                    current_memory / 1_048_576,
                    memory_growth,
                    growth_rate
                );

                if growth_rate > 2000.0 {
                    eprintln!("CRITICAL: Memory growth rate exceeds 2GB/hour!");
                }
            }
        });

        let producer = tokio::spawn(async move {
            let mut slot = 0u64;
            let mut interval = interval(Duration::from_millis(400));

            for _ in 0..30_000 {
                interval.tick().await;

                let _ = slots_tx.send(SlotUpdateWithStatus {
                    slot,
                    slot_status: SlotStatus::SlotFirstShredReceived,
                });

                let _ = slots_tx.send(SlotUpdateWithStatus {
                    slot,
                    slot_status: SlotStatus::SlotCreatedBank,
                });

                // Send block meta
                let _ = block_meta_tx.send(BlockMetaWithCommitment {
                    slot,
                    block_height: slot - slot / 100 * 20, // -20% because some slots are dead
                    block_hash: Hash::new_unique(),
                    commitment: CommitmentLevel::Processed,
                });

                // More slot updates
                let _ = slots_tx.send(SlotUpdateWithStatus {
                    slot,
                    slot_status: SlotStatus::SlotProcessed,
                });

                let _ = slots_tx.send(SlotUpdateWithStatus {
                    slot,
                    slot_status: SlotStatus::SlotCompleted,
                });

                // Send 10 transactions per slot
                for _ in 0..10 {
                    let _ = transactions_tx.send(GrpcUpdateMessage::Transaction(TransactionReceived {
                        slot,
                        signature: Signature::new_unique(),
                    })).await;
                }

                slot += 1;
            }
        });

        // Run for 2 minutes
        tokio::time::sleep(Duration::from_secs(120)).await;

        // Cleanup
        producer.abort();
        monitor_handle.abort();
        tx_consumer.abort();

        let end_memory = get_memory_usage();
        let total_time = start_time.elapsed().as_secs();
        let memory_growth = (end_memory as i64 - start_memory as i64) / 1_048_576;
        let growth_rate = (memory_growth as f64 / total_time as f64) * 3600.0;

        println!(
            "\nFinal results:\nTotal time: {}s\nMemory growth: {} MB\nGrowth rate: {:.1} MB/hour",
            total_time, memory_growth, growth_rate
        );
    }
}
