use {
    futures::stream,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signature::Signature,
    tokio::sync::{broadcast, mpsc},
    tokio_util::sync::CancellationToken,
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
    let cancellation_token = CancellationToken::new();
    let _ = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
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

    let cancellation_token = CancellationToken::new();
    let _ = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
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

    let cancellation_token = CancellationToken::new();
    let _ = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
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

    let cancellation_token = CancellationToken::new();
    let result = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
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
    let cancellation_token = CancellationToken::new();
    let _ = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
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
    let cancellation_token = CancellationToken::new();
    let result = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
    .await;

    // Should return StreamError
    assert!(result.is_err());
    match result.unwrap_err() {
        yellowstone_jet::grpc_geyser::GeyserError::StreamError(_) => {}
        e => panic!("Expected StreamError, got {e:?}"),
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
    let cancellation_token = CancellationToken::new();
    let result = GeyserSubscriber::process_grpc_stream(
        stream,
        &slots_tx,
        &block_meta_tx,
        &transactions_tx,
        true,
        cancellation_token.clone(),
    )
    .await;

    // Should return MissingBlockHeight error
    assert!(result.is_err());
    match result.unwrap_err() {
        yellowstone_jet::grpc_geyser::GeyserError::MissingBlockHeight => {}
        e => panic!("Expected MissingBlockHeight, got {e:?}"),
    }
}
