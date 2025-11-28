use {
    crate::slot::AtomicSlotTracker,
    futures::Stream,
    std::{collections::HashMap, panic, sync::Arc},
    tokio::task::JoinHandle,
    tokio_stream::StreamExt,
    yellowstone_grpc_client::{GeyserGrpcClientResult, Interceptor},
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdate,
            subscribe_update::UpdateOneof,
        },
        tonic::Status,
    },
};

pub(crate) const SLOT_TRACKER_DM_FILTER_NAME: &str = "jet-tpu-client";

pub struct YellowstoneSlotTrackerOk {
    pub atomic_slot_tracker: Arc<AtomicSlotTracker>,
    pub join_handle: JoinHandle<()>,
}

pub(crate) fn get_yellowstone_slot_tracker_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            SLOT_TRACKER_DM_FILTER_NAME.to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        ..Default::default()
    }
}

struct AutoCloseSlotTracker {
    slot_tracker: Arc<AtomicSlotTracker>,
}

impl Drop for AutoCloseSlotTracker {
    fn drop(&mut self) {
        self.slot_tracker
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

///
/// Background task to update the AtomicSlotTracker from the Yellowstone Geyser slot stream
///
async fn atomic_slot_tracker_loop<S>(mut dm_slot_stream: S, to_drop: AutoCloseSlotTracker)
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
{
    let shared = Arc::clone(&to_drop.slot_tracker);
    let mut current_slot = shared.slot.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let result = dm_slot_stream.next().await;
        if result.is_none() {
            tracing::warn!("Yellowstone slot tracker stream ended");
            break;
        }

        let response = match result.unwrap() {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker stream error: {:?}", err);
                drop(to_drop);
                panic::panic_any(err);
            }
        };
        match response.update_oneof.expect("update_oneof") {
            UpdateOneof::Slot(subscribe_update_slot) => {
                let slot = subscribe_update_slot.slot;
                if slot <= current_slot {
                    // Ignore out-of-order or duplicate slot updates
                    continue;
                }
                current_slot = slot;
                tracing::trace!("Yellowstone slot tracker received slot update: {}", slot);
                shared
                    .slot
                    .store(current_slot, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {
                // Ignore other updates
            }
        }
    }
    drop(to_drop);
}

///
/// Creates an [`AtomicSlotTracker`] that tracks the latest slot from Yellowstone Geyser.
///
pub async fn atomic_slot_tracker<I>(
    mut geyser_client: yellowstone_grpc_client::GeyserGrpcClient<I>,
) -> GeyserGrpcClientResult<Option<YellowstoneSlotTrackerOk>>
where
    I: Interceptor + 'static,
{
    let subscribe_request = get_yellowstone_slot_tracker_subscribe_request();
    let mut stream = geyser_client.subscribe_once(subscribe_request).await?;

    let initial_slot: u64;
    // wait for the first slot update to establish the tip
    loop {
        let Some(result) = stream.next().await else {
            return Ok(None);
        };

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker stream error: {:?}", err);
                return Err(yellowstone_grpc_client::GeyserGrpcClientError::TonicStatus(
                    err,
                ));
            }
        };

        match response.update_oneof.expect("update_oneof") {
            UpdateOneof::Slot(subscribe_update_slot) => {
                initial_slot = subscribe_update_slot.slot;
                break;
            }
            _ => {
                // Ignore other updates
                continue;
            }
        }
    }

    let shared: Arc<AtomicSlotTracker> = Arc::new(AtomicSlotTracker::new(initial_slot));
    let to_drop = AutoCloseSlotTracker {
        slot_tracker: Arc::clone(&shared),
    };
    let jh = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

    Ok(Some(YellowstoneSlotTrackerOk {
        atomic_slot_tracker: shared,
        join_handle: jh,
    }))
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        std::time::Duration,
        tokio_stream::wrappers::UnboundedReceiverStream,
        yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeUpdateSlot},
    };

    #[tokio::test]
    async fn test_atomic_slot_tracker_loop() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let to_drop = AutoCloseSlotTracker {
            slot_tracker: Arc::clone(&slot_tracker),
        };

        let updates = vec![
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 1,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotProcessed as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 2,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotProcessed as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 3,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotFirstShredReceived as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
        ];
        let expected_slot_views = [1, 2, 3];
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(rx);
        let handle = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

        for (i, update) in updates.into_iter().enumerate() {
            tx.send(update).expect("send update");
            tokio::time::sleep(Duration::from_millis(10)).await;
            let expected_slot = expected_slot_views[i];
            let current_slot = slot_tracker.load().expect("load");
            assert_eq!(current_slot, expected_slot);
        }

        // Drop the handle to clean up
        handle.abort();

        // Sleep a bit to ensure the drop has taken effect
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn test_it_should_poison_when_stream_empty() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let to_drop = AutoCloseSlotTracker {
            slot_tracker: Arc::clone(&slot_tracker),
        };

        let stream = tokio_stream::iter(vec![]);
        let handle = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

        let _ = handle.await;

        assert!(
            slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }
}
