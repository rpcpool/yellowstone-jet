use {
    crate::slot::AtomicSlotTracker,
    futures::Stream,
    std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration},
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

type SlotStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

pub(crate) const SLOT_TRACKER_DM_FILTER_NAME: &str = "jet-tpu-client";

const MAX_RETRY_DELAY: Duration = Duration::from_secs(15);
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

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

/// Consume a single stream until it errors or ends.
/// Returns `true` if it ended cleanly, `false` on error (caller should retry).
async fn drain_slot_stream(
    dm_slot_stream: &mut (impl Stream<Item = Result<SubscribeUpdate, Status>> + Unpin),
    shared: &Arc<AtomicSlotTracker>,
    current_slot: &mut u64,
) -> bool
{
    loop {
        let result = dm_slot_stream.next().await;
        if result.is_none() {
            tracing::warn!("Yellowstone slot tracker stream ended");
            return true;
        }

        let response = match result.unwrap() {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker stream error: {:?}", err);
                return false;
            }
        };
        match response.update_oneof.expect("update_oneof") {
            UpdateOneof::Slot(subscribe_update_slot) => {
                let slot = subscribe_update_slot.slot;
                if slot <= *current_slot {
                    continue;
                }
                *current_slot = slot;
                tracing::trace!("Yellowstone slot tracker received slot update: {}", slot);
                shared
                    .slot
                    .store(*current_slot, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

///
/// Background task to update the AtomicSlotTracker from the Yellowstone Geyser slot stream.
/// On stream error, resubscribes with exponential backoff instead of panicking.
///
async fn atomic_slot_tracker_loop_reconnect(
    mut resubscribe: Box<dyn ResubscribeFn>,
    mut stream: SlotStream,
    to_drop: AutoCloseSlotTracker,
) {
    let shared = Arc::clone(&to_drop.slot_tracker);
    let mut current_slot = shared.slot.load(std::sync::atomic::Ordering::Relaxed);
    let mut retry_delay = INITIAL_RETRY_DELAY;

    let clean = drain_slot_stream(&mut stream, &shared, &mut current_slot).await;
    if clean {
        drop(to_drop);
        return;
    }

    loop {
        tracing::warn!(
            "Yellowstone slot tracker reconnecting in {:?}...",
            retry_delay
        );
        tokio::time::sleep(retry_delay).await;
        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);

        let new_stream = match resubscribe.resubscribe().await {
            Ok(s) => s,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker resubscribe failed: {:?}", err);
                continue;
            }
        };

        tracing::info!("Yellowstone slot tracker reconnected");
        retry_delay = INITIAL_RETRY_DELAY;

        stream = new_stream;
        let clean = drain_slot_stream(&mut stream, &shared, &mut current_slot).await;
        if clean {
            drop(to_drop);
            return;
        }
    }
}

#[async_trait::async_trait]
trait ResubscribeFn: Send + 'static {
    async fn resubscribe(
        &mut self,
    ) -> Result<SlotStream, yellowstone_grpc_client::GeyserGrpcClientError>;
}

struct GeyserResubscriber<I: Interceptor> {
    client: yellowstone_grpc_client::GeyserGrpcClient<I>,
}

#[async_trait::async_trait]
impl<I: Interceptor + Send + 'static> ResubscribeFn for GeyserResubscriber<I> {
    async fn resubscribe(
        &mut self,
    ) -> Result<SlotStream, yellowstone_grpc_client::GeyserGrpcClientError> {
        let req = get_yellowstone_slot_tracker_subscribe_request();
        let stream = self.client.subscribe_once(req).await?;
        Ok(Box::pin(stream))
    }
}

///
/// Creates an [`AtomicSlotTracker`] that tracks the latest slot from Yellowstone Geyser.
///
pub async fn atomic_slot_tracker<I>(
    mut geyser_client: yellowstone_grpc_client::GeyserGrpcClient<I>,
) -> GeyserGrpcClientResult<Option<YellowstoneSlotTrackerOk>>
where
    I: Interceptor + Clone + Send + 'static,
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
                continue;
            }
        }
    }

    let shared: Arc<AtomicSlotTracker> = Arc::new(AtomicSlotTracker::new(initial_slot));
    let to_drop = AutoCloseSlotTracker {
        slot_tracker: Arc::clone(&shared),
    };
    let resubscriber: Box<dyn ResubscribeFn> = Box::new(GeyserResubscriber {
        client: geyser_client,
    });
    let jh = tokio::spawn(atomic_slot_tracker_loop_reconnect(
        resubscriber,
        Box::pin(stream),
        to_drop,
    ));

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
    async fn test_drain_slot_stream() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let mut current_slot = 0u64;

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
        let mut stream = tokio_stream::iter(updates);
        let clean = drain_slot_stream(&mut stream, &slot_tracker, &mut current_slot).await;

        assert!(clean);
        assert_eq!(current_slot, 3);
        assert_eq!(
            slot_tracker.slot.load(std::sync::atomic::Ordering::Relaxed),
            3
        );
    }

    #[tokio::test]
    async fn test_drain_slot_stream_returns_false_on_error() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let mut current_slot = 0u64;

        let updates: Vec<Result<SubscribeUpdate, Status>> = vec![
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
            Err(Status::cancelled("stream terminated by user")),
        ];

        let mut stream = tokio_stream::iter(updates);
        let clean = drain_slot_stream(&mut stream, &slot_tracker, &mut current_slot).await;

        assert!(!clean);
        assert_eq!(current_slot, 1);
    }

    #[tokio::test]
    async fn test_empty_stream_closes_tracker() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let mut current_slot = 0u64;

        let stream_items: Vec<Result<SubscribeUpdate, Status>> = vec![];
        let mut stream = tokio_stream::iter(stream_items);
        let clean = drain_slot_stream(&mut stream, &slot_tracker, &mut current_slot).await;

        assert!(clean);
    }
}
