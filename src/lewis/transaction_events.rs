//! Transaction Event Aggregation System
//!
//! This module implements a state machine that tracks all send attempts for each transaction
//! across multiple validators. The system provides complete visibility into transaction
//! routing, including policy decisions, connection failures, and send attempts.
//!
//! # Architecture
//!
//! In QuicGateway, each transaction is distributed to multiple leader validators, with each
//! leader handled by a dedicated worker. Workers retry failed attempts up to N times before
//! giving up. This aggregator collects all these events to provide a complete picture.
//!
//! Events flow through the system as follows:
//!
//! 1. **TransactionReceived** - Marks transaction arrival with target leaders and slot
//! 2. **Routing Events** - For each leader validator:
//!    - PolicySkipped: Leader rejected by policy (no attempts made)
//!    - SendAttempt: Each retry attempt with success/failure result
//!    - ConnectionFailed: Unable to establish QUIC connection
//! 3. **Aggregation** - Collects events until all leaders reach terminal state:
//!    - Success: Transaction sent successfully (no more retries)
//!    - Policy skip: Rejected by policy
//!    - Max retries: All N attempts exhausted
//! 4. **Emission** - Complete transaction history sent to Lewis for persistence
//!
//! # State Machine Logic
//!
//! The aggregator tracks each transaction's progress across all assigned leaders.
//! A transaction is considered complete when one of these conditions is met:
//!
//! - All target leaders have reached a terminal state (success, skip, or max retries)
//! - Aggregation timeout is reached (default 30s) - handles stuck transactions
//! - The aggregator is shutting down - ensures all data is persisted
//!
//! This design ensures we capture every routing decision and attempt, providing
//! valuable insights into transaction propagation while preventing memory leaks
//! from transactions that never complete.

use {
    crate::{
        config::ConfigLewisEvents,
        lewis::grpc_lewis::{LewisClientError, LewisEventClient},
        metrics::jet as metrics,
    },
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::{HashMap, hash_map::Entry},
        future::Future,
        net::SocketAddr,
        sync::Arc,
        time::{Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{sync::mpsc, time::interval},
};

/// Represents a single event in a transaction's lifecycle.
///
/// Events are ordered: TransactionReceived always comes first, followed by
/// various routing attempts to different validators.
#[derive(Debug, Clone)]
pub enum TransactionEvent {
    /// Transaction arrived TransactionFanout
    TransactionReceived {
        leaders: Vec<Pubkey>,
        slot: Slot,
        timestamp: i64,
    },
    /// Transaction skipped for a validator due to policy
    PolicySkipped { validator: Pubkey, timestamp: i64 },
    /// Attempt to send transaction to a validator
    SendAttempt {
        validator: Pubkey,
        tpu_addr: SocketAddr,
        attempt_num: u8,
        result: Result<(), String>,
        timestamp: i64,
    },
    /// Failed to establish connection to validator
    ConnectionFailed {
        validator: Pubkey,
        tpu_addr: SocketAddr,
        error: String,
        timestamp: i64,
    },
}

impl TransactionEvent {
    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_else(|e| {
                tracing::error!("System clock before UNIX epoch: {:?}", e);
                // Return 0 as a fallback - better than panicking
                0
            })
    }

    pub fn transaction_received(leaders: Vec<Pubkey>, slot: Slot) -> Self {
        Self::TransactionReceived {
            leaders,
            slot,
            timestamp: Self::current_timestamp(),
        }
    }

    pub fn policy_skipped(validator: Pubkey) -> Self {
        Self::PolicySkipped {
            validator,
            timestamp: Self::current_timestamp(),
        }
    }

    pub fn send_attempt(
        validator: Pubkey,
        tpu_addr: SocketAddr,
        attempt_num: u8,
        result: Result<(), String>,
    ) -> Self {
        Self::SendAttempt {
            validator,
            tpu_addr,
            attempt_num,
            result,
            timestamp: Self::current_timestamp(),
        }
    }

    pub fn connection_failed(validator: Pubkey, tpu_addr: SocketAddr, error: String) -> Self {
        Self::ConnectionFailed {
            validator,
            tpu_addr,
            error,
            timestamp: Self::current_timestamp(),
        }
    }
}

pub trait EventReporter: Send + Sync {
    fn report_transaction_received(&self, signature: Signature, leaders: Vec<Pubkey>, slot: Slot);
    fn report_policy_skip(&self, signature: Signature, validator: Pubkey);
    fn report_send_attempt(
        &self,
        signature: Signature,
        validator: Pubkey,
        tpu_addr: SocketAddr,
        attempt_num: u8,
        result: Result<(), String>,
    );
    fn report_connection_failed(
        &self,
        signature: Signature,
        validator: Pubkey,
        tpu_addr: SocketAddr,
        error: String,
    );
}

pub trait TransactionEventTracker: Send + Sync {
    fn track_transaction_send(
        &self,
        signature: &Signature,
        slot: Slot,
        ts_received: i64,
        events: Vec<TransactionEvent>,
    );
}

pub struct EventChannelReporter {
    tx: mpsc::UnboundedSender<(Signature, TransactionEvent)>,
}

impl EventChannelReporter {
    pub const fn new(tx: mpsc::UnboundedSender<(Signature, TransactionEvent)>) -> Self {
        Self { tx }
    }
}

impl EventReporter for EventChannelReporter {
    fn report_transaction_received(&self, signature: Signature, leaders: Vec<Pubkey>, slot: Slot) {
        if self
            .tx
            .send((
                signature,
                TransactionEvent::transaction_received(leaders, slot),
            ))
            .is_err()
        {
            tracing::warn!("Failed to report transaction received: channel closed");
            metrics::lewis_event_channel_closed_inc();
        }
    }

    fn report_policy_skip(&self, signature: Signature, validator: Pubkey) {
        if self
            .tx
            .send((signature, TransactionEvent::policy_skipped(validator)))
            .is_err()
        {
            tracing::warn!("Failed to report policy skip: channel closed");
            metrics::lewis_event_channel_closed_inc();
        }
    }

    fn report_send_attempt(
        &self,
        signature: Signature,
        validator: Pubkey,
        tpu_addr: SocketAddr,
        attempt_num: u8,
        result: Result<(), String>,
    ) {
        if self
            .tx
            .send((
                signature,
                TransactionEvent::send_attempt(validator, tpu_addr, attempt_num, result),
            ))
            .is_err()
        {
            tracing::warn!("Failed to report send attempt: channel closed");
            metrics::lewis_event_channel_closed_inc();
        }
    }

    fn report_connection_failed(
        &self,
        signature: Signature,
        validator: Pubkey,
        tpu_addr: SocketAddr,
        error: String,
    ) {
        if self
            .tx
            .send((
                signature,
                TransactionEvent::connection_failed(validator, tpu_addr, error),
            ))
            .is_err()
        {
            tracing::warn!("Failed to report connection failure: channel closed");
            metrics::lewis_event_channel_closed_inc();
        }
    }
}

/// Creates the complete Lewis event tracking pipeline if configured.
/// Returns the event reporter for components to emit events, and futures for the aggregator and Lewis client.
#[allow(clippy::type_complexity)]
pub fn create_lewis_event_pipeline(
    config: Option<ConfigLewisEvents>,
    max_retries: usize,
) -> (
    Option<Arc<dyn EventReporter>>,
    Option<impl Future<Output = ()> + Send>,
    Option<impl Future<Output = Result<(), LewisClientError>> + Send>,
) {
    let Some(config) = config else {
        return (None, None, None);
    };

    let (lewis_client, lewis_fut) = LewisEventClient::create_event_tracker(Some(config.clone()));

    let Some(lewis_client) = lewis_client else {
        return (None, None, lewis_fut);
    };

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx)) as Arc<dyn EventReporter>;

    let aggregator_fut =
        transaction_event_aggregator_loop(event_rx, lewis_client, config, max_retries);

    (Some(event_reporter), Some(aggregator_fut), lewis_fut)
}

struct TransactionTracking {
    signature: Signature,
    slot: Slot,
    ts_received: i64,
    events: Vec<TransactionEvent>,
    created_at: Instant,
    expected_per_validator: HashMap<Pubkey, usize>,
    received_per_validator: HashMap<Pubkey, usize>,
    validator_attempt_count: HashMap<Pubkey, usize>,
    total_remaining_events: usize,
}

impl TransactionTracking {
    fn new(signature: Signature, event: TransactionEvent) -> Option<Self> {
        match &event {
            TransactionEvent::TransactionReceived {
                leaders,
                slot,
                timestamp,
            } => {
                // Count expected events per validator
                let mut expected_per_validator = HashMap::new();
                let mut total_expected = 0;

                if leaders.is_empty() {
                    tracing::warn!(
                        "TransactionReceived event has empty leaders list for {}",
                        signature
                    );
                    return None;
                }

                for leader in leaders {
                    *expected_per_validator.entry(*leader).or_insert(0) += 1;
                    total_expected += 1;
                }

                // Add metric if we have duplicate leaders
                let has_duplicates = expected_per_validator.values().any(|&count| count > 1);
                if has_duplicates {
                    metrics::lewis_event_aggregator_duplicate_leaders_inc();
                    tracing::debug!(
                        "Transaction {} has duplicate leaders in schedule: {:?}",
                        signature,
                        expected_per_validator
                    );
                }

                Some(Self {
                    signature,
                    slot: *slot,
                    ts_received: *timestamp,
                    events: vec![event],
                    created_at: Instant::now(),
                    expected_per_validator,
                    received_per_validator: HashMap::new(),
                    validator_attempt_count: HashMap::new(),
                    total_remaining_events: total_expected,
                })
            }
            _ => {
                tracing::warn!("First event must be TransactionReceived for {}", signature);
                None
            }
        }
    }

    /// Add event and return true if transaction is complete
    fn add_event(&mut self, event: TransactionEvent, max_retries: usize) -> bool {
        self.events.push(event.clone());

        let validator = match &event {
            TransactionEvent::PolicySkipped { validator, .. }
            | TransactionEvent::ConnectionFailed { validator, .. } => Some(validator),
            TransactionEvent::SendAttempt {
                validator, result, ..
            } => {
                if result.is_ok() {
                    Some(validator)
                } else {
                    // Failed - track retry attempts
                    let count = self.validator_attempt_count.entry(*validator).or_insert(0);
                    *count += 1;
                    if *count >= max_retries {
                        Some(validator) // Max retries reached
                    } else {
                        None // Still retrying
                    }
                }
            }
            TransactionEvent::TransactionReceived { .. } => None, // Should not happen
        };

        // If we have a terminal event for a validator
        if let Some(validator) = validator {
            // Check if this validator is expected
            match self.expected_per_validator.get(validator) {
                Some(&expected_count) => {
                    let received_count = self.received_per_validator.entry(*validator).or_insert(0);

                    // Check if we've already completed this validator
                    if *received_count >= expected_count {
                        tracing::warn!(
                            "Received extra event for validator {} in transaction {}: already have {}/{} events",
                            validator,
                            self.signature,
                            received_count,
                            expected_count
                        );
                        // Don't process this event further
                        return self.total_remaining_events == 0;
                    }

                    // Increment received count
                    *received_count += 1;

                    // Decrement remaining events
                    self.total_remaining_events = self.total_remaining_events.saturating_sub(1);
                }
                None => {
                    // Event for unexpected validator
                    tracing::warn!(
                        "Received event for unexpected validator {} in transaction {}",
                        validator,
                        self.signature
                    );
                }
            }
        }

        self.total_remaining_events == 0
    }
}

/// Aggregates transaction events and sends complete sets to Lewis.
///
/// This function runs until the event channel is closed, maintaining a state
/// machine for each active transaction. It checks for completed
/// or timed-out transactions and sends them to Lewis for persistence.
pub async fn transaction_event_aggregator_loop(
    mut event_rx: mpsc::UnboundedReceiver<(Signature, TransactionEvent)>,
    lewis_client: Arc<LewisEventClient>,
    config: ConfigLewisEvents,
    transaction_max_retries: usize,
) {
    let mut trackers: HashMap<Signature, TransactionTracking> = HashMap::new();
    let mut timeout_check = interval(config.check_interval);

    loop {
        metrics::lewis_event_aggregator_queue_size_set(event_rx.len());
        metrics::lewis_event_aggregator_tracking_size_set(trackers.len());

        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some((sig, event)) => {
                        match &event {
                            TransactionEvent::TransactionReceived { .. } => {
                                match trackers.entry(sig) {
                                    Entry::Occupied(_) => {
                                        tracing::debug!("Duplicate TransactionReceived for {} - ignoring", sig);
                                        metrics::lewis_event_aggregator_duplicate_transaction_inc();
                                    }
                                    Entry::Vacant(entry) => {
                                        if let Some(tracker) = TransactionTracking::new(sig, event) {
                                            entry.insert(tracker);
                                        }
                                    }
                                }
                            }
                            _ => {
                                if let Some(tracker) = trackers.get_mut(&sig) {
                                    let is_complete = tracker.add_event(event, transaction_max_retries);

                                    if is_complete {
                                        // Complete - send immediately
                                        if let Some(tracker) = trackers.remove(&sig) {
                                            lewis_client.track_transaction_send(
                                                &tracker.signature,
                                                tracker.slot,
                                                tracker.ts_received,
                                                tracker.events,
                                            );
                                            metrics::lewis_event_aggregator_completed_inc();
                                        }
                                    }
                                } else {
                                    let event_type = match &event {
                                        TransactionEvent::PolicySkipped { .. } => "policy_skipped",
                                        TransactionEvent::SendAttempt { .. } => "send_attempt",
                                        TransactionEvent::ConnectionFailed { .. } => "connection_failed",
                                        TransactionEvent::TransactionReceived { .. } => unreachable!(),
                                    };

                                    tracing::debug!(
                                        "Orphaned {} event for {} - missing TransactionReceived",
                                        event_type,
                                        sig
                                    );

                                    metrics::lewis_event_aggregator_orphaned_events_inc(event_type);
                                }
                            }
                        }
                    }
                    None => {
                        // Channel closed, exit loop
                        break;
                    }
                }
            }

            _ = timeout_check.tick() => {
                let now = Instant::now();
                let mut timed_out = Vec::new();

                for (sig, tracker) in trackers.iter() {
                    if now.duration_since(tracker.created_at) > config.aggregation_timeout {
                        timed_out.push(*sig);
                    }
                }

                for sig in timed_out {
                    if let Some(tracker) = trackers.remove(&sig) {
                        lewis_client.track_transaction_send(
                            &tracker.signature,
                            tracker.slot,
                            tracker.ts_received,
                            tracker.events
                        );
                        metrics::lewis_event_aggregator_timeout_inc();
                    }
                }
            }
        }
    }

    // Send all remaining on shutdown
    for (_, tracker) in trackers.drain() {
        lewis_client.track_transaction_send(
            &tracker.signature,
            tracker.slot,
            tracker.ts_received,
            tracker.events,
        );
    }
}
