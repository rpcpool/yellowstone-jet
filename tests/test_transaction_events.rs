use {
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    },
    yellowstone_jet::{
        config::ConfigLewisEvents,
        grpc_lewis::{LewisEventClient, LewisEventClientImpl},
        proto::lewis::{Event, event::Event as ProtoEvent},
        transaction_events::{
            EventChannelReporter, EventReporter, transaction_event_aggregator_loop,
        },
    },
};

/// Simple mock Lewis client implementation that just counts events
#[derive(Clone)]
struct SimpleMockLewisClientImpl {
    event_count: Arc<Mutex<usize>>,
    signatures: Arc<Mutex<Vec<Signature>>>,
}

impl SimpleMockLewisClientImpl {
    fn new() -> Self {
        Self {
            event_count: Arc::new(Mutex::new(0)),
            signatures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_event_count(&self) -> usize {
        *self.event_count.lock().unwrap()
    }

    fn get_signatures(&self) -> Vec<Signature> {
        self.signatures.lock().unwrap().clone()
    }
}

impl LewisEventClientImpl for SimpleMockLewisClientImpl {
    fn emit(&self, event: Event) {
        if let Some(ProtoEvent::Jet(jet_event)) = event.event {
            let sig_bytes: [u8; 64] = jet_event.sig.as_slice().try_into().unwrap();
            let sig = Signature::from(sig_bytes);

            *self.event_count.lock().unwrap() += 1;
            self.signatures.lock().unwrap().push(sig);
        }
    }
}

fn make_test_config() -> ConfigLewisEvents {
    ConfigLewisEvents {
        endpoint: "http://test".to_string(),
        queue_size_grpc: 100,
        queue_size_buffer: 100,
        jet_id: Some("test-jet".to_string()),
        aggregation_timeout: Duration::from_secs(2),
        check_interval: Duration::from_secs(1),
    }
}

const fn make_test_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000)
}

#[tokio::test]
async fn test_basic_transaction_flow_single_validator() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3, // max_retries
    ));

    let sig = Signature::new_unique();
    let validator = Pubkey::new_unique();
    let slot = 12345;

    // Send TransactionReceived
    event_reporter.report_transaction_received(sig, vec![validator], slot);

    // Send successful attempt
    event_reporter.report_send_attempt(sig, validator, make_test_addr(), 1, Ok(()));

    // Wait a bit for aggregation
    sleep(Duration::from_millis(100)).await;

    // Check that events were sent to Lewis
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_multiple_validators_with_mixed_results() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3, // max_retries
    ));

    let sig = Signature::new_unique();
    let validator1 = Pubkey::new_unique();
    let validator2 = Pubkey::new_unique();
    let validator3 = Pubkey::new_unique();
    let slot = 12345;

    // Send TransactionReceived with 3 validators
    event_reporter.report_transaction_received(sig, vec![validator1, validator2, validator3], slot);

    // Validator 1: Policy skip
    event_reporter.report_policy_skip(sig, validator1);

    // Validator 2: Successful send
    event_reporter.report_send_attempt(sig, validator2, make_test_addr(), 1, Ok(()));

    // Validator 3: Connection failed
    event_reporter.report_connection_failed(
        sig,
        validator3,
        make_test_addr(),
        "timeout".to_string(),
    );

    // Wait for completion
    sleep(Duration::from_millis(100)).await;

    // Should have sent one transaction with all events
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_retry_attempts_until_max_retries() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let max_retries = 3;
    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        max_retries,
    ));

    let sig = Signature::new_unique();
    let validator = Pubkey::new_unique();
    let slot = 12345;

    event_reporter.report_transaction_received(sig, vec![validator], slot);

    // Send max_retries failures
    for i in 1..=max_retries {
        event_reporter.report_send_attempt(
            sig,
            validator,
            make_test_addr(),
            i as u8,
            Err("failed".to_string()),
        );
    }

    // Wait for completion
    sleep(Duration::from_millis(100)).await;

    // Should complete after max retries
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_timeout_handling() {
    let mut config = make_test_config();
    config.aggregation_timeout = Duration::from_millis(200);
    config.check_interval = Duration::from_millis(50);

    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig = Signature::new_unique();
    let validator1 = Pubkey::new_unique();
    let validator2 = Pubkey::new_unique();
    let slot = 12345;

    // Send TransactionReceived with 2 validators
    event_reporter.report_transaction_received(sig, vec![validator1, validator2], slot);

    // Only send event for validator1
    event_reporter.report_send_attempt(sig, validator1, make_test_addr(), 1, Ok(()));

    // Wait longer than timeout + check interval
    sleep(Duration::from_millis(300)).await;

    // Should have sent incomplete transaction due to timeout
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_orphaned_events() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig = Signature::new_unique();
    let validator = Pubkey::new_unique();

    // Send events WITHOUT TransactionReceived first
    event_reporter.report_send_attempt(sig, validator, make_test_addr(), 1, Ok(()));
    event_reporter.report_policy_skip(sig, validator);

    // Wait a bit
    sleep(Duration::from_millis(100)).await;

    // These should be orphaned - no events sent to Lewis
    assert_eq!(mock_impl.get_event_count(), 0);

    // Now send TransactionReceived
    event_reporter.report_transaction_received(sig, vec![validator], 12345);

    // And another attempt
    event_reporter.report_send_attempt(sig, validator, make_test_addr(), 2, Ok(()));

    sleep(Duration::from_millis(100)).await;

    // Now we should have events
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_duplicate_transaction_received() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig = Signature::new_unique();
    let validator = Pubkey::new_unique();
    let slot = 12345;

    // Send first TransactionReceived
    event_reporter.report_transaction_received(sig, vec![validator], slot);

    // Send duplicate before completing the first one
    event_reporter.report_transaction_received(sig, vec![validator], slot);

    // Now complete the first transaction
    event_reporter.report_send_attempt(sig, validator, make_test_addr(), 1, Ok(()));

    sleep(Duration::from_millis(100)).await;

    // Should only have one transaction (duplicate was ignored)
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_incremental_completion_tracking() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig = Signature::new_unique();
    let validators: Vec<_> = (0..5).map(|_| Pubkey::new_unique()).collect();
    let slot = 12345;

    // Start with 5 validators
    event_reporter.report_transaction_received(sig, validators.clone(), slot);

    // Complete validators one by one
    for (i, validator) in validators.iter().enumerate() {
        if i < 4 {
            // First 4 succeed
            event_reporter.report_send_attempt(sig, *validator, make_test_addr(), 1, Ok(()));
        } else {
            // Last one fails after retries
            for attempt in 1..=3 {
                event_reporter.report_send_attempt(
                    sig,
                    *validator,
                    make_test_addr(),
                    attempt,
                    Err("failed".to_string()),
                );
            }
        }
    }

    // Should complete immediately after last validator
    sleep(Duration::from_millis(100)).await;

    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}

#[tokio::test]
async fn test_shutdown_sends_remaining_events() {
    let mut config = make_test_config();
    config.check_interval = Duration::from_millis(50);

    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig = Signature::new_unique();
    let validator1 = Pubkey::new_unique();
    let validator2 = Pubkey::new_unique();

    // Send partial transaction
    event_reporter.report_transaction_received(sig, vec![validator1, validator2], 12345);
    event_reporter.report_send_attempt(sig, validator1, make_test_addr(), 1, Ok(()));
    // Don't complete validator2

    // Give it a moment to process
    sleep(Duration::from_millis(50)).await;

    // Drop reporter to close channel
    drop(event_reporter);

    // Wait for aggregator to finish and send remaining
    let _ = aggregator_handle.await;

    // Should have sent incomplete transaction
    assert_eq!(mock_impl.get_event_count(), 1);
    assert_eq!(mock_impl.get_signatures(), vec![sig]);
}

#[tokio::test]
async fn test_multiple_transactions_concurrent() {
    let config = make_test_config();
    let mock_impl = Arc::new(SimpleMockLewisClientImpl::new());
    let lewis_client = Arc::new(LewisEventClient::new_mock(
        Arc::<SimpleMockLewisClientImpl>::clone(&mock_impl),
        Some("test-jet".to_string()),
    ));
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_reporter = Arc::new(EventChannelReporter::new(event_tx));

    let aggregator_handle = tokio::spawn(transaction_event_aggregator_loop(
        event_rx,
        lewis_client,
        config,
        3,
    ));

    let sig1 = Signature::new_unique();
    let sig2 = Signature::new_unique();
    let sig3 = Signature::new_unique();
    let validator = Pubkey::new_unique();

    // Send multiple transactions
    event_reporter.report_transaction_received(sig1, vec![validator], 100);
    event_reporter.report_transaction_received(sig2, vec![validator], 101);
    event_reporter.report_transaction_received(sig3, vec![validator], 102);

    // Complete them out of order
    event_reporter.report_send_attempt(sig2, validator, make_test_addr(), 1, Ok(()));
    event_reporter.report_send_attempt(sig1, validator, make_test_addr(), 1, Ok(()));
    event_reporter.report_send_attempt(sig3, validator, make_test_addr(), 1, Ok(()));

    sleep(Duration::from_millis(100)).await;

    // All three should be sent
    assert_eq!(mock_impl.get_event_count(), 3);
    let sigs = mock_impl.get_signatures();
    assert_eq!(sigs.len(), 3);
    assert!(sigs.contains(&sig1));
    assert!(sigs.contains(&sig2));
    assert!(sigs.contains(&sig3));

    drop(event_reporter);
    let _ = timeout(Duration::from_secs(1), aggregator_handle).await;
}
