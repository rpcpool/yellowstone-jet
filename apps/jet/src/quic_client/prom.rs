use {
    prometheus::{
        Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
        Opts, Registry,
    },
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, sync::Mutex, time::Duration},
};

lazy_static::lazy_static! {
    static ref QUIC_IDENTITY: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity", "Current QUIC identity"), &["identity"]).unwrap();
    static ref QUIC_IDENTITY_EXPECTED: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity_expected", "Expected QUIC identity"), &["identity"]).unwrap();
    static ref QUIC_SEND_ATTEMPTS: IntCounterVec = IntCounterVec::new(
        Opts::new("quic_send_attempts", "Status of sending transactions with QUIC"),
        &["leader", "address", "status"]
    ).unwrap();

    static ref QUIC_IDENTITY_VALUE: Mutex<Option<Pubkey>> = Mutex::new(None);
    static ref QUIC_IDENTITY_EXPECTED_VALUE: Mutex<Option<Pubkey>> = Mutex::new(None);



    static ref GATEWAY_CONNECTED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("gateway_connected", "Connected gateway endpoint"),
        &["endpoint"]
    ).unwrap();

    static ref FORWADED_TRANSACTION_LATENCY: HistogramVec = HistogramVec::new(
            HistogramOpts::new("forward_latency", "Latency of transactions forwarded from jet-gateway to a jet instance")
        .buckets(vec![
                0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 3.0, 4.0, 5.0,
                7.5, 10.0, 12.5, 15.0, 17.5, 20.0, 25.0, 30.0, 35.0, 40.0,
                45.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0
        ]),
        &[]
    ).unwrap();

    static ref SHIELD_POLICIES_NOT_FOUND_TOTAL: IntCounter =
        IntCounter::new("shield_policies_not_found_total", "Number of shield policies not found").unwrap();

    static ref TRANSACTION_DECODE_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new("transaction_decode_errors_total", "Number of transaction decoding errors (base58/base64)"),
        &["error_type"]
    ).unwrap();

    static ref TRANSACTION_DESERIALIZE_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new("transaction_deserialize_errors_total", "Number of transaction deserialization errors (bincode)"),
        &["error_type"]
    ).unwrap();

    static ref SEND_TRANSACTION_ERROR: IntCounter = IntCounter::new("send_transaction_error", "Number of errors when sending transaction").unwrap();
    static ref SEND_TRANSACTION_SUCCESS: IntCounter = IntCounter::new("send_transaction_success", "Number of successful transactions sent").unwrap();
    static ref SEND_TRANSACTION_ATTEMPT: IntCounterVec = IntCounterVec::new(
        Opts::new("send_transaction_attempt", "Number of attempts to send transaction"),
        &["leader"]
    ).unwrap();

    static ref SEND_TRANSACTION_E2E_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("send_transaction_e2e_latency", "End-to-end transmission latency of sending transaction to a leader and waiting for acks")
            // 0ms to 2 seconds, above that it means the remote connection is really bad and will probably crash soon.
            .buckets(vec![
                0.0, 10.0, 15.0, 25.0, 35.0, 50.0, 75.0, 100.0,
                150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0,
                600.0, 700.0, 800.0, 900.0, 1000.0, 1500.0, 2000.0
            ]),
        &["leader"]
    ).unwrap();

    static ref LEADER_RTT: HistogramVec = HistogramVec::new(
        HistogramOpts::new("leader_rtt", "Leader Rounrd-trip-time")
            // 0ms to 50ms, above that it means the remote connection is really bad and will probably crash soon.
            .buckets(vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0,
                15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0,
            ]),
        &["leader"]
    ).unwrap();

    static ref LEADER_MTU: IntGaugeVec = IntGaugeVec::new(
        Opts::new("leader_mtu", "Leader current MTU"),
        &["leader"]
    ).unwrap();


    static ref QUIC_GW_CONNECTING_GAUGE: IntGauge = IntGauge::new(
        "quic_gw_connecting", "Number of ongoing connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_CONNECTION_SUCCESS_CNT: IntCounter = IntCounter::new(
        "quic_gw_connection_success", "Number of successful connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_CONNECTION_FAILURE_CNT: IntCounter = IntCounter::new(
        "quic_gw_connection_failure", "Number of failed connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_ACTIVE_CONNECTION_GAUGE: IntGauge = IntGauge::new(
        "quic_gw_active_connection", "Number of active connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_TOTAL_CONNECTION_EVICTIONS_CNT: IntCounter = IntCounter::new(
        "quic_gw_total_connection_evictions", "Total number of evicted connections to remote peer validators since the start of the service"
    ).unwrap();

    static ref QUIC_GW_CONNECTION_CLOSE_CNT: IntCounter = IntCounter::new(
        "quic_gw_connection_close", "Number of closed connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_UNREACHABLE_PEER_CNT: IntCounterVec = IntCounterVec::new(
        Opts::new("quic_gw_unreachable_peer_count", "Number of unreachable remote peer validators"),
        &["leader"]
    ).unwrap();

    static ref QUIC_GW_ONGOING_EVICTIONS_GAUGE: IntGauge = IntGauge::new(
        "quic_gw_ongoing_evictions", "Number of ongoing evictions of connections to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_TX_CONNECTION_CACHE_HIT_CNT: IntCounter = IntCounter::new(
        "quic_gw_tx_connection_cache_hit", "Number of hits transaction got forward to an existing connection to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_TX_CONNECTION_CACHE_MISS_CNT: IntCounter = IntCounter::new(
        "quic_gw_tx_connection_cache_miss", "Number of misses transaction got forward to a new connection to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_TX_BLOCKED_BY_CONNECTING_GAUGE: IntGauge = IntGauge::new(
        "quic_gw_tx_blocked_by_connecting", "Number of transactions waiting for remote peer connection to be established"
    ).unwrap();

    static ref QUIC_GW_CONNECTION_TIME_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "quic_gw_connection_time_ms",
            "Time taken to establish a connection to remote peer validators in milliseconds"
        )
        .buckets(vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0, 233.0, 377.0, 610.0, 987.0, 1597.0, 2584.0, f64::INFINITY])
    ).unwrap();

    static ref QUIC_GW_REMOTE_PEER_ADDR_CHANGES_DETECTED: IntCounter = IntCounter::new(
        "quic_gw_remote_peer_addr_changes_detected",
        "Number of detected changes in remote peer address"
    ).unwrap();

    static ref QUIC_GW_LEADER_PREDICTION_HIT: IntCounter = IntCounter::new(
        "quic_gw_leader_prediction_hit",
        "Number of times the leader prediction was successfully used to proactively connect to a remote peer"
    ).unwrap();

    static ref QUIC_GW_LEADER_PREDICTION_MISS: IntCounter = IntCounter::new(
        "quic_gw_leader_prediction_miss",
        "Number of times the leader prediction was uselessly used to proactively connect to a remote peer"
    ).unwrap();

    // Lewis Metrics
    static ref LEWIS_EVENTS_DROPPED: IntCounter = IntCounter::new(
        "lewis_events_dropped_total",
        "Total number of events dropped due to channel closure"
    ).unwrap();

    static ref LEWIS_EVENTS_SENT: IntCounter = IntCounter::new(
        "lewis_events_sent_total",
        "Total number of events sent to Lewis gRPC stream"
    ).unwrap();

    static ref QUIC_GW_DROP_TX_CNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "quic_gw_drop_tx_cnt",
            "Number of transactions dropped due to worker queue being full"
        ),
        &["leader"]
    ).unwrap();

    ///
    /// Number of transactions processed by the worker
    /// status is either success/error.
    ///
    /// Unlike `quic_send_attempts`, it removes duplicate attempt for the same transaction and summarizes them.
    static ref QUIC_GW_WORKER_TX_PROCESS_CNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "quic_gw_worker_tx_process_cnt",
            "Number of transactions processed by the worker"
        ),
        &["remote_peer", "status"]
    ).unwrap();


    static ref QUIC_GW_TX_RELAYED_TO_WORKER_CNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "quic_gw_tx_relayed_to_worker_cnt",
            "Number of transactions successfully relayed to installed transaction worker"
        ),
        &["remote_peer"]
    ).unwrap();

    // Metrics for Investigating decrease of performance when using first_shred_received
    // Lock acquisition time - shows thread contention
    static ref CLUSTER_TPU_LOCK_ACQUISITION_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("cluster_tpu_lock_acquisition_us", "Time to acquire read/write locks in ClusterTpuInfo in microseconds")
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]),
        &["method", "lock_type"]
    ).unwrap();


    // HashMap lookup time for leader schedule
    static ref LEADER_SCHEDULE_EXISTS_CHECK_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new("leader_schedule_exists_check_us", "Time spent checking if slot exists in leader_schedule HashMap in microseconds")
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0])
    ).unwrap();

    // Time per slot update loop iteration
    static ref SLOT_UPDATE_LOOP_ITERATION_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new("slot_update_loop_iteration_us", "Time for one complete iteration of update_latest_slot_and_leader_schedule loop in microseconds")
            .buckets(vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0])
    ).unwrap();

    // Batch size - high values mean backpressure
    static ref SLOT_UPDATES_DRAINED_COUNT: Histogram = Histogram::with_opts(
        HistogramOpts::new("slot_updates_drained_count", "Number of pending slot updates consumed in one loop iteration")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0])
    ).unwrap();

    // Rate of each slot status type
    static ref SLOT_STATUS_RECEIVED_BY_TYPE: IntCounterVec = IntCounterVec::new(
        Opts::new("slot_status_received_by_type_total", "Count of each SlotStatus type received in ClusterTpuInfo"),
        &["status"]
    ).unwrap();

    // RPC call duration
    static ref LEADER_SCHEDULE_RPC_FETCH_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new("leader_schedule_rpc_fetch_ms", "Time to fetch leader schedule via RPC")
            .buckets(vec![100.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0])
    ).unwrap();

    // RPC retry count
    static ref LEADER_SCHEDULE_RPC_ATTEMPTS: IntCounter = IntCounter::new(
        "leader_schedule_rpc_attempts_total", "Total RPC attempts to fetch leader schedule"
    ).unwrap();

    // Schedule parsing time
    static ref LEADER_SCHEDULE_PARSE_AND_INSERT_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new("leader_schedule_parse_insert_us", "Time to parse RPC response and insert into HashMap in microseconds")
            .buckets(vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0])
    ).unwrap();

    // HashMap size for memory tracking
    static ref LEADER_SCHEDULE_SIZE: IntGauge = IntGauge::new(
        "leader_schedule_hashmap_size", "Number of entries in leader_schedule HashMap"
    ).unwrap();

    static ref LEADER_SCHEDULE_ENTRIES_ADDED: IntGauge = IntGauge::new(
        "leader_schedule_entries_added_last_update", "Number of entries added in last schedule update"
    ).unwrap();

    static ref LEADER_SCHEDULE_ENTRIES_CLEANED: IntGauge = IntGauge::new(
        "leader_schedule_entries_cleaned_last_update", "Number of entries removed in last cleanup"
    ).unwrap();

    // gRPC message processing time by type
    static ref GRPC_MESSAGE_HANDLE_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("grpc_message_handle_time_us", "Time to process each gRPC message type in microseconds")
            .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]),
        &["message_type"]
    ).unwrap();

    // Slot update specific timing
    static ref GRPC_SLOT_UPDATE_HANDLE_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new("grpc_slot_update_handle_time_us", "Time to handle slot update message specifically")
            .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0])
    ).unwrap();

    // Channel send latency
    static ref GRPC_CHANNEL_SEND_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("grpc_channel_send_time_us", "Time to send on broadcast channels")
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0]),
        &["channel"]
    ).unwrap();

    // Channel overflow detection
    static ref GRPC_CHANNEL_SEND_FAILURES: IntCounterVec = IntCounterVec::new(
        Opts::new("grpc_channel_send_failures_total", "Failed channel sends"),
        &["channel"]
    ).unwrap();

    // Tracks cleanup effectiveness
    static ref SLOT_TRACKING_BTREEMAP_SIZE: IntGauge = IntGauge::new(
        "slot_tracking_btreemap_size", "Number of slots in grpc_geyser slot_tracking BTreeMap"
    ).unwrap();

    // Duplicate processing detection
    static ref BLOCK_META_EMISSIONS_COUNT: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_meta_emissions_per_slot", "Number of times block meta is emitted for a single slot")
            .buckets(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    ).unwrap();

    // Message throughput
    static ref GRPC_MESSAGES_PROCESSED_RATE: IntCounterVec = IntCounterVec::new(
        Opts::new("grpc_messages_processed_total", "Total gRPC messages processed"),
        &["message_type"]
    ).unwrap();

    // New slot arrival interval - this can tell us if we have any weird delays
    static ref NEW_SLOT_ARRIVAL_INTERVAL: Histogram = Histogram::with_opts(
        HistogramOpts::new("new_slot_arrival_interval_ms", "Time between receiving consecutive new slot numbers in milliseconds")
            .buckets(vec![
                10.0, 20.0, 30.0, 50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0,
            ])
    ).unwrap();

}

pub fn incr_quic_gw_tx_relayed_to_worker(remote_peer: Pubkey) {
    QUIC_GW_TX_RELAYED_TO_WORKER_CNT
        .with_label_values(&[&remote_peer.to_string()])
        .inc();
}

pub fn incr_quic_gw_worker_tx_process_cnt(remote_peer: Pubkey, status: &str) {
    QUIC_GW_WORKER_TX_PROCESS_CNT
        .with_label_values(&[remote_peer.to_string().as_str(), status])
        .inc_by(1);
}

pub fn incr_quic_gw_drop_tx_cnt(leader: Pubkey, count: u64) {
    QUIC_GW_DROP_TX_CNT
        .with_label_values(&[&leader.to_string()])
        .inc_by(count);
}

pub fn incr_quic_gw_leader_prediction_hit() {
    QUIC_GW_LEADER_PREDICTION_HIT.inc();
}

pub fn incr_quic_gw_leader_prediction_miss() {
    QUIC_GW_LEADER_PREDICTION_MISS.inc();
}

pub fn incr_quic_gw_remote_peer_addr_changes_detected() {
    QUIC_GW_REMOTE_PEER_ADDR_CHANGES_DETECTED.inc();
}

pub fn observe_quic_gw_connection_time(duration: Duration) {
    QUIC_GW_CONNECTION_TIME_HIST.observe(duration.as_millis() as f64);
}

pub fn set_quic_gw_tx_blocked_by_connecting_cnt(blocked: usize) {
    QUIC_GW_TX_BLOCKED_BY_CONNECTING_GAUGE.set(blocked as i64);
}
pub fn set_quic_gw_connecting_cnt(connecting: usize) {
    QUIC_GW_CONNECTING_GAUGE.set(connecting as i64);
}
pub fn set_quic_gw_ongoing_evictions_cnt(ongoing: usize) {
    QUIC_GW_ONGOING_EVICTIONS_GAUGE.set(ongoing as i64);
}
pub fn set_quic_gw_active_connection_cnt(active: usize) {
    QUIC_GW_ACTIVE_CONNECTION_GAUGE.set(active as i64);
}
pub fn incr_quic_gw_connection_failure_cnt() {
    QUIC_GW_CONNECTION_FAILURE_CNT.inc();
}
pub fn incr_quic_gw_connection_success_cnt() {
    QUIC_GW_CONNECTION_SUCCESS_CNT.inc();
}
pub fn incr_quic_gw_total_connection_evictions_cnt(amount: usize) {
    QUIC_GW_TOTAL_CONNECTION_EVICTIONS_CNT.inc_by(amount as u64);
}
pub fn incr_quic_gw_connection_close_cnt() {
    QUIC_GW_CONNECTION_CLOSE_CNT.inc();
}
pub fn incr_quic_gw_tx_connection_cache_hit_cnt() {
    QUIC_GW_TX_CONNECTION_CACHE_HIT_CNT.inc();
}
pub fn incr_quic_gw_tx_connection_cache_miss_cnt() {
    QUIC_GW_TX_CONNECTION_CACHE_MISS_CNT.inc();
}

pub fn observe_leader_rtt(leader: Pubkey, rtt: Duration) {
    LEADER_RTT
        .with_label_values(&[&leader.to_string()])
        .observe(rtt.as_millis() as f64);
}

pub fn observe_send_transaction_e2e_latency(leader: Pubkey, duration: Duration) {
    SEND_TRANSACTION_E2E_LATENCY
        .with_label_values(&[&leader.to_string()])
        .observe(duration.as_millis() as f64);
}

pub fn set_leader_mtu(leader: Pubkey, mtu: u16) {
    LEADER_MTU
        .with_label_values(&[&leader.to_string()])
        .set(mtu as i64);
}

pub fn regiser_metrics(reg: &Registry) {
    reg.register(Box::new(QUIC_GW_ACTIVE_CONNECTION_GAUGE.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_CONNECTION_CLOSE_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_CONNECTION_FAILURE_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_CONNECTION_SUCCESS_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_CONNECTING_GAUGE.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_TOTAL_CONNECTION_EVICTIONS_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_ONGOING_EVICTIONS_GAUGE.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_TX_BLOCKED_BY_CONNECTING_GAUGE.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_TX_CONNECTION_CACHE_HIT_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_TX_CONNECTION_CACHE_MISS_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_CONNECTION_TIME_HIST.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_REMOTE_PEER_ADDR_CHANGES_DETECTED.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_LEADER_PREDICTION_HIT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_LEADER_PREDICTION_MISS.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_UNREACHABLE_PEER_CNT.clone()))
        .unwrap();
}

pub fn inc_quic_gw_unreachable_peer_count(leader: Pubkey) {
    QUIC_GW_UNREACHABLE_PEER_CNT
        .with_label_values(&[&leader.to_string()])
        .inc();
}

pub fn shield_policies_not_found_inc() {
    SHIELD_POLICIES_NOT_FOUND_TOTAL.inc();
}

pub fn quic_set_identity(identity: Pubkey) {
    QUIC_IDENTITY.reset();
    QUIC_IDENTITY
        .with_label_values(&[&identity.to_string()])
        .set(1);
    *QUIC_IDENTITY_VALUE.lock().unwrap() = Some(identity);
}

pub fn quic_set_identity_expected(identity: Pubkey) {
    QUIC_IDENTITY_EXPECTED.reset();
    QUIC_IDENTITY_EXPECTED
        .with_label_values(&[&identity.to_string()])
        .set(1);
    *QUIC_IDENTITY_EXPECTED_VALUE.lock().unwrap() = Some(identity);
}

pub fn quic_send_attempts_inc(leader: Pubkey, address: SocketAddr, status: &str) {
    QUIC_SEND_ATTEMPTS
        .with_label_values(&[
            leader.to_string().as_str(),
            address.to_string().as_str(),
            status,
        ])
        .inc();
}

pub fn gateway_set_connected(endpoints: &[String], endpoint: String) {
    for endpoint in endpoints {
        GATEWAY_CONNECTED
            .with_label_values(&[endpoint.as_str()])
            .set(0);
    }
    GATEWAY_CONNECTED
        .with_label_values(&[endpoint.as_str()])
        .set(1);
}

pub fn gateway_set_disconnected(endpoints: &[String]) {
    for endpoint in endpoints {
        GATEWAY_CONNECTED
            .with_label_values(&[endpoint.as_str()])
            .set(0);
    }
}
