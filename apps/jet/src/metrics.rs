use {
    crate::version::VERSION as VERSION_INFO,
    prometheus::{IntCounterVec, Opts, Registry, TextEncoder},
    std::sync::Once,
    tracing::error,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "version"]
    ).unwrap();
}

macro_rules! register {
    ($collector:ident) => {
        REGISTRY
            .register(Box::new($collector.clone()))
            .expect("collector can't be registered")
    };
}

fn init2() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        register!(VERSION);
        VERSION
            .with_label_values(&[
                VERSION_INFO.buildts,
                VERSION_INFO.git,
                VERSION_INFO.package,
                VERSION_INFO.proto,
                VERSION_INFO.rustc,
                VERSION_INFO.version,
            ])
            .inc();
    });
}

pub fn collect_to_text() -> String {
    TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        })
}

pub mod jet {
    use {
        super::{REGISTRY, init2},
        crate::{
            grpc_lewis,
            util::{CommitmentLevel, SlotStatus},
        },
        prometheus::{
            Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
            IntGaugeVec, Opts,
        },
        solana_clock::Slot,
        solana_pubkey::Pubkey,
        std::{sync::Once, time::Duration},
    };

    lazy_static::lazy_static! {
        static ref GRPC_SLOT_RECEIVED: IntGaugeVec = IntGaugeVec::new(
            Opts::new("grpc_slot_received", "Grpc slot by commitment"),
            &["commitment"]
        ).unwrap();

        static ref QUIC_IDENTITY_EXPECTED: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity_expected", "Expected QUIC identity"), &["identity"]).unwrap();

        static ref BLOCKHASH_QUEUE_SIZE: IntGauge = IntGauge::new("blockhash_queue_size", "Total number of blocks in cache").unwrap();
        static ref BLOCKHASH_QUEUE_LATEST: IntGaugeVec = IntGaugeVec::new(
            Opts::new("blockhash_queue_slot", "Block cache slot by commitment"),
            &["commitment"]
        ).unwrap();

        static ref CLUSTER_NODES_TOTAL: IntGauge = IntGauge::new("cluster_nodes_total", "Total number of cluster nodes from gossip").unwrap();
        static ref CLUSTER_LEADERS_SCHEDULE_SIZE: IntGauge = IntGauge::new("cluster_leaders_schedule_size", "Total number of leaders in local schedule").unwrap();
        static ref CLUSTER_IDENTITY_STAKE: IntGaugeVec = IntGaugeVec::new(
            Opts::new("cluster_identity_stake", "Jet identity stake information"),
            &["kind"]
        ).unwrap();

        static ref ROOTED_TRANSACTIONS_POOL_SIZE: IntGauge = IntGauge::new("rooted_transactions_pool_size", "Total number of transactions in landed pool").unwrap();

        static ref STS_POOL_SIZE: IntGauge = IntGauge::new("sts_pool_size", "Number of transactions in the pool").unwrap();
        static ref STS_INFLIGHT_SIZE: IntGauge = IntGauge::new("sts_inflight_size", "Number of transactions sending right now").unwrap();
        static ref STS_RECEIVED_TOTAL: IntCounter = IntCounter::new("sts_received_total", "Total number of received transactions").unwrap();
        static ref STS_LANDED_TOTAL: IntCounter = IntCounter::new("sts_landed_total", "Total number of landed transactions").unwrap();
        // TODO we should rename this in another version of jet.
        static ref STS_TPU_DENIED_TOTAL: IntCounter = IntCounter::new("sts_tpu_denied_total", "Total number of denied TPUs by Shield policy").unwrap();

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

    pub fn init() {
        init2();

        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            register!(BLOCKHASH_QUEUE_LATEST);
            register!(BLOCKHASH_QUEUE_SIZE);
            register!(CLUSTER_IDENTITY_STAKE);
            register!(CLUSTER_LEADERS_SCHEDULE_SIZE);
            register!(CLUSTER_NODES_TOTAL);
            register!(FORWADED_TRANSACTION_LATENCY);
            register!(GATEWAY_CONNECTED);
            register!(GRPC_SLOT_RECEIVED);

            register!(ROOTED_TRANSACTIONS_POOL_SIZE);
            register!(SEND_TRANSACTION_ATTEMPT);
            register!(SEND_TRANSACTION_ERROR);
            register!(SEND_TRANSACTION_SUCCESS);
            register!(SHIELD_POLICIES_NOT_FOUND_TOTAL);
            register!(STS_INFLIGHT_SIZE);
            register!(STS_LANDED_TOTAL);
            register!(STS_POOL_SIZE);
            register!(STS_RECEIVED_TOTAL);
            register!(STS_TPU_DENIED_TOTAL);
            register!(TRANSACTION_DECODE_ERRORS);
            register!(TRANSACTION_DESERIALIZE_ERRORS);

            register!(CLUSTER_TPU_LOCK_ACQUISITION_TIME);
            register!(LEADER_SCHEDULE_EXISTS_CHECK_TIME);
            register!(SLOT_UPDATE_LOOP_ITERATION_TIME);
            register!(SLOT_UPDATES_DRAINED_COUNT);
            register!(SLOT_STATUS_RECEIVED_BY_TYPE);
            register!(LEADER_SCHEDULE_RPC_FETCH_TIME);
            register!(LEADER_SCHEDULE_RPC_ATTEMPTS);
            register!(LEADER_SCHEDULE_PARSE_AND_INSERT_TIME);
            register!(LEADER_SCHEDULE_SIZE);
            register!(LEADER_SCHEDULE_ENTRIES_ADDED);
            register!(LEADER_SCHEDULE_ENTRIES_CLEANED);
            register!(GRPC_MESSAGE_HANDLE_TIME);
            register!(GRPC_SLOT_UPDATE_HANDLE_TIME);
            register!(GRPC_CHANNEL_SEND_TIME);
            register!(GRPC_CHANNEL_SEND_FAILURES);
            register!(SLOT_TRACKING_BTREEMAP_SIZE);
            register!(BLOCK_META_EMISSIONS_COUNT);
            register!(GRPC_MESSAGES_PROCESSED_RATE);
            register!(NEW_SLOT_ARRIVAL_INTERVAL);

            yellowstone_jet_tpu_client::prom::register_metrics(&REGISTRY);
            grpc_lewis::prom::register_metrics(&REGISTRY);
        });
    }

    pub fn incr_send_tx_attempt(leader: Pubkey) {
        SEND_TRANSACTION_ATTEMPT
            .with_label_values(&[&leader.to_string()])
            .inc();
    }

    pub fn increment_send_transaction_error() {
        SEND_TRANSACTION_ERROR.inc();
    }

    pub fn increment_send_transaction_success() {
        SEND_TRANSACTION_SUCCESS.inc();
    }

    pub fn observe_forwarded_txn_latency(duration: f64) {
        FORWADED_TRANSACTION_LATENCY
            .with_label_values::<&str>(&[])
            .observe(duration);
    }

    pub fn increment_transaction_decode_error(error_type: &str) {
        TRANSACTION_DECODE_ERRORS
            .with_label_values(&[error_type])
            .inc();
    }

    pub fn increment_transaction_deserialize_error(error_type: &str) {
        TRANSACTION_DESERIALIZE_ERRORS
            .with_label_values(&[error_type])
            .inc();
    }

    pub fn quic_set_identity_expected(identity: Pubkey) {
        QUIC_IDENTITY_EXPECTED.reset();
        QUIC_IDENTITY_EXPECTED
            .with_label_values(&[&identity.to_string()])
            .set(1);
    }

    // TODO:  this logic should not be in metrics module as its use for RPC admin module.
    pub fn get_health_status() -> anyhow::Result<()> {
        anyhow::ensure!(
            GRPC_SLOT_RECEIVED
                .with_label_values(&[CommitmentLevel::Processed.as_str()])
                .get()
                > 0,
            "gRPC is not initialized",
        );

        anyhow::ensure!(
            CLUSTER_NODES_TOTAL.get() > 0,
            "no information about cluster nodes"
        );
        anyhow::ensure!(
            CLUSTER_LEADERS_SCHEDULE_SIZE.get() > 0,
            "no information about leaders"
        );

        anyhow::ensure!(
            ROOTED_TRANSACTIONS_POOL_SIZE.get() > 0,
            "no transactions in the landing pool"
        );

        Ok(())
    }

    pub fn blockhash_queue_set_size(size: usize) {
        BLOCKHASH_QUEUE_SIZE.set(size as i64)
    }

    pub fn blockhash_queue_set_slot(commitment: CommitmentLevel, slot: Slot) {
        BLOCKHASH_QUEUE_LATEST
            .with_label_values(&[commitment.as_str()])
            .set(slot as i64)
    }

    pub fn grpc_slot_set(slot_status: SlotStatus, slot: Slot) {
        GRPC_SLOT_RECEIVED
            .with_label_values(&[slot_status.as_str()])
            .set(slot as i64);
    }

    pub fn cluster_nodes_set_size(size: usize) {
        CLUSTER_NODES_TOTAL.set(size as i64);
    }

    pub fn cluster_leaders_schedule_set_size(size: usize) {
        CLUSTER_LEADERS_SCHEDULE_SIZE.set(size as i64);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClusterIdentityStakeKind {
        Total,
        Jet,
        MaxPermitPer100ms,
        MaxStreams,
    }

    impl ClusterIdentityStakeKind {
        const fn as_str(self) -> &'static str {
            match self {
                Self::Total => "total",
                Self::Jet => "jet",
                Self::MaxPermitPer100ms => "max_per100ms",
                Self::MaxStreams => "max_stream",
            }
        }
    }

    pub fn cluster_identity_stake_set(kind: ClusterIdentityStakeKind, value: u64) {
        CLUSTER_IDENTITY_STAKE
            .with_label_values(&[kind.as_str()])
            .set(value.try_into().expect("failed to convert to i64"))
    }

    pub fn cluster_identity_stake_get_max_streams() -> u64 {
        CLUSTER_IDENTITY_STAKE
            .with_label_values(&[ClusterIdentityStakeKind::MaxPermitPer100ms.as_str()])
            .get()
            .try_into()
            .expect("failed to convert to u64")
    }

    pub fn rooted_transactions_pool_set_size(size: usize) {
        ROOTED_TRANSACTIONS_POOL_SIZE.set(size as i64)
    }

    pub fn sts_pool_set_size(size: usize) {
        STS_POOL_SIZE.set(size as i64)
    }

    pub fn sts_pool_get_size() -> i64 {
        STS_POOL_SIZE.get()
    }

    pub fn sts_inflight_set_size(size: usize) {
        STS_INFLIGHT_SIZE.set(size as i64)
    }

    pub fn sts_received_inc() {
        STS_RECEIVED_TOTAL.inc();
    }

    pub fn sts_landed_inc() {
        STS_LANDED_TOTAL.inc();
    }

    pub fn sts_tpu_denied_inc_by(denied: usize) {
        STS_TPU_DENIED_TOTAL.inc_by(denied as u64);
    }

    pub fn shield_policies_not_found_inc() {
        SHIELD_POLICIES_NOT_FOUND_TOTAL.inc();
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

    pub fn observe_cluster_tpu_lock_time(method: &str, lock_type: &str, duration: Duration) {
        CLUSTER_TPU_LOCK_ACQUISITION_TIME
            .with_label_values(&[method, lock_type])
            .observe(duration.as_micros() as f64);
    }

    pub fn observe_leader_schedule_exists_check_time(duration: Duration) {
        LEADER_SCHEDULE_EXISTS_CHECK_TIME.observe(duration.as_micros() as f64);
    }

    pub fn observe_slot_update_loop_iteration_time(duration: Duration) {
        SLOT_UPDATE_LOOP_ITERATION_TIME.observe(duration.as_micros() as f64);
    }

    pub fn observe_slot_updates_drained_count(count: usize) {
        SLOT_UPDATES_DRAINED_COUNT.observe(count as f64);
    }

    pub fn incr_slot_status_received_by_type(status: &str) {
        SLOT_STATUS_RECEIVED_BY_TYPE
            .with_label_values(&[status])
            .inc();
    }

    pub fn observe_leader_schedule_rpc_fetch_time(duration: Duration) {
        LEADER_SCHEDULE_RPC_FETCH_TIME.observe(duration.as_millis() as f64);
    }

    pub fn incr_leader_schedule_rpc_attempts() {
        LEADER_SCHEDULE_RPC_ATTEMPTS.inc();
    }

    pub fn observe_leader_schedule_parse_insert_time(duration: Duration) {
        LEADER_SCHEDULE_PARSE_AND_INSERT_TIME.observe(duration.as_micros() as f64);
    }

    pub fn set_leader_schedule_size(size: usize) {
        LEADER_SCHEDULE_SIZE.set(size as i64);
    }

    pub fn set_leader_schedule_entries_added(count: usize) {
        LEADER_SCHEDULE_ENTRIES_ADDED.set(count as i64);
    }

    pub fn set_leader_schedule_entries_cleaned(count: usize) {
        LEADER_SCHEDULE_ENTRIES_CLEANED.set(count as i64);
    }

    pub fn observe_grpc_message_handle_time(message_type: &str, duration: Duration) {
        GRPC_MESSAGE_HANDLE_TIME
            .with_label_values(&[message_type])
            .observe(duration.as_micros() as f64);
    }

    pub fn observe_grpc_slot_update_handle_time(duration: Duration) {
        GRPC_SLOT_UPDATE_HANDLE_TIME.observe(duration.as_micros() as f64);
    }

    pub fn observe_grpc_channel_send_time(channel: &str, duration: Duration) {
        GRPC_CHANNEL_SEND_TIME
            .with_label_values(&[channel])
            .observe(duration.as_micros() as f64);
    }

    pub fn incr_grpc_channel_send_failures(channel: &str) {
        GRPC_CHANNEL_SEND_FAILURES
            .with_label_values(&[channel])
            .inc();
    }

    pub fn set_slot_tracking_btreemap_size(size: usize) {
        SLOT_TRACKING_BTREEMAP_SIZE.set(size as i64);
    }

    pub fn observe_block_meta_emissions_count(count: usize) {
        BLOCK_META_EMISSIONS_COUNT.observe(count as f64);
    }

    pub fn incr_grpc_messages_processed(message_type: &str) {
        GRPC_MESSAGES_PROCESSED_RATE
            .with_label_values(&[message_type])
            .inc();
    }

    pub fn observe_new_slot_arrival_interval(duration: Duration) {
        NEW_SLOT_ARRIVAL_INTERVAL.observe(duration.as_millis() as f64);
    }
}
