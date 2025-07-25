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
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
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
                VERSION_INFO.solana,
                VERSION_INFO.version,
            ])
            .inc();
    });
}

pub fn inject_job_label(metrics: &str, job: &str, instance: &str) -> String {
    metrics
        .lines()
        .map(|line| {
            if line.starts_with("#") {
                line.to_string()
            } else if let Some(pos) = line.find('{') {
                let (metric_name, rest) = line.split_at(pos + 1);
                format!(
                    r#"{}job="{}", instance="{}", {rest}"#,
                    metric_name, job, instance
                )
            } else if let Some(pos) = line.find(' ') {
                let (metric_name, value) = line.split_at(pos);
                format!(r#"{metric_name}{{job="{job}", instance="{instance}"}}{value}"#)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
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
        crate::util::{CommitmentLevel, SlotStatus},
        prometheus::{
            Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
            IntGaugeVec, Opts,
        },
        solana_clock::Slot,
        solana_pubkey::Pubkey,
        std::{
            net::SocketAddr,
            sync::{Mutex, Once},
            time::Duration,
        },
    };

    lazy_static::lazy_static! {
        static ref GRPC_SLOT_RECEIVED: IntGaugeVec = IntGaugeVec::new(
            Opts::new("grpc_slot_received", "Grpc slot by commitment"),
            &["commitment"]
        ).unwrap();

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

        static ref QUIC_IDENTITY: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity", "Current QUIC identity"), &["identity"]).unwrap();
        static ref QUIC_IDENTITY_EXPECTED: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity_expected", "Expected QUIC identity"), &["identity"]).unwrap();
        static ref QUIC_SEND_ATTEMPTS: IntCounterVec = IntCounterVec::new(
            Opts::new("quic_send_attempts", "Status of sending transactions with QUIC"),
            &["leader", "address", "status"]
        ).unwrap();

        static ref QUIC_IDENTITY_VALUE: Mutex<Option<Pubkey>> = Mutex::new(None);
        static ref QUIC_IDENTITY_EXPECTED_VALUE: Mutex<Option<Pubkey>> = Mutex::new(None);

        static ref METRICS_UPSTREAM_PUSH: IntCounterVec = IntCounterVec::new(
            Opts::new("metrics_upstream_push_total", "Total number of events pushed to send queue"),
            &["status"]
        ).unwrap();
        static ref METRICS_UPSTREAM_FEED: IntCounter = IntCounter::new("metrics_upstream_feed_total", "Total number of events feed to gRPC").unwrap();

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

        static ref QUIC_GW_DROP_TX_CNT: IntCounterVec = IntCounterVec::new(
            Opts::new(
                "quic_gw_drop_tx_cnt",
                "Number of transactions dropped due to worker queue being full"
            ),
            &["leader"]
        ).unwrap();

        static ref TRANSACTION_LANDING_TIME: Histogram = Histogram::with_opts(
           HistogramOpts::new("transaction_landing_time_ms", "Time taken to land a transaction in ms")
           .buckets(vec![0.5, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0, 200.0, 300.0, 400.0,500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1500.0, 2000.0, f64::INFINITY])
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

    }

    pub fn incr_quic_gw_tx_relayed_to_worker(remote_peer: Pubkey) {
        QUIC_GW_TX_RELAYED_TO_WORKER_CNT
            .with_label_values(&[&remote_peer.to_string()])
            .inc();
    }

    pub fn incr_quic_gw_worker_tx_process_cnt(remote_peer: Pubkey, status: &str) {
        QUIC_GW_WORKER_TX_PROCESS_CNT
            .with_label_values(&[&remote_peer.to_string(), status])
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
            register!(LEADER_MTU);
            register!(LEADER_RTT);
            register!(METRICS_UPSTREAM_FEED);
            register!(METRICS_UPSTREAM_PUSH);
            register!(QUIC_IDENTITY);
            register!(QUIC_IDENTITY_EXPECTED);
            register!(QUIC_SEND_ATTEMPTS);
            register!(ROOTED_TRANSACTIONS_POOL_SIZE);
            register!(SEND_TRANSACTION_ATTEMPT);
            register!(SEND_TRANSACTION_E2E_LATENCY);
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

            register!(QUIC_GW_CONNECTING_GAUGE);
            register!(QUIC_GW_CONNECTION_SUCCESS_CNT);
            register!(QUIC_GW_CONNECTION_FAILURE_CNT);
            register!(QUIC_GW_ACTIVE_CONNECTION_GAUGE);
            register!(QUIC_GW_TOTAL_CONNECTION_EVICTIONS_CNT);
            register!(QUIC_GW_CONNECTION_CLOSE_CNT);
            register!(QUIC_GW_ONGOING_EVICTIONS_GAUGE);
            register!(QUIC_GW_TX_CONNECTION_CACHE_HIT_CNT);
            register!(QUIC_GW_TX_CONNECTION_CACHE_MISS_CNT);
            register!(QUIC_GW_TX_BLOCKED_BY_CONNECTING_GAUGE);
            register!(QUIC_GW_CONNECTION_TIME_HIST);
            register!(QUIC_GW_REMOTE_PEER_ADDR_CHANGES_DETECTED);
            register!(QUIC_GW_LEADER_PREDICTION_HIT);
            register!(QUIC_GW_LEADER_PREDICTION_MISS);
            register!(QUIC_GW_DROP_TX_CNT);
            register!(QUIC_GW_WORKER_TX_PROCESS_CNT);
            register!(QUIC_GW_TX_RELAYED_TO_WORKER_CNT);
            register!(TRANSACTION_LANDING_TIME);
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
            .with_label_values(&[])
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

    pub fn get_health_status() -> anyhow::Result<()> {
        if let Some(expected) = *QUIC_IDENTITY_EXPECTED_VALUE.lock().unwrap() {
            if let Some(current) = *QUIC_IDENTITY_VALUE.lock().unwrap() {
                anyhow::ensure!(expected == current, "identity mismatch");
            }
        }

        anyhow::ensure!(
            GRPC_SLOT_RECEIVED
                .with_label_values(&[CommitmentLevel::Processed.as_str()])
                .get()
                > 0,
            "gRPC is not initialized",
        );

        // anyhow::ensure!(
        //     BLOCKHASH_QUEUE_SIZE.get() >= solana_sdk::clock::MAX_PROCESSING_AGE as i64,
        //     "not enough blocks in the cache"
        // );

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
            .with_label_values(&[&leader.to_string(), &address.to_string(), status])
            .inc();
    }

    pub fn metrics_upstream_push_inc(status: Result<(), ()>) {
        METRICS_UPSTREAM_PUSH
            .with_label_values(&[if status.is_ok() { "ok" } else { "overflow" }])
            .inc()
    }

    pub fn metrics_upstream_feed_inc() {
        METRICS_UPSTREAM_FEED.inc()
    }

    pub fn gateway_set_connected(endpoints: &[String], endpoint: String) {
        for endpoint in endpoints {
            GATEWAY_CONNECTED
                .with_label_values(&[endpoint.as_ref()])
                .set(0);
        }
        GATEWAY_CONNECTED
            .with_label_values(&[endpoint.as_ref()])
            .set(1);
    }

    pub fn gateway_set_disconnected(endpoints: &[String]) {
        for endpoint in endpoints {
            GATEWAY_CONNECTED
                .with_label_values(&[endpoint.as_ref()])
                .set(0);
        }
    }
    pub fn observe_transaction_landing_time(duration: Duration) {
    TRANSACTION_LANDING_TIME.observe(duration.as_millis() as f64);
    }   
}
