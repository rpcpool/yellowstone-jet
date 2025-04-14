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
        super::{init2, REGISTRY},
        crate::util::CommitmentLevel,
        prometheus::{
            HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
        },
        solana_sdk::{clock::Slot, pubkey::Pubkey},
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
        static ref STS_TPU_SEND: IntCounterVec = IntCounterVec::new(
            Opts::new("sts_tpu_send_total", "Number of transactions sent to TPU"),
            &["leaders"]
        ).unwrap();
        static ref STS_TPU_DENIED_TOTAL: IntCounter = IntCounter::new("sts_tpu_denied_total", "Total number of denied TPUs by Shield policy").unwrap();

        static ref BANNED_TRANSACTIONS_TOTAL: IntCounter = IntCounter::new("banned_transactions_total", "Total number of banned transactions").unwrap();

        static ref QUIC_IDENTITY: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity", "Current QUIC identity"), &["identity"]).unwrap();
        static ref QUIC_IDENTITY_EXPECTED: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity_expected", "Expected QUIC identity"), &["identity"]).unwrap();
        static ref QUIC_SEND_ATTEMPTS: IntCounterVec = IntCounterVec::new(
            Opts::new("quic_send_attempts", "Status of sending transactions with QUIC"),
            &["leader", "address", "status", "failure_message"]
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
            register!(BANNED_TRANSACTIONS_TOTAL);
            register!(CLUSTER_NODES_TOTAL);
            register!(CLUSTER_LEADERS_SCHEDULE_SIZE);
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
            register!(STS_TPU_SEND);
            register!(TRANSACTION_DECODE_ERRORS);
            register!(TRANSACTION_DESERIALIZE_ERRORS);
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

    pub fn increment_banned_transactions_total() {
        BANNED_TRANSACTIONS_TOTAL.inc();
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

    pub fn grpc_slot_set(commitment: CommitmentLevel, slot: Slot) {
        GRPC_SLOT_RECEIVED
            .with_label_values(&[commitment.as_str()])
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

    pub fn sts_tpu_send_inc(leaders: usize) {
        STS_TPU_SEND
            .with_label_values(&[leaders.to_string().as_str()])
            .inc();
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

    pub fn quic_send_attempts_inc(
        leader: &Pubkey,
        address: &SocketAddr,
        status: &str,
        failure_message: &str,
    ) {
        QUIC_SEND_ATTEMPTS
            .with_label_values(&[
                &leader.to_string(),
                &address.to_string(),
                status,
                failure_message,
            ])
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
}
