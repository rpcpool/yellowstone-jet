use {
    prometheus::{
        Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
        Opts, Registry,
    },
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, time::Duration},
};

lazy_static::lazy_static! {
    static ref QUIC_IDENTITY: IntGaugeVec = IntGaugeVec::new(Opts::new("quic_identity", "Current QUIC identity"), &["identity"]).unwrap();
    static ref QUIC_SEND_ATTEMPTS: IntCounterVec = IntCounterVec::new(
        Opts::new("quic_send_attempts", "Status of sending transactions with QUIC"),
        &["leader", "address", "status"]
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
                50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 750.0, 1000.0,
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

    static ref QUIC_GW_TX_CONNECTION_CACHE_HIT_CNT: IntCounter = IntCounter::new(
        "quic_gw_tx_connection_cache_hit", "Number of hits transaction got forward to an existing connection to remote peer validators"
    ).unwrap();

    static ref QUIC_GW_TX_CONNECTION_CACHE_MISS_CNT: IntCounter = IntCounter::new(
        "quic_gw_tx_connection_cache_miss", "Number of misses transaction got forward to a new connection to remote peer validators"
    ).unwrap();

    static ref TXN_BLOCKED_BY_CONNECTION: IntGauge = IntGauge::new(
        "jet_tpu_txn_blocked_by_connecting", "Number of transactions waiting for remote peer connection to be established"
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

    static ref ACTIVE_QUIC_CONNECTIONS: IntGauge = IntGauge::new(
        "jet_tpu_client_active_quic_connection",
        "Number of active QUIC connections"
    ).unwrap();

    static ref ACTIVE_QUIC_TX_SENDERS: IntGauge = IntGauge::new(
        "jet_tpu_client_active_quic_tx_senders",
        "Number of active QUIC transaction senders"
    ).unwrap();

    static ref NUM_CONN_TO_EVICT: IntGauge = IntGauge::new(
        "jet_tpu_num_conn_to_evict",
        "Number of connections waiting to be evicted"
    ).unwrap();

    static ref TXN_WORKER_PRE_INSTALL_MISS: IntCounter = IntCounter::new(
        "jet_tpu_client_worker_pre_install_miss",
        "Number of times a transaction worker was not pre-installed for a remote peer"
    ).unwrap();

    static ref INVALID_TXN_PACKET_SIZE: IntCounter = IntCounter::new(
        "jet_tpu_client_invalid_txn_packet_size",
        "Number of times a transaction packet exceeded the maximum allowed size"
    ).unwrap();

    static ref ORPHAN_CONNECTIONS: IntGauge = IntGauge::new(
        "jet_tpu_client_orphan_connections",
        "Number of orphan connections (no sender tasks referencing them)"
    ).unwrap();

    static ref EVICTED_ORPHAN_CONNECTIONS: IntCounter = IntCounter::new(
        "jet_tpu_client_evicted_orphan_connections",
        "Number of evicted orphan connections"
    ).unwrap();

    static ref FAST_TXN_WORKER_INSTALL_PATH: IntCounter = IntCounter::new(
        "jet_tpu_client_fast_txn_worker_install_path",
        "Number of times the fast path was taken to install a transaction worker"
    ).unwrap();
}

pub fn incr_evicted_orphan_connections() {
    EVICTED_ORPHAN_CONNECTIONS.inc();
}

pub fn incr_fast_txn_worker_install_path() {
    FAST_TXN_WORKER_INSTALL_PATH.inc();
}

pub fn set_orphan_connections(count: usize) {
    ORPHAN_CONNECTIONS.set(count as i64);
}

pub fn incr_invalid_txn_packet_size() {
    INVALID_TXN_PACKET_SIZE.inc();
}

pub fn incr_txn_worker_pre_installed_miss() {
    TXN_WORKER_PRE_INSTALL_MISS.inc();
}

pub fn set_num_conn_to_evict(count: usize) {
    NUM_CONN_TO_EVICT.set(count as i64);
}

pub fn set_active_quic_tx_senders(count: usize) {
    ACTIVE_QUIC_TX_SENDERS.set(count as i64);
}

pub fn set_active_quic_connections(count: usize) {
    ACTIVE_QUIC_CONNECTIONS.set(count as i64);
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

pub fn set_txn_blocked_by_connection(blocked: usize) {
    TXN_BLOCKED_BY_CONNECTION.set(blocked as i64);
}
pub fn set_quic_gw_connecting_cnt(connecting: usize) {
    QUIC_GW_CONNECTING_GAUGE.set(connecting as i64);
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
    LEADER_RTT
        .with_label_values(&["all"])
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

pub fn register_metrics(reg: &Registry) {
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
    reg.register(Box::new(TXN_BLOCKED_BY_CONNECTION.clone()))
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

    reg.register(Box::new(QUIC_GW_DROP_TX_CNT.clone())).unwrap();
    reg.register(Box::new(QUIC_GW_WORKER_TX_PROCESS_CNT.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_GW_TX_RELAYED_TO_WORKER_CNT.clone()))
        .unwrap();

    reg.register(Box::new(QUIC_IDENTITY.clone())).unwrap();
    reg.register(Box::new(LEADER_MTU.clone())).unwrap();
    reg.register(Box::new(LEADER_RTT.clone())).unwrap();
    reg.register(Box::new(SEND_TRANSACTION_E2E_LATENCY.clone()))
        .unwrap();
    reg.register(Box::new(QUIC_SEND_ATTEMPTS.clone())).unwrap();
    reg.register(Box::new(ACTIVE_QUIC_CONNECTIONS.clone()))
        .unwrap();
    reg.register(Box::new(ACTIVE_QUIC_TX_SENDERS.clone()))
        .unwrap();
    reg.register(Box::new(NUM_CONN_TO_EVICT.clone())).unwrap();
    reg.register(Box::new(TXN_WORKER_PRE_INSTALL_MISS.clone()))
        .unwrap();
    reg.register(Box::new(INVALID_TXN_PACKET_SIZE.clone()))
        .unwrap();
    reg.register(Box::new(ORPHAN_CONNECTIONS.clone())).unwrap();
    reg.register(Box::new(FAST_TXN_WORKER_INSTALL_PATH.clone()))
        .unwrap();
    reg.register(Box::new(EVICTED_ORPHAN_CONNECTIONS.clone()))
        .unwrap();
}

pub fn inc_quic_gw_unreachable_peer_count(leader: Pubkey) {
    QUIC_GW_UNREACHABLE_PEER_CNT
        .with_label_values(&[&leader.to_string()])
        .inc();
}

pub fn quic_set_identity(identity: Pubkey) {
    QUIC_IDENTITY.reset();
    QUIC_IDENTITY
        .with_label_values(&[&identity.to_string()])
        .set(1);
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
