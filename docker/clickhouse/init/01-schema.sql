CREATE DATABASE IF NOT EXISTS jet;

USE DATABASE jet;

CREATE TABLE IF NOT EXISTS txn_trace
(
    signature String,
    x_request_id Nullable(UUID),
    state LowCardinality(String),
    error_msg Nullable(String),
    remote_peer_solana_client_id Nullable(String),
    remote_peer_identity Nullable(String),
    remote_peer_addr Nullable(String),
    drop_reason Nullable(String),
    send_at_slot Nullable(UInt64),
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_x_request_id x_request_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX set_remote_peer remote_peer_identity TYPE set(2048) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (x_request_id, signature, ts);