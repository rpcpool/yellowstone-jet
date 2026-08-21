--
-- Migrates the live tables from the pre-"trace ids + entry position" schema (base commit
-- d81f556) to the current schema.sql, preserving existing data.
--
-- These tables are S3-backed, so this avoids ALTER TABLE ... ADD COLUMN entirely and instead:
--   1. creates a `<table>_new` with the full new schema,
--   2. backfills it from the live table (new columns default to NULL / 0 for old rows, since
--      that data genuinely didn't exist yet),
--   3. atomically swaps names via `RENAME TABLE old TO old_bak, new TO old` (one statement,
--      so there's no window where the table doesn't exist under its expected name),
--   4. drops and recreates the (non-refreshable) materialized views that depend on any swapped
--      table.
--
-- Step 4 is not optional. A plain `MATERIALIZED VIEW ... TO <target>` (and, separately, its
-- `FROM <source>`) binds to that table's underlying storage at CREATE time, not by name -- after
-- a rename swap, an MV created before the swap keeps writing into (or reading from) the OLD
-- storage, now sitting under the `_bak` name, not the freshly-swapped-in table. Skipping this
-- step means the pipeline silently stops flowing into the "new" tables after the swap.
-- Refreshable MVs (`REFRESH EVERY ...`) and plain `VIEW`s are NOT affected -- both re-resolve
-- table names by name on every run/query, so they're left alone.
--
-- Affected MVs: mv_populate_sent_pending, mv_landed_transactions, mv_landed_transaction_entry_pos.
-- Not affected (left alone): mv_landed_transaction_slot_latency_1m (refreshable),
-- landed_transaction_slot_latency and chain_entry_with_txn_count_bounds (plain views).
--
-- Not covered here: `chain_entry_staging` and `landed_transaction_entry_pos` are brand new
-- tables with no prior data -- just run schema.sql's plain `CREATE TABLE IF NOT EXISTS` for
-- those, nothing to migrate.
--
-- Assumes all of chain_transaction_staging / landed_transactions / sent_transaction_pending /
-- txn_trace live in the same database (adjust the `jet.` prefixes below if not). Run with
-- `clickhouse-client --multiquery < migrate_add_trace_ids_and_txn_index.sql`.
--
-- This script does not DROP the `_bak` tables it creates -- do that yourself once you've
-- confirmed the swap looks right (see the bottom of this file).
--

-- ============================================================================
-- 1. Create the new tables and backfill them from the live ones.
-- ============================================================================

CREATE TABLE IF NOT EXISTS jet.chain_transaction_staging_new
(
    signature String,
    slot UInt64,
    txn_index UInt64,
    failed Bool,
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (slot, signature)
TTL ts + INTERVAL 1 DAY;

-- txn_index didn't exist yet for these rows; backfilled as 0 (same default ALTER TABLE ADD
-- COLUMN would have used). This table is a purge-drained staging buffer with a 1-day TTL, so
-- anything still here is either brand new or already past the point mv_landed_transactions
-- could still match it -- the placeholder value is not expected to matter in practice.
INSERT INTO jet.chain_transaction_staging_new (signature, slot, txn_index, failed, ts)
SELECT signature, slot, 0 AS txn_index, failed, ts
FROM jet.chain_transaction_staging;


CREATE TABLE IF NOT EXISTS jet.landed_transactions_new
(
    signature String,
    send_at_slot UInt64,
    landed_slot UInt64,
    txn_index UInt64,
    failed Bool,
    trace_remote_peer_identity Nullable(String),
    trace_remote_peer_addr Nullable(String),
    trace_x_request_id Nullable(UUID),
    trace_x_subscription_id Nullable(UUID),
    trace_inserted_at DateTime64(3),
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toDate(ts)
ORDER BY (landed_slot, signature)
TTL ts + INTERVAL 90 DAY;

-- trace_x_request_id / trace_x_subscription_id backfill as NULL -- these trace fields didn't
-- exist yet when these rows landed, there's no historical value to recover.
--
-- txn_index backfills as 0 too: it would need to come from the chain_transaction_staging row
-- that originally produced this landed_transactions row, but that table only has a 1-day TTL,
-- so by migration time the source row for any pre-existing landed_transactions entry has
-- almost certainly already expired -- there's nothing left to join against to recover it.
INSERT INTO jet.landed_transactions_new
    (signature, send_at_slot, landed_slot, txn_index, failed, trace_remote_peer_identity,
     trace_remote_peer_addr, trace_inserted_at, ts)
SELECT signature, send_at_slot, landed_slot, 0 AS txn_index, failed, trace_remote_peer_identity,
       trace_remote_peer_addr, trace_inserted_at, ts
FROM jet.landed_transactions;


CREATE TABLE IF NOT EXISTS jet.sent_transaction_pending_new
(
    signature String,
    send_at_slot UInt64,
    remote_peer_identity Nullable(String),
    remote_peer_addr Nullable(String),
    x_request_id Nullable(UUID),
    x_subscription_id Nullable(UUID),
    trace_inserted_at DateTime64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX mm_send_at_slot send_at_slot TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(trace_inserted_at)
PARTITION BY toDate(trace_inserted_at)
ORDER BY signature
TTL trace_inserted_at + INTERVAL 1 DAY;

-- x_request_id / x_subscription_id backfill as NULL. This table is a transient matching
-- buffer (1-day TTL); rows still here either haven't matched into landed_transactions yet
-- (and will still match fine without these two fields carried over) or are already stale.
INSERT INTO jet.sent_transaction_pending_new
    (signature, send_at_slot, remote_peer_identity, remote_peer_addr, trace_inserted_at)
SELECT signature, send_at_slot, remote_peer_identity, remote_peer_addr, trace_inserted_at
FROM jet.sent_transaction_pending;


CREATE TABLE IF NOT EXISTS jet.txn_trace_new
(
    signature String,
    x_request_id Nullable(UUID),
    x_subscription_id Nullable(UUID),
    state LowCardinality(String),
    error_msg Nullable(String),
    remote_peer_solana_client_id Nullable(String),
    remote_peer_identity Nullable(String),
    remote_peer_addr Nullable(String),
    drop_reason Nullable(String),
    send_at_slot Nullable(UInt64),
    drain_id Nullable(String),
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_x_request_id x_request_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_x_subscription_id x_subscription_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX set_remote_peer remote_peer_identity TYPE set(2048) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, signature);

-- ASSUMPTION: backfills BOTH x_request_id and x_subscription_id as NULL. If your currently
-- live txn_trace already has one of these two columns (e.g. x_request_id was added earlier,
-- separately from x_subscription_id), add it to both the column list and the SELECT below
-- instead of leaving it NULL.
INSERT INTO jet.txn_trace_new
    (signature, state, error_msg, remote_peer_solana_client_id, remote_peer_identity,
     remote_peer_addr, drop_reason, send_at_slot, drain_id, ts)
SELECT signature, state, error_msg, remote_peer_solana_client_id, remote_peer_identity,
       remote_peer_addr, drop_reason, send_at_slot, drain_id, ts
FROM jet.txn_trace;


-- ============================================================================
-- 2. Drop the materialized views that depend on any of the four tables above (as source or
--    target) -- see the header comment for why this has to happen before the rename swap.
--    The pipeline stops flowing between this point and step 4; keep that window short.
-- ============================================================================

DROP VIEW IF EXISTS jet.mv_populate_sent_pending;
DROP VIEW IF EXISTS jet.mv_landed_transactions;
DROP VIEW IF EXISTS jet.mv_landed_transaction_entry_pos;


-- ============================================================================
-- 3. Atomically swap each table in for its `_new` replacement. Each RENAME TABLE is one
--    statement (comma-separated), so there's no instant where the table is missing.
-- ============================================================================

RENAME TABLE
    jet.chain_transaction_staging TO jet.chain_transaction_staging_bak,
    jet.chain_transaction_staging_new TO jet.chain_transaction_staging;

RENAME TABLE
    jet.landed_transactions TO jet.landed_transactions_bak,
    jet.landed_transactions_new TO jet.landed_transactions;

RENAME TABLE
    jet.sent_transaction_pending TO jet.sent_transaction_pending_bak,
    jet.sent_transaction_pending_new TO jet.sent_transaction_pending;

RENAME TABLE
    jet.txn_trace TO jet.txn_trace_bak,
    jet.txn_trace_new TO jet.txn_trace;


-- ============================================================================
-- 4. Recreate the three materialized views dropped in step 2, verbatim from schema.sql --
--    now that the tables above have their final names again, these bind to the right storage.
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS jet.mv_populate_sent_pending
TO jet.sent_transaction_pending
AS
SELECT
    tt.signature AS signature,
    tt.send_at_slot AS send_at_slot,
    tt.remote_peer_identity AS remote_peer_identity,
    tt.remote_peer_addr AS remote_peer_addr,
    tt.x_request_id AS x_request_id,
    tt.x_subscription_id AS x_subscription_id,
    tt.ts AS trace_inserted_at
FROM jet.txn_trace AS tt
WHERE
    tt.state = 'sent'
    AND tt.send_at_slot IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS jet.mv_landed_transactions
TO jet.landed_transactions
AS
WITH
    (SELECT min(slot) FROM jet.chain_transaction_staging) AS batch_min_slot,
    (SELECT max(slot) FROM jet.chain_transaction_staging) AS batch_max_slot
SELECT
    lt.signature AS signature,
    lt.failed AS failed,
    lt.txn_index AS txn_index,
    tt.remote_peer_identity AS trace_remote_peer_identity,
    tt.remote_peer_addr AS trace_remote_peer_addr,
    tt.x_request_id AS trace_x_request_id,
    tt.x_subscription_id AS trace_x_subscription_id,
    tt.send_at_slot AS send_at_slot,
    lt.slot AS landed_slot,
    tt.trace_inserted_at AS trace_inserted_at
FROM jet.chain_transaction_staging AS lt
ANY INNER JOIN jet.sent_transaction_pending AS tt ON tt.signature = lt.signature
WHERE tt.send_at_slot BETWEEN batch_min_slot - 64 AND batch_max_slot
SETTINGS join_use_nulls = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS jet.mv_landed_transaction_entry_pos
TO jet.landed_transaction_entry_pos
AS
SELECT
    lt.signature AS signature,
    lt.landed_slot AS slot,
    es.entry_index AS entry_index,
    lt.txn_index AS txn_index
FROM jet.landed_transactions AS lt
ANY INNER JOIN jet.chain_entry_with_txn_count_bounds AS es
ON lt.landed_slot = es.slot
AND lt.txn_index < es.txn_count_cumu_upper_bound
AND lt.txn_index >= es.txn_count_cumu_lower_bound
SETTINGS join_use_nulls = 1;


-- ============================================================================
-- 5. Once you've verified row counts / spot-checked the swapped-in tables, drop the `_bak`
--    tables. Left commented out on purpose -- this script never drops data on its own; run
--    these explicitly once you're confident:
--
-- DROP TABLE jet.chain_transaction_staging_bak;
-- DROP TABLE jet.landed_transactions_bak;
-- DROP TABLE jet.sent_transaction_pending_bak;
-- DROP TABLE jet.txn_trace_bak;
-- ============================================================================
