
CREATE TABLE IF NOT EXISTS chain_transaction_staging
(
    signature String,
    slot UInt64,
    failed Bool,
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (slot, signature)
-- Safety net only: the slot-watermark purge below (see purge_chain_transaction_staging.sql)
-- is the primary drain. This TTL just bounds growth if that purge ever stops running.
TTL ts + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS landed_transactions
(
    signature String,
    send_at_slot UInt64,
    landed_slot UInt64,
    failed Bool,
    trace_remote_peer_identity Nullable(String),
    trace_remote_peer_addr Nullable(String),
    trace_inserted_at DateTime64(3),
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
-- Dedups by (landed_slot, signature) during background merges, keeping the row with the
-- latest `ts`. This is what makes the `>=` watermark boundary in mv_landed_transactions safe
-- to rescan every refresh without accumulating duplicate rows -- see the note there.
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toDate(ts)
ORDER BY (landed_slot, signature)
TTL ts + INTERVAL 1 DAY;


-- Watermark for `chain_transaction_staging` (same domain as its own `slot`: landed slot).
-- Used by purge_chain_transaction_staging.sql, not by the MV below.
CREATE VIEW IF NOT EXISTS landed_transaction_max_slot AS
SELECT max(landed_slot) AS max_slot FROM landed_transactions;

-- Watermark for `jet.txn_trace`. Uses `trace_inserted_at` (jet.txn_trace's own `ts`, i.e.
-- insertion order), not `send_at_slot` -- `send_at_slot` is a business value with no
-- guaranteed relationship to insertion order, and isn't part of jet.txn_trace's ORDER BY
-- (ts, signature), so filtering on it never got any index pruning either.
CREATE VIEW IF NOT EXISTS landed_transaction_max_trace_ts AS
SELECT max(trace_inserted_at) AS max_ts FROM landed_transactions;



--
-- Every minute, joins `jet.txn_trace` rows in "sent" state (past the last insertion-time
-- watermark) against `chain_transaction_staging` rows at or past the last landed-slot
-- watermark (raw on-chain transactions observed via the Fumarole block stream, with no claim
-- yet about whether jet sent them), and appends matches. No upper bound on `lt.slot`: every
-- block reaching `chain_transaction_staging` is already complete (the ingester only flushes a
-- slot once Fumarole signals it ended) and a single INSERT is atomic, so there's no
-- still-filling-block risk to guard against here. Only a signature present in both tables --
-- confirmed on-chain AND recorded as "sent" by jet -- is a transaction we know we landed,
-- which is what makes it into this table. Both watermarks are self-referential (read back from
-- `landed_transactions` itself) -- no separate state table needed.
--
-- Only "sent" state is of interest here, so `state`/`error_msg`/`drop_reason` (only populated
-- for "failed"/"drop" trace rows) are dropped rather than carried into this table.
--
-- Note: this only inserts. It does NOT purge `chain_transaction_staging` -- ClickHouse
-- refreshable views are SELECT-only, so the corresponding purge is a plain `ALTER TABLE ...
-- DELETE` statement (see purge_chain_transaction_staging.sql) that must be triggered by
-- something outside ClickHouse (systemd timer / k8s CronJob / crontab), on the same cadence.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_landed_transactions
REFRESH EVERY 1 MINUTE
APPEND
TO landed_transactions
AS
SELECT
    lt.signature AS signature,
    lt.failed AS failed,
    tt.remote_peer_identity AS trace_remote_peer_identity,
    tt.remote_peer_addr AS trace_remote_peer_addr,
    tt.send_at_slot AS send_at_slot,
    lt.slot AS landed_slot,
    tt.ts AS trace_inserted_at
FROM jet.txn_trace AS tt
ANY INNER JOIN chain_transaction_staging AS lt ON lt.signature = tt.signature

WHERE
    tt.state = 'sent'   -- We only care about transactions that were sent, not failed or dropped
    AND lt.slot >= (SELECT max_slot FROM landed_transaction_max_slot)
    -- Deliberately overlaps with rows already scanned in a previous refresh (>=, not >, plus a
    -- 30s grace margin): safe because `landed_transactions` is a ReplacingMergeTree, so
    -- re-matched rows just collapse into duplicates that merge away. A precise, non-overlapping
    -- boundary would instead risk permanently dropping any row sharing the exact watermark
    -- value with an already-matched row -- silent data loss that dedup can't undo, since a row
    -- that's never rescanned is never inserted at all.
    AND tt.ts >= coalesce(
        (SELECT max_ts FROM landed_transaction_max_trace_ts),
        toDateTime64(0, 3)
    ) - INTERVAL 30 SECOND
SETTINGS join_use_nulls = 1;
