
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
-- latest `ts`. This is what makes it safe for mv_landed_transactions to rejoin the same
-- candidates across refreshes without accumulating duplicate rows -- see the note there.
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toDate(ts)
ORDER BY (landed_slot, signature)
TTL ts + INTERVAL 90 DAY;

-- Slot-latency summary: how many slots after being sent a transaction actually landed.
-- FINAL is required here, not optional -- landed_transactions is a ReplacingMergeTree, and
-- without FINAL this would double-count rows that haven't merged away yet. Cheap while this
-- table stays small; re-check the FINAL cost if 90 days' worth of matched transactions ever
-- makes that untrue (see the note in mv_landed_transactions for the general reasoning).
CREATE VIEW IF NOT EXISTS landed_transaction_slot_latency AS
SELECT
    count() AS n,
    quantile(0.5)(abs(landed_slot - send_at_slot)) AS p50_slot_latency,
    quantile(0.9)(abs(landed_slot - send_at_slot)) AS p90_slot_latency,
    quantile(0.99)(abs(landed_slot - send_at_slot)) AS p99_slot_latency
FROM landed_transactions FINAL;

CREATE TABLE IF NOT EXISTS landed_transaction_slot_latency_1m
(
    window_start DateTime64(3),
    n UInt64,
    p50_slot_latency Float64,
    p90_slot_latency Float64,
    p99_slot_latency Float64
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY window_start
-- One row per 1-minute window instead of per transaction, so this stays small -- kept longer
-- than landed_transactions' own 90-day TTL to track latency trends past that point.
TTL window_start + INTERVAL 180 DAY;

--
-- Every minute, summarizes the *previous* (just-completed) 1-minute window of
-- landed_transactions into one row. FINAL is required for the same reason as
-- landed_transaction_slot_latency above: landed_transactions is a ReplacingMergeTree, and
-- without FINAL this would double-count rows that haven't merged away yet.
--
-- Approximate at the edges, not a correctness-critical pipeline step like the tables further
-- up: a manual `SYSTEM REFRESH VIEW` between scheduled ticks can duplicate a window's row, and
-- a long outage skips a window entirely rather than catching it up later (this only ever
-- computes "the window that just ended", not "every window since the last successful run").
-- Fine for a monitoring aggregate.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_landed_transaction_slot_latency_1m
REFRESH EVERY 1 MINUTE
APPEND
TO landed_transaction_slot_latency_1m
AS
SELECT
    toStartOfMinute(now()) - INTERVAL 1 MINUTE AS window_start,
    count() AS n,
    quantile(0.5)(abs(landed_slot - send_at_slot)) AS p50_slot_latency,
    quantile(0.9)(abs(landed_slot - send_at_slot)) AS p90_slot_latency,
    quantile(0.99)(abs(landed_slot - send_at_slot)) AS p99_slot_latency
FROM landed_transactions FINAL
WHERE
    ts >= toStartOfMinute(now()) - INTERVAL 1 MINUTE
    AND ts < toStartOfMinute(now());


CREATE TABLE IF NOT EXISTS sent_transaction_pending
(
    signature String,
    send_at_slot UInt64,
    remote_peer_identity Nullable(String),
    remote_peer_addr Nullable(String),
    trace_inserted_at DateTime64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
-- Dedups by signature during background merges, keeping the row with the latest
-- trace_inserted_at. Mirrors landed_transactions' own rationale: safe to rescan jet.txn_trace
-- with a small overlap window every refresh without unbounded duplication.
ENGINE = ReplacingMergeTree(trace_inserted_at)
PARTITION BY toDate(trace_inserted_at)
ORDER BY signature
-- Safety net only, same philosophy as chain_transaction_staging's own TTL: the purge script
-- (purge_sent_transaction_pending.sql) is the primary drain, removing matched rows. This just
-- bounds growth for the rare "sent but genuinely never landed" tail, which has no active
-- expiry -- see mv_landed_transactions below for why.
TTL trace_inserted_at + INTERVAL 1 DAY;

-- Watermark for populating sent_transaction_pending from jet.txn_trace -- tracks "have I
-- captured this row into pending at all", independent of match status, so a slow-to-match row
-- doesn't cause every later row to be re-pulled forever. Deliberately NOT a watermark over
-- landed_transactions: that would only advance on actual matches, which is a different thing.
--
-- Uses maxOrNull, not max: max() over an empty sent_transaction_pending doesn't reliably
-- coalesce to the epoch fallback below (observed in practice: mv_populate_sent_pending read 0
-- rows from jet.txn_trace because of this), even though trace_inserted_at is non-Nullable.
-- maxOrNull forces an explicit NULL when the table is empty, so coalesce's fallback actually
-- applies.
CREATE OR REPLACE VIEW sent_transaction_pending_max_trace_ts AS
SELECT maxOrNull(trace_inserted_at) AS max_ts FROM sent_transaction_pending;

--
-- Every minute, incrementally captures new "sent" rows from `jet.txn_trace` (which has no TTL,
-- so this capture step must stay bounded/incremental -- never a full scan) into
-- sent_transaction_pending. Only "sent" state is of interest here, so
-- `state`/`error_msg`/`drop_reason` (only populated for "failed"/"drop" trace rows) are dropped
-- rather than carried forward. Rows with a null `send_at_slot` are excluded from the whole
-- pipeline -- a "sent" trace row should always have one.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_populate_sent_pending
REFRESH EVERY 1 MINUTE
APPEND
TO sent_transaction_pending
AS
SELECT
    tt.signature AS signature,
    tt.send_at_slot AS send_at_slot,
    tt.remote_peer_identity AS remote_peer_identity,
    tt.remote_peer_addr AS remote_peer_addr,
    tt.ts AS trace_inserted_at
FROM txn_trace AS tt
WHERE
    tt.state = 'sent'
    AND tt.send_at_slot IS NOT NULL
    -- Deliberately overlaps with rows already captured in a previous refresh (>=, not >, plus
    -- a 30s grace margin): safe because sent_transaction_pending is a ReplacingMergeTree keyed
    -- on signature, so re-captured rows just collapse into duplicates that merge away. A
    -- precise, non-overlapping boundary would instead risk permanently dropping any row sharing
    -- the exact watermark value with an already-captured row.
    --
    -- Written as `tt.ts + 30s >= watermark` rather than `tt.ts >= watermark - 30s`: the two are
    -- algebraically identical, but the latter constructs a pre-1970 boundary on the cold-start
    -- path (watermark at epoch, before anything's ever been captured). The raw DateTime64
    -- arithmetic for that is fine (`toDateTime64(0,3) - INTERVAL 30 SECOND` is a perfectly valid
    -- `1969-12-31 23:59:30`) -- but jet.txn_trace is PARTITION BY toDate(ts), and partition
    -- pruning against a pre-1970 boundary has to derive a `Date` (unsigned, 1970-onward only)
    -- from it, which can't represent that value. Observed in practice: every partition got
    -- pruned as a result, so the very first refresh always read zero rows, the watermark never
    -- advanced past epoch, and it stayed permanently stuck. Adding the margin to `tt.ts` instead
    -- never constructs a pre-1970 boundary at all, regardless of how "cold" the watermark is.
    AND tt.ts + INTERVAL 30 SECOND >= coalesce(
        (SELECT max_ts FROM sent_transaction_pending_max_trace_ts),
        toDateTime64(0, 3)
    );

--
-- Every minute, joins every currently-pending "sent" transaction against
-- `chain_transaction_staging` (raw on-chain transactions observed via the Fumarole block
-- stream, with no claim yet about whether jet sent them) and appends matches. No upper bound on
-- `lt.slot`: every block reaching `chain_transaction_staging` is already complete (the ingester
-- only flushes a slot once Fumarole signals it ended) and a single INSERT is atomic, so there's
-- no still-filling-block risk to guard against here.
--
-- Deliberately NO lower bound / time-window exclusion on either side of this join: both
-- `sent_transaction_pending` and `chain_transaction_staging` are kept small by their own
-- purges (see purge_sent_transaction_pending.sql and purge_chain_transaction_staging.sql), not
-- by rescanning only "recent" rows here. `chain_transaction_staging`'s own arrival order can't
-- be trusted for windowing anyway -- Fumarole doesn't guarantee slot-ordered delivery, and the
-- sink's concurrent HTTP sends (see http_ndjson_drain.rs) can complete out of order -- so any
-- boundary keyed off its own `slot`/insertion order can silently and permanently drop a late,
-- out-of-order row. Instead, completeness is derived entirely from the send side: see the
-- purge scripts for why that's safe.
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
    tt.trace_inserted_at AS trace_inserted_at
FROM sent_transaction_pending AS tt
ANY INNER JOIN chain_transaction_staging AS lt ON lt.signature = tt.signature
SETTINGS join_use_nulls = 1;
