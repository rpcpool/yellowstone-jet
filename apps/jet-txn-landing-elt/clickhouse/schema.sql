
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
-- latest `ts`. Not load-bearing for mv_landed_transactions' correctness (each chain row is
-- only ever considered once, on insert), but harmless insurance against ever recapturing the
-- same signature twice (e.g. a future symmetric MV -- see the note on mv_landed_transactions).
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

CREATE TABLE IF NOT EXISTS txn_trace_success_rate_1m
(
    window_start DateTime64(3),
    n UInt64,
    n_sent UInt64,
    n_failed UInt64,
    n_drop UInt64,
    success_rate Float64
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY window_start
-- Same rollup-table rationale as landed_transaction_slot_latency_1m: one row per
-- 1-minute window instead of per trace row, so this stays small regardless of traffic.
TTL window_start + INTERVAL 180 DAY;

--
-- One-time backfill for txn_trace_success_rate_1m -- same rationale as
-- landed_transaction_slot_latency_1m's REFRESH EVERY caveat: a materialized view like
-- this only ever computes windows from its creation forward, so this fills in every
-- 1-minute window that already has data in txn_trace as of whenever this migration is
-- applied.
--
-- Run this once, and BEFORE creating the materialized view below: it excludes the
-- still-in-progress current minute (`ts < toStartOfMinute(now())`), leaving that
-- boundary window for the view's own first scheduled tick to capture. Creating the
-- view first would instead race this backfill's `now()` against the view's first
-- refresh over that same boundary minute, risking a duplicated row -- this table is a
-- plain MergeTree, not deduplicating, so a duplicate here doesn't get merged away on
-- its own.
--
INSERT INTO txn_trace_success_rate_1m
SELECT
    toStartOfMinute(ts) AS window_start,
    count() AS n,
    countIf(state = 'sent') AS n_sent,
    countIf(state = 'failed') AS n_failed,
    countIf(state = 'drop') AS n_drop,
    countIf(state = 'sent') / count() AS success_rate
FROM txn_trace
WHERE ts < toStartOfMinute(now())
GROUP BY window_start
ORDER BY window_start;

--
-- Every minute, summarizes the *previous* (just-completed) 1-minute window of
-- txn_trace into one row -- same window logic as mv_landed_transaction_slot_latency_1m
-- above. No FINAL needed here (unlike the landed_transactions-based views above):
-- txn_trace is a plain (non-Replacing) MergeTree -- each row is one jet-side send
-- decision/attempt, never revised in place, so there's nothing to deduplicate.
--
-- "Success" here is jet's own send outcome (state = 'sent': jet forwarded the
-- transaction onward) -- a different question from on-chain execution success, which
-- is what landed_transaction_success_rate_1m-style aggregates would cover instead (see
-- mv_landed_transaction_slot_latency_1m's FROM clause for that data source). `failed`
-- (jet tried to send and the send itself failed) and `drop` (jet decided not to send
-- it at all -- see mv_populate_sent_pending's notes on drop_reason) are kept as
-- separate counts rather than collapsed into one "not ok" bucket, since they're
-- different failure modes worth telling apart on a dashboard.
--
-- success_rate is `nan` for any window with zero trace rows (n = 0) -- ClickHouse's
-- `/` is float division, so 0/0 is `nan` rather than throwing.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_txn_trace_success_rate_1m
REFRESH EVERY 1 MINUTE
APPEND
TO txn_trace_success_rate_1m
AS
SELECT
    toStartOfMinute(now()) - INTERVAL 1 MINUTE AS window_start,
    count() AS n,
    countIf(state = 'sent') AS n_sent,
    countIf(state = 'failed') AS n_failed,
    countIf(state = 'drop') AS n_drop,
    countIf(state = 'sent') / count() AS success_rate
FROM txn_trace
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
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64,
    -- Needed for mv_landed_transactions' `send_at_slot BETWEEN ...` filter below to actually
    -- prune granules instead of scanning the whole table on every chain insert.
    INDEX mm_send_at_slot send_at_slot TYPE minmax GRANULARITY 4
)
-- Dedups by signature during background merges, keeping the row with the latest
-- trace_inserted_at. Mirrors landed_transactions' own rationale: safe to recapture the same
-- row more than once without unbounded duplication.
ENGINE = ReplacingMergeTree(trace_inserted_at)
PARTITION BY toDate(trace_inserted_at)
ORDER BY signature
-- Safety net only, same philosophy as chain_transaction_staging's own TTL: the purge script
-- (purge_sent_transaction_pending.sql) is the primary drain, removing matched rows. This just
-- bounds growth for the rare "sent but genuinely never landed" tail, which has no active
-- expiry -- see mv_landed_transactions below for why.
TTL trace_inserted_at + INTERVAL 1 DAY;

--
-- Fires the instant jet.txn_trace gets a new row (event-driven, not on a schedule) and copies
-- "sent" rows into sent_transaction_pending. Plain (non-refreshable) MATERIALIZED VIEW: no
-- REFRESH EVERY, no APPEND (that modifier only exists for refreshable views -- a plain MV
-- always inserts into its target, there's no "replace" mode to opt out of), and critically no
-- watermark at all -- it only ever sees the just-inserted block, substituted in place of
-- jet.txn_trace, so there's no scan window to compute and no cold-start/epoch edge case to
-- worry about (the whole class of bug the REFRESH EVERY version of this hit).
--
-- Only "sent" state is of interest here, so `state`/`error_msg`/`drop_reason` (only populated
-- for "failed"/"drop" trace rows) are dropped rather than carried forward. Rows with a null
-- `send_at_slot` are excluded from the whole pipeline -- a "sent" trace row should always have
-- one.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_populate_sent_pending
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
    AND tt.send_at_slot IS NOT NULL;

--
-- Fires the instant chain_transaction_staging gets a new row (i.e. whenever clickhouse-sink.rs
-- posts a batch) and tries to match just that batch against sent_transaction_pending. Plain
-- MATERIALIZED VIEW, same reasoning as mv_populate_sent_pending above: no schedule, no
-- watermark table, no purge-ordering dance -- the join happens synchronously as part of the
-- insert itself, so by the time any purge could possibly run, this has already had its one
-- shot at matching.
--
-- The `send_at_slot BETWEEN ...` filter is a performance bound, not a correctness one -- the
-- join is by exact signature, which is correct on its own regardless of slot distance. It
-- exists purely so this doesn't need to hash the entire sent_transaction_pending table on every
-- single insert:
--   - Lower bound: batch_min_slot - 64. `send_at_slot` values arrive with a bounded local
--     jitter of about 64 slots (same operational constant as purge_chain_transaction_staging.sql
--     -- keep the two in sync). Nothing with send_at_slot below this could plausibly correspond
--     to anything landing in this batch.
--   - Upper bound: batch_max_slot, with NO margin subtracted. A transaction can never be sent
--     after it lands (send_at_slot <= landed_slot, always), so this is a hard, exact bound, not
--     an approximation -- subtracting a margin here would wrongly exclude fast-landing
--     transactions sent just a few slots before the batch's highest slot.
--
-- Both bounds are derived from THIS batch's own slot range, not a global "how far has real
-- time progressed" watermark -- which is what makes this naturally robust to
-- clickhouse-sink.rs being down for hours: backlog data arriving late carries its own old,
-- small slot values, so the bound it computes stays anchored to that same old era, matching
-- wherever the corresponding sent_transaction_pending rows still are (they're never purged for
-- being merely "old" -- see purge_sent_transaction_pending.sql).
--
-- Known gap: this only fires on chain_transaction_staging inserts, not on
-- sent_transaction_pending inserts. A transaction that lands fast enough to beat its own
-- sent-side capture (now near-instant, since mv_populate_sent_pending is event-driven too, but
-- not provably zero-latency) would arrive here before sent_transaction_pending has the
-- matching row yet, and there's no retry -- this MV only ever gets one shot per chain row. The
-- symmetric MV (trigger on sent_transaction_pending, look up chain_transaction_staging) would
-- close that gap; add it if this race turns out to matter in practice.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_landed_transactions
TO landed_transactions
AS
WITH
    (SELECT min(slot) FROM chain_transaction_staging) AS batch_min_slot,
    (SELECT max(slot) FROM chain_transaction_staging) AS batch_max_slot
SELECT
    lt.signature AS signature,
    lt.failed AS failed,
    tt.remote_peer_identity AS trace_remote_peer_identity,
    tt.remote_peer_addr AS trace_remote_peer_addr,
    tt.send_at_slot AS send_at_slot,
    lt.slot AS landed_slot,
    tt.trace_inserted_at AS trace_inserted_at
FROM chain_transaction_staging AS lt
ANY INNER JOIN sent_transaction_pending AS tt ON tt.signature = lt.signature
WHERE tt.send_at_slot BETWEEN batch_min_slot - 64 AND batch_max_slot
SETTINGS join_use_nulls = 1;
