
CREATE TABLE IF NOT EXISTS chain_transaction_staging
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
-- Safety net only: the slot-watermark purge below (see purge_chain_transaction_staging.sql)
-- is the primary drain. This TTL just bounds growth if that purge ever stops running.
TTL ts + INTERVAL 1 DAY;


CREATE TABLE IF NOT EXISTS chain_entry_staging
(
    slot UInt64,
    entry_index UInt64,
    executed_transactions_count UInt64,
    ts DateTime64(3) DEFAULT now64(3),
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (slot, entry_index)
TTL ts + INTERVAL 1 DAY;

--
-- The table `jet` (the sender app, apps/jet) itself writes into directly, over HTTP, via its
-- HttpTxnTraceDrain -- see apps/jet/src/txn_trace_drain.rs. This is the source table the rest
-- of this directory's schema.sql (the jet-txn-landing-elt ELT pipeline) reads from; it doesn't
-- create or write to it itself.
--
CREATE TABLE IF NOT EXISTS txn_trace
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
    signer Nullable(String),
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_x_request_id x_request_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_x_subscription_id x_subscription_id TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX bf_signer signer TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX set_remote_peer remote_peer_identity TYPE set(2048) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, signature)
TTL ts + INTERVAL 180 DAY;


CREATE OR REPLACE VIEW chain_entry_with_txn_count_bounds AS
SELECT
    slot,
    entry_index,
    executed_transactions_count,
    sum(executed_transactions_count) OVER (
        PARTITION BY slot
        ORDER BY entry_index
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS txn_count_cumu_upper_bound,
    sum(executed_transactions_count) OVER (
        PARTITION BY slot
        ORDER BY entry_index
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) - executed_transactions_count AS txn_count_cumu_lower_bound
FROM chain_entry_staging;


CREATE TABLE IF NOT EXISTS landed_transaction_entry_pos (
    signature String,
    slot UInt64,
    entry_index UInt64,
    txn_index UInt64,
    ts DateTime64(3) DEFAULT now64(3),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toDate(ts)
ORDER BY (slot, entry_index, txn_index)
TTL ts + INTERVAL 90 DAY;


CREATE TABLE IF NOT EXISTS landed_transactions
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
    trace_signer Nullable(String),
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


CREATE TABLE IF NOT EXISTS sent_transaction_pending
(
    signature String,
    send_at_slot UInt64,
    remote_peer_identity Nullable(String),
    remote_peer_addr Nullable(String),
    x_request_id Nullable(UUID),
    x_subscription_id Nullable(UUID),
    signer Nullable(String),
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
    tt.x_request_id AS x_request_id,
    tt.x_subscription_id AS x_subscription_id,
    tt.signer AS signer,
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
    lt.txn_index AS txn_index,
    tt.remote_peer_identity AS trace_remote_peer_identity,
    tt.remote_peer_addr AS trace_remote_peer_addr,
    tt.x_request_id AS trace_x_request_id,
    tt.x_subscription_id AS trace_x_subscription_id,
    tt.signer AS trace_signer,
    tt.send_at_slot AS send_at_slot,
    lt.slot AS landed_slot,
    tt.trace_inserted_at AS trace_inserted_at
FROM chain_transaction_staging AS lt
ANY INNER JOIN sent_transaction_pending AS tt ON tt.signature = lt.signature
WHERE tt.send_at_slot BETWEEN batch_min_slot - 64 AND batch_max_slot
SETTINGS join_use_nulls = 1;


-- Fires the instant landed_transactions gets a new row (i.e. whenever mv_landed_transactions posts a batch)
-- Match landed transaction with their on chain position (slot, entry_index, txn_index) in the chain_entry_staging table.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_landed_transaction_entry_pos
TO landed_transaction_entry_pos
AS
SELECT
    lt.signature AS signature,
    lt.landed_slot AS slot,
    es.entry_index AS entry_index,
    lt.txn_index AS txn_index
FROM landed_transactions AS lt
ANY INNER JOIN chain_entry_with_txn_count_bounds AS es
ON lt.landed_slot = es.slot
AND lt.txn_index < es.txn_count_cumu_upper_bound
AND lt.txn_index >= es.txn_count_cumu_lower_bound
SETTINGS join_use_nulls = 1;


-- Per-slot histogram of where transactions land: each slot's entries are bucketed into 256
-- equal-width position bins (bucket 0 = start of slot, 255 = end), giving a fixed-length
-- distribution comparable across slots regardless of how many entries they actually had.
-- Durable beyond chain_entry_staging's own 1-day TTL -- this is the long-lived record of slot
-- shape history.
CREATE TABLE IF NOT EXISTS chain_entry_txn_bucket_distribution
(
    slot UInt64,
    bucket UInt8,
    slot_first_seen_ts DateTime64(3),
    bucket_txn_count UInt64,
    bucket_txn_share Float64,
    computed_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toDate(slot_first_seen_ts)
ORDER BY (slot, bucket)
TTL slot_first_seen_ts + INTERVAL 90 DAY;

-- Can't be a plain event-driven MV: bucket boundaries need each entry's position relative to
-- its slot's FINAL entry count, which isn't known until the slot is complete -- a plain MV only
-- ever sees whatever partial batch of entries was just inserted, not the slot's full history.
--
-- Instead, refreshes every 10 seconds over a rolling 30-second window that stays 2 seconds
-- behind `now()`. Assumes a slot's entries all arrive within ~2s of its first entry (slots are
-- produced roughly every 400ms) -- so any slot fully inside a window ending 2s ago is safe to
-- treat as complete. The 30s width (3x the 10s refresh cadence) gives every slot multiple
-- refresh cycles to be captured before it ages out of the window; the explicit
-- `slot_last_seen_ts - slot_first_seen_ts <= 2` filter is a backstop that drops any slot whose
-- entries genuinely spread wider than assumed, rather than risk silently computing wrong bucket
-- boundaries for it. Dropping the occasional straggling slot is fine -- the goal is a
-- probabilistic distribution over many slots, not an exhaustive per-slot record.
--
-- Overlapping refreshes will recompute the same slot more than once; APPEND into a
-- ReplacingMergeTree(computed_at) target makes that harmless (query with FINAL, or after a
-- merge, to see the deduplicated result).
--
-- Runs as SQL SECURITY DEFINER (whoever creates it), same as mv_landed_transaction_slot_latency_1m
-- above -- unlike the plain MVs elsewhere in this file, it does NOT run under the privileges of
-- whoever inserted into chain_entry_staging, so it needs no additional grants for that user.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chain_entry_txn_bucket_distribution
REFRESH EVERY 10 SECOND
APPEND
TO chain_entry_txn_bucket_distribution
AS
SELECT
    slot,
    bucket,
    min(slot_first_seen_ts) AS slot_first_seen_ts,
    sum(executed_transactions_count) AS bucket_txn_count,
    bucket_txn_count / sum(bucket_txn_count) OVER (PARTITION BY slot) AS bucket_txn_share
FROM
(
    SELECT slot, entry_index, executed_transactions_count, slot_first_seen_ts, bucket
    FROM
    (
        SELECT
            slot,
            entry_index,
            executed_transactions_count,
            ts,
            min(ts) OVER (PARTITION BY slot) AS slot_first_seen_ts,
            max(ts) OVER (PARTITION BY slot) AS slot_last_seen_ts,
            least(255, intDiv(entry_index * 256, count() OVER (PARTITION BY slot))) AS bucket
        FROM chain_entry_staging
        WHERE ts >= now() - INTERVAL 30 SECOND
          AND ts < now() - INTERVAL 2 SECOND
    )
    WHERE (slot_last_seen_ts - slot_first_seen_ts) <= 2
)
GROUP BY slot, bucket;


-- One row per (minute, bucket, density_bin): how many slots observed a given relative density
-- of transactions in that bucket, where relative density = bucket_txn_share * 256 (1.0 = exactly
-- what uniform spread across 256 buckets would predict, 2.0 = double, etc). A plain average per
-- (minute, bucket) would collapse each bucket's whole distribution to one number and lose
-- spread/shape; this keeps enough resolution to plot a heatmap (bucket on one axis, density_bin
-- on the other, count as intensity) and see where observations actually concentrate per bucket,
-- not just their mean.
--
-- density_bin is relative density bucketed in steps of 0.1, clamped to [0, 50] -- so density_bin
-- * 0.1 is a bin's lower edge, and bin 50 is a "5.0x and above" overflow catch-all. Median
-- relative density across the table today is ~1.0 (as expected) with p99 ~1.85 and a max of 256
-- (a slot with its entire volume in one bucket) -- the 0.1-wide/5.0-cap scheme keeps resolution
-- where the real variation lives without rare extreme outliers blowing out the bin range.
CREATE TABLE IF NOT EXISTS chain_entry_txn_bucket_density_histogram_1m
(
    period_start DateTime,
    bucket UInt8,
    density_bin UInt8,
    n_observations UInt64
)
ENGINE = MergeTree
PARTITION BY toDate(period_start)
ORDER BY (period_start, bucket, density_bin)
TTL period_start + INTERVAL 180 DAY;

-- Same timing/staleness reasoning as mv_chain_entry_txn_bucket_distribution_1m above (2-minute
-- buffer, plain MergeTree, approximate at the edges).
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chain_entry_txn_bucket_density_histogram_1m
REFRESH EVERY 1 MINUTE
APPEND
TO chain_entry_txn_bucket_density_histogram_1m
AS
SELECT
    toStartOfMinute(now()) - INTERVAL 2 MINUTE AS period_start,
    bucket,
    least(50, toUInt8(floor(bucket_txn_share * 256 * 10))) AS density_bin,
    count() AS n_observations
FROM chain_entry_txn_bucket_distribution FINAL
WHERE slot_first_seen_ts >= toStartOfMinute(now()) - INTERVAL 2 MINUTE
  AND slot_first_seen_ts <  toStartOfMinute(now()) - INTERVAL 1 MINUTE
GROUP BY bucket, density_bin;