--
-- Drains `sent_transaction_pending` of rows that have been confirmed matched into
-- `landed_transactions`. No slot-based expiry here for the rare "sent but genuinely never
-- landed" tail -- that would require knowing how far chain_transaction_staging's ingestion
-- has genuinely progressed, which we can't trust (see purge_chain_transaction_staging.sql).
-- That tail ages out via sent_transaction_pending's own 1-day TTL instead (schema.sql).
--
-- This is a mutation, not a lightweight delete -- it rewrites whole parts containing matching
-- rows.
--
-- Not run by ClickHouse itself: run alongside purge_chain_transaction_staging.sql via
-- clickhouse-purge-runner (src/bin/clickhouse-purge-runner.rs, `purge_scripts` config), or
-- invoke it directly for a one-off run, e.g.:
--   clickhouse-client --connection <name> --multiquery < purge_sent_transaction_pending.sql
--
ALTER TABLE sent_transaction_pending
DELETE WHERE signature IN (
    -- Bounded lookback on landed_transactions' own `ts` (partition-pruned by its own
    -- PARTITION BY toDate(ts)) instead of scanning a full day -- safe because this purge runs
    -- every minute, far more often than this 10-minute window.
    SELECT signature FROM landed_transactions WHERE ts >= now() - INTERVAL 10 MINUTE
);
