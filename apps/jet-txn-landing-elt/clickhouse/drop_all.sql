--
-- Drops every object created by schema.sql, in dependency-safe order (materialized views and
-- plain views before the tables they read from or write to).
--
-- DESTRUCTIVE: this deletes the underlying data, not just the schema -- there's no undo. For
-- local/dev iteration (e.g. after changing schema.sql and wanting a clean slate), not intended
-- to be run against a real deployment.
--
-- Usage:
--   clickhouse-client --connection <name> --database jet --multiquery < drop_all.sql
--

DROP VIEW IF EXISTS mv_landed_transactions;
DROP VIEW IF EXISTS mv_populate_sent_pending;
DROP TABLE IF EXISTS sent_transaction_pending;

DROP VIEW IF EXISTS mv_landed_transaction_slot_latency_1m;
DROP TABLE IF EXISTS landed_transaction_slot_latency_1m;

-- Reads from txn_trace, but doesn't write to or depend on any table below -- txn_trace
-- itself isn't created by this file (see docker/clickhouse/init/01-schema.sql), so
-- there's nothing here to drop it in order relative to.
DROP VIEW IF EXISTS mv_txn_trace_success_rate_1m;
DROP TABLE IF EXISTS txn_trace_success_rate_1m;

DROP VIEW IF EXISTS landed_transaction_slot_latency;

DROP TABLE IF EXISTS landed_transactions;
DROP TABLE IF EXISTS chain_transaction_staging;
