--
-- Drains `chain_transaction_staging` of rows already covered by the enrichment refresh in
-- schema.sql (mv_landed_transactions): once a slot has been scanned for matching txn_trace
-- rows, chain_transaction_staging rows at or below that slot are safe to remove.
--
-- This is a mutation, not a lightweight delete -- it rewrites whole parts containing matching
-- rows. Run it on the same cadence as the refresh (e.g. once a minute), not more often.
--
-- Not run by ClickHouse itself: schedule this externally (systemd timer / k8s CronJob /
-- crontab) invoking, e.g.:
--   clickhouse-client --connection <name> --multiquery < purge_chain_transaction_staging.sql
--
ALTER TABLE chain_transaction_staging
DELETE WHERE slot < (SELECT max_slot FROM landed_transaction_max_slot);
