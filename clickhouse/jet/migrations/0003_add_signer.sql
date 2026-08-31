--
-- Migration: add the transaction `signer` (fee payer) to the txn_trace pipeline.
--
-- Idempotent -- every statement here can be re-run any number of times, against a cluster at
-- any prior migration state (including a brand new one created straight from schema.sql, which
-- already has `signer`/`trace_signer` baked in):
--   - `ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS` no-op if already applied.
--   - `MATERIALIZE INDEX` is safe to repeat; it just re-skips parts that already have the index.
--   - `MODIFY QUERY` unconditionally overwrites the view's query with the (identical) new one,
--     so re-running it is a no-op in effect even though it isn't literally conditional.
--
-- Order matters: columns are added to every table a view reads from or writes into *before*
-- that view's query is repointed to reference them.
--
-- Adding a `Nullable(String)` column is a metadata-only change -- no rewrite of existing parts,
-- existing rows just read back as NULL for it. Only rows inserted after the `MODIFY QUERY`
-- steps below will have `signer`/`trace_signer` populated; historical rows stay NULL (no
-- backfill is attempted here).
--

ALTER TABLE jet.txn_trace
    ADD COLUMN IF NOT EXISTS signer Nullable(String) AFTER drain_id;

ALTER TABLE jet.txn_trace
    ADD INDEX IF NOT EXISTS bf_signer signer TYPE bloom_filter(0.01) GRANULARITY 64;

ALTER TABLE jet.txn_trace MATERIALIZE INDEX bf_signer;

ALTER TABLE jet.sent_transaction_pending
    ADD COLUMN IF NOT EXISTS signer Nullable(String) AFTER x_subscription_id;

ALTER TABLE jet.landed_transactions
    ADD COLUMN IF NOT EXISTS trace_signer Nullable(String) AFTER trace_x_subscription_id;

ALTER TABLE jet.mv_populate_sent_pending MODIFY QUERY
SELECT
    tt.signature AS signature,
    tt.send_at_slot AS send_at_slot,
    tt.remote_peer_identity AS remote_peer_identity,
    tt.remote_peer_addr AS remote_peer_addr,
    tt.x_request_id AS x_request_id,
    tt.x_subscription_id AS x_subscription_id,
    tt.signer AS signer,
    tt.ts AS trace_inserted_at
FROM jet.txn_trace AS tt
WHERE
    tt.state = 'sent'
    AND tt.send_at_slot IS NOT NULL;

ALTER TABLE jet.mv_landed_transactions MODIFY QUERY
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
    tt.signer AS trace_signer,
    tt.send_at_slot AS send_at_slot,
    lt.slot AS landed_slot,
    tt.trace_inserted_at AS trace_inserted_at
FROM jet.chain_transaction_staging AS lt
ANY INNER JOIN jet.sent_transaction_pending AS tt ON tt.signature = lt.signature
WHERE tt.send_at_slot BETWEEN batch_min_slot - 64 AND batch_max_slot
SETTINGS join_use_nulls = 1;
