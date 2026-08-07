# jet-txn-landing-elt

ELT pipeline that confirms which transactions `jet` sent actually landed on-chain, by joining
`jet`'s own send-trace against raw on-chain transaction data observed via a Fumarole block
stream.

## Flow

```
 ┌────────────┐   HTTP POST NDJSON    ┌──────────────────────┐
 │    jet     │ ─────────────────────▶│   jet.txn_trace       │  (external table, no TTL,
 │ (sends txs)│  state=sent/failed/drop│   ts, signature,      │   written by jet itself --
 └────────────┘                        │   send_at_slot, ...   │   see docker/clickhouse/init)
                                        └──────────┬────────────┘
                                                    │ mv_populate_sent_pending
                                                    │ (REFRESH EVERY 1 MINUTE)
                                                    │ WHERE state='sent', incremental by ts
                                                    ▼
                                        ┌────────────────────────┐
                                        │ sent_transaction_pending│  candidates awaiting a match
                                        └───────────┬─────────────┘
                                                    │
                          ┌─────────────────────────┴─────────────────────────┐
                          │              mv_landed_transactions                │
                          │        (REFRESH EVERY 1 MINUTE, ANY INNER JOIN     │
                          │         ... ON signature, no time/slot window)     │
                          └─────────────────────────┬─────────────────────────┘
                                                    ▲
                                                    │
 ┌───────────────────┐   Fumarole block stream      │
 │ Solana validators  │ ────────────────────────────┤
 │  (rooted blocks)   │                              │
 └───────────────────┘                              │
           │                                          │
           ▼                                          │
 ┌───────────────────────┐   HTTP POST NDJSON  ┌───────────────────────┐
 │ clickhouse-sink.rs     │ ───────────────────▶│ chain_transaction_    │
 │ (bin, this crate)      │  (http_ndjson_drain) │ staging                │  ALL rooted txs,
 │ FumaroleClient         │                       │ (slot, signature, ...)│  not just jet's
 │  .block_stream()       │                       └───────────────────────┘
 └───────────────────────┘                                    │
                                                                │ (joined above)
                                                                ▼
                                                    ┌────────────────────────┐
                                                    │   landed_transactions   │  final table:
                                                    │  (signature, send_slot, │  jet-sent txs
                                                    │   landed_slot, ...)     │  confirmed on-chain
                                                    └────────────────────────┘

 clickhouse-purge-runner (bin, this crate): loops `SYSTEM WAIT VIEW mv_landed_transactions`
 then runs, in order:
   purge_sent_transaction_pending.sql  -- drops rows once matched into landed_transactions
   purge_chain_transaction_staging.sql -- drops rows once send-side completeness proves they
                                           can never match (see comments in that file)
```

## Why a pending table instead of a direct join

`jet.txn_trace` has no TTL, so `mv_landed_transactions` can't scan it directly every refresh --
`mv_populate_sent_pending` incrementally copies just the new `state='sent'` rows into
`sent_transaction_pending`, a small table that only holds transactions still awaiting a match.
`mv_landed_transactions` then joins that small pending set against `chain_transaction_staging`
with **no time or slot window on either side** -- both tables are kept small by their own purge
scripts instead, so a transaction stays a match candidate for as long as it takes (e.g. across a
`clickhouse-sink.rs` crash-and-restart), rather than being silently dropped after a fixed
rolling window. See the comments in `clickhouse/schema.sql` and the two `purge_*.sql` scripts
for the full reasoning, including why `chain_transaction_staging`'s own arrival order can't be
trusted for windowing (Fumarole doesn't guarantee slot-ordered delivery, and this crate's
concurrent HTTP sends in `http_ndjson_drain.rs` can complete out of order).

## Files

- `src/bin/clickhouse-sink.rs` -- subscribes to a Fumarole persistent block stream, flattens
  each block into its landed transactions, and drains them to ClickHouse.
- `src/bin/clickhouse-purge-runner.rs` -- waits for `mv_landed_transactions`'s refresh cycle to
  complete (`SYSTEM WAIT VIEW`), then runs the purge scripts below, in a loop. This is what
  makes the "purge must run after that cycle's refresh" requirement (see the purge scripts'
  own comments) an enforced guarantee rather than a scheduling hope.
- `src/http_ndjson_drain.rs` -- generic `Stream<Item: IntoIterator<Item: Serialize>>` →
  batched NDJSON-over-HTTP drain, used by `clickhouse-sink.rs`.
- `clickhouse/schema.sql` -- table/view/materialized-view DDL (see [Flow](#flow) above).
- `clickhouse/purge_chain_transaction_staging.sql`, `clickhouse/purge_sent_transaction_pending.sql`
  -- purge SQL, run via `clickhouse-purge-runner` (not executed by ClickHouse itself).
