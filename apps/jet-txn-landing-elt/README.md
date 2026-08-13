# jet-txn-landing-elt

ELT pipeline that confirms which transactions `jet` sent actually landed on-chain, by matching
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
                                                    │ fires the instant a row lands here --
                                                    │ plain MATERIALIZED VIEW, no schedule
                                                    │ WHERE state='sent'
                                                    ▼
                                        ┌────────────────────────┐
                                        │ sent_transaction_pending│  candidates awaiting a match
                                        └───────────┬─────────────┘
                                                    │ read live, by mv_landed_transactions,
                                                    │ filtered to [batch_min_slot-64, batch_max_slot]
                                                    ▼
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
 │  .block_stream()       │                       └───────────┬───────────┘
 └───────────────────────┘                                    │
                                                                │ fires mv_landed_transactions
                                                                │ the instant a batch lands here
                                                                ▼
                                                    ┌────────────────────────┐
                                                    │   landed_transactions   │  final table:
                                                    │  (signature, send_slot, │  jet-sent txs
                                                    │   landed_slot, ...)     │  confirmed on-chain
                                                    └───────────┬─────────────┘
                                                                │
                                          ┌─────────────────────┴─────────────────────┐
                                          │ landed_transaction_slot_latency (snapshot)  │
                                          │ landed_transaction_slot_latency_1m (history,│
                                          │   REFRESH EVERY 1 MINUTE -- the one thing   │
                                          │   in this pipeline still on a schedule)     │
                                          └──────────────────────────────────────────────┘

 External cron, via clickhouse-purge-runner (bin, this crate) -- bounds table SIZE, not
 join cost or correctness (see "Why still purge?" below):
   purge_sent_transaction_pending.sql  -- drops rows once matched into landed_transactions
   purge_chain_transaction_staging.sql -- drops rows once send-side completeness proves they
                                           can never match (see comments in that file)
```

## Why event-driven materialized views instead of scheduled ones

Both `mv_populate_sent_pending` and `mv_landed_transactions` are plain (non-refreshable)
`MATERIALIZED VIEW`s: they fire the instant their source table receives a new row, rather than
polling on a `REFRESH EVERY` schedule. This replaced an earlier design built on
`REFRESH EVERY 1 MINUTE` views plus a self-referential watermark (`sent_transaction_pending_max_trace_ts`)
that computed "how far have we captured" as a live aggregate -- which turned out to have real,
hard-to-predict edge cases (an empty-table aggregate not coalescing the way expected, and a
watermark landing on a pre-1970 date tripping up partition pruning on a `PARTITION BY toDate(ts)`
table). Event-driven views need no watermark at all: `mv_populate_sent_pending` only ever sees
the just-inserted block, and `mv_landed_transactions` derives its matching bound
(`[batch_min_slot - 64, batch_max_slot]`) from that same batch's own slot range rather than any
global "how far has real time progressed" state -- which is also what makes it naturally robust
to `clickhouse-sink.rs` being down for hours: backlog data arriving late carries its own old slot
values, so the bound stays anchored to that same old era, matching wherever the corresponding
`sent_transaction_pending` rows still are (they're never purged for being merely "old").

**Known gap:** `mv_landed_transactions` only fires on `chain_transaction_staging` inserts, not on
`sent_transaction_pending` inserts. A transaction landing fast enough to beat its own sent-side
capture would arrive here before `sent_transaction_pending` has the matching row yet, with no
retry. Add the symmetric direction (trigger on `sent_transaction_pending`, look up
`chain_transaction_staging`) if this race turns out to matter in practice.

**Operational consequence worth knowing:** a plain materialized view runs under the privileges of
whoever performed the triggering insert, not some separate scheduled-refresh identity. Every
writer into a table with an attached MV needs grants covering that MV's *entire* query -- e.g.
the ClickHouse user `clickhouse-sink.rs` connects as needs `SELECT` on `sent_transaction_pending`
(not just `INSERT` on `chain_transaction_staging`), and whatever user `jet` writes
`jet.txn_trace` as needs `INSERT` on `sent_transaction_pending`. A missing grant here surfaces as
the *insert* failing (e.g. an HTTP 500 from the sink), not as some separate view-refresh error --
check `system.query_views_log` (filter by `view_name`) for the real exception if that happens.

## Why still purge, if the join is already efficient?

`purge_chain_transaction_staging.sql` and `purge_sent_transaction_pending.sql` are not there to
help the join run efficiently at whatever size these tables happen to be today -- confirmed via
`EXPLAIN`, the `sent_transaction_pending` side of `mv_landed_transactions`'s join is already
filtered before the hash build, not scanned in full. The purges solve a different, longer-horizon
problem: keeping that size small *forever*. Without them, `sent_transaction_pending` holds
already-matched rows (needed for nothing) until its 1-day TTL catches up, and
`chain_transaction_staging` holds a full day of *every* mainnet transaction rather than just the
recent slice actually relevant to matching. A perfectly efficient query against an ever-growing
table still gets slower as the table grows -- the purges are what keep "today's size" small
indefinitely, not a performance fix for the query itself.

One thing that *did* go away with the redesign: the purges no longer need to run strictly after
some scheduled join, since the join is now synchronous with the insert that triggers it -- there's
no scheduling race left to protect against. (`purge_chain_transaction_staging.sql`'s header
comment still describes the old `SYSTEM WAIT VIEW mv_landed_transactions`-based ordering
requirement from when that view was refreshable -- it no longer applies now that
`mv_landed_transactions` is a plain MV, and `clickhouse-purge-runner` needs a corresponding update
before this is redeployed.)

## Files

- `src/bin/clickhouse-sink.rs` -- subscribes to a Fumarole persistent block stream, flattens
  each block into its landed transactions, and drains them to ClickHouse. Reads its own logging
  format from the config file (`tracing.json: true` for JSON logs; omit for plain text).
- `src/bin/clickhouse-purge-runner.rs` -- runs the two purge scripts below on a loop.
  Predates the switch to event-driven materialized views, when it also had to
  `SYSTEM WAIT VIEW mv_landed_transactions` before every round to enforce join-before-purge
  ordering -- that ordering requirement is gone, but this binary hasn't been updated to drop the
  now-broken wait step yet.
- `src/http_ndjson_drain.rs` -- generic `Stream<Item: IntoIterator<Item: Serialize>>` →
  batched NDJSON-over-HTTP drain, used by `clickhouse-sink.rs`.
- `clickhouse/schema.sql` -- table/view/materialized-view DDL (see [Flow](#flow) above).
- `clickhouse/purge_chain_transaction_staging.sql`, `clickhouse/purge_sent_transaction_pending.sql`
  -- purge SQL, run via `clickhouse-purge-runner` (not executed by ClickHouse itself).
- `clickhouse/drop_all.sql` -- dev/reset teardown: drops every object `schema.sql` creates.
  Destructive (deletes data, not just schema) -- not for use against a real deployment.
