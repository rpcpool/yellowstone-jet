
create role if not exists 'jet-clickhouse-sink';

grant select, insert on jet.chain_transaction_staging to 'jet-clickhouse-sink';
grant select, insert on jet.sent_transaction_pending to 'jet-clickhouse-sink';
grant select on jet.txn_trace to 'jet-clickhouse-sink';
grant select, insert on jet.chain_entry_staging to 'jet-clickhouse-sink';
-- Needed even though landed_transactions/landed_transaction_entry_pos are only ever written by
-- chained materialized views (mv_landed_transactions, mv_landed_transaction_entry_pos), never
-- directly by the sink: a plain MV runs under the privileges of whoever performed the
-- triggering insert (see the README's "Operational consequence" note), and its SELECT/INSERT
-- grants must cover every table its query touches -- including a table that's simultaneously
-- the MV's own trigger, like landed_transactions is here.
grant select, insert on jet.landed_transactions to 'jet-clickhouse-sink';
grant select, insert on jet.landed_transaction_entry_pos to 'jet-clickhouse-sink';
-- A plain VIEW needs its own SELECT grant too -- ClickHouse checks it on the view object itself,
-- not just on the underlying table(s) the view's query reads from.
grant select on jet.chain_entry_with_txn_count_bounds to 'jet-clickhouse-sink';