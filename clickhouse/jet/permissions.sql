
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

-- `jet` (the sender app itself) posts transaction trace rows directly into txn_trace over HTTP.
create role if not exists 'jet';

grant insert on jet.txn_trace to 'jet';
-- Needed even though `jet` never reads txn_trace itself: mv_populate_sent_pending fires the
-- instant this insert lands, and a plain MV runs under the privileges of whoever performed the
-- triggering insert -- here, 'jet'. Its query does `FROM txn_trace`, so 'jet' needs SELECT on
-- it too (same reasoning as the jet-clickhouse-sink grants above).
grant select on jet.txn_trace to 'jet';
-- mv_populate_sent_pending's target table, written under 'jet's own privileges as part of the
-- same chained insert. No SELECT needed here: nothing currently fires on
-- sent_transaction_pending inserts (see the "Known gap" note on mv_landed_transactions in
-- schema.sql -- it only triggers off chain_transaction_staging inserts, which come from
-- 'jet-clickhouse-sink', not 'jet').
grant insert on jet.sent_transaction_pending to 'jet';