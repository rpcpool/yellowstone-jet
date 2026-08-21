
grant select, insert on jet.chain_transaction_staging to 'jet-clickhouse-sink';
grant select, insert on jet.sent_transaction_pending to 'jet-clickhouse-sink';
grant select, insert on jet.txn_trace to 'jet-clickhouse-sink';
grant select, insert on jet.chain_entry_staging to 'jet-clickhouse-sink';