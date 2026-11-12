# Fuzzing

The `sendtx` target feeds each input to the transaction parsers behind `/api/v1/transactions` and the `/api/v1/send-via-tip` tip check:

- `TransactionHandler::handle_raw_transaction`: the `application/octet-stream` body. It checks the size, decodes with `wincode` and calls `sanitize()`.
- `TransactionHandler::handle_transaction` with base58 and base64: the text body.
- `TipAccounts::find_tip` on every transaction that `wincode` decodes, also when `sanitize()` rejects it.

## Run

Install [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) and a nightly toolchain, then run from the repository root:

```bash
cargo +nightly fuzz run sendtx -- -max_len=6618
```

`6618` is `MAX_BODY_SIZE` in `apps/jet/src/http_tx_handler.rs`: the largest base58 transaction (5594 bytes) plus 1 KiB for whitespace. The libFuzzer default of 4096 bytes is too short.

A crash writes the input to `fuzz/artifacts/sendtx/`. Run that input again with `cargo +nightly fuzz run sendtx <file>`.
