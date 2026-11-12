#![no_main]
//! Feeds each input to the transaction parsers behind `/api/v1/transactions` and the tip check.
//! See `fuzz/README.md` for the run command.

use {
    bytes::Bytes,
    futures::executor::block_on,
    libfuzzer_sys::fuzz_target,
    solana_rpc_client_api::config::RpcSendTransactionConfig,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::UiTransactionEncoding,
    std::sync::{LazyLock, Mutex},
    tokio::sync::mpsc,
    yellowstone_jet::{
        payload::JetRpcSendTransactionConfig, tip::TipAccounts,
        transaction_handler::TransactionHandler, transactions::SendTransactionRequest,
    },
};

struct Harness {
    handler: TransactionHandler,
    sink: mpsc::Receiver<SendTransactionRequest>,
    tips: TipAccounts,
}

static HARNESS: LazyLock<Mutex<Harness>> = LazyLock::new(|| {
    // One input makes at most three sends, so a capacity of 4 never blocks.
    let (sender, sink) = mpsc::channel(4);
    Mutex::new(Harness {
        handler: TransactionHandler::new(sender, true),
        sink,
        // The System Program address is all zeros, so the fuzzer finds a matching tip quickly.
        tips: TipAccounts::parse("11111111111111111111111111111111").expect("valid address"),
    })
});

fn config(encoding: Option<UiTransactionEncoding>) -> JetRpcSendTransactionConfig {
    JetRpcSendTransactionConfig {
        config: RpcSendTransactionConfig {
            skip_preflight: true,
            encoding,
            ..Default::default()
        },
        forwarding_policies: vec![],
    }
}

fuzz_target!(|data: &[u8]| {
    let mut harness = HARNESS.lock().expect("harness lock");
    let Harness {
        handler,
        sink,
        tips,
    } = &mut *harness;

    // `find_tip` must accept a transaction that is not sanitized.
    if let Ok(transaction) = wincode::deserialize::<VersionedTransaction>(data) {
        tips.find_tip(&transaction, 0);
    }

    let raw = Bytes::copy_from_slice(data);
    let _ = block_on(handler.handle_raw_transaction(raw, config(None), None, None));

    if let Ok(text) = std::str::from_utf8(data) {
        for encoding in [UiTransactionEncoding::Base58, UiTransactionEncoding::Base64] {
            let config = Some(config(Some(encoding)));
            let _ = block_on(handler.handle_transaction(text.to_owned(), config, None, None));
        }
    }

    while sink.try_recv().is_ok() {}
});
