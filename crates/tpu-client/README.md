# yellowstone-jet-tpu-client

This library is a standalone port of the TPU-QUIC client code ported from [Yellowstone Jet](https://github.com/rpcpool/yellowstone-jet).

## Smart client

When you enable `yellowstone-grpc` feature-flag, you will have access to a the _smart_ `YellowstoneTpuSender` which handles:

- Connection lifecycle: connect/disconnect
- Slot tracking
- Leader tracking and upcoming schedules
- Connection evictions
- Blocklist
- Certificates management
- Retries
- QUIC Stream handling
- Connection prediction
- Remote peer transaction queues.

It implements the state-of-the-art approach to send transaction, that includes:

- No transaction fragmentation
- By pass most `quinn` lock-contention issues using a dedicated event-loop and endpoint per remote peer.

## Usage

```rust
use {
    clap::Parser,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_keypair::Keypair,
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        env,
        io::{self, IsTerminal as _},
        path::PathBuf,
        sync::Arc,
        vec,
    },
    tracing::level_filters::LevelFilter,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt},
    yellowstone_jet_tpu_client::{
        core::{TpuSenderResponse, TpuSenderTxnInfo},
        yellowstone_grpc::sender::{
            Endpoints, NewYellowstoneTpuSender, create_yellowstone_tpu_sender,
        },
    },
};

pub fn setup_tracing() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let subscriber = tracing_subscriber::registry().with(env_filter);
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_ansi(is_atty);
    subscriber.with(io_layer).try_init().expect("try_init");
}

#[derive(clap::Parser, Debug)]
struct Args {
    #[clap(long, short)]
    /// Path to .env file to load
    dotenv: Option<PathBuf>,
    /// Endpoint to Yellowstone gRPC service
    #[clap(long, short)]
    rpc: Option<String>,
    #[clap(long, short)]
    grpc: Option<String>,
    /// X-Token for Yellowstone gRPC service
    x_token: Option<String>,
    ///
    /// Path to identity keypair file
    ///
    identity: Option<PathBuf>,
    ///
    /// Recipient pubkey
    ///
    #[clap(long)]
    recipient: Option<String>,
}

#[tokio::main]
async fn main() {
    use std::io::Write;
    setup_tracing();

    let mut out = std::io::stdout();

    let args = Args::parse();
    if let Ok(env_path) = args.dotenv.unwrap_or("./.env".into()).canonicalize() {
        if dotenvy::from_path(env_path).is_err() {
            tracing::warn!("Failed to load .env file");
        }
    } else {
        tracing::warn!("Failed to canonicalize .env file path");
    }

    let recipient_pubkey: Pubkey = match args.recipient {
        Some(recipient) => recipient.parse().expect("Failed to parse recipient pubkey"),
        None => Pubkey::new_unique(),
    };

    let rpc_endpoint = match args.rpc {
        Some(endpoint) => endpoint,
        None => env::var("RPC_ENDPOINT")
            .expect("RPC_ENDPOINT must be set in dotenv file or environment"),
    };

    let grpc_endpoint = match args.grpc {
        Some(endpoint) => endpoint,
        None => env::var("GRPC_ENDPOINT")
            .expect("GRPC_ENDPOINT must be set in dotenv file or environment"),
    };

    let grpc_x_token = match args.x_token {
        Some(x_token) => Some(x_token),
        None => match env::var("GRPC_X_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => {
                tracing::warn!("GRPC_X_TOKEN not set in dotenv file or environment");
                None
            }
        },
    };

    let identity = match args.identity {
        Some(path) => {
            solana_keypair::read_keypair_file(path).expect("Failed to read identity keypair file")
        }
        None => {
            if let Ok(identity_path) = env::var("IDENTITY") {
                solana_keypair::read_keypair_file(identity_path)
                    .expect("Failed to read identity keypair file from ENV")
            } else {
                tracing::warn!(
                    "IDENTITY not set in dotenv file or environment, using new random identity"
                );
                Keypair::new()
            }
        }
    };

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_endpoint.clone(),
        CommitmentConfig::confirmed(),
    ));

    writeln!(out, "Using identity: {}", identity.pubkey()).expect("writeln");
    let endpoints = Endpoints {
        rpc: rpc_endpoint,
        grpc: grpc_endpoint,
        grpc_x_token,
    };
    let NewYellowstoneTpuSender {
        mut sender,
        related_objects_jh: _,
        mut response,
    } = create_yellowstone_tpu_sender(Default::default(), identity.insecure_clone(), endpoints)
        .await
        .expect("tpu-sender");

    const LAMPORTS: u64 = 1000;
    let instructions = vec![transfer(&identity.pubkey(), &recipient_pubkey, LAMPORTS)];

    let latest_blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("get_latest_blockhash");

    let transaction = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&identity.pubkey(), &instructions, &[], latest_blockhash)
                .expect("try_compile"),
        ),
        &[&identity],
    )
    .expect("try_new");
    let signature = transaction.signatures[0];
    tracing::info!("generate transaction {signature} with send lamports {LAMPORTS}");
    let bincoded_txn = bincode::serialize(&transaction).expect("bincode::serialize");
    let txn_info = TpuSenderTxnInfo::new(signature);

    // Send the transaction to the current leader
    sender
        .send_txn(bincoded_txn, Some(txn_info))
        .await
        .expect("send_transaction");

    let TpuSenderResponse::TxSent(resp) = response.recv().await.expect("response") else {
        panic!("unexpected response");
    };

    let actual_sig = resp
        .info
        .as_ref()
        .and_then(|info| info.downcast_ref::<Signature>())
        .copied()
        .expect("response contains Signature metadata");
    assert!(actual_sig == signature, "unexpected tx signature in response");

    writeln!(
        &mut out,
        "sent transaction with signature `{}` to validator `{}`",
        actual_sig, resp.remote_peer_identity
    )
    .expect("writeln");
}
```

## `TpuSenderTxnInfo` detailed usage

`TpuSenderTxnInfo` is an optional typed metadata envelope attached to each `TpuSenderTxn`.
`TpuSenderResponse` carries this metadata back in `TxSent`, `TxFailed`, and `TxDrop` responses.

Key points:

- The value must be `Copy + Sized + 'static`.
- The encoded value must fit in `TXN_INFO_CAP` bytes.
- You recover the original type using `downcast_ref::<T>()`.

```rust
use {
    solana_signature::Signature,
    yellowstone_jet_tpu_client::core::{TpuSenderResponse, TpuSenderTxnInfo, TXN_INFO_CAP},
};

#[derive(Clone, Copy, Debug)]
struct TxMeta {
    sig: Signature,
    attempt: u16,
}

fn read_meta(info: &Option<TpuSenderTxnInfo>) -> Option<TxMeta> {
    info.as_ref().and_then(|i| i.downcast_ref::<TxMeta>()).copied()
}

async fn send_with_metadata(
    sender: &mut yellowstone_jet_tpu_client::yellowstone_grpc::sender::YellowstoneTpuSender,
    wire: Vec<u8>,
    sig: Signature,
) {
    assert!(std::mem::size_of::<TxMeta>() <= TXN_INFO_CAP);
    let meta = TxMeta { sig, attempt: 1 };

    sender
        .send_txn(wire, Some(TpuSenderTxnInfo::new(meta)))
        .await
        .expect("send_txn");
}

fn handle_response(resp: TpuSenderResponse) {
    match resp {
        TpuSenderResponse::TxSent(ok) => {
            if let Some(meta) = read_meta(&ok.info) {
                println!("sent {} attempt {}", meta.sig, meta.attempt);
            }
        }
        TpuSenderResponse::TxFailed(err) => {
            if let Some(meta) = read_meta(&err.info) {
                println!("failed {}: {}", meta.sig, err.failure_reason);
            }
        }
        TpuSenderResponse::TxDrop(drop) => {
            for (txn, _attempt) in drop.dropped_tx_vec {
                if let Some(meta) = read_meta(&txn.info) {
                    println!("dropped {}", meta.sig);
                }
            }
        }
    }
}
```

In simple flows, storing only `Signature` is enough:

```rust
let txn_info = TpuSenderTxnInfo::new(signature);
sender.send_txn(wire_txn, Some(txn_info)).await?;
```

## Compile-time metadata capacity (`TXN_INFO_CAP`)

`TpuSenderTxnInfo` storage capacity is a compile-time constant generated by `build.rs`.

- Env var: `TXN_INFO_CAP`
- Default: `64` bytes
- Validation: must be greater than `0`

Set it for a one-off build:

```sh
TXN_INFO_CAP=128 cargo build -p yellowstone-jet-tpu-client
```

Run tests with a custom capacity:

```sh
TXN_INFO_CAP=256 cargo test -p yellowstone-jet-tpu-client --all-features
```

You can verify the active compiled value in code:

```rust
use yellowstone_jet_tpu_client::TXN_INFO_CAP;

println!("TXN_INFO_CAP={}", TXN_INFO_CAP);
assert!(std::mem::size_of::<TxMeta>() <= TXN_INFO_CAP);
```


You can run the above ^ code using:

```sh
cargo run --bin test-tpu-send --features "examples" --
```

To faciliate your run, write `.env` in your working directory:

```sh
IDENTITY=/path/to/dev-id.json
RPC_ENDPOINT=http://127.0.0.1:8899
GRPC_ENDPOINT=http://127.0.0.1:10000
```