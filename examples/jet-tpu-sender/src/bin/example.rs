
use {
    clap::Parser,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_hash::Hash,
    solana_keypair::{Keypair, Signature},
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        env,
        path::PathBuf,
        sync::Arc,
        vec,
    },
    yellowstone_jet_tpu_client::{
        core::TpuSenderResponse,
        yellowstone_grpc::sender::{
            Endpoints, NewYellowstoneTpuSender, YellowstoneTpuSender,
            create_yellowstone_tpu_sender_with_callback,
        },
    },
};

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

async fn send_lamports(
    mut tpu_sender: YellowstoneTpuSender,
    identity: &Keypair,
    recipient: &Pubkey,
    lamports: u64,
    latest_blockhash: Hash,
) -> Signature {
    let instructions = vec![transfer(&identity.pubkey(), &recipient, lamports)];

    let transaction = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&identity.pubkey(), &instructions, &[], latest_blockhash)
                .expect("try_compile"),
        ),
        &[&identity],
    )
    .expect("try_new");
    let signature = transaction.signatures[0];
    let bincoded_txn = bincode::serialize(&transaction).expect("bincode::serialize");

    // Send the transaction to the current leader
    tpu_sender
        .send_txn(signature, bincoded_txn)
        .await
        .expect("send_transaction");

    signature
}

#[tokio::main]
async fn main() {
    use std::io::Write;

    let mut out = std::io::stdout();

    let args = Args::parse();
    if let Ok(env_path) = args.dotenv.unwrap_or("./.env".into()).canonicalize() {
        if dotenvy::from_path(env_path).is_err() {
            eprintln!("Warning: Failed to load .env file");
        }
    } else {
        eprintln!("Warning: Failed to canonicalize .env file path");
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
                eprintln!("Warning: GRPC_X_TOKEN not set in dotenv file or environment");
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
                eprintln!(
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

    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let NewYellowstoneTpuSender {
        sender,
        related_objects_jh: _,
    } = create_yellowstone_tpu_sender_with_callback(
        Default::default(),
        identity.insecure_clone(),
        endpoints,
        callback_tx,
    )
    .await
    .expect("tpu-sender");

    const LAMPORTS: u64 = 1000;

    let latest_blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("get_latest_blockhash");
    let signature = send_lamports(
        sender,
        &identity,
        &recipient_pubkey,
        LAMPORTS,
        latest_blockhash,
    )
    .await;

    let TpuSenderResponse::TxSent(resp) = callback_rx
        .recv()
        .await
        .expect("receive tpu sender response")
    else {
        panic!("unexpected tpu sender response");
    };

    assert!(
        resp.tx_sig == signature,
        "unexpected tx signature in response"
    );

    writeln!(
        &mut out,
        "sent transaction with signature `{}` to validator `{}`",
        resp.tx_sig, resp.remote_peer_identity
    )
    .expect("writeln");
}