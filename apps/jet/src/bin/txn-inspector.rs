use {
    anyhow::{Context, Result, bail},
    base64::{Engine, prelude::BASE64_STANDARD},
    clap::{Parser, ValueEnum},
    solana_message::VersionedMessage,
    solana_transaction::versioned::VersionedTransaction,
    std::io::Read,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Encoding {
    Auto,
    Base64,
    Base58,
}

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Decode and inspect a Solana VersionedTransaction"
)]
struct Args {
    /// Encoded transaction input. Use "-" or omit to read stdin.
    #[clap(value_name = "INPUT")]
    input: Option<String>,

    /// Transaction encoding
    #[clap(long, value_enum, default_value_t = Encoding::Auto)]
    encoding: Encoding,

    /// Print full transaction debug output
    #[clap(long)]
    detailed: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let input = read_input(args.input)?;
    let normalized = normalize_input(&input)?;
    let (encoding, bytes) = decode_input(&normalized, args.encoding)?;

    let tx: VersionedTransaction = bincode::deserialize(&bytes).with_context(|| {
        format!(
            "failed to deserialize {} bytes as VersionedTransaction",
            bytes.len()
        )
    })?;

    print_summary(encoding, &bytes, &tx, args.detailed);
    Ok(())
}

fn read_input(input: Option<String>) -> Result<String> {
    if let Some(input) = input {
        if input != "-" {
            return Ok(input);
        }
    }

    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .context("failed to read stdin")?;
    Ok(buf)
}

fn normalize_input(input: &str) -> Result<String> {
    let normalized = input
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches(';')
        .trim_matches(',')
        .trim()
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect::<String>();

    if normalized.is_empty() {
        bail!("no encoded transaction data found in input");
    }

    Ok(normalized)
}

fn decode_input(input: &str, encoding: Encoding) -> Result<(&'static str, Vec<u8>)> {
    match encoding {
        Encoding::Base64 => Ok((
            "base64",
            BASE64_STANDARD
                .decode(input)
                .with_context(|| "base64 decode failed")?,
        )),
        Encoding::Base58 => Ok((
            "base58",
            bs58::decode(input)
                .into_vec()
                .with_context(|| "base58 decode failed")?,
        )),
        Encoding::Auto => {
            if let Ok(decoded) = BASE64_STANDARD.decode(input) {
                return Ok(("base64", decoded));
            }

            if let Ok(decoded) = bs58::decode(input).into_vec() {
                return Ok(("base58", decoded));
            }

            bail!("failed to decode input as base64 or base58")
        }
    }
}

fn print_summary(encoding: &str, bytes: &[u8], tx: &VersionedTransaction, detailed: bool) {
    let message_version = match &tx.message {
        VersionedMessage::Legacy(_) => "legacy",
        VersionedMessage::V0(_) => "v0",
        VersionedMessage::V1(_) => "v1",
    };
    let instruction_count = tx.message.instructions().len();
    let static_accounts = tx
        .message
        .static_account_keys()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    println!("decoded encoding: {encoding}");
    println!("decoded bytes: {}", bytes.len());
    println!("message version: {message_version}");
    println!("signatures: {}", tx.signatures.len());
    println!("instruction count: {instruction_count}");
    println!("static accounts ({}):", static_accounts.len());
    for account in &static_accounts {
        println!("- {account}");
    }

    match &tx.message {
        VersionedMessage::Legacy(_) => {
            println!("alt accounts: none (legacy message)");
        }
        VersionedMessage::V0(message) => {
            println!(
                "alt accounts (lookups: {}):",
                message.address_table_lookups.len()
            );
            for lookup in &message.address_table_lookups {
                println!(
                    "- table={} writable_indexes={:?} readonly_indexes={:?}",
                    lookup.account_key, lookup.writable_indexes, lookup.readonly_indexes
                );
            }
        }
        VersionedMessage::V1(_) => {
            println!("alt accounts: none (v1 message)");
        }
    }

    if let Some(first_sig) = tx.signatures.first() {
        println!("first signature: {first_sig}");
    }

    if detailed {
        println!();
        println!("transaction (detailed):");
        print_detailed_transaction(tx);
    }
}

fn print_detailed_transaction(tx: &VersionedTransaction) {
    println!("signatures ({}):", tx.signatures.len());
    for (i, sig) in tx.signatures.iter().enumerate() {
        println!("- [{i}] {sig}");
    }

    match &tx.message {
        VersionedMessage::Legacy(message) => {
            println!("message: legacy");
            println!(
                "header: required_signatures={} readonly_signed={} readonly_unsigned={}",
                message.header.num_required_signatures,
                message.header.num_readonly_signed_accounts,
                message.header.num_readonly_unsigned_accounts
            );
            println!("recent_blockhash: {}", message.recent_blockhash);
            println!("account_keys ({}):", message.account_keys.len());
            for (i, key) in message.account_keys.iter().enumerate() {
                println!("- [{i}] {key}");
            }
            println!("instructions ({}):", message.instructions.len());
            for (i, ix) in message.instructions.iter().enumerate() {
                println!(
                    "- [{i}] program_id_index={} accounts={:?} data_len={} data={:?}",
                    ix.program_id_index,
                    ix.accounts,
                    ix.data.len(),
                    ix.data
                );
            }
        }
        VersionedMessage::V0(message) => {
            println!("message: v0");
            println!(
                "header: required_signatures={} readonly_signed={} readonly_unsigned={}",
                message.header.num_required_signatures,
                message.header.num_readonly_signed_accounts,
                message.header.num_readonly_unsigned_accounts
            );
            println!("recent_blockhash: {}", message.recent_blockhash);
            println!("account_keys ({}):", message.account_keys.len());
            for (i, key) in message.account_keys.iter().enumerate() {
                println!("- [{i}] {key}");
            }
            println!("instructions ({}):", message.instructions.len());
            for (i, ix) in message.instructions.iter().enumerate() {
                println!(
                    "- [{i}] program_id_index={} accounts={:?} data_len={} data={:?}",
                    ix.program_id_index,
                    ix.accounts,
                    ix.data.len(),
                    ix.data
                );
            }
            println!(
                "address_table_lookups ({}):",
                message.address_table_lookups.len()
            );
            for (i, lookup) in message.address_table_lookups.iter().enumerate() {
                println!(
                    "- [{i}] table={} writable_indexes={:?} readonly_indexes={:?}",
                    lookup.account_key, lookup.writable_indexes, lookup.readonly_indexes
                );
            }
        }
        VersionedMessage::V1(message) => {
            println!("message: v1");
            println!(
                "header: required_signatures={} readonly_signed={} readonly_unsigned={}",
                message.header.num_required_signatures,
                message.header.num_readonly_signed_accounts,
                message.header.num_readonly_unsigned_accounts
            );
            println!("recent_blockhash: {}", message.lifetime_specifier);
            println!("config: {:?}", message.config);
            println!("account_keys ({}):", message.account_keys.len());
            for (i, key) in message.account_keys.iter().enumerate() {
                println!("- [{i}] {key}");
            }
            println!("instructions ({}):", message.instructions.len());
            for (i, ix) in message.instructions.iter().enumerate() {
                println!(
                    "- [{i}] program_id_index={} accounts={:?} data_len={} data={:?}",
                    ix.program_id_index,
                    ix.accounts,
                    ix.data.len(),
                    ix.data
                );
            }
            println!("address_table_lookups: none (v1 message)");
        }
    }
}
