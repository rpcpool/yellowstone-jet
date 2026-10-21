//! Sample: how jet can ingest a single-use, response-wrapped Vault token
//! (provided by its Nomad job) and unwrap it itself to obtain its identity
//! keypair — in memory, never written back to disk.
//!
//! Vault access goes through the [`vaultrs`] crate, which ships the
//! response-wrapping endpoints out of the box
//! (`vaultrs::sys::wrapping::{lookup, unwrap}`).
//!
//! The Nomad job only hands jet a *wrapping token*, rendered into the task
//! environment by a `template { env = true }` stanza. The real keypair sits
//! in a single-use cubbyhole inside Vault. At startup jet:
//!
//! 1. reads the wrapping token from the environment — it is the only Vault
//!    credential jet has or needs, so it becomes the client token,
//! 2. (optionally) verifies via `sys/wrapping/lookup` — which does NOT
//!    consume the token — that it was created by the expected Vault path,
//!    refusing a substituted/forged token,
//! 3. calls `sys/wrapping/unwrap`. This is the single use: it succeeds
//!    exactly once. If it fails and this process never unwrapped it, someone
//!    else did — treat it as interception and rotate the identity,
//! 4. parses the `keypair` field of the unwrapped secret into a
//!    `solana_keypair::Keypair` and uses it as its identity.
//!
//! The producer side wraps the identity read, for example:
//!
//! ```bash
//! vault kv get -wrap-ttl=300s -field=wrapping_token kv/jet/identity
//! ```
//!
//! or, in a Nomad template stanza rendered by the cluster's Vault
//! integration, exports only the wrapping token into the task environment.
//!
//! Everything comes from the environment, so there is no config to thread
//! through:
//!
//! * `VAULT_ADDR` — Vault address (read by vaultrs itself; defaults to
//!   `http://127.0.0.1:8200`)
//! * `VAULT_WRAPPED_TOKEN` — the response-wrapped token, required
//! * `VAULT_EXPECTED_CREATION_PATH` — optional; when set, enables the
//!   creation-path tamper check
//!
//! In jet proper, `load_identity_from_wrapped_token` is what would be called
//! from `main` to produce `initial_identity`.

use {
    anyhow::{bail, Context},
    serde_json::Value,
    solana_keypair::Keypair,
    solana_signer::Signer,
    vaultrs::{
        client::{VaultClient, VaultClientSettingsBuilder},
        error::ClientError,
        sys::wrapping,
    },
};

/// Ingest the response-wrapped token from `$VAULT_WRAPPED_TOKEN` and unwrap
/// it into the jet identity.
///
/// The returned `Keypair` only ever lives in memory.
pub async fn load_identity_from_wrapped_token() -> anyhow::Result<Keypair> {
    // 1. Ingest the wrapping token from the environment, where the Nomad job
    //    rendered it — so the token never lands on disk.
    //
    //    It does stay in the process environment (readable through
    //    /proc/self/environ). Scrubbing it needs an `unsafe` block in edition
    //    2024 and is only sound before any thread starts, so do that at the
    //    very top of `main` if your threat model calls for it, not here.
    let wrapping_token = std::env::var("VAULT_WRAPPED_TOKEN")
        .context("VAULT_WRAPPED_TOKEN is not set")?
        .trim()
        .to_owned();
    if wrapping_token.is_empty() {
        bail!(
            "VAULT_WRAPPED_TOKEN is empty — the Nomad template that mints the wrapping token \
             probably did not render"
        );
    }

    // The wrapping token authenticates its own lookup and unwrap, so it is
    // the client token — jet needs no other Vault credentials. The address
    // is left to vaultrs, which reads $VAULT_ADDR and otherwise falls back to
    // http://127.0.0.1:8200.
    let client = VaultClient::new(
        VaultClientSettingsBuilder::default()
            .token(&wrapping_token)
            .build()
            .context("invalid Vault client settings (check $VAULT_ADDR)")?,
    )
    .context("failed to build Vault client")?;

    // 2. Optional tamper check, enabled by setting
    //    $VAULT_EXPECTED_CREATION_PATH. `sys/wrapping/lookup` does not consume
    //    the token, and a wrapping token is allowed to look itself up.
    if let Ok(expected) = std::env::var("VAULT_EXPECTED_CREATION_PATH") {
        let info = wrapping::lookup(&client, &wrapping_token)
            .await
            .context("sys/wrapping/lookup failed")?;
        if info.creation_path != expected {
            bail!(
                "SECURITY: wrapping token creation_path mismatch: expected {expected}, got {} — \
                 refusing to unwrap, the token may have been substituted",
                info.creation_path
            );
        }
    }

    // 3. Unwrap — the single use of the token. Passing `None` unwraps the
    //    client token itself.
    let unwrapped: Value = match wrapping::unwrap(&client, None).await {
        Ok(data) => data,
        Err(ClientError::APIError { code: 400, errors }) => bail!(
            "unwrap rejected ({errors:?}): the wrapping token is invalid, expired, or WAS \
             ALREADY UNWRAPPED. If this process never unwrapped it, treat the identity as \
             potentially exposed and rotate it."
        ),
        Err(err) => return Err(err).context("sys/wrapping/unwrap failed"),
    };

    // 4. Extract the keypair. A wrapped `vault kv get` of a kv-v2 secret
    //    nests the fields under `data`; kv-v1 (or `sys/wrapping/wrap`) does
    //    not.
    let keypair = unwrapped
        .pointer("/data/keypair")
        .or_else(|| unwrapped.get("keypair"))
        .context("no `keypair` field in unwrapped secret")?;
    parse_keypair(keypair)
}

/// Accept the keypair either as the standard solana `id.json` content (a JSON
/// array of 64 bytes, possibly stored as a string) or as base58.
fn parse_keypair(value: &Value) -> anyhow::Result<Keypair> {
    let bytes: Vec<u8> = match value {
        Value::Array(_) => serde_json::from_value(value.clone())
            .context("keypair JSON array is not a byte array")?,
        Value::String(s) => {
            let s = s.trim();
            if s.starts_with('[') {
                serde_json::from_str(s).context("keypair string is not a JSON byte array")?
            } else {
                bs58::decode(s)
                    .into_vec()
                    .context("keypair string is neither a JSON byte array nor base58")?
            }
        }
        other => bail!("unsupported keypair encoding: {other:?}"),
    };
    Keypair::try_from(bytes.as_slice()).context("invalid keypair bytes")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let identity = load_identity_from_wrapped_token().await?;

    // In jet this Keypair would become `initial_identity`, handed to
    // `JetIdentitySyncGroup::new(...)` — here we just prove we have it.
    println!("jet identity unwrapped: {}", identity.pubkey());
    Ok(())
}
