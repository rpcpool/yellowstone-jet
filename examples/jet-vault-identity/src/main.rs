//! Sample: how jet can ingest a single-use, response-wrapped Vault token
//! (provided by its Nomad job) and unwrap it itself to obtain its identity
//! keypair — in memory, never written back to disk.
//!
//! Vault access goes through the [`vaultrs`] crate, which ships the
//! response-wrapping endpoints out of the box
//! (`vaultrs::sys::wrapping::{lookup, unwrap}`).
//!
//! The Nomad job only hands jet a *wrapping token* (e.g. rendered into
//! `${NOMAD_SECRETS_DIR}/identity.wrapped` by a template stanza). The real
//! keypair sits in a single-use cubbyhole inside Vault. At startup jet:
//!
//! 1. reads the wrapping token — it is the only Vault credential jet has or
//!    needs, so it becomes the client token,
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
//! integration, writes only the wrapping token into the task secrets dir.
//!
//! In jet proper, `load_identity_from_wrapped_token` is what would be called
//! from `main` to produce `initial_identity` when the config selects the
//! Vault-wrapped source, e.g.:
//!
//! ```yaml
//! identity:
//!   vault:
//!     addr: https://vault.service.consul:8200
//!     wrapped_token_file: /secrets/identity.wrapped
//!     expected_creation_path: kv/data/jet/identity
//! ```

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

/// Where jet finds the wrapping token. Mirrors what a `ConfigIdentity`
/// extension would carry.
#[derive(Debug)]
pub struct VaultIdentityConfig {
    /// Vault address, e.g. `https://vault.service.consul:8200`
    pub addr: String,
    /// File the Nomad job rendered the wrapping token into
    /// (e.g. `${NOMAD_SECRETS_DIR}/identity.wrapped`)
    pub wrapped_token_file: String,
    /// If set, refuse to unwrap a token that was not created by exactly this
    /// Vault path (e.g. `kv/data/jet/identity`)
    pub expected_creation_path: Option<String>,
}

/// Ingest the response-wrapped token and unwrap it into the jet identity.
///
/// The returned `Keypair` only ever lives in memory.
pub async fn load_identity_from_wrapped_token(
    config: &VaultIdentityConfig,
) -> anyhow::Result<Keypair> {
    // 1. Ingest the wrapping token provided by the Nomad job
    let wrapping_token = tokio::fs::read_to_string(&config.wrapped_token_file)
        .await
        .with_context(|| {
            format!(
                "failed to read wrapped token from {}",
                config.wrapped_token_file
            )
        })?
        .trim()
        .to_owned();

    // The wrapping token authenticates its own lookup and unwrap, so it is
    // the client token — jet needs no other Vault credentials.
    let client = VaultClient::new(
        VaultClientSettingsBuilder::default()
            .address(&config.addr)
            .token(&wrapping_token)
            .build()
            .context("invalid Vault client settings")?,
    )
    .context("failed to build Vault client")?;

    // 2. Optional tamper check. `sys/wrapping/lookup` does not consume the
    //    token, and a wrapping token is allowed to look itself up.
    if let Some(expected) = &config.expected_creation_path {
        let info = wrapping::lookup(&client, &wrapping_token)
            .await
            .context("sys/wrapping/lookup failed")?;
        if &info.creation_path != expected {
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
    let config = VaultIdentityConfig {
        addr: std::env::var("VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_owned()),
        wrapped_token_file: std::env::var("VAULT_WRAPPED_TOKEN_FILE")
            .context("VAULT_WRAPPED_TOKEN_FILE is not set")?,
        expected_creation_path: std::env::var("VAULT_EXPECTED_CREATION_PATH").ok(),
    };

    let identity = load_identity_from_wrapped_token(&config).await?;

    // In jet this Keypair would become `initial_identity`, handed to
    // `JetIdentitySyncGroup::new(...)` — here we just prove we have it.
    println!("jet identity unwrapped: {}", identity.pubkey());
    Ok(())
}
