//! Load the TPU identity from Vault after the guest attestation agent proves this VM to Trustee.
//! Vault JWT auth trusts Trustee's attestation token, so only an attested guest can read the key.

use {
    crate::{config::ConfigVaultIdentity, metrics::jet as metrics, rpc::admin::JetIdentityUpdater},
    anyhow::{Context as _, bail},
    serde::{Deserialize, Serialize},
    solana_pubkey::Pubkey,
    std::{fmt, net::IpAddr, path::Path, time::Duration},
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
    url::Url,
    yellowstone_jet_tpu_client::identity::HardenedKeypair,
    zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing},
};

const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The step that failed, used as a metric label. Error text never carries a response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStage {
    Attestation,
    Login,
    Read,
    Key,
}

impl LoadStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Attestation => "attestation",
            Self::Login => "login",
            Self::Read => "read",
            Self::Key => "key",
        }
    }
}

impl fmt::Display for LoadStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("vault identity {stage} failed: {reason}")]
pub struct LoadError {
    pub stage: LoadStage,
    reason: String,
}

impl LoadError {
    fn new(stage: LoadStage, reason: impl Into<String>) -> Self {
        Self {
            stage,
            reason: reason.into(),
        }
    }
}

pub struct VaultIdentityLoader {
    config: ConfigVaultIdentity,
    expected: Pubkey,
    login_url: Url,
    read_url: Url,
    revoke_url: Url,
    vault: reqwest::Client,
    attestation_agent: reqwest::Client,
}

impl VaultIdentityLoader {
    pub fn new(config: ConfigVaultIdentity, expected: Pubkey) -> anyhow::Result<Self> {
        validate_vault_address(&config.address)?;
        validate_attestation_agent_url(&config.attestation_agent_url)?;
        validate_vault_path("auth_mount", &config.auth_mount)?;
        validate_vault_path("identity_path", &config.identity_path)?;
        if config.role.trim().is_empty() {
            bail!("vault role must not be empty");
        }
        if !config.identity_path.contains("/data/") {
            bail!("identity_path must have the form <kv-v2-mount>/data/<secret>");
        }
        if config.retry_min.is_zero() || config.retry_min > config.retry_max {
            bail!("vault retry_min must be non-zero and not larger than retry_max");
        }

        let api = config.address.join("v1/")?;
        let login_url = api.join(&format!("auth/{}/login", config.auth_mount))?;
        let read_url = api.join(&config.identity_path)?;
        let revoke_url = api.join("auth/token/revoke-self")?;

        let vault = vault_transport(config.ca_cert_file.as_deref())?;
        let attestation_agent = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build attestation agent client")?;

        Ok(Self {
            config,
            expected,
            login_url,
            read_url,
            revoke_url,
            vault,
            attestation_agent,
        })
    }

    /// Attest, log in, read the identity once and check it matches `identity.expected`.
    pub async fn load(&self) -> Result<HardenedKeypair, LoadError> {
        let attestation_token = self.attestation_token().await?;
        let vault_token = self.login(&attestation_token).await?;
        drop(attestation_token);

        let identity = self.read_identity(&vault_token).await;
        self.revoke(&vault_token).await;
        let identity = identity?;

        if identity.pubkey() != self.expected {
            return Err(LoadError::new(
                LoadStage::Key,
                "the Vault identity does not match identity.expected",
            ));
        }
        Ok(identity)
    }

    async fn attestation_token(&self) -> Result<Zeroizing<String>, LoadError> {
        let stage = LoadStage::Attestation;
        let body = get_body(
            self.attestation_agent
                .get(self.config.attestation_agent_url.clone()),
            stage,
        )
        .await?;
        parse_attestation_token(&body)
    }

    async fn login(&self, attestation_token: &str) -> Result<Zeroizing<String>, LoadError> {
        let stage = LoadStage::Login;
        let mut request = Zeroizing::new(
            serde_json::to_vec(&LoginRequest {
                role: &self.config.role,
                jwt: attestation_token,
            })
            .map_err(|_| LoadError::new(stage, "failed to encode the login request"))?,
        );
        let body = get_body(
            self.vault
                .post(self.login_url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(std::mem::take(&mut *request)),
            stage,
        )
        .await?;
        let mut response: LoginResponse = serde_json::from_slice(&body)
            .map_err(|_| LoadError::new(stage, "Vault returned an unexpected login response"))?;
        let token = std::mem::take(&mut response.auth.client_token);
        if token.is_empty() {
            return Err(LoadError::new(
                stage,
                "Vault returned an empty client token",
            ));
        }
        Ok(Zeroizing::new(token))
    }

    async fn read_identity(&self, vault_token: &str) -> Result<HardenedKeypair, LoadError> {
        let stage = LoadStage::Read;
        let body = get_body(
            self.vault
                .get(self.read_url.clone())
                .header("X-Vault-Token", vault_token),
            stage,
        )
        .await?;
        let response: ReadResponse = serde_json::from_slice(&body)
            .map_err(|_| LoadError::new(stage, "Vault returned an unexpected KV v2 response"))?;
        response.data.data.priv_key.to_keypair()
    }

    /// Revoke the Vault token after the read. Its short TTL still bounds it if this fails.
    async fn revoke(&self, vault_token: &str) {
        let result = self
            .vault
            .post(self.revoke_url.clone())
            .header("X-Vault-Token", vault_token)
            .send()
            .await;
        if !matches!(result, Ok(ref response) if response.status().is_success()) {
            warn!("failed to revoke the Vault token after the identity read");
        }
    }
}

/// Retry `load` until it succeeds, install the key, then wait for shutdown.
pub async fn run_vault_identity_loader<IU>(
    loader: VaultIdentityLoader,
    mut identity_updater: IU,
    cancellation_token: CancellationToken,
) where
    IU: JetIdentityUpdater + Send,
{
    let retry_max = loader.config.retry_max;
    let mut delay = loader.config.retry_min;
    metrics::vault_identity_loaded_set(false);
    loop {
        let result = tokio::select! {
            result = loader.load() => result,
            () = cancellation_token.cancelled() => return,
        };
        match result {
            Ok(identity) => {
                let pubkey = identity.pubkey();
                identity_updater.update_identity(identity).await;
                metrics::vault_identity_loaded_set(true);
                info!("loaded identity {pubkey} from Vault after attestation");
                break;
            }
            Err(error) => {
                metrics::vault_identity_load_error_inc(error.stage.as_str());
                warn!("{error}; retrying in {delay:?}");
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = cancellation_token.cancelled() => return,
                }
                delay = (delay * 2).min(retry_max);
            }
        }
    }
    cancellation_token.cancelled().await;
}

fn vault_transport(ca_cert_file: Option<&Path>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT);
    if let Some(path) = ca_cert_file {
        let pem = std::fs::read(path).context("failed to read the Vault CA bundle")?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .context("the Vault CA bundle is not valid PEM")?;
        if certs.is_empty() {
            bail!("the Vault CA bundle contains no certificates");
        }
        builder = builder.tls_certs_merge(certs);
    }
    builder.build().context("failed to build the Vault client")
}

/// Send a request and return the body only on HTTP 2xx. Bodies stay out of errors.
async fn get_body(
    request: reqwest::RequestBuilder,
    stage: LoadStage,
) -> Result<Zeroizing<Vec<u8>>, LoadError> {
    let mut response = request
        .send()
        .await
        .map_err(|error| LoadError::new(stage, transport_reason(&error)))?;
    let status = response.status();
    let mut body = Zeroizing::new(Vec::new());
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| LoadError::new(stage, "failed to read the response body"))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(LoadError::new(stage, "the response body is too large"));
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(LoadError::new(stage, format!("HTTP {}", status.as_u16())));
    }
    Ok(body)
}

fn transport_reason(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "the request timed out".to_owned()
    } else if error.is_connect() {
        "connection failed".to_owned()
    } else if error.is_redirect() {
        "redirects are refused".to_owned()
    } else if error.is_builder() {
        "the request was refused before sending".to_owned()
    } else {
        "the request failed".to_owned()
    }
}

/// The CoCo attestation agent returns `{"token": ..., "tee_keypair": ...}` for a KBS token.
/// Serde skips `tee_keypair`, and the caller zeroizes the body that holds it.
fn parse_attestation_token(body: &[u8]) -> Result<Zeroizing<String>, LoadError> {
    let stage = LoadStage::Attestation;
    let text = std::str::from_utf8(body)
        .map_err(|_| LoadError::new(stage, "the attestation token is not UTF-8"))?
        .trim();
    let token = if text.starts_with('{') {
        let mut response: AttestationTokenResponse = serde_json::from_str(text).map_err(|_| {
            LoadError::new(
                stage,
                "the attestation agent returned an unexpected response",
            )
        })?;
        std::mem::take(&mut response.token)
    } else {
        text.to_owned()
    };
    if token.is_empty() {
        return Err(LoadError::new(stage, "the attestation token is empty"));
    }
    Ok(Zeroizing::new(token))
}

fn validate_vault_address(address: &Url) -> anyhow::Result<()> {
    if address.scheme() != "https"
        || address.host_str().is_none()
        || !address.username().is_empty()
        || address.password().is_some()
        || !matches!(address.path(), "" | "/")
        || address.query().is_some()
        || address.fragment().is_some()
    {
        bail!("vault address must be an HTTPS origin without credentials, path, query or fragment");
    }
    Ok(())
}

/// The attestation agent API has no TLS, so it must stay on guest loopback.
fn validate_attestation_agent_url(url: &Url) -> anyhow::Result<()> {
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip).is_loopback(),
        Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip).is_loopback(),
        Some(url::Host::Domain(domain)) => domain == "localhost",
        None => false,
    };
    if url.scheme() != "http" || !loopback {
        bail!("attestation_agent_url must be an http URL on loopback");
    }
    Ok(())
}

fn validate_vault_path(name: &str, path: &str) -> anyhow::Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['?', '#', '%'])
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("{name} must be a relative Vault path without traversal, query or escapes");
    }
    Ok(())
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct AttestationTokenResponse {
    token: String,
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    role: &'a str,
    jwt: &'a str,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct LoginResponse {
    auth: LoginAuth,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct LoginAuth {
    client_token: String,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct ReadResponse {
    data: ReadData,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct ReadData {
    data: IdentityData,
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
struct IdentityData {
    priv_key: KeyMaterial,
}

/// `priv_key` holds a Solana keypair as a byte array, a JSON array string or base58.
#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(untagged)]
enum KeyMaterial {
    Bytes(Vec<u8>),
    Encoded(String),
}

impl KeyMaterial {
    fn to_keypair(&self) -> Result<HardenedKeypair, LoadError> {
        let stage = LoadStage::Key;
        let bytes: Zeroizing<Vec<u8>> = match self {
            Self::Bytes(bytes) => Zeroizing::new(bytes.clone()),
            Self::Encoded(encoded) if encoded.trim_start().starts_with('[') => Zeroizing::new(
                serde_json::from_str(encoded)
                    .map_err(|_| LoadError::new(stage, "priv_key is not a JSON byte array"))?,
            ),
            Self::Encoded(encoded) => Zeroizing::new(
                bs58::decode(encoded.trim())
                    .into_vec()
                    .map_err(|_| LoadError::new(stage, "priv_key is not valid base58"))?,
            ),
        };
        HardenedKeypair::try_from(bytes.as_slice())
            .map_err(|_| LoadError::new(stage, "priv_key is not a valid ed25519 keypair"))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_keypair::Keypair, solana_signer::Signer};

    #[test]
    fn attestation_token_from_kbs_json() {
        let body = br#"{"token":"eyJ.a.b","tee_keypair":"-----BEGIN RSA PRIVATE KEY-----"}"#;
        assert_eq!(parse_attestation_token(body).unwrap().as_str(), "eyJ.a.b");
    }

    #[test]
    fn attestation_token_from_plain_body() {
        assert_eq!(
            parse_attestation_token(b"  eyJ.a.b\n").unwrap().as_str(),
            "eyJ.a.b"
        );
    }

    #[test]
    fn attestation_token_rejects_empty() {
        assert!(parse_attestation_token(b"").is_err());
        assert!(parse_attestation_token(br#"{"token":""}"#).is_err());
    }

    #[test]
    fn key_material_formats() {
        let keypair = Keypair::new();
        let bytes = keypair.to_bytes().to_vec();
        let json = serde_json::to_string(&bytes).unwrap();
        for material in [
            KeyMaterial::Bytes(bytes.clone()),
            KeyMaterial::Encoded(json),
            KeyMaterial::Encoded(keypair.to_base58_string()),
        ] {
            assert_eq!(material.to_keypair().unwrap().pubkey(), keypair.pubkey());
        }
        assert!(
            KeyMaterial::Encoded("not-base58-0OIl".to_owned())
                .to_keypair()
                .is_err()
        );
        assert!(KeyMaterial::Bytes(vec![1; 63]).to_keypair().is_err());
    }

    #[test]
    fn address_checks() {
        for ok in ["https://vault.example:8200", "https://vault.example/"] {
            validate_vault_address(&Url::parse(ok).unwrap()).unwrap();
        }
        for bad in [
            "http://vault.example",
            "https://user:pw@vault.example",
            "https://vault.example/v1",
            "https://vault.example/?x=1",
        ] {
            assert!(
                validate_vault_address(&Url::parse(bad).unwrap()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn attestation_agent_must_be_loopback_http() {
        for ok in [
            "http://127.0.0.1:8006/aa/token?token_type=kbs",
            "http://[::1]:8006/aa/token",
            "http://localhost:8006/aa/token",
        ] {
            validate_attestation_agent_url(&Url::parse(ok).unwrap()).unwrap();
        }
        for bad in [
            "http://10.0.0.1:8006/aa/token",
            "https://127.0.0.1:8006/aa/token",
        ] {
            assert!(
                validate_attestation_agent_url(&Url::parse(bad).unwrap()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn vault_path_checks() {
        validate_vault_path("p", "jet-testnet/data/identities/abc").unwrap();
        for bad in ["", "/abs", "a//b", "a/../b", "a/./b", "a?b", "a#b", "a%2fb"] {
            assert!(validate_vault_path("p", bad).is_err(), "{bad}");
        }
    }
}
