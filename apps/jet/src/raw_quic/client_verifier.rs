//! Customer mTLS authentication: a hot-reloadable allow-list of exact, pinned client
//! certificates (no CA — see plan discussion), loadable from either a local directory or
//! a remote HTTP endpoint, plus a debug verifier that accepts any client cert.

use {
    arc_swap::ArcSwap,
    rustls::{
        DigitallySignedStruct, DistinguishedName, Error as RustlsError, SignatureScheme,
        client::danger::HandshakeSignatureValid,
        crypto::CryptoProvider,
        pki_types::{CertificateDer, UnixTime, pem::PemObject},
        server::danger::{ClientCertVerified, ClientCertVerifier},
    },
    serde::Deserialize,
    sha2::{Digest, Sha256},
    std::{
        collections::HashMap,
        fmt::{self, Debug, Formatter},
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
    tokio::task::JoinError,
    url::Url,
};

pub type Fingerprint = [u8; 32];

pub(crate) fn fingerprint(cert: &CertificateDer<'_>) -> Fingerprint {
    let mut hasher = Sha256::new();
    hasher.update(cert.as_ref());
    hasher.finalize().into()
}

#[derive(Debug, thiserror::Error)]
pub enum AllowlistSourceError {
    #[error("failed to read allow-list directory {0:?}: {1}")]
    ReadDir(PathBuf, std::io::Error),
    #[error("failed to read cert file {0:?}: {1}")]
    ReadFile(PathBuf, std::io::Error),
    #[error("invalid certificate in {0:?}: {1}")]
    InvalidCertificate(PathBuf, rustls::pki_types::pem::Error),
    #[error("background task panicked: {0}")]
    Join(#[from] JoinError),
    #[error("HTTP request to allow-list endpoint failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid certificate for allow-list entry {label:?}: {source}")]
    InvalidHttpCertificate {
        label: String,
        source: rustls::pki_types::pem::Error,
    },
}

/// Where the pinned client-certificate allow-list is fetched from.
#[async_trait::async_trait]
pub trait AllowlistSource: Send + Sync {
    async fn fetch(&self) -> Result<HashMap<Fingerprint, String>, AllowlistSourceError>;
}

/// One cert per file; the accepted label defaults to the filename stem.
pub struct DirAllowlistSource {
    dir: PathBuf,
}

impl DirAllowlistSource {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

#[async_trait::async_trait]
impl AllowlistSource for DirAllowlistSource {
    async fn fetch(&self) -> Result<HashMap<Fingerprint, String>, AllowlistSourceError> {
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || load_dir(&dir)).await?
    }
}

fn load_dir(dir: &Path) -> Result<HashMap<Fingerprint, String>, AllowlistSourceError> {
    let entries =
        fs::read_dir(dir).map_err(|e| AllowlistSourceError::ReadDir(dir.to_path_buf(), e))?;

    let mut map = HashMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| AllowlistSourceError::ReadDir(dir.to_path_buf(), e))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let bytes = fs::read(&path).map_err(|e| AllowlistSourceError::ReadFile(path.clone(), e))?;
        let cert = CertificateDer::from_pem_slice(&bytes)
            .map_err(|e| AllowlistSourceError::InvalidCertificate(path.clone(), e))?;
        let label = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned();

        map.insert(fingerprint(&cert), label);
    }

    Ok(map)
}

/// GETs `url`, expecting a JSON array of `{ "label": ..., "cert_pem": ... }` objects.
/// Lets the allow-list be centrally managed instead of file-synced to every jet instance.
pub struct HttpAllowlistSource {
    client: reqwest::Client,
    url: Url,
    timeout: Duration,
}

impl HttpAllowlistSource {
    pub fn new(url: Url, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
            timeout,
        }
    }
}

#[derive(Debug, Deserialize)]
struct HttpAllowlistEntry {
    label: String,
    cert_pem: String,
}

#[async_trait::async_trait]
impl AllowlistSource for HttpAllowlistSource {
    async fn fetch(&self) -> Result<HashMap<Fingerprint, String>, AllowlistSourceError> {
        let entries: Vec<HttpAllowlistEntry> = self
            .client
            .get(self.url.clone())
            .timeout(self.timeout)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let mut map = HashMap::with_capacity(entries.len());
        for entry in entries {
            let cert = CertificateDer::from_pem_slice(entry.cert_pem.as_bytes()).map_err(|e| {
                AllowlistSourceError::InvalidHttpCertificate {
                    label: entry.label.clone(),
                    source: e,
                }
            })?;
            map.insert(fingerprint(&cert), entry.label);
        }
        Ok(map)
    }
}

/// The live, hot-reloadable allow-list state, decoupled from wherever it's sourced from.
pub struct Allowlist {
    state: ArcSwap<HashMap<Fingerprint, String>>,
    source: Box<dyn AllowlistSource>,
}

impl Allowlist {
    pub async fn load(source: Box<dyn AllowlistSource>) -> Result<Arc<Self>, AllowlistSourceError> {
        let initial = source.fetch().await?;
        Ok(Arc::new(Self {
            state: ArcSwap::from_pointee(initial),
            source,
        }))
    }

    /// Re-fetches from the source. On failure, the previously loaded list is left in
    /// place (never cleared) — a transient blip fetching a remote list must never lock
    /// out all customers. Returns the new entry count on success.
    pub async fn reload(&self) -> Result<usize, AllowlistSourceError> {
        let fresh = self.source.fetch().await?;
        let len = fresh.len();
        self.state.store(Arc::new(fresh));
        Ok(len)
    }

    pub fn len(&self) -> usize {
        self.state.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.load().is_empty()
    }

    pub fn lookup(&self, fp: &Fingerprint) -> Option<String> {
        self.state.load().get(fp).cloned()
    }

    pub fn verifier(self: &Arc<Self>) -> Arc<dyn ClientCertVerifier> {
        Arc::new(PinnedCertVerifier {
            allowlist: Arc::clone(self),
            provider: Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        })
    }
}

struct PinnedCertVerifier {
    allowlist: Arc<Allowlist>,
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for PinnedCertVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        if self.allowlist.lookup(&fingerprint(end_entity)).is_some() {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(RustlsError::General(
                "client certificate is not on the allow-list".to_owned(),
            ))
        }
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

impl Debug for PinnedCertVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedCertVerifier").finish_non_exhaustive()
    }
}

/// Dev/debug only: accepts any presented client certificate, bypassing the allow-list
/// entirely. A client cert is still required (`client_auth_mandatory` is `true`) — this
/// only skips the allow-list *lookup*.
pub struct AllowAnyClientVerifier {
    provider: Arc<CryptoProvider>,
}

impl AllowAnyClientVerifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            provider: Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        })
    }
}

impl ClientCertVerifier for AllowAnyClientVerifier {
    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        Ok(ClientCertVerified::assertion())
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

impl Debug for AllowAnyClientVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllowAnyClientVerifier")
            .finish_non_exhaustive()
    }
}

/// Skips client certificate verification entirely -- unlike [`AllowAnyClientVerifier`],
/// this doesn't even request a client certificate (`offer_client_auth` is `false`), so
/// there's no mTLS at all. This is [`super::ServerBuilder`]'s default when no
/// [`ClientCertVerifier`] is configured via
/// [`super::ServerBuilder::client_verifier`] -- fine for a server that doesn't need
/// per-customer identity, but see [`AllowAnyClientVerifier`] or [`Allowlist`] if it does.
pub struct SkipClientVerifier {
    provider: Arc<CryptoProvider>,
}

impl SkipClientVerifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            provider: Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        })
    }
}

impl ClientCertVerifier for SkipClientVerifier {
    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        Ok(ClientCertVerified::assertion())
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }

    fn offer_client_auth(&self) -> bool {
        false
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }
}

impl Debug for SkipClientVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkipClientVerifier").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_client_verifier_disables_client_auth() {
        let verifier = SkipClientVerifier::new();
        assert!(
            !verifier.offer_client_auth(),
            "SkipClientVerifier must not even request a client certificate"
        );
        assert!(!verifier.client_auth_mandatory());
    }

    fn self_signed_cert_pem() -> String {
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["example.invalid".to_owned()])
                .expect("self-signed cert");
        cert.pem()
    }

    #[tokio::test]
    async fn dir_source_labels_by_filename_stem() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("customer-a.pem"), self_signed_cert_pem()).unwrap();

        let source = DirAllowlistSource::new(dir.path());
        let map = source.fetch().await.expect("fetch");
        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next().unwrap(), "customer-a");
    }

    #[tokio::test]
    async fn allowlist_accepts_pinned_and_rejects_unknown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pem = self_signed_cert_pem();
        fs::write(dir.path().join("customer-a.pem"), &pem).unwrap();

        let allowlist = Allowlist::load(Box::new(DirAllowlistSource::new(dir.path())))
            .await
            .expect("load");

        let known_cert = CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&known_cert)).is_some());

        let other_pem = self_signed_cert_pem();
        let unknown_cert = CertificateDer::from_pem_slice(other_pem.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&unknown_cert)).is_none());
    }

    #[tokio::test]
    async fn reload_add_and_remove() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pem_a = self_signed_cert_pem();
        fs::write(dir.path().join("customer-a.pem"), &pem_a).unwrap();

        let allowlist = Allowlist::load(Box::new(DirAllowlistSource::new(dir.path())))
            .await
            .expect("load");
        let cert_a = CertificateDer::from_pem_slice(pem_a.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_some());

        // Add a new cert and reload: it becomes accepted.
        let pem_b = self_signed_cert_pem();
        fs::write(dir.path().join("customer-b.pem"), &pem_b).unwrap();
        allowlist.reload().await.expect("reload");
        let cert_b = CertificateDer::from_pem_slice(pem_b.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&cert_b)).is_some());

        // Remove customer-a and reload: it becomes rejected.
        fs::remove_file(dir.path().join("customer-a.pem")).unwrap();
        allowlist.reload().await.expect("reload");
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_none());
    }

    #[tokio::test]
    async fn reload_failure_keeps_previous_list() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pem_a = self_signed_cert_pem();
        fs::write(dir.path().join("customer-a.pem"), &pem_a).unwrap();

        let allowlist = Allowlist::load(Box::new(DirAllowlistSource::new(dir.path())))
            .await
            .expect("load");
        let cert_a = CertificateDer::from_pem_slice(pem_a.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_some());

        // Point the source at a directory that no longer exists and reload: the fetch
        // fails, but the previously loaded allow-list must remain intact.
        fs::remove_dir_all(dir.path()).unwrap();
        let err = allowlist.reload().await.unwrap_err();
        assert!(matches!(err, AllowlistSourceError::ReadDir(_, _)));
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_some());
    }

    /// Serves exactly one HTTP/1.1 response on a fresh loopback listener, then exits.
    /// Minimal by design: `HttpAllowlistSource` only ever does one GET per `fetch()`.
    async fn serve_one_response(status_line: &'static str, body: String) -> std::net::SocketAddr {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 1024];
            // Drain (don't parse) the request; we only ever serve one canned response.
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        addr
    }

    #[tokio::test]
    async fn http_source_fetch_populates_allowlist() {
        let pem = self_signed_cert_pem();
        let body = serde_json::json!([{ "label": "customer-a", "cert_pem": pem }]).to_string();
        let addr = serve_one_response("HTTP/1.1 200 OK", body).await;
        let url = Url::parse(&format!("http://{addr}/allowlist")).unwrap();

        let source = HttpAllowlistSource::new(url, Duration::from_secs(5));
        let map = source.fetch().await.expect("fetch");
        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next().unwrap(), "customer-a");
    }

    #[tokio::test]
    async fn http_source_failed_fetch_keeps_previous_allowlist() {
        let pem = self_signed_cert_pem();
        let body = serde_json::json!([{ "label": "customer-a", "cert_pem": pem }]).to_string();
        let addr = serve_one_response("HTTP/1.1 200 OK", body).await;
        let url = Url::parse(&format!("http://{addr}/allowlist")).unwrap();

        let allowlist = Allowlist::load(Box::new(HttpAllowlistSource::new(
            url.clone(),
            Duration::from_secs(5),
        )))
        .await
        .expect("initial load");
        let cert_a = CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_some());

        // The mock server only answers once; this second fetch hits a closed
        // connection and must fail — without wiping the already-loaded list.
        let err = allowlist.reload().await.unwrap_err();
        assert!(matches!(err, AllowlistSourceError::Http(_)));
        assert!(allowlist.lookup(&fingerprint(&cert_a)).is_some());
    }
}
