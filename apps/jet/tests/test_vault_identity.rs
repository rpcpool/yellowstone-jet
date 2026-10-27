use {
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::HashMap,
        io::Write as _,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::TcpListener,
    },
    tokio_rustls::{TlsAcceptor, rustls},
    tokio_util::sync::CancellationToken,
    url::Url,
    yellowstone_jet::{
        config::ConfigVaultIdentity,
        rpc::admin::JetIdentityUpdater,
        vault_identity::{LoadError, LoadStage, VaultIdentityLoader, run_vault_identity_loader},
    },
    yellowstone_jet_tpu_client::identity::HardenedKeypair,
};

const ROLE: &str = "jet-snp-testnet";
const ATTESTATION_TOKEN: &str = "ear.attested.token";
const VAULT_TOKEN: &str = "hvs.short-lived";
const IDENTITY_PATH: &str = "jet-testnet/data/identities/test";
const LEAKY_BODY: &str = "SECRET-ERROR-TEXT";

#[derive(Debug, Clone)]
struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct Response {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
}

impl Response {
    fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![("content-type", "application/json".to_owned())],
            body: body.into(),
        }
    }
}

type Handler = Arc<dyn Fn(&Request) -> Response + Send + Sync>;

/// A one-request-per-connection HTTP/1.1 server, with TLS when an acceptor is given.
async fn spawn_server(
    tls: Option<TlsAcceptor>,
    handler: Handler,
) -> (SocketAddr, Arc<Mutex<Vec<Request>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_task = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let handler = Arc::clone(&handler);
            let seen = Arc::clone(&seen_task);
            let tls = tls.clone();
            tokio::spawn(async move {
                match tls {
                    Some(acceptor) => {
                        if let Ok(stream) = acceptor.accept(stream).await {
                            serve(stream, handler, seen).await;
                        }
                    }
                    None => serve(stream, handler, seen).await,
                }
            });
        }
    });
    (addr, seen)
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    handler: Handler,
    seen: Arc<Mutex<Vec<Request>>>,
) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let Ok(n) = stream.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut start = lines.next().unwrap().split(' ');
    let method = start.next().unwrap().to_owned();
    let path = start.next().unwrap().to_owned();
    let headers: HashMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < length {
        let Ok(n) = stream.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    let request = Request {
        method,
        path,
        headers,
        body,
    };
    let response = handler(&request);
    seen.lock().unwrap().push(request);

    let mut out = Vec::new();
    write!(out, "HTTP/1.1 {} X\r\n", response.status).unwrap();
    for (name, value) in &response.headers {
        write!(out, "{name}: {value}\r\n").unwrap();
    }
    write!(
        out,
        "content-length: {}\r\nconnection: close\r\n\r\n{}",
        response.body.len(),
        response.body
    )
    .unwrap();
    let _ = stream.write_all(&out).await;
    let _ = stream.shutdown().await;
}

struct Pki {
    ca_pem: String,
    acceptor: TlsAcceptor,
}

fn pki() -> Pki {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let leaf = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
        )
        .unwrap();
    Pki {
        ca_pem: ca.pem(),
        acceptor: TlsAcceptor::from(Arc::new(config)),
    }
}

struct Fixture {
    config: ConfigVaultIdentity,
    vault_requests: Arc<Mutex<Vec<Request>>>,
    _ca_file: tempfile_path::TempPath,
}

/// Minimal temp file that is removed on drop, so the tests need no extra crate.
mod tempfile_path {
    pub struct TempPath(pub std::path::PathBuf);

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

async fn fixture(aa: Handler, vault: Handler) -> Fixture {
    let pki = pki();
    let (aa_addr, _) = spawn_server(None, aa).await;
    let (vault_addr, vault_requests) = spawn_server(Some(pki.acceptor), vault).await;
    let ca_path = std::env::temp_dir().join(format!("jet-vault-ca-{}.pem", rand_suffix()));
    std::fs::write(&ca_path, pki.ca_pem).unwrap();

    let config = ConfigVaultIdentity {
        address: Url::parse(&format!("https://localhost:{}", vault_addr.port())).unwrap(),
        ca_cert_file: Some(ca_path.clone()),
        auth_mount: "trustee".to_owned(),
        role: ROLE.to_owned(),
        identity_path: IDENTITY_PATH.to_owned(),
        attestation_agent_url: Url::parse(&format!(
            "http://127.0.0.1:{}/aa/token?token_type=kbs",
            aa_addr.port()
        ))
        .unwrap(),
        retry_min: Duration::from_millis(10),
        retry_max: Duration::from_millis(40),
    };
    Fixture {
        config,
        vault_requests,
        _ca_file: tempfile_path::TempPath(ca_path),
    }
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    u64::from(std::process::id()) << 32 | NEXT.fetch_add(1, Ordering::Relaxed)
}

fn attestation_agent() -> Handler {
    Arc::new(|request| {
        assert_eq!(request.path, "/aa/token?token_type=kbs");
        Response::json(
            200,
            format!(
                r#"{{"token":"{ATTESTATION_TOKEN}","tee_keypair":"-----BEGIN RSA PRIVATE KEY-----"}}"#
            ),
        )
    })
}

fn vault(keypair_bytes: Vec<u8>) -> Handler {
    let priv_key = serde_json::to_string(&keypair_bytes).unwrap();
    Arc::new(
        move |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v1/auth/trustee/login") => {
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                if body["role"] == ROLE && body["jwt"] == ATTESTATION_TOKEN {
                    Response::json(
                        200,
                        format!(r#"{{"auth":{{"client_token":"{VAULT_TOKEN}"}}}}"#),
                    )
                } else {
                    Response::json(403, format!(r#"{{"errors":["{LEAKY_BODY}"]}}"#))
                }
            }
            ("GET", path) if path == format!("/v1/{IDENTITY_PATH}") => {
                if request.headers.get("x-vault-token").map(String::as_str) == Some(VAULT_TOKEN) {
                    Response::json(
                        200,
                        serde_json::json!({ "data": { "data": { "priv_key": priv_key } } })
                            .to_string(),
                    )
                } else {
                    Response::json(403, format!(r#"{{"errors":["{LEAKY_BODY}"]}}"#))
                }
            }
            ("POST", "/v1/auth/token/revoke-self") => Response::json(204, ""),
            _ => Response::json(404, "{}"),
        },
    )
}

async fn load_error(loader: &VaultIdentityLoader) -> LoadError {
    match loader.load().await {
        Ok(identity) => panic!("unexpected identity {}", identity.pubkey()),
        Err(error) => error,
    }
}

fn paths(requests: &Arc<Mutex<Vec<Request>>>) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.path.clone())
        .collect()
}

#[tokio::test]
async fn loads_identity_after_attestation_and_revokes_token() {
    let keypair = Keypair::new();
    let f = fixture(attestation_agent(), vault(keypair.to_bytes().to_vec())).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), keypair.pubkey()).unwrap();

    let identity = loader.load().await.unwrap();

    assert_eq!(identity.pubkey(), keypair.pubkey());
    assert_eq!(
        paths(&f.vault_requests),
        vec![
            "/v1/auth/trustee/login".to_owned(),
            format!("/v1/{IDENTITY_PATH}"),
            "/v1/auth/token/revoke-self".to_owned(),
        ]
    );
}

#[tokio::test]
async fn rejected_login_does_not_leak_the_response_body() {
    let keypair = Keypair::new();
    let aa: Handler = Arc::new(|_| Response::json(200, r#"{"token":"forged"}"#));
    let f = fixture(aa, vault(keypair.to_bytes().to_vec())).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), keypair.pubkey()).unwrap();

    let error = load_error(&loader).await;

    assert_eq!(error.stage, LoadStage::Login);
    assert!(error.to_string().contains("HTTP 403"));
    assert!(!error.to_string().contains(LEAKY_BODY));
}

#[tokio::test]
async fn identity_must_match_expected() {
    let stored = Keypair::new();
    let f = fixture(attestation_agent(), vault(stored.to_bytes().to_vec())).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), Pubkey::new_unique()).unwrap();

    let error = load_error(&loader).await;

    assert_eq!(error.stage, LoadStage::Key);
    assert!(paths(&f.vault_requests).contains(&"/v1/auth/token/revoke-self".to_owned()));
}

#[tokio::test]
async fn vault_redirects_are_refused() {
    let (other_addr, other_requests) =
        spawn_server(None, Arc::new(|_| Response::json(200, "{}"))).await;
    let redirect: Handler = Arc::new(move |_| Response {
        status: 307,
        headers: vec![(
            "location",
            format!("http://{other_addr}/v1/auth/trustee/login"),
        )],
        body: String::new(),
    });
    let f = fixture(attestation_agent(), redirect).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), Pubkey::new_unique()).unwrap();

    let error = load_error(&loader).await;

    assert_eq!(error.stage, LoadStage::Login);
    assert!(other_requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn untrusted_vault_certificate_is_refused() {
    let keypair = Keypair::new();
    let mut f = fixture(attestation_agent(), vault(keypair.to_bytes().to_vec())).await;
    f.config.ca_cert_file = None;
    let loader = VaultIdentityLoader::new(f.config.clone(), keypair.pubkey()).unwrap();

    let error = load_error(&loader).await;

    assert_eq!(error.stage, LoadStage::Login);
    assert!(paths(&f.vault_requests).is_empty());
}

#[tokio::test]
async fn attestation_agent_failure_stops_before_vault() {
    let keypair = Keypair::new();
    let aa: Handler = Arc::new(|_| Response::json(500, LEAKY_BODY));
    let f = fixture(aa, vault(keypair.to_bytes().to_vec())).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), keypair.pubkey()).unwrap();

    let error = load_error(&loader).await;

    assert_eq!(error.stage, LoadStage::Attestation);
    assert!(!error.to_string().contains(LEAKY_BODY));
    assert!(paths(&f.vault_requests).is_empty());
}

#[tokio::test]
async fn config_checks() {
    let keypair = Keypair::new();
    let f = fixture(attestation_agent(), vault(keypair.to_bytes().to_vec())).await;
    let mut bad = Vec::new();

    let mut config = f.config.clone();
    config.address = Url::parse("http://localhost:8200").unwrap();
    bad.push(config);

    let mut config = f.config.clone();
    config.attestation_agent_url = Url::parse("http://10.1.2.3:8006/aa/token").unwrap();
    bad.push(config);

    let mut config = f.config.clone();
    config.identity_path = "jet-testnet/identities/test".to_owned();
    bad.push(config);

    let mut config = f.config.clone();
    config.auth_mount = "../sys".to_owned();
    bad.push(config);

    let mut config = f.config.clone();
    config.retry_min = Duration::ZERO;
    bad.push(config);

    for config in bad {
        assert!(VaultIdentityLoader::new(config, keypair.pubkey()).is_err());
    }
}

struct RecordingUpdater {
    current: Arc<Mutex<Option<Pubkey>>>,
}

#[async_trait::async_trait]
impl JetIdentityUpdater for RecordingUpdater {
    async fn update_identity(&mut self, identity: HardenedKeypair) {
        *self.current.lock().unwrap() = Some(identity.pubkey());
    }

    fn get_identity(&self) -> Pubkey {
        self.current.lock().unwrap().unwrap_or_default()
    }
}

#[tokio::test]
async fn loader_retries_installs_identity_and_waits_for_shutdown() {
    let keypair = Keypair::new();
    let attempts = Arc::new(Mutex::new(0u32));
    let attempts_aa = Arc::clone(&attempts);
    let aa: Handler = Arc::new(move |_| {
        let mut attempts = attempts_aa.lock().unwrap();
        *attempts += 1;
        if *attempts < 3 {
            Response::json(503, "{}")
        } else {
            Response::json(200, format!(r#"{{"token":"{ATTESTATION_TOKEN}"}}"#))
        }
    });
    let f = fixture(aa, vault(keypair.to_bytes().to_vec())).await;
    let loader = VaultIdentityLoader::new(f.config.clone(), keypair.pubkey()).unwrap();
    let current = Arc::new(Mutex::new(None));
    let updater = RecordingUpdater {
        current: Arc::clone(&current),
    };
    let cancel = CancellationToken::new();

    let task = tokio::spawn(run_vault_identity_loader(loader, updater, cancel.clone()));
    tokio::time::timeout(Duration::from_secs(10), async {
        while current.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("identity loaded");

    assert_eq!(*current.lock().unwrap(), Some(keypair.pubkey()));
    assert_eq!(*attempts.lock().unwrap(), 3);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !task.is_finished(),
        "the loader must not exit after loading"
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("loader stops on cancel")
        .unwrap();
}
