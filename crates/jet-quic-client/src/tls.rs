//! TLS: the client identity we present, how we validate jet's server certificate, and
//! building the `quinn::ClientConfig` that ties both together.

pub use rustls::RootCertStore;
use {
    crate::ConnectError,
    quinn::{IdleTimeout, TransportConfig},
    rustls::{
        DigitallySignedStruct, Error as RustlsError, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::CryptoProvider,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::Error as PemError},
    },
    std::{
        fmt::{self, Debug, Formatter},
        sync::Arc,
        time::Duration,
    },
};

/// How the client should validate jet's server certificate.
#[derive(Clone)]
pub enum ServerVerification {
    /// Standard root-of-trust validation against the supplied root store.
    WebPki(Arc<RootCertStore>),
    /// Accept only a single, exact, pre-shared server certificate. Useful when jet's
    /// server cert is self-signed / not chained to a public CA.
    Pinned(CertificateDer<'static>),
    /// Skip server certificate validation entirely. **Dev/test only** — provides no
    /// protection against a MITM impersonating the server.
    Insecure,
}

impl ServerVerification {
    /// Standard root-of-trust validation against the OS's native root store (see
    /// [`native_root_cert_store`]) -- the same trust a browser would use.
    pub fn with_native_root_cert_store() -> Self {
        Self::WebPki(Arc::new(native_root_cert_store()))
    }
}

/// The client's mTLS identity presented to jet's server during the handshake: the
/// certificate and the private key that signs for it.
#[derive(Debug)]
pub struct ClientIdentity {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

impl ClientIdentity {
    /// Generates a fresh, random self-signed certificate/key pair. For tests and local
    /// development against a server that doesn't check the client certificate's
    /// identity, just that the mTLS handshake itself succeeds -- not a substitute for a
    /// real, provisioned identity in production, since jet's server is free to reject
    /// unrecognized client certificates.
    pub fn random() -> Result<Self, rcgen::Error> {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["jet-client.invalid".to_owned()])?;
        Ok(Self {
            cert: load_cert_pem(cert.pem().as_bytes())
                .expect("freshly generated certificate PEM should always parse"),
            key: load_key_pem(key_pair.serialize_pem().as_bytes())
                .expect("freshly generated key PEM should always parse"),
        })
    }
}

/// Builds a `quinn::ClientConfig` presenting `identity` as the mTLS client identity,
/// and validating jet's server certificate against the OS's native root store (see
/// [`native_root_cert_store`]) -- the same trust a browser would use. Use
/// [`client_config_with_verification`] instead if jet's server certificate needs a
/// pinned certificate, a private/custom CA, or (dev/test only) no verification at all.
pub fn default_client_config(
    identity: ClientIdentity,
) -> Result<quinn::ClientConfig, ConnectError> {
    client_config_with_verification(identity, ServerVerification::with_native_root_cert_store())
}

/// Builds a `quinn::ClientConfig` presenting `identity` as the mTLS client identity,
/// and validating jet's server certificate per `server_verification`. Extracted as a
/// standalone function (rather than tied to any connecting/connector type) so it can
/// be called once and the result reused across many
/// [`JetQuicEndpoint::connect`](crate::JetQuicEndpoint::connect) calls.
pub fn client_config_with_verification(
    identity: ClientIdentity,
    server_verification: ServerVerification,
) -> Result<quinn::ClientConfig, ConnectError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = build_server_verifier(server_verification, Arc::clone(&provider))?;

    let transport_config = {
        let mut res = TransportConfig::default();

        let max_idle_timeout = IdleTimeout::try_from(Duration::from_secs(20))
            .expect("Failed to set QUIC max idle timeout");
        res.max_idle_timeout(Some(max_idle_timeout));
        res.keep_alive_interval(Some(Duration::from_secs(2)));
        // We don't want fairness : https://github.com/quinn-rs/quinn/pull/2002
        // Fairness use round-robin scheduling to write stream data into the next frame.
        // Disabling fairness makes that once a stream starts to write it won't be interrupted by round-robin.
        // This reduce the time the receive the (fin) "end" of a transaction, thus reducing latency.
        res.send_fairness(false);
        res
    };

    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(ConnectError::Tls)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![identity.cert], identity.key)?;
    crypto.alpn_protocols = vec![crate::ALPN_JET_RAW_TX_PROTOCOL_ID.to_vec()];

    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| ConnectError::Tls(RustlsError::General(e.to_string())))?;
    let mut quic_client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));
    quic_client_config.transport_config(Arc::new(transport_config));
    Ok(quic_client_config)
}

fn build_server_verifier(
    verification: ServerVerification,
    provider: Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, ConnectError> {
    match verification {
        ServerVerification::WebPki(roots) => {
            rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
                .build()
                .map(|v| v as Arc<dyn ServerCertVerifier>)
                .map_err(|e| ConnectError::Tls(RustlsError::General(e.to_string())))
        }
        ServerVerification::Pinned(expected) => {
            Ok(Arc::new(PinnedServerCertVerifier { expected, provider }))
        }
        ServerVerification::Insecure => Ok(Arc::new(InsecureServerCertVerifier { provider })),
    }
}

/// Parses a PEM-encoded X.509 certificate (e.g. read from a customer's cert file).
pub fn load_cert_pem(pem: &[u8]) -> Result<CertificateDer<'static>, PemError> {
    use rustls::pki_types::pem::PemObject;
    CertificateDer::from_pem_slice(pem)
}

/// Parses a PEM-encoded private key (PKCS#8, PKCS#1, or SEC1).
pub fn load_key_pem(pem: &[u8]) -> Result<PrivateKeyDer<'static>, PemError> {
    use rustls::pki_types::pem::PemObject;
    PrivateKeyDer::from_pem_slice(pem)
}

/// Builds a [`RootCertStore`] from the OS's own trust store -- the certificates the
/// platform already trusts (`/etc/ssl/certs` on Linux, Keychain on macOS, the Windows
/// certificate store, etc.), for use with [`ServerVerification::WebPki`] when jet's
/// server certificate chains to a public/well-known CA rather than a pinned or
/// private one.
///
/// Certificates that fail to parse are skipped rather than treated as a hard failure --
/// a single unreadable entry in a large native store shouldn't prevent using the rest
/// of it.
pub fn native_root_cert_store() -> RootCertStore {
    let native_certs = rustls_native_certs::load_native_certs();
    let mut store = RootCertStore::empty();
    store.add_parsable_certificates(native_certs.certs);
    store
}

struct InsecureServerCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
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
}

impl Debug for InsecureServerCertVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InsecureServerCertVerifier")
            .finish_non_exhaustive()
    }
}

struct PinnedServerCertVerifier {
    expected: CertificateDer<'static>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        if end_entity.as_ref() == self.expected.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(RustlsError::General(
                "server certificate does not match pinned certificate".to_owned(),
            ))
        }
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
}

impl Debug for PinnedServerCertVerifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedServerCertVerifier")
            .finish_non_exhaustive()
    }
}
