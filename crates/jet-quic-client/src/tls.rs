//! TLS: the client identity we present, how we validate jet's server certificate, and
//! building the `quinn::ClientConfig` that ties both together.

pub use rustls::RootCertStore;
use {
    crate::ConnectError,
    rustls::{
        DigitallySignedStruct, Error as RustlsError, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::CryptoProvider,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::Error as PemError},
    },
    std::{
        fmt::{self, Debug, Formatter},
        sync::Arc,
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

/// Builds a `quinn::ClientConfig` presenting `client_cert`/`client_key` as the mTLS
/// client identity, and validating jet's server certificate per `server_verification`.
/// Extracted as a standalone function (rather than tied to any connecting/connector
/// type) so it can be called once and the result reused across many
/// [`JetQuicEndpoint::connect`](crate::JetQuicEndpoint::connect) calls.
pub fn default_client_config(
    client_cert: CertificateDer<'static>,
    client_key: PrivateKeyDer<'static>,
    server_verification: ServerVerification,
) -> Result<quinn::ClientConfig, ConnectError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = build_server_verifier(server_verification, Arc::clone(&provider))?;

    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(ConnectError::Tls)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![client_cert], client_key)?;
    crypto.alpn_protocols = vec![crate::ALPN_JET_RAW_TX_PROTOCOL_ID.to_vec()];

    let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| ConnectError::Tls(RustlsError::General(e.to_string())))?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_client_config)))
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
