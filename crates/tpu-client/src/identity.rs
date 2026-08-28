//!
//! [`TpuIdentity`]: the TPU sender's notion of "who am I" -- a [`Pubkey`] paired with the
//! derived QUIC client crypto config used to authenticate connections to remote peers.
//!

use {
    crate::core::{ALPN_TPU_PROTOCOL_ID, crypto_provider},
    bytes::BufMut,
    ed25519_dalek::{SignatureError, Signer as DalekSigner},
    quinn::{
        ConnectError,
        crypto::{ClientConfig as QuicCryptoClientConfig, Session, rustls::QuicClientConfig},
    },
    quinn_proto::transport_parameters::TransportParameters,
    rustls::{
        SignatureScheme,
        pki_types::CertificateDer,
        sign::{CertifiedKey, Signer as RustlsSigner, SigningKey, SingleCertAndKey},
    },
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        fmt,
        fs::File,
        io::{self, Read},
        path::Path,
        sync::Arc,
    },
    zeroize::Zeroize,
};

///
/// Owns the one authoritative copy of the private key used to sign the TLS handshake.
///
/// This holds the PKCS#8-formatted bytes (a 16-byte ASN.1 prefix followed by the raw 32-byte
/// Ed25519 seed) that [`new_dummy_x509_certificate`] writes directly into -- the caller mlocks
/// this exact buffer *before* the secret is ever copied into it, so the raw key never exists in
/// any other allocation, not even transiently.
///
/// Declaring `_key_lock` after `pkcs8_der_bytes` isn't load-bearing here (we implement `Drop`
/// ourselves, in the correct zeroize-then-unlock order, rather than relying on field order).
///
struct KeypairMaterial {
    pkcs8_der_bytes: Box<[u8]>,
    _key_lock: Option<region::LockGuard>,
}

impl Zeroize for KeypairMaterial {
    fn zeroize(&mut self) {
        self.pkcs8_der_bytes.zeroize();
    }
}

impl Drop for KeypairMaterial {
    fn drop(&mut self) {
        self.zeroize();
        self._key_lock.take();
    }
}

impl KeypairMaterial {
    /// The raw 32-byte Ed25519 seed, i.e. `pkcs8_der_bytes` with its fixed ASN.1 prefix skipped.
    fn secret_bytes(&self) -> &[u8; 32] {
        self.pkcs8_der_bytes[PKCS8_PREFIX.len()..]
            .try_into()
            .expect("pkcs8_der_bytes has a fixed, checked length")
    }
}

///
/// A `rustls` [`SigningKey`] that signs directly from a mlocked [`KeypairMaterial`].
///
/// This exists so the TLS client-auth signature is produced straight from our own protected
/// copy of the secret, instead of going through `rustls`'s standard
/// `ConfigBuilder::with_client_auth_cert` path -- which would hand the key to the configured
/// `CryptoProvider`'s `KeyProvider::load_private_key()` (`ring`, here) and leave a second,
/// unlocked, never-zeroized copy of the secret parsed into `ring`'s own key representation for
/// as long as the resulting config is in use.
///
/// Each signature reconstructs an `ed25519_dalek::SigningKey` from the mlocked seed for the
/// duration of a single `sign()` call; that transient value has its own `ZeroizeOnDrop` and is
/// dropped (and wiped) the moment the signature is produced, so it's never held any longer than
/// one handshake needs it.
///
#[derive(Clone)]
struct MlockedSigningKey {
    material: Arc<KeypairMaterial>,
}

impl fmt::Debug for MlockedSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MlockedSigningKey").finish_non_exhaustive()
    }
}

impl SigningKey for MlockedSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn RustlsSigner>> {
        offered.contains(&SignatureScheme::ED25519).then(|| {
            Box::new(MlockedSigner {
                material: Arc::clone(&self.material),
            }) as Box<dyn RustlsSigner>
        })
    }

    fn algorithm(&self) -> rustls::SignatureAlgorithm {
        rustls::SignatureAlgorithm::ED25519
    }
}

struct MlockedSigner {
    material: Arc<KeypairMaterial>,
}

impl fmt::Debug for MlockedSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MlockedSigner").finish_non_exhaustive()
    }
}

impl RustlsSigner for MlockedSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(self.material.secret_bytes());
        Ok(signing_key.sign(message).to_bytes().to_vec())
    }

    fn scheme(&self) -> SignatureScheme {
        SignatureScheme::ED25519
    }
}

struct TpuIdentityInner {
    pubkey: Pubkey,
    quic_client_config: Arc<QuicClientConfig>,
}

///
/// The TPU sender's identity: a [`Pubkey`] plus the QUIC client crypto config derived from the
/// [`Keypair`] that produced it.
///
/// Cheap to [`Clone`] (an `Arc` bump) -- pass it around by value rather than wrapping it in an
/// `Arc` yourself.
///
/// Once built via [`TpuIdentity::from_keypair`], nothing downstream needs the originating
/// [`Keypair`] again: the `rustls`/`quinn` client crypto config is constructed exactly once
/// here and reused for every connection, rather than being rebuilt on every connect attempt.
///
/// # Memory hardening
///
/// The private key is signed with directly out of a dedicated, mlocked [`KeypairMaterial`]
/// (see [`MlockedSigningKey`]) -- it never passes through `rustls`'s standard certificate-key
/// loading path, so no copy of it ever ends up unprotected inside `ring`'s internals. The DER
/// encoding of the key is also written directly into that same mlocked buffer by our own
/// [`new_dummy_x509_certificate`] (rather than `solana-tls-utils`'s, which would allocate and
/// return it in an ordinary, unlocked `Vec`), so the secret never exists outside mlocked memory
/// at any point, even momentarily.
///
pub struct TpuIdentity {
    inner: Arc<TpuIdentityInner>,
}

pub trait TpuEd25519SigningKey {
    fn secret_bytes(&self) -> &[u8; 32];
    fn solana_address(&self) -> Pubkey;
}

impl TpuEd25519SigningKey for ed25519_dalek::SigningKey {
    fn secret_bytes(&self) -> &[u8; 32] {
        self.as_bytes()
    }

    fn solana_address(&self) -> Pubkey {
        let pubkey = self.verifying_key().to_bytes();
        Pubkey::new_from_array(pubkey)
    }
}

impl TpuEd25519SigningKey for Keypair {
    fn secret_bytes(&self) -> &[u8; 32] {
        self.secret_bytes()
    }

    fn solana_address(&self) -> Pubkey {
        Signer::pubkey(self)
    }
}

impl TpuEd25519SigningKey for HardenedKeypair {
    fn secret_bytes(&self) -> &[u8; 32] {
        &self.mlocked_private_bytes
    }

    fn solana_address(&self) -> Pubkey {
        Pubkey::new_from_array(self.public_bytes)
    }
}

impl TpuIdentity {
    pub(crate) fn insecure_clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn from_ed25519_signing_key(signing_key: &dyn TpuEd25519SigningKey) -> Self {
        let pubkey = signing_key.solana_address();
        let mut cert_der_bytes = Vec::<u8>::with_capacity(CERT_TOTAL_LEN);
        let mut pkcs8_der_bytes = vec![0u8; PKCS8_TOTAL_LEN].into_boxed_slice();

        // Lock the destination *before* anything is written into it, so the secret is mlocked
        // from the instant it first exists.
        let _key_lock = region::lock(pkcs8_der_bytes.as_ptr(), pkcs8_der_bytes.len())
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock TPU identity private key for {pubkey}: {error}; secret may be swapped to disk"
                );
            })
            .ok();

        let mut pkcs8_cursor: &mut [u8] = &mut pkcs8_der_bytes;
        new_dummy_x509_certificate(signing_key, &mut cert_der_bytes, &mut pkcs8_cursor);

        let material = Arc::new(KeypairMaterial {
            pkcs8_der_bytes,
            _key_lock,
        });
        let signing_key: Arc<dyn SigningKey> = Arc::new(MlockedSigningKey { material });
        let certified_key =
            CertifiedKey::new(vec![CertificateDer::from(cert_der_bytes)], signing_key);
        let resolver = Arc::new(SingleCertAndKey::from(certified_key));

        let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("Failed to set QUIC client protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(solana_tls_utils::SkipServerVerification::new())
            .with_client_cert_resolver(resolver);
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let quic_client_config = Arc::new(
            QuicClientConfig::try_from(crypto).expect("Failed to build QUIC client config"),
        );

        Self {
            inner: Arc::new(TpuIdentityInner {
                pubkey,
                quic_client_config,
            }),
        }
    }

    ///
    /// Derives a [`TpuIdentity`] from a [`Keypair`].
    ///
    pub fn from_keypair(keypair: &Keypair) -> Self {
        Self::from_ed25519_signing_key(keypair)
    }

    ///
    /// The public key of this identity.
    ///
    pub fn pubkey(&self) -> Pubkey {
        self.inner.pubkey
    }
}

///
/// A growable buffer whose backing allocation is always `mlock`ed.
///
/// Growing allocates a fresh, larger buffer, locks it *before* copying anything into it, moves
/// the existing bytes over, and zeroizes the old allocation before it's dropped (which unlocks
/// it) -- so at no point does a byte that was ever written here exist in memory that isn't
/// mlocked, not even during a resize.
///
struct GrowableHardenedBuffer {
    buffer: Box<[u8]>,
    len: usize,
    _lock: Option<region::LockGuard>,
}

impl GrowableHardenedBuffer {
    /// Absolute cap on how large this buffer will grow. mlock'ing an unbounded amount of memory
    /// is itself a resource-exhaustion risk, so growth still needs *some* ceiling even though it
    /// isn't fixed upfront -- this is far larger than any real keypair file needs.
    const MAX_LEN: usize = 64 * 1024;

    fn with_capacity(capacity: usize) -> Self {
        let buffer = vec![0u8; capacity].into_boxed_slice();
        let _lock = Self::lock(&buffer);
        Self {
            buffer,
            len: 0,
            _lock,
        }
    }

    fn lock(buffer: &[u8]) -> Option<region::LockGuard> {
        region::lock(buffer.as_ptr(), buffer.len())
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock a {}-byte buffer: {error}; secret may be swapped to disk",
                    buffer.len()
                );
            })
            .ok()
    }

    /// Replaces the backing allocation with a new, larger, freshly locked one; copies the bytes
    /// written so far into it; and wipes the old allocation before dropping it.
    fn grow(&mut self) {
        let new_capacity = (self.buffer.len() * 2).min(Self::MAX_LEN);
        let mut new_buffer = vec![0u8; new_capacity].into_boxed_slice();
        let new_lock = Self::lock(&new_buffer);
        new_buffer[..self.len].copy_from_slice(&self.buffer[..self.len]);
        self.buffer.zeroize();
        self.buffer = new_buffer;
        self._lock = new_lock;
    }

    /// Reads more bytes from `reader`, growing first if the buffer is currently full. Returns
    /// `Ok(0)` exactly when `reader` has reached EOF -- since we never hand `read` a zero-length
    /// destination (we grow beforehand instead), a `0` here is unambiguous.
    fn read_from<R: Read>(&mut self, reader: &mut R) -> io::Result<usize> {
        if self.len == self.buffer.len() {
            if self.buffer.len() >= Self::MAX_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "input exceeds maximum expected size",
                ));
            }
            self.grow();
        }
        let n = reader.read(&mut self.buffer[self.len..])?;
        self.len += n;
        Ok(n)
    }

    fn as_slice(&self) -> &[u8] {
        &self.buffer[..self.len]
    }
}

impl Zeroize for GrowableHardenedBuffer {
    fn zeroize(&mut self) {
        self.buffer.zeroize();
    }
}

impl Drop for GrowableHardenedBuffer {
    fn drop(&mut self) {
        self.zeroize();
        self._lock.take();
    }
}

impl QuicCryptoClientConfig for TpuIdentity {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn Session>, ConnectError> {
        Arc::clone(&self.inner.quic_client_config).start_session(version, server_name, params)
    }
}

const PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

const PKCS8_TOTAL_LEN: usize = PKCS8_PREFIX.len() + 32;
const CERT_TOTAL_LEN: usize = 0xf9;

///
/// A keypair read from disk (or any [`Read`]er) whose secret bytes are `mlock`ed from the
/// moment they first exist and zeroized on drop -- unlike [`solana_keypair::read_keypair`],
/// which reads the whole input into an ordinary, unlocked `String` before parsing it (see
/// [`HardenedKeypair::read_from_reader`] for the details).
///
/// Since this implements [`TpuEd25519SigningKey`], it can be handed directly to
/// [`TpuIdentity::from_ed25519_signing_key`] -- so a keypair loaded this way never needs to
/// pass through a plain, unlocked [`solana_keypair::Keypair`] at all on its way to becoming a
/// [`TpuIdentity`].
///
pub struct HardenedKeypair {
    public_bytes: [u8; 32],
    mlocked_private_bytes: Box<[u8; 32]>,
    _lock: Option<region::LockGuard>,
}

impl HardenedKeypair {
    ///
    /// Reads a JSON-encoded keypair (`[n0,...,n63]`: 32 secret bytes then 32 public bytes) from
    /// `reader`, without ever copying the secret into memory that isn't mlocked.
    ///
    /// The raw file text is read directly into a mlocked scratch buffer instead of a `String`;
    /// `str::trim`/`str::split` on it only ever produce borrowed `&str` views into that same
    /// buffer, never a new allocation. The 32 secret bytes it parses out are written straight
    /// into the (already mlocked) `mlocked_private_bytes` destination -- at no point does the
    /// secret exist anywhere else, not even transiently.
    ///
    pub fn read_from_reader<R: Read>(reader: &mut R) -> Result<Self, io::Error> {
        // A typical keypair file is ~200-235 bytes; this initial capacity avoids a grow in the
        // common case while staying small. `GrowableHardenedBuffer` takes it from here if the
        // input turns out to be bigger.
        let mut file_buf = GrowableHardenedBuffer::with_capacity(256);
        loop {
            let n = file_buf.read_from(reader)?;
            if n == 0 {
                // EOF: `GrowableHardenedBuffer::read_from` never passes `read` an empty
                // destination, so a `0` here is unambiguous.
                break;
            }
        }

        let text = std::str::from_utf8(file_buf.as_slice())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "input is not valid utf8"))?;
        let trimmed = text.trim();
        if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "input must be a JSON array",
            ));
        }
        // we already checked that the string has at least two chars (the brackets), so this
        // range is in bounds.
        #[allow(clippy::arithmetic_side_effects)]
        let contents = &trimmed[1..trimmed.len() - 1];

        let mut public_bytes = [0u8; 32];
        let mut mlocked_private_bytes = Box::new([0u8; 32]);
        let _lock = region::lock(mlocked_private_bytes.as_ptr(), 32)
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock keypair private key: {error}; secret may be swapped to disk"
                );
            })
            .ok();

        let mut count = 0usize;
        for part in contents.split(',') {
            if count >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected 64 elements",
                ));
            }
            let byte: u8 = part
                .trim()
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid byte value"))?;
            if count < 32 {
                mlocked_private_bytes[count] = byte;
            } else {
                public_bytes[count - 32] = byte;
            }
            count += 1;
        }
        if count != 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected 64 elements, found {count}"),
            ));
        }

        // The file text (which also contained the secret, spelled out in ASCII) is no longer
        // needed -- wipe it now rather than waiting for `_file_lock`'s buffer to be dropped.
        file_buf.zeroize();

        Ok(Self {
            public_bytes,
            mlocked_private_bytes,
            _lock,
        })
    }

    ///
    /// Reads a JSON-encoded keypair from a file the same way [`HardenedKeypair::read_from_reader`]
    /// does.
    ///
    pub fn read_from_file<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let mut file = File::open(path)?;
        Self::read_from_reader(&mut file)
    }

    pub const fn public_bytes(&self) -> &[u8; 32] {
        &self.public_bytes
    }

    pub const fn pubkey(&self) -> Pubkey {
        Pubkey::new_from_array(self.public_bytes)
    }

    pub fn private_bytes(&self) -> &[u8; 32] {
        &self.mlocked_private_bytes
    }

    pub fn insecure_clone(&self) -> HardenedKeypair {
        let mut private_copy = Box::new([0u8; 32]);
        let lock = region::lock(private_copy.as_ptr(), 32)
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock cloned keypair private key: {error}; secret may be swapped to disk"
                );
            })
            .ok();
        // write the private key bytes into the newly allocated, mlocked buffer
        private_copy[..32].copy_from_slice(self.mlocked_private_bytes.as_slice());
        HardenedKeypair {
            public_bytes: self.public_bytes,
            mlocked_private_bytes: private_copy,
            _lock: lock,
        }
    }

    pub fn from_keypair(keypair: &Keypair) -> Self {
        let mut private_copy = Box::new([0u8; 32]);
        private_copy[..32].copy_from_slice(&keypair.to_bytes()[..32]);
        let lock = region::lock(private_copy.as_ptr(), 32)
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock keypair private key: {error}; secret may be swapped to disk"
                );
            })
            .ok();
        Self {
            public_bytes: keypair.pubkey().to_bytes(),
            mlocked_private_bytes: private_copy,
            _lock: lock,
        }
    }

    pub fn new() -> Self {
        let keypair = Keypair::new();
        Self::from_keypair(&keypair)
    }
}

impl Default for HardenedKeypair {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&[u8]> for HardenedKeypair {
    type Error = SignatureError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        // Scratch buffer for the candidate 64-byte keypair (secret half + public half), mlocked
        // before anything is copied into it.
        let mut data = Box::new([0u8; ed25519_dalek::KEYPAIR_LENGTH]);
        let _data_lock = region::lock(data.as_ptr(), ed25519_dalek::KEYPAIR_LENGTH)
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock keypair private key: {error}; secret may be swapped to disk"
                );
            })
            .ok();

        let bytes: &[u8; ed25519_dalek::KEYPAIR_LENGTH] = bytes.try_into().map_err(|_| {
            SignatureError::from_source(String::from(
                "candidate keypair byte array is the wrong length",
            ))
        })?;
        data[..].copy_from_slice(bytes);

        // Reject a corrupted or tampered keypair file where the public half doesn't actually
        // correspond to the secret half -- the same check `ed25519-dalek` itself does when
        // building a `SigningKey` from raw keypair bytes. `signing_key` is a transient copy of
        // the secret (it has its own `ZeroizeOnDrop`), so it doesn't leave a second unlocked
        // copy behind once we're done with it below.
        let signing_key = ed25519_dalek::SigningKey::from_keypair_bytes(&data)?;
        let public_bytes = signing_key.verifying_key().to_bytes();
        drop(signing_key);

        let mut mlocked_private_bytes = Box::new([0u8; 32]);
        let _lock = region::lock(mlocked_private_bytes.as_ptr(), 32)
            .inspect_err(|error| {
                tracing::warn!(
                    "failed to mlock keypair private key: {error}; secret may be swapped to disk"
                );
            })
            .ok();
        mlocked_private_bytes.copy_from_slice(&data[..32]);

        // The scratch copy of the secret is no longer needed -- wipe it now rather than waiting
        // for `_data_lock`'s buffer to be dropped.
        data.zeroize();

        Ok(Self {
            public_bytes,
            mlocked_private_bytes,
            _lock,
        })
    }
}

impl Zeroize for HardenedKeypair {
    fn zeroize(&mut self) {
        self.mlocked_private_bytes.as_mut().zeroize();
    }
}

impl Drop for HardenedKeypair {
    fn drop(&mut self) {
        self.zeroize();
        self._lock.take();
    }
}

///
/// Builds a self-signed X.509 certificate (into `cert_der_bytes`) and a PKCS#8-encoded private
/// key (into `private_key_der_bytes`) for `keypair`, writing directly into caller-supplied
/// buffers instead of allocating and returning fresh ones.
///
/// This is a copy of `solana_tls_utils::new_dummy_x509_certificate`'s ASN.1 byte layout, changed
/// only so the private key destination can be a buffer the caller has already `mlock`ed -- the
/// upstream version builds and returns an ordinary, unlocked `Vec` for it, which is exactly the
/// kind of stray copy [`TpuIdentity`] is trying to avoid. Only the `SubjectPublicKeyInfo` field
/// of the certificate is meaningful to Solana's QUIC peers; its signature is deliberately invalid
/// (peer authenticity is established by the TLS 1.3 `CertificateVerify`, not by the cert itself).
///
fn new_dummy_x509_certificate<BUF1, BUF2>(
    signing_key: &dyn TpuEd25519SigningKey,
    cert_der_bytes: &mut BUF1,
    private_key_der_bytes: &mut BUF2,
) where
    BUF1: BufMut,
    BUF2: BufMut,
{
    assert!(cert_der_bytes.remaining_mut() >= CERT_TOTAL_LEN);
    assert!(private_key_der_bytes.remaining_mut() >= PKCS8_TOTAL_LEN);
    let secret_bytes = signing_key.secret_bytes();
    // Convert private key into PKCS#8 v1 object.
    // RFC 8410, Section 7: Private Key Format
    // https://www.rfc-editor.org/rfc/rfc8410#section-7
    //
    // The hardcoded prefix decodes to the following ASN.1 structure:
    //
    //   PrivateKeyInfo SEQUENCE (3 elem)
    //     version Version INTEGER 0
    //     privateKeyAlgorithm AlgorithmIdentifier SEQUENCE (1 elem)
    //       algorithm OBJECT IDENTIFIER 1.3.101.112 curveEd25519 (EdDSA 25519 signature algorithm)
    //     privateKey PrivateKey OCTET STRING (34 byte)
    {
        private_key_der_bytes.chunk_mut()[..PKCS8_PREFIX.len()].copy_from_slice(&PKCS8_PREFIX);
        unsafe { private_key_der_bytes.advance_mut(PKCS8_PREFIX.len()) };
        private_key_der_bytes.chunk_mut()[..secret_bytes.len()].copy_from_slice(secret_bytes);
        unsafe { private_key_der_bytes.advance_mut(secret_bytes.len()) };
    }

    // Create a dummy certificate. Only the SubjectPublicKeyInfo field
    // is relevant to the peer-to-peer protocols. The signature of the
    // X.509 certificate is deliberately invalid. (Peer authenticity is
    // checked in the TLS 1.3 CertificateVerify)
    // See https://www.itu.int/rec/T-REC-X.509-201910-I/en for detailed definitions.
    //
    //    Certificate SEQUENCE (3 elem)
    //      tbsCertificate TBSCertificate SEQUENCE (8 elem)
    //        version [0] (1 elem)
    //          INTEGER  2
    //        serialNumber CertificateSerialNumber INTEGER (62 bit)
    //        signature AlgorithmIdentifier SEQUENCE (1 elem)
    //          algorithm OBJECT IDENTIFIER 1.3.101.112 curveEd25519 (EdDSA 25519 signature algorithm)
    //        issuer Name SEQUENCE (1 elem)
    //          RelativeDistinguishedName SET (1 elem)
    //            AttributeTypeAndValue SEQUENCE (2 elem)
    //              type AttributeType OBJECT IDENTIFIER 2.5.4.3 commonName (X.520 DN component)
    //              value AttributeValue [?] UTF8String Solana
    //        validity Validity SEQUENCE (2 elem)
    //          notBefore Time UTCTime 1970-01-01 00:00:00 UTC
    //          notAfter Time GeneralizedTime 4096-01-01 00:00:00 UTC
    //        subject Name SEQUENCE (0 elem)
    //        subjectPublicKeyInfo SubjectPublicKeyInfo SEQUENCE (2 elem)
    //          algorithm AlgorithmIdentifier SEQUENCE (1 elem)
    //            algorithm OBJECT IDENTIFIER 1.3.101.112 curveEd25519 (EdDSA 25519 signature algorithm)
    //          subjectPublicKey BIT STRING (256 bit)
    const CERT_DER_PREFIX: [u8; 100] = [
        0x30, 0x81, 0xf6, 0x30, 0x81, 0xa9, 0xa0, 0x03, 0x02, 0x01, 0x02, 0x02, 0x08, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x30, 0x16,
        0x31, 0x14, 0x30, 0x12, 0x06, 0x03, 0x55, 0x04, 0x03, 0x0c, 0x0b, 0x53, 0x6f, 0x6c, 0x61,
        0x6e, 0x61, 0x20, 0x6e, 0x6f, 0x64, 0x65, 0x30, 0x20, 0x17, 0x0d, 0x37, 0x30, 0x30, 0x31,
        0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x5a, 0x18, 0x0f, 0x34, 0x30, 0x39, 0x36,
        0x30, 0x31, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x5a, 0x30, 0x00, 0x30, 0x2a,
        0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    cert_der_bytes.chunk_mut()[..CERT_DER_PREFIX.len()].copy_from_slice(&CERT_DER_PREFIX);
    unsafe {
        cert_der_bytes.advance_mut(CERT_DER_PREFIX.len());
    }
    let pubkey_bytes = signing_key.solana_address().to_bytes();
    cert_der_bytes.chunk_mut()[..pubkey_bytes.len()].copy_from_slice(&pubkey_bytes);
    unsafe {
        cert_der_bytes.advance_mut(pubkey_bytes.len());
    }

    //        extensions [3] (1 elem)
    //          Extensions SEQUENCE (2 elem)
    //            Extension SEQUENCE (3 elem)
    //              extnID OBJECT IDENTIFIER 2.5.29.17 subjectAltName (X.509 extension)
    //              critical BOOLEAN true
    //              extnValue OCTET STRING (13 byte) encapsulating
    //                SEQUENCE (1 elem)
    //                [2] (9 byte) localhost
    //            Extension SEQUENCE (3 elem)
    //              extnID OBJECT IDENTIFIER 2.5.29.19 basicConstraints (X.509 extension)
    //              critical BOOLEAN true
    //              extnValue OCTET STRING (2 byte) encapsulating
    //                SEQUENCE (0 elem)
    //      signatureAlgorithm AlgorithmIdentifier SEQUENCE (1 elem)
    //        algorithm OBJECT IDENTIFIER 1.3.101.112 curveEd25519 (EdDSA 25519 signature algorithm)
    //        signature BIT STRING (512 bit)
    const CERT_DER_EXTENSION: [u8; 117] = [
        0xa3, 0x29, 0x30, 0x27, 0x30, 0x17, 0x06, 0x03, 0x55, 0x1d, 0x11, 0x01, 0x01, 0xff, 0x04,
        0x0d, 0x30, 0x0b, 0x82, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x30,
        0x0c, 0x06, 0x03, 0x55, 0x1d, 0x13, 0x01, 0x01, 0xff, 0x04, 0x02, 0x30, 0x00, 0x30, 0x05,
        0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x41, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    ];
    cert_der_bytes.chunk_mut()[..CERT_DER_EXTENSION.len()].copy_from_slice(&CERT_DER_EXTENSION);
    unsafe {
        cert_der_bytes.advance_mut(CERT_DER_EXTENSION.len());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn from_keypair_preserves_pubkey() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        let identity = TpuIdentity::from_keypair(&keypair);
        assert_eq!(identity.pubkey(), pubkey);
    }

    /// Our own `new_dummy_x509_certificate` is a copy of
    /// `solana_tls_utils::new_dummy_x509_certificate`'s ASN.1 byte layout, changed only so the
    /// private key can be written directly into a caller-mlocked buffer instead of an ordinary,
    /// unlocked `Vec`. This checks the two produce byte-for-byte identical output, so a drift
    /// between the two implementations (e.g. an upstream change to the cert layout) gets caught
    /// here instead of silently producing a certificate or key encoding that differs from what
    /// `solana-tls-utils` (and thus the rest of the Solana ecosystem) expects.
    #[test]
    fn new_dummy_x509_certificate_matches_solana_tls_utils() {
        let keypair = Keypair::new();

        let mut cert_der_bytes = Vec::<u8>::with_capacity(CERT_TOTAL_LEN);
        let mut pkcs8_der_bytes = vec![0u8; PKCS8_TOTAL_LEN].into_boxed_slice();
        let mut pkcs8_cursor: &mut [u8] = &mut pkcs8_der_bytes;
        new_dummy_x509_certificate(&keypair, &mut cert_der_bytes, &mut pkcs8_cursor);

        let (solana_cert, solana_key) = solana_tls_utils::new_dummy_x509_certificate(&keypair);

        assert_eq!(
            cert_der_bytes.as_slice(),
            solana_cert.as_ref(),
            "certificate DER bytes differ from solana_tls_utils::new_dummy_x509_certificate"
        );
        assert_eq!(
            pkcs8_der_bytes.as_ref(),
            solana_key.secret_der(),
            "PKCS8 private key DER bytes differ from solana_tls_utils::new_dummy_x509_certificate"
        );
    }

    /// Ed25519 signing is deterministic (RFC 8032): for the same key and message, any
    /// spec-compliant implementation must produce the exact same signature bytes. This checks
    /// that our [`MlockedSigningKey`] (backed by `ed25519-dalek`) agrees byte-for-byte with
    /// `ring`'s own EdDSA signer -- the same one `rustls`'s standard
    /// `ConfigBuilder::with_client_auth_cert` path would have used had we gone through
    /// `KeyProvider::load_private_key()` instead of supplying our own `SigningKey`. In other
    /// words: our signer is cryptographically equivalent to the "normal" path, just with the
    /// secret kept in mlocked memory throughout.
    #[test]
    fn signing_key_matches_ring_eddsa_signer() {
        let keypair = Keypair::new();

        let mut cert_der_bytes = Vec::<u8>::with_capacity(CERT_TOTAL_LEN);
        let mut pkcs8_der_bytes = vec![0u8; PKCS8_TOTAL_LEN].into_boxed_slice();
        {
            let mut pkcs8_cursor: &mut [u8] = &mut pkcs8_der_bytes;
            new_dummy_x509_certificate(&keypair, &mut cert_der_bytes, &mut pkcs8_cursor);
        }

        let our_signing_key = MlockedSigningKey {
            material: Arc::new(KeypairMaterial {
                pkcs8_der_bytes: pkcs8_der_bytes.clone(),
                _key_lock: None,
            }),
        };
        let our_signer = our_signing_key
            .choose_scheme(&[SignatureScheme::ED25519])
            .expect("our signing key should support ED25519");

        let pkcs8_key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(pkcs8_der_bytes.into_vec());
        let ring_signing_key = rustls::crypto::ring::sign::any_eddsa_type(&pkcs8_key_der)
            .expect("ring should load our PKCS8-encoded Ed25519 key");
        let ring_signer = ring_signing_key
            .choose_scheme(&[SignatureScheme::ED25519])
            .expect("ring signing key should support ED25519");

        let message = b"some handshake transcript bytes to sign";
        let our_signature = our_signer.sign(message).expect("our signer should sign");
        let ring_signature = ring_signer.sign(message).expect("ring signer should sign");

        assert_eq!(
            our_signature, ring_signature,
            "our Ed25519 signature differs from ring's for the same key and message"
        );
    }

    /// Confirms [`HardenedKeypair::read_from_reader`] parses the exact same JSON keypair format
    /// as `solana_keypair::read_keypair` and recovers byte-identical secret and public keys --
    /// so switching to it doesn't change what keypair files are accepted, only how the bytes
    /// are handled in memory while getting there.
    #[test]
    fn read_from_reader_matches_solana_keypair_read_keypair() {
        let keypair = Keypair::new();
        let mut file_contents = Vec::new();
        solana_keypair::write_keypair(&keypair, &mut file_contents).expect("write_keypair");

        let hardened = HardenedKeypair::read_from_reader(&mut file_contents.as_slice())
            .expect("read_from_reader");
        assert_eq!(hardened.pubkey(), keypair.pubkey());
        assert_eq!(hardened.private_bytes(), keypair.secret_bytes());

        let reference = solana_keypair::read_keypair(&mut file_contents.as_slice())
            .expect("solana_keypair::read_keypair");
        assert_eq!(hardened.pubkey(), reference.pubkey());
        assert_eq!(hardened.private_bytes(), reference.secret_bytes());
    }

    #[test]
    fn read_from_file_round_trips() {
        let keypair = Keypair::new();
        let path =
            std::env::temp_dir().join(format!("hardened-keypair-test-{}.json", std::process::id()));
        solana_keypair::write_keypair_file(&keypair, &path).expect("write_keypair_file");

        let hardened = HardenedKeypair::read_from_file(&path).expect("read_from_file");
        assert_eq!(hardened.pubkey(), keypair.pubkey());
        assert_eq!(hardened.private_bytes(), keypair.secret_bytes());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_from_reader_rejects_malformed_input() {
        let mut bad_input = b"not a keypair".as_slice();
        assert!(HardenedKeypair::read_from_reader(&mut bad_input).is_err());
    }

    /// A [`HardenedKeypair`] should be usable everywhere a [`Keypair`]-derived
    /// [`TpuIdentity`] is, via [`TpuEd25519SigningKey`], without ever needing to
    /// materialize a plain `Keypair`.
    #[test]
    fn hardened_keypair_builds_a_matching_tpu_identity() {
        let keypair = Keypair::new();
        let mut file_contents = Vec::new();
        solana_keypair::write_keypair(&keypair, &mut file_contents).expect("write_keypair");

        let hardened = HardenedKeypair::read_from_reader(&mut file_contents.as_slice())
            .expect("read_from_reader");
        let identity = TpuIdentity::from_ed25519_signing_key(&hardened);

        assert_eq!(identity.pubkey(), keypair.pubkey());
    }

    /// Forces multiple `grow()`s (starting from a 4-byte capacity against a much longer input)
    /// and checks every byte survives each resize intact.
    #[test]
    fn growable_hardened_buffer_grows_to_fit_input() {
        let mut buf = GrowableHardenedBuffer::with_capacity(4);
        let input = b"a somewhat longer input than four bytes, to force multiple grows";
        let mut reader = input.as_slice();
        loop {
            let n = buf.read_from(&mut reader).expect("read_from");
            if n == 0 {
                break;
            }
        }
        assert_eq!(buf.as_slice(), input);
    }

    /// A keypair file larger than [`HardenedKeypair::read_from_reader`]'s initial 256-byte
    /// guess (e.g. one with extra whitespace) should still parse correctly -- exercising the
    /// same growth path as the real function, not just the buffer in isolation.
    #[test]
    fn read_from_reader_handles_input_larger_than_initial_capacity() {
        let keypair = Keypair::new();
        let mut file_contents = Vec::new();
        solana_keypair::write_keypair(&keypair, &mut file_contents).expect("write_keypair");

        // Pad well past the 256-byte initial capacity with insignificant leading whitespace.
        let mut padded = vec![b' '; 1024];
        padded.extend_from_slice(&file_contents);

        let hardened =
            HardenedKeypair::read_from_reader(&mut padded.as_slice()).expect("read_from_reader");
        assert_eq!(hardened.pubkey(), keypair.pubkey());
        assert_eq!(hardened.private_bytes(), keypair.secret_bytes());
    }

    #[test]
    fn try_from_bytes_accepts_valid_keypair_bytes() {
        let keypair = Keypair::new();
        let bytes = keypair.to_bytes();

        let hardened = HardenedKeypair::try_from(&bytes[..]).expect("try_from");
        assert_eq!(hardened.pubkey(), keypair.pubkey());
        assert_eq!(hardened.private_bytes(), keypair.secret_bytes());
    }

    /// If the public half doesn't actually correspond to the secret half -- a corrupted or
    /// tampered keypair file -- `try_from` must reject it rather than silently building a
    /// `HardenedKeypair` whose `pubkey()` lies about which secret it holds.
    #[test]
    fn try_from_bytes_rejects_mismatched_public_key() {
        let keypair = Keypair::new();
        let mut bytes = keypair.to_bytes();
        let unrelated_pubkey = Keypair::new().pubkey();
        bytes[32..].copy_from_slice(&unrelated_pubkey.to_bytes());

        assert!(HardenedKeypair::try_from(&bytes[..]).is_err());
    }

    #[test]
    fn try_from_bytes_rejects_wrong_length() {
        let too_short = [0u8; 32];
        assert!(HardenedKeypair::try_from(&too_short[..]).is_err());
    }
}
