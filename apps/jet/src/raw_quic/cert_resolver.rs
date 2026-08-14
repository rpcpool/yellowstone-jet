//! Haproxy-style directory loader for jet's own raw-QUIC server certificate(s).
//!
//! Each `*.pem` file in the directory is a self-contained bundle: a certificate chain
//! followed by its private key, concatenated in one file (haproxy's `crt <dir>`
//! convention). The SNI name a bundle answers to is its filename stem (e.g.
//! `customer-a.example.com.pem` answers SNI `customer-a.example.com`). A file named
//! `default.pem` — or, absent that, the lexicographically-first bundle — is used when a
//! connection presents no SNI or one that doesn't match any bundle, which is the common
//! case here since customers typically connect by IP, not hostname.

use {
    arc_swap::ArcSwap,
    rustls::{
        crypto::aws_lc_rs::sign::any_supported_type,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        server::{ClientHello, ResolvesServerCert},
        sign::CertifiedKey,
    },
    std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
    },
};

const DEFAULT_BUNDLE_STEM: &str = "default";
const BUNDLE_EXTENSION: &str = "pem";

#[derive(Debug, thiserror::Error)]
pub enum CertResolverError {
    #[error("server cert directory {0:?} does not contain any *.{BUNDLE_EXTENSION} bundle")]
    EmptyDirectory(PathBuf),
    #[error("failed to read server cert directory {0:?}: {1}")]
    ReadDir(PathBuf, std::io::Error),
    #[error("failed to read bundle {0:?}: {1}")]
    ReadBundle(PathBuf, std::io::Error),
    #[error("bundle {0:?} does not contain a valid certificate: {1}")]
    InvalidCertificate(PathBuf, rustls::pki_types::pem::Error),
    #[error("bundle {0:?} does not contain a valid private key: {1}")]
    InvalidPrivateKey(PathBuf, rustls::pki_types::pem::Error),
    #[error("bundle {0:?} certificate/key pair is not usable: {1}")]
    UnusableKeyPair(PathBuf, rustls::Error),
}

struct CertStore {
    by_sni: HashMap<String, Arc<CertifiedKey>>,
    default: Arc<CertifiedKey>,
}

/// A hot-reloadable [`ResolvesServerCert`] backed by a directory of PEM bundles.
pub struct CertResolver {
    dir: PathBuf,
    store: ArcSwap<CertStore>,
}

impl CertResolver {
    pub fn from_dir(dir: impl Into<PathBuf>) -> Result<Arc<Self>, CertResolverError> {
        let dir = dir.into();
        let store = load_cert_store(&dir)?;
        Ok(Arc::new(Self {
            dir,
            store: ArcSwap::from_pointee(store),
        }))
    }

    /// Re-scans the configured directory and, if it still contains at least one usable
    /// bundle, atomically swaps in the freshly loaded certificates. Already-open
    /// connections are unaffected; only handshakes started after this call see the
    /// change.
    pub fn reload(&self) -> Result<(), CertResolverError> {
        let store = load_cert_store(&self.dir)?;
        self.store.store(Arc::new(store));
        Ok(())
    }
}

impl std::fmt::Debug for CertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertResolver")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let store = self.store.load();
        if let Some(name) = hello.server_name()
            && let Some(certified_key) = store.by_sni.get(name)
        {
            return Some(Arc::clone(certified_key));
        }
        Some(Arc::clone(&store.default))
    }
}

fn load_cert_store(dir: &Path) -> Result<CertStore, CertResolverError> {
    let entries =
        fs::read_dir(dir).map_err(|e| CertResolverError::ReadDir(dir.to_path_buf(), e))?;

    let mut bundle_paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| CertResolverError::ReadDir(dir.to_path_buf(), e))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some(BUNDLE_EXTENSION) {
            bundle_paths.push(path);
        }
    }
    bundle_paths.sort();

    if bundle_paths.is_empty() {
        return Err(CertResolverError::EmptyDirectory(dir.to_path_buf()));
    }

    let mut by_sni = HashMap::with_capacity(bundle_paths.len());
    let mut default = None;
    let mut first = None;

    for path in bundle_paths {
        let certified_key = load_bundle(&path)?;
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned();

        if first.is_none() {
            first = Some(Arc::clone(&certified_key));
        }
        if stem == DEFAULT_BUNDLE_STEM {
            default = Some(Arc::clone(&certified_key));
        }

        by_sni.insert(stem, certified_key);
    }

    let default = default.or(first).expect("bundle_paths was non-empty");

    Ok(CertStore { by_sni, default })
}

fn load_bundle(path: &Path) -> Result<Arc<CertifiedKey>, CertResolverError> {
    let bytes = fs::read(path).map_err(|e| CertResolverError::ReadBundle(path.to_path_buf(), e))?;

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<_, _>>()
        .map_err(|e| CertResolverError::InvalidCertificate(path.to_path_buf(), e))?;
    if certs.is_empty() {
        return Err(CertResolverError::InvalidCertificate(
            path.to_path_buf(),
            rustls::pki_types::pem::Error::NoItemsFound,
        ));
    }

    let key = PrivateKeyDer::from_pem_slice(&bytes)
        .map_err(|e| CertResolverError::InvalidPrivateKey(path.to_path_buf(), e))?;

    let signing_key = any_supported_type(&key)
        .map_err(|e| CertResolverError::UnusableKeyPair(path.to_path_buf(), e))?;

    Ok(Arc::new(CertifiedKey::new(certs, signing_key)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_bundle(dir: &Path, name: &str, cert_pem: &str, key_pem: &str) {
        let mut contents = cert_pem.to_owned();
        contents.push('\n');
        contents.push_str(key_pem);
        fs::write(dir.join(format!("{name}.pem")), contents).expect("write bundle");
    }

    fn generate_self_signed() -> (String, String) {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["example.invalid".to_owned()])
                .expect("self-signed cert");
        (cert.pem(), key_pair.serialize_pem())
    }

    #[test]
    fn loads_default_bundle_by_reserved_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert_pem, key_pem) = generate_self_signed();
        write_bundle(dir.path(), "default", &cert_pem, &key_pem);

        let resolver = CertResolver::from_dir(dir.path()).expect("load");
        let store = resolver.store.load();
        assert!(store.by_sni.contains_key("default"));
    }

    #[test]
    fn falls_back_to_lexicographically_first_when_no_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert_pem, key_pem) = generate_self_signed();
        write_bundle(dir.path(), "zzz", &cert_pem, &key_pem);
        let (cert_pem2, key_pem2) = generate_self_signed();
        write_bundle(dir.path(), "aaa", &cert_pem2, &key_pem2);

        let resolver = CertResolver::from_dir(dir.path()).expect("load");
        let store = resolver.store.load();
        // "aaa" sorts before "zzz", so it becomes the default.
        assert!(Arc::ptr_eq(
            &store.default,
            store.by_sni.get("aaa").unwrap()
        ));
    }

    #[test]
    fn empty_directory_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = CertResolver::from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, CertResolverError::EmptyDirectory(_)));
    }

    #[test]
    fn reload_picks_up_added_bundle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert_pem, key_pem) = generate_self_signed();
        write_bundle(dir.path(), "default", &cert_pem, &key_pem);
        let resolver = CertResolver::from_dir(dir.path()).expect("load");

        let (cert_pem2, key_pem2) = generate_self_signed();
        write_bundle(dir.path(), "customer-a", &cert_pem2, &key_pem2);
        resolver.reload().expect("reload");

        let store = resolver.store.load();
        assert!(store.by_sni.contains_key("customer-a"));
    }
}
