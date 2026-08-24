//! Extracts jet's own customer identity out of a client certificate's Subject
//! Alternative Name, rather than keying per-client behavior (e.g.
//! [`super::connection_limiter::ConnectionLimiter`]) off the raw certificate itself --
//! that would tie a limit to one specific cert and reset it every time a customer
//! rotates their certificate.

use {
    rustls::pki_types::CertificateDer,
    std::fmt,
    uuid::Uuid,
    x509_parser::{asn1_rs::FromDer, certificate::X509Certificate, extensions::GeneralName},
};

const URN_PREFIX: &str = "urn:jet:account:";
const URN_SUBSCRIPTION_INFIX: &str = ":subscription:";

/// A customer identity, as encoded by jet's cert issuance into the leaf certificate's
/// Subject Alternative Name: a URI entry of the form
/// `urn:jet:account:<account-id>:subscription:<subscription-id>`, where
/// `<subscription-id>` is a UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    pub account_id: String,
    pub subscription_id: Uuid,
}

impl ClientIdentity {
    /// Parses `cert`'s Subject Alternative Name for a jet URI entry. Returns `None` if
    /// the certificate is malformed, has no SAN extension, or has no URI entry matching
    /// the expected `urn:jet:account:...:subscription:<uuid>` shape.
    pub fn from_leaf_certificate(cert: &CertificateDer<'_>) -> Option<Self> {
        let (_, cert) = X509Certificate::from_der(cert.as_ref()).ok()?;
        let san = cert.subject_alternative_name().ok()??;
        san.value.general_names.iter().find_map(|name| match name {
            GeneralName::URI(uri) => Self::parse_urn(uri),
            _ => None,
        })
    }

    fn parse_urn(uri: &str) -> Option<Self> {
        let rest = uri.strip_prefix(URN_PREFIX)?;
        let (account_id, subscription_id) = rest.split_once(URN_SUBSCRIPTION_INFIX)?;
        if account_id.is_empty() {
            return None;
        }
        let subscription_id = Uuid::parse_str(subscription_id).ok()?;
        Some(Self {
            account_id: account_id.to_owned(),
            subscription_id,
        })
    }
}

impl fmt::Display for ClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{URN_PREFIX}{}{URN_SUBSCRIPTION_INFIX}{}",
            self.account_id, self.subscription_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUBSCRIPTION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn identity(account_id: &str, subscription_id: &str) -> ClientIdentity {
        ClientIdentity {
            account_id: account_id.to_owned(),
            subscription_id: Uuid::parse_str(subscription_id).expect("valid uuid literal"),
        }
    }

    #[test]
    fn parses_a_well_formed_urn() {
        assert_eq!(
            ClientIdentity::parse_urn(&format!(
                "urn:jet:account:acct-1:subscription:{SUBSCRIPTION_ID}"
            )),
            Some(identity("acct-1", SUBSCRIPTION_ID))
        );
    }

    #[test]
    fn rejects_urns_missing_either_id() {
        assert_eq!(
            ClientIdentity::parse_urn(&format!("urn:jet:account::subscription:{SUBSCRIPTION_ID}")),
            None
        );
        assert_eq!(
            ClientIdentity::parse_urn("urn:jet:account:acct-1:subscription:"),
            None
        );
    }

    #[test]
    fn rejects_urns_with_a_non_uuid_subscription_id() {
        assert_eq!(
            ClientIdentity::parse_urn("urn:jet:account:acct-1:subscription:not-a-uuid"),
            None
        );
    }

    #[test]
    fn rejects_urns_with_the_wrong_shape() {
        assert_eq!(ClientIdentity::parse_urn("urn:other:thing"), None);
        assert_eq!(
            ClientIdentity::parse_urn("urn:jet:account:acct-1"),
            None,
            "missing the subscription segment entirely"
        );
    }

    #[test]
    fn display_round_trips_through_parse_urn() {
        let identity = identity("acct-1", SUBSCRIPTION_ID);
        assert_eq!(
            ClientIdentity::parse_urn(&identity.to_string()),
            Some(identity)
        );
    }

    #[test]
    fn from_leaf_certificate_extracts_the_san_uri() {
        use rcgen::{CertificateParams, KeyPair, SanType};

        let mut params = CertificateParams::new(Vec::<String>::new()).expect("cert params");
        params.subject_alt_names = vec![SanType::URI(
            format!("urn:jet:account:acct-1:subscription:{SUBSCRIPTION_ID}")
                .try_into()
                .expect("valid SAN URI string"),
        )];
        let key_pair = KeyPair::generate().expect("key pair");
        let cert = params.self_signed(&key_pair).expect("self-signed cert");

        assert_eq!(
            ClientIdentity::from_leaf_certificate(cert.der()),
            Some(identity("acct-1", SUBSCRIPTION_ID))
        );
    }

    #[test]
    fn from_leaf_certificate_is_none_without_a_matching_san() {
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["example.invalid".to_owned()])
                .expect("self-signed cert");

        assert_eq!(ClientIdentity::from_leaf_certificate(cert.der()), None);
    }
}
