//! Transaction payload handling for the Jet ecosystem.
//!
//! This module implements a dual-format transaction system:
//! 1. Legacy format: Simple binary serialized transactions for backward compatibility
//! 2. New format: Structured format with additional metadata and configuration
//!
//! The system ensures backward compatibility in the following ways:
//! - New -> Old: New clients can detect old servers and fall back to legacy format
//! - Old -> New: Old clients send legacy format which new servers can still process
//! - New -> New: Full feature support with structured format
//!
//! Version detection happens at the protocol level, allowing graceful degradation
//! of features when communicating with older versions.

use {
    crate::{
        proto::jet::{
            publish_transaction,
            subscribe_transaction::{self, Payload},
            PublishTransaction, SubscribeTransaction, TransactionConfig, TransactionWrapper,
        },
        util::ms_since_epoch,
    },
    anyhow::Result,
    base64::prelude::{Engine, BASE64_STANDARD},
    serde::{Deserialize, Serialize},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::transaction::VersionedTransaction,
    solana_transaction_status::UiTransactionEncoding,
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum PayloadError {
    #[error("empty transaction payload")]
    EmptyPayload,
    #[error("failed to deserialize legacy JSON payload: {0}")]
    LegacyDeserialize(#[from] serde_json::Error),
    #[error("failed to decode base58 transaction: {0}")]
    Base58Decode(#[from] bs58::decode::Error),
    #[error("failed to decode base64 transaction: {0}")]
    Base64Decode(#[from] base64::DecodeError),
    #[error("unsupported encoding in legacy payload")]
    UnsupportedEncoding,
    #[error("failed to deserialize transaction: {0}")]
    BincodeError(#[from] bincode::Error),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LegacyPayload {
    pub transaction: String, // base58/base64 encoded transaction
    pub config: RpcSendTransactionConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

#[derive(Debug)]
pub enum TransactionPayload {
    Legacy(LegacyPayload),
    New(TransactionWrapper),
}

impl From<TransactionWrapper> for PublishTransaction {
    fn from(wrapper: TransactionWrapper) -> Self {
        Self {
            payload: Some(publish_transaction::Payload::NewPayload(wrapper)),
        }
    }
}

impl From<TransactionWrapper> for SubscribeTransaction {
    fn from(wrapper: TransactionWrapper) -> Self {
        Self {
            payload: Some(subscribe_transaction::Payload::NewPayload(wrapper)),
        }
    }
}

impl HasLegacyPayload for PublishTransaction {
    fn from_legacy_payload(bytes: Vec<u8>) -> Self {
        Self {
            payload: Some(publish_transaction::Payload::LegacyPayload(bytes)),
        }
    }
}

impl HasLegacyPayload for SubscribeTransaction {
    fn from_legacy_payload(bytes: Vec<u8>) -> Self {
        Self {
            payload: Some(subscribe_transaction::Payload::LegacyPayload(bytes)),
        }
    }
}

impl TryFrom<SubscribeTransaction> for TransactionPayload {
    type Error = PayloadError;

    fn try_from(tx: SubscribeTransaction) -> Result<Self, Self::Error> {
        match tx.payload {
            Some(Payload::LegacyPayload(bytes)) => {
                let legacy = serde_json::from_slice(&bytes)?;
                Ok(Self::Legacy(legacy))
            }
            Some(Payload::NewPayload(wrapper)) => Ok(Self::New(wrapper)),
            None => Err(PayloadError::EmptyPayload),
        }
    }
}

impl TryFrom<(&VersionedTransaction, RpcSendTransactionConfig)> for TransactionPayload {
    type Error = PayloadError;

    fn try_from(
        (transaction, config): (&VersionedTransaction, RpcSendTransactionConfig),
    ) -> Result<Self, Self::Error> {
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

        match encoding {
            UiTransactionEncoding::Base64 | UiTransactionEncoding::Base58 => (),
            _ => return Err(PayloadError::UnsupportedEncoding),
        }

        let tx_bytes = bincode::serialize(transaction)?;
        let tx_str = match encoding {
            UiTransactionEncoding::Base58 => bs58::encode(tx_bytes).into_string(),
            _ => BASE64_STANDARD.encode(tx_bytes),
        };

        Ok(Self::Legacy(LegacyPayload {
            transaction: tx_str,
            config,
            timestamp: Some(ms_since_epoch()),
        }))
    }
}

pub trait HasLegacyPayload {
    fn from_legacy_payload(bytes: Vec<u8>) -> Self;
}

impl TransactionPayload {
    pub fn to_proto<T>(&self) -> T
    where
        T: From<TransactionWrapper>, // For the New variant
        T: HasLegacyPayload,         // For the Legacy variant
    {
        match self {
            Self::Legacy(legacy) => {
                let bytes = serde_json::to_vec(legacy).expect("Failed to serialize legacy payload");
                T::from_legacy_payload(bytes)
            }
            Self::New(wrapper) => T::from(wrapper.clone()),
        }
    }
}

pub struct TransactionDecoder;

impl TransactionDecoder {
    pub fn decode(
        payload: &TransactionPayload,
    ) -> Result<(VersionedTransaction, Option<RpcSendTransactionConfig>), PayloadError> {
        match payload {
            TransactionPayload::Legacy(legacy) => {
                let tx_bytes = match legacy
                    .config
                    .encoding
                    .unwrap_or(UiTransactionEncoding::Base58)
                {
                    UiTransactionEncoding::Base58 => {
                        bs58::decode(&legacy.transaction).into_vec()?
                    }
                    UiTransactionEncoding::Base64 => BASE64_STANDARD.decode(&legacy.transaction)?,
                    _ => return Err(PayloadError::UnsupportedEncoding),
                };

                let tx = bincode::deserialize(&tx_bytes)?;
                Ok((tx, Some(legacy.config)))
            }
            TransactionPayload::New(wrapper) => {
                let tx = bincode::deserialize(&wrapper.transaction)?;
                let config = wrapper.config.as_ref().map(convert_transaction_config);
                Ok((tx, config))
            }
        }
    }
}

fn convert_transaction_config(proto_config: &TransactionConfig) -> RpcSendTransactionConfig {
    RpcSendTransactionConfig {
        max_retries: proto_config.max_retries.map(|r| r as usize),
        skip_preflight: proto_config.skip_preflight,
        skip_sanitize: proto_config.skip_sanitize,
        preflight_commitment: None,
        encoding: None,
        min_context_slot: None,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::util::ms_since_epoch};

    fn create_test_wrapper(tx: &VersionedTransaction) -> TransactionWrapper {
        TransactionWrapper {
            transaction: bincode::serialize(tx).unwrap(),
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec!["test1".to_string(), "test2".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(1234567890),
        }
    }

    #[test]
    fn test_legacy_format() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            skip_preflight: true,
            skip_sanitize: true,
            ..Default::default()
        };

        let payload = TransactionPayload::try_from((&tx, config)).unwrap();

        let proto_tx = payload.to_proto::<SubscribeTransaction>();
        let decoded = TransactionPayload::try_from(proto_tx).unwrap();

        let (decoded_tx, decoded_config) = TransactionDecoder::decode(&decoded).unwrap();
        assert_eq!(decoded_tx.signatures, tx.signatures);
        assert!(decoded_config.is_some());
        let decoded_config = decoded_config.unwrap();
        assert_eq!(decoded_config.skip_preflight, config.skip_preflight);
    }

    #[test]
    fn test_new_format_features() {
        let tx = VersionedTransaction::default();
        let wrapper = TransactionWrapper {
            transaction: bincode::serialize(&tx).unwrap(),
            config: Some(TransactionConfig {
                max_retries: Some(5),
                blocklist_pdas: vec!["test1".to_string()],
                skip_preflight: true,
                skip_sanitize: true,
            }),
            timestamp: Some(ms_since_epoch()),
        };

        let payload = TransactionPayload::New(wrapper);
        let (_, config) = TransactionDecoder::decode(&payload).unwrap();

        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.max_retries, Some(5));
    }

    #[test]
    fn test_new_format() {
        let tx = VersionedTransaction::default();
        let wrapper = create_test_wrapper(&tx);

        let payload = TransactionPayload::New(wrapper);
        let proto_tx = payload.to_proto::<SubscribeTransaction>();
        let decoded = TransactionPayload::try_from(proto_tx).unwrap();

        let (decoded_tx, config) = TransactionDecoder::decode(&decoded).unwrap();
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.max_retries, Some(5));
        assert!(config.skip_preflight);
        assert_eq!(decoded_tx.signatures, tx.signatures);
    }

    #[test]
    fn test_config_conversion() {
        let proto_config = TransactionConfig {
            max_retries: Some(3),
            blocklist_pdas: vec!["test".to_string()],
            skip_preflight: true,
            skip_sanitize: true,
        };

        let rpc_config = convert_transaction_config(&proto_config);
        assert_eq!(rpc_config.max_retries, Some(3));
        assert!(rpc_config.skip_preflight);
        assert_eq!(rpc_config.preflight_commitment, None);
        assert_eq!(rpc_config.encoding, None);
        assert_eq!(rpc_config.min_context_slot, None);
    }

    #[test]
    fn test_transaction_config_sanitize_flags() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            skip_sanitize: true,
            encoding: Some(UiTransactionEncoding::Base64),
            ..Default::default()
        };

        let payload = TransactionPayload::try_from((&tx, config)).unwrap();
        let proto_tx = payload.to_proto::<SubscribeTransaction>();

        let decoded = TransactionPayload::try_from(proto_tx).unwrap();
        let (_, decoded_config) = TransactionDecoder::decode(&decoded).unwrap();

        assert!(decoded_config.is_some());
        let decoded_config = decoded_config.unwrap();
        assert!(decoded_config.skip_preflight);
        assert!(decoded_config.skip_sanitize);
    }

    #[test]
    fn test_config_preservation_through_conversion() {
        let tx = VersionedTransaction::default();
        let original_config = RpcSendTransactionConfig {
            skip_preflight: true,
            skip_sanitize: false,
            max_retries: Some(3),
            preflight_commitment: None,
            encoding: Some(UiTransactionEncoding::Base64),
            min_context_slot: None,
        };

        let payload = TransactionPayload::try_from((&tx, original_config)).unwrap();

        let (_, decoded_config) = TransactionDecoder::decode(&payload).unwrap();

        let decoded_config = decoded_config.unwrap();
        assert_eq!(decoded_config.max_retries, original_config.max_retries);
        assert_eq!(
            decoded_config.skip_preflight,
            original_config.skip_preflight
        );
        assert_eq!(decoded_config.skip_sanitize, original_config.skip_sanitize);
    }

    #[test]
    fn test_invalid_encoding() {
        let tx = VersionedTransaction::default();
        let config = RpcSendTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            ..Default::default()
        };

        assert!(TransactionPayload::try_from((&tx, config)).is_err());
    }

    #[test]
    fn test_empty_config_conversion() {
        let proto_config = TransactionConfig::default();
        let rpc_config = convert_transaction_config(&proto_config);

        assert_eq!(rpc_config.max_retries, None);
        assert!(!rpc_config.skip_preflight);
        assert!(!rpc_config.skip_sanitize);
    }
}
